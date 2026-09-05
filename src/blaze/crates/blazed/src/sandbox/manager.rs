// SPDX-License-Identifier: Apache-2.0
//! Recoverable sandbox create, destroy, and startup cleanup.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use blaze_core::BlazeError;
use blaze_core::backend::{BackendKind, RestoreRequest, SpawnRequest};
use blaze_core::lifecycle::{BackendOwnership, OperationKind, SandboxInstance, SandboxState};
use blaze_core::policy::RuntimeDecision;
use blaze_core::storage::{StorageProvider, StorageSlot};
use blaze_provider_api::{
    AbortRequest, CommitRequest, DataPlaneProvider, FinalizeRequest, InspectRequest, LeaseBinding,
    LeaseState, PrepareRequest, PrepareSource, PreparedLease, PreparedResources, ProviderError,
    PublicTransitionRef, ReleaseRequest, RequestContext, StopRequest, TemplateSource,
};
use blaze_provider_conformance::{
    validate_descriptor, validate_prepared, validate_prepared_binding, validate_transition,
};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard, Semaphore};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::checkpoint_store::CheckpointStore;
use crate::error::{BlazeDaemonError, Result};
use crate::guest::{GuestClient, GuestExecResult, MAX_GUEST_FILE_BYTES};
use crate::metrics::Metrics;
use crate::sandbox::template::{ResolvedTemplate, TemplateCatalog};
use crate::spawner::{
    BackendRestoreRequest, BackendSpawnRequest, DynBackendInstance, DynSpawner, PinnedExecutable,
    SpawnerRegistry, restore_with_runtime_directory, spawn_with_runtime_directory,
};
use crate::state_store::{OwnedRunDir, StateStore};

const GUEST_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Inputs already parsed and policy-evaluated by the API.
#[derive(Debug, Clone)]
pub struct CreateSandbox {
    /// Policy decision for this request.
    pub decision: RuntimeDecision,
    /// Image identity used by storage allocation.
    pub image_digest: String,
    /// Concrete backend selected from the policy and daemon availability.
    pub runtime_backend: BackendKind,
    /// Executable selected during daemon startup.
    pub binary_path: PathBuf,
    /// Published template to restore from, when the request named one.
    pub template: Option<String>,
}

/// Prepared inputs for one template-backed create, validated before allocation.
struct TemplateCreate {
    resolved: ResolvedTemplate,
    spawner: DynSpawner,
    executable: Option<Arc<PinnedExecutable>>,
    /// Console-recording shape the matched policy would launch.
    ///
    /// A restore derives its effective backend config from the request, so this
    /// must carry the policy's setting instead of silently disabling recording.
    record_console_log: bool,
}

/// Restore inputs derived from a materialized template slot.
struct TemplateRestore {
    payload_dir: PathBuf,
    expected_version: Option<String>,
    snapshot_kind: blaze_core::backend::SnapshotKind,
    expose_guest_socket: bool,
    preserve_network: bool,
    record_console_log: bool,
}

/// Restore metadata retained while the provider prepares its payload.
struct TemplateRestorePlan {
    expected_version: Option<String>,
    snapshot_kind: blaze_core::backend::SnapshotKind,
    expose_guest_socket: bool,
    preserve_network: bool,
    record_console_log: bool,
}

/// Provider preparation converted into inputs understood by existing backends.
struct PreparedCreateResources {
    binding: LeaseBinding,
    storage: Option<StorageSlot>,
    provider_attachments: Option<crate::spawner::ProviderRestoreAttachments>,
}

/// Result of one managed create request.
#[derive(Debug, Clone)]
pub struct CreateSandboxResult {
    /// Persisted sandbox metadata.
    pub instance: SandboxInstance,
    /// Backend implementation that owns the runtime.
    pub selected_backend: BackendKind,
}

/// One startup cleanup failure. Other records continue to be reconciled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileFailure {
    /// Sandbox whose cleanup remains incomplete.
    pub instance_id: Uuid,
    /// Actionable failure description.
    pub error: String,
}

/// Aggregate startup cleanup outcome.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    /// Number of non-terminal records examined.
    pub attempted: usize,
    /// Number moved to the terminal state.
    pub completed: usize,
    /// Records that remain recoverable.
    pub failures: Vec<ReconcileFailure>,
}

/// Owns durable lifecycle metadata and non-serializable runtime handles.
///
/// The maps are shared with read-only and non-lifecycle API paths. All
/// Create, destroy, and restart cleanup mutations enter
/// through this type and are serialized by a per-sandbox async lock.
pub struct SandboxManager {
    instances: Arc<Mutex<HashMap<Uuid, SandboxInstance>>>,
    backend_instances: Arc<Mutex<HashMap<Uuid, DynBackendInstance>>>,
    operation_locks: Mutex<HashMap<Uuid, Arc<AsyncMutex<()>>>>,
    pub(super) storage_sync_inflight: Arc<Mutex<HashSet<Uuid>>>,
    pub(super) storage_sync_permits: Arc<Semaphore>,
    spawners: Arc<SpawnerRegistry>,
    active_backend: BackendKind,
    pub(super) storage: Arc<dyn StorageProvider>,
    data_plane: Arc<dyn DataPlaneProvider>,
    data_plane_leases: Mutex<HashMap<Uuid, LeaseBinding>>,
    state_store: StateStore,
    pub(super) checkpoints: CheckpointStore,
    rootfs_size: u64,
    mem_size: u64,
    metrics: Arc<Metrics>,
    pub(super) template_catalog: TemplateCatalog,
}

/// Construction inputs grouped to keep daemon wiring explicit.
pub struct SandboxManagerInit {
    pub instances: HashMap<Uuid, SandboxInstance>,
    pub spawners: SpawnerRegistry,
    pub active_backend: BackendKind,
    pub storage: Arc<dyn StorageProvider>,
    pub data_plane: Arc<dyn DataPlaneProvider>,
    pub state_store: StateStore,
    pub rootfs_size: u64,
    pub mem_size: u64,
    pub template_catalog: TemplateCatalog,
}

/// Shared resources returned to the daemon wiring and test harness.
pub struct SandboxManagerResources {
    #[cfg(test)]
    pub instances: Arc<Mutex<HashMap<Uuid, SandboxInstance>>>,
    pub metrics: Arc<Metrics>,
}

impl SandboxManager {
    /// Return the retained runtime-directory owner for one sandbox.
    pub(super) fn run_directory(&self, id: Uuid) -> Result<OwnedRunDir> {
        self.state_store.run_dir(id)
    }

    /// Build a manager around state loaded from the durable state directory.
    pub fn new(init: SandboxManagerInit) -> (Self, SandboxManagerResources) {
        let SandboxManagerInit {
            instances,
            spawners,
            active_backend,
            storage,
            data_plane,
            state_store,
            rootfs_size,
            mem_size,
            template_catalog,
        } = init;
        let operation_locks = instances
            .keys()
            .copied()
            .map(|id| (id, Arc::new(AsyncMutex::new(()))))
            .collect();
        let instances = Arc::new(Mutex::new(instances));
        let backend_instances = Arc::new(Mutex::new(HashMap::new()));
        let metrics = Arc::new(Metrics::new());
        let checkpoints = CheckpointStore::new(state_store.clone());
        let resources = SandboxManagerResources {
            #[cfg(test)]
            instances: instances.clone(),
            metrics: metrics.clone(),
        };
        (
            Self {
                instances,
                backend_instances,
                operation_locks: Mutex::new(operation_locks),
                storage_sync_inflight: Arc::new(Mutex::new(HashSet::new())),
                // The periodic worker is sequential. Retain that bound when a
                // timed-out provider operation has to finish in the background.
                storage_sync_permits: Arc::new(Semaphore::new(1)),
                spawners: Arc::new(spawners),
                active_backend,
                storage,
                data_plane,
                data_plane_leases: Mutex::new(HashMap::new()),
                state_store,
                checkpoints,
                rootfs_size,
                mem_size,
                metrics,
                template_catalog,
            },
            resources,
        )
    }

    /// Return the async operation lock that serializes one sandbox mutation.
    pub fn operation_lock(&self, id: Uuid) -> Arc<AsyncMutex<()>> {
        match self.operation_locks.lock() {
            Ok(mut locks) => locks
                .entry(id)
                .or_insert_with(|| Arc::new(AsyncMutex::new(())))
                .clone(),
            Err(poisoned) => poisoned
                .into_inner()
                .entry(id)
                .or_insert_with(|| Arc::new(AsyncMutex::new(())))
                .clone(),
        }
    }

    pub(crate) fn backend_owner(&self, id: Uuid) -> Option<DynBackendInstance> {
        match self.backend_instances.lock() {
            Ok(instances) => instances.get(&id).cloned(),
            Err(poisoned) => poisoned.into_inner().get(&id).cloned(),
        }
    }

    pub(super) fn spawner(&self, backend: BackendKind) -> Option<DynSpawner> {
        self.spawners.get(backend)
    }

    pub(super) fn remove_backend_owner(&self, id: Uuid) -> Option<DynBackendInstance> {
        match self.backend_instances.lock() {
            Ok(mut instances) => instances.remove(&id),
            Err(poisoned) => poisoned.into_inner().remove(&id),
        }
    }

    #[cfg(test)]
    pub(crate) fn insert_backend_owner(&self, id: Uuid, owner: DynBackendInstance) -> Result<()> {
        self.backend_instances
            .lock()
            .map_err(|_| poisoned("backend_instances"))?
            .insert(id, owner);
        Ok(())
    }

    pub(super) async fn reconstruct_storage(&self, id: Uuid) -> Result<StorageSlot> {
        self.storage
            .reconstruct(&id.to_string())
            .await
            .map_err(Into::into)
    }

    pub(super) async fn sync_storage(&self, slot: &StorageSlot) -> Result<()> {
        self.storage.sync_artifacts(slot).await.map_err(Into::into)
    }

    async fn prepare_data_plane(&self, request: PrepareRequest) -> Result<PreparedLease> {
        let descriptor = self.data_plane.descriptor();
        validate_descriptor(descriptor).map_err(|_| {
            BlazeDaemonError::Internal("data-plane descriptor is incompatible".to_string())
        })?;
        let capabilities = self.data_plane.capabilities();
        let supported = match &request.source {
            PrepareSource::Image { .. } => capabilities.images,
            PrepareSource::Template(_) => capabilities.templates,
        };
        if !supported {
            return Err(BlazeDaemonError::UnsupportedOperation(
                "configured data plane does not support the requested source".to_string(),
            ));
        }
        let context = request.context;
        let template_source = matches!(&request.source, PrepareSource::Template(_));
        let root_filesystem_bytes = request.root_filesystem_bytes;
        let guest_memory_bytes = request.guest_memory_bytes;
        match self.data_plane.prepare(request).await {
            Ok(prepared) => {
                let validation = validate_prepared(
                    context,
                    template_source,
                    root_filesystem_bytes,
                    guest_memory_bytes,
                    &prepared,
                );
                let undeclared_opened_resources =
                    matches!(&prepared.resources, PreparedResources::OpenedRestore { .. })
                        && !capabilities.opened_restore_resources;
                if validation.is_err() || undeclared_opened_resources {
                    let violation = if undeclared_opened_resources {
                        "data plane returned opened resources without declaring the capability"
                    } else {
                        "data-plane prepare returned an invalid response"
                    };
                    if validate_prepared_binding(context, prepared.binding).is_err()
                        || prepared.binding.provider_instance_id != descriptor.provider_instance_id
                    {
                        return Err(BlazeDaemonError::RecoveryRequired(format!(
                            "{violation}; returned binding is not safe to compensate"
                        )));
                    }
                    return match self
                        .data_plane
                        .abort(AbortRequest {
                            binding: prepared.binding,
                        })
                        .await
                    {
                        Ok(aborted)
                            if validate_transition(
                                prepared.binding,
                                aborted.binding,
                                LeaseState::Released,
                            )
                            .is_ok() =>
                        {
                            Err(BlazeDaemonError::Internal(violation.to_string()))
                        }
                        Ok(_) => Err(BlazeDaemonError::RecoveryRequired(format!(
                            "{violation}; provider compensation returned an invalid transition"
                        ))),
                        Err(error) => Err(BlazeDaemonError::RecoveryRequired(format!(
                            "{violation}; provider compensation failed: {error}"
                        ))),
                    };
                }
                Ok(prepared)
            }
            Err(ProviderError::OutcomeUnknown) => {
                let observed = self
                    .data_plane
                    .inspect(InspectRequest { context })
                    .await
                    .map_err(|error| {
                        BlazeDaemonError::RecoveryRequired(format!(
                            "data-plane preparation outcome is unknown and inspection failed: {error}"
                        ))
                    })?;
                if validate_prepared_binding(context, observed.binding).is_err()
                    || observed.binding.provider_instance_id != descriptor.provider_instance_id
                {
                    return Err(BlazeDaemonError::RecoveryRequired(
                        "data-plane preparation inspection returned an unsafe state".to_string(),
                    ));
                }
                let aborted = self
                    .data_plane
                    .abort(AbortRequest {
                        binding: observed.binding,
                    })
                    .await
                    .map_err(|error| {
                        BlazeDaemonError::RecoveryRequired(format!(
                            "data-plane preparation was observed but compensation failed: {error}"
                        ))
                    })?;
                validate_transition(observed.binding, aborted.binding, LeaseState::Released)
                    .map_err(|_| {
                        BlazeDaemonError::RecoveryRequired(
                            "data-plane preparation compensation returned an invalid transition"
                                .to_string(),
                        )
                    })?;
                Err(BlazeDaemonError::DataPlane(ProviderError::OutcomeUnknown))
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn commit_data_plane(&self, binding: LeaseBinding) -> Result<LeaseBinding> {
        match self.data_plane.commit(CommitRequest { binding }).await {
            Ok(committed) => {
                validate_transition(binding, committed.binding, LeaseState::Committed).map_err(
                    |_| {
                        BlazeDaemonError::Internal(
                            "data-plane commit returned an invalid transition".to_string(),
                        )
                    },
                )?;
                Ok(committed.binding)
            }
            Err(ProviderError::OutcomeUnknown) => {
                let observed = self
                    .data_plane
                    .inspect(InspectRequest {
                        context: binding.context,
                    })
                    .await?;
                validate_transition(binding, observed.binding, LeaseState::Committed).map_err(
                    |_| {
                        BlazeDaemonError::RecoveryRequired(
                            "data-plane commit outcome cannot be proved safe".to_string(),
                        )
                    },
                )?;
                Ok(observed.binding)
            }
            Err(error) => Err(error.into()),
        }
    }

    fn retain_data_plane_lease(&self, id: Uuid, binding: LeaseBinding) -> Result<()> {
        let mut leases = self
            .data_plane_leases
            .lock()
            .map_err(|_| poisoned("data_plane_leases"))?;
        if let Some(previous) = leases.insert(id, binding)
            && previous.context.lease_id != binding.context.lease_id
        {
            leases.insert(id, previous);
            return Err(BlazeDaemonError::Conflict(format!(
                "sandbox {id} already owns another data-plane lease"
            )));
        }
        Ok(())
    }

    fn data_plane_lease(&self, id: Uuid) -> Result<Option<LeaseBinding>> {
        Ok(self
            .data_plane_leases
            .lock()
            .map_err(|_| poisoned("data_plane_leases"))?
            .get(&id)
            .copied())
    }

    fn remove_data_plane_lease(&self, id: Uuid) -> Result<()> {
        self.data_plane_leases
            .lock()
            .map_err(|_| poisoned("data_plane_leases"))?
            .remove(&id);
        Ok(())
    }

    /// Return all persisted sandbox metadata.
    pub fn list(&self) -> Result<Vec<SandboxInstance>> {
        Ok(self
            .instances
            .lock()
            .map_err(|_| poisoned("instances"))?
            .values()
            .cloned()
            .collect())
    }

    /// Return one persisted sandbox.
    pub fn get(&self, id: Uuid) -> Result<SandboxInstance> {
        self.instances
            .lock()
            .map_err(|_| poisoned("instances"))?
            .get(&id)
            .cloned()
            .ok_or_else(|| BlazeDaemonError::NotFound(format!("instance {id}")))
    }

    /// Return every sandbox for which lifecycle cleanup still owns resources.
    ///
    /// Shutdown uses this snapshot to start cleanup concurrently while all
    /// mutations remain serialized by the manager's per-sandbox locks.
    pub(crate) fn owned_instance_ids(&self) -> Result<BTreeSet<Uuid>> {
        let mut ids = self
            .instances
            .lock()
            .map_err(|_| poisoned("instances"))?
            .values()
            .filter(|instance| requires_automatic_cleanup(instance))
            .map(|instance| instance.id)
            .collect::<BTreeSet<_>>();
        ids.extend(
            self.backend_instances
                .lock()
                .map_err(|_| poisoned("backend_instances"))?
                .keys()
                .copied(),
        );
        Ok(ids)
    }

    /// Execute one command through the running sandbox guest.
    pub async fn exec(
        &self,
        id: Uuid,
        command: String,
        cwd: Option<String>,
        env: Option<HashMap<String, String>>,
        timeout_secs: u32,
    ) -> Result<GuestExecResult> {
        let _operation = self.lock_running(id).await?;
        self.guest_client(id)?
            .exec(command, cwd, env, timeout_secs)
            .await
            .map_err(BlazeDaemonError::from)
    }

    /// Read one file through the running sandbox guest.
    pub async fn read_file(&self, id: Uuid, path: String) -> Result<Vec<u8>> {
        let _operation = self.lock_running(id).await?;
        self.guest_client(id)?
            .read_file(path)
            .await
            .map_err(BlazeDaemonError::from)
    }

    /// Replace one file through the running sandbox guest.
    pub async fn write_file(&self, id: Uuid, path: String, data: &[u8]) -> Result<()> {
        let _operation = self.lock_running(id).await?;
        self.guest_client(id)?
            .write_file(path, data)
            .await
            .map_err(BlazeDaemonError::from)
    }

    /// Validate a template-backed create before any lifecycle state is written.
    ///
    /// Returns `None` for an ordinary create. For a template request it checks
    /// the policy allow-list, storage support, and catalog metadata, then
    /// confirms the published snapshot's image, backend, version, kernel
    /// command line, VM shape, and guest transport all match what the current
    /// policy would launch. The pinned executable and resolved artifacts are
    /// carried forward so the create path restores exactly what was validated.
    async fn prepare_template_create(
        &self,
        request: &CreateSandbox,
    ) -> Result<Option<TemplateCreate>> {
        let Some(name) = request.template.as_ref() else {
            return Ok(None);
        };
        if !request
            .decision
            .templates
            .iter()
            .any(|allowed| allowed == name)
        {
            return Err(BlazeDaemonError::Conflict(format!(
                "template {name} is not allowed by policy {}",
                request.decision.policy_name
            )));
        }
        if !self.data_plane.capabilities().templates {
            return Err(BlazeDaemonError::UnsupportedOperation(
                "configured data plane does not support templates".to_string(),
            ));
        }

        let resolved = self.resolve_template_for_create(name.clone()).await?;
        if resolved.image_digest != request.image_digest {
            return Err(BlazeDaemonError::Conflict(format!(
                "template {name} image identity does not match the create request"
            )));
        }
        if resolved.backend != request.runtime_backend {
            return Err(BlazeDaemonError::Conflict(format!(
                "template {name} requires backend {}, but the request selected {}",
                resolved.backend, request.runtime_backend
            )));
        }

        if resolved.backend == BackendKind::Firecracker {
            let config = request
                .decision
                .backend
                .firecracker
                .as_ref()
                .cloned()
                .unwrap_or_default();
            if config.enable_vsock != resolved.expose_guest_socket
                || config.enable_network != resolved.network
            {
                return Err(BlazeDaemonError::Conflict(format!(
                    "template {name} guest transport shape does not match policy {}",
                    request.decision.policy_name
                )));
            }
            let effective_boot_args =
                crate::spawner::firecracker::effective_boot_args(&config, config.enable_network)?;
            validate_template_boot_args(
                name,
                resolved.boot_args.as_deref(),
                &effective_boot_args,
                &request.decision.policy_name,
            )?;
            let (vcpus, memory_mib) = crate::spawner::firecracker::effective_vm_shape(
                &config,
                request.decision.vm.as_ref(),
            )?;
            if resolved.vcpus != Some(vcpus) || resolved.memory_mib != Some(memory_mib) {
                return Err(BlazeDaemonError::Conflict(format!(
                    "template {name} VM shape does not match policy {}",
                    request.decision.policy_name
                )));
            }
        } else {
            if resolved.expose_guest_socket {
                return Err(BlazeDaemonError::UnsupportedOperation(format!(
                    "template {name} requests guest transport for unsupported backend {}",
                    resolved.backend
                )));
            }
            if resolved.network {
                return Err(BlazeDaemonError::UnsupportedOperation(format!(
                    "template {name} requests networking for unsupported backend {}",
                    resolved.backend
                )));
            }
        }

        let spawner = self.spawner(resolved.backend).ok_or_else(|| {
            BlazeDaemonError::UnsupportedOperation(format!(
                "template {name} has no restore adapter for {}",
                resolved.backend
            ))
        })?;
        // A backend that runs no separate program of its own carries no
        // configured path; pin one only when a real executable is configured.
        let executable = if request.binary_path.as_os_str().is_empty() {
            None
        } else {
            Some(Arc::new(PinnedExecutable::open(&request.binary_path)?))
        };
        let capability = spawner
            .restore_capability(executable.as_deref())
            .await?
            .ok_or_else(|| {
                BlazeDaemonError::UnsupportedOperation(format!(
                    "template {name} backend {} does not support restore",
                    resolved.backend
                ))
            })?;
        if capability.backend != resolved.backend
            || capability.version != resolved.backend_version
            || capability.snapshot_kind != resolved.snapshot_kind
        {
            return Err(BlazeDaemonError::UnsupportedOperation(format!(
                "template {name} is incompatible with the current restore adapter"
            )));
        }

        Ok(Some(TemplateCreate {
            resolved,
            spawner,
            executable,
            record_console_log: request
                .decision
                .backend
                .firecracker
                .as_ref()
                .is_some_and(|config| config.serial_log),
        }))
    }

    /// Create a sandbox from a fresh runtime allocation or a published template.
    pub async fn create(&self, request: CreateSandbox) -> Result<CreateSandboxResult> {
        let template = self.prepare_template_create(&request).await?;
        let mut instance = SandboxInstance::new(
            request.runtime_backend,
            request.decision.workload_class,
            request.image_digest.clone(),
            request.decision.policy_name.clone(),
        );
        instance.template = template
            .as_ref()
            .map(|template| template.resolved.name.clone());
        let operation_lock = self.operation_lock(instance.id);
        let _operation = operation_lock.lock().await;
        instance.transition(SandboxState::Creating)?;
        instance.begin_operation(OperationKind::Create);

        // Publish the stable identity and create intent before allocation.
        if let Err(error) = self.state_store.persist(&instance) {
            match self.state_store.has_run_dir_residual(instance.id) {
                Ok(true) => {}
                Ok(false) => return Err(error),
                Err(residual_error) => {
                    return Err(BlazeDaemonError::RecoveryRequired(format!(
                        "create {}: initial state publication failed: {error}; could not inspect \
                         publication residual: {residual_error}",
                        instance.id
                    )));
                }
            }
            let rollback_errors = self.commit_create_rollback(&mut instance);
            if rollback_errors.is_empty() {
                return Err(error);
            }
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "create {}: initial state publication failed: {error}; {}",
                instance.id,
                rollback_errors.join("; ")
            )));
        }
        if let Some(error) = self.retain_instance(instance.clone()) {
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "create {}: {error}",
                instance.id
            )));
        }

        let context = RequestContext {
            instance_id: instance.id,
            request_id: Uuid::new_v4(),
            operation_id: Uuid::new_v4(),
            lease_id: Uuid::new_v4(),
            generation: 1,
        };
        let (prepare_request, template_plan, template) = match template {
            Some(TemplateCreate {
                resolved,
                spawner,
                executable,
                record_console_log,
            }) => {
                let ResolvedTemplate {
                    image_digest,
                    backend_version,
                    snapshot_kind,
                    expose_guest_socket,
                    network,
                    rootfs_size,
                    memory_size,
                    storage: source,
                    ..
                } = resolved;
                (
                    PrepareRequest {
                        context,
                        source: PrepareSource::Template(TemplateSource {
                            image_digest,
                            storage: source,
                        }),
                        root_filesystem_bytes: rootfs_size,
                        guest_memory_bytes: memory_size,
                    },
                    Some(TemplateRestorePlan {
                        expected_version: backend_version,
                        snapshot_kind,
                        expose_guest_socket,
                        // A new sandbox never inherits the source's network
                        // slot, so a networked template requests a fresh one.
                        preserve_network: network,
                        record_console_log,
                    }),
                    Some((spawner, executable)),
                )
            }
            None => (
                PrepareRequest {
                    context,
                    source: PrepareSource::Image {
                        image_digest: request.image_digest.clone(),
                    },
                    root_filesystem_bytes: self.rootfs_size,
                    guest_memory_bytes: self.mem_size,
                },
                None,
                None,
            ),
        };
        let prepared = match self.prepare_data_plane(prepare_request).await {
            Ok(prepared) => prepared,
            Err(error) => {
                let errors = self.commit_create_rollback(&mut instance);
                return if errors.is_empty() {
                    Err(error)
                } else {
                    Err(BlazeDaemonError::RecoveryRequired(format!(
                        "{error}; {}",
                        errors.join("; ")
                    )))
                };
            }
        };
        let binding = prepared.binding;
        let (resources, template_restore) = match (prepared.resources, template_plan) {
            (
                PreparedResources::PathBacked {
                    storage,
                    restore_payload_dir,
                },
                plan,
            ) => {
                let restore = match (restore_payload_dir, plan) {
                    (Some(payload_dir), Some(plan)) => Some(TemplateRestore {
                        payload_dir,
                        expected_version: plan.expected_version,
                        snapshot_kind: plan.snapshot_kind,
                        expose_guest_socket: plan.expose_guest_socket,
                        preserve_network: plan.preserve_network,
                        record_console_log: plan.record_console_log,
                    }),
                    (None, None) => None,
                    _ => {
                        return Err(self
                            .cleanup_failed_create(
                                &mut instance,
                                binding,
                                None,
                                false,
                                BlazeDaemonError::Internal(
                                    "data-plane restore payload does not match the create source"
                                        .to_string(),
                                ),
                            )
                            .await);
                    }
                };
                (
                    PreparedCreateResources {
                        binding,
                        storage: Some(storage),
                        provider_attachments: None,
                    },
                    restore,
                )
            }
            (
                PreparedResources::OpenedRestore {
                    restore_payload_dir,
                    attachments,
                },
                Some(plan),
            ) => {
                let provider_attachments = provider_restore_attachments(binding, attachments);
                (
                    PreparedCreateResources {
                        binding,
                        storage: None,
                        provider_attachments: Some(provider_attachments),
                    },
                    Some(TemplateRestore {
                        payload_dir: restore_payload_dir,
                        expected_version: plan.expected_version,
                        snapshot_kind: plan.snapshot_kind,
                        expose_guest_socket: plan.expose_guest_socket,
                        preserve_network: plan.preserve_network,
                        record_console_log: plan.record_console_log,
                    }),
                )
            }
            (PreparedResources::OpenedRestore { .. }, None) => {
                return Err(self
                    .cleanup_failed_create(
                        &mut instance,
                        binding,
                        None,
                        false,
                        BlazeDaemonError::UnsupportedOperation(
                            "ordinary image creation requires path-backed provider resources"
                                .to_string(),
                        ),
                    )
                    .await);
            }
        };
        crate::failpoint::pause("create-after-storage-acquire").await;

        let work_dir = match self.state_store.run_dir(instance.id) {
            Ok(work_dir) => work_dir,
            Err(error) => {
                return Err(self
                    .cleanup_failed_create(&mut instance, resources.binding, None, false, error)
                    .await);
            }
        };
        let mut lease_binding = resources.binding;
        let storage = resources.storage;
        let provider_attachments = resources.provider_attachments;
        let (spawner, template_executable) = match template {
            Some((spawner, executable)) => (Some(spawner), executable),
            None => (self.spawners.get(self.active_backend), None),
        };
        let spawner = match spawner {
            Some(spawner) => spawner,
            None => {
                return Err(self
                    .cleanup_failed_create(
                        &mut instance,
                        lease_binding,
                        None,
                        false,
                        BlazeDaemonError::Internal(format!(
                            "active backend {} has no registered spawner",
                            self.active_backend
                        )),
                    )
                    .await);
            }
        };
        if let Err(error) = spawner.prepare_spawn(&work_dir).await {
            return Err(self
                .cleanup_failed_create(&mut instance, lease_binding, None, false, error.into())
                .await);
        }

        instance.backend_ownership = BackendOwnership::Starting;
        if let Err(error) = self.state_store.persist(&instance) {
            instance.backend_ownership = BackendOwnership::NotStarted;
            return Err(self
                .cleanup_failed_create(&mut instance, lease_binding, None, false, error)
                .await);
        }
        if let Some(error) = self.retain_instance(instance.clone()) {
            instance.backend_ownership = BackendOwnership::NotStarted;
            return Err(self
                .cleanup_failed_create(
                    &mut instance,
                    lease_binding,
                    None,
                    false,
                    BlazeDaemonError::Internal(error),
                )
                .await);
        }

        let template_backed = template_restore.is_some();
        let spawn = if let Some(template) = template_restore {
            let mut restore_request = match BackendRestoreRequest::new(
                RestoreRequest {
                    instance_id: instance.id,
                    binary_path: request.binary_path,
                    storage: storage.clone(),
                    payload_dir: template.payload_dir,
                    checkpoint_backend: instance.backend,
                    expected_version: template.expected_version,
                    snapshot_kind: template.snapshot_kind,
                    expose_guest_socket: template.expose_guest_socket,
                    preserve_network: template.preserve_network,
                    record_console_log: template.record_console_log,
                    // One published capture restores into many new sandboxes.
                    snapshot_from_other_sandbox: true,
                },
                work_dir.clone(),
                template_executable,
            ) {
                Ok(request) => request,
                Err(error) => {
                    instance.backend_ownership = BackendOwnership::NotStarted;
                    return Err(self
                        .cleanup_failed_create(
                            &mut instance,
                            lease_binding,
                            None,
                            false,
                            error.into(),
                        )
                        .await);
                }
            };
            restore_request.provider_attachments = provider_attachments;
            match crate::failpoint::backend("create-spawn") {
                Ok(()) => restore_with_runtime_directory(spawner.as_ref(), restore_request).await,
                Err(error) => Err(crate::spawner::SpawnFailure::clean(error)),
            }
        } else {
            let Some(storage) = storage.clone() else {
                instance.backend_ownership = BackendOwnership::NotStarted;
                return Err(self
                    .cleanup_failed_create(
                        &mut instance,
                        lease_binding,
                        None,
                        false,
                        BlazeDaemonError::UnsupportedOperation(
                            "ordinary image creation requires path-backed provider resources"
                                .to_string(),
                        ),
                    )
                    .await);
            };
            let backend_request = match BackendSpawnRequest::new(
                SpawnRequest {
                    instance_id: instance.id,
                    binary_path: request.binary_path,
                    storage: Some(storage),
                    backend: request.decision.backend,
                    vm: request.decision.vm,
                },
                work_dir.clone(),
            ) {
                Ok(request) => request,
                Err(error) => {
                    instance.backend_ownership = BackendOwnership::NotStarted;
                    return Err(self
                        .cleanup_failed_create(
                            &mut instance,
                            lease_binding,
                            None,
                            false,
                            error.into(),
                        )
                        .await);
                }
            };
            match crate::failpoint::backend("create-spawn") {
                Ok(()) => spawn_with_runtime_directory(spawner.as_ref(), backend_request).await,
                Err(error) => Err(crate::spawner::SpawnFailure::clean(error)),
            }
        };
        let actual_backend = match spawn {
            Ok(backend_instance) => {
                instance.backend_ownership = BackendOwnership::Running;
                // A restore reloads a captured identity; refuse to adopt a
                // backend owner whose identity diverges from durable state.
                if template_backed
                    && (backend_instance.instance_id() != instance.id
                        || backend_instance.backend() != instance.backend)
                {
                    return Err(self
                        .cleanup_failed_create(
                            &mut instance,
                            lease_binding,
                            Some(backend_instance),
                            false,
                            BlazeDaemonError::Internal(
                                "restored backend owner identity does not match durable state"
                                    .to_string(),
                            ),
                        )
                        .await);
                }
                let actual_backend = backend_instance.backend();
                if let Err(error) = self
                    .wait_for_guest_ready(&backend_instance, "create-guest-ready")
                    .await
                {
                    return Err(self
                        .cleanup_failed_create(
                            &mut instance,
                            lease_binding,
                            Some(backend_instance),
                            false,
                            error.into(),
                        )
                        .await);
                }
                let mut backend_instance = Some(backend_instance);
                let registered = match self.backend_instances.lock() {
                    Ok(mut instances) => {
                        instances.insert(
                            instance.id,
                            backend_instance
                                .take()
                                .expect("backend instance is present"),
                        );
                        true
                    }
                    Err(_) => false,
                };
                if !registered {
                    return Err(self
                        .cleanup_failed_create(
                            &mut instance,
                            lease_binding,
                            backend_instance,
                            false,
                            BlazeDaemonError::Internal(
                                "backend_instances lock poisoned".to_string(),
                            ),
                        )
                        .await);
                }
                actual_backend
            }
            Err(error) => {
                let (source, backend) = error.into_parts();
                instance.backend_ownership = if backend.is_some() {
                    BackendOwnership::Running
                } else {
                    BackendOwnership::Stopped
                };
                return Err(self
                    .cleanup_failed_create(
                        &mut instance,
                        lease_binding,
                        backend,
                        false,
                        source.into(),
                    )
                    .await);
            }
        };

        lease_binding = match self.commit_data_plane(lease_binding).await {
            Ok(binding) => binding,
            Err(error) => {
                return Err(self
                    .cleanup_failed_create(&mut instance, lease_binding, None, true, error)
                    .await);
            }
        };
        if let Err(error) = self.retain_data_plane_lease(instance.id, lease_binding) {
            return Err(self
                .cleanup_failed_create(&mut instance, lease_binding, None, true, error)
                .await);
        }

        if let Err(error) = instance.transition(SandboxState::Running) {
            return Err(self
                .cleanup_failed_create(&mut instance, lease_binding, None, true, error.into())
                .await);
        }
        instance.finish_operation();
        if let Err(error) = crate::failpoint::state("create-state-commit")
            .and_then(|_| self.state_store.persist(&instance))
        {
            return Err(self
                .cleanup_failed_create(&mut instance, lease_binding, None, true, error)
                .await);
        }
        if let Some(error) = self.retain_instance(instance.clone()) {
            return Err(self
                .cleanup_failed_create(
                    &mut instance,
                    lease_binding,
                    None,
                    true,
                    BlazeDaemonError::Internal(error),
                )
                .await);
        }
        lease_binding = match self
            .data_plane
            .finalize(FinalizeRequest {
                binding: lease_binding,
                public_transition: PublicTransitionRef {
                    instance_id: instance.id,
                    operation_id: lease_binding.context.operation_id,
                },
            })
            .await
        {
            Ok(finalized) => {
                if validate_transition(lease_binding, finalized.binding, LeaseState::Finalized)
                    .is_err()
                {
                    let _ = self.mark_recovery(instance.id);
                    return Err(BlazeDaemonError::RecoveryRequired(format!(
                        "create {}: data-plane finalize returned an invalid transition",
                        instance.id
                    )));
                }
                finalized.binding
            }
            Err(error) => {
                let _ = self.mark_recovery(instance.id);
                return Err(BlazeDaemonError::RecoveryRequired(format!(
                    "create {}: public state is durable but data-plane finalize failed: {error}",
                    instance.id
                )));
            }
        };
        self.retain_data_plane_lease(instance.id, lease_binding)?;
        self.metrics.inc(&self.metrics.instances_created);
        Ok(CreateSandboxResult {
            instance,
            selected_backend: actual_backend,
        })
    }

    /// Idempotently destroy one sandbox and its owned runtime resources.
    ///
    /// The supervised task retains per-sandbox serialization after a caller
    /// disconnects, so blocking filesystem cleanup cannot race a retry.
    pub async fn destroy(self: &Arc<Self>, id: Uuid) -> Result<bool> {
        let manager = Arc::clone(self);
        crate::failpoint::spawn(async move {
            let operation = manager.operation_lock(id).lock_owned().await;
            let result = manager.destroy_locked(id).await;
            drop(operation);
            result
        })
        .await
        .map_err(|error| {
            BlazeDaemonError::Internal(format!("destroy supervisor failed: {error}"))
        })?
    }

    async fn destroy_locked(&self, id: Uuid) -> Result<bool> {
        let mut original = self.get(id)?;
        if original.state == SandboxState::Destroyed {
            return Ok(false);
        }

        if original.operation.as_ref().map(|operation| operation.kind)
            != Some(OperationKind::Destroy)
        {
            original.begin_operation(OperationKind::Destroy);
        }
        if let Err(error) = crate::failpoint::state("destroy-intent-state-commit")
            .and_then(|_| self.state_store.persist(&original))
        {
            let _ = self.mark_recovery(id);
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "destroy {id}: intent persistence failed: {error}; resources retained"
            )));
        }
        if let Some(error) = self.retain_instance(original.clone()) {
            let _ = self.mark_recovery(id);
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "destroy {id}: {error}; resources retained"
            )));
        }
        let mut data_plane_binding = self.data_plane_lease(id)?;
        let mut data_plane_released = false;

        let backend = self
            .backend_instances
            .lock()
            .map_err(|_| poisoned("backend_instances"))?
            .get(&id)
            .cloned();
        let stop_result = match crate::failpoint::backend("destroy-kill") {
            Ok(()) => {
                if let Some(backend) = backend.as_ref() {
                    backend.kill().await
                } else if matches!(
                    original.backend_ownership,
                    BackendOwnership::NotStarted | BackendOwnership::Stopped
                ) {
                    Ok(())
                } else {
                    match self.spawners.get(original.backend) {
                        Some(spawner) => match self.state_store.run_dir(id) {
                            Ok(run_dir) => spawner.cleanup_orphan(id, &run_dir).await,
                            Err(error) => Err(BlazeError::BackendError {
                                msg: format!(
                                    "open owned run directory for persisted instance {id}: {error}"
                                ),
                            }),
                        },
                        None => Err(BlazeError::BackendError {
                            msg: format!(
                                "no recovery spawner registered for persisted backend {}",
                                original.backend
                            ),
                        }),
                    }
                }
            }
            Err(error) => Err(error),
        };
        if let Err(error) = stop_result {
            let recovery = self.mark_recovery(id).err();
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "destroy {id}: backend termination failed: {error}; owner and storage retained{}",
                recovery
                    .map(|error| format!("; recovery state persistence failed: {error}"))
                    .unwrap_or_default()
            )));
        }

        if let Some(binding) = data_plane_binding {
            match binding.state {
                LeaseState::Finalized => {
                    let stopped = self
                        .data_plane
                        .stop(StopRequest { binding })
                        .await
                        .map_err(|error| {
                            BlazeDaemonError::RecoveryRequired(format!(
                                "destroy {id}: backend stopped but data-plane stop failed: {error}"
                            ))
                        })?;
                    validate_transition(binding, stopped.binding, LeaseState::Stopped).map_err(
                        |_| {
                            BlazeDaemonError::RecoveryRequired(format!(
                                "destroy {id}: data-plane stop returned an invalid transition"
                            ))
                        },
                    )?;
                    data_plane_binding = Some(stopped.binding);
                    self.retain_data_plane_lease(id, stopped.binding)?;
                }
                LeaseState::Prepared | LeaseState::Committed => {
                    let aborted = self
                        .data_plane
                        .abort(AbortRequest { binding })
                        .await
                        .map_err(|error| {
                            BlazeDaemonError::RecoveryRequired(format!(
                                "destroy {id}: unfinished data-plane preparation could not be aborted: {error}"
                            ))
                        })?;
                    validate_transition(binding, aborted.binding, LeaseState::Released).map_err(
                        |_| {
                            BlazeDaemonError::RecoveryRequired(format!(
                                "destroy {id}: data-plane abort returned an invalid transition"
                            ))
                        },
                    )?;
                    data_plane_binding = Some(aborted.binding);
                    data_plane_released = true;
                }
                LeaseState::Stopped => {}
                LeaseState::Released => data_plane_released = true,
            }
        }

        original.backend_ownership = BackendOwnership::Stopped;
        if let Err(error) = crate::failpoint::state("destroy-stop-state-commit")
            .and_then(|_| self.state_store.persist(&original))
        {
            let recovery = self.mark_instance_recovery(original.clone()).err();
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "destroy {id}: backend stopped but stop state persistence failed: {error}; \
                 storage retained{}",
                recovery
                    .map(|error| format!("; recovery state persistence failed: {error}"))
                    .unwrap_or_default()
            )));
        }
        if let Some(error) = self.retain_instance(original.clone()) {
            let _ = self.mark_recovery(id);
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "destroy {id}: backend stopped but lifecycle retention failed: {error}; \
                 storage retained"
            )));
        }

        let checkpoints = self.checkpoints.clone();
        let checkpoint_cleanup = crate::failpoint::spawn_blocking(move || {
            crate::failpoint::pause_blocking("checkpoint-before-store-remove");
            checkpoints.remove_sandbox(id)
        })
        .await;
        let checkpoint_cleanup_error = match checkpoint_cleanup {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(error.to_string()),
            Err(error) => Some(format!("blocking task failed: {error}")),
        };
        if let Some(error) = checkpoint_cleanup_error {
            let recovery = self.mark_recovery(id).err();
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "destroy {id}: backend stopped but checkpoint cleanup failed: {error}; \
                 storage retained{}",
                recovery
                    .map(|error| format!("; recovery state persistence failed: {error}"))
                    .unwrap_or_default()
            )));
        }

        if let Err(error) = self.cleanup_hibernate_artifacts(id).await {
            let recovery = self.mark_recovery(id).err();
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "destroy {id}: backend stopped but hibernation cleanup failed: {error}; \
                 storage retained{}",
                recovery
                    .map(|error| format!("; recovery state persistence failed: {error}"))
                    .unwrap_or_default()
            )));
        }

        if !data_plane_released {
            if let Some(binding) = data_plane_binding {
                let released = self
                    .data_plane
                    .release(ReleaseRequest { binding })
                    .await
                    .map_err(|error| {
                        BlazeDaemonError::RecoveryRequired(format!(
                            "destroy {id}: backend stopped but data-plane release failed: {error}"
                        ))
                    })?;
                validate_transition(binding, released.binding, LeaseState::Released).map_err(
                    |_| {
                        BlazeDaemonError::RecoveryRequired(format!(
                            "destroy {id}: data-plane release returned an invalid transition"
                        ))
                    },
                )?;
                data_plane_released = true;
            } else if self.data_plane.capabilities().daemon_managed_storage {
                if let Err(error) = self.storage.release_by_id(&id.to_string()).await {
                    let recovery = self.mark_recovery(id).err();
                    return Err(BlazeDaemonError::RecoveryRequired(format!(
                        "destroy {id}: backend stopped but daemon-managed storage release failed: {error}; \
                         lifecycle retained for retry{}",
                        recovery
                            .map(|error| format!("; recovery state persistence failed: {error}"))
                            .unwrap_or_default()
                    )));
                }
                data_plane_released = true;
            } else {
                let recovery = self.mark_recovery(id).err();
                return Err(BlazeDaemonError::RecoveryRequired(format!(
                    "destroy {id}: no in-memory data-plane lease is available and this provider \
                     does not use daemon-managed storage{}",
                    recovery
                        .map(|error| format!("; recovery state persistence failed: {error}"))
                        .unwrap_or_default()
                )));
            }
        }
        if data_plane_released {
            self.remove_data_plane_lease(id)?;
        }

        let mut destroyed = original;
        if destroyed.state != SandboxState::Destroyed {
            destroyed.transition(SandboxState::Destroyed)?;
        }
        destroyed.finish_operation();
        if let Err(error) = crate::failpoint::state("destroy-final-state-commit")
            .and_then(|_| self.state_store.persist(&destroyed))
        {
            let recovery = self.mark_recovery(id).err();
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "destroy {id}: resources released but final state persistence failed: {error}{}",
                recovery
                    .map(|error| format!("; recovery state persistence failed: {error}"))
                    .unwrap_or_default()
            )));
        }
        let retention_error = self.retain_instance(destroyed);
        match self.backend_instances.lock() {
            Ok(mut instances) => {
                instances.remove(&id);
            }
            Err(poisoned) => {
                poisoned.into_inner().remove(&id);
            }
        }
        if let Some(error) = retention_error {
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "destroy {id}: resources released but {error}"
            )));
        }
        self.metrics.inc(&self.metrics.instances_destroyed);
        Ok(true)
    }

    /// Reconcile every non-terminal record without aborting on one failure.
    pub async fn reconcile_startup(&self) -> ReconcileReport {
        let mut classification_failures = self.classify_interrupted_hibernation();
        let mut report = self.cleanup_owned_instances().await;
        report.failures.append(&mut classification_failures);
        report
    }

    async fn lock_running(&self, id: Uuid) -> Result<OwnedMutexGuard<()>> {
        let operation = self.operation_lock(id).lock_owned().await;
        let instance = self.get(id)?;
        if instance.state != SandboxState::Running || instance.operation.is_some() {
            return Err(BlazeDaemonError::Conflict(format!(
                "instance {id} is not available for guest operations"
            )));
        }
        Ok(operation)
    }

    fn classify_interrupted_hibernation(&self) -> Vec<ReconcileFailure> {
        let interrupted = match self.instances.lock() {
            Ok(instances) => instances
                .values()
                .filter(|instance| {
                    matches!(
                        instance.state,
                        SandboxState::Hibernating | SandboxState::Resuming
                    ) || matches!(
                        instance.operation.as_ref().map(|operation| operation.kind),
                        Some(OperationKind::Hibernate | OperationKind::Resume)
                    )
                })
                .cloned()
                .collect::<Vec<_>>(),
            Err(poisoned) => poisoned
                .into_inner()
                .values()
                .filter(|instance| {
                    matches!(
                        instance.state,
                        SandboxState::Hibernating | SandboxState::Resuming
                    ) || matches!(
                        instance.operation.as_ref().map(|operation| operation.kind),
                        Some(OperationKind::Hibernate | OperationKind::Resume)
                    )
                })
                .cloned()
                .collect::<Vec<_>>(),
        };
        interrupted
            .into_iter()
            .filter_map(|instance| {
                let id = instance.id;
                self.mark_instance_recovery(instance)
                    .err()
                    .map(|error| ReconcileFailure {
                        instance_id: id,
                        error: format!("interrupted hibernation classification failed: {error}"),
                    })
            })
            .collect()
    }

    fn guest_client(&self, id: Uuid) -> Result<GuestClient> {
        let backend = self
            .backend_instances
            .lock()
            .map_err(|_| poisoned("backend_instances"))?
            .get(&id)
            .cloned()
            .ok_or_else(|| {
                BlazeDaemonError::Conflict(format!("instance {id} has no backend owner"))
            })?;
        let socket = backend.guest_socket_path();
        if socket.as_os_str().is_empty() {
            return Err(BlazeDaemonError::Conflict(format!(
                "instance {id} has no guest transport"
            )));
        }
        Ok(GuestClient::new(
            socket.to_path_buf(),
            GUEST_REQUEST_TIMEOUT,
            MAX_GUEST_FILE_BYTES,
        ))
    }

    pub(super) async fn wait_for_guest_ready(
        &self,
        backend: &DynBackendInstance,
        failpoint: &str,
    ) -> crate::guest::Result<()> {
        let socket = backend.guest_socket_path();
        if socket.as_os_str().is_empty() {
            return Ok(());
        }
        crate::failpoint::guest(failpoint)?;
        GuestClient::new(
            socket.to_path_buf(),
            GUEST_REQUEST_TIMEOUT,
            MAX_GUEST_FILE_BYTES,
        )
        .wait_ready(GUEST_REQUEST_TIMEOUT, &CancellationToken::new())
        .await
    }

    /// Release every instance that lifecycle cleanup still owns.
    ///
    /// Startup reconciliation has no external deadline, so each record gets the
    /// full per-sandbox operation lock without a timeout.
    pub async fn cleanup_owned_instances(&self) -> ReconcileReport {
        let ids = match self.owned_instance_ids() {
            Ok(ids) => ids,
            Err(error) => {
                return ReconcileReport {
                    attempted: 0,
                    completed: 0,
                    failures: vec![ReconcileFailure {
                        instance_id: Uuid::nil(),
                        error: format!("owned instance inventory unavailable: {error}"),
                    }],
                };
            }
        };
        let mut report = ReconcileReport {
            attempted: ids.len(),
            ..ReconcileReport::default()
        };
        for id in ids {
            let operation_lock = self.operation_lock(id);
            let _operation = operation_lock.lock().await;
            match self.destroy_locked(id).await {
                Ok(_) => report.completed += 1,
                Err(error) => {
                    let recovery = self.mark_recovery(id).err();
                    report.failures.push(ReconcileFailure {
                        instance_id: id,
                        error: match recovery {
                            Some(recovery) => {
                                format!("{error}; recovery state persistence failed: {recovery}")
                            }
                            None => error.to_string(),
                        },
                    });
                }
            }
        }
        report
    }

    async fn cleanup_failed_create(
        &self,
        instance: &mut SandboxInstance,
        binding: LeaseBinding,
        backend: Option<DynBackendInstance>,
        registered: bool,
        original: BlazeDaemonError,
    ) -> BlazeDaemonError {
        if instance.operation.is_none() {
            instance.begin_operation(OperationKind::Create);
        }
        let mut cleanup_errors = Vec::new();
        let backend = if registered {
            match self.backend_instances.lock() {
                Ok(mut instances) => instances.remove(&instance.id),
                Err(poisoned) => poisoned.into_inner().remove(&instance.id),
            }
        } else {
            backend
        };
        let mut backend_stopped = matches!(
            instance.backend_ownership,
            BackendOwnership::NotStarted | BackendOwnership::Stopped
        );
        if registered && backend.is_none() {
            backend_stopped = false;
            cleanup_errors.push("registered backend owner is missing".to_string());
        }
        if let Some(backend) = backend.as_ref() {
            match backend.kill().await {
                Ok(()) => {
                    backend_stopped = true;
                    instance.backend_ownership = BackendOwnership::Stopped;
                }
                Err(error) => {
                    backend_stopped = false;
                    cleanup_errors.push(format!("backend termination failed: {error}"));
                }
            }
        }

        let mut data_plane_released = false;
        if backend_stopped {
            match self.data_plane.abort(AbortRequest { binding }).await {
                Ok(aborted) => {
                    if validate_transition(binding, aborted.binding, LeaseState::Released).is_ok() {
                        data_plane_released = true;
                        if let Err(error) = self.remove_data_plane_lease(instance.id) {
                            cleanup_errors.push(format!(
                                "data-plane lease retention cleanup failed: {error}"
                            ));
                        }
                    } else {
                        cleanup_errors
                            .push("data-plane abort returned an invalid transition".to_string());
                    }
                }
                Err(error) => {
                    cleanup_errors.push(format!("data-plane abort failed: {error}"));
                }
            }
        } else {
            cleanup_errors.push(
                "data-plane resources retained until backend termination succeeds".to_string(),
            );
        }

        if backend_stopped && data_plane_released {
            cleanup_errors.extend(self.commit_create_rollback(instance));
            if cleanup_errors.is_empty() {
                self.metrics.inc(&self.metrics.instances_destroyed);
                return original;
            }
            return BlazeDaemonError::RecoveryRequired(format!(
                "{original}; cleanup completed but {}",
                cleanup_errors.join("; ")
            ));
        }

        if let Some(backend) = backend
            && let Some(error) = self.retain_backend(instance.id, backend)
        {
            cleanup_errors.push(error);
        }
        if instance.state != SandboxState::RecoveryRequired
            && let Err(error) = instance.transition(SandboxState::RecoveryRequired)
        {
            cleanup_errors.push(format!("recovery state update failed: {error}"));
        }
        if let Err(error) = self.state_store.persist(instance) {
            cleanup_errors.push(format!("state persistence failed: {error}"));
        }
        if let Some(error) = self.retain_instance(instance.clone()) {
            cleanup_errors.push(error);
        }
        BlazeDaemonError::RecoveryRequired(format!(
            "{original}; cleanup incomplete: {}",
            cleanup_errors.join("; ")
        ))
    }

    /// Commit a fully compensated create as terminal without losing the
    /// operation record when that terminal commit itself fails.
    fn commit_create_rollback(&self, instance: &mut SandboxInstance) -> Vec<String> {
        let recoverable = instance.clone();
        let mut terminal = recoverable.clone();
        terminal.backend_ownership = BackendOwnership::Stopped;
        let terminal_result = (|| -> Result<()> {
            if terminal.state != SandboxState::Destroyed {
                terminal.transition(SandboxState::Destroyed)?;
            }
            terminal.finish_operation();
            crate::failpoint::state("create-rollback-final-state-commit")?;
            self.state_store.persist(&terminal)
        })();

        match terminal_result {
            Ok(()) => {
                *instance = terminal.clone();
                self.retain_instance(terminal).into_iter().collect()
            }
            Err(error) => {
                let mut errors = vec![format!("final state persistence failed: {error}")];
                let mut recovery = recoverable;
                recovery.backend_ownership = BackendOwnership::Stopped;
                if recovery.state != SandboxState::RecoveryRequired
                    && let Err(error) = recovery.transition(SandboxState::RecoveryRequired)
                {
                    errors.push(format!("recovery state update failed: {error}"));
                }
                if let Err(error) = self.state_store.persist(&recovery) {
                    errors.push(format!("recovery state persistence failed: {error}"));
                }
                if let Some(error) = self.retain_instance(recovery.clone()) {
                    errors.push(error);
                }
                *instance = recovery;
                errors
            }
        }
    }

    pub(super) fn mark_recovery(&self, id: Uuid) -> Result<()> {
        self.mark_instance_recovery(self.get(id)?)
    }

    pub(super) fn persist_and_retain(&self, instance: SandboxInstance) -> Result<()> {
        self.state_store.persist(&instance)?;
        if let Some(error) = self.retain_instance(instance) {
            return Err(BlazeDaemonError::RecoveryRequired(error));
        }
        Ok(())
    }

    pub(super) fn mark_instance_recovery(&self, mut instance: SandboxInstance) -> Result<()> {
        if instance.state != SandboxState::RecoveryRequired {
            instance.transition(SandboxState::RecoveryRequired)?;
        }
        let persist = self.state_store.persist(&instance);
        let retained = self.retain_instance(instance);
        match (persist, retained) {
            (Ok(()), None) => Ok(()),
            (Err(error), None) => Err(error),
            (Ok(()), Some(error)) => Err(BlazeDaemonError::Internal(error)),
            (Err(persist), Some(retain)) => Err(BlazeDaemonError::RecoveryRequired(format!(
                "recovery state persistence failed: {persist}; {retain}"
            ))),
        }
    }

    pub(super) fn retain_backend(&self, id: Uuid, backend: DynBackendInstance) -> Option<String> {
        match self.backend_instances.lock() {
            Ok(mut instances) => {
                instances.insert(id, backend);
                None
            }
            Err(poisoned) => {
                poisoned.into_inner().insert(id, backend);
                Some("backend owner retained in poisoned runtime map".to_string())
            }
        }
    }

    pub(super) fn retain_instance(&self, instance: SandboxInstance) -> Option<String> {
        match self.instances.lock() {
            Ok(mut instances) => {
                instances.insert(instance.id, instance);
                None
            }
            Err(poisoned) => {
                poisoned.into_inner().insert(instance.id, instance);
                Some("instance state retained in poisoned lifecycle map".to_string())
            }
        }
    }
}

fn poisoned(name: &str) -> BlazeDaemonError {
    BlazeDaemonError::Internal(format!("{name} lock poisoned"))
}

fn is_clean_terminal(instance: &SandboxInstance) -> bool {
    instance.state == SandboxState::Destroyed
        && instance.operation.is_none()
        && matches!(
            instance.backend_ownership,
            BackendOwnership::NotStarted | BackendOwnership::Stopped
        )
}

fn requires_automatic_cleanup(instance: &SandboxInstance) -> bool {
    !(is_clean_terminal(instance)
        || (instance.state == SandboxState::Hibernated
            && instance.operation.is_none()
            && instance.backend_ownership == BackendOwnership::Stopped)
        || (instance.state == SandboxState::RecoveryRequired
            && matches!(
                instance.operation.as_ref().map(|operation| operation.kind),
                Some(OperationKind::Hibernate | OperationKind::Resume)
            )))
}

/// Require the command line captured in a Firecracker snapshot to equal the
/// command line the matched policy would use for a cold start.
///
/// Restore loads the captured machine configuration and does not call
/// `write_vm_config`, so accepting a mismatch would silently bypass current
/// policy controls.
fn validate_template_boot_args(
    template_name: &str,
    captured: Option<&str>,
    expected: &str,
    policy_name: &str,
) -> Result<()> {
    if captured == Some(expected) {
        return Ok(());
    }
    Err(BlazeDaemonError::Conflict(format!(
        "template {template_name} kernel boot arguments do not match policy {policy_name}"
    )))
}

fn provider_restore_attachments(
    binding: LeaseBinding,
    attachments: Vec<blaze_provider_api::OpenedAttachment>,
) -> crate::spawner::ProviderRestoreAttachments {
    use crate::spawner::{
        ProviderAttachmentAccess, ProviderAttachmentKind, ProviderAttachmentRole,
        ProviderAttachmentSharing, ProviderRestoreAttachment, ProviderRestoreAttachments,
    };
    use blaze_provider_api::{AttachmentAccess, AttachmentKind, AttachmentRole, AttachmentSharing};

    let attachments = attachments
        .into_iter()
        .map(|attachment| ProviderRestoreAttachment {
            role: match attachment.role {
                AttachmentRole::RootDrive => ProviderAttachmentRole::RootDrive,
                AttachmentRole::GuestMemory => ProviderAttachmentRole::GuestMemory,
            },
            file: Arc::new(std::fs::File::from(attachment.descriptor)),
            access: match attachment.access {
                AttachmentAccess::ReadOnly => ProviderAttachmentAccess::ReadOnly,
                AttachmentAccess::ReadWrite => ProviderAttachmentAccess::ReadWrite,
            },
            sharing: match attachment.sharing {
                AttachmentSharing::Exclusive => ProviderAttachmentSharing::Exclusive,
                AttachmentSharing::SharedReadOnly => ProviderAttachmentSharing::SharedReadOnly,
            },
            kind: match attachment.kind {
                AttachmentKind::RegularFile => ProviderAttachmentKind::RegularFile,
                AttachmentKind::CharacterDevice => ProviderAttachmentKind::CharacterDevice,
                AttachmentKind::BlockDevice => ProviderAttachmentKind::BlockDevice,
            },
            logical_size_bytes: attachment.logical_size_bytes,
            consumer_path: attachment.consumer_path,
        })
        .collect();
    ProviderRestoreAttachments {
        instance_id: binding.context.instance_id,
        lease_id: binding.context.lease_id,
        generation: binding.generation,
        attachments,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn firecracker_template_boot_arguments_must_match_the_policy_exactly() {
        validate_template_boot_args(
            "runtime-base",
            Some("console=ttyS0 panic=1"),
            "console=ttyS0 panic=1",
            "agent-tool",
        )
        .expect("identical command lines");

        for captured in [None, Some("console=ttyS0 panic=2")] {
            let error = validate_template_boot_args(
                "runtime-base",
                captured,
                "console=ttyS0 panic=1",
                "agent-tool",
            )
            .expect_err("missing or different command lines must be rejected");
            assert!(matches!(error, BlazeDaemonError::Conflict(_)));
        }
    }
}
