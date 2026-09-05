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
    AbortRequest, BeginInventoryRequest, CapacityRequest, CapacityScope, CapacitySnapshot,
    CommitRequest, DataPlaneProvider, DrainRequest, DrainResult, FinalizeRequest, InspectRequest,
    InventoryPageRequest, LeaseBinding, LeaseState, PrepareRequest, PrepareSource, PreparedLease,
    PreparedResources, ProviderCheckpointRef, ProviderError, PublicTransitionRef, ReconcileAction,
    ReconcileRequest, ReleaseRequest, RequestContext, StopRequest, TemplateSource,
};
use blaze_provider_conformance::{
    validate_capacity_snapshot, validate_descriptor, validate_drain_result,
    validate_inventory_lease, validate_inventory_snapshot, validate_prepared,
    validate_prepared_binding, validate_reconcile_result, validate_transition,
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
    SpawnerRegistry, adopt_with_runtime_directory, restore_with_runtime_directory,
    spawn_with_runtime_directory,
};
use crate::state_store::{OwnedRunDir, StateStore};

pub(super) const GUEST_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

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
    /// Number safely adopted or moved to the terminal state.
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
    pub(super) data_plane: Arc<dyn DataPlaneProvider>,
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
        let provider_instance_id = data_plane.descriptor().provider_instance_id;
        let data_plane_leases = instances
            .values()
            .filter_map(|instance| {
                instance
                    .data_plane_lease
                    .filter(|record| record.provider_instance_id == provider_instance_id)
                    .map(|record| (instance.id, LeaseBinding::from_record(instance.id, record)))
            })
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
                data_plane_leases: Mutex::new(data_plane_leases),
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
                        Ok(_) => {
                            let retained = self
                                .retain_data_plane_lease(context.instance_id, prepared.binding)
                                .err();
                            Err(BlazeDaemonError::RecoveryRequired(format!(
                                "{violation}; provider compensation returned an invalid transition{}",
                                retained
                                    .map(|error| format!("; lease retention also failed: {error}"))
                                    .unwrap_or_default()
                            )))
                        }
                        Err(error) => {
                            let retained = self
                                .retain_data_plane_lease(context.instance_id, prepared.binding)
                                .err();
                            Err(BlazeDaemonError::RecoveryRequired(format!(
                                "{violation}; provider compensation failed: {error}{}",
                                retained
                                    .map(|error| format!("; lease retention also failed: {error}"))
                                    .unwrap_or_default()
                            )))
                        }
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
                let aborted = match self
                    .data_plane
                    .abort(AbortRequest {
                        binding: observed.binding,
                    })
                    .await
                {
                    Ok(aborted) => aborted,
                    Err(error) => {
                        let retained = self
                            .retain_data_plane_lease(context.instance_id, observed.binding)
                            .err();
                        return Err(BlazeDaemonError::RecoveryRequired(format!(
                            "data-plane preparation was observed but compensation failed: {error}{}",
                            retained
                                .map(|error| format!("; lease retention also failed: {error}"))
                                .unwrap_or_default()
                        )));
                    }
                };
                if validate_transition(observed.binding, aborted.binding, LeaseState::Released)
                    .is_err()
                {
                    let retained = self
                        .retain_data_plane_lease(context.instance_id, observed.binding)
                        .err();
                    return Err(BlazeDaemonError::RecoveryRequired(format!(
                        "data-plane preparation compensation returned an invalid transition{}",
                        retained
                            .map(|error| format!("; lease retention also failed: {error}"))
                            .unwrap_or_default()
                    )));
                }
                Err(BlazeDaemonError::DataPlane(ProviderError::OutcomeUnknown))
            }
            Err(error) => Err(error.into()),
        }
    }

    pub(super) async fn commit_data_plane(&self, binding: LeaseBinding) -> Result<LeaseBinding> {
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

    pub(super) fn persist_data_plane_binding(
        &self,
        instance: &mut SandboxInstance,
        binding: LeaseBinding,
        extents: Option<(u64, u64)>,
    ) -> Result<()> {
        let (root_filesystem_bytes, guest_memory_bytes) = match extents {
            Some(extents) => extents,
            None => {
                let record = instance.data_plane_lease.ok_or_else(|| {
                    BlazeDaemonError::RecoveryRequired(format!(
                        "sandbox {} has no durable data-plane lease to advance",
                        instance.id
                    ))
                })?;
                (record.root_filesystem_bytes, record.guest_memory_bytes)
            }
        };
        if let Some(previous) = instance.data_plane_lease
            && (previous.provider_instance_id != binding.provider_instance_id
                || previous.lease_id != binding.context.lease_id
                || previous.request_id != binding.context.request_id
                || previous.operation_id != binding.context.operation_id
                || previous.initial_generation != binding.context.generation
                || binding.generation < previous.generation)
        {
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "sandbox {} data-plane lease identity or generation changed unexpectedly",
                instance.id
            )));
        }
        instance.data_plane_lease =
            Some(binding.to_record(root_filesystem_bytes, guest_memory_bytes));
        self.state_store.persist(instance)?;
        if let Some(error) = self.retain_instance(instance.clone()) {
            return Err(BlazeDaemonError::RecoveryRequired(error));
        }
        self.retain_data_plane_lease(instance.id, binding)
    }

    pub(super) fn persist_replacement_data_plane_binding(
        &self,
        instance: &mut SandboxInstance,
        binding: LeaseBinding,
        extents: Option<(u64, u64)>,
    ) -> Result<()> {
        let (root_filesystem_bytes, guest_memory_bytes) = match extents {
            Some(extents) => extents,
            None => {
                let record = instance.replacement_data_plane_lease.ok_or_else(|| {
                    BlazeDaemonError::RecoveryRequired(format!(
                        "sandbox {} has no durable replacement lease to advance",
                        instance.id
                    ))
                })?;
                (record.root_filesystem_bytes, record.guest_memory_bytes)
            }
        };
        if let Some(previous) = instance.replacement_data_plane_lease
            && (previous.provider_instance_id != binding.provider_instance_id
                || previous.lease_id != binding.context.lease_id
                || previous.request_id != binding.context.request_id
                || previous.operation_id != binding.context.operation_id
                || previous.initial_generation != binding.context.generation
                || binding.generation < previous.generation)
        {
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "sandbox {} replacement lease identity or generation changed unexpectedly",
                instance.id
            )));
        }
        instance.replacement_data_plane_lease =
            Some(binding.to_record(root_filesystem_bytes, guest_memory_bytes));
        self.persist_and_retain(instance.clone())
    }

    pub(super) fn retain_data_plane_lease(&self, id: Uuid, binding: LeaseBinding) -> Result<()> {
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

    pub(super) fn remove_data_plane_lease(&self, id: Uuid) -> Result<()> {
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

    /// Return one validated reusable-resource capacity partition.
    pub async fn provider_capacity(&self, scope: CapacityScope) -> Result<CapacitySnapshot> {
        if scope.class_digest == [0; 32] {
            return Err(BlazeDaemonError::BadRequest(
                "capacity class digest must not be zero".to_string(),
            ));
        }
        let extension = self.data_plane.capacity_control().ok_or_else(|| {
            BlazeDaemonError::UnsupportedOperation(
                "data-plane capacity management is not implemented".to_string(),
            )
        })?;
        let descriptor = self.data_plane.descriptor();
        validate_descriptor(descriptor).map_err(|_| {
            BlazeDaemonError::Internal("data-plane descriptor is incompatible".to_string())
        })?;
        let request = CapacityRequest { scope };
        let snapshot = extension.capacity(request).await?;
        validate_capacity_snapshot(descriptor, request, snapshot)
            .map_err(|_| BlazeDaemonError::DataPlane(ProviderError::InvalidResponse))?;
        Ok(snapshot)
    }

    /// Drain one exact capacity partition using an idempotent operation identity.
    pub async fn drain_provider_capacity(&self, request: DrainRequest) -> Result<DrainResult> {
        if request.scope.class_digest == [0; 32] || request.operation_id.is_nil() {
            return Err(BlazeDaemonError::BadRequest(
                "capacity drain requires nonzero class and operation identities".to_string(),
            ));
        }
        let extension = self.data_plane.capacity_control().ok_or_else(|| {
            BlazeDaemonError::UnsupportedOperation(
                "data-plane capacity management is not implemented".to_string(),
            )
        })?;
        let descriptor = self.data_plane.descriptor();
        validate_descriptor(descriptor).map_err(|_| {
            BlazeDaemonError::Internal("data-plane descriptor is incompatible".to_string())
        })?;
        let first = extension.drain(request).await;
        let result = match first {
            Err(ProviderError::OutcomeUnknown) => extension.drain(request).await?,
            result => result?,
        };
        validate_drain_result(descriptor, request, result)
            .map_err(|_| BlazeDaemonError::DataPlane(ProviderError::InvalidResponse))?;
        Ok(result)
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
        let lease_extents = (
            prepare_request.root_filesystem_bytes,
            prepare_request.guest_memory_bytes,
        );
        let prepared = match self.prepare_data_plane(prepare_request).await {
            Ok(prepared) => prepared,
            Err(error) => {
                match self.data_plane_lease(instance.id) {
                    Ok(Some(binding)) => {
                        instance.data_plane_lease =
                            Some(binding.to_record(lease_extents.0, lease_extents.1));
                        let recovery = self.mark_instance_recovery(instance).err();
                        return Err(BlazeDaemonError::RecoveryRequired(format!(
                            "{error}; prepared provider ownership was retained{}",
                            recovery
                                .map(|error| format!(
                                    "; recovery state persistence failed: {error}"
                                ))
                                .unwrap_or_default()
                        )));
                    }
                    Ok(None) => {}
                    Err(retention_error) => {
                        let recovery = self.mark_instance_recovery(instance).err();
                        return Err(BlazeDaemonError::RecoveryRequired(format!(
                            "{error}; provider lease retention is unreadable: {retention_error}{}",
                            recovery
                                .map(|error| format!(
                                    "; recovery state persistence failed: {error}"
                                ))
                                .unwrap_or_default()
                        )));
                    }
                }
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
        if let Err(error) =
            self.persist_data_plane_binding(&mut instance, binding, Some(lease_extents))
        {
            return Err(self
                .cleanup_failed_create(&mut instance, binding, None, false, error)
                .await);
        }
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
            (
                PreparedResources::CheckpointRestore { .. }
                | PreparedResources::SuspensionRestore { .. },
                _,
            ) => {
                return Err(self
                    .cleanup_failed_create(
                        &mut instance,
                        binding,
                        None,
                        false,
                        BlazeDaemonError::Internal(
                            "data-plane provider returned lifecycle restore resources for creation"
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
        let (actual_backend, backend_runtime) = match spawn {
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
                let backend_runtime = backend_instance.runtime_record();
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
                (actual_backend, backend_runtime)
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

        instance.backend_runtime = Some(backend_runtime);
        if let Err(error) = self.persist_data_plane_binding(&mut instance, lease_binding, None) {
            return Err(self
                .cleanup_failed_create(&mut instance, lease_binding, None, true, error)
                .await);
        }

        lease_binding = match self.commit_data_plane(lease_binding).await {
            Ok(binding) => binding,
            Err(error) => {
                return Err(self
                    .cleanup_failed_create(&mut instance, lease_binding, None, true, error)
                    .await);
            }
        };
        if let Err(error) = self.persist_data_plane_binding(&mut instance, lease_binding, None) {
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
        if let Err(error) = self.persist_data_plane_binding(&mut instance, lease_binding, None) {
            let recovery = self.mark_instance_recovery(instance.clone()).err();
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "create {}: data-plane final ownership is durable but its public ledger update failed: {error}{}",
                instance.id,
                recovery
                    .map(|error| format!("; recovery state persistence failed: {error}"))
                    .unwrap_or_default()
            )));
        }
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
        let replacement_binding = original
            .replacement_data_plane_lease
            .map(|record| LeaseBinding::from_record(id, record));
        let mut data_plane_released = original.provider_suspension.is_some()
            && original.data_plane_lease.is_none()
            && original.replacement_data_plane_lease.is_none();

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
        original.backend_ownership = BackendOwnership::Stopped;

        if let Some(mut binding) = replacement_binding {
            match binding.state {
                LeaseState::Prepared | LeaseState::Committed => {
                    let aborted = self
                        .data_plane
                        .abort(AbortRequest { binding })
                        .await
                        .map_err(|error| {
                            BlazeDaemonError::RecoveryRequired(format!(
                                "destroy {id}: replacement lease abort failed: {error}"
                            ))
                        })?;
                    validate_transition(binding, aborted.binding, LeaseState::Released).map_err(
                        |_| {
                            BlazeDaemonError::RecoveryRequired(format!(
                                "destroy {id}: replacement lease abort returned an invalid transition"
                            ))
                        },
                    )?;
                }
                LeaseState::Finalized => {
                    let stopped = self
                        .data_plane
                        .stop(StopRequest { binding })
                        .await
                        .map_err(|error| {
                            BlazeDaemonError::RecoveryRequired(format!(
                                "destroy {id}: replacement lease stop failed: {error}"
                            ))
                        })?;
                    validate_transition(binding, stopped.binding, LeaseState::Stopped).map_err(
                        |_| {
                            BlazeDaemonError::RecoveryRequired(format!(
                                "destroy {id}: replacement lease stop returned an invalid transition"
                            ))
                        },
                    )?;
                    binding = stopped.binding;
                    let released = self
                        .data_plane
                        .release(ReleaseRequest { binding })
                        .await
                        .map_err(|error| {
                            BlazeDaemonError::RecoveryRequired(format!(
                                "destroy {id}: replacement lease release failed: {error}"
                            ))
                        })?;
                    validate_transition(binding, released.binding, LeaseState::Released).map_err(
                        |_| {
                            BlazeDaemonError::RecoveryRequired(format!(
                                "destroy {id}: replacement lease release returned an invalid transition"
                            ))
                        },
                    )?;
                }
                LeaseState::Stopped => {
                    let released = self
                        .data_plane
                        .release(ReleaseRequest { binding })
                        .await
                        .map_err(|error| {
                            BlazeDaemonError::RecoveryRequired(format!(
                                "destroy {id}: replacement lease release failed: {error}"
                            ))
                        })?;
                    validate_transition(binding, released.binding, LeaseState::Released).map_err(
                        |_| {
                            BlazeDaemonError::RecoveryRequired(format!(
                                "destroy {id}: replacement lease release returned an invalid transition"
                            ))
                        },
                    )?;
                }
                LeaseState::Released => {}
                LeaseState::Quarantined => {
                    return Err(BlazeDaemonError::RecoveryRequired(format!(
                        "destroy {id}: quarantined replacement resources require operator resolution"
                    )));
                }
            }
            original.replacement_data_plane_lease = None;
            self.persist_and_retain(original.clone())?;
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
                    self.persist_data_plane_binding(&mut original, stopped.binding, None)?;
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
                    self.persist_data_plane_binding(&mut original, aborted.binding, None)?;
                }
                LeaseState::Stopped => {}
                LeaseState::Released => data_plane_released = true,
                LeaseState::Quarantined => {
                    return Err(BlazeDaemonError::RecoveryRequired(format!(
                        "destroy {id}: quarantined data-plane resources require operator resolution"
                    )));
                }
            }
        }

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

        let metadata_store = self.checkpoints.clone();
        let checkpoint_metadata =
            crate::failpoint::spawn_blocking(move || metadata_store.list_metadata(id))
                .await
                .map_err(|error| {
                    BlazeDaemonError::RecoveryRequired(format!(
                        "destroy {id}: checkpoint inventory task failed: {error}"
                    ))
                })?
                .map_err(|error| {
                    BlazeDaemonError::RecoveryRequired(format!(
                        "destroy {id}: checkpoint inventory failed: {error}"
                    ))
                })?;
        for record in checkpoint_metadata
            .into_iter()
            .filter_map(|metadata| metadata.provider_checkpoint)
        {
            if !original
                .pending_provider_retirements
                .iter()
                .any(|pending| pending.reference_id == record.reference_id)
            {
                original.pending_provider_retirements.push(record);
            }
        }
        if !original.pending_provider_retirements.is_empty() {
            if self.data_plane.checkpoints().is_none() {
                let recovery = self.mark_instance_recovery(original).err();
                return Err(BlazeDaemonError::RecoveryRequired(format!(
                    "destroy {id}: provider checkpoint retirement is unavailable{}",
                    recovery
                        .map(|error| format!("; recovery state persistence failed: {error}"))
                        .unwrap_or_default()
                )));
            }
            self.persist_and_retain(original.clone())?;
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

        for record in original.pending_provider_retirements.clone() {
            let checkpoint = ProviderCheckpointRef::from_record(&record);
            if let Err(error) = self.retire_provider_checkpoint(&checkpoint).await {
                let recovery = self.mark_instance_recovery(original).err();
                return Err(BlazeDaemonError::RecoveryRequired(format!(
                    "destroy {id}: provider checkpoint retirement failed: {error}{}",
                    recovery
                        .map(|error| format!("; recovery state persistence failed: {error}"))
                        .unwrap_or_default()
                )));
            }
            original
                .pending_provider_retirements
                .retain(|pending| pending.reference_id != record.reference_id);
            self.persist_and_retain(original.clone())?;
        }

        if let Some(record) = original.provider_suspension.clone()
            && !original
                .pending_provider_suspension_retirements
                .iter()
                .any(|pending| pending.reference_id == record.reference_id)
        {
            original
                .pending_provider_suspension_retirements
                .push(record);
            self.persist_and_retain(original.clone())?;
        }
        if (original.pending_provider_suspension_id.is_some()
            || !original.pending_provider_suspension_retirements.is_empty())
            && self.data_plane.suspension().is_none()
        {
            let recovery = self.mark_instance_recovery(original).err();
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "destroy {id}: provider suspension retirement is unavailable{}",
                recovery
                    .map(|error| format!("; recovery state persistence failed: {error}"))
                    .unwrap_or_default()
            )));
        }
        if let Some(suspension_id) = original.pending_provider_suspension_id {
            let has_exact_reference = original
                .pending_provider_suspension_retirements
                .iter()
                .any(|record| record.suspension_id == suspension_id);
            if !has_exact_reference {
                self.retire_provider_suspension_identity(
                    self.data_plane.descriptor().provider_instance_id,
                    suspension_id,
                    None,
                )
                .await
                .map_err(|error| {
                    BlazeDaemonError::RecoveryRequired(format!(
                        "destroy {id}: unknown provider suspension retirement failed: {error}"
                    ))
                })?;
            }
            original.pending_provider_suspension_id = None;
            self.persist_and_retain(original.clone())?;
        }
        for record in original.pending_provider_suspension_retirements.clone() {
            let suspension = blaze_provider_api::ProviderSuspensionRef::from_record(&record);
            if let Err(error) = self.retire_provider_suspension(&suspension).await {
                let recovery = self.mark_instance_recovery(original).err();
                return Err(BlazeDaemonError::RecoveryRequired(format!(
                    "destroy {id}: provider suspension retirement failed: {error}{}",
                    recovery
                        .map(|error| format!("; recovery state persistence failed: {error}"))
                        .unwrap_or_default()
                )));
            }
            original
                .pending_provider_suspension_retirements
                .retain(|pending| pending.reference_id != record.reference_id);
            if original
                .provider_suspension
                .as_ref()
                .is_some_and(|active| active.reference_id == record.reference_id)
            {
                original.provider_suspension = None;
            }
            self.persist_and_retain(original.clone())?;
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
                self.persist_data_plane_binding(&mut original, released.binding, None)?;
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
                    "destroy {id}: no compatible data-plane lease is available and this provider \
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
        destroyed.data_plane_lease = None;
        destroyed.replacement_data_plane_lease = None;
        destroyed.provider_suspension = None;
        destroyed.pending_provider_suspension_id = None;
        destroyed.pending_provider_suspension_retirements.clear();
        destroyed.backend_runtime = None;
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

    /// Reconcile every non-terminal record.
    ///
    /// A provider inventory failure aborts daemon startup before the API is
    /// exposed. Per-sandbox conflicts remain visible in the returned report and
    /// are retained in recovery or quarantine state.
    pub async fn reconcile_startup(&self) -> Result<ReconcileReport> {
        if self.data_plane.inventory().is_some() {
            return self.reconcile_provider_startup().await;
        }
        let mut classification_failures = self.classify_interrupted_hibernation();
        let mut report = self.cleanup_owned_instances().await;
        report.failures.append(&mut classification_failures);
        Ok(report)
    }

    async fn reconcile_provider_startup(&self) -> Result<ReconcileReport> {
        const INVENTORY_PAGE_SIZE: u32 = 256;
        const MAX_INVENTORY_LEASES: usize = 1_000_000;

        let inventory = self
            .data_plane
            .inventory()
            .expect("inventory extension was checked");
        let descriptor = self.data_plane.descriptor();
        let snapshot = inventory
            .begin_inventory(BeginInventoryRequest {
                page_size: INVENTORY_PAGE_SIZE,
            })
            .await?;
        if validate_inventory_snapshot(descriptor, snapshot).is_err() {
            return Err(BlazeDaemonError::RecoveryRequired(
                "data-plane inventory returned an invalid snapshot identity".to_string(),
            ));
        }

        let mut observed_by_lease = HashMap::new();
        let mut seen_cursors = HashSet::new();
        let mut cursor: Option<String> = None;
        loop {
            if let Some(value) = cursor.as_ref()
                && !seen_cursors.insert(value.clone())
            {
                return Err(BlazeDaemonError::RecoveryRequired(
                    "data-plane inventory repeated a page cursor".to_string(),
                ));
            }
            let page = inventory
                .inventory_page(InventoryPageRequest {
                    snapshot_id: snapshot.snapshot_id,
                    cursor: cursor.clone(),
                    page_size: INVENTORY_PAGE_SIZE,
                })
                .await?;
            if page.leases.len() > INVENTORY_PAGE_SIZE as usize {
                return Err(BlazeDaemonError::RecoveryRequired(
                    "data-plane inventory exceeded the requested page size".to_string(),
                ));
            }
            for lease in page.leases {
                let binding = lease.binding;
                if validate_inventory_lease(descriptor, binding).is_err() {
                    return Err(BlazeDaemonError::RecoveryRequired(
                        "data-plane inventory contains an invalid lease identity".to_string(),
                    ));
                }
                if observed_by_lease
                    .insert(binding.context.lease_id, binding)
                    .is_some()
                {
                    return Err(BlazeDaemonError::RecoveryRequired(
                        "data-plane inventory contains a duplicate lease".to_string(),
                    ));
                }
                if observed_by_lease.len() > MAX_INVENTORY_LEASES {
                    return Err(BlazeDaemonError::RecoveryRequired(
                        "data-plane inventory exceeds the safety bound".to_string(),
                    ));
                }
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }

        let persisted = self.list()?;
        let mut report = ReconcileReport {
            attempted: persisted
                .iter()
                .filter(|instance| requires_automatic_cleanup(instance))
                .count(),
            ..ReconcileReport::default()
        };
        for mut instance in persisted {
            if !requires_automatic_cleanup(&instance) {
                continue;
            }
            let id = instance.id;
            let operation_lock = self.operation_lock(id);
            let _operation = operation_lock.lock().await;
            let expected = instance
                .data_plane_lease
                .map(|record| LeaseBinding::from_record(id, record));
            let observed =
                expected.and_then(|binding| observed_by_lease.remove(&binding.context.lease_id));
            let adoptable = instance.state == SandboxState::Running
                && instance.operation.is_none()
                && instance.backend_ownership == BackendOwnership::Running
                && instance.backend_runtime.is_some()
                && instance.replacement_data_plane_lease.is_none()
                && instance.pending_provider_retirements.is_empty()
                && expected.is_some()
                && expected == observed
                && expected.is_some_and(|binding| {
                    matches!(binding.state, LeaseState::Committed | LeaseState::Finalized)
                });

            if adoptable {
                match self
                    .adopt_running_instance(&mut instance, observed.expect("checked"), inventory)
                    .await
                {
                    Ok(true) => {
                        report.completed += 1;
                        continue;
                    }
                    Ok(false) => {}
                    Err(error) => {
                        report.failures.push(ReconcileFailure {
                            instance_id: id,
                            error: error.to_string(),
                        });
                    }
                }
            }

            if let Some(observed) = observed {
                let quarantine = inventory
                    .reconcile(ReconcileRequest {
                        expected,
                        observed,
                        action: ReconcileAction::Quarantine,
                    })
                    .await;
                match quarantine {
                    Ok(result)
                        if validate_reconcile_result(
                            observed,
                            result.binding,
                            ReconcileAction::Quarantine,
                        )
                        .is_ok() =>
                    {
                        if let Some(record) = instance.data_plane_lease {
                            instance.data_plane_lease = Some(result.binding.to_record(
                                record.root_filesystem_bytes,
                                record.guest_memory_bytes,
                            ));
                        }
                    }
                    Ok(_) => report.failures.push(ReconcileFailure {
                        instance_id: id,
                        error: "provider quarantine returned an invalid transition".to_string(),
                    }),
                    Err(error) => report.failures.push(ReconcileFailure {
                        instance_id: id,
                        error: format!("provider quarantine failed: {error}"),
                    }),
                }
            }
            if let Some(spawner) = self.spawner(instance.backend)
                && !matches!(
                    instance.backend_ownership,
                    BackendOwnership::NotStarted | BackendOwnership::Stopped
                )
                && let Ok(run_dir) = self.state_store.run_dir(id)
            {
                if let Err(error) = spawner.cleanup_orphan(id, &run_dir).await {
                    report.failures.push(ReconcileFailure {
                        instance_id: id,
                        error: format!("backend quarantine failed: {error}"),
                    });
                } else {
                    instance.backend_ownership = BackendOwnership::Stopped;
                }
            }
            if let Err(error) = self.mark_instance_recovery(instance) {
                report.failures.push(ReconcileFailure {
                    instance_id: id,
                    error: format!("recovery state persistence failed: {error}"),
                });
            } else if !report
                .failures
                .iter()
                .any(|failure| failure.instance_id == id)
            {
                report.failures.push(ReconcileFailure {
                    instance_id: id,
                    error: "provider, public state, and backend identity did not agree".to_string(),
                });
            }
        }

        for observed in observed_by_lease.into_values() {
            match inventory
                .reconcile(ReconcileRequest {
                    expected: None,
                    observed,
                    action: ReconcileAction::Quarantine,
                })
                .await
            {
                Ok(result)
                    if validate_reconcile_result(
                        observed,
                        result.binding,
                        ReconcileAction::Quarantine,
                    )
                    .is_ok() =>
                {
                    report.failures.push(ReconcileFailure {
                        instance_id: observed.context.instance_id,
                        error: "provider lease has no public owner and was quarantined".to_string(),
                    });
                }
                Ok(_) => report.failures.push(ReconcileFailure {
                    instance_id: observed.context.instance_id,
                    error: "orphan quarantine returned an invalid transition".to_string(),
                }),
                Err(error) => report.failures.push(ReconcileFailure {
                    instance_id: observed.context.instance_id,
                    error: format!("orphan quarantine failed: {error}"),
                }),
            }
        }
        Ok(report)
    }

    async fn adopt_running_instance(
        &self,
        instance: &mut SandboxInstance,
        observed: LeaseBinding,
        inventory: &dyn blaze_provider_api::DataPlaneInventory,
    ) -> Result<bool> {
        let runtime = instance.backend_runtime.as_ref().ok_or_else(|| {
            BlazeDaemonError::RecoveryRequired(format!(
                "sandbox {} has no durable backend identity",
                instance.id
            ))
        })?;
        let process = runtime.process.ok_or_else(|| {
            BlazeDaemonError::RecoveryRequired(format!(
                "sandbox {} has no adoptable backend process",
                instance.id
            ))
        })?;
        let spawner = self.spawner(instance.backend).ok_or_else(|| {
            BlazeDaemonError::RecoveryRequired(format!(
                "sandbox {} has no registered recovery backend",
                instance.id
            ))
        })?;
        let record = instance.data_plane_lease.ok_or_else(|| {
            BlazeDaemonError::RecoveryRequired(format!(
                "sandbox {} has no durable data-plane lease",
                instance.id
            ))
        })?;
        let run_dir = self.state_store.run_dir(instance.id)?;
        let Some(owner) = adopt_with_runtime_directory(
            spawner.as_ref(),
            instance.id,
            runtime,
            run_dir,
            record.guest_memory_bytes,
        )
        .await?
        else {
            return Ok(false);
        };
        if let Err(error) = self
            .wait_for_guest_ready(&owner, "startup-adopt-guest-ready")
            .await
        {
            let cleanup = owner.kill().await.err();
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "sandbox {} adopted backend failed readiness: {error}{}",
                instance.id,
                cleanup
                    .map(|error| format!("; backend cleanup failed: {error}"))
                    .unwrap_or_default()
            )));
        }
        let reconciled = match inventory
            .reconcile(ReconcileRequest {
                expected: Some(observed),
                observed,
                action: ReconcileAction::Adopt {
                    backend_process: process,
                },
            })
            .await
        {
            Ok(reconciled) => reconciled,
            Err(error) => {
                let cleanup = owner.kill().await.err();
                return Err(BlazeDaemonError::RecoveryRequired(format!(
                    "sandbox {} provider adoption failed: {error}{}",
                    instance.id,
                    cleanup
                        .map(|error| format!("; backend cleanup failed: {error}"))
                        .unwrap_or_default()
                )));
            }
        };
        if validate_reconcile_result(
            observed,
            reconciled.binding,
            ReconcileAction::Adopt {
                backend_process: process,
            },
        )
        .is_err()
        {
            let cleanup = owner.kill().await.err();
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "sandbox {} provider adoption returned an invalid transition{}",
                instance.id,
                cleanup
                    .map(|error| format!("; backend cleanup failed: {error}"))
                    .unwrap_or_default()
            )));
        }
        instance.backend_runtime = Some(owner.runtime_record());
        if let Err(error) = self.persist_data_plane_binding(instance, reconciled.binding, None) {
            let quarantine = inventory
                .reconcile(ReconcileRequest {
                    expected: Some(reconciled.binding),
                    observed: reconciled.binding,
                    action: ReconcileAction::Quarantine,
                })
                .await;
            let quarantine_error = match quarantine {
                Ok(result)
                    if validate_reconcile_result(
                        reconciled.binding,
                        result.binding,
                        ReconcileAction::Quarantine,
                    )
                    .is_ok() =>
                {
                    if let Some(record) = instance.data_plane_lease {
                        instance.data_plane_lease = Some(
                            result
                                .binding
                                .to_record(record.root_filesystem_bytes, record.guest_memory_bytes),
                        );
                    }
                    self.state_store
                        .persist(instance)
                        .err()
                        .map(|error| format!("; quarantined state persistence failed: {error}"))
                }
                Ok(_) => Some("; provider returned an invalid quarantine transition".to_string()),
                Err(error) => Some(format!("; provider quarantine failed: {error}")),
            };
            let cleanup = owner.kill().await.err();
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "sandbox {} adopted ownership could not be persisted: {error}{}{}",
                instance.id,
                quarantine_error.unwrap_or_default(),
                cleanup
                    .map(|error| format!("; backend cleanup failed: {error}"))
                    .unwrap_or_default()
            )));
        }
        self.backend_instances
            .lock()
            .map_err(|_| poisoned("backend_instances"))?
            .insert(instance.id, owner);
        Ok(true)
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
                        if let Some(record) = instance.data_plane_lease {
                            instance.data_plane_lease = Some(aborted.binding.to_record(
                                record.root_filesystem_bytes,
                                record.guest_memory_bytes,
                            ));
                        }
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
        terminal.backend_runtime = None;
        terminal.data_plane_lease = None;
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
        && instance.data_plane_lease.is_none()
        && instance.replacement_data_plane_lease.is_none()
        && instance.pending_provider_retirements.is_empty()
        && instance.provider_suspension.is_none()
        && instance.pending_provider_suspension_id.is_none()
        && instance.pending_provider_suspension_retirements.is_empty()
        && instance.backend_runtime.is_none()
        && matches!(
            instance.backend_ownership,
            BackendOwnership::NotStarted | BackendOwnership::Stopped
        )
}

fn requires_automatic_cleanup(instance: &SandboxInstance) -> bool {
    !(is_clean_terminal(instance)
        || (instance.state == SandboxState::Hibernated
            && instance.operation.is_none()
            && instance.backend_ownership == BackendOwnership::Stopped
            && instance.replacement_data_plane_lease.is_none()
            && instance.pending_provider_suspension_id.is_none()
            && (instance.provider_suspension.is_none() || instance.data_plane_lease.is_none()))
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

pub(super) fn provider_restore_attachments(
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
