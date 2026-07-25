// SPDX-License-Identifier: Apache-2.0
//! Core sandbox create, guest I/O, destroy, and runtime ownership.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use blaze_core::backend::{BackendKind, NetworkConfig, SpawnRequest};
use blaze_core::config::DaemonConfig;
use blaze_core::lifecycle::{OperationKind, SandboxInstance, SandboxState, StartPath};
use blaze_core::policy::{BackendConfigs, RuntimeDecision, VmConfig, parse_duration};
use blaze_core::storage::{AcquireOpts, PoolStatus, StorageProvider, StorageSlot};
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::error::{BlazeDaemonError, Result};
use crate::guest::{GuestClient, GuestExecResult};
use crate::runtime_pool::{PoolPrototype, RuntimePoolSlot, RuntimeWarmPool};
use crate::spawner::{DynBackendInstance, DynSpawner};

/// Inputs already parsed and policy-evaluated by the HTTP layer.
#[derive(Debug, Clone)]
pub struct CreateSandbox {
    /// Optional idempotency key. Only UUIDs are accepted by the API.
    pub requested_id: Option<Uuid>,
    /// Policy decision for the request.
    pub decision: RuntimeDecision,
    /// Image identity.
    pub image_digest: String,
    /// Optional user-visible template.
    pub template_name: String,
    /// Backend executable selected at daemon boot.
    pub binary_path: PathBuf,
}

/// Manager result used by both canonical and compatibility create routes.
#[derive(Debug, Clone)]
pub struct CreateSandboxResult {
    /// Persisted metadata.
    pub instance: SandboxInstance,
    /// Concrete backend that owns the runtime.
    pub selected_backend: BackendKind,
    /// Whether an existing idempotent result was returned.
    pub existing: bool,
}

pub(super) struct SandboxRuntime {
    pub(super) storage: StorageSlot,
    pub(super) backend: Option<DynBackendInstance>,
    pub(super) guest: Option<GuestClient>,
}

/// Owns persistent metadata separately from non-serializable runtime handles.
pub struct SandboxManager {
    pub(super) config: DaemonConfig,
    pub(super) instances: Mutex<HashMap<Uuid, SandboxInstance>>,
    pub(super) runtimes: Mutex<HashMap<Uuid, Arc<AsyncMutex<SandboxRuntime>>>>,
    pub(super) spawner: DynSpawner,
    pub(super) active_backend: BackendKind,
    pub(super) storage: Arc<dyn StorageProvider>,
    pub(super) state_dir: PathBuf,
    pub(super) cancellation: CancellationToken,
    pub(super) warm_pool: Arc<RuntimeWarmPool>,
}

impl SandboxManager {
    /// Build a manager and mark non-recoverable stale runtimes for diagnosis.
    pub fn new(
        config: DaemonConfig,
        mut instances: HashMap<Uuid, SandboxInstance>,
        spawner: DynSpawner,
        active_backend: BackendKind,
        storage: Arc<dyn StorageProvider>,
        cancellation: CancellationToken,
    ) -> Result<Self> {
        for instance in instances.values_mut() {
            if !matches!(
                instance.state,
                SandboxState::Destroyed | SandboxState::Hibernated | SandboxState::RecoveryRequired
            ) {
                instance.transition(SandboxState::RecoveryRequired)?;
                instance.persist(&config.daemon.state_dir)?;
            }
        }
        let warm_pool = RuntimeWarmPool::new(
            config.storage.pool_size,
            config.storage.prefork,
            config.storage.rootfs_size,
            config.storage.mem_size,
            config.daemon.state_dir.join("runtime-pool"),
            storage.clone(),
            spawner.clone(),
            parse_duration(&config.api.request_timeout).unwrap_or(Duration::from_secs(30)),
            config.api.max_file_bytes,
            cancellation.clone(),
        );
        Ok(Self {
            state_dir: config.daemon.state_dir.clone(),
            config,
            instances: Mutex::new(instances),
            runtimes: Mutex::new(HashMap::new()),
            spawner,
            active_backend,
            storage,
            cancellation,
            warm_pool,
        })
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

    /// Kill and clean backend resources whose handles were lost across a
    /// previous daemon exit, before accepting new create requests.
    pub async fn reconcile_orphans(&self) -> Result<usize> {
        let (recovery_instances, known_instances) = {
            let instances = self.instances.lock().map_err(|_| poisoned("instances"))?;
            (
                instances
                    .values()
                    .filter(|metadata| metadata.state == SandboxState::RecoveryRequired)
                    .map(|metadata| (metadata.id, metadata.start_path))
                    .collect::<Vec<_>>(),
                instances
                    .values()
                    .map(|metadata| (metadata.id, metadata.state))
                    .collect::<HashMap<_, _>>(),
            )
        };
        for (id, start_path) in &recovery_instances {
            self.spawner
                .cleanup_orphan(*id, &self.runtime_dir_for(*id, *start_path))
                .await?;
        }

        let pool_root = self.state_dir.join("runtime-pool");
        let mut pool_slots = Vec::new();
        if pool_root.is_dir() {
            for entry in std::fs::read_dir(&pool_root)? {
                let entry = entry?;
                if !entry.file_type()?.is_dir() {
                    return Err(BlazeDaemonError::Internal(format!(
                        "unexpected non-directory entry in runtime pool: {}",
                        entry.path().display()
                    )));
                }
                let name = entry.file_name();
                let id = name
                    .to_str()
                    .and_then(|value| Uuid::parse_str(value).ok())
                    .ok_or_else(|| {
                        BlazeDaemonError::Internal(format!(
                            "invalid runtime-pool ownership directory: {}",
                            entry.path().display()
                        ))
                    })?;
                pool_slots.push((id, entry.path()));
            }
        }
        for (id, run_dir) in &pool_slots {
            if known_instances
                .get(id)
                .is_some_and(|state| *state != SandboxState::Destroyed)
            {
                continue;
            }
            self.spawner.cleanup_orphan(*id, run_dir).await?;
            self.storage.release_by_id(&id.to_string()).await?;
            match tokio::fs::remove_dir_all(run_dir).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(recovery_instances.len()
            + pool_slots
                .iter()
                .filter(|(id, _)| {
                    !known_instances
                        .get(id)
                        .is_some_and(|state| *state != SandboxState::Destroyed)
                })
                .count())
    }

    /// Return one persisted sandbox.
    pub fn get(&self, id: Uuid) -> Result<SandboxInstance> {
        self.instances
            .lock()
            .map_err(|_| poisoned("instances"))?
            .get(&id)
            .cloned()
            .ok_or_else(|| BlazeDaemonError::NotFound(format!("sandbox {id}")))
    }

    /// Create and wait for a usable sandbox runtime.
    pub async fn create(self: &Arc<Self>, request: CreateSandbox) -> Result<CreateSandboxResult> {
        if let Some(id) = request.requested_id
            && let Some(existing) = self
                .instances
                .lock()
                .map_err(|_| poisoned("instances"))?
                .get(&id)
                .cloned()
        {
            if existing.state == SandboxState::Running && existing.operation.is_none() {
                if existing.image_digest != request.image_digest
                    || existing.template_name != request.template_name
                    || existing.policy_name != request.decision.policy_name
                {
                    return Err(BlazeDaemonError::Conflict(format!(
                        "sandbox {id} already exists with different create parameters"
                    )));
                }
                return Ok(CreateSandboxResult {
                    selected_backend: existing.backend,
                    instance: existing,
                    existing: true,
                });
            }
            return Err(BlazeDaemonError::Conflict(format!(
                "sandbox {id} already exists in state {}",
                existing.state
            )));
        }

        let pool_compatible = if request.requested_id.is_none() && request.decision.pool_eligible {
            self.warm_pool.configure(PoolPrototype {
                binary_path: request.binary_path.clone(),
                backend: request.decision.backend.clone(),
                vm: request.decision.vm.clone(),
                network: network_config(&request.decision.backend),
            })?
        } else {
            false
        };
        let pooled = if pool_compatible
            && request.requested_id.is_none()
            && request.decision.pool_eligible
        {
            self.warm_pool.acquire()?
        } else {
            None
        };
        let id = request
            .requested_id
            .or_else(|| pooled.as_ref().map(|slot| slot.instance_id))
            .unwrap_or_else(Uuid::new_v4);
        let start_path = if pooled.is_some() {
            StartPath::Warm
        } else {
            StartPath::Cold
        };
        let mut instance = SandboxInstance::new_with_id(
            id,
            self.active_backend,
            request.decision.workload_class,
            request.image_digest,
            start_path,
            request.decision.policy_name.clone(),
        );
        instance.template_name = request.template_name;
        instance.backend_config = request.decision.backend.clone();
        instance.vm_config = request.decision.vm.clone();
        instance.transition(SandboxState::Creating)?;
        instance.start_path = start_path;
        instance.begin_operation(OperationKind::Create, None);
        let duplicate = {
            let mut instances = self.instances.lock().map_err(|_| poisoned("instances"))?;
            match instances.entry(id) {
                Entry::Vacant(entry) => {
                    entry.insert(instance.clone());
                    false
                }
                Entry::Occupied(_) => true,
            }
        };
        if duplicate {
            if let Some(slot) = pooled {
                self.warm_pool.discard(slot).await?;
            }
            return Err(BlazeDaemonError::Conflict(format!(
                "sandbox {id} was created concurrently"
            )));
        }
        if let Err(error) = instance.persist(&self.state_dir) {
            self.instances
                .lock()
                .map_err(|_| poisoned("instances"))?
                .remove(&id);
            if let Some(slot) = pooled {
                self.warm_pool.discard(slot).await?;
            }
            return Err(error.into());
        }

        let resources = self
            .start_runtime(
                id,
                pooled,
                &request.binary_path,
                &request.decision.backend,
                request.decision.vm.as_ref(),
            )
            .await;
        let (runtime, selected_backend) = match resources {
            Ok(resources) => resources,
            Err(error) => {
                if matches!(error, BlazeDaemonError::RecoveryRequired(_)) {
                    self.mark_recovery(id)?;
                } else {
                    self.fail_create(id)?;
                }
                return Err(error);
            }
        };

        let supervised_backend = runtime.backend.as_ref().cloned();
        let runtime = Arc::new(AsyncMutex::new(runtime));
        self.runtimes
            .lock()
            .map_err(|_| poisoned("runtimes"))?
            .insert(id, runtime.clone());
        let completed = self.update_instance(id, |metadata| {
            metadata.backend = selected_backend;
            metadata.transition(SandboxState::Running)?;
            metadata.finish_operation();
            Ok(())
        });
        let completed = match completed {
            Ok(completed) => completed,
            Err(error) => {
                self.runtimes
                    .lock()
                    .map_err(|_| poisoned("runtimes"))?
                    .remove(&id);
                let mut runtime = runtime.lock().await;
                if let Some(backend) = runtime.backend.as_ref().cloned() {
                    if let Err(cleanup) = backend.kill().await {
                        let storage = runtime.storage.clone();
                        let backend = runtime.backend.take();
                        drop(runtime);
                        self.retain_recovery_runtime(id, storage, backend)?;
                        self.mark_recovery(id)?;
                        return Err(BlazeDaemonError::RecoveryRequired(format!(
                            "create state commit failed ({error}); backend cleanup failed ({cleanup})"
                        )));
                    }
                    runtime.backend = None;
                }
                let runtime_dir = self.runtime_dir_for(id, self.get(id)?.start_path);
                if let Err(cleanup) = remove_runtime_dir(&runtime_dir).await {
                    let storage = runtime.storage.clone();
                    drop(runtime);
                    self.retain_recovery_runtime(id, storage, None)?;
                    self.mark_recovery(id)?;
                    return Err(BlazeDaemonError::RecoveryRequired(format!(
                        "create state commit failed ({error}); runtime directory cleanup failed ({cleanup})"
                    )));
                }
                if let Err(cleanup) = self.storage.release(runtime.storage.clone()).await {
                    let storage = runtime.storage.clone();
                    drop(runtime);
                    self.retain_recovery_runtime(id, storage, None)?;
                    self.mark_recovery(id)?;
                    return Err(BlazeDaemonError::RecoveryRequired(format!(
                        "create state commit failed ({error}); storage cleanup failed ({cleanup})"
                    )));
                }
                let _ = self.fail_create(id);
                return Err(error);
            }
        };
        if let Some(backend) = supervised_backend {
            self.start_backend_supervisor(id, backend);
        }
        Ok(CreateSandboxResult {
            instance: completed,
            selected_backend,
            existing: false,
        })
    }

    /// Execute one command through the guest agent.
    pub async fn exec(
        &self,
        id: Uuid,
        command: String,
        cwd: Option<String>,
        env: Option<HashMap<String, String>>,
        timeout_secs: u32,
    ) -> Result<GuestExecResult> {
        let runtime = self.runtime(id)?;
        let runtime = runtime.lock().await;
        self.require_state(id, SandboxState::Running)?;
        let guest = runtime.guest.as_ref().ok_or_else(|| {
            BlazeDaemonError::Conflict(format!("sandbox {id} has no guest agent"))
        })?;
        Ok(guest.exec(command, cwd, env, timeout_secs).await?)
    }

    /// Read one guest file.
    pub async fn read_file(&self, id: Uuid, path: String) -> Result<Vec<u8>> {
        let runtime = self.runtime(id)?;
        let runtime = runtime.lock().await;
        self.require_state(id, SandboxState::Running)?;
        let guest = runtime.guest.as_ref().ok_or_else(|| {
            BlazeDaemonError::Conflict(format!("sandbox {id} has no guest agent"))
        })?;
        Ok(guest.read_file(path).await?)
    }

    /// Replace one guest file.
    pub async fn write_file(&self, id: Uuid, path: String, data: &[u8]) -> Result<()> {
        let runtime = self.runtime(id)?;
        let runtime = runtime.lock().await;
        self.require_state(id, SandboxState::Running)?;
        let guest = runtime.guest.as_ref().ok_or_else(|| {
            BlazeDaemonError::Conflict(format!("sandbox {id} has no guest agent"))
        })?;
        guest.write_file(path, data).await?;
        Ok(())
    }

    /// Idempotently destroy one sandbox and all runtime resources.
    pub async fn destroy(&self, id: Uuid) -> Result<bool> {
        let initial = self.get(id)?;
        if initial.state == SandboxState::Destroyed {
            return Ok(false);
        }
        let runtime = match self.runtime(id) {
            Ok(runtime) => runtime,
            Err(error) => {
                let latest = self.get(id)?;
                if latest.state == SandboxState::Destroyed {
                    return Ok(false);
                }
                if latest.state == SandboxState::Hibernated {
                    match self.reconstruct_hibernated_runtime(id).await {
                        Ok(runtime) => runtime,
                        Err(_reconstruct_error)
                            if self.get(id)?.state == SandboxState::Destroyed =>
                        {
                            return Ok(false);
                        }
                        Err(reconstruct_error) => return Err(reconstruct_error),
                    }
                } else if latest.state == SandboxState::RecoveryRequired {
                    self.reconstruct_recovery_runtime(id).await?
                } else {
                    self.mark_recovery(id)?;
                    return Err(error);
                }
            }
        };
        let mut runtime = runtime.lock().await;
        if self.get(id)?.state == SandboxState::Destroyed {
            return Ok(false);
        }
        self.update_instance(id, |metadata| {
            metadata.begin_operation(OperationKind::Destroy, None);
            Ok(())
        })?;
        if let Some(backend) = runtime.backend.take() {
            let killed = match crate::failpoint::backend("destroy-kill") {
                Ok(()) => backend.kill().await,
                Err(error) => Err(error),
            };
            if let Err(error) = killed {
                let run_dir = self.runtime_dir_for(id, initial.start_path);
                let cleaned = match crate::failpoint::backend("destroy-orphan-cleanup") {
                    Ok(()) => self.spawner.cleanup_orphan(id, &run_dir).await,
                    Err(error) => Err(error),
                };
                if let Err(cleanup) = cleaned {
                    runtime.backend = Some(backend);
                    self.mark_recovery(id)?;
                    return Err(BlazeDaemonError::RecoveryRequired(format!(
                        "backend kill failed ({error}); orphan cleanup failed ({cleanup})"
                    )));
                }
            }
        }
        let runtime_dir = self.runtime_dir_for(id, initial.start_path);
        if initial.state == SandboxState::RecoveryRequired
            && let Err(cleanup) = self.spawner.cleanup_orphan(id, &runtime_dir).await
        {
            self.mark_recovery(id)?;
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "recovery cleanup failed for sandbox {id}: {cleanup}"
            )));
        }
        if let Err(error) = remove_runtime_dir(&runtime_dir).await {
            self.mark_recovery(id)?;
            return Err(error);
        }
        let release = self.storage.release(runtime.storage.clone()).await;
        drop(runtime);
        if let Err(error) = release {
            self.mark_recovery(id)?;
            return Err(error.into());
        }
        self.runtimes
            .lock()
            .map_err(|_| poisoned("runtimes"))?
            .remove(&id);
        self.update_instance(id, |metadata| {
            metadata.transition(SandboxState::Destroyed)?;
            metadata.finish_operation();
            Ok(())
        })?;
        Ok(true)
    }

    /// Return real async warm-pool status.
    pub fn pool_status(&self) -> PoolStatus {
        self.warm_pool.status()
    }

    /// Drain actual warm-pool resources.
    pub async fn drain_pool(&self) -> Result<usize> {
        Ok(self.warm_pool.drain_and_refill().await?)
    }

    /// Cancel background work and release warm resources.
    pub async fn shutdown(&self) -> Result<()> {
        self.cancellation.cancel();
        let mut errors = Vec::new();
        let runtimes = self
            .runtimes
            .lock()
            .map_err(|_| poisoned("runtimes"))?
            .iter()
            .map(|(id, runtime)| (*id, runtime.clone()))
            .collect::<Vec<_>>();
        for (id, runtime) in runtimes {
            let mut runtime = runtime.lock().await;
            if let Some(backend) = runtime.backend.as_ref().cloned() {
                let killed = match crate::failpoint::backend("shutdown-kill") {
                    Ok(()) => backend.kill().await,
                    Err(error) => Err(error),
                };
                match killed {
                    Ok(()) => runtime.backend = None,
                    Err(error) => {
                        tracing::warn!(sandbox_id = %id, %error, "backend shutdown failed");
                        errors.push(format!("sandbox {id}: {error}"));
                    }
                }
            }
            runtime.guest = None;
            if self
                .get(id)
                .map(|metadata| {
                    !matches!(
                        metadata.state,
                        SandboxState::Destroyed
                            | SandboxState::Hibernated
                            | SandboxState::RecoveryRequired
                    )
                })
                .unwrap_or(false)
                && let Err(error) = self.mark_recovery(id)
            {
                tracing::warn!(sandbox_id = %id, %error, "persist shutdown recovery state failed");
                errors.push(format!("sandbox {id} recovery state: {error}"));
            }
        }
        if let Err(error) = self.warm_pool.shutdown().await {
            tracing::warn!(%error, "runtime warm-pool shutdown failed");
            errors.push(format!("runtime pool: {error}"));
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(BlazeDaemonError::RecoveryRequired(format!(
                "daemon shutdown left recoverable resources: {}",
                errors.join("; ")
            )))
        }
    }

    pub(super) fn runtime(&self, id: Uuid) -> Result<Arc<AsyncMutex<SandboxRuntime>>> {
        self.runtimes
            .lock()
            .map_err(|_| poisoned("runtimes"))?
            .get(&id)
            .cloned()
            .ok_or_else(|| {
                BlazeDaemonError::Conflict(format!(
                    "sandbox {id} has no live runtime; recovery is required"
                ))
            })
    }

    pub(super) fn require_state(&self, id: Uuid, expected: SandboxState) -> Result<()> {
        let actual = self.get(id)?.state;
        if actual != expected {
            return Err(BlazeDaemonError::Conflict(format!(
                "sandbox {id} must be {expected}, currently {actual}"
            )));
        }
        Ok(())
    }

    pub(super) fn update_instance(
        &self,
        id: Uuid,
        update: impl FnOnce(&mut SandboxInstance) -> blaze_core::Result<()>,
    ) -> Result<SandboxInstance> {
        let mut instances = self.instances.lock().map_err(|_| poisoned("instances"))?;
        let mut candidate = instances
            .get(&id)
            .cloned()
            .ok_or_else(|| BlazeDaemonError::NotFound(format!("sandbox {id}")))?;
        update(&mut candidate)?;
        candidate.persist(&self.state_dir)?;
        instances.insert(id, candidate.clone());
        Ok(candidate)
    }

    pub(super) fn mark_recovery(&self, id: Uuid) -> Result<()> {
        self.update_instance(id, |metadata| {
            if metadata.state != SandboxState::RecoveryRequired {
                metadata.transition(SandboxState::RecoveryRequired)?;
            }
            Ok(())
        })?;
        Ok(())
    }

    pub(super) async fn reconstruct_hibernated_runtime(
        &self,
        id: Uuid,
    ) -> Result<Arc<AsyncMutex<SandboxRuntime>>> {
        if let Ok(runtime) = self.runtime(id) {
            return Ok(runtime);
        }
        self.require_state(id, SandboxState::Hibernated)?;
        let storage = self.storage.reconstruct(&id.to_string()).await?;
        let runtime = Arc::new(AsyncMutex::new(SandboxRuntime {
            storage,
            backend: None,
            guest: None,
        }));
        let mut runtimes = self.runtimes.lock().map_err(|_| poisoned("runtimes"))?;
        Ok(runtimes
            .entry(id)
            .or_insert_with(|| runtime.clone())
            .clone())
    }

    async fn reconstruct_recovery_runtime(
        &self,
        id: Uuid,
    ) -> Result<Arc<AsyncMutex<SandboxRuntime>>> {
        if let Ok(runtime) = self.runtime(id) {
            return Ok(runtime);
        }
        self.require_state(id, SandboxState::RecoveryRequired)?;
        let metadata = self.get(id)?;
        self.spawner
            .cleanup_orphan(id, &self.runtime_dir_for(id, metadata.start_path))
            .await?;
        let storage = self.storage.reconstruct(&id.to_string()).await?;
        let runtime = Arc::new(AsyncMutex::new(SandboxRuntime {
            storage,
            backend: None,
            guest: None,
        }));
        let mut runtimes = self.runtimes.lock().map_err(|_| poisoned("runtimes"))?;
        Ok(runtimes
            .entry(id)
            .or_insert_with(|| runtime.clone())
            .clone())
    }

    pub(super) fn request_timeout(&self) -> Duration {
        parse_duration(&self.config.api.request_timeout).unwrap_or(Duration::from_secs(30))
    }

    pub(super) fn binary_path(&self) -> PathBuf {
        self.config
            .backends
            .get(self.active_backend.as_str())
            .cloned()
            .unwrap_or_default()
    }

    pub(super) fn runtime_dir_for(&self, id: Uuid, start_path: StartPath) -> PathBuf {
        match start_path {
            StartPath::Cold => self.state_dir.join(id.to_string()).join("runtime"),
            StartPath::Warm => self.state_dir.join("runtime-pool").join(id.to_string()),
        }
    }

    async fn start_runtime(
        &self,
        id: Uuid,
        pooled: Option<RuntimePoolSlot>,
        binary_path: &Path,
        backend: &BackendConfigs,
        vm: Option<&VmConfig>,
    ) -> Result<(SandboxRuntime, BackendKind)> {
        let (storage, mut backend_instance, run_dir) = match pooled {
            Some(slot) => (slot.storage, slot.backend, slot.run_dir),
            None => {
                let storage = self
                    .storage
                    .acquire(&AcquireOpts {
                        instance_id: id.to_string(),
                        rootfs_size: self.config.storage.rootfs_size,
                        mem_size: self.config.storage.mem_size,
                    })
                    .await?;
                (
                    storage,
                    None,
                    self.state_dir.join(id.to_string()).join("runtime"),
                )
            }
        };
        if backend_instance.is_none() {
            let request = SpawnRequest {
                instance_id: id,
                run_dir: run_dir.clone(),
                binary_path: binary_path.to_path_buf(),
                storage: storage.clone(),
                backend: backend.clone(),
                vm: vm.cloned(),
                network: network_config(backend),
            };
            let spawned = match crate::failpoint::backend("create-spawn") {
                Ok(()) => self.spawner.spawn(request).await,
                Err(error) => Err(error),
            };
            match spawned {
                Ok(instance) => backend_instance = Some(instance),
                Err(error) => {
                    if let Err(cleanup) = self.spawner.cleanup_orphan(id, &run_dir).await {
                        self.retain_recovery_runtime(id, storage, None)?;
                        return Err(BlazeDaemonError::RecoveryRequired(format!(
                            "spawn failed ({error}); orphan cleanup failed ({cleanup})"
                        )));
                    }
                    if let Err(cleanup) = remove_runtime_dir(&run_dir).await {
                        self.retain_recovery_runtime(id, storage, None)?;
                        return Err(BlazeDaemonError::RecoveryRequired(format!(
                            "spawn failed ({error}); runtime directory cleanup failed ({cleanup})"
                        )));
                    }
                    return match self.storage.release(storage.clone()).await {
                        Ok(()) => Err(error.into()),
                        Err(cleanup) => {
                            self.retain_recovery_runtime(id, storage, None)?;
                            Err(BlazeDaemonError::RecoveryRequired(format!(
                                "spawn failed ({error}); storage cleanup failed ({cleanup})"
                            )))
                        }
                    };
                }
            }
        }
        let backend_instance = backend_instance.ok_or_else(|| {
            BlazeDaemonError::Internal("backend spawn returned no instance".to_string())
        })?;
        let selected_backend = backend_instance.backend();
        let guest = if guest_enabled(selected_backend, backend) {
            let guest = GuestClient::new(
                backend_instance.guest_socket_path().to_path_buf(),
                self.request_timeout(),
                self.config.api.max_file_bytes,
            );
            let ready = match crate::failpoint::guest("create-guest-ready") {
                Ok(()) => {
                    guest
                        .wait_ready(self.request_timeout(), &self.cancellation)
                        .await
                }
                Err(error) => Err(error),
            };
            if let Err(error) = ready {
                if let Err(cleanup) = backend_instance.kill().await {
                    self.retain_recovery_runtime(id, storage, Some(backend_instance.clone()))?;
                    return Err(BlazeDaemonError::RecoveryRequired(format!(
                        "guest readiness failed ({error}); backend cleanup failed ({cleanup})"
                    )));
                }
                if let Err(cleanup) = remove_runtime_dir(&run_dir).await {
                    self.retain_recovery_runtime(id, storage, None)?;
                    return Err(BlazeDaemonError::RecoveryRequired(format!(
                        "guest readiness failed ({error}); runtime directory cleanup failed ({cleanup})"
                    )));
                }
                return match self.storage.release(storage.clone()).await {
                    Ok(()) => Err(error.into()),
                    Err(cleanup) => {
                        self.retain_recovery_runtime(id, storage, None)?;
                        Err(BlazeDaemonError::RecoveryRequired(format!(
                            "guest readiness failed ({error}); storage cleanup failed ({cleanup})"
                        )))
                    }
                };
            }
            Some(guest)
        } else {
            None
        };
        Ok((
            SandboxRuntime {
                storage,
                backend: Some(backend_instance),
                guest,
            },
            selected_backend,
        ))
    }

    fn fail_create(&self, id: Uuid) -> Result<()> {
        self.update_instance(id, |metadata| {
            metadata.transition(SandboxState::Destroyed)?;
            metadata.finish_operation();
            Ok(())
        })?;
        Ok(())
    }

    fn retain_recovery_runtime(
        &self,
        id: Uuid,
        storage: StorageSlot,
        backend: Option<DynBackendInstance>,
    ) -> Result<()> {
        self.runtimes
            .lock()
            .map_err(|_| poisoned("runtimes"))?
            .entry(id)
            .or_insert_with(|| {
                Arc::new(AsyncMutex::new(SandboxRuntime {
                    storage,
                    backend,
                    guest: None,
                }))
            });
        Ok(())
    }

    pub(super) fn start_backend_supervisor(
        self: &Arc<Self>,
        id: Uuid,
        backend: DynBackendInstance,
    ) {
        tracing::debug!(
            sandbox_id = %id,
            backend_instance_id = %backend.instance_id(),
            pid = ?backend.pid(),
            "starting backend supervisor"
        );
        let manager = Arc::downgrade(self);
        tokio::spawn(async move {
            let outcome = backend.wait().await;
            let Some(manager) = manager.upgrade() else {
                return;
            };
            if manager.cancellation.is_cancelled() {
                return;
            }
            let runtime = match manager.runtime(id) {
                Ok(runtime) => runtime,
                Err(_) => return,
            };
            let mut runtime = runtime.lock().await;
            let is_current = runtime
                .backend
                .as_ref()
                .map(|current| Arc::ptr_eq(current, &backend))
                .unwrap_or(false);
            let is_running = manager
                .get(id)
                .map(|metadata| {
                    metadata.state == SandboxState::Running && metadata.operation.is_none()
                })
                .unwrap_or(false);
            if !is_current || !is_running {
                return;
            }
            runtime.guest = None;
            drop(runtime);
            if let Err(error) = manager.mark_recovery(id) {
                tracing::error!(sandbox_id = %id, %error, "failed to persist backend exit");
            }
            match outcome {
                Ok(result) => {
                    tracing::warn!(
                        sandbox_id = %id,
                        exit_code = ?result.exit_code,
                        signal = ?result.signal,
                        "backend exited unexpectedly"
                    );
                }
                Err(error) => {
                    tracing::warn!(sandbox_id = %id, %error, "backend supervisor failed");
                }
            }
        });
    }
}

pub(super) fn network_config(backend: &BackendConfigs) -> Option<NetworkConfig> {
    backend
        .firecracker
        .as_ref()
        .map(|firecracker| NetworkConfig {
            enabled: firecracker.enable_network,
            interface_id: "eth0".to_string(),
        })
}

pub(super) fn guest_enabled(backend: BackendKind, config: &BackendConfigs) -> bool {
    backend == BackendKind::Mock
        || (backend == BackendKind::Firecracker
            && config
                .firecracker
                .as_ref()
                .map(|firecracker| firecracker.enable_vsock)
                .unwrap_or(false))
}

fn poisoned(name: &str) -> BlazeDaemonError {
    BlazeDaemonError::Internal(format!("{name} lock poisoned"))
}

async fn remove_runtime_dir(path: &std::path::Path) -> Result<()> {
    match tokio::fs::remove_dir_all(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use blaze_core::backend::BackendKind;
    use blaze_core::policy::{FallbackOnMissingHook, PolicyHooks, RuntimeDecision, WorkloadClass};

    use crate::file_provider::FileStorageProvider;
    use crate::spawner::MockSpawner;

    use super::*;

    fn decision() -> RuntimeDecision {
        RuntimeDecision {
            policy_name: "test".into(),
            workload_class: WorkloadClass::AgentTool,
            backend_priority: vec![BackendKind::Mock],
            kernel_hooks: Vec::new(),
            templates: Vec::new(),
            fallback_on_missing_hook: FallbackOnMissingHook::Fail,
            pool: None,
            checkpoint: None,
            quota: None,
            hooks: PolicyHooks::default(),
            backend: BackendConfigs::default(),
            vm: None,
            pool_eligible: false,
        }
    }

    fn manager(temp: &Path) -> Arc<SandboxManager> {
        let images = temp.join("images");
        let instances = temp.join("instances");
        std::fs::create_dir_all(&images).expect("images");
        std::fs::create_dir_all(&instances).expect("instances");
        let mut config = DaemonConfig::default();
        config.daemon.state_dir = temp.join("state");
        config.storage.images_dir = images.clone();
        config.storage.instances_dir = instances.clone();
        config.storage.rootfs_size = 64;
        config.storage.mem_size = 32;
        std::fs::create_dir_all(&config.daemon.state_dir).expect("state");
        Arc::new(
            SandboxManager::new(
                config,
                HashMap::new(),
                Arc::new(MockSpawner),
                BackendKind::Mock,
                Arc::new(FileStorageProvider::with_images(images, instances)),
                CancellationToken::new(),
            )
            .expect("manager"),
        )
    }

    #[tokio::test]
    async fn create_guest_io_and_idempotent_destroy() {
        let temp = tempfile::tempdir().expect("temp");
        let manager = manager(temp.path());
        let id = Uuid::new_v4();
        let created = manager
            .create(CreateSandbox {
                requested_id: Some(id),
                decision: decision(),
                image_digest: "sha256:test".into(),
                template_name: "base".into(),
                binary_path: PathBuf::new(),
            })
            .await
            .expect("create");
        assert_eq!(created.instance.state, SandboxState::Running);
        manager
            .write_file(id, "/tmp/value".into(), b"hello")
            .await
            .expect("write");
        assert_eq!(
            manager
                .read_file(id, "/tmp/value".into())
                .await
                .expect("read"),
            b"hello"
        );
        assert!(manager.destroy(id).await.expect("destroy"));
        assert!(!manager.destroy(id).await.expect("idempotent destroy"));
    }

    #[tokio::test]
    async fn duplicate_requested_id_returns_existing_running_sandbox() {
        let temp = tempfile::tempdir().expect("temp");
        let manager = manager(temp.path());
        let id = Uuid::new_v4();
        let request = CreateSandbox {
            requested_id: Some(id),
            decision: decision(),
            image_digest: "sha256:test".into(),
            template_name: "base".into(),
            binary_path: PathBuf::new(),
        };
        manager.create(request.clone()).await.expect("create");
        assert!(manager.create(request).await.expect("repeat").existing);
    }
}
