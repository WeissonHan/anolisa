// SPDX-License-Identifier: Apache-2.0
//! Bounded background construction and ownership transfer for warm runtimes.

use std::collections::{BTreeSet, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use blaze_core::backend::{BackendKind, SpawnRequest};
use blaze_core::lifecycle::BackendOwnership;
use blaze_core::policy::{BackendConfigs, VmConfig, WorkloadClass};
use blaze_core::storage::{AcquireOpts, StorageProvider, StorageSlot};
use blaze_core::{BlazeError, Result};
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::recovery::{
    RuntimeSlotOwnership, RuntimeSlotPhase, finish_ownership_handoff, read_ownership,
    remove_pool_tombstone, tombstone_pool_slot, write_ownership,
};
use crate::guest::{GuestClient, MAX_GUEST_FILE_BYTES};
use crate::spawner::{DynBackendInstance, SpawnerRegistry};

const RETRY_BASE_DELAY: Duration = Duration::from_millis(100);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(30);
const GUEST_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Immutable inputs shared by every slot in one runtime pool generation.
#[derive(Debug, Clone)]
pub(crate) struct PoolPrototype {
    pub(crate) image_digest: String,
    pub(crate) policy_name: String,
    pub(crate) workload_class: WorkloadClass,
    pub(crate) templates: Vec<String>,
    pub(crate) kernel_hooks: Vec<String>,
    pub(crate) binary_path: PathBuf,
    pub(crate) runtime_backend: BackendKind,
    pub(crate) backend: BackendConfigs,
    pub(crate) vm: Option<VmConfig>,
    pub(crate) warm_ttl: Duration,
}

impl PoolPrototype {
    fn fingerprint(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(&(
            &self.image_digest,
            &self.policy_name,
            self.workload_class,
            &self.templates,
            &self.kernel_hooks,
            &self.binary_path,
            self.runtime_backend,
            &self.backend,
            &self.vm,
            self.warm_ttl.as_nanos(),
        ))
        .map_err(|error| pool_error(format!("serialize runtime pool prototype: {error}")))
    }
}

/// Resources transferred from the pool into lifecycle ownership.
pub(crate) struct RuntimePoolSlot {
    pub(crate) instance_id: Uuid,
    pub(crate) storage: StorageSlot,
    pub(crate) backend: Option<DynBackendInstance>,
    pub(crate) run_dir: PathBuf,
    pub(crate) runtime_backend: BackendKind,
    pub(crate) backend_ownership: BackendOwnership,
    ready_at: Instant,
}

struct UnresolvedHandoff {
    slot: RuntimePoolSlot,
    token: Uuid,
    reason: String,
}

struct CleanupSlot {
    ownership: RuntimeSlotOwnership,
    backend: Option<DynBackendInstance>,
    run_dir: PathBuf,
    authority: PoolCleanupAuthority,
}

impl CleanupSlot {
    fn from_runtime(slot: RuntimePoolSlot) -> Self {
        Self::from_runtime_with_authority(slot, PoolCleanupAuthority::Ready)
    }

    fn from_runtime_with_authority(slot: RuntimePoolSlot, authority: PoolCleanupAuthority) -> Self {
        let mut ownership = RuntimeSlotOwnership::new(slot.instance_id, slot.runtime_backend);
        ownership.backend_ownership = slot.backend_ownership;
        ownership.storage_owned = true;
        ownership.phase = RuntimeSlotPhase::Ready;
        Self {
            ownership,
            backend: slot.backend,
            run_dir: slot.run_dir,
            authority,
        }
    }
}

#[derive(Clone, Copy)]
enum PoolCleanupAuthority {
    Build,
    Ready,
    Handoff(Uuid),
}

#[derive(Default)]
struct PoolState {
    ready: VecDeque<RuntimePoolSlot>,
    quarantined: VecDeque<CleanupSlot>,
    unresolved: VecDeque<UnresolvedHandoff>,
    building: BTreeSet<Uuid>,
    leased: BTreeSet<Uuid>,
    cleanup_pending: usize,
    prototype: Option<PoolPrototype>,
    prototype_fingerprint: Option<Vec<u8>>,
    generation: u64,
    consecutive_build_failures: u32,
    consecutive_cleanup_failures: u32,
    shutting_down: bool,
}

impl PoolState {
    fn physical_count(&self) -> usize {
        self.ready
            .len()
            .saturating_add(self.building.len())
            .saturating_add(self.leased.len())
            .saturating_add(self.quarantined.len())
            .saturating_add(self.unresolved.len())
            .saturating_add(self.cleanup_pending)
    }
}

/// Bounded worker that retains every incomplete resource for retry.
pub(crate) struct RuntimeWarmPool {
    target: usize,
    prefork: bool,
    rootfs_size: u64,
    mem_size: u64,
    runtime_root: PathBuf,
    storage: Arc<dyn StorageProvider>,
    spawners: Arc<SpawnerRegistry>,
    default_warm_ttl: Duration,
    gc_interval: Duration,
    state: Mutex<PoolState>,
    maintenance: AsyncMutex<()>,
    shutdown: AsyncMutex<()>,
    cancellation: CancellationToken,
    wake: Notify,
    worker: Mutex<Option<JoinHandle<()>>>,
}

struct WorkerJoinGuard<'a> {
    worker_slot: &'a Mutex<Option<JoinHandle<()>>>,
    handle: Option<JoinHandle<()>>,
}

impl<'a> WorkerJoinGuard<'a> {
    fn new(worker_slot: &'a Mutex<Option<JoinHandle<()>>>, handle: Option<JoinHandle<()>>) -> Self {
        Self {
            worker_slot,
            handle,
        }
    }

    fn disarm(&mut self) {
        self.handle.take();
    }
}

impl Drop for WorkerJoinGuard<'_> {
    fn drop(&mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        handle.abort();
        let mut worker = match self.worker_slot.lock() {
            Ok(worker) => worker,
            Err(poisoned) => poisoned.into_inner(),
        };
        if worker.is_none() {
            *worker = Some(handle);
        } else {
            tracing::error!(
                "runtime pool worker slot was occupied while retaining an aborted worker"
            );
        }
    }
}

impl RuntimeWarmPool {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        target: usize,
        prefork: bool,
        rootfs_size: u64,
        mem_size: u64,
        runtime_root: PathBuf,
        storage: Arc<dyn StorageProvider>,
        spawners: Arc<SpawnerRegistry>,
        default_warm_ttl: Duration,
        gc_interval: Duration,
        cancellation: CancellationToken,
    ) -> Result<Arc<Self>> {
        if target > 0 && !storage.supports_runtime_pool_recovery() {
            return Err(pool_error(
                "configured storage provider does not expose runtime slot cleanup inventory",
            ));
        }
        Ok(Arc::new(Self {
            target,
            prefork,
            rootfs_size,
            mem_size,
            runtime_root,
            storage,
            spawners,
            default_warm_ttl,
            gc_interval,
            state: Mutex::new(PoolState::default()),
            maintenance: AsyncMutex::new(()),
            shutdown: AsyncMutex::new(()),
            cancellation,
            wake: Notify::new(),
            worker: Mutex::new(None),
        }))
    }

    /// Fix the first compatible build shape and start maintenance on demand.
    pub(crate) fn configure(self: &Arc<Self>, prototype: PoolPrototype) -> Result<bool> {
        if self.target == 0 {
            return Ok(false);
        }
        let fingerprint = prototype.fingerprint()?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| pool_error("runtime pool state lock poisoned"))?;
        if state.shutting_down {
            return Err(pool_error("runtime pool is shutting down"));
        }
        match state.prototype_fingerprint.as_deref() {
            None => {
                state.prototype = Some(prototype);
                state.prototype_fingerprint = Some(fingerprint);
            }
            Some(existing) if existing == fingerprint.as_slice() => {}
            Some(_) => return Ok(false),
        }
        drop(state);
        self.ensure_worker()?;
        self.wake.notify_one();
        Ok(true)
    }

    /// Lease one ready slot while a synchronous guard protects cancellation.
    pub(crate) async fn acquire(self: &Arc<Self>) -> Result<Option<RuntimePoolLease>> {
        loop {
            let (slot, warm_ttl) = {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| pool_error("runtime pool state lock poisoned"))?;
                let warm_ttl = state
                    .prototype
                    .as_ref()
                    .map(|prototype| prototype.warm_ttl)
                    .unwrap_or(self.default_warm_ttl);
                let slot = state.ready.pop_front();
                if let Some(slot) = &slot {
                    state.leased.insert(slot.instance_id);
                }
                (slot, warm_ttl)
            };
            self.wake.notify_one();
            let Some(slot) = slot else {
                return Ok(None);
            };
            let lease = RuntimePoolLease::new(self.clone(), slot);
            if lease.slot()?.ready_at.elapsed() >= warm_ttl {
                tracing::info!(
                    instance = %lease.slot()?.instance_id,
                    "discarding expired runtime slot before claim"
                );
                lease.quarantine();
                continue;
            }
            let Some(backend) = lease.slot()?.backend.as_ref().cloned() else {
                return Ok(Some(lease));
            };
            let live = tokio::time::timeout(GUEST_REQUEST_TIMEOUT, backend.try_wait()).await;
            match live {
                Ok(Ok(None)) => return Ok(Some(lease)),
                Ok(Ok(Some(result))) => tracing::warn!(
                    instance = %lease.slot()?.instance_id,
                    exit_code = ?result.exit_code,
                    signal = ?result.signal,
                    "discarding exited prefork runtime"
                ),
                Ok(Err(error)) => tracing::warn!(
                    instance = %lease.slot()?.instance_id,
                    %error,
                    "discarding runtime after liveness check failed"
                ),
                Err(_) => tracing::warn!(
                    instance = %lease.slot()?.instance_id,
                    "discarding runtime after liveness check timed out"
                ),
            }
            lease.quarantine();
        }
    }

    pub(crate) fn begin_shutdown(&self) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.shutting_down = true;
        state.generation = state.generation.wrapping_add(1);
        drop(state);
        self.cancellation.cancel();
        self.wake.notify_waiters();
    }

    pub(crate) fn default_warm_ttl(&self) -> Duration {
        self.default_warm_ttl
    }

    /// Stop maintenance and release pool-owned slots before one deadline.
    pub(crate) async fn shutdown_until(
        self: &Arc<Self>,
        deadline: tokio::time::Instant,
    ) -> Result<()> {
        self.begin_shutdown();
        let _shutdown = tokio::time::timeout_at(deadline, self.shutdown.lock())
            .await
            .map_err(|_| {
                pool_error("runtime pool shutdown coordination exceeded the shared deadline")
            })?;
        let worker = self
            .worker
            .lock()
            .map_err(|_| pool_error("runtime pool worker lock poisoned"))?
            .take();
        let mut worker = WorkerJoinGuard::new(&self.worker, worker);
        let mut errors = Vec::new();
        if let Some(handle) = worker.handle.as_mut() {
            match tokio::time::timeout_at(deadline, &mut *handle).await {
                Ok(Ok(())) => {
                    worker.disarm();
                }
                Ok(Err(error)) if error.is_cancelled() => {
                    worker.disarm();
                }
                Ok(Err(error)) => {
                    worker.disarm();
                    errors.push(format!(
                        "runtime pool worker failed while stopping: {error}"
                    ));
                }
                Err(_) => {
                    handle.abort();
                    let result = (&mut *handle).await;
                    worker.disarm();
                    if let Err(error) = result
                        && !error.is_cancelled()
                    {
                        errors.push(format!(
                            "runtime pool worker failed while stopping: {error}"
                        ));
                    }
                    errors.push(
                        "runtime pool worker exceeded the shared shutdown deadline".to_string(),
                    );
                }
            }
        }

        loop {
            let (building, leased) = {
                let state = self
                    .state
                    .lock()
                    .map_err(|_| pool_error("runtime pool state lock poisoned"))?;
                (state.building.len(), state.leased.len())
            };
            if building == 0 && leased == 0 {
                break;
            }
            if tokio::time::timeout_at(deadline, self.wake.notified())
                .await
                .is_err()
            {
                if building != 0 {
                    errors.push(format!(
                        "{building} runtime pool build(s) exceeded the shared shutdown deadline"
                    ));
                }
                if leased != 0 {
                    errors.push(format!(
                        "{leased} runtime pool lease(s) exceeded the shared shutdown deadline"
                    ));
                }
                break;
            }
        }

        let unresolved = {
            let state = self
                .state
                .lock()
                .map_err(|_| pool_error("runtime pool state lock poisoned"))?;
            state
                .unresolved
                .iter()
                .map(|handoff| {
                    format!(
                        "{} (token {}, {})",
                        handoff.slot.instance_id, handoff.token, handoff.reason
                    )
                })
                .collect::<Vec<_>>()
        };
        if !unresolved.is_empty() {
            errors.push(format!(
                "{} runtime pool owner(s) have unresolved lifecycle publication: {}",
                unresolved.len(),
                unresolved.join(", ")
            ));
        }

        let maintenance = match tokio::time::timeout_at(deadline, self.maintenance.lock()).await {
            Ok(maintenance) => Some(maintenance),
            Err(_) => {
                errors.push(
                    "runtime pool maintenance lock exceeded the shared shutdown deadline"
                        .to_string(),
                );
                None
            }
        };
        if let Some(_maintenance) = maintenance {
            let attempts = {
                let state = self
                    .state
                    .lock()
                    .map_err(|_| pool_error("runtime pool state lock poisoned"))?;
                state.ready.len().saturating_add(state.quarantined.len())
            };
            for _ in 0..attempts {
                let cleanup = {
                    let mut state = self
                        .state
                        .lock()
                        .map_err(|_| pool_error("runtime pool state lock poisoned"))?;
                    let cleanup = state
                        .quarantined
                        .pop_front()
                        .or_else(|| state.ready.pop_front().map(CleanupSlot::from_runtime));
                    if cleanup.is_some() {
                        state.cleanup_pending = state.cleanup_pending.saturating_add(1);
                    }
                    cleanup
                };
                let Some(cleanup) = cleanup else {
                    break;
                };
                let instance_id = cleanup.ownership.instance_id;
                let mut cleanup = CleanupGuard::new(self.clone(), cleanup);
                match tokio::time::timeout_at(deadline, self.cleanup_slot(cleanup.slot_mut()?))
                    .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        errors.push(format!("{instance_id}: {error}"));
                        cleanup.retry();
                        continue;
                    }
                    Err(_) => {
                        errors.push(format!(
                            "{instance_id}: cleanup exceeded the shared shutdown deadline"
                        ));
                        cleanup.retry();
                        break;
                    }
                }
                cleanup.complete();
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(pool_error(errors.join("; ")))
        }
    }

    #[cfg(test)]
    pub(crate) fn status(&self) -> RuntimePoolStatus {
        match self.state.lock() {
            Ok(state) => RuntimePoolStatus::from_state(self.target, &state),
            Err(poisoned) => RuntimePoolStatus::from_state(self.target, &poisoned.into_inner()),
        }
    }

    #[cfg(test)]
    pub(crate) fn has_tracked_worker(&self) -> bool {
        match self.worker.lock() {
            Ok(worker) => worker.is_some(),
            Err(poisoned) => poisoned.into_inner().is_some(),
        }
    }

    fn ensure_worker(self: &Arc<Self>) -> Result<()> {
        let state = self
            .state
            .lock()
            .map_err(|_| pool_error("runtime pool state lock poisoned"))?;
        if state.shutting_down {
            return Err(pool_error("runtime pool is shutting down"));
        }
        let mut worker = self
            .worker
            .lock()
            .map_err(|_| pool_error("runtime pool worker lock poisoned"))?;
        if worker.as_ref().is_some_and(JoinHandle::is_finished) {
            worker.take();
        }
        if worker.is_none() {
            let pool = self.clone();
            *worker = Some(tokio::spawn(async move {
                pool.run_worker().await;
            }));
        }
        Ok(())
    }

    async fn run_worker(self: Arc<Self>) {
        let mut gc = tokio::time::interval(self.gc_interval);
        gc.set_missed_tick_behavior(MissedTickBehavior::Skip);
        gc.tick().await;
        loop {
            tokio::select! {
                _ = self.cancellation.cancelled() => return,
                _ = self.wake.notified() => {}
                _ = gc.tick() => {}
            }
            loop {
                if self.cancellation.is_cancelled() {
                    return;
                }
                match self.maintain_once().await {
                    Ok(Maintenance::Progress) => continue,
                    Ok(Maintenance::Idle) => break,
                    Ok(Maintenance::RetryAfter(delay)) => {
                        tokio::select! {
                            _ = self.cancellation.cancelled() => return,
                            _ = tokio::time::sleep(delay) => {}
                        }
                    }
                    Err(error) => {
                        tracing::warn!(%error, "runtime pool maintenance failed");
                        tokio::select! {
                            _ = self.cancellation.cancelled() => return,
                            _ = tokio::time::sleep(RETRY_BASE_DELAY) => {}
                        }
                    }
                }
            }
        }
    }

    async fn maintain_once(self: &Arc<Self>) -> Result<Maintenance> {
        let _maintenance = self.maintenance.lock().await;
        let action = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| pool_error("runtime pool state lock poisoned"))?;
            if state.shutting_down || state.prototype.is_none() {
                return Ok(Maintenance::Idle);
            }
            let warm_ttl = state
                .prototype
                .as_ref()
                .map(|prototype| prototype.warm_ttl)
                .unwrap_or(self.default_warm_ttl);
            let now = Instant::now();
            let mut retained = VecDeque::new();
            while let Some(slot) = state.ready.pop_front() {
                if now.duration_since(slot.ready_at) >= warm_ttl {
                    state.quarantined.push_back(CleanupSlot::from_runtime(slot));
                } else {
                    retained.push_back(slot);
                }
            }
            state.ready = retained;
            if let Some(cleanup) = state.quarantined.pop_front() {
                state.cleanup_pending = state.cleanup_pending.saturating_add(1);
                PoolAction::Cleanup(cleanup)
            } else if state.physical_count() < self.target {
                PoolAction::Build(state.generation)
            } else {
                PoolAction::Idle
            }
        };

        match action {
            PoolAction::Idle => Ok(Maintenance::Idle),
            PoolAction::Build(generation) => match self.build_slot(generation).await {
                Ok(()) => {
                    if let Ok(mut state) = self.state.lock() {
                        state.consecutive_build_failures = 0;
                    }
                    Ok(Maintenance::Progress)
                }
                Err(error) => {
                    let delay = {
                        let mut state = self
                            .state
                            .lock()
                            .map_err(|_| pool_error("runtime pool state lock poisoned"))?;
                        state.consecutive_build_failures =
                            state.consecutive_build_failures.saturating_add(1);
                        build_retry_delay(state.consecutive_build_failures)
                    };
                    tracing::warn!(
                        %error,
                        retry_delay_ms = delay.as_millis(),
                        "runtime slot build failed"
                    );
                    Ok(Maintenance::RetryAfter(delay))
                }
            },
            PoolAction::Cleanup(cleanup) => {
                let mut cleanup = CleanupGuard::new(self.clone(), cleanup);
                match self.cleanup_slot(cleanup.slot_mut()?).await {
                    Ok(()) => {
                        cleanup.complete();
                        Ok(Maintenance::Progress)
                    }
                    Err(error) => {
                        let delay = {
                            let mut state = self
                                .state
                                .lock()
                                .map_err(|_| pool_error("runtime pool state lock poisoned"))?;
                            state.consecutive_cleanup_failures =
                                state.consecutive_cleanup_failures.saturating_add(1);
                            build_retry_delay(state.consecutive_cleanup_failures)
                        };
                        cleanup.retry();
                        tracing::warn!(
                            %error,
                            retry_delay_ms = delay.as_millis(),
                            "runtime slot cleanup will be retried"
                        );
                        Ok(Maintenance::RetryAfter(delay))
                    }
                }
            }
        }
    }

    async fn build_slot(self: &Arc<Self>, generation: u64) -> Result<()> {
        crate::failpoint::storage("pool-build")?;
        let prototype = self
            .state
            .lock()
            .map_err(|_| pool_error("runtime pool state lock poisoned"))?
            .prototype
            .clone()
            .ok_or_else(|| pool_error("runtime pool has no build prototype"))?;
        let instance_id = Uuid::new_v4();
        let run_dir = self.runtime_root.join(instance_id.to_string());
        std::fs::create_dir(&run_dir)?;
        let ownership = RuntimeSlotOwnership::new(instance_id, prototype.runtime_backend);
        let mut build = BuildGuard::new(self.clone(), run_dir, ownership);
        std::fs::File::open(&self.runtime_root)?.sync_all()?;
        build.persist().await?;

        // Acquire may be cancelled after the provider has created artifacts
        // but before it can return a residual slot. Claim cleanup authority
        // first; recovery-capable providers make release-by-ID safe when no
        // artifact was created.
        build.ownership.storage_owned = true;
        build.persist().await?;
        let acquire_opts = AcquireOpts {
            instance_id: instance_id.to_string(),
            rootfs_size: self.rootfs_size,
            mem_size: self.mem_size,
        };
        let acquire = self.storage.acquire(&acquire_opts);
        let storage = match tokio::select! {
            _ = self.cancellation.cancelled() => {
                return Err(pool_error("runtime slot build cancelled during storage acquire"));
            }
            result = acquire => result,
        } {
            Ok(storage) => storage,
            Err(error) => {
                let (source, residual) = error.into_parts();
                if let Some(residual) = residual {
                    build.storage = Some(residual);
                    if let Err(persist) = build.persist().await {
                        return Err(pool_error(format!(
                            "{source}; retain residual storage journal: {persist}"
                        )));
                    }
                }
                return Err(source);
            }
        };
        build.storage = Some(storage);
        build.persist().await?;

        if self.prefork {
            let spawner = self
                .spawners
                .get(prototype.runtime_backend)
                .ok_or_else(|| {
                    pool_error(format!(
                        "no spawner registered for runtime backend {}",
                        prototype.runtime_backend
                    ))
                })?;
            build.ownership.backend_ownership = BackendOwnership::Starting;
            build.persist().await?;
            tokio::select! {
                _ = self.cancellation.cancelled() => {
                    return Err(pool_error(
                        "runtime slot build cancelled during backend preparation"
                    ));
                }
                result = spawner.prepare_spawn(&build.run_dir) => result?,
            }
            let spawn_request = SpawnRequest {
                instance_id,
                run_dir: build.run_dir.clone(),
                binary_path: prototype.binary_path,
                storage: build
                    .storage
                    .as_ref()
                    .ok_or_else(|| pool_error("runtime slot lost storage ownership"))?
                    .clone(),
                backend: prototype.backend,
                vm: prototype.vm,
            };
            let spawn = tokio::select! {
                _ = self.cancellation.cancelled() => {
                    return Err(pool_error(
                        "runtime slot build cancelled during backend spawn"
                    ));
                }
                result = spawner.spawn(spawn_request) => result,
            };
            let backend = match spawn {
                Ok(backend) => backend,
                Err(error) => {
                    let (source, owner) = error.into_parts();
                    if let Some(owner) = owner {
                        build.backend = Some(owner);
                        build.ownership.backend_ownership = BackendOwnership::Running;
                    } else {
                        build.ownership.backend_ownership = BackendOwnership::Stopped;
                    }
                    if let Err(persist) = build.persist().await {
                        return Err(pool_error(format!(
                            "{source}; retain failed backend journal: {persist}"
                        )));
                    }
                    return Err(source);
                }
            };
            build.ownership.backend_ownership = BackendOwnership::Running;
            build.backend = Some(backend.clone());
            build.persist().await?;
            if backend.backend() != prototype.runtime_backend {
                return Err(pool_error(format!(
                    "runtime slot requested {} but spawner returned {}",
                    prototype.runtime_backend,
                    backend.backend()
                )));
            }
            let socket = backend.guest_socket_path();
            if !socket.as_os_str().is_empty() {
                GuestClient::new(
                    socket.to_path_buf(),
                    GUEST_REQUEST_TIMEOUT,
                    MAX_GUEST_FILE_BYTES,
                )
                .wait_ready(GUEST_REQUEST_TIMEOUT, &self.cancellation)
                .await
                .map_err(|error| pool_error(format!("prefork guest readiness failed: {error}")))?;
            }
        }

        build.ownership.phase = RuntimeSlotPhase::Ready;
        build.persist().await?;
        build.publish(generation)
    }

    async fn cleanup_slot(&self, slot: &mut CleanupSlot) -> Result<()> {
        let persisted = match read_ownership(&slot.run_dir, slot.ownership.instance_id).await {
            Ok(persisted) => Some(persisted),
            Err(error) => {
                let ownership_path = slot.run_dir.join("ownership.json");
                let journal_missing = matches!(
                    tokio::fs::symlink_metadata(&ownership_path).await,
                    Err(io_error) if io_error.kind() == std::io::ErrorKind::NotFound
                );
                if matches!(slot.authority, PoolCleanupAuthority::Build) && journal_missing {
                    None
                } else {
                    return Err(pool_error(error));
                }
            }
        };
        let cleanup_committed = persisted
            .as_ref()
            .is_some_and(|ownership| ownership.phase == RuntimeSlotPhase::PoolCleanup);
        if let Some(persisted) = persisted {
            if persisted.backend != slot.ownership.backend {
                return Err(pool_error(format!(
                    "runtime slot {} changed backend from {} to {} before pool cleanup",
                    slot.ownership.instance_id, slot.ownership.backend, persisted.backend
                )));
            }
            let phase_allowed = match (slot.authority, persisted.phase) {
                (PoolCleanupAuthority::Build, RuntimeSlotPhase::Building)
                | (PoolCleanupAuthority::Build, RuntimeSlotPhase::Ready)
                | (PoolCleanupAuthority::Ready, RuntimeSlotPhase::Ready)
                | (_, RuntimeSlotPhase::PoolCleanup) => true,
                (PoolCleanupAuthority::Handoff(expected), RuntimeSlotPhase::Handoff { token }) => {
                    expected == token
                }
                _ => false,
            };
            if !phase_allowed {
                return Err(pool_error(format!(
                    "refusing pool cleanup for runtime slot {} with persisted phase {:?}",
                    slot.ownership.instance_id, persisted.phase
                )));
            }
            slot.ownership.backend_ownership = strongest_backend_ownership(
                slot.ownership.backend_ownership,
                persisted.backend_ownership,
            );
            slot.ownership.storage_owned |= persisted.storage_owned;
        }
        slot.ownership.phase = RuntimeSlotPhase::PoolCleanup;
        if !cleanup_committed {
            write_ownership(&slot.run_dir, &slot.ownership)
                .await
                .map_err(pool_error)?;
        }

        if let Some(backend) = slot.backend.as_ref() {
            backend.kill().await?;
            slot.backend = None;
            slot.ownership.backend_ownership = BackendOwnership::Stopped;
            write_ownership(&slot.run_dir, &slot.ownership)
                .await
                .map_err(pool_error)?;
        } else if matches!(
            slot.ownership.backend_ownership,
            BackendOwnership::Unknown | BackendOwnership::Starting | BackendOwnership::Running
        ) {
            let spawner = self.spawners.get(slot.ownership.backend).ok_or_else(|| {
                pool_error(format!(
                    "no cleanup spawner registered for {}",
                    slot.ownership.backend
                ))
            })?;
            spawner
                .cleanup_orphan(slot.ownership.instance_id, &slot.run_dir)
                .await?;
            slot.ownership.backend_ownership = BackendOwnership::Stopped;
            write_ownership(&slot.run_dir, &slot.ownership)
                .await
                .map_err(pool_error)?;
        }

        if slot.ownership.storage_owned {
            self.storage
                .release_by_id(&slot.ownership.instance_id.to_string())
                .await?;
            slot.ownership.storage_owned = false;
            write_ownership(&slot.run_dir, &slot.ownership)
                .await
                .map_err(pool_error)?;
        }
        tombstone_pool_slot(&self.runtime_root, slot.ownership.instance_id)
            .await
            .map_err(pool_error)?;
        remove_pool_tombstone(&self.runtime_root, slot.ownership.instance_id)
            .await
            .map_err(pool_error)
    }

    fn abandon_lease(&self, slot: RuntimePoolSlot, authority: PoolCleanupAuthority) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.leased.remove(&slot.instance_id);
        state
            .quarantined
            .push_back(CleanupSlot::from_runtime_with_authority(slot, authority));
        drop(state);
        self.wake.notify_one();
    }

    fn retain_unresolved(&self, handoff: UnresolvedHandoff) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.leased.remove(&handoff.slot.instance_id);
        state.unresolved.push_back(handoff);
        drop(state);
        self.wake.notify_one();
    }

    fn complete_lease(&self, instance_id: Uuid) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.leased.remove(&instance_id);
        drop(state);
        self.wake.notify_one();
    }
}

/// Claim guard whose drop path never loses pool or lifecycle ownership.
pub(crate) struct RuntimePoolLease {
    pool: Arc<RuntimeWarmPool>,
    slot: Option<RuntimePoolSlot>,
    owner: LeaseOwner,
    cleanup_authority: PoolCleanupAuthority,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum LeaseOwner {
    Pool,
    Lifecycle,
}

impl RuntimePoolLease {
    fn new(pool: Arc<RuntimeWarmPool>, slot: RuntimePoolSlot) -> Self {
        Self {
            pool,
            slot: Some(slot),
            owner: LeaseOwner::Pool,
            cleanup_authority: PoolCleanupAuthority::Ready,
        }
    }

    pub(crate) fn slot(&self) -> Result<&RuntimePoolSlot> {
        self.slot
            .as_ref()
            .ok_or_else(|| pool_error("runtime pool lease lost its slot"))
    }

    pub(crate) async fn begin_handoff(&mut self, token: Uuid) -> Result<()> {
        let slot = self
            .slot
            .as_ref()
            .ok_or_else(|| pool_error("runtime pool lease lost its slot"))?;
        let mut ownership = read_ownership(&slot.run_dir, slot.instance_id)
            .await
            .map_err(pool_error)?;
        if ownership.phase != RuntimeSlotPhase::Ready {
            return Err(pool_error(format!(
                "runtime slot {} is not ready for handoff",
                slot.instance_id
            )));
        }
        ownership.phase = RuntimeSlotPhase::Handoff { token };
        match write_ownership(&slot.run_dir, &ownership).await {
            Ok(()) => {
                self.cleanup_authority = PoolCleanupAuthority::Handoff(token);
                Ok(())
            }
            Err(error) => {
                if read_ownership(&slot.run_dir, slot.instance_id)
                    .await
                    .is_ok_and(|persisted| persisted == ownership)
                {
                    self.cleanup_authority = PoolCleanupAuthority::Handoff(token);
                }
                Err(pool_error(error))
            }
        }
    }

    /// Switch the synchronous cancellation fallback after lifecycle is durable.
    pub(crate) fn transfer_to_lifecycle(&mut self) {
        self.owner = LeaseOwner::Lifecycle;
    }

    pub(crate) async fn finish_handoff(&mut self, token: Uuid) -> Result<()> {
        let slot = self
            .slot
            .as_ref()
            .ok_or_else(|| pool_error("runtime pool lease lost its slot"))?;
        finish_ownership_handoff(
            &slot.run_dir,
            slot.instance_id,
            slot.runtime_backend,
            slot.backend_ownership,
            token,
        )
        .await
        .map_err(pool_error)
    }

    pub(crate) fn into_slot(mut self) -> Result<RuntimePoolSlot> {
        let slot = self
            .slot
            .take()
            .ok_or_else(|| pool_error("runtime pool lease lost its slot"))?;
        self.pool.complete_lease(slot.instance_id);
        Ok(slot)
    }

    /// Keep an ambiguous handoff visible and counted without choosing a
    /// cleanup owner in the current process.
    pub(crate) fn retain_unresolved(mut self, token: Uuid, reason: String) -> Result<()> {
        if !matches!(
            self.cleanup_authority,
            PoolCleanupAuthority::Handoff(expected) if expected == token
        ) {
            return Err(pool_error(
                "runtime pool lease has no matching handoff authority",
            ));
        }
        let slot = self
            .slot
            .take()
            .ok_or_else(|| pool_error("runtime pool lease lost its slot"))?;
        self.pool.retain_unresolved(UnresolvedHandoff {
            slot,
            token,
            reason,
        });
        Ok(())
    }

    fn quarantine(mut self) {
        if let Some(slot) = self.slot.take() {
            self.pool.abandon_lease(slot, self.cleanup_authority);
        }
    }
}

impl Drop for RuntimePoolLease {
    fn drop(&mut self) {
        let Some(slot) = self.slot.take() else {
            return;
        };
        match self.owner {
            LeaseOwner::Pool => self.pool.abandon_lease(slot, self.cleanup_authority),
            LeaseOwner::Lifecycle => self.pool.complete_lease(slot.instance_id),
        }
    }
}

struct BuildGuard {
    pool: Arc<RuntimeWarmPool>,
    ownership: RuntimeSlotOwnership,
    storage: Option<StorageSlot>,
    backend: Option<DynBackendInstance>,
    run_dir: PathBuf,
    armed: bool,
}

impl BuildGuard {
    fn new(pool: Arc<RuntimeWarmPool>, run_dir: PathBuf, ownership: RuntimeSlotOwnership) -> Self {
        let mut state = match pool.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.building.insert(ownership.instance_id);
        drop(state);
        Self {
            pool,
            ownership,
            storage: None,
            backend: None,
            run_dir,
            armed: true,
        }
    }

    async fn persist(&self) -> Result<()> {
        write_ownership(&self.run_dir, &self.ownership)
            .await
            .map_err(pool_error)
    }

    fn publish(mut self, generation: u64) -> Result<()> {
        let storage = self
            .storage
            .take()
            .ok_or_else(|| pool_error("completed runtime slot has no storage owner"))?;
        let slot = RuntimePoolSlot {
            instance_id: self.ownership.instance_id,
            storage,
            backend: self.backend.take(),
            run_dir: self.run_dir.clone(),
            runtime_backend: self.ownership.backend,
            backend_ownership: self.ownership.backend_ownership,
            ready_at: Instant::now(),
        };
        let mut state = self
            .pool
            .state
            .lock()
            .map_err(|_| pool_error("runtime pool state lock poisoned"))?;
        state.building.remove(&slot.instance_id);
        if !state.shutting_down
            && state.generation == generation
            && state.physical_count() < self.pool.target
        {
            state.ready.push_back(slot);
        } else {
            state.quarantined.push_back(CleanupSlot::from_runtime(slot));
        }
        self.armed = false;
        drop(state);
        self.pool.wake.notify_one();
        Ok(())
    }
}

impl Drop for BuildGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let cleanup = CleanupSlot {
            ownership: RuntimeSlotOwnership {
                version: self.ownership.version,
                instance_id: self.ownership.instance_id,
                backend: self.ownership.backend,
                backend_ownership: self.ownership.backend_ownership,
                storage_owned: self.ownership.storage_owned,
                phase: self.ownership.phase,
            },
            backend: self.backend.take(),
            run_dir: self.run_dir.clone(),
            authority: PoolCleanupAuthority::Build,
        };
        let mut state = match self.pool.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.building.remove(&cleanup.ownership.instance_id);
        state.quarantined.push_back(cleanup);
        drop(state);
        self.pool.wake.notify_one();
    }
}

struct CleanupGuard {
    pool: Arc<RuntimeWarmPool>,
    slot: Option<CleanupSlot>,
}

impl CleanupGuard {
    fn new(pool: Arc<RuntimeWarmPool>, slot: CleanupSlot) -> Self {
        Self {
            pool,
            slot: Some(slot),
        }
    }

    fn slot_mut(&mut self) -> Result<&mut CleanupSlot> {
        self.slot
            .as_mut()
            .ok_or_else(|| pool_error("runtime pool cleanup guard lost its slot"))
    }

    fn complete(mut self) {
        self.slot.take();
        let mut state = match self.pool.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.cleanup_pending = state.cleanup_pending.saturating_sub(1);
        state.consecutive_cleanup_failures = 0;
        drop(state);
        self.pool.wake.notify_one();
    }

    fn retry(mut self) {
        self.retain();
    }

    fn retain(&mut self) {
        let Some(slot) = self.slot.take() else {
            return;
        };
        let mut state = match self.pool.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.cleanup_pending = state.cleanup_pending.saturating_sub(1);
        state.quarantined.push_back(slot);
        drop(state);
        self.pool.wake.notify_one();
    }
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        self.retain();
    }
}

enum PoolAction {
    Build(u64),
    Cleanup(CleanupSlot),
    Idle,
}

enum Maintenance {
    Progress,
    Idle,
    RetryAfter(Duration),
}

fn build_retry_delay(consecutive_failures: u32) -> Duration {
    let exponent = consecutive_failures.saturating_sub(1).min(9);
    RETRY_BASE_DELAY
        .saturating_mul(1_u32 << exponent)
        .min(RETRY_MAX_DELAY)
}

fn strongest_backend_ownership(
    left: BackendOwnership,
    right: BackendOwnership,
) -> BackendOwnership {
    use BackendOwnership::{NotStarted, Running, Starting, Stopped, Unknown};

    if matches!(left, Unknown) || matches!(right, Unknown) {
        Unknown
    } else if matches!(left, Running) || matches!(right, Running) {
        Running
    } else if matches!(left, Starting) || matches!(right, Starting) {
        Starting
    } else if matches!(left, Stopped) || matches!(right, Stopped) {
        Stopped
    } else {
        NotStarted
    }
}

fn pool_error(message: impl Into<String>) -> BlazeError {
    BlazeError::StorageError {
        msg: message.into(),
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RuntimePoolStatus {
    pub(crate) ready: usize,
    pub(crate) building: usize,
    pub(crate) leased: usize,
    pub(crate) quarantined: usize,
    pub(crate) unresolved: usize,
    pub(crate) cleanup_pending: usize,
    pub(crate) capacity: usize,
    pub(crate) deficit: usize,
}

#[cfg(test)]
impl RuntimePoolStatus {
    fn from_state(capacity: usize, state: &PoolState) -> Self {
        Self {
            ready: state.ready.len(),
            building: state.building.len(),
            leased: state.leased.len(),
            quarantined: state.quarantined.len(),
            unresolved: state.unresolved.len(),
            cleanup_pending: state.cleanup_pending,
            capacity,
            deficit: capacity.saturating_sub(state.physical_count()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use async_trait::async_trait;
    use blaze_core::storage::{PoolStatus, StorageAcquireError};

    use crate::spawner::{
        BackendInstance, BackendSpawner, SpawnFailure, SpawnResult, SpawnerRegistry,
    };

    use super::*;

    struct RecordingStorage {
        root: PathBuf,
        acquire_count: AtomicUsize,
        release_started: AtomicUsize,
        release_completed: AtomicUsize,
        release_delay: Duration,
        fail_with_residual_once: AtomicBool,
        pending_after_ownership_once: AtomicBool,
        owned: Mutex<BTreeSet<String>>,
    }

    struct RetainedBackend {
        kill_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl BackendInstance for RetainedBackend {
        fn backend(&self) -> BackendKind {
            BackendKind::Mock
        }

        async fn try_wait(&self) -> Result<Option<SpawnResult>> {
            Ok(None)
        }

        async fn kill(&self) -> Result<()> {
            self.kill_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct ResidualSpawner {
        kill_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl BackendSpawner for ResidualSpawner {
        async fn spawn(
            &self,
            _request: SpawnRequest,
        ) -> std::result::Result<DynBackendInstance, SpawnFailure> {
            Err(SpawnFailure::with_owner(
                pool_error("test spawn retained backend owner"),
                Arc::new(RetainedBackend {
                    kill_count: self.kill_count.clone(),
                }),
            ))
        }

        async fn probe(&self, _binary_path: &Path) -> Result<bool> {
            Ok(true)
        }

        async fn cleanup_orphan(&self, _instance_id: Uuid, _run_dir: &Path) -> Result<()> {
            Err(pool_error(
                "test retained backend must be cleaned through its handle",
            ))
        }
    }

    impl RecordingStorage {
        fn new(root: PathBuf) -> Self {
            Self {
                root,
                acquire_count: AtomicUsize::new(0),
                release_started: AtomicUsize::new(0),
                release_completed: AtomicUsize::new(0),
                release_delay: Duration::ZERO,
                fail_with_residual_once: AtomicBool::new(false),
                pending_after_ownership_once: AtomicBool::new(false),
                owned: Mutex::new(BTreeSet::new()),
            }
        }

        fn with_release_delay(root: PathBuf, release_delay: Duration) -> Self {
            Self {
                release_delay,
                ..Self::new(root)
            }
        }

        fn with_residual_failure(root: PathBuf) -> Self {
            Self {
                fail_with_residual_once: AtomicBool::new(true),
                ..Self::new(root)
            }
        }

        fn with_pending_acquire(root: PathBuf) -> Self {
            Self {
                pending_after_ownership_once: AtomicBool::new(true),
                ..Self::new(root)
            }
        }

        fn slot(&self, instance_id: &str) -> StorageSlot {
            let instance_dir = self.root.join(instance_id);
            StorageSlot {
                id: instance_id.to_string(),
                rootfs_path: instance_dir.join("rootfs"),
                mem_path: instance_dir.join("memory"),
                mem_diff_path: instance_dir.join("memory.diff"),
                rootfs_diff_path: instance_dir.join("rootfs.diff"),
                instance_dir,
            }
        }
    }

    #[async_trait]
    impl StorageProvider for RecordingStorage {
        async fn probe(&self) -> Result<bool> {
            Ok(true)
        }

        async fn acquire(
            &self,
            opts: &AcquireOpts,
        ) -> std::result::Result<StorageSlot, StorageAcquireError> {
            self.acquire_count.fetch_add(1, Ordering::SeqCst);
            let slot = self.slot(&opts.instance_id);
            tokio::fs::create_dir_all(&slot.instance_dir)
                .await
                .map_err(BlazeError::from)?;
            self.owned
                .lock()
                .map_err(|_| StorageAcquireError::clean(pool_error("test storage lock poisoned")))?
                .insert(opts.instance_id.clone());
            if self
                .pending_after_ownership_once
                .swap(false, Ordering::SeqCst)
            {
                std::future::pending::<()>().await;
            }
            if self.fail_with_residual_once.swap(false, Ordering::SeqCst) {
                return Err(StorageAcquireError::with_residual(
                    pool_error("test acquire retained residual storage"),
                    slot,
                ));
            }
            Ok(slot)
        }

        async fn release(&self, slot: StorageSlot) -> Result<()> {
            self.release_by_id(&slot.id).await
        }

        async fn release_by_id(&self, instance_id: &str) -> Result<()> {
            self.release_started.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.release_delay).await;
            self.owned
                .lock()
                .map_err(|_| pool_error("test storage lock poisoned"))?
                .remove(instance_id);
            self.release_completed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn supports_runtime_pool_recovery(&self) -> bool {
            true
        }

        async fn list_owned_ids(&self) -> Result<Vec<String>> {
            Ok(self
                .owned
                .lock()
                .map_err(|_| pool_error("test storage lock poisoned"))?
                .iter()
                .cloned()
                .collect())
        }

        async fn reconstruct(&self, instance_id: &str) -> Result<StorageSlot> {
            Ok(self.slot(instance_id))
        }

        async fn flush_dirty(&self, _slot: &StorageSlot) -> Result<()> {
            Ok(())
        }

        fn pool_status(&self) -> PoolStatus {
            PoolStatus::default()
        }

        async fn drain_pool(&self) -> Result<usize> {
            Ok(0)
        }
    }

    fn prototype(warm_ttl: Duration) -> PoolPrototype {
        PoolPrototype {
            image_digest: "sha256:test".to_string(),
            policy_name: "test-policy".to_string(),
            workload_class: WorkloadClass::AgentTool,
            templates: Vec::new(),
            kernel_hooks: Vec::new(),
            binary_path: PathBuf::from("/unused"),
            runtime_backend: BackendKind::Mock,
            backend: BackendConfigs::default(),
            vm: None,
            warm_ttl,
        }
    }

    fn make_pool(
        target: usize,
        runtime_root: &Path,
        storage: Arc<RecordingStorage>,
        cancellation: CancellationToken,
    ) -> Arc<RuntimeWarmPool> {
        std::fs::create_dir_all(runtime_root).expect("runtime root");
        RuntimeWarmPool::new(
            target,
            false,
            1024,
            1024,
            runtime_root.to_path_buf(),
            storage,
            Arc::new(SpawnerRegistry::new()),
            Duration::from_secs(60),
            Duration::from_secs(3600),
            cancellation,
        )
        .expect("runtime pool")
    }

    fn slot(storage: &RecordingStorage, runtime_root: &Path, age: Duration) -> RuntimePoolSlot {
        let instance_id = Uuid::new_v4();
        RuntimePoolSlot {
            instance_id,
            storage: storage.slot(&instance_id.to_string()),
            backend: None,
            run_dir: runtime_root.join(instance_id.to_string()),
            runtime_backend: BackendKind::Mock,
            backend_ownership: BackendOwnership::NotStarted,
            ready_at: Instant::now()
                .checked_sub(age)
                .expect("slot age must fit in test clock"),
        }
    }

    async fn persist_ready_slot(slot: &RuntimePoolSlot) {
        std::fs::create_dir_all(&slot.run_dir).expect("slot run directory");
        let mut ownership = RuntimeSlotOwnership::new(slot.instance_id, slot.runtime_backend);
        ownership.storage_owned = true;
        ownership.backend_ownership = slot.backend_ownership;
        ownership.phase = RuntimeSlotPhase::Ready;
        write_ownership(&slot.run_dir, &ownership)
            .await
            .expect("ready ownership");
    }

    async fn wait_for_ready(pool: &RuntimeWarmPool) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if pool.status().ready != 0 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("runtime slot becomes ready");
    }

    #[tokio::test]
    async fn zero_capacity_never_starts_worker_or_storage() {
        let temp = tempfile::tempdir().expect("tempdir");
        let storage = Arc::new(RecordingStorage::new(temp.path().join("storage")));
        let pool = make_pool(
            0,
            &temp.path().join("runtime"),
            storage.clone(),
            CancellationToken::new(),
        );

        assert!(
            !pool
                .configure(prototype(Duration::from_secs(60)))
                .expect("configure disabled pool")
        );
        tokio::task::yield_now().await;

        assert_eq!(storage.acquire_count.load(Ordering::SeqCst), 0);
        assert!(pool.worker.lock().expect("worker lock").is_none());
        assert_eq!(
            pool.status(),
            RuntimePoolStatus {
                ready: 0,
                building: 0,
                leased: 0,
                quarantined: 0,
                unresolved: 0,
                cleanup_pending: 0,
                capacity: 0,
                deficit: 0,
            }
        );
    }

    #[tokio::test]
    async fn non_prefork_build_persists_ready_slot_before_claim() {
        let temp = tempfile::tempdir().expect("tempdir");
        let storage = Arc::new(RecordingStorage::new(temp.path().join("storage")));
        let pool = make_pool(
            1,
            &temp.path().join("runtime"),
            storage.clone(),
            CancellationToken::new(),
        );

        assert!(
            pool.configure(prototype(Duration::from_secs(60)))
                .expect("configure pool")
        );
        wait_for_ready(&pool).await;

        let (instance_id, run_dir) = {
            let state = pool.state.lock().expect("pool state");
            let slot = state.ready.front().expect("ready slot");
            (slot.instance_id, slot.run_dir.clone())
        };
        let ownership = read_ownership(&run_dir, instance_id)
            .await
            .expect("read ready ownership");
        assert_eq!(ownership.phase, RuntimeSlotPhase::Ready);
        assert!(ownership.storage_owned);
        assert_eq!(ownership.backend_ownership, BackendOwnership::NotStarted);

        let lease = pool
            .acquire()
            .await
            .expect("claim ready slot")
            .expect("available ready slot");
        assert_eq!(lease.slot().expect("leased slot").instance_id, instance_id);
        assert_eq!(pool.status().leased, 1);

        pool.begin_shutdown();
        drop(lease);
        pool.shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("shutdown pool");
        assert_eq!(storage.acquire_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cleanup_does_not_overwrite_a_transferred_owner() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        let storage = Arc::new(RecordingStorage::new(temp.path().join("storage")));
        let pool = make_pool(1, &runtime_root, storage.clone(), CancellationToken::new());
        let runtime = slot(&storage, &runtime_root, Duration::ZERO);
        persist_ready_slot(&runtime).await;
        storage
            .owned
            .lock()
            .expect("owned storage")
            .insert(runtime.instance_id.to_string());
        let owner_token = Uuid::new_v4();
        let mut transferred = read_ownership(&runtime.run_dir, runtime.instance_id)
            .await
            .expect("ready ownership");
        transferred.phase = RuntimeSlotPhase::LifecycleOwned { token: owner_token };
        write_ownership(&runtime.run_dir, &transferred)
            .await
            .expect("transferred ownership");
        let mut cleanup = CleanupSlot::from_runtime(runtime);

        let error = pool
            .cleanup_slot(&mut cleanup)
            .await
            .expect_err("stale pool owner must not clean lifecycle resources");

        assert!(error.to_string().contains("refusing pool cleanup"));
        assert_eq!(
            read_ownership(&cleanup.run_dir, cleanup.ownership.instance_id)
                .await
                .expect("ownership remains")
                .phase,
            RuntimeSlotPhase::LifecycleOwned { token: owner_token }
        );
        assert_eq!(storage.release_started.load(Ordering::SeqCst), 0);
        assert!(
            storage
                .owned
                .lock()
                .expect("owned storage")
                .contains(&cleanup.ownership.instance_id.to_string())
        );
    }

    #[tokio::test]
    async fn matching_handoff_can_return_to_pool_cleanup() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        let storage = Arc::new(RecordingStorage::new(temp.path().join("storage")));
        let pool = make_pool(1, &runtime_root, storage.clone(), CancellationToken::new());
        let runtime = slot(&storage, &runtime_root, Duration::ZERO);
        let instance_id = runtime.instance_id;
        persist_ready_slot(&runtime).await;
        storage
            .owned
            .lock()
            .expect("owned storage")
            .insert(instance_id.to_string());
        pool.state
            .lock()
            .expect("pool state")
            .ready
            .push_back(runtime);
        let mut lease = pool
            .acquire()
            .await
            .expect("acquire slot")
            .expect("ready slot");
        let owner_token = Uuid::new_v4();
        lease
            .begin_handoff(owner_token)
            .await
            .expect("begin handoff");

        drop(lease);
        let mut cleanup = pool
            .state
            .lock()
            .expect("pool state")
            .quarantined
            .pop_front()
            .expect("abandoned handoff");
        pool.cleanup_slot(&mut cleanup)
            .await
            .expect("matching handoff cleanup");

        assert_eq!(storage.release_completed.load(Ordering::SeqCst), 1);
        assert!(
            !storage
                .owned
                .lock()
                .expect("owned storage")
                .contains(&instance_id.to_string())
        );
        assert!(!runtime_root.join(instance_id.to_string()).exists());
    }

    #[tokio::test]
    async fn cleanup_retry_recommits_intent_before_release() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        let storage = Arc::new(RecordingStorage::with_release_delay(
            temp.path().join("storage"),
            Duration::from_secs(1),
        ));
        let pool = make_pool(1, &runtime_root, storage.clone(), CancellationToken::new());
        let runtime = slot(&storage, &runtime_root, Duration::ZERO);
        persist_ready_slot(&runtime).await;
        storage
            .owned
            .lock()
            .expect("owned storage")
            .insert(runtime.instance_id.to_string());
        let mut cleanup = CleanupSlot::from_runtime(runtime);
        cleanup.ownership.phase = RuntimeSlotPhase::PoolCleanup;
        let instance_id = cleanup.ownership.instance_id;
        let run_dir = cleanup.run_dir.clone();

        let cleanup_pool = pool.clone();
        let cleanup_task =
            tokio::spawn(async move { cleanup_pool.cleanup_slot(&mut cleanup).await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while storage.release_started.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("release starts after cleanup intent is durable");
        cleanup_task.abort();
        assert!(
            cleanup_task
                .await
                .expect_err("cleanup is cancelled during release")
                .is_cancelled()
        );

        assert_eq!(storage.release_started.load(Ordering::SeqCst), 1);
        assert_eq!(storage.release_completed.load(Ordering::SeqCst), 0);
        assert_eq!(
            read_ownership(&run_dir, instance_id)
                .await
                .expect("cleanup journal")
                .phase,
            RuntimeSlotPhase::PoolCleanup
        );
    }

    #[tokio::test]
    async fn shutdown_reports_a_panicked_worker() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        let storage = Arc::new(RecordingStorage::new(temp.path().join("storage")));
        let pool = make_pool(1, &runtime_root, storage, CancellationToken::new());
        *pool.worker.lock().expect("worker lock") = Some(tokio::spawn(async {
            panic!("injected runtime pool worker panic");
        }));

        let error = pool
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect_err("worker panic must fail shutdown");

        assert!(
            error
                .to_string()
                .contains("runtime pool worker failed while stopping")
        );
    }

    #[tokio::test]
    async fn every_owned_state_consumes_physical_capacity() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        let storage = Arc::new(RecordingStorage::new(temp.path().join("storage")));
        let pool = make_pool(5, &runtime_root, storage.clone(), CancellationToken::new());
        let ready = slot(&storage, &runtime_root, Duration::ZERO);
        let quarantined = slot(&storage, &runtime_root, Duration::ZERO);
        persist_ready_slot(&quarantined).await;
        storage
            .owned
            .lock()
            .expect("owned storage")
            .insert(quarantined.instance_id.to_string());
        {
            let mut state = pool.state.lock().expect("pool state");
            state.prototype = Some(prototype(Duration::from_secs(60)));
            state.ready.push_back(ready);
            state.building.insert(Uuid::new_v4());
            state.leased.insert(Uuid::new_v4());
            state
                .quarantined
                .push_back(CleanupSlot::from_runtime(quarantined));
            state.cleanup_pending = 1;
        }

        let status = pool.status();
        assert_eq!(status.ready, 1);
        assert_eq!(status.building, 1);
        assert_eq!(status.leased, 1);
        assert_eq!(status.quarantined, 1);
        assert_eq!(status.cleanup_pending, 1);
        assert_eq!(status.capacity, 5);
        assert_eq!(status.deficit, 0);
        assert_eq!(storage.acquire_count.load(Ordering::SeqCst), 0);
        assert!(matches!(
            pool.maintain_once().await.expect("maintain full pool"),
            Maintenance::Progress
        ));
        assert_eq!(storage.acquire_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn worker_start_rechecks_shutdown_state() {
        let temp = tempfile::tempdir().expect("tempdir");
        let storage = Arc::new(RecordingStorage::new(temp.path().join("storage")));
        let pool = make_pool(
            1,
            &temp.path().join("runtime"),
            storage,
            CancellationToken::new(),
        );

        pool.begin_shutdown();
        let error = pool
            .ensure_worker()
            .expect_err("worker must not start after shutdown wins the race");

        assert!(error.to_string().contains("shutting down"));
        assert!(!pool.has_tracked_worker());
    }

    #[tokio::test]
    async fn expired_shutdown_wait_still_stops_new_work() {
        let temp = tempfile::tempdir().expect("tempdir");
        let storage = Arc::new(RecordingStorage::new(temp.path().join("storage")));
        let pool = make_pool(
            1,
            &temp.path().join("runtime"),
            storage,
            CancellationToken::new(),
        );
        let _held_shutdown = pool.shutdown.lock().await;

        let error = pool
            .shutdown_until(Instant::now())
            .await
            .expect_err("expired coordination wait must fail");

        assert!(error.to_string().contains("coordination"));
        assert!(pool.cancellation.is_cancelled());
        assert!(pool.state.lock().expect("pool state").shutting_down);
        assert!(
            pool.ensure_worker().is_err(),
            "expired shutdown still prevents later worker creation"
        );
    }

    #[tokio::test]
    async fn expired_slot_is_quarantined_instead_of_claimed() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        let storage = Arc::new(RecordingStorage::new(temp.path().join("storage")));
        let pool = make_pool(1, &runtime_root, storage.clone(), CancellationToken::new());
        {
            let mut state = pool.state.lock().expect("pool state");
            state
                .ready
                .push_back(slot(&storage, &runtime_root, Duration::from_secs(61)));
        }

        assert!(pool.acquire().await.expect("claim expired slot").is_none());
        let status = pool.status();
        assert_eq!(status.ready, 0);
        assert_eq!(status.leased, 0);
        assert_eq!(status.quarantined, 1);
        assert_eq!(status.deficit, 0);
    }

    #[tokio::test]
    async fn residual_acquire_failure_remains_owned_until_cleanup() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        let storage = Arc::new(RecordingStorage::with_residual_failure(
            temp.path().join("storage"),
        ));
        let pool = make_pool(1, &runtime_root, storage.clone(), CancellationToken::new());
        pool.state.lock().expect("pool state").prototype = Some(prototype(Duration::from_secs(60)));

        pool.build_slot(0)
            .await
            .expect_err("acquire reports residual owner");

        let status = pool.status();
        assert_eq!(status.building, 0);
        assert_eq!(status.quarantined, 1);
        assert_eq!(status.deficit, 0);
        assert_eq!(storage.owned.lock().expect("owned storage").len(), 1);

        assert!(matches!(
            pool.maintain_once().await.expect("cleanup residual"),
            Maintenance::Progress
        ));
        assert!(storage.owned.lock().expect("owned storage").is_empty());
        assert_eq!(pool.status().quarantined, 0);
    }

    #[tokio::test]
    async fn shutdown_cleans_storage_from_a_cancelled_acquire() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        let storage = Arc::new(RecordingStorage::with_pending_acquire(
            temp.path().join("storage"),
        ));
        let pool = make_pool(1, &runtime_root, storage.clone(), CancellationToken::new());
        pool.configure(prototype(Duration::from_secs(60)))
            .expect("configure pool");
        let instance_id = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(instance_id) = storage
                    .owned
                    .lock()
                    .expect("owned storage")
                    .iter()
                    .next()
                    .cloned()
                {
                    return Uuid::parse_str(&instance_id).expect("owned UUID");
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("provider creates an owner before acquire returns");
        let ownership = read_ownership(&runtime_root.join(instance_id.to_string()), instance_id)
            .await
            .expect("conservative storage journal");
        assert!(ownership.storage_owned);
        assert_eq!(ownership.phase, RuntimeSlotPhase::Building);

        pool.shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("shutdown cancels acquire and cleans the owner");

        assert_eq!(storage.release_completed.load(Ordering::SeqCst), 1);
        assert!(storage.owned.lock().expect("owned storage").is_empty());
        assert_eq!(pool.status().building, 0);
        assert_eq!(pool.status().quarantined, 0);
        assert!(!runtime_root.join(instance_id.to_string()).exists());
    }

    #[tokio::test]
    async fn residual_prefork_failure_remains_owned_until_cleanup() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        std::fs::create_dir_all(&runtime_root).expect("runtime root");
        let storage = Arc::new(RecordingStorage::new(temp.path().join("storage")));
        let kill_count = Arc::new(AtomicUsize::new(0));
        let mut registry = SpawnerRegistry::new();
        registry.insert(
            BackendKind::Mock,
            Arc::new(ResidualSpawner {
                kill_count: kill_count.clone(),
            }),
        );
        let pool = RuntimeWarmPool::new(
            1,
            true,
            1024,
            1024,
            runtime_root,
            storage.clone(),
            Arc::new(registry),
            Duration::from_secs(60),
            Duration::from_secs(3600),
            CancellationToken::new(),
        )
        .expect("runtime pool");
        pool.state.lock().expect("pool state").prototype = Some(prototype(Duration::from_secs(60)));

        pool.build_slot(0)
            .await
            .expect_err("prefork spawn reports residual owner");

        let status = pool.status();
        assert_eq!(status.building, 0);
        assert_eq!(status.quarantined, 1);
        assert_eq!(status.deficit, 0);
        assert_eq!(kill_count.load(Ordering::SeqCst), 0);

        assert!(matches!(
            pool.maintain_once().await.expect("cleanup residual"),
            Maintenance::Progress
        ));
        assert_eq!(kill_count.load(Ordering::SeqCst), 1);
        assert!(storage.owned.lock().expect("owned storage").is_empty());
        assert_eq!(pool.status().quarantined, 0);
    }

    #[tokio::test]
    async fn unresolved_handoff_counts_capacity_and_blocks_shutdown() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        let storage = Arc::new(RecordingStorage::new(temp.path().join("storage")));
        let pool = make_pool(1, &runtime_root, storage.clone(), CancellationToken::new());
        let ready = slot(&storage, &runtime_root, Duration::ZERO);
        let instance_id = ready.instance_id;
        persist_ready_slot(&ready).await;
        storage
            .owned
            .lock()
            .expect("owned storage")
            .insert(instance_id.to_string());
        {
            let mut state = pool.state.lock().expect("pool state");
            state.prototype = Some(prototype(Duration::from_secs(60)));
            state.ready.push_back(ready);
        }

        let mut lease = pool
            .acquire()
            .await
            .expect("acquire slot")
            .expect("ready slot");
        let token = Uuid::new_v4();
        lease.begin_handoff(token).await.expect("begin handoff");
        lease
            .retain_unresolved(token, "test publication is ambiguous".to_string())
            .expect("retain ambiguous handoff");

        let status = pool.status();
        assert_eq!(status.leased, 0);
        assert_eq!(status.unresolved, 1);
        assert_eq!(status.deficit, 0);
        assert!(matches!(
            pool.maintain_once().await.expect("maintain bounded pool"),
            Maintenance::Idle
        ));
        assert_eq!(storage.acquire_count.load(Ordering::SeqCst), 0);
        let ownership = read_ownership(&runtime_root.join(instance_id.to_string()), instance_id)
            .await
            .expect("retained handoff journal");
        assert_eq!(ownership.phase, RuntimeSlotPhase::Handoff { token });

        let error = pool
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect_err("unresolved owner must be reported");
        assert!(error.to_string().contains(&instance_id.to_string()));
        assert!(
            error
                .to_string()
                .contains("unresolved lifecycle publication")
        );
        assert!(error.to_string().contains("test publication is ambiguous"));
        assert_eq!(pool.status().unresolved, 1);
        assert!(
            storage
                .owned
                .lock()
                .expect("owned storage")
                .contains(&instance_id.to_string())
        );
    }

    #[tokio::test]
    async fn cancelled_shutdown_retains_active_and_unstarted_cleanup() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        let storage = Arc::new(RecordingStorage::with_release_delay(
            temp.path().join("storage"),
            Duration::from_millis(500),
        ));
        let pool = make_pool(2, &runtime_root, storage.clone(), CancellationToken::new());
        let ready = slot(&storage, &runtime_root, Duration::ZERO);
        let quarantined = slot(&storage, &runtime_root, Duration::ZERO);
        let expected = BTreeSet::from([ready.instance_id, quarantined.instance_id]);
        for runtime in [&ready, &quarantined] {
            persist_ready_slot(runtime).await;
            storage
                .owned
                .lock()
                .expect("owned storage")
                .insert(runtime.instance_id.to_string());
        }
        {
            let mut state = pool.state.lock().expect("pool state");
            state.ready.push_back(ready);
            state
                .quarantined
                .push_back(CleanupSlot::from_runtime(quarantined));
        }

        let shutdown_pool = pool.clone();
        let shutdown = tokio::spawn(async move {
            shutdown_pool
                .shutdown_until(Instant::now() + Duration::from_secs(30))
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while storage.release_started.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("shutdown starts one cleanup");

        shutdown.abort();
        assert!(
            shutdown
                .await
                .expect_err("shutdown task is cancelled")
                .is_cancelled()
        );

        let status = pool.status();
        assert_eq!(status.ready, 1);
        assert_eq!(status.quarantined, 1);
        assert_eq!(status.cleanup_pending, 0);
        assert_eq!(status.deficit, 0);
        assert_eq!(storage.release_completed.load(Ordering::SeqCst), 0);
        assert_eq!(storage.owned.lock().expect("owned storage").len(), 2);
        let retained = {
            let state = pool.state.lock().expect("pool state");
            state
                .ready
                .iter()
                .map(|slot| slot.instance_id)
                .chain(
                    state
                        .quarantined
                        .iter()
                        .map(|slot| slot.ownership.instance_id),
                )
                .collect::<BTreeSet<_>>()
        };
        assert_eq!(retained, expected);

        pool.shutdown_until(Instant::now() + Duration::from_secs(2))
            .await
            .expect("retry retained shutdown cleanup");
        let status = pool.status();
        assert_eq!(status.ready, 0);
        assert_eq!(status.quarantined, 0);
        assert_eq!(status.cleanup_pending, 0);
        assert!(storage.owned.lock().expect("owned storage").is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_joins_worker_and_shares_one_deadline() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        let storage = Arc::new(RecordingStorage::with_release_delay(
            temp.path().join("storage"),
            Duration::from_millis(40),
        ));
        let cancellation = CancellationToken::new();
        let pool = make_pool(2, &runtime_root, storage.clone(), cancellation.clone());
        for _ in 0..2 {
            let ready = slot(&storage, &runtime_root, Duration::ZERO);
            persist_ready_slot(&ready).await;
            storage
                .owned
                .lock()
                .expect("owned storage")
                .insert(ready.instance_id.to_string());
            pool.state
                .lock()
                .expect("pool state")
                .ready
                .push_back(ready);
        }

        let worker_joined = Arc::new(AtomicBool::new(false));
        let joined = worker_joined.clone();
        *pool.worker.lock().expect("worker lock") = Some(tokio::spawn(async move {
            cancellation.cancelled().await;
            joined.store(true, Ordering::SeqCst);
        }));

        let start = Instant::now();
        let deadline = start + Duration::from_millis(60);
        let error = pool
            .shutdown_until(deadline)
            .await
            .expect_err("second cleanup must share the first deadline");

        assert!(error.to_string().contains("shared shutdown deadline"));
        assert_eq!(Instant::now(), deadline);
        assert!(worker_joined.load(Ordering::SeqCst));
        assert!(pool.worker.lock().expect("worker lock").is_none());
        assert_eq!(storage.release_started.load(Ordering::SeqCst), 2);
        assert_eq!(storage.release_completed.load(Ordering::SeqCst), 1);
        assert_eq!(pool.status().quarantined, 1);
    }

    #[tokio::test]
    async fn aborted_worker_retains_its_active_cleanup_owner() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        let storage = Arc::new(RecordingStorage::with_release_delay(
            temp.path().join("storage"),
            Duration::from_secs(1),
        ));
        let pool = make_pool(1, &runtime_root, storage.clone(), CancellationToken::new());
        let ready = slot(&storage, &runtime_root, Duration::ZERO);
        persist_ready_slot(&ready).await;
        storage
            .owned
            .lock()
            .expect("owned storage")
            .insert(ready.instance_id.to_string());
        {
            let mut state = pool.state.lock().expect("pool state");
            state.prototype = Some(prototype(Duration::from_secs(60)));
            state
                .quarantined
                .push_back(CleanupSlot::from_runtime(ready));
        }
        pool.ensure_worker().expect("start worker");
        pool.wake.notify_one();
        tokio::time::timeout(Duration::from_secs(1), async {
            while storage.release_started.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("worker starts cleanup");

        let error = pool
            .shutdown_until(Instant::now() + Duration::from_millis(10))
            .await
            .expect_err("active cleanup exceeds shutdown deadline");

        assert!(error.to_string().contains("shared shutdown deadline"));
        let status = pool.status();
        assert_eq!(status.cleanup_pending, 0);
        assert_eq!(status.quarantined, 1);
        assert_eq!(storage.release_completed.load(Ordering::SeqCst), 0);
        assert!(pool.worker.lock().expect("worker lock").is_none());
    }

    #[tokio::test]
    async fn cancelled_shutdown_retains_worker_for_a_joining_retry() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        let storage = Arc::new(RecordingStorage::with_release_delay(
            temp.path().join("storage"),
            Duration::from_secs(1),
        ));
        let pool = make_pool(1, &runtime_root, storage.clone(), CancellationToken::new());
        let ready = slot(&storage, &runtime_root, Duration::ZERO);
        persist_ready_slot(&ready).await;
        storage
            .owned
            .lock()
            .expect("owned storage")
            .insert(ready.instance_id.to_string());
        {
            let mut state = pool.state.lock().expect("pool state");
            state.prototype = Some(prototype(Duration::from_secs(60)));
            state
                .quarantined
                .push_back(CleanupSlot::from_runtime(ready));
        }
        pool.ensure_worker().expect("start worker");
        pool.wake.notify_one();
        tokio::time::timeout(Duration::from_secs(1), async {
            while storage.release_started.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("worker starts cleanup");

        let shutdown_pool = pool.clone();
        let shutdown = tokio::spawn(async move {
            shutdown_pool
                .shutdown_until(Instant::now() + Duration::from_secs(10))
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while pool.worker.lock().expect("worker lock").is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("shutdown takes worker handle");
        shutdown.abort();
        assert!(
            shutdown
                .await
                .expect_err("outer shutdown is cancelled")
                .is_cancelled()
        );

        assert_eq!(storage.release_completed.load(Ordering::SeqCst), 0);
        assert!(
            pool.worker.lock().expect("worker lock").is_some(),
            "cancelled shutdown must retain the aborted worker handle"
        );

        pool.shutdown_until(Instant::now() + Duration::from_secs(2))
            .await
            .expect("retry joins the worker and releases the retained owner");
        let status = pool.status();
        assert_eq!(status.quarantined, 0);
        assert_eq!(status.cleanup_pending, 0);
        assert!(pool.worker.lock().expect("worker lock").is_none());
        assert_eq!(storage.release_completed.load(Ordering::SeqCst), 1);
        assert!(storage.owned.lock().expect("owned storage").is_empty());
    }

    #[tokio::test]
    async fn concurrent_shutdown_waiter_joins_a_worker_retained_by_cancellation() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        let storage = Arc::new(RecordingStorage::with_release_delay(
            temp.path().join("storage"),
            Duration::from_secs(1),
        ));
        let pool = make_pool(1, &runtime_root, storage.clone(), CancellationToken::new());
        let ready = slot(&storage, &runtime_root, Duration::ZERO);
        persist_ready_slot(&ready).await;
        storage
            .owned
            .lock()
            .expect("owned storage")
            .insert(ready.instance_id.to_string());
        {
            let mut state = pool.state.lock().expect("pool state");
            state.prototype = Some(prototype(Duration::from_secs(60)));
            state
                .quarantined
                .push_back(CleanupSlot::from_runtime(ready));
        }
        pool.ensure_worker().expect("start worker");
        pool.wake.notify_one();
        tokio::time::timeout(Duration::from_secs(1), async {
            while storage.release_started.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("worker starts cleanup");

        let first_pool = pool.clone();
        let first = tokio::spawn(async move {
            first_pool
                .shutdown_until(Instant::now() + Duration::from_secs(10))
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while pool.has_tracked_worker() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first shutdown takes worker handle");
        let second_pool = pool.clone();
        let second = tokio::spawn(async move {
            second_pool
                .shutdown_until(Instant::now() + Duration::from_secs(3))
                .await
        });
        tokio::task::yield_now().await;

        first.abort();
        assert!(
            first
                .await
                .expect_err("first shutdown is cancelled")
                .is_cancelled()
        );
        tokio::time::timeout(Duration::from_secs(3), second)
            .await
            .expect("second shutdown finishes")
            .expect("second shutdown task")
            .expect("second shutdown joins and cleans the retained worker");

        assert!(!pool.has_tracked_worker());
        assert_eq!(pool.status().quarantined, 0);
        assert_eq!(pool.status().cleanup_pending, 0);
        assert_eq!(storage.release_completed.load(Ordering::SeqCst), 1);
        assert!(storage.owned.lock().expect("owned storage").is_empty());
    }
}
