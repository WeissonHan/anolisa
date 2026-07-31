// SPDX-License-Identifier: Apache-2.0
//! Periodic synchronization of provider-owned sandbox storage.

use std::sync::Arc;
use std::time::Duration;

use blaze_core::lifecycle::{BackendOwnership, SandboxState};
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[cfg(test)]
use tokio::sync::Notify;

use crate::error::{BlazeDaemonError, Result};

use super::manager::SandboxManager;

const FLUSH_LOOP_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Supervised periodic synchronization task.
pub(crate) struct FlushLoop {
    cancellation: CancellationToken,
    task: Option<JoinHandle<()>>,
    #[cfg(test)]
    started: Arc<Notify>,
}

impl FlushLoop {
    /// Wait for an early worker exit and report it as a daemon failure.
    pub(crate) async fn observe_exit(&mut self) -> Result<()> {
        self.join().await?;
        Err(BlazeDaemonError::Internal(
            "provider synchronization task exited unexpectedly".to_string(),
        ))
    }

    /// Request cooperative shutdown and join the worker before returning.
    pub(crate) async fn shutdown(&mut self) -> Result<()> {
        self.cancellation.cancel();
        let Some(task) = self.task.as_mut() else {
            return Ok(());
        };
        match tokio::time::timeout(FLUSH_LOOP_SHUTDOWN_TIMEOUT, task).await {
            Ok(result) => {
                self.task.take();
                result.map_err(join_error)
            }
            Err(_) => {
                let task = self.task.as_mut().expect("flush task is present");
                task.abort();
                let result = task.await;
                self.task.take();
                match result {
                    Err(error) if error.is_cancelled() => Err(BlazeDaemonError::Internal(
                        "provider synchronization task exceeded its shutdown deadline".to_string(),
                    )),
                    Err(error) => Err(join_error(error)),
                    Ok(()) => Err(BlazeDaemonError::Internal(
                        "provider synchronization task ignored cancellation".to_string(),
                    )),
                }
            }
        }
    }

    async fn join(&mut self) -> Result<()> {
        let Some(task) = self.task.as_mut() else {
            return Ok(());
        };
        let result = task.await;
        self.task.take();
        result.map_err(join_error)
    }

    #[cfg(test)]
    async fn wait_started(&self) {
        self.started.notified().await;
    }
}

impl Drop for FlushLoop {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(task) = self.task.as_ref() {
            task.abort();
        }
    }
}

fn join_error(error: tokio::task::JoinError) -> BlazeDaemonError {
    BlazeDaemonError::Internal(format!(
        "provider synchronization task join failed: {error}"
    ))
}

/// Counters emitted for one synchronization sweep.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FlushSummary {
    /// Running records selected before any provider await.
    pub(crate) selected: usize,
    /// Provider calls that completed successfully.
    pub(crate) flushed: usize,
    /// Records that stopped being Running before their operation lock won.
    pub(crate) skipped: usize,
    /// Invalid owners and provider failures isolated from the rest of the sweep.
    pub(crate) failed: usize,
}

enum FlushAttempt {
    Flushed,
    Skipped,
    Cancelled,
}

impl SandboxManager {
    /// Start a cancellable periodic storage synchronization worker.
    ///
    /// The first sweep starts after one complete interval. Missed ticks are
    /// skipped instead of being queued behind a slow sweep.
    pub(crate) fn start_flush_loop(
        self: &Arc<Self>,
        interval: Duration,
        attempt_timeout: Duration,
    ) -> FlushLoop {
        let manager = self.clone();
        let cancellation = CancellationToken::new();
        let worker_cancellation = cancellation.clone();
        #[cfg(test)]
        let started = Arc::new(Notify::new());
        #[cfg(test)]
        let worker_started = started.clone();
        tracing::info!(
            interval_secs = interval.as_secs_f64(),
            attempt_timeout_secs = attempt_timeout.as_secs_f64(),
            "starting provider synchronization task"
        );
        let task = tokio::spawn(async move {
            let first_tick = Instant::now() + interval;
            let mut ticker = tokio::time::interval_at(first_tick, interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            #[cfg(test)]
            worker_started.notify_one();
            loop {
                tokio::select! {
                    biased;
                    _ = worker_cancellation.cancelled() => break,
                    _ = ticker.tick() => {
                        let summary = manager
                            .flush_all_until(&worker_cancellation, attempt_timeout)
                            .await;
                        let Some(summary) = summary else {
                            break;
                        };
                        tracing::debug!(
                            selected = summary.selected,
                            flushed = summary.flushed,
                            skipped = summary.skipped,
                            failed = summary.failed,
                            "provider synchronization sweep completed"
                        );
                    }
                }
            }
            tracing::info!("provider synchronization task stopped");
        });
        FlushLoop {
            cancellation,
            task: Some(task),
            #[cfg(test)]
            started,
        }
    }

    #[cfg(test)]
    async fn flush_all(&self, attempt_timeout: Duration) -> FlushSummary {
        self.flush_all_until(&CancellationToken::new(), attempt_timeout)
            .await
            .expect("uncancelled sweep")
    }

    async fn flush_all_until(
        &self,
        cancellation: &CancellationToken,
        attempt_timeout: Duration,
    ) -> Option<FlushSummary> {
        let running_ids = match self.list() {
            Ok(instances) => instances
                .into_iter()
                .filter_map(|instance| {
                    (instance.state == SandboxState::Running).then_some(instance.id)
                })
                .collect::<Vec<_>>(),
            Err(error) => {
                tracing::error!(%error, "cannot select provider synchronization candidates");
                return Some(FlushSummary {
                    failed: 1,
                    ..FlushSummary::default()
                });
            }
        };
        let mut summary = FlushSummary {
            selected: running_ids.len(),
            ..FlushSummary::default()
        };
        for id in running_ids {
            match self
                .flush_if_running(id, cancellation, attempt_timeout)
                .await
            {
                Ok(FlushAttempt::Flushed) => summary.flushed += 1,
                Ok(FlushAttempt::Skipped) => summary.skipped += 1,
                Ok(FlushAttempt::Cancelled) => return None,
                Err(error) => {
                    summary.failed += 1;
                    tracing::warn!(
                        sandbox_id = %id,
                        %error,
                        "sandbox provider synchronization failed"
                    );
                }
            }
        }
        Some(summary)
    }

    async fn flush_if_running(
        &self,
        id: Uuid,
        cancellation: &CancellationToken,
        attempt_timeout: Duration,
    ) -> Result<FlushAttempt> {
        let operation_lock = self.operation_lock(id);
        let _operation = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Ok(FlushAttempt::Cancelled),
            operation = operation_lock.lock() => operation,
        };
        let instance = self.get(id)?;
        if instance.state != SandboxState::Running {
            return Ok(FlushAttempt::Skipped);
        }
        if let Some(operation) = instance.operation {
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "instance {id} is Running with unfinished {} operation",
                operation.kind
            )));
        }
        if instance.backend_ownership != BackendOwnership::Running {
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "instance {id} is Running with {} backend ownership",
                format!("{:?}", instance.backend_ownership).to_lowercase()
            )));
        }
        let backend = self.backend_owner(id).ok_or_else(|| {
            BlazeDaemonError::RecoveryRequired(format!(
                "instance {id} is Running without a backend owner"
            ))
        })?;
        match backend.try_wait().await? {
            None => {}
            Some(status) => {
                return Err(BlazeDaemonError::RecoveryRequired(format!(
                    "instance {id} backend already exited: {status:?}"
                )));
            }
        }
        let storage = self.reconstruct_storage(id).await.map_err(|error| {
            BlazeDaemonError::RecoveryRequired(format!(
                "instance {id} has no complete storage owner: {error}"
            ))
        })?;
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Ok(FlushAttempt::Cancelled),
            result = tokio::time::timeout(attempt_timeout, self.flush_storage(&storage)) => {
                match result {
                    Ok(result) => {
                        result?;
                        Ok(FlushAttempt::Flushed)
                    }
                    Err(_) => Err(BlazeDaemonError::Internal(format!(
                        "provider synchronization for {id} timed out after {:.3} seconds",
                        attempt_timeout.as_secs_f64()
                    ))),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use blaze_core::backend::{BackendKind, SpawnRequest};
    use blaze_core::config::RuntimeTemplateSection;
    use blaze_core::error::{BlazeError, Result as CoreResult};
    use blaze_core::lifecycle::{
        BackendOwnership, OperationKind, SandboxInstance, SandboxState, StartPath,
    };
    use blaze_core::policy::{BackendConfigs, WorkloadClass};
    use blaze_core::pool::PoolManager;
    use blaze_core::storage::{
        AcquireOpts, PoolStatus, StorageAcquireError, StorageProvider, StorageSlot,
    };
    use tokio::sync::Notify;

    use crate::file_provider::FileStorageProvider;
    use crate::sandbox::manager::{SandboxManagerInit, SandboxManagerResources};
    use crate::sandbox::template::RuntimeTemplateCatalog;
    use crate::spawner::{BackendSpawner, MockSpawner, SpawnerRegistry};

    use super::*;

    struct RecordingStorage {
        inner: FileStorageProvider,
        instances: PathBuf,
        calls: Mutex<Vec<String>>,
        call_recorded: Notify,
        failures: Mutex<HashSet<String>>,
        block_next: AtomicBool,
        started: Notify,
    }

    impl RecordingStorage {
        fn new(images: PathBuf, instances: PathBuf) -> Self {
            Self {
                inner: FileStorageProvider::with_images(images, instances.clone()),
                instances,
                calls: Mutex::new(Vec::new()),
                call_recorded: Notify::new(),
                failures: Mutex::new(HashSet::new()),
                block_next: AtomicBool::new(false),
                started: Notify::new(),
            }
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().expect("calls").clone()
        }

        async fn wait_for_calls(&self, expected: usize) {
            loop {
                let call_recorded = self.call_recorded.notified();
                if self.calls.lock().expect("calls").len() >= expected {
                    return;
                }
                call_recorded.await;
            }
        }

        fn fail(&self, id: Uuid) {
            self.failures
                .lock()
                .expect("failures")
                .insert(id.to_string());
        }

        fn block_once(&self) {
            self.block_next.store(true, Ordering::Release);
        }
    }

    #[async_trait]
    impl StorageProvider for RecordingStorage {
        async fn probe(&self) -> CoreResult<bool> {
            self.inner.probe().await
        }

        async fn acquire(
            &self,
            opts: &AcquireOpts,
        ) -> std::result::Result<StorageSlot, StorageAcquireError> {
            self.inner.acquire(opts).await
        }

        async fn release(&self, slot: StorageSlot) -> CoreResult<()> {
            self.inner.release(slot).await
        }

        async fn release_by_id(&self, instance_id: &str) -> CoreResult<()> {
            self.inner.release_by_id(instance_id).await
        }

        async fn reconstruct(&self, instance_id: &str) -> CoreResult<StorageSlot> {
            self.inner.reconstruct(instance_id).await
        }

        async fn flush_dirty(&self, slot: &StorageSlot) -> CoreResult<()> {
            self.calls.lock().expect("calls").push(slot.id.clone());
            self.call_recorded.notify_waiters();
            if self.block_next.swap(false, Ordering::AcqRel) {
                self.started.notify_one();
                std::future::pending::<()>().await;
            }
            if self.failures.lock().expect("failures").contains(&slot.id) {
                return Err(BlazeError::StorageError {
                    msg: format!("injected provider synchronization failure for {}", slot.id),
                });
            }
            Ok(())
        }

        fn pool_status(&self) -> PoolStatus {
            self.inner.pool_status()
        }

        async fn drain_pool(&self) -> CoreResult<usize> {
            self.inner.drain_pool().await
        }
    }

    fn manager(
        temp: &Path,
        storage: Arc<RecordingStorage>,
    ) -> (Arc<SandboxManager>, SandboxManagerResources) {
        let state_dir = temp.join("state");
        let images = temp.join("images");
        let instances = temp.join("instances");
        for directory in [&state_dir, &images, &instances] {
            std::fs::create_dir_all(directory).expect("test directory");
        }
        let mut spawners = SpawnerRegistry::new();
        spawners.insert(BackendKind::Mock, Arc::new(MockSpawner));
        let runtime_templates = RuntimeTemplateCatalog::open(&RuntimeTemplateSection {
            dir: temp.join("runtime-templates"),
            ..RuntimeTemplateSection::default()
        })
        .expect("runtime template catalog");
        let (manager, resources) = SandboxManager::new(SandboxManagerInit {
            instances: HashMap::new(),
            pool: PoolManager::new(),
            spawners,
            active_backend: BackendKind::Mock,
            storage,
            state_dir,
            rootfs_size: 64,
            mem_size: 32,
            pool_size: 0,
            prefork: false,
            default_warm_ttl: "30m".to_string(),
            gc_interval: "5m".to_string(),
            runtime_templates,
        })
        .expect("manager");
        (Arc::new(manager), resources)
    }

    async fn insert_running(
        manager: &SandboxManager,
        resources: &SandboxManagerResources,
        storage: &RecordingStorage,
        id: Uuid,
        acquire_storage: bool,
        insert_backend: bool,
        active_operation: bool,
    ) {
        let slot = if acquire_storage {
            Some(
                storage
                    .acquire(&AcquireOpts {
                        instance_id: id.to_string(),
                        rootfs_size: 64,
                        mem_size: 32,
                    })
                    .await
                    .expect("slot"),
            )
        } else {
            None
        };
        let mut metadata = SandboxInstance::new(
            BackendKind::Mock,
            WorkloadClass::AgentTool,
            "sha256:flush-test".into(),
            StartPath::Cold,
            "flush-test".into(),
        );
        metadata.id = id;
        metadata
            .transition(SandboxState::Creating)
            .expect("pending to creating");
        metadata
            .transition(SandboxState::Running)
            .expect("creating to running");
        metadata.backend_ownership = BackendOwnership::Running;
        if active_operation {
            metadata
                .begin_operation(OperationKind::Create)
                .expect("active operation");
        }
        resources
            .instances
            .lock()
            .expect("instances")
            .insert(id, metadata);
        if insert_backend {
            let slot = slot.clone().unwrap_or_else(|| StorageSlot {
                id: id.to_string(),
                rootfs_path: PathBuf::new(),
                mem_path: PathBuf::new(),
                mem_diff_path: PathBuf::new(),
                rootfs_diff_path: PathBuf::new(),
                instance_dir: PathBuf::new(),
            });
            let owner = MockSpawner
                .spawn(SpawnRequest {
                    instance_id: id,
                    run_dir: storage.instances.join(id.to_string()).join("runtime"),
                    binary_path: PathBuf::new(),
                    storage: slot,
                    backend: BackendConfigs::default(),
                    vm: None,
                })
                .await
                .expect("mock owner");
            manager
                .insert_backend_owner(id, owner)
                .expect("register owner");
        }
    }

    async fn settle() {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn sweep_flushes_running_records_and_isolates_failures() {
        let temp = tempfile::tempdir().expect("temp");
        let storage = Arc::new(RecordingStorage::new(
            temp.path().join("images"),
            temp.path().join("instances"),
        ));
        let (manager, resources) = manager(temp.path(), storage.clone());
        let failing = Uuid::new_v4();
        let succeeding = Uuid::new_v4();
        insert_running(&manager, &resources, &storage, failing, true, true, false).await;
        insert_running(
            &manager, &resources, &storage, succeeding, true, true, false,
        )
        .await;
        storage.fail(failing);

        let summary = manager.flush_all(Duration::from_secs(1)).await;

        assert_eq!(
            summary,
            FlushSummary {
                selected: 2,
                flushed: 1,
                skipped: 0,
                failed: 1,
            }
        );
        assert_eq!(
            storage.calls().into_iter().collect::<HashSet<_>>(),
            HashSet::from([failing.to_string(), succeeding.to_string()])
        );
    }

    #[tokio::test]
    async fn sweep_reports_incomplete_running_owners_without_flushing() {
        let temp = tempfile::tempdir().expect("temp");
        let storage = Arc::new(RecordingStorage::new(
            temp.path().join("images"),
            temp.path().join("instances"),
        ));
        let (manager, resources) = manager(temp.path(), storage.clone());
        insert_running(
            &manager,
            &resources,
            &storage,
            Uuid::new_v4(),
            true,
            false,
            false,
        )
        .await;
        insert_running(
            &manager,
            &resources,
            &storage,
            Uuid::new_v4(),
            false,
            true,
            false,
        )
        .await;
        insert_running(
            &manager,
            &resources,
            &storage,
            Uuid::new_v4(),
            true,
            true,
            true,
        )
        .await;

        assert_eq!(
            manager.flush_all(Duration::from_secs(1)).await,
            FlushSummary {
                selected: 3,
                flushed: 0,
                skipped: 0,
                failed: 3,
            }
        );
        assert!(storage.calls().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn timed_out_provider_call_keeps_slot_retryable() {
        let temp = tempfile::tempdir().expect("temp");
        let storage = Arc::new(RecordingStorage::new(
            temp.path().join("images"),
            temp.path().join("instances"),
        ));
        let (manager, resources) = manager(temp.path(), storage.clone());
        let id = Uuid::new_v4();
        insert_running(&manager, &resources, &storage, id, true, true, false).await;
        storage.block_once();

        let first = {
            let manager = manager.clone();
            tokio::spawn(async move { manager.flush_all(Duration::from_secs(5)).await })
        };
        storage.started.notified().await;
        tokio::time::advance(Duration::from_secs(5)).await;
        assert_eq!(first.await.expect("sweep").failed, 1);

        let second = manager.flush_all(Duration::from_secs(5)).await;
        assert_eq!(second.flushed, 1);
        assert_eq!(storage.calls(), vec![id.to_string(), id.to_string()]);
    }

    #[tokio::test]
    async fn sweep_waits_for_operation_lock_and_rechecks_state() {
        let temp = tempfile::tempdir().expect("temp");
        let storage = Arc::new(RecordingStorage::new(
            temp.path().join("images"),
            temp.path().join("instances"),
        ));
        let (manager, resources) = manager(temp.path(), storage.clone());
        let id = Uuid::new_v4();
        insert_running(&manager, &resources, &storage, id, true, true, false).await;
        let lock = manager.operation_lock(id);
        let guard = lock.lock().await;
        let sweep = {
            let manager = manager.clone();
            tokio::spawn(async move { manager.flush_all(Duration::from_secs(1)).await })
        };
        settle().await;
        resources
            .instances
            .lock()
            .expect("instances")
            .get_mut(&id)
            .expect("metadata")
            .transition(SandboxState::RecoveryRequired)
            .expect("running to recovery-required");
        drop(guard);

        assert_eq!(sweep.await.expect("sweep").skipped, 1);
        assert!(storage.calls().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn periodic_worker_delays_first_tick_and_stops_before_next_sweep() {
        let temp = tempfile::tempdir().expect("temp");
        let storage = Arc::new(RecordingStorage::new(
            temp.path().join("images"),
            temp.path().join("instances"),
        ));
        let (manager, resources) = manager(temp.path(), storage.clone());
        let id = Uuid::new_v4();
        insert_running(&manager, &resources, &storage, id, true, true, false).await;

        let mut worker = manager.start_flush_loop(Duration::from_secs(10), Duration::from_secs(5));
        worker.wait_started().await;
        settle().await;
        assert!(storage.calls().is_empty());
        tokio::time::advance(Duration::from_secs(10)).await;
        storage.wait_for_calls(1).await;
        assert_eq!(storage.calls(), vec![id.to_string()]);

        worker.shutdown().await.expect("worker shutdown");
        tokio::time::advance(Duration::from_secs(30)).await;
        settle().await;
        assert_eq!(storage.calls(), vec![id.to_string()]);
    }

    #[tokio::test(start_paused = true)]
    async fn worker_shutdown_cancels_a_sweep_waiting_for_operation_lock() {
        let temp = tempfile::tempdir().expect("temp");
        let storage = Arc::new(RecordingStorage::new(
            temp.path().join("images"),
            temp.path().join("instances"),
        ));
        let (manager, resources) = manager(temp.path(), storage.clone());
        let id = Uuid::new_v4();
        insert_running(&manager, &resources, &storage, id, true, true, false).await;
        let lock = manager.operation_lock(id);
        let guard = lock.lock().await;
        let mut worker = manager.start_flush_loop(Duration::from_secs(10), Duration::from_secs(5));
        worker.wait_started().await;
        tokio::time::advance(Duration::from_secs(10)).await;
        settle().await;

        worker.shutdown().await.expect("worker shutdown");
        drop(guard);
        assert!(storage.calls().is_empty());
    }

    #[tokio::test]
    async fn supervisor_reports_an_unexpected_worker_exit() {
        let cancellation = CancellationToken::new();
        let mut worker = FlushLoop {
            cancellation,
            task: Some(tokio::spawn(async {})),
            started: Arc::new(Notify::new()),
        };

        let error = worker
            .observe_exit()
            .await
            .expect_err("early worker exit must stop the daemon");

        assert!(error.to_string().contains("exited unexpectedly"));
        worker
            .shutdown()
            .await
            .expect("finished worker is already joined");
    }
}
