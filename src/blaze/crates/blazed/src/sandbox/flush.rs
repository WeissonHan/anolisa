// SPDX-License-Identifier: Apache-2.0
//! Background dirty-data flush loop.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use blaze_core::lifecycle::SandboxState;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};
use uuid::Uuid;

use crate::error::{BlazeDaemonError, Result};

use super::manager::{SandboxManager, SandboxRuntime};

/// Counters emitted for one provider flush sweep.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FlushSummary {
    /// Running runtime references copied before any provider await.
    pub(crate) selected: usize,
    /// Provider flushes that completed successfully.
    pub(crate) flushed: usize,
    /// Sandboxes that stopped being Running before their instance lock won.
    pub(crate) skipped: usize,
    /// Per-sandbox provider failures isolated from the rest of the sweep.
    pub(crate) failed: usize,
}

impl SandboxManager {
    /// Start the cancellable periodic flush task.
    ///
    /// The caller supplies an already parsed and validated interval. The first
    /// sweep starts after one full interval, matching `time.Ticker` semantics.
    pub fn start_flush_loop(self: &Arc<Self>, interval: Duration) -> JoinHandle<()> {
        let manager = self.clone();
        tracing::info!(
            interval_secs = interval.as_secs(),
            "starting provider flush loop"
        );
        tokio::spawn(async move {
            let first_tick = Instant::now() + interval;
            let mut ticker = tokio::time::interval_at(first_tick, interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    biased;
                    _ = manager.cancellation.cancelled() => break,
                    _ = ticker.tick() => {
                        match manager.flush_all().await {
                            Ok(summary) => {
                                tracing::debug!(
                                    selected = summary.selected,
                                    flushed = summary.flushed,
                                    skipped = summary.skipped,
                                    failed = summary.failed,
                                    "provider flush sweep completed"
                                );
                            }
                            Err(error) => {
                                tracing::error!(%error, "provider flush sweep failed");
                            }
                        }
                    }
                }
            }
            tracing::info!("provider flush loop stopped");
        })
    }

    /// Cancel and await the periodic task before runtime teardown begins.
    pub async fn stop_flush_loop(&self, task: JoinHandle<()>) -> Result<()> {
        self.cancellation.cancel();
        task.await.map_err(|error| {
            BlazeDaemonError::Internal(format!("provider flush task join failed: {error}"))
        })
    }

    /// Flush every running sandbox without holding a global map lock across
    /// any await.
    pub async fn flush_all(&self) -> Result<FlushSummary> {
        let running_ids = self
            .instances
            .lock()
            .map_err(|_| BlazeDaemonError::Internal("instances lock poisoned".into()))?
            .iter()
            .filter_map(|(id, metadata)| (metadata.state == SandboxState::Running).then_some(*id))
            .collect::<HashSet<_>>();
        let runtimes = self
            .runtimes
            .lock()
            .map_err(|_| BlazeDaemonError::Internal("runtimes lock poisoned".into()))?
            .iter()
            .filter_map(|(id, runtime)| {
                running_ids
                    .contains(id)
                    .then_some((*id, Arc::clone(runtime)))
            })
            .collect::<Vec<_>>();

        let mut summary = FlushSummary {
            selected: runtimes.len(),
            ..FlushSummary::default()
        };
        for (id, runtime) in runtimes {
            match self.flush_if_running(id, runtime).await {
                Ok(true) => summary.flushed += 1,
                Ok(false) => summary.skipped += 1,
                Err(error) => {
                    summary.failed += 1;
                    tracing::warn!(sandbox_id = %id, %error, "sandbox provider flush failed");
                }
            }
        }
        Ok(summary)
    }

    async fn flush_if_running(
        &self,
        id: Uuid,
        runtime: Arc<AsyncMutex<SandboxRuntime>>,
    ) -> Result<bool> {
        let runtime = runtime.lock().await;
        if self.get(id)?.state != SandboxState::Running {
            return Ok(false);
        }
        self.storage.flush_dirty(&runtime.storage).await?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use blaze_core::backend::BackendKind;
    use blaze_core::config::DaemonConfig;
    use blaze_core::error::{BlazeError, Result as CoreResult};
    use blaze_core::lifecycle::{SandboxInstance, SandboxState, StartPath};
    use blaze_core::policy::WorkloadClass;
    use blaze_core::storage::{AcquireOpts, PoolStatus, StorageProvider, StorageSlot};
    use tokio_util::sync::CancellationToken;

    use crate::file_provider::FileStorageProvider;
    use crate::spawner::MockSpawner;

    use super::*;

    struct RecordingStorage {
        inner: FileStorageProvider,
        calls: Mutex<Vec<String>>,
        failures: Mutex<HashSet<String>>,
    }

    impl RecordingStorage {
        fn new(images: PathBuf, instances: PathBuf) -> Self {
            Self {
                inner: FileStorageProvider::with_images(images, instances),
                calls: Mutex::new(Vec::new()),
                failures: Mutex::new(HashSet::new()),
            }
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().expect("calls").clone()
        }

        fn fail(&self, id: Uuid) {
            self.failures
                .lock()
                .expect("failures")
                .insert(id.to_string());
        }
    }

    #[async_trait]
    impl StorageProvider for RecordingStorage {
        async fn probe(&self) -> CoreResult<bool> {
            self.inner.probe().await
        }

        async fn acquire(&self, opts: &AcquireOpts) -> CoreResult<StorageSlot> {
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
            if self.failures.lock().expect("failures").contains(&slot.id) {
                return Err(BlazeError::StorageError {
                    msg: format!("injected provider flush failure for {}", slot.id),
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

    fn manager(temp: &Path, storage: Arc<RecordingStorage>) -> Arc<SandboxManager> {
        let mut config = DaemonConfig::default();
        config.daemon.state_dir = temp.join("state");
        config.storage.images_dir = temp.join("images");
        config.storage.instances_dir = temp.join("instances");
        config.storage.rootfs_size = 64;
        config.storage.mem_size = 32;
        for directory in [
            &config.daemon.state_dir,
            &config.storage.images_dir,
            &config.storage.instances_dir,
        ] {
            std::fs::create_dir_all(directory).expect("test directory");
        }
        Arc::new(
            SandboxManager::new(
                config,
                HashMap::new(),
                Arc::new(MockSpawner),
                BackendKind::Mock,
                storage,
                CancellationToken::new(),
            )
            .expect("manager"),
        )
    }

    async fn insert_runtime(
        manager: &SandboxManager,
        storage: &RecordingStorage,
        id: Uuid,
        state: SandboxState,
    ) {
        let slot = storage
            .acquire(&AcquireOpts {
                instance_id: id.to_string(),
                rootfs_size: 64,
                mem_size: 32,
            })
            .await
            .expect("slot");
        let mut metadata = SandboxInstance::new_with_id(
            id,
            BackendKind::Mock,
            WorkloadClass::AgentTool,
            "sha256:flush-test".into(),
            StartPath::Cold,
            "flush-test".into(),
        );
        metadata
            .transition(SandboxState::Creating)
            .expect("pending to creating");
        metadata
            .transition(SandboxState::Running)
            .expect("creating to running");
        match state {
            SandboxState::Running => {}
            SandboxState::Hibernated => {
                metadata
                    .transition(SandboxState::Hibernating)
                    .expect("running to hibernating");
                metadata
                    .transition(SandboxState::Hibernated)
                    .expect("hibernating to hibernated");
            }
            other => panic!("unsupported flush fixture state: {other}"),
        }
        manager
            .instances
            .lock()
            .expect("instances")
            .insert(id, metadata);
        manager.runtimes.lock().expect("runtimes").insert(
            id,
            Arc::new(AsyncMutex::new(SandboxRuntime {
                storage: slot,
                backend: None,
            })),
        );
    }

    async fn settle() {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn flush_all_selects_only_running_and_isolates_failures() {
        let temp = tempfile::tempdir().expect("temp");
        let storage = Arc::new(RecordingStorage::new(
            temp.path().join("images"),
            temp.path().join("instances"),
        ));
        let manager = manager(temp.path(), storage.clone());
        let failing = Uuid::new_v4();
        let succeeding = Uuid::new_v4();
        let hibernated = Uuid::new_v4();
        insert_runtime(&manager, &storage, failing, SandboxState::Running).await;
        insert_runtime(&manager, &storage, succeeding, SandboxState::Running).await;
        insert_runtime(&manager, &storage, hibernated, SandboxState::Hibernated).await;
        storage.fail(failing);

        let summary = manager.flush_all().await.expect("sweep");

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
    async fn sweep_item_waits_for_instance_lock() {
        let temp = tempfile::tempdir().expect("temp");
        let storage = Arc::new(RecordingStorage::new(
            temp.path().join("images"),
            temp.path().join("instances"),
        ));
        let manager = manager(temp.path(), storage.clone());
        let id = Uuid::new_v4();
        insert_runtime(&manager, &storage, id, SandboxState::Running).await;
        let runtime = manager.runtime(id).expect("runtime");
        let guard = runtime.lock().await;
        let flush = {
            let manager = manager.clone();
            let runtime = runtime.clone();
            tokio::spawn(async move { manager.flush_if_running(id, runtime).await })
        };
        settle().await;
        assert!(storage.calls().is_empty());

        drop(guard);
        let flushed = tokio::time::timeout(Duration::from_secs(1), flush)
            .await
            .expect("flush unblocked")
            .expect("task")
            .expect("flush");
        assert!(flushed);
        assert_eq!(storage.calls(), vec![id.to_string()]);
    }

    #[tokio::test]
    async fn sweep_skips_runtime_that_stops_running_while_waiting_for_lock() {
        let temp = tempfile::tempdir().expect("temp");
        let storage = Arc::new(RecordingStorage::new(
            temp.path().join("images"),
            temp.path().join("instances"),
        ));
        let manager = manager(temp.path(), storage.clone());
        let id = Uuid::new_v4();
        insert_runtime(&manager, &storage, id, SandboxState::Running).await;
        let runtime = manager.runtime(id).expect("runtime");
        let guard = runtime.lock().await;
        let sweep = {
            let manager = manager.clone();
            tokio::spawn(async move { manager.flush_all().await })
        };
        settle().await;
        {
            let mut instances = manager.instances.lock().expect("instances");
            let metadata = instances.get_mut(&id).expect("metadata");
            metadata
                .transition(SandboxState::Hibernating)
                .expect("running to hibernating");
            metadata
                .transition(SandboxState::Hibernated)
                .expect("hibernating to hibernated");
        }
        drop(guard);

        let summary = sweep.await.expect("task").expect("sweep");
        assert_eq!(
            summary,
            FlushSummary {
                selected: 1,
                flushed: 0,
                skipped: 1,
                failed: 0,
            }
        );
        assert!(storage.calls().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn periodic_loop_delays_first_tick_and_stops_cleanly() {
        let temp = tempfile::tempdir().expect("temp");
        let storage = Arc::new(RecordingStorage::new(
            temp.path().join("images"),
            temp.path().join("instances"),
        ));
        let manager = manager(temp.path(), storage.clone());
        let id = Uuid::new_v4();
        insert_runtime(&manager, &storage, id, SandboxState::Running).await;

        let task = manager.start_flush_loop(Duration::from_secs(10));
        settle().await;
        assert!(storage.calls().is_empty(), "first tick must be delayed");

        tokio::time::advance(Duration::from_secs(9)).await;
        settle().await;
        assert!(storage.calls().is_empty());

        tokio::time::advance(Duration::from_secs(1)).await;
        settle().await;
        assert_eq!(storage.calls(), vec![id.to_string()]);

        manager
            .stop_flush_loop(task)
            .await
            .expect("cancel and join");
        let calls_after_stop = storage.calls();
        tokio::time::advance(Duration::from_secs(30)).await;
        settle().await;
        assert_eq!(storage.calls(), calls_after_stop);
    }
}
