// SPDX-License-Identifier: Apache-2.0
//! Async warm pool that owns storage slots and optional paused backend VMs.

#![allow(dead_code)] // Activated by SandboxManager in the lifecycle commit.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use blaze_core::backend::{BackendKind, NetworkConfig, SpawnRequest};
use blaze_core::policy::{BackendConfigs, VmConfig};
use blaze_core::storage::{AcquireOpts, PoolStatus, StorageProvider, StorageSlot};
use blaze_core::{BlazeError, Result};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::guest::GuestClient;
use crate::spawner::{DynBackendInstance, DynSpawner};

/// Backend inputs copied into every asynchronously built slot.
#[derive(Debug, Clone)]
pub struct PoolPrototype {
    /// Backend executable selected during daemon startup.
    pub binary_path: PathBuf,
    /// Backend-specific policy.
    pub backend: BackendConfigs,
    /// Generic VM resources.
    pub vm: Option<VmConfig>,
    /// Optional isolated network.
    pub network: Option<NetworkConfig>,
}

/// One ready warm-pool resource bundle.
pub struct RuntimePoolSlot {
    /// Stable ID used as the sandbox ID when the slot is assigned.
    pub instance_id: Uuid,
    /// Provider-owned storage.
    pub storage: StorageSlot,
    /// Running, unassigned pre-forked backend when `prefork=true`.
    pub backend: Option<DynBackendInstance>,
    /// Backend runtime directory.
    pub run_dir: PathBuf,
}

#[derive(Default)]
struct PoolState {
    ready: VecDeque<RuntimePoolSlot>,
    quarantined: Vec<RuntimePoolSlot>,
    pending: usize,
    prototype: Option<PoolPrototype>,
    prototype_fingerprint: Option<Vec<u8>>,
    generation: u64,
}

/// Bounded async warm pool.
pub struct RuntimeWarmPool {
    target: usize,
    prefork: bool,
    rootfs_size: u64,
    mem_size: u64,
    runtime_root: PathBuf,
    storage: Arc<dyn StorageProvider>,
    spawner: DynSpawner,
    guest_timeout: Duration,
    max_file_bytes: usize,
    state: Mutex<PoolState>,
    cancellation: CancellationToken,
    pending_changed: Notify,
}

impl RuntimeWarmPool {
    /// Create an empty pool. Building begins after the first prototype is set.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        target: usize,
        prefork: bool,
        rootfs_size: u64,
        mem_size: u64,
        runtime_root: PathBuf,
        storage: Arc<dyn StorageProvider>,
        spawner: DynSpawner,
        guest_timeout: Duration,
        max_file_bytes: usize,
        cancellation: CancellationToken,
    ) -> Arc<Self> {
        Arc::new(Self {
            target,
            prefork,
            rootfs_size,
            mem_size,
            runtime_root,
            storage,
            spawner,
            guest_timeout,
            max_file_bytes,
            state: Mutex::new(PoolState::default()),
            cancellation,
            pending_changed: Notify::new(),
        })
    }

    /// Pin the build inputs used for global warm slots and trigger initial fill.
    pub fn configure(self: &Arc<Self>, prototype: PoolPrototype) -> Result<bool> {
        if self.target == 0 {
            return Ok(false);
        }
        let fingerprint = serde_json::to_vec(&(
            &prototype.binary_path,
            &prototype.backend,
            &prototype.vm,
            &prototype.network,
        ))
        .map_err(|error| BlazeError::StorageError {
            msg: format!("serialize runtime pool prototype: {error}"),
        })?;
        let mut state = self.state.lock().map_err(|_| BlazeError::StorageError {
            msg: "runtime pool lock poisoned".to_string(),
        })?;
        if state.prototype.is_none() {
            state.prototype = Some(prototype);
            state.prototype_fingerprint = Some(fingerprint);
        } else if state.prototype_fingerprint.as_deref() != Some(fingerprint.as_slice()) {
            return Ok(false);
        }
        drop(state);
        self.trigger_refill();
        Ok(true)
    }

    /// Acquire one ready slot and refill asynchronously.
    pub fn acquire(self: &Arc<Self>) -> Result<Option<RuntimePoolSlot>> {
        let slot = self
            .state
            .lock()
            .map_err(|_| BlazeError::StorageError {
                msg: "runtime pool lock poisoned".to_string(),
            })?
            .ready
            .pop_front();
        self.trigger_refill();
        Ok(slot)
    }

    /// Destroy a consumed slot after create fails.
    pub async fn discard(self: &Arc<Self>, slot: RuntimePoolSlot) -> Result<()> {
        self.destroy_slot(slot).await
    }

    /// Current ready/capacity/pending values.
    pub fn status(&self) -> PoolStatus {
        self.state
            .lock()
            .map(|state| PoolStatus {
                ready: state.ready.len(),
                capacity: self.target,
                pending: state.pending,
                quarantined: state.quarantined.len(),
            })
            .unwrap_or(PoolStatus {
                ready: 0,
                capacity: self.target,
                pending: 0,
                quarantined: 0,
            })
    }

    /// Destroy all ready slots without starting a refill.
    pub async fn drain(&self) -> Result<usize> {
        let slots = {
            let mut state = self.state.lock().map_err(|_| BlazeError::StorageError {
                msg: "runtime pool lock poisoned".to_string(),
            })?;
            state.generation = state.generation.wrapping_add(1);
            let mut slots = state.ready.drain(..).collect::<Vec<_>>();
            slots.append(&mut state.quarantined);
            slots
        };
        let count = slots.len();
        let mut errors = Vec::new();
        for slot in slots {
            if let Err(error) = self.destroy_slot(slot).await {
                errors.push(error.to_string());
            }
        }
        if !errors.is_empty() {
            return Err(BlazeError::StorageError {
                msg: format!(
                    "failed to drain {} runtime pool slot(s): {}",
                    errors.len(),
                    errors.join("; ")
                ),
            });
        }
        Ok(count)
    }

    /// Drain all current resources, invalidate in-flight builders, then refill.
    pub async fn drain_and_refill(self: &Arc<Self>) -> Result<usize> {
        let count = self.drain().await?;
        self.trigger_refill();
        Ok(count)
    }

    /// Cancel refill work, wait for pending builders, and release ready slots.
    pub async fn shutdown(&self) -> Result<()> {
        self.cancellation.cancel();
        loop {
            if self.status().pending == 0 {
                break;
            }
            self.pending_changed.notified().await;
        }
        self.drain().await?;
        Ok(())
    }

    fn trigger_refill(self: &Arc<Self>) {
        if self.target == 0 || self.cancellation.is_cancelled() {
            return;
        }
        let (reservations, generation) = {
            let Ok(mut state) = self.state.lock() else {
                return;
            };
            if state.prototype.is_none() {
                return;
            }
            let missing = self.target.saturating_sub(
                state
                    .ready
                    .len()
                    .saturating_add(state.quarantined.len())
                    .saturating_add(state.pending),
            );
            state.pending = state.pending.saturating_add(missing);
            (missing, state.generation)
        };
        for _ in 0..reservations {
            let pool = self.clone();
            tokio::spawn(async move {
                let built = pool.build_slot().await;
                let mut surplus = None;
                let mut pending_cleanup = false;
                let mut retry_delay = false;
                if let Ok(mut state) = pool.state.lock() {
                    match built {
                        Ok(slot)
                            if !pool.cancellation.is_cancelled()
                                && state.generation == generation
                                && state.ready.len() < pool.target =>
                        {
                            state.ready.push_back(slot);
                            state.pending = state.pending.saturating_sub(1);
                        }
                        Ok(slot) => {
                            surplus = Some(slot);
                            pending_cleanup = true;
                        }
                        Err(error) => {
                            state.pending = state.pending.saturating_sub(1);
                            retry_delay = true;
                            tracing::warn!(%error, "runtime warm-pool build failed");
                        }
                    }
                }
                if let Some(slot) = surplus
                    && let Err(error) = pool.destroy_slot(slot).await
                {
                    tracing::warn!(%error, "failed to destroy surplus warm-pool slot");
                }
                if pending_cleanup && let Ok(mut state) = pool.state.lock() {
                    state.pending = state.pending.saturating_sub(1);
                }
                pool.pending_changed.notify_one();
                if retry_delay {
                    tokio::select! {
                        _ = pool.cancellation.cancelled() => return,
                        _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
                    }
                }
                pool.trigger_refill();
            });
        }
    }

    async fn build_slot(&self) -> Result<RuntimePoolSlot> {
        if self.cancellation.is_cancelled() {
            return Err(BlazeError::StorageError {
                msg: "runtime pool is shutting down".to_string(),
            });
        }
        crate::failpoint::storage("pool-build")?;
        let prototype = self
            .state
            .lock()
            .map_err(|_| BlazeError::StorageError {
                msg: "runtime pool lock poisoned".to_string(),
            })?
            .prototype
            .clone()
            .ok_or_else(|| BlazeError::StorageError {
                msg: "runtime pool has no build prototype".to_string(),
            })?;
        let instance_id = Uuid::new_v4();
        let storage = self
            .storage
            .acquire(&AcquireOpts {
                instance_id: instance_id.to_string(),
                rootfs_size: self.rootfs_size,
                mem_size: self.mem_size,
            })
            .await?;
        let run_dir = self.runtime_root.join(instance_id.to_string());
        let backend = if self.prefork {
            match self
                .spawner
                .spawn(SpawnRequest {
                    instance_id,
                    run_dir: run_dir.clone(),
                    binary_path: prototype.binary_path,
                    storage: storage.clone(),
                    backend: prototype.backend.clone(),
                    vm: prototype.vm,
                    network: prototype.network,
                })
                .await
            {
                Ok(backend) => {
                    if guest_enabled(backend.backend(), &prototype.backend) {
                        let guest = GuestClient::new(
                            backend.guest_socket_path().to_path_buf(),
                            self.guest_timeout,
                            self.max_file_bytes,
                        );
                        if let Err(error) = guest
                            .wait_ready(self.guest_timeout, &self.cancellation)
                            .await
                        {
                            let cleanup = self
                                .destroy_slot(RuntimePoolSlot {
                                    instance_id,
                                    storage,
                                    backend: Some(backend),
                                    run_dir,
                                })
                                .await;
                            return match cleanup {
                                Ok(()) => Err(BlazeError::BackendError {
                                    msg: format!("prefork guest readiness failed: {error}"),
                                }),
                                Err(cleanup) => Err(BlazeError::BackendError {
                                    msg: format!(
                                        "prefork guest readiness failed ({error}); cleanup failed ({cleanup})"
                                    ),
                                }),
                            };
                        }
                    }
                    Some(backend)
                }
                Err(error) => {
                    let slot = RuntimePoolSlot {
                        instance_id,
                        storage,
                        backend: None,
                        run_dir,
                    };
                    if let Err(cleanup) = self
                        .spawner
                        .cleanup_orphan(instance_id, &slot.run_dir)
                        .await
                    {
                        self.quarantine(slot)?;
                        return Err(BlazeError::BackendError {
                            msg: format!(
                                "prefork spawn failed ({error}); orphan cleanup failed ({cleanup})"
                            ),
                        });
                    }
                    let cleanup = self.destroy_slot(slot).await;
                    return match cleanup {
                        Ok(()) => Err(error),
                        Err(cleanup) => Err(BlazeError::BackendError {
                            msg: format!(
                                "prefork spawn failed ({error}); cleanup failed ({cleanup})"
                            ),
                        }),
                    };
                }
            }
        } else {
            None
        };
        Ok(RuntimePoolSlot {
            instance_id,
            storage,
            backend,
            run_dir,
        })
    }

    async fn destroy_slot(&self, mut slot: RuntimePoolSlot) -> Result<()> {
        if let Some(backend) = slot.backend.as_ref().cloned() {
            if let Err(error) = backend.kill().await {
                self.quarantine(slot)?;
                return Err(error);
            }
            slot.backend = None;
        }
        if let Err(error) = self.storage.release(slot.storage.clone()).await {
            self.quarantine(slot)?;
            return Err(error);
        }
        if let Err(error) = remove_directory_if_exists(&slot.run_dir).await {
            self.quarantine(slot)?;
            return Err(error);
        }
        Ok(())
    }

    fn quarantine(&self, slot: RuntimePoolSlot) -> Result<()> {
        self.state
            .lock()
            .map_err(|_| BlazeError::StorageError {
                msg: "runtime pool lock poisoned while retaining failed cleanup".to_string(),
            })?
            .quarantined
            .push(slot);
        Ok(())
    }
}

fn guest_enabled(backend: BackendKind, config: &BackendConfigs) -> bool {
    backend == BackendKind::Mock
        || (backend == BackendKind::Firecracker
            && config
                .firecracker
                .as_ref()
                .map(|firecracker| firecracker.enable_vsock)
                .unwrap_or(false))
}

async fn remove_directory_if_exists(path: &std::path::Path) -> Result<()> {
    match tokio::fs::remove_dir_all(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use crate::file_provider::FileStorageProvider;
    use crate::spawner::MockSpawner;

    use super::*;

    async fn wait_for_status(pool: &RuntimeWarmPool, ready: usize, pending: usize) {
        for _ in 0..400 {
            let status = pool.status();
            if status.ready == ready && status.pending == pending {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let status = pool.status();
        panic!(
            "pool status did not converge: ready={}, pending={}, quarantined={}",
            status.ready, status.pending, status.quarantined
        );
    }

    fn prototype() -> PoolPrototype {
        PoolPrototype {
            binary_path: PathBuf::new(),
            backend: BackendConfigs::default(),
            vm: None,
            network: None,
        }
    }

    #[tokio::test]
    async fn concurrent_refill_never_exceeds_target_and_shutdown_releases() {
        let temp = tempfile::tempdir().expect("temp");
        let images = temp.path().join("images");
        let instances = temp.path().join("instances");
        std::fs::create_dir_all(&images).expect("images");
        std::fs::create_dir_all(&instances).expect("instances");
        let storage: Arc<dyn StorageProvider> =
            Arc::new(FileStorageProvider::with_images(images, instances.clone()));
        let cancellation = CancellationToken::new();
        let pool = RuntimeWarmPool::new(
            4,
            true,
            64,
            32,
            temp.path().join("runtime"),
            storage,
            Arc::new(MockSpawner),
            Duration::from_secs(1),
            1024,
            cancellation,
        );
        pool.configure(prototype()).expect("configure");
        wait_for_status(&pool, 4, 0).await;
        assert_eq!(pool.status().ready, 4);
        assert_eq!(pool.status().pending, 0);
        let mut ids = std::collections::HashSet::new();
        for _ in 0..4 {
            let slot = pool.acquire().expect("acquire").expect("ready");
            assert!(ids.insert(slot.instance_id));
            pool.discard(slot).await.expect("discard");
        }
        pool.shutdown().await.expect("shutdown");
        assert_eq!(pool.status().ready, 0);
        assert_eq!(pool.status().pending, 0);
        assert_eq!(
            std::fs::read_dir(instances).expect("read").count(),
            0,
            "all slot directories must be released"
        );
    }

    #[tokio::test]
    async fn concurrent_drain_and_refill_replaces_each_slot_once() {
        let temp = tempfile::tempdir().expect("temp");
        let images = temp.path().join("images");
        let instances = temp.path().join("instances");
        std::fs::create_dir_all(&images).expect("images");
        std::fs::create_dir_all(&instances).expect("instances");
        let storage: Arc<dyn StorageProvider> =
            Arc::new(FileStorageProvider::with_images(images, instances.clone()));
        let pool = RuntimeWarmPool::new(
            3,
            true,
            64,
            32,
            temp.path().join("runtime"),
            storage,
            Arc::new(MockSpawner),
            Duration::from_secs(1),
            1024,
            CancellationToken::new(),
        );
        pool.configure(prototype()).expect("configure");
        wait_for_status(&pool, 3, 0).await;
        let before = pool
            .state
            .lock()
            .expect("state")
            .ready
            .iter()
            .map(|slot| slot.instance_id)
            .collect::<std::collections::HashSet<_>>();

        let (first, second) = tokio::join!(pool.drain_and_refill(), pool.drain_and_refill());
        assert_eq!(
            first.expect("first drain") + second.expect("second drain"),
            3
        );
        wait_for_status(&pool, 3, 0).await;
        let after = pool
            .state
            .lock()
            .expect("state")
            .ready
            .iter()
            .map(|slot| slot.instance_id)
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(after.len(), 3);
        assert!(before.is_disjoint(&after));

        pool.shutdown().await.expect("shutdown");
        assert_eq!(std::fs::read_dir(instances).expect("read").count(), 0);
    }

    struct FlakyStorage {
        inner: FileStorageProvider,
        remaining_failures: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl StorageProvider for FlakyStorage {
        async fn probe(&self) -> Result<bool> {
            self.inner.probe().await
        }

        async fn acquire(&self, opts: &AcquireOpts) -> Result<StorageSlot> {
            if self
                .remaining_failures
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(BlazeError::StorageError {
                    msg: "injected transient pool build failure".to_string(),
                });
            }
            self.inner.acquire(opts).await
        }

        async fn release(&self, slot: StorageSlot) -> Result<()> {
            self.inner.release(slot).await
        }

        async fn release_by_id(&self, instance_id: &str) -> Result<()> {
            self.inner.release_by_id(instance_id).await
        }

        async fn reconstruct(&self, instance_id: &str) -> Result<StorageSlot> {
            self.inner.reconstruct(instance_id).await
        }

        async fn flush_dirty(&self, slot: &StorageSlot) -> Result<()> {
            self.inner.flush_dirty(slot).await
        }

        fn pool_status(&self) -> PoolStatus {
            self.inner.pool_status()
        }

        async fn drain_pool(&self) -> Result<usize> {
            self.inner.drain_pool().await
        }
    }

    #[tokio::test]
    async fn transient_build_failures_release_pending_and_recover_waterline() {
        let temp = tempfile::tempdir().expect("temp");
        let images = temp.path().join("images");
        let instances = temp.path().join("instances");
        std::fs::create_dir_all(&images).expect("images");
        std::fs::create_dir_all(&instances).expect("instances");
        let storage: Arc<dyn StorageProvider> = Arc::new(FlakyStorage {
            inner: FileStorageProvider::with_images(images, instances.clone()),
            remaining_failures: AtomicUsize::new(3),
        });
        let pool = RuntimeWarmPool::new(
            2,
            false,
            64,
            32,
            temp.path().join("runtime"),
            storage,
            Arc::new(MockSpawner),
            Duration::from_secs(1),
            1024,
            CancellationToken::new(),
        );
        pool.configure(prototype()).expect("configure");
        wait_for_status(&pool, 2, 0).await;
        assert_eq!(pool.status().quarantined, 0);

        pool.shutdown().await.expect("shutdown");
        assert_eq!(pool.status().pending, 0);
        assert_eq!(std::fs::read_dir(instances).expect("read").count(), 0);
    }
}
