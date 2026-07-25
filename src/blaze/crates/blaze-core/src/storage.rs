// SPDX-License-Identifier: Apache-2.0
//! Generic storage provider abstraction.
//!
//! Different providers may offer different performance characteristics
//! (warm pools, copy-on-write, content-addressable dedup) but present
//! a uniform interface to the daemon layer.

use std::path::PathBuf;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::Result;

/// A storage slot allocated for one sandbox instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageSlot {
    /// Stable identifier used to reconstruct paths after daemon restart.
    pub id: String,
    /// Writable root filesystem exposed to the backend.
    pub rootfs_path: PathBuf,
    /// Base or merged guest memory file exposed to the backend.
    pub mem_path: PathBuf,
    /// Cumulative memory delta relative to the base image.
    pub mem_diff_path: PathBuf,
    /// Cumulative root filesystem delta relative to the base image.
    pub rootfs_diff_path: PathBuf,
    /// Provider-owned directory containing all slot artifacts.
    pub instance_dir: PathBuf,
}

/// Pool readiness status.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct PoolStatus {
    pub ready: usize,
    pub capacity: usize,
    pub pending: usize,
    /// Slots retained because backend or storage cleanup must be retried.
    pub quarantined: usize,
}

/// Options for acquiring a storage slot.
#[derive(Debug, Clone)]
pub struct AcquireOpts {
    /// Stable sandbox identifier. Providers must reject path components.
    pub instance_id: String,
    /// Logical root filesystem size in bytes.
    pub rootfs_size: u64,
    /// Logical guest memory file size in bytes.
    pub mem_size: u64,
}

/// Generic storage backend trait.
#[async_trait]
pub trait StorageProvider: Send + Sync {
    /// Probe whether this provider is available in the current environment.
    async fn probe(&self) -> Result<bool>;

    /// Acquire a ready storage slot (may come from a warm pool).
    async fn acquire(&self, opts: &AcquireOpts) -> Result<StorageSlot>;

    /// Release a storage slot (cleanup all associated resources).
    async fn release(&self, slot: StorageSlot) -> Result<()>;

    /// Release a slot using only its stable identifier during crash recovery.
    ///
    /// Providers whose `release` operation is idempotent for a missing slot
    /// should override this method. The default requires reconstruction first.
    async fn release_by_id(&self, instance_id: &str) -> Result<()> {
        let slot = self.reconstruct(instance_id).await?;
        self.release(slot).await
    }

    /// Reconstruct a previously allocated slot from a stable instance id.
    ///
    /// Implementations must derive every returned path from their configured
    /// root and must not trust persisted path strings.
    async fn reconstruct(&self, instance_id: &str) -> Result<StorageSlot>;

    /// Flush dirty data to persistent storage (implementation may be no-op).
    async fn flush_dirty(&self, slot: &StorageSlot) -> Result<()>;

    /// Query warm pool status.
    fn pool_status(&self) -> PoolStatus;

    /// Drain all ready slots from the warm pool.
    async fn drain_pool(&self) -> Result<usize>;
}
