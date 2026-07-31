// SPDX-License-Identifier: Apache-2.0
//! Generic storage provider abstraction.
//!
//! Different providers may offer different performance characteristics
//! (warm pools, copy-on-write, content-addressable dedup) but present
//! a uniform interface to the daemon layer.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use thiserror::Error;

use crate::error::{BlazeError, Result};

/// A storage slot allocated for one sandbox instance.
///
/// This capability is runtime-only. Persist the stable `id`, then ask the
/// configured provider to reconstruct every path after restart.
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// Stable handle for one provider-owned rootfs restore transaction.
///
/// Callers must keep this handle from staging through activation and
/// finalization. Providers must validate both fields against durable state
/// before changing storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageRestoreTransaction {
    /// Stable sandbox identifier whose rootfs is being replaced.
    pub instance_id: String,
    /// Unique transaction identifier used to reject stale handles.
    pub transaction_id: uuid::Uuid,
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

/// Storage allocation failure with an optional residual slot owner.
///
/// A provider returns `residual` only when rollback could not remove resources
/// that were created for this request. The caller must retain the stable slot
/// ID until a later release succeeds.
#[derive(Debug, Error)]
#[error("{source}")]
pub struct StorageAcquireError {
    #[source]
    source: BlazeError,
    residual: Option<StorageSlot>,
}

impl StorageAcquireError {
    /// Build a failure after the provider confirmed that no resources remain.
    pub fn clean(source: BlazeError) -> Self {
        Self {
            source,
            residual: None,
        }
    }

    /// Build a failure that transfers residual slot ownership to the caller.
    pub fn with_residual(source: BlazeError, residual: StorageSlot) -> Self {
        Self {
            source,
            residual: Some(residual),
        }
    }

    /// Split the original provider error from any residual slot owner.
    pub fn into_parts(self) -> (BlazeError, Option<StorageSlot>) {
        (self.source, self.residual)
    }
}

impl From<BlazeError> for StorageAcquireError {
    fn from(source: BlazeError) -> Self {
        Self::clean(source)
    }
}

/// Generic storage backend trait.
#[async_trait]
pub trait StorageProvider: Send + Sync {
    /// Probe whether this provider is available in the current environment.
    async fn probe(&self) -> Result<bool>;

    /// Acquire a ready storage slot (may come from a warm pool).
    async fn acquire(
        &self,
        opts: &AcquireOpts,
    ) -> std::result::Result<StorageSlot, StorageAcquireError>;

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

    /// Report whether pool-owned slots can be inventoried and released by ID.
    ///
    /// Returning true promises both a complete [`Self::list_owned_ids`]
    /// inventory for the provider's currently configured root and an
    /// idempotent [`Self::release_by_id`] that can retry complete, missing,
    /// and partially created slots. It does not identify a provider or root
    /// recorded by another subsystem.
    fn supports_runtime_pool_recovery(&self) -> bool {
        false
    }

    /// List every stable slot identifier currently owned by the provider.
    ///
    /// Implementations must reject entries that cannot be classified as a
    /// provider-owned slot. Callers use this inventory only when
    /// [`Self::supports_runtime_pool_recovery`] returns true.
    async fn list_owned_ids(&self) -> Result<Vec<String>> {
        Err(BlazeError::StorageError {
            msg: "storage provider does not expose stable slot inventory".to_string(),
        })
    }

    /// Reconstruct a previously allocated slot from a stable instance id.
    ///
    /// Implementations must derive every returned path from their configured
    /// root and must not trust persisted path strings.
    async fn reconstruct(&self, instance_id: &str) -> Result<StorageSlot>;

    /// Flush dirty data to persistent storage (implementation may be no-op).
    ///
    /// The daemon may cancel this future when its configured attempt deadline
    /// or shutdown signal wins. Cancellation must retain slot ownership and
    /// leave a later synchronization or cleanup attempt safe.
    async fn flush_dirty(&self, slot: &StorageSlot) -> Result<()>;

    /// Report whether this provider can capture a self-contained checkpoint.
    ///
    /// The default is conservative so existing providers do not advertise a
    /// data path they have not implemented.
    fn supports_checkpoint_capture(&self) -> bool {
        false
    }

    /// Capture the slot's writable root filesystem at `target`.
    async fn capture_checkpoint(&self, slot: &StorageSlot, target: &Path) -> Result<()> {
        let _ = (slot, target);
        Err(BlazeError::StorageError {
            msg: "storage provider does not support checkpoint capture".to_string(),
        })
    }

    /// Report whether this provider can restore a self-contained checkpoint.
    ///
    /// The default is conservative so existing providers cannot enter a
    /// partially implemented replacement flow.
    fn supports_checkpoint_restore(&self) -> bool {
        false
    }

    /// Copy a checkpoint rootfs into provider-owned staging storage.
    ///
    /// Staging must leave the live rootfs unchanged so callers may prepare the
    /// replacement before stopping the current runtime.
    async fn stage_checkpoint_restore(
        &self,
        slot: &StorageSlot,
        source: &Path,
    ) -> Result<StorageRestoreTransaction> {
        let _ = (slot, source);
        Err(checkpoint_restore_unsupported())
    }

    /// Select the staged rootfs while retaining the previous rootfs.
    ///
    /// A successful activation must remain abortable until
    /// [`Self::commit_checkpoint_restore`] starts.
    async fn activate_checkpoint_restore(
        &self,
        transaction: &StorageRestoreTransaction,
    ) -> Result<()> {
        let _ = transaction;
        Err(checkpoint_restore_unsupported())
    }

    /// Finalize an activated rootfs and release its retained predecessor.
    async fn commit_checkpoint_restore(
        &self,
        transaction: &StorageRestoreTransaction,
    ) -> Result<()> {
        let _ = transaction;
        Err(checkpoint_restore_unsupported())
    }

    /// Restore the predecessor retained by a staged or activated transaction.
    async fn abort_checkpoint_restore(
        &self,
        transaction: &StorageRestoreTransaction,
    ) -> Result<()> {
        let _ = transaction;
        Err(checkpoint_restore_unsupported())
    }

    /// Resolve an interrupted restore transaction after process restart.
    ///
    /// Implementations choose the outcome from durable transaction state:
    /// work not yet committed should roll back, while a durable commit intent
    /// should finish committing.
    async fn reconcile_checkpoint_restore(&self, instance_id: &str) -> Result<()> {
        let _ = instance_id;
        Err(checkpoint_restore_unsupported())
    }

    /// Query warm pool status.
    fn pool_status(&self) -> PoolStatus;

    /// Drain all ready slots from the warm pool.
    async fn drain_pool(&self) -> Result<usize>;
}

fn checkpoint_restore_unsupported() -> BlazeError {
    BlazeError::StorageError {
        msg: "storage provider does not support checkpoint restore".to_string(),
    }
}
