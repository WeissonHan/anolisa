// SPDX-License-Identifier: Apache-2.0
//! Source-level contract for data-plane implementations composed with Blaze at build time.
//!
//! Downstream crates can implement this contract to integrate custom storage
//! and restore resources without modifying the Blaze source tree.
//!
//! The contract is a Rust source interface, not a stable dynamic-library ABI.
//! A provider and the daemon that consumes it must be built with a compatible
//! source revision, Rust toolchain, and dependency lock.

#![forbid(unsafe_code)]

use std::os::fd::OwnedFd;
use std::path::PathBuf;

use async_trait::async_trait;
use blaze_core::checkpoint::ProviderCheckpointRecord;
use blaze_core::data_plane::{BackendProcessIdentity, DataPlaneLeaseRecord, DataPlaneLeaseState};
use blaze_core::storage::{StorageSlot, TemplateStorage};
use thiserror::Error;
use uuid::Uuid;

/// First source-level provider contract understood by Blaze.
pub const PROVIDER_CONTRACT_VERSION: u16 = 1;

/// Opaque identity and contract revision of one provider instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderDescriptor {
    /// Contract revision implemented by this provider.
    pub contract_version: u16,
    /// Stable identity used to reject a response from another provider.
    pub provider_instance_id: Uuid,
}

/// Operations implemented by one provider build.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProviderCapabilities {
    /// Prepare an ordinary image-backed sandbox.
    pub images: bool,
    /// Prepare a sandbox from a published runtime template.
    pub templates: bool,
    /// Return typed, already-opened resources for a template restore.
    pub opened_restore_resources: bool,
    /// Allow Blaze to manage path-backed resources through its configured
    /// `StorageProvider`.
    ///
    /// Set this only when every `PreparedResources::PathBacked` value returned
    /// by this provider belongs to that storage provider and remains
    /// reconstructible by sandbox identifier. Blaze may then use its standard
    /// synchronization, checkpoint, hibernation, restore, and release paths
    /// when no provider-specific lifecycle extension is selected.
    pub daemon_managed_storage: bool,
}

/// Stable identifiers chosen before a provider may create resources.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestContext {
    /// Public sandbox that will own the resources.
    pub instance_id: Uuid,
    /// Idempotency key for this provider call sequence.
    pub request_id: Uuid,
    /// One public lifecycle operation spanning all provider transitions.
    pub operation_id: Uuid,
    /// Preselected lease identity, including calls whose result is unknown.
    pub lease_id: Uuid,
    /// Expected first lease generation.
    pub generation: u64,
}

/// Immutable source selected by the public control operation.
#[derive(Debug)]
pub enum PrepareSource {
    /// Allocate writable resources for an ordinary image identity.
    Image {
        /// Image identity already accepted by policy evaluation.
        image_digest: String,
    },
    /// Materialize one already-validated runtime template.
    Template(TemplateSource),
}

/// Opened template artifacts and their public image identity.
#[derive(Debug)]
pub struct TemplateSource {
    /// Image identity recorded by the validated template manifest.
    pub image_digest: String,
    /// Opened VM state, memory, and root-filesystem artifacts.
    pub storage: TemplateStorage,
}

/// Request to prepare all provider-owned resources for one sandbox.
#[derive(Debug)]
pub struct PrepareRequest {
    /// Stable idempotency and ownership context.
    pub context: RequestContext,
    /// Ordinary image or validated template input.
    pub source: PrepareSource,
    /// Required logical root-filesystem extent.
    pub root_filesystem_bytes: u64,
    /// Required logical guest-memory extent.
    pub guest_memory_bytes: u64,
}

/// Backend-visible purpose of one opened attachment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AttachmentRole {
    /// Writable root drive required by captured virtual-machine state.
    RootDrive,
    /// Writable guest-memory backend consumed by snapshot loading.
    GuestMemory,
}

/// Access mode frozen when an attachment is opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttachmentAccess {
    /// Consumer cannot modify the object.
    ReadOnly,
    /// Consumer may read and write the object.
    ReadWrite,
}

/// Sharing rule of one opened attachment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttachmentSharing {
    /// Exactly one backend may consume the object.
    Exclusive,
    /// Multiple consumers may share an immutable object.
    SharedReadOnly,
}

/// Filesystem object kind expected from descriptor metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttachmentKind {
    /// Ordinary file.
    RegularFile,
    /// Character device.
    CharacterDevice,
    /// Block device.
    BlockDevice,
}

/// One ownership-transferring resource attachment.
#[derive(Debug)]
pub struct OpenedAttachment {
    /// Purpose understood by the backend restore adapter.
    pub role: AttachmentRole,
    /// Owned descriptor transferred to Blaze for backend consumption.
    pub descriptor: OwnedFd,
    /// Declared access mode, checked again by Blaze.
    pub access: AttachmentAccess,
    /// Declared sharing rule.
    pub sharing: AttachmentSharing,
    /// Declared object kind, checked again by Blaze.
    pub kind: AttachmentKind,
    /// Logical extent exposed to the backend.
    pub logical_size_bytes: u64,
    /// Pre-provisioned path required by captured backend state, if any.
    pub consumer_path: Option<PathBuf>,
}

/// Runtime resources produced by preparation.
#[derive(Debug)]
pub enum PreparedResources {
    /// Existing file-backed runtime layout.
    PathBacked {
        /// Writable storage slot owned by this lease.
        storage: StorageSlot,
        /// Provider-owned restore payload for template preparation.
        restore_payload_dir: Option<PathBuf>,
    },
    /// Restore payload plus already-opened resources transferred by descriptor.
    OpenedRestore {
        /// Provider-owned directory containing backend VM-state payload.
        restore_payload_dir: PathBuf,
        /// Root-drive and guest-memory descriptors transferred to Blaze.
        attachments: Vec<OpenedAttachment>,
    },
    /// Resources for an in-place restore whose backend payload remains in the
    /// daemon checkpoint catalog.
    CheckpointRestore {
        /// Path-backed replacement slot, when the provider exposes files.
        storage: Option<StorageSlot>,
        /// Opened root-drive and guest-memory attachments otherwise.
        attachments: Vec<OpenedAttachment>,
    },
}

/// Durable phase of one provider resource lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseState {
    /// Resources exist but the backend has not reached readiness.
    Prepared,
    /// Backend readiness was accepted by the provider.
    Committed,
    /// Public state was persisted and final ownership was handed over.
    Finalized,
    /// Backend use ended while provider resources remain retained.
    Stopped,
    /// Provider proved that all lease resources are absent.
    Released,
    /// Resources are retained until an operator resolves an ownership conflict.
    Quarantined,
}

/// Exact identity and state of one provider-owned resource lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseBinding {
    /// Provider that issued this binding.
    pub provider_instance_id: Uuid,
    /// Stable idempotency and public ownership context.
    pub context: RequestContext,
    /// Monotonic provider state generation.
    pub generation: u64,
    /// Current provider-side state.
    pub state: LeaseState,
}

impl LeaseBinding {
    /// Convert a provider response into the implementation-neutral durable ledger.
    pub fn to_record(
        self,
        root_filesystem_bytes: u64,
        guest_memory_bytes: u64,
    ) -> DataPlaneLeaseRecord {
        DataPlaneLeaseRecord {
            provider_instance_id: self.provider_instance_id,
            request_id: self.context.request_id,
            operation_id: self.context.operation_id,
            lease_id: self.context.lease_id,
            initial_generation: self.context.generation,
            generation: self.generation,
            state: self.state.into(),
            root_filesystem_bytes,
            guest_memory_bytes,
        }
    }

    /// Rebuild a provider binding from one sandbox's durable ledger record.
    pub fn from_record(instance_id: Uuid, record: DataPlaneLeaseRecord) -> Self {
        Self {
            provider_instance_id: record.provider_instance_id,
            context: RequestContext {
                instance_id,
                request_id: record.request_id,
                operation_id: record.operation_id,
                lease_id: record.lease_id,
                generation: record.initial_generation,
            },
            generation: record.generation,
            state: record.state.into(),
        }
    }
}

impl From<LeaseState> for DataPlaneLeaseState {
    fn from(state: LeaseState) -> Self {
        match state {
            LeaseState::Prepared => Self::Prepared,
            LeaseState::Committed => Self::Committed,
            LeaseState::Finalized => Self::Finalized,
            LeaseState::Stopped => Self::Stopped,
            LeaseState::Released => Self::Released,
            LeaseState::Quarantined => Self::Quarantined,
        }
    }
}

impl From<DataPlaneLeaseState> for LeaseState {
    fn from(state: DataPlaneLeaseState) -> Self {
        match state {
            DataPlaneLeaseState::Prepared => Self::Prepared,
            DataPlaneLeaseState::Committed => Self::Committed,
            DataPlaneLeaseState::Finalized => Self::Finalized,
            DataPlaneLeaseState::Stopped => Self::Stopped,
            DataPlaneLeaseState::Released => Self::Released,
            DataPlaneLeaseState::Quarantined => Self::Quarantined,
        }
    }
}

/// Prepared resources and their exact lease binding.
#[derive(Debug)]
pub struct PreparedLease {
    /// Binding confirmed by the provider.
    pub binding: LeaseBinding,
    /// Backend resources owned by this binding.
    pub resources: PreparedResources,
}

/// Read-only query for a preselected or returned lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InspectRequest {
    /// Stable request and lease identifiers.
    pub context: RequestContext,
}

/// State observed without changing provider resources.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservedLease {
    /// Current exact provider binding.
    pub binding: LeaseBinding,
}

/// Mark a prepared backend as ready for public state publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitRequest {
    /// Prepared lease being committed.
    pub binding: LeaseBinding,
}

/// Provider-side result ready for public state publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommittedLease {
    /// Binding advanced to [`LeaseState::Committed`].
    pub binding: LeaseBinding,
}

/// Reference to the public transition persisted by Blaze.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicTransitionRef {
    /// Sandbox whose public state was changed.
    pub instance_id: Uuid,
    /// Lifecycle operation that published the state.
    pub operation_id: Uuid,
}

/// Complete provider handoff after public state is durable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinalizeRequest {
    /// Committed provider binding.
    pub binding: LeaseBinding,
    /// Matching durable public transition.
    pub public_transition: PublicTransitionRef,
}

/// Final provider ownership returned to the manager.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinalizedLease {
    /// Binding advanced to [`LeaseState::Finalized`].
    pub binding: LeaseBinding,
}

/// Compensate a preparation that has no durable public transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AbortRequest {
    /// Prepared or committed binding to release.
    pub binding: LeaseBinding,
}

/// Confirmed result of preparation compensation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AbortResult {
    /// Binding advanced to [`LeaseState::Released`].
    pub binding: LeaseBinding,
}

/// End active backend use while retaining provider cleanup state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StopRequest {
    /// Finalized binding whose backend has stopped.
    pub binding: LeaseBinding,
}

/// Provider result after active use ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoppedLease {
    /// Binding advanced to [`LeaseState::Stopped`].
    pub binding: LeaseBinding,
}

/// Release all resources after backend termination is confirmed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReleaseRequest {
    /// Stopped binding to release.
    pub binding: LeaseBinding,
}

/// Confirmed terminal result of provider cleanup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReleaseResult {
    /// Binding advanced to [`LeaseState::Released`].
    pub binding: LeaseBinding,
}

/// Opaque, immutable provider content paired with one public checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderCheckpointRef {
    /// Build-time provider instance that owns the immutable content.
    pub provider_instance_id: Uuid,
    /// UUID portion of the public checkpoint that owns this reference.
    pub public_checkpoint_id: Uuid,
    /// Opaque provider-local immutable content reference.
    pub reference_id: Uuid,
    /// SHA-256 identity of the provider's canonical content manifest.
    pub content_digest: String,
    /// Opaque provider reference of the public parent checkpoint.
    pub parent_reference_id: Option<Uuid>,
    /// Finalized lease frozen by this capture.
    pub source_lease_id: Uuid,
    /// Source lease generation after the provider accepted capture.
    pub source_generation: u64,
    /// Whether this reference owns the writable root-filesystem view.
    pub root_filesystem: bool,
    /// Whether this reference owns the guest-memory view.
    pub guest_memory: bool,
}

impl ProviderCheckpointRef {
    /// Convert the result into an implementation-neutral durable ownership record.
    pub fn to_record(&self) -> ProviderCheckpointRecord {
        ProviderCheckpointRecord {
            provider_instance_id: self.provider_instance_id,
            public_checkpoint_id: self.public_checkpoint_id,
            reference_id: self.reference_id,
            content_digest: self.content_digest.clone(),
            parent_reference_id: self.parent_reference_id,
            source_lease_id: self.source_lease_id,
            source_generation: self.source_generation,
            root_filesystem: self.root_filesystem,
            guest_memory: self.guest_memory,
        }
    }

    /// Reconstruct the source-level provider reference from a validated ledger.
    pub fn from_record(record: &ProviderCheckpointRecord) -> Self {
        Self {
            provider_instance_id: record.provider_instance_id,
            public_checkpoint_id: record.public_checkpoint_id,
            reference_id: record.reference_id,
            content_digest: record.content_digest.clone(),
            parent_reference_id: record.parent_reference_id,
            source_lease_id: record.source_lease_id,
            source_generation: record.source_generation,
            root_filesystem: record.root_filesystem,
            guest_memory: record.guest_memory,
        }
    }
}

/// Freeze provider-owned state while the backend is quiesced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderCheckpointRequest {
    /// Finalized active lease captured while backend writes are stopped.
    pub binding: LeaseBinding,
    /// UUID portion of the already allocated public `ckpt-...` identity.
    pub checkpoint_id: Uuid,
    /// Exact parent reference selected from the public checkpoint head.
    pub parent: Option<ProviderCheckpointRef>,
}

/// Provider result for one immutable checkpoint capture.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointSubmission {
    /// Active lease advanced by exactly one generation and still finalized.
    pub binding: LeaseBinding,
    /// Immutable content reference paired with daemon-owned backend artifacts.
    pub checkpoint: ProviderCheckpointRef,
}

/// Prepare an independent replacement lease from immutable checkpoint data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreCheckpointRequest {
    /// Fresh identity for the independent replacement lease.
    pub context: RequestContext,
    /// Immutable provider content selected by the verified public catalog.
    pub checkpoint: ProviderCheckpointRef,
    /// Required writable root-filesystem extent.
    pub root_filesystem_bytes: u64,
    /// Required guest-memory extent.
    pub guest_memory_bytes: u64,
}

/// Retire provider content after the public catalog no longer references it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetireCheckpointRequest {
    /// Build-time provider instance expected to own the content.
    pub provider_instance_id: Uuid,
    /// Public checkpoint identity used to make unknown captures idempotent.
    pub public_checkpoint_id: Uuid,
    /// Absent only when capture had an unknown outcome before a reference was returned.
    pub reference_id: Option<Uuid>,
    /// Idempotency identity chosen by Blaze for this retirement attempt.
    pub operation_id: Uuid,
}

/// Confirm that a provider checkpoint reference no longer owns content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetireCheckpointResult {
    /// Public checkpoint identity accepted by the provider.
    pub public_checkpoint_id: Uuid,
    /// Exact opaque reference retired, or absent for an unknown capture.
    pub reference_id: Option<Uuid>,
    /// True when content was removed by this call, false when already absent.
    pub retired: bool,
}

/// Provider-independent failures mapped into stable Blaze diagnostics.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum ProviderError {
    /// Required runtime dependency is unavailable.
    #[error("data-plane provider is unavailable")]
    Unavailable,
    /// Request conflicts with retained provider state.
    #[error("data-plane provider state conflicts with the request")]
    Conflict,
    /// Mutation may have occurred and requires inspection or reconciliation.
    #[error("data-plane provider operation outcome is unknown")]
    OutcomeUnknown,
    /// Provider does not implement the requested generic operation.
    #[error("data-plane provider operation is unsupported")]
    Unsupported,
    /// Provider returned a value that violates the public contract.
    #[error("data-plane provider returned an invalid response")]
    InvalidResponse,
    /// Immutable source or combined contract is incompatible.
    #[error("data-plane provider is incompatible with the request")]
    Incompatible,
}

/// Start one consistent, paged view of all leases owned by a provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BeginInventoryRequest {
    /// Maximum entries Blaze will accept in one page.
    pub page_size: u32,
}

/// Stable provider inventory frozen for one traversal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InventorySnapshot {
    /// Provider that owns this snapshot.
    pub provider_instance_id: Uuid,
    /// Opaque snapshot identity used only in follow-up page requests.
    pub snapshot_id: Uuid,
}

/// Request one page from a previously frozen inventory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InventoryPageRequest {
    pub snapshot_id: Uuid,
    pub cursor: Option<String>,
    pub page_size: u32,
}

/// One provider lease visible in the frozen inventory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InventoryLease {
    pub binding: LeaseBinding,
}

/// Bounded inventory page. A missing cursor completes the traversal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InventoryPage {
    pub leases: Vec<InventoryLease>,
    pub next_cursor: Option<String>,
}

/// Safe convergence action selected after comparing all ownership ledgers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileAction {
    /// Retain an exact live lease and associate it with the proven backend.
    Adopt {
        backend_process: BackendProcessIdentity,
    },
    /// Retain resources without allowing them to serve traffic or be reused.
    Quarantine,
    /// Release resources whose public owner is terminal or absent.
    Release,
}

/// Reconcile one observed provider lease against a public expectation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconcileRequest {
    pub expected: Option<LeaseBinding>,
    pub observed: LeaseBinding,
    pub action: ReconcileAction,
}

/// Provider-confirmed convergence result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconcileResult {
    pub binding: LeaseBinding,
}

/// Optional lease inventory and restart-convergence extension.
#[async_trait]
pub trait DataPlaneInventory: DataPlaneProvider {
    async fn begin_inventory(
        &self,
        request: BeginInventoryRequest,
    ) -> Result<InventorySnapshot, ProviderError>;

    async fn inventory_page(
        &self,
        request: InventoryPageRequest,
    ) -> Result<InventoryPage, ProviderError>;

    async fn reconcile(&self, request: ReconcileRequest) -> Result<ReconcileResult, ProviderError>;
}

/// Optional immutable checkpoint capture, restore, and retirement extension.
#[async_trait]
pub trait DataPlaneCheckpoint: DataPlaneProvider {
    /// Freeze immutable provider content at the backend's capture boundary.
    async fn checkpoint(
        &self,
        request: ProviderCheckpointRequest,
    ) -> Result<CheckpointSubmission, ProviderError>;

    /// Prepare a new exclusive lease from immutable checkpoint content.
    async fn restore_checkpoint(
        &self,
        request: RestoreCheckpointRequest,
    ) -> Result<PreparedLease, ProviderError>;

    /// Idempotently release content after its public owner is removed.
    async fn retire_checkpoint(
        &self,
        request: RetireCheckpointRequest,
    ) -> Result<RetireCheckpointResult, ProviderError>;
}

/// Source-level data-plane provider compiled into one Blaze daemon binary.
#[async_trait]
pub trait DataPlaneProvider: Send + Sync {
    /// Return the provider identity and exact source-contract revision.
    fn descriptor(&self) -> ProviderDescriptor;

    /// Return optional operations implemented by this provider.
    fn capabilities(&self) -> ProviderCapabilities;

    /// Return restart reconciliation support when this provider implements it.
    fn inventory(&self) -> Option<&dyn DataPlaneInventory> {
        None
    }

    /// Return checkpoint support when provider-owned immutable content exists.
    fn checkpoints(&self) -> Option<&dyn DataPlaneCheckpoint> {
        None
    }

    /// Check prerequisites without allocating sandbox resources.
    async fn probe(&self) -> Result<(), ProviderError>;

    /// Create or materialize one preselected resource lease.
    async fn prepare(&self, request: PrepareRequest) -> Result<PreparedLease, ProviderError>;

    /// Observe one lease without creating, stopping, or releasing resources.
    async fn inspect(&self, request: InspectRequest) -> Result<ObservedLease, ProviderError>;

    /// Mark backend readiness before public state is published.
    async fn commit(&self, request: CommitRequest) -> Result<CommittedLease, ProviderError>;

    /// Complete ownership handoff after public state is durable.
    async fn finalize(&self, request: FinalizeRequest) -> Result<FinalizedLease, ProviderError>;

    /// Compensate a preparation that has no durable public transition.
    async fn abort(&self, request: AbortRequest) -> Result<AbortResult, ProviderError>;

    /// Retain cleanup state after backend use ends.
    async fn stop(&self, request: StopRequest) -> Result<StoppedLease, ProviderError>;

    /// Prove that all resources owned by a stopped lease are absent.
    async fn release(&self, request: ReleaseRequest) -> Result<ReleaseResult, ProviderError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_lease_round_trip_preserves_every_identity() {
        let instance_id = Uuid::new_v4();
        let binding = LeaseBinding {
            provider_instance_id: Uuid::new_v4(),
            context: RequestContext {
                instance_id,
                request_id: Uuid::new_v4(),
                operation_id: Uuid::new_v4(),
                lease_id: Uuid::new_v4(),
                generation: 7,
            },
            generation: 11,
            state: LeaseState::Finalized,
        };

        let record = binding.to_record(64 * 1024 * 1024, 512 * 1024 * 1024);

        assert_eq!(LeaseBinding::from_record(instance_id, record), binding);
        assert_eq!(record.initial_generation, 7);
        assert_eq!(record.generation, 11);
    }
}
