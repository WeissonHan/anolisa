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

/// Source-level data-plane provider compiled into one Blaze daemon binary.
#[async_trait]
pub trait DataPlaneProvider: Send + Sync {
    /// Return the provider identity and exact source-contract revision.
    fn descriptor(&self) -> ProviderDescriptor;

    /// Return optional operations implemented by this provider.
    fn capabilities(&self) -> ProviderCapabilities;

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
