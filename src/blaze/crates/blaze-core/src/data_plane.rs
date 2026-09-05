// SPDX-License-Identifier: Apache-2.0
//! Durable, provider-independent data-plane identities.
//!
//! These records define the portable side of the ownership ledger. Extension
//! implementations map their resource model into these identities so the daemon
//! can compare persisted intent with a provider inventory after restart.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Durable lifecycle phase of one provider-owned resource lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DataPlaneLeaseState {
    Prepared,
    Committed,
    Finalized,
    Stopped,
    Released,
    /// Resources are retained because ownership or safety cannot be proved.
    Quarantined,
}

/// Provider-independent identity of one resource lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataPlaneLeaseRecord {
    /// Provider process or durable provider store that issued the lease.
    pub provider_instance_id: Uuid,
    /// Idempotency key of the preparation request.
    pub request_id: Uuid,
    /// Public lifecycle operation that owns the transition.
    pub operation_id: Uuid,
    /// Stable provider lease identity.
    pub lease_id: Uuid,
    /// Expected generation chosen before the first provider mutation.
    pub initial_generation: u64,
    /// Monotonic provider-side state generation.
    pub generation: u64,
    /// Last provider state accepted by Blaze.
    pub state: DataPlaneLeaseState,
    /// Logical root-filesystem extent promised to the backend.
    pub root_filesystem_bytes: u64,
    /// Logical guest-memory extent promised to the backend.
    pub guest_memory_bytes: u64,
}

/// Public identity of immutable provider content retained while a sandbox sleeps.
///
/// Extension implementations map retained content into the opaque identity and
/// integrity fields below. Blaze can therefore resume or retire the same object
/// without depending on one implementation's storage model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataPlaneSuspensionRecord {
    /// Build-time provider instance that owns the immutable content.
    pub provider_instance_id: Uuid,
    /// Stable identity chosen before the provider may create suspension content.
    pub suspension_id: Uuid,
    /// Opaque provider-local immutable content reference.
    pub reference_id: Uuid,
    /// SHA-256 identity of the provider's canonical content manifest.
    pub content_digest: String,
    /// Active lease from which the suspension content was frozen.
    pub source_lease_id: Uuid,
    /// Source lease generation after the provider accepted suspension.
    pub source_generation: u64,
    /// Whether the provider owns the retained root-filesystem view.
    pub root_filesystem: bool,
    /// Whether the provider owns the retained guest-memory view.
    pub guest_memory: bool,
    /// Logical root-filesystem extent required by a fresh resume lease.
    pub root_filesystem_bytes: u64,
    /// Logical guest-memory extent required by a fresh resume lease.
    pub guest_memory_bytes: u64,
}

/// Linux process identity strong enough to detect PID reuse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendProcessIdentity {
    /// Process identifier observed when ownership was published.
    pub pid: u32,
    /// `/proc/<pid>/stat` start-time field, measured in clock ticks.
    pub start_time_ticks: u64,
}

/// Durable backend shape needed to adopt a live process after restart.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendRuntimeRecord {
    /// Process identity; absent for backends that cannot be adopted.
    pub process: Option<BackendProcessIdentity>,
    /// Backend version frozen when the process started.
    pub version: Option<String>,
    /// Whether the owner exposes a guest-agent transport.
    pub guest_transport: bool,
    /// Whether the owner holds a host network slot.
    pub network_slot: bool,
    /// Whether console output is retained in the runtime directory.
    pub console_log: bool,
}
