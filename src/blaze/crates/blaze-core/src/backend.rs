// SPDX-License-Identifier: Apache-2.0
//! Sandbox backend kinds + selection / fallback.

use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{BlazeError, Result};
use crate::policy::{BackendConfigs, VmConfig};
use crate::storage::StorageSlot;

/// All backends that blaze v0.1 knows about. Each backend maps to a
/// binary path configured in the daemon `[backends]` section.
///
/// `LinuxSandbox` and `Landlock` are recognized for policy deserialization
/// but are not yet backed by a `BackendSpawner` implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BackendKind {
    Firecracker,
    Gvisor,
    GvisorSubstrate,
    Rund,
    KataFc,
    KataClh,
    KataQemu,
    Runc,
    Bubblewrap,
    LinuxSandbox,
    Landlock,
    Mock,
}

impl BackendKind {
    /// Stable string label used in policy files / metrics / config keys.
    pub const fn as_str(&self) -> &'static str {
        match self {
            BackendKind::Firecracker => "firecracker",
            BackendKind::Gvisor => "gvisor",
            BackendKind::GvisorSubstrate => "gvisor-substrate",
            BackendKind::Rund => "rund",
            BackendKind::KataFc => "kata-fc",
            BackendKind::KataClh => "kata-clh",
            BackendKind::KataQemu => "kata-qemu",
            BackendKind::Runc => "runc",
            BackendKind::Bubblewrap => "bubblewrap",
            BackendKind::LinuxSandbox => "linux-sandbox",
            BackendKind::Landlock => "landlock",
            BackendKind::Mock => "mock",
        }
    }
}

impl fmt::Display for BackendKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for BackendKind {
    type Err = BlazeError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "firecracker" => Ok(BackendKind::Firecracker),
            "gvisor" => Ok(BackendKind::Gvisor),
            "gvisor-substrate" => Ok(BackendKind::GvisorSubstrate),
            "rund" => Ok(BackendKind::Rund),
            "kata-fc" => Ok(BackendKind::KataFc),
            "kata-clh" => Ok(BackendKind::KataClh),
            "kata-qemu" => Ok(BackendKind::KataQemu),
            "runc" => Ok(BackendKind::Runc),
            "bubblewrap" => Ok(BackendKind::Bubblewrap),
            "linux-sandbox" => Ok(BackendKind::LinuxSandbox),
            "landlock" => Ok(BackendKind::Landlock),
            "mock" => Ok(BackendKind::Mock),
            other => Err(BlazeError::PolicyEvalError {
                reason: format!("unknown backend kind: {other}"),
            }),
        }
    }
}

/// Complete input for starting a backend instance.
#[derive(Debug, Clone)]
pub struct SpawnRequest {
    /// Stable sandbox identifier.
    pub instance_id: Uuid,
    /// Provider-owned runtime directory.
    pub run_dir: PathBuf,
    /// Backend executable selected during daemon startup.
    pub binary_path: PathBuf,
    /// Storage resources owned by this sandbox.
    pub storage: StorageSlot,
    /// Backend-specific policy configuration.
    pub backend: BackendConfigs,
    /// Generic VM resource configuration.
    pub vm: Option<VmConfig>,
}

/// Compression applied to a checkpoint payload. Values map onto
/// `runsc checkpoint --compression`; backends that cannot compress
/// ignore anything other than [`SnapshotCompression::None`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum SnapshotCompression {
    #[default]
    None,
    FlateBestSpeed,
}

/// Complete input for capturing a checkpoint of a running sandbox.
#[derive(Debug, Clone)]
pub struct SnapshotRequest {
    /// Sandbox being captured.
    pub instance_id: Uuid,
    /// Payload root. The backend owns this entire subtree and chooses its
    /// internal layout; the daemon only supplies the directory.
    pub snapshot_dir: PathBuf,
    /// Keep the sandbox running after the payload is written.
    pub leave_running: bool,
    pub compression: SnapshotCompression,
}

/// Backend-reported outcome of a completed checkpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotArtifacts {
    /// Backend that wrote the payload. Only the same kind can restore it.
    pub backend: BackendKind,
    /// Bytes written under the payload root, for store accounting.
    pub size_bytes: u64,
    /// Whether the sandbox is still running. Authoritative over
    /// [`SnapshotRequest::leave_running`]: a backend may be forced to
    /// hibernate even when a live snapshot was requested.
    pub left_running: bool,
}

/// Complete input for re-establishing a sandbox from a checkpoint.
#[derive(Debug, Clone)]
pub struct RestoreRequest {
    /// Instance that will own the restored sandbox. Differs from the
    /// snapshot's source instance when hatching a new sandbox.
    pub instance_id: Uuid,
    /// Backend kind recorded in the snapshot; must match the spawner.
    pub kind: BackendKind,
    /// Provider-owned runtime directory.
    pub run_dir: PathBuf,
    pub binary_path: PathBuf,
    /// Payload root previously handed to [`SnapshotRequest::snapshot_dir`].
    pub snapshot_dir: PathBuf,
    pub storage: StorageSlot,
    pub backend: BackendConfigs,
    pub vm: Option<VmConfig>,
}

/// Probed availability of a single backend on this host.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackendStatus {
    pub kind: BackendKind,
    pub available: bool,
    #[serde(default)]
    pub version: Option<String>,
}

/// Walk `priority` in order and return the first backend that is marked
/// available. Returns [`BlazeError::BackendUnavailable`] when no entry in
/// `priority` is available.
pub fn select_backend(
    priority: &[BackendKind],
    available: &[BackendStatus],
) -> Result<BackendKind> {
    for kind in priority {
        if available
            .iter()
            .any(|status| status.kind == *kind && status.available)
        {
            tracing::info!(backend = %kind, "selected backend");
            return Ok(*kind);
        }
        tracing::warn!(backend = %kind, "backend not available, falling back");
    }

    let requested = priority.iter().map(|b| b.as_str().to_string()).collect();
    let available = available
        .iter()
        .filter(|s| s.available)
        .map(|s| s.kind.as_str().to_string())
        .collect();
    Err(BlazeError::BackendUnavailable {
        requested,
        available,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_str() {
        for kind in [
            BackendKind::Firecracker,
            BackendKind::Gvisor,
            BackendKind::GvisorSubstrate,
            BackendKind::Rund,
            BackendKind::KataFc,
            BackendKind::KataClh,
            BackendKind::KataQemu,
            BackendKind::Runc,
            BackendKind::Bubblewrap,
            BackendKind::LinuxSandbox,
            BackendKind::Landlock,
            BackendKind::Mock,
        ] {
            let s = kind.as_str();
            let parsed: BackendKind = s.parse().expect("round-trip");
            assert_eq!(parsed, kind);
        }
    }

    #[test]
    fn select_picks_first_available() {
        let priority = vec![
            BackendKind::Firecracker,
            BackendKind::Gvisor,
            BackendKind::Bubblewrap,
        ];
        let available = vec![
            BackendStatus {
                kind: BackendKind::Firecracker,
                available: false,
                version: None,
            },
            BackendStatus {
                kind: BackendKind::Gvisor,
                available: true,
                version: Some("20260601".into()),
            },
            BackendStatus {
                kind: BackendKind::Bubblewrap,
                available: true,
                version: None,
            },
        ];
        let chosen = select_backend(&priority, &available).expect("selects");
        assert_eq!(chosen, BackendKind::Gvisor);
    }

    #[test]
    fn select_errors_when_none_available() {
        let priority = vec![BackendKind::Firecracker];
        let available = vec![BackendStatus {
            kind: BackendKind::Firecracker,
            available: false,
            version: None,
        }];
        let err = select_backend(&priority, &available).expect_err("must fail");
        assert!(matches!(err, BlazeError::BackendUnavailable { .. }));
    }
}
