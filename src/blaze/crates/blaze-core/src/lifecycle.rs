// SPDX-License-Identifier: Apache-2.0
//! Sandbox lifecycle state machine + JSON persistence.

use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::backend::BackendKind;
use crate::error::{BlazeError, Result};
use crate::policy::WorkloadClass;

/// All known states. Transitions are enforced by [`SandboxInstance::transition`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SandboxState {
    Pending,
    Creating,
    Running,
    Paused,
    Checkpointed,
    Reset,
    Warm,
    Destroyed,
}

impl SandboxState {
    pub const fn as_str(&self) -> &'static str {
        match self {
            SandboxState::Pending => "pending",
            SandboxState::Creating => "creating",
            SandboxState::Running => "running",
            SandboxState::Paused => "paused",
            SandboxState::Checkpointed => "checkpointed",
            SandboxState::Reset => "reset",
            SandboxState::Warm => "warm",
            SandboxState::Destroyed => "destroyed",
        }
    }
}

impl std::fmt::Display for SandboxState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Whether a request entered `creating` from cold boot, a warm pool
/// reuse, or a checkpoint restore — used as the primary latency /
/// capacity SLO dimension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StartPath {
    Cold,
    Warm,
    Restored,
}

/// Durable knowledge about whether a backend may still own a live process.
///
/// `Unknown` is the safe default for state written by older daemon versions.
/// Recovery must confirm termination for both `Unknown` and `Starting`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BackendOwnership {
    #[default]
    Unknown,
    NotStarted,
    Starting,
    Running,
    Stopped,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxInstance {
    pub id: Uuid,
    pub state: SandboxState,
    pub backend: BackendKind,
    pub workload_class: WorkloadClass,
    pub image_digest: String,
    /// Image reference the rootfs was provisioned from, e.g.
    /// `docker.io/library/alpine:latest`. `image_digest` stays the identity
    /// used for policy matching and warm-pool keying; this is the locator a
    /// backend needs to materialise the filesystem again.
    #[serde(default)]
    pub image: Option<String>,
    pub start_path: StartPath,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub policy_name: String,
    /// Last durably known backend ownership state.
    #[serde(default)]
    pub backend_ownership: BackendOwnership,
    /// Snapshot this instance was last restored or hatched from.
    #[serde(default)]
    pub restored_from: Option<Uuid>,
    /// Most recent snapshot taken of this instance. While `checkpointed`
    /// this is the image a restore defaults to.
    #[serde(default)]
    pub last_snapshot: Option<Uuid>,
}

impl SandboxInstance {
    /// Create a new instance in [`SandboxState::Pending`] with `start_path`
    /// pre-classified by the caller (cold for fresh boots, warm for
    /// pool reuses, restored when hatching from a snapshot).
    pub fn new(
        backend: BackendKind,
        workload_class: WorkloadClass,
        image_digest: String,
        start_path: StartPath,
        policy_name: String,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            state: SandboxState::Pending,
            backend,
            workload_class,
            image_digest,
            image: None,
            start_path,
            created_at: now,
            updated_at: now,
            policy_name,
            backend_ownership: BackendOwnership::NotStarted,
            restored_from: None,
            last_snapshot: None,
        }
    }

    /// Apply a state transition. Returns
    /// [`BlazeError::InvalidStateTransition`] when the move is not part
    /// of the lifecycle state graph.
    pub fn transition(&mut self, target: SandboxState) -> Result<()> {
        if !is_valid_transition(self.state, target) {
            return Err(BlazeError::InvalidStateTransition {
                from: self.state.to_string(),
                to: target.to_string(),
            });
        }
        let prev = self.state;
        self.state = target;
        self.updated_at = Utc::now();
        // entering `creating` re-classifies the start path: warm-pool
        // reuse goes warm → creating, restores come from checkpointed or
        // declare themselves at construction, fresh boots go
        // pending → creating.
        if target == SandboxState::Creating {
            self.start_path = match prev {
                SandboxState::Warm => StartPath::Warm,
                SandboxState::Checkpointed => StartPath::Restored,
                // A hatch-from-snapshot request declares `Restored` before
                // its first transition; do not downgrade it here.
                SandboxState::Pending if self.start_path == StartPath::Restored => {
                    StartPath::Restored
                }
                _ => StartPath::Cold,
            };
        }
        tracing::info!(
            instance = %self.id,
            from = %prev,
            to = %target,
            backend = %self.backend,
            class = %self.workload_class,
            "sandbox state transition"
        );
        Ok(())
    }

    /// Refresh `updated_at` after a metadata-only change, such as recording
    /// a live snapshot, that does not move the state machine.
    pub fn touch(&mut self) {
        self.updated_at = Utc::now();
    }

    /// Persist this instance to `{state_dir}/{id}/state.json`. Atomic
    /// rename via `state.json.tmp` to avoid torn reads on daemon restart.
    pub fn persist(&self, state_dir: &Path) -> Result<()> {
        let dir = state_dir.join(self.id.to_string());
        fs::create_dir_all(&dir)?;
        let final_path = dir.join("state.json");
        let tmp_path = dir.join("state.json.tmp");
        let json = serde_json::to_vec_pretty(self)?;
        fs::write(&tmp_path, &json)?;
        fs::rename(&tmp_path, &final_path)?;
        Ok(())
    }

    /// Reload an instance previously persisted via [`Self::persist`].
    pub fn load(state_dir: &Path, id: Uuid) -> Result<Self> {
        let path: PathBuf = state_dir.join(id.to_string()).join("state.json");
        let raw = fs::read(&path)?;
        let instance: SandboxInstance = serde_json::from_slice(&raw)?;
        Ok(instance)
    }
}

fn is_valid_transition(from: SandboxState, to: SandboxState) -> bool {
    use SandboxState::{Checkpointed, Creating, Destroyed, Paused, Pending, Reset, Running, Warm};
    if to == Destroyed {
        // `* → destroyed` is always valid (terminal sink).
        return from != Destroyed;
    }
    match (from, to) {
        (Pending, Creating) => true,
        (Creating, Running) => true,
        (Running, Paused) => true,
        (Running, Reset) => true,
        (Paused, Checkpointed) => true,
        (Paused, Running) => true,        // resume
        (Checkpointed, Creating) => true, // restore in place
        (Reset, Warm) => true,
        (Warm, Creating) => true, // pool reuse / warm path
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh() -> SandboxInstance {
        SandboxInstance::new(
            BackendKind::KataFc,
            WorkloadClass::AgentRl,
            "sha256:deadbeef".into(),
            StartPath::Cold,
            "agent-rl-default".into(),
        )
    }

    #[test]
    fn happy_path_cold() {
        let mut inst = fresh();
        for target in [
            SandboxState::Creating,
            SandboxState::Running,
            SandboxState::Paused,
            SandboxState::Checkpointed,
            SandboxState::Destroyed,
        ] {
            inst.transition(target).expect("legal transition");
            assert_eq!(inst.state, target);
        }
    }

    #[test]
    fn happy_path_warm_reuse() {
        let mut inst = fresh();
        inst.transition(SandboxState::Creating).expect("ok");
        inst.transition(SandboxState::Running).expect("ok");
        inst.transition(SandboxState::Reset).expect("ok");
        inst.transition(SandboxState::Warm).expect("ok");
        // warm → creating must flip start_path to Warm.
        inst.transition(SandboxState::Creating).expect("ok");
        assert_eq!(inst.start_path, StartPath::Warm);
    }

    #[test]
    fn destroy_is_always_legal_except_from_destroyed() {
        let mut inst = fresh();
        inst.transition(SandboxState::Destroyed).expect("ok");
        let again = inst.transition(SandboxState::Destroyed);
        assert!(matches!(
            again,
            Err(BlazeError::InvalidStateTransition { .. })
        ));
    }

    #[test]
    fn illegal_pending_to_running() {
        let mut inst = fresh();
        let err = inst.transition(SandboxState::Running).expect_err("illegal");
        assert!(matches!(err, BlazeError::InvalidStateTransition { .. }));
    }

    #[test]
    fn illegal_running_to_warm() {
        let mut inst = fresh();
        inst.transition(SandboxState::Creating).expect("ok");
        inst.transition(SandboxState::Running).expect("ok");
        let err = inst.transition(SandboxState::Warm).expect_err("illegal");
        assert!(matches!(err, BlazeError::InvalidStateTransition { .. }));
    }

    #[test]
    fn illegal_warm_to_running() {
        let mut inst = fresh();
        inst.transition(SandboxState::Creating).expect("ok");
        inst.transition(SandboxState::Running).expect("ok");
        inst.transition(SandboxState::Reset).expect("ok");
        inst.transition(SandboxState::Warm).expect("ok");
        let err = inst.transition(SandboxState::Running).expect_err("illegal");
        assert!(matches!(err, BlazeError::InvalidStateTransition { .. }));
    }

    #[test]
    fn persist_then_load_round_trip() {
        let tmp = tempfile::tempdir().expect("tmp");
        let mut inst = fresh();
        inst.transition(SandboxState::Creating).expect("ok");
        inst.persist(tmp.path()).expect("persist");

        let loaded = SandboxInstance::load(tmp.path(), inst.id).expect("load");
        assert_eq!(loaded.id, inst.id);
        assert_eq!(loaded.state, SandboxState::Creating);
        assert_eq!(loaded.policy_name, inst.policy_name);
    }

    #[test]
    fn checkpointed_restores_into_creating_with_restored_path() {
        let mut inst = fresh();
        for target in [
            SandboxState::Creating,
            SandboxState::Running,
            SandboxState::Paused,
            SandboxState::Checkpointed,
        ] {
            inst.transition(target).expect("legal transition");
        }

        inst.transition(SandboxState::Creating).expect("restore");
        assert_eq!(inst.start_path, StartPath::Restored);
        inst.transition(SandboxState::Running).expect("restored");
        assert_eq!(inst.state, SandboxState::Running);
    }

    #[test]
    fn pending_to_creating_preserves_declared_restored_path() {
        let mut inst = SandboxInstance::new(
            BackendKind::Gvisor,
            WorkloadClass::AgentTool,
            "sha256:deadbeef".into(),
            StartPath::Restored,
            "agent-tool-default".into(),
        );
        inst.transition(SandboxState::Creating).expect("hatch");
        assert_eq!(inst.start_path, StartPath::Restored);
    }

    #[test]
    fn checkpointed_remains_a_dead_end_apart_from_restore_and_destroy() {
        for target in [
            SandboxState::Running,
            SandboxState::Paused,
            SandboxState::Warm,
            SandboxState::Reset,
        ] {
            let mut inst = fresh();
            for hop in [
                SandboxState::Creating,
                SandboxState::Running,
                SandboxState::Paused,
                SandboxState::Checkpointed,
            ] {
                inst.transition(hop).expect("legal transition");
            }
            let err = inst.transition(target).expect_err("illegal");
            assert!(matches!(err, BlazeError::InvalidStateTransition { .. }));
        }
    }

    #[test]
    fn snapshot_linkage_survives_persist_round_trip() {
        let tmp = tempfile::tempdir().expect("tmp");
        let mut inst = fresh();
        let snapshot = Uuid::new_v4();
        inst.last_snapshot = Some(snapshot);
        inst.restored_from = Some(snapshot);
        inst.image = Some("docker.io/library/alpine:latest".to_string());
        inst.persist(tmp.path()).expect("persist");

        let loaded = SandboxInstance::load(tmp.path(), inst.id).expect("load");
        assert_eq!(loaded.last_snapshot, Some(snapshot));
        assert_eq!(loaded.restored_from, Some(snapshot));
        assert_eq!(
            loaded.image.as_deref(),
            Some("docker.io/library/alpine:latest")
        );
    }

    #[test]
    fn legacy_state_json_without_optional_fields_loads() {
        let raw = r#"{
            "id": "3f2b1c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d",
            "state": "running",
            "backend": "gvisor",
            "workload_class": "agent-tool",
            "image_digest": "sha256:deadbeef",
            "start_path": "cold",
            "created_at": "2026-07-31T09:31:10.280195680Z",
            "updated_at": "2026-07-31T09:31:10.282515945Z",
            "policy_name": "agent-tool-default"
        }"#;

        let loaded: SandboxInstance = serde_json::from_str(raw).expect("legacy state loads");
        assert_eq!(loaded.state, SandboxState::Running);
        assert_eq!(loaded.start_path, StartPath::Cold);
        assert_eq!(loaded.backend_ownership, BackendOwnership::Unknown);
        assert_eq!(loaded.last_snapshot, None);
        assert_eq!(loaded.restored_from, None);
        assert_eq!(loaded.image, None);
    }
}
