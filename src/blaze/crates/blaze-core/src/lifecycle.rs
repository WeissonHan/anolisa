// SPDX-License-Identifier: Apache-2.0
//! Sandbox lifecycle state machine + JSON persistence.

use std::fs::{self, File};
use std::io::Write;
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
    RecoveryRequired,
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
            SandboxState::RecoveryRequired => "recovery-required",
            SandboxState::Reset => "reset",
            SandboxState::Warm => "warm",
            SandboxState::Destroyed => "destroyed",
        }
    }
}

/// Persisted multi-step operation used for crash diagnosis and recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OperationKind {
    /// Sandbox creation is acquiring resources or starting a backend.
    Create,
    /// A point-in-time checkpoint is being captured and published.
    Checkpoint,
    /// Runtime resources are being destroyed.
    Destroy,
}

impl OperationKind {
    pub const fn as_str(&self) -> &'static str {
        match self {
            OperationKind::Create => "create",
            OperationKind::Checkpoint => "checkpoint",
            OperationKind::Destroy => "destroy",
        }
    }
}

impl std::fmt::Display for OperationKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Durable boundary reached by a multi-step lifecycle operation.
///
/// The journal keeps this separate from [`SandboxState`]: state describes
/// externally visible runtime availability, while the phase identifies which
/// checkpoint resources may already have been published after interruption.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OperationPhase {
    /// A staging directory exists, but the backend has not been paused.
    CheckpointPreparing,
    /// The backend is paused while snapshot artifacts are being written.
    CheckpointPaused,
    /// A complete checkpoint directory is visible, but HEAD is unchanged.
    CheckpointPublished,
    /// HEAD references the checkpoint; runtime resume is not yet committed.
    CheckpointHeadUpdated,
}

impl OperationPhase {
    const fn as_str(self) -> &'static str {
        match self {
            OperationPhase::CheckpointPreparing => "checkpoint-preparing",
            OperationPhase::CheckpointPaused => "checkpoint-paused",
            OperationPhase::CheckpointPublished => "checkpoint-published",
            OperationPhase::CheckpointHeadUpdated => "checkpoint-head-updated",
        }
    }

    const fn rank(self) -> u8 {
        match self {
            OperationPhase::CheckpointPreparing => 0,
            OperationPhase::CheckpointPaused => 1,
            OperationPhase::CheckpointPublished => 2,
            OperationPhase::CheckpointHeadUpdated => 3,
        }
    }
}

/// Durable journal entry for one active lifecycle operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationJournal {
    /// Operation being performed.
    pub kind: OperationKind,
    /// UTC time at which the operation became externally visible.
    pub started_at: DateTime<Utc>,
    /// Checkpoint selected by this operation, when applicable.
    #[serde(default)]
    pub checkpoint_id: Option<String>,
    /// Last durably committed operation boundary.
    #[serde(default)]
    pub phase: Option<OperationPhase>,
}

impl std::fmt::Display for SandboxState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Whether a request entered `creating` from cold boot or via a warm
/// pool reuse — used as the primary latency / capacity SLO dimension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StartPath {
    Cold,
    Warm,
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
    pub start_path: StartPath,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub policy_name: String,
    /// Last durably known backend ownership state.
    #[serde(default)]
    pub backend_ownership: BackendOwnership,
    /// Active multi-step operation, if any.
    #[serde(default)]
    pub operation: Option<OperationJournal>,
    /// Last checkpoint whose capture completed and returned the sandbox to running.
    #[serde(default)]
    pub last_checkpoint: Option<String>,
}

impl SandboxInstance {
    /// Create a new instance in [`SandboxState::Pending`] with `start_path`
    /// pre-classified by the caller (cold for fresh boots, warm for
    /// pool reuses).
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
            start_path,
            created_at: now,
            updated_at: now,
            policy_name,
            backend_ownership: BackendOwnership::NotStarted,
            operation: None,
            last_checkpoint: None,
        }
    }

    /// Record a new operation before starting its first owned-resource
    /// mutation. An unfinished journal must be recovered rather than silently
    /// replaced by a later request.
    pub fn begin_operation(&mut self, kind: OperationKind) -> Result<()> {
        if let Some(active) = &self.operation {
            return Err(BlazeError::OperationInProgress {
                active: active.kind.to_string(),
                requested: kind.to_string(),
            });
        }
        self.operation = Some(OperationJournal {
            kind,
            started_at: Utc::now(),
            checkpoint_id: None,
            phase: None,
        });
        self.updated_at = Utc::now();
        Ok(())
    }

    /// Record checkpoint intent before pausing the backend.
    pub fn begin_checkpoint_operation(&mut self, checkpoint_id: String) -> Result<()> {
        self.begin_operation(OperationKind::Checkpoint)?;
        let operation = self
            .operation
            .as_mut()
            .expect("begin_operation installs a journal");
        operation.checkpoint_id = Some(checkpoint_id);
        operation.phase = Some(OperationPhase::CheckpointPreparing);
        self.updated_at = Utc::now();
        Ok(())
    }

    /// Advance the active checkpoint journal without replacing its identity.
    pub fn advance_checkpoint_phase(&mut self, phase: OperationPhase) -> Result<()> {
        let operation = self
            .operation
            .as_mut()
            .ok_or_else(|| BlazeError::OperationInProgress {
                active: "none".to_string(),
                requested: OperationKind::Checkpoint.to_string(),
            })?;
        if operation.kind != OperationKind::Checkpoint {
            return Err(BlazeError::OperationInProgress {
                active: operation.kind.to_string(),
                requested: OperationKind::Checkpoint.to_string(),
            });
        }
        if let Some(current) = operation.phase
            && phase.rank() < current.rank()
        {
            return Err(BlazeError::InvalidStateTransition {
                from: current.as_str().to_string(),
                to: phase.as_str().to_string(),
            });
        }
        operation.phase = Some(phase);
        self.updated_at = Utc::now();
        Ok(())
    }

    /// Transfer an interrupted lifecycle operation to destroy recovery.
    ///
    /// Cleanup is the only operation allowed to supersede an unfinished
    /// journal because it releases, rather than acquires, owned resources.
    pub fn begin_destroy_recovery(&mut self) {
        if self.operation.as_ref().map(|operation| operation.kind) == Some(OperationKind::Destroy) {
            return;
        }
        self.operation = Some(OperationJournal {
            kind: OperationKind::Destroy,
            started_at: Utc::now(),
            checkpoint_id: None,
            phase: None,
        });
        self.updated_at = Utc::now();
    }

    /// Clear the marker before atomically persisting the final state.
    pub fn finish_operation(&mut self) {
        self.operation = None;
        self.updated_at = Utc::now();
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
        // reuse goes warm → creating, fresh boots go pending → creating.
        if target == SandboxState::Creating {
            self.start_path = if prev == SandboxState::Warm {
                StartPath::Warm
            } else {
                StartPath::Cold
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

    /// Persist this instance to `{state_dir}/{id}/state.json`. Atomic
    /// rename via `state.json.tmp` to avoid torn reads on daemon restart.
    pub fn persist(&self, state_dir: &Path) -> Result<()> {
        self.persist_with(state_dir, |tmp_path, final_path, json| {
            let mut file = File::create(tmp_path)?;
            file.write_all(json)?;
            file.write_all(b"\n")?;
            file.sync_all()?;
            drop(file);
            fs::rename(tmp_path, final_path)?;
            Ok(())
        })
    }

    fn persist_with<F>(&self, state_dir: &Path, publish: F) -> Result<()>
    where
        F: FnOnce(&Path, &Path, &[u8]) -> Result<()>,
    {
        self.persist_with_directory_sync(state_dir, publish, |directory| {
            File::open(directory)?.sync_all()?;
            Ok(())
        })
    }

    fn persist_with_directory_sync<F, S>(
        &self,
        state_dir: &Path,
        publish: F,
        mut sync_directory: S,
    ) -> Result<()>
    where
        F: FnOnce(&Path, &Path, &[u8]) -> Result<()>,
        S: FnMut(&Path) -> Result<()>,
    {
        let owner_dir = state_dir.join(self.id.to_string());
        let json = serde_json::to_vec_pretty(self)?;
        fs::create_dir_all(state_dir)?;

        match fs::symlink_metadata(&owner_dir) {
            Ok(metadata) if metadata.file_type().is_dir() => {
                let final_path = owner_dir.join("state.json");
                let tmp_path = owner_dir.join("state.json.tmp");
                let result = publish(&tmp_path, &final_path, &json);
                if result.is_err() {
                    let _ = fs::remove_file(&tmp_path);
                }
                result?;
                sync_directory(&owner_dir)
            }
            Ok(_) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "lifecycle owner {} is not a real directory",
                    owner_dir.display()
                ),
            )
            .into()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let staging_dir =
                    state_dir.join(format!(".state-{}-{}.tmp", self.id, Uuid::new_v4()));
                fs::create_dir(&staging_dir)?;
                let final_path = staging_dir.join("state.json");
                let tmp_path = staging_dir.join("state.json.tmp");
                if let Err(error) = publish(&tmp_path, &final_path, &json) {
                    let _ = fs::remove_dir_all(&staging_dir);
                    return Err(error);
                }
                if let Err(error) = sync_directory(&staging_dir) {
                    let _ = fs::remove_dir_all(&staging_dir);
                    return Err(error);
                }
                if let Err(error) = fs::rename(&staging_dir, &owner_dir) {
                    let _ = fs::remove_dir_all(&staging_dir);
                    return Err(error.into());
                }
                sync_directory(state_dir)
            }
            Err(error) => Err(error.into()),
        }
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
    use SandboxState::{
        Checkpointed, Creating, Destroyed, Paused, Pending, RecoveryRequired, Reset, Running, Warm,
    };
    if to == Destroyed {
        // `* → destroyed` is always valid (terminal sink).
        return from != Destroyed;
    }
    if to == RecoveryRequired {
        return !matches!(from, Destroyed | RecoveryRequired);
    }
    match (from, to) {
        (Pending, Creating) => true,
        (Creating, Running) => true,
        (Running, Paused) => true,
        (Running, Reset) => true,
        (Paused, Checkpointed) => true,
        (Paused, Running) => true, // resume
        (Checkpointed, Running) => true,
        (Reset, Warm) => true,
        (Warm, Creating) => true, // pool reuse / warm path
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

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
            SandboxState::Running,
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
    fn recovery_required_can_finish_but_cannot_be_reentered() {
        let mut inst = fresh();
        inst.transition(SandboxState::Creating).expect("creating");
        inst.transition(SandboxState::Running).expect("running");
        inst.transition(SandboxState::RecoveryRequired)
            .expect("recovery required");

        let repeated = inst.transition(SandboxState::RecoveryRequired);
        assert!(matches!(
            repeated,
            Err(BlazeError::InvalidStateTransition { .. })
        ));

        inst.transition(SandboxState::Destroyed)
            .expect("destroyed from recovery");
        let terminal = inst.transition(SandboxState::RecoveryRequired);
        assert!(matches!(
            terminal,
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
    fn failed_first_persist_removes_an_empty_owner_directory() {
        let tmp = tempfile::tempdir().expect("tmp");
        let instance = fresh();

        let error = instance
            .persist_with(tmp.path(), |_tmp_path, _final_path, _json| {
                Err(std::io::Error::other("injected publication failure").into())
            })
            .expect_err("publication must fail");

        assert!(error.to_string().contains("injected publication failure"));
        assert!(!tmp.path().join(instance.id.to_string()).exists());
        assert_eq!(
            std::fs::read_dir(tmp.path())
                .expect("state directory")
                .count(),
            0
        );
    }

    #[test]
    fn first_persist_syncs_the_staged_owner_and_state_root() {
        let tmp = tempfile::tempdir().expect("tmp");
        let instance = fresh();
        let sync_count = Cell::new(0);

        instance
            .persist_with_directory_sync(
                tmp.path(),
                |tmp_path, final_path, json| {
                    std::fs::write(tmp_path, json)?;
                    std::fs::rename(tmp_path, final_path)?;
                    Ok(())
                },
                |_| {
                    sync_count.set(sync_count.get() + 1);
                    Ok(())
                },
            )
            .expect("publish lifecycle");

        assert_eq!(sync_count.get(), 2);
        assert_eq!(
            SandboxInstance::load(tmp.path(), instance.id)
                .expect("published lifecycle")
                .id,
            instance.id
        );
    }

    #[test]
    fn parent_sync_failure_preserves_the_published_owner() {
        let tmp = tempfile::tempdir().expect("tmp");
        let instance = fresh();
        let sync_count = Cell::new(0);

        let error = instance
            .persist_with_directory_sync(
                tmp.path(),
                |tmp_path, final_path, json| {
                    std::fs::write(tmp_path, json)?;
                    std::fs::rename(tmp_path, final_path)?;
                    Ok(())
                },
                |_| {
                    let next = sync_count.get() + 1;
                    sync_count.set(next);
                    if next == 2 {
                        Err(std::io::Error::other("injected parent sync failure").into())
                    } else {
                        Ok(())
                    }
                },
            )
            .expect_err("parent sync result is uncertain");

        assert!(error.to_string().contains("injected parent sync failure"));
        assert_eq!(
            SandboxInstance::load(tmp.path(), instance.id)
                .expect("published lifecycle remains")
                .id,
            instance.id
        );
    }

    #[test]
    fn failed_persist_preserves_a_directory_with_owned_artifacts() {
        let tmp = tempfile::tempdir().expect("tmp");
        let instance = fresh();
        let owner_dir = tmp.path().join(instance.id.to_string());
        std::fs::create_dir_all(&owner_dir).expect("owner dir");
        std::fs::write(owner_dir.join("backend.pid"), b"owner").expect("owner marker");

        instance
            .persist_with(tmp.path(), |_tmp_path, _final_path, _json| {
                Err(std::io::Error::other("injected publication failure").into())
            })
            .expect_err("publication must fail");

        assert!(owner_dir.join("backend.pid").exists());
    }

    #[test]
    fn legacy_state_without_optional_fields_deserializes() {
        let inst = fresh();
        let value = serde_json::json!({
            "id": inst.id,
            "state": "running",
            "backend": "mock",
            "workload_class": "agent-rl",
            "image_digest": "sha256:old",
            "start_path": "cold",
            "created_at": inst.created_at,
            "updated_at": inst.updated_at,
            "policy_name": "legacy"
        });
        let loaded: SandboxInstance = serde_json::from_value(value).expect("legacy state");
        assert!(loaded.operation.is_none());
        assert!(loaded.last_checkpoint.is_none());
        assert_eq!(loaded.backend_ownership, BackendOwnership::Unknown);
    }

    #[test]
    fn create_journal_round_trips() {
        let tmp = tempfile::tempdir().expect("tmp");
        let mut instance = fresh();
        instance
            .begin_operation(OperationKind::Create)
            .expect("begin create");
        instance.persist(tmp.path()).expect("persist");

        let mut loaded = SandboxInstance::load(tmp.path(), instance.id).expect("load");
        assert_eq!(
            loaded.operation.as_ref().map(|operation| operation.kind),
            Some(OperationKind::Create)
        );
        loaded.finish_operation();
        assert!(loaded.operation.is_none());
    }

    #[test]
    fn unfinished_journal_cannot_be_overwritten() {
        let mut instance = fresh();
        instance
            .begin_operation(OperationKind::Create)
            .expect("begin create");
        let journal = instance.operation.clone().expect("journal");

        let error = instance
            .begin_operation(OperationKind::Destroy)
            .expect_err("unfinished operation must be preserved");

        assert!(matches!(
            error,
            BlazeError::OperationInProgress { active, requested }
                if active == "create" && requested == "destroy"
        ));
        assert_eq!(instance.operation, Some(journal));
    }

    #[test]
    fn checkpoint_journal_preserves_identity_and_phase() {
        let tmp = tempfile::tempdir().expect("tmp");
        let mut instance = fresh();
        instance
            .begin_checkpoint_operation("ckpt-00000000-0000-0000-0000-000000000001".into())
            .expect("begin checkpoint");
        instance
            .advance_checkpoint_phase(OperationPhase::CheckpointPublished)
            .expect("advance checkpoint");
        instance.persist(tmp.path()).expect("persist");

        let loaded = SandboxInstance::load(tmp.path(), instance.id).expect("load");
        let journal = loaded.operation.expect("checkpoint journal");
        assert_eq!(journal.kind, OperationKind::Checkpoint);
        assert_eq!(
            journal.checkpoint_id.as_deref(),
            Some("ckpt-00000000-0000-0000-0000-000000000001")
        );
        assert_eq!(journal.phase, Some(OperationPhase::CheckpointPublished));
    }

    #[test]
    fn checkpoint_journal_rejects_phase_regression() {
        let mut instance = fresh();
        instance
            .begin_checkpoint_operation("ckpt-00000000-0000-0000-0000-000000000001".into())
            .expect("begin checkpoint");
        instance
            .advance_checkpoint_phase(OperationPhase::CheckpointPublished)
            .expect("advance checkpoint");

        let error = instance
            .advance_checkpoint_phase(OperationPhase::CheckpointPaused)
            .expect_err("checkpoint phase must remain a durable lower bound");

        assert!(matches!(error, BlazeError::InvalidStateTransition { .. }));
        assert_eq!(
            instance
                .operation
                .as_ref()
                .and_then(|journal| journal.phase),
            Some(OperationPhase::CheckpointPublished)
        );
    }

    #[test]
    fn checkpoint_journal_cannot_replace_an_active_operation() {
        let mut instance = fresh();
        instance
            .begin_operation(OperationKind::Create)
            .expect("begin create");
        let journal = instance.operation.clone();

        let error = instance
            .begin_checkpoint_operation("ckpt-00000000-0000-0000-0000-000000000001".into())
            .expect_err("checkpoint must not replace create");

        assert!(matches!(
            error,
            BlazeError::OperationInProgress { active, requested }
                if active == "create" && requested == "checkpoint"
        ));
        assert_eq!(instance.operation, journal);
    }
}
