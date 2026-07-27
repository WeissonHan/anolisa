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
use crate::policy::{BackendConfigs, VmConfig, WorkloadClass};

/// All known states. Transitions are enforced by [`SandboxInstance::transition`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SandboxState {
    Pending,
    Creating,
    Running,
    Paused,
    Checkpointed,
    Hibernating,
    Hibernated,
    Resuming,
    RollingBack,
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
            SandboxState::Hibernating => "hibernating",
            SandboxState::Hibernated => "hibernated",
            SandboxState::Resuming => "resuming",
            SandboxState::RollingBack => "rolling-back",
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
    /// A point-in-time checkpoint is being committed.
    Checkpoint,
    /// A running sandbox is being converted into a hibernated sandbox.
    Hibernate,
    /// A hibernated sandbox is being restored.
    Resume,
    /// A checkpoint is replacing the current runtime.
    Rollback,
    /// Runtime resources are being destroyed.
    Destroy,
}

/// Durable journal entry for one active lifecycle operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationJournal {
    /// Operation being performed.
    pub kind: OperationKind,
    /// UTC time at which the operation became externally visible.
    pub started_at: DateTime<Utc>,
    /// Optional checkpoint involved in the operation.
    #[serde(default)]
    pub checkpoint_id: Option<String>,
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
    /// User-visible template name when creation used the canonical API.
    #[serde(default)]
    pub template_name: String,
    /// Most recently committed or selected checkpoint.
    #[serde(default)]
    pub last_checkpoint: Option<String>,
    /// Active multi-step operation, if any.
    #[serde(default)]
    pub operation: Option<OperationJournal>,
    /// Backend-specific policy needed for exact hibernate/rollback restore.
    #[serde(default)]
    pub backend_config: BackendConfigs,
    /// Generic VM resources needed for exact restore.
    #[serde(default)]
    pub vm_config: Option<VmConfig>,
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
            template_name: String::new(),
            last_checkpoint: None,
            operation: None,
            backend_config: BackendConfigs::default(),
            vm_config: None,
        }
    }

    /// Create an instance using a caller-supplied UUID.
    ///
    /// UUID-only IDs keep the persisted directory layout unambiguous while
    /// still supporting idempotent platform requests.
    pub fn new_with_id(
        id: Uuid,
        backend: BackendKind,
        workload_class: WorkloadClass,
        image_digest: String,
        start_path: StartPath,
        policy_name: String,
    ) -> Self {
        let mut instance = Self::new(
            backend,
            workload_class,
            image_digest,
            start_path,
            policy_name,
        );
        instance.id = id;
        instance
    }

    /// Persist an operation before starting its first data-plane mutation.
    pub fn begin_operation(&mut self, kind: OperationKind, checkpoint_id: Option<String>) {
        self.operation = Some(OperationJournal {
            kind,
            started_at: Utc::now(),
            checkpoint_id,
        });
        self.updated_at = Utc::now();
    }

    /// Clear the durable operation marker after the final state is persisted.
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
        // A durable warm → creating transition always records the warm path;
        // pending → creating preserves the caller's pre-classification.
        if target == SandboxState::Creating && prev == SandboxState::Warm {
            self.start_path = StartPath::Warm;
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
        let dir = state_dir.join(self.id.to_string());
        fs::create_dir_all(&dir)?;
        let final_path = dir.join("state.json");
        let tmp_path = dir.join("state.json.tmp");
        let json = serde_json::to_vec_pretty(self)?;
        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(&json)?;
            file.write_all(b"\n")?;
            file.sync_all()?;
        }
        fs::rename(&tmp_path, &final_path)?;
        File::open(&dir)?.sync_all()?;
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
    use SandboxState::{
        Checkpointed, Creating, Destroyed, Hibernated, Hibernating, Paused, Pending,
        RecoveryRequired, Reset, Resuming, RollingBack, Running, Warm,
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
        (Running, Hibernating) => true,
        (Hibernating, Running) => true,
        (Hibernating, Hibernated) => true,
        (Hibernated, Resuming) => true,
        (Resuming, Hibernated) => true,
        (Resuming, Running) => true,
        (Running, RollingBack) => true,
        (Checkpointed, RollingBack) => true,
        (RollingBack, Running) => true,
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
        assert!(loaded.template_name.is_empty());
        assert!(loaded.last_checkpoint.is_none());
        assert!(loaded.operation.is_none());
    }

    #[test]
    fn extended_lifecycle_paths_are_legal() {
        let mut hibernate = fresh();
        hibernate
            .transition(SandboxState::Creating)
            .expect("create");
        hibernate.transition(SandboxState::Running).expect("run");
        hibernate
            .transition(SandboxState::Hibernating)
            .expect("hibernate");
        hibernate
            .transition(SandboxState::Hibernated)
            .expect("hibernated");
        hibernate
            .transition(SandboxState::Resuming)
            .expect("resume");
        hibernate
            .transition(SandboxState::Running)
            .expect("running");

        let mut checkpoint = fresh();
        checkpoint
            .transition(SandboxState::Creating)
            .expect("create");
        checkpoint.transition(SandboxState::Running).expect("run");
        checkpoint.transition(SandboxState::Paused).expect("pause");
        checkpoint
            .transition(SandboxState::Checkpointed)
            .expect("checkpoint");
        checkpoint
            .transition(SandboxState::RollingBack)
            .expect("rollback");
        checkpoint.transition(SandboxState::Running).expect("run");
    }
}
