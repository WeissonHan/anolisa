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
    /// The previous backend is stopped while replacement resources are owned.
    Restoring,
    /// A live backend is being converted into durable hibernation artifacts.
    Hibernating,
    /// Durable hibernation artifacts and storage are retained without a backend.
    Hibernated,
    /// A backend is being started from retained hibernation artifacts.
    Resuming,
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
            SandboxState::Restoring => "restoring",
            SandboxState::Hibernating => "hibernating",
            SandboxState::Hibernated => "hibernated",
            SandboxState::Resuming => "resuming",
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
    /// A running sandbox is being replaced from a selected checkpoint.
    Restore,
    /// A live backend is being stopped after durable artifacts are prepared.
    Hibernate,
    /// A backend is being started from retained hibernation artifacts.
    Resume,
    /// Runtime resources are being destroyed.
    Destroy,
}

impl OperationKind {
    pub const fn as_str(&self) -> &'static str {
        match self {
            OperationKind::Create => "create",
            OperationKind::Checkpoint => "checkpoint",
            OperationKind::Restore => "restore",
            OperationKind::Hibernate => "hibernate",
            OperationKind::Resume => "resume",
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
/// externally visible runtime availability, while the phase identifies the
/// last resource-ownership or catalog boundary committed before interruption.
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
    /// Restore intent is durable, but the current runtime is still owned.
    RestorePreparing,
    /// Replacement storage is staged without changing the live rootfs.
    RestoreStorageStaged,
    /// The current backend has been confirmed stopped.
    RestoreBackendStopped,
    /// Staged storage is active while the predecessor remains recoverable.
    RestoreStorageActivated,
    /// A replacement backend has started and is owned by the runtime.
    RestoreBackendStarted,
    /// HEAD references the restored checkpoint; storage and lifecycle commits remain.
    RestoreHeadUpdated,
    /// The storage replacement is committed and can no longer be aborted.
    RestoreStorageCommitted,
    /// Hibernate intent is durable, but the backend is still running.
    HibernatePreparing,
    /// The backend is paused while hibernation artifacts are written.
    HibernatePaused,
    /// Hibernation artifacts are complete and durable.
    HibernateArtifactsSynced,
    /// The live backend has stopped and no longer owns runtime resources.
    HibernateBackendStopped,
    /// The replacement hibernation directory is durably visible.
    HibernatePublished,
    /// Resume intent is durable and no backend has started.
    ResumePreparing,
    /// Backend ownership intent is durable before restore starts.
    ResumeBackendStarting,
    /// A restored backend is owned, but readiness is not yet confirmed.
    ResumeBackendStarted,
    /// The restored backend and optional guest transport are ready.
    ResumeBackendReady,
}

impl OperationPhase {
    const fn as_str(self) -> &'static str {
        match self {
            OperationPhase::CheckpointPreparing => "checkpoint-preparing",
            OperationPhase::CheckpointPaused => "checkpoint-paused",
            OperationPhase::CheckpointPublished => "checkpoint-published",
            OperationPhase::CheckpointHeadUpdated => "checkpoint-head-updated",
            OperationPhase::RestorePreparing => "restore-preparing",
            OperationPhase::RestoreStorageStaged => "restore-storage-staged",
            OperationPhase::RestoreBackendStopped => "restore-backend-stopped",
            OperationPhase::RestoreStorageActivated => "restore-storage-activated",
            OperationPhase::RestoreBackendStarted => "restore-backend-started",
            OperationPhase::RestoreHeadUpdated => "restore-head-updated",
            OperationPhase::RestoreStorageCommitted => "restore-storage-committed",
            OperationPhase::HibernatePreparing => "hibernate-preparing",
            OperationPhase::HibernatePaused => "hibernate-paused",
            OperationPhase::HibernateArtifactsSynced => "hibernate-artifacts-synced",
            OperationPhase::HibernateBackendStopped => "hibernate-backend-stopped",
            OperationPhase::HibernatePublished => "hibernate-published",
            OperationPhase::ResumePreparing => "resume-preparing",
            OperationPhase::ResumeBackendStarting => "resume-backend-starting",
            OperationPhase::ResumeBackendStarted => "resume-backend-started",
            OperationPhase::ResumeBackendReady => "resume-backend-ready",
        }
    }

    const fn operation_kind(self) -> OperationKind {
        match self {
            OperationPhase::CheckpointPreparing
            | OperationPhase::CheckpointPaused
            | OperationPhase::CheckpointPublished
            | OperationPhase::CheckpointHeadUpdated => OperationKind::Checkpoint,
            OperationPhase::RestorePreparing
            | OperationPhase::RestoreStorageStaged
            | OperationPhase::RestoreBackendStopped
            | OperationPhase::RestoreStorageActivated
            | OperationPhase::RestoreBackendStarted
            | OperationPhase::RestoreHeadUpdated
            | OperationPhase::RestoreStorageCommitted => OperationKind::Restore,
            OperationPhase::HibernatePreparing
            | OperationPhase::HibernatePaused
            | OperationPhase::HibernateArtifactsSynced
            | OperationPhase::HibernateBackendStopped
            | OperationPhase::HibernatePublished => OperationKind::Hibernate,
            OperationPhase::ResumePreparing
            | OperationPhase::ResumeBackendStarting
            | OperationPhase::ResumeBackendStarted
            | OperationPhase::ResumeBackendReady => OperationKind::Resume,
        }
    }

    const fn rank(self) -> u8 {
        match self {
            OperationPhase::CheckpointPreparing => 0,
            OperationPhase::CheckpointPaused => 1,
            OperationPhase::CheckpointPublished => 2,
            OperationPhase::CheckpointHeadUpdated => 3,
            OperationPhase::RestorePreparing => 0,
            OperationPhase::RestoreStorageStaged => 1,
            OperationPhase::RestoreBackendStopped => 2,
            OperationPhase::RestoreStorageActivated => 3,
            OperationPhase::RestoreBackendStarted => 4,
            OperationPhase::RestoreHeadUpdated => 5,
            OperationPhase::RestoreStorageCommitted => 6,
            OperationPhase::HibernatePreparing => 0,
            OperationPhase::HibernatePaused => 1,
            OperationPhase::HibernateArtifactsSynced => 2,
            OperationPhase::HibernateBackendStopped => 3,
            OperationPhase::HibernatePublished => 4,
            OperationPhase::ResumePreparing => 0,
            OperationPhase::ResumeBackendStarting => 1,
            OperationPhase::ResumeBackendStarted => 2,
            OperationPhase::ResumeBackendReady => 3,
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

/// Canonical directory family that owns backend runtime artifacts.
///
/// This is independent from [`StartPath`]: a sandbox can be activated through
/// a warm lifecycle transition while retaining its original sandbox directory.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeLocation {
    /// Runtime artifacts live under the sandbox's lifecycle state directory.
    #[default]
    Sandbox,
    /// Runtime artifacts live under the daemon's warm-slot directory.
    WarmPool,
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
    /// Canonical directory family containing backend runtime artifacts.
    #[serde(default)]
    pub runtime_location: RuntimeLocation,
    /// Stable nonce that links a claimed warm runtime to its slot journal.
    #[serde(default)]
    pub runtime_owner_token: Option<Uuid>,
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
            runtime_location: RuntimeLocation::Sandbox,
            runtime_owner_token: None,
            backend_ownership: BackendOwnership::NotStarted,
            operation: None,
            last_checkpoint: None,
        }
    }

    /// Adopt a ready runtime slot into a recoverable create operation.
    pub fn new_warm_claim(
        id: Uuid,
        backend: BackendKind,
        workload_class: WorkloadClass,
        image_digest: String,
        policy_name: String,
        backend_ownership: BackendOwnership,
        runtime_owner_token: Uuid,
    ) -> Result<Self> {
        if !matches!(
            backend_ownership,
            BackendOwnership::NotStarted | BackendOwnership::Running
        ) {
            return Err(BlazeError::BackendError {
                msg: format!(
                    "ready runtime slot {id} has invalid backend ownership {backend_ownership:?}"
                ),
            });
        }
        let mut instance = Self::new(
            backend,
            workload_class,
            image_digest,
            StartPath::Warm,
            policy_name,
        );
        instance.id = id;
        instance.state = SandboxState::Warm;
        instance.runtime_location = RuntimeLocation::WarmPool;
        instance.runtime_owner_token = Some(runtime_owner_token);
        instance.backend_ownership = backend_ownership;
        instance.begin_operation(OperationKind::Create)?;
        instance.transition(SandboxState::Creating)?;
        Ok(instance)
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
        self.advance_operation_phase(OperationKind::Checkpoint, phase)
    }

    /// Record restore intent without changing the last completed checkpoint.
    pub fn begin_restore_operation(&mut self, checkpoint_id: String) -> Result<()> {
        self.begin_operation(OperationKind::Restore)?;
        let operation = self
            .operation
            .as_mut()
            .expect("begin_operation installs a journal");
        operation.checkpoint_id = Some(checkpoint_id);
        operation.phase = Some(OperationPhase::RestorePreparing);
        self.updated_at = Utc::now();
        Ok(())
    }

    /// Advance the active restore journal without replacing its identity.
    pub fn advance_restore_phase(&mut self, phase: OperationPhase) -> Result<()> {
        self.advance_operation_phase(OperationKind::Restore, phase)
    }

    /// Record hibernation intent before pausing the backend.
    pub fn begin_hibernate_operation(&mut self) -> Result<()> {
        self.begin_operation(OperationKind::Hibernate)?;
        let operation = self
            .operation
            .as_mut()
            .expect("begin_operation installs a journal");
        operation.phase = Some(OperationPhase::HibernatePreparing);
        self.updated_at = Utc::now();
        Ok(())
    }

    /// Advance the active hibernation journal without replacing its identity.
    pub fn advance_hibernate_phase(&mut self, phase: OperationPhase) -> Result<()> {
        self.advance_operation_phase(OperationKind::Hibernate, phase)
    }

    /// Record resume intent before preparing a replacement backend.
    pub fn begin_resume_operation(&mut self) -> Result<()> {
        self.begin_operation(OperationKind::Resume)?;
        let operation = self
            .operation
            .as_mut()
            .expect("begin_operation installs a journal");
        operation.phase = Some(OperationPhase::ResumePreparing);
        self.updated_at = Utc::now();
        Ok(())
    }

    /// Advance the active resume journal without replacing its identity.
    pub fn advance_resume_phase(&mut self, phase: OperationPhase) -> Result<()> {
        self.advance_operation_phase(OperationKind::Resume, phase)
    }

    fn advance_operation_phase(
        &mut self,
        requested_kind: OperationKind,
        phase: OperationPhase,
    ) -> Result<()> {
        if phase.operation_kind() != requested_kind {
            return Err(BlazeError::InvalidStateTransition {
                from: requested_kind.to_string(),
                to: phase.as_str().to_string(),
            });
        }
        let operation = self
            .operation
            .as_mut()
            .ok_or_else(|| BlazeError::OperationInProgress {
                active: "none".to_string(),
                requested: requested_kind.to_string(),
            })?;
        if operation.kind != requested_kind {
            return Err(BlazeError::OperationInProgress {
                active: operation.kind.to_string(),
                requested: requested_kind.to_string(),
            });
        }
        if let Some(current) = operation.phase
            && (current.operation_kind() != requested_kind || phase.rank() < current.rank())
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

    /// Return whether lifecycle metadata proves that no runtime owner remains.
    pub fn is_clean_terminal(&self) -> bool {
        self.state == SandboxState::Destroyed
            && self.operation.is_none()
            && matches!(
                self.backend_ownership,
                BackendOwnership::NotStarted | BackendOwnership::Stopped
            )
    }

    /// Apply a state transition.
    ///
    /// Restore transitions additionally require the durable backend-stop and
    /// storage-commit boundaries before changing externally visible state.
    /// Returns
    /// [`BlazeError::InvalidStateTransition`] when the move is not part
    /// of the lifecycle state graph.
    pub fn transition(&mut self, target: SandboxState) -> Result<()> {
        let restore_boundary_reached = match (self.state, target) {
            (_, SandboxState::Restoring) => {
                self.restore_phase_reached(OperationPhase::RestoreBackendStopped)
            }
            (SandboxState::Restoring, SandboxState::Running) => {
                self.restore_phase_reached(OperationPhase::RestoreStorageCommitted)
            }
            _ => true,
        };
        let hibernate_boundary_reached = match (self.state, target) {
            (SandboxState::Running, SandboxState::Hibernating) => self.operation_phase_reached(
                OperationKind::Hibernate,
                OperationPhase::HibernatePreparing,
            ),
            (SandboxState::Hibernating, SandboxState::Hibernated) => {
                self.backend_ownership == BackendOwnership::Stopped
                    && self.operation_phase_reached(
                        OperationKind::Hibernate,
                        OperationPhase::HibernatePublished,
                    )
            }
            (SandboxState::Hibernating, SandboxState::Running) => {
                self.backend_ownership == BackendOwnership::Running
                    && self
                        .operation
                        .as_ref()
                        .is_some_and(|operation| operation.kind == OperationKind::Hibernate)
            }
            (SandboxState::Hibernated, SandboxState::Resuming) => {
                self.backend_ownership == BackendOwnership::Stopped
                    && self.operation_phase_reached(
                        OperationKind::Resume,
                        OperationPhase::ResumePreparing,
                    )
            }
            (SandboxState::Resuming, SandboxState::Running) => {
                self.backend_ownership == BackendOwnership::Running
                    && self.operation_phase_reached(
                        OperationKind::Resume,
                        OperationPhase::ResumeBackendReady,
                    )
            }
            (SandboxState::Resuming, SandboxState::Hibernated) => {
                self.backend_ownership == BackendOwnership::Stopped
                    && self
                        .operation
                        .as_ref()
                        .is_some_and(|operation| operation.kind == OperationKind::Resume)
            }
            _ => true,
        };
        if !restore_boundary_reached
            || !hibernate_boundary_reached
            || !is_valid_transition(self.state, target)
        {
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

    fn restore_phase_reached(&self, minimum: OperationPhase) -> bool {
        self.operation_phase_reached(OperationKind::Restore, minimum)
    }

    fn operation_phase_reached(
        &self,
        operation_kind: OperationKind,
        minimum: OperationPhase,
    ) -> bool {
        minimum.operation_kind() == operation_kind
            && self.operation.as_ref().is_some_and(|operation| {
                operation.kind == operation_kind
                    && operation.phase.is_some_and(|phase| {
                        phase.operation_kind() == operation_kind && phase.rank() >= minimum.rank()
                    })
            })
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
        Checkpointed, Creating, Destroyed, Hibernated, Hibernating, Paused, Pending,
        RecoveryRequired, Reset, Restoring, Resuming, Running, Warm,
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
        (Running, Restoring) => true,
        (Restoring, Running) => true,
        (Running, Hibernating) => true,
        (Hibernating, Hibernated) => true,
        (Hibernating, Running) => true,
        (Hibernated, Resuming) => true,
        (Resuming, Running) => true,
        (Resuming, Hibernated) => true,
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
    fn restore_state_requires_owned_replacement_boundaries() {
        let mut inst = fresh();
        let error = inst
            .transition(SandboxState::Restoring)
            .expect_err("pending sandbox cannot restore");
        assert!(matches!(error, BlazeError::InvalidStateTransition { .. }));
        assert_eq!(inst.state, SandboxState::Pending);

        inst.transition(SandboxState::Creating).expect("creating");
        inst.transition(SandboxState::Running).expect("running");
        inst.begin_restore_operation("ckpt-00000000-0000-0000-0000-000000000001".to_string())
            .expect("begin restore");
        inst.advance_restore_phase(OperationPhase::RestoreStorageStaged)
            .expect("stage storage");
        let error = inst
            .transition(SandboxState::Restoring)
            .expect_err("running remains visible until the backend is stopped");
        assert!(matches!(error, BlazeError::InvalidStateTransition { .. }));
        assert_eq!(inst.state, SandboxState::Running);

        inst.advance_restore_phase(OperationPhase::RestoreBackendStopped)
            .expect("stop backend");
        inst.transition(SandboxState::Restoring)
            .expect("restore starts");
        inst.advance_restore_phase(OperationPhase::RestoreStorageActivated)
            .expect("activate storage");
        inst.advance_restore_phase(OperationPhase::RestoreBackendStarted)
            .expect("start backend");
        inst.advance_restore_phase(OperationPhase::RestoreHeadUpdated)
            .expect("update head");
        let error = inst
            .transition(SandboxState::Running)
            .expect_err("storage commit precedes the final running state");
        assert!(matches!(error, BlazeError::InvalidStateTransition { .. }));
        assert_eq!(inst.state, SandboxState::Restoring);

        inst.advance_restore_phase(OperationPhase::RestoreStorageCommitted)
            .expect("commit storage");
        inst.transition(SandboxState::Running)
            .expect("restore commits");
    }

    #[test]
    fn hibernation_state_requires_durable_ownership_boundaries() {
        let mut inst = fresh();
        inst.transition(SandboxState::Creating).expect("creating");
        inst.transition(SandboxState::Running).expect("running");
        inst.backend_ownership = BackendOwnership::Running;

        let error = inst
            .transition(SandboxState::Hibernating)
            .expect_err("hibernate requires a journal");
        assert!(matches!(error, BlazeError::InvalidStateTransition { .. }));

        inst.begin_hibernate_operation().expect("begin hibernate");
        inst.transition(SandboxState::Hibernating)
            .expect("hibernate starts");
        inst.advance_hibernate_phase(OperationPhase::HibernateArtifactsSynced)
            .expect("artifacts durable");
        let error = inst
            .transition(SandboxState::Hibernated)
            .expect_err("a live backend prevents hibernated state");
        assert!(matches!(error, BlazeError::InvalidStateTransition { .. }));

        inst.backend_ownership = BackendOwnership::Stopped;
        inst.advance_hibernate_phase(OperationPhase::HibernatePublished)
            .expect("publish hibernation");
        inst.transition(SandboxState::Hibernated)
            .expect("hibernate commits");
        inst.finish_operation();

        let error = inst
            .transition(SandboxState::Resuming)
            .expect_err("resume requires a journal");
        assert!(matches!(error, BlazeError::InvalidStateTransition { .. }));

        inst.begin_resume_operation().expect("begin resume");
        inst.transition(SandboxState::Resuming)
            .expect("resume starts");
        inst.advance_resume_phase(OperationPhase::ResumeBackendStarted)
            .expect("backend started");
        inst.backend_ownership = BackendOwnership::Running;
        let error = inst
            .transition(SandboxState::Running)
            .expect_err("readiness must precede running state");
        assert!(matches!(error, BlazeError::InvalidStateTransition { .. }));

        inst.advance_resume_phase(OperationPhase::ResumeBackendReady)
            .expect("backend ready");
        inst.transition(SandboxState::Running)
            .expect("resume commits");
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
    fn clean_terminal_requires_safe_ownership_and_no_operation() {
        let mut destroyed = fresh();
        destroyed
            .transition(SandboxState::Destroyed)
            .expect("destroyed");
        for ownership in [BackendOwnership::NotStarted, BackendOwnership::Stopped] {
            destroyed.backend_ownership = ownership;
            assert!(destroyed.is_clean_terminal());
        }

        destroyed.backend_ownership = BackendOwnership::Running;
        assert!(!destroyed.is_clean_terminal());

        let mut unfinished = fresh();
        unfinished
            .begin_operation(OperationKind::Create)
            .expect("begin create");
        unfinished
            .transition(SandboxState::Destroyed)
            .expect("destroyed");
        unfinished.backend_ownership = BackendOwnership::Stopped;
        assert!(!unfinished.is_clean_terminal());

        let mut running = fresh();
        running
            .transition(SandboxState::Creating)
            .expect("creating");
        running.transition(SandboxState::Running).expect("running");
        running.backend_ownership = BackendOwnership::Stopped;
        assert!(!running.is_clean_terminal());
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
        assert_eq!(loaded.runtime_location, RuntimeLocation::Sandbox);
        assert!(loaded.runtime_owner_token.is_none());
    }

    #[test]
    fn warm_start_classification_does_not_move_runtime_artifacts() {
        let mut instance = fresh();
        instance
            .transition(SandboxState::Creating)
            .expect("creating");
        instance.transition(SandboxState::Running).expect("running");
        instance.transition(SandboxState::Reset).expect("reset");
        instance.transition(SandboxState::Warm).expect("warm");
        instance
            .transition(SandboxState::Creating)
            .expect("warm creating");

        assert_eq!(instance.start_path, StartPath::Warm);
        assert_eq!(instance.runtime_location, RuntimeLocation::Sandbox);
    }

    #[test]
    fn runtime_location_round_trips() {
        let tmp = tempfile::tempdir().expect("tmp");
        let mut instance = fresh();
        let owner_token = Uuid::new_v4();
        instance.runtime_location = RuntimeLocation::WarmPool;
        instance.runtime_owner_token = Some(owner_token);
        instance.persist(tmp.path()).expect("persist");

        let loaded = SandboxInstance::load(tmp.path(), instance.id).expect("load");

        assert_eq!(loaded.runtime_location, RuntimeLocation::WarmPool);
        assert_eq!(loaded.runtime_owner_token, Some(owner_token));
    }

    #[test]
    fn warm_claim_starts_one_durable_create_operation() {
        let id = Uuid::new_v4();
        let owner_token = Uuid::new_v4();

        let instance = SandboxInstance::new_warm_claim(
            id,
            BackendKind::Mock,
            WorkloadClass::AgentTool,
            "sha256:warm".into(),
            "warm-policy".into(),
            BackendOwnership::Running,
            owner_token,
        )
        .expect("warm claim");

        assert_eq!(instance.id, id);
        assert_eq!(instance.state, SandboxState::Creating);
        assert_eq!(instance.start_path, StartPath::Warm);
        assert_eq!(instance.runtime_location, RuntimeLocation::WarmPool);
        assert_eq!(instance.runtime_owner_token, Some(owner_token));
        assert_eq!(instance.backend_ownership, BackendOwnership::Running);
        assert_eq!(
            instance.operation.as_ref().map(|operation| operation.kind),
            Some(OperationKind::Create)
        );
    }

    #[test]
    fn warm_claim_rejects_an_unstable_backend_owner() {
        let error = SandboxInstance::new_warm_claim(
            Uuid::new_v4(),
            BackendKind::Mock,
            WorkloadClass::AgentTool,
            "sha256:warm".into(),
            "warm-policy".into(),
            BackendOwnership::Starting,
            Uuid::new_v4(),
        )
        .expect_err("starting backend cannot be claimed");

        assert!(matches!(error, BlazeError::BackendError { .. }));
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

    #[test]
    fn restore_journal_round_trips_without_overwriting_last_checkpoint() {
        let tmp = tempfile::tempdir().expect("tmp");
        let mut instance = fresh();
        let completed = "ckpt-00000000-0000-0000-0000-000000000001".to_string();
        let selected = "ckpt-00000000-0000-0000-0000-000000000002".to_string();
        instance.last_checkpoint = Some(completed.clone());
        instance
            .transition(SandboxState::Creating)
            .expect("creating");
        instance.transition(SandboxState::Running).expect("running");
        instance
            .begin_restore_operation(selected.clone())
            .expect("begin restore");
        instance
            .advance_restore_phase(OperationPhase::RestoreStorageStaged)
            .expect("stage storage");
        instance
            .advance_restore_phase(OperationPhase::RestoreBackendStopped)
            .expect("stop backend");
        instance
            .transition(SandboxState::Restoring)
            .expect("restoring");

        for phase in [
            OperationPhase::RestoreStorageActivated,
            OperationPhase::RestoreBackendStarted,
            OperationPhase::RestoreHeadUpdated,
            OperationPhase::RestoreStorageCommitted,
        ] {
            instance
                .advance_restore_phase(phase)
                .expect("advance restore");
            assert_eq!(
                instance.last_checkpoint.as_deref(),
                Some(completed.as_str())
            );
        }
        instance.persist(tmp.path()).expect("persist");

        let loaded = SandboxInstance::load(tmp.path(), instance.id).expect("load");
        let journal = loaded.operation.expect("restore journal");
        assert_eq!(journal.kind, OperationKind::Restore);
        assert_eq!(journal.checkpoint_id.as_deref(), Some(selected.as_str()));
        assert_eq!(journal.phase, Some(OperationPhase::RestoreStorageCommitted));
        assert_eq!(loaded.last_checkpoint.as_deref(), Some(completed.as_str()));
        assert_eq!(
            serde_json::to_value(journal.kind).expect("serialize kind"),
            serde_json::json!("restore")
        );
        assert_eq!(
            serde_json::to_value(journal.phase).expect("serialize phase"),
            serde_json::json!("restore-storage-committed")
        );
    }

    #[test]
    fn restore_journal_rejects_phase_regression() {
        let mut instance = fresh();
        let completed = "ckpt-00000000-0000-0000-0000-000000000001".to_string();
        instance.last_checkpoint = Some(completed.clone());
        instance
            .begin_restore_operation("ckpt-00000000-0000-0000-0000-000000000002".to_string())
            .expect("begin restore");
        instance
            .advance_restore_phase(OperationPhase::RestoreStorageActivated)
            .expect("advance restore");

        let error = instance
            .advance_restore_phase(OperationPhase::RestoreStorageStaged)
            .expect_err("restore phase must remain a durable lower bound");

        assert!(matches!(error, BlazeError::InvalidStateTransition { .. }));
        assert_eq!(
            instance
                .operation
                .as_ref()
                .and_then(|journal| journal.phase),
            Some(OperationPhase::RestoreStorageActivated)
        );
        assert_eq!(instance.last_checkpoint, Some(completed));
    }

    #[test]
    fn operation_journals_reject_phases_from_the_other_operation() {
        let mut checkpoint = fresh();
        checkpoint
            .begin_checkpoint_operation("ckpt-00000000-0000-0000-0000-000000000001".to_string())
            .expect("begin checkpoint");
        let checkpoint_journal = checkpoint.operation.clone();
        let checkpoint_error = checkpoint
            .advance_checkpoint_phase(OperationPhase::RestoreBackendStopped)
            .expect_err("checkpoint cannot record restore progress");
        assert!(matches!(
            checkpoint_error,
            BlazeError::InvalidStateTransition { .. }
        ));
        assert_eq!(checkpoint.operation, checkpoint_journal);

        let mut restore = fresh();
        restore
            .begin_restore_operation("ckpt-00000000-0000-0000-0000-000000000002".to_string())
            .expect("begin restore");
        let restore_journal = restore.operation.clone();
        let restore_error = restore
            .advance_restore_phase(OperationPhase::CheckpointPublished)
            .expect_err("restore cannot record checkpoint progress");
        assert!(matches!(
            restore_error,
            BlazeError::InvalidStateTransition { .. }
        ));
        assert_eq!(restore.operation, restore_journal);
    }

    #[test]
    fn restore_journal_cannot_replace_an_active_operation() {
        let mut instance = fresh();
        instance
            .begin_operation(OperationKind::Create)
            .expect("begin create");
        let journal = instance.operation.clone();

        let error = instance
            .begin_restore_operation("ckpt-00000000-0000-0000-0000-000000000001".to_string())
            .expect_err("restore must not replace create");

        assert!(matches!(
            error,
            BlazeError::OperationInProgress { active, requested }
                if active == "create" && requested == "restore"
        ));
        assert_eq!(instance.operation, journal);
    }
}
