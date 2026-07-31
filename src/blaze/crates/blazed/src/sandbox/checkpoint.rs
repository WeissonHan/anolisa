// SPDX-License-Identifier: Apache-2.0
//! Durable checkpoint capture and listing.

use blaze_core::backend::{BackendKind, SnapshotKind, SnapshotRequest};
use blaze_core::checkpoint::{CheckpointInfo, CheckpointMetadata, CommitCheckpoint};
use blaze_core::lifecycle::{OperationPhase, SandboxInstance, SandboxState};
use uuid::Uuid;

use crate::error::{BlazeDaemonError, Result};
use crate::spawner::DynBackendInstance;

use super::manager::SandboxManager;

impl SandboxManager {
    /// Capture a self-contained checkpoint and resume the existing backend.
    pub async fn checkpoint(&self, id: Uuid) -> Result<CheckpointMetadata> {
        let operation = self.operation_lock(id).lock_owned().await;
        let mut instance = self.get(id)?;
        if let Some(journal) = &instance.operation {
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "instance {id} has unfinished {} operation",
                journal.kind
            )));
        }
        if instance.state != SandboxState::Running {
            return Err(BlazeDaemonError::Conflict(format!(
                "instance {id} is {}, expected running",
                instance.state
            )));
        }

        let backend = self.backend_owner(id).ok_or_else(|| {
            BlazeDaemonError::Conflict(format!("instance {id} has no backend owner"))
        })?;
        if !backend.supports_checkpoint_capture() || !self.storage.supports_checkpoint_capture() {
            return Err(BlazeDaemonError::UnsupportedOperation(format!(
                "instance {id} backend {} and configured storage do not support checkpoint capture",
                backend.backend()
            )));
        }
        if backend.instance_id() != id || backend.backend() != instance.backend {
            self.mark_recovery(id)?;
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "instance {id} backend owner identity does not match durable state"
            )));
        }
        let backend_version = backend.version().map(str::to_string);
        if backend_version
            .as_deref()
            .is_some_and(|version| version.trim().is_empty())
            || (backend.backend() == BackendKind::Firecracker && backend_version.is_none())
        {
            return Err(BlazeDaemonError::UnsupportedOperation(format!(
                "instance {id} backend {} does not report a usable checkpoint version",
                backend.backend()
            )));
        }
        self.require_live_backend(id, &backend).await?;
        let storage = self.storage.reconstruct(&id.to_string()).await?;

        let stage = self.checkpoints.begin(id).map_err(checkpoint_store_error)?;
        let checkpoint_id = stage.id().to_string();
        let snapshot_path = match stage.artifact_path("vmstate.snap") {
            Ok(path) => path,
            Err(error) => {
                let _ = self.checkpoints.abort(stage);
                return Err(checkpoint_store_error(error));
            }
        };
        let memory_path = match stage.artifact_path("memory.snap") {
            Ok(path) => path,
            Err(error) => {
                let _ = self.checkpoints.abort(stage);
                return Err(checkpoint_store_error(error));
            }
        };
        let rootfs_path = match stage.artifact_path("rootfs.snap") {
            Ok(path) => path,
            Err(error) => {
                let _ = self.checkpoints.abort(stage);
                return Err(checkpoint_store_error(error));
            }
        };
        if let Err(error) = crate::failpoint::state("checkpoint-begin-state") {
            let _ = self.checkpoints.abort(stage);
            return Err(error);
        }
        if let Err(error) = instance.begin_checkpoint_operation(checkpoint_id.clone()) {
            let _ = self.checkpoints.abort(stage);
            return Err(error.into());
        }
        if let Err(error) = self.persist_and_retain(instance.clone()) {
            let _ = self.checkpoints.abort(stage);
            return Err(error);
        }
        crate::failpoint::pause("checkpoint-after-begin").await;

        let paused = match crate::failpoint::backend("checkpoint-pause") {
            Ok(()) => backend.pause().await,
            Err(error) => Err(error),
        };
        if let Err(error) = paused {
            return self
                .finish_failed_unpublished_checkpoint(id, &backend, &checkpoint_id, error.into())
                .await;
        }

        if let Err(error) = instance
            .transition(SandboxState::Paused)
            .and_then(|_| instance.advance_checkpoint_phase(OperationPhase::CheckpointPaused))
        {
            return self
                .finish_failed_unpublished_checkpoint(id, &backend, &checkpoint_id, error.into())
                .await;
        }
        if let Err(error) = crate::failpoint::state("checkpoint-paused-state")
            .and_then(|_| self.persist_and_retain(instance.clone()))
        {
            return self
                .finish_failed_unpublished_checkpoint(id, &backend, &checkpoint_id, error)
                .await;
        }
        crate::failpoint::pause("checkpoint-after-pause").await;

        let snapshot = SnapshotRequest {
            snapshot_path,
            mem_path: memory_path,
            kind: SnapshotKind::Full,
        };
        let snapshot_result = match crate::failpoint::backend("checkpoint-snapshot") {
            Ok(()) => backend.snapshot(snapshot).await,
            Err(error) => Err(error),
        };
        if let Err(error) = snapshot_result {
            return self
                .finish_failed_unpublished_checkpoint(id, &backend, &checkpoint_id, error.into())
                .await;
        }

        let flushed = match crate::failpoint::storage("checkpoint-storage-flush") {
            Ok(()) => self.storage.flush_dirty(&storage).await,
            Err(error) => Err(error),
        };
        if let Err(error) = flushed {
            return self
                .finish_failed_unpublished_checkpoint(id, &backend, &checkpoint_id, error.into())
                .await;
        }

        let captured = match crate::failpoint::storage("checkpoint-rootfs-capture") {
            Ok(()) => {
                self.storage
                    .capture_checkpoint(&storage, &rootfs_path)
                    .await
            }
            Err(error) => Err(error),
        };
        if let Err(error) = captured {
            return self
                .finish_failed_unpublished_checkpoint(id, &backend, &checkpoint_id, error.into())
                .await;
        }

        let parent = match self.checkpoints.read_head(id) {
            Ok(parent) => parent,
            Err(error) => {
                return self
                    .finish_failed_unpublished_checkpoint(
                        id,
                        &backend,
                        &checkpoint_id,
                        checkpoint_store_error(error),
                    )
                    .await;
            }
        };
        if let Err(error) = crate::failpoint::storage("checkpoint-publish") {
            return self
                .finish_failed_unpublished_checkpoint(id, &backend, &checkpoint_id, error.into())
                .await;
        }
        let published = self
            .checkpoints
            .publish(
                &stage,
                CommitCheckpoint {
                    parent,
                    policy_name: instance.policy_name.clone(),
                    image_digest: instance.image_digest.clone(),
                    backend: instance.backend,
                    backend_version,
                    snapshot_kind: SnapshotKind::Full,
                },
            )
            .map_err(checkpoint_store_error);
        let metadata = match published {
            Ok(metadata) => metadata,
            Err(error) => {
                return self
                    .fail_published_checkpoint(
                        &backend,
                        &instance,
                        error,
                        "publication with uncertain outcome",
                    )
                    .await;
            }
        };

        if let Err(error) = instance.advance_checkpoint_phase(OperationPhase::CheckpointPublished) {
            return self
                .fail_published_checkpoint(
                    &backend,
                    &instance,
                    error.into(),
                    "published journal update",
                )
                .await;
        }
        if let Err(error) = crate::failpoint::state("checkpoint-published-state")
            .and_then(|_| self.persist_and_retain(instance.clone()))
        {
            return self
                .fail_published_checkpoint(&backend, &instance, error, "published state commit")
                .await;
        }
        crate::failpoint::pause("checkpoint-after-publish-before-head").await;

        if let Err(error) = crate::failpoint::storage("checkpoint-head-update") {
            return self
                .finish_failed_published_checkpoint(id, &backend, error.into())
                .await;
        }
        if let Err(error) = self.checkpoints.set_head(id, &checkpoint_id) {
            return self
                .fail_published_checkpoint(
                    &backend,
                    &instance,
                    checkpoint_store_error(error),
                    "HEAD update with uncertain outcome",
                )
                .await;
        }

        if let Err(error) = instance.advance_checkpoint_phase(OperationPhase::CheckpointHeadUpdated)
        {
            return self
                .fail_published_checkpoint(&backend, &instance, error.into(), "HEAD journal update")
                .await;
        }
        if let Err(error) = crate::failpoint::state("checkpoint-head-state")
            .and_then(|_| self.persist_and_retain(instance.clone()))
        {
            return self
                .fail_published_checkpoint(&backend, &instance, error, "HEAD state commit")
                .await;
        }
        crate::failpoint::pause("checkpoint-after-head").await;

        let resumed = match crate::failpoint::backend("checkpoint-resume") {
            Ok(()) => backend.resume().await,
            Err(error) => Err(error),
        };
        if let Err(error) = resumed {
            self.mark_recovery(id)?;
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "checkpoint {checkpoint_id} became HEAD, but backend resume failed: {error}"
            )));
        }
        if let Err(error) = self.verify_backend_ready(id, &backend).await {
            self.mark_recovery(id)?;
            return Err(error);
        }

        if let Err(error) = instance
            .transition(SandboxState::Checkpointed)
            .and_then(|_| instance.transition(SandboxState::Running))
        {
            self.mark_recovery(id)?;
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "checkpoint runtime resumed, but lifecycle transition failed: {error}"
            )));
        }
        instance.last_checkpoint = Some(checkpoint_id);
        instance.finish_operation();
        if let Err(error) = crate::failpoint::state("checkpoint-final-state")
            .and_then(|_| self.persist_and_retain(instance))
        {
            self.mark_recovery(id)?;
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "checkpoint completed, but final lifecycle state could not be committed: {error}"
            )));
        }
        drop(operation);
        Ok(metadata)
    }

    /// List every committed checkpoint and its HEAD reachability.
    pub async fn list_checkpoints(&self, id: Uuid) -> Result<Vec<CheckpointInfo>> {
        let _operation = self.operation_lock(id).lock_owned().await;
        self.get(id)?;
        self.checkpoints.list(id).map_err(checkpoint_store_error)
    }

    async fn finish_failed_unpublished_checkpoint<T>(
        &self,
        id: Uuid,
        backend: &DynBackendInstance,
        checkpoint_id: &str,
        original: BlazeDaemonError,
    ) -> Result<T> {
        let compensation = self
            .resume_and_clear_checkpoint(id, backend, Some(checkpoint_id))
            .await;
        match compensation {
            Ok(()) => Err(original),
            Err(compensation) => Err(BlazeDaemonError::RecoveryRequired(format!(
                "{original}; checkpoint compensation failed: {compensation}"
            ))),
        }
    }

    async fn finish_failed_published_checkpoint<T>(
        &self,
        id: Uuid,
        backend: &DynBackendInstance,
        original: BlazeDaemonError,
    ) -> Result<T> {
        let compensation = self.resume_and_clear_checkpoint(id, backend, None).await;
        match compensation {
            Ok(()) => Err(original),
            Err(compensation) => Err(BlazeDaemonError::RecoveryRequired(format!(
                "{original}; checkpoint compensation failed: {compensation}"
            ))),
        }
    }

    async fn fail_published_checkpoint<T>(
        &self,
        backend: &DynBackendInstance,
        instance: &SandboxInstance,
        original: BlazeDaemonError,
        boundary: &str,
    ) -> Result<T> {
        let resume = self.resume_backend(backend).await;
        let recovery = self.mark_instance_recovery(instance.clone());
        Err(BlazeDaemonError::RecoveryRequired(format!(
            "checkpoint {boundary} failed: {original}{}{}",
            resume
                .err()
                .map(|error| format!("; backend resume failed: {error}"))
                .unwrap_or_default(),
            recovery
                .err()
                .map(|error| format!("; recovery state persistence failed: {error}"))
                .unwrap_or_default()
        )))
    }

    async fn resume_and_clear_checkpoint(
        &self,
        id: Uuid,
        backend: &DynBackendInstance,
        staging_checkpoint_id: Option<&str>,
    ) -> Result<()> {
        if let Err(error) = self.resume_backend(backend).await {
            let recovery = self.mark_recovery(id).err();
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "backend resume failed: {error}{}",
                recovery
                    .map(|error| format!("; recovery state persistence failed: {error}"))
                    .unwrap_or_default()
            )));
        }
        if let Some(checkpoint_id) = staging_checkpoint_id
            && let Err(error) = self.checkpoints.abort_staging(id, checkpoint_id)
        {
            let recovery = self.mark_recovery(id).err();
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "checkpoint staging cleanup failed: {error}{}",
                recovery
                    .map(|error| format!("; recovery state persistence failed: {error}"))
                    .unwrap_or_default()
            )));
        }

        let mut instance = self.get(id)?;
        if instance.state == SandboxState::Paused {
            instance.transition(SandboxState::Running)?;
        }
        instance.finish_operation();
        if let Err(error) = self.persist_and_retain(instance) {
            let recovery = self.mark_recovery(id).err();
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "checkpoint compensation state commit failed: {error}{}",
                recovery
                    .map(|error| format!("; recovery state persistence failed: {error}"))
                    .unwrap_or_default()
            )));
        }
        Ok(())
    }

    async fn resume_backend(&self, backend: &DynBackendInstance) -> Result<()> {
        match crate::failpoint::backend("checkpoint-compensation-resume") {
            Ok(()) => backend.resume().await?,
            Err(error) => return Err(error.into()),
        }
        self.verify_backend_ready(backend.instance_id(), backend)
            .await
    }

    async fn require_live_backend(&self, id: Uuid, backend: &DynBackendInstance) -> Result<()> {
        match backend.try_wait().await {
            Ok(None) => Ok(()),
            Ok(Some(result)) => {
                self.mark_recovery(id)?;
                Err(BlazeDaemonError::RecoveryRequired(format!(
                    "instance {id} backend exited before checkpoint capture \
                     (exit={:?}, signal={:?})",
                    result.exit_code, result.signal
                )))
            }
            Err(error) => {
                self.mark_recovery(id)?;
                Err(BlazeDaemonError::RecoveryRequired(format!(
                    "instance {id} backend liveness is unknown: {error}"
                )))
            }
        }
    }

    async fn verify_backend_ready(&self, id: Uuid, backend: &DynBackendInstance) -> Result<()> {
        self.require_live_backend(id, backend).await?;
        self.wait_for_guest_ready(backend, "checkpoint-guest-ready")
            .await?;
        self.require_live_backend(id, backend).await
    }
}

fn checkpoint_store_error(error: impl std::fmt::Display) -> BlazeDaemonError {
    BlazeDaemonError::Internal(format!("checkpoint store: {error}"))
}
