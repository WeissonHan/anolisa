// SPDX-License-Identifier: Apache-2.0
//! Checkpoint, rollback, lineage, and pruning transactions.

use std::path::Path;
use std::sync::Arc;

use blaze_core::backend::{RestoreRequest, SnapshotKind, SnapshotRequest, SpawnRequest};
use blaze_core::checkpoint::{CheckpointInfo, CheckpointMetadata, CommitCheckpoint};
use blaze_core::lifecycle::{OperationKind, SandboxState};
use uuid::Uuid;

use crate::error::{BlazeDaemonError, Result};
use crate::guest::GuestClient;

use super::manager::{SandboxManager, guest_enabled, network_config};

impl SandboxManager {
    /// Commit a point-in-time checkpoint and resume the existing VM.
    pub async fn checkpoint(&self, id: Uuid) -> Result<CheckpointMetadata> {
        let runtime = self.runtime(id)?;
        let runtime = runtime.lock().await;
        self.require_state(id, SandboxState::Running)?;
        let backend = runtime.backend.as_ref().cloned().ok_or_else(|| {
            BlazeDaemonError::Conflict(format!("sandbox {id} has no backend instance"))
        })?;
        let sandbox = self.get(id)?;
        let guest = guest_enabled(backend.backend(), &sandbox.backend_config).then(|| {
            GuestClient::new(
                backend.guest_socket_path().to_path_buf(),
                self.request_timeout(),
            )
        });
        let stage = self.checkpoints.begin(id)?;
        let checkpoint_id = stage.id().to_string();
        if let Err(error) = crate::failpoint::state("checkpoint-begin-state") {
            let _ = self.checkpoints.abort(stage);
            return Err(error);
        }
        if let Err(error) = self.update_instance(id, |metadata| {
            metadata.begin_operation(OperationKind::Checkpoint, Some(checkpoint_id.clone()));
            Ok(())
        }) {
            let _ = self.checkpoints.abort(stage);
            return Err(error);
        }
        crate::failpoint::pause("checkpoint-after-begin").await;

        let paused = match crate::failpoint::backend("checkpoint-pause") {
            Ok(()) => backend.pause().await,
            Err(error) => Err(error),
        };
        if let Err(error) = paused {
            self.recover_paused_checkpoint(id, &backend, guest.as_ref(), Some(&checkpoint_id))
                .await?;
            return Err(error.into());
        }
        let paused_state = match crate::failpoint::state("checkpoint-paused-state") {
            Ok(()) => self.update_instance(id, |metadata| {
                metadata.transition(SandboxState::Paused)?;
                Ok(())
            }),
            Err(error) => Err(error),
        };
        if let Err(error) = paused_state {
            self.recover_paused_checkpoint(id, &backend, guest.as_ref(), Some(&checkpoint_id))
                .await?;
            return Err(error);
        }

        let request = SnapshotRequest {
            snapshot_path: stage.artifact_path("vmstate.snap")?,
            mem_path: stage.artifact_path("mem.diff")?,
            kind: SnapshotKind::Full,
        };
        let snapshot = match crate::failpoint::backend("checkpoint-snapshot") {
            Ok(()) => backend.snapshot(request).await,
            Err(error) => Err(error),
        };
        if let Err(error) = snapshot {
            self.recover_paused_checkpoint(id, &backend, guest.as_ref(), Some(&checkpoint_id))
                .await?;
            return Err(error.into());
        }
        let flushed = match crate::failpoint::storage("checkpoint-storage-flush") {
            Ok(()) => self.storage.flush_dirty(&runtime.storage).await,
            Err(error) => Err(error),
        };
        if let Err(error) = flushed {
            self.recover_paused_checkpoint(id, &backend, guest.as_ref(), Some(&checkpoint_id))
                .await?;
            return Err(error.into());
        }
        let rootfs_copied = match crate::failpoint::storage("checkpoint-rootfs-copy") {
            Ok(()) => {
                copy_sparse(
                    &runtime.storage.rootfs_path,
                    &stage.artifact_path("rootfs.diff")?,
                )
                .await
            }
            Err(error) => Err(error.into()),
        };
        if let Err(error) = rootfs_copied {
            self.recover_paused_checkpoint(id, &backend, guest.as_ref(), Some(&checkpoint_id))
                .await?;
            return Err(error);
        }

        let metadata = match self.get(id) {
            Ok(metadata) => metadata,
            Err(error) => {
                self.recover_paused_checkpoint(id, &backend, guest.as_ref(), Some(&checkpoint_id))
                    .await?;
                return Err(error);
            }
        };
        let parent = match self.checkpoints.read_head(id) {
            Ok(parent) => parent,
            Err(error) => {
                self.recover_paused_checkpoint(id, &backend, guest.as_ref(), Some(&checkpoint_id))
                    .await?;
                return Err(error.into());
            }
        };
        let store = self.checkpoints.clone();
        let backend_version = backend.version().map(str::to_string);
        let published = match crate::failpoint::storage("checkpoint-publish") {
            Ok(()) => {
                blocking_checkpoint(move || {
                    store.publish(
                        &stage,
                        CommitCheckpoint {
                            parent,
                            template_name: metadata.template_name.clone(),
                            image_digest: metadata.image_digest.clone(),
                            backend: metadata.backend,
                            backend_version,
                            snapshot_kind: SnapshotKind::Full,
                        },
                    )
                })
                .await
            }
            Err(error) => Err(error.into()),
        };
        let committed = match published {
            Ok(committed) => committed,
            Err(error) => {
                self.recover_paused_checkpoint(id, &backend, guest.as_ref(), Some(&checkpoint_id))
                    .await?;
                return Err(error);
            }
        };
        crate::failpoint::pause("checkpoint-after-publish-before-head").await;
        let store = self.checkpoints.clone();
        let checkpoint_for_head = checkpoint_id.clone();
        let head_updated = match crate::failpoint::storage("checkpoint-head-update") {
            Ok(()) => blocking_checkpoint(move || store.set_head(id, &checkpoint_for_head)).await,
            Err(error) => Err(error.into()),
        };
        if let Err(error) = head_updated {
            self.recover_paused_checkpoint(id, &backend, guest.as_ref(), None)
                .await?;
            return Err(error);
        }
        crate::failpoint::pause("checkpoint-after-head").await;
        let resumed = match crate::failpoint::backend("checkpoint-resume") {
            Ok(()) => backend.resume().await,
            Err(error) => Err(error),
        };
        if let Err(error) = resumed {
            self.mark_recovery(id)?;
            return Err(error.into());
        }
        self.wait_guest_ready_after_checkpoint_resume(id, guest.as_ref(), "checkpoint-guest-ready")
            .await?;
        let final_state = match crate::failpoint::state("checkpoint-final-state") {
            Ok(()) => self.update_instance(id, |metadata| {
                metadata.transition(SandboxState::Checkpointed)?;
                metadata.last_checkpoint = Some(checkpoint_id);
                metadata.transition(SandboxState::Running)?;
                metadata.finish_operation();
                Ok(())
            }),
            Err(error) => Err(error),
        };
        if let Err(error) = final_state {
            self.mark_recovery(id)?;
            return Err(error);
        }
        Ok(committed)
    }

    /// List committed checkpoints with HEAD reachability.
    pub async fn list_checkpoints(&self, id: Uuid) -> Result<Vec<CheckpointInfo>> {
        self.get(id)?;
        let store = self.checkpoints.clone();
        blocking_checkpoint(move || store.list(id)).await
    }

    /// Delete branches not reachable from HEAD while serializing with runtime operations.
    pub async fn prune_checkpoints(&self, id: Uuid) -> Result<Vec<String>> {
        let runtime = match self.runtime(id) {
            Ok(runtime) => runtime,
            Err(_) => self.reconstruct_hibernated_runtime(id).await?,
        };
        let _runtime = runtime.lock().await;
        let store = self.checkpoints.clone();
        blocking_checkpoint(move || store.prune(id)).await
    }

    /// Replace the current runtime with a verified checkpoint.
    pub async fn rollback(self: &Arc<Self>, id: Uuid, checkpoint_id: &str) -> Result<()> {
        let runtime = self.runtime(id)?;
        let mut runtime = runtime.lock().await;
        self.require_state(id, SandboxState::Running)?;
        let store = self.checkpoints.clone();
        let checkpoint_id_owned = checkpoint_id.to_string();
        let checkpoint = blocking_checkpoint(move || {
            let checkpoint = store.verify(id, &checkpoint_id_owned)?;
            if let Some(parent) = &checkpoint.parent {
                store.validate_chain_from(id, parent)?;
            }
            Ok(checkpoint)
        })
        .await?;
        let current = self.get(id)?;
        if checkpoint.image_digest != current.image_digest
            || checkpoint.template_name != current.template_name
            || checkpoint.backend != current.backend
        {
            return Err(BlazeDaemonError::Conflict(format!(
                "checkpoint {checkpoint_id} identity does not match sandbox {id}"
            )));
        }

        crate::failpoint::state("rollback-begin-state")?;
        self.update_instance(id, |metadata| {
            metadata.begin_operation(OperationKind::Rollback, Some(checkpoint_id.to_string()));
            metadata.transition(SandboxState::RollingBack)?;
            Ok(())
        })?;
        crate::failpoint::pause("rollback-after-begin").await;

        if let Some(backend) = runtime.backend.take() {
            let killed = match crate::failpoint::backend("rollback-kill") {
                Ok(()) => backend.kill().await,
                Err(error) => Err(error),
            };
            if let Err(error) = killed {
                runtime.backend = Some(backend);
                self.mark_recovery(id)?;
                return Err(error.into());
            }
        }
        let backup = runtime.storage.instance_dir.join("rootfs.pre-rollback");
        let backed_up = match crate::failpoint::storage("rollback-rootfs-backup") {
            Ok(()) => copy_sparse(&runtime.storage.rootfs_path, &backup).await,
            Err(error) => Err(error.into()),
        };
        if let Err(error) = backed_up {
            self.mark_recovery(id)?;
            return Err(error);
        }
        let checkpoint_rootfs = self
            .checkpoints
            .artifact_path(id, checkpoint_id, "rootfs.diff")?;
        let applied = match crate::failpoint::storage("rollback-rootfs-apply") {
            Ok(()) => copy_sparse(&checkpoint_rootfs, &runtime.storage.rootfs_path).await,
            Err(error) => Err(error.into()),
        };
        if let Err(error) = applied {
            self.mark_recovery(id)?;
            return Err(error);
        }
        crate::failpoint::pause("rollback-after-rootfs").await;

        let backend_config = current.backend_config.clone();
        let run_dir = self.runtime_dir_for(id, current.start_path);
        let request = RestoreRequest {
            spawn: SpawnRequest {
                instance_id: id,
                run_dir: run_dir.clone(),
                binary_path: self.binary_path(),
                storage: runtime.storage.clone(),
                backend: backend_config.clone(),
                vm: current.vm_config.clone(),
                network: network_config(&backend_config),
            },
            snapshot_path: self
                .checkpoints
                .artifact_path(id, checkpoint_id, "vmstate.snap")?,
            mem_path: self
                .checkpoints
                .artifact_path(id, checkpoint_id, "mem.diff")?,
            track_dirty: true,
        };
        let restored = match crate::failpoint::backend("rollback-restore") {
            Ok(()) => self.spawner.restore(request).await,
            Err(error) => Err(error),
        };
        let restored = match restored {
            Ok(restored) => restored,
            Err(error) => {
                let cleaned = match crate::failpoint::backend("rollback-orphan-cleanup") {
                    Ok(()) => self.spawner.cleanup_orphan(id, &run_dir).await,
                    Err(error) => Err(error),
                };
                if let Err(cleanup) = cleaned {
                    self.mark_recovery(id)?;
                    return Err(BlazeDaemonError::RecoveryRequired(format!(
                        "rollback restore failed ({error}); orphan cleanup failed ({cleanup})"
                    )));
                }
                self.mark_recovery(id)?;
                return Err(error.into());
            }
        };
        if guest_enabled(restored.backend(), &backend_config) {
            let guest = GuestClient::new(
                restored.guest_socket_path().to_path_buf(),
                self.request_timeout(),
            );
            let ready = match crate::failpoint::guest("rollback-guest-ready") {
                Ok(()) => {
                    guest
                        .wait_ready(self.request_timeout(), &self.cancellation)
                        .await
                }
                Err(error) => Err(error),
            };
            if let Err(error) = ready {
                let cleaned = match crate::failpoint::backend("rollback-restored-kill") {
                    Ok(()) => restored.kill().await,
                    Err(error) => Err(error),
                };
                if let Err(cleanup) = cleaned {
                    runtime.backend = Some(restored);
                    self.mark_recovery(id)?;
                    return Err(BlazeDaemonError::RecoveryRequired(format!(
                        "rollback guest readiness failed ({error}); backend cleanup failed ({cleanup})"
                    )));
                }
                self.mark_recovery(id)?;
                return Err(error.into());
            }
        }
        let store = self.checkpoints.clone();
        let checkpoint_id_owned = checkpoint_id.to_string();
        let head_updated = match crate::failpoint::storage("rollback-head-update") {
            Ok(()) => blocking_checkpoint(move || store.set_head(id, &checkpoint_id_owned)).await,
            Err(error) => Err(error.into()),
        };
        if let Err(error) = head_updated {
            let cleaned = match crate::failpoint::backend("rollback-restored-kill") {
                Ok(()) => restored.kill().await,
                Err(error) => Err(error),
            };
            if let Err(cleanup) = cleaned {
                runtime.backend = Some(restored);
                self.mark_recovery(id)?;
                return Err(BlazeDaemonError::RecoveryRequired(format!(
                    "checkpoint HEAD update failed ({error}); backend cleanup failed ({cleanup})"
                )));
            }
            self.mark_recovery(id)?;
            return Err(error);
        }
        crate::failpoint::pause("rollback-after-head").await;
        let final_state = match crate::failpoint::state("rollback-final-state") {
            Ok(()) => self.update_instance(id, |metadata| {
                metadata.last_checkpoint = Some(checkpoint_id.to_string());
                metadata.transition(SandboxState::Running)?;
                metadata.finish_operation();
                Ok(())
            }),
            Err(error) => Err(error),
        };
        if let Err(error) = final_state {
            let cleaned = match crate::failpoint::backend("rollback-restored-kill") {
                Ok(()) => restored.kill().await,
                Err(error) => Err(error),
            };
            if let Err(cleanup) = cleaned {
                runtime.backend = Some(restored);
                self.mark_recovery(id)?;
                return Err(BlazeDaemonError::RecoveryRequired(format!(
                    "rollback state commit failed ({error}); backend cleanup failed ({cleanup})"
                )));
            }
            self.mark_recovery(id)?;
            return Err(error);
        }
        runtime.backend = Some(restored);
        let supervised_backend = runtime.backend.as_ref().cloned();
        if let Err(error) = tokio::fs::remove_file(&backup).await {
            tracing::warn!(sandbox_id = %id, %error, "failed to remove rollback backup");
        }
        drop(runtime);
        if let Some(backend) = supervised_backend {
            self.start_backend_supervisor(id, backend);
        }
        Ok(())
    }

    /// Restore the checkpoint recorded as the sandbox HEAD.
    ///
    /// Keeps the compatibility `reset` precondition and lifecycle decision in
    /// the manager instead of the HTTP handler.
    pub async fn reset_to_head(self: &Arc<Self>, id: Uuid) -> Result<String> {
        let checkpoint_id = self.get(id)?.last_checkpoint.ok_or_else(|| {
            BlazeDaemonError::Conflict(format!("sandbox {id} has no checkpoint to reset to"))
        })?;
        self.rollback(id, &checkpoint_id).await?;
        Ok(checkpoint_id)
    }

    async fn recover_paused_checkpoint(
        &self,
        id: Uuid,
        backend: &crate::spawner::DynBackendInstance,
        guest: Option<&GuestClient>,
        staging_checkpoint_id: Option<&str>,
    ) -> Result<()> {
        let resumed = match crate::failpoint::backend("checkpoint-compensation-resume") {
            Ok(()) => backend.resume().await,
            Err(error) => Err(error),
        };
        match resumed {
            Ok(()) => {
                self.wait_guest_ready_after_checkpoint_resume(
                    id,
                    guest,
                    "checkpoint-compensation-guest-ready",
                )
                .await?;
                if let Some(checkpoint_id) = staging_checkpoint_id {
                    let store = self.checkpoints.clone();
                    let checkpoint_id = checkpoint_id.to_string();
                    if let Err(error) =
                        blocking_checkpoint(move || store.abort_staging(id, &checkpoint_id)).await
                    {
                        self.mark_recovery(id)?;
                        return Err(error);
                    }
                }
                if let Err(error) = self.update_instance(id, |metadata| {
                    if metadata.state != SandboxState::Running {
                        metadata.transition(SandboxState::Running)?;
                    }
                    metadata.finish_operation();
                    Ok(())
                }) {
                    self.mark_recovery(id)?;
                    return Err(error);
                }
            }
            Err(error) => {
                self.mark_recovery(id)?;
                return Err(error.into());
            }
        }
        Ok(())
    }

    async fn wait_guest_ready_after_checkpoint_resume(
        &self,
        id: Uuid,
        guest: Option<&GuestClient>,
        failpoint: &str,
    ) -> Result<()> {
        let Some(guest) = guest else {
            return Ok(());
        };
        let ready = match crate::failpoint::guest(failpoint) {
            Ok(()) => {
                guest
                    .wait_ready(self.request_timeout(), &self.cancellation)
                    .await
            }
            Err(error) => Err(error),
        };
        if let Err(error) = ready {
            self.mark_recovery(id)?;
            return Err(error.into());
        }
        Ok(())
    }

    pub(super) async fn cleanup_checkpoint_transactions(&self, id: Uuid) -> Result<()> {
        let store = self.checkpoints.clone();
        blocking_checkpoint(move || store.cleanup_transaction_artifacts(id).map(|_| ())).await
    }
}

async fn blocking_checkpoint<T>(
    operation: impl FnOnce() -> blaze_core::Result<T> + Send + 'static,
) -> Result<T>
where
    T: Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| {
            BlazeDaemonError::Internal(format!("checkpoint filesystem task failed: {error}"))
        })?
        .map_err(BlazeDaemonError::from)
}

async fn copy_sparse(source: &Path, target: &Path) -> Result<()> {
    if let Some(parent) = target.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    #[cfg(target_os = "linux")]
    {
        let output = tokio::process::Command::new("cp")
            .arg("--sparse=always")
            .arg("-f")
            .arg(source)
            .arg(target)
            .output()
            .await?;
        if !output.status.success() {
            return Err(BlazeDaemonError::Internal(format!(
                "copy {} -> {} failed with {}: {}",
                source.display(),
                target.display(),
                output.status,
                String::from_utf8_lossy(&output.stderr)
            )));
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        tokio::fs::copy(source, target).await?;
    }
    Ok(())
}
