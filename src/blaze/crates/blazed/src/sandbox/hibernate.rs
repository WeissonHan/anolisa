// SPDX-License-Identifier: Apache-2.0
//! Hibernate and resume transactions with durable operation markers.

use std::sync::Arc;

use blaze_core::backend::{RestoreRequest, SnapshotKind, SnapshotRequest, SpawnRequest};
use blaze_core::lifecycle::{OperationKind, SandboxState};
use uuid::Uuid;

use crate::error::{BlazeDaemonError, Result};
use crate::guest::GuestClient;

use super::manager::{SandboxManager, guest_enabled, network_config};

impl SandboxManager {
    /// Snapshot and terminate a running backend while retaining its storage.
    pub async fn hibernate(&self, id: Uuid) -> Result<()> {
        let runtime = self.runtime(id)?;
        let mut runtime = runtime.lock().await;
        self.require_state(id, SandboxState::Running)?;
        let backend = runtime.backend.as_ref().cloned().ok_or_else(|| {
            BlazeDaemonError::Conflict(format!("sandbox {id} has no backend instance"))
        })?;
        crate::failpoint::state("hibernate-begin-state")?;
        self.update_instance(id, |metadata| {
            metadata.begin_operation(OperationKind::Hibernate, None);
            metadata.transition(SandboxState::Hibernating)?;
            Ok(())
        })?;
        crate::failpoint::pause("hibernate-after-begin").await;
        let paused = match crate::failpoint::backend("hibernate-pause") {
            Ok(()) => backend.pause().await,
            Err(error) => Err(error),
        };
        if let Err(error) = paused {
            self.compensate_hibernate_failure(id, &backend, None)
                .await?;
            return Err(error.into());
        }

        let final_dir = self.state_dir.join(id.to_string()).join("hibernate");
        let staging_dir = self
            .state_dir
            .join(id.to_string())
            .join(format!(".hibernate.{}.tmp", Uuid::new_v4()));
        if let Err(error) = tokio::fs::create_dir_all(&staging_dir).await {
            self.compensate_hibernate_failure(id, &backend, Some(&staging_dir))
                .await?;
            return Err(error.into());
        }
        let request = SnapshotRequest {
            snapshot_path: staging_dir.join("vmstate.snap"),
            mem_path: staging_dir.join("mem.diff"),
            kind: SnapshotKind::Full,
        };
        let snapshot = match crate::failpoint::backend("hibernate-snapshot") {
            Ok(()) => backend.snapshot(request).await,
            Err(error) => Err(error),
        };
        if let Err(error) = snapshot {
            self.compensate_hibernate_failure(id, &backend, Some(&staging_dir))
                .await?;
            return Err(error.into());
        }
        let flushed = match crate::failpoint::storage("hibernate-storage-flush") {
            Ok(()) => self.storage.flush_dirty(&runtime.storage).await,
            Err(error) => Err(error),
        };
        if let Err(error) = flushed {
            self.compensate_hibernate_failure(id, &backend, Some(&staging_dir))
                .await?;
            return Err(error.into());
        }
        let synced = match crate::failpoint::storage("hibernate-artifact-sync") {
            Ok(()) => sync_hibernate_artifacts(&staging_dir).await,
            Err(error) => Err(error.into()),
        };
        if let Err(error) = synced {
            self.compensate_hibernate_failure(id, &backend, Some(&staging_dir))
                .await?;
            return Err(error);
        }
        let killed = match crate::failpoint::backend("hibernate-kill") {
            Ok(()) => backend.kill().await,
            Err(error) => Err(error),
        };
        if let Err(error) = killed {
            self.mark_recovery(id)?;
            return Err(error.into());
        }
        runtime.backend = None;
        runtime.guest = None;
        if final_dir.exists()
            && let Err(error) = tokio::fs::remove_dir_all(&final_dir).await
        {
            self.mark_recovery(id)?;
            return Err(error.into());
        }
        if let Err(error) = crate::failpoint::storage("hibernate-publish") {
            self.mark_recovery(id)?;
            return Err(error.into());
        }
        if let Err(error) = tokio::fs::rename(&staging_dir, &final_dir).await {
            self.mark_recovery(id)?;
            return Err(error.into());
        }
        let committed = match crate::failpoint::state("hibernate-final-state") {
            Ok(()) => self.update_instance(id, |metadata| {
                metadata.transition(SandboxState::Hibernated)?;
                metadata.finish_operation();
                Ok(())
            }),
            Err(error) => Err(error),
        };
        if let Err(error) = committed {
            self.mark_recovery(id)?;
            return Err(error);
        }
        Ok(())
    }

    /// Restore a hibernated sandbox and wait until its guest is usable.
    pub async fn resume(self: &Arc<Self>, id: Uuid) -> Result<()> {
        let runtime = match self.runtime(id) {
            Ok(runtime) => runtime,
            Err(_) => self.reconstruct_hibernated_runtime(id).await?,
        };
        let mut runtime = runtime.lock().await;
        self.require_state(id, SandboxState::Hibernated)?;
        crate::failpoint::state("resume-begin-state")?;
        let metadata = self.update_instance(id, |metadata| {
            metadata.begin_operation(OperationKind::Resume, None);
            metadata.transition(SandboxState::Resuming)?;
            Ok(())
        })?;
        crate::failpoint::pause("resume-after-begin").await;
        let backend_config = metadata.backend_config.clone();
        let hibernate_dir = self.state_dir.join(id.to_string()).join("hibernate");
        let run_dir = self.runtime_dir_for(id, metadata.start_path);
        let request = RestoreRequest {
            spawn: SpawnRequest {
                instance_id: id,
                run_dir: run_dir.clone(),
                binary_path: self.binary_path(),
                storage: runtime.storage.clone(),
                backend: backend_config.clone(),
                vm: metadata.vm_config.clone(),
                network: network_config(&backend_config),
            },
            snapshot_path: hibernate_dir.join("vmstate.snap"),
            mem_path: hibernate_dir.join("mem.diff"),
            track_dirty: true,
        };
        let restored = match crate::failpoint::backend("resume-restore") {
            Ok(()) => self.spawner.restore(request).await,
            Err(error) => Err(error),
        };
        let restored = match restored {
            Ok(restored) => restored,
            Err(error) => {
                let cleaned = match crate::failpoint::backend("resume-orphan-cleanup") {
                    Ok(()) => self.spawner.cleanup_orphan(id, &run_dir).await,
                    Err(error) => Err(error),
                };
                if let Err(cleanup) = cleaned {
                    self.mark_recovery(id)?;
                    return Err(BlazeDaemonError::RecoveryRequired(format!(
                        "resume restore failed ({error}); orphan cleanup failed ({cleanup})"
                    )));
                }
                self.restore_hibernated_after_resume_error(id)?;
                return Err(error.into());
            }
        };
        let guest = if guest_enabled(restored.backend(), &backend_config) {
            let guest = GuestClient::new(
                restored.guest_socket_path().to_path_buf(),
                self.request_timeout(),
                self.config.api.max_file_bytes,
            );
            let ready = match crate::failpoint::guest("resume-guest-ready") {
                Ok(()) => {
                    guest
                        .wait_ready(self.request_timeout(), &self.cancellation)
                        .await
                }
                Err(error) => Err(error),
            };
            if let Err(error) = ready {
                let cleaned = match crate::failpoint::backend("resume-kill") {
                    Ok(()) => restored.kill().await,
                    Err(error) => Err(error),
                };
                if let Err(cleanup) = cleaned {
                    runtime.backend = Some(restored);
                    self.mark_recovery(id)?;
                    return Err(BlazeDaemonError::RecoveryRequired(format!(
                        "resume guest readiness failed ({error}); backend cleanup failed ({cleanup})"
                    )));
                }
                self.restore_hibernated_after_resume_error(id)?;
                return Err(error.into());
            }
            Some(guest)
        } else {
            None
        };
        let committed = match crate::failpoint::state("resume-final-state") {
            Ok(()) => self.update_instance(id, |metadata| {
                metadata.transition(SandboxState::Running)?;
                metadata.finish_operation();
                Ok(())
            }),
            Err(error) => Err(error),
        };
        if let Err(error) = committed {
            let cleaned = match crate::failpoint::backend("resume-kill") {
                Ok(()) => restored.kill().await,
                Err(error) => Err(error),
            };
            if let Err(cleanup) = cleaned {
                runtime.backend = Some(restored);
                runtime.guest = guest;
                self.mark_recovery(id)?;
                return Err(BlazeDaemonError::RecoveryRequired(format!(
                    "resume state commit failed ({error}); backend cleanup failed ({cleanup})"
                )));
            }
            self.restore_hibernated_after_resume_error(id)?;
            return Err(error);
        }
        runtime.backend = Some(restored);
        runtime.guest = guest;
        let supervised_backend = runtime.backend.as_ref().cloned();
        drop(runtime);
        if let Some(backend) = supervised_backend {
            self.start_backend_supervisor(id, backend);
        }
        Ok(())
    }

    async fn compensate_hibernate_failure(
        &self,
        id: Uuid,
        backend: &crate::spawner::DynBackendInstance,
        staging_dir: Option<&std::path::Path>,
    ) -> Result<()> {
        let resumed = match crate::failpoint::backend("hibernate-compensation-resume") {
            Ok(()) => backend.resume().await,
            Err(error) => Err(error),
        };
        match resumed {
            Ok(()) => {
                if let Some(staging_dir) = staging_dir
                    && let Err(error) = remove_dir_all_if_exists(staging_dir).await
                {
                    self.mark_recovery(id)?;
                    return Err(error);
                }
                self.restore_running_after_hibernate_error(id)
            }
            Err(error) => {
                self.mark_recovery(id)?;
                Err(error.into())
            }
        }
    }

    fn restore_running_after_hibernate_error(&self, id: Uuid) -> Result<()> {
        self.update_instance(id, |metadata| {
            metadata.transition(SandboxState::Running)?;
            metadata.finish_operation();
            Ok(())
        })?;
        Ok(())
    }

    fn restore_hibernated_after_resume_error(&self, id: Uuid) -> Result<()> {
        self.update_instance(id, |metadata| {
            metadata.transition(SandboxState::Hibernated)?;
            metadata.finish_operation();
            Ok(())
        })?;
        Ok(())
    }

    /// Remove published and unpublished hibernate artifacts after an explicit
    /// destroy. Recovery-required sandboxes retain them until this point.
    pub(super) async fn cleanup_hibernate_artifacts(&self, id: Uuid) -> Result<()> {
        let directory = self.state_dir.join(id.to_string());
        let mut entries = match tokio::fs::read_dir(&directory).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name == "hibernate" || (name.starts_with(".hibernate.") && name.ends_with(".tmp")) {
                remove_dir_all_if_exists(&entry.path()).await?;
            }
        }
        Ok(())
    }
}

async fn sync_hibernate_artifacts(directory: &std::path::Path) -> Result<()> {
    for name in ["vmstate.snap", "mem.diff"] {
        tokio::fs::File::open(directory.join(name))
            .await?
            .sync_all()
            .await?;
    }
    tokio::fs::File::open(directory).await?.sync_all().await?;
    Ok(())
}

async fn remove_dir_all_if_exists(path: &std::path::Path) -> Result<()> {
    match tokio::fs::remove_dir_all(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}
