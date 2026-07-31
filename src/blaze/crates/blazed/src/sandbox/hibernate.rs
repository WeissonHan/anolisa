// SPDX-License-Identifier: Apache-2.0
//! Durable hibernation and restartable resume for managed sandboxes.

use std::collections::BTreeSet;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use blaze_core::backend::{BackendKind, RestoreRequest, SnapshotKind, SnapshotRequest};
use blaze_core::checkpoint::CheckpointArtifact;
use blaze_core::lifecycle::{BackendOwnership, OperationPhase, SandboxInstance, SandboxState};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

use crate::error::{BlazeDaemonError, Result};
use crate::spawner::DynBackendInstance;

use super::manager::SandboxManager;

const HIBERNATE_FORMAT_VERSION: u32 = 1;
const HIBERNATE_DIRECTORY: &str = "hibernate";
const MANIFEST_ARTIFACT: &str = "manifest.json";
const MEMORY_ARTIFACT: &str = "memory.snap";
const VMSTATE_ARTIFACT: &str = "vmstate.snap";
const REQUIRED_ARTIFACTS: [&str; 2] = [VMSTATE_ARTIFACT, MEMORY_ARTIFACT];

/// Inputs resolved from the current daemon configuration before hibernation.
#[derive(Debug, Clone)]
pub struct HibernateSandbox {
    /// Current executable for the sandbox backend.
    pub binary_path: PathBuf,
}

/// Inputs resolved from the current daemon configuration before resume.
#[derive(Debug, Clone)]
pub struct ResumeSandbox {
    /// Current executable for the sandbox backend.
    pub binary_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct HibernateManifest {
    format_version: u32,
    sandbox_id: Uuid,
    policy_name: String,
    image_digest: String,
    backend: BackendKind,
    backend_version: Option<String>,
    snapshot_kind: SnapshotKind,
    expose_guest_socket: bool,
    network_slot: Option<usize>,
    artifacts: Vec<CheckpointArtifact>,
}

impl SandboxManager {
    /// Stop a running backend after publishing durable hibernation artifacts.
    pub fn hibernate(
        &self,
        id: Uuid,
        request: HibernateSandbox,
    ) -> Pin<Box<dyn Future<Output = Result<SandboxInstance>> + Send + '_>> {
        Box::pin(async move {
            let _operation = self.operation_lock(id).lock_owned().await;
            let mut instance = self.get(id)?;
            require_quiescent_state(&instance, SandboxState::Running)?;

            let backend = self.backend_owner(id).ok_or_else(|| {
                BlazeDaemonError::Conflict(format!("instance {id} has no backend owner"))
            })?;
            if backend.instance_id() != id || backend.backend() != instance.backend {
                self.mark_instance_recovery(instance)?;
                return Err(BlazeDaemonError::RecoveryRequired(format!(
                    "instance {id} backend owner identity does not match durable state"
                )));
            }
            require_backend_live(id, &backend).await.map_err(|error| {
                let recovery = self.mark_instance_recovery(instance.clone()).err();
                with_recovery_error(error, recovery)
            })?;
            if !backend.supports_checkpoint_capture() {
                return Err(BlazeDaemonError::UnsupportedOperation(format!(
                    "instance {id} backend {} does not support hibernation",
                    backend.backend()
                )));
            }

            let spawner = self.spawner(instance.backend).ok_or_else(|| {
                BlazeDaemonError::UnsupportedOperation(format!(
                    "instance {id} has no resume adapter for {}",
                    instance.backend
                ))
            })?;
            let capability = spawner
                .restore_capability(&request.binary_path)
                .await?
                .ok_or_else(|| {
                    BlazeDaemonError::UnsupportedOperation(format!(
                        "instance {id} backend {} does not support resume",
                        instance.backend
                    ))
                })?;
            let backend_version = backend.version().map(str::to_string);
            if capability.backend != instance.backend
                || capability.version != backend_version
                || capability.snapshot_kind != SnapshotKind::Full
            {
                return Err(BlazeDaemonError::UnsupportedOperation(format!(
                    "instance {id} backend capture identity does not match its resume adapter"
                )));
            }
            let storage = self.storage.reconstruct(&id.to_string()).await?;
            let expose_guest_socket = !backend.guest_socket_path().as_os_str().is_empty();
            let network_slot = backend.network_slot();
            let hibernate_dir = self.hibernate_dir(id);
            if let Err(error) = prepare_hibernate_directory(&hibernate_dir).await {
                let recovery = self.mark_instance_recovery(instance).err();
                return Err(with_recovery_error(error, recovery));
            }

            instance.begin_hibernate_operation()?;
            instance.transition(SandboxState::Hibernating)?;
            crate::failpoint::state("hibernate-begin-state")
                .and_then(|_| self.persist_and_retain(instance.clone()))?;
            crate::failpoint::pause("hibernate-after-begin").await;

            let paused = match crate::failpoint::backend("hibernate-pause") {
                Ok(()) => backend.pause().await,
                Err(error) => Err(error),
            };
            if let Err(error) = paused {
                return Err(self
                    .compensate_hibernate(
                        instance,
                        &backend,
                        None,
                        format!("backend pause failed: {error}"),
                    )
                    .await);
            }
            if let Err(error) = instance
                .advance_hibernate_phase(OperationPhase::HibernatePaused)
                .map_err(BlazeDaemonError::from)
                .and_then(|_| {
                    crate::failpoint::state("hibernate-paused-state")?;
                    self.persist_and_retain(instance.clone())
                })
            {
                return Err(self
                    .compensate_hibernate(
                        instance,
                        &backend,
                        None,
                        format!("paused-state commit failed: {error}"),
                    )
                    .await);
            }

            let staging_dir = self.hibernate_staging_dir(id);
            if let Err(error) = tokio::fs::create_dir(&staging_dir).await {
                return Err(self
                    .compensate_hibernate(
                        instance,
                        &backend,
                        Some(&staging_dir),
                        format!("staging directory creation failed: {error}"),
                    )
                    .await);
            }
            let snapshot_path = staging_dir.join(VMSTATE_ARTIFACT);
            let memory_path = staging_dir.join(MEMORY_ARTIFACT);
            let snapshot = match crate::failpoint::backend("hibernate-snapshot") {
                Ok(()) => {
                    backend
                        .snapshot(SnapshotRequest {
                            snapshot_path: snapshot_path.clone(),
                            mem_path: memory_path.clone(),
                            kind: SnapshotKind::Full,
                        })
                        .await
                }
                Err(error) => Err(error),
            };
            match snapshot {
                Ok(snapshot)
                    if snapshot.snapshot_path == snapshot_path
                        && snapshot.mem_path == memory_path => {}
                Ok(snapshot) => {
                    return Err(self
                        .compensate_hibernate(
                            instance,
                            &backend,
                            Some(&staging_dir),
                            format!(
                                "backend returned unexpected hibernation artifacts ({}, {})",
                                snapshot.snapshot_path.display(),
                                snapshot.mem_path.display()
                            ),
                        )
                        .await);
                }
                Err(error) => {
                    return Err(self
                        .compensate_hibernate(
                            instance,
                            &backend,
                            Some(&staging_dir),
                            format!("snapshot capture failed: {error}"),
                        )
                        .await);
                }
            }
            let flushed = match crate::failpoint::storage("hibernate-storage-flush") {
                Ok(()) => self.storage.flush_dirty(&storage).await,
                Err(error) => Err(error),
            };
            if let Err(error) = flushed {
                return Err(self
                    .compensate_hibernate(
                        instance,
                        &backend,
                        Some(&staging_dir),
                        format!("storage flush failed: {error}"),
                    )
                    .await);
            }

            let manifest = match build_hibernate_manifest(
                &staging_dir,
                &instance,
                capability.version,
                expose_guest_socket,
                network_slot,
            )
            .await
            {
                Ok(manifest) => manifest,
                Err(error) => {
                    return Err(self
                        .compensate_hibernate(
                            instance,
                            &backend,
                            Some(&staging_dir),
                            format!("artifact hashing failed: {error}"),
                        )
                        .await);
                }
            };
            if let Err(error) = write_and_sync_manifest(&staging_dir, &manifest).await {
                return Err(self
                    .compensate_hibernate(
                        instance,
                        &backend,
                        Some(&staging_dir),
                        format!("artifact publication failed: {error}"),
                    )
                    .await);
            }
            if let Err(error) = instance
                .advance_hibernate_phase(OperationPhase::HibernateArtifactsSynced)
                .map_err(BlazeDaemonError::from)
                .and_then(|_| {
                    crate::failpoint::state("hibernate-artifacts-state")?;
                    self.persist_and_retain(instance.clone())
                })
            {
                return Err(self
                    .compensate_hibernate(
                        instance,
                        &backend,
                        Some(&staging_dir),
                        format!("artifact-state commit failed: {error}"),
                    )
                    .await);
            }

            let stopped = match crate::failpoint::backend("hibernate-backend-stop") {
                Ok(()) => backend.kill().await,
                Err(error) => Err(error),
            };
            if let Err(error) = stopped {
                instance.backend_ownership = BackendOwnership::Unknown;
                return Err(self.fail_hibernate_after_stop(
                    instance,
                    format!("backend termination failed: {error}"),
                ));
            }
            instance.backend_ownership = BackendOwnership::Stopped;
            if let Err(error) = instance
                .advance_hibernate_phase(OperationPhase::HibernateBackendStopped)
                .map_err(BlazeDaemonError::from)
                .and_then(|_| {
                    crate::failpoint::state("hibernate-stopped-state")?;
                    self.persist_and_retain(instance.clone())
                })
            {
                return Err(self.fail_hibernate_after_stop(
                    instance,
                    format!("backend stopped but lifecycle commit failed: {error}"),
                ));
            }
            self.remove_backend_owner(id);
            crate::failpoint::pause("hibernate-after-stop").await;

            let backup_dir = self.hibernate_backup_dir(id);
            if hibernate_dir.exists() {
                if let Err(error) = tokio::fs::rename(&hibernate_dir, &backup_dir).await {
                    return Err(self.fail_hibernate_after_stop(
                        instance,
                        format!("previous hibernation backup failed: {error}"),
                    ));
                }
                if let Err(error) = sync_directory(self.instance_dir(id)).await {
                    return Err(self.fail_hibernate_after_stop(
                        instance,
                        format!("previous hibernation backup sync failed: {error}"),
                    ));
                }
            }
            let published = match crate::failpoint::storage("hibernate-publish") {
                Ok(()) => tokio::fs::rename(&staging_dir, &hibernate_dir)
                    .await
                    .map_err(BlazeDaemonError::from),
                Err(error) => Err(error.into()),
            };
            if let Err(error) = published {
                return Err(self.fail_hibernate_after_stop(
                    instance,
                    format!("hibernate directory publication failed: {error}"),
                ));
            }
            if let Err(error) = sync_directory(self.instance_dir(id)).await {
                return Err(self.fail_hibernate_after_stop(
                    instance,
                    format!("hibernate directory sync failed: {error}"),
                ));
            }
            if let Err(error) = instance
                .advance_hibernate_phase(OperationPhase::HibernatePublished)
                .map_err(BlazeDaemonError::from)
                .and_then(|_| {
                    crate::failpoint::state("hibernate-published-state")?;
                    self.persist_and_retain(instance.clone())
                })
            {
                return Err(self.fail_hibernate_after_stop(
                    instance,
                    format!("published-state commit failed: {error}"),
                ));
            }

            let recovery = instance.clone();
            instance.transition(SandboxState::Hibernated)?;
            instance.finish_operation();
            if let Err(error) = crate::failpoint::state("hibernate-final-state")
                .and_then(|_| self.persist_and_retain(instance.clone()))
            {
                return Err(self.fail_hibernate_after_stop(
                    recovery,
                    format!("final hibernated-state commit failed: {error}"),
                ));
            }
            if backup_dir.exists()
                && let Err(error) = remove_directory_and_sync(&backup_dir).await
            {
                tracing::warn!(
                    instance = %id,
                    %error,
                    "obsolete hibernation backup retained for later cleanup"
                );
            }
            Ok(instance)
        })
    }

    /// Start a backend from verified hibernation artifacts.
    pub fn resume(
        &self,
        id: Uuid,
        request: ResumeSandbox,
    ) -> Pin<Box<dyn Future<Output = Result<SandboxInstance>> + Send + '_>> {
        Box::pin(async move {
            let _operation = self.operation_lock(id).lock_owned().await;
            let mut instance = self.get(id)?;
            require_quiescent_state(&instance, SandboxState::Hibernated)?;
            if instance.backend_ownership != BackendOwnership::Stopped {
                self.mark_instance_recovery(instance)?;
                return Err(BlazeDaemonError::RecoveryRequired(format!(
                    "instance {id} is hibernated with unresolved backend ownership"
                )));
            }
            if self.backend_owner(id).is_some() {
                self.mark_instance_recovery(instance)?;
                return Err(BlazeDaemonError::RecoveryRequired(format!(
                    "instance {id} is hibernated but still retains a backend owner"
                )));
            }

            let hibernate_dir = self.hibernate_dir(id);
            let manifest = load_and_verify_manifest(&hibernate_dir)
                .await
                .map_err(|error| {
                    let recovery = self.mark_instance_recovery(instance.clone()).err();
                    with_recovery_error(
                        BlazeDaemonError::RecoveryRequired(format!(
                            "instance {id} hibernation artifacts are invalid: {error}"
                        )),
                        recovery,
                    )
                })?;
            if let Err(error) = validate_manifest_identity(&manifest, &instance) {
                let recovery = self.mark_instance_recovery(instance).err();
                return Err(with_recovery_error(error, recovery));
            }
            let spawner = self.spawner(instance.backend).ok_or_else(|| {
                BlazeDaemonError::UnsupportedOperation(format!(
                    "instance {id} has no resume adapter for {}",
                    instance.backend
                ))
            })?;
            let capability = spawner
                .restore_capability(&request.binary_path)
                .await?
                .ok_or_else(|| {
                    BlazeDaemonError::UnsupportedOperation(format!(
                        "instance {id} backend {} does not support resume",
                        instance.backend
                    ))
                })?;
            if capability.backend != manifest.backend
                || capability.version != manifest.backend_version
                || capability.snapshot_kind != manifest.snapshot_kind
            {
                return Err(BlazeDaemonError::UnsupportedOperation(format!(
                    "instance {id} hibernation image is incompatible with the current resume adapter"
                )));
            }
            let storage = self.storage.reconstruct(&id.to_string()).await?;

            instance.begin_resume_operation()?;
            instance.transition(SandboxState::Resuming)?;
            crate::failpoint::state("resume-begin-state")
                .and_then(|_| self.persist_and_retain(instance.clone()))?;
            crate::failpoint::pause("resume-after-begin").await;

            let run_dir = self.instance_dir(id);
            if let Err(error) = spawner.prepare_spawn(&run_dir).await {
                return Err(self.fail_resume_without_owner(
                    instance,
                    format!("resume ownership preparation failed: {error}"),
                ));
            }
            instance.backend_ownership = BackendOwnership::Starting;
            if let Err(error) = instance
                .advance_resume_phase(OperationPhase::ResumeBackendStarting)
                .map_err(BlazeDaemonError::from)
                .and_then(|_| {
                    crate::failpoint::state("resume-starting-state")?;
                    self.persist_and_retain(instance.clone())
                })
            {
                return Err(self.fail_resume_without_owner(
                    instance,
                    format!("resume ownership intent commit failed: {error}"),
                ));
            }

            let restored = match crate::failpoint::backend("resume-backend-start") {
                Ok(()) => {
                    spawner
                        .restore(RestoreRequest {
                            instance_id: id,
                            run_dir,
                            binary_path: request.binary_path,
                            storage,
                            snapshot_path: hibernate_dir.join(VMSTATE_ARTIFACT),
                            mem_path: hibernate_dir.join(MEMORY_ARTIFACT),
                            checkpoint_backend: manifest.backend,
                            expected_version: manifest.backend_version.clone(),
                            snapshot_kind: manifest.snapshot_kind,
                            expose_guest_socket: manifest.expose_guest_socket,
                            network_slot: manifest.network_slot,
                        })
                        .await
                }
                Err(error) => Err(crate::spawner::SpawnFailure::clean(error)),
            };
            let restored = match restored {
                Ok(owner) => owner,
                Err(error) => {
                    let (source, owner) = error.into_parts();
                    if let Some(owner) = owner {
                        let _ = self.retain_backend(id, owner);
                        instance.backend_ownership = BackendOwnership::Running;
                        return Err(self.fail_resume_with_owner(
                            instance,
                            format!("resume backend start failed: {source}"),
                        ));
                    }
                    return Err(self.fail_resume_without_owner(
                        instance,
                        format!("resume backend start failed: {source}"),
                    ));
                }
            };
            instance.backend_ownership = BackendOwnership::Running;
            if let Some(error) = self.retain_backend(id, restored.clone()) {
                return Err(self.fail_resume_with_owner(instance, error));
            }
            if restored.instance_id() != id
                || restored.backend() != manifest.backend
                || restored.version().map(str::to_string) != manifest.backend_version
            {
                return Err(self
                    .abort_resumed_backend(
                        instance,
                        &restored,
                        "restored backend identity does not match the hibernation manifest"
                            .to_string(),
                    )
                    .await);
            }
            if let Err(error) = instance
                .advance_resume_phase(OperationPhase::ResumeBackendStarted)
                .map_err(BlazeDaemonError::from)
                .and_then(|_| {
                    crate::failpoint::state("resume-started-state")?;
                    self.persist_and_retain(instance.clone())
                })
            {
                return Err(self.fail_resume_with_owner(
                    instance,
                    format!("restored backend ownership commit failed: {error}"),
                ));
            }
            if let Err(error) = self
                .verify_resumed_backend(id, &restored, manifest.expose_guest_socket)
                .await
            {
                return Err(self
                    .abort_resumed_backend(
                        instance,
                        &restored,
                        format!("restored backend readiness failed: {error}"),
                    )
                    .await);
            }
            if let Err(error) = instance
                .advance_resume_phase(OperationPhase::ResumeBackendReady)
                .map_err(BlazeDaemonError::from)
                .and_then(|_| {
                    crate::failpoint::state("resume-ready-state")?;
                    self.persist_and_retain(instance.clone())
                })
            {
                return Err(self.fail_resume_with_owner(
                    instance,
                    format!("restored backend readiness commit failed: {error}"),
                ));
            }

            let recovery = instance.clone();
            instance.transition(SandboxState::Running)?;
            instance.finish_operation();
            if let Err(error) = crate::failpoint::state("resume-final-state")
                .and_then(|_| self.persist_and_retain(instance.clone()))
            {
                return Err(self.fail_resume_with_owner(
                    recovery,
                    format!("final running-state commit failed: {error}"),
                ));
            }
            Ok(instance)
        })
    }

    async fn compensate_hibernate(
        &self,
        mut instance: SandboxInstance,
        backend: &DynBackendInstance,
        staging_dir: Option<&Path>,
        cause: String,
    ) -> BlazeDaemonError {
        let resumed = match crate::failpoint::backend("hibernate-compensation-resume") {
            Ok(()) => backend.resume().await,
            Err(error) => Err(error),
        };
        if let Err(error) = resumed {
            instance.backend_ownership = BackendOwnership::Unknown;
            return self.fail_hibernate_after_stop(
                instance,
                format!("{cause}; backend resume compensation failed: {error}"),
            );
        }
        if let Err(error) = self
            .verify_resumed_backend(
                instance.id,
                backend,
                !backend.guest_socket_path().as_os_str().is_empty(),
            )
            .await
        {
            instance.backend_ownership = BackendOwnership::Unknown;
            return self.fail_hibernate_after_stop(
                instance,
                format!("{cause}; resumed backend readiness failed: {error}"),
            );
        }
        if let Some(staging_dir) = staging_dir
            && let Err(error) = remove_directory_and_sync(staging_dir).await
        {
            return self.fail_hibernate_after_stop(
                instance,
                format!("{cause}; staging cleanup failed: {error}"),
            );
        }
        let recovery = instance.clone();
        instance.backend_ownership = BackendOwnership::Running;
        if let Err(error) = instance.transition(SandboxState::Running) {
            return self.fail_hibernate_after_stop(
                recovery,
                format!("{cause}; running-state compensation failed: {error}"),
            );
        }
        instance.finish_operation();
        if let Err(error) = self.persist_and_retain(instance) {
            return self.fail_hibernate_after_stop(
                recovery,
                format!("{cause}; running-state compensation commit failed: {error}"),
            );
        }
        BlazeDaemonError::Internal(cause)
    }

    async fn verify_resumed_backend(
        &self,
        id: Uuid,
        backend: &DynBackendInstance,
        expose_guest_socket: bool,
    ) -> Result<()> {
        require_backend_live(id, backend).await?;
        if expose_guest_socket {
            self.wait_for_guest_ready(backend, "resume-guest-ready")
                .await?;
        }
        require_backend_live(id, backend).await
    }

    async fn abort_resumed_backend(
        &self,
        mut instance: SandboxInstance,
        backend: &DynBackendInstance,
        cause: String,
    ) -> BlazeDaemonError {
        let stopped = match crate::failpoint::backend("resume-backend-stop") {
            Ok(()) => backend.kill().await,
            Err(error) => Err(error),
        };
        if let Err(error) = stopped {
            instance.backend_ownership = BackendOwnership::Unknown;
            return self.fail_resume_with_owner(
                instance,
                format!("{cause}; restored backend termination failed: {error}"),
            );
        }
        self.remove_backend_owner(instance.id);
        instance.backend_ownership = BackendOwnership::Stopped;
        self.fail_resume_without_owner(instance, cause)
    }

    fn fail_resume_without_owner(
        &self,
        mut instance: SandboxInstance,
        cause: String,
    ) -> BlazeDaemonError {
        let recovery = instance.clone();
        instance.backend_ownership = BackendOwnership::Stopped;
        if let Err(error) = instance.transition(SandboxState::Hibernated) {
            return self.fail_resume_with_owner(
                recovery,
                format!("{cause}; hibernated-state compensation failed: {error}"),
            );
        }
        instance.finish_operation();
        if let Err(error) = self.persist_and_retain(instance) {
            return self.fail_resume_with_owner(
                recovery,
                format!("{cause}; hibernated-state compensation commit failed: {error}"),
            );
        }
        BlazeDaemonError::Internal(cause)
    }

    fn fail_hibernate_after_stop(
        &self,
        instance: SandboxInstance,
        cause: String,
    ) -> BlazeDaemonError {
        let id = instance.id;
        let recovery = self.mark_instance_recovery(instance).err();
        BlazeDaemonError::RecoveryRequired(format!(
            "hibernate {id}: {cause}; resources retained{}",
            recovery
                .map(|error| format!("; recovery state persistence failed: {error}"))
                .unwrap_or_default()
        ))
    }

    fn fail_resume_with_owner(&self, instance: SandboxInstance, cause: String) -> BlazeDaemonError {
        let id = instance.id;
        let recovery = self.mark_instance_recovery(instance).err();
        BlazeDaemonError::RecoveryRequired(format!(
            "resume {id}: {cause}; resources retained{}",
            recovery
                .map(|error| format!("; recovery state persistence failed: {error}"))
                .unwrap_or_default()
        ))
    }

    pub(super) async fn cleanup_hibernate_artifacts(&self, id: Uuid) -> Result<()> {
        let instance_dir = self.instance_dir(id);
        let mut entries = match tokio::fs::read_dir(&instance_dir).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name == HIBERNATE_DIRECTORY
                || (name.starts_with(".hibernate.")
                    && (name.ends_with(".tmp") || name.ends_with(".bak")))
            {
                remove_directory_and_sync(&entry.path()).await?;
            }
        }
        Ok(())
    }

    fn instance_dir(&self, id: Uuid) -> PathBuf {
        self.state_dir.join(id.to_string())
    }

    fn hibernate_dir(&self, id: Uuid) -> PathBuf {
        self.instance_dir(id).join(HIBERNATE_DIRECTORY)
    }

    fn hibernate_staging_dir(&self, id: Uuid) -> PathBuf {
        self.instance_dir(id)
            .join(format!(".hibernate.{}.tmp", Uuid::new_v4()))
    }

    fn hibernate_backup_dir(&self, id: Uuid) -> PathBuf {
        self.instance_dir(id)
            .join(format!(".hibernate.{}.bak", Uuid::new_v4()))
    }
}

fn require_quiescent_state(instance: &SandboxInstance, expected: SandboxState) -> Result<()> {
    if let Some(journal) = &instance.operation {
        return Err(BlazeDaemonError::RecoveryRequired(format!(
            "instance {} has unfinished {} operation",
            instance.id, journal.kind
        )));
    }
    if instance.state != expected {
        return Err(BlazeDaemonError::Conflict(format!(
            "instance {} is {}, expected {expected}",
            instance.id, instance.state
        )));
    }
    Ok(())
}

async fn require_backend_live(id: Uuid, backend: &DynBackendInstance) -> Result<()> {
    match backend.try_wait().await {
        Ok(None) => Ok(()),
        Ok(Some(result)) => Err(BlazeDaemonError::RecoveryRequired(format!(
            "instance {id} backend exited (exit={:?}, signal={:?})",
            result.exit_code, result.signal
        ))),
        Err(error) => Err(BlazeDaemonError::RecoveryRequired(format!(
            "instance {id} backend liveness is unknown: {error}"
        ))),
    }
}

async fn build_hibernate_manifest(
    directory: &Path,
    instance: &SandboxInstance,
    backend_version: Option<String>,
    expose_guest_socket: bool,
    network_slot: Option<usize>,
) -> Result<HibernateManifest> {
    let mut artifacts = Vec::with_capacity(REQUIRED_ARTIFACTS.len());
    for name in REQUIRED_ARTIFACTS {
        artifacts.push(hash_artifact(&directory.join(name), name).await?);
    }
    Ok(HibernateManifest {
        format_version: HIBERNATE_FORMAT_VERSION,
        sandbox_id: instance.id,
        policy_name: instance.policy_name.clone(),
        image_digest: instance.image_digest.clone(),
        backend: instance.backend,
        backend_version,
        snapshot_kind: SnapshotKind::Full,
        expose_guest_socket,
        network_slot,
        artifacts,
    })
}

async fn write_and_sync_manifest(directory: &Path, manifest: &HibernateManifest) -> Result<()> {
    for name in REQUIRED_ARTIFACTS {
        tokio::fs::File::open(directory.join(name))
            .await?
            .sync_all()
            .await?;
    }
    let mut encoded = serde_json::to_vec_pretty(manifest)?;
    encoded.push(b'\n');
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(directory.join(MANIFEST_ARTIFACT))
        .await?;
    file.write_all(&encoded).await?;
    file.sync_all().await?;
    drop(file);
    sync_directory(directory.to_path_buf()).await
}

async fn load_and_verify_manifest(directory: &Path) -> Result<HibernateManifest> {
    let manifest_path = directory.join(MANIFEST_ARTIFACT);
    let manifest_metadata = tokio::fs::symlink_metadata(&manifest_path).await?;
    if manifest_metadata.file_type().is_symlink() || !manifest_metadata.is_file() {
        return Err(BlazeDaemonError::Internal(format!(
            "hibernate manifest {} is not a regular file",
            manifest_path.display()
        )));
    }
    let manifest: HibernateManifest =
        serde_json::from_slice(&tokio::fs::read(&manifest_path).await?)?;
    if manifest.format_version != HIBERNATE_FORMAT_VERSION {
        return Err(BlazeDaemonError::UnsupportedOperation(format!(
            "unsupported hibernation format {}",
            manifest.format_version
        )));
    }
    if manifest.snapshot_kind != SnapshotKind::Full {
        return Err(BlazeDaemonError::UnsupportedOperation(
            "hibernation image is not self-contained".to_string(),
        ));
    }
    let expected_names = REQUIRED_ARTIFACTS
        .into_iter()
        .map(str::to_string)
        .collect::<BTreeSet<_>>();
    let observed_names = manifest
        .artifacts
        .iter()
        .map(|artifact| artifact.name.clone())
        .collect::<BTreeSet<_>>();
    if observed_names != expected_names || manifest.artifacts.len() != REQUIRED_ARTIFACTS.len() {
        return Err(BlazeDaemonError::Internal(
            "hibernation manifest has an invalid artifact set".to_string(),
        ));
    }
    let directory_names = read_directory_names(directory).await?;
    let expected_directory_names = [
        MANIFEST_ARTIFACT.to_string(),
        MEMORY_ARTIFACT.to_string(),
        VMSTATE_ARTIFACT.to_string(),
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    if directory_names != expected_directory_names {
        return Err(BlazeDaemonError::Internal(
            "hibernation directory has an unexpected file set".to_string(),
        ));
    }
    for artifact in &manifest.artifacts {
        let path = directory.join(&artifact.name);
        let metadata = tokio::fs::symlink_metadata(&path).await?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(BlazeDaemonError::Internal(format!(
                "hibernate artifact {} is not a regular file",
                path.display()
            )));
        }
        let observed = hash_artifact(&path, &artifact.name).await?;
        if &observed != artifact {
            return Err(BlazeDaemonError::Internal(format!(
                "hibernate artifact {} failed integrity verification",
                artifact.name
            )));
        }
    }
    Ok(manifest)
}

fn validate_manifest_identity(
    manifest: &HibernateManifest,
    instance: &SandboxInstance,
) -> Result<()> {
    if manifest.sandbox_id != instance.id
        || manifest.policy_name != instance.policy_name
        || manifest.image_digest != instance.image_digest
        || manifest.backend != instance.backend
    {
        return Err(BlazeDaemonError::RecoveryRequired(format!(
            "instance {} hibernation identity does not match durable lifecycle state",
            instance.id
        )));
    }
    Ok(())
}

async fn hash_artifact(path: &Path, name: &str) -> Result<CheckpointArtifact> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut size_bytes = 0_u64;
    let mut buffer = [0_u8; 128 * 1024];
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        size_bytes = size_bytes
            .checked_add(read as u64)
            .ok_or_else(|| BlazeDaemonError::Internal("artifact size overflow".to_string()))?;
        hasher.update(&buffer[..read]);
    }
    Ok(CheckpointArtifact {
        name: name.to_string(),
        size_bytes,
        sha256: format!("{:x}", hasher.finalize()),
    })
}

async fn read_directory_names(directory: &Path) -> Result<BTreeSet<String>> {
    let mut names = BTreeSet::new();
    let mut entries = tokio::fs::read_dir(directory).await?;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name().into_string().map_err(|_| {
            BlazeDaemonError::Internal(format!(
                "hibernate directory {} contains a non-UTF-8 name",
                directory.display()
            ))
        })?;
        names.insert(name);
    }
    Ok(names)
}

async fn prepare_hibernate_directory(final_dir: &Path) -> Result<()> {
    let parent = final_dir.parent().ok_or_else(|| {
        BlazeDaemonError::Internal(format!(
            "hibernate directory {} has no parent",
            final_dir.display()
        ))
    })?;
    let mut entries = tokio::fs::read_dir(parent).await?;
    let mut obsolete_backups = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(".hibernate.") && name.ends_with(".tmp") {
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "instance directory {} contains unfinished hibernation artifacts",
                parent.display()
            )));
        }
        if name.starts_with(".hibernate.") && name.ends_with(".bak") {
            if !final_dir.is_dir() {
                return Err(BlazeDaemonError::RecoveryRequired(format!(
                    "instance directory {} contains an unpaired hibernation backup",
                    parent.display()
                )));
            }
            obsolete_backups.push(entry.path());
        }
    }
    for backup in obsolete_backups {
        remove_directory_and_sync(&backup).await?;
    }
    Ok(())
}

async fn sync_directory(directory: PathBuf) -> Result<()> {
    tokio::fs::File::open(directory).await?.sync_all().await?;
    Ok(())
}

async fn remove_directory_and_sync(directory: &Path) -> Result<()> {
    match tokio::fs::remove_dir_all(directory).await {
        Ok(()) => {
            let parent = directory.parent().ok_or_else(|| {
                BlazeDaemonError::Internal(format!(
                    "removed hibernation path {} has no parent",
                    directory.display()
                ))
            })?;
            sync_directory(parent.to_path_buf()).await
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn with_recovery_error(
    error: BlazeDaemonError,
    recovery: Option<BlazeDaemonError>,
) -> BlazeDaemonError {
    match recovery {
        Some(recovery) => BlazeDaemonError::RecoveryRequired(format!(
            "{error}; recovery state persistence failed: {recovery}"
        )),
        None => error,
    }
}

#[cfg(test)]
mod tests {
    use blaze_core::backend::BackendKind;
    use blaze_core::lifecycle::StartPath;
    use blaze_core::policy::WorkloadClass;

    use super::*;

    #[tokio::test]
    async fn manifest_preserves_the_network_slot_for_resume() {
        let temp = tempfile::tempdir().expect("temp");
        tokio::fs::write(temp.path().join(VMSTATE_ARTIFACT), b"vmstate")
            .await
            .expect("VM state");
        tokio::fs::write(temp.path().join(MEMORY_ARTIFACT), b"memory")
            .await
            .expect("memory");
        let instance = SandboxInstance::new(
            BackendKind::Firecracker,
            WorkloadClass::AgentTool,
            "sha256:image".to_string(),
            StartPath::Cold,
            "default".to_string(),
        );

        let manifest = build_hibernate_manifest(
            temp.path(),
            &instance,
            Some("Firecracker v1.16.0".to_string()),
            true,
            Some(7),
        )
        .await
        .expect("manifest");

        assert!(manifest.expose_guest_socket);
        assert_eq!(manifest.network_slot, Some(7));
    }
}
