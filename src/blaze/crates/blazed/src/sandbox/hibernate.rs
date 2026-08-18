// SPDX-License-Identifier: Apache-2.0
//! Durable hibernation and restartable resume for managed sandboxes.

use std::collections::BTreeSet;
use std::future::Future;
use std::os::fd::OwnedFd;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use blaze_core::backend::{BackendKind, RestoreRequest, SnapshotKind, SnapshotRequest};
use blaze_core::checkpoint::CheckpointArtifact;
use blaze_core::lifecycle::{BackendOwnership, OperationPhase, SandboxInstance, SandboxState};
use rustix::fs::{
    AtFlags, Dir, Mode, OFlags, RenameFlags, fsync, mkdirat, openat, renameat_with, unlinkat,
};
use rustix::io::Errno;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::error::{BlazeDaemonError, Result};
use crate::spawner::{BackendRestoreRequest, DynBackendInstance};
use crate::state_store::OwnedRunDir;

use super::manager::SandboxManager;

const HIBERNATE_FORMAT_VERSION: u32 = 1;
const HIBERNATE_DIRECTORY: &str = "hibernate";
const HIBERNATE_DIRECTORY_MODE: Mode = Mode::RWXU;
const HIBERNATE_FILE_MODE: Mode = Mode::RUSR.union(Mode::WUSR);
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
            let sandbox_dir = match self.hibernate_root(id) {
                Ok(sandbox_dir) => sandbox_dir,
                Err(error) => {
                    let recovery = self.mark_instance_recovery(instance).err();
                    return Err(with_recovery_error(error, recovery));
                }
            };
            if let Err(error) = prepare_hibernate_directory(&sandbox_dir) {
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

            let staging_name = hibernate_staging_name();
            let staging_dir = match create_child_directory(&sandbox_dir, &staging_name) {
                Ok(staging_dir) => staging_dir,
                Err(error) => {
                    return Err(self
                        .compensate_hibernate(
                            instance,
                            &backend,
                            Some((&sandbox_dir, &staging_name)),
                            format!("staging directory creation failed: {error}"),
                        )
                        .await);
                }
            };
            let snapshot_path = staging_dir.configured_path().join(VMSTATE_ARTIFACT);
            let memory_path = staging_dir.configured_path().join(MEMORY_ARTIFACT);
            // The request names both destinations, so a successful adapter has
            // written exactly these paths. The manifest step below reopens them
            // through the staging descriptor and hashes whatever landed there
            // before the image is published.
            let snapshot = match crate::failpoint::backend("hibernate-snapshot") {
                Ok(()) => {
                    backend
                        .snapshot(SnapshotRequest {
                            snapshot_path,
                            mem_path: memory_path,
                            kind: SnapshotKind::Full,
                        })
                        .await
                }
                Err(error) => Err(error),
            };
            if let Err(error) = snapshot {
                return Err(self
                    .compensate_hibernate(
                        instance,
                        &backend,
                        Some((&sandbox_dir, &staging_name)),
                        format!("snapshot capture failed: {error}"),
                    )
                    .await);
            }
            let flushed = match crate::failpoint::storage("hibernate-storage-flush") {
                Ok(()) => self.storage.sync_artifacts(&storage).await,
                Err(error) => Err(error),
            };
            if let Err(error) = flushed {
                return Err(self
                    .compensate_hibernate(
                        instance,
                        &backend,
                        Some((&sandbox_dir, &staging_name)),
                        format!("storage flush failed: {error}"),
                    )
                    .await);
            }

            let manifest = match build_hibernate_manifest(
                &staging_dir,
                &instance,
                capability.version,
                expose_guest_socket,
            )
            .await
            {
                Ok(manifest) => manifest,
                Err(error) => {
                    return Err(self
                        .compensate_hibernate(
                            instance,
                            &backend,
                            Some((&sandbox_dir, &staging_name)),
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
                        Some((&sandbox_dir, &staging_name)),
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
                        Some((&sandbox_dir, &staging_name)),
                        format!("artifact-state commit failed: {error}"),
                    )
                    .await);
            }
            // The staging descriptor is no longer needed: publication renames
            // the name, and every later step reopens through the sandbox
            // descriptor.
            drop(staging_dir);

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

            let backup_name = hibernate_backup_name();
            let previous = match optional_child_directory(&sandbox_dir, hibernate_dir_name()) {
                Ok(previous) => previous,
                Err(error) => {
                    return Err(self.fail_hibernate_after_stop(
                        instance,
                        format!("previous hibernation lookup failed: {error}"),
                    ));
                }
            };
            let had_previous = previous.is_some();
            drop(previous);
            if had_previous {
                if let Err(error) = renameat_with(
                    sandbox_dir.descriptor(),
                    hibernate_dir_name(),
                    sandbox_dir.descriptor(),
                    backup_name.as_str(),
                    RenameFlags::NOREPLACE,
                ) {
                    return Err(self.fail_hibernate_after_stop(
                        instance,
                        format!("previous hibernation backup failed: {error}"),
                    ));
                }
                if let Err(error) = sync_run_dir(&sandbox_dir) {
                    return Err(self.fail_hibernate_after_stop(
                        instance,
                        format!("previous hibernation backup sync failed: {error}"),
                    ));
                }
            }
            let published = match crate::failpoint::storage("hibernate-publish") {
                Ok(()) => renameat_with(
                    sandbox_dir.descriptor(),
                    staging_name.as_str(),
                    sandbox_dir.descriptor(),
                    hibernate_dir_name(),
                    RenameFlags::NOREPLACE,
                )
                .map_err(|source| {
                    hibernate_io_error(
                        "publish hibernation directory",
                        sandbox_dir.configured_path().join(hibernate_dir_name()),
                        std::io::Error::from(source),
                    )
                }),
                Err(error) => Err(error.into()),
            };
            if let Err(error) = published {
                return Err(self.fail_hibernate_after_stop(
                    instance,
                    format!("hibernate directory publication failed: {error}"),
                ));
            }
            if let Err(error) = sync_run_dir(&sandbox_dir) {
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
            if had_previous && let Err(error) = remove_child_directory(&sandbox_dir, &backup_name) {
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

            let sandbox_dir = match self.hibernate_root(id) {
                Ok(sandbox_dir) => sandbox_dir,
                Err(error) => {
                    let recovery = self.mark_instance_recovery(instance).err();
                    return Err(with_recovery_error(error, recovery));
                }
            };
            let hibernate_dir = match open_child_directory(&sandbox_dir, hibernate_dir_name()) {
                Ok(hibernate_dir) => hibernate_dir,
                Err(error) => {
                    let recovery = self.mark_instance_recovery(instance).err();
                    return Err(with_recovery_error(
                        BlazeDaemonError::RecoveryRequired(format!(
                            "instance {id} hibernation image is unavailable: {error}"
                        )),
                        recovery,
                    ));
                }
            };
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

            let run_dir = match self.run_directory(id) {
                Ok(run_dir) => run_dir,
                Err(error) => {
                    return Err(self.fail_resume_without_owner(
                        instance,
                        format!("resume runtime directory lookup failed: {error}"),
                    ));
                }
            };
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
                Ok(()) => match BackendRestoreRequest::new(
                    RestoreRequest {
                        instance_id: id,
                        binary_path: request.binary_path,
                        storage,
                        snapshot_path: hibernate_dir.configured_path().join(VMSTATE_ARTIFACT),
                        mem_path: hibernate_dir.configured_path().join(MEMORY_ARTIFACT),
                        checkpoint_backend: manifest.backend,
                        expected_version: manifest.backend_version.clone(),
                        snapshot_kind: manifest.snapshot_kind,
                        expose_guest_socket: manifest.expose_guest_socket,
                    },
                    run_dir,
                ) {
                    Ok(request) => spawner.restore(request).await,
                    Err(error) => Err(crate::spawner::SpawnFailure::clean(error)),
                },
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
        staging: Option<(&OwnedRunDir, &str)>,
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
        if let Some((sandbox_dir, staging_name)) = staging
            && let Err(error) = remove_child_directory(sandbox_dir, staging_name)
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
        // Destroy runs after the runtime directory may already be released, so
        // a missing sandbox directory simply means there is nothing to reclaim.
        let sandbox_dir = match self.hibernate_root(id) {
            Ok(sandbox_dir) => sandbox_dir,
            Err(BlazeDaemonError::NotFound(_)) => return Ok(()),
            Err(error) => return Err(error),
        };
        for name in run_dir_names(&sandbox_dir)? {
            if name == HIBERNATE_DIRECTORY
                || (name.starts_with(".hibernate.")
                    && (name.ends_with(".tmp") || name.ends_with(".bak")))
            {
                remove_child_directory(&sandbox_dir, &name)?;
            }
        }
        Ok(())
    }

    /// Borrow the retained sandbox directory that owns hibernation state.
    ///
    /// Every hibernation object is resolved relative to this descriptor, so a
    /// replaced or symlinked instance directory cannot redirect the image.
    fn hibernate_root(&self, id: Uuid) -> Result<OwnedRunDir> {
        self.run_directory(id)
    }
}

/// Name of the published hibernation directory.
fn hibernate_dir_name() -> &'static str {
    HIBERNATE_DIRECTORY
}

/// Name of a private staging directory that only one hibernate call owns.
fn hibernate_staging_name() -> String {
    format!(".hibernate.{}.tmp", Uuid::new_v4())
}

/// Name of the backup that retains the previous image during publication.
fn hibernate_backup_name() -> String {
    format!(".hibernate.{}.bak", Uuid::new_v4())
}

fn hibernate_io_error(
    operation: &'static str,
    path: PathBuf,
    source: std::io::Error,
) -> BlazeDaemonError {
    BlazeDaemonError::Internal(format!("{operation} {}: {source}", path.display()))
}

/// Create one directory below the sandbox directory and return its owner.
fn create_child_directory(parent: &OwnedRunDir, name: &str) -> Result<OwnedHibernateDir> {
    mkdirat(parent.descriptor(), name, HIBERNATE_DIRECTORY_MODE).map_err(|source| {
        hibernate_io_error(
            "create hibernation directory",
            parent.configured_path().join(name),
            std::io::Error::from(source),
        )
    })?;
    open_child_directory(parent, name)
}

/// Open one existing directory below the sandbox directory.
fn open_child_directory(parent: &OwnedRunDir, name: &str) -> Result<OwnedHibernateDir> {
    let directory = openat(
        parent.descriptor(),
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|source| {
        hibernate_io_error(
            "open hibernation directory",
            parent.configured_path().join(name),
            std::io::Error::from(source),
        )
    })?;
    Ok(OwnedHibernateDir {
        configured_path: parent.configured_path().join(name),
        directory,
    })
}

/// Open one directory below the sandbox directory when it exists.
fn optional_child_directory(parent: &OwnedRunDir, name: &str) -> Result<Option<OwnedHibernateDir>> {
    match openat(
        parent.descriptor(),
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    ) {
        Ok(directory) => Ok(Some(OwnedHibernateDir {
            configured_path: parent.configured_path().join(name),
            directory,
        })),
        Err(Errno::NOENT) => Ok(None),
        Err(source) => Err(hibernate_io_error(
            "open hibernation directory",
            parent.configured_path().join(name),
            std::io::Error::from(source),
        )),
    }
}

/// Owner of one hibernation directory resolved from the sandbox descriptor.
struct OwnedHibernateDir {
    configured_path: PathBuf,
    directory: OwnedFd,
}

impl OwnedHibernateDir {
    fn descriptor(&self) -> &OwnedFd {
        &self.directory
    }

    /// Report the configured pathname for diagnostics only.
    fn configured_path(&self) -> &Path {
        &self.configured_path
    }

    /// Open one regular file below this directory, refusing symbolic links.
    fn open_file(&self, name: &str, operation: &'static str) -> Result<std::fs::File> {
        let descriptor = openat(
            self.descriptor(),
            name,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|source| {
            hibernate_io_error(
                operation,
                self.configured_path().join(name),
                std::io::Error::from(source),
            )
        })?;
        let file = std::fs::File::from(descriptor);
        let metadata = file.metadata().map_err(|source| {
            hibernate_io_error(operation, self.configured_path().join(name), source)
        })?;
        if !metadata.is_file() {
            return Err(BlazeDaemonError::Internal(format!(
                "hibernation object {} is not a regular file",
                self.configured_path().join(name).display()
            )));
        }
        Ok(file)
    }

    /// Create one new regular file below this directory.
    fn create_new_file(&self, name: &str, operation: &'static str) -> Result<std::fs::File> {
        let descriptor = openat(
            self.descriptor(),
            name,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            HIBERNATE_FILE_MODE,
        )
        .map_err(|source| {
            hibernate_io_error(
                operation,
                self.configured_path().join(name),
                std::io::Error::from(source),
            )
        })?;
        Ok(std::fs::File::from(descriptor))
    }

    /// Report every name this directory currently links.
    fn names(&self) -> Result<BTreeSet<String>> {
        let directory = self.directory.try_clone().map_err(|source| {
            hibernate_io_error(
                "scan hibernation directory",
                self.configured_path().to_path_buf(),
                source,
            )
        })?;
        let mut names = BTreeSet::new();
        for entry in Dir::new(directory).map_err(|source| {
            hibernate_io_error(
                "scan hibernation directory",
                self.configured_path().to_path_buf(),
                std::io::Error::from(source),
            )
        })? {
            let entry = entry.map_err(|source| {
                hibernate_io_error(
                    "scan hibernation directory",
                    self.configured_path().to_path_buf(),
                    std::io::Error::from(source),
                )
            })?;
            let name = entry.file_name().to_str().map_err(|_| {
                BlazeDaemonError::Internal(format!(
                    "hibernation directory {} contains a non-UTF-8 name",
                    self.configured_path().display()
                ))
            })?;
            if name == "." || name == ".." {
                continue;
            }
            names.insert(name.to_string());
        }
        Ok(names)
    }

    fn sync(&self) -> Result<()> {
        fsync(self.descriptor()).map_err(|source| {
            hibernate_io_error(
                "sync hibernation directory",
                self.configured_path().to_path_buf(),
                std::io::Error::from(source),
            )
        })
    }
}

/// Flush one directory so a rename or unlink survives a crash.
fn sync_run_dir(parent: &OwnedRunDir) -> Result<()> {
    fsync(parent.descriptor()).map_err(|source| {
        hibernate_io_error(
            "sync sandbox directory",
            parent.configured_path().to_path_buf(),
            std::io::Error::from(source),
        )
    })
}

/// Remove one directory tree below the sandbox directory and flush the parent.
fn remove_child_directory(parent: &OwnedRunDir, name: &str) -> Result<()> {
    let Some(directory) = optional_child_directory(parent, name)? else {
        return Ok(());
    };
    for entry in directory.names()? {
        unlinkat(directory.descriptor(), entry.as_str(), AtFlags::empty()).map_err(|source| {
            hibernate_io_error(
                "remove hibernation file",
                directory.configured_path().join(&entry),
                std::io::Error::from(source),
            )
        })?;
    }
    directory.sync()?;
    drop(directory);
    unlinkat(parent.descriptor(), name, AtFlags::REMOVEDIR).map_err(|source| {
        hibernate_io_error(
            "remove hibernation directory",
            parent.configured_path().join(name),
            std::io::Error::from(source),
        )
    })?;
    sync_run_dir(parent)
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
    directory: &OwnedHibernateDir,
    instance: &SandboxInstance,
    backend_version: Option<String>,
    expose_guest_socket: bool,
) -> Result<HibernateManifest> {
    let mut artifacts = Vec::with_capacity(REQUIRED_ARTIFACTS.len());
    for name in REQUIRED_ARTIFACTS {
        artifacts.push(hash_artifact(directory, name).await?);
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
        artifacts,
    })
}

async fn write_and_sync_manifest(
    directory: &OwnedHibernateDir,
    manifest: &HibernateManifest,
) -> Result<()> {
    for name in REQUIRED_ARTIFACTS {
        let artifact = directory.open_file(name, "sync hibernation artifact")?;
        crate::failpoint::spawn_blocking(move || artifact.sync_all())
            .await
            .map_err(|error| {
                BlazeDaemonError::Internal(format!(
                    "hibernation artifact sync task failed: {error}"
                ))
            })??;
    }
    let mut encoded = serde_json::to_vec_pretty(manifest)?;
    encoded.push(b'\n');
    let file = directory.create_new_file(MANIFEST_ARTIFACT, "publish hibernation manifest")?;
    crate::failpoint::spawn_blocking(move || {
        use std::io::Write;

        let mut file = file;
        file.write_all(&encoded)?;
        file.sync_all()
    })
    .await
    .map_err(|error| {
        BlazeDaemonError::Internal(format!("hibernation manifest write task failed: {error}"))
    })??;
    directory.sync()
}

async fn load_and_verify_manifest(directory: &OwnedHibernateDir) -> Result<HibernateManifest> {
    let manifest_file = directory.open_file(MANIFEST_ARTIFACT, "read hibernation manifest")?;
    let encoded = crate::failpoint::spawn_blocking(move || {
        use std::io::Read;

        let mut manifest_file = manifest_file;
        let mut encoded = Vec::new();
        manifest_file.read_to_end(&mut encoded).map(|_| encoded)
    })
    .await
    .map_err(|error| {
        BlazeDaemonError::Internal(format!("hibernation manifest read task failed: {error}"))
    })??;
    let manifest: HibernateManifest = serde_json::from_slice(&encoded)?;
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
    let expected_directory_names = [
        MANIFEST_ARTIFACT.to_string(),
        MEMORY_ARTIFACT.to_string(),
        VMSTATE_ARTIFACT.to_string(),
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    if directory.names()? != expected_directory_names {
        return Err(BlazeDaemonError::Internal(
            "hibernation directory has an unexpected file set".to_string(),
        ));
    }
    for artifact in &manifest.artifacts {
        let observed = hash_artifact(directory, &artifact.name).await?;
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

/// Hash one artifact through its retained directory descriptor.
///
/// Hashing reads the whole artifact, so it runs on the blocking pool instead of
/// occupying an async worker.
async fn hash_artifact(directory: &OwnedHibernateDir, name: &str) -> Result<CheckpointArtifact> {
    let file = directory.open_file(name, "read hibernation artifact")?;
    let owned_name = name.to_string();
    crate::failpoint::spawn_blocking(move || {
        use std::io::Read;

        let mut file = file;
        let mut hasher = Sha256::new();
        let mut size_bytes = 0_u64;
        let mut buffer = [0_u8; 128 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            size_bytes = size_bytes.checked_add(read as u64).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "artifact size overflow")
            })?;
            hasher.update(&buffer[..read]);
        }
        Ok(CheckpointArtifact {
            name: owned_name,
            size_bytes,
            sha256: format!("{:x}", hasher.finalize()),
        })
    })
    .await
    .map_err(|error| {
        BlazeDaemonError::Internal(format!("hibernation artifact hash task failed: {error}"))
    })?
    .map_err(|source: std::io::Error| {
        hibernate_io_error(
            "hash hibernation artifact",
            directory.configured_path().join(name),
            source,
        )
    })
}

/// Reject unfinished scratch and release obsolete backups before publication.
fn prepare_hibernate_directory(parent: &OwnedRunDir) -> Result<()> {
    let published = optional_child_directory(parent, hibernate_dir_name())?.is_some();
    let mut obsolete_backups = Vec::new();
    for name in run_dir_names(parent)? {
        if name.starts_with(".hibernate.") && name.ends_with(".tmp") {
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "instance directory {} contains unfinished hibernation artifacts",
                parent.configured_path().display()
            )));
        }
        if name.starts_with(".hibernate.") && name.ends_with(".bak") {
            if !published {
                return Err(BlazeDaemonError::RecoveryRequired(format!(
                    "instance directory {} contains an unpaired hibernation backup",
                    parent.configured_path().display()
                )));
            }
            obsolete_backups.push(name);
        }
    }
    for backup in obsolete_backups {
        remove_child_directory(parent, &backup)?;
    }
    Ok(())
}

/// Report every name the sandbox directory currently links.
fn run_dir_names(parent: &OwnedRunDir) -> Result<BTreeSet<String>> {
    let directory = parent.descriptor().try_clone().map_err(|source| {
        hibernate_io_error(
            "scan sandbox directory",
            parent.configured_path().to_path_buf(),
            source,
        )
    })?;
    let mut names = BTreeSet::new();
    for entry in Dir::new(directory).map_err(|source| {
        hibernate_io_error(
            "scan sandbox directory",
            parent.configured_path().to_path_buf(),
            std::io::Error::from(source),
        )
    })? {
        let entry = entry.map_err(|source| {
            hibernate_io_error(
                "scan sandbox directory",
                parent.configured_path().to_path_buf(),
                std::io::Error::from(source),
            )
        })?;
        let name = entry.file_name().to_str().map_err(|_| {
            BlazeDaemonError::Internal(format!(
                "instance directory {} contains a non-UTF-8 name",
                parent.configured_path().display()
            ))
        })?;
        if name == "." || name == ".." {
            continue;
        }
        names.insert(name.to_string());
    }
    Ok(names)
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
