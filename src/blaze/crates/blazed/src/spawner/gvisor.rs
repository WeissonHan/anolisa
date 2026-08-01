// SPDX-License-Identifier: Apache-2.0
//! gVisor (runsc) process ownership via foreground `runsc run`.
//!
//! PoC spawner: each sandbox is a foreground `runsc run` child process whose
//! lifetime mirrors the container. The OCI bundle is generated per instance
//! under the run directory; the rootfs is a shared read-only base image at
//! `<images_dir>/gvisor-rootfs`.

use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use blaze_core::backend::{
    BackendKind, RestoreRequest, SnapshotArtifacts, SnapshotCompression, SnapshotRequest,
    SpawnRequest,
};
use blaze_core::{BlazeError, Result};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use uuid::Uuid;

use super::{
    BackendInstance, BackendSpawner, DynBackendInstance, SpawnFailure, SpawnResult,
    record_backend_stopped, remove_file_if_exists, spawn_result, stopped_marker, terminate_child,
};

const ROOTFS_DIR_NAME: &str = "gvisor-rootfs";
const RUNSC_ROOT_DIR_NAME: &str = "runsc";
const DELETE_TIMEOUT: Duration = Duration::from_secs(10);
/// Budget for short runsc state operations (pause, resume, state).
const LIFECYCLE_TIMEOUT: Duration = Duration::from_secs(10);

/// gVisor backend factory.
///
/// The runsc state root is pinned explicitly. runsc's implicit default is
/// `$XDG_RUNTIME_DIR/runsc` falling back to `/var/run/runsc`, so a daemon
/// restarted under a different environment would not be able to address the
/// containers it started.
pub struct GvisorSpawner {
    images_dir: PathBuf,
    root_dir: PathBuf,
    /// Configured runsc binary. `cleanup_orphan` needs it because the trait
    /// hands it no binary path, and a PATH lookup could resolve a different
    /// runsc than the one that created the container.
    binary_path: PathBuf,
}

impl GvisorSpawner {
    /// Create a spawner using `<images_dir>/gvisor-rootfs` as the base image
    /// and `<state_dir>/runsc` as the runsc state root.
    ///
    /// One shared root per daemon is sufficient because container ids embed
    /// the instance uuid, and it keeps `runsc --root=<dir> list` usable as a
    /// reconciliation primitive.
    pub fn new(images_dir: PathBuf, state_dir: PathBuf, binary_path: PathBuf) -> Self {
        Self {
            images_dir,
            root_dir: state_dir.join(RUNSC_ROOT_DIR_NAME),
            binary_path,
        }
    }

    fn rootfs_path(&self) -> PathBuf {
        self.images_dir.join(ROOTFS_DIR_NAME)
    }
}

/// Top-level runsc flags. `--root` must precede the subcommand: runsc
/// rejects it afterwards with "flag provided but not defined".
fn base_argv(root_dir: &Path) -> Vec<OsString> {
    let mut root = OsString::from("--root=");
    root.push(root_dir);
    vec![root, OsString::from("--network=none")]
}

/// Argv for starting a sandbox in the foreground.
fn run_argv(root_dir: &Path, bundle: &Path, container_id: &str) -> Vec<OsString> {
    let mut argv = base_argv(root_dir);
    argv.push(OsString::from("run"));
    argv.push(OsString::from("--bundle"));
    argv.push(bundle.as_os_str().to_owned());
    argv.push(OsString::from(container_id));
    argv
}

/// Argv for a subcommand that only addresses existing container state.
fn state_argv(root_dir: &Path, subcommand: &str, args: &[&OsStr]) -> Vec<OsString> {
    let mut root = OsString::from("--root=");
    root.push(root_dir);
    let mut argv = vec![root, OsString::from(subcommand)];
    argv.extend(args.iter().map(|arg| (*arg).to_owned()));
    argv
}

/// Argv for tearing a sandbox down regardless of its current state.
fn delete_force_argv(root_dir: &Path, container_id: &str) -> Vec<OsString> {
    state_argv(
        root_dir,
        "delete",
        &[OsStr::new("--force"), OsStr::new(container_id)],
    )
}

/// Argv for freezing a running sandbox.
fn pause_argv(root_dir: &Path, container_id: &str) -> Vec<OsString> {
    state_argv(root_dir, "pause", &[OsStr::new(container_id)])
}

/// Argv for thawing a paused sandbox.
fn resume_argv(root_dir: &Path, container_id: &str) -> Vec<OsString> {
    state_argv(root_dir, "resume", &[OsStr::new(container_id)])
}

/// Argv for writing a checkpoint payload.
fn checkpoint_argv(
    root_dir: &Path,
    image_dir: &Path,
    leave_running: bool,
    compression: SnapshotCompression,
    container_id: &str,
) -> Vec<OsString> {
    let mut root = OsString::from("--root=");
    root.push(root_dir);
    let mut image = OsString::from("--image-path=");
    image.push(image_dir);
    let mut argv = vec![root, OsString::from("checkpoint"), image];
    if leave_running {
        argv.push(OsString::from("--leave-running"));
    }
    argv.push(OsString::from(match compression {
        SnapshotCompression::None => "--compression=none",
        SnapshotCompression::FlateBestSpeed => "--compression=flate-best-speed",
    }));
    argv.push(OsString::from(container_id));
    argv
}

/// Layout inside a snapshot payload. The daemon only supplies the payload
/// root; these names are private to the gVisor backend.
const SNAPSHOT_IMAGE_DIR: &str = "image";
const SNAPSHOT_BUNDLE_DIR: &str = "bundle";
const BUNDLE_SPEC_FILE: &str = "config.json";
/// Checkpointing a large sandbox writes its whole memory image, so this is
/// far more generous than the short state operations.
const CHECKPOINT_TIMEOUT: Duration = Duration::from_secs(120);
/// How long a restored sandbox may take to report itself running.
const RESTORE_READY_TIMEOUT: Duration = Duration::from_secs(30);
const RESTORE_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Bytes stored under `dir`, used for snapshot accounting.
fn dir_size(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|entry| match entry.metadata() {
            Ok(meta) if meta.is_dir() => dir_size(&entry.path()),
            Ok(meta) => meta.len(),
            Err(_) => 0,
        })
        .sum()
}

/// Argv for re-establishing a sandbox from a checkpoint payload.
///
/// The top-level flag set must match [`run_argv`]: those flags configure the
/// sandbox itself, and a mismatch between save and restore is a classic
/// restore failure.
fn restore_argv(
    root_dir: &Path,
    bundle: &Path,
    image_dir: &Path,
    container_id: &str,
) -> Vec<OsString> {
    let mut argv = base_argv(root_dir);
    argv.push(OsString::from("restore"));
    argv.push(OsString::from("--bundle"));
    argv.push(bundle.as_os_str().to_owned());
    let mut image = OsString::from("--image-path=");
    image.push(image_dir);
    argv.push(image);
    argv.push(OsString::from(container_id));
    argv
}

/// Launch a foreground `runsc restore` and wait until the sandbox is live.
///
/// Foreground rather than `--detach`: detaching would leave no child to own,
/// force trusting a pid runsc created (which defeats the
/// `BLAZE_INSTANCE_ID` authentication used to reclaim orphans), and lose the
/// crash detection `try_wait` provides. Because it blocks for the sandbox's
/// lifetime, readiness is polled instead of awaited.
async fn spawn_restore(
    binary_path: &Path,
    root_dir: &Path,
    bundle_dir: &Path,
    image_dir: &Path,
    container_id: &str,
    instance_id: Uuid,
) -> Result<Child> {
    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(bundle_dir.with_file_name("runsc.log"))
        .map_err(BlazeError::from)?;
    let mut child = Command::new(binary_path)
        .args(restore_argv(root_dir, bundle_dir, image_dir, container_id))
        .env("BLAZE_INSTANCE_ID", instance_id.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(log_file))
        .spawn()?;

    let deadline = tokio::time::Instant::now() + RESTORE_READY_TIMEOUT;
    loop {
        if let Some(status) = child.try_wait()? {
            return Err(BlazeError::BackendError {
                msg: format!(
                    "runsc restore for {container_id} exited early with {status}; see runsc.log"
                ),
            });
        }
        if container_status(binary_path, root_dir, container_id).await
            == Some("running".to_string())
        {
            return Ok(child);
        }
        if tokio::time::Instant::now() >= deadline {
            let _ = terminate_child(&mut child, BackendKind::Gvisor.as_str()).await;
            return Err(BlazeError::BackendError {
                msg: format!("runsc restore for {container_id} did not become ready"),
            });
        }
        tokio::time::sleep(RESTORE_POLL_INTERVAL).await;
    }
}

/// Container status reported by `runsc state`, or `None` when unavailable.
async fn container_status(
    binary_path: &Path,
    root_dir: &Path,
    container_id: &str,
) -> Option<String> {
    let stdout = runsc_state_command(
        binary_path,
        state_argv(root_dir, "state", &[OsStr::new(container_id)]),
        "state",
        LIFECYCLE_TIMEOUT,
    )
    .await
    .ok()?;
    serde_json::from_slice::<serde_json::Value>(&stdout)
        .ok()?
        .get("status")?
        .as_str()
        .map(str::to_owned)
}

/// Run a runsc subcommand that only addresses existing container state.
///
/// runsc reports precondition failures on stderr (for example "cannot pause
/// container X in state paused"), so it is captured and surfaced instead of
/// being discarded — that message is the whole diagnostic.
async fn runsc_state_command(
    binary_path: &Path,
    argv: Vec<OsString>,
    what: &str,
    timeout: Duration,
) -> Result<Vec<u8>> {
    let output = tokio::time::timeout(
        timeout,
        Command::new(binary_path)
            .args(argv)
            .stdin(Stdio::null())
            .output(),
    )
    .await
    .map_err(|_| BlazeError::BackendError {
        msg: format!("runsc {what} timed out"),
    })??;
    if !output.status.success() {
        return Err(BlazeError::BackendError {
            msg: format!(
                "runsc {what} failed with {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            ),
        });
    }
    Ok(output.stdout)
}

fn oci_spec(rootfs: &Path) -> serde_json::Value {
    serde_json::json!({
        "ociVersion": "1.0.0",
        "process": {
            "user": {"uid": 0, "gid": 0},
            "args": ["/bin/sleep", "3600"],
            "env": [
                "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                "TERM=xterm"
            ],
            "cwd": "/",
            "rlimits": [
                {"type": "RLIMIT_NOFILE", "hard": 1024, "soft": 1024}
            ]
        },
        "root": {"path": rootfs, "readonly": true},
        "hostname": "blaze-gvisor",
        "mounts": [
            {"destination": "/proc", "type": "proc", "source": "proc"},
            {"destination": "/dev", "type": "tmpfs", "source": "tmpfs"},
            {"destination": "/tmp", "type": "tmpfs", "source": "tmpfs"}
        ],
        "linux": {
            "namespaces": [
                {"type": "pid"},
                {"type": "network"},
                {"type": "ipc"},
                {"type": "uts"},
                {"type": "mount"}
            ]
        }
    })
}

async fn runsc_delete_force(binary_path: &Path, root_dir: &Path, container_id: &str) -> Result<()> {
    let status = tokio::time::timeout(
        DELETE_TIMEOUT,
        Command::new(binary_path)
            .args(delete_force_argv(root_dir, container_id))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status(),
    )
    .await
    .map_err(|_| BlazeError::BackendError {
        msg: format!("runsc delete --force {container_id} timed out"),
    })??;
    if !status.success() {
        return Err(BlazeError::BackendError {
            msg: format!("runsc delete --force {container_id} exited with {status}"),
        });
    }
    Ok(())
}

#[async_trait]
impl BackendSpawner for GvisorSpawner {
    async fn spawn(
        &self,
        request: SpawnRequest,
    ) -> std::result::Result<DynBackendInstance, SpawnFailure> {
        let rootfs = self.rootfs_path();
        if !rootfs.is_dir() {
            return Err(SpawnFailure::clean(BlazeError::BackendError {
                msg: format!("gvisor base rootfs missing: {}", rootfs.display()),
            }));
        }
        let bundle = request.run_dir.join("bundle");
        tokio::fs::create_dir_all(&bundle).await?;
        let pid_file = request.run_dir.join("backend.pid");
        let stopped_marker = stopped_marker(&request.run_dir);
        remove_file_if_exists(&stopped_marker).await?;
        let spec = oci_spec(&rootfs);
        tokio::fs::write(
            bundle.join("config.json"),
            serde_json::to_vec_pretty(&spec).map_err(|error| BlazeError::BackendError {
                msg: format!("serialize OCI spec: {error}"),
            })?,
        )
        .await?;
        let container_id = format!("blaze-{}", request.instance_id);
        let log_file =
            std::fs::File::create(request.run_dir.join("runsc.log")).map_err(BlazeError::from)?;
        let child = Command::new(&request.binary_path)
            .args(run_argv(&self.root_dir, &bundle, &container_id))
            .env("BLAZE_INSTANCE_ID", request.instance_id.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::from(log_file))
            .spawn()?;
        let paths = GvisorPaths {
            binary_path: request.binary_path,
            root_dir: self.root_dir.clone(),
            bundle_dir: bundle,
            pid_file: pid_file.clone(),
            stopped_marker,
        };
        if let Some(pid) = child.id()
            && let Err(error) = tokio::fs::write(&pid_file, format!("{pid}\n")).await
        {
            let owner: DynBackendInstance = Arc::new(GvisorInstance::new(
                request.instance_id,
                container_id,
                paths,
                child,
            ));
            return Err(SpawnFailure::compensate_started(error.into(), owner).await);
        }
        Ok(Arc::new(GvisorInstance::new(
            request.instance_id,
            container_id,
            paths,
            child,
        )))
    }

    async fn probe(&self, binary_path: &Path) -> Result<bool> {
        if !binary_path.is_file() {
            return Ok(false);
        }
        let status = tokio::time::timeout(
            Duration::from_secs(5),
            Command::new(binary_path)
                .arg("--version")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status(),
        )
        .await;
        match status {
            Ok(Ok(status)) => Ok(status.success()),
            Ok(Err(error)) => {
                tracing::debug!(%error, binary = %binary_path.display(), "runsc version probe failed");
                Ok(false)
            }
            Err(_) => Ok(false),
        }
    }

    async fn cleanup_orphan(&self, instance_id: Uuid, run_dir: &Path) -> Result<()> {
        let marker = stopped_marker(run_dir);
        if marker.is_file() {
            return Ok(());
        }
        // Best-effort: the runsc container id is derived from the instance id.
        let container_id = format!("blaze-{instance_id}");
        if let Err(error) =
            runsc_delete_force(&self.binary_path, &self.root_dir, &container_id).await
        {
            tracing::warn!(%error, container_id, "orphan runsc delete failed");
        }
        let pid_file = run_dir.join("backend.pid");
        #[cfg(target_os = "linux")]
        super::terminate_recorded_process(instance_id, &pid_file, "gvisor").await?;
        record_backend_stopped(&marker).await?;
        remove_file_if_exists(&pid_file).await?;
        Ok(())
    }

    async fn restore(
        &self,
        request: RestoreRequest,
    ) -> std::result::Result<DynBackendInstance, SpawnFailure> {
        let image_dir = request.snapshot_dir.join(SNAPSHOT_IMAGE_DIR);
        let stored_spec = request
            .snapshot_dir
            .join(SNAPSHOT_BUNDLE_DIR)
            .join(BUNDLE_SPEC_FILE);
        if !image_dir.is_dir() || !stored_spec.is_file() {
            return Err(SpawnFailure::clean(BlazeError::BackendError {
                msg: format!(
                    "snapshot payload at {} is incomplete",
                    request.snapshot_dir.display()
                ),
            }));
        }

        // Copy the spec instead of pointing --bundle at the snapshot: the
        // restored instance then owns its bundle, can be snapshotted again,
        // and cannot corrupt a payload other instances may still restore from.
        let bundle_dir = request.run_dir.join("bundle");
        tokio::fs::create_dir_all(&bundle_dir).await?;
        tokio::fs::copy(&stored_spec, bundle_dir.join(BUNDLE_SPEC_FILE)).await?;
        let stopped_marker = stopped_marker(&request.run_dir);
        remove_file_if_exists(&stopped_marker).await?;

        let container_id = format!("blaze-{}", request.instance_id);
        // Clear any stale record so in-place restore and hatching take the
        // same path, and so a retry after a crash is safe.
        if let Err(error) =
            runsc_delete_force(&request.binary_path, &self.root_dir, &container_id).await
        {
            tracing::debug!(%error, container_id, "pre-restore delete found no container");
        }

        let child = spawn_restore(
            &request.binary_path,
            &self.root_dir,
            &bundle_dir,
            &image_dir,
            &container_id,
            request.instance_id,
        )
        .await
        .map_err(SpawnFailure::clean)?;

        let pid_file = request.run_dir.join("backend.pid");
        let paths = GvisorPaths {
            binary_path: request.binary_path,
            root_dir: self.root_dir.clone(),
            bundle_dir,
            pid_file: pid_file.clone(),
            stopped_marker,
        };
        // Record our own child's pid: orphan reclamation authenticates it via
        // BLAZE_INSTANCE_ID in the process environment, which only holds for a
        // process we launched.
        if let Some(pid) = child.id()
            && let Err(error) = tokio::fs::write(&pid_file, format!("{pid}\n")).await
        {
            let owner: DynBackendInstance = Arc::new(GvisorInstance::new(
                request.instance_id,
                container_id,
                paths,
                child,
            ));
            return Err(SpawnFailure::compensate_started(error.into(), owner).await);
        }
        Ok(Arc::new(GvisorInstance::new(
            request.instance_id,
            container_id,
            paths,
            child,
        )))
    }
}

/// Filesystem and runsc locations one sandbox owner needs.
struct GvisorPaths {
    binary_path: PathBuf,
    root_dir: PathBuf,
    /// Instance-owned OCI bundle. Snapshots copy its spec so a restore can
    /// satisfy runsc's spec validation.
    bundle_dir: PathBuf,
    pid_file: PathBuf,
    stopped_marker: PathBuf,
}

struct GvisorInstance {
    instance_id: Uuid,
    container_id: String,
    paths: GvisorPaths,
    child: Mutex<Option<Child>>,
    killed: AtomicBool,
    /// Set when a hibernating checkpoint intentionally stopped the sandbox,
    /// so an observed exit is not reported as a sandbox crash.
    expected_exit: AtomicBool,
}

impl GvisorInstance {
    fn new(instance_id: Uuid, container_id: String, paths: GvisorPaths, child: Child) -> Self {
        Self {
            instance_id,
            container_id,
            paths,
            child: Mutex::new(Some(child)),
            killed: AtomicBool::new(false),
            expected_exit: AtomicBool::new(false),
        }
    }

    /// Reject a lifecycle operation on an owner that has already been torn
    /// down, so the caller sees why rather than runsc's missing-container
    /// error.
    fn ensure_live(&self, operation: &str) -> Result<()> {
        if self.killed.load(Ordering::Acquire) {
            return Err(BlazeError::BackendError {
                msg: format!(
                    "cannot {operation} container {}: the sandbox has been torn down",
                    self.container_id
                ),
            });
        }
        Ok(())
    }

    /// Whether runsc still reports this container as running.
    async fn container_is_running(&self) -> bool {
        container_status(
            &self.paths.binary_path,
            &self.paths.root_dir,
            &self.container_id,
        )
        .await
            == Some("running".to_string())
    }

    /// Reap the foreground child that a hibernating checkpoint stopped and
    /// record the termination durably.
    ///
    /// A non-zero exit means the save may be partial, so it is reported as an
    /// error and the caller discards the payload.
    async fn finish_hibernation(&self, guard: &mut Option<Child>) -> Result<()> {
        self.expected_exit.store(true, Ordering::Release);
        if let Some(child) = guard.as_mut() {
            let status = tokio::time::timeout(LIFECYCLE_TIMEOUT, child.wait())
                .await
                .map_err(|_| BlazeError::BackendError {
                    msg: format!(
                        "runsc run for {} did not exit after checkpoint",
                        self.container_id
                    ),
                })??;
            if !status.success() {
                return Err(BlazeError::BackendError {
                    msg: format!(
                        "runsc run for {} exited with {status} during checkpoint; the payload may be partial",
                        self.container_id
                    ),
                });
            }
        }
        *guard = None;
        record_backend_stopped(&self.paths.stopped_marker).await?;
        remove_file_if_exists(&self.paths.pid_file).await?;
        // Everything `kill` would release has been released, so a later
        // destroy is a cheap no-op.
        self.killed.store(true, Ordering::Release);
        // Drop the stale container record so the same id can be restored.
        if let Err(error) = runsc_delete_force(
            &self.paths.binary_path,
            &self.paths.root_dir,
            &self.container_id,
        )
        .await
        {
            tracing::debug!(%error, container_id = %self.container_id, "post-checkpoint delete failed");
        }
        Ok(())
    }

    /// Re-establish ownership from a payload this owner just wrote, keeping
    /// the same owner object so the daemon's backend map stays valid.
    async fn reattach_from(&self, guard: &mut Option<Child>, image_dir: &Path) -> Result<()> {
        if let Some(child) = guard.as_mut() {
            let _ = tokio::time::timeout(LIFECYCLE_TIMEOUT, child.wait()).await;
        }
        *guard = None;
        if let Err(error) = runsc_delete_force(
            &self.paths.binary_path,
            &self.paths.root_dir,
            &self.container_id,
        )
        .await
        {
            tracing::debug!(%error, container_id = %self.container_id, "pre-reattach delete failed");
        }
        let child = spawn_restore(
            &self.paths.binary_path,
            &self.paths.root_dir,
            &self.paths.bundle_dir,
            image_dir,
            &self.container_id,
            self.instance_id,
        )
        .await?;
        if let Some(pid) = child.id() {
            tokio::fs::write(&self.paths.pid_file, format!("{pid}\n")).await?;
        }
        *guard = Some(child);
        Ok(())
    }
}

#[async_trait]
impl BackendInstance for GvisorInstance {
    fn backend(&self) -> BackendKind {
        BackendKind::Gvisor
    }

    async fn try_wait(&self) -> Result<Option<SpawnResult>> {
        let mut guard = self.child.lock().await;
        let Some(child) = guard.as_mut() else {
            return Ok(Some(SpawnResult {
                instance_id: self.instance_id,
                // A hibernating checkpoint stops the sandbox on purpose;
                // report that as a clean exit, not an unknown one.
                exit_code: self.expected_exit.load(Ordering::Acquire).then_some(0),
                signal: None,
            }));
        };
        let Some(status) = child.try_wait()? else {
            return Ok(None);
        };
        record_backend_stopped(&self.paths.stopped_marker).await?;
        *guard = None;
        Ok(Some(spawn_result(self.instance_id, status)))
    }

    async fn kill(&self) -> Result<()> {
        if self.killed.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut guard = self.child.lock().await;
        if self.killed.load(Ordering::Acquire) {
            return Ok(());
        }
        // Tear down the sandbox first so the foreground `runsc run` child
        // exits on its own; fall back to signalling the child directly.
        if let Err(error) = runsc_delete_force(
            &self.paths.binary_path,
            &self.paths.root_dir,
            &self.container_id,
        )
        .await
        {
            tracing::warn!(%error, container_id = %self.container_id, "runsc delete failed; terminating child");
        }
        if let Some(child) = guard.as_mut() {
            terminate_child(child, BackendKind::Gvisor.as_str()).await?;
        }
        record_backend_stopped(&self.paths.stopped_marker).await?;
        *guard = None;
        remove_file_if_exists(&self.paths.pid_file).await?;
        self.killed.store(true, Ordering::Release);
        Ok(())
    }

    async fn pause(&self) -> Result<()> {
        self.ensure_live("pause")?;
        runsc_state_command(
            &self.paths.binary_path,
            pause_argv(&self.paths.root_dir, &self.container_id),
            "pause",
            LIFECYCLE_TIMEOUT,
        )
        .await?;
        Ok(())
    }

    async fn resume(&self) -> Result<()> {
        self.ensure_live("resume")?;
        runsc_state_command(
            &self.paths.binary_path,
            resume_argv(&self.paths.root_dir, &self.container_id),
            "resume",
            LIFECYCLE_TIMEOUT,
        )
        .await?;
        Ok(())
    }

    async fn snapshot(&self, request: &SnapshotRequest) -> Result<SnapshotArtifacts> {
        self.ensure_live("snapshot")?;
        // Held for the whole operation: `try_wait` and `kill` take the same
        // mutex, so no observer can misread a deliberate checkpoint exit as
        // a sandbox crash.
        let mut guard = self.child.lock().await;

        let image_dir = request.snapshot_dir.join(SNAPSHOT_IMAGE_DIR);
        tokio::fs::create_dir_all(&image_dir).await?;
        // Restore validates the supplied spec against the one embedded in the
        // payload, so the spec must travel with the image byte-for-byte.
        let bundle_dir = request.snapshot_dir.join(SNAPSHOT_BUNDLE_DIR);
        tokio::fs::create_dir_all(&bundle_dir).await?;
        tokio::fs::copy(
            self.paths.bundle_dir.join(BUNDLE_SPEC_FILE),
            bundle_dir.join(BUNDLE_SPEC_FILE),
        )
        .await?;

        runsc_state_command(
            &self.paths.binary_path,
            checkpoint_argv(
                &self.paths.root_dir,
                &image_dir,
                request.leave_running,
                request.compression,
                &self.container_id,
            ),
            "checkpoint",
            CHECKPOINT_TIMEOUT,
        )
        .await?;

        let size_bytes = dir_size(&request.snapshot_dir);
        if !request.leave_running {
            self.finish_hibernation(&mut guard).await?;
            return Ok(SnapshotArtifacts {
                backend: BackendKind::Gvisor,
                size_bytes,
                left_running: false,
            });
        }

        // Verify rather than assume: `--leave-running` is documented only as
        // "restart the container after checkpointing", and a build that tears
        // the sandbox down would invalidate our owned child.
        let child_alive = guard
            .as_mut()
            .map(|child| child.try_wait().map(|status| status.is_none()))
            .transpose()?
            .unwrap_or(false);
        if child_alive && self.container_is_running().await {
            return Ok(SnapshotArtifacts {
                backend: BackendKind::Gvisor,
                size_bytes,
                left_running: true,
            });
        }

        // The payload on disk is valid, so re-establish ownership from it and
        // keep this owner's identity: the daemon's backend map stays correct.
        match self.reattach_from(&mut guard, &image_dir).await {
            Ok(()) => Ok(SnapshotArtifacts {
                backend: BackendKind::Gvisor,
                size_bytes,
                left_running: true,
            }),
            Err(error) => {
                tracing::warn!(
                    %error,
                    container_id = %self.container_id,
                    "live snapshot could not keep the sandbox running; reporting it as hibernated"
                );
                self.finish_hibernation(&mut guard).await?;
                Ok(SnapshotArtifacts {
                    backend: BackendKind::Gvisor,
                    size_bytes,
                    left_running: false,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv_strings(argv: &[OsString]) -> Vec<String> {
        argv.iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect()
    }

    #[test]
    fn top_level_flags_precede_the_subcommand() {
        let argv = argv_strings(&run_argv(
            Path::new("/var/lib/blaze/runsc"),
            Path::new("/var/lib/blaze/abc/bundle"),
            "blaze-abc",
        ));

        let subcommand = argv.iter().position(|arg| arg == "run").expect("run");
        let root = argv
            .iter()
            .position(|arg| arg.starts_with("--root="))
            .expect("root");
        let network = argv
            .iter()
            .position(|arg| arg == "--network=none")
            .expect("network");
        assert!(
            root < subcommand && network < subcommand,
            "runsc rejects top-level flags placed after the subcommand"
        );
        assert_eq!(argv[root], "--root=/var/lib/blaze/runsc");
        assert_eq!(argv.last().expect("container id"), "blaze-abc");
    }

    #[test]
    fn state_subcommands_carry_the_pinned_root() {
        let argv = argv_strings(&delete_force_argv(
            Path::new("/var/lib/blaze/runsc"),
            "blaze-abc",
        ));
        assert_eq!(
            argv,
            vec![
                "--root=/var/lib/blaze/runsc",
                "delete",
                "--force",
                "blaze-abc"
            ]
        );
    }

    #[test]
    fn oci_spec_is_instance_independent() {
        let spec =
            serde_json::to_string(&oci_spec(Path::new("/var/lib/blaze/images/gvisor-rootfs")))
                .expect("serialize");
        // Hatching reuses a snapshot's spec verbatim, so the spec must not
        // embed anything tied to one instance.
        assert!(!spec.contains("BLAZE_INSTANCE_ID"));
        assert!(spec.contains("\"hostname\":\"blaze-gvisor\""));
    }

    #[tokio::test]
    async fn cleanup_orphan_is_a_noop_when_stop_is_recorded() {
        let temp = tempfile::tempdir().expect("temp");
        let spawner = GvisorSpawner::new(
            temp.path().join("images"),
            temp.path().to_path_buf(),
            PathBuf::from("/nonexistent/runsc"),
        );
        record_backend_stopped(&stopped_marker(temp.path()))
            .await
            .expect("record stopped");

        spawner
            .cleanup_orphan(Uuid::new_v4(), temp.path())
            .await
            .expect("durable stop record proves termination");
    }

    #[tokio::test]
    async fn cleanup_orphan_uses_the_configured_binary() {
        let temp = tempfile::tempdir().expect("temp");
        let missing = temp.path().join("no-such-runsc");
        let spawner = GvisorSpawner::new(
            temp.path().join("images"),
            temp.path().to_path_buf(),
            missing.clone(),
        );

        // Neither a stop record nor a pid file exists, so cleanup cannot
        // prove termination and must refuse rather than assume success.
        let error = spawner
            .cleanup_orphan(Uuid::new_v4(), temp.path())
            .await
            .expect_err("termination is unproven");
        assert!(error.to_string().contains("missing PID metadata"));
        assert!(!missing.exists(), "the configured binary is used, not PATH");
    }
}
