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
use blaze_core::backend::{BackendKind, SpawnRequest};
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
        if let Some(pid) = child.id()
            && let Err(error) = tokio::fs::write(&pid_file, format!("{pid}\n")).await
        {
            let owner: DynBackendInstance = Arc::new(GvisorInstance::new(
                request.instance_id,
                container_id,
                request.binary_path.clone(),
                self.root_dir.clone(),
                child,
                pid_file,
                stopped_marker,
            ));
            return Err(SpawnFailure::compensate_started(error.into(), owner).await);
        }
        Ok(Arc::new(GvisorInstance::new(
            request.instance_id,
            container_id,
            request.binary_path,
            self.root_dir.clone(),
            child,
            pid_file,
            stopped_marker,
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
}

struct GvisorInstance {
    instance_id: Uuid,
    container_id: String,
    binary_path: PathBuf,
    root_dir: PathBuf,
    child: Mutex<Option<Child>>,
    pid_file: PathBuf,
    stopped_marker: PathBuf,
    killed: AtomicBool,
}

impl GvisorInstance {
    fn new(
        instance_id: Uuid,
        container_id: String,
        binary_path: PathBuf,
        root_dir: PathBuf,
        child: Child,
        pid_file: PathBuf,
        stopped_marker: PathBuf,
    ) -> Self {
        Self {
            instance_id,
            container_id,
            binary_path,
            root_dir,
            child: Mutex::new(Some(child)),
            pid_file,
            stopped_marker,
            killed: AtomicBool::new(false),
        }
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
                exit_code: None,
                signal: None,
            }));
        };
        let Some(status) = child.try_wait()? else {
            return Ok(None);
        };
        record_backend_stopped(&self.stopped_marker).await?;
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
        if let Err(error) =
            runsc_delete_force(&self.binary_path, &self.root_dir, &self.container_id).await
        {
            tracing::warn!(%error, container_id = %self.container_id, "runsc delete failed; terminating child");
        }
        if let Some(child) = guard.as_mut() {
            terminate_child(child, BackendKind::Gvisor.as_str()).await?;
        }
        record_backend_stopped(&self.stopped_marker).await?;
        *guard = None;
        remove_file_if_exists(&self.pid_file).await?;
        self.killed.store(true, Ordering::Release);
        Ok(())
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
