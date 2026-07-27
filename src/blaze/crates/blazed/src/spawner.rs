// SPDX-License-Identifier: Apache-2.0
//! Backend process ownership and runtime lifecycle abstraction.

#![allow(dead_code)] // Runtime foundations are wired by the lifecycle layer.

pub mod firecracker;
pub mod netns;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use blaze_core::backend::{
    BackendKind, FlushResult, RestoreRequest, SnapshotRequest, SnapshotResult, SpawnRequest,
};
use blaze_core::guest_protocol::DEFAULT_MAX_RESPONSE_BYTES;
use blaze_core::{BlazeError, Result};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub use firecracker::FirecrackerSpawner;

const TERMINATION_GRACE: Duration = Duration::from_secs(5);

/// Result reported when a backend process exits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpawnResult {
    /// Sandbox whose process exited.
    pub instance_id: Uuid,
    /// Normal process exit status.
    pub exit_code: Option<i32>,
    /// Terminating signal on Unix.
    pub signal: Option<i32>,
}

/// Owned runtime instance returned by a backend spawner.
#[async_trait]
pub trait BackendInstance: Send + Sync {
    /// Stable sandbox identifier.
    fn instance_id(&self) -> Uuid;
    /// Concrete backend implementation.
    fn backend(&self) -> BackendKind;
    /// Backend version frozen into checkpoint metadata when available.
    fn version(&self) -> Option<&str> {
        None
    }
    /// Child PID when a real process exists.
    fn pid(&self) -> Option<u32>;
    /// Guest transport socket, or an empty path for guestless backends.
    fn guest_socket_path(&self) -> &Path;
    /// Wait until the owned backend process exits.
    async fn wait(&self) -> Result<SpawnResult>;
    /// Pause guest execution when the backend supports whole-sandbox pause.
    async fn pause(&self) -> Result<()>;
    /// Resume guest execution when the backend supports whole-sandbox pause.
    async fn resume(&self) -> Result<()>;
    /// Write one snapshot.
    async fn snapshot(&self, request: SnapshotRequest) -> Result<SnapshotResult>;
    /// Ask a non-standard backend to persist backend-owned dirty data.
    ///
    /// The standard loop flushes through `StorageProvider`; this capability
    /// remains reserved for a future custom backend.
    #[allow(dead_code)]
    async fn flush_dirty(&self) -> Result<FlushResult>;
    /// Terminate the process and release all backend-owned resources.
    async fn kill(&self) -> Result<()>;
}

/// Shared backend instance handle stored in the daemon runtime map.
pub type DynBackendInstance = Arc<dyn BackendInstance>;

/// Factory for owned backend runtime instances.
#[async_trait]
pub trait BackendSpawner: Send + Sync {
    /// Start a new sandbox.
    async fn spawn(&self, request: SpawnRequest) -> Result<DynBackendInstance>;

    /// Restore a sandbox from VM-state and memory artifacts.
    async fn restore(&self, request: RestoreRequest) -> Result<DynBackendInstance> {
        let _ = request;
        Err(BlazeError::BackendError {
            msg: "restore not supported by this backend".to_string(),
        })
    }

    /// Probe whether the configured backend executable is usable.
    async fn probe(&self, binary_path: &Path) -> Result<bool>;

    /// Clean up a backend process and resources whose in-memory handle was
    /// lost across daemon restart.
    async fn cleanup_orphan(&self, instance_id: Uuid, run_dir: &Path) -> Result<()> {
        firecracker::cleanup_orphan_run_dir(instance_id, run_dir).await
    }
}

/// Shared backend spawner selected during daemon startup.
pub type DynSpawner = Arc<dyn BackendSpawner>;

/// Bubblewrap process owner used when a VM backend is not selected.
pub struct BubblewrapSpawner;

#[async_trait]
impl BackendSpawner for BubblewrapSpawner {
    async fn spawn(&self, request: SpawnRequest) -> Result<DynBackendInstance> {
        tokio::fs::create_dir_all(&request.run_dir).await?;
        let mut child = Command::new(&request.binary_path)
            .args([
                "--ro-bind",
                "/",
                "/",
                "--proc",
                "/proc",
                "--dev",
                "/dev",
                "--tmpfs",
                "/tmp",
                "--unshare-pid",
                "--unshare-net",
                "--die-with-parent",
                "--",
                "/bin/sleep",
                "3600",
            ])
            .env("BLAZE_INSTANCE_ID", request.instance_id.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        let pid_file = request.run_dir.join("backend.pid");
        if let Some(pid) = child.id()
            && let Err(error) = tokio::fs::write(&pid_file, format!("{pid}\n")).await
        {
            if let Err(termination) = terminate_child(&mut child, "bubblewrap").await {
                return Err(BlazeError::BackendError {
                    msg: format!(
                        "write bubblewrap pid metadata failed ({error}); process cleanup failed ({termination})"
                    ),
                });
            }
            return Err(error.into());
        }
        let instance = ProcessInstance::new(
            request.instance_id,
            BackendKind::Bubblewrap,
            child,
            PathBuf::new(),
            pid_file,
        );
        Ok(Arc::new(instance))
    }

    async fn probe(&self, binary_path: &Path) -> Result<bool> {
        Ok(binary_path.is_file())
    }

    async fn cleanup_orphan(&self, instance_id: Uuid, run_dir: &Path) -> Result<()> {
        cleanup_process_run_dir(instance_id, run_dir, "bubblewrap").await
    }
}

struct ProcessInstance {
    instance_id: Uuid,
    backend: BackendKind,
    pid: Option<u32>,
    child: Mutex<Option<Child>>,
    guest_socket_path: PathBuf,
    pid_file: PathBuf,
    killed: AtomicBool,
}

impl ProcessInstance {
    fn new(
        instance_id: Uuid,
        backend: BackendKind,
        child: Child,
        guest_socket_path: PathBuf,
        pid_file: PathBuf,
    ) -> Self {
        let pid = child.id();
        Self {
            instance_id,
            backend,
            pid,
            child: Mutex::new(Some(child)),
            guest_socket_path,
            pid_file,
            killed: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl BackendInstance for ProcessInstance {
    fn instance_id(&self) -> Uuid {
        self.instance_id
    }

    fn backend(&self) -> BackendKind {
        self.backend
    }

    fn pid(&self) -> Option<u32> {
        self.pid
    }

    fn guest_socket_path(&self) -> &Path {
        &self.guest_socket_path
    }

    async fn wait(&self) -> Result<SpawnResult> {
        loop {
            let status = {
                let mut guard = self.child.lock().await;
                let Some(child) = guard.as_mut() else {
                    return Ok(SpawnResult {
                        instance_id: self.instance_id,
                        exit_code: None,
                        signal: None,
                    });
                };
                child.try_wait()?
            };
            if let Some(status) = status {
                *self.child.lock().await = None;
                return Ok(spawn_result(self.instance_id, status));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn pause(&self) -> Result<()> {
        Err(BlazeError::BackendError {
            msg: format!(
                "{} does not support whole-sandbox pause",
                self.backend.as_str()
            ),
        })
    }

    async fn resume(&self) -> Result<()> {
        Err(BlazeError::BackendError {
            msg: format!(
                "{} does not support whole-sandbox resume",
                self.backend.as_str()
            ),
        })
    }

    async fn snapshot(&self, _request: SnapshotRequest) -> Result<SnapshotResult> {
        Err(BlazeError::BackendError {
            msg: format!("{} does not support snapshots", self.backend),
        })
    }

    async fn flush_dirty(&self) -> Result<FlushResult> {
        Err(BlazeError::BackendError {
            msg: format!("{} does not support backend dirty-data flush", self.backend),
        })
    }

    async fn kill(&self) -> Result<()> {
        if self.killed.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut guard = self.child.lock().await;
        if self.killed.load(Ordering::Acquire) {
            return Ok(());
        }
        if let Some(child) = guard.as_mut() {
            terminate_child(child, self.backend.as_str()).await?;
        }
        *guard = None;
        match tokio::fs::remove_file(&self.pid_file).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        self.killed.store(true, Ordering::Release);
        Ok(())
    }
}

/// Portable backend used for API and lifecycle integration tests.
pub struct MockSpawner;

#[async_trait]
impl BackendSpawner for MockSpawner {
    async fn spawn(&self, request: SpawnRequest) -> Result<DynBackendInstance> {
        spawn_mock_instance(request.instance_id, request.run_dir, None).await
    }

    async fn restore(&self, request: RestoreRequest) -> Result<DynBackendInstance> {
        spawn_mock_instance(
            request.spawn.instance_id,
            request.spawn.run_dir,
            Some(request.mem_path),
        )
        .await
    }

    async fn probe(&self, _binary_path: &Path) -> Result<bool> {
        Ok(true)
    }
}

struct MockInstance {
    instance_id: Uuid,
    guest_socket_path: PathBuf,
    cancellation: CancellationToken,
    task: Mutex<Option<JoinHandle<()>>>,
    files: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    killed: AtomicBool,
}

async fn spawn_mock_instance(
    instance_id: Uuid,
    run_dir: PathBuf,
    restore_memory: Option<PathBuf>,
) -> Result<DynBackendInstance> {
    tokio::fs::create_dir_all(&run_dir).await?;
    let socket = run_dir.join("vsock.uds");
    if socket.exists() {
        tokio::fs::remove_file(&socket).await?;
    }
    let listener = UnixListener::bind(&socket)?;
    let cancellation = CancellationToken::new();
    let task_token = cancellation.clone();
    let restored_files = match restore_memory {
        Some(path) if path.is_file() => {
            let encoded = tokio::fs::read(path).await?;
            serde_json::from_slice(&encoded).map_err(|error| BlazeError::BackendError {
                msg: format!("invalid mock snapshot: {error}"),
            })?
        }
        _ => HashMap::new(),
    };
    let files = Arc::new(Mutex::new(restored_files));
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = task_token.cancelled() => break,
                accepted = listener.accept() => {
                    let Ok((stream, _)) = accepted else {
                        break;
                    };
                    tokio::spawn(async move {
                        if let Err(error) = serve_mock_guest(stream).await {
                            tracing::debug!(%error, "mock guest connection ended");
                        }
                    });
                }
            }
        }
    });
    Ok(Arc::new(MockInstance {
        instance_id,
        guest_socket_path: socket,
        cancellation,
        task: Mutex::new(Some(task)),
        files,
        killed: AtomicBool::new(false),
    }))
}

#[async_trait]
impl BackendInstance for MockInstance {
    fn instance_id(&self) -> Uuid {
        self.instance_id
    }

    fn backend(&self) -> BackendKind {
        BackendKind::Mock
    }

    fn version(&self) -> Option<&str> {
        Some("mock")
    }

    fn pid(&self) -> Option<u32> {
        None
    }

    fn guest_socket_path(&self) -> &Path {
        &self.guest_socket_path
    }

    async fn wait(&self) -> Result<SpawnResult> {
        loop {
            let finished = self
                .task
                .lock()
                .await
                .as_ref()
                .map(JoinHandle::is_finished)
                .unwrap_or(true);
            if finished {
                if let Some(task) = self.task.lock().await.take() {
                    let _ = task.await;
                }
                return Ok(SpawnResult {
                    instance_id: self.instance_id,
                    exit_code: Some(0),
                    signal: None,
                });
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn pause(&self) -> Result<()> {
        Ok(())
    }

    async fn resume(&self) -> Result<()> {
        Ok(())
    }

    async fn snapshot(&self, request: SnapshotRequest) -> Result<SnapshotResult> {
        tokio::fs::write(&request.snapshot_path, b"mock-vmstate").await?;
        let memory = serde_json::to_vec(&*self.files.lock().await).map_err(|error| {
            BlazeError::BackendError {
                msg: format!("serialize mock snapshot: {error}"),
            }
        })?;
        tokio::fs::write(&request.mem_path, memory).await?;
        Ok(SnapshotResult {
            snapshot_path: request.snapshot_path,
            mem_path: request.mem_path,
        })
    }

    async fn flush_dirty(&self) -> Result<FlushResult> {
        Err(BlazeError::BackendError {
            msg: "mock does not support backend dirty-data flush".to_string(),
        })
    }

    async fn kill(&self) -> Result<()> {
        if self.killed.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut task = self.task.lock().await;
        if self.killed.load(Ordering::Acquire) {
            return Ok(());
        }
        self.cancellation.cancel();
        if let Some(task) = task.take() {
            let _ = task.await;
        }
        if self.guest_socket_path.exists() {
            tokio::fs::remove_file(&self.guest_socket_path).await?;
        }
        self.killed.store(true, Ordering::Release);
        Ok(())
    }
}

async fn serve_mock_guest(mut stream: tokio::net::UnixStream) -> std::io::Result<()> {
    let connect = read_mock_line(&mut stream, 128).await?;
    if !connect.starts_with(b"CONNECT ") {
        return Ok(());
    }
    stream.write_all(b"OK 5000\n").await?;
    let request = read_mock_line(&mut stream, DEFAULT_MAX_RESPONSE_BYTES).await?;
    let request: serde_json::Value = match serde_json::from_slice(&request) {
        Ok(request) => request,
        Err(_) => return Ok(()),
    };
    let id = request.get("id").cloned().unwrap_or_default();
    let response = serde_json::json!({"id": id, "ok": true});
    let mut encoded = serde_json::to_vec(&response).unwrap_or_else(|_| b"{}".to_vec());
    encoded.push(b'\n');
    stream.write_all(&encoded).await
}

async fn read_mock_line<R>(stream: &mut R, limit: usize) -> std::io::Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut reader = BufReader::new(stream).take(limit.saturating_add(1) as u64);
    let mut output = Vec::with_capacity(limit.min(8192));
    reader.read_until(b'\n', &mut output).await?;
    if output.last() == Some(&b'\n') {
        output.pop();
        if output.len() <= limit {
            return Ok(output);
        }
    }
    if output.len() > limit {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "mock guest line too long",
        ));
    }
    Ok(output)
}

pub(super) async fn terminate_child(child: &mut Child, backend: &str) -> Result<()> {
    if child.try_wait()?.is_some() {
        return Ok(());
    }
    if let Err(error) = signal_process(child.id(), "-TERM").await {
        if child.try_wait()?.is_some() {
            return Ok(());
        }
        tracing::warn!(backend, %error, "SIGTERM request failed; sending SIGKILL");
        child.start_kill()?;
        child.wait().await?;
        return Ok(());
    }
    match tokio::time::timeout(TERMINATION_GRACE, child.wait()).await {
        Ok(status) => {
            status?;
        }
        Err(_) => {
            tracing::warn!(backend, "graceful termination timed out; sending SIGKILL");
            child.start_kill()?;
            child.wait().await?;
        }
    }
    Ok(())
}

async fn cleanup_process_run_dir(instance_id: Uuid, run_dir: &Path, backend: &str) -> Result<()> {
    let pid_file = run_dir.join("backend.pid");
    #[cfg(target_os = "linux")]
    terminate_recorded_process(instance_id, &pid_file, backend).await?;
    #[cfg(not(target_os = "linux"))]
    if pid_file.exists() {
        return Err(BlazeError::BackendError {
            msg: format!(
                "cannot validate {backend} orphan {} outside Linux",
                pid_file.display()
            ),
        });
    }
    match tokio::fs::remove_file(&pid_file).await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

#[cfg(target_os = "linux")]
pub(super) async fn terminate_recorded_process(
    instance_id: Uuid,
    pid_file: &Path,
    backend: &str,
) -> Result<()> {
    let raw = match tokio::fs::read_to_string(pid_file).await {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let pid: u32 = raw
        .trim()
        .parse()
        .map_err(|error| BlazeError::BackendError {
            msg: format!("invalid {backend} pid file {}: {error}", pid_file.display()),
        })?;
    let process_dir = PathBuf::from(format!("/proc/{pid}"));
    let environ = match tokio::fs::read(process_dir.join("environ")).await {
        Ok(environ) => environ,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let expected = format!("BLAZE_INSTANCE_ID={instance_id}");
    if !environ
        .split(|byte| *byte == 0)
        .any(|entry| entry == expected.as_bytes())
    {
        return Err(BlazeError::BackendError {
            msg: format!(
                "refusing to terminate {backend} pid {pid}: BLAZE_INSTANCE_ID does not match {instance_id}"
            ),
        });
    }

    if let Err(error) = signal_process(Some(pid), "-TERM").await {
        if !process_is_running(&process_dir)? {
            return Ok(());
        }
        return Err(error);
    }
    if wait_for_process_exit(&process_dir, TERMINATION_GRACE).await? {
        return Ok(());
    }
    tracing::warn!(backend, pid, "orphan ignored SIGTERM; sending SIGKILL");
    if let Err(error) = signal_process(Some(pid), "-KILL").await {
        if !process_is_running(&process_dir)? {
            return Ok(());
        }
        return Err(error);
    }
    if !wait_for_process_exit(&process_dir, TERMINATION_GRACE).await? {
        return Err(BlazeError::BackendError {
            msg: format!("{backend} orphan pid {pid} did not exit"),
        });
    }
    Ok(())
}

#[cfg(target_os = "linux")]
async fn wait_for_process_exit(process_dir: &Path, timeout: Duration) -> Result<bool> {
    let deadline = Instant::now() + timeout;
    while process_is_running(process_dir)? && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(!process_is_running(process_dir)?)
}

#[cfg(target_os = "linux")]
fn process_is_running(process_dir: &Path) -> Result<bool> {
    let stat = match std::fs::read_to_string(process_dir.join("stat")) {
        Ok(stat) => stat,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    let state = stat
        .rsplit_once(") ")
        .and_then(|(_, fields)| fields.chars().next())
        .ok_or_else(|| BlazeError::BackendError {
            msg: format!("invalid process status in {}", process_dir.display()),
        })?;
    Ok(state != 'Z')
}

async fn signal_process(pid: Option<u32>, signal: &str) -> Result<()> {
    let Some(pid) = pid else {
        return Ok(());
    };
    let status = tokio::time::timeout(
        Duration::from_secs(5),
        Command::new("kill")
            .arg(signal)
            .arg(pid.to_string())
            .env("LC_ALL", "C")
            .status(),
    )
    .await
    .map_err(|_| BlazeError::BackendError {
        msg: format!("kill {signal} {pid} timed out"),
    })??;
    if !status.success() {
        return Err(BlazeError::BackendError {
            msg: format!("kill {signal} {pid} exited with {status}"),
        });
    }
    Ok(())
}

fn spawn_result(instance_id: Uuid, status: std::process::ExitStatus) -> SpawnResult {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        SpawnResult {
            instance_id,
            exit_code: status.code(),
            signal: status.signal(),
        }
    }
    #[cfg(not(unix))]
    {
        SpawnResult {
            instance_id,
            exit_code: status.code(),
            signal: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use blaze_core::backend::SpawnRequest;
    use blaze_core::policy::BackendConfigs;
    use blaze_core::storage::StorageSlot;

    use crate::guest::GuestClient;

    use super::*;

    fn request(root: &Path) -> SpawnRequest {
        let id = Uuid::new_v4();
        let slot_dir = root.join("slot");
        SpawnRequest {
            instance_id: id,
            run_dir: root.join("run"),
            binary_path: PathBuf::new(),
            storage: StorageSlot {
                id: id.to_string(),
                rootfs_path: slot_dir.join("rootfs.ext4"),
                mem_path: slot_dir.join("mem.bin"),
                mem_diff_path: slot_dir.join("mem.diff"),
                rootfs_diff_path: slot_dir.join("rootfs.diff"),
                instance_dir: slot_dir,
            },
            backend: BackendConfigs::default(),
            vm: None,
            network: None,
        }
    }

    #[cfg(target_os = "linux")]
    async fn wait_for_instance_marker(child: &Child, instance_id: Uuid) {
        let pid = child.id().expect("child pid");
        let expected = format!("BLAZE_INSTANCE_ID={instance_id}");
        let environ_path = PathBuf::from(format!("/proc/{pid}/environ"));
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            if let Ok(environ) = tokio::fs::read(&environ_path).await
                && environ
                    .split(|byte| *byte == 0)
                    .any(|entry| entry == expected.as_bytes())
            {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "child environment marker did not become visible"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn mock_instance_supports_readiness_snapshot_and_idempotent_kill() {
        let temp = tempfile::tempdir().expect("temp");
        let instance = MockSpawner
            .spawn(request(temp.path()))
            .await
            .expect("spawn");
        let client = GuestClient::new(
            instance.guest_socket_path().to_path_buf(),
            Duration::from_secs(1),
        );
        client.ping().await.expect("guest readiness");
        let snapshot = temp.path().join("vmstate.snap");
        let memory = temp.path().join("mem.diff");
        instance
            .snapshot(SnapshotRequest {
                snapshot_path: snapshot.clone(),
                mem_path: memory.clone(),
                kind: blaze_core::backend::SnapshotKind::Diff,
            })
            .await
            .expect("snapshot");
        assert!(snapshot.exists());
        assert!(memory.exists());
        instance.kill().await.expect("kill");
        instance.kill().await.expect("idempotent kill");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn child_termination_requests_graceful_exit_first() {
        let temp = tempfile::tempdir().expect("temp");
        let marker = temp.path().join("terminated");
        let mut child = Command::new("sh")
            .arg("-c")
            .arg("trap 'printf term > \"$MARKER\"; exit 0' TERM; while :; do sleep 1; done")
            .env("MARKER", &marker)
            .spawn()
            .expect("spawn child");
        tokio::time::sleep(Duration::from_millis(50)).await;

        terminate_child(&mut child, "test")
            .await
            .expect("terminate child");

        assert_eq!(std::fs::read_to_string(marker).expect("marker"), "term");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn orphan_cleanup_requires_matching_instance_marker() {
        let temp = tempfile::tempdir().expect("temp");
        let expected_id = Uuid::new_v4();
        let actual_id = Uuid::new_v4();
        let pid_file = temp.path().join("backend.pid");
        let mut child = Command::new("sleep")
            .arg("60")
            .env("BLAZE_INSTANCE_ID", actual_id.to_string())
            .spawn()
            .expect("spawn child");
        wait_for_instance_marker(&child, actual_id).await;
        std::fs::write(&pid_file, format!("{}\n", child.id().expect("child pid")))
            .expect("write pid");

        let error = terminate_recorded_process(expected_id, &pid_file, "test")
            .await
            .expect_err("mismatched process must be retained");

        assert!(error.to_string().contains("does not match"));
        assert!(child.try_wait().expect("child status").is_none());
        child.start_kill().expect("kill child");
        child.wait().await.expect("wait child");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn orphan_cleanup_terminates_matching_instance() {
        let temp = tempfile::tempdir().expect("temp");
        let instance_id = Uuid::new_v4();
        let pid_file = temp.path().join("backend.pid");
        let mut child = Command::new("sleep")
            .arg("60")
            .env("BLAZE_INSTANCE_ID", instance_id.to_string())
            .spawn()
            .expect("spawn child");
        wait_for_instance_marker(&child, instance_id).await;
        std::fs::write(&pid_file, format!("{}\n", child.id().expect("child pid")))
            .expect("write pid");

        terminate_recorded_process(instance_id, &pid_file, "test")
            .await
            .expect("matching process is terminated");
        child.wait().await.expect("reap child");
    }
}
