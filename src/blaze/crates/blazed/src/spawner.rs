// SPDX-License-Identifier: Apache-2.0
//! Backend process ownership and runtime lifecycle abstraction.

#![allow(dead_code)] // Activated by sandbox-manager API wiring.

pub mod firecracker;
pub mod netns;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use blaze_core::backend::{
    BackendKind, FlushResult, RestoreRequest, SnapshotRequest, SnapshotResult, SpawnRequest,
};
use blaze_core::guest_protocol::DEFAULT_MAX_RESPONSE_BYTES;
use blaze_core::{BlazeError, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub use firecracker::FirecrackerSpawner;

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
    /// Firecracker vsock proxy path used by the guest client.
    fn guest_socket_path(&self) -> &Path;
    /// Wait until the owned backend process exits.
    async fn wait(&self) -> Result<SpawnResult>;
    /// Pause guest execution.
    async fn pause(&self) -> Result<()>;
    /// Resume guest execution.
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
        let pid_file = request.run_dir.join("firecracker.pid");
        if let Some(pid) = child.id()
            && let Err(error) = tokio::fs::write(&pid_file, format!("{pid}\n")).await
        {
            let _ = child.start_kill();
            let _ = child.wait().await;
            return Err(error.into());
        }
        let instance = ProcessInstance::new(
            request.instance_id,
            BackendKind::Bubblewrap,
            child,
            request.run_dir.join("vsock.uds"),
            pid_file,
        );
        Ok(Arc::new(instance))
    }

    async fn probe(&self, binary_path: &Path) -> Result<bool> {
        Ok(binary_path.is_file())
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

    async fn signal(&self, signal: &str) -> Result<()> {
        let mut guard = self.child.lock().await;
        let child = guard.as_mut().ok_or_else(|| BlazeError::BackendError {
            msg: format!(
                "{} backend {} is not running",
                self.backend, self.instance_id
            ),
        })?;
        if let Some(status) = child.try_wait()? {
            *guard = None;
            return Err(BlazeError::BackendError {
                msg: format!(
                    "{} backend {} exited before {signal}: {status}",
                    self.backend, self.instance_id
                ),
            });
        }
        signal_process(child.id(), signal).await
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
        self.signal("-STOP").await
    }

    async fn resume(&self) -> Result<()> {
        self.signal("-CONT").await
    }

    async fn snapshot(&self, _request: SnapshotRequest) -> Result<SnapshotResult> {
        Err(BlazeError::BackendError {
            msg: format!("{} does not support snapshots", self.backend),
        })
    }

    async fn flush_dirty(&self) -> Result<FlushResult> {
        Ok(FlushResult::default())
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
            match child.try_wait()? {
                Some(_) => {}
                None => {
                    child.start_kill()?;
                    child.wait().await?;
                }
            }
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
    let task_files = files.clone();
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = task_token.cancelled() => break,
                accepted = listener.accept() => {
                    let Ok((stream, _)) = accepted else {
                        break;
                    };
                    let files = task_files.clone();
                    tokio::spawn(async move {
                        if let Err(error) = serve_mock_guest(stream, files).await {
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
        let memory = serde_json::to_vec(&*self.files.lock().await)?;
        tokio::fs::write(&request.mem_path, memory).await?;
        Ok(SnapshotResult {
            snapshot_path: request.snapshot_path,
            mem_path: request.mem_path,
        })
    }

    async fn flush_dirty(&self) -> Result<FlushResult> {
        Ok(FlushResult::default())
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

async fn serve_mock_guest(
    mut stream: tokio::net::UnixStream,
    files: Arc<Mutex<HashMap<String, Vec<u8>>>>,
) -> std::io::Result<()> {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD as BASE64;

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
    let response = match request.get("op").and_then(serde_json::Value::as_str) {
        Some("exec") => {
            let command = request
                .get("cmd")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            serde_json::json!({
                "id": id,
                "ok": true,
                "rc": 0,
                "stdout_b64": BASE64.encode(command.as_bytes()),
                "stderr_b64": ""
            })
        }
        Some("read") => {
            let path = request
                .get("path")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            let data = files.lock().await.get(path).cloned().unwrap_or_default();
            serde_json::json!({"id": id, "ok": true, "data_b64": BASE64.encode(data)})
        }
        Some("write") => {
            let path = request
                .get("path")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string();
            let data = request
                .get("data_b64")
                .and_then(serde_json::Value::as_str)
                .and_then(|encoded| BASE64.decode(encoded).ok())
                .unwrap_or_default();
            files.lock().await.insert(path, data);
            serde_json::json!({"id": id, "ok": true})
        }
        _ => serde_json::json!({"id": id, "ok": true}),
    };
    let mut encoded = serde_json::to_vec(&response).unwrap_or_else(|_| b"{}".to_vec());
    encoded.push(b'\n');
    stream.write_all(&encoded).await
}

async fn read_mock_line(
    stream: &mut tokio::net::UnixStream,
    limit: usize,
) -> std::io::Result<Vec<u8>> {
    let mut output = Vec::new();
    let mut byte = [0_u8; 1];
    loop {
        if stream.read(&mut byte).await? == 0 {
            return Ok(output);
        }
        if byte[0] == b'\n' {
            return Ok(output);
        }
        if output.len() >= limit {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "mock guest line too long",
            ));
        }
        output.push(byte[0]);
    }
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

    #[tokio::test]
    async fn mock_instance_supports_guest_io_snapshot_and_idempotent_kill() {
        let temp = tempfile::tempdir().expect("temp");
        let instance = MockSpawner
            .spawn(request(temp.path()))
            .await
            .expect("spawn");
        let client = GuestClient::new(
            instance.guest_socket_path().to_path_buf(),
            Duration::from_secs(1),
            1024,
        );
        client
            .write_file("/tmp/value".into(), b"hello")
            .await
            .expect("write");
        assert_eq!(
            client.read_file("/tmp/value".into()).await.expect("read"),
            b"hello"
        );
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
}
