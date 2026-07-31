// SPDX-License-Identifier: Apache-2.0
//! Firecracker process ownership and HTTP API over Unix domain sockets.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use blaze_core::backend::{
    BackendKind, RestoreCapability, RestoreRequest, SnapshotKind, SnapshotRequest, SnapshotResult,
    SpawnRequest,
};
use blaze_core::policy::{FirecrackerConfig, VmConfig, parse_memory_value, to_mib_ceil};
use blaze_core::{BlazeError, Result};
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::client::conn::http1;
use hyper::{Method, Request};
use hyper_util::rt::TokioIo;
use tokio::net::UnixStream;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use uuid::Uuid;

#[cfg(target_os = "linux")]
use super::terminate_recorded_process;
use super::{
    BackendInstance, BackendSpawner, DynBackendInstance, RestoreResult, SpawnFailure, SpawnResult,
    configure_pid_handoff, prepare_pid_handoff, record_backend_stopped, remove_file_if_exists,
    spawn_result, stopped_marker, terminate_child,
};

const MAX_API_RESPONSE_BYTES: usize = 64 * 1024;

/// Firecracker backend factory.
pub struct FirecrackerSpawner {
    images_dir: PathBuf,
    api_timeout: Duration,
    socket_timeout: Duration,
}

impl FirecrackerSpawner {
    /// Create a spawner resolving the guest kernel from `images_dir`.
    pub fn new(images_dir: PathBuf) -> Self {
        Self {
            images_dir,
            api_timeout: Duration::from_secs(30),
            socket_timeout: Duration::from_secs(5),
        }
    }

    async fn capture_for(
        &self,
        binary_path: &Path,
        api_socket: PathBuf,
    ) -> Result<FirecrackerCapture> {
        let backend_version = read_backend_version(binary_path).await?;
        Ok(FirecrackerCapture::new(
            api_socket,
            self.api_timeout,
            backend_version,
        ))
    }

    async fn start(
        &self,
        request: SpawnRequest,
        restore: Option<FirecrackerRestore>,
    ) -> std::result::Result<DynBackendInstance, SpawnFailure> {
        validate_regular_file(&request.binary_path, "firecracker binary")?;
        validate_regular_file(&request.storage.rootfs_path, "rootfs")?;
        match &restore {
            Some(restore) => {
                validate_regular_file(&restore.snapshot_path, "VM-state snapshot")?;
                validate_regular_file(&restore.mem_path, "memory snapshot")?;
            }
            None => validate_regular_file(&self.images_dir.join("vmlinux"), "vmlinux")?,
        }
        let api_socket = request.run_dir.join("api.sock");
        let capture = self
            .capture_for(&request.binary_path, api_socket.clone())
            .await?;
        if let Some(restore) = &restore {
            validate_restore_compatibility(restore, &capture.backend_version)?;
        }
        tokio::fs::create_dir_all(&request.run_dir).await?;
        let guest_socket = request.run_dir.join("vsock.uds");
        let pid_file = request.run_dir.join("firecracker.pid");
        let stopped_marker = stopped_marker(&request.run_dir);
        remove_if_exists(&api_socket).await?;
        remove_if_exists(&guest_socket).await?;
        remove_file_if_exists(&stopped_marker).await?;
        let fc_config = request
            .backend
            .firecracker
            .as_ref()
            .cloned()
            .unwrap_or_default();
        let expose_guest_socket = restore.as_ref().map_or(fc_config.enable_vsock, |restore| {
            restore.expose_guest_socket
        });
        let mut command = build_launch_command(&request.binary_path, &api_socket);
        if restore.is_none() {
            let config_path =
                write_vm_config(&self.images_dir, &request, &fc_config, &guest_socket)?;
            command.arg("--config-file").arg(config_path);
        }
        configure_logs(&mut command, &request.run_dir, fc_config.serial_log)?;
        command.env("BLAZE_INSTANCE_ID", request.instance_id.to_string());
        let pid_handoff = configure_pid_handoff(&mut command, &pid_file)?;
        let child = command.spawn();
        drop(pid_handoff);
        let mut child = match child {
            Ok(child) => child,
            Err(source) => return Err(source.into()),
        };
        if let Err(error) = wait_for_socket(&api_socket, &mut child, self.socket_timeout).await {
            let owner: DynBackendInstance = Arc::new(FirecrackerInstance::new(
                request.instance_id,
                child,
                capture,
                guest_socket,
                expose_guest_socket,
                pid_file,
                stopped_marker,
            ));
            return Err(SpawnFailure::compensate_started(error, owner).await);
        }

        let instance = Arc::new(FirecrackerInstance::new(
            request.instance_id,
            child,
            capture,
            guest_socket,
            expose_guest_socket,
            pid_file,
            stopped_marker,
        ));
        if let Some(restore) = restore
            && let Err(error) = instance.load_snapshot(&restore).await
        {
            let owner: DynBackendInstance = instance;
            return Err(SpawnFailure::compensate_started(error, owner).await);
        }
        Ok(instance)
    }
}

#[async_trait]
impl BackendSpawner for FirecrackerSpawner {
    async fn prepare_spawn(&self, run_dir: &Path) -> Result<()> {
        tokio::fs::create_dir_all(run_dir).await?;
        prepare_pid_handoff(&run_dir.join("firecracker.pid"))
    }

    async fn spawn(
        &self,
        request: SpawnRequest,
    ) -> std::result::Result<DynBackendInstance, SpawnFailure> {
        self.start(request, None).await
    }

    async fn restore_capability(&self, binary_path: &Path) -> Result<Option<RestoreCapability>> {
        validate_regular_file(binary_path, "firecracker binary")?;
        Ok(Some(RestoreCapability {
            backend: BackendKind::Firecracker,
            version: Some(read_backend_version(binary_path).await?),
            snapshot_kind: SnapshotKind::Full,
        }))
    }

    async fn restore(&self, request: RestoreRequest) -> RestoreResult {
        let RestoreRequest {
            instance_id,
            run_dir,
            binary_path,
            storage,
            snapshot_path,
            mem_path,
            checkpoint_backend,
            expected_version,
            snapshot_kind,
            expose_guest_socket,
        } = request;
        self.start(
            SpawnRequest {
                instance_id,
                run_dir,
                binary_path,
                storage,
                // Snapshot restore does not reconstruct the original policy.
                // Keep host-side logging disabled unless a future restore
                // contract models that setting explicitly.
                backend: blaze_core::policy::BackendConfigs::default(),
                vm: None,
            },
            Some(FirecrackerRestore {
                snapshot_path,
                mem_path,
                backend: checkpoint_backend,
                expected_version,
                snapshot_kind,
                expose_guest_socket,
            }),
        )
        .await
    }

    async fn probe(&self, binary_path: &Path) -> Result<bool> {
        if !binary_path.is_file() || !executable_in_path("unshare") {
            return Ok(false);
        }
        match read_backend_version(binary_path).await {
            Ok(_) => Ok(true),
            Err(error) => {
                tracing::debug!(%error, binary = %binary_path.display(), "firecracker version probe failed");
                Ok(false)
            }
        }
    }

    async fn cleanup_orphan(&self, instance_id: Uuid, run_dir: &Path) -> Result<()> {
        cleanup_orphan_run_dir(instance_id, run_dir).await
    }
}

struct FirecrackerInstance {
    instance_id: Uuid,
    child: Mutex<Option<Child>>,
    capture: FirecrackerCapture,
    guest_socket: PathBuf,
    pid_file: PathBuf,
    stopped_marker: PathBuf,
    killed: AtomicBool,
}

impl FirecrackerInstance {
    fn new(
        instance_id: Uuid,
        child: Child,
        capture: FirecrackerCapture,
        guest_socket: PathBuf,
        enable_vsock: bool,
        pid_file: PathBuf,
        stopped_marker: PathBuf,
    ) -> Self {
        Self {
            instance_id,
            child: Mutex::new(Some(child)),
            capture,
            guest_socket: configured_guest_socket(enable_vsock, guest_socket),
            pid_file,
            stopped_marker,
            killed: AtomicBool::new(false),
        }
    }

    async fn load_snapshot(&self, restore: &FirecrackerRestore) -> Result<()> {
        self.capture.api.load_snapshot(restore).await
    }
}

#[async_trait]
impl BackendInstance for FirecrackerInstance {
    fn instance_id(&self) -> Uuid {
        self.instance_id
    }

    fn backend(&self) -> BackendKind {
        BackendKind::Firecracker
    }

    fn version(&self) -> Option<&str> {
        Some(&self.capture.backend_version)
    }

    fn supports_checkpoint_capture(&self) -> bool {
        true
    }

    fn guest_socket_path(&self) -> &Path {
        &self.guest_socket
    }

    async fn try_wait(&self) -> Result<Option<SpawnResult>> {
        let status = {
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
            status
        };
        self.cleanup().await?;
        Ok(Some(spawn_result(self.instance_id, status)))
    }

    async fn pause(&self) -> Result<()> {
        self.capture
            .api
            .call_json(
                Method::PATCH,
                "/vm",
                Some(serde_json::json!({"state": "Paused"})),
            )
            .await?;
        Ok(())
    }

    async fn resume(&self) -> Result<()> {
        self.capture
            .api
            .call_json(
                Method::PATCH,
                "/vm",
                Some(serde_json::json!({"state": "Resumed"})),
            )
            .await?;
        Ok(())
    }

    async fn snapshot(&self, request: SnapshotRequest) -> Result<SnapshotResult> {
        let SnapshotRequest {
            snapshot_path,
            mem_path,
            kind: SnapshotKind::Full,
        } = request;
        for path in [&snapshot_path, &mem_path] {
            if let Some(parent) = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                tokio::fs::create_dir_all(parent).await?;
            }
        }
        self.capture
            .api
            .call_json(
                Method::PUT,
                "/snapshot/create",
                Some(serde_json::json!({
                    "snapshot_path": snapshot_path,
                    "mem_file_path": mem_path,
                    "snapshot_type": "Full"
                })),
            )
            .await?;
        Ok(SnapshotResult {
            snapshot_path,
            mem_path,
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
            terminate_child(child, "firecracker").await?;
        }
        record_backend_stopped(&self.stopped_marker).await?;
        *guard = None;
        drop(guard);
        self.cleanup().await?;
        self.killed.store(true, Ordering::Release);
        Ok(())
    }
}

struct FirecrackerCapture {
    api: FirecrackerApiClient,
    backend_version: String,
}

#[derive(Debug)]
struct FirecrackerRestore {
    snapshot_path: PathBuf,
    mem_path: PathBuf,
    backend: BackendKind,
    expected_version: Option<String>,
    snapshot_kind: SnapshotKind,
    expose_guest_socket: bool,
}

impl FirecrackerCapture {
    fn new(api_socket: PathBuf, api_timeout: Duration, backend_version: String) -> Self {
        Self {
            api: FirecrackerApiClient::new(api_socket, api_timeout),
            backend_version,
        }
    }
}

#[derive(Debug, Clone)]
struct FirecrackerApiClient {
    socket: PathBuf,
    timeout: Duration,
}

impl FirecrackerApiClient {
    fn new(socket: PathBuf, timeout: Duration) -> Self {
        Self { socket, timeout }
    }

    async fn load_snapshot(&self, restore: &FirecrackerRestore) -> Result<()> {
        self.call_json(
            Method::PUT,
            "/snapshot/load",
            Some(serde_json::json!({
                "snapshot_path": path_string(&restore.snapshot_path, "VM-state snapshot")?,
                "mem_backend": {
                    "backend_type": "File",
                    "backend_path": path_string(&restore.mem_path, "memory snapshot")?
                },
                "resume_vm": true
            })),
        )
        .await?;
        Ok(())
    }

    async fn call_json(
        &self,
        method: Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> Result<Vec<u8>> {
        let operation = async {
            let stream = UnixStream::connect(&self.socket).await?;
            let (mut sender, connection) = http1::handshake(TokioIo::new(stream))
                .await
                .map_err(backend_protocol_error)?;
            tokio::spawn(async move {
                if let Err(error) = connection.await {
                    tracing::debug!(%error, "firecracker API connection ended");
                }
            });
            let bytes = match body {
                Some(body) => {
                    serde_json::to_vec(&body).map_err(|error| BlazeError::BackendError {
                        msg: format!("serialize Firecracker API request: {error}"),
                    })?
                }
                None => Vec::new(),
            };
            let mut builder = Request::builder()
                .method(method.clone())
                .uri(format!("http://localhost{path}"));
            if !bytes.is_empty() {
                builder = builder.header("content-type", "application/json");
            }
            let request = builder
                .body(Full::new(Bytes::from(bytes)))
                .map_err(|error| BlazeError::BackendError {
                    msg: format!("build Firecracker API request: {error}"),
                })?;
            let response = sender
                .send_request(request)
                .await
                .map_err(backend_protocol_error)?;
            let status = response.status();
            let mut response_body = response.into_body();
            let mut collected = Vec::new();
            while let Some(frame) = response_body.frame().await {
                let frame = frame.map_err(backend_protocol_error)?;
                if let Ok(data) = frame.into_data() {
                    let remaining = MAX_API_RESPONSE_BYTES.saturating_sub(collected.len());
                    collected.extend_from_slice(&data[..data.len().min(remaining)]);
                    if data.len() > remaining {
                        return Err(BlazeError::BackendError {
                            msg: format!(
                                "Firecracker {method} {path} response exceeded \
                                 {MAX_API_RESPONSE_BYTES} bytes"
                            ),
                        });
                    }
                }
            }
            if !status.is_success() {
                return Err(BlazeError::BackendError {
                    msg: format!(
                        "Firecracker {method} {path} returned {status}: {}",
                        String::from_utf8_lossy(&collected)
                    ),
                });
            }
            Ok(collected)
        };
        tokio::time::timeout(self.timeout, operation)
            .await
            .map_err(|_| BlazeError::BackendError {
                msg: format!(
                    "Firecracker {method} {path} timed out after {:?}",
                    self.timeout
                ),
            })?
    }
}

fn validate_restore_compatibility(
    restore: &FirecrackerRestore,
    actual_version: &str,
) -> Result<()> {
    if restore.backend != BackendKind::Firecracker {
        return Err(BlazeError::BackendError {
            msg: format!(
                "Firecracker cannot restore a {} checkpoint",
                restore.backend
            ),
        });
    }
    if restore.snapshot_kind != SnapshotKind::Full {
        return Err(BlazeError::BackendError {
            msg: "Firecracker restore accepts only full checkpoints".to_string(),
        });
    }
    let expected_version =
        restore
            .expected_version
            .as_deref()
            .ok_or_else(|| BlazeError::BackendError {
                msg: "Firecracker restore requires a checkpoint backend version".to_string(),
            })?;
    if expected_version != actual_version {
        return Err(BlazeError::BackendError {
            msg: format!(
                "Firecracker checkpoint version {expected_version:?} does not match \
                 executable version {actual_version:?}"
            ),
        });
    }
    Ok(())
}

impl FirecrackerInstance {
    async fn cleanup(&self) -> Result<()> {
        remove_if_exists(&self.capture.api.socket).await?;
        remove_if_exists(&self.guest_socket).await?;
        remove_if_exists(&self.pid_file).await?;
        Ok(())
    }
}

fn write_vm_config(
    images_dir: &Path,
    request: &SpawnRequest,
    config: &FirecrackerConfig,
    guest_socket: &Path,
) -> Result<PathBuf> {
    let vcpus = config
        .vcpus
        .or(request.vm.as_ref().map(|vm| vm.vcpus))
        .unwrap_or(1);
    let memory_mib = resolve_memory(config, request.vm.as_ref())?;
    let mut value = serde_json::json!({
        "boot-source": {
            "kernel_image_path": path_string(&images_dir.join("vmlinux"), "vmlinux")?,
            "boot_args": config.boot_args
        },
        "drives": [{
            "drive_id": "rootfs",
            "path_on_host": path_string(&request.storage.rootfs_path, "rootfs")?,
            "is_root_device": true,
            "is_read_only": false
        }],
        "machine-config": {
            "vcpu_count": vcpus,
            "mem_size_mib": memory_mib
        }
    });
    if config.enable_vsock {
        value["vsock"] = serde_json::json!({
            "guest_cid": 3,
            "uds_path": path_string(guest_socket, "guest socket")?
        });
    }
    let path = request.run_dir.join("vmconfig.json");
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(&value).map_err(|error| BlazeError::BackendError {
            msg: format!("serialize Firecracker VM config: {error}"),
        })?,
    )?;
    Ok(path)
}

fn resolve_memory(config: &FirecrackerConfig, vm: Option<&VmConfig>) -> Result<u64> {
    let value = config
        .memory
        .as_deref()
        .or_else(|| vm.map(|vm| vm.memory.as_str()))
        .unwrap_or("256Mi");
    parse_memory_value(value)
        .map(to_mib_ceil)
        .map_err(|error| BlazeError::BackendError {
            msg: format!("invalid Firecracker memory {value:?}: {error}"),
        })
}

fn build_launch_command(binary: &Path, api_socket: &Path) -> Command {
    #[cfg(target_os = "linux")]
    let mut command = {
        let mut command = Command::new("unshare");
        command
            .arg("--mount")
            .arg("--propagation")
            .arg("private")
            .arg("--")
            .arg(binary);
        command
    };
    #[cfg(not(target_os = "linux"))]
    let mut command = Command::new(binary);
    command.arg("--api-sock").arg(api_socket);
    command.arg("--id").arg(format!(
        "fc-{}",
        api_socket
            .parent()
            .and_then(Path::file_name)
            .and_then(std::ffi::OsStr::to_str)
            .unwrap_or("blaze")
    ));
    command
}

fn configure_logs(command: &mut Command, run_dir: &Path, serial_log: bool) -> Result<()> {
    if serial_log {
        let serial_log = run_dir.join("serial.log");
        rotate_serial_log_if_needed(&serial_log)?;
        let stdout = std::fs::File::create(serial_log)?;
        command.stdout(stdout);
    } else {
        command.stdout(Stdio::null());
    }
    let stderr = std::fs::File::create(run_dir.join("stderr.log"))?;
    command.stderr(stderr);
    command.stdin(Stdio::null());
    Ok(())
}

async fn read_backend_version(binary_path: &Path) -> Result<String> {
    let mut busy_retries = 0;
    let output = loop {
        match tokio::time::timeout(
            Duration::from_secs(5),
            Command::new(binary_path).arg("--version").output(),
        )
        .await
        {
            Ok(Ok(output)) => break output,
            Ok(Err(error))
                if error.kind() == std::io::ErrorKind::ExecutableFileBusy && busy_retries < 3 =>
            {
                busy_retries += 1;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => {
                return Err(BlazeError::BackendError {
                    msg: format!("firecracker probe timed out: {}", binary_path.display()),
                });
            }
        }
    };
    if !output.status.success() {
        return Err(BlazeError::BackendError {
            msg: format!(
                "firecracker version probe failed with {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            ),
        });
    }
    parse_backend_version(&output.stdout)
}

fn parse_backend_version(stdout: &[u8]) -> Result<String> {
    let stdout = std::str::from_utf8(stdout).map_err(|error| BlazeError::BackendError {
        msg: format!("firecracker version probe returned non-UTF-8 output: {error}"),
    })?;
    let mut versions = stdout
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("Firecracker v"));
    let version = versions.next().ok_or_else(|| BlazeError::BackendError {
        msg: "firecracker version probe did not return a Firecracker version line".to_string(),
    })?;
    if versions.next().is_some() {
        return Err(BlazeError::BackendError {
            msg: "firecracker version probe returned multiple Firecracker version lines"
                .to_string(),
        });
    }
    let release = version
        .strip_prefix("Firecracker v")
        .expect("version prefix checked");
    if release.is_empty() || release.chars().any(char::is_whitespace) {
        return Err(BlazeError::BackendError {
            msg: format!("firecracker version probe returned an invalid version line: {version:?}"),
        });
    }
    Ok(version.to_string())
}

async fn wait_for_socket(socket: &Path, child: &mut Child, timeout: Duration) -> Result<()> {
    let started = Instant::now();
    loop {
        if socket.exists() && UnixStream::connect(socket).await.is_ok() {
            return Ok(());
        }
        if let Some(status) = child.try_wait()? {
            return Err(BlazeError::BackendError {
                msg: format!(
                    "Firecracker exited before API socket {} became ready: {status}",
                    socket.display()
                ),
            });
        }
        if started.elapsed() >= timeout {
            return Err(BlazeError::BackendError {
                msg: format!(
                    "Firecracker API socket {} was not ready within {timeout:?}",
                    socket.display()
                ),
            });
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn remove_if_exists(path: &Path) -> Result<()> {
    if path.as_os_str().is_empty() {
        return Ok(());
    }
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn configured_guest_socket(enable_vsock: bool, socket: PathBuf) -> PathBuf {
    if enable_vsock { socket } else { PathBuf::new() }
}

fn validate_regular_file(path: &Path, label: &str) -> Result<()> {
    if !path.is_file() {
        return Err(BlazeError::BackendError {
            msg: format!("{label} not found at {}", path.display()),
        });
    }
    Ok(())
}

fn executable_in_path(name: &str) -> bool {
    let Some(path) = std::env::var_os("PATH") else {
        return false;
    };
    std::env::split_paths(&path).any(|directory| is_executable_file(&directory.join(name)))
}

fn is_executable_file(candidate: &Path) -> bool {
    if !candidate.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(candidate)
            .map(|metadata| metadata.permissions().mode() & 0o111 != 0)
            .unwrap_or(false)
    }
    #[cfg(not(unix))]
    {
        true
    }
}

fn rotate_serial_log_if_needed(path: &Path) -> Result<()> {
    const MAX_SERIAL_LOG_BYTES: u64 = 16 * 1024 * 1024;
    let Ok(metadata) = std::fs::metadata(path) else {
        return Ok(());
    };
    if metadata.len() <= MAX_SERIAL_LOG_BYTES {
        return Ok(());
    }
    let backup = path.with_extension("log.1");
    match std::fs::remove_file(&backup) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    std::fs::rename(path, backup)?;
    Ok(())
}

fn path_string<'a>(path: &'a Path, label: &str) -> Result<&'a str> {
    path.to_str().ok_or_else(|| BlazeError::BackendError {
        msg: format!("{label} path is not valid UTF-8: {}", path.display()),
    })
}

fn backend_protocol_error(error: hyper::Error) -> BlazeError {
    BlazeError::BackendError {
        msg: format!("Firecracker API protocol error: {error}"),
    }
}

pub(super) async fn cleanup_orphan_run_dir(instance_id: Uuid, run_dir: &Path) -> Result<()> {
    let stopped_marker = stopped_marker(run_dir);
    if stopped_marker.is_file() {
        return Ok(());
    }
    let pid_file = run_dir.join("firecracker.pid");
    #[cfg(target_os = "linux")]
    {
        terminate_recorded_process(instance_id, &pid_file, "firecracker").await?;
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = instance_id;
        if pid_file.exists() {
            return Err(BlazeError::BackendError {
                msg: format!(
                    "cannot validate Firecracker orphan {} outside Linux",
                    pid_file.display()
                ),
            });
        }
    }

    record_backend_stopped(&stopped_marker).await?;
    remove_if_exists(&run_dir.join("api.sock")).await?;
    remove_if_exists(&run_dir.join("vsock.uds")).await?;
    remove_if_exists(&pid_file).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #[cfg(target_os = "linux")]
    use std::convert::Infallible;

    use blaze_core::storage::StorageSlot;
    #[cfg(target_os = "linux")]
    use http_body_util::BodyExt;
    #[cfg(target_os = "linux")]
    use hyper::Response;
    #[cfg(target_os = "linux")]
    use hyper::server::conn::http1 as server_http1;
    #[cfg(target_os = "linux")]
    use hyper::service::service_fn;
    #[cfg(target_os = "linux")]
    use tokio::net::UnixListener;
    #[cfg(target_os = "linux")]
    use tokio::sync::oneshot;

    #[cfg(target_os = "linux")]
    use crate::spawner::SpawnerRegistry;

    use super::*;

    #[test]
    fn version_parser_discards_non_version_log_lines() {
        let stdout = b"Firecracker v1.16.0\n\n\
            2026-07-24T21:55:14Z [anonymous-instance:main] \
            Firecracker exiting successfully. exit_code=0\n";
        assert_eq!(
            parse_backend_version(stdout).expect("version"),
            "Firecracker v1.16.0"
        );
    }

    #[test]
    fn version_parser_rejects_missing_or_ambiguous_version() {
        assert!(parse_backend_version(b"Firecracker exiting successfully\n").is_err());
        assert!(parse_backend_version(b"Firecracker v1.15.0\nFirecracker v1.16.0\n").is_err());
    }

    #[test]
    fn restore_compatibility_requires_the_matching_firecracker_version() {
        let mut restore = FirecrackerRestore {
            snapshot_path: PathBuf::from("vmstate.snap"),
            mem_path: PathBuf::from("memory.snap"),
            backend: BackendKind::Firecracker,
            expected_version: Some("Firecracker v1.16.0".to_string()),
            snapshot_kind: SnapshotKind::Full,
            expose_guest_socket: false,
        };

        validate_restore_compatibility(&restore, "Firecracker v1.16.0").expect("matching version");
        assert!(
            validate_restore_compatibility(&restore, "Firecracker v1.17.0")
                .expect_err("mismatched version")
                .to_string()
                .contains("does not match executable version")
        );
        restore.expected_version = None;
        assert!(
            validate_restore_compatibility(&restore, "Firecracker v1.16.0")
                .expect_err("missing version")
                .to_string()
                .contains("requires a checkpoint backend version")
        );
        restore.expected_version = Some("Firecracker v1.16.0".to_string());
        restore.backend = BackendKind::Mock;
        assert!(
            validate_restore_compatibility(&restore, "Firecracker v1.16.0")
                .expect_err("wrong backend")
                .to_string()
                .contains("cannot restore a mock checkpoint")
        );
    }

    #[cfg(target_os = "linux")]
    async fn spawn_api(
        socket: &Path,
        call_count: usize,
    ) -> oneshot::Receiver<Vec<(Method, String, serde_json::Value)>> {
        let listener = UnixListener::bind(socket).expect("bind");
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let observed = Arc::new(Mutex::new(Vec::with_capacity(call_count)));
            for _ in 0..call_count {
                let (stream, _) = listener.accept().await.expect("accept");
                let observed = observed.clone();
                let service = service_fn(move |request: Request<hyper::body::Incoming>| {
                    let observed = observed.clone();
                    async move {
                        let method = request.method().clone();
                        let path = request.uri().path().to_string();
                        let body = request
                            .into_body()
                            .collect()
                            .await
                            .expect("request body")
                            .to_bytes();
                        let body = if body.is_empty() {
                            serde_json::Value::Null
                        } else {
                            serde_json::from_slice(&body).expect("request JSON")
                        };
                        observed.lock().await.push((method, path, body));
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(hyper::StatusCode::NO_CONTENT)
                                .body(Full::new(Bytes::new()))
                                .expect("response"),
                        )
                    }
                });
                server_http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await
                    .expect("serve");
            }
            let calls = observed.lock().await.clone();
            let _ = tx.send(calls);
        });
        rx
    }

    #[cfg(target_os = "linux")]
    fn spawn_api_response(
        socket: &Path,
        status: hyper::StatusCode,
        body: Vec<u8>,
        delay: Duration,
    ) -> tokio::task::JoinHandle<()> {
        let listener = UnixListener::bind(socket).expect("bind");
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let body = Bytes::from(body);
            let service = service_fn(move |_request: Request<hyper::body::Incoming>| {
                let body = body.clone();
                async move {
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(status)
                            .body(Full::new(body))
                            .expect("response"),
                    )
                }
            });
            let _ = server_http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await;
        })
    }

    #[cfg(target_os = "linux")]
    fn write_version_binary(path: &Path, output: &str) {
        use std::os::unix::fs::PermissionsExt;

        let staged = path.with_extension("new");
        std::fs::write(&staged, format!("#!/bin/sh\nprintf '%s\\n' '{output}'\n"))
            .expect("write version binary");
        std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o755))
            .expect("make version binary executable");
        std::fs::rename(staged, path).expect("replace version binary");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn launch_capture_reads_the_requested_binary_each_time() {
        let temp = tempfile::tempdir().expect("temp");
        let first = temp.path().join("first");
        let second = temp.path().join("second");
        write_version_binary(&first, "Firecracker v1.15.0");
        write_version_binary(&second, "Firecracker v1.16.0");
        let spawner = FirecrackerSpawner::new(temp.path().join("images"));

        let first_capture = spawner
            .capture_for(&first, temp.path().join("first.sock"))
            .await
            .expect("first capture");
        let second_capture = spawner
            .capture_for(&second, temp.path().join("second.sock"))
            .await
            .expect("second capture");
        assert_eq!(first_capture.backend_version, "Firecracker v1.15.0");
        assert_eq!(second_capture.backend_version, "Firecracker v1.16.0");

        write_version_binary(&first, "Firecracker v1.17.0");
        let replaced_capture = spawner
            .capture_for(&first, temp.path().join("replaced.sock"))
            .await
            .expect("replaced capture");
        assert_eq!(replaced_capture.backend_version, "Firecracker v1.17.0");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn probe_checks_each_requested_binary() {
        let temp = tempfile::tempdir().expect("temp");
        let valid = temp.path().join("valid");
        let invalid = temp.path().join("invalid");
        write_version_binary(&valid, "Firecracker v1.16.0");
        write_version_binary(&invalid, "not a Firecracker version");
        let spawner = FirecrackerSpawner::new(temp.path().join("images"));

        assert!(spawner.probe(&valid).await.expect("valid probe"));
        assert!(!spawner.probe(&invalid).await.expect("invalid probe"));

        write_version_binary(&invalid, "Firecracker v1.17.0");
        assert!(spawner.probe(&invalid).await.expect("replaced probe"));
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn registry_restore_capability_reads_each_requested_binary() {
        let temp = tempfile::tempdir().expect("temp");
        let first = temp.path().join("first");
        let second = temp.path().join("second");
        write_version_binary(&first, "Firecracker v1.15.0");
        write_version_binary(&second, "Firecracker v1.16.0");
        let mut registry = SpawnerRegistry::new();
        registry.insert(
            BackendKind::Firecracker,
            Arc::new(FirecrackerSpawner::new(temp.path().join("images"))),
        );
        let adapter = registry
            .get(BackendKind::Firecracker)
            .expect("registered Firecracker adapter");

        let first_capability = adapter
            .restore_capability(&first)
            .await
            .expect("first capability")
            .expect("restore supported");
        let second_capability = adapter
            .restore_capability(&second)
            .await
            .expect("second capability")
            .expect("restore supported");
        assert_eq!(first_capability.backend, BackendKind::Firecracker);
        assert_eq!(
            first_capability.version.as_deref(),
            Some("Firecracker v1.15.0")
        );
        assert_eq!(first_capability.snapshot_kind, SnapshotKind::Full);
        assert_eq!(
            second_capability.version.as_deref(),
            Some("Firecracker v1.16.0")
        );

        write_version_binary(&first, "Firecracker v1.17.0");
        let replaced_capability = adapter
            .restore_capability(&first)
            .await
            .expect("replaced capability")
            .expect("restore supported");
        assert_eq!(
            replaced_capability.version.as_deref(),
            Some("Firecracker v1.17.0")
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn restore_rejects_a_binary_version_change_before_start() {
        let temp = tempfile::tempdir().expect("temp");
        let spawn = spawn_request(temp.path());
        let run_dir = spawn.run_dir.clone();
        std::fs::remove_dir_all(&run_dir).expect("remove fixture run dir");
        write_version_binary(&spawn.binary_path, "Firecracker v1.17.0");
        std::fs::create_dir_all(spawn.storage.rootfs_path.parent().expect("rootfs parent"))
            .expect("slot");
        std::fs::write(&spawn.storage.rootfs_path, b"rootfs").expect("rootfs");
        let snapshot_path = temp.path().join("vmstate.snap");
        let mem_path = temp.path().join("memory.snap");
        std::fs::write(&snapshot_path, b"vmstate").expect("VM state");
        std::fs::write(&mem_path, b"memory").expect("memory");
        let spawner = FirecrackerSpawner::new(temp.path().join("images"));

        let failure = match spawner
            .restore(RestoreRequest {
                instance_id: spawn.instance_id,
                run_dir: spawn.run_dir,
                binary_path: spawn.binary_path,
                storage: spawn.storage,
                snapshot_path,
                mem_path,
                checkpoint_backend: BackendKind::Firecracker,
                expected_version: Some("Firecracker v1.16.0".to_string()),
                snapshot_kind: SnapshotKind::Full,
                expose_guest_socket: false,
            })
            .await
        {
            Ok(_) => panic!("version change must fail before process start"),
            Err(failure) => failure,
        };
        let (source, owner) = failure.into_parts();

        assert!(
            source
                .to_string()
                .contains("does not match executable version")
        );
        assert!(owner.is_none());
        assert!(!run_dir.exists());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn instance_loads_a_full_snapshot_with_a_minimal_payload() {
        let temp = tempfile::tempdir().expect("temp");
        let api_socket = temp.path().join("api.sock");
        let observed = spawn_api(&api_socket, 1).await;
        let child = Command::new("sleep")
            .arg("60")
            .spawn()
            .expect("spawn child");
        let instance = FirecrackerInstance::new(
            Uuid::new_v4(),
            child,
            FirecrackerCapture::new(
                api_socket,
                Duration::from_secs(1),
                "Firecracker v1.16.0".to_string(),
            ),
            PathBuf::new(),
            false,
            temp.path().join("firecracker.pid"),
            stopped_marker(temp.path()),
        );
        let snapshot_path = temp.path().join("vmstate.snap");
        let mem_path = temp.path().join("memory.snap");

        instance
            .load_snapshot(&FirecrackerRestore {
                snapshot_path: snapshot_path.clone(),
                mem_path: mem_path.clone(),
                backend: BackendKind::Firecracker,
                expected_version: Some("Firecracker v1.16.0".to_string()),
                snapshot_kind: SnapshotKind::Full,
                expose_guest_socket: false,
            })
            .await
            .expect("load snapshot");

        let calls = observed.await.expect("observed call");
        assert_eq!(
            calls,
            vec![(
                Method::PUT,
                "/snapshot/load".to_string(),
                serde_json::json!({
                    "snapshot_path": snapshot_path,
                    "mem_backend": {
                        "backend_type": "File",
                        "backend_path": mem_path,
                    },
                    "resume_vm": true,
                }),
            )]
        );
        instance.kill().await.expect("kill");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn restored_owner_exposes_the_guest_socket_when_requested() {
        let temp = tempfile::tempdir().expect("temp");
        let api_socket = temp.path().join("api.sock");
        let observed = spawn_api(&api_socket, 1).await;
        let child = Command::new("sleep")
            .arg("60")
            .spawn()
            .expect("spawn child");
        let guest_socket = temp.path().join("vsock.uds");
        let instance = FirecrackerInstance::new(
            Uuid::new_v4(),
            child,
            FirecrackerCapture::new(
                api_socket,
                Duration::from_secs(1),
                "Firecracker v1.16.0".to_string(),
            ),
            guest_socket.clone(),
            true,
            temp.path().join("firecracker.pid"),
            stopped_marker(temp.path()),
        );
        let snapshot_path = temp.path().join("vmstate.snap");
        let mem_path = temp.path().join("memory.snap");

        assert_eq!(instance.guest_socket_path(), guest_socket);
        instance
            .load_snapshot(&FirecrackerRestore {
                snapshot_path: snapshot_path.clone(),
                mem_path: mem_path.clone(),
                backend: BackendKind::Firecracker,
                expected_version: Some("Firecracker v1.16.0".to_string()),
                snapshot_kind: SnapshotKind::Full,
                expose_guest_socket: true,
            })
            .await
            .expect("load snapshot");

        let calls = observed.await.expect("observed call");
        assert_eq!(
            calls,
            vec![(
                Method::PUT,
                "/snapshot/load".to_string(),
                serde_json::json!({
                    "snapshot_path": snapshot_path,
                    "mem_backend": {
                        "backend_type": "File",
                        "backend_path": mem_path,
                    },
                    "resume_vm": true,
                }),
            )]
        );
        instance.kill().await.expect("kill");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn failed_snapshot_load_retains_an_owner_when_cleanup_is_incomplete() {
        let temp = tempfile::tempdir().expect("temp");
        let api_socket = temp.path().join("api.sock");
        let server = spawn_api_response(
            &api_socket,
            hyper::StatusCode::BAD_REQUEST,
            b"incompatible snapshot".to_vec(),
            Duration::ZERO,
        );
        let child = Command::new("sleep")
            .arg("60")
            .spawn()
            .expect("spawn child");
        let guest_socket = temp.path().join("guest.sock");
        std::fs::create_dir(&guest_socket).expect("cleanup blocker");
        let instance_id = Uuid::new_v4();
        let instance = Arc::new(FirecrackerInstance::new(
            instance_id,
            child,
            FirecrackerCapture::new(
                api_socket,
                Duration::from_secs(1),
                "Firecracker v1.16.0".to_string(),
            ),
            guest_socket.clone(),
            true,
            temp.path().join("firecracker.pid"),
            stopped_marker(temp.path()),
        ));
        let restore = FirecrackerRestore {
            snapshot_path: temp.path().join("vmstate.snap"),
            mem_path: temp.path().join("memory.snap"),
            backend: BackendKind::Firecracker,
            expected_version: Some("Firecracker v1.16.0".to_string()),
            snapshot_kind: SnapshotKind::Full,
            expose_guest_socket: true,
        };

        let load_error = instance
            .load_snapshot(&restore)
            .await
            .expect_err("snapshot load must fail");
        server.await.expect("server");
        let owner: DynBackendInstance = instance;
        let failure = SpawnFailure::compensate_started(load_error, owner).await;
        let (source, retained) = failure.into_parts();

        assert!(source.to_string().contains("cleanup failed"));
        let retained = retained.expect("incomplete cleanup must retain ownership");
        assert_eq!(retained.instance_id(), instance_id);
        assert_eq!(retained.version(), Some("Firecracker v1.16.0"));

        std::fs::remove_dir(&guest_socket).expect("remove cleanup blocker");
        retained.kill().await.expect("retry cleanup");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn instance_reports_version_and_captures_full_snapshot_over_uds() {
        let temp = tempfile::tempdir().expect("temp");
        let api_socket = temp.path().join("api.sock");
        let observed = spawn_api(&api_socket, 3).await;
        let child = Command::new("sleep")
            .arg("60")
            .spawn()
            .expect("spawn child");
        let instance_id = Uuid::new_v4();
        let instance = FirecrackerInstance::new(
            instance_id,
            child,
            FirecrackerCapture::new(
                api_socket,
                Duration::from_secs(1),
                "Firecracker v1.16.0".to_string(),
            ),
            PathBuf::new(),
            false,
            temp.path().join("firecracker.pid"),
            stopped_marker(temp.path()),
        );
        let snapshot_path = temp.path().join("checkpoint/vmstate.snap");
        let mem_path = temp.path().join("checkpoint/memory.snap");

        assert_eq!(instance.instance_id(), instance_id);
        assert_eq!(instance.version(), Some("Firecracker v1.16.0"));
        assert!(instance.supports_checkpoint_capture());
        instance.pause().await.expect("pause");
        let result = instance
            .snapshot(SnapshotRequest {
                snapshot_path: snapshot_path.clone(),
                mem_path: mem_path.clone(),
                kind: SnapshotKind::Full,
            })
            .await
            .expect("snapshot");
        instance.resume().await.expect("resume");

        assert_eq!(result.snapshot_path, snapshot_path);
        assert_eq!(result.mem_path, mem_path);
        let calls = observed.await.expect("observed calls");
        assert_eq!(
            calls,
            vec![
                (
                    Method::PATCH,
                    "/vm".to_string(),
                    serde_json::json!({"state": "Paused"}),
                ),
                (
                    Method::PUT,
                    "/snapshot/create".to_string(),
                    serde_json::json!({
                        "snapshot_path": snapshot_path,
                        "mem_file_path": mem_path,
                        "snapshot_type": "Full",
                    }),
                ),
                (
                    Method::PATCH,
                    "/vm".to_string(),
                    serde_json::json!({"state": "Resumed"}),
                ),
            ]
        );
        instance.kill().await.expect("kill");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn api_client_reports_non_success_response_body() {
        let temp = tempfile::tempdir().expect("temp");
        let socket = temp.path().join("api.sock");
        let server = spawn_api_response(
            &socket,
            hyper::StatusCode::BAD_REQUEST,
            b"invalid VM state".to_vec(),
            Duration::ZERO,
        );
        let client = FirecrackerApiClient::new(socket, Duration::from_secs(1));

        let error = client
            .call_json(Method::PATCH, "/vm", None)
            .await
            .expect_err("non-success response");
        server.await.expect("server");

        let message = error.to_string();
        assert!(message.contains("400 Bad Request"));
        assert!(message.contains("invalid VM state"));
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn api_client_rejects_an_oversized_response() {
        let temp = tempfile::tempdir().expect("temp");
        let socket = temp.path().join("api.sock");
        let server = spawn_api_response(
            &socket,
            hyper::StatusCode::OK,
            vec![b'x'; MAX_API_RESPONSE_BYTES + 1],
            Duration::ZERO,
        );
        let client = FirecrackerApiClient::new(socket, Duration::from_secs(1));

        let error = client
            .call_json(Method::GET, "/vm", None)
            .await
            .expect_err("oversized response");
        server.await.expect("server");

        assert!(error.to_string().contains("response exceeded 65536 bytes"));
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn api_client_times_out_a_stalled_response() {
        let temp = tempfile::tempdir().expect("temp");
        let socket = temp.path().join("api.sock");
        let server = spawn_api_response(
            &socket,
            hyper::StatusCode::OK,
            Vec::new(),
            Duration::from_millis(200),
        );
        let client = FirecrackerApiClient::new(socket, Duration::from_millis(20));

        let error = client
            .call_json(Method::GET, "/vm", None)
            .await
            .expect_err("stalled response");
        server.await.expect("server");

        assert!(error.to_string().contains("timed out after 20ms"));
    }

    #[test]
    fn vm_config_omits_network_until_the_network_capability_is_enabled() {
        let temp = tempfile::tempdir().expect("temp");
        let request = spawn_request(temp.path());

        let path = write_vm_config(
            &temp.path().join("images"),
            &request,
            &FirecrackerConfig::default(),
            &temp.path().join("guest.sock"),
        )
        .expect("write config");
        let value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).expect("read config"))
                .expect("parse config");
        assert!(value.get("network-interfaces").is_none());
    }

    #[test]
    fn vm_config_and_reported_guest_transport_agree() {
        let temp = tempfile::tempdir().expect("temp");
        let request = spawn_request(temp.path());
        let socket = temp.path().join("vsock.uds");
        let disabled = FirecrackerConfig::default();
        let disabled_path =
            write_vm_config(&temp.path().join("images"), &request, &disabled, &socket)
                .expect("disabled config");
        let disabled_value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(disabled_path).expect("read disabled config"))
                .expect("parse disabled config");
        assert!(disabled_value.get("vsock").is_none());
        assert!(
            configured_guest_socket(disabled.enable_vsock, socket.clone())
                .as_os_str()
                .is_empty()
        );

        let enabled = FirecrackerConfig {
            enable_vsock: true,
            ..FirecrackerConfig::default()
        };
        let enabled_path =
            write_vm_config(&temp.path().join("images"), &request, &enabled, &socket)
                .expect("enabled config");
        let enabled_value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(enabled_path).expect("read enabled config"))
                .expect("parse enabled config");
        assert_eq!(
            enabled_value["vsock"]["uds_path"],
            path_string(&socket, "socket").unwrap()
        );
        assert_eq!(
            configured_guest_socket(enabled.enable_vsock, socket.clone()),
            socket
        );
    }

    #[test]
    fn serial_log_rotates_before_reuse() {
        let temp = tempfile::tempdir().expect("temp");
        let log = temp.path().join("serial.log");
        let file = std::fs::File::create(&log).expect("create log");
        file.set_len(16 * 1024 * 1024 + 1).expect("grow log");

        rotate_serial_log_if_needed(&log).expect("rotate");

        assert!(!log.exists());
        assert_eq!(
            std::fs::metadata(temp.path().join("serial.log.1"))
                .expect("rotated log")
                .len(),
            16 * 1024 * 1024 + 1
        );
    }

    #[cfg(unix)]
    #[test]
    fn executable_check_requires_an_executable_file() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().expect("temp");
        let tool = temp.path().join("tool");
        std::fs::write(&tool, b"#!/bin/sh\n").expect("write tool");
        std::fs::set_permissions(&tool, std::fs::Permissions::from_mode(0o644))
            .expect("non-executable permissions");
        assert!(!is_executable_file(&tool));
        std::fs::set_permissions(&tool, std::fs::Permissions::from_mode(0o755))
            .expect("executable permissions");
        assert!(is_executable_file(&tool));
    }

    #[tokio::test]
    async fn start_failure_terminates_child_and_removes_process_metadata() {
        let temp = tempfile::tempdir().expect("temp");
        let pid_file = temp.path().join("firecracker.pid");
        let termination_marker = temp.path().join("terminated");
        let child = Command::new("sh")
            .arg("-c")
            .arg("trap 'printf term > \"$MARKER\"; exit 0' TERM; while :; do sleep 1; done")
            .env("MARKER", &termination_marker)
            .spawn()
            .expect("spawn child");
        std::fs::write(&pid_file, format!("{}\n", child.id().expect("child pid")))
            .expect("pid metadata");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let owner: DynBackendInstance = Arc::new(FirecrackerInstance::new(
            Uuid::new_v4(),
            child,
            FirecrackerCapture::new(
                temp.path().join("api.sock"),
                Duration::from_secs(1),
                "Firecracker v1.16.0".to_string(),
            ),
            temp.path().join("guest.sock"),
            true,
            pid_file.clone(),
            stopped_marker(temp.path()),
        ));
        let failure = SpawnFailure::compensate_started(
            BlazeError::BackendError {
                msg: "injected start failure".to_string(),
            },
            owner,
        )
        .await;
        let (source, owner) = failure.into_parts();

        assert!(source.to_string().contains("injected start failure"));
        assert!(
            owner.is_none(),
            "successful compensation must drop ownership"
        );
        assert_eq!(
            std::fs::read_to_string(termination_marker).expect("termination marker"),
            "term"
        );
        assert!(!pid_file.exists());
    }

    fn spawn_request(root: &Path) -> SpawnRequest {
        let instance_id = Uuid::new_v4();
        let run_dir = root.join("run");
        std::fs::create_dir_all(&run_dir).expect("run dir");
        let slot_dir = root.join("slot");
        SpawnRequest {
            instance_id,
            run_dir,
            binary_path: root.join("firecracker"),
            storage: StorageSlot {
                id: instance_id.to_string(),
                rootfs_path: slot_dir.join("rootfs.ext4"),
                mem_path: slot_dir.join("mem.bin"),
                mem_diff_path: slot_dir.join("mem.diff"),
                rootfs_diff_path: slot_dir.join("rootfs.diff"),
                instance_dir: slot_dir,
            },
            backend: blaze_core::policy::BackendConfigs::default(),
            vm: None,
        }
    }
}
