// SPDX-License-Identifier: Apache-2.0
//! Firecracker process ownership and HTTP API over Unix domain sockets.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use blaze_core::backend::{
    BackendKind, FlushResult, RestoreRequest, SnapshotKind, SnapshotRequest, SnapshotResult,
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

use super::netns::{NetworkManager, NetworkSlot};
use super::{
    BackendInstance, BackendSpawner, DynBackendInstance, SpawnResult, spawn_result,
    terminate_child, terminate_recorded_process,
};

const MAX_API_RESPONSE_BYTES: usize = 64 * 1024;

/// Firecracker backend factory.
pub struct FirecrackerSpawner {
    images_dir: PathBuf,
    api_timeout: Duration,
    socket_timeout: Duration,
    network: Arc<NetworkManager>,
    version: Mutex<Option<String>>,
}

impl FirecrackerSpawner {
    /// Create a spawner resolving the guest kernel from `images_dir`.
    pub fn new(images_dir: PathBuf) -> Self {
        Self {
            images_dir,
            api_timeout: Duration::from_secs(30),
            socket_timeout: Duration::from_secs(5),
            network: Arc::new(NetworkManager::default()),
            version: Mutex::new(None),
        }
    }

    async fn start(
        &self,
        request: SpawnRequest,
        restore: Option<RestoreParameters>,
    ) -> Result<DynBackendInstance> {
        validate_regular_file(&request.binary_path, "firecracker binary")?;
        let backend_version = match self.version.lock().await.clone() {
            Some(version) => Some(version),
            None => {
                let version = read_backend_version(&request.binary_path).await?;
                *self.version.lock().await = Some(version.clone());
                Some(version)
            }
        };
        validate_regular_file(&request.storage.rootfs_path, "rootfs")?;
        if let Some(restore) = &restore {
            validate_regular_file(&restore.snapshot_path, "snapshot")?;
            validate_regular_file(&restore.mem_path, "restore memory")?;
        } else {
            validate_regular_file(&self.images_dir.join("vmlinux"), "vmlinux")?;
        }
        tokio::fs::create_dir_all(&request.run_dir).await?;
        let api_socket = request.run_dir.join("api.sock");
        let guest_socket = request.run_dir.join("vsock.uds");
        let pid_file = request.run_dir.join("firecracker.pid");
        let network_file = request.run_dir.join("network.json");
        remove_if_exists(&api_socket).await?;
        remove_if_exists(&guest_socket).await?;
        remove_if_exists(&pid_file).await?;
        remove_if_exists(&network_file).await?;

        let network = match request.network.as_ref() {
            Some(config) if config.enabled => Some(self.network.create().await?),
            _ => None,
        };
        if let Some(network) = &network
            && let Err(error) =
                tokio::fs::write(&network_file, serde_json::to_vec_pretty(network)?).await
        {
            self.network.destroy(network).await?;
            return Err(error.into());
        }
        let fc_config = request
            .backend
            .firecracker
            .as_ref()
            .cloned()
            .unwrap_or_default();
        let mut command = build_launch_command(
            &request.binary_path,
            network.as_ref(),
            &api_socket,
            restore.is_none(),
        );

        if restore.is_none() {
            let config_path = match write_vm_config(
                &self.images_dir,
                &request,
                &fc_config,
                &guest_socket,
                network.as_ref(),
            ) {
                Ok(path) => path,
                Err(error) => {
                    return Err(self
                        .cleanup_start_failure(network.as_ref(), &network_file, error)
                        .await);
                }
            };
            command.arg("--config-file").arg(config_path);
        }
        if let Err(error) = configure_logs(&mut command, &request.run_dir, fc_config.serial_log) {
            return Err(self
                .cleanup_start_failure(network.as_ref(), &network_file, error)
                .await);
        }
        command.env("BLAZE_INSTANCE_ID", request.instance_id.to_string());
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(source) => {
                return Err(self
                    .cleanup_start_failure(network.as_ref(), &network_file, source.into())
                    .await);
            }
        };
        if let Some(pid) = child.id()
            && let Err(error) = tokio::fs::write(&pid_file, format!("{pid}\n")).await
        {
            return Err(self
                .cleanup_started_child_failure(
                    &mut child,
                    &pid_file,
                    network.as_ref(),
                    &network_file,
                    error.into(),
                )
                .await);
        }
        if let Err(error) = wait_for_socket(&api_socket, &mut child, self.socket_timeout).await {
            return Err(self
                .cleanup_started_child_failure(
                    &mut child,
                    &pid_file,
                    network.as_ref(),
                    &network_file,
                    error,
                )
                .await);
        }

        let api = FirecrackerApiClient::new(api_socket.clone(), self.api_timeout);
        if let Some(restore) = restore
            && let Err(error) = api
                .load_snapshot(&restore, network.as_ref(), &restore.interface_id)
                .await
        {
            return Err(self
                .cleanup_started_child_failure(
                    &mut child,
                    &pid_file,
                    network.as_ref(),
                    &network_file,
                    error,
                )
                .await);
        }

        let instance = FirecrackerInstance {
            instance_id: request.instance_id,
            pid: child.id(),
            child: Mutex::new(Some(child)),
            api,
            api_socket,
            guest_socket,
            pid_file,
            network: Mutex::new(network),
            network_file,
            network_manager: self.network.clone(),
            killed: AtomicBool::new(false),
            backend_version,
        };
        Ok(Arc::new(instance))
    }

    async fn cleanup_start_failure(
        &self,
        network: Option<&NetworkSlot>,
        network_file: &Path,
        original: BlazeError,
    ) -> BlazeError {
        if let Some(network) = network
            && let Err(cleanup) = self.network.destroy(network).await
        {
            return BlazeError::BackendError {
                msg: format!(
                    "Firecracker start failed ({original}); network cleanup failed ({cleanup})"
                ),
            };
        }
        if let Err(cleanup) = remove_if_exists(network_file).await {
            return BlazeError::BackendError {
                msg: format!(
                    "Firecracker start failed ({original}); metadata cleanup failed ({cleanup})"
                ),
            };
        }
        original
    }

    async fn cleanup_started_child_failure(
        &self,
        child: &mut Child,
        pid_file: &Path,
        network: Option<&NetworkSlot>,
        network_file: &Path,
        original: BlazeError,
    ) -> BlazeError {
        if let Err(cleanup) = terminate_child(child, "firecracker").await {
            return BlazeError::BackendError {
                msg: format!(
                    "Firecracker start failed ({original}); process cleanup failed ({cleanup}); network retained"
                ),
            };
        }
        let pid_cleanup = remove_if_exists(pid_file).await.err();
        let result = self
            .cleanup_start_failure(network, network_file, original)
            .await;
        if let Some(cleanup) = pid_cleanup {
            return BlazeError::BackendError {
                msg: format!("{result}; PID metadata cleanup failed ({cleanup})"),
            };
        }
        result
    }
}

#[async_trait]
impl BackendSpawner for FirecrackerSpawner {
    async fn spawn(&self, request: SpawnRequest) -> Result<DynBackendInstance> {
        self.start(request, None).await
    }

    async fn restore(&self, request: RestoreRequest) -> Result<DynBackendInstance> {
        let interface_id = request
            .spawn
            .network
            .as_ref()
            .map(|network| network.interface_id.clone())
            .unwrap_or_else(|| "eth0".to_string());
        let restore = RestoreParameters {
            snapshot_path: request.snapshot_path,
            mem_path: request.mem_path,
            track_dirty: request.track_dirty,
            interface_id,
        };
        self.start(request.spawn, Some(restore)).await
    }

    async fn probe(&self, binary_path: &Path) -> Result<bool> {
        if !binary_path.is_file() || !executable_in_path("unshare") || !executable_in_path("ip") {
            return Ok(false);
        }
        match read_backend_version(binary_path).await {
            Ok(version) => {
                *self.version.lock().await = Some(version);
                Ok(true)
            }
            Err(error) => {
                tracing::debug!(%error, binary = %binary_path.display(), "firecracker version probe failed");
                Ok(false)
            }
        }
    }

    async fn cleanup_orphan(&self, instance_id: Uuid, run_dir: &Path) -> Result<()> {
        cleanup_orphan_run_dir_with(instance_id, run_dir, &self.network).await
    }
}

struct FirecrackerInstance {
    instance_id: Uuid,
    pid: Option<u32>,
    child: Mutex<Option<Child>>,
    api: FirecrackerApiClient,
    api_socket: PathBuf,
    guest_socket: PathBuf,
    pid_file: PathBuf,
    network: Mutex<Option<NetworkSlot>>,
    network_file: PathBuf,
    network_manager: Arc<NetworkManager>,
    killed: AtomicBool,
    backend_version: Option<String>,
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
        self.backend_version.as_deref()
    }

    fn pid(&self) -> Option<u32> {
        self.pid
    }

    fn guest_socket_path(&self) -> &Path {
        &self.guest_socket
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
                self.cleanup().await?;
                return Ok(spawn_result(self.instance_id, status));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn pause(&self) -> Result<()> {
        self.api
            .call_json(
                Method::PATCH,
                "/vm",
                Some(serde_json::json!({"state": "Paused"})),
            )
            .await?;
        Ok(())
    }

    async fn resume(&self) -> Result<()> {
        self.api
            .call_json(
                Method::PATCH,
                "/vm",
                Some(serde_json::json!({"state": "Resumed"})),
            )
            .await?;
        Ok(())
    }

    async fn snapshot(&self, request: SnapshotRequest) -> Result<SnapshotResult> {
        if let Some(parent) = request.snapshot_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        if let Some(parent) = request.mem_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        self.api
            .call_json(
                Method::PUT,
                "/snapshot/create",
                Some(serde_json::json!({
                    "snapshot_path": request.snapshot_path,
                    "mem_file_path": request.mem_path,
                    "snapshot_type": match request.kind {
                        SnapshotKind::Full => "Full",
                        SnapshotKind::Diff => "Diff",
                    }
                })),
            )
            .await?;
        Ok(SnapshotResult {
            snapshot_path: request.snapshot_path,
            mem_path: request.mem_path,
        })
    }

    async fn flush_dirty(&self) -> Result<FlushResult> {
        Err(BlazeError::BackendError {
            msg: "standard Firecracker has no dirty-page flush API; use the configured storage provider"
                .to_string(),
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
        *guard = None;
        drop(guard);
        self.cleanup().await?;
        self.killed.store(true, Ordering::Release);
        Ok(())
    }
}

impl FirecrackerInstance {
    async fn cleanup(&self) -> Result<()> {
        remove_if_exists(&self.api_socket).await?;
        remove_if_exists(&self.guest_socket).await?;
        remove_if_exists(&self.pid_file).await?;
        let mut network_guard = self.network.lock().await;
        if let Some(network) = network_guard.as_ref().cloned() {
            self.network_manager.destroy(&network).await?;
            *network_guard = None;
        }
        remove_if_exists(&self.network_file).await?;
        Ok(())
    }
}

#[derive(Debug)]
struct RestoreParameters {
    snapshot_path: PathBuf,
    mem_path: PathBuf,
    track_dirty: bool,
    interface_id: String,
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

    async fn load_snapshot(
        &self,
        restore: &RestoreParameters,
        network: Option<&NetworkSlot>,
        interface_id: &str,
    ) -> Result<()> {
        let mut payload = serde_json::json!({
            "snapshot_path": restore.snapshot_path,
            "mem_backend": {
                "backend_type": "File",
                "backend_path": restore.mem_path
            },
            "enable_diff_snapshots": true,
            "resume_vm": true,
            "track_dirty_pages": restore.track_dirty
        });
        if let Some(network) = network {
            payload["network_overrides"] = serde_json::json!([{
                "iface_id": interface_id,
                "host_dev_name": network.tap_name()
            }]);
        }
        self.call_json(Method::PUT, "/snapshot/load", Some(payload))
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

fn write_vm_config(
    images_dir: &Path,
    request: &SpawnRequest,
    config: &FirecrackerConfig,
    guest_socket: &Path,
    network: Option<&NetworkSlot>,
) -> Result<PathBuf> {
    let vcpus = config
        .vcpus
        .or(request.vm.as_ref().map(|vm| vm.vcpus))
        .unwrap_or(1);
    let memory_mib = resolve_memory(config, request.vm.as_ref())?;
    let interface_id = request
        .network
        .as_ref()
        .map(|network| network.interface_id.as_str())
        .unwrap_or("eth0");
    validate_interface_id(interface_id)?;
    let mut boot_args = config.boot_args.clone();
    if network.is_some()
        && !boot_args
            .split_whitespace()
            .any(|arg| arg.starts_with("ip="))
    {
        boot_args.push_str(&format!(
            " ip=169.254.0.2::169.254.0.1:255.255.255.252::{interface_id}:off"
        ));
    }
    let mut value = serde_json::json!({
        "boot-source": {
            "kernel_image_path": path_string(&images_dir.join("vmlinux"), "vmlinux")?,
            "boot_args": boot_args
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
    if let Some(network) = network {
        value["network-interfaces"] = serde_json::json!([{
            "iface_id": interface_id,
            "guest_mac": "02:FC:00:00:00:02",
            "host_dev_name": network.tap_name()
        }]);
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

fn build_launch_command(
    binary: &Path,
    network: Option<&NetworkSlot>,
    api_socket: &Path,
    include_id: bool,
) -> Command {
    #[cfg(target_os = "linux")]
    let mut command = if let Some(network) = network {
        let mut command = Command::new("ip");
        command
            .arg("netns")
            .arg("exec")
            .arg(network.netns())
            .arg("unshare")
            .arg("--mount")
            .arg("--propagation")
            .arg("private")
            .arg("--")
            .arg(binary);
        command
    } else {
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
    let mut command = {
        let _ = network;
        Command::new(binary)
    };
    command.arg("--api-sock").arg(api_socket);
    if include_id {
        command.arg("--id").arg(format!(
            "fc-{}",
            api_socket
                .parent()
                .and_then(Path::file_name)
                .and_then(std::ffi::OsStr::to_str)
                .unwrap_or("blaze")
        ));
    }
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
    let output = tokio::time::timeout(
        Duration::from_secs(5),
        Command::new(binary_path).arg("--version").output(),
    )
    .await
    .map_err(|_| BlazeError::BackendError {
        msg: format!("firecracker probe timed out: {}", binary_path.display()),
    })??;
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
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn validate_regular_file(path: &Path, label: &str) -> Result<()> {
    if !path.is_file() {
        return Err(BlazeError::BackendError {
            msg: format!("{label} not found at {}", path.display()),
        });
    }
    Ok(())
}

fn validate_interface_id(interface_id: &str) -> Result<()> {
    if interface_id.is_empty()
        || interface_id.len() > 64
        || !interface_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(BlazeError::BackendError {
            msg: format!("invalid Firecracker interface id {interface_id:?}"),
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
    cleanup_orphan_run_dir_with(instance_id, run_dir, &NetworkManager::default()).await
}

async fn cleanup_orphan_run_dir_with(
    instance_id: Uuid,
    run_dir: &Path,
    network_manager: &NetworkManager,
) -> Result<()> {
    let pid_file = run_dir.join("firecracker.pid");
    #[cfg(target_os = "linux")]
    {
        terminate_recorded_process(instance_id, &pid_file, "firecracker").await?;
    }
    #[cfg(not(target_os = "linux"))]
    if pid_file.exists() {
        return Err(BlazeError::BackendError {
            msg: format!(
                "cannot validate Firecracker orphan {} outside Linux",
                pid_file.display()
            ),
        });
    }

    let network_file = run_dir.join("network.json");
    if network_file.is_file() {
        let network: NetworkSlot = serde_json::from_slice(&tokio::fs::read(&network_file).await?)
            .map_err(|error| BlazeError::BackendError {
            msg: format!(
                "parse orphan network metadata {}: {error}",
                network_file.display()
            ),
        })?;
        network_manager.destroy(&network).await?;
        remove_if_exists(&network_file).await?;
    }
    remove_if_exists(&run_dir.join("api.sock")).await?;
    remove_if_exists(&run_dir.join("vsock.uds")).await?;
    remove_if_exists(&pid_file).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::convert::Infallible;
    use std::sync::Mutex as StdMutex;

    use async_trait::async_trait;
    use blaze_core::backend::NetworkConfig;
    use blaze_core::storage::StorageSlot;
    use http_body_util::BodyExt;
    use hyper::Response;
    use hyper::server::conn::http1 as server_http1;
    use hyper::service::service_fn;
    use tokio::net::UnixListener;
    use tokio::sync::oneshot;

    use crate::spawner::netns::{IpCommandRunner, IpOutput, test_network_slot};

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

    async fn spawn_api(
        socket: &Path,
        status: hyper::StatusCode,
        response_body: Vec<u8>,
    ) -> oneshot::Receiver<(Method, String, serde_json::Value)> {
        let listener = UnixListener::bind(socket).expect("bind");
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let tx = Arc::new(Mutex::new(Some(tx)));
            let service = service_fn(move |request: Request<hyper::body::Incoming>| {
                let tx = tx.clone();
                let response_body = response_body.clone();
                async move {
                    let method = request.method().clone();
                    let path = request.uri().path().to_string();
                    let body = request
                        .into_body()
                        .collect()
                        .await
                        .expect("body")
                        .to_bytes();
                    let body = if body.is_empty() {
                        serde_json::Value::Null
                    } else {
                        serde_json::from_slice(&body).expect("json")
                    };
                    if let Some(tx) = tx.lock().await.take() {
                        let _ = tx.send((method, path, body));
                    }
                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(status)
                            .body(Full::new(Bytes::from(response_body)))
                            .expect("response"),
                    )
                }
            });
            server_http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await
                .expect("serve");
        });
        rx
    }

    #[tokio::test]
    async fn uds_client_preserves_method_path_and_json() {
        let temp = tempfile::tempdir().expect("temp");
        let socket = temp.path().join("api.sock");
        let observed = spawn_api(&socket, hyper::StatusCode::NO_CONTENT, Vec::new()).await;
        let client = FirecrackerApiClient::new(socket, Duration::from_secs(1));
        client
            .call_json(
                Method::PATCH,
                "/vm",
                Some(serde_json::json!({"state": "Paused"})),
            )
            .await
            .expect("call");
        let (method, path, body) = observed.await.expect("observed");
        assert_eq!(method, Method::PATCH);
        assert_eq!(path, "/vm");
        assert_eq!(body, serde_json::json!({"state": "Paused"}));
    }

    #[tokio::test]
    async fn non_success_keeps_status_and_bounded_body() {
        let temp = tempfile::tempdir().expect("temp");
        let socket = temp.path().join("api.sock");
        let _observed = spawn_api(
            &socket,
            hyper::StatusCode::BAD_REQUEST,
            b"invalid snapshot".to_vec(),
        )
        .await;
        let error = FirecrackerApiClient::new(socket, Duration::from_secs(1))
            .call_json(Method::PUT, "/snapshot/load", None)
            .await
            .expect_err("non-success");
        let message = error.to_string();
        assert!(message.contains("400 Bad Request"));
        assert!(message.contains("invalid snapshot"));
    }

    #[tokio::test]
    async fn api_timeout_is_bounded() {
        let temp = tempfile::tempdir().expect("temp");
        let socket = temp.path().join("api.sock");
        let listener = UnixListener::bind(&socket).expect("bind");
        tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.expect("accept");
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        let error = FirecrackerApiClient::new(socket, Duration::from_millis(10))
            .call_json(Method::GET, "/", None)
            .await
            .expect_err("timeout");
        assert!(error.to_string().contains("timed out"));
    }

    #[tokio::test]
    async fn oversized_api_response_is_rejected() {
        let temp = tempfile::tempdir().expect("temp");
        let socket = temp.path().join("api.sock");
        let _observed = spawn_api(
            &socket,
            hyper::StatusCode::OK,
            vec![b'x'; MAX_API_RESPONSE_BYTES + 1],
        )
        .await;
        let error = FirecrackerApiClient::new(socket, Duration::from_secs(1))
            .call_json(Method::GET, "/", None)
            .await
            .expect_err("oversized");
        assert!(error.to_string().contains("exceeded"));
    }

    #[test]
    fn vm_config_uses_requested_interface_id_consistently() {
        let temp = tempfile::tempdir().expect("temp");
        let mut request = spawn_request(temp.path());
        request.network = Some(NetworkConfig {
            enabled: true,
            interface_id: "ens5".to_string(),
        });
        let network = test_network_slot(0);

        let path = write_vm_config(
            &temp.path().join("images"),
            &request,
            &FirecrackerConfig::default(),
            &temp.path().join("guest.sock"),
            Some(&network),
        )
        .expect("write config");
        let value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).expect("read config"))
                .expect("parse config");

        assert_eq!(value["network-interfaces"][0]["iface_id"], "ens5");
        assert!(
            value["boot-source"]["boot_args"]
                .as_str()
                .expect("boot args")
                .contains("::ens5:off")
        );
    }

    #[test]
    fn vm_config_rejects_invalid_interface_id() {
        let temp = tempfile::tempdir().expect("temp");
        let mut request = spawn_request(temp.path());
        request.network = Some(NetworkConfig {
            enabled: true,
            interface_id: "../eth0".to_string(),
        });

        let error = write_vm_config(
            &temp.path().join("images"),
            &request,
            &FirecrackerConfig::default(),
            &temp.path().join("guest.sock"),
            None,
        )
        .expect_err("invalid interface id");

        assert!(
            error
                .to_string()
                .contains("invalid Firecracker interface id")
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
    async fn start_failure_terminates_child_then_releases_network_and_metadata() {
        let runner = Arc::new(RecordingIpRunner::default());
        let network_manager = Arc::new(NetworkManager::with_runner(runner.clone()));
        let network = network_manager.create().await.expect("network");
        let temp = tempfile::tempdir().expect("temp");
        let network_file = temp.path().join("network.json");
        let pid_file = temp.path().join("firecracker.pid");
        let termination_marker = temp.path().join("terminated");
        std::fs::write(
            &network_file,
            serde_json::to_vec(&network).expect("network json"),
        )
        .expect("network metadata");
        let mut child = Command::new("sh")
            .arg("-c")
            .arg("trap 'printf term > \"$MARKER\"; exit 0' TERM; while :; do sleep 1; done")
            .env("MARKER", &termination_marker)
            .spawn()
            .expect("spawn child");
        std::fs::write(&pid_file, format!("{}\n", child.id().expect("child pid")))
            .expect("pid metadata");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let spawner = FirecrackerSpawner {
            images_dir: temp.path().join("images"),
            api_timeout: Duration::from_secs(1),
            socket_timeout: Duration::from_secs(1),
            network: network_manager,
            version: Mutex::new(None),
        };

        let returned = spawner
            .cleanup_started_child_failure(
                &mut child,
                &pid_file,
                Some(&network),
                &network_file,
                BlazeError::BackendError {
                    msg: "injected start failure".to_string(),
                },
            )
            .await;

        assert!(returned.to_string().contains("injected start failure"));
        assert_eq!(
            std::fs::read_to_string(termination_marker).expect("termination marker"),
            "term"
        );
        assert!(!pid_file.exists());
        assert!(!network_file.exists());
        let calls = runner.calls();
        assert_eq!(&calls[calls.len() - 2], &["link", "del", "blz-veth-0"]);
        assert_eq!(&calls[calls.len() - 1], &["netns", "del", "blz-ns-0"]);
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
            network: None,
        }
    }

    #[derive(Default)]
    struct RecordingIpRunner {
        responses: StdMutex<VecDeque<IpOutput>>,
        calls: StdMutex<Vec<Vec<String>>>,
    }

    impl RecordingIpRunner {
        fn calls(&self) -> Vec<Vec<String>> {
            self.calls.lock().expect("calls lock").clone()
        }
    }

    #[async_trait]
    impl IpCommandRunner for RecordingIpRunner {
        async fn output(&self, args: &[String], _timeout: Duration) -> Result<IpOutput> {
            self.calls.lock().expect("calls lock").push(args.to_vec());
            Ok(self
                .responses
                .lock()
                .expect("responses lock")
                .pop_front()
                .unwrap_or_else(|| IpOutput {
                    success: true,
                    status: "exit status: 0".to_string(),
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                }))
        }
    }
}
