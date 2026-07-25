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
use super::{BackendInstance, BackendSpawner, DynBackendInstance, SpawnResult, spawn_result};

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
            let _ = child.start_kill();
            let _ = child.wait().await;
            let _ = remove_if_exists(&pid_file).await;
            return Err(self
                .cleanup_start_failure(network.as_ref(), &network_file, error.into())
                .await);
        }
        if let Err(error) = wait_for_socket(&api_socket, &mut child, self.socket_timeout).await {
            let _ = child.start_kill();
            let _ = child.wait().await;
            let _ = remove_if_exists(&pid_file).await;
            return Err(self
                .cleanup_start_failure(network.as_ref(), &network_file, error)
                .await);
        }

        let api = FirecrackerApiClient::new(api_socket.clone(), self.api_timeout);
        if let Some(restore) = restore
            && let Err(error) = api
                .load_snapshot(&restore, network.as_ref(), &restore.interface_id)
                .await
        {
            let _ = child.start_kill();
            let _ = child.wait().await;
            let _ = remove_if_exists(&pid_file).await;
            return Err(self
                .cleanup_start_failure(network.as_ref(), &network_file, error)
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
        if !binary_path.is_file() {
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
            match child.try_wait()? {
                Some(_) => {}
                None => {
                    child.start_kill()?;
                    child.wait().await?;
                }
            }
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
                                "Firecracker {method} {path} response exceeded {} bytes",
                                MAX_API_RESPONSE_BYTES
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
    let mut boot_args = config.boot_args.clone();
    if network.is_some()
        && !boot_args
            .split_whitespace()
            .any(|arg| arg.starts_with("ip="))
    {
        boot_args.push_str(" ip=169.254.0.2::169.254.0.1:255.255.255.252::eth0:off");
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
            "iface_id": "eth0",
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
        let stdout = std::fs::File::create(run_dir.join("serial.log"))?;
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
        terminate_recorded_process(instance_id, &pid_file).await?;
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

#[cfg(target_os = "linux")]
async fn terminate_recorded_process(instance_id: Uuid, pid_file: &Path) -> Result<()> {
    let raw = match tokio::fs::read_to_string(pid_file).await {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let pid: u32 = raw
        .trim()
        .parse()
        .map_err(|error| BlazeError::BackendError {
            msg: format!(
                "invalid Firecracker pid file {}: {error}",
                pid_file.display()
            ),
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
                "refusing to kill pid {pid}: BLAZE_INSTANCE_ID does not match {instance_id}"
            ),
        });
    }
    let status = tokio::time::timeout(
        Duration::from_secs(5),
        Command::new("kill")
            .arg("-KILL")
            .arg(pid.to_string())
            .status(),
    )
    .await
    .map_err(|_| BlazeError::BackendError {
        msg: format!("timed out killing orphan Firecracker pid {pid}"),
    })??;
    if !status.success() && process_dir.exists() {
        return Err(BlazeError::BackendError {
            msg: format!("kill -KILL {pid} exited with {status}"),
        });
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while process_dir.exists() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    if process_dir.exists() {
        return Err(BlazeError::BackendError {
            msg: format!("orphan Firecracker pid {pid} did not exit"),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use http_body_util::BodyExt;
    use hyper::Response;
    use hyper::server::conn::http1 as server_http1;
    use hyper::service::service_fn;
    use tokio::net::UnixListener;
    use tokio::sync::oneshot;

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
}
