// SPDX-License-Identifier: Apache-2.0
//! Firecracker process ownership and HTTP API over Unix domain sockets.

use std::io::Write;
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

use super::netns::{NetworkManager, NetworkSlot};
#[cfg(target_os = "linux")]
use super::terminate_recorded_process;
use super::{
    BackendInstance, BackendSpawner, DynBackendInstance, RestoreResult, SpawnFailure, SpawnResult,
    configure_pid_handoff, prepare_pid_handoff, record_backend_stopped, remove_file_if_exists,
    spawn_result, stopped_marker, terminate_child,
};

const NETWORK_BOOT_IP: &str = "ip=169.254.0.2::169.254.0.1:255.255.255.252::eth0:off";
const MAX_API_RESPONSE_BYTES: usize = 64 * 1024;

/// Firecracker backend factory.
pub struct FirecrackerSpawner {
    images_dir: PathBuf,
    api_timeout: Duration,
    socket_timeout: Duration,
    network: Arc<NetworkManager>,
    network_required: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum NetworkProcessState {
    PreSpawn,
    #[default]
    Launching,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct NetworkRecord {
    slot: usize,
    owner: Uuid,
    #[serde(default)]
    process_state: NetworkProcessState,
}

impl FirecrackerSpawner {
    /// Create a spawner without requiring host networking during startup
    /// probing. Individual network-enabled requests still run the full probe.
    pub fn new(images_dir: PathBuf) -> Self {
        Self {
            images_dir,
            api_timeout: Duration::from_secs(30),
            socket_timeout: Duration::from_secs(5),
            network: Arc::new(NetworkManager::default()),
            network_required: false,
        }
    }

    /// Create a spawner whose startup probe includes network prerequisites
    /// when at least one loaded policy enables Firecracker networking.
    pub fn with_network_requirement(images_dir: PathBuf, network_required: bool) -> Self {
        Self {
            network_required,
            ..Self::new(images_dir)
        }
    }

    async fn network_probe_ready(&self) -> Result<bool> {
        if !self.network_required {
            return Ok(true);
        }
        self.network.probe().await
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
        let network_file = request.run_dir.join("network.json");
        let network_temp_file = network_metadata_temp(&network_file);
        remove_if_exists(&api_socket).await?;
        remove_if_exists(&guest_socket).await?;
        remove_file_if_exists(&stopped_marker).await?;
        remove_if_exists(&network_file).await?;
        remove_if_exists(&network_temp_file).await?;
        let fc_config = request
            .backend
            .firecracker
            .as_ref()
            .cloned()
            .unwrap_or_default();
        let expose_guest_socket = restore.as_ref().map_or(fc_config.enable_vsock, |restore| {
            restore.expose_guest_socket
        });
        let network = if fc_config.enable_network {
            if !self.network.probe().await? {
                return Err(BlazeError::BackendError {
                    msg: "Firecracker networking is unavailable; it requires Linux root and executable ip, sysctl, and iptables commands".to_string(),
                }
                .into());
            }
            let created = match restore.as_ref().and_then(|restore| restore.network_slot) {
                Some(slot) => {
                    self.network
                        .create_at(request.instance_id, slot, |slot| {
                            write_network_metadata(&network_file, slot)
                        })
                        .await
                }
                None => {
                    self.network
                        .create(request.instance_id, |slot| {
                            write_network_metadata(&network_file, slot)
                        })
                        .await
                }
            };
            match created {
                Ok(network) => Some(network),
                Err(error) => {
                    let (source, residual) = error.into_parts();
                    if let Some(network) = residual {
                        let owner: DynBackendInstance = Arc::new(FirecrackerInstance::new(
                            request.instance_id,
                            None,
                            capture.clone(),
                            runtime_files(
                                api_socket,
                                guest_socket,
                                pid_file,
                                stopped_marker,
                                network_file,
                            ),
                            Some(network),
                            self.network.clone(),
                            expose_guest_socket,
                        ));
                        return Err(SpawnFailure::compensate_started(source, owner).await);
                    }
                    if let Err(cleanup) = remove_if_exists(&network_file).await {
                        let owner: DynBackendInstance = Arc::new(FirecrackerInstance::new(
                            request.instance_id,
                            None,
                            capture.clone(),
                            runtime_files(
                                api_socket,
                                guest_socket,
                                pid_file,
                                stopped_marker,
                                network_file,
                            ),
                            None,
                            self.network.clone(),
                            expose_guest_socket,
                        ));
                        return Err(SpawnFailure::compensate_started(
                            BlazeError::BackendError {
                                msg: format!(
                                    "{source}; network metadata cleanup failed: {cleanup}"
                                ),
                            },
                            owner,
                        )
                        .await);
                    }
                    if let Err(cleanup) = remove_if_exists(&network_temp_file).await {
                        let owner: DynBackendInstance = Arc::new(FirecrackerInstance::new(
                            request.instance_id,
                            None,
                            capture.clone(),
                            runtime_files(
                                api_socket,
                                guest_socket,
                                pid_file,
                                stopped_marker,
                                network_file,
                            ),
                            None,
                            self.network.clone(),
                            expose_guest_socket,
                        ));
                        return Err(SpawnFailure::compensate_started(
                            BlazeError::BackendError {
                                msg: format!(
                                    "{source}; temporary network metadata cleanup failed: {cleanup}"
                                ),
                            },
                            owner,
                        )
                        .await);
                    }
                    return Err(source.into());
                }
            }
        } else {
            None
        };

        let mut command = build_launch_command(&request.binary_path, network.as_ref(), &api_socket);
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
                        .compensate_before_spawn(
                            request.instance_id,
                            capture.clone(),
                            runtime_files(
                                api_socket,
                                guest_socket,
                                pid_file,
                                stopped_marker,
                                network_file,
                            ),
                            network,
                            expose_guest_socket,
                            error,
                        )
                        .await);
                }
            };
            command.arg("--config-file").arg(config_path);
        }
        if let Err(error) = configure_logs(&mut command, &request.run_dir, fc_config.serial_log) {
            return Err(self
                .compensate_before_spawn(
                    request.instance_id,
                    capture.clone(),
                    runtime_files(
                        api_socket,
                        guest_socket,
                        pid_file,
                        stopped_marker,
                        network_file,
                    ),
                    network,
                    expose_guest_socket,
                    error,
                )
                .await);
        }
        command.env("BLAZE_INSTANCE_ID", request.instance_id.to_string());
        if let Some(slot) = network.as_ref()
            && let Err(error) =
                write_network_record(&network_file, slot, NetworkProcessState::Launching)
        {
            return Err(self
                .compensate_before_spawn(
                    request.instance_id,
                    capture.clone(),
                    runtime_files(
                        api_socket,
                        guest_socket,
                        pid_file,
                        stopped_marker,
                        network_file,
                    ),
                    network,
                    expose_guest_socket,
                    error,
                )
                .await);
        }
        let pid_handoff = match configure_pid_handoff(&mut command, &pid_file) {
            Ok(pid_handoff) => pid_handoff,
            Err(error) => {
                return Err(self
                    .compensate_before_spawn(
                        request.instance_id,
                        capture.clone(),
                        runtime_files(
                            api_socket,
                            guest_socket,
                            pid_file,
                            stopped_marker,
                            network_file,
                        ),
                        network,
                        expose_guest_socket,
                        error,
                    )
                    .await);
            }
        };
        let child = command.spawn();
        drop(pid_handoff);
        let mut child = match child {
            Ok(child) => child,
            Err(source) => {
                return Err(self
                    .compensate_before_spawn(
                        request.instance_id,
                        capture.clone(),
                        runtime_files(
                            api_socket,
                            guest_socket,
                            pid_file,
                            stopped_marker,
                            network_file,
                        ),
                        network,
                        expose_guest_socket,
                        source.into(),
                    )
                    .await);
            }
        };
        if let Err(error) = wait_for_socket(&api_socket, &mut child, self.socket_timeout).await {
            let owner: DynBackendInstance = Arc::new(FirecrackerInstance::new(
                request.instance_id,
                Some(child),
                capture,
                runtime_files(
                    api_socket,
                    guest_socket,
                    pid_file,
                    stopped_marker,
                    network_file,
                ),
                network,
                self.network.clone(),
                expose_guest_socket,
            ));
            return Err(SpawnFailure::compensate_started(error, owner).await);
        }

        let instance = Arc::new(FirecrackerInstance::new(
            request.instance_id,
            Some(child),
            capture,
            runtime_files(
                api_socket,
                guest_socket,
                pid_file,
                stopped_marker,
                network_file,
            ),
            network,
            self.network.clone(),
            expose_guest_socket,
        ));
        if let Some(restore) = restore
            && let Err(error) = instance.load_snapshot(&restore).await
        {
            let owner: DynBackendInstance = instance;
            return Err(SpawnFailure::compensate_started(error, owner).await);
        }
        Ok(instance)
    }

    async fn compensate_before_spawn(
        &self,
        instance_id: Uuid,
        capture: FirecrackerCapture,
        files: FirecrackerRuntimeFiles,
        network: Option<NetworkSlot>,
        enable_vsock: bool,
        source: BlazeError,
    ) -> SpawnFailure {
        if network.is_none() {
            return SpawnFailure::clean(source);
        }
        let owner: DynBackendInstance = Arc::new(FirecrackerInstance::new(
            instance_id,
            None,
            capture,
            files,
            network,
            self.network.clone(),
            enable_vsock,
        ));
        SpawnFailure::compensate_started(source, owner).await
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
            network_slot,
        } = request;
        let backend = blaze_core::policy::BackendConfigs {
            firecracker: Some(blaze_core::policy::FirecrackerConfig {
                enable_vsock: expose_guest_socket,
                enable_network: network_slot.is_some(),
                ..blaze_core::policy::FirecrackerConfig::default()
            }),
        };
        self.start(
            SpawnRequest {
                instance_id,
                run_dir,
                binary_path,
                storage,
                // Snapshot restore only reconstructs host-side resources that
                // are required to make the captured runtime reachable.
                backend,
                vm: None,
            },
            Some(FirecrackerRestore {
                snapshot_path,
                mem_path,
                backend: checkpoint_backend,
                expected_version,
                snapshot_kind,
                expose_guest_socket,
                network_slot,
            }),
        )
        .await
    }

    async fn probe(&self, binary_path: &Path) -> Result<bool> {
        if !binary_path.is_file() || !executable_in_path("unshare") {
            return Ok(false);
        }
        if !self.network_probe_ready().await? {
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
        cleanup_orphan_run_dir_with(instance_id, run_dir, &self.network).await
    }
}

struct FirecrackerInstance {
    instance_id: Uuid,
    child: Mutex<Option<Child>>,
    exit_result: Mutex<Option<SpawnResult>>,
    capture: FirecrackerCapture,
    files: FirecrackerRuntimeFiles,
    guest_socket: PathBuf,
    network: Mutex<Option<NetworkSlot>>,
    network_slot: Option<usize>,
    network_manager: Arc<NetworkManager>,
    cleanup_complete: AtomicBool,
    killed: AtomicBool,
}

struct FirecrackerRuntimeFiles {
    api_socket: PathBuf,
    guest_socket: PathBuf,
    pid_file: PathBuf,
    stopped_marker: PathBuf,
    network_file: PathBuf,
}

fn runtime_files(
    api_socket: PathBuf,
    guest_socket: PathBuf,
    pid_file: PathBuf,
    stopped_marker: PathBuf,
    network_file: PathBuf,
) -> FirecrackerRuntimeFiles {
    FirecrackerRuntimeFiles {
        api_socket,
        guest_socket,
        pid_file,
        stopped_marker,
        network_file,
    }
}

impl FirecrackerInstance {
    fn new(
        instance_id: Uuid,
        child: Option<Child>,
        capture: FirecrackerCapture,
        files: FirecrackerRuntimeFiles,
        network: Option<NetworkSlot>,
        network_manager: Arc<NetworkManager>,
        enable_vsock: bool,
    ) -> Self {
        let guest_socket = configured_guest_socket(enable_vsock, files.guest_socket.clone());
        let network_slot = network.as_ref().map(NetworkSlot::slot);
        Self {
            instance_id,
            child: Mutex::new(child),
            exit_result: Mutex::new(None),
            capture,
            files,
            guest_socket,
            network: Mutex::new(network),
            network_slot,
            network_manager,
            cleanup_complete: AtomicBool::new(false),
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

    fn network_slot(&self) -> Option<usize> {
        self.network_slot
    }

    async fn try_wait(&self) -> Result<Option<SpawnResult>> {
        let result = {
            let mut guard = self.child.lock().await;
            let Some(child) = guard.as_mut() else {
                let result = self.exit_result.lock().await.unwrap_or(SpawnResult {
                    instance_id: self.instance_id,
                    exit_code: None,
                    signal: None,
                });
                drop(guard);
                self.cleanup().await?;
                return Ok(Some(result));
            };
            let Some(status) = child.try_wait()? else {
                return Ok(None);
            };
            record_backend_stopped(&self.files.stopped_marker).await?;
            let result = spawn_result(self.instance_id, status);
            *self.exit_result.lock().await = Some(result);
            *guard = None;
            result
        };
        self.cleanup().await?;
        Ok(Some(result))
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
        record_backend_stopped(&self.files.stopped_marker).await?;
        *guard = None;
        drop(guard);
        self.cleanup().await?;
        self.killed.store(true, Ordering::Release);
        Ok(())
    }
}

#[derive(Clone)]
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
    network_slot: Option<usize>,
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
        if self.cleanup_complete.load(Ordering::Acquire) {
            return Ok(());
        }
        remove_if_exists(&self.files.api_socket).await?;
        remove_if_exists(&self.files.guest_socket).await?;
        remove_if_exists(&self.files.pid_file).await?;
        let mut network = self.network.lock().await;
        if let Some(slot) = network.as_ref().cloned() {
            self.network_manager.destroy(&slot).await?;
            *network = None;
        }
        remove_if_exists(&self.files.network_file).await?;
        remove_if_exists(&network_metadata_temp(&self.files.network_file)).await?;
        self.cleanup_complete.store(true, Ordering::Release);
        Ok(())
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
    if network.is_some() {
        let network_arguments = boot_args
            .split_whitespace()
            .filter(|argument| argument.starts_with("ip="))
            .collect::<Vec<_>>();
        match network_arguments.as_slice() {
            [] => {
                boot_args.push(' ');
                boot_args.push_str(NETWORK_BOOT_IP);
            }
            [argument] if *argument == NETWORK_BOOT_IP => {}
            arguments => {
                return Err(BlazeError::BackendError {
                    msg: format!(
                        "Firecracker networking requires exactly {NETWORK_BOOT_IP:?}, found {}",
                        arguments
                            .iter()
                            .map(|argument| format!("{argument:?}"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                });
            }
        }
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

fn write_network_metadata(path: &Path, network: &NetworkSlot) -> Result<()> {
    write_network_record(path, network, NetworkProcessState::PreSpawn)
}

fn write_network_record(
    path: &Path,
    network: &NetworkSlot,
    process_state: NetworkProcessState,
) -> Result<()> {
    let parent = path.parent().ok_or_else(|| BlazeError::BackendError {
        msg: format!("network metadata has no parent: {}", path.display()),
    })?;
    let temporary = network_metadata_temp(path);
    (|| -> Result<()> {
        let bytes = serde_json::to_vec_pretty(&NetworkRecord {
            slot: network.slot(),
            owner: network.owner(),
            process_state,
        })
        .map_err(|error| BlazeError::BackendError {
            msg: format!("serialize network metadata: {error}"),
        })?;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(&temporary, path)?;
        std::fs::File::open(parent)?.sync_all()?;
        Ok(())
    })()
}

fn read_network_metadata(path: &Path) -> Result<(NetworkSlot, NetworkProcessState)> {
    let record: NetworkRecord = serde_json::from_slice(&std::fs::read(path)?).map_err(|error| {
        BlazeError::BackendError {
            msg: format!("parse network metadata {}: {error}", path.display()),
        }
    })?;
    Ok((
        NetworkSlot::from_record(record.slot, record.owner)?,
        record.process_state,
    ))
}

fn network_metadata_temp(path: &Path) -> PathBuf {
    path.with_extension("json.tmp")
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

async fn cleanup_orphan_run_dir_with(
    instance_id: Uuid,
    run_dir: &Path,
    network_manager: &NetworkManager,
) -> Result<()> {
    let stopped_marker = stopped_marker(run_dir);
    let pid_file = run_dir.join("firecracker.pid");
    let network_file = run_dir.join("network.json");
    let network_temp_file = network_metadata_temp(&network_file);
    let record_path = if network_file.is_file() {
        Some(network_file.as_path())
    } else if network_temp_file.is_file() {
        Some(network_temp_file.as_path())
    } else {
        None
    };
    let network_record = match record_path {
        Some(path) => match read_network_metadata(path) {
            Ok((network, state)) => {
                if network.owner() != instance_id {
                    return Err(BlazeError::BackendError {
                        msg: format!(
                            "network record owner {} does not match instance {instance_id}",
                            network.owner()
                        ),
                    });
                }
                Some((network, Some(state)))
            }
            Err(error) if path == network_temp_file.as_path() && !network_file.exists() => {
                match network_manager.find_by_owner(instance_id).await? {
                    // The namespace name proves ownership, but it cannot prove
                    // whether the backend crossed the spawn boundary.
                    Some(network) => Some((network, None)),
                    None => return Err(error),
                }
            }
            Err(error) => return Err(error),
        },
        None => network_manager
            .find_by_owner(instance_id)
            .await?
            .map(|network| (network, None)),
    };
    let process_may_exist = pid_file.exists()
        || network_record
            .as_ref()
            .is_none_or(|(_, state)| *state != Some(NetworkProcessState::PreSpawn));
    if !stopped_marker.is_file() {
        #[cfg(target_os = "linux")]
        {
            if process_may_exist {
                terminate_recorded_process(instance_id, &pid_file, "firecracker").await?;
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = instance_id;
            if process_may_exist {
                return Err(BlazeError::BackendError {
                    msg: format!(
                        "cannot validate Firecracker orphan {} outside Linux",
                        pid_file.display()
                    ),
                });
            }
        }
        record_backend_stopped(&stopped_marker).await?;
    }

    if let Some((network, _)) = network_record {
        network_manager.destroy(&network).await?;
        remove_if_exists(&network_file).await?;
    }
    remove_if_exists(&network_temp_file).await?;
    remove_if_exists(&run_dir.join("api.sock")).await?;
    remove_if_exists(&run_dir.join("vsock.uds")).await?;
    remove_if_exists(&pid_file).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
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

    use crate::spawner::netns::{IpCommandRunner, IpOutput, NetworkManager, test_network_slot};

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
            network_slot: None,
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

    #[test]
    fn instance_preserves_its_network_slot_for_restore() {
        let temp = tempfile::tempdir().expect("temp");
        let api_socket = temp.path().join("api.sock");
        let instance = FirecrackerInstance::new(
            Uuid::new_v4(),
            None,
            FirecrackerCapture::new(
                api_socket.clone(),
                Duration::from_secs(1),
                "Firecracker v1.16.0".to_string(),
            ),
            runtime_files(
                api_socket,
                temp.path().join("vsock.uds"),
                temp.path().join("firecracker.pid"),
                stopped_marker(temp.path()),
                temp.path().join("network.json"),
            ),
            Some(test_network_slot(7)),
            Arc::new(NetworkManager::default()),
            false,
        );

        assert_eq!(instance.network_slot(), Some(7));
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
                network_slot: None,
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
            Some(child),
            FirecrackerCapture::new(
                api_socket.clone(),
                Duration::from_secs(1),
                "Firecracker v1.16.0".to_string(),
            ),
            runtime_files(
                api_socket,
                PathBuf::new(),
                temp.path().join("firecracker.pid"),
                stopped_marker(temp.path()),
                temp.path().join("network.json"),
            ),
            None,
            Arc::new(NetworkManager::default()),
            false,
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
                network_slot: None,
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
            Some(child),
            FirecrackerCapture::new(
                api_socket.clone(),
                Duration::from_secs(1),
                "Firecracker v1.16.0".to_string(),
            ),
            runtime_files(
                api_socket,
                guest_socket.clone(),
                temp.path().join("firecracker.pid"),
                stopped_marker(temp.path()),
                temp.path().join("network.json"),
            ),
            None,
            Arc::new(NetworkManager::default()),
            true,
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
                network_slot: None,
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
            Some(child),
            FirecrackerCapture::new(
                api_socket.clone(),
                Duration::from_secs(1),
                "Firecracker v1.16.0".to_string(),
            ),
            runtime_files(
                api_socket,
                guest_socket.clone(),
                temp.path().join("firecracker.pid"),
                stopped_marker(temp.path()),
                temp.path().join("network.json"),
            ),
            None,
            Arc::new(NetworkManager::default()),
            true,
        ));
        let restore = FirecrackerRestore {
            snapshot_path: temp.path().join("vmstate.snap"),
            mem_path: temp.path().join("memory.snap"),
            backend: BackendKind::Firecracker,
            expected_version: Some("Firecracker v1.16.0".to_string()),
            snapshot_kind: SnapshotKind::Full,
            expose_guest_socket: true,
            network_slot: None,
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
            Some(child),
            FirecrackerCapture::new(
                api_socket.clone(),
                Duration::from_secs(1),
                "Firecracker v1.16.0".to_string(),
            ),
            runtime_files(
                api_socket,
                PathBuf::new(),
                temp.path().join("firecracker.pid"),
                stopped_marker(temp.path()),
                temp.path().join("network.json"),
            ),
            None,
            Arc::new(NetworkManager::default()),
            false,
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
            None,
        )
        .expect("write config");
        let value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).expect("read config"))
                .expect("parse config");
        assert!(value.get("network-interfaces").is_none());
    }

    #[test]
    fn vm_config_wires_an_allocated_network_slot() {
        let temp = tempfile::tempdir().expect("temp");
        let request = spawn_request(temp.path());
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
        assert_eq!(value["network-interfaces"][0]["iface_id"], "eth0");
        assert_eq!(value["network-interfaces"][0]["host_dev_name"], "tap0");
        assert!(
            value["boot-source"]["boot_args"]
                .as_str()
                .expect("boot args")
                .contains("::eth0:off")
        );
    }

    #[test]
    fn vm_config_accepts_the_matching_network_boot_argument() {
        let temp = tempfile::tempdir().expect("temp");
        let request = spawn_request(temp.path());
        let network = test_network_slot(0);
        let config = FirecrackerConfig {
            boot_args: format!("console=ttyS0 {NETWORK_BOOT_IP}"),
            ..FirecrackerConfig::default()
        };

        write_vm_config(
            &temp.path().join("images"),
            &request,
            &config,
            &temp.path().join("guest.sock"),
            Some(&network),
        )
        .expect("matching network boot argument");
    }

    #[test]
    fn vm_config_rejects_an_incompatible_network_boot_argument() {
        let temp = tempfile::tempdir().expect("temp");
        let request = spawn_request(temp.path());
        let network = test_network_slot(0);
        let config = FirecrackerConfig {
            boot_args: "console=ttyS0 ip=dhcp".to_string(),
            ..FirecrackerConfig::default()
        };

        let error = write_vm_config(
            &temp.path().join("images"),
            &request,
            &config,
            &temp.path().join("guest.sock"),
            Some(&network),
        )
        .expect_err("incompatible network boot argument");

        assert!(error.to_string().contains("requires"));
        assert!(error.to_string().contains("ip=dhcp"));
    }

    #[test]
    fn vm_config_rejects_conflicting_network_boot_arguments() {
        let temp = tempfile::tempdir().expect("temp");
        let request = spawn_request(temp.path());
        let network = test_network_slot(0);
        let config = FirecrackerConfig {
            boot_args: format!("console=ttyS0 {NETWORK_BOOT_IP} ip=dhcp"),
            ..FirecrackerConfig::default()
        };

        let error = write_vm_config(
            &temp.path().join("images"),
            &request,
            &config,
            &temp.path().join("guest.sock"),
            Some(&network),
        )
        .expect_err("conflicting network boot arguments");

        assert!(error.to_string().contains("exactly"));
        assert!(error.to_string().contains("ip=dhcp"));
    }

    #[test]
    fn network_metadata_is_published_atomically() {
        let temp = tempfile::tempdir().expect("temp");
        let path = temp.path().join("network.json");
        let slot = test_network_slot(7);

        write_network_metadata(&path, &slot).expect("write metadata");

        let (stored, state) = read_network_metadata(&path).expect("parse metadata");
        assert_eq!(stored, slot);
        assert_eq!(state, NetworkProcessState::PreSpawn);
        assert!(!network_metadata_temp(&path).exists());
    }

    #[test]
    fn network_metadata_records_launch_intent_before_spawn() {
        let temp = tempfile::tempdir().expect("temp");
        let path = temp.path().join("network.json");
        let slot = test_network_slot(7);
        write_network_metadata(&path, &slot).expect("write pre-spawn metadata");

        write_network_record(&path, &slot, NetworkProcessState::Launching)
            .expect("record launch intent");

        let (stored, state) = read_network_metadata(&path).expect("parse metadata");
        assert_eq!(stored, slot);
        assert_eq!(state, NetworkProcessState::Launching);
        assert!(!network_metadata_temp(&path).exists());
    }

    #[test]
    fn network_metadata_rejects_out_of_range_slots() {
        let temp = tempfile::tempdir().expect("temp");
        let path = temp.path().join("network.json");
        std::fs::write(
            &path,
            br#"{"slot":16383,"owner":"00000000-0000-0000-0000-000000000001"}"#,
        )
        .expect("metadata");

        let error = read_network_metadata(&path).expect_err("invalid slot");

        assert!(error.to_string().contains("outside"));
    }

    #[tokio::test]
    async fn network_cleanup_failure_retains_a_retryable_backend_owner() {
        let temp = tempfile::tempdir().expect("temp");
        let network_file = temp.path().join("network.json");
        let slot = test_network_slot(0);
        write_network_metadata(&network_file, &slot).expect("network metadata");
        let namespace = format!("{}\n", slot.netns());
        let runner = Arc::new(TestIpRunner::with_responses([
            ip_success(namespace.as_bytes()),
            ip_failure("delete peer failed"),
            ip_success(namespace.as_bytes()),
            ip_success(b""),
            ip_success(b""),
        ]));
        let network_manager = Arc::new(NetworkManager::with_runner(runner.clone()));
        let owner: DynBackendInstance = Arc::new(FirecrackerInstance::new(
            slot.owner(),
            None,
            FirecrackerCapture::new(
                temp.path().join("api.sock"),
                Duration::from_secs(1),
                "Firecracker v1.16.0".to_string(),
            ),
            runtime_files(
                temp.path().join("api.sock"),
                temp.path().join("guest.sock"),
                temp.path().join("firecracker.pid"),
                stopped_marker(temp.path()),
                network_file.clone(),
            ),
            Some(slot.clone()),
            network_manager,
            false,
        ));

        owner.kill().await.expect_err("first cleanup must fail");
        assert!(network_file.exists());
        owner.kill().await.expect("retry cleanup");
        assert!(!network_file.exists());
        assert!(
            runner
                .calls()
                .iter()
                .any(|args| args == &["netns", "del", slot.netns()])
        );
        assert!(
            !runner
                .calls()
                .iter()
                .any(|args| args == &["link", "del", "blz-veth-0"])
        );
    }

    #[tokio::test]
    async fn try_wait_retries_cleanup_after_observing_process_exit() {
        let temp = tempfile::tempdir().expect("temp");
        let network_file = temp.path().join("network.json");
        let slot = test_network_slot(0);
        write_network_metadata(&network_file, &slot).expect("network metadata");
        let namespace = format!("{}\n", slot.netns());
        let runner = Arc::new(TestIpRunner::with_responses([
            ip_success(namespace.as_bytes()),
            ip_failure("delete peer failed"),
            ip_success(namespace.as_bytes()),
            ip_success(b""),
            ip_success(b""),
        ]));
        let child = Command::new("sh")
            .arg("-c")
            .arg("exit 7")
            .spawn()
            .expect("spawn child");
        let instance = FirecrackerInstance::new(
            slot.owner(),
            Some(child),
            FirecrackerCapture::new(
                temp.path().join("api.sock"),
                Duration::from_secs(1),
                "Firecracker v1.16.0".to_string(),
            ),
            runtime_files(
                temp.path().join("api.sock"),
                temp.path().join("guest.sock"),
                temp.path().join("firecracker.pid"),
                stopped_marker(temp.path()),
                network_file.clone(),
            ),
            Some(slot.clone()),
            Arc::new(NetworkManager::with_runner(runner.clone())),
            false,
        );

        let first_error = loop {
            match instance.try_wait().await {
                Ok(None) => tokio::time::sleep(Duration::from_millis(5)).await,
                Ok(Some(result)) => {
                    panic!("cleanup failure must not report completion: {result:?}")
                }
                Err(error) => break error,
            }
        };
        assert!(first_error.to_string().contains("delete peer failed"));
        assert!(network_file.exists());

        let result = instance
            .try_wait()
            .await
            .expect("retry cleanup")
            .expect("completed process");
        assert_eq!(result.exit_code, Some(7));
        assert!(!network_file.exists());
        assert!(
            runner
                .calls()
                .iter()
                .any(|args| args == &["netns", "del", slot.netns()])
        );
    }

    #[tokio::test]
    async fn stopped_orphan_still_releases_recorded_network() {
        let temp = tempfile::tempdir().expect("temp");
        record_backend_stopped(&stopped_marker(temp.path()))
            .await
            .expect("stopped marker");
        let network_file = temp.path().join("network.json");
        let network = test_network_slot(0);
        write_network_metadata(&network_file, &network).expect("network metadata");
        let namespace = format!("{}\n", network.netns());
        let runner = Arc::new(TestIpRunner::with_responses([
            ip_success(namespace.as_bytes()),
            ip_success(b""),
            ip_success(b""),
        ]));
        let network_manager = NetworkManager::with_runner(runner.clone());

        cleanup_orphan_run_dir_with(network.owner(), temp.path(), &network_manager)
            .await
            .expect("orphan cleanup");

        assert!(!network_file.exists());
        let calls = runner.calls();
        assert!(calls.iter().any(|args| {
            args == &[
                "netns",
                "exec",
                network.netns(),
                "ip",
                "link",
                "del",
                "blz-vpeer-0",
            ]
        }));
        assert!(
            calls
                .iter()
                .any(|args| args == &["netns", "del", network.netns()])
        );
    }

    #[tokio::test]
    async fn orphan_cleanup_recovers_a_complete_temporary_network_record() {
        let temp = tempfile::tempdir().expect("temp");
        record_backend_stopped(&stopped_marker(temp.path()))
            .await
            .expect("stopped marker");
        let network_file = temp.path().join("network.json");
        let network_temp_file = network_metadata_temp(&network_file);
        let network = test_network_slot(0);
        let bytes = serde_json::to_vec(&NetworkRecord {
            slot: network.slot(),
            owner: network.owner(),
            process_state: NetworkProcessState::PreSpawn,
        })
        .expect("serialize metadata");
        std::fs::write(&network_temp_file, bytes).expect("temporary metadata");
        let namespace = format!("{}\n", network.netns());
        let runner = Arc::new(TestIpRunner::with_responses([
            ip_success(namespace.as_bytes()),
            ip_success(b""),
            ip_success(b""),
        ]));
        let network_manager = NetworkManager::with_runner(runner.clone());

        cleanup_orphan_run_dir_with(network.owner(), temp.path(), &network_manager)
            .await
            .expect("orphan cleanup");

        assert!(!network_file.exists());
        assert!(!network_temp_file.exists());
        assert!(
            runner
                .calls()
                .iter()
                .any(|args| args == &["netns", "del", network.netns()])
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn orphan_cleanup_retains_a_truncated_network_record_without_pid_proof() {
        let temp = tempfile::tempdir().expect("temp");
        let network_file = temp.path().join("network.json");
        let network_temp_file = network_metadata_temp(&network_file);
        std::fs::write(&network_temp_file, b"{").expect("truncated metadata");
        let network = test_network_slot(0);
        let namespace = format!("{}\n", network.netns());
        let runner = Arc::new(TestIpRunner::with_responses([ip_success(
            namespace.as_bytes(),
        )]));
        let network_manager = NetworkManager::with_runner(runner.clone());

        let error = cleanup_orphan_run_dir_with(network.owner(), temp.path(), &network_manager)
            .await
            .expect_err("unknown launch state must fail closed");

        assert!(error.to_string().contains("missing PID handoff"));
        assert!(network_temp_file.exists());
        assert!(!stopped_marker(temp.path()).exists());
        assert_eq!(
            runner.calls(),
            vec![vec!["netns".to_string(), "list".to_string()]]
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn orphan_cleanup_retains_an_unrecorded_namespace_without_pid_proof() {
        let temp = tempfile::tempdir().expect("temp");
        let network = test_network_slot(0);
        let namespace = format!("{}\n", network.netns());
        let runner = Arc::new(TestIpRunner::with_responses([ip_success(
            namespace.as_bytes(),
        )]));
        let network_manager = NetworkManager::with_runner(runner.clone());

        let error = cleanup_orphan_run_dir_with(network.owner(), temp.path(), &network_manager)
            .await
            .expect_err("unknown launch state must fail closed");

        assert!(error.to_string().contains("missing PID handoff"));
        assert!(!stopped_marker(temp.path()).exists());
        assert_eq!(
            runner.calls(),
            vec![vec!["netns".to_string(), "list".to_string()]]
        );
    }

    #[tokio::test]
    async fn stopped_orphan_releases_an_unrecorded_owner_namespace() {
        let temp = tempfile::tempdir().expect("temp");
        record_backend_stopped(&stopped_marker(temp.path()))
            .await
            .expect("stopped marker");
        let network = test_network_slot(0);
        let namespace = format!("{}\n", network.netns());
        let runner = Arc::new(TestIpRunner::with_responses([
            ip_success(namespace.as_bytes()),
            ip_success(namespace.as_bytes()),
            ip_success(b""),
            ip_success(b""),
        ]));
        let network_manager = NetworkManager::with_runner(runner.clone());

        cleanup_orphan_run_dir_with(network.owner(), temp.path(), &network_manager)
            .await
            .expect("stopped process permits network recovery");

        assert!(
            runner
                .calls()
                .iter()
                .any(|args| args == &["netns", "del", network.netns()])
        );
    }

    #[tokio::test]
    async fn network_record_owner_mismatch_issues_no_host_commands() {
        let temp = tempfile::tempdir().expect("temp");
        let network_file = temp.path().join("network.json");
        let network = test_network_slot(0);
        write_network_metadata(&network_file, &network).expect("network metadata");
        let runner = Arc::new(TestIpRunner::default());
        let network_manager = NetworkManager::with_runner(runner.clone());

        let error = cleanup_orphan_run_dir_with(Uuid::from_u128(2), temp.path(), &network_manager)
            .await
            .expect_err("mismatched owner must fail");

        assert!(error.to_string().contains("does not match instance"));
        assert!(network_file.exists());
        assert!(runner.calls().is_empty());
    }

    #[tokio::test]
    async fn stale_network_record_does_not_delete_a_reused_slot() {
        let temp = tempfile::tempdir().expect("temp");
        record_backend_stopped(&stopped_marker(temp.path()))
            .await
            .expect("stopped marker");
        let network_file = temp.path().join("network.json");
        let old_network = test_network_slot(0);
        write_network_metadata(&network_file, &old_network).expect("network metadata");
        let new_network =
            NetworkSlot::from_record(0, Uuid::from_u128(2)).expect("new network owner");
        let namespace = format!("{}\n", new_network.netns());
        let runner = Arc::new(TestIpRunner::with_responses([ip_success(
            namespace.as_bytes(),
        )]));
        let network_manager = NetworkManager::with_runner(runner.clone());

        cleanup_orphan_run_dir_with(old_network.owner(), temp.path(), &network_manager)
            .await
            .expect("retire stale record");

        assert!(!network_file.exists());
        let calls = runner.calls();
        assert_eq!(calls, vec![vec!["netns".to_string(), "list".to_string()]]);
    }

    #[tokio::test]
    async fn pre_spawn_orphan_releases_network_without_pid_metadata() {
        let temp = tempfile::tempdir().expect("temp");
        let network_file = temp.path().join("network.json");
        let network = test_network_slot(0);
        write_network_metadata(&network_file, &network).expect("network metadata");
        let namespace = format!("{}\n", network.netns());
        let runner = Arc::new(TestIpRunner::with_responses([
            ip_success(namespace.as_bytes()),
            ip_success(b""),
            ip_success(b""),
        ]));
        let network_manager = NetworkManager::with_runner(runner.clone());

        cleanup_orphan_run_dir_with(network.owner(), temp.path(), &network_manager)
            .await
            .expect("pre-spawn cleanup");

        assert!(!network_file.exists());
        assert!(stopped_marker(temp.path()).exists());
        assert!(
            runner
                .calls()
                .iter()
                .any(|args| args == &["netns", "del", network.netns()])
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn unconfirmed_process_ownership_retains_network_metadata() {
        let temp = tempfile::tempdir().expect("temp");
        let network_file = temp.path().join("network.json");
        let network = test_network_slot(0);
        write_network_record(&network_file, &network, NetworkProcessState::Launching)
            .expect("network metadata");
        let runner = Arc::new(TestIpRunner::default());
        let network_manager = NetworkManager::with_runner(runner.clone());

        let error = cleanup_orphan_run_dir_with(network.owner(), temp.path(), &network_manager)
            .await
            .expect_err("missing process metadata must block cleanup");

        assert!(error.to_string().contains("missing PID handoff"));
        assert!(network_file.exists());
        assert!(runner.calls().is_empty());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn launch_intent_with_empty_handoff_releases_network() {
        let temp = tempfile::tempdir().expect("temp");
        let network_file = temp.path().join("network.json");
        let network = test_network_slot(0);
        write_network_record(&network_file, &network, NetworkProcessState::Launching)
            .expect("network metadata");
        prepare_pid_handoff(&temp.path().join("firecracker.pid")).expect("prepare PID handoff");
        let namespace = format!("{}\n", network.netns());
        let runner = Arc::new(TestIpRunner::with_responses([
            ip_success(namespace.as_bytes()),
            ip_success(b""),
            ip_success(b""),
        ]));
        let network_manager = NetworkManager::with_runner(runner.clone());

        cleanup_orphan_run_dir_with(network.owner(), temp.path(), &network_manager)
            .await
            .expect("empty handoff proves launch did not start");

        assert!(!network_file.exists());
        assert!(stopped_marker(temp.path()).exists());
        assert!(
            runner
                .calls()
                .iter()
                .any(|args| args == &["netns", "del", network.netns()])
        );
    }

    #[test]
    fn vm_config_and_reported_guest_transport_agree() {
        let temp = tempfile::tempdir().expect("temp");
        let request = spawn_request(temp.path());
        let socket = temp.path().join("vsock.uds");
        let disabled = FirecrackerConfig::default();
        let disabled_path = write_vm_config(
            &temp.path().join("images"),
            &request,
            &disabled,
            &socket,
            None,
        )
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
        let enabled_path = write_vm_config(
            &temp.path().join("images"),
            &request,
            &enabled,
            &socket,
            None,
        )
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
    async fn backend_probe_skips_network_checks_when_no_policy_enables_them() {
        let temp = tempfile::tempdir().expect("temp");
        let called = Arc::new(AtomicBool::new(false));
        let network = Arc::new(NetworkManager::with_runner(Arc::new(
            UnavailableNetworkRunner {
                called: called.clone(),
            },
        )));
        let spawner = FirecrackerSpawner {
            images_dir: temp.path().join("images"),
            api_timeout: Duration::from_secs(1),
            socket_timeout: Duration::from_secs(1),
            network,
            network_required: false,
        };

        assert!(spawner.network_probe_ready().await.expect("probe"));
        assert!(!called.load(Ordering::Acquire));
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
            Some(child),
            FirecrackerCapture::new(
                temp.path().join("api.sock"),
                Duration::from_secs(1),
                "Firecracker v1.16.0".to_string(),
            ),
            runtime_files(
                temp.path().join("api.sock"),
                temp.path().join("guest.sock"),
                pid_file.clone(),
                stopped_marker(temp.path()),
                temp.path().join("network.json"),
            ),
            None,
            Arc::new(NetworkManager::default()),
            true,
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

    #[derive(Default)]
    struct TestIpRunner {
        responses: std::sync::Mutex<VecDeque<IpOutput>>,
        calls: std::sync::Mutex<Vec<Vec<String>>>,
    }

    struct UnavailableNetworkRunner {
        called: Arc<AtomicBool>,
    }

    #[async_trait]
    impl IpCommandRunner for UnavailableNetworkRunner {
        async fn output(&self, _args: &[String], _timeout: Duration) -> Result<IpOutput> {
            self.called.store(true, Ordering::Release);
            Ok(ip_failure("network commands unavailable"))
        }

        #[cfg(target_os = "linux")]
        fn executable_in_path(&self, _name: &str) -> bool {
            false
        }

        #[cfg(target_os = "linux")]
        fn has_network_admin(&self) -> bool {
            false
        }
    }

    impl TestIpRunner {
        fn with_responses<const N: usize>(responses: [IpOutput; N]) -> Self {
            Self {
                responses: std::sync::Mutex::new(responses.into()),
                calls: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<Vec<String>> {
            self.calls.lock().expect("calls lock").clone()
        }
    }

    #[async_trait]
    impl IpCommandRunner for TestIpRunner {
        async fn output(&self, args: &[String], _timeout: Duration) -> Result<IpOutput> {
            self.calls.lock().expect("calls lock").push(args.to_vec());
            Ok(self
                .responses
                .lock()
                .expect("responses lock")
                .pop_front()
                .unwrap_or_else(|| ip_success(b"")))
        }
    }

    fn ip_success(stdout: &[u8]) -> IpOutput {
        IpOutput {
            success: true,
            status: "exit status: 0".to_string(),
            stdout: stdout.to_vec(),
            stderr: Vec::new(),
        }
    }

    fn ip_failure(stderr: &str) -> IpOutput {
        IpOutput {
            success: false,
            status: "exit status: 1".to_string(),
            stdout: Vec::new(),
            stderr: stderr.as_bytes().to_vec(),
        }
    }
}
