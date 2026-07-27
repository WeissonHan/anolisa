// SPDX-License-Identifier: Apache-2.0
//! End-to-end daemon/client contract using MockSpawner and a controlled guest responder.
//!
//! The runner exercises release binaries through isolated UDS and TCP endpoints
//! and proves guest exit-code propagation while preserving MockSpawner
//! ownership and cleanup behavior.

#![cfg(target_os = "linux")]

use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use uuid::Uuid;

const CONTROLLED_EXIT: i64 = 7;
const CONTROLLED_STDERR: &[u8] = b"controlled guest exit\n";

#[tokio::test]
async fn controlled_responder_returns_nonzero_without_reflecting_command() {
    let temporary = TestDirectory::new();
    let socket = temporary.path().join("vsock.uds");
    let responder = ControlledResponder::bind(&socket).expect("controlled responder");
    let server = tokio::spawn(responder.serve_once());
    let command = "guest-command-sentinel";

    let correlation_id = "00000000-0000-4000-8000-000000000001";
    let (response, encoded) = guest_exec(&socket, correlation_id, command)
        .await
        .expect("guest exec");
    let observed = server.await.expect("responder task").expect("response");

    assert_eq!(response["id"], correlation_id);
    assert_eq!(response["ok"].as_bool(), Some(true));
    assert_eq!(response["rc"], CONTROLLED_EXIT);
    assert_eq!(response["stdout_b64"], "");
    assert_eq!(
        BASE64
            .decode(response["stderr_b64"].as_str().expect("stderr base64"))
            .expect("decode stderr"),
        CONTROLLED_STDERR
    );
    assert_eq!(observed.operation, "exec");
    assert_eq!(observed.command, command);
    assert!(
        !encoded
            .windows(command.len())
            .any(|window| window == command.as_bytes())
    );

    std::fs::remove_file(&socket).expect("remove controlled socket");
    let root = temporary.path().to_path_buf();
    drop(temporary);
    assert!(!root.exists());
}

#[tokio::test]
async fn pathname_takeover_preserves_original_listener_and_cleans_all_resources() {
    let temporary = TestDirectory::new();
    let socket = temporary.path().join("vsock.uds");
    let original_mock_listener = UnixListener::bind(&socket).expect("original mock listener");

    std::fs::remove_file(&socket).expect("unlink original pathname");
    let responder = ControlledResponder::bind(&socket).expect("replacement responder");
    assert!(original_mock_listener.local_addr().is_ok());
    let server = tokio::spawn(responder.serve_once());

    let (response, _) = guest_exec(
        &socket,
        "00000000-0000-4000-8000-000000000002",
        "second-command-sentinel",
    )
    .await
    .expect("guest exec");
    assert_eq!(response["rc"], CONTROLLED_EXIT);
    server.await.expect("responder task").expect("response");
    assert!(original_mock_listener.local_addr().is_ok());
    assert!(socket.exists());

    std::fs::remove_file(&socket).expect("simulate backend pathname cleanup");
    drop(original_mock_listener);
    let root = temporary.path().to_path_buf();
    drop(temporary);
    assert!(!socket.exists());
    assert!(!root.exists());
}

const IO_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_HANDSHAKE_BYTES: usize = 128;
const MAX_REQUEST_BYTES: usize = 32 * 1024;

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "blazectl-daemon-client-guest-{}-{}",
            std::process::id(),
            Uuid::new_v4()
        ));
        std::fs::create_dir(&path).expect("create temporary directory");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

struct ControlledResponder {
    listener: UnixListener,
}

struct ObservedRequest {
    operation: String,
    command: String,
}

impl ControlledResponder {
    fn bind(path: &Path) -> io::Result<Self> {
        Ok(Self {
            listener: UnixListener::bind(path)?,
        })
    }

    async fn serve_once(self) -> io::Result<ObservedRequest> {
        tokio::time::timeout(IO_TIMEOUT, self.serve_once_inner())
            .await
            .map_err(|_| timeout_error("controlled responder"))?
    }

    async fn serve_once_inner(self) -> io::Result<ObservedRequest> {
        let (mut stream, _) = self.listener.accept().await?;
        let handshake = read_line(&mut stream, MAX_HANDSHAKE_BYTES).await?;
        if handshake != b"CONNECT 5000" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected guest CONNECT handshake",
            ));
        }
        stream.write_all(b"OK 5000\n").await?;

        let request = read_line(&mut stream, MAX_REQUEST_BYTES).await?;
        let request: Value = serde_json::from_slice(&request).map_err(invalid_json)?;
        let id = required_string(&request, "id")?;
        let operation = required_string(&request, "op")?;
        let command = required_string(&request, "cmd")?;
        if operation != "exec" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "controlled responder accepts only exec",
            ));
        }

        let response = json!({
            "id": id,
            "ok": true,
            "rc": CONTROLLED_EXIT,
            "stdout_b64": "",
            "stderr_b64": BASE64.encode(CONTROLLED_STDERR)
        });
        let mut encoded = serde_json::to_vec(&response).map_err(invalid_json)?;
        encoded.push(b'\n');
        stream.write_all(&encoded).await?;
        stream.flush().await?;
        Ok(ObservedRequest { operation, command })
    }
}

async fn guest_exec(
    socket: &Path,
    correlation_id: &str,
    command: &str,
) -> io::Result<(Value, Vec<u8>)> {
    tokio::time::timeout(
        IO_TIMEOUT,
        guest_exec_inner(socket, correlation_id, command),
    )
    .await
    .map_err(|_| timeout_error("guest client"))?
}

async fn guest_exec_inner(
    socket: &Path,
    correlation_id: &str,
    command: &str,
) -> io::Result<(Value, Vec<u8>)> {
    let mut stream = UnixStream::connect(socket).await?;
    stream.write_all(b"CONNECT 5000\n").await?;
    let handshake = read_line(&mut stream, MAX_HANDSHAKE_BYTES).await?;
    if handshake != b"OK 5000" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected controlled CONNECT response",
        ));
    }

    let request = json!({
        "id": correlation_id,
        "op": "exec",
        "cmd": command,
        "cwd": "/",
        "timeout": 1
    });
    let mut encoded_request = serde_json::to_vec(&request).map_err(invalid_json)?;
    encoded_request.push(b'\n');
    stream.write_all(&encoded_request).await?;
    stream.flush().await?;

    let encoded_response = read_line(&mut stream, MAX_REQUEST_BYTES).await?;
    let response = serde_json::from_slice(&encoded_response).map_err(invalid_json)?;
    Ok((response, encoded_response))
}

async fn read_line(stream: &mut UnixStream, limit: usize) -> io::Result<Vec<u8>> {
    let mut output = Vec::new();
    let mut byte = [0_u8; 1];
    loop {
        if stream.read(&mut byte).await? == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "guest frame ended before newline",
            ));
        }
        if byte[0] == b'\n' {
            return Ok(output);
        }
        if output.len() == limit {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "guest frame exceeds limit",
            ));
        }
        output.push(byte[0]);
    }
}

fn required_string(value: &Value, field: &str) -> io::Result<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("guest request lacks {field}"),
            )
        })
}

fn invalid_json(error: serde_json::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

fn timeout_error(operation: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        format!("{operation} exceeded test deadline"),
    )
}

#[cfg(feature = "integration")]
mod release_runner {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};
    use std::ffi::{OsStr, OsString};
    use std::fs::{self, OpenOptions};
    use std::net::{Ipv4Addr, SocketAddrV4, TcpListener as StdTcpListener};
    use std::os::unix::fs::PermissionsExt;
    use std::process::{Output, Stdio};

    use blazectl::cli::EndpointSelection as ClientEndpointSelection;
    use blazectl::client::{BlazeClient, ClientConfig};
    use hyper::{Method, StatusCode};
    use tokio::process::{Child, Command};
    use tokio::time::Instant;

    const UDS_SANDBOX_ID: &str = "00000000-0000-4000-8000-000000000011";
    const TCP_SANDBOX_ID: &str = "00000000-0000-4000-8000-000000000012";
    const CONTROLLED_SANDBOX_ID: &str = "00000000-0000-4000-8000-000000000013";
    const MISSING_SANDBOX_ID: &str = "00000000-0000-4000-8000-000000000014";
    const KILL_ALL_BASE: u128 = 0x00000000_0000_4000_8000_000000001000;
    const COMMAND_TIMEOUT: Duration = Duration::from_secs(20);
    const DAEMON_READY_TIMEOUT: Duration = Duration::from_secs(20);
    const DAEMON_STOP_TIMEOUT: Duration = Duration::from_secs(20);
    const POLL_INTERVAL: Duration = Duration::from_millis(50);
    const MAX_DAEMON_LOG_BYTES: u64 = 1024 * 1024;
    const RELEASE_CHILD_ENVIRONMENT: [(&str, &str); 10] = [
        ("LC_ALL", "C"),
        ("LANG", "C"),
        ("LANGUAGE", "C"),
        ("TERM", "dumb"),
        ("NO_COLOR", "1"),
        ("CLICOLOR", "0"),
        ("CLICOLOR_FORCE", "0"),
        ("FORCE_COLOR", "0"),
        ("RUST_LOG_STYLE", "never"),
        ("CARGO_TERM_COLOR", "never"),
    ];
    const ORIGINAL_FILE_BYTES: &[u8] = b"\0daemon-client\xff\r\nbinary\n";
    const STDIN_FILE_BYTES: &[u8] = b"stdin\0payload\x80\xff\n";
    const MUTATED_FILE_BYTES: &[u8] = b"mutated-after-checkpoint";
    const MOCK_EXEC_COMMAND: &str = "mock-guest-success";
    const CONTROLLED_EXEC_COMMAND: &str = "controlled-guest-command";
    const POLICY_TOML: &str = r#"manifest_version = 1
policy_name = "daemon-client"
priority = 1

[match]
workload_class = "agent-tool"

[select]
backend_priority = ["firecracker"]
kernel_hooks = []
templates = []
fallback_on_missing_hook = "fail"
"#;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn release_blazed_and_blazectl_complete_mock_acceptance_matrix() {
        assert_release_child_environment_contract();
        let binaries = ReleaseBinaries::locate().expect("locate candidate release binaries");
        let cli = CliRunner::new(binaries.blazectl.clone());
        assert_release_versions(&binaries, &cli).await;

        let environment =
            DaemonClientEnvironment::new().expect("create isolated daemon-client environment");
        let environment_root = environment.root().to_path_buf();
        let uds = Endpoint::Uds(environment.api_socket().to_path_buf());
        let tcp = Endpoint::Http(environment.http_origin());
        let mut daemon = DaemonProcess::start(
            &binaries.blazed,
            environment.config_path(),
            environment.daemon_log(),
        )
        .await
        .expect("start release blazed");

        wait_until_mock_healthy(&environment).await;
        wait_until_ready(&cli, &uds, &tcp).await;
        let uds_checkpoint = run_lifecycle_matrix(&cli, &environment, &uds, UDS_SANDBOX_ID).await;
        let tcp_checkpoint = run_lifecycle_matrix(&cli, &environment, &tcp, TCP_SANDBOX_ID).await;
        run_controlled_guest_nonzero(&cli, &environment, &uds, CONTROLLED_SANDBOX_ID).await;
        run_daemon_error_matrix(&cli, &uds, MISSING_SANDBOX_ID).await;
        let kill_all_ids = run_kill_all_51(&cli, &uds).await;
        assert_public_list_empty(&cli, &uds).await;

        daemon.stop().await.expect("stop release blazed");
        assert_endpoint_unreachable(&cli, &uds).await;
        assert_endpoint_unreachable(&cli, &tcp).await;
        assert_release_versions(&binaries, &cli).await;
        environment.assert_daemon_log_confidentiality();
        environment.assert_no_runtime_resources();
        environment.assert_no_storage_or_metrics_resources();
        environment.assert_expected_persistent_metadata(
            &uds_checkpoint,
            &tcp_checkpoint,
            &kill_all_ids,
        );
        drop(environment);
        assert!(
            !environment_root.exists(),
            "isolated daemon-client environment must be removed"
        );
    }

    #[derive(Clone, Copy)]
    enum RenderMode {
        Text,
        Json,
    }

    impl RenderMode {
        const fn as_str(self) -> &'static str {
            match self {
                Self::Text => "text",
                Self::Json => "json",
            }
        }
    }

    #[derive(Clone)]
    enum Endpoint {
        Uds(PathBuf),
        Http(String),
    }

    impl Endpoint {
        fn apply(&self, command: &mut Command) {
            match self {
                Self::Uds(path) => {
                    command.arg("--socket").arg(path);
                }
                Self::Http(origin) => {
                    command.arg("--url").arg(origin);
                }
            }
        }
    }

    struct ReleaseBinaries {
        blazed: PathBuf,
        blazectl: PathBuf,
    }

    impl ReleaseBinaries {
        fn locate() -> io::Result<Self> {
            let test_binary = std::env::current_exe()
                .map_err(|_| io::Error::other("integration test executable is unavailable"))?;
            let debug_root = test_binary
                .parent()
                .and_then(Path::parent)
                .ok_or_else(|| io::Error::other("integration target profile is unavailable"))?;
            let target_scope = debug_root
                .parent()
                .ok_or_else(|| io::Error::other("integration target scope is unavailable"))?;
            let release_root = target_scope.join("release");
            let binaries = Self {
                blazed: release_root.join("blazed"),
                blazectl: release_root.join("blazectl"),
            };
            validate_release_binary(&binaries.blazed)?;
            validate_release_binary(&binaries.blazectl)?;
            Ok(binaries)
        }
    }

    fn validate_release_binary(path: &Path) -> io::Result<()> {
        let metadata = fs::metadata(path)
            .map_err(|_| io::Error::other("candidate release binary is missing"))?;
        if !metadata.is_file() || metadata.permissions().mode() & 0o111 == 0 {
            return Err(io::Error::other(
                "candidate release artifact is not an executable file",
            ));
        }
        Ok(())
    }

    fn configure_release_child_environment(command: &mut Command) {
        command.envs(RELEASE_CHILD_ENVIRONMENT);
        command.env_remove("COLORTERM");
    }

    fn assert_release_child_environment_contract() {
        let mut command = Command::new("unused-release-child");
        configure_release_child_environment(&mut command);
        for (key, expected) in RELEASE_CHILD_ENVIRONMENT {
            let actual = command
                .as_std()
                .get_envs()
                .find_map(|(name, value)| (name == OsStr::new(key)).then_some(value))
                .flatten();
            assert_eq!(
                actual,
                Some(OsStr::new(expected)),
                "release child environment must freeze {key}"
            );
        }
        assert!(
            command
                .as_std()
                .get_envs()
                .any(|(name, value)| name == OsStr::new("COLORTERM") && value.is_none()),
            "release child environment must remove COLORTERM"
        );
    }

    struct DaemonClientEnvironment {
        root: PathBuf,
        config: PathBuf,
        state: PathBuf,
        images: PathBuf,
        instances: PathBuf,
        api_socket: PathBuf,
        metrics_socket: PathBuf,
        daemon_log: PathBuf,
        http_addr: String,
    }

    impl DaemonClientEnvironment {
        fn new() -> io::Result<Self> {
            let nonce = Uuid::new_v4().simple().to_string();
            let process_id = std::process::id();
            let nonce = &nonce[..12];
            let root = PathBuf::from(format!("/tmp/bz4-{process_id}-{nonce}"));
            fs::create_dir(&root)?;

            let state = root.join("state");
            let images = root.join("images");
            let instances = root.join("instances");
            let policies = root.join("policies");
            let templates = root.join("templates");
            let api_socket = root.join("api.sock");
            let metrics_socket = root.join("metrics.sock");
            let daemon_log = root.join("daemon.log");
            for directory in [&state, &images, &instances, &policies, &templates] {
                fs::create_dir(directory)?;
            }
            fs::write(policies.join("daemon-client.toml"), POLICY_TOML)?;

            let listener = StdTcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))?;
            let http_addr = listener.local_addr()?.to_string();
            drop(listener);

            let config = root.join("config.toml");
            let body = format!(
                r#"[daemon]
log_level = "warn"
state_dir = "{state}"
socket = "{api_socket}"

[listen]
http_addr = "{http_addr}"

[policy]
dir = "{policies}"
on_load_error = "fail"

[storage]
images_dir = "{images}"
instances_dir = "{instances}"
provider = "file"
pool_size = 0
prefork = false
flush_interval = "60s"
rootfs_size = 4096
mem_size = 4096

[template]
dir = "{templates}"

[metrics]
prometheus_socket = "{metrics_socket}"

[api]
max_body_bytes = 1048576
max_file_bytes = 1048576
request_timeout = "30s"
"#,
                state = state.display(),
                api_socket = api_socket.display(),
                policies = policies.display(),
                images = images.display(),
                instances = instances.display(),
                templates = templates.display(),
                metrics_socket = metrics_socket.display(),
            );
            fs::write(&config, body)?;

            Ok(Self {
                root,
                config,
                state,
                images,
                instances,
                api_socket,
                metrics_socket,
                daemon_log,
                http_addr,
            })
        }

        fn root(&self) -> &Path {
            &self.root
        }

        fn config_path(&self) -> &Path {
            &self.config
        }

        fn api_socket(&self) -> &Path {
            &self.api_socket
        }

        fn daemon_log(&self) -> &Path {
            &self.daemon_log
        }

        fn http_origin(&self) -> String {
            let address = &self.http_addr;
            format!("http://{address}")
        }

        fn host_file(&self, name: &str) -> PathBuf {
            self.root.join(name)
        }

        fn guest_socket(&self, sandbox_id: &str) -> PathBuf {
            self.state
                .join(sandbox_id)
                .join("runtime")
                .join("vsock.uds")
        }

        fn assert_no_runtime_resources(&self) {
            assert!(
                !self.api_socket.exists(),
                "daemon API socket must be removed"
            );
            assert_directory_empty(&self.instances, "provider instance directory");
            assert_no_transient_paths(&self.state);
        }

        fn assert_no_storage_or_metrics_resources(&self) {
            assert_directory_empty(&self.images, "provider image directory");
            assert!(
                !self.metrics_socket.exists(),
                "metrics socket must not remain after daemon shutdown"
            );
        }

        fn assert_daemon_log_confidentiality(&self) {
            let metadata = fs::metadata(&self.daemon_log).expect("read daemon log metadata");
            assert!(metadata.is_file(), "daemon log must be a regular file");
            assert!(
                metadata.len() <= MAX_DAEMON_LOG_BYTES,
                "daemon log must remain within the review bound"
            );
            let log = fs::read(&self.daemon_log).expect("read bounded daemon log");
            let host_root = self.root.to_string_lossy();
            assert!(
                !bytes_contain(&log, host_root.as_bytes()),
                "daemon log must not disclose the isolated host path"
            );
            let guest_sentinels: [&[u8]; 8] = [
                ORIGINAL_FILE_BYTES,
                STDIN_FILE_BYTES,
                MUTATED_FILE_BYTES,
                MOCK_EXEC_COMMAND.as_bytes(),
                CONTROLLED_EXEC_COMMAND.as_bytes(),
                CONTROLLED_STDERR,
                b"/tmp/release-file.bin",
                b"/tmp/release-stdin.bin",
            ];
            for (index, sentinel) in guest_sentinels.into_iter().enumerate() {
                assert!(
                    !bytes_contain(&log, sentinel),
                    "daemon log must not contain guest sentinel {index}"
                );
            }
        }

        fn assert_expected_persistent_metadata(
            &self,
            uds_checkpoint: &str,
            tcp_checkpoint: &str,
            kill_all_ids: &[String],
        ) {
            assert_eq!(kill_all_ids.len(), 51, "kill-all tombstone inventory");

            let mut sandboxes = BTreeMap::from([
                (UDS_SANDBOX_ID, Some(uds_checkpoint)),
                (TCP_SANDBOX_ID, Some(tcp_checkpoint)),
                (CONTROLLED_SANDBOX_ID, None),
            ]);
            for id in kill_all_ids {
                assert!(
                    sandboxes.insert(id.as_str(), None).is_none(),
                    "sandbox inventory must not contain duplicate IDs"
                );
            }
            assert!(
                !sandboxes.contains_key(MISSING_SANDBOX_ID),
                "not-found requests must not create persistent metadata"
            );

            let mut actual_state = directory_inventory(&self.state);
            if let Some(kind) = actual_state.remove("runtime-pool") {
                assert_eq!(
                    kind, "directory",
                    "runtime-pool, when present, must be a directory"
                );
            }
            let mut expected_state = BTreeMap::from([("checkpoints".to_string(), "directory")]);
            expected_state.extend(sandboxes.keys().map(|id| ((*id).to_string(), "directory")));
            assert_eq!(
                actual_state, expected_state,
                "durable state root must contain only the frozen metadata inventory"
            );

            for (sandbox_id, checkpoint_id) in &sandboxes {
                let state_directory = self.state.join(sandbox_id);
                assert_eq!(
                    directory_inventory(&state_directory),
                    BTreeMap::from([("state.json".to_string(), "file")]),
                    "destroyed sandbox directory must contain only its tombstone"
                );
                let state: Value = serde_json::from_slice(
                    &fs::read(state_directory.join("state.json")).expect("read sandbox tombstone"),
                )
                .expect("parse sandbox tombstone");
                assert_eq!(state["id"], *sandbox_id);
                assert_eq!(state["state"], "destroyed");
                assert!(state["operation"].is_null());
                match checkpoint_id {
                    Some(checkpoint_id) => {
                        assert_eq!(state["last_checkpoint"], *checkpoint_id);
                    }
                    None => assert!(state["last_checkpoint"].is_null()),
                }
            }

            let checkpoint_root = self.state.join("checkpoints");
            assert_eq!(
                directory_inventory(&checkpoint_root),
                BTreeMap::from([
                    (UDS_SANDBOX_ID.to_string(), "directory"),
                    (TCP_SANDBOX_ID.to_string(), "directory"),
                ]),
                "only lifecycle sandboxes may retain committed checkpoints"
            );
            assert_checkpoint_inventory(&checkpoint_root, UDS_SANDBOX_ID, uds_checkpoint);
            assert_checkpoint_inventory(&checkpoint_root, TCP_SANDBOX_ID, tcp_checkpoint);
        }
    }

    impl Drop for DaemonClientEnvironment {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    struct DaemonProcess {
        child: Option<Child>,
        pid: u32,
    }

    impl DaemonProcess {
        async fn start(binary: &Path, config: &Path, log_path: &Path) -> io::Result<Self> {
            let log = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(log_path)?;
            let stderr = log.try_clone()?;
            let mut command = Command::new(binary);
            configure_release_child_environment(&mut command);
            command
                .args(["daemon", "start", "--config"])
                .arg(config)
                .env("RUST_LOG", "warn")
                .stdin(Stdio::null())
                .stdout(Stdio::from(log))
                .stderr(Stdio::from(stderr))
                .kill_on_drop(true);
            let child = command.spawn()?;
            let pid = child
                .id()
                .ok_or_else(|| io::Error::other("release daemon PID is unavailable"))?;
            Ok(Self {
                child: Some(child),
                pid,
            })
        }

        async fn stop(&mut self) -> io::Result<()> {
            let pid = self.pid.to_string();
            let signal_status = Command::new("kill")
                .args(["-TERM", pid.as_str()])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await?;
            if !signal_status.success() {
                return Err(io::Error::other("failed to signal release daemon"));
            }
            let child = self
                .child
                .take()
                .ok_or_else(|| io::Error::other("release daemon was already reaped"))?;
            let status = tokio::time::timeout(DAEMON_STOP_TIMEOUT, child.wait_with_output())
                .await
                .map_err(|_| timeout_error("release daemon shutdown"))??
                .status;
            if !status.success() {
                return Err(io::Error::other("release daemon exited unsuccessfully"));
            }
            assert_process_reaped(self.pid, "release daemon");
            Ok(())
        }
    }

    impl Drop for DaemonProcess {
        fn drop(&mut self) {
            if let Some(child) = self.child.as_mut() {
                let _ = child.start_kill();
            }
        }
    }

    struct CliRunner {
        binary: PathBuf,
    }

    impl CliRunner {
        fn new(binary: PathBuf) -> Self {
            Self { binary }
        }

        async fn run(
            &self,
            endpoint: &Endpoint,
            mode: RenderMode,
            arguments: Vec<OsString>,
            stdin: Option<&[u8]>,
            label: &'static str,
        ) -> Output {
            let mut command = Command::new(&self.binary);
            endpoint.apply(&mut command);
            command
                .arg("--output")
                .arg(mode.as_str())
                .args(arguments)
                .env_remove("BLAZED_URL")
                .env_remove("BLAZECTL_OUTPUT");
            run_child(command, stdin, label).await
        }

        async fn run_local(
            &self,
            mode: Option<RenderMode>,
            arguments: Vec<OsString>,
            label: &'static str,
        ) -> Output {
            let mut command = Command::new(&self.binary);
            if let Some(mode) = mode {
                command.arg("--output").arg(mode.as_str());
            }
            command
                .args(arguments)
                .env("BLAZED_URL", "not-an-http-origin")
                .env_remove("BLAZECTL_OUTPUT");
            run_child(command, None, label).await
        }
    }

    async fn run_child(mut command: Command, stdin: Option<&[u8]>, label: &'static str) -> Output {
        configure_release_child_environment(&mut command);
        command
            .stdin(if stdin.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().unwrap_or_else(|_| panic!("{label}: spawn"));
        let pid = child
            .id()
            .unwrap_or_else(|| panic!("{label}: child PID unavailable"));
        if let Some(input) = stdin {
            let mut child_stdin = child
                .stdin
                .take()
                .unwrap_or_else(|| panic!("{label}: stdin unavailable"));
            child_stdin
                .write_all(input)
                .await
                .unwrap_or_else(|_| panic!("{label}: stdin write"));
            child_stdin
                .shutdown()
                .await
                .unwrap_or_else(|_| panic!("{label}: stdin close"));
        }
        let output = tokio::time::timeout(COMMAND_TIMEOUT, child.wait_with_output())
            .await
            .unwrap_or_else(|_| panic!("{label}: deadline"))
            .unwrap_or_else(|_| panic!("{label}: wait"));
        assert_process_reaped(pid, label);
        output
    }

    fn assert_process_reaped(pid: u32, label: &str) {
        assert!(
            !Path::new("/proc").join(pid.to_string()).exists(),
            "{label}: process must be reaped"
        );
    }

    async fn assert_release_versions(binaries: &ReleaseBinaries, cli: &CliRunner) {
        let version = env!("CARGO_PKG_VERSION");
        let expected_text = format!("blazectl {version}\n");
        let text = cli
            .run_local(
                Some(RenderMode::Text),
                arguments(&["version"]),
                "local version text",
            )
            .await;
        assert_success(&text, "local version text");
        assert_eq!(text.stdout, expected_text.as_bytes());

        let json_output = cli
            .run_local(
                Some(RenderMode::Json),
                arguments(&["version"]),
                "local version JSON",
            )
            .await;
        let value = success_json(&json_output, "local version JSON");
        assert_eq!(value["name"], "blazectl");
        assert_eq!(value["version"], env!("CARGO_PKG_VERSION"));

        let clap_version = cli
            .run_local(None, arguments(&["--version"]), "local clap version")
            .await;
        assert_success(&clap_version, "local clap version");
        assert_eq!(clap_version.stdout, expected_text.as_bytes());

        let mut command = Command::new(&binaries.blazed);
        command.arg("--version");
        let blazed_version = run_child(command, None, "blazed local version").await;
        assert_success(&blazed_version, "blazed local version");
        assert_eq!(
            blazed_version.stdout,
            format!("blazed {version}\n").as_bytes()
        );
    }

    async fn wait_until_mock_healthy(environment: &DaemonClientEnvironment) {
        let uds = BlazeClient::new(
            ClientConfig::from_selection(ClientEndpointSelection::Unix(
                environment.api_socket().to_path_buf(),
            ))
            .expect("daemon-client health UDS config"),
        );
        let tcp = BlazeClient::new(
            ClientConfig::from_selection(ClientEndpointSelection::Http(environment.http_origin()))
                .expect("daemon-client health TCP config"),
        );

        let uds_health = wait_for_health(&uds, "daemon-client health UDS").await;
        assert_mock_health(&uds_health, "daemon-client health UDS");
        let tcp_health = wait_for_health(&tcp, "daemon-client health TCP").await;
        assert_mock_health(&tcp_health, "daemon-client health TCP");
    }

    async fn wait_for_health(client: &BlazeClient, label: &'static str) -> Value {
        let deadline = Instant::now() + DAEMON_READY_TIMEOUT;
        loop {
            match client.request(Method::GET, "/v1/health", Vec::new()).await {
                Ok(response) if response.status == StatusCode::OK => {
                    return serde_json::from_slice(&response.body)
                        .unwrap_or_else(|_| panic!("{label}: health JSON"));
                }
                Ok(_) => panic!("{label}: unexpected health status"),
                Err(_) => {
                    assert!(Instant::now() < deadline, "{label}: health deadline");
                    tokio::time::sleep(POLL_INTERVAL).await;
                }
            }
        }
    }

    fn assert_mock_health(value: &Value, label: &str) {
        assert_eq!(value["status"], "ok", "{label}: health status");
        assert_eq!(
            value["version"],
            env!("CARGO_PKG_VERSION"),
            "{label}: daemon version"
        );
        assert_eq!(value["backend"], "mock", "{label}: active backend");
        assert_eq!(
            value["storage_pool"]["capacity"], 0,
            "{label}: pool capacity"
        );
        assert_eq!(value["storage_pool"]["ready"], 0, "{label}: pool ready");
        assert_eq!(value["storage_pool"]["pending"], 0, "{label}: pool pending");
        assert_eq!(
            value["storage_pool"]["quarantined"], 0,
            "{label}: pool quarantine"
        );
    }

    async fn wait_until_ready(cli: &CliRunner, uds: &Endpoint, tcp: &Endpoint) {
        let deadline = Instant::now() + DAEMON_READY_TIMEOUT;
        loop {
            let output = cli
                .run(
                    uds,
                    RenderMode::Json,
                    arguments(&["list"]),
                    None,
                    "daemon readiness",
                )
                .await;
            if output.status.success() {
                let value = success_json(&output, "daemon readiness");
                assert!(value.is_array());
                break;
            }
            assert!(
                Instant::now() < deadline,
                "release daemon did not become ready"
            );
            tokio::time::sleep(POLL_INTERVAL).await;
        }

        let tcp_output = cli
            .run(
                tcp,
                RenderMode::Json,
                arguments(&["list"]),
                None,
                "TCP readiness",
            )
            .await;
        assert!(success_json(&tcp_output, "TCP readiness").is_array());
    }

    async fn run_lifecycle_matrix(
        cli: &CliRunner,
        environment: &DaemonClientEnvironment,
        endpoint: &Endpoint,
        sandbox_id: &str,
    ) -> String {
        let create = cli
            .run(
                endpoint,
                RenderMode::Json,
                arguments(&["create", sandbox_id]),
                None,
                "lifecycle create",
            )
            .await;
        let created = success_json(&create, "lifecycle create");
        assert_eq!(created["id"], sandbox_id);
        assert_eq!(created["status"], "running");

        let list = cli
            .run(
                endpoint,
                RenderMode::Json,
                arguments(&["list"]),
                None,
                "lifecycle list JSON",
            )
            .await;
        assert!(list_contains(
            &success_json(&list, "lifecycle list JSON"),
            sandbox_id
        ));

        let alias = cli
            .run(
                endpoint,
                RenderMode::Text,
                arguments(&["ls"]),
                None,
                "lifecycle list text alias",
            )
            .await;
        assert_success(&alias, "lifecycle list text alias");
        assert!(bytes_contain(&alias.stdout, sandbox_id.as_bytes()));

        let exec = cli
            .run(
                endpoint,
                RenderMode::Text,
                arguments(&["exec", sandbox_id, MOCK_EXEC_COMMAND]),
                None,
                "lifecycle exec success",
            )
            .await;
        assert_success(&exec, "lifecycle exec success");
        assert_eq!(exec.stdout, MOCK_EXEC_COMMAND.as_bytes());

        let host_file = environment.host_file("release-input.bin");
        fs::write(&host_file, ORIGINAL_FILE_BYTES).expect("write daemon-client host file");
        let mut write_file_args =
            arguments(&["write", sandbox_id, "/tmp/release-file.bin", "--file"]);
        write_file_args.push(host_file.into_os_string());
        let write_file = cli
            .run(
                endpoint,
                RenderMode::Json,
                write_file_args,
                None,
                "lifecycle write file",
            )
            .await;
        assert_eq!(
            success_json(&write_file, "lifecycle write file")["status"],
            "ok"
        );

        let write_stdin = cli
            .run(
                endpoint,
                RenderMode::Text,
                arguments(&["write", sandbox_id, "/tmp/release-stdin.bin", "--file", "-"]),
                Some(STDIN_FILE_BYTES),
                "lifecycle write stdin",
            )
            .await;
        assert_success(&write_stdin, "lifecycle write stdin");
        assert_eq!(write_stdin.stdout, b"STATUS\tok\n");

        assert_guest_file(
            cli,
            endpoint,
            sandbox_id,
            "/tmp/release-file.bin",
            ORIGINAL_FILE_BYTES,
            "lifecycle read file",
        )
        .await;
        assert_guest_file(
            cli,
            endpoint,
            sandbox_id,
            "/tmp/release-stdin.bin",
            STDIN_FILE_BYTES,
            "lifecycle read stdin file",
        )
        .await;

        let checkpoint = cli
            .run(
                endpoint,
                RenderMode::Json,
                arguments(&["checkpoint", sandbox_id]),
                None,
                "lifecycle checkpoint",
            )
            .await;
        let checkpoint = success_json(&checkpoint, "lifecycle checkpoint");
        assert_eq!(checkpoint["status"], "checkpointed");
        let checkpoint_id = checkpoint["checkpoint_id"]
            .as_str()
            .expect("checkpoint identifier")
            .to_string();
        assert!(checkpoint_id.starts_with("ckpt-"));

        let checkpoints = cli
            .run(
                endpoint,
                RenderMode::Json,
                arguments(&["checkpoints", sandbox_id]),
                None,
                "lifecycle checkpoints",
            )
            .await;
        let checkpoints = success_json(&checkpoints, "lifecycle checkpoints");
        assert!(
            checkpoints["checkpoints"]
                .as_array()
                .expect("checkpoint array")
                .iter()
                .any(|entry| entry["id"].as_str() == Some(checkpoint_id.as_str()))
        );

        let mutate = cli
            .run(
                endpoint,
                RenderMode::Json,
                arguments(&["write", sandbox_id, "/tmp/release-file.bin", "--file", "-"]),
                Some(MUTATED_FILE_BYTES),
                "lifecycle mutate",
            )
            .await;
        assert_eq!(success_json(&mutate, "lifecycle mutate")["status"], "ok");
        assert_guest_file(
            cli,
            endpoint,
            sandbox_id,
            "/tmp/release-file.bin",
            MUTATED_FILE_BYTES,
            "lifecycle read mutation",
        )
        .await;

        let rollback = cli
            .run(
                endpoint,
                RenderMode::Json,
                arguments(&["rollback", sandbox_id, checkpoint_id.as_str()]),
                None,
                "lifecycle rollback",
            )
            .await;
        let rollback = success_json(&rollback, "lifecycle rollback");
        assert_eq!(rollback["status"], "rolledback");
        assert_eq!(rollback["checkpoint"], checkpoint_id);
        assert_guest_file(
            cli,
            endpoint,
            sandbox_id,
            "/tmp/release-file.bin",
            ORIGINAL_FILE_BYTES,
            "lifecycle read rollback",
        )
        .await;

        let prune = cli
            .run(
                endpoint,
                RenderMode::Text,
                arguments(&["prune-checkpoints", sandbox_id]),
                None,
                "lifecycle prune",
            )
            .await;
        assert_success(&prune, "lifecycle prune");
        assert!(bytes_contain(&prune.stdout, b"STATUS\tpruned\n"));

        let hibernate = cli
            .run(
                endpoint,
                RenderMode::Json,
                arguments(&["hibernate", sandbox_id]),
                None,
                "lifecycle hibernate",
            )
            .await;
        assert_eq!(
            success_json(&hibernate, "lifecycle hibernate")["status"],
            "hibernated"
        );

        let conflict = cli
            .run(
                endpoint,
                RenderMode::Json,
                arguments(&["exec", sandbox_id, "must-not-run"]),
                None,
                "lifecycle state conflict",
            )
            .await;
        assert_diagnostic(&conflict, 1, "state_conflict", Some(sandbox_id));

        let resume = cli
            .run(
                endpoint,
                RenderMode::Text,
                arguments(&["resume", sandbox_id]),
                None,
                "lifecycle resume",
            )
            .await;
        assert_success(&resume, "lifecycle resume");
        assert_eq!(resume.stdout, b"STATUS\trunning\n");

        let resumed_exec = cli
            .run(
                endpoint,
                RenderMode::Json,
                arguments(&["exec", sandbox_id, MOCK_EXEC_COMMAND]),
                None,
                "lifecycle exec after resume",
            )
            .await;
        let resumed_exec = success_json(&resumed_exec, "lifecycle exec after resume");
        assert_eq!(resumed_exec["exit_code"], 0);
        assert_eq!(resumed_exec["stdout"], MOCK_EXEC_COMMAND);
        assert_eq!(resumed_exec["stderr"], "");

        let pool = cli
            .run(
                endpoint,
                RenderMode::Json,
                arguments(&["pool-status"]),
                None,
                "lifecycle pool status",
            )
            .await;
        let pool = success_json(&pool, "lifecycle pool status");
        assert_eq!(pool["capacity"], 0);
        assert_eq!(pool["ready"], 0);

        let cleanup = cli
            .run(
                endpoint,
                RenderMode::Text,
                arguments(&["cleanup-devices"]),
                None,
                "lifecycle cleanup devices",
            )
            .await;
        assert_success(&cleanup, "lifecycle cleanup devices");
        assert!(bytes_contain(&cleanup.stdout, b"DESTROYED\t0\n"));

        let kill = cli
            .run(
                endpoint,
                RenderMode::Text,
                arguments(&["kill", sandbox_id]),
                None,
                "lifecycle kill",
            )
            .await;
        assert_success(&kill, "lifecycle kill");
        assert_eq!(kill.stdout, b"STATUS\tok\n");

        let repeated = cli
            .run(
                endpoint,
                RenderMode::Json,
                arguments(&["kill", sandbox_id]),
                None,
                "lifecycle repeated kill",
            )
            .await;
        assert_eq!(
            success_json(&repeated, "lifecycle repeated kill")["status"],
            "ok"
        );

        let list = cli
            .run(
                endpoint,
                RenderMode::Json,
                arguments(&["list"]),
                None,
                "lifecycle post-kill list",
            )
            .await;
        assert!(!list_contains(
            &success_json(&list, "lifecycle post-kill list"),
            sandbox_id
        ));
        checkpoint_id
    }

    async fn assert_guest_file(
        cli: &CliRunner,
        endpoint: &Endpoint,
        sandbox_id: &str,
        guest_path: &str,
        expected: &[u8],
        label: &'static str,
    ) {
        let output = cli
            .run(
                endpoint,
                RenderMode::Text,
                arguments(&["read", sandbox_id, guest_path]),
                None,
                label,
            )
            .await;
        assert_success(&output, label);
        assert_eq!(output.stdout, expected, "{label}: binary payload");
    }

    async fn run_controlled_guest_nonzero(
        cli: &CliRunner,
        environment: &DaemonClientEnvironment,
        endpoint: &Endpoint,
        sandbox_id: &str,
    ) {
        let create = cli
            .run(
                endpoint,
                RenderMode::Json,
                arguments(&["create", sandbox_id]),
                None,
                "controlled create",
            )
            .await;
        assert_eq!(success_json(&create, "controlled create")["id"], sandbox_id);

        let socket = environment.guest_socket(sandbox_id);
        assert!(socket.exists(), "controlled guest socket must exist");
        fs::remove_file(&socket).expect("unlink original mock guest socket");
        let responder = ControlledResponder::bind(&socket).expect("bind controlled responder");
        let server = tokio::spawn(responder.serve_once());

        let exec = cli
            .run(
                endpoint,
                RenderMode::Text,
                arguments(&["exec", sandbox_id, CONTROLLED_EXEC_COMMAND]),
                None,
                "controlled guest nonzero",
            )
            .await;
        assert_exit(&exec, CONTROLLED_EXIT as i32, "controlled guest nonzero");
        assert!(exec.stdout.is_empty());
        assert_eq!(exec.stderr, CONTROLLED_STDERR);
        let observed = server
            .await
            .expect("controlled responder task")
            .expect("controlled responder request");
        assert_eq!(observed.operation, "exec");
        assert_eq!(observed.command, CONTROLLED_EXEC_COMMAND);

        let kill = cli
            .run(
                endpoint,
                RenderMode::Json,
                arguments(&["kill", sandbox_id]),
                None,
                "controlled kill",
            )
            .await;
        assert_eq!(success_json(&kill, "controlled kill")["status"], "ok");
        assert!(!socket.exists(), "controlled guest socket must be removed");
    }

    async fn run_daemon_error_matrix(cli: &CliRunner, endpoint: &Endpoint, missing_id: &str) {
        let invalid = cli
            .run(
                endpoint,
                RenderMode::Json,
                arguments(&["read", missing_id, "relative-path"]),
                None,
                "daemon invalid request",
            )
            .await;
        assert_diagnostic(&invalid, 1, "invalid_request", Some(missing_id));

        let missing = cli
            .run(
                endpoint,
                RenderMode::Text,
                arguments(&["kill", missing_id]),
                None,
                "daemon not found text",
            )
            .await;
        assert_exit(&missing, 1, "daemon not found text");
        assert!(missing.stdout.is_empty());
        assert_eq!(
            missing.stderr,
            b"error: not_found: requested resource was not found\n"
        );

        let missing = cli
            .run(
                endpoint,
                RenderMode::Json,
                arguments(&["kill", missing_id]),
                None,
                "daemon not found JSON",
            )
            .await;
        assert_diagnostic(&missing, 1, "not_found", Some(missing_id));
    }

    async fn run_kill_all_51(cli: &CliRunner, endpoint: &Endpoint) -> Vec<String> {
        let ids = (0_u128..51)
            .map(|offset| Uuid::from_u128(KILL_ALL_BASE + offset).to_string())
            .collect::<Vec<_>>();
        for id in &ids {
            let create = cli
                .run(
                    endpoint,
                    RenderMode::Json,
                    arguments(&["create", id]),
                    None,
                    "kill-all create",
                )
                .await;
            assert_eq!(success_json(&create, "kill-all create")["id"], id.as_str());
        }

        let kill_all = cli
            .run(
                endpoint,
                RenderMode::Json,
                arguments(&["kill", "--all"]),
                None,
                "kill-all 51",
            )
            .await;
        let summary = success_json(&kill_all, "kill-all 51");
        assert_eq!(summary["total"], 51);
        assert_eq!(summary["succeeded"].as_array().map(Vec::len), Some(51));
        assert_eq!(summary["failed"].as_array().map(Vec::len), Some(0));
        assert_eq!(summary["unfinished"].as_array().map(Vec::len), Some(0));
        for id in &ids {
            assert!(
                summary["succeeded"]
                    .as_array()
                    .expect("kill-all succeeded array")
                    .iter()
                    .any(|value| value.as_str() == Some(id.as_str()))
            );
        }

        assert_public_list_empty(cli, endpoint).await;
        let repeated = cli
            .run(
                endpoint,
                RenderMode::Json,
                arguments(&["kill", "--all"]),
                None,
                "repeated empty kill-all",
            )
            .await;
        let repeated = success_json(&repeated, "repeated empty kill-all");
        assert_eq!(repeated["total"], 0);
        assert_eq!(repeated["succeeded"], json!([]));
        assert_eq!(repeated["failed"], json!([]));
        assert_eq!(repeated["unfinished"], json!([]));
        ids
    }

    async fn assert_public_list_empty(cli: &CliRunner, endpoint: &Endpoint) {
        let output = cli
            .run(
                endpoint,
                RenderMode::Json,
                arguments(&["list"]),
                None,
                "empty public list",
            )
            .await;
        assert_eq!(success_json(&output, "empty public list"), json!([]));
    }

    async fn assert_endpoint_unreachable(cli: &CliRunner, endpoint: &Endpoint) {
        let output = cli
            .run(
                endpoint,
                RenderMode::Json,
                arguments(&["list"]),
                None,
                "stopped daemon endpoint",
            )
            .await;
        assert_diagnostic(&output, 1, "connect_error", None);
    }

    fn arguments(values: &[&str]) -> Vec<OsString> {
        values.iter().map(|value| OsString::from(*value)).collect()
    }

    fn assert_success(output: &Output, label: &str) {
        assert!(output.status.success(), "{label}: expected successful exit");
        assert!(output.stderr.is_empty(), "{label}: stderr must be empty");
    }

    fn assert_exit(output: &Output, expected: i32, label: &str) {
        assert_eq!(
            output.status.code(),
            Some(expected),
            "{label}: unexpected exit code"
        );
    }

    fn success_json(output: &Output, label: &str) -> Value {
        assert_success(output, label);
        serde_json::from_slice(&output.stdout)
            .unwrap_or_else(|_| panic!("{label}: stdout must be one JSON value"))
    }

    fn assert_diagnostic(
        output: &Output,
        expected_exit: i32,
        expected_code: &str,
        expected_id: Option<&str>,
    ) {
        assert_exit(output, expected_exit, "diagnostic");
        assert!(output.stdout.is_empty(), "diagnostic stdout must be empty");
        let value: Value =
            serde_json::from_slice(&output.stderr).expect("diagnostic must be one JSON value");
        assert_eq!(value["code"], expected_code);
        match expected_id {
            Some(id) => assert_eq!(value["sandbox_id"], id),
            None => assert!(value["sandbox_id"].is_null()),
        }
    }

    fn list_contains(list: &Value, sandbox_id: &str) -> bool {
        list.as_array()
            .expect("sandbox list array")
            .iter()
            .any(|entry| entry["id"] == sandbox_id)
    }

    fn bytes_contain(haystack: &[u8], needle: &[u8]) -> bool {
        haystack
            .windows(needle.len())
            .any(|window| window == needle)
    }

    fn assert_directory_empty(path: &Path, label: &str) {
        let mut entries = fs::read_dir(path).unwrap_or_else(|_| panic!("{label}: read directory"));
        assert!(
            entries.next().is_none(),
            "{label}: expected no provider-owned resources"
        );
    }

    fn directory_inventory(path: &Path) -> BTreeMap<String, &'static str> {
        fs::read_dir(path)
            .expect("read inventory directory")
            .map(|entry| {
                let entry = entry.expect("read inventory entry");
                let file_type = entry.file_type().expect("read inventory entry type");
                let kind = if file_type.is_dir() {
                    "directory"
                } else if file_type.is_file() {
                    "file"
                } else {
                    "other"
                };
                (entry.file_name().to_string_lossy().into_owned(), kind)
            })
            .collect()
    }

    fn assert_checkpoint_inventory(root: &Path, sandbox_id: &str, checkpoint_id: &str) {
        let sandbox_directory = root.join(sandbox_id);
        assert_eq!(
            directory_inventory(&sandbox_directory),
            BTreeMap::from([
                ("HEAD".to_string(), "file"),
                (checkpoint_id.to_string(), "directory"),
            ]),
            "checkpoint owner directory must contain only HEAD and its committed checkpoint"
        );
        assert_eq!(
            fs::read_to_string(sandbox_directory.join("HEAD")).expect("read checkpoint HEAD"),
            format!("{checkpoint_id}\n")
        );

        let checkpoint_directory = sandbox_directory.join(checkpoint_id);
        assert_eq!(
            directory_inventory(&checkpoint_directory),
            BTreeMap::from([
                ("mem.diff".to_string(), "file"),
                ("metadata.json".to_string(), "file"),
                ("rootfs.diff".to_string(), "file"),
                ("vmstate.snap".to_string(), "file"),
            ]),
            "committed checkpoint must contain only frozen artifacts and metadata"
        );
        let metadata: Value = serde_json::from_slice(
            &fs::read(checkpoint_directory.join("metadata.json"))
                .expect("read checkpoint metadata"),
        )
        .expect("parse checkpoint metadata");
        assert_eq!(metadata["id"], checkpoint_id);
        assert_eq!(metadata["sandbox_id"], sandbox_id);
        assert_eq!(metadata["backend"], "mock");
        assert!(metadata["parent"].is_null());
        let artifacts = metadata["artifacts"]
            .as_array()
            .expect("checkpoint artifact metadata");
        let artifact_names = artifacts
            .iter()
            .map(|artifact| {
                artifact["name"]
                    .as_str()
                    .expect("checkpoint artifact name")
                    .to_string()
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            artifact_names,
            BTreeSet::from([
                "mem.diff".to_string(),
                "rootfs.diff".to_string(),
                "vmstate.snap".to_string(),
            ])
        );
    }

    fn assert_no_transient_paths(root: &Path) {
        let mut pending = vec![root.to_path_buf()];
        while let Some(directory) = pending.pop() {
            for entry in fs::read_dir(&directory).expect("scan daemon-client state directory") {
                let entry = entry.expect("read daemon-client state entry");
                let path = entry.path();
                let name = entry.file_name();
                let name = name.to_string_lossy();
                let file_type = entry
                    .file_type()
                    .expect("read daemon-client state entry type");
                if file_type.is_dir() {
                    assert_ne!(name, "runtime", "runtime directory must be removed");
                    if name == "runtime-pool" {
                        assert_directory_empty(&path, "runtime pool directory");
                    } else {
                        pending.push(path);
                    }
                } else {
                    assert!(
                        !name.ends_with(".uds") && !name.contains(".tmp"),
                        "transient runtime artifact must be removed"
                    );
                }
            }
        }
    }
}
