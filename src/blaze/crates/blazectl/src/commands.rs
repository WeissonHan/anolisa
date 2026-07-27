// SPDX-License-Identifier: Apache-2.0
//! Typed execution of the frozen remote command surface.

use std::collections::{BTreeSet, HashMap};
use std::io::{Read, Write};

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use hyper::Method;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::task::{Id as TaskId, JoinSet};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::cli::{
    ArgumentError, Command, CreateArgs, ExecArgs, KillArgs, OutputMode, ReadArgs, RollbackArgs,
    SandboxArgs, WriteArgs, validate_checkpoint_id,
};
use crate::client::{BlazeClient, ClientError, RawResponse};
use crate::input::{WriteInputError, load_write_input};
use crate::output::{
    Diagnostic, OutputError, write_checkpoint_list, write_diagnostic, write_exec, write_json,
    write_read, write_sandbox_list, write_text_fields,
};
use crate::protocol::{
    CheckpointListResponse, CheckpointResponse, CleanupResponse, CreateRequest, CreateResponse,
    ExecRequest, ExecResponse, FileRequest, LifecycleResponse, PoolStatusResponse, PruneResponse,
    ReadResponse, RollbackResponse, SandboxSummary,
};
use crate::response::{ResponseError, decode_empty, decode_json};

/// Maximum number of kill-all delete requests in flight.
pub const KILL_ALL_CONCURRENCY: usize = 50;

/// Testable bounded request interface used by the command runner.
#[async_trait]
pub trait Transport: Clone + Send + Sync + 'static {
    /// Send one request without automatic mutation retries.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when bounded transport fails.
    async fn request(
        &self,
        method: Method,
        path: &str,
        body: Vec<u8>,
    ) -> Result<RawResponse, ClientError>;
}

#[async_trait]
impl Transport for BlazeClient {
    async fn request(
        &self,
        method: Method,
        path: &str,
        body: Vec<u8>,
    ) -> Result<RawResponse, ClientError> {
        BlazeClient::request(self, method, path, body).await
    }
}

#[derive(Clone)]
struct CancellableTransport<T> {
    inner: T,
    cancellation: CancellationToken,
}

#[async_trait]
impl<T: Transport> Transport for CancellableTransport<T> {
    async fn request(
        &self,
        method: Method,
        path: &str,
        body: Vec<u8>,
    ) -> Result<RawResponse, ClientError> {
        tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => Err(ClientError::Cancelled),
            result = self.inner.request(method, path, body) => result,
        }
    }
}

/// Execute one remote command and return its frozen process exit code.
///
/// Runtime failures leave stdout untouched when no command data has already
/// been written and emit one best-effort bounded diagnostic to stderr.
#[allow(clippy::too_many_arguments)]
pub async fn execute_remote<T, R, O, E>(
    transport: T,
    cancellation: CancellationToken,
    command: Command,
    mode: OutputMode,
    stdin: &mut R,
    stdin_is_terminal: bool,
    stdout: &mut O,
    stderr: &mut E,
) -> u8
where
    T: Transport,
    R: Read,
    O: Write,
    E: Write,
{
    let transport = CancellableTransport {
        inner: transport,
        cancellation,
    };
    match execute_inner(
        transport,
        command,
        mode,
        stdin,
        stdin_is_terminal,
        stdout,
        stderr,
    )
    .await
    {
        Ok(exit_code) => exit_code,
        Err(failure) => {
            let _ = write_diagnostic(stderr, mode, &failure.diagnostic);
            1
        }
    }
}

async fn execute_inner<T, R, O, E>(
    transport: T,
    command: Command,
    mode: OutputMode,
    stdin: &mut R,
    stdin_is_terminal: bool,
    stdout: &mut O,
    stderr: &mut E,
) -> Result<u8, CommandFailure>
where
    T: Transport,
    R: Read,
    O: Write,
    E: Write,
{
    match command {
        Command::Create(args) => {
            create(&transport, args, mode, stdout).await?;
            Ok(0)
        }
        Command::Exec(args) => exec(&transport, args, mode, stdout, stderr).await,
        Command::List => {
            list(&transport, mode, stdout).await?;
            Ok(0)
        }
        Command::Kill(args) => kill(&transport, args, mode, stdout, stderr).await,
        Command::Hibernate(args) => {
            lifecycle(&transport, args, "hibernate", "hibernate", mode, stdout).await?;
            Ok(0)
        }
        Command::Checkpoint(args) => {
            checkpoint(&transport, args, mode, stdout).await?;
            Ok(0)
        }
        Command::Rollback(args) => {
            rollback(&transport, args, mode, stdout).await?;
            Ok(0)
        }
        Command::Checkpoints(args) => {
            checkpoints(&transport, args, mode, stdout).await?;
            Ok(0)
        }
        Command::PruneCheckpoints(args) => {
            prune_checkpoints(&transport, args, mode, stdout).await?;
            Ok(0)
        }
        Command::Resume(args) => {
            lifecycle(&transport, args, "resume", "resume", mode, stdout).await?;
            Ok(0)
        }
        Command::CleanupDevices => {
            cleanup_devices(&transport, mode, stdout).await?;
            Ok(0)
        }
        Command::PoolStatus => {
            pool_status(&transport, mode, stdout).await?;
            Ok(0)
        }
        Command::Read(args) => {
            read_file(&transport, args, mode, stdout).await?;
            Ok(0)
        }
        Command::Write(args) => {
            write_file(&transport, args, mode, stdin, stdin_is_terminal, stdout).await?;
            Ok(0)
        }
        Command::Version => {
            crate::write_version(stdout, mode)
                .map_err(|_| CommandFailure::output_io("version", None))?;
            Ok(0)
        }
    }
}

async fn create<T: Transport>(
    transport: &T,
    args: CreateArgs,
    mode: OutputMode,
    stdout: &mut impl Write,
) -> Result<(), CommandFailure> {
    let operation = "create";
    let request = CreateRequest {
        id: args.id,
        template: args.template,
    };
    let body = encode_request(operation, args.id, &request)?;
    let response: CreateResponse = request_json(
        transport,
        operation,
        args.id,
        Method::POST,
        "/v1/sandboxes",
        body,
    )
    .await?;
    write_typed(
        stdout,
        mode,
        operation,
        Some(response.id),
        &response,
        &[
            ("ID", response.id.to_string()),
            ("STATUS", response.status.clone()),
            ("TEMPLATE", display_optional(&response.template).to_string()),
        ],
    )
}

async fn exec<T: Transport>(
    transport: &T,
    args: ExecArgs,
    mode: OutputMode,
    stdout: &mut impl Write,
    stderr: &mut impl Write,
) -> Result<u8, CommandFailure> {
    let operation = "exec";
    let id = args.id;
    let request = ExecRequest {
        cmd: args.cmd,
        cwd: args.cwd,
    };
    let body = encode_request(operation, Some(id), &request)?;
    let path = format!("/v1/sandboxes/{id}/exec");
    let response: ExecResponse =
        request_json(transport, operation, Some(id), Method::POST, &path, body).await?;
    write_exec(stdout, &mut *stderr, mode, &response)
        .map_err(|error| CommandFailure::output(operation, Some(id), error))?;
    match response.exit_code {
        0 => Ok(0),
        1..=125 => Ok(response.exit_code as u8),
        _ => {
            let diagnostic = Diagnostic::local(
                "guest_exit_out_of_range",
                "guest exit code is outside the supported process range",
                operation,
                Some(id),
            );
            let _ = write_diagnostic(stderr, mode, &diagnostic);
            Ok(1)
        }
    }
}

async fn list<T: Transport>(
    transport: &T,
    mode: OutputMode,
    stdout: &mut impl Write,
) -> Result<(), CommandFailure> {
    let operation = "list";
    let response: Vec<SandboxSummary> = request_json(
        transport,
        operation,
        None,
        Method::GET,
        "/v1/sandboxes",
        Vec::new(),
    )
    .await?;
    write_sandbox_list(stdout, mode, &response)
        .map_err(|error| CommandFailure::output(operation, None, error))
}

async fn kill<T: Transport>(
    transport: &T,
    args: KillArgs,
    mode: OutputMode,
    stdout: &mut impl Write,
    stderr: &mut impl Write,
) -> Result<u8, CommandFailure> {
    match args.id {
        Some(id) => {
            let operation = "kill";
            let path = format!("/v1/sandboxes/{id}");
            request_empty(
                transport,
                operation,
                Some(id),
                Method::DELETE,
                &path,
                Vec::new(),
            )
            .await?;
            write_empty_success(stdout, mode, operation, Some(id))?;
            Ok(0)
        }
        None if args.all => kill_all(transport.clone(), mode, stdout, stderr).await,
        None => Err(CommandFailure::internal("kill")),
    }
}

async fn lifecycle<T: Transport>(
    transport: &T,
    args: SandboxArgs,
    operation: &'static str,
    route: &'static str,
    mode: OutputMode,
    stdout: &mut impl Write,
) -> Result<(), CommandFailure> {
    let id = args.id;
    let path = format!("/v1/sandboxes/{id}/{route}");
    let response: LifecycleResponse = request_json(
        transport,
        operation,
        Some(id),
        Method::POST,
        &path,
        Vec::new(),
    )
    .await?;
    write_typed(
        stdout,
        mode,
        operation,
        Some(id),
        &response,
        &[("STATUS", response.status.clone())],
    )
}

async fn checkpoint<T: Transport>(
    transport: &T,
    args: SandboxArgs,
    mode: OutputMode,
    stdout: &mut impl Write,
) -> Result<(), CommandFailure> {
    let operation = "checkpoint";
    let id = args.id;
    let path = format!("/v1/sandboxes/{id}/checkpoint");
    let response: CheckpointResponse = request_json(
        transport,
        operation,
        Some(id),
        Method::POST,
        &path,
        Vec::new(),
    )
    .await?;
    write_typed(
        stdout,
        mode,
        operation,
        Some(id),
        &response,
        &[
            ("STATUS", response.status.clone()),
            ("CHECKPOINT", response.checkpoint_id.clone()),
        ],
    )
}

async fn rollback<T: Transport>(
    transport: &T,
    args: RollbackArgs,
    mode: OutputMode,
    stdout: &mut impl Write,
) -> Result<(), CommandFailure> {
    let operation = "rollback";
    let id = args.id;
    let checkpoint = validate_checkpoint_id(&args.checkpoint)
        .map_err(|error| CommandFailure::argument(operation, Some(id), error))?;
    let path = format!("/v1/sandboxes/{id}/rollback/{checkpoint}");
    let response: RollbackResponse = request_json(
        transport,
        operation,
        Some(id),
        Method::POST,
        &path,
        Vec::new(),
    )
    .await?;
    write_typed(
        stdout,
        mode,
        operation,
        Some(id),
        &response,
        &[
            ("STATUS", response.status.clone()),
            ("CHECKPOINT", response.checkpoint.clone()),
        ],
    )
}

async fn checkpoints<T: Transport>(
    transport: &T,
    args: SandboxArgs,
    mode: OutputMode,
    stdout: &mut impl Write,
) -> Result<(), CommandFailure> {
    let operation = "checkpoints";
    let id = args.id;
    let path = format!("/v1/sandboxes/{id}/checkpoints");
    let response: CheckpointListResponse = request_json(
        transport,
        operation,
        Some(id),
        Method::GET,
        &path,
        Vec::new(),
    )
    .await?;
    write_checkpoint_list(stdout, mode, &response)
        .map_err(|error| CommandFailure::output(operation, Some(id), error))
}

async fn prune_checkpoints<T: Transport>(
    transport: &T,
    args: SandboxArgs,
    mode: OutputMode,
    stdout: &mut impl Write,
) -> Result<(), CommandFailure> {
    let operation = "prune-checkpoints";
    let id = args.id;
    let path = format!("/v1/sandboxes/{id}/checkpoints/prune");
    let mut response: PruneResponse = request_json(
        transport,
        operation,
        Some(id),
        Method::POST,
        &path,
        Vec::new(),
    )
    .await?;
    response.removed.sort();
    let removed = display_list(&response.removed);
    write_typed(
        stdout,
        mode,
        operation,
        Some(id),
        &response,
        &[
            ("STATUS", response.status.clone()),
            ("REMOVED_COUNT", response.removed_count.to_string()),
            ("REMOVED", removed),
        ],
    )
}

async fn cleanup_devices<T: Transport>(
    transport: &T,
    mode: OutputMode,
    stdout: &mut impl Write,
) -> Result<(), CommandFailure> {
    let operation = "cleanup-devices";
    let response: CleanupResponse = request_json(
        transport,
        operation,
        None,
        Method::POST,
        "/v1/pool/cleanup",
        Vec::new(),
    )
    .await?;
    write_typed(
        stdout,
        mode,
        operation,
        None,
        &response,
        &[
            ("DESTROYED", response.destroyed.to_string()),
            ("MESSAGE", response.message.clone()),
        ],
    )
}

async fn pool_status<T: Transport>(
    transport: &T,
    mode: OutputMode,
    stdout: &mut impl Write,
) -> Result<(), CommandFailure> {
    let operation = "pool-status";
    let response: PoolStatusResponse = request_json(
        transport,
        operation,
        None,
        Method::GET,
        "/v1/pool/status",
        Vec::new(),
    )
    .await?;
    write_typed(
        stdout,
        mode,
        operation,
        None,
        &response,
        &[
            ("READY", response.ready.to_string()),
            ("CAPACITY", response.capacity.to_string()),
            ("PENDING", response.pending.to_string()),
            ("QUARANTINED", response.quarantined.to_string()),
        ],
    )
}

async fn read_file<T: Transport>(
    transport: &T,
    args: ReadArgs,
    mode: OutputMode,
    stdout: &mut impl Write,
) -> Result<(), CommandFailure> {
    let operation = "read";
    let id = args.id;
    let request = FileRequest {
        path: args.path,
        data_b64: None,
    };
    let body = encode_request(operation, Some(id), &request)?;
    let path = format!("/v1/sandboxes/{id}/read");
    let response: ReadResponse =
        request_json(transport, operation, Some(id), Method::POST, &path, body).await?;
    write_read(stdout, mode, &response)
        .map_err(|error| CommandFailure::output(operation, Some(id), error))
}

async fn write_file<T: Transport>(
    transport: &T,
    args: WriteArgs,
    mode: OutputMode,
    stdin: &mut impl Read,
    stdin_is_terminal: bool,
    stdout: &mut impl Write,
) -> Result<(), CommandFailure> {
    let operation = "write";
    let id = args.id;
    let bytes = load_write_input(args.file.as_deref(), stdin, stdin_is_terminal)
        .map_err(|error| CommandFailure::input(operation, Some(id), error))?;
    let request = FileRequest {
        path: args.path,
        data_b64: Some(BASE64.encode(bytes)),
    };
    let body = encode_request(operation, Some(id), &request)?;
    let path = format!("/v1/sandboxes/{id}/write");
    request_empty(transport, operation, Some(id), Method::POST, &path, body).await?;
    write_empty_success(stdout, mode, operation, Some(id))
}

async fn kill_all<T: Transport>(
    transport: T,
    mode: OutputMode,
    stdout: &mut impl Write,
    stderr: &mut impl Write,
) -> Result<u8, CommandFailure> {
    let operation = "kill";
    let sandboxes: Vec<SandboxSummary> = request_json(
        &transport,
        operation,
        None,
        Method::GET,
        "/v1/sandboxes",
        Vec::new(),
    )
    .await?;
    let mut pending: Vec<_> = sandboxes
        .into_iter()
        .map(|sandbox| sandbox.id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    pending.reverse();
    let total = pending.len();
    let mut tasks = JoinSet::new();
    let mut task_ids = HashMap::<TaskId, Uuid>::new();
    let mut succeeded = Vec::new();
    let mut failed = Vec::new();
    let mut unfinished = Vec::new();

    while tasks.len() < KILL_ALL_CONCURRENCY {
        let Some(id) = pending.pop() else {
            break;
        };
        spawn_kill(&mut tasks, &mut task_ids, transport.clone(), id);
    }

    while let Some(joined) = tasks.join_next_with_id().await {
        match joined {
            Ok((task_id, (returned_id, result))) => {
                let Some(expected_id) = task_ids.remove(&task_id) else {
                    return Err(CommandFailure::internal(operation));
                };
                if returned_id != expected_id {
                    return Err(CommandFailure::internal(operation));
                }
                match result {
                    Ok(()) => succeeded.push(returned_id),
                    Err(failure) if failure.unfinished => unfinished.push(returned_id),
                    Err(_) => failed.push(returned_id),
                }
            }
            Err(error) => {
                let Some(id) = task_ids.remove(&error.id()) else {
                    return Err(CommandFailure::internal(operation));
                };
                if error.is_cancelled() {
                    unfinished.push(id);
                } else {
                    failed.push(id);
                }
            }
        }
        if let Some(id) = pending.pop() {
            spawn_kill(&mut tasks, &mut task_ids, transport.clone(), id);
        }
    }
    if !task_ids.is_empty() || !pending.is_empty() {
        return Err(CommandFailure::internal(operation));
    }

    succeeded.sort();
    failed.sort();
    unfinished.sort();
    let summary = KillAllSummary {
        succeeded,
        failed,
        unfinished,
        total,
    };
    let partial_failure = !summary.failed.is_empty() || !summary.unfinished.is_empty();
    if partial_failure {
        write_kill_summary(stderr, mode, &summary)
            .map_err(|error| CommandFailure::output(operation, None, error))?;
        Ok(1)
    } else {
        write_kill_summary(stdout, mode, &summary)
            .map_err(|error| CommandFailure::output(operation, None, error))?;
        Ok(0)
    }
}

fn spawn_kill<T: Transport>(
    tasks: &mut JoinSet<(Uuid, Result<(), CommandFailure>)>,
    task_ids: &mut HashMap<TaskId, Uuid>,
    transport: T,
    id: Uuid,
) {
    let handle = tasks.spawn(async move {
        let path = format!("/v1/sandboxes/{id}");
        let result = request_empty(
            &transport,
            "kill",
            Some(id),
            Method::DELETE,
            &path,
            Vec::new(),
        )
        .await;
        (id, result)
    });
    task_ids.insert(handle.id(), id);
}

#[derive(Debug, Serialize)]
struct EmptySuccess {
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct KillAllSummary {
    succeeded: Vec<Uuid>,
    failed: Vec<Uuid>,
    unfinished: Vec<Uuid>,
    total: usize,
}

fn write_empty_success(
    stdout: &mut impl Write,
    mode: OutputMode,
    operation: &'static str,
    sandbox_id: Option<Uuid>,
) -> Result<(), CommandFailure> {
    write_typed(
        stdout,
        mode,
        operation,
        sandbox_id,
        &EmptySuccess { status: "ok" },
        &[("STATUS", "ok".to_string())],
    )
}

fn write_kill_summary(
    writer: &mut impl Write,
    mode: OutputMode,
    summary: &KillAllSummary,
) -> Result<(), OutputError> {
    if mode == OutputMode::Json {
        return write_json(writer, summary);
    }
    write_text_fields(
        writer,
        &[
            ("SUCCEEDED", display_uuid_list(&summary.succeeded)),
            ("FAILED", display_uuid_list(&summary.failed)),
            ("UNFINISHED", display_uuid_list(&summary.unfinished)),
            ("TOTAL", summary.total.to_string()),
        ],
    )
}

fn write_typed<T: Serialize>(
    stdout: &mut impl Write,
    mode: OutputMode,
    operation: &'static str,
    sandbox_id: Option<Uuid>,
    response: &T,
    fields: &[(&'static str, String)],
) -> Result<(), CommandFailure> {
    let result = match mode {
        OutputMode::Text => write_text_fields(stdout, fields),
        OutputMode::Json => write_json(stdout, response),
    };
    result.map_err(|error| CommandFailure::output(operation, sandbox_id, error))
}

fn encode_request(
    operation: &'static str,
    sandbox_id: Option<Uuid>,
    value: &impl Serialize,
) -> Result<Vec<u8>, CommandFailure> {
    serde_json::to_vec(value).map_err(|_| CommandFailure::protocol(operation, sandbox_id))
}

async fn request_json<T: Transport, R: DeserializeOwned>(
    transport: &T,
    operation: &'static str,
    sandbox_id: Option<Uuid>,
    method: Method,
    path: &str,
    body: Vec<u8>,
) -> Result<R, CommandFailure> {
    let response = transport
        .request(method, path, body)
        .await
        .map_err(|error| CommandFailure::client(operation, sandbox_id, error))?;
    decode_json(response).map_err(|error| CommandFailure::response(operation, sandbox_id, error))
}

async fn request_empty<T: Transport>(
    transport: &T,
    operation: &'static str,
    sandbox_id: Option<Uuid>,
    method: Method,
    path: &str,
    body: Vec<u8>,
) -> Result<(), CommandFailure> {
    let response = transport
        .request(method, path, body)
        .await
        .map_err(|error| CommandFailure::client(operation, sandbox_id, error))?;
    decode_empty(response).map_err(|error| CommandFailure::response(operation, sandbox_id, error))
}

#[derive(Debug)]
struct CommandFailure {
    diagnostic: Diagnostic,
    unfinished: bool,
}

impl CommandFailure {
    fn client(operation: &'static str, sandbox_id: Option<Uuid>, error: ClientError) -> Self {
        let unfinished = matches!(
            &error,
            ClientError::ConnectTimeout | ClientError::RequestTimeout | ClientError::Cancelled
        );
        let (code, message) = match error {
            ClientError::ConnectTimeout => ("connect_timeout", "daemon connection timed out"),
            ClientError::Connect { .. } => ("connect_error", "could not connect to the daemon"),
            ClientError::RequestTimeout => ("request_timeout", "daemon request timed out"),
            ClientError::Cancelled => ("cancelled", "daemon request was cancelled"),
            ClientError::ResponseTooLarge => (
                "response_too_large",
                "daemon response exceeded the configured size limit",
            ),
            ClientError::InvalidPath
            | ClientError::RequestBuildInvariant
            | ClientError::Handshake { .. }
            | ClientError::RequestBuild { .. }
            | ClientError::Request { .. } => ("protocol_error", "daemon HTTP exchange failed"),
        };
        Self {
            diagnostic: Diagnostic::local(code, message, operation, sandbox_id),
            unfinished,
        }
    }

    fn response(operation: &'static str, sandbox_id: Option<Uuid>, error: ResponseError) -> Self {
        let diagnostic = match error {
            ResponseError::Daemon { response, .. } => Diagnostic::daemon(operation, &response),
            ResponseError::HttpStatus { .. } => Diagnostic::local(
                "daemon_error",
                "daemon rejected the request",
                operation,
                sandbox_id,
            ),
            ResponseError::UnexpectedEmpty { .. }
            | ResponseError::UnexpectedBody { .. }
            | ResponseError::UnexpectedSuccessStatus { .. }
            | ResponseError::MalformedJson { .. } => Diagnostic::local(
                "protocol_error",
                "daemon returned an unexpected response",
                operation,
                sandbox_id,
            ),
        };
        Self {
            diagnostic,
            unfinished: false,
        }
    }

    fn output(operation: &'static str, sandbox_id: Option<Uuid>, _error: OutputError) -> Self {
        Self::output_io(operation, sandbox_id)
    }

    fn output_io(operation: &'static str, sandbox_id: Option<Uuid>) -> Self {
        Self {
            diagnostic: Diagnostic::local(
                "output_error",
                "failed to write command output",
                operation,
                sandbox_id,
            ),
            unfinished: false,
        }
    }

    fn input(operation: &'static str, sandbox_id: Option<Uuid>, error: WriteInputError) -> Self {
        let (code, message) = match error {
            WriteInputError::TerminalStdin => (
                "input_required",
                "write input is required; use --file PATH or pipe stdin",
            ),
            WriteInputError::Open { .. } => ("input_open_error", "could not open write input"),
            WriteInputError::Read { .. } => ("input_read_error", "could not read write input"),
            WriteInputError::TooLarge => (
                "input_too_large",
                "write input exceeded the configured size limit",
            ),
        };
        Self {
            diagnostic: Diagnostic::local(code, message, operation, sandbox_id),
            unfinished: false,
        }
    }

    fn argument(operation: &'static str, sandbox_id: Option<Uuid>, error: ArgumentError) -> Self {
        let (code, message) = match error {
            ArgumentError::InvalidCheckpointId => (
                "invalid_checkpoint",
                "identifier must use ckpt-<uuid> format",
            ),
        };
        Self {
            diagnostic: Diagnostic::local(code, message, operation, sandbox_id),
            unfinished: false,
        }
    }

    fn protocol(operation: &'static str, sandbox_id: Option<Uuid>) -> Self {
        Self {
            diagnostic: Diagnostic::local(
                "protocol_error",
                "could not encode daemon request",
                operation,
                sandbox_id,
            ),
            unfinished: false,
        }
    }

    fn internal(operation: &'static str) -> Self {
        Self {
            diagnostic: Diagnostic::local(
                "internal_error",
                "command execution invariant failed",
                operation,
                None,
            ),
            unfinished: false,
        }
    }
}

fn display_optional(value: &str) -> &str {
    if value.is_empty() { "-" } else { value }
}

fn display_list(values: &[String]) -> String {
    if values.is_empty() {
        "-".to_string()
    } else {
        values.join(",")
    }
}

fn display_uuid_list(values: &[Uuid]) -> String {
    if values.is_empty() {
        "-".to_string()
    } else {
        values
            .iter()
            .map(Uuid::to_string)
            .collect::<Vec<_>>()
            .join(",")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet, VecDeque};
    use std::io::{self, Cursor};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use clap::Parser;
    use hyper::StatusCode;
    use hyper::header::HeaderMap;
    use serde_json::{Value, json};
    use tokio::sync::Notify;

    use super::*;

    const ID: &str = "00000000-0000-4000-8000-000000000001";
    const CHECKPOINT: &str = "ckpt-00000000-0000-4000-8000-000000000002";

    #[test]
    fn concurrency_bound_matches_the_frozen_contract() {
        assert_eq!(KILL_ALL_CONCURRENCY, 50);
    }

    #[test]
    fn display_helpers_are_deterministic() {
        assert_eq!(display_list(&[]), "-");
        assert_eq!(display_list(&["b".into(), "a".into()]), "b,a");
        assert_eq!(display_uuid_list(&[]), "-");
    }

    #[test]
    fn kill_all_text_summary_has_frozen_field_order() {
        let summary = KillAllSummary {
            succeeded: vec![id(1)],
            failed: vec![id(2)],
            unfinished: vec![id(3)],
            total: 3,
        };
        let mut output = Vec::new();
        write_kill_summary(&mut output, OutputMode::Text, &summary).expect("text summary");
        assert_eq!(
            output,
            concat!(
                "SUCCEEDED\t00000000-0000-0000-0000-000000000001\n",
                "FAILED\t00000000-0000-0000-0000-000000000002\n",
                "UNFINISHED\t00000000-0000-0000-0000-000000000003\n",
                "TOTAL\t3\n"
            )
            .as_bytes()
        );
    }

    #[test]
    fn daemon_status_is_not_used_as_output_data() {
        assert!(StatusCode::INTERNAL_SERVER_ERROR.is_server_error());
    }

    #[tokio::test]
    async fn fourteen_command_wire_matrix_is_exact() {
        assert_case(
            &["blazectl", "create", ID, "--template", "base"],
            raw_json(
                StatusCode::CREATED,
                json!({"id": ID, "status": "running", "template": "base"}),
            ),
            Method::POST,
            "/v1/sandboxes",
            Some(json!({"id": ID, "template": "base"})),
            &[],
        )
        .await;
        assert_case(
            &["blazectl", "exec", ID, "printf sentinel"],
            raw_json(
                StatusCode::OK,
                json!({"exit_code": 0, "stdout": "ok", "stderr": ""}),
            ),
            Method::POST,
            &format!("/v1/sandboxes/{ID}/exec"),
            Some(json!({"cmd": "printf sentinel"})),
            &[],
        )
        .await;
        assert_case(
            &["blazectl", "list"],
            raw_json(StatusCode::OK, json!([sandbox_value(id(1))])),
            Method::GET,
            "/v1/sandboxes",
            None,
            &[],
        )
        .await;
        assert_case(
            &["blazectl", "kill", ID],
            raw_empty(StatusCode::NO_CONTENT),
            Method::DELETE,
            &format!("/v1/sandboxes/{ID}"),
            None,
            &[],
        )
        .await;
        assert_case(
            &["blazectl", "hibernate", ID],
            raw_json(StatusCode::OK, json!({"status": "hibernated"})),
            Method::POST,
            &format!("/v1/sandboxes/{ID}/hibernate"),
            None,
            &[],
        )
        .await;
        assert_case(
            &["blazectl", "checkpoint", ID],
            raw_json(
                StatusCode::OK,
                json!({"status": "checkpointed", "checkpoint_id": CHECKPOINT}),
            ),
            Method::POST,
            &format!("/v1/sandboxes/{ID}/checkpoint"),
            None,
            &[],
        )
        .await;
        assert_case(
            &["blazectl", "rollback", ID, CHECKPOINT],
            raw_json(
                StatusCode::OK,
                json!({"status": "rolledback", "checkpoint": CHECKPOINT}),
            ),
            Method::POST,
            &format!("/v1/sandboxes/{ID}/rollback/{CHECKPOINT}"),
            None,
            &[],
        )
        .await;
        assert_case(
            &["blazectl", "checkpoints", ID],
            raw_json(StatusCode::OK, json!({"checkpoints": []})),
            Method::GET,
            &format!("/v1/sandboxes/{ID}/checkpoints"),
            None,
            &[],
        )
        .await;
        assert_case(
            &["blazectl", "prune-checkpoints", ID],
            raw_json(
                StatusCode::OK,
                json!({"status": "pruned", "removed_count": 0, "removed": []}),
            ),
            Method::POST,
            &format!("/v1/sandboxes/{ID}/checkpoints/prune"),
            None,
            &[],
        )
        .await;
        assert_case(
            &["blazectl", "resume", ID],
            raw_json(StatusCode::OK, json!({"status": "running"})),
            Method::POST,
            &format!("/v1/sandboxes/{ID}/resume"),
            None,
            &[],
        )
        .await;
        assert_case(
            &["blazectl", "cleanup-devices"],
            raw_json(
                StatusCode::OK,
                json!({"destroyed": 2, "message": "pool drained"}),
            ),
            Method::POST,
            "/v1/pool/cleanup",
            None,
            &[],
        )
        .await;
        assert_case(
            &["blazectl", "pool-status"],
            raw_json(
                StatusCode::OK,
                json!({"ready": 2, "capacity": 4, "pending": 1, "quarantined": 0}),
            ),
            Method::GET,
            "/v1/pool/status",
            None,
            &[],
        )
        .await;
        assert_case(
            &["blazectl", "read", ID, "/tmp/data.bin"],
            raw_json(StatusCode::OK, json!({"data_b64": "AAEC"})),
            Method::POST,
            &format!("/v1/sandboxes/{ID}/read"),
            Some(json!({"path": "/tmp/data.bin"})),
            &[],
        )
        .await;
        assert_case(
            &["blazectl", "write", ID, "/tmp/data.bin"],
            raw_empty(StatusCode::NO_CONTENT),
            Method::POST,
            &format!("/v1/sandboxes/{ID}/write"),
            Some(json!({"path": "/tmp/data.bin", "data_b64": "AAEC"})),
            &[0, 1, 2],
        )
        .await;
    }

    #[tokio::test]
    async fn structured_daemon_error_is_stderr_only_and_non_reflecting() {
        let response = raw_json(
            StatusCode::NOT_FOUND,
            json!({
                "code": "not_found",
                "message": "HOST_MESSAGE_SENTINEL",
                "operation": "HOST_OPERATION_SENTINEL",
                "sandbox_id": null
            }),
        );
        let (exit, stdout, stderr, requests) = run(
            &["blazectl", "list"],
            QueueTransport::new([response]),
            &[],
            false,
        )
        .await;
        assert_eq!(exit, 1);
        assert!(stdout.is_empty());
        assert_eq!(requests.len(), 1);
        assert_eq!(
            parse_json(&stderr),
            json!({
                "code": "not_found",
                "message": "requested resource was not found",
                "operation": "list",
                "sandbox_id": null
            })
        );
        assert!(
            !stderr
                .windows(21)
                .any(|value| value == b"HOST_MESSAGE_SENTINEL")
        );
        assert!(
            !stderr
                .windows(23)
                .any(|value| value == b"HOST_OPERATION_SENTINEL")
        );
    }

    #[tokio::test]
    async fn guest_exit_matrix_preserves_streams_and_process_codes() {
        for (guest_exit, expected_exit, diagnostic) in [(7, 7, false), (126, 1, true)] {
            let response = raw_json(
                StatusCode::OK,
                json!({"exit_code": guest_exit, "stdout": "out", "stderr": "err"}),
            );
            let (exit, stdout, stderr, _) = run(
                &["blazectl", "exec", ID, "printf sentinel"],
                QueueTransport::new([response]),
                &[],
                false,
            )
            .await;
            assert_eq!(exit, expected_exit);
            assert_eq!(parse_json(&stdout)["exit_code"], guest_exit);
            if diagnostic {
                assert_eq!(parse_json(&stderr)["code"], "guest_exit_out_of_range");
            } else {
                assert!(stderr.is_empty());
            }
        }
    }

    #[tokio::test]
    async fn invalid_checkpoint_and_terminal_stdin_make_no_request() {
        let (exit, stdout, stderr, requests) = run(
            &["blazectl", "rollback", ID, "../checkpoint"],
            QueueTransport::new([]),
            &[],
            false,
        )
        .await;
        assert_eq!(exit, 1);
        assert!(stdout.is_empty());
        assert!(requests.is_empty());
        assert_eq!(parse_json(&stderr)["code"], "invalid_checkpoint");
        assert!(!stderr.windows(13).any(|value| value == b"../checkpoint"));

        let (exit, stdout, stderr, requests) = run(
            &["blazectl", "write", ID, "/tmp/data.bin"],
            QueueTransport::new([]),
            &[],
            true,
        )
        .await;
        assert_eq!(exit, 1);
        assert!(stdout.is_empty());
        assert!(requests.is_empty());
        assert_eq!(parse_json(&stderr)["code"], "input_required");
    }

    #[tokio::test]
    async fn output_write_failure_returns_one_and_emits_a_stable_diagnostic() {
        let cli = crate::cli::Cli::try_parse_from(["blazectl", "list"]).expect("CLI");
        let transport = QueueTransport::new([raw_json(StatusCode::OK, json!([]))]);
        let mut stdin = Cursor::new(Vec::<u8>::new());
        let mut stderr = Vec::new();
        let exit = execute_remote(
            transport,
            CancellationToken::new(),
            cli.command,
            OutputMode::Json,
            &mut stdin,
            false,
            &mut FailingWriter,
            &mut stderr,
        )
        .await;
        assert_eq!(exit, 1);
        assert_eq!(parse_json(&stderr)["code"], "output_error");
    }

    #[tokio::test]
    async fn kill_all_boundaries_never_exceed_fifty() {
        for size in [0, 1, 49, 50, 51] {
            let transport = KillTransport::new(size, [], []);
            let (exit, stdout, stderr, _) = run(
                &["blazectl", "kill", "--all"],
                transport.clone(),
                &[],
                false,
            )
            .await;
            assert_eq!(exit, 0, "size={size}");
            assert!(stderr.is_empty(), "size={size}");
            let summary = parse_json(&stdout);
            assert_eq!(summary["total"], size);
            assert_eq!(
                summary["succeeded"],
                Value::Array((1..=size).map(|value| json!(id(value as u128))).collect())
            );
            assert_eq!(summary["failed"], json!([]));
            assert_eq!(summary["unfinished"], json!([]));
            let snapshot = transport.snapshot();
            assert_eq!(snapshot.attempted.len(), size);
            assert!(snapshot.max_concurrency <= KILL_ALL_CONCURRENCY);
            if size == 51 {
                assert_eq!(snapshot.max_concurrency, KILL_ALL_CONCURRENCY);
            }
        }
    }

    #[tokio::test]
    async fn kill_all_attempts_every_target_after_failure_or_task_panic() {
        for panic_task in [false, true] {
            let failed_id = id(26);
            let failures = if panic_task { vec![] } else { vec![failed_id] };
            let panics = if panic_task { vec![failed_id] } else { vec![] };
            let transport = KillTransport::new(51, failures, panics);
            let (exit, stdout, stderr, _) = run(
                &["blazectl", "kill", "--all"],
                transport.clone(),
                &[],
                false,
            )
            .await;
            assert_eq!(exit, 1);
            assert!(stdout.is_empty());
            let summary = parse_json(&stderr);
            assert_eq!(summary["total"], 51);
            assert_eq!(summary["failed"], json!([failed_id]));
            let snapshot = transport.snapshot();
            assert_eq!(snapshot.attempted.len(), 51);
            assert!(snapshot.max_concurrency <= KILL_ALL_CONCURRENCY);
        }
    }

    #[tokio::test]
    async fn kill_all_timeout_is_unfinished_and_does_not_stop_other_targets() {
        let unfinished_id = id(26);
        let transport = KillTransport::new(51, [], []).with_timeouts([unfinished_id]);
        let (exit, stdout, stderr, _) = run(
            &["blazectl", "kill", "--all"],
            transport.clone(),
            &[],
            false,
        )
        .await;
        assert_eq!(exit, 1);
        assert!(stdout.is_empty());
        let summary = parse_json(&stderr);
        assert_eq!(summary["total"], 51);
        assert_eq!(summary["failed"], json!([]));
        assert_eq!(summary["unfinished"], json!([unfinished_id]));
        assert_eq!(transport.snapshot().attempted.len(), 51);
    }

    #[tokio::test]
    async fn kill_all_sorts_summary_after_reverse_completion_order() {
        let transport = KillTransport::new(5, [], []).with_delays(
            (1_u128..=5).map(|value| (id(value), Duration::from_millis((6 - value) as u64 * 20))),
        );
        let (exit, stdout, stderr, _) = run(
            &["blazectl", "kill", "--all"],
            transport.clone(),
            &[],
            false,
        )
        .await;
        assert_eq!(exit, 0);
        assert!(stderr.is_empty());
        let summary = parse_json(&stdout);
        assert_eq!(
            summary["succeeded"],
            Value::Array((1_u128..=5).map(|value| json!(id(value))).collect())
        );
        assert_eq!(
            transport.snapshot().completed,
            (1_u128..=5).rev().map(id).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn cancellation_is_stderr_only_and_makes_no_request() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let (exit, stdout, stderr, requests) = run_with_cancellation(
            &["blazectl", "list"],
            QueueTransport::new([]),
            &[],
            false,
            cancellation,
        )
        .await;
        assert_eq!(exit, 1);
        assert!(stdout.is_empty());
        assert!(requests.is_empty());
        assert_eq!(parse_json(&stderr)["code"], "cancelled");
    }

    #[tokio::test]
    async fn version_runner_is_local_even_when_transport_is_cancelled() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let (exit, stdout, stderr, requests) = run_with_cancellation(
            &["blazectl", "version"],
            QueueTransport::new([]),
            &[],
            false,
            cancellation,
        )
        .await;
        assert_eq!(exit, 0);
        assert_eq!(parse_json(&stdout)["name"], "blazectl");
        assert!(stderr.is_empty());
        assert!(requests.is_empty());
    }

    #[tokio::test]
    async fn kill_all_cancellation_accounts_for_every_target_as_unfinished() {
        let transport = KillTransport::new(51, [], []);
        let cancellation = CancellationToken::new();
        let cancel_when_started = {
            let started = transport.started.clone();
            let cancellation = cancellation.clone();
            tokio::spawn(async move {
                started.notified().await;
                cancellation.cancel();
            })
        };
        let (exit, stdout, stderr, _) = run_with_cancellation(
            &["blazectl", "kill", "--all"],
            transport,
            &[],
            false,
            cancellation,
        )
        .await;
        cancel_when_started.await.expect("cancellation task");
        assert_eq!(exit, 1);
        assert!(stdout.is_empty());
        let summary = parse_json(&stderr);
        assert_eq!(summary["total"], 51);
        assert_eq!(summary["succeeded"], json!([]));
        assert_eq!(summary["failed"], json!([]));
        assert_eq!(summary["unfinished"].as_array().map(Vec::len), Some(51));
    }

    async fn assert_case(
        argv: &[&str],
        response: RawResponse,
        method: Method,
        path: &str,
        expected_body: Option<Value>,
        stdin: &[u8],
    ) {
        let (exit, stdout, stderr, requests) =
            run(argv, QueueTransport::new([response]), stdin, false).await;
        assert_eq!(exit, 0, "{argv:?}");
        assert!(stderr.is_empty(), "{argv:?}");
        assert!(serde_json::from_slice::<Value>(&stdout).is_ok(), "{argv:?}");
        assert_eq!(requests.len(), 1, "{argv:?}");
        assert_eq!(requests[0].method, method, "{argv:?}");
        assert_eq!(requests[0].path, path, "{argv:?}");
        match expected_body {
            Some(expected) => {
                assert_eq!(parse_json(&requests[0].body), expected, "{argv:?}");
            }
            None => assert!(requests[0].body.is_empty(), "{argv:?}"),
        }
    }

    async fn run<T>(
        argv: &[&str],
        transport: T,
        stdin: &[u8],
        stdin_is_terminal: bool,
    ) -> (u8, Vec<u8>, Vec<u8>, Vec<RequestRecord>)
    where
        T: RecordedTransport,
    {
        run_with_cancellation(
            argv,
            transport,
            stdin,
            stdin_is_terminal,
            CancellationToken::new(),
        )
        .await
    }

    async fn run_with_cancellation<T>(
        argv: &[&str],
        transport: T,
        stdin: &[u8],
        stdin_is_terminal: bool,
        cancellation: CancellationToken,
    ) -> (u8, Vec<u8>, Vec<u8>, Vec<RequestRecord>)
    where
        T: RecordedTransport,
    {
        let cli = crate::cli::Cli::try_parse_from(argv).expect("CLI");
        let mut stdin = Cursor::new(stdin.to_vec());
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let exit = execute_remote(
            transport.clone(),
            cancellation,
            cli.command,
            OutputMode::Json,
            &mut stdin,
            stdin_is_terminal,
            &mut stdout,
            &mut stderr,
        )
        .await;
        (exit, stdout, stderr, transport.requests())
    }

    fn raw_json(status: StatusCode, value: Value) -> RawResponse {
        RawResponse {
            status,
            headers: HeaderMap::new(),
            body: serde_json::to_vec(&value).expect("JSON body"),
        }
    }

    fn raw_empty(status: StatusCode) -> RawResponse {
        RawResponse {
            status,
            headers: HeaderMap::new(),
            body: Vec::new(),
        }
    }

    fn parse_json(bytes: &[u8]) -> Value {
        serde_json::from_slice(bytes).expect("JSON value")
    }

    fn sandbox_value(id: Uuid) -> Value {
        json!({
            "id": id,
            "state": "running",
            "template_name": "base",
            "created_at": "2026-01-01T00:00:00Z"
        })
    }

    fn id(value: u128) -> Uuid {
        Uuid::from_u128(value)
    }

    #[derive(Debug, Clone)]
    struct RequestRecord {
        method: Method,
        path: String,
        body: Vec<u8>,
    }

    trait RecordedTransport: Transport {
        fn requests(&self) -> Vec<RequestRecord>;
    }

    #[derive(Clone)]
    struct QueueTransport {
        state: Arc<Mutex<QueueState>>,
    }

    struct QueueState {
        responses: VecDeque<RawResponse>,
        requests: Vec<RequestRecord>,
    }

    impl QueueTransport {
        fn new(responses: impl IntoIterator<Item = RawResponse>) -> Self {
            Self {
                state: Arc::new(Mutex::new(QueueState {
                    responses: responses.into_iter().collect(),
                    requests: Vec::new(),
                })),
            }
        }
    }

    #[async_trait]
    impl Transport for QueueTransport {
        async fn request(
            &self,
            method: Method,
            path: &str,
            body: Vec<u8>,
        ) -> Result<RawResponse, ClientError> {
            let mut state = self.state.lock().expect("queue lock");
            state.requests.push(RequestRecord {
                method,
                path: path.to_string(),
                body,
            });
            state
                .responses
                .pop_front()
                .ok_or_else(|| ClientError::Connect {
                    source: io::Error::other("no queued response"),
                })
        }
    }

    impl RecordedTransport for QueueTransport {
        fn requests(&self) -> Vec<RequestRecord> {
            self.state.lock().expect("queue lock").requests.clone()
        }
    }

    #[derive(Clone)]
    struct KillTransport {
        ids: Arc<Vec<Uuid>>,
        failures: Arc<BTreeSet<Uuid>>,
        panics: Arc<BTreeSet<Uuid>>,
        timeouts: Arc<BTreeSet<Uuid>>,
        delays: Arc<BTreeMap<Uuid, Duration>>,
        state: Arc<Mutex<KillState>>,
        started: Arc<Notify>,
    }

    #[derive(Debug, Clone, Default)]
    struct KillState {
        current: usize,
        max_concurrency: usize,
        attempted: Vec<Uuid>,
        completed: Vec<Uuid>,
        requests: Vec<RequestRecord>,
    }

    impl KillTransport {
        fn new(
            size: usize,
            failures: impl IntoIterator<Item = Uuid>,
            panics: impl IntoIterator<Item = Uuid>,
        ) -> Self {
            Self {
                ids: Arc::new((1..=size).map(|value| id(value as u128)).collect()),
                failures: Arc::new(failures.into_iter().collect()),
                panics: Arc::new(panics.into_iter().collect()),
                timeouts: Arc::new(BTreeSet::new()),
                delays: Arc::new(BTreeMap::new()),
                state: Arc::new(Mutex::new(KillState::default())),
                started: Arc::new(Notify::new()),
            }
        }

        fn with_timeouts(mut self, timeouts: impl IntoIterator<Item = Uuid>) -> Self {
            self.timeouts = Arc::new(timeouts.into_iter().collect());
            self
        }

        fn with_delays(mut self, delays: impl IntoIterator<Item = (Uuid, Duration)>) -> Self {
            self.delays = Arc::new(delays.into_iter().collect());
            self
        }

        fn snapshot(&self) -> KillState {
            self.state.lock().expect("kill state lock").clone()
        }
    }

    #[async_trait]
    impl Transport for KillTransport {
        async fn request(
            &self,
            method: Method,
            path: &str,
            body: Vec<u8>,
        ) -> Result<RawResponse, ClientError> {
            {
                let mut state = self.state.lock().expect("kill state lock");
                state.requests.push(RequestRecord {
                    method: method.clone(),
                    path: path.to_string(),
                    body,
                });
            }
            if method == Method::GET && path == "/v1/sandboxes" {
                return Ok(raw_json(
                    StatusCode::OK,
                    Value::Array(self.ids.iter().copied().map(sandbox_value).collect()),
                ));
            }
            assert_eq!(method, Method::DELETE);
            let sandbox_id =
                Uuid::parse_str(path.rsplit('/').next().expect("sandbox path segment"))
                    .expect("sandbox UUID");
            {
                let mut state = self.state.lock().expect("kill state lock");
                state.current += 1;
                state.max_concurrency = state.max_concurrency.max(state.current);
                state.attempted.push(sandbox_id);
            }
            self.started.notify_one();
            if self.panics.contains(&sandbox_id) {
                let mut state = self.state.lock().expect("kill state lock");
                state.current -= 1;
                drop(state);
                panic!("injected task panic");
            }
            tokio::time::sleep(
                self.delays
                    .get(&sandbox_id)
                    .copied()
                    .unwrap_or(Duration::from_millis(5)),
            )
            .await;
            {
                let mut state = self.state.lock().expect("kill state lock");
                state.current -= 1;
                state.completed.push(sandbox_id);
            }
            if self.timeouts.contains(&sandbox_id) {
                return Err(ClientError::RequestTimeout);
            }
            if self.failures.contains(&sandbox_id) {
                Ok(raw_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    json!({
                        "code": "internal_error",
                        "message": "failed",
                        "operation": "DELETE /v1/sandboxes",
                        "sandbox_id": sandbox_id
                    }),
                ))
            } else {
                Ok(raw_empty(StatusCode::NO_CONTENT))
            }
        }
    }

    impl RecordedTransport for KillTransport {
        fn requests(&self) -> Vec<RequestRecord> {
            self.state.lock().expect("kill state lock").requests.clone()
        }
    }

    struct FailingWriter;

    impl Write for FailingWriter {
        fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
}
