// SPDX-License-Identifier: Apache-2.0
#![cfg(feature = "integration")]

use std::collections::{BTreeSet, VecDeque};
use std::convert::Infallible;
use std::fmt;
use std::io::{self, Cursor, Read, Write};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use blazectl::cli::{Cli, EndpointSelection, OutputMode};
use blazectl::client::{BlazeClient, ClientConfig, ClientConfigError};
use blazectl::input::MAX_WRITE_BYTES;
use clap::Parser;
use http_body_util::BodyExt;
use hyper::body::{Body, Bytes, Frame, Incoming, SizeHint};
use hyper::header::{ACCEPT, CONTENT_TYPE, HOST};
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use tokio::net::{TcpListener, UnixListener};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const ID: &str = "00000000-0000-4000-8000-000000000001";
const ID_TWO: &str = "00000000-0000-4000-8000-000000000002";
const CHECKPOINT: &str = "ckpt-00000000-0000-4000-8000-000000000002";
const SANDBOX_PATH: &str = "/v1/sandboxes/00000000-0000-4000-8000-000000000001";
const EXEC_PATH: &str = "/v1/sandboxes/00000000-0000-4000-8000-000000000001/exec";
const HIBERNATE_PATH: &str = "/v1/sandboxes/00000000-0000-4000-8000-000000000001/hibernate";
const CHECKPOINT_PATH: &str = "/v1/sandboxes/00000000-0000-4000-8000-000000000001/checkpoint";
const ROLLBACK_PATH: &str = "/v1/sandboxes/00000000-0000-4000-8000-000000000001/rollback/ckpt-00000000-0000-4000-8000-000000000002";
const CHECKPOINTS_PATH: &str = "/v1/sandboxes/00000000-0000-4000-8000-000000000001/checkpoints";
const PRUNE_PATH: &str = "/v1/sandboxes/00000000-0000-4000-8000-000000000001/checkpoints/prune";
const RESUME_PATH: &str = "/v1/sandboxes/00000000-0000-4000-8000-000000000001/resume";
const READ_PATH: &str = "/v1/sandboxes/00000000-0000-4000-8000-000000000001/read";
const WRITE_PATH: &str = "/v1/sandboxes/00000000-0000-4000-8000-000000000001/write";

#[tokio::test]
async fn tcp_fourteen_command_wire_and_json_matrix() {
    let cases = command_cases();
    assert_eq!(cases.len(), 14);
    let expected_requests = cases.len();
    let (client, state, server, authority) =
        spawn_tcp(expected_requests, Arc::new(matrix_response)).await;

    for case in &cases {
        let (exit, stdout, stderr) =
            run_command(client.clone(), &case.argv, OutputMode::Json, &case.stdin).await;
        assert_eq!(exit, 0, "{:?}", case.argv);
        assert!(stderr.is_empty(), "{:?}", case.argv);
        assert_eq!(parse_json(&stdout), case.output, "{:?}", case.argv);
        assert_eq!(
            stdout.iter().filter(|byte| **byte == b'\n').count(),
            1,
            "{:?}",
            case.argv
        );
        assert_eq!(stdout.last(), Some(&b'\n'), "{:?}", case.argv);
    }

    finish_server(server).await;
    let requests = state.requests();
    assert_eq!(requests.len(), expected_requests);
    for (request, case) in requests.iter().zip(&cases) {
        assert_eq!(request.method, case.method, "{:?}", case.argv);
        assert_eq!(request.path, case.path, "{:?}", case.argv);
        assert_eq!(request.host, authority, "{:?}", case.argv);
        assert_eq!(request.accept.as_deref(), Some("application/json"));
        match &case.body {
            Some(expected) => {
                assert_eq!(request.content_type.as_deref(), Some("application/json"));
                let actual = parse_json(&request.body);
                assert_eq!(&actual, expected, "{:?}", case.argv);
            }
            None => {
                assert!(request.content_type.is_none(), "{:?}", case.argv);
                assert!(request.body.is_empty(), "{:?}", case.argv);
            }
        }
    }
}

#[tokio::test]
async fn tcp_fourteen_command_text_output_matrix() {
    let cases = text_cases();
    let wire_cases = command_cases();
    assert_eq!(cases.len(), 14);
    assert_eq!(cases.len(), wire_cases.len());
    for (case, wire_case) in cases.iter().zip(&wire_cases) {
        assert_eq!(case.argv, wire_case.argv);
    }
    let expected_requests = cases.len();
    let (client, state, server, _) = spawn_tcp(expected_requests, Arc::new(matrix_response)).await;

    for case in &cases {
        let (exit, stdout, stderr) =
            run_command(client.clone(), &case.argv, OutputMode::Text, &case.stdin).await;
        assert_eq!(exit, 0, "{:?}", case.argv);
        assert_eq!(stdout, case.stdout, "{:?}", case.argv);
        assert!(stderr.is_empty(), "{:?}", case.argv);
    }

    finish_server(server).await;
    assert_eq!(state.requests().len(), expected_requests);
}

#[tokio::test]
async fn tcp_fourteen_command_daemon_error_matrix_is_stderr_only() {
    let cases = command_cases();
    assert_eq!(cases.len(), 14);
    let responder: Responder =
        Arc::new(|_| structured_error_spec(StatusCode::CONFLICT, "state_conflict"));
    let (client, state, server, _) = spawn_tcp(cases.len(), responder).await;

    for case in &cases {
        let operation = case.argv.get(1).expect("canonical operation");
        let (exit, stdout, stderr) =
            run_command(client.clone(), &case.argv, OutputMode::Json, &case.stdin).await;
        assert_eq!(exit, 1, "{:?}", case.argv);
        assert!(stdout.is_empty(), "{:?}", case.argv);
        assert_eq!(
            parse_json(&stderr),
            json!({
                "code": "state_conflict",
                "message": "request conflicts with the current sandbox state",
                "operation": operation.as_str(),
                "sandbox_id": null
            }),
            "{:?}",
            case.argv
        );
        assert!(
            !String::from_utf8_lossy(&stderr).contains("DAEMON_DETAIL_SENTINEL"),
            "{:?}",
            case.argv
        );
    }

    finish_server(server).await;
    assert_eq!(state.requests().len(), cases.len());
}

#[tokio::test]
async fn tcp_write_stdin_selector_and_empty_input_wire_matrix() {
    let cases = [
        (
            words(&["blazectl", "write", ID, "/tmp/data.bin", "--file", "-"]),
            vec![0, 159, 255, 10],
            true,
            "AJ//Cg==",
        ),
        (
            words(&["blazectl", "write", ID, "/tmp/data.bin"]),
            vec![0, 159, 255, 10],
            false,
            "AJ//Cg==",
        ),
        (
            words(&["blazectl", "write", ID, "/tmp/data.bin", "--file", "-"]),
            Vec::new(),
            true,
            "",
        ),
        (
            words(&["blazectl", "write", ID, "/tmp/data.bin"]),
            Vec::new(),
            false,
            "",
        ),
    ];
    let (client, state, server, _) = spawn_tcp(cases.len(), Arc::new(matrix_response)).await;

    for (argv, stdin, stdin_is_terminal, _) in &cases {
        let (exit, stdout, stderr) = run_command_with_terminal(
            client.clone(),
            argv,
            OutputMode::Json,
            stdin,
            *stdin_is_terminal,
        )
        .await;
        assert_eq!(exit, 0, "{argv:?}");
        assert_eq!(parse_json(&stdout), json!({"status": "ok"}), "{argv:?}");
        assert!(stderr.is_empty(), "{argv:?}");
    }

    finish_server(server).await;
    let requests = state.requests();
    assert_eq!(requests.len(), cases.len());
    for (request, (argv, _, _, expected_data_b64)) in requests.iter().zip(&cases) {
        assert_eq!(request.method, Method::POST, "{argv:?}");
        assert_eq!(request.path, WRITE_PATH, "{argv:?}");
        assert_eq!(
            parse_json(&request.body),
            json!({"path": "/tmp/data.bin", "data_b64": expected_data_b64}),
            "{argv:?}"
        );
    }
}

#[tokio::test]
async fn local_usage_endpoint_input_and_writer_failure_matrix() {
    for argv in [
        vec!["blazectl", "exec", ID],
        vec!["blazectl", "kill", ID, "--all"],
        vec!["blazectl", "--output", "yaml", "list"],
        vec!["blazectl", "kill", "not-a-uuid"],
    ] {
        let error = Cli::try_parse_from(argv).expect_err("usage error");
        assert_eq!(error.exit_code(), 2);
    }

    let endpoint_error =
        ClientConfig::from_selection(EndpointSelection::Http("https://127.0.0.1".to_string()))
            .expect_err("unsupported endpoint scheme");
    assert_eq!(endpoint_error, ClientConfigError::UnsupportedScheme);

    let temp = SocketDirectory::new();
    let missing_socket = temp.path.join("missing.sock");
    let local_client = BlazeClient::new(
        ClientConfig::from_selection(EndpointSelection::Unix(missing_socket.clone()))
            .expect("local failure client"),
    );
    let missing_input = temp.path.join("missing-input.bin");
    let missing_input_text = missing_input.display().to_string();
    let missing_socket_text = missing_socket.display().to_string();
    let mut missing_argv = words(&["blazectl", "write", ID, "/tmp/data.bin", "--file"]);
    missing_argv.push(missing_input_text.clone());
    let (exit, stdout, stderr) =
        run_command(local_client.clone(), &missing_argv, OutputMode::Json, &[]).await;
    assert_eq!(exit, 1);
    assert!(stdout.is_empty());
    assert_eq!(parse_json(&stderr)["code"], "input_open_error");
    assert!(!String::from_utf8_lossy(&stderr).contains(missing_input_text.as_str()));
    assert!(!String::from_utf8_lossy(&stderr).contains(missing_socket_text.as_str()));

    let cli = Cli::try_parse_from(["blazectl", "write", ID, "/tmp/data.bin"]).expect("write CLI");
    let mut oversized = io::repeat(0).take((MAX_WRITE_BYTES + 1) as u64);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = blazectl::commands::execute_remote(
        local_client.clone(),
        CancellationToken::new(),
        cli.command,
        OutputMode::Json,
        &mut oversized,
        false,
        &mut stdout,
        &mut stderr,
    )
    .await;
    assert_eq!(exit, 1);
    assert!(stdout.is_empty());
    assert_eq!(parse_json(&stderr)["code"], "input_too_large");

    let cli = Cli::try_parse_from(["blazectl", "write", ID, "/tmp/data.bin"]).expect("write CLI");
    let mut terminal_stdin = PanicReader;
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = blazectl::commands::execute_remote(
        local_client.clone(),
        CancellationToken::new(),
        cli.command,
        OutputMode::Json,
        &mut terminal_stdin,
        true,
        &mut stdout,
        &mut stderr,
    )
    .await;
    assert_eq!(exit, 1);
    assert!(stdout.is_empty());
    assert_eq!(parse_json(&stderr)["code"], "input_required");

    let cli = Cli::try_parse_from(["blazectl", "write", ID, "/tmp/data.bin"]).expect("write CLI");
    let mut failing_stdin = FailingReader;
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = blazectl::commands::execute_remote(
        local_client,
        CancellationToken::new(),
        cli.command,
        OutputMode::Json,
        &mut failing_stdin,
        false,
        &mut stdout,
        &mut stderr,
    )
    .await;
    assert_eq!(exit, 1);
    assert!(stdout.is_empty());
    assert_eq!(parse_json(&stderr)["code"], "input_read_error");

    let (client, state, server, _) = spawn_tcp(1, Arc::new(matrix_response)).await;
    let cli = Cli::try_parse_from(["blazectl", "list"]).expect("list CLI");
    let mut stdin = Cursor::new(Vec::<u8>::new());
    let mut failing_stdout = FailingWriter;
    let mut stderr = Vec::new();
    let exit = blazectl::commands::execute_remote(
        client,
        CancellationToken::new(),
        cli.command,
        OutputMode::Json,
        &mut stdin,
        false,
        &mut failing_stdout,
        &mut stderr,
    )
    .await;
    assert_eq!(exit, 1);
    assert_eq!(parse_json(&stderr)["code"], "output_error");
    finish_server(server).await;
    assert_eq!(state.requests().len(), 1);

    let responder: Responder =
        Arc::new(|_| structured_error_spec(StatusCode::CONFLICT, "state_conflict"));
    let (client, state, server, _) = spawn_tcp(1, responder).await;
    let cli = Cli::try_parse_from(["blazectl", "list"]).expect("list CLI");
    let mut stdin = Cursor::new(Vec::<u8>::new());
    let mut stdout = Vec::new();
    let mut failing_stderr = FailingWriter;
    let exit = blazectl::commands::execute_remote(
        client,
        CancellationToken::new(),
        cli.command,
        OutputMode::Json,
        &mut stdin,
        false,
        &mut stdout,
        &mut failing_stderr,
    )
    .await;
    assert_eq!(exit, 1);
    assert!(stdout.is_empty());
    finish_server(server).await;
    assert_eq!(state.requests().len(), 1);
}

#[tokio::test]
async fn tcp_guest_path_remains_json_data_not_request_target() {
    let guest_path = "/tmp/data?segment#sentinel";
    let (client, state, server, _) = spawn_tcp(1, Arc::new(matrix_response)).await;
    let (exit, stdout, stderr) = run_command(
        client,
        &words(&["blazectl", "read", ID, guest_path]),
        OutputMode::Json,
        &[],
    )
    .await;
    assert_eq!(exit, 0);
    assert!(stderr.is_empty());
    assert_eq!(parse_json(&stdout), json!({"data_b64": "AAEC"}));

    finish_server(server).await;
    let requests = state.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].path, READ_PATH);
    assert_eq!(parse_json(&requests[0].body), json!({"path": guest_path}));
}

#[tokio::test]
async fn transport_and_protocol_failures_are_stderr_only() {
    let temp = SocketDirectory::new();
    let missing_socket = temp.path.join("missing.sock");
    let missing_socket_text = missing_socket.display().to_string();
    let client = BlazeClient::new(
        ClientConfig::from_selection(EndpointSelection::Unix(missing_socket.clone()))
            .expect("missing UDS config"),
    );
    let (exit, stdout, stderr) =
        run_command(client, &words(&["blazectl", "list"]), OutputMode::Json, &[]).await;
    assert_eq!(exit, 1);
    assert!(stdout.is_empty());
    assert_eq!(parse_json(&stderr)["code"], "connect_error");
    assert!(!String::from_utf8_lossy(&stderr).contains(missing_socket_text.as_str()));

    let malformed: Responder = Arc::new(|_| ResponseSpec {
        status: StatusCode::OK,
        body: ResponseBody::Bytes(b"{".to_vec()),
        delay: Duration::ZERO,
        allow_connection_error: false,
    });
    let (client, _, server, _) = spawn_tcp(1, malformed).await;
    let (exit, stdout, stderr) =
        run_command(client, &words(&["blazectl", "list"]), OutputMode::Json, &[]).await;
    assert_eq!(exit, 1);
    assert!(stdout.is_empty());
    assert_eq!(parse_json(&stderr)["code"], "protocol_error");
    finish_server(server).await;
}

#[tokio::test]
async fn tcp_structured_daemon_status_matrix_is_stderr_only() {
    let cases = [
        (
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "daemon rejected the request",
        ),
        (
            StatusCode::NOT_FOUND,
            "not_found",
            "requested resource was not found",
        ),
        (
            StatusCode::CONFLICT,
            "state_conflict",
            "request conflicts with the current sandbox state",
        ),
        (
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            "daemon rejected an oversized payload",
        ),
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_request",
            "daemon rejected the request",
        ),
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "daemon request failed",
        ),
        (
            StatusCode::BAD_GATEWAY,
            "backend_unavailable",
            "sandbox backend is unavailable",
        ),
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "shutting_down",
            "daemon is shutting down",
        ),
        (
            StatusCode::GATEWAY_TIMEOUT,
            "guest_timeout",
            "guest operation timed out",
        ),
    ];
    assert_eq!(cases.len(), 9);

    for (status, code, expected_message) in cases {
        let responder: Responder = Arc::new(move |_| structured_error_spec(status, code));
        let (client, _, server, _) = spawn_tcp(1, responder).await;
        let (exit, stdout, stderr) =
            run_command(client, &words(&["blazectl", "list"]), OutputMode::Json, &[]).await;

        assert_eq!(exit, 1, "status={status}");
        assert!(stdout.is_empty(), "status={status}");
        assert_eq!(
            parse_json(&stderr),
            json!({
                "code": code,
                "message": expected_message,
                "operation": "list",
                "sandbox_id": null
            }),
            "status={status}"
        );
        assert!(
            !String::from_utf8_lossy(&stderr).contains("DAEMON_DETAIL_SENTINEL"),
            "status={status}"
        );
        finish_server(server).await;
    }
}

#[tokio::test]
async fn tcp_chunk_disconnect_and_unexpected_response_matrix() {
    let response_chunks = vec![
        b"[".to_vec(),
        serde_json::to_vec(&sandbox_value(1)).expect("chunked sandbox"),
        b"]".to_vec(),
    ];
    let chunked: Responder =
        Arc::new(move |_| ResponseSpec::chunked(StatusCode::OK, response_chunks.clone()));
    let (client, _, server, _) = spawn_tcp(1, chunked).await;
    let (exit, stdout, stderr) =
        run_command(client, &words(&["blazectl", "list"]), OutputMode::Json, &[]).await;
    assert_eq!(exit, 0);
    assert!(stderr.is_empty());
    assert_eq!(parse_json(&stdout), json!([sandbox_value(1)]));
    finish_server(server).await;

    let truncated: Responder =
        Arc::new(|_| ResponseSpec::disconnect(StatusCode::OK, b"{".to_vec()));
    let (client, _, server, _) = spawn_tcp(1, truncated).await;
    assert_json_failure(client, &["blazectl", "list"], "protocol_error").await;
    finish_server(server).await;

    let (client, _, server, _) =
        spawn_tcp(1, Arc::new(|_| empty_spec(StatusCode::NO_CONTENT))).await;
    assert_json_failure(client, &["blazectl", "list"], "protocol_error").await;
    finish_server(server).await;

    let (client, _, server, _) = spawn_tcp(1, Arc::new(|_| empty_spec(StatusCode::OK))).await;
    assert_json_failure(client, &["blazectl", "kill", ID], "protocol_error").await;
    finish_server(server).await;

    let non_json: Responder = Arc::new(|_| {
        ResponseSpec::bytes(StatusCode::BAD_GATEWAY, b"DAEMON_DETAIL_SENTINEL".to_vec())
    });
    let (client, _, server, _) = spawn_tcp(1, non_json).await;
    let (exit, stdout, stderr) =
        run_command(client, &words(&["blazectl", "list"]), OutputMode::Json, &[]).await;
    assert_eq!(exit, 1);
    assert!(stdout.is_empty());
    assert_eq!(parse_json(&stderr)["code"], "daemon_error");
    assert!(!String::from_utf8_lossy(&stderr).contains("DAEMON_DETAIL_SENTINEL"));
    finish_server(server).await;
}

#[tokio::test]
async fn tcp_response_bound_timeout_cancellation_and_invalid_base64_matrix() {
    let oversized: Responder =
        Arc::new(|_| ResponseSpec::bytes(StatusCode::OK, b"[12345]".to_vec()));
    let (_, _, server, authority) = spawn_tcp(1, oversized).await;
    let mut config = tcp_config(&authority);
    config.max_response_bytes = 4;
    assert_json_failure(
        BlazeClient::new(config),
        &["blazectl", "list"],
        "response_too_large",
    )
    .await;
    finish_server(server).await;

    let delayed: Responder = Arc::new(|_| ResponseSpec {
        status: StatusCode::OK,
        body: ResponseBody::Bytes(b"[]".to_vec()),
        delay: Duration::from_millis(100),
        allow_connection_error: true,
    });
    let (_, _, server, authority) = spawn_tcp(1, delayed).await;
    let mut config = tcp_config(&authority);
    config.request_timeout = Duration::from_millis(20);
    assert_json_failure(
        BlazeClient::new(config),
        &["blazectl", "list"],
        "request_timeout",
    )
    .await;
    finish_server(server).await;

    let cancellable: Responder = Arc::new(|_| ResponseSpec {
        status: StatusCode::OK,
        body: ResponseBody::Bytes(b"[]".to_vec()),
        delay: Duration::from_millis(100),
        allow_connection_error: true,
    });
    let (client, state, server, _) = spawn_tcp(1, cancellable).await;
    let cancellation = CancellationToken::new();
    let trigger = cancellation.clone();
    let observed = state.clone();
    let cancel_task = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(1), async {
            while observed.requests().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("request observation");
        trigger.cancel();
    });
    let (exit, stdout, stderr) = run_command_with_cancellation(
        client,
        &words(&["blazectl", "list"]),
        OutputMode::Json,
        &[],
        cancellation,
    )
    .await;
    cancel_task.await.expect("cancellation task");
    assert_eq!(exit, 1);
    assert!(stdout.is_empty());
    assert_eq!(parse_json(&stderr)["code"], "cancelled");
    finish_server(server).await;

    let invalid_base64: Responder = Arc::new(|_| {
        ResponseSpec::bytes(
            StatusCode::OK,
            serde_json::to_vec(&json!({"data_b64": "***"})).expect("invalid base64 fixture"),
        )
    });
    let (client, _, server, _) = spawn_tcp(1, invalid_base64).await;
    assert_json_failure(
        client,
        &["blazectl", "read", ID, "/tmp/data.bin"],
        "output_error",
    )
    .await;
    finish_server(server).await;
}

#[tokio::test]
async fn uds_text_binary_error_and_exit_matrix() {
    let (client, state, server, _temp) = spawn_uds(4, Arc::new(uds_matrix_response)).await;

    let (exit, stdout, stderr) = run_command(
        client.clone(),
        &words(&["blazectl", "list"]),
        OutputMode::Text,
        &[],
    )
    .await;
    assert_eq!(exit, 0);
    assert_eq!(
        stdout,
        concat!(
            "ID\tSTATUS\tTEMPLATE\tCREATED\n",
            "00000000-0000-4000-8000-000000000001\trunning\tbase\t",
            "2026-01-01T00:00:00Z\n",
            "00000000-0000-4000-8000-000000000002\thibernated\t-\t",
            "2026-01-02T00:00:00Z\n"
        )
        .as_bytes()
    );
    assert!(stderr.is_empty());

    let (exit, stdout, stderr) = run_command(
        client.clone(),
        &words(&["blazectl", "exec", ID, "printf sentinel"]),
        OutputMode::Text,
        &[],
    )
    .await;
    assert_eq!(exit, 7);
    assert_eq!(stdout, b"guest stdout");
    assert_eq!(stderr, b"guest stderr");

    let (exit, stdout, stderr) = run_command(
        client.clone(),
        &words(&["blazectl", "read", ID, "/tmp/data.bin"]),
        OutputMode::Text,
        &[],
    )
    .await;
    assert_eq!(exit, 0);
    assert_eq!(stdout, [0, 159, 255, 10]);
    assert!(stderr.is_empty());

    let (exit, stdout, stderr) = run_command(
        client,
        &words(&["blazectl", "kill", ID]),
        OutputMode::Json,
        &[],
    )
    .await;
    assert_eq!(exit, 1);
    assert!(stdout.is_empty());
    assert_eq!(
        parse_json(&stderr),
        json!({
            "code": "not_found",
            "message": "requested resource was not found",
            "operation": "kill",
            "sandbox_id": ID
        })
    );

    finish_server(server).await;
    let requests = state.requests();
    assert_eq!(requests.len(), 4);
    assert!(requests.iter().all(|request| request.host == "localhost"));
}

#[tokio::test]
async fn tcp_kill_all_boundary_concurrency_matrix() {
    for size in [0, 1, 49, 50, 51] {
        let expected_requests = size + 1;
        let (client, state, server, _) =
            spawn_tcp(expected_requests, kill_all_success_responder(size)).await;

        let (exit, stdout, stderr) = run_command(
            client,
            &words(&["blazectl", "kill", "--all"]),
            OutputMode::Json,
            &[],
        )
        .await;
        assert_eq!(exit, 0, "size={size}");
        assert!(stderr.is_empty(), "size={size}");
        let summary = parse_json(&stdout);
        assert_eq!(summary["total"], size, "size={size}");
        assert_eq!(
            summary["succeeded"],
            Value::Array((1..=size).map(|value| json!(id_string(value))).collect()),
            "size={size}"
        );
        assert_eq!(summary["failed"], json!([]), "size={size}");
        assert_eq!(summary["unfinished"], json!([]), "size={size}");

        finish_server(server).await;
        let requests = state.requests();
        assert_eq!(requests.len(), expected_requests, "size={size}");
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.method == Method::GET)
                .count(),
            1,
            "size={size}"
        );
        let deleted = requests
            .iter()
            .filter(|request| request.method == Method::DELETE)
            .map(|request| request.path.clone())
            .collect::<BTreeSet<_>>();
        let expected_deleted = (1..=size)
            .map(|value| format!("/v1/sandboxes/{}", id_string(value)))
            .collect::<BTreeSet<_>>();
        assert_eq!(deleted, expected_deleted, "size={size}");
        assert!(
            state.max_in_flight() <= size.min(50),
            "size={size}, max={}",
            state.max_in_flight()
        );
    }
}

fn kill_all_success_responder(size: usize) -> Responder {
    Arc::new(move |request| {
        if request.method == Method::GET && request.path == "/v1/sandboxes" {
            return json_spec(
                StatusCode::OK,
                Value::Array((1..=size).map(sandbox_value).collect()),
            );
        }
        if request.method == Method::DELETE && request.path.starts_with("/v1/sandboxes/") {
            return ResponseSpec {
                status: StatusCode::NO_CONTENT,
                body: ResponseBody::Bytes(Vec::new()),
                delay: Duration::from_millis(20),
                allow_connection_error: false,
            };
        }
        json_spec(
            StatusCode::NOT_FOUND,
            json!({
                "code": "not_found",
                "message": "unexpected fixture route",
                "operation": "fixture",
                "sandbox_id": null
            }),
        )
    })
}

#[tokio::test]
async fn tcp_kill_all_fifty_one_attempts_all_with_partial_failure() {
    let failed_id = id_string(26);
    let responder_failed_id = failed_id.clone();
    let responder: Responder = Arc::new(move |request| {
        if request.method == Method::GET && request.path == "/v1/sandboxes" {
            return json_spec(
                StatusCode::OK,
                Value::Array((1..=51).map(sandbox_value).collect()),
            );
        }
        if request.method == Method::DELETE && request.path.starts_with("/v1/sandboxes/") {
            let sandbox_id = request
                .path
                .rsplit('/')
                .next()
                .expect("sandbox path segment");
            if sandbox_id == responder_failed_id.as_str() {
                return ResponseSpec {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    body: ResponseBody::Bytes(
                        serde_json::to_vec(&json!({
                            "code": "internal_error",
                            "message": "bounded fixture failure",
                            "operation": "DELETE /v1/sandboxes/{id}",
                            "sandbox_id": sandbox_id
                        }))
                        .expect("error response"),
                    ),
                    delay: Duration::from_millis(20),
                    allow_connection_error: false,
                };
            }
            return ResponseSpec {
                status: StatusCode::NO_CONTENT,
                body: ResponseBody::Bytes(Vec::new()),
                delay: Duration::from_millis(20),
                allow_connection_error: false,
            };
        }
        json_spec(
            StatusCode::NOT_FOUND,
            json!({
                "code": "not_found",
                "message": "unexpected fixture route",
                "operation": "fixture",
                "sandbox_id": null
            }),
        )
    });
    let (client, state, server, _) = spawn_tcp(52, responder).await;

    let (exit, stdout, stderr) = run_command(
        client,
        &words(&["blazectl", "kill", "--all"]),
        OutputMode::Json,
        &[],
    )
    .await;
    assert_eq!(exit, 1);
    assert!(stdout.is_empty());
    let summary = parse_json(&stderr);
    assert_eq!(summary["total"], 51);
    assert_eq!(summary["failed"], json!([failed_id]));
    assert_eq!(summary["unfinished"], json!([]));
    assert_eq!(summary["succeeded"].as_array().map(Vec::len), Some(50));

    finish_server(server).await;
    let requests = state.requests();
    assert_eq!(requests.len(), 52);
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method == Method::GET)
            .count(),
        1
    );
    let deleted: BTreeSet<_> = requests
        .iter()
        .filter(|request| request.method == Method::DELETE)
        .map(|request| request.path.clone())
        .collect();
    assert_eq!(deleted.len(), 51);
    assert!(state.max_in_flight() <= 50);
}

#[derive(Debug)]
struct CommandCase {
    argv: Vec<String>,
    method: Method,
    path: String,
    body: Option<Value>,
    output: Value,
    stdin: Vec<u8>,
}

impl CommandCase {
    fn new(
        argv: &[&str],
        method: Method,
        path: impl Into<String>,
        body: Option<Value>,
        output: Value,
        stdin: &[u8],
    ) -> Self {
        Self {
            argv: words(argv),
            method,
            path: path.into(),
            body,
            output,
            stdin: stdin.to_vec(),
        }
    }
}

#[derive(Debug)]
struct TextCase {
    argv: Vec<String>,
    stdout: Vec<u8>,
    stdin: Vec<u8>,
}

impl TextCase {
    fn new(argv: &[&str], stdout: &[u8], stdin: &[u8]) -> Self {
        Self {
            argv: words(argv),
            stdout: stdout.to_vec(),
            stdin: stdin.to_vec(),
        }
    }
}

fn text_cases() -> Vec<TextCase> {
    vec![
        TextCase::new(
            &["blazectl", "create", ID, "--template", "base"],
            concat!(
                "ID\t00000000-0000-4000-8000-000000000001\n",
                "STATUS\trunning\n",
                "TEMPLATE\tbase\n"
            )
            .as_bytes(),
            &[],
        ),
        TextCase::new(&["blazectl", "exec", ID, "printf sentinel"], b"ok", &[]),
        TextCase::new(
            &["blazectl", "list"],
            concat!(
                "ID\tSTATUS\tTEMPLATE\tCREATED\n",
                "00000000-0000-4000-8000-000000000001\trunning\tbase\t",
                "2026-01-01T00:00:00Z\n"
            )
            .as_bytes(),
            &[],
        ),
        TextCase::new(&["blazectl", "kill", ID], b"STATUS\tok\n", &[]),
        TextCase::new(&["blazectl", "hibernate", ID], b"STATUS\thibernated\n", &[]),
        TextCase::new(
            &["blazectl", "checkpoint", ID],
            concat!(
                "STATUS\tcheckpointed\n",
                "CHECKPOINT\tckpt-00000000-0000-4000-8000-000000000002\n"
            )
            .as_bytes(),
            &[],
        ),
        TextCase::new(
            &["blazectl", "rollback", ID, CHECKPOINT],
            concat!(
                "STATUS\trolledback\n",
                "CHECKPOINT\tckpt-00000000-0000-4000-8000-000000000002\n"
            )
            .as_bytes(),
            &[],
        ),
        TextCase::new(
            &["blazectl", "checkpoints", ID],
            b"ID\tPARENT\tCREATED\tSIZE_BYTES\tHEAD\tON_HEAD_CHAIN\n",
            &[],
        ),
        TextCase::new(
            &["blazectl", "prune-checkpoints", ID],
            b"STATUS\tpruned\nREMOVED_COUNT\t0\nREMOVED\t-\n",
            &[],
        ),
        TextCase::new(&["blazectl", "resume", ID], b"STATUS\trunning\n", &[]),
        TextCase::new(
            &["blazectl", "cleanup-devices"],
            b"DESTROYED\t2\nMESSAGE\twarm pool drained\n",
            &[],
        ),
        TextCase::new(
            &["blazectl", "pool-status"],
            b"READY\t2\nCAPACITY\t4\nPENDING\t1\nQUARANTINED\t0\n",
            &[],
        ),
        TextCase::new(&["blazectl", "read", ID, "/tmp/data.bin"], &[0, 1, 2], &[]),
        TextCase::new(
            &["blazectl", "write", ID, "/tmp/data.bin"],
            b"STATUS\tok\n",
            &[0, 1, 2],
        ),
    ]
}

fn command_cases() -> Vec<CommandCase> {
    vec![
        CommandCase::new(
            &["blazectl", "create", ID, "--template", "base"],
            Method::POST,
            "/v1/sandboxes",
            Some(json!({"id": ID, "template": "base"})),
            json!({"id": ID, "status": "running", "template": "base"}),
            &[],
        ),
        CommandCase::new(
            &["blazectl", "exec", ID, "printf sentinel"],
            Method::POST,
            EXEC_PATH,
            Some(json!({"cmd": "printf sentinel"})),
            json!({"exit_code": 0, "stdout": "ok", "stderr": ""}),
            &[],
        ),
        CommandCase::new(
            &["blazectl", "list"],
            Method::GET,
            "/v1/sandboxes",
            None,
            json!([{
                "id": ID,
                "state": "running",
                "template_name": "base",
                "created_at": "2026-01-01T00:00:00Z"
            }]),
            &[],
        ),
        CommandCase::new(
            &["blazectl", "kill", ID],
            Method::DELETE,
            SANDBOX_PATH,
            None,
            json!({"status": "ok"}),
            &[],
        ),
        CommandCase::new(
            &["blazectl", "hibernate", ID],
            Method::POST,
            HIBERNATE_PATH,
            None,
            json!({"status": "hibernated"}),
            &[],
        ),
        CommandCase::new(
            &["blazectl", "checkpoint", ID],
            Method::POST,
            CHECKPOINT_PATH,
            None,
            json!({"status": "checkpointed", "checkpoint_id": CHECKPOINT}),
            &[],
        ),
        CommandCase::new(
            &["blazectl", "rollback", ID, CHECKPOINT],
            Method::POST,
            ROLLBACK_PATH,
            None,
            json!({"status": "rolledback", "checkpoint": CHECKPOINT}),
            &[],
        ),
        CommandCase::new(
            &["blazectl", "checkpoints", ID],
            Method::GET,
            CHECKPOINTS_PATH,
            None,
            json!({"checkpoints": []}),
            &[],
        ),
        CommandCase::new(
            &["blazectl", "prune-checkpoints", ID],
            Method::POST,
            PRUNE_PATH,
            None,
            json!({"status": "pruned", "removed_count": 0, "removed": []}),
            &[],
        ),
        CommandCase::new(
            &["blazectl", "resume", ID],
            Method::POST,
            RESUME_PATH,
            None,
            json!({"status": "running"}),
            &[],
        ),
        CommandCase::new(
            &["blazectl", "cleanup-devices"],
            Method::POST,
            "/v1/pool/cleanup",
            None,
            json!({"destroyed": 2, "message": "warm pool drained"}),
            &[],
        ),
        CommandCase::new(
            &["blazectl", "pool-status"],
            Method::GET,
            "/v1/pool/status",
            None,
            json!({"ready": 2, "capacity": 4, "pending": 1, "quarantined": 0}),
            &[],
        ),
        CommandCase::new(
            &["blazectl", "read", ID, "/tmp/data.bin"],
            Method::POST,
            READ_PATH,
            Some(json!({"path": "/tmp/data.bin"})),
            json!({"data_b64": "AAEC"}),
            &[],
        ),
        CommandCase::new(
            &["blazectl", "write", ID, "/tmp/data.bin"],
            Method::POST,
            WRITE_PATH,
            Some(json!({"path": "/tmp/data.bin", "data_b64": "AAEC"})),
            json!({"status": "ok"}),
            &[0, 1, 2],
        ),
    ]
}

fn matrix_response(request: &RequestRecord) -> ResponseSpec {
    match (request.method.as_str(), request.path.as_str()) {
        ("POST", "/v1/sandboxes") => json_spec(
            StatusCode::CREATED,
            json!({"id": ID, "status": "running", "template": "base"}),
        ),
        ("POST", EXEC_PATH) => json_spec(
            StatusCode::OK,
            json!({"exit_code": 0, "stdout": "ok", "stderr": ""}),
        ),
        ("GET", "/v1/sandboxes") => json_spec(
            StatusCode::OK,
            json!([{
                "id": ID,
                "state": "running",
                "template_name": "base",
                "created_at": "2026-01-01T00:00:00Z"
            }]),
        ),
        ("DELETE", SANDBOX_PATH) => empty_spec(StatusCode::NO_CONTENT),
        ("POST", HIBERNATE_PATH) => json_spec(StatusCode::OK, json!({"status": "hibernated"})),
        ("POST", CHECKPOINT_PATH) => json_spec(
            StatusCode::OK,
            json!({"status": "checkpointed", "checkpoint_id": CHECKPOINT}),
        ),
        ("POST", ROLLBACK_PATH) => json_spec(
            StatusCode::OK,
            json!({"status": "rolledback", "checkpoint": CHECKPOINT}),
        ),
        ("GET", CHECKPOINTS_PATH) => json_spec(StatusCode::OK, json!({"checkpoints": []})),
        ("POST", PRUNE_PATH) => json_spec(
            StatusCode::OK,
            json!({"status": "pruned", "removed_count": 0, "removed": []}),
        ),
        ("POST", RESUME_PATH) => json_spec(StatusCode::OK, json!({"status": "running"})),
        ("POST", "/v1/pool/cleanup") => json_spec(
            StatusCode::OK,
            json!({"destroyed": 2, "message": "warm pool drained"}),
        ),
        ("GET", "/v1/pool/status") => json_spec(
            StatusCode::OK,
            json!({"ready": 2, "capacity": 4, "pending": 1, "quarantined": 0}),
        ),
        ("POST", READ_PATH) => json_spec(StatusCode::OK, json!({"data_b64": "AAEC"})),
        ("POST", WRITE_PATH) => empty_spec(StatusCode::NO_CONTENT),
        _ => json_spec(
            StatusCode::NOT_FOUND,
            json!({
                "code": "not_found",
                "message": "unexpected fixture route",
                "operation": "fixture",
                "sandbox_id": null
            }),
        ),
    }
}

fn uds_matrix_response(request: &RequestRecord) -> ResponseSpec {
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/v1/sandboxes") => json_spec(
            StatusCode::OK,
            json!([
                {
                    "id": ID_TWO,
                    "state": "hibernated",
                    "template_name": "",
                    "created_at": "2026-01-02T00:00:00Z"
                },
                {
                    "id": ID,
                    "state": "running",
                    "template_name": "base",
                    "created_at": "2026-01-01T00:00:00Z"
                }
            ]),
        ),
        ("POST", EXEC_PATH) => json_spec(
            StatusCode::OK,
            json!({"exit_code": 7, "stdout": "guest stdout", "stderr": "guest stderr"}),
        ),
        ("POST", READ_PATH) => json_spec(StatusCode::OK, json!({"data_b64": "AJ//Cg=="})),
        ("DELETE", SANDBOX_PATH) => json_spec(
            StatusCode::NOT_FOUND,
            json!({
                "code": "not_found",
                "message": "fixture detail must not be reflected",
                "operation": "DELETE /v1/sandboxes/{id}",
                "sandbox_id": ID
            }),
        ),
        _ => json_spec(
            StatusCode::NOT_FOUND,
            json!({
                "code": "not_found",
                "message": "unexpected fixture route",
                "operation": "fixture",
                "sandbox_id": null
            }),
        ),
    }
}

async fn run_command(
    client: BlazeClient,
    argv: &[String],
    mode: OutputMode,
    stdin: &[u8],
) -> (u8, Vec<u8>, Vec<u8>) {
    run_command_with_cancellation(client, argv, mode, stdin, CancellationToken::new()).await
}

async fn run_command_with_terminal(
    client: BlazeClient,
    argv: &[String],
    mode: OutputMode,
    stdin: &[u8],
    stdin_is_terminal: bool,
) -> (u8, Vec<u8>, Vec<u8>) {
    run_command_configured(
        client,
        argv,
        mode,
        stdin,
        stdin_is_terminal,
        CancellationToken::new(),
    )
    .await
}

async fn run_command_with_cancellation(
    client: BlazeClient,
    argv: &[String],
    mode: OutputMode,
    stdin: &[u8],
    cancellation: CancellationToken,
) -> (u8, Vec<u8>, Vec<u8>) {
    run_command_configured(client, argv, mode, stdin, false, cancellation).await
}

async fn run_command_configured(
    client: BlazeClient,
    argv: &[String],
    mode: OutputMode,
    stdin: &[u8],
    stdin_is_terminal: bool,
    cancellation: CancellationToken,
) -> (u8, Vec<u8>, Vec<u8>) {
    let cli = Cli::try_parse_from(argv.iter().map(String::as_str)).expect("CLI");
    let mut stdin = Cursor::new(stdin.to_vec());
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = blazectl::commands::execute_remote(
        client,
        cancellation,
        cli.command,
        mode,
        &mut stdin,
        stdin_is_terminal,
        &mut stdout,
        &mut stderr,
    )
    .await;
    (exit, stdout, stderr)
}

fn words(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

fn parse_json(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes).expect("JSON value")
}

async fn assert_json_failure(client: BlazeClient, argv: &[&str], expected_code: &str) {
    let (exit, stdout, stderr) = run_command(client, &words(argv), OutputMode::Json, &[]).await;
    assert_eq!(exit, 1, "{argv:?}");
    assert!(stdout.is_empty(), "{argv:?}");
    assert_eq!(parse_json(&stderr)["code"], expected_code, "{argv:?}");
}

fn tcp_config(authority: &str) -> ClientConfig {
    ClientConfig::from_selection(EndpointSelection::Http(format!("http://{authority}")))
        .expect("TCP config")
}

fn sandbox_value(value: usize) -> Value {
    json!({
        "id": id_string(value),
        "state": "running",
        "template_name": "base",
        "created_at": "2026-01-01T00:00:00Z"
    })
}

fn id_string(value: usize) -> String {
    format!("00000000-0000-4000-8000-{value:012}")
}

#[derive(Debug, Clone)]
struct RequestRecord {
    method: Method,
    path: String,
    body: Vec<u8>,
    host: String,
    accept: Option<String>,
    content_type: Option<String>,
}

#[derive(Clone)]
struct HarnessState {
    responder: Responder,
    requests: Arc<Mutex<Vec<RequestRecord>>>,
    concurrency: Arc<Mutex<Concurrency>>,
}

impl HarnessState {
    fn new(responder: Responder) -> Self {
        Self {
            responder,
            requests: Arc::new(Mutex::new(Vec::new())),
            concurrency: Arc::new(Mutex::new(Concurrency::default())),
        }
    }

    fn requests(&self) -> Vec<RequestRecord> {
        self.requests.lock().expect("request records").clone()
    }

    fn max_in_flight(&self) -> usize {
        self.concurrency.lock().expect("concurrency state").maximum
    }
}

#[derive(Debug, Default)]
struct Concurrency {
    current: usize,
    maximum: usize,
}

type Responder = Arc<dyn Fn(&RequestRecord) -> ResponseSpec + Send + Sync>;

struct ResponseSpec {
    status: StatusCode,
    body: ResponseBody,
    delay: Duration,
    allow_connection_error: bool,
}

impl ResponseSpec {
    fn bytes(status: StatusCode, body: Vec<u8>) -> Self {
        Self {
            status,
            body: ResponseBody::Bytes(body),
            delay: Duration::ZERO,
            allow_connection_error: false,
        }
    }

    fn chunked(status: StatusCode, chunks: Vec<Vec<u8>>) -> Self {
        Self {
            status,
            body: ResponseBody::Chunks(chunks),
            delay: Duration::ZERO,
            allow_connection_error: false,
        }
    }

    fn disconnect(status: StatusCode, prefix: Vec<u8>) -> Self {
        Self {
            status,
            body: ResponseBody::Disconnect(prefix),
            delay: Duration::ZERO,
            allow_connection_error: true,
        }
    }
}

enum ResponseBody {
    Bytes(Vec<u8>),
    Chunks(Vec<Vec<u8>>),
    Disconnect(Vec<u8>),
}

impl ResponseBody {
    fn has_content(&self) -> bool {
        match self {
            Self::Bytes(bytes) | Self::Disconnect(bytes) => !bytes.is_empty(),
            Self::Chunks(chunks) => chunks.iter().any(|chunk| !chunk.is_empty()),
        }
    }
}

struct ScriptedBody {
    frames: VecDeque<Result<Frame<Bytes>, FixtureBodyError>>,
    exact_size: Option<u64>,
}

impl ScriptedBody {
    fn new(body: ResponseBody) -> Self {
        let (frames, exact_size) = match body {
            ResponseBody::Bytes(bytes) => {
                let exact_size = u64::try_from(bytes.len()).expect("response body length");
                let mut frames = VecDeque::new();
                if !bytes.is_empty() {
                    frames.push_back(Ok(Frame::data(Bytes::from(bytes))));
                }
                (frames, Some(exact_size))
            }
            ResponseBody::Chunks(chunks) => {
                let frames = chunks
                    .into_iter()
                    .map(|chunk| Ok(Frame::data(Bytes::from(chunk))))
                    .collect();
                (frames, None)
            }
            ResponseBody::Disconnect(prefix) => {
                let mut frames = VecDeque::new();
                if !prefix.is_empty() {
                    frames.push_back(Ok(Frame::data(Bytes::from(prefix))));
                }
                frames.push_back(Err(FixtureBodyError));
                (frames, None)
            }
        };
        Self { frames, exact_size }
    }
}

impl Body for ScriptedBody {
    type Data = Bytes;
    type Error = FixtureBodyError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Poll::Ready(self.frames.pop_front())
    }

    fn is_end_stream(&self) -> bool {
        self.frames.is_empty()
    }

    fn size_hint(&self) -> SizeHint {
        let mut hint = SizeHint::new();
        if let Some(exact_size) = self.exact_size {
            hint.set_exact(exact_size);
        }
        hint
    }
}

#[derive(Debug)]
struct FixtureBodyError;

impl fmt::Display for FixtureBodyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("fixture response body interrupted")
    }
}

impl std::error::Error for FixtureBodyError {}

fn json_spec(status: StatusCode, value: Value) -> ResponseSpec {
    ResponseSpec::bytes(
        status,
        serde_json::to_vec(&value).expect("fixture response"),
    )
}

fn empty_spec(status: StatusCode) -> ResponseSpec {
    ResponseSpec::bytes(status, Vec::new())
}

fn structured_error_spec(status: StatusCode, code: &'static str) -> ResponseSpec {
    ResponseSpec::bytes(
        status,
        serde_json::to_vec(&json!({
            "code": code,
            "message": "DAEMON_DETAIL_SENTINEL",
            "operation": "GET /v1/sandboxes",
            "sandbox_id": null
        }))
        .expect("structured error response"),
    )
}

async fn handle_request(
    request: Request<Incoming>,
    state: HarnessState,
    allow_connection_error: Arc<AtomicBool>,
) -> Result<Response<ScriptedBody>, Infallible> {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let host = header_value(&request, HOST);
    let accept = optional_header_value(&request, ACCEPT);
    let content_type = optional_header_value(&request, CONTENT_TYPE);
    let body = request
        .into_body()
        .collect()
        .await
        .expect("request body")
        .to_bytes()
        .to_vec();
    let record = RequestRecord {
        method,
        path,
        body,
        host,
        accept,
        content_type,
    };
    let response = (state.responder)(&record);
    state.requests.lock().expect("request records").push(record);
    allow_connection_error.store(response.allow_connection_error, Ordering::Release);

    if !response.delay.is_zero() {
        {
            let mut concurrency = state.concurrency.lock().expect("concurrency state");
            concurrency.current += 1;
            concurrency.maximum = concurrency.maximum.max(concurrency.current);
        }
        tokio::time::sleep(response.delay).await;
        state.concurrency.lock().expect("concurrency state").current -= 1;
    }

    let mut builder = Response::builder().status(response.status);
    if response.body.has_content() {
        builder = builder.header(CONTENT_TYPE, "application/json");
    }
    Ok(builder
        .body(ScriptedBody::new(response.body))
        .expect("fixture response"))
}

fn header_value(request: &Request<Incoming>, name: hyper::header::HeaderName) -> String {
    request
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

fn optional_header_value(
    request: &Request<Incoming>,
    name: hyper::header::HeaderName,
) -> Option<String> {
    request
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

async fn spawn_tcp(
    expected_requests: usize,
    responder: Responder,
) -> (BlazeClient, HarnessState, JoinHandle<()>, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("TCP bind");
    let address = listener.local_addr().expect("TCP address");
    let state = HarnessState::new(responder);
    let server_state = state.clone();
    let server = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        for _ in 0..expected_requests {
            let (stream, _) = listener.accept().await.expect("TCP accept");
            let connection_state = server_state.clone();
            connections.spawn(async move {
                let allow_connection_error = Arc::new(AtomicBool::new(false));
                let service_flag = allow_connection_error.clone();
                let connection_result = hyper::server::conn::http1::Builder::new()
                    .serve_connection(
                        TokioIo::new(stream),
                        service_fn(move |request| {
                            handle_request(request, connection_state.clone(), service_flag.clone())
                        }),
                    )
                    .await;
                if let Err(error) = connection_result {
                    assert!(
                        allow_connection_error.load(Ordering::Acquire),
                        "unexpected TCP connection failure: {error}"
                    );
                }
            });
        }
        while let Some(connection) = connections.join_next().await {
            connection.expect("TCP connection task");
        }
    });
    let authority = address.to_string();
    let client = BlazeClient::new(tcp_config(&authority));
    (client, state, server, authority)
}

async fn spawn_uds(
    expected_requests: usize,
    responder: Responder,
) -> (BlazeClient, HarnessState, JoinHandle<()>, SocketDirectory) {
    let temp = SocketDirectory::new();
    let socket = temp.path.join("api.sock");
    let listener = UnixListener::bind(&socket).expect("UDS bind");
    let state = HarnessState::new(responder);
    let server_state = state.clone();
    let server = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        for _ in 0..expected_requests {
            let (stream, _) = listener.accept().await.expect("UDS accept");
            let connection_state = server_state.clone();
            connections.spawn(async move {
                let allow_connection_error = Arc::new(AtomicBool::new(false));
                let service_flag = allow_connection_error.clone();
                let connection_result = hyper::server::conn::http1::Builder::new()
                    .serve_connection(
                        TokioIo::new(stream),
                        service_fn(move |request| {
                            handle_request(request, connection_state.clone(), service_flag.clone())
                        }),
                    )
                    .await;
                if let Err(error) = connection_result {
                    assert!(
                        allow_connection_error.load(Ordering::Acquire),
                        "unexpected UDS connection failure: {error}"
                    );
                }
            });
        }
        while let Some(connection) = connections.join_next().await {
            connection.expect("UDS connection task");
        }
    });
    let client = BlazeClient::new(
        ClientConfig::from_selection(EndpointSelection::Unix(socket)).expect("UDS config"),
    );
    (client, state, server, temp)
}

struct PanicReader;

impl Read for PanicReader {
    fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
        panic!("terminal stdin must not be read")
    }
}

struct FailingReader;

impl Read for FailingReader {
    fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::other("fixture read failure"))
    }
}

struct FailingWriter;

impl Write for FailingWriter {
    fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
        Err(io::Error::other("fixture write failure"))
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct SocketDirectory {
    path: PathBuf,
}

impl SocketDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("blazectl-hermetic-{}", Uuid::new_v4()));
        std::fs::create_dir(&path).expect("temporary socket directory");
        Self { path }
    }
}

impl Drop for SocketDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(self.path.join("api.sock"));
        let _ = std::fs::remove_dir(&self.path);
    }
}

async fn finish_server(mut server: JoinHandle<()>) {
    match tokio::time::timeout(Duration::from_secs(5), &mut server).await {
        Ok(result) => result.expect("server task"),
        Err(_) => {
            server.abort();
            let _ = server.await;
            panic!("hermetic server did not observe the expected request count");
        }
    }
}
