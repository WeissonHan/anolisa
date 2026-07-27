// SPDX-License-Identifier: Apache-2.0
//! Deterministic command output and stream routing.

use std::io::{self, Write};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::SecondsFormat;
use serde::Serialize;
use thiserror::Error;
use uuid::Uuid;

use crate::cli::OutputMode;
use crate::protocol::{
    CheckpointListResponse, DaemonErrorResponse, ExecResponse, ReadResponse, SandboxSummary,
};

/// Write exactly one compact JSON value followed by one newline.
///
/// # Errors
///
/// Returns [`OutputError`] when serialization or writing fails.
pub fn write_json(mut writer: impl Write, value: &impl Serialize) -> Result<(), OutputError> {
    let mut encoded = serde_json::to_vec(value).map_err(|source| OutputError::Json { source })?;
    encoded.push(b'\n');
    writer
        .write_all(&encoded)
        .map_err(|source| OutputError::Write { source })
}

/// Write a deterministic sandbox list in the selected format.
///
/// Text rows and the JSON array are sorted by sandbox UUID.
///
/// # Errors
///
/// Returns [`OutputError`] when serialization or writing fails.
pub fn write_sandbox_list(
    mut writer: impl Write,
    mode: OutputMode,
    sandboxes: &[SandboxSummary],
) -> Result<(), OutputError> {
    let mut sorted: Vec<_> = sandboxes.iter().collect();
    sorted.sort_by_key(|sandbox| sandbox.id);
    if mode == OutputMode::Json {
        return write_json(writer, &sorted);
    }

    let mut rendered = String::from("ID\tSTATUS\tTEMPLATE\tCREATED\n");
    for sandbox in sorted {
        rendered.push_str(&sandbox.id.to_string());
        rendered.push('\t');
        push_text_cell(&mut rendered, &sandbox.state);
        rendered.push('\t');
        if sandbox.template_name.is_empty() {
            rendered.push('-');
        } else {
            push_text_cell(&mut rendered, &sandbox.template_name);
        }
        rendered.push('\t');
        rendered.push_str(
            &sandbox
                .created_at
                .to_rfc3339_opts(SecondsFormat::Secs, true),
        );
        rendered.push('\n');
    }
    writer
        .write_all(rendered.as_bytes())
        .map_err(|source| OutputError::Write { source })
}

/// Write stable labeled text fields, one field per line.
///
/// Labels are reviewed static strings. Values have control characters escaped
/// so one value cannot inject another field or row.
///
/// # Errors
///
/// Returns [`OutputError`] when writing fails.
pub fn write_text_fields(
    mut writer: impl Write,
    fields: &[(&'static str, String)],
) -> Result<(), OutputError> {
    let mut rendered = String::new();
    for (label, value) in fields {
        rendered.push_str(label);
        rendered.push('\t');
        push_text_cell(&mut rendered, value);
        rendered.push('\n');
    }
    writer
        .write_all(rendered.as_bytes())
        .map_err(|source| OutputError::Write { source })
}

/// Write deterministic checkpoint columns or one sorted JSON object.
///
/// # Errors
///
/// Returns [`OutputError`] when serialization or writing fails.
pub fn write_checkpoint_list(
    mut writer: impl Write,
    mode: OutputMode,
    response: &CheckpointListResponse,
) -> Result<(), OutputError> {
    let mut checkpoints = response.checkpoints.clone();
    checkpoints.sort_by(|left, right| {
        left.created_at
            .cmp(&right.created_at)
            .then_with(|| left.id.cmp(&right.id))
    });
    if mode == OutputMode::Json {
        return write_json(writer, &CheckpointListResponse { checkpoints });
    }

    let mut rendered = String::from("ID\tPARENT\tCREATED\tSIZE_BYTES\tHEAD\tON_HEAD_CHAIN\n");
    for checkpoint in checkpoints {
        push_text_cell(&mut rendered, &checkpoint.id);
        rendered.push('\t');
        match checkpoint.parent {
            Some(parent) => push_text_cell(&mut rendered, &parent),
            None => rendered.push('-'),
        }
        rendered.push('\t');
        rendered.push_str(
            &checkpoint
                .created_at
                .to_rfc3339_opts(SecondsFormat::Secs, true),
        );
        rendered.push('\t');
        rendered.push_str(&checkpoint.size_bytes.to_string());
        rendered.push('\t');
        rendered.push_str(if checkpoint.is_head { "true" } else { "false" });
        rendered.push('\t');
        rendered.push_str(if checkpoint.on_head_chain {
            "true"
        } else {
            "false"
        });
        rendered.push('\n');
    }
    writer
        .write_all(rendered.as_bytes())
        .map_err(|source| OutputError::Write { source })
}

/// Write guest command output without invoking or interpreting a local shell.
///
/// Text mode preserves the daemon stdout/stderr split and adds no bytes. JSON
/// mode writes one response object to stdout and leaves stderr untouched.
///
/// # Errors
///
/// Returns [`OutputError`] when serialization or either stream write fails.
pub fn write_exec(
    mut stdout: impl Write,
    mut stderr: impl Write,
    mode: OutputMode,
    response: &ExecResponse,
) -> Result<(), OutputError> {
    if mode == OutputMode::Json {
        return write_json(stdout, response);
    }
    stdout
        .write_all(response.stdout.as_bytes())
        .map_err(|source| OutputError::Write { source })?;
    stderr
        .write_all(response.stderr.as_bytes())
        .map_err(|source| OutputError::Write { source })
}

/// Write a guest file read without lossy byte conversion.
///
/// Text mode decodes standard base64 and writes the exact bytes without an
/// added newline. JSON mode retains the base64 wire value.
///
/// # Errors
///
/// Returns [`OutputError`] for invalid base64, serialization, or write
/// failures.
pub fn write_read(
    mut stdout: impl Write,
    mode: OutputMode,
    response: &ReadResponse,
) -> Result<(), OutputError> {
    let bytes = BASE64
        .decode(response.data_b64.as_bytes())
        .map_err(|source| OutputError::InvalidBase64 { source })?;
    if mode == OutputMode::Json {
        return write_json(stdout, response);
    }
    stdout
        .write_all(&bytes)
        .map_err(|source| OutputError::Write { source })
}

/// Stable, bounded diagnostic fields suitable for stderr.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Diagnostic {
    code: &'static str,
    message: &'static str,
    operation: &'static str,
    sandbox_id: Option<Uuid>,
}

impl Diagnostic {
    /// Construct a diagnostic from reviewed static fields.
    pub const fn local(
        code: &'static str,
        message: &'static str,
        operation: &'static str,
        sandbox_id: Option<Uuid>,
    ) -> Self {
        Self {
            code,
            message,
            operation,
            sandbox_id,
        }
    }

    /// Convert a daemon error to reviewed fields without reflecting its
    /// message or operation.
    pub fn daemon(operation: &'static str, response: &DaemonErrorResponse) -> Self {
        let (code, message) = daemon_diagnostic(response.code.as_str());
        Self::local(code, message, operation, response.sandbox_id)
    }
}

/// Write exactly one stable diagnostic to stderr.
///
/// # Errors
///
/// Returns [`OutputError`] when serialization or writing fails.
pub fn write_diagnostic(
    mut stderr: impl Write,
    mode: OutputMode,
    diagnostic: &Diagnostic,
) -> Result<(), OutputError> {
    if mode == OutputMode::Json {
        return write_json(stderr, diagnostic);
    }
    let rendered = format!("error: {}: {}\n", diagnostic.code, diagnostic.message);
    stderr
        .write_all(rendered.as_bytes())
        .map_err(|source| OutputError::Write { source })
}

/// Stable output failures that never reflect output destinations or payloads.
#[derive(Debug, Error)]
pub enum OutputError {
    /// JSON encoding failed before any output write was attempted.
    #[error("failed to encode JSON output")]
    Json {
        /// JSON encoder failure retained for error chaining.
        #[source]
        source: serde_json::Error,
    },
    /// A command output stream rejected a write.
    #[error("failed to write command output")]
    Write {
        /// I/O failure retained without reflecting it in the stable display.
        #[source]
        source: io::Error,
    },
    /// A daemon file-read response contained invalid standard base64.
    #[error("daemon returned invalid base64 file data")]
    InvalidBase64 {
        /// Base64 decoder failure retained without retaining the payload.
        #[source]
        source: base64::DecodeError,
    },
}

fn push_text_cell(rendered: &mut String, value: &str) {
    for character in value.chars() {
        match character {
            '\\' => rendered.push_str("\\\\"),
            '\t' => rendered.push_str("\\t"),
            '\r' => rendered.push_str("\\r"),
            '\n' => rendered.push_str("\\n"),
            character if character.is_control() => rendered.push('\u{fffd}'),
            character => rendered.push(character),
        }
    }
}

fn daemon_diagnostic(code: &str) -> (&'static str, &'static str) {
    match code {
        "invalid_request" => ("invalid_request", "daemon rejected the request"),
        "not_found" => ("not_found", "requested resource was not found"),
        "state_conflict" => (
            "state_conflict",
            "request conflicts with the current sandbox state",
        ),
        "payload_too_large" => ("payload_too_large", "daemon rejected an oversized payload"),
        "recovery_required" => ("recovery_required", "sandbox recovery is required"),
        "guest_timeout" => ("guest_timeout", "guest operation timed out"),
        "shutting_down" => ("shutting_down", "daemon is shutting down"),
        "guest_error" => ("guest_error", "guest operation failed"),
        "backend_unavailable" => ("backend_unavailable", "sandbox backend is unavailable"),
        "invalid_state_transition" => (
            "invalid_state_transition",
            "sandbox state transition is invalid",
        ),
        "internal_error" => ("internal_error", "daemon request failed"),
        _ => ("daemon_error", "daemon request failed"),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use serde_json::{Value, json};

    use crate::protocol::CheckpointSummary;

    use super::*;

    #[test]
    fn sandbox_list_text_has_stable_columns_and_id_order() {
        let sandboxes = vec![
            sandbox(2, "hibernated", "", "2026-01-02T03:04:05Z"),
            sandbox(1, "running", "base", "2026-01-01T00:00:00Z"),
        ];
        let mut output = Vec::new();
        write_sandbox_list(&mut output, OutputMode::Text, &sandboxes).expect("text list");
        assert_eq!(
            output,
            concat!(
                "ID\tSTATUS\tTEMPLATE\tCREATED\n",
                "00000000-0000-4000-8000-000000000001\trunning\tbase\t",
                "2026-01-01T00:00:00Z\n",
                "00000000-0000-4000-8000-000000000002\thibernated\t-\t",
                "2026-01-02T03:04:05Z\n"
            )
            .as_bytes()
        );
    }

    #[test]
    fn sandbox_list_json_is_one_sorted_value() {
        let sandboxes = vec![
            sandbox(2, "hibernated", "", "2026-01-02T03:04:05Z"),
            sandbox(1, "running", "base", "2026-01-01T00:00:00Z"),
        ];
        let mut output = Vec::new();
        write_sandbox_list(&mut output, OutputMode::Json, &sandboxes).expect("JSON list");
        let values = json_values(&output);
        assert_eq!(values.len(), 1);
        assert_eq!(values[0][0]["id"], "00000000-0000-4000-8000-000000000001");
        assert_eq!(values[0][1]["id"], "00000000-0000-4000-8000-000000000002");
    }

    #[test]
    fn text_cells_cannot_inject_rows_or_columns() {
        let sandboxes = vec![sandbox(
            1,
            "run\nning",
            "base\tname",
            "2026-01-01T00:00:00Z",
        )];
        let mut output = Vec::new();
        write_sandbox_list(&mut output, OutputMode::Text, &sandboxes).expect("text list");
        assert_eq!(
            output,
            concat!(
                "ID\tSTATUS\tTEMPLATE\tCREATED\n",
                "00000000-0000-4000-8000-000000000001\trun\\nning\t",
                "base\\tname\t2026-01-01T00:00:00Z\n"
            )
            .as_bytes()
        );
    }

    #[test]
    fn checkpoint_output_has_stable_columns_and_order() {
        let response = CheckpointListResponse {
            checkpoints: vec![
                checkpoint(2, "2026-01-02T00:00:00Z", false),
                checkpoint(1, "2026-01-01T00:00:00Z", true),
            ],
        };
        let mut text = Vec::new();
        write_checkpoint_list(&mut text, OutputMode::Text, &response).expect("text checkpoints");
        assert_eq!(
            text,
            concat!(
                "ID\tPARENT\tCREATED\tSIZE_BYTES\tHEAD\tON_HEAD_CHAIN\n",
                "ckpt-00000000-0000-4000-8000-000000000001\t-\t",
                "2026-01-01T00:00:00Z\t1\ttrue\ttrue\n",
                "ckpt-00000000-0000-4000-8000-000000000002\t-\t",
                "2026-01-02T00:00:00Z\t2\tfalse\ttrue\n"
            )
            .as_bytes()
        );

        let mut json = Vec::new();
        write_checkpoint_list(&mut json, OutputMode::Json, &response).expect("JSON checkpoints");
        assert_eq!(
            json_values(&json)[0]["checkpoints"][0]["id"],
            "ckpt-00000000-0000-4000-8000-000000000001"
        );
    }

    #[test]
    fn labeled_text_fields_escape_control_characters() {
        let fields = [("STATUS", "run\nning".to_string())];
        let mut output = Vec::new();
        write_text_fields(&mut output, &fields).expect("fields");
        assert_eq!(output, b"STATUS\trun\\nning\n");
    }

    #[test]
    fn exec_text_preserves_the_two_daemon_streams() {
        let response = ExecResponse {
            exit_code: 7,
            stdout: "stdout-without-newline".to_string(),
            stderr: "stderr-without-newline".to_string(),
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        write_exec(&mut stdout, &mut stderr, OutputMode::Text, &response).expect("exec output");
        assert_eq!(stdout, b"stdout-without-newline");
        assert_eq!(stderr, b"stderr-without-newline");
    }

    #[test]
    fn exec_json_is_one_stdout_value_and_leaves_stderr_empty() {
        let response = ExecResponse {
            exit_code: 7,
            stdout: "out".to_string(),
            stderr: "err".to_string(),
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        write_exec(&mut stdout, &mut stderr, OutputMode::Json, &response).expect("exec output");
        assert_eq!(
            json_values(&stdout),
            vec![json!({
                "exit_code": 7,
                "stdout": "out",
                "stderr": "err"
            })]
        );
        assert!(stderr.is_empty());
    }

    #[test]
    fn read_text_is_binary_safe_and_json_retains_base64() {
        let response = ReadResponse {
            data_b64: "AJ//Cg==".to_string(),
        };
        let mut text = Vec::new();
        write_read(&mut text, OutputMode::Text, &response).expect("binary read");
        assert_eq!(text, [0, 159, 255, 10]);

        let mut json = Vec::new();
        write_read(&mut json, OutputMode::Json, &response).expect("JSON read");
        assert_eq!(json_values(&json), vec![json!({"data_b64": "AJ//Cg=="})]);
    }

    #[test]
    fn invalid_read_base64_writes_nothing() {
        let response = ReadResponse {
            data_b64: "not standard base64".to_string(),
        };
        for mode in [OutputMode::Text, OutputMode::Json] {
            let mut output = Vec::new();
            let error = write_read(&mut output, mode, &response).expect_err("invalid base64");
            assert!(matches!(error, OutputError::InvalidBase64 { .. }));
            assert!(output.is_empty());
        }
    }

    #[test]
    fn daemon_diagnostic_never_reflects_daemon_message_or_operation() {
        let daemon = DaemonErrorResponse {
            code: "not_found".to_string(),
            message: "HOST_LOCATION_SENTINEL".to_string(),
            operation: "HOST_OPERATION_SENTINEL".to_string(),
            sandbox_id: Some(id(1)),
        };
        let diagnostic = Diagnostic::daemon("list", &daemon);

        let mut text = Vec::new();
        write_diagnostic(&mut text, OutputMode::Text, &diagnostic).expect("text diagnostic");
        assert_eq!(
            text,
            b"error: not_found: requested resource was not found\n"
        );

        let mut json = Vec::new();
        write_diagnostic(&mut json, OutputMode::Json, &diagnostic).expect("JSON diagnostic");
        assert_eq!(
            json,
            concat!(
                "{\"code\":\"not_found\",\"message\":\"requested resource was not found\",",
                "\"operation\":\"list\",\"sandbox_id\":",
                "\"00000000-0000-4000-8000-000000000001\"}\n"
            )
            .as_bytes()
        );
        assert_eq!(
            json_values(&json),
            vec![json!({
                "code": "not_found",
                "message": "requested resource was not found",
                "operation": "list",
                "sandbox_id": "00000000-0000-4000-8000-000000000001"
            })]
        );
        for output in [&text, &json] {
            assert!(
                !output
                    .windows(22)
                    .any(|value| value == b"HOST_LOCATION_SENTINEL")
            );
            assert!(
                !output
                    .windows(23)
                    .any(|value| value == b"HOST_OPERATION_SENTINEL")
            );
        }
    }

    #[test]
    fn unknown_daemon_error_code_is_not_reflected() {
        let daemon = DaemonErrorResponse {
            code: "HOST_CODE_SENTINEL".to_string(),
            message: "HOST_MESSAGE_SENTINEL".to_string(),
            operation: "HOST_OPERATION_SENTINEL".to_string(),
            sandbox_id: None,
        };
        let diagnostic = Diagnostic::daemon("create", &daemon);
        let mut output = Vec::new();
        write_diagnostic(&mut output, OutputMode::Json, &diagnostic).expect("diagnostic");
        assert_eq!(
            json_values(&output),
            vec![json!({
                "code": "daemon_error",
                "message": "daemon request failed",
                "operation": "create",
                "sandbox_id": null
            })]
        );
        assert!(
            !output
                .windows(18)
                .any(|value| value == b"HOST_CODE_SENTINEL")
        );
        assert!(
            !output
                .windows(21)
                .any(|value| value == b"HOST_MESSAGE_SENTINEL")
        );
        assert!(
            !output
                .windows(23)
                .any(|value| value == b"HOST_OPERATION_SENTINEL")
        );
    }

    #[test]
    fn output_write_failures_are_stable_and_non_reflecting() {
        let error =
            write_json(&mut FailingWriter, &json!({"status": "ok"})).expect_err("output failure");
        assert!(matches!(error, OutputError::Write { .. }));
        assert_eq!(error.to_string(), "failed to write command output");
    }

    fn sandbox(suffix: u128, state: &str, template_name: &str, created_at: &str) -> SandboxSummary {
        SandboxSummary {
            id: id(suffix),
            state: state.to_string(),
            template_name: template_name.to_string(),
            created_at: DateTime::parse_from_rfc3339(created_at)
                .expect("timestamp")
                .with_timezone(&Utc),
        }
    }

    fn checkpoint(suffix: u128, created_at: &str, is_head: bool) -> CheckpointSummary {
        CheckpointSummary {
            id: format!("ckpt-{}", id(suffix)),
            parent: None,
            created_at: DateTime::parse_from_rfc3339(created_at)
                .expect("timestamp")
                .with_timezone(&Utc),
            size_bytes: suffix as u64,
            is_head,
            on_head_chain: true,
        }
    }

    fn id(value: u128) -> Uuid {
        Uuid::from_u128(0x0000_0000_0000_4000_8000_0000_0000_0000 | value)
    }

    fn json_values(bytes: &[u8]) -> Vec<Value> {
        serde_json::Deserializer::from_slice(bytes)
            .into_iter::<Value>()
            .collect::<Result<_, _>>()
            .expect("JSON stream")
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
