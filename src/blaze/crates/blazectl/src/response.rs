// SPDX-License-Identifier: Apache-2.0
//! Typed decoding for bounded daemon HTTP responses.

use hyper::StatusCode;
use serde::de::DeserializeOwned;
use serde_json::Value;
use thiserror::Error;

use crate::client::RawResponse;
use crate::protocol::DaemonErrorResponse;

/// Decode a successful JSON response or classify a bounded daemon failure.
///
/// # Errors
///
/// Returns [`ResponseError`] when the daemon returns a non-success status,
/// an empty JSON response, or malformed JSON.
pub fn decode_json<T: DeserializeOwned>(response: RawResponse) -> Result<T, ResponseError> {
    let RawResponse { status, body, .. } = response;
    if !status.is_success() {
        return Err(decode_failure(status, &body));
    }
    if body.is_empty() {
        return Err(ResponseError::UnexpectedEmpty { status });
    }
    if status == StatusCode::NO_CONTENT {
        return Err(ResponseError::UnexpectedBody { status });
    }
    serde_json::from_slice(&body).map_err(|source| ResponseError::MalformedJson { status, source })
}

/// Decode an operation whose only valid success response is an empty 204.
///
/// # Errors
///
/// Returns [`ResponseError`] for daemon failures, any other success status, or
/// a non-empty 204 response.
pub fn decode_empty(response: RawResponse) -> Result<(), ResponseError> {
    let RawResponse { status, body, .. } = response;
    if !status.is_success() {
        return Err(decode_failure(status, &body));
    }
    if status != StatusCode::NO_CONTENT {
        return Err(ResponseError::UnexpectedSuccessStatus { status });
    }
    if !body.is_empty() {
        return Err(ResponseError::UnexpectedBody { status });
    }
    Ok(())
}

/// Bounded response classification that never reflects raw response bodies.
#[derive(Debug, Error)]
pub enum ResponseError {
    /// A complete four-field daemon error was decoded.
    #[error("daemon returned an error response ({status})")]
    Daemon {
        /// Daemon HTTP status.
        status: StatusCode,
        /// Typed daemon error retained for the output layer.
        response: DaemonErrorResponse,
    },
    /// A non-success response did not contain the complete daemon error shape.
    #[error("daemon returned HTTP status {status}")]
    HttpStatus {
        /// Daemon HTTP status.
        status: StatusCode,
    },
    /// JSON was required but the successful response was empty.
    #[error("daemon returned an empty response where JSON was required ({status})")]
    UnexpectedEmpty {
        /// Daemon HTTP status.
        status: StatusCode,
    },
    /// The response contained bytes where an empty body was required.
    #[error("daemon returned a response body where none was allowed ({status})")]
    UnexpectedBody {
        /// Daemon HTTP status.
        status: StatusCode,
    },
    /// An empty operation returned a different successful status.
    #[error("daemon returned an unexpected success status ({status})")]
    UnexpectedSuccessStatus {
        /// Daemon HTTP status.
        status: StatusCode,
    },
    /// A successful JSON response could not be decoded.
    #[error("daemon returned malformed JSON ({status})")]
    MalformedJson {
        /// Daemon HTTP status.
        status: StatusCode,
        /// JSON decoder failure retained without retaining the body.
        #[source]
        source: serde_json::Error,
    },
}

fn decode_failure(status: StatusCode, body: &[u8]) -> ResponseError {
    let response = serde_json::from_slice::<Value>(body)
        .ok()
        .filter(has_complete_error_shape)
        .and_then(|value| serde_json::from_value::<DaemonErrorResponse>(value).ok());
    match response {
        Some(response) => ResponseError::Daemon { status, response },
        None => ResponseError::HttpStatus { status },
    }
}

fn has_complete_error_shape(value: &Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    ["code", "message", "operation", "sandbox_id"]
        .iter()
        .all(|field| object.contains_key(*field))
}

#[cfg(test)]
mod tests {
    use hyper::header::HeaderMap;
    use serde::Deserialize;

    use super::*;

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct TestPayload {
        status: String,
    }

    #[test]
    fn json_success_decodes_typed_payload_and_allows_extensions() {
        let decoded: TestPayload = decode_json(raw(
            StatusCode::CREATED,
            br#"{"status":"running","future_field":true}"#,
        ))
        .expect("typed response");
        assert_eq!(
            decoded,
            TestPayload {
                status: "running".to_string()
            }
        );
    }

    #[test]
    fn empty_success_requires_an_exactly_empty_204() {
        decode_empty(raw(StatusCode::NO_CONTENT, b"")).expect("empty response");

        let wrong_status =
            decode_empty(raw(StatusCode::OK, b"")).expect_err("unexpected success status");
        assert!(matches!(
            wrong_status,
            ResponseError::UnexpectedSuccessStatus {
                status: StatusCode::OK
            }
        ));

        let unexpected_body =
            decode_empty(raw(StatusCode::NO_CONTENT, b"{}")).expect_err("body on empty response");
        assert!(matches!(
            unexpected_body,
            ResponseError::UnexpectedBody {
                status: StatusCode::NO_CONTENT
            }
        ));
    }

    #[test]
    fn json_success_rejects_empty_and_malformed_payloads() {
        for status in [StatusCode::OK, StatusCode::NO_CONTENT] {
            let error =
                decode_json::<TestPayload>(raw(status, b"")).expect_err("empty JSON response");
            assert!(matches!(
                error,
                ResponseError::UnexpectedEmpty { status: actual } if actual == status
            ));
        }

        let error = decode_json::<TestPayload>(raw(StatusCode::OK, b"{"))
            .expect_err("malformed JSON response");
        assert!(matches!(
            error,
            ResponseError::MalformedJson {
                status: StatusCode::OK,
                ..
            }
        ));

        let error = decode_json::<TestPayload>(raw(StatusCode::NO_CONTENT, b"{}"))
            .expect_err("body on 204 response");
        assert!(matches!(
            error,
            ResponseError::UnexpectedBody {
                status: StatusCode::NO_CONTENT
            }
        ));
    }

    #[test]
    fn structured_daemon_error_is_preserved_without_displaying_its_message() {
        let error = decode_json::<TestPayload>(raw(
            StatusCode::NOT_FOUND,
            br#"{
                "code":"not_found",
                "message":"HOST_LOCATION_SENTINEL",
                "operation":"GET /v1/sandboxes/00000000-0000-4000-8000-000000000001",
                "sandbox_id":"00000000-0000-4000-8000-000000000001",
                "future_field":true
            }"#,
        ))
        .expect_err("daemon error");
        let rendered = error.to_string();
        match &error {
            ResponseError::Daemon { status, response } => {
                assert_eq!(*status, StatusCode::NOT_FOUND);
                assert_eq!(response.code, "not_found");
                assert_eq!(
                    response.sandbox_id.expect("sandbox ID").to_string(),
                    "00000000-0000-4000-8000-000000000001"
                );
            }
            other => panic!("unexpected error: {other}"),
        }
        assert!(!rendered.contains("HOST_LOCATION_SENTINEL"));
    }

    #[test]
    fn empty_decoder_preserves_a_complete_daemon_error() {
        let error = decode_empty(raw(
            StatusCode::CONFLICT,
            br#"{
                "code":"state_conflict",
                "message":"conflict",
                "operation":"DELETE /v1/sandboxes/00000000-0000-4000-8000-000000000001",
                "sandbox_id":"00000000-0000-4000-8000-000000000001"
            }"#,
        ))
        .expect_err("daemon error");
        assert!(matches!(
            error,
            ResponseError::Daemon {
                status: StatusCode::CONFLICT,
                ..
            }
        ));
    }

    #[test]
    fn invalid_daemon_error_uses_a_non_reflecting_fallback() {
        for body in [
            b"HOST_LOCATION_SENTINEL".as_slice(),
            br#"{
                "code":"not_found",
                "message":"HOST_LOCATION_SENTINEL",
                "operation":"GET /v1/sandboxes"
            }"#
            .as_slice(),
        ] {
            let error = decode_json::<TestPayload>(raw(StatusCode::BAD_GATEWAY, body))
                .expect_err("fallback error");
            assert!(matches!(
                error,
                ResponseError::HttpStatus {
                    status: StatusCode::BAD_GATEWAY
                }
            ));
            assert!(!error.to_string().contains("HOST_LOCATION_SENTINEL"));
        }
    }

    fn raw(status: StatusCode, body: &[u8]) -> RawResponse {
        RawResponse {
            status,
            headers: HeaderMap::new(),
            body: body.to_vec(),
        }
    }
}
