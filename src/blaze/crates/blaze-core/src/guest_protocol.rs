// SPDX-License-Identifier: Apache-2.0
//! Readiness wire DTOs shared with the Blaze guest agent.

use serde::{Deserialize, Serialize};

/// Firecracker vsock port used by the Blaze guest agent.
pub const DEFAULT_GUEST_PORT: u32 = 5000;

/// Maximum accepted JSON response line, excluding the newline delimiter.
pub const DEFAULT_MAX_RESPONSE_BYTES: usize = 32 * 1024 * 1024;

/// Guest operation needed before a runtime can enter the ready pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GuestOp {
    /// Check whether the guest agent can serve requests.
    Ping,
}

/// One newline-delimited readiness request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GuestRequest {
    /// Correlation identifier echoed by the guest.
    pub id: String,
    /// Requested guest operation.
    pub op: GuestOp,
}

/// One newline-delimited readiness response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GuestResponse {
    /// Correlation identifier copied from the request.
    #[serde(default)]
    pub id: String,
    /// Whether the operation completed successfully.
    #[serde(default)]
    pub ok: bool,
    /// Guest error message when `ok` is false.
    #[serde(default)]
    pub err: Option<String>,
}

impl GuestRequest {
    /// Build a readiness request.
    pub fn new(id: String, op: GuestOp) -> Self {
        Self { id, op }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn readiness_request_uses_the_ping_wire_name() {
        let request = GuestRequest::new("request-1".to_string(), GuestOp::Ping);

        assert_eq!(
            serde_json::to_value(request).expect("serialize request"),
            json!({
                "id": "request-1",
                "op": "ping",
            })
        );
    }

    #[test]
    fn readiness_response_defaults_optional_wire_fields() {
        let response: GuestResponse =
            serde_json::from_value(json!({"id": "request-1"})).expect("deserialize response");

        assert_eq!(response.id, "request-1");
        assert!(!response.ok);
        assert!(response.err.is_none());
    }
}
