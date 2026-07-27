// SPDX-License-Identifier: Apache-2.0
//! Local wire DTOs for the frozen daemon HTTP contract.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Canonical sandbox create request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CreateRequest {
    /// Optional caller-selected sandbox UUID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Uuid>,
    /// Optional template name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
}

/// Canonical sandbox create response fields used by the CLI.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CreateResponse {
    /// Created or reused sandbox UUID.
    pub id: Uuid,
    /// Current daemon lifecycle state.
    pub status: String,
    /// Selected template name.
    pub template: String,
}

/// Read-only sandbox fields used by list output.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct SandboxSummary {
    /// Sandbox UUID.
    pub id: Uuid,
    /// Current daemon lifecycle state.
    pub state: String,
    /// User-visible template name.
    #[serde(default)]
    pub template_name: String,
    /// Original creation time.
    pub created_at: DateTime<Utc>,
}

/// Guest command request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExecRequest {
    /// Command string passed as data to the daemon.
    pub cmd: String,
    /// Optional guest working directory.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
}

/// Guest command response.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ExecResponse {
    /// Guest process exit status.
    pub exit_code: i32,
    /// Daemon-provided command stdout.
    pub stdout: String,
    /// Daemon-provided command stderr.
    pub stderr: String,
}

/// Guest file request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FileRequest {
    /// Absolute guest path.
    pub path: String,
    /// Standard-base64 bytes for write requests.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_b64: Option<String>,
}

/// Guest file read response.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ReadResponse {
    /// Standard-base64 guest file bytes.
    pub data_b64: String,
}

/// Lifecycle mutation response.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct LifecycleResponse {
    /// Resulting lifecycle state.
    pub status: String,
}

/// Checkpoint creation response.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CheckpointResponse {
    /// Stable result label.
    pub status: String,
    /// Created checkpoint identifier.
    pub checkpoint_id: String,
}

/// Read-only checkpoint fields used by list output.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CheckpointSummary {
    /// Checkpoint identifier.
    pub id: String,
    /// Parent checkpoint identifier.
    pub parent: Option<String>,
    /// Commit time.
    pub created_at: DateTime<Utc>,
    /// Sum of logical artifact sizes.
    pub size_bytes: u64,
    /// Whether this checkpoint is current HEAD.
    pub is_head: bool,
    /// Whether this checkpoint is reachable from HEAD.
    pub on_head_chain: bool,
}

/// Checkpoint list response.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CheckpointListResponse {
    /// Checkpoints returned by the daemon.
    pub checkpoints: Vec<CheckpointSummary>,
}

/// Checkpoint prune response.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PruneResponse {
    /// Stable result label.
    pub status: String,
    /// Number of deleted checkpoints.
    pub removed_count: usize,
    /// Deleted checkpoint identifiers.
    pub removed: Vec<String>,
}

/// Rollback response.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct RollbackResponse {
    /// Stable result label.
    pub status: String,
    /// Selected checkpoint identifier.
    pub checkpoint: String,
}

/// Warm-pool status response.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PoolStatusResponse {
    /// Ready slots.
    pub ready: usize,
    /// Configured capacity.
    pub capacity: usize,
    /// Slots being prepared.
    pub pending: usize,
    /// Slots removed from service.
    pub quarantined: usize,
}

/// Warm-pool cleanup response.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CleanupResponse {
    /// Number of destroyed pool entries.
    pub destroyed: usize,
    /// Stable daemon result message.
    pub message: String,
}

/// Frozen structured daemon error response.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct DaemonErrorResponse {
    /// Stable machine-readable error code.
    pub code: String,
    /// Bounded human-readable message.
    pub message: String,
    /// HTTP method and canonical route.
    pub operation: String,
    /// Sandbox UUID when the route contains one.
    pub sandbox_id: Option<Uuid>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn request_bodies_match_the_daemon_contract() {
        let id = Uuid::parse_str("00000000-0000-4000-8000-000000000001").expect("UUID");
        assert_eq!(
            serde_json::to_value(CreateRequest {
                id: Some(id),
                template: Some("base".to_string()),
            })
            .expect("create request"),
            json!({"id": id, "template": "base"})
        );
        assert_eq!(
            serde_json::to_value(ExecRequest {
                cmd: "printf sentinel".to_string(),
                cwd: None,
            })
            .expect("exec request"),
            json!({"cmd": "printf sentinel"})
        );
        assert_eq!(
            serde_json::to_value(FileRequest {
                path: "/tmp/data.bin".to_string(),
                data_b64: None,
            })
            .expect("read request"),
            json!({"path": "/tmp/data.bin"})
        );
        assert_eq!(
            serde_json::to_value(FileRequest {
                path: "/tmp/data.bin".to_string(),
                data_b64: Some("AAEC".to_string()),
            })
            .expect("write request"),
            json!({"path": "/tmp/data.bin", "data_b64": "AAEC"})
        );
    }

    #[test]
    fn response_dtos_allow_daemon_extensions() {
        let summary: SandboxSummary = serde_json::from_value(json!({
            "id": "00000000-0000-4000-8000-000000000001",
            "state": "running",
            "template_name": "base",
            "created_at": "2026-01-01T00:00:00Z",
            "future_field": true
        }))
        .expect("sandbox summary");
        assert_eq!(summary.state, "running");
        assert_eq!(summary.template_name, "base");

        let pool: PoolStatusResponse = serde_json::from_value(json!({
            "ready": 2,
            "capacity": 4,
            "pending": 1,
            "quarantined": 0,
            "pool_ready": 2,
            "pool_size": 4
        }))
        .expect("pool response");
        assert_eq!(pool.ready, 2);
        assert_eq!(pool.capacity, 4);
    }

    #[test]
    fn missing_required_response_field_is_a_protocol_error() {
        let result = serde_json::from_value::<ExecResponse>(json!({
            "exit_code": 0,
            "stdout": ""
        }));
        assert!(result.is_err());
    }

    #[test]
    fn operation_response_shapes_match_the_daemon_contract() {
        let create: CreateResponse = serde_json::from_value(json!({
            "id": "00000000-0000-4000-8000-000000000001",
            "status": "running",
            "template": "base",
            "instance": {},
            "decision": {}
        }))
        .expect("create response");
        assert_eq!(create.status, "running");

        let exec: ExecResponse = serde_json::from_value(json!({
            "exit_code": 0,
            "stdout": "ok",
            "stderr": ""
        }))
        .expect("exec response");
        assert_eq!(exec.exit_code, 0);

        let read: ReadResponse =
            serde_json::from_value(json!({"data_b64": "AAEC"})).expect("read response");
        assert_eq!(read.data_b64, "AAEC");

        let lifecycle: LifecycleResponse =
            serde_json::from_value(json!({"status": "hibernated"})).expect("lifecycle response");
        assert_eq!(lifecycle.status, "hibernated");

        let checkpoint: CheckpointResponse = serde_json::from_value(json!({
            "status": "checkpointed",
            "checkpoint": "ckpt-00000000-0000-4000-8000-000000000001",
            "checkpoint_id": "ckpt-00000000-0000-4000-8000-000000000001"
        }))
        .expect("checkpoint response");
        assert_eq!(
            checkpoint.checkpoint_id,
            "ckpt-00000000-0000-4000-8000-000000000001"
        );

        let checkpoints: CheckpointListResponse = serde_json::from_value(json!({
            "checkpoints": [{
                "id": "ckpt-00000000-0000-4000-8000-000000000001",
                "parent": null,
                "created_at": "2026-01-01T00:00:00Z",
                "size_bytes": 3,
                "is_head": true,
                "on_head_chain": true
            }]
        }))
        .expect("checkpoint list");
        assert_eq!(checkpoints.checkpoints.len(), 1);

        let prune: PruneResponse = serde_json::from_value(json!({
            "status": "pruned",
            "removed_count": 1,
            "removed": ["ckpt-00000000-0000-4000-8000-000000000002"]
        }))
        .expect("prune response");
        assert_eq!(prune.removed_count, 1);

        let rollback: RollbackResponse = serde_json::from_value(json!({
            "status": "rolledback",
            "checkpoint": "ckpt-00000000-0000-4000-8000-000000000001"
        }))
        .expect("rollback response");
        assert_eq!(rollback.status, "rolledback");

        let cleanup: CleanupResponse = serde_json::from_value(json!({
            "destroyed": 2,
            "message": "warm pool drained"
        }))
        .expect("cleanup response");
        assert_eq!(cleanup.destroyed, 2);
    }

    #[test]
    fn daemon_error_shape_is_exact_and_nullable() {
        let error: DaemonErrorResponse = serde_json::from_value(json!({
            "code": "not_found",
            "message": "not found",
            "operation": "GET /v1/sandboxes/00000000-0000-4000-8000-000000000001",
            "sandbox_id": "00000000-0000-4000-8000-000000000001"
        }))
        .expect("daemon error");
        assert_eq!(error.code, "not_found");
        assert!(error.sandbox_id.is_some());
    }
}
