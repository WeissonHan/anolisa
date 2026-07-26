// SPDX-License-Identifier: Apache-2.0
//! Firecracker-vsock guest agent client.

pub mod client;

pub use client::GuestClient;
pub use client::GuestExecResult;

use thiserror::Error;

/// Guest protocol and transport failures.
#[derive(Debug, Error)]
pub enum GuestError {
    /// Unix socket I/O failed.
    #[error("guest transport error: {0}")]
    Io(#[from] std::io::Error),
    /// JSON encoding or decoding failed.
    #[error("guest JSON error: {0}")]
    Json(#[from] serde_json::Error),
    /// Firecracker vsock or guest framing was invalid.
    #[error("guest protocol error: {0}")]
    #[allow(dead_code)] // Constructed once lifecycle handlers own guest operations.
    Protocol(String),
    /// Caller supplied an invalid guest operation argument.
    #[error("invalid guest request: {0}")]
    #[allow(dead_code)] // Constructed once lifecycle handlers own guest operations.
    InvalidArgument(String),
    /// A bounded guest operation timed out.
    #[error("guest operation timed out: {0}")]
    #[allow(dead_code)] // Constructed once lifecycle handlers own guest operations.
    Timeout(String),
    /// The guest returned an application error.
    #[error("guest operation failed: {0}")]
    #[allow(dead_code)] // Constructed once lifecycle handlers own guest operations.
    Rejected(String),
    /// Read or write data exceeded the configured API limit.
    #[error("guest payload too large: {actual} bytes exceeds {limit}")]
    #[allow(dead_code)] // Constructed once lifecycle handlers own guest operations.
    PayloadTooLarge {
        /// Decoded or framed byte count.
        actual: usize,
        /// Configured limit.
        limit: usize,
    },
    /// Readiness polling was cancelled during daemon shutdown.
    #[error("guest readiness wait cancelled")]
    #[allow(dead_code)] // Constructed once lifecycle handlers own guest operations.
    Cancelled,
}

/// Result alias for guest operations.
pub type Result<T> = std::result::Result<T, GuestError>;
