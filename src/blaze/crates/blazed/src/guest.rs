// SPDX-License-Identifier: Apache-2.0
//! Firecracker-vsock guest readiness client.

pub mod client;

pub use client::GuestClient;

use thiserror::Error;

/// Guest readiness protocol and transport failures.
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
    Protocol(String),
    /// A bounded readiness operation timed out.
    #[error("guest readiness timed out: {0}")]
    Timeout(String),
    /// The guest returned an application error.
    #[error("guest readiness failed: {0}")]
    Rejected(String),
    /// Framed data exceeded the configured protocol limit.
    #[error("guest payload too large: {actual} bytes exceeds {limit}")]
    PayloadTooLarge {
        /// Framed byte count.
        actual: usize,
        /// Configured limit.
        limit: usize,
    },
    /// Readiness polling was cancelled during daemon shutdown.
    #[error("guest readiness wait cancelled")]
    Cancelled,
}

/// Result alias for guest readiness operations.
pub type Result<T> = std::result::Result<T, GuestError>;
