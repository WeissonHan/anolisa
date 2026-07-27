// SPDX-License-Identifier: Apache-2.0
//! Local errors for the blazed binary (daemon + CLI client).
//!
//! Wraps [`blaze_core::BlazeError`] so the daemon can additionally
//! surface I/O, hyper, and CLI-side failures without expanding the
//! public core error enum.

use std::path::PathBuf;

use thiserror::Error;

pub type Result<T> = std::result::Result<T, BlazeDaemonError>;

#[derive(Debug, Error)]
pub enum BlazeDaemonError {
    #[error("core error: {0}")]
    Core(#[from] blaze_core::BlazeError),

    #[error(transparent)]
    Guest(#[from] crate::guest::GuestError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("toml error: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("hyper http error: {0}")]
    HyperHttp(#[from] hyper::http::Error),

    #[error("hyper protocol error: {0}")]
    Hyper(#[from] hyper::Error),

    #[error(
        "could not connect to blaze daemon at {socket}: {source}\nIs the daemon running? Try: blazed daemon start --foreground"
    )]
    #[allow(dead_code)] // Constructed by client code; kept for future use.
    SocketConnect {
        socket: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("daemon returned status {status}: {body}")]
    #[allow(dead_code)] // Constructed by client code; kept for future use.
    HttpStatus { status: u16, body: String },

    #[error("invalid request: {0}")]
    BadRequest(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("request body too large: {actual} bytes exceeds {limit}")]
    PayloadTooLarge { actual: usize, limit: usize },

    #[error("operation requires recovery: {0}")]
    RecoveryRequired(String),

    #[error("internal error: {0}")]
    Internal(String),
}

impl BlazeDaemonError {
    /// HTTP status code that should accompany this error in API responses.
    pub fn status_code(&self) -> u16 {
        match self {
            BlazeDaemonError::BadRequest(_) => 400,
            BlazeDaemonError::NotFound(_) => 404,
            BlazeDaemonError::Conflict(_) => 409,
            BlazeDaemonError::PayloadTooLarge { .. } => 413,
            BlazeDaemonError::RecoveryRequired(_) => 500,
            BlazeDaemonError::HttpStatus { status, .. } => *status,
            BlazeDaemonError::Core(blaze_core::BlazeError::PolicyEvalError { .. })
            | BlazeDaemonError::Core(blaze_core::BlazeError::InvalidStateTransition { .. }) => 422,
            BlazeDaemonError::Core(blaze_core::BlazeError::BackendUnavailable { .. }) => 503,
            BlazeDaemonError::Guest(crate::guest::GuestError::InvalidArgument(_)) => 400,
            BlazeDaemonError::Guest(crate::guest::GuestError::Timeout(_)) => 504,
            BlazeDaemonError::Guest(crate::guest::GuestError::PayloadTooLarge { .. }) => 413,
            BlazeDaemonError::Guest(crate::guest::GuestError::Cancelled) => 503,
            BlazeDaemonError::Guest(_) => 502,
            _ => 500,
        }
    }

    /// Stable machine-readable API error code.
    pub fn code(&self) -> &'static str {
        match self {
            BlazeDaemonError::BadRequest(_) => "invalid_request",
            BlazeDaemonError::NotFound(_) => "not_found",
            BlazeDaemonError::Conflict(_) => "state_conflict",
            BlazeDaemonError::PayloadTooLarge { .. } => "payload_too_large",
            BlazeDaemonError::RecoveryRequired(_) => "recovery_required",
            BlazeDaemonError::Guest(crate::guest::GuestError::InvalidArgument(_)) => {
                "invalid_request"
            }
            BlazeDaemonError::Guest(crate::guest::GuestError::Timeout(_)) => "guest_timeout",
            BlazeDaemonError::Guest(crate::guest::GuestError::PayloadTooLarge { .. }) => {
                "payload_too_large"
            }
            BlazeDaemonError::Guest(crate::guest::GuestError::Cancelled) => "shutting_down",
            BlazeDaemonError::Guest(_) => "guest_error",
            BlazeDaemonError::Core(blaze_core::BlazeError::BackendUnavailable { .. }) => {
                "backend_unavailable"
            }
            BlazeDaemonError::Core(blaze_core::BlazeError::InvalidStateTransition { .. }) => {
                "invalid_state_transition"
            }
            _ => "internal_error",
        }
    }
}
