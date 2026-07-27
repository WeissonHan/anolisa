// SPDX-License-Identifier: Apache-2.0
//! Command-line grammar for `blazectl`.

use std::path::PathBuf;

use clap::{Args, ColorChoice, Parser, Subcommand, ValueEnum};
use thiserror::Error;

/// Default local daemon API socket.
pub const DEFAULT_SOCKET: &str = "/run/blaze/api.sock";

const HELP_TERM_WIDTH: usize = 100;

/// Control Blaze sandboxes through the daemon HTTP API.
#[derive(Debug, Parser)]
#[command(
    name = "blazectl",
    version,
    about,
    disable_help_subcommand = true,
    color = ColorChoice::Never,
    term_width = HELP_TERM_WIDTH
)]
pub struct Cli {
    /// Daemon endpoint selection.
    #[command(flatten)]
    pub endpoint: EndpointArgs,
    /// Render successful results as text or JSON.
    #[arg(long, global = true, value_enum)]
    pub output: Option<OutputMode>,
    /// Operation to perform.
    #[command(subcommand)]
    pub command: Command,
}

/// Mutually exclusive daemon endpoint arguments.
#[derive(Debug, Default, Args)]
pub struct EndpointArgs {
    /// Connect to a Unix domain socket.
    #[arg(long, global = true, value_name = "PATH", conflicts_with = "url")]
    pub socket: Option<PathBuf>,
    /// Connect to an explicit TCP HTTP endpoint.
    #[arg(long, global = true, value_name = "URL", conflicts_with = "socket")]
    pub url: Option<String>,
}

impl EndpointArgs {
    /// Resolve CLI flags over the optional URL environment value.
    pub fn resolve(&self, env_url: Option<&str>) -> EndpointSelection {
        if let Some(socket) = &self.socket {
            return EndpointSelection::Unix(socket.clone());
        }
        if let Some(url) = &self.url {
            return EndpointSelection::Http(url.clone());
        }
        if let Some(url) = env_url {
            return EndpointSelection::Http(url.to_string());
        }
        EndpointSelection::Unix(PathBuf::from(DEFAULT_SOCKET))
    }
}

/// Selected transport before endpoint validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EndpointSelection {
    /// HTTP over a Unix domain socket.
    Unix(PathBuf),
    /// HTTP over an explicitly selected TCP endpoint.
    Http(String),
}

/// Supported success output formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "lower")]
pub enum OutputMode {
    /// Stable human-readable output.
    Text,
    /// Exactly one machine-readable JSON value.
    Json,
}

impl OutputMode {
    /// Resolve a CLI value over the optional environment value and text default.
    ///
    /// # Errors
    ///
    /// Returns [`SelectionError::InvalidOutput`] when the environment value is
    /// neither `text` nor `json`.
    pub fn resolve(cli: Option<Self>, env_value: Option<&str>) -> Result<Self, SelectionError> {
        if let Some(mode) = cli {
            return Ok(mode);
        }
        match env_value {
            None => Ok(Self::Text),
            Some("text") => Ok(Self::Text),
            Some("json") => Ok(Self::Json),
            Some(value) => Err(SelectionError::InvalidOutput(value.to_string())),
        }
    }
}

/// Configuration selection failures detected before transport.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum SelectionError {
    /// Unsupported `BLAZECTL_OUTPUT` value.
    #[error("invalid output mode; expected text or json")]
    InvalidOutput(String),
}

/// Local client operations.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Print the client version without contacting the daemon.
    Version,
}

#[cfg(test)]
mod tests {
    use clap::error::ErrorKind;

    use super::*;

    #[test]
    fn version_subcommand_parses_locally() {
        let cli = Cli::try_parse_from(["blazectl", "version"]).expect("version command");
        assert!(matches!(cli.command, Command::Version));
        assert_eq!(
            cli.endpoint.resolve(None),
            EndpointSelection::Unix(PathBuf::from(DEFAULT_SOCKET))
        );
        assert_eq!(OutputMode::resolve(cli.output, None), Ok(OutputMode::Text));
    }

    #[test]
    fn clap_version_surface_is_local() {
        let error = Cli::try_parse_from(["blazectl", "--version"]).expect_err("display version");
        assert_eq!(error.kind(), ErrorKind::DisplayVersion);
        assert_eq!(
            error.to_string(),
            format!("blazectl {}\n", env!("CARGO_PKG_VERSION"))
        );
    }

    #[test]
    fn version_text_is_stable() {
        assert_eq!(
            crate::version_text(),
            format!("blazectl {}", env!("CARGO_PKG_VERSION"))
        );
    }

    #[test]
    fn endpoint_flags_conflict() {
        let error = Cli::try_parse_from([
            "blazectl",
            "--socket",
            "/tmp/blaze-test.sock",
            "--url",
            "http://127.0.0.1:14159",
            "version",
        ])
        .expect_err("conflicting endpoints");
        assert_eq!(error.kind(), ErrorKind::ArgumentConflict);
    }

    #[test]
    fn endpoint_flag_overrides_environment() {
        let cli = Cli::try_parse_from(["blazectl", "--socket", "/tmp/blaze-test.sock", "version"])
            .expect("socket endpoint");
        assert_eq!(
            cli.endpoint.resolve(Some("http://127.0.0.1:14159")),
            EndpointSelection::Unix(PathBuf::from("/tmp/blaze-test.sock"))
        );

        let cli = Cli::try_parse_from(["blazectl", "--url", "http://127.0.0.1:14159", "version"])
            .expect("URL endpoint");
        assert_eq!(
            cli.endpoint.resolve(Some("http://127.0.0.1:14160")),
            EndpointSelection::Http("http://127.0.0.1:14159".to_string())
        );
    }

    #[test]
    fn environment_endpoint_overrides_default() {
        assert_eq!(
            EndpointArgs::default().resolve(Some("http://127.0.0.1:14159")),
            EndpointSelection::Http("http://127.0.0.1:14159".to_string())
        );
    }

    #[test]
    fn output_precedence_is_flag_then_environment_then_text() {
        assert_eq!(
            OutputMode::resolve(Some(OutputMode::Json), Some("text")),
            Ok(OutputMode::Json)
        );
        assert_eq!(
            OutputMode::resolve(None, Some("json")),
            Ok(OutputMode::Json)
        );
        assert_eq!(OutputMode::resolve(None, None), Ok(OutputMode::Text));
        assert_eq!(
            OutputMode::resolve(None, Some("yaml")),
            Err(SelectionError::InvalidOutput("yaml".to_string()))
        );
    }
}
