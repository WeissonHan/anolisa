// SPDX-License-Identifier: Apache-2.0
//! Command-line grammar for `blazectl`.

use std::path::PathBuf;

use clap::{Args, ColorChoice, Parser, Subcommand, ValueEnum};
use thiserror::Error;
use uuid::Uuid;

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

/// Client operations.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Create a sandbox.
    Create(CreateArgs),
    /// Execute one command through the guest agent.
    Exec(ExecArgs),
    /// List sandboxes.
    #[command(alias = "ls")]
    List,
    /// Destroy one sandbox or all current sandboxes.
    #[command(alias = "rm")]
    Kill(KillArgs),
    /// Hibernate a sandbox.
    Hibernate(SandboxArgs),
    /// Create a sandbox checkpoint.
    Checkpoint(SandboxArgs),
    /// Roll a sandbox back to one checkpoint.
    Rollback(RollbackArgs),
    /// List sandbox checkpoints.
    Checkpoints(SandboxArgs),
    /// Remove checkpoints outside the current head chain.
    PruneCheckpoints(SandboxArgs),
    /// Resume a hibernated sandbox.
    Resume(SandboxArgs),
    /// Drain and refresh pool devices.
    CleanupDevices,
    /// Show the current warm-pool status.
    PoolStatus,
    /// Read one guest file.
    Read(ReadArgs),
    /// Write one guest file.
    Write(WriteArgs),
    /// Print the client version without contacting the daemon.
    Version,
}

impl Command {
    /// Canonical names of the exact remote command surface.
    pub const REMOTE_NAMES: [&str; 14] = [
        "create",
        "exec",
        "list",
        "kill",
        "hibernate",
        "checkpoint",
        "rollback",
        "checkpoints",
        "prune-checkpoints",
        "resume",
        "cleanup-devices",
        "pool-status",
        "read",
        "write",
    ];

    /// Return whether this command requires a daemon request.
    pub fn is_remote(&self) -> bool {
        !matches!(self, Self::Version)
    }
}

/// Arguments for sandbox creation.
#[derive(Debug, Args)]
pub struct CreateArgs {
    /// Optional caller-selected sandbox UUID.
    #[arg(value_name = "ID")]
    pub id: Option<Uuid>,
    /// Optional template name.
    #[arg(long, value_name = "NAME")]
    pub template: Option<String>,
}

/// Arguments for guest command execution.
#[derive(Debug, Args)]
pub struct ExecArgs {
    /// Sandbox UUID.
    #[arg(value_name = "ID")]
    pub id: Uuid,
    /// Command string passed as data to the guest agent.
    #[arg(value_name = "CMD")]
    pub cmd: String,
    /// Optional guest working directory.
    #[arg(long, value_name = "PATH")]
    pub cwd: Option<String>,
}

/// Arguments shared by one-sandbox lifecycle commands.
#[derive(Debug, Args)]
pub struct SandboxArgs {
    /// Sandbox UUID.
    #[arg(value_name = "ID")]
    pub id: Uuid,
}

/// Mutually exclusive kill target forms.
#[derive(Debug, Args)]
pub struct KillArgs {
    /// Sandbox UUID.
    #[arg(
        value_name = "ID",
        conflicts_with = "all",
        required_unless_present = "all"
    )]
    pub id: Option<Uuid>,
    /// Destroy every sandbox returned by the daemon list operation.
    #[arg(long, conflicts_with = "id", required_unless_present = "id")]
    pub all: bool,
}

/// Arguments for checkpoint rollback.
#[derive(Debug, Args)]
pub struct RollbackArgs {
    /// Sandbox UUID.
    #[arg(value_name = "ID")]
    pub id: Uuid,
    /// Frozen `ckpt-<uuid>` checkpoint identifier, validated before transport.
    #[arg(value_name = "CHECKPOINT")]
    pub checkpoint: String,
}

/// Validate a checkpoint identifier before constructing a request path.
///
/// # Errors
///
/// Returns [`ArgumentError::InvalidCheckpointId`] unless the value is an
/// injection-safe `ckpt-<uuid>` identifier accepted by the daemon contract.
pub fn validate_checkpoint_id(value: &str) -> Result<&str, ArgumentError> {
    let raw = value
        .strip_prefix("ckpt-")
        .ok_or(ArgumentError::InvalidCheckpointId)?;
    if raw.contains('/') || raw.contains('\\') || Uuid::parse_str(raw).is_err() {
        return Err(ArgumentError::InvalidCheckpointId);
    }
    Ok(value)
}

/// Pre-transport argument validation failures that never reflect input.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum ArgumentError {
    /// The checkpoint identifier does not match `ckpt-<uuid>`.
    #[error("identifier must use ckpt-<uuid> format")]
    InvalidCheckpointId,
}

/// Arguments for guest file reads.
#[derive(Debug, Args)]
pub struct ReadArgs {
    /// Sandbox UUID.
    #[arg(value_name = "ID")]
    pub id: Uuid,
    /// Absolute guest path carried in the JSON body.
    #[arg(value_name = "PATH")]
    pub path: String,
}

/// Arguments for guest file writes.
#[derive(Debug, Args)]
pub struct WriteArgs {
    /// Sandbox UUID.
    #[arg(value_name = "ID")]
    pub id: Uuid,
    /// Absolute guest path carried in the JSON body.
    #[arg(value_name = "PATH")]
    pub path: String,
    /// Input file, `-` for stdin, or omitted for approved implicit stdin.
    #[arg(long, value_name = "PATH")]
    pub file: Option<PathBuf>,
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;
    use clap::error::ErrorKind;

    use super::*;

    const ID: &str = "00000000-0000-4000-8000-000000000001";
    const CHECKPOINT: &str = "ckpt-00000000-0000-4000-8000-000000000002";

    #[test]
    fn command_surface_is_exact_and_has_no_scope_creep() {
        let command = Cli::command();
        let names: Vec<_> = command
            .get_subcommands()
            .map(|subcommand| subcommand.get_name())
            .collect();
        assert_eq!(
            names,
            [
                "create",
                "exec",
                "list",
                "kill",
                "hibernate",
                "checkpoint",
                "rollback",
                "checkpoints",
                "prune-checkpoints",
                "resume",
                "cleanup-devices",
                "pool-status",
                "read",
                "write",
                "version",
            ]
        );
        assert_eq!(Command::REMOTE_NAMES.len(), 14);
        assert_eq!(&names[..14], Command::REMOTE_NAMES.as_slice());
        for forbidden in ["help", "template", "policy", "metrics", "admin", "daemon"] {
            assert!(!names.contains(&forbidden));
        }
    }

    #[test]
    fn rendered_help_is_colorless_fixed_width_and_exact() {
        let mut command = Cli::command();
        assert_eq!(command.get_color(), ColorChoice::Never);
        let help = command.render_long_help().to_string();
        let mut second_command = Cli::command();
        assert_eq!(help, second_command.render_long_help().to_string());
        assert!(!help.contains('\u{1b}'));
        assert!(!help.contains('\r'));
        assert!(
            help.lines()
                .all(|line| line.chars().count() <= HELP_TERM_WIDTH)
        );

        for name in Command::REMOTE_NAMES.into_iter().chain(["version"]) {
            assert!(help_has_command(&help, name), "missing help row: {name}");
        }
        for forbidden in ["help", "template", "policy", "metrics", "admin", "daemon"] {
            assert!(
                !help_has_command(&help, forbidden),
                "unexpected help row: {forbidden}"
            );
        }
        assert!(help.contains("--version"));

        let usage = Cli::try_parse_from(["blazectl", "kill"])
            .expect_err("missing kill target")
            .to_string();
        assert!(!usage.contains('\u{1b}'));
        assert!(!usage.contains('\r'));
        assert!(
            usage
                .lines()
                .all(|line| line.chars().count() <= HELP_TERM_WIDTH)
        );
    }

    #[test]
    fn every_remote_command_parses_without_io() {
        let invocations = [
            vec!["blazectl", "create"],
            vec!["blazectl", "exec", ID, "printf sentinel"],
            vec!["blazectl", "list"],
            vec!["blazectl", "kill", ID],
            vec!["blazectl", "hibernate", ID],
            vec!["blazectl", "checkpoint", ID],
            vec!["blazectl", "rollback", ID, CHECKPOINT],
            vec!["blazectl", "checkpoints", ID],
            vec!["blazectl", "prune-checkpoints", ID],
            vec!["blazectl", "resume", ID],
            vec!["blazectl", "cleanup-devices"],
            vec!["blazectl", "pool-status"],
            vec!["blazectl", "read", ID, "/tmp/data.bin"],
            vec!["blazectl", "write", ID, "/tmp/data.bin"],
        ];
        for invocation in invocations {
            let cli = Cli::try_parse_from(&invocation)
                .unwrap_or_else(|error| panic!("{invocation:?}: {error}"));
            assert!(cli.command.is_remote(), "{invocation:?}");
        }
    }

    #[test]
    fn command_arguments_preserve_the_frozen_shapes() {
        let create =
            Cli::try_parse_from(["blazectl", "create", ID, "--template", "base"]).expect("create");
        match create.command {
            Command::Create(args) => {
                assert_eq!(args.id, Some(Uuid::parse_str(ID).expect("UUID")));
                assert_eq!(args.template.as_deref(), Some("base"));
            }
            other => panic!("unexpected command: {other:?}"),
        }

        let exec =
            Cli::try_parse_from(["blazectl", "exec", ID, "printf sentinel", "--cwd", "/tmp"])
                .expect("exec");
        match exec.command {
            Command::Exec(args) => {
                assert_eq!(args.cmd, "printf sentinel");
                assert_eq!(args.cwd.as_deref(), Some("/tmp"));
            }
            other => panic!("unexpected command: {other:?}"),
        }

        let write = Cli::try_parse_from(["blazectl", "write", ID, "/tmp/data.bin", "--file", "-"])
            .expect("write");
        match write.command {
            Command::Write(args) => assert_eq!(args.file, Some(PathBuf::from("-"))),
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn list_and_kill_aliases_parse_to_canonical_commands() {
        assert!(matches!(
            Cli::try_parse_from(["blazectl", "ls"])
                .expect("list alias")
                .command,
            Command::List
        ));
        assert!(matches!(
            Cli::try_parse_from(["blazectl", "rm", ID])
                .expect("kill alias")
                .command,
            Command::Kill(KillArgs {
                id: Some(_),
                all: false
            })
        ));
    }

    #[test]
    fn kill_requires_exactly_one_target_form() {
        let missing = Cli::try_parse_from(["blazectl", "kill"]).expect_err("missing kill target");
        assert_eq!(missing.kind(), ErrorKind::MissingRequiredArgument);

        let conflict = Cli::try_parse_from(["blazectl", "kill", ID, "--all"])
            .expect_err("conflicting kill targets");
        assert_eq!(conflict.kind(), ErrorKind::ArgumentConflict);

        assert!(matches!(
            Cli::try_parse_from(["blazectl", "kill", "--all"])
                .expect("kill all")
                .command,
            Command::Kill(KillArgs {
                id: None,
                all: true
            })
        ));
    }

    #[test]
    fn invalid_uuid_fails_during_argument_parsing() {
        let invalid_uuid =
            Cli::try_parse_from(["blazectl", "hibernate", "not-a-uuid"]).expect_err("invalid UUID");
        assert_eq!(invalid_uuid.kind(), ErrorKind::ValueValidation);
    }

    #[test]
    fn checkpoint_validation_is_stable_and_non_reflecting() {
        assert_eq!(validate_checkpoint_id(CHECKPOINT), Ok(CHECKPOINT));
        for invalid in [
            "checkpoint",
            "../checkpoint",
            "ckpt-00000000-0000-4000-8000-000000000001/extra",
        ] {
            let error = validate_checkpoint_id(invalid).expect_err("invalid checkpoint");
            assert_eq!(error, ArgumentError::InvalidCheckpointId);
            assert!(!error.to_string().contains(invalid));
        }
    }

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

    fn help_has_command(help: &str, name: &str) -> bool {
        help.lines().any(|line| {
            line.trim_start()
                .strip_prefix(name)
                .and_then(|tail| tail.chars().next())
                .is_some_and(char::is_whitespace)
        })
    }
}
