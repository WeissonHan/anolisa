// SPDX-License-Identifier: Apache-2.0

use std::io::{self, IsTerminal, Write};
use std::process::ExitCode;

use blazectl::cli::{Cli, Command};
use blazectl::client::{BlazeClient, ClientConfig, ClientConfigError};
use blazectl::output::{Diagnostic, write_diagnostic};
use clap::Parser;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let env_output = std::env::var("BLAZECTL_OUTPUT").ok();
    let output = match blazectl::cli::OutputMode::resolve(cli.output, env_output.as_deref()) {
        Ok(output) => output,
        Err(error) => return fail_text(error),
    };
    if matches!(&cli.command, Command::Version) {
        return match blazectl::write_version(io::stdout().lock(), output) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => fail_text(error),
        };
    }

    let env_url = std::env::var("BLAZED_URL").ok();
    let selection = cli.endpoint.resolve(env_url.as_deref());
    let config = match ClientConfig::from_selection(selection) {
        Ok(config) => config,
        Err(error) => return fail_diagnostic(output, endpoint_diagnostic(error)),
    };
    let client = BlazeClient::new(config);
    let stdin_handle = io::stdin();
    let stdin_is_terminal = stdin_handle.is_terminal();
    let mut stdin = stdin_handle.lock();
    let stdout_handle = io::stdout();
    let mut stdout = stdout_handle.lock();
    let stderr_handle = io::stderr();
    let mut stderr = stderr_handle.lock();
    let cancellation = CancellationToken::new();
    let signal_cancellation = cancellation.clone();
    let signal_task = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            signal_cancellation.cancel();
        }
    });
    let exit_code = blazectl::commands::execute_remote(
        client,
        cancellation,
        cli.command,
        output,
        &mut stdin,
        stdin_is_terminal,
        &mut stdout,
        &mut stderr,
    )
    .await;
    signal_task.abort();
    let _ = signal_task.await;
    ExitCode::from(exit_code)
}

fn fail_text(error: impl std::fmt::Display) -> ExitCode {
    let _ = writeln!(io::stderr().lock(), "blazectl: {error}");
    ExitCode::FAILURE
}

fn fail_diagnostic(mode: blazectl::cli::OutputMode, diagnostic: Diagnostic) -> ExitCode {
    let _ = write_diagnostic(io::stderr().lock(), mode, &diagnostic);
    ExitCode::FAILURE
}

fn endpoint_diagnostic(error: ClientConfigError) -> Diagnostic {
    let (code, message) = match error {
        ClientConfigError::InvalidSocket => (
            "invalid_socket",
            "daemon socket path must be absolute and NUL-free",
        ),
        ClientConfigError::InvalidUrl => {
            ("invalid_url", "daemon URL must be an absolute HTTP origin")
        }
        ClientConfigError::UnsupportedScheme => {
            ("invalid_url_scheme", "daemon URL scheme must be http")
        }
        ClientConfigError::UserInfo => (
            "invalid_url_userinfo",
            "daemon URL must not contain userinfo",
        ),
        ClientConfigError::QueryOrFragment => (
            "invalid_url_component",
            "daemon URL must not contain query or fragment components",
        ),
        ClientConfigError::BasePath => (
            "invalid_url_path",
            "daemon URL must not contain a base path",
        ),
    };
    Diagnostic::local(code, message, "configuration", None)
}
