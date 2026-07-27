// SPDX-License-Identifier: Apache-2.0

use std::io::{self, Write};
use std::process::ExitCode;

use blazectl::cli::{Cli, Command};
use clap::Parser;

fn main() -> ExitCode {
    let cli = Cli::parse();
    let env_output = std::env::var("BLAZECTL_OUTPUT").ok();
    let output = match blazectl::cli::OutputMode::resolve(cli.output, env_output.as_deref()) {
        Ok(output) => output,
        Err(error) => return fail(error),
    };
    match cli.command {
        Command::Version => match blazectl::write_version(io::stdout().lock(), output) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => fail(error),
        },
    }
}

fn fail(error: impl std::fmt::Display) -> ExitCode {
    let _ = writeln!(io::stderr().lock(), "blazectl: {error}");
    ExitCode::FAILURE
}
