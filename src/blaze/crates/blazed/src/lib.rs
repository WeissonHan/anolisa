// SPDX-License-Identifier: Apache-2.0
//! Blaze daemon runtime and build-time provider composition.

mod api;
mod checkpoint_store;
mod cli;
mod daemon;
mod data_plane;
mod error;
#[cfg(feature = "test-failpoints")]
mod failpoint;
#[cfg(not(feature = "test-failpoints"))]
#[path = "failpoint_disabled.rs"]
mod failpoint;
mod file_provider;
mod guest;
mod metrics;
mod sandbox;
mod spawner;
mod state;
mod state_store;

use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;

use blaze_provider_api::DataPlaneProvider;
use clap::Parser;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::cli::{Cli, Command, DaemonAction};
use crate::error::Result;

/// Composition root for one data-plane provider compiled with the daemon.
///
/// This is source-level composition, not a stable dynamic-library interface.
/// The provider and `blazed` must use compatible source revisions and one
/// dependency lock when the final binary is built.
pub struct BlazeDaemonBuilder<P> {
    provider: Arc<P>,
}

impl<P: DataPlaneProvider + 'static> BlazeDaemonBuilder<P> {
    /// Select the only primary data-plane provider in this binary.
    pub fn new(provider: P) -> Self {
        Self {
            provider: Arc::new(provider),
        }
    }

    /// Run the daemon with the build-time provider and standard daemon config.
    ///
    /// Provider selection is fixed here. Tenant requests, configuration
    /// values, and filesystem plugin locations cannot replace it at runtime.
    /// An unsuccessful provider probe stops startup without falling back to
    /// the standard file provider.
    pub async fn run(self, config_path: &Path) -> anyhow::Result<()> {
        daemon::run_with_provider(config_path, self.provider)
            .await
            .map_err(anyhow::Error::from)
    }
}

/// Install the daemon's structured logging subscriber if none is installed.
pub fn initialize_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let layer = fmt::layer()
        .json()
        .with_target(true)
        .with_current_span(false);
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(layer)
        .try_init();
}

/// Execute the standard command line with the built-in file provider.
pub async fn main_entry() -> ExitCode {
    initialize_tracing();
    failpoint::announce();

    let cli = Cli::parse();
    match run_cli(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("blazed: {error}");
            ExitCode::from(1)
        }
    }
}

async fn run_cli(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Daemon(action) => match action {
            DaemonAction::Start { config } => daemon::run(&config).await,
            DaemonAction::Reload { socket } => {
                println!("Sending reload signal to daemon at {}", socket.display());
                println!("  hint: kill -HUP $(pidof blazed)");
                Ok(())
            }
            DaemonAction::Doctor { config } => {
                let config_path = config.unwrap_or_else(|| "/etc/anolisa/blaze/config.toml".into());
                println!("blazed doctor");
                println!("  config : {}", config_path.display());
                match blaze_core::config::DaemonConfig::load(&config_path) {
                    Ok(_) => println!("  config parse : ok"),
                    Err(error) => println!("  config parse : FAIL ({error})"),
                }
                Ok(())
            }
        },
    }
}
