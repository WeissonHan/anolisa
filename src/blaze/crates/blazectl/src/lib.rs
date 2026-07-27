// SPDX-License-Identifier: Apache-2.0
//! Command-line client surfaces for the Blaze daemon.

use std::io::{self, Write};

pub mod cli;
pub mod client;
pub mod commands;
pub mod input;
pub mod output;
pub mod protocol;
pub mod response;

/// Return the stable local version output without contacting the daemon.
pub fn version_text() -> String {
    format!("blazectl {}", env!("CARGO_PKG_VERSION"))
}

/// Write the local version in the selected format.
///
/// # Errors
///
/// Returns an I/O error when the output cannot be written.
pub fn write_version(mut writer: impl Write, output: cli::OutputMode) -> io::Result<()> {
    match output {
        cli::OutputMode::Text => writeln!(writer, "{}", version_text()),
        cli::OutputMode::Json => {
            serde_json::to_writer(
                &mut writer,
                &serde_json::json!({
                    "name": "blazectl",
                    "version": env!("CARGO_PKG_VERSION")
                }),
            )
            .map_err(io::Error::other)?;
            writeln!(writer)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FailingWriter;

    impl Write for FailingWriter {
        fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn version_output_is_deterministic() {
        let mut text = Vec::new();
        write_version(&mut text, cli::OutputMode::Text).expect("text version");
        assert_eq!(text, format!("{}\n", version_text()).as_bytes());

        let mut json = Vec::new();
        write_version(&mut json, cli::OutputMode::Json).expect("JSON version");
        let value: serde_json::Value = serde_json::from_slice(&json).expect("JSON value");
        assert_eq!(value["name"], "blazectl");
        assert_eq!(value["version"], env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn version_output_propagates_write_failure() {
        let error = write_version(FailingWriter, cli::OutputMode::Text).expect_err("write failure");
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    }
}
