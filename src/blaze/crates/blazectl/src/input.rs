// SPDX-License-Identifier: Apache-2.0
//! Bounded binary input for guest file writes.

use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

use thiserror::Error;

/// Maximum decoded bytes accepted by one write command.
pub const MAX_WRITE_BYTES: usize = 16 * 1024 * 1024;

/// Read approved file/stdin input without allocating beyond the frozen bound.
///
/// An explicit `-` always reads stdin. Omitted input reads non-terminal stdin
/// but rejects terminal stdin immediately.
///
/// # Errors
///
/// Returns [`WriteInputError`] when the source is missing, unreadable, or
/// exceeds [`MAX_WRITE_BYTES`].
pub fn load_write_input(
    file: Option<&Path>,
    stdin: &mut impl Read,
    stdin_is_terminal: bool,
) -> Result<Vec<u8>, WriteInputError> {
    match file {
        Some(path) if path == Path::new("-") => read_bounded(stdin),
        Some(path) => {
            let mut input = File::open(path).map_err(|source| WriteInputError::Open { source })?;
            read_bounded(&mut input)
        }
        None if stdin_is_terminal => Err(WriteInputError::TerminalStdin),
        None => read_bounded(stdin),
    }
}

/// Stable write-input failures that never reflect local paths or bytes.
#[derive(Debug, Error)]
pub enum WriteInputError {
    /// No file was selected and implicit stdin is a terminal.
    #[error("write input is required; use --file PATH or pipe stdin")]
    TerminalStdin,
    /// The selected local file could not be opened.
    #[error("could not open write input")]
    Open {
        /// I/O failure retained without reflecting its path.
        #[source]
        source: io::Error,
    },
    /// The selected input could not be read.
    #[error("could not read write input")]
    Read {
        /// I/O failure retained without reflecting source data.
        #[source]
        source: io::Error,
    },
    /// The decoded input exceeded the frozen limit.
    #[error("write input exceeded the configured size limit")]
    TooLarge,
}

fn read_bounded(reader: &mut impl Read) -> Result<Vec<u8>, WriteInputError> {
    let mut bytes = Vec::new();
    reader
        .take((MAX_WRITE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|source| WriteInputError::Read { source })?;
    if bytes.len() > MAX_WRITE_BYTES {
        return Err(WriteInputError::TooLarge);
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Cursor;
    use std::path::PathBuf;

    use uuid::Uuid;

    use super::*;

    #[test]
    fn explicit_and_implicit_stdin_preserve_binary_data() {
        let binary = [0, 159, 255, 10];
        for file in [Some(Path::new("-")), None] {
            let mut stdin = Cursor::new(binary);
            assert_eq!(
                load_write_input(file, &mut stdin, false).expect("stdin"),
                binary
            );
        }
    }

    #[test]
    fn empty_input_is_valid() {
        let mut stdin = Cursor::new(Vec::<u8>::new());
        assert_eq!(
            load_write_input(Some(Path::new("-")), &mut stdin, true).expect("empty stdin"),
            Vec::<u8>::new()
        );
    }

    #[test]
    fn omitted_terminal_stdin_returns_without_reading() {
        let mut stdin = PanicReader;
        let error = load_write_input(None, &mut stdin, true).expect_err("terminal stdin rejection");
        assert!(matches!(error, WriteInputError::TerminalStdin));
    }

    #[test]
    fn input_limit_accepts_exactly_the_bound_and_rejects_one_more() {
        let mut exact = Cursor::new(vec![0; MAX_WRITE_BYTES]);
        assert_eq!(
            load_write_input(Some(Path::new("-")), &mut exact, false)
                .expect("exact input")
                .len(),
            MAX_WRITE_BYTES
        );

        let mut oversized = Cursor::new(vec![0; MAX_WRITE_BYTES + 1]);
        let error = load_write_input(Some(Path::new("-")), &mut oversized, false)
            .expect_err("oversized input");
        assert!(matches!(error, WriteInputError::TooLarge));
    }

    #[test]
    fn named_file_is_binary_safe_and_errors_do_not_reflect_paths() {
        let path = temporary_file_path();
        let _guard = FileGuard(path.clone());
        fs::write(&path, [0, 159, 255, 10]).expect("write fixture");
        let mut stdin = PanicReader;
        assert_eq!(
            load_write_input(Some(&path), &mut stdin, true).expect("file input"),
            [0, 159, 255, 10]
        );

        let missing = temporary_file_path();
        let error = load_write_input(Some(&missing), &mut stdin, true).expect_err("missing file");
        assert!(matches!(error, WriteInputError::Open { .. }));
        assert!(!error.to_string().contains(&missing.display().to_string()));
    }

    fn temporary_file_path() -> PathBuf {
        std::env::temp_dir().join(format!("blazectl-input-{}", Uuid::new_v4()))
    }

    struct FileGuard(PathBuf);

    impl Drop for FileGuard {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    struct PanicReader;

    impl Read for PanicReader {
        fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
            panic!("stdin must not be read")
        }
    }
}
