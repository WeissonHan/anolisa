// SPDX-License-Identifier: Apache-2.0
//! Feature-gated fault hooks for daemon-level integration verification.

#![allow(dead_code)] // Call sites land with their owning lifecycle commits.

const FAILPOINTS_ENV: &str = "BLAZE_TEST_FAILPOINTS";
const FAILPOINT_FILE_ENV: &str = "BLAZE_TEST_FAILPOINT_FILE";

/// Log that a test-only binary is accepting failpoint configuration.
pub(crate) fn announce() {
    tracing::warn!(
        failpoints = %std::env::var(FAILPOINTS_ENV).unwrap_or_default(),
        failpoint_file = %std::env::var(FAILPOINT_FILE_ENV).unwrap_or_default(),
        "test-only failpoint feature enabled"
    );
}

/// Return a backend-domain error when `name` is currently armed.
pub(crate) fn backend(name: &str) -> blaze_core::Result<()> {
    if hit(name) {
        return Err(blaze_core::BlazeError::BackendError {
            msg: format!("test failpoint '{name}' triggered"),
        });
    }
    Ok(())
}

/// Return a storage-domain error when `name` is currently armed.
pub(crate) fn storage(name: &str) -> blaze_core::Result<()> {
    if hit(name) {
        return Err(blaze_core::BlazeError::StorageError {
            msg: format!("test failpoint '{name}' triggered"),
        });
    }
    Ok(())
}

/// Return a guest-domain error when `name` is currently armed.
pub(crate) fn guest(name: &str) -> crate::guest::Result<()> {
    if hit(name) {
        return Err(crate::guest::GuestError::Rejected(format!(
            "test failpoint '{name}' triggered"
        )));
    }
    Ok(())
}

/// Return a daemon state-commit error when `name` is currently armed.
pub(crate) fn state(name: &str) -> crate::error::Result<()> {
    if hit(name) {
        return Err(crate::error::BlazeDaemonError::Internal(format!(
            "test failpoint '{name}' triggered"
        )));
    }
    Ok(())
}

/// Hold an in-flight request at a durable transaction boundary.
///
/// This is compiled into test-only binaries so a verifier can terminate the
/// daemon after observing the persisted operation marker. Removing the name
/// from the failpoint file releases the request when the daemon is not killed.
pub(crate) fn pause(name: &str) {
    if armed(name) {
        tracing::warn!(failpoint = name, "test failpoint paused");
        while armed(name) {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        tracing::warn!(failpoint = name, "test failpoint released");
    }
}

fn hit(name: &str) -> bool {
    if armed(name) {
        tracing::warn!(failpoint = name, "test failpoint triggered");
        return true;
    }
    false
}

fn armed(name: &str) -> bool {
    let inline = std::env::var(FAILPOINTS_ENV).unwrap_or_default();
    let file = std::env::var(FAILPOINT_FILE_ENV)
        .ok()
        .and_then(|path| std::fs::read_to_string(path).ok())
        .unwrap_or_default();
    configured(name, &inline, &file)
}

fn configured(name: &str, inline: &str, file: &str) -> bool {
    inline
        .split(|character: char| character == ',' || character.is_whitespace())
        .chain(file.split(|character: char| character == ',' || character.is_whitespace()))
        .filter(|token| !token.is_empty())
        .any(|token| token == name)
}

#[cfg(test)]
mod tests {
    use super::configured;

    #[test]
    fn configuration_matches_complete_tokens_from_both_sources() {
        assert!(configured("before-publish", "start, before-publish", ""));
        assert!(configured("after-publish", "", "start\nafter-publish"));
        assert!(!configured("publish", "before-publish", "after-publish"));
    }
}
