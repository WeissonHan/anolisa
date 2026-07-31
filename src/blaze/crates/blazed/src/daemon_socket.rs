// SPDX-License-Identifier: Apache-2.0
//! Exclusive ownership and binding of the daemon Unix-domain socket.

use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::net::{UnixListener, UnixStream};
use tokio::time::timeout;

use crate::error::{BlazeDaemonError, Result};

const LOCK_MODE: u32 = 0o600;
const SOCKET_PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// A bound API socket whose singleton lock remains owned for the same lifetime.
pub(super) struct DaemonSocket {
    listener: Option<UnixListener>,
    _lock: DaemonLock,
}

impl DaemonSocket {
    /// Examines and replaces the socket only under exclusive ownership.
    pub(super) async fn bind(lock: DaemonLock) -> Result<Self> {
        let socket_path = &lock.socket_path;
        prepare_socket_path(socket_path).await?;
        let listener =
            UnixListener::bind(socket_path).map_err(|source| BlazeDaemonError::DaemonSocketIo {
                path: socket_path.to_path_buf(),
                source,
            })?;
        Ok(Self {
            listener: Some(listener),
            _lock: lock,
        })
    }

    /// Accepts the next client while retaining exclusive socket ownership.
    pub(super) async fn accept(&self) -> io::Result<(UnixStream, tokio::net::unix::SocketAddr)> {
        let listener = self.listener.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotConnected,
                "daemon socket is no longer accepting connections",
            )
        })?;
        listener.accept().await
    }

    /// Closes the listener while retaining exclusive ownership for cleanup.
    pub(super) fn stop_accepting(&mut self) {
        self.listener.take();
    }
}

/// Exclusive daemon ownership tied to one configured API socket.
pub(super) struct DaemonLock {
    _file: File,
    socket_path: PathBuf,
}

impl DaemonLock {
    /// Acquires ownership before daemon subsystems begin startup.
    pub(super) fn acquire(socket_path: &Path) -> Result<Self> {
        let lock_path = lock_path_for(socket_path);
        let (file, created) = open_lock_file(&lock_path)?;
        if created {
            file.set_permissions(fs::Permissions::from_mode(LOCK_MODE))
                .map_err(|source| BlazeDaemonError::DaemonLockIo {
                    path: lock_path.clone(),
                    source,
                })?;
        }

        let opened_metadata = validate_opened_lock(&file, &lock_path).map_err(|reason| {
            BlazeDaemonError::InvalidDaemonLock {
                path: lock_path.clone(),
                reason,
            }
        })?;

        // SAFETY: `file` owns a valid descriptor for the entire call. `flock`
        // changes only the advisory lock associated with that open file.
        let lock_result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if lock_result != 0 {
            let source = io::Error::last_os_error();
            if source.kind() == io::ErrorKind::WouldBlock {
                return Err(BlazeDaemonError::DaemonAlreadyRunning {
                    socket: socket_path.to_path_buf(),
                });
            }
            return Err(BlazeDaemonError::DaemonLockIo {
                path: lock_path,
                source,
            });
        }

        validate_locked_path(&lock_path, &opened_metadata).map_err(|reason| {
            BlazeDaemonError::InvalidDaemonLock {
                path: lock_path,
                reason,
            }
        })?;

        // The file is deliberately left on disk. Removing an advisory-lock
        // file would let a new process lock a different inode during teardown.
        Ok(Self {
            _file: file,
            socket_path: socket_path.to_path_buf(),
        })
    }
}

impl Drop for DaemonLock {
    fn drop(&mut self) {
        // SAFETY: `_file` remains open while its advisory lock is released.
        // Process termination still closes the descriptor and releases the
        // lock if this destructor cannot run.
        unsafe {
            libc::flock(self._file.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

fn lock_path_for(socket_path: &Path) -> PathBuf {
    let mut lock_path = OsString::from(socket_path.as_os_str());
    lock_path.push(".lock");
    PathBuf::from(lock_path)
}

fn open_lock_file(path: &Path) -> Result<(File, bool)> {
    match lock_options(true).open(path) {
        Ok(file) => Ok((file, true)),
        Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
            let file = lock_options(false).open(path).map_err(|source| {
                BlazeDaemonError::DaemonLockIo {
                    path: path.to_path_buf(),
                    source,
                }
            })?;
            Ok((file, false))
        }
        Err(source) => Err(BlazeDaemonError::DaemonLockIo {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn lock_options(create_new: bool) -> OpenOptions {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(true)
        .create_new(create_new)
        .mode(LOCK_MODE)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    options
}

fn validate_opened_lock(file: &File, path: &Path) -> std::result::Result<fs::Metadata, String> {
    let metadata = file
        .metadata()
        .map_err(|error| format!("cannot inspect opened file: {error}"))?;
    if !metadata.file_type().is_file() {
        return Err("lock target is not a regular file".to_string());
    }
    if metadata.mode() & 0o7777 != LOCK_MODE {
        return Err(format!(
            "mode must be {LOCK_MODE:#o}, found {:#o}",
            metadata.mode() & 0o7777
        ));
    }
    // SAFETY: `geteuid` has no preconditions and does not modify memory.
    let effective_uid = unsafe { libc::geteuid() };
    if metadata.uid() != effective_uid {
        return Err(format!(
            "owner uid {} does not match effective uid {effective_uid}",
            metadata.uid()
        ));
    }
    if metadata.nlink() != 1 {
        return Err(format!(
            "lock file must have one link, found {}",
            metadata.nlink()
        ));
    }

    let path_metadata =
        fs::symlink_metadata(path).map_err(|error| format!("cannot inspect path: {error}"))?;
    if path_metadata.file_type().is_symlink() {
        return Err("lock path is a symbolic link".to_string());
    }
    if path_metadata.dev() != metadata.dev() || path_metadata.ino() != metadata.ino() {
        return Err("lock path changed while it was opened".to_string());
    }
    Ok(metadata)
}

fn validate_locked_path(
    path: &Path,
    opened_metadata: &fs::Metadata,
) -> std::result::Result<(), String> {
    let path_metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("cannot inspect locked path: {error}"))?;
    if path_metadata.file_type().is_symlink() {
        return Err("lock path became a symbolic link".to_string());
    }
    if path_metadata.dev() != opened_metadata.dev() || path_metadata.ino() != opened_metadata.ino()
    {
        return Err("lock path changed while ownership was acquired".to_string());
    }
    if path_metadata.mode() & 0o7777 != LOCK_MODE {
        return Err(format!(
            "mode changed while ownership was acquired: found {:#o}",
            path_metadata.mode() & 0o7777
        ));
    }
    Ok(())
}

async fn prepare_socket_path(socket_path: &Path) -> Result<()> {
    match fs::symlink_metadata(socket_path) {
        Ok(metadata) if metadata.file_type().is_socket() => {
            match timeout(SOCKET_PROBE_TIMEOUT, UnixStream::connect(socket_path)).await {
                Ok(Ok(_stream)) => Err(BlazeDaemonError::DaemonAlreadyRunning {
                    socket: socket_path.to_path_buf(),
                }),
                Ok(Err(source)) if source.kind() == io::ErrorKind::ConnectionRefused => {
                    fs::remove_file(socket_path).map_err(|source| {
                        BlazeDaemonError::DaemonSocketIo {
                            path: socket_path.to_path_buf(),
                            source,
                        }
                    })
                }
                Ok(Err(source)) if source.kind() == io::ErrorKind::NotFound => Ok(()),
                Ok(Err(source)) => Err(BlazeDaemonError::DaemonSocketIo {
                    path: socket_path.to_path_buf(),
                    source,
                }),
                Err(_) => Err(BlazeDaemonError::InvalidDaemonSocket {
                    path: socket_path.to_path_buf(),
                    reason: "existing socket did not complete the ownership probe".to_string(),
                }),
            }
        }
        Ok(metadata) => {
            let kind = if metadata.file_type().is_symlink() {
                "symbolic link"
            } else if metadata.is_dir() {
                "directory"
            } else {
                "non-socket file"
            };
            Err(BlazeDaemonError::InvalidDaemonSocket {
                path: socket_path.to_path_buf(),
                reason: format!("existing path is a {kind}"),
            })
        }
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(BlazeDaemonError::DaemonSocketIo {
            path: socket_path.to_path_buf(),
            source,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;
    use std::os::unix::net::UnixListener as StdUnixListener;
    use std::process::{Command, Stdio};

    const ABRUPT_EXIT_SOCKET_ENV: &str = "BLAZE_TEST_ABRUPT_EXIT_SOCKET";
    const ABRUPT_EXIT_READY_ENV: &str = "BLAZE_TEST_ABRUPT_EXIT_READY";
    static SOCKET_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    async fn serialize_socket_test() -> tokio::sync::MutexGuard<'static, ()> {
        // The abrupt-exit test starts a helper process. Between fork and exec,
        // that helper can inherit another concurrent test's listener and keep
        // its endpoint reachable after the parent test drops the listener.
        SOCKET_TEST_LOCK.lock().await
    }

    fn socket_inode(path: &Path) -> u64 {
        fs::symlink_metadata(path).expect("socket metadata").ino()
    }

    async fn claim_and_bind(path: &Path) -> Result<DaemonSocket> {
        DaemonSocket::bind(DaemonLock::acquire(path)?).await
    }

    async fn claim_and_bind_after_listener_handoff(path: &Path) -> Result<DaemonSocket> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        loop {
            // Lock release must be immediate. Only bind is retried because an
            // unrelated child between fork and exec can briefly inherit the
            // old CLOEXEC listener and keep its endpoint reachable.
            let lock = DaemonLock::acquire(path)?;
            match DaemonSocket::bind(lock).await {
                Err(BlazeDaemonError::DaemonAlreadyRunning { .. })
                    if tokio::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                result => return result,
            }
        }
    }

    #[tokio::test]
    async fn second_daemon_cannot_replace_owned_socket() {
        let _test_guard = serialize_socket_test().await;
        let temp = tempfile::tempdir().expect("tempdir");
        let socket_path = temp.path().join("api.sock");
        let first = claim_and_bind(&socket_path)
            .await
            .expect("first daemon binds");
        let first_inode = socket_inode(&socket_path);

        let error = DaemonLock::acquire(&socket_path)
            .err()
            .expect("second daemon must be rejected");

        assert!(matches!(
            error,
            BlazeDaemonError::DaemonAlreadyRunning { .. }
        ));
        assert_eq!(socket_inode(&socket_path), first_inode);
        drop(first);
    }

    #[tokio::test]
    async fn released_lock_can_be_reacquired_and_stale_socket_replaced() {
        let _test_guard = serialize_socket_test().await;
        let temp = tempfile::tempdir().expect("tempdir");
        let socket_path = temp.path().join("api.sock");
        let first = claim_and_bind(&socket_path)
            .await
            .expect("first daemon binds");
        drop(first);

        let second = claim_and_bind_after_listener_handoff(&socket_path)
            .await
            .expect("released lock is reusable");

        assert!(
            fs::symlink_metadata(&socket_path)
                .expect("replacement socket metadata")
                .file_type()
                .is_socket()
        );
        drop(second);
    }

    #[tokio::test]
    async fn closing_listener_retains_daemon_ownership() {
        let _test_guard = serialize_socket_test().await;
        let temp = tempfile::tempdir().expect("tempdir");
        let socket_path = temp.path().join("api.sock");
        let mut daemon = claim_and_bind(&socket_path).await.expect("daemon binds");

        daemon.stop_accepting();
        let error = DaemonLock::acquire(&socket_path)
            .err()
            .expect("closed listener must retain daemon ownership");
        assert!(matches!(
            error,
            BlazeDaemonError::DaemonAlreadyRunning { .. }
        ));

        drop(daemon);
        let recovered = DaemonLock::acquire(&socket_path).expect("ownership releases with daemon");
        drop(recovered);
    }

    #[tokio::test]
    async fn lock_is_released_after_owner_process_exits_abruptly() {
        let _test_guard = serialize_socket_test().await;
        let temp = tempfile::tempdir().expect("tempdir");
        let socket_path = temp.path().join("api.sock");
        let ready_path = temp.path().join("lock-ready");
        let status = Command::new(std::env::current_exe().expect("current test binary"))
            .arg("--exact")
            .arg("daemon_socket::tests::abrupt_exit_lock_helper")
            .arg("--nocapture")
            .env(ABRUPT_EXIT_SOCKET_ENV, &socket_path)
            .env(ABRUPT_EXIT_READY_ENV, &ready_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("run abrupt-exit helper");

        assert_eq!(status.code(), Some(73));
        assert_eq!(
            fs::read(&ready_path).expect("helper acquired lock"),
            b"locked"
        );

        let recovered = DaemonLock::acquire(&socket_path).expect("kernel released process lock");
        drop(recovered);
    }

    #[test]
    fn abrupt_exit_lock_helper() {
        let Some(socket_path) = std::env::var_os(ABRUPT_EXIT_SOCKET_ENV) else {
            return;
        };
        let Some(ready_path) = std::env::var_os(ABRUPT_EXIT_READY_ENV) else {
            return;
        };
        let _lock =
            DaemonLock::acquire(Path::new(&socket_path)).expect("helper acquires daemon lock");
        fs::write(ready_path, b"locked").expect("publish helper readiness");

        // SAFETY: `_exit` terminates only this dedicated helper process. It
        // intentionally skips Rust destructors to exercise kernel lock release.
        unsafe {
            libc::_exit(73);
        }
    }

    #[tokio::test]
    async fn stale_socket_is_untouched_when_lock_is_held() {
        let _test_guard = serialize_socket_test().await;
        let temp = tempfile::tempdir().expect("tempdir");
        let socket_path = temp.path().join("api.sock");
        let stale = StdUnixListener::bind(&socket_path).expect("bind stale socket");
        drop(stale);
        let stale_inode = socket_inode(&socket_path);
        let lock = DaemonLock::acquire(&socket_path).expect("hold daemon lock");

        let error = DaemonLock::acquire(&socket_path)
            .err()
            .expect("competing daemon must be rejected");

        assert!(matches!(
            error,
            BlazeDaemonError::DaemonAlreadyRunning { .. }
        ));
        assert_eq!(socket_inode(&socket_path), stale_inode);
        drop(lock);

        let daemon = claim_and_bind_after_listener_handoff(&socket_path)
            .await
            .expect("owner may replace stale socket");
        assert!(
            fs::symlink_metadata(&socket_path)
                .expect("replacement socket metadata")
                .file_type()
                .is_socket()
        );
        drop(daemon);
    }

    #[tokio::test]
    async fn symlinked_lock_is_rejected_without_touching_socket() {
        let _test_guard = serialize_socket_test().await;
        let temp = tempfile::tempdir().expect("tempdir");
        let socket_path = temp.path().join("api.sock");
        let stale = StdUnixListener::bind(&socket_path).expect("bind stale socket");
        drop(stale);
        let stale_inode = socket_inode(&socket_path);
        let target = temp.path().join("lock-target");
        File::create(&target).expect("create target");
        symlink(&target, lock_path_for(&socket_path)).expect("create lock symlink");

        assert!(DaemonLock::acquire(&socket_path).is_err());
        assert_eq!(socket_inode(&socket_path), stale_inode);
    }

    #[tokio::test]
    async fn live_socket_without_lock_is_preserved() {
        let _test_guard = serialize_socket_test().await;
        let temp = tempfile::tempdir().expect("tempdir");
        let socket_path = temp.path().join("api.sock");
        let legacy_listener =
            StdUnixListener::bind(&socket_path).expect("bind legacy daemon socket");
        let original_inode = socket_inode(&socket_path);
        let lock = DaemonLock::acquire(&socket_path).expect("acquire new daemon lock");

        let error = DaemonSocket::bind(lock)
            .await
            .err()
            .expect("live socket must be rejected");

        assert!(matches!(
            error,
            BlazeDaemonError::DaemonAlreadyRunning { .. }
        ));
        assert_eq!(socket_inode(&socket_path), original_inode);
        drop(legacy_listener);
    }

    #[tokio::test]
    async fn insecure_lock_mode_is_rejected_without_touching_socket() {
        let _test_guard = serialize_socket_test().await;
        let temp = tempfile::tempdir().expect("tempdir");
        let socket_path = temp.path().join("api.sock");
        let stale = StdUnixListener::bind(&socket_path).expect("bind stale socket");
        drop(stale);
        let stale_inode = socket_inode(&socket_path);
        let lock_path = lock_path_for(&socket_path);
        let lock = File::create(&lock_path).expect("create lock");
        lock.set_permissions(fs::Permissions::from_mode(0o644))
            .expect("set insecure mode");

        let error = DaemonLock::acquire(&socket_path)
            .err()
            .expect("insecure lock must be rejected");

        assert!(matches!(error, BlazeDaemonError::InvalidDaemonLock { .. }));
        assert_eq!(socket_inode(&socket_path), stale_inode);
    }

    #[tokio::test]
    async fn non_socket_endpoint_is_rejected_without_removal() {
        let _test_guard = serialize_socket_test().await;
        let temp = tempfile::tempdir().expect("tempdir");
        let socket_path = temp.path().join("api.sock");
        fs::write(&socket_path, b"do not remove").expect("write endpoint sentinel");

        let lock = DaemonLock::acquire(&socket_path).expect("acquire daemon lock");
        let error = DaemonSocket::bind(lock)
            .await
            .err()
            .expect("non-socket endpoint must be rejected");

        assert!(matches!(
            error,
            BlazeDaemonError::InvalidDaemonSocket { .. }
        ));
        assert_eq!(
            fs::read(&socket_path).expect("sentinel remains"),
            b"do not remove"
        );
    }
}
