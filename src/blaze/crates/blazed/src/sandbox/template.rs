// SPDX-License-Identifier: Apache-2.0
//! Durable runtime artifact publication and lookup.

use std::collections::HashSet;
use std::ffi::{CStr, CString, OsStr, OsString};
use std::fs::{DirBuilder, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};

#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};

use blaze_core::config::RuntimeTemplateSection;
use serde_json::json;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::error::{BlazeDaemonError, Result};

use super::manager::SandboxManager;

const CATALOG_DIR_MODE: u32 = 0o700;
const CATALOG_FILE_MODE: u32 = 0o600;
const COPY_BUFFER_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy)]
struct ImportLimits {
    max_files: usize,
    max_bytes: u64,
    max_metadata_bytes: u64,
    max_total_bytes: u64,
}

#[derive(Clone)]
pub(crate) struct RuntimeTemplateCatalog {
    inner: Arc<CatalogInner>,
}

struct CatalogInner {
    root: PathBuf,
    import_root: Option<PathBuf>,
    limits: ImportLimits,
    state: Mutex<CatalogState>,
    active_count: watch::Sender<usize>,
    cancellation: CancellationToken,
    #[cfg(test)]
    copy_gate: Mutex<Option<Arc<TestCopyGate>>>,
}

struct CatalogState {
    active_names: HashSet<String>,
    committed_bytes: u64,
    reserved_bytes: u64,
    stopping: bool,
    blocked: Option<String>,
}

struct ImportClaim {
    inner: Arc<CatalogInner>,
    name: String,
    reserved_bytes: u64,
}

struct PreparedFile {
    name: OsString,
    file: File,
    observed_bytes: u64,
    observed_dev: u64,
    observed_ino: u64,
    observed_mtime: i64,
    observed_mtime_nsec: i64,
    observed_ctime: i64,
    observed_ctime_nsec: i64,
}

struct PreparedImport {
    files: Vec<PreparedFile>,
    metadata: serde_json::Value,
    metadata_bytes: Vec<u8>,
    reserved_bytes: u64,
}

#[cfg(test)]
struct TestCopyGate {
    entered: tokio::sync::mpsc::UnboundedSender<()>,
    release: AtomicBool,
}

impl RuntimeTemplateCatalog {
    pub(crate) fn open(config: &RuntimeTemplateSection) -> Result<Self> {
        create_catalog_root(&config.dir)?;
        cleanup_staging(&config.dir)?;
        let limits = ImportLimits {
            max_files: config.max_files,
            max_bytes: config.max_bytes,
            max_metadata_bytes: config.max_metadata_bytes,
            max_total_bytes: config.max_total_bytes,
        };
        let committed_bytes = catalog_usage(&config.dir, limits)?;
        if committed_bytes > limits.max_total_bytes {
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "runtime template catalog uses {committed_bytes} bytes; configured limit is {}",
                limits.max_total_bytes
            )));
        }
        let (active_count, _) = watch::channel(0);
        Ok(Self {
            inner: Arc::new(CatalogInner {
                root: config.dir.clone(),
                import_root: config.import_root.clone(),
                limits,
                state: Mutex::new(CatalogState {
                    active_names: HashSet::new(),
                    committed_bytes,
                    reserved_bytes: 0,
                    stopping: false,
                    blocked: None,
                }),
                active_count,
                cancellation: CancellationToken::new(),
                #[cfg(test)]
                copy_gate: Mutex::new(None),
            }),
        })
    }

    async fn list(&self) -> Result<Vec<serde_json::Value>> {
        let catalog = self.clone();
        tokio::task::spawn_blocking(move || {
            list_published(&catalog.inner.root, catalog.inner.limits)
        })
        .await
        .map_err(join_error("runtime template list"))?
    }

    async fn get(&self, name: String) -> Result<serde_json::Value> {
        validate_name(&name, "runtime template")?;
        let catalog = self.clone();
        tokio::task::spawn_blocking(move || {
            get_published(&catalog.inner.root, &name, catalog.inner.limits)
        })
        .await
        .map_err(join_error("runtime template read"))?
    }

    async fn import(
        &self,
        name: String,
        source: PathBuf,
        description: String,
    ) -> Result<serde_json::Value> {
        validate_name(&name, "runtime template")?;
        validate_relative_source(&source)?;
        if self.inner.import_root.is_none() {
            return Err(BlazeDaemonError::Conflict(
                "runtime template import is disabled; configure \
                 runtime_templates.import_root"
                    .to_string(),
            ));
        }

        // Register before scheduling blocking work. Shutdown can therefore
        // observe and wait for an import even when the blocking pool has not
        // started its closure yet.
        let claim = ImportClaim::begin(Arc::clone(&self.inner), name.clone())?;
        let catalog = self.clone();
        tokio::task::spawn_blocking(move || {
            catalog.import_blocking(claim, name, source, description)
        })
        .await
        .map_err(join_error("runtime template import"))?
    }

    fn import_blocking(
        &self,
        mut claim: ImportClaim,
        name: String,
        source: PathBuf,
        description: String,
    ) -> Result<serde_json::Value> {
        check_cancelled(&self.inner.cancellation)?;
        let import_root =
            self.inner.import_root.as_deref().ok_or_else(|| {
                BlazeDaemonError::Conflict("runtime template import disabled".into())
            })?;
        let source = open_import_source(import_root, &source)?;
        let prepared = prepare_import(
            &source,
            &name,
            &description,
            self.inner.limits,
            &self.inner.cancellation,
        )?;
        claim.reserve(prepared.reserved_bytes)?;
        publish_prepared(
            &self.inner.root,
            &name,
            prepared,
            &self.inner.cancellation,
            &mut claim,
        )
    }

    pub(super) fn cancel_imports(&self) {
        let mut state = lock_catalog_state(&self.inner);
        state.stopping = true;
        drop(state);
        self.inner.cancellation.cancel();
    }

    pub(super) async fn wait_for_imports(&self) -> Result<()> {
        let mut active = self.inner.active_count.subscribe();
        loop {
            if *active.borrow_and_update() == 0 {
                return Ok(());
            }
            active.changed().await.map_err(|_| {
                BlazeDaemonError::Internal(
                    "runtime template import supervisor closed unexpectedly".to_string(),
                )
            })?;
        }
    }

    #[cfg(test)]
    fn active_imports(&self) -> usize {
        *self.inner.active_count.borrow()
    }

    #[cfg(test)]
    fn install_copy_gate(&self) -> tokio::sync::mpsc::UnboundedReceiver<()> {
        let (entered, receiver) = tokio::sync::mpsc::unbounded_channel();
        let gate = Arc::new(TestCopyGate {
            entered,
            release: AtomicBool::new(false),
        });
        *self.inner.copy_gate.lock().expect("copy gate lock") = Some(gate);
        receiver
    }
}

impl ImportClaim {
    fn begin(inner: Arc<CatalogInner>, name: String) -> Result<Self> {
        let mut state = lock_catalog_state(&inner);
        if state.stopping {
            return Err(BlazeDaemonError::ServiceUnavailable(
                "runtime template imports are stopping".to_string(),
            ));
        }
        if let Some(error) = &state.blocked {
            return Err(BlazeDaemonError::RecoveryRequired(error.clone()));
        }
        if !state.active_names.insert(name.clone()) {
            return Err(BlazeDaemonError::Conflict(format!(
                "runtime template {name} import is already in progress"
            )));
        }
        let count = state.active_names.len();
        inner.active_count.send_replace(count);
        drop(state);
        Ok(Self {
            inner,
            name,
            reserved_bytes: 0,
        })
    }

    fn reserve(&mut self, bytes: u64) -> Result<()> {
        let mut state = lock_catalog_state(&self.inner);
        if state.stopping || self.inner.cancellation.is_cancelled() {
            return Err(BlazeDaemonError::ServiceUnavailable(
                "runtime template imports are stopping".to_string(),
            ));
        }
        if let Some(error) = &state.blocked {
            return Err(BlazeDaemonError::RecoveryRequired(error.clone()));
        }
        let used = state
            .committed_bytes
            .checked_add(state.reserved_bytes)
            .and_then(|value| value.checked_add(bytes))
            .ok_or_else(|| payload_too_large(u64::MAX, self.inner.limits.max_total_bytes))?;
        if used > self.inner.limits.max_total_bytes {
            return Err(payload_too_large(used, self.inner.limits.max_total_bytes));
        }
        state.reserved_bytes += bytes;
        self.reserved_bytes = bytes;
        Ok(())
    }

    fn publish(&mut self, actual_bytes: u64) -> Result<()> {
        if actual_bytes > self.reserved_bytes {
            let message = format!(
                "runtime template {} wrote {actual_bytes} bytes beyond its {}-byte reservation",
                self.name, self.reserved_bytes
            );
            self.block_catalog(message.clone());
            return Err(BlazeDaemonError::RecoveryRequired(message));
        }
        let mut state = lock_catalog_state(&self.inner);
        let Some(remaining_reserved) = state.reserved_bytes.checked_sub(self.reserved_bytes) else {
            let message = "runtime template reservation accounting underflow".to_string();
            state.blocked = Some(message.clone());
            return Err(BlazeDaemonError::RecoveryRequired(message));
        };
        let Some(committed_bytes) = state.committed_bytes.checked_add(actual_bytes) else {
            let message = "runtime template catalog byte accounting overflow".to_string();
            state.blocked = Some(message.clone());
            return Err(BlazeDaemonError::RecoveryRequired(message));
        };
        if committed_bytes
            .checked_add(remaining_reserved)
            .is_none_or(|used| used > self.inner.limits.max_total_bytes)
        {
            let message = "runtime template catalog accounting exceeded the configured total limit"
                .to_string();
            state.blocked = Some(message.clone());
            return Err(BlazeDaemonError::RecoveryRequired(message));
        }
        state.reserved_bytes = remaining_reserved;
        state.committed_bytes = committed_bytes;
        self.reserved_bytes = 0;
        Ok(())
    }

    fn block_catalog(&self, message: String) {
        let mut state = lock_catalog_state(&self.inner);
        state.blocked = Some(message);
    }
}

impl Drop for ImportClaim {
    fn drop(&mut self) {
        let mut state = lock_catalog_state(&self.inner);
        if self.reserved_bytes > 0 {
            state.reserved_bytes = state.reserved_bytes.saturating_sub(self.reserved_bytes);
        }
        state.active_names.remove(&self.name);
        let count = state.active_names.len();
        self.inner.active_count.send_replace(count);
    }
}

impl SandboxManager {
    /// List atomically published runtime artifact sets.
    pub async fn list_runtime_templates(&self) -> Result<Vec<serde_json::Value>> {
        self.runtime_templates.list().await
    }

    /// Read one published runtime artifact set by name.
    pub async fn get_runtime_template(&self, name: String) -> Result<serde_json::Value> {
        self.runtime_templates.get(name).await
    }

    /// Copy and atomically publish one operator-prepared artifact directory.
    pub async fn import_runtime_template(
        &self,
        name: String,
        source: PathBuf,
        description: String,
    ) -> Result<serde_json::Value> {
        self.runtime_templates
            .import(name, source, description)
            .await
    }

    /// Reject new imports and request cancellation of every active import.
    pub(crate) fn cancel_runtime_template_imports(&self) {
        self.runtime_templates.cancel_imports();
    }

    /// Wait until every registered import has released its filesystem handles.
    pub(crate) async fn wait_for_runtime_template_imports(&self) -> Result<()> {
        self.runtime_templates.wait_for_imports().await
    }
}

fn prepare_import(
    source: &File,
    name: &str,
    description: &str,
    limits: ImportLimits,
    cancellation: &CancellationToken,
) -> Result<PreparedImport> {
    let names = source_entry_names(source)?;
    let mut files = Vec::with_capacity(names.len());
    let mut metadata_file = None;
    let mut artifact_bytes = 0_u64;

    for entry_name in names {
        check_cancelled(cancellation)?;
        validate_artifact_name(&entry_name)?;
        let file = openat_regular(source, &entry_name)?;
        let metadata = file.metadata()?;
        validate_source_file(&metadata, &entry_name)?;
        if entry_name == OsStr::new("template.json") {
            if metadata.len() > limits.max_metadata_bytes {
                return Err(payload_too_large(metadata.len(), limits.max_metadata_bytes));
            }
            metadata_file = Some(file);
        } else {
            artifact_bytes = artifact_bytes
                .checked_add(metadata.len())
                .ok_or_else(|| payload_too_large(u64::MAX, limits.max_bytes))?;
            files.push(PreparedFile {
                name: entry_name,
                file,
                observed_bytes: metadata.len(),
                observed_dev: metadata.dev(),
                observed_ino: metadata.ino(),
                observed_mtime: metadata.mtime(),
                observed_mtime_nsec: metadata.mtime_nsec(),
                observed_ctime: metadata.ctime(),
                observed_ctime_nsec: metadata.ctime_nsec(),
            });
        }
    }

    let published_files = files.len() + 1;
    if published_files > limits.max_files {
        return Err(BlazeDaemonError::BadRequest(format!(
            "runtime template contains {published_files} files; limit is {}",
            limits.max_files
        )));
    }
    let present = files
        .iter()
        .map(|file| file.name.as_os_str())
        .collect::<HashSet<_>>();
    for required in ["vmstate.snap", "mem.bin", "rootfs.ext4"] {
        if !present.contains(OsStr::new(required)) {
            return Err(BlazeDaemonError::BadRequest(format!(
                "runtime template source is missing regular artifact {required}"
            )));
        }
    }

    let mut metadata = match metadata_file {
        Some(mut file) => {
            let observed = file.metadata()?;
            let metadata = read_json_bounded(&mut file, limits.max_metadata_bytes)?;
            let current = file.metadata()?;
            if !same_file_identity(&observed, &current) {
                return Err(BlazeDaemonError::BadRequest(
                    "runtime template source metadata changed while it was imported".to_string(),
                ));
            }
            metadata
        }
        None => json!({"name": name}),
    };
    if !metadata.is_object() {
        return Err(BlazeDaemonError::BadRequest(
            "template.json must contain a JSON object".to_string(),
        ));
    }
    metadata["name"] = json!(name);
    if !description.is_empty() {
        metadata["description"] = json!(description);
    }
    if metadata
        .get("rootfs_size")
        .and_then(serde_json::Value::as_u64)
        .is_none()
    {
        metadata["rootfs_size"] = json!(8_u64 * 1024 * 1024 * 1024);
    }
    if metadata
        .get("memory_size")
        .and_then(serde_json::Value::as_u64)
        .is_none()
    {
        metadata["memory_size"] = json!(4_u64 * 1024 * 1024 * 1024);
    }
    let metadata_bytes = serde_json::to_vec_pretty(&metadata)?;
    let metadata_len = u64::try_from(metadata_bytes.len()).unwrap_or(u64::MAX);
    if metadata_len > limits.max_metadata_bytes {
        return Err(payload_too_large(metadata_len, limits.max_metadata_bytes));
    }
    let reserved_bytes = artifact_bytes
        .checked_add(metadata_len)
        .ok_or_else(|| payload_too_large(u64::MAX, limits.max_bytes))?;
    if reserved_bytes > limits.max_bytes {
        return Err(payload_too_large(reserved_bytes, limits.max_bytes));
    }

    Ok(PreparedImport {
        files,
        metadata,
        metadata_bytes,
        reserved_bytes,
    })
}

fn publish_prepared(
    root: &Path,
    name: &str,
    prepared: PreparedImport,
    cancellation: &CancellationToken,
    claim: &mut ImportClaim,
) -> Result<serde_json::Value> {
    let destination = root.join(name);
    if destination.exists() {
        return Err(BlazeDaemonError::Conflict(format!(
            "runtime template {name} already exists"
        )));
    }

    let staging = root.join(format!(".import-{name}-{}.tmp", Uuid::new_v4()));
    create_private_directory(&staging)?;
    #[cfg(test)]
    wait_for_copy_gate(&claim.inner, cancellation);
    let result = populate_and_publish(
        root,
        &destination,
        &staging,
        name,
        prepared,
        cancellation,
        claim,
    );
    if result.is_err() && staging.exists() {
        if let Err(cleanup_error) = std::fs::remove_dir_all(&staging) {
            claim.block_catalog(format!(
                "runtime template staging cleanup failed; restart after repairing the catalog: \
                 {cleanup_error}"
            ));
            tracing::error!(
                path = %staging.display(),
                error = %cleanup_error,
                "runtime template staging cleanup failed"
            );
        } else if let Err(sync_error) = sync_directory(root) {
            claim.block_catalog(format!(
                "runtime template cleanup durability is unknown; restart after repairing the \
                 catalog: {sync_error}"
            ));
            tracing::error!(
                path = %root.display(),
                error = %sync_error,
                "runtime template cleanup durability is unknown"
            );
        }
    }
    result
}

fn populate_and_publish(
    root: &Path,
    destination: &Path,
    staging: &Path,
    name: &str,
    mut prepared: PreparedImport,
    cancellation: &CancellationToken,
    claim: &mut ImportClaim,
) -> Result<serde_json::Value> {
    let mut actual_bytes = 0_u64;
    for source in &mut prepared.files {
        check_cancelled(cancellation)?;
        let remaining = prepared
            .reserved_bytes
            .checked_sub(actual_bytes)
            .and_then(|value| {
                value.checked_sub(u64::try_from(prepared.metadata_bytes.len()).unwrap_or(u64::MAX))
            })
            .ok_or_else(|| payload_too_large(u64::MAX, prepared.reserved_bytes))?;
        let destination_file = staging.join(&source.name);
        let copied =
            copy_regular_file(&mut source.file, &destination_file, remaining, cancellation)?;
        let current = source.file.metadata()?;
        if copied != source.observed_bytes
            || current.len() != source.observed_bytes
            || current.dev() != source.observed_dev
            || current.ino() != source.observed_ino
            || current.mtime() != source.observed_mtime
            || current.mtime_nsec() != source.observed_mtime_nsec
            || current.ctime() != source.observed_ctime
            || current.ctime_nsec() != source.observed_ctime_nsec
        {
            return Err(BlazeDaemonError::BadRequest(format!(
                "runtime template source file {} changed while it was imported",
                source.name.to_string_lossy()
            )));
        }
        actual_bytes = actual_bytes
            .checked_add(copied)
            .ok_or_else(|| payload_too_large(u64::MAX, prepared.reserved_bytes))?;
    }

    let metadata_len = u64::try_from(prepared.metadata_bytes.len()).unwrap_or(u64::MAX);
    actual_bytes = actual_bytes
        .checked_add(metadata_len)
        .ok_or_else(|| payload_too_large(u64::MAX, prepared.reserved_bytes))?;
    if actual_bytes > prepared.reserved_bytes {
        return Err(payload_too_large(actual_bytes, prepared.reserved_bytes));
    }
    write_file_durable(&staging.join("template.json"), &prepared.metadata_bytes)?;
    sync_directory(staging)?;
    check_cancelled(cancellation)?;

    if destination.exists() {
        return Err(BlazeDaemonError::Conflict(format!(
            "runtime template {name} already exists"
        )));
    }
    rename_no_replace(staging, destination).map_err(|error| {
        if destination.exists() {
            BlazeDaemonError::Conflict(format!("runtime template {name} already exists"))
        } else {
            error.into()
        }
    })?;

    // The directory is now publicly owned even if the parent fsync fails.
    // Account for it before reporting an uncertain durability result.
    claim.publish(actual_bytes)?;
    if let Err(error) = sync_directory(root) {
        let message = format!(
            "runtime template {name} was published but catalog durability is unknown: {error}"
        );
        claim.block_catalog(message.clone());
        return Err(BlazeDaemonError::RecoveryRequired(message));
    }
    Ok(prepared.metadata)
}

fn open_import_source(import_root: &Path, relative: &Path) -> Result<File> {
    let mut directory = open_directory_no_follow(import_root).map_err(|error| {
        BlazeDaemonError::BadRequest(format!(
            "cannot open configured runtime template import root {}: {error}",
            import_root.display()
        ))
    })?;
    validate_source_directory(&directory.metadata()?, import_root)?;
    for component in relative.components() {
        let Component::Normal(name) = component else {
            return Err(BlazeDaemonError::BadRequest(
                "runtime template source must be a non-empty relative path below the configured \
                 import root"
                    .to_string(),
            ));
        };
        directory = openat_directory(&directory, name).map_err(|error| {
            BlazeDaemonError::BadRequest(format!(
                "cannot open runtime template source {}: {error}",
                relative.display()
            ))
        })?;
        validate_source_directory(&directory.metadata()?, relative)?;
    }
    Ok(directory)
}

fn source_entry_names(directory: &File) -> Result<Vec<OsString>> {
    let duplicated = unsafe { libc::dup(directory.as_raw_fd()) };
    if duplicated < 0 {
        return Err(io::Error::last_os_error().into());
    }
    let stream = unsafe { libc::fdopendir(duplicated) };
    if stream.is_null() {
        let error = io::Error::last_os_error();
        unsafe {
            libc::close(duplicated);
        }
        return Err(error.into());
    }
    let mut names = Vec::new();
    loop {
        clear_errno();
        let entry = unsafe { libc::readdir(stream) };
        if entry.is_null() {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(0) {
                unsafe {
                    libc::closedir(stream);
                }
                return Err(error.into());
            }
            break;
        }
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
        if name != b"." && name != b".." {
            names.push(OsString::from_vec(name.to_vec()));
        }
    }
    let close_result = unsafe { libc::closedir(stream) };
    if close_result != 0 {
        return Err(io::Error::last_os_error().into());
    }
    names.sort();
    Ok(names)
}

#[cfg(target_os = "linux")]
fn clear_errno() {
    unsafe {
        *libc::__errno_location() = 0;
    }
}

#[cfg(not(target_os = "linux"))]
fn clear_errno() {
    unsafe {
        *libc::__error() = 0;
    }
}

fn open_directory_no_follow(path: &Path) -> io::Result<File> {
    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))?;
    let fd = unsafe {
        libc::open(
            path.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW,
        )
    };
    file_from_fd(fd)
}

fn openat_directory(parent: &File, name: &OsStr) -> io::Result<File> {
    let name = CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "name contains NUL"))?;
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW,
        )
    };
    file_from_fd(fd)
}

fn openat_regular(parent: &File, name: &OsStr) -> Result<File> {
    let name_c = CString::new(name.as_bytes())
        .map_err(|_| BlazeDaemonError::BadRequest("artifact name contains NUL".to_string()))?;
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name_c.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        )
    };
    file_from_fd(fd).map_err(|error| {
        BlazeDaemonError::BadRequest(format!(
            "cannot open runtime template source entry {} without following links: {error}",
            name.to_string_lossy()
        ))
    })
}

fn file_from_fd(fd: libc::c_int) -> io::Result<File> {
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn validate_source_directory(metadata: &std::fs::Metadata, path: &Path) -> Result<()> {
    if !metadata.is_dir() {
        return Err(BlazeDaemonError::BadRequest(format!(
            "runtime template source {} is not a directory",
            path.display()
        )));
    }
    let expected_uid = unsafe { libc::geteuid() };
    if metadata.uid() != expected_uid || metadata.mode() & 0o022 != 0 {
        return Err(BlazeDaemonError::BadRequest(format!(
            "runtime template source directory {} must be owned by the daemon user and not \
             writable by group or other users",
            path.display()
        )));
    }
    Ok(())
}

fn validate_source_file(metadata: &std::fs::Metadata, name: &OsStr) -> Result<()> {
    if !metadata.file_type().is_file() {
        return Err(BlazeDaemonError::BadRequest(format!(
            "runtime template source entry {} is not a regular file",
            name.to_string_lossy()
        )));
    }
    let expected_uid = unsafe { libc::geteuid() };
    if metadata.uid() != expected_uid || metadata.mode() & 0o022 != 0 {
        return Err(BlazeDaemonError::BadRequest(format!(
            "runtime template source file {} must be owned by the daemon user and not writable \
             by group or other users",
            name.to_string_lossy()
        )));
    }
    Ok(())
}

fn same_file_identity(observed: &std::fs::Metadata, current: &std::fs::Metadata) -> bool {
    observed.len() == current.len()
        && observed.dev() == current.dev()
        && observed.ino() == current.ino()
        && observed.mtime() == current.mtime()
        && observed.mtime_nsec() == current.mtime_nsec()
        && observed.ctime() == current.ctime()
        && observed.ctime_nsec() == current.ctime_nsec()
}

fn create_catalog_root(root: &Path) -> Result<()> {
    if !root.exists() {
        DirBuilder::new()
            .recursive(true)
            .mode(CATALOG_DIR_MODE)
            .create(root)?;
        if let Some(parent) = root.parent() {
            sync_directory(parent)?;
        }
    }
    enforce_owned_mode(root, true, CATALOG_DIR_MODE)
}

fn create_private_directory(path: &Path) -> Result<()> {
    DirBuilder::new().mode(CATALOG_DIR_MODE).create(path)?;
    enforce_owned_mode(path, true, CATALOG_DIR_MODE)
}

fn enforce_owned_mode(path: &Path, directory: bool, mode: u32) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink()
        || (directory && !metadata.is_dir())
        || (!directory && !metadata.file_type().is_file())
    {
        return Err(BlazeDaemonError::RecoveryRequired(format!(
            "runtime template catalog path {} has an unexpected file type",
            path.display()
        )));
    }
    let expected_uid = unsafe { libc::geteuid() };
    if metadata.uid() != expected_uid {
        return Err(BlazeDaemonError::RecoveryRequired(format!(
            "runtime template catalog path {} is not owned by the daemon user",
            path.display()
        )));
    }
    if metadata.mode() & 0o777 != mode {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))?;
    }
    Ok(())
}

fn catalog_usage(root: &Path, limits: ImportLimits) -> Result<u64> {
    let mut total = 0_u64;
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        if name.to_string_lossy().starts_with('.') {
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "runtime template catalog contains unresolved hidden entry {}",
                entry.path().display()
            )));
        }
        enforce_owned_mode(&entry.path(), true, CATALOG_DIR_MODE)?;
        let mut file_count = 0_usize;
        let mut template_bytes = 0_u64;
        for artifact in std::fs::read_dir(entry.path())? {
            let artifact = artifact?;
            enforce_owned_mode(&artifact.path(), false, CATALOG_FILE_MODE)?;
            file_count = file_count.checked_add(1).ok_or_else(|| {
                BlazeDaemonError::RecoveryRequired(
                    "runtime template catalog file count overflow".to_string(),
                )
            })?;
            if file_count > limits.max_files {
                return Err(BlazeDaemonError::RecoveryRequired(format!(
                    "runtime template {} exceeds the configured file limit",
                    name.to_string_lossy()
                )));
            }
            let artifact_bytes = artifact.metadata()?.len();
            if artifact.file_name() == OsStr::new("template.json")
                && artifact_bytes > limits.max_metadata_bytes
            {
                return Err(BlazeDaemonError::RecoveryRequired(format!(
                    "runtime template {} metadata exceeds the configured limit",
                    name.to_string_lossy()
                )));
            }
            template_bytes = template_bytes.checked_add(artifact_bytes).ok_or_else(|| {
                BlazeDaemonError::RecoveryRequired(
                    "runtime template byte accounting overflow".to_string(),
                )
            })?;
            total = total.checked_add(artifact_bytes).ok_or_else(|| {
                BlazeDaemonError::RecoveryRequired(
                    "runtime template catalog byte accounting overflow".to_string(),
                )
            })?;
        }
        if template_bytes > limits.max_bytes {
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "runtime template {} exceeds the configured per-import byte limit",
                name.to_string_lossy()
            )));
        }
        let name = name.into_string().map_err(|_| {
            BlazeDaemonError::RecoveryRequired(
                "runtime template catalog contains a non-UTF-8 published name".to_string(),
            )
        })?;
        validate_name(&name, "runtime template").map_err(|error| {
            BlazeDaemonError::RecoveryRequired(format!(
                "runtime template catalog contains invalid published name {name}: {error}"
            ))
        })?;
        read_published(root, &name, limits)?;
    }
    Ok(total)
}

fn list_published(root: &Path, limits: ImportLimits) -> Result<Vec<serde_json::Value>> {
    let mut templates = Vec::new();
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        if name.to_string_lossy().starts_with('.') {
            continue;
        }
        let name = name.into_string().map_err(|_| {
            BlazeDaemonError::RecoveryRequired(format!(
                "runtime template catalog contains a non-UTF-8 name at {}",
                entry.path().display()
            ))
        })?;
        templates.push(read_published(root, &name, limits)?);
    }
    templates.sort_by(|left, right| {
        left.get("name")
            .and_then(serde_json::Value::as_str)
            .cmp(&right.get("name").and_then(serde_json::Value::as_str))
    });
    Ok(templates)
}

fn get_published(root: &Path, name: &str, limits: ImportLimits) -> Result<serde_json::Value> {
    let root = open_directory_no_follow(root).map_err(|error| {
        BlazeDaemonError::RecoveryRequired(format!(
            "cannot open runtime template catalog {}: {error}",
            root.display()
        ))
    })?;
    let directory = match openat_directory(&root, OsStr::new(name)) {
        Ok(directory) => directory,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(BlazeDaemonError::NotFound(format!(
                "runtime template {name}"
            )));
        }
        Err(error) => return Err(error.into()),
    };
    read_published_directory(&directory, name, limits)
}

fn read_published(root: &Path, name: &str, limits: ImportLimits) -> Result<serde_json::Value> {
    let root = open_directory_no_follow(root)?;
    let directory = openat_directory(&root, OsStr::new(name)).map_err(|error| {
        BlazeDaemonError::RecoveryRequired(format!("cannot open runtime template {name}: {error}"))
    })?;
    read_published_directory(&directory, name, limits)
}

fn read_published_directory(
    directory: &File,
    expected_name: &str,
    limits: ImportLimits,
) -> Result<serde_json::Value> {
    let names = source_entry_names(directory).map_err(|error| {
        BlazeDaemonError::RecoveryRequired(format!(
            "cannot inspect runtime template {expected_name}: {error}"
        ))
    })?;
    if names.len() > limits.max_files {
        return Err(BlazeDaemonError::RecoveryRequired(format!(
            "runtime template {expected_name} exceeds the configured file limit"
        )));
    }
    let mut total_bytes = 0_u64;
    for name in names {
        validate_artifact_name(&name).map_err(|error| {
            BlazeDaemonError::RecoveryRequired(format!(
                "runtime template {expected_name} contains an invalid artifact: {error}"
            ))
        })?;
        let file = openat_regular(directory, &name).map_err(|error| {
            BlazeDaemonError::RecoveryRequired(format!(
                "runtime template {expected_name} contains an invalid artifact {}: {error}",
                name.to_string_lossy()
            ))
        })?;
        let metadata = file.metadata()?;
        validate_published_file(&metadata, expected_name, &name)?;
        if name == OsStr::new("template.json") && metadata.len() > limits.max_metadata_bytes {
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "runtime template {expected_name} metadata exceeds the configured limit"
            )));
        }
        total_bytes = total_bytes.checked_add(metadata.len()).ok_or_else(|| {
            BlazeDaemonError::RecoveryRequired(format!(
                "runtime template {expected_name} byte accounting overflow"
            ))
        })?;
    }
    if total_bytes > limits.max_bytes {
        return Err(BlazeDaemonError::RecoveryRequired(format!(
            "runtime template {expected_name} exceeds the configured byte limit"
        )));
    }

    let mut metadata = openat_regular(directory, OsStr::new("template.json")).map_err(|error| {
        BlazeDaemonError::RecoveryRequired(format!(
            "cannot open runtime template {expected_name} metadata: {error}"
        ))
    })?;
    let value = read_json_bounded(&mut metadata, limits.max_metadata_bytes).map_err(|error| {
        BlazeDaemonError::RecoveryRequired(format!(
            "cannot read runtime template {expected_name} metadata: {error}"
        ))
    })?;
    if !value.is_object()
        || value.get("name").and_then(serde_json::Value::as_str) != Some(expected_name)
    {
        return Err(BlazeDaemonError::RecoveryRequired(format!(
            "runtime template {expected_name} metadata does not match its catalog name"
        )));
    }
    for required in ["vmstate.snap", "mem.bin", "rootfs.ext4"] {
        openat_regular(directory, OsStr::new(required)).map_err(|error| {
            BlazeDaemonError::RecoveryRequired(format!(
                "runtime template {expected_name} is missing regular artifact {required}: {error}"
            ))
        })?;
    }
    Ok(value)
}

fn validate_published_file(
    metadata: &std::fs::Metadata,
    template: &str,
    name: &OsStr,
) -> Result<()> {
    let expected_uid = unsafe { libc::geteuid() };
    if !metadata.file_type().is_file()
        || metadata.uid() != expected_uid
        || metadata.mode() & 0o777 != CATALOG_FILE_MODE
    {
        return Err(BlazeDaemonError::RecoveryRequired(format!(
            "runtime template {template} artifact {} has unexpected type, ownership, or mode",
            name.to_string_lossy()
        )));
    }
    Ok(())
}

fn copy_regular_file(
    source: &mut File,
    destination: &Path,
    max_bytes: u64,
    cancellation: &CancellationToken,
) -> Result<u64> {
    let mut destination = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(CATALOG_FILE_MODE)
        .custom_flags(libc::O_NOFOLLOW)
        .open(destination)?;
    let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];
    let mut copied = 0_u64;
    loop {
        check_cancelled(cancellation)?;
        let read = source.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        let next = copied
            .checked_add(u64::try_from(read).unwrap_or(u64::MAX))
            .ok_or_else(|| payload_too_large(u64::MAX, max_bytes))?;
        if next > max_bytes {
            return Err(payload_too_large(next, max_bytes));
        }
        destination.write_all(&buffer[..read])?;
        copied = next;
    }
    destination.sync_all()?;
    destination.set_permissions(std::fs::Permissions::from_mode(CATALOG_FILE_MODE))?;
    Ok(copied)
}

fn write_file_durable(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(CATALOG_FILE_MODE)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    file.set_permissions(std::fs::Permissions::from_mode(CATALOG_FILE_MODE))?;
    Ok(())
}

#[cfg(target_os = "linux")]
fn rename_no_replace(source: &Path, destination: &Path) -> io::Result<()> {
    rename_no_replace_linux(source, destination)
}

#[cfg(not(target_os = "linux"))]
fn rename_no_replace(source: &Path, destination: &Path) -> io::Result<()> {
    if destination.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "destination already exists",
        ));
    }
    std::fs::rename(source, destination)
}

#[cfg(target_os = "linux")]
fn rename_no_replace_linux(source: &Path, destination: &Path) -> io::Result<()> {
    let source = CString::new(source.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "source path contains NUL"))?;
    let destination = CString::new(destination.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidInput, "destination path contains NUL")
    })?;
    let result = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            source.as_ptr(),
            libc::AT_FDCWD,
            destination.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn read_json_bounded(file: &mut File, limit: u64) -> Result<serde_json::Value> {
    let mut bytes = Vec::new();
    file.take(limit.saturating_add(1)).read_to_end(&mut bytes)?;
    let actual = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if actual > limit {
        return Err(payload_too_large(actual, limit));
    }
    Ok(serde_json::from_slice(&bytes)?)
}

fn sync_directory(path: &Path) -> Result<()> {
    open_directory_no_follow(path)?.sync_all()?;
    Ok(())
}

fn cleanup_staging(root: &Path) -> Result<usize> {
    let mut removed = 0;
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        if !is_staging_name(&name) {
            continue;
        }
        let metadata = std::fs::symlink_metadata(entry.path())?;
        let expected_uid = unsafe { libc::geteuid() };
        if !metadata.is_dir() || metadata.uid() != expected_uid {
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "runtime template staging entry {} has unexpected ownership or type",
                entry.path().display()
            )));
        }
        std::fs::remove_dir_all(entry.path())?;
        removed += 1;
    }
    if removed > 0 {
        sync_directory(root)?;
        tracing::info!(
            removed,
            "removed stale runtime template staging directories"
        );
    }
    Ok(removed)
}

fn is_staging_name(name: &OsStr) -> bool {
    let name = name.to_string_lossy();
    name.starts_with(".import-") && name.ends_with(".tmp")
}

fn validate_name(value: &str, label: &str) -> Result<()> {
    let mut chars = value.chars();
    let first = chars.next();
    if value.len() > 128
        || !first.is_some_and(|ch| ch.is_ascii_alphanumeric())
        || !chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'))
    {
        return Err(BlazeDaemonError::BadRequest(format!(
            "{label} must start with an ASCII letter or digit and contain at most 128 \
             letters, digits, dots, dashes, or underscores"
        )));
    }
    Ok(())
}

fn validate_artifact_name(value: &OsStr) -> Result<()> {
    let value = value.to_str().ok_or_else(|| {
        BlazeDaemonError::BadRequest(
            "runtime template artifact names must be valid UTF-8".to_string(),
        )
    })?;
    validate_name(value, "runtime template artifact")
}

fn validate_relative_source(source: &Path) -> Result<()> {
    if source.as_os_str().is_empty()
        || source
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(BlazeDaemonError::BadRequest(
            "runtime template source must be a non-empty relative path below the configured \
             import root"
                .to_string(),
        ));
    }
    Ok(())
}

fn check_cancelled(cancellation: &CancellationToken) -> Result<()> {
    if cancellation.is_cancelled() {
        return Err(BlazeDaemonError::ServiceUnavailable(
            "runtime template import cancelled during daemon shutdown".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
fn wait_for_copy_gate(inner: &CatalogInner, cancellation: &CancellationToken) {
    let gate = inner.copy_gate.lock().expect("copy gate lock").clone();
    if let Some(gate) = gate {
        let _ = gate.entered.send(());
        while !gate.release.load(Ordering::Acquire) && !cancellation.is_cancelled() {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }
}

fn payload_too_large(actual: u64, limit: u64) -> BlazeDaemonError {
    BlazeDaemonError::PayloadTooLarge {
        actual,
        limit: usize::try_from(limit).unwrap_or(usize::MAX),
    }
}

fn join_error(context: &'static str) -> impl FnOnce(tokio::task::JoinError) -> BlazeDaemonError {
    move |error| BlazeDaemonError::Internal(format!("{context} task: {error}"))
}

fn lock_catalog_state(inner: &CatalogInner) -> std::sync::MutexGuard<'_, CatalogState> {
    match inner.state.lock() {
        Ok(state) => state,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(root: &Path, import_root: &Path) -> RuntimeTemplateSection {
        RuntimeTemplateSection {
            dir: root.to_path_buf(),
            import_root: Some(import_root.to_path_buf()),
            max_files: 8,
            max_bytes: 1024,
            max_metadata_bytes: 512,
            max_total_bytes: 2048,
        }
    }

    #[tokio::test]
    async fn import_publishes_artifacts_with_private_permissions() {
        let temp = tempfile::tempdir().expect("tempdir");
        let import_root = temp.path().join("imports");
        let source = import_root.join("source");
        let root = temp.path().join("catalog");
        write_artifacts(&source);
        let catalog =
            RuntimeTemplateCatalog::open(&test_config(&root, &import_root)).expect("catalog");

        let metadata = catalog
            .import(
                "runtime-base".to_string(),
                PathBuf::from("source"),
                "base runtime template".to_string(),
            )
            .await
            .expect("import");
        let destination = root.join("runtime-base");

        assert_eq!(metadata["name"], "runtime-base");
        assert_eq!(metadata["description"], "base runtime template");
        assert_eq!(
            std::fs::symlink_metadata(&destination)
                .expect("directory")
                .mode()
                & 0o777,
            CATALOG_DIR_MODE
        );
        for file in ["vmstate.snap", "mem.bin", "rootfs.ext4", "template.json"] {
            assert_eq!(
                std::fs::symlink_metadata(destination.join(file))
                    .expect("artifact")
                    .mode()
                    & 0o777,
                CATALOG_FILE_MODE
            );
        }
    }

    #[tokio::test]
    async fn import_rejects_special_entries_and_cleans_staging() {
        let temp = tempfile::tempdir().expect("tempdir");
        let import_root = temp.path().join("imports");
        let source = import_root.join("source");
        let root = temp.path().join("catalog");
        write_artifacts(&source);
        let fifo = CString::new(source.join("fifo").as_os_str().as_bytes()).expect("fifo path");
        assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
        let catalog =
            RuntimeTemplateCatalog::open(&test_config(&root, &import_root)).expect("catalog");

        catalog
            .import("special".into(), PathBuf::from("source"), String::new())
            .await
            .expect_err("special file");

        assert!(!root.join("special").exists());
        assert_eq!(std::fs::read_dir(root).expect("catalog").count(), 0);
    }

    #[tokio::test]
    async fn import_does_not_follow_source_links() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("tempdir");
        let import_root = temp.path().join("imports");
        let source = import_root.join("source");
        let root = temp.path().join("catalog");
        write_artifacts(&source);
        symlink("mem.bin", source.join("linked-memory")).expect("source link");
        symlink("source", import_root.join("source-link")).expect("directory link");
        let catalog =
            RuntimeTemplateCatalog::open(&test_config(&root, &import_root)).expect("catalog");

        catalog
            .import("file-link".into(), PathBuf::from("source"), String::new())
            .await
            .expect_err("file link");
        catalog
            .import(
                "directory-link".into(),
                PathBuf::from("source-link"),
                String::new(),
            )
            .await
            .expect_err("directory link");

        assert_eq!(std::fs::read_dir(root).expect("catalog").count(), 0);
    }

    #[tokio::test]
    async fn metadata_and_catalog_capacity_are_enforced() {
        let temp = tempfile::tempdir().expect("tempdir");
        let import_root = temp.path().join("imports");
        let source = import_root.join("source");
        let root = temp.path().join("catalog");
        write_artifacts(&source);
        let mut config = test_config(&root, &import_root);
        config.max_metadata_bytes = 64;
        let catalog = RuntimeTemplateCatalog::open(&config).expect("catalog");

        let error = catalog
            .import("metadata".into(), PathBuf::from("source"), "x".repeat(128))
            .await
            .expect_err("metadata limit");
        assert!(matches!(error, BlazeDaemonError::PayloadTooLarge { .. }));

        let mut config = test_config(&root, &import_root);
        config.max_total_bytes = 8;
        let catalog = RuntimeTemplateCatalog::open(&config).expect("catalog");
        let error = catalog
            .import("capacity".into(), PathBuf::from("source"), String::new())
            .await
            .expect_err("catalog capacity");
        assert!(matches!(error, BlazeDaemonError::PayloadTooLarge { .. }));
    }

    #[test]
    fn concurrent_reservations_share_one_catalog_limit() {
        let temp = tempfile::tempdir().expect("tempdir");
        let import_root = temp.path().join("imports");
        std::fs::create_dir(&import_root).expect("import root");
        let root = temp.path().join("catalog");
        let mut config = test_config(&root, &import_root);
        config.max_total_bytes = 100;
        let catalog = RuntimeTemplateCatalog::open(&config).expect("catalog");
        let mut first =
            ImportClaim::begin(Arc::clone(&catalog.inner), "first".into()).expect("first claim");
        let mut second =
            ImportClaim::begin(Arc::clone(&catalog.inner), "second".into()).expect("second claim");

        first.reserve(60).expect("first reservation");
        assert!(matches!(
            second.reserve(60),
            Err(BlazeDaemonError::PayloadTooLarge { .. })
        ));
    }

    #[test]
    fn accounting_failure_blocks_later_imports() {
        let temp = tempfile::tempdir().expect("tempdir");
        let import_root = temp.path().join("imports");
        std::fs::create_dir(&import_root).expect("import root");
        let root = temp.path().join("catalog");
        let catalog =
            RuntimeTemplateCatalog::open(&test_config(&root, &import_root)).expect("catalog");
        let mut claim =
            ImportClaim::begin(Arc::clone(&catalog.inner), "first".into()).expect("claim");
        claim.reserve(10).expect("reservation");

        let error = claim.publish(11).expect_err("reservation mismatch");
        assert!(matches!(error, BlazeDaemonError::RecoveryRequired(_)));
        assert!(matches!(
            ImportClaim::begin(Arc::clone(&catalog.inner), "later".into()),
            Err(BlazeDaemonError::RecoveryRequired(_))
        ));
    }

    #[test]
    fn copy_counts_bytes_read_after_preflight() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source_path = temp.path().join("source");
        std::fs::write(&source_path, b"one").expect("source");
        let mut source = OpenOptions::new()
            .read(true)
            .open(&source_path)
            .expect("open source");
        std::fs::write(&source_path, b"longer").expect("grow source");
        let destination = temp.path().join("destination");

        let error = copy_regular_file(&mut source, &destination, 3, &CancellationToken::new())
            .expect_err("actual bytes exceed reservation");

        assert!(matches!(error, BlazeDaemonError::PayloadTooLarge { .. }));
    }

    #[tokio::test]
    async fn shutdown_waits_for_registered_import_claims() {
        let temp = tempfile::tempdir().expect("tempdir");
        let import_root = temp.path().join("imports");
        std::fs::create_dir(&import_root).expect("import root");
        let root = temp.path().join("catalog");
        let catalog =
            RuntimeTemplateCatalog::open(&test_config(&root, &import_root)).expect("catalog");
        let claim = ImportClaim::begin(Arc::clone(&catalog.inner), "active".into()).expect("claim");
        assert_eq!(catalog.active_imports(), 1);

        catalog.cancel_imports();
        let waiting = tokio::spawn({
            let catalog = catalog.clone();
            async move { catalog.wait_for_imports().await }
        });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());

        drop(claim);
        waiting.await.expect("join").expect("imports stopped");
        assert_eq!(catalog.active_imports(), 0);
        assert!(matches!(
            ImportClaim::begin(Arc::clone(&catalog.inner), "late".into()),
            Err(BlazeDaemonError::ServiceUnavailable(_))
        ));
    }

    #[tokio::test]
    async fn shutdown_cancels_copy_and_removes_staging() {
        let temp = tempfile::tempdir().expect("tempdir");
        let import_root = temp.path().join("imports");
        let source = import_root.join("source");
        let root = temp.path().join("catalog");
        write_artifacts(&source);
        let catalog =
            RuntimeTemplateCatalog::open(&test_config(&root, &import_root)).expect("catalog");
        let mut entered = catalog.install_copy_gate();
        let import = tokio::spawn({
            let catalog = catalog.clone();
            async move {
                catalog
                    .import("cancelled".into(), PathBuf::from("source"), String::new())
                    .await
            }
        });
        entered.recv().await.expect("copy entered");

        catalog.cancel_imports();
        catalog.wait_for_imports().await.expect("imports quiescent");
        let error = import
            .await
            .expect("import task")
            .expect_err("cancelled import");

        assert!(matches!(error, BlazeDaemonError::ServiceUnavailable(_)));
        assert!(!root.join("cancelled").exists());
        assert_eq!(std::fs::read_dir(root).expect("catalog").count(), 0);
    }

    #[test]
    fn list_reports_corrupt_published_metadata() {
        let temp = tempfile::tempdir().expect("tempdir");
        let import_root = temp.path().join("imports");
        std::fs::create_dir(&import_root).expect("import root");
        let root = temp.path().join("catalog");
        let catalog =
            RuntimeTemplateCatalog::open(&test_config(&root, &import_root)).expect("catalog");
        let published = root.join("published");
        create_private_directory(&published).expect("published");
        write_file_durable(&published.join("template.json"), b"{broken").expect("metadata");

        let error = list_published(&catalog.inner.root, catalog.inner.limits)
            .expect_err("corrupt metadata");
        assert!(matches!(error, BlazeDaemonError::RecoveryRequired(_)));
    }

    #[test]
    fn startup_removes_owned_staging_directories() {
        let temp = tempfile::tempdir().expect("tempdir");
        let import_root = temp.path().join("imports");
        std::fs::create_dir(&import_root).expect("import root");
        let root = temp.path().join("catalog");
        create_catalog_root(&root).expect("root");
        let staging = root.join(".import-pending-uuid.tmp");
        create_private_directory(&staging).expect("staging");

        RuntimeTemplateCatalog::open(&test_config(&root, &import_root)).expect("catalog");

        assert!(!staging.exists());
    }

    fn write_artifacts(source: &Path) {
        std::fs::create_dir_all(source).expect("source directory");
        std::fs::write(source.join("vmstate.snap"), b"snapshot").expect("snapshot");
        std::fs::write(source.join("mem.bin"), b"memory").expect("memory");
        std::fs::write(source.join("rootfs.ext4"), b"rootfs").expect("rootfs");
    }
}
