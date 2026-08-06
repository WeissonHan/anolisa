// SPDX-License-Identifier: Apache-2.0
//! File-based storage provider: creates per-instance directories with
//! rootfs and memory files on a local filesystem. Base images and mutable
//! instance slots use separate roots; runtime pooling is owned by the daemon.

use std::ffi::OsString;
use std::io::SeekFrom;
use std::path::{Component, Path, PathBuf};

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use uuid::Uuid;

use blaze_core::error::{BlazeError, Result};
use blaze_core::storage::{
    AcquireOpts, PoolStatus, RuntimeTemplateArtifact, RuntimeTemplateStorage,
    RuntimeTemplateStorageSlot, StorageAcquireError, StorageProvider, StorageRestoreTransaction,
    StorageSlot,
};

mod restore;

/// A filesystem-based provider that copies base artifacts when available and
/// otherwise creates sparse rootfs and memory files at configured sizes.
pub struct FileStorageProvider {
    images_dir: PathBuf,
    instances_dir: PathBuf,
}

impl FileStorageProvider {
    /// Create a provider with no separate image directory.
    ///
    /// This constructor is kept for focused tests. Daemon startup uses
    /// [`Self::with_images`] so immutable images and runtime slots cannot mix.
    #[cfg(test)]
    pub fn new(instances_dir: PathBuf) -> Self {
        Self {
            images_dir: instances_dir.clone(),
            instances_dir,
        }
    }

    /// Create a provider with distinct immutable image and runtime roots.
    pub fn with_images(images_dir: PathBuf, instances_dir: PathBuf) -> Self {
        Self {
            images_dir,
            instances_dir,
        }
    }

    fn slot_for_id(&self, instance_id: &str) -> Result<StorageSlot> {
        validate_instance_id(instance_id)?;
        let instance_dir = self.instances_dir.join(instance_id);
        if !instance_dir.starts_with(&self.instances_dir) || instance_dir == self.instances_dir {
            return Err(BlazeError::StorageError {
                msg: format!("slot '{instance_id}': path escapes instances_dir"),
            });
        }
        Ok(StorageSlot {
            id: instance_id.to_string(),
            rootfs_path: instance_dir.join("rootfs.ext4"),
            mem_path: instance_dir.join("mem.bin"),
            mem_diff_path: instance_dir.join("mem.diff"),
            rootfs_diff_path: instance_dir.join("rootfs.diff"),
            instance_dir,
        })
    }
}

#[derive(Clone, Copy)]
enum RequiredPathType {
    Directory,
    File,
}

impl RequiredPathType {
    fn description(self) -> &'static str {
        match self {
            Self::Directory => "directory",
            Self::File => "file",
        }
    }

    fn matches(self, metadata: &std::fs::Metadata) -> bool {
        match self {
            Self::Directory => metadata.is_dir(),
            Self::File => metadata.is_file(),
        }
    }
}

/// Removes incomplete capture files if an error or cancellation interrupts
/// publication before the target directory is durably synchronized.
struct UnpublishedCheckpoint {
    temporary: Option<PathBuf>,
    target: Option<PathBuf>,
}

impl UnpublishedCheckpoint {
    fn new() -> Self {
        Self {
            temporary: None,
            target: None,
        }
    }

    fn mark_temporary(&mut self, temporary: PathBuf) {
        self.temporary = Some(temporary);
    }

    fn mark_target(&mut self, target: PathBuf) {
        self.target = Some(target);
    }

    fn clear_temporary(&mut self) {
        self.temporary = None;
    }

    fn commit(&mut self) {
        self.temporary = None;
        self.target = None;
    }
}

impl Drop for UnpublishedCheckpoint {
    fn drop(&mut self) {
        if let Some(target) = self.target.take() {
            let _ = std::fs::remove_file(target);
        }
        if let Some(temporary) = self.temporary.take() {
            let _ = std::fs::remove_file(temporary);
        }
    }
}

async fn require_slot_path(
    instance_id: &str,
    path: &Path,
    required_type: RequiredPathType,
) -> Result<()> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) if required_type.matches(&metadata) => Ok(()),
        Ok(_) => Err(BlazeError::StorageIncomplete {
            instance_id: instance_id.to_string(),
            path: path.to_path_buf(),
            expected: required_type.description(),
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Err(BlazeError::StorageIncomplete {
                instance_id: instance_id.to_string(),
                path: path.to_path_buf(),
                expected: required_type.description(),
            })
        }
        Err(error) => Err(BlazeError::StorageError {
            msg: format!(
                "reconstruct '{instance_id}': inspect {}: {error}",
                path.display()
            ),
        }),
    }
}

#[async_trait]
impl StorageProvider for FileStorageProvider {
    async fn probe(&self) -> Result<bool> {
        Ok(self.images_dir.exists() && self.instances_dir.exists())
    }

    async fn acquire(
        &self,
        opts: &AcquireOpts,
    ) -> std::result::Result<StorageSlot, StorageAcquireError> {
        crate::failpoint::storage("storage-acquire")?;
        let slot = self.slot_for_id(&opts.instance_id)?;
        let instance_dir = slot.instance_dir.clone();

        // Atomic: create_dir fails with AlreadyExists if concurrent acquire races
        match tokio::fs::create_dir(&instance_dir).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                return Err(StorageAcquireError::clean(BlazeError::StorageError {
                    msg: format!(
                        "acquire '{}': instance directory already exists",
                        opts.instance_id
                    ),
                }));
            }
            Err(e) => {
                return Err(StorageAcquireError::clean(BlazeError::StorageError {
                    msg: format!("acquire '{}': create dir: {}", opts.instance_id, e),
                }));
            }
        }

        // Create rootfs + mem; rollback dir on failure
        let result = async {
            create_or_copy(
                &self.images_dir.join("rootfs.ext4"),
                &slot.rootfs_path,
                opts.rootfs_size,
            )
            .await?;
            create_or_copy(
                &self.images_dir.join("mem.bin"),
                &slot.mem_path,
                opts.mem_size,
            )
            .await?;
            tokio::fs::File::create(&slot.mem_diff_path).await?;
            tokio::fs::File::create(&slot.rootfs_diff_path).await?;
            crate::failpoint::storage("storage-acquire-artifacts")?;
            Ok::<(), BlazeError>(())
        }
        .await;

        if let Err(e) = result {
            let rollback = match crate::failpoint::storage("storage-acquire-rollback") {
                Ok(()) => tokio::fs::remove_dir_all(&instance_dir)
                    .await
                    .map_err(BlazeError::from),
                Err(error) => Err(error),
            };
            let source = match rollback {
                Ok(()) => BlazeError::StorageError {
                    msg: format!(
                        "acquire '{}': file setup failed, rolled back: {}",
                        opts.instance_id, e
                    ),
                },
                Err(cleanup) => {
                    return Err(StorageAcquireError::with_residual(
                        BlazeError::StorageError {
                            msg: format!(
                                "acquire '{}': file setup failed ({e}); rollback failed for {}: {cleanup}",
                                opts.instance_id,
                                instance_dir.display()
                            ),
                        },
                        slot,
                    ));
                }
            };
            return Err(StorageAcquireError::clean(source));
        }

        Ok(slot)
    }

    async fn acquire_runtime_template(
        &self,
        opts: &AcquireOpts,
        source: RuntimeTemplateStorage,
    ) -> std::result::Result<RuntimeTemplateStorageSlot, StorageAcquireError> {
        crate::failpoint::storage("storage-acquire-runtime-template")?;
        if opts.rootfs_size != source.rootfs.size_bytes || opts.mem_size != source.memory.size_bytes
        {
            return Err(StorageAcquireError::clean(BlazeError::StorageError {
                msg: format!(
                    "acquire runtime template '{}': requested sizes do not match the template",
                    opts.instance_id
                ),
            }));
        }

        let slot = self.slot_for_id(&opts.instance_id)?;
        let instance_dir = slot.instance_dir.clone();
        match tokio::fs::create_dir(&instance_dir).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                return Err(StorageAcquireError::clean(BlazeError::StorageError {
                    msg: format!(
                        "acquire runtime template '{}': instance directory already exists",
                        opts.instance_id
                    ),
                }));
            }
            Err(error) => {
                return Err(StorageAcquireError::clean(BlazeError::StorageError {
                    msg: format!(
                        "acquire runtime template '{}': create dir: {error}",
                        opts.instance_id
                    ),
                }));
            }
        }

        let snapshot_path = instance_dir.join("vmstate.snap");
        let result = async {
            copy_runtime_template_artifact(source.rootfs, &slot.rootfs_path).await?;
            copy_runtime_template_artifact(source.memory, &slot.mem_path).await?;
            copy_runtime_template_artifact(source.vmstate, &snapshot_path).await?;
            create_empty_durable_file(&slot.mem_diff_path).await?;
            create_empty_durable_file(&slot.rootfs_diff_path).await?;
            crate::failpoint::storage("storage-acquire-runtime-template-artifacts")?;
            tokio::fs::File::open(&instance_dir)
                .await?
                .sync_all()
                .await?;
            Ok::<(), BlazeError>(())
        }
        .await;

        if let Err(error) = result {
            let rollback = match crate::failpoint::storage("storage-acquire-rollback") {
                Ok(()) => tokio::fs::remove_dir_all(&instance_dir)
                    .await
                    .map_err(BlazeError::from),
                Err(cleanup) => Err(cleanup),
            };
            return match rollback {
                Ok(()) => Err(StorageAcquireError::clean(BlazeError::StorageError {
                    msg: format!(
                        "acquire runtime template '{}': artifact setup failed, rolled back: {error}",
                        opts.instance_id
                    ),
                })),
                Err(cleanup) => Err(StorageAcquireError::with_residual(
                    BlazeError::StorageError {
                        msg: format!(
                            "acquire runtime template '{}': artifact setup failed ({error}); rollback failed for {}: {cleanup}",
                            opts.instance_id,
                            instance_dir.display()
                        ),
                    },
                    slot,
                )),
            };
        }

        Ok(RuntimeTemplateStorageSlot {
            storage: slot,
            snapshot_path,
        })
    }

    fn supports_runtime_templates(&self) -> bool {
        true
    }

    async fn release(&self, slot: StorageSlot) -> Result<()> {
        crate::failpoint::storage("storage-release")?;
        // Re-derive the canonical path from instances_dir + slot.id. Do not
        // trust path strings carried in a persisted or externally built slot.
        let canonical_dir = self.slot_for_id(&slot.id)?.instance_dir;
        match tokio::fs::symlink_metadata(&canonical_dir).await {
            Ok(metadata) if metadata.file_type().is_dir() => {
                tokio::fs::remove_dir_all(&canonical_dir)
                    .await
                    .map_err(|error| BlazeError::StorageError {
                        msg: format!("release '{}': {error}", slot.id),
                    })?;
            }
            Ok(_) => {
                return Err(BlazeError::StorageError {
                    msg: format!(
                        "release '{}': refusing non-directory slot {}",
                        slot.id,
                        canonical_dir.display()
                    ),
                });
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(BlazeError::StorageError {
                    msg: format!("release '{}': inspect: {error}", slot.id),
                });
            }
        }
        Ok(())
    }

    async fn release_by_id(&self, instance_id: &str) -> Result<()> {
        let slot = self.slot_for_id(instance_id)?;
        self.release(slot).await
    }

    fn supports_runtime_pool_recovery(&self) -> bool {
        true
    }

    async fn list_owned_ids(&self) -> Result<Vec<String>> {
        let mut entries = tokio::fs::read_dir(&self.instances_dir)
            .await
            .map_err(|error| BlazeError::StorageError {
                msg: format!("inventory {}: {error}", self.instances_dir.display()),
            })?;
        let mut ids = Vec::new();
        while let Some(entry) =
            entries
                .next_entry()
                .await
                .map_err(|error| BlazeError::StorageError {
                    msg: format!("inventory {}: {error}", self.instances_dir.display()),
                })?
        {
            let path = entry.path();
            let file_type = entry
                .file_type()
                .await
                .map_err(|error| BlazeError::StorageError {
                    msg: format!("inventory {}: inspect: {error}", path.display()),
                })?;
            if !file_type.is_dir() {
                return Err(BlazeError::StorageError {
                    msg: format!(
                        "inventory {}: unexpected non-directory entry",
                        path.display()
                    ),
                });
            }
            let id = entry
                .file_name()
                .into_string()
                .map_err(|_| BlazeError::StorageError {
                    msg: format!("inventory {}: slot name is not UTF-8", path.display()),
                })?;
            validate_instance_id(&id)?;
            ids.push(id);
        }
        ids.sort();
        Ok(ids)
    }

    async fn reconstruct(&self, instance_id: &str) -> Result<StorageSlot> {
        let slot = self.slot_for_id(instance_id)?;
        require_slot_path(instance_id, &slot.instance_dir, RequiredPathType::Directory).await?;
        for path in [
            &slot.rootfs_path,
            &slot.mem_path,
            &slot.mem_diff_path,
            &slot.rootfs_diff_path,
        ] {
            require_slot_path(instance_id, path, RequiredPathType::File).await?;
        }
        Ok(slot)
    }

    async fn flush_dirty(&self, slot: &StorageSlot) -> Result<()> {
        crate::failpoint::storage("flush-storage")?;
        // Never trust paths carried by a runtime or persisted slot. Rebuild
        // the complete provider-owned artifact set from the validated ID.
        let canonical = self.slot_for_id(&slot.id)?;
        require_slot_path(
            &slot.id,
            &canonical.instance_dir,
            RequiredPathType::Directory,
        )
        .await?;
        for path in [
            &canonical.rootfs_path,
            &canonical.mem_path,
            &canonical.mem_diff_path,
            &canonical.rootfs_diff_path,
        ] {
            require_slot_path(&slot.id, path, RequiredPathType::File).await?;
            let file = tokio::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .await
                .map_err(|error| BlazeError::StorageError {
                    msg: format!("flush '{}': open {}: {error}", slot.id, path.display()),
                })?;
            file.sync_all()
                .await
                .map_err(|error| BlazeError::StorageError {
                    msg: format!("flush '{}': sync {}: {error}", slot.id, path.display()),
                })?;
        }
        let directory = tokio::fs::File::open(&canonical.instance_dir)
            .await
            .map_err(|error| BlazeError::StorageError {
                msg: format!(
                    "flush '{}': open directory {}: {error}",
                    slot.id,
                    canonical.instance_dir.display()
                ),
            })?;
        directory
            .sync_all()
            .await
            .map_err(|error| BlazeError::StorageError {
                msg: format!(
                    "flush '{}': sync directory {}: {error}",
                    slot.id,
                    canonical.instance_dir.display()
                ),
            })?;
        Ok(())
    }

    fn supports_checkpoint_capture(&self) -> bool {
        true
    }

    async fn capture_checkpoint(&self, slot: &StorageSlot, target: &Path) -> Result<()> {
        let source = self.checkpoint_source(slot).await?;
        let (target_parent, target) = checkpoint_target(target).await?;
        ensure_checkpoint_target_absent(&target).await?;

        let temporary = checkpoint_temporary_path(&target_parent, &target);
        let mut cleanup = UnpublishedCheckpoint::new();
        let result =
            capture_rootfs(&source, &temporary, &target_parent, &target, &mut cleanup).await;
        result.map_err(|error| BlazeError::StorageError {
            msg: format!(
                "capture checkpoint for '{}': copy {} to {}: {error}",
                slot.id,
                source.display(),
                target.display()
            ),
        })
    }

    fn supports_checkpoint_restore(&self) -> bool {
        true
    }

    async fn stage_checkpoint_restore(
        &self,
        slot: &StorageSlot,
        source: &Path,
    ) -> Result<StorageRestoreTransaction> {
        restore::stage(self, slot, source).await
    }

    async fn activate_checkpoint_restore(
        &self,
        transaction: &StorageRestoreTransaction,
    ) -> Result<()> {
        restore::activate(self, transaction).await
    }

    async fn commit_checkpoint_restore(
        &self,
        transaction: &StorageRestoreTransaction,
    ) -> Result<()> {
        restore::commit(self, transaction).await
    }

    async fn abort_checkpoint_restore(
        &self,
        transaction: &StorageRestoreTransaction,
    ) -> Result<()> {
        restore::abort(self, transaction).await
    }

    async fn reconcile_checkpoint_restore(&self, instance_id: &str) -> Result<()> {
        restore::reconcile(self, instance_id).await
    }

    fn pool_status(&self) -> PoolStatus {
        PoolStatus::default()
    }

    async fn drain_pool(&self) -> Result<usize> {
        Ok(0)
    }
}

impl FileStorageProvider {
    async fn checkpoint_source(&self, slot: &StorageSlot) -> Result<PathBuf> {
        let canonical = self.slot_for_id(&slot.id)?;
        let instances_dir =
            canonical_plain_path(&self.instances_dir, RequiredPathType::Directory).await?;
        let instance_dir =
            canonical_plain_path(&canonical.instance_dir, RequiredPathType::Directory).await?;
        if instance_dir.parent() != Some(instances_dir.as_path()) {
            return Err(BlazeError::StorageError {
                msg: format!(
                    "capture checkpoint for '{}': slot {} is outside instances directory {}",
                    slot.id,
                    instance_dir.display(),
                    instances_dir.display()
                ),
            });
        }

        let source = canonical_plain_path(&canonical.rootfs_path, RequiredPathType::File).await?;
        if source.parent() != Some(instance_dir.as_path())
            || source.file_name() != canonical.rootfs_path.file_name()
        {
            return Err(BlazeError::StorageError {
                msg: format!(
                    "capture checkpoint for '{}': rootfs {} is outside slot {}",
                    slot.id,
                    source.display(),
                    instance_dir.display()
                ),
            });
        }
        Ok(source)
    }
}

async fn create_or_copy(
    source: &std::path::Path,
    target: &std::path::Path,
    size: u64,
) -> std::io::Result<()> {
    if source.is_file() && source != target {
        tokio::fs::copy(source, target).await?;
        return Ok(());
    }
    let file = tokio::fs::File::create(target).await?;
    if size > 0 {
        file.set_len(size).await?;
    }
    Ok(())
}

async fn copy_runtime_template_artifact(
    source: RuntimeTemplateArtifact,
    target: &Path,
) -> Result<()> {
    let metadata = source
        .file
        .metadata()
        .map_err(|error| BlazeError::StorageError {
            msg: format!("inspect runtime template artifact: {error}"),
        })?;
    if !metadata.is_file() || metadata.len() != source.size_bytes {
        return Err(BlazeError::StorageError {
            msg: format!(
                "runtime template artifact has size {}; expected {}",
                metadata.len(),
                source.size_bytes
            ),
        });
    }

    let mut source_file = tokio::fs::File::from_std(source.file);
    source_file.seek(SeekFrom::Start(0)).await?;
    let mut destination = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(target)
        .await?;
    let mut digest = Sha256::new();
    let mut copied = 0_u64;
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = source_file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        copied = copied
            .checked_add(u64::try_from(read).unwrap_or(u64::MAX))
            .ok_or_else(|| BlazeError::StorageError {
                msg: "runtime template artifact size overflow".to_string(),
            })?;
        if copied > source.size_bytes {
            return Err(BlazeError::StorageError {
                msg: format!(
                    "runtime template artifact exceeds declared size {}",
                    source.size_bytes
                ),
            });
        }
        digest.update(&buffer[..read]);
        destination.write_all(&buffer[..read]).await?;
    }
    if copied != source.size_bytes {
        return Err(BlazeError::StorageError {
            msg: format!(
                "runtime template artifact has {copied} bytes; expected {}",
                source.size_bytes
            ),
        });
    }
    let actual = format!("{:x}", digest.finalize());
    if actual != source.sha256 {
        return Err(BlazeError::StorageError {
            msg: format!(
                "runtime template artifact digest mismatch: expected {}, got {actual}",
                source.sha256
            ),
        });
    }
    destination.sync_all().await?;
    Ok(())
}

async fn create_empty_durable_file(path: &Path) -> Result<()> {
    tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await?
        .sync_all()
        .await?;
    Ok(())
}

async fn canonical_plain_path(path: &Path, required_type: RequiredPathType) -> Result<PathBuf> {
    let metadata =
        tokio::fs::symlink_metadata(path)
            .await
            .map_err(|error| BlazeError::StorageError {
                msg: format!("inspect checkpoint path {}: {error}", path.display()),
            })?;
    if !required_type.matches(&metadata) || metadata.file_type().is_symlink() {
        return Err(BlazeError::StorageError {
            msg: format!(
                "checkpoint path {} is not a plain {}",
                path.display(),
                required_type.description()
            ),
        });
    }
    tokio::fs::canonicalize(path)
        .await
        .map_err(|error| BlazeError::StorageError {
            msg: format!("canonicalize checkpoint path {}: {error}", path.display()),
        })
}

async fn checkpoint_target(target: &Path) -> Result<(PathBuf, PathBuf)> {
    if !matches!(target.components().next_back(), Some(Component::Normal(_))) {
        return Err(BlazeError::StorageError {
            msg: format!(
                "checkpoint target {} must end in a file name",
                target.display()
            ),
        });
    }
    let parent = target.parent().ok_or_else(|| BlazeError::StorageError {
        msg: format!(
            "checkpoint target {} has no parent directory",
            target.display()
        ),
    })?;
    let parent = canonical_plain_path(parent, RequiredPathType::Directory).await?;
    let file_name = target.file_name().ok_or_else(|| BlazeError::StorageError {
        msg: format!("checkpoint target {} has no file name", target.display()),
    })?;
    let target = parent.join(file_name);
    Ok((parent, target))
}

async fn ensure_checkpoint_target_absent(target: &Path) -> Result<()> {
    match tokio::fs::symlink_metadata(target).await {
        Ok(_) => Err(BlazeError::StorageError {
            msg: format!("checkpoint target {} already exists", target.display()),
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(BlazeError::StorageError {
            msg: format!("inspect checkpoint target {}: {error}", target.display()),
        }),
    }
}

fn checkpoint_temporary_path(parent: &Path, target: &Path) -> PathBuf {
    let mut name = OsString::from(".");
    name.push(target.file_name().expect("validated checkpoint target"));
    name.push(format!(".capture-{}.tmp", Uuid::new_v4()));
    parent.join(name)
}

async fn capture_rootfs(
    source: &Path,
    temporary: &Path,
    parent: &Path,
    target: &Path,
    cleanup: &mut UnpublishedCheckpoint,
) -> std::io::Result<()> {
    let mut source_options = tokio::fs::OpenOptions::new();
    source_options.read(true);
    #[cfg(unix)]
    source_options.custom_flags(libc::O_NOFOLLOW);
    let mut source_file = source_options.open(source).await?;
    if !source_file.metadata().await?.is_file() {
        return Err(std::io::Error::other(format!(
            "checkpoint source {} is not a regular file",
            source.display()
        )));
    }
    let mut temporary_file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(temporary)
        .await?;
    cleanup.mark_temporary(temporary.to_path_buf());
    tokio::io::copy(&mut source_file, &mut temporary_file).await?;
    temporary_file.sync_all().await?;
    drop(temporary_file);
    drop(source_file);

    tokio::fs::hard_link(temporary, target).await?;
    cleanup.mark_target(target.to_path_buf());
    crate::failpoint::storage("storage-capture-after-link")
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    tokio::fs::remove_file(temporary).await?;
    cleanup.clear_temporary();
    tokio::fs::File::open(parent).await?.sync_all().await?;
    cleanup.commit();
    Ok(())
}

fn validate_instance_id(instance_id: &str) -> Result<()> {
    if instance_id.is_empty()
        || instance_id.contains('/')
        || instance_id.contains('\\')
        || instance_id == ".."
        || instance_id == "."
        || std::path::Path::new(instance_id).is_absolute()
    {
        return Err(BlazeError::StorageError {
            msg: format!("invalid instance_id '{instance_id}': must be a single path component"),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn runtime_template_artifact(root: &Path, name: &str, bytes: &[u8]) -> RuntimeTemplateArtifact {
        let path = root.join(name);
        std::fs::write(&path, bytes).expect("template artifact");
        RuntimeTemplateArtifact {
            file: std::fs::File::open(path).expect("open template artifact"),
            size_bytes: u64::try_from(bytes.len()).expect("artifact length"),
            sha256: format!("{:x}", Sha256::digest(bytes)),
        }
    }

    fn runtime_template_storage(root: &Path) -> RuntimeTemplateStorage {
        RuntimeTemplateStorage {
            vmstate: runtime_template_artifact(root, "source-vmstate", b"snapshot"),
            memory: runtime_template_artifact(root, "source-memory", b"memory"),
            rootfs: runtime_template_artifact(root, "source-rootfs", b"rootfs"),
        }
    }

    async fn checkpoint_fixture(
        instance_id: &str,
    ) -> (tempfile::TempDir, FileStorageProvider, StorageSlot, PathBuf) {
        let temp = tempfile::TempDir::new().unwrap();
        let instances = temp.path().join("instances");
        let checkpoints = temp.path().join("checkpoints");
        tokio::fs::create_dir(&instances).await.unwrap();
        tokio::fs::create_dir(&checkpoints).await.unwrap();
        let provider = FileStorageProvider::new(instances);
        let slot = provider
            .acquire(&AcquireOpts {
                instance_id: instance_id.to_string(),
                rootfs_size: 64,
                mem_size: 32,
            })
            .await
            .unwrap();
        (temp, provider, slot, checkpoints)
    }

    #[tokio::test]
    async fn probe_existing_dir_returns_true() {
        let tmp = tempfile::TempDir::new().unwrap();
        let provider = FileStorageProvider::new(tmp.path().to_path_buf());
        assert!(provider.probe().await.unwrap());
    }

    #[tokio::test]
    async fn probe_missing_dir_returns_false() {
        let provider =
            FileStorageProvider::new(PathBuf::from("/nonexistent/blaze-test-storage-probe"));
        assert!(!provider.probe().await.unwrap());
    }

    #[tokio::test]
    async fn acquire_creates_slot_files() {
        let tmp = tempfile::TempDir::new().unwrap();
        let provider = FileStorageProvider::new(tmp.path().to_path_buf());
        let opts = AcquireOpts {
            instance_id: "test-inst-001".to_string(),
            rootfs_size: 1024,
            mem_size: 512,
        };
        let slot = provider.acquire(&opts).await.unwrap();
        assert_eq!(slot.id, "test-inst-001");
        assert!(slot.rootfs_path.exists());
        assert!(slot.mem_path.exists());
        assert!(slot.instance_dir.exists());
        // Verify sparse file lengths match requested sizes
        assert_eq!(
            tokio::fs::metadata(&slot.rootfs_path).await.unwrap().len(),
            1024
        );
        assert_eq!(
            tokio::fs::metadata(&slot.mem_path).await.unwrap().len(),
            512
        );
    }

    #[tokio::test]
    async fn runtime_template_acquire_owns_independent_artifacts() {
        let temp = tempfile::TempDir::new().unwrap();
        let instances = temp.path().join("instances");
        let source = temp.path().join("source");
        tokio::fs::create_dir(&instances).await.unwrap();
        tokio::fs::create_dir(&source).await.unwrap();
        let provider = FileStorageProvider::new(instances);
        let materialized = provider
            .acquire_runtime_template(
                &AcquireOpts {
                    instance_id: "template-instance".to_string(),
                    rootfs_size: 6,
                    mem_size: 6,
                },
                runtime_template_storage(&source),
            )
            .await
            .expect("materialize template");

        std::fs::write(source.join("source-rootfs"), b"changed").unwrap();
        std::fs::write(source.join("source-memory"), b"changed").unwrap();
        std::fs::write(source.join("source-vmstate"), b"changed").unwrap();

        assert_eq!(
            tokio::fs::read(&materialized.storage.rootfs_path)
                .await
                .unwrap(),
            b"rootfs"
        );
        assert_eq!(
            tokio::fs::read(&materialized.storage.mem_path)
                .await
                .unwrap(),
            b"memory"
        );
        assert_eq!(
            tokio::fs::read(&materialized.snapshot_path).await.unwrap(),
            b"snapshot"
        );
        assert!(materialized.storage.mem_diff_path.is_file());
        assert!(materialized.storage.rootfs_diff_path.is_file());
    }

    #[tokio::test]
    async fn runtime_template_acquire_rolls_back_digest_mismatch() {
        let temp = tempfile::TempDir::new().unwrap();
        let instances = temp.path().join("instances");
        let source = temp.path().join("source");
        tokio::fs::create_dir(&instances).await.unwrap();
        tokio::fs::create_dir(&source).await.unwrap();
        let provider = FileStorageProvider::new(instances.clone());
        let mut storage = runtime_template_storage(&source);
        storage.rootfs.sha256 = "0".repeat(64);

        let error = provider
            .acquire_runtime_template(
                &AcquireOpts {
                    instance_id: "bad-template".to_string(),
                    rootfs_size: 6,
                    mem_size: 6,
                },
                storage,
            )
            .await
            .expect_err("digest mismatch");
        let (_, residual) = error.into_parts();

        assert!(residual.is_none());
        assert!(!instances.join("bad-template").exists());
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn runtime_template_acquire_retains_failed_rollback() {
        let temp = tempfile::TempDir::new().unwrap();
        let instances = temp.path().join("instances");
        let source = temp.path().join("source");
        tokio::fs::create_dir(&instances).await.unwrap();
        tokio::fs::create_dir(&source).await.unwrap();
        let provider = FileStorageProvider::new(instances.clone());
        let hook = crate::failpoint::TestFailpoint::new(&[
            "storage-acquire-runtime-template-artifacts",
            "storage-acquire-rollback",
        ]);

        let error = hook
            .run(provider.acquire_runtime_template(
                &AcquireOpts {
                    instance_id: "residual-template".to_string(),
                    rootfs_size: 6,
                    mem_size: 6,
                },
                runtime_template_storage(&source),
            ))
            .await
            .expect_err("rollback failure");
        let (_, residual) = error.into_parts();

        assert_eq!(residual.expect("residual owner").id, "residual-template");
        assert!(instances.join("residual-template").is_dir());
    }

    #[tokio::test]
    async fn release_removes_instance_dir() {
        let tmp = tempfile::TempDir::new().unwrap();
        let provider = FileStorageProvider::new(tmp.path().to_path_buf());
        let opts = AcquireOpts {
            instance_id: "test-inst-release".to_string(),
            rootfs_size: 1024,
            mem_size: 512,
        };
        let slot = provider.acquire(&opts).await.unwrap();
        let dir = slot.instance_dir.clone();
        assert!(dir.exists());
        provider.release(slot).await.unwrap();
        assert!(!dir.exists());
    }

    #[tokio::test]
    async fn release_by_id_recovers_missing_and_partial_slots() {
        let tmp = tempfile::TempDir::new().unwrap();
        let provider = FileStorageProvider::new(tmp.path().to_path_buf());
        let id = Uuid::new_v4().to_string();
        let missing_id = Uuid::new_v4().to_string();
        provider.release_by_id(&missing_id).await.unwrap();
        provider.release_by_id(&missing_id).await.unwrap();
        let partial = tmp.path().join(&id);
        tokio::fs::create_dir(&partial).await.unwrap();
        tokio::fs::write(partial.join("rootfs.ext4"), b"partial")
            .await
            .unwrap();

        provider.release_by_id(&id).await.unwrap();
        provider.release_by_id(&id).await.unwrap();

        assert!(!partial.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn release_by_id_rejects_non_directory_and_symlink_slots() {
        use std::os::unix::fs::symlink;

        let tmp = tempfile::TempDir::new().unwrap();
        let provider = FileStorageProvider::new(tmp.path().to_path_buf());
        let id = Uuid::new_v4().to_string();
        let slot_path = tmp.path().join(&id);
        tokio::fs::write(&slot_path, b"not a directory")
            .await
            .unwrap();

        let file_error = provider.release_by_id(&id).await.unwrap_err();
        assert!(file_error.to_string().contains("refusing non-directory"));
        assert!(slot_path.is_file());

        tokio::fs::remove_file(&slot_path).await.unwrap();
        let target = tempfile::TempDir::new().unwrap();
        symlink(target.path(), &slot_path).unwrap();

        let symlink_error = provider.release_by_id(&id).await.unwrap_err();
        assert!(symlink_error.to_string().contains("refusing non-directory"));
        assert!(std::fs::symlink_metadata(&slot_path).unwrap().is_symlink());
        assert!(target.path().is_dir());
    }

    #[tokio::test]
    async fn owned_slot_inventory_returns_stable_ids() {
        let tmp = tempfile::TempDir::new().unwrap();
        let provider = FileStorageProvider::new(tmp.path().to_path_buf());
        let first = Uuid::new_v4().to_string();
        let second = Uuid::new_v4().to_string();
        tokio::fs::create_dir(tmp.path().join(&second))
            .await
            .unwrap();
        tokio::fs::create_dir(tmp.path().join(&first))
            .await
            .unwrap();
        let mut expected = vec![first, second];
        expected.sort();

        assert!(provider.supports_runtime_pool_recovery());
        assert_eq!(provider.list_owned_ids().await.unwrap(), expected);
    }

    #[tokio::test]
    async fn owned_slot_inventory_rejects_unknown_entry_types() {
        let tmp = tempfile::TempDir::new().unwrap();
        let provider = FileStorageProvider::new(tmp.path().to_path_buf());
        tokio::fs::write(tmp.path().join("unexpected"), b"not a slot")
            .await
            .unwrap();

        let error = provider.list_owned_ids().await.unwrap_err();

        assert!(error.to_string().contains("unexpected non-directory"));
    }

    #[tokio::test]
    async fn pool_status_returns_defaults() {
        let tmp = tempfile::TempDir::new().unwrap();
        let provider = FileStorageProvider::new(tmp.path().to_path_buf());
        let status = provider.pool_status();
        assert_eq!(status.ready, 0);
        assert_eq!(status.capacity, 0);
        assert_eq!(status.pending, 0);
        assert_eq!(provider.drain_pool().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn release_rejects_forged_path() {
        let dir = tempfile::TempDir::new().unwrap();
        let fp = FileStorageProvider::new(dir.path().to_path_buf());
        let forged_slot = StorageSlot {
            id: "../../etc".into(),
            rootfs_path: PathBuf::from("/etc/passwd"),
            mem_path: PathBuf::from("/etc/shadow"),
            mem_diff_path: PathBuf::from("/etc/shadow"),
            rootfs_diff_path: PathBuf::from("/etc/passwd"),
            instance_dir: PathBuf::from("/etc"),
        };
        assert!(fp.release(forged_slot).await.is_err());
    }

    #[tokio::test]
    async fn acquire_rejects_duplicate_id() {
        let dir = tempfile::TempDir::new().unwrap();
        let fp = FileStorageProvider::new(dir.path().to_path_buf());
        let opts = AcquireOpts {
            instance_id: "dup-1".into(),
            rootfs_size: 64,
            mem_size: 32,
        };

        // First acquire succeeds
        let _ = fp.acquire(&opts).await.unwrap();

        // Second acquire with same ID fails
        let r = fp.acquire(&opts).await;
        assert!(r.is_err());
        assert!(r.unwrap_err().to_string().contains("already exists"));
    }

    #[tokio::test]
    async fn acquire_rejects_path_traversal() {
        let tmp = tempfile::TempDir::new().unwrap();
        let provider = FileStorageProvider::new(tmp.path().to_path_buf());

        // Absolute path
        let r = provider
            .acquire(&AcquireOpts {
                instance_id: "/etc/passwd".into(),
                rootfs_size: 0,
                mem_size: 0,
            })
            .await;
        assert!(r.is_err());

        // Parent traversal
        let r = provider
            .acquire(&AcquireOpts {
                instance_id: "../escape".into(),
                rootfs_size: 0,
                mem_size: 0,
            })
            .await;
        assert!(r.is_err());

        // Slash in middle
        let r = provider
            .acquire(&AcquireOpts {
                instance_id: "foo/bar".into(),
                rootfs_size: 0,
                mem_size: 0,
            })
            .await;
        assert!(r.is_err());

        // Empty string
        let r = provider
            .acquire(&AcquireOpts {
                instance_id: "".into(),
                rootfs_size: 0,
                mem_size: 0,
            })
            .await;
        assert!(r.is_err());

        // Dot-dot
        let r = provider
            .acquire(&AcquireOpts {
                instance_id: "..".into(),
                rootfs_size: 0,
                mem_size: 0,
            })
            .await;
        assert!(r.is_err());
    }

    #[tokio::test]
    async fn reconstruct_derives_paths_from_id() {
        let temp = tempfile::TempDir::new().unwrap();
        let provider = FileStorageProvider::new(temp.path().to_path_buf());
        let slot = provider
            .acquire(&AcquireOpts {
                instance_id: "restore-me".into(),
                rootfs_size: 64,
                mem_size: 32,
            })
            .await
            .unwrap();
        let reconstructed = provider.reconstruct("restore-me").await.unwrap();
        assert_eq!(reconstructed, slot);
    }

    #[tokio::test]
    async fn reconstruct_classifies_missing_artifact_as_incomplete() {
        let temp = tempfile::TempDir::new().unwrap();
        let provider = FileStorageProvider::new(temp.path().to_path_buf());
        let slot = provider
            .acquire(&AcquireOpts {
                instance_id: "missing-artifact".into(),
                rootfs_size: 64,
                mem_size: 32,
            })
            .await
            .unwrap();
        tokio::fs::remove_file(&slot.mem_diff_path).await.unwrap();

        let error = provider
            .reconstruct("missing-artifact")
            .await
            .expect_err("missing artifact must invalidate the slot");

        assert!(matches!(
            error,
            BlazeError::StorageIncomplete {
                ref instance_id,
                ref path,
                expected: "file",
            } if instance_id == "missing-artifact" && path == &slot.mem_diff_path
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn reconstruct_rejects_a_linked_slot_root() {
        use std::os::unix::fs::symlink;

        let storage = tempfile::TempDir::new().unwrap();
        let target = tempfile::TempDir::new().unwrap();
        for artifact in ["rootfs.ext4", "mem.bin", "mem.diff", "rootfs.diff"] {
            tokio::fs::write(target.path().join(artifact), b"external")
                .await
                .unwrap();
        }
        symlink(target.path(), storage.path().join("linked-slot")).unwrap();
        let provider = FileStorageProvider::new(storage.path().to_path_buf());

        let error = provider
            .reconstruct("linked-slot")
            .await
            .expect_err("linked slot root must be rejected");

        assert!(matches!(
            error,
            BlazeError::StorageIncomplete {
                ref instance_id,
                ref path,
                expected: "directory",
            } if instance_id == "linked-slot" && path == &storage.path().join("linked-slot")
        ));
        assert!(
            std::fs::symlink_metadata(storage.path().join("linked-slot"))
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert!(target.path().is_dir());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn reconstruct_rejects_a_linked_slot_artifact() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::TempDir::new().unwrap();
        let provider = FileStorageProvider::new(temp.path().to_path_buf());
        let slot = provider
            .acquire(&AcquireOpts {
                instance_id: "linked-artifact".into(),
                rootfs_size: 64,
                mem_size: 32,
            })
            .await
            .unwrap();
        tokio::fs::remove_file(&slot.mem_diff_path).await.unwrap();
        let external = temp.path().join("external-memory-diff");
        tokio::fs::write(&external, b"external").await.unwrap();
        symlink(&external, &slot.mem_diff_path).unwrap();

        let error = provider
            .reconstruct("linked-artifact")
            .await
            .expect_err("linked artifact must be rejected");

        assert!(matches!(
            error,
            BlazeError::StorageIncomplete {
                ref instance_id,
                ref path,
                expected: "file",
            } if instance_id == "linked-artifact" && path == &slot.mem_diff_path
        ));
        assert!(
            std::fs::symlink_metadata(&slot.mem_diff_path)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert!(external.is_file());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn flush_rejects_a_linked_slot_root() {
        use std::os::unix::fs::symlink;

        let storage = tempfile::TempDir::new().unwrap();
        let target = tempfile::TempDir::new().unwrap();
        for artifact in ["rootfs.ext4", "mem.bin", "mem.diff", "rootfs.diff"] {
            tokio::fs::write(target.path().join(artifact), b"external")
                .await
                .unwrap();
        }
        symlink(target.path(), storage.path().join("linked-flush")).unwrap();
        let provider = FileStorageProvider::new(storage.path().to_path_buf());
        let slot = provider.slot_for_id("linked-flush").unwrap();

        let error = provider
            .flush_dirty(&slot)
            .await
            .expect_err("linked slot root must not be flushed");

        assert!(matches!(
            error,
            BlazeError::StorageIncomplete {
                ref instance_id,
                ref path,
                expected: "directory",
            } if instance_id == "linked-flush" && path == &storage.path().join("linked-flush")
        ));
        assert!(target.path().is_dir());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn flush_rejects_a_linked_slot_artifact() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::TempDir::new().unwrap();
        let provider = FileStorageProvider::new(temp.path().to_path_buf());
        let slot = provider
            .acquire(&AcquireOpts {
                instance_id: "linked-flush-artifact".into(),
                rootfs_size: 64,
                mem_size: 32,
            })
            .await
            .unwrap();
        tokio::fs::remove_file(&slot.mem_diff_path).await.unwrap();
        let external = temp.path().join("external-memory-diff");
        tokio::fs::write(&external, b"external").await.unwrap();
        symlink(&external, &slot.mem_diff_path).unwrap();

        let error = provider
            .flush_dirty(&slot)
            .await
            .expect_err("linked artifact must not be flushed");

        assert!(matches!(
            error,
            BlazeError::StorageIncomplete {
                ref instance_id,
                ref path,
                expected: "file",
            } if instance_id == "linked-flush-artifact" && path == &slot.mem_diff_path
        ));
        assert_eq!(tokio::fs::read(&external).await.unwrap(), b"external");
    }

    #[tokio::test]
    async fn flush_rederives_canonical_paths_from_slot_id() {
        let temp = tempfile::TempDir::new().unwrap();
        let provider = FileStorageProvider::new(temp.path().to_path_buf());
        let slot = provider
            .acquire(&AcquireOpts {
                instance_id: "flush-canonical".into(),
                rootfs_size: 64,
                mem_size: 32,
            })
            .await
            .unwrap();
        tokio::fs::write(&slot.mem_diff_path, b"dirty-memory")
            .await
            .unwrap();
        tokio::fs::write(&slot.rootfs_diff_path, b"dirty-rootfs")
            .await
            .unwrap();

        let mut forged = slot.clone();
        forged.rootfs_path = PathBuf::from("/must/not/be/opened/rootfs");
        forged.mem_path = PathBuf::from("/must/not/be/opened/memory");
        forged.mem_diff_path = PathBuf::from("/must/not/be/opened/memory-diff");
        forged.rootfs_diff_path = PathBuf::from("/must/not/be/opened/rootfs-diff");
        forged.instance_dir = PathBuf::from("/must/not/be/opened");

        provider
            .flush_dirty(&forged)
            .await
            .expect("provider uses canonical paths");
    }

    #[tokio::test]
    async fn flush_rejects_incomplete_provider_slot() {
        let temp = tempfile::TempDir::new().unwrap();
        let provider = FileStorageProvider::new(temp.path().to_path_buf());
        let slot = provider
            .acquire(&AcquireOpts {
                instance_id: "flush-incomplete".into(),
                rootfs_size: 64,
                mem_size: 32,
            })
            .await
            .unwrap();
        tokio::fs::remove_file(&slot.mem_diff_path).await.unwrap();

        let error = provider
            .flush_dirty(&slot)
            .await
            .expect_err("missing artifact must fail the sweep item");
        assert!(error.to_string().contains("mem.diff"), "{error}");
    }

    #[tokio::test]
    async fn checkpoint_capture_is_explicit_and_independent() {
        let (_temp, provider, slot, checkpoints) = checkpoint_fixture("capture-independent").await;
        tokio::fs::write(&slot.rootfs_path, b"captured-rootfs")
            .await
            .unwrap();
        let target = checkpoints.join("rootfs.snap");

        assert!(provider.supports_checkpoint_capture());
        provider.capture_checkpoint(&slot, &target).await.unwrap();
        tokio::fs::write(&slot.rootfs_path, b"changed-live-rootfs")
            .await
            .unwrap();

        assert_eq!(tokio::fs::read(&target).await.unwrap(), b"captured-rootfs");
    }

    #[tokio::test]
    async fn checkpoint_capture_does_not_replace_the_live_rootfs() {
        let (_temp, provider, slot, checkpoints) = checkpoint_fixture("capture-read-only").await;
        tokio::fs::write(&slot.rootfs_path, b"live-rootfs")
            .await
            .unwrap();

        provider
            .capture_checkpoint(&slot, &checkpoints.join("rootfs.snap"))
            .await
            .unwrap();

        assert_eq!(
            tokio::fs::read(&slot.rootfs_path).await.unwrap(),
            b"live-rootfs"
        );
    }

    #[tokio::test]
    async fn checkpoint_capture_ignores_forged_slot_paths() {
        let (temp, provider, slot, checkpoints) = checkpoint_fixture("capture-canonical").await;
        tokio::fs::write(&slot.rootfs_path, b"canonical-rootfs")
            .await
            .unwrap();
        let forged_source = temp.path().join("forged-rootfs");
        tokio::fs::write(&forged_source, b"forged-rootfs")
            .await
            .unwrap();
        let mut forged = slot.clone();
        forged.rootfs_path = forged_source;
        forged.mem_path = temp.path().join("forged-memory");
        forged.mem_diff_path = temp.path().join("forged-memory-diff");
        forged.rootfs_diff_path = temp.path().join("forged-rootfs-diff");
        forged.instance_dir = temp.path().to_path_buf();
        let target = checkpoints.join("rootfs.snap");

        provider.capture_checkpoint(&forged, &target).await.unwrap();

        assert_eq!(tokio::fs::read(&target).await.unwrap(), b"canonical-rootfs");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn checkpoint_capture_rejects_a_linked_rootfs() {
        use std::os::unix::fs::symlink;

        let (temp, provider, slot, checkpoints) = checkpoint_fixture("capture-linked-source").await;
        tokio::fs::remove_file(&slot.rootfs_path).await.unwrap();
        let external = temp.path().join("external-rootfs");
        tokio::fs::write(&external, b"external").await.unwrap();
        symlink(&external, &slot.rootfs_path).unwrap();
        let target = checkpoints.join("rootfs.snap");

        provider
            .capture_checkpoint(&slot, &target)
            .await
            .expect_err("linked rootfs must not be captured");

        assert!(!target.exists());
        assert_eq!(tokio::fs::read(external).await.unwrap(), b"external");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn checkpoint_capture_rejects_a_linked_slot_directory() {
        use std::os::unix::fs::symlink;

        let (temp, provider, slot, checkpoints) = checkpoint_fixture("capture-linked-slot").await;
        tokio::fs::remove_dir_all(&slot.instance_dir).await.unwrap();
        let external = temp.path().join("external-slot");
        tokio::fs::create_dir(&external).await.unwrap();
        tokio::fs::write(external.join("rootfs.ext4"), b"external")
            .await
            .unwrap();
        symlink(&external, &slot.instance_dir).unwrap();
        let target = checkpoints.join("rootfs.snap");

        provider
            .capture_checkpoint(&slot, &target)
            .await
            .expect_err("linked slot directory must be rejected");

        assert!(!target.exists());
        assert_eq!(
            tokio::fs::read(external.join("rootfs.ext4")).await.unwrap(),
            b"external"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn checkpoint_capture_rejects_a_linked_target_parent() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::TempDir::new().unwrap();
        let instances = temp.path().join("instances");
        let external = temp.path().join("external-checkpoints");
        tokio::fs::create_dir(&instances).await.unwrap();
        tokio::fs::create_dir(&external).await.unwrap();
        let linked_parent = temp.path().join("linked-checkpoints");
        symlink(&external, &linked_parent).unwrap();
        let provider = FileStorageProvider::new(instances);
        let slot = provider
            .acquire(&AcquireOpts {
                instance_id: "capture-linked-parent".into(),
                rootfs_size: 64,
                mem_size: 32,
            })
            .await
            .unwrap();
        let target = linked_parent.join("rootfs.snap");

        provider
            .capture_checkpoint(&slot, &target)
            .await
            .expect_err("linked target parent must be rejected");

        assert!(!external.join("rootfs.snap").exists());
    }

    #[tokio::test]
    async fn checkpoint_capture_preserves_an_existing_target() {
        let (_temp, provider, slot, checkpoints) =
            checkpoint_fixture("capture-existing-target").await;
        tokio::fs::write(&slot.rootfs_path, b"new-checkpoint")
            .await
            .unwrap();
        let target = checkpoints.join("rootfs.snap");
        tokio::fs::write(&target, b"existing-checkpoint")
            .await
            .unwrap();

        provider
            .capture_checkpoint(&slot, &target)
            .await
            .expect_err("capture must never replace an existing target");

        assert_eq!(
            tokio::fs::read(&target).await.unwrap(),
            b"existing-checkpoint"
        );
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn checkpoint_capture_cleans_temporary_data_after_failure() {
        let (_temp, provider, slot, checkpoints) = checkpoint_fixture("capture-cleanup").await;
        tokio::fs::write(&slot.rootfs_path, b"complete-temporary-copy")
            .await
            .unwrap();
        let target = checkpoints.join("rootfs.snap");
        let hook = crate::failpoint::TestFailpoint::new(&["storage-capture-after-link"]);

        hook.run(provider.capture_checkpoint(&slot, &target))
            .await
            .expect_err("armed capture must roll back its unpublished target");

        assert!(!target.exists());
        assert!(
            tokio::fs::read_dir(&checkpoints)
                .await
                .unwrap()
                .next_entry()
                .await
                .unwrap()
                .is_none(),
            "capture failure must remove its temporary file"
        );
    }
}
