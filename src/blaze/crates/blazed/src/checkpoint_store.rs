// SPDX-License-Identifier: Apache-2.0
//! Filesystem-backed checkpoint catalog owned by the daemon.
//!
//! Publication and HEAD updates are separate durability boundaries. A
//! checkpoint can therefore be published but unreachable after an interrupted
//! operation; listing exposes that state for explicit inspection.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use blaze_core::checkpoint::{
    CHECKPOINT_FORMAT_VERSION, CheckpointArtifact, CheckpointInfo, CheckpointMetadata,
    CheckpointValidationError, CommitCheckpoint, REQUIRED_ARTIFACTS, validate_artifact_name,
    validate_checkpoint_id, validate_checkpoint_manifest, validate_commit_checkpoint,
};
use chrono::Utc;
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

const METADATA_FILE: &str = "metadata.json";
const HEAD_FILE: &str = "HEAD";
const STAGING_SUFFIX: &str = ".tmp";
const TOMBSTONE_SUFFIX: &str = ".tombstone";
const ABORT_TOMBSTONE_PREFIX: &str = ".abort.";

/// Failure while reading or mutating the daemon checkpoint catalog.
#[derive(Debug, Error)]
pub enum CheckpointStoreError {
    /// A checkpoint record failed pure model validation.
    #[error(transparent)]
    Validation(#[from] CheckpointValidationError),

    /// A catalog filesystem operation failed.
    #[error("checkpoint catalog {operation} failed for {}: {source}", path.display())]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// A metadata file could not be encoded or decoded.
    #[error("checkpoint metadata at {} is invalid: {source}", path.display())]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },

    /// The catalog layout violates an invariant required for safe mutation.
    #[error("checkpoint catalog invariant failed: {0}")]
    Invariant(String),
}

/// Convenient result type for checkpoint catalog operations.
pub type Result<T> = std::result::Result<T, CheckpointStoreError>;

/// Verified checkpoint metadata and provider-safe artifact paths.
#[derive(Debug)]
pub struct VerifiedCheckpoint {
    /// Validated checkpoint manifest.
    pub metadata: CheckpointMetadata,
    /// Full backend-state snapshot.
    pub snapshot_path: PathBuf,
    /// Full guest-memory snapshot.
    pub memory_path: PathBuf,
    /// Self-contained root filesystem snapshot.
    pub rootfs_path: PathBuf,
}

/// Temporary checkpoint directory populated before atomic publication.
#[derive(Debug)]
pub struct CheckpointStage {
    id: String,
    sandbox_id: Uuid,
    path: PathBuf,
    final_path: PathBuf,
}

impl CheckpointStage {
    /// Generated checkpoint identifier.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Resolve one frozen artifact inside the stage.
    pub fn artifact_path(&self, name: &str) -> Result<PathBuf> {
        validate_artifact_name(name)?;
        Ok(self.path.join(name))
    }
}

/// Filesystem-backed checkpoint catalog.
#[derive(Debug, Clone)]
pub struct CheckpointStore {
    root: PathBuf,
}

impl CheckpointStore {
    /// Create a catalog rooted at `root` without performing I/O.
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    /// Create and durably expose a unique staging directory.
    pub fn begin(&self, sandbox_id: Uuid) -> Result<CheckpointStage> {
        let sandbox_dir = self.ensure_sandbox_dir(sandbox_id)?;
        let id = format!("ckpt-{}", Uuid::new_v4());
        let path = sandbox_dir.join(format!(".{id}{STAGING_SUFFIX}"));
        create_directory(&path, "create staging directory")?;
        sync_directory(&sandbox_dir)?;
        Ok(CheckpointStage {
            final_path: sandbox_dir.join(&id),
            id,
            sandbox_id,
            path,
        })
    }

    /// Hash, sync, and atomically publish a populated stage without moving HEAD.
    pub fn publish(
        &self,
        stage: &CheckpointStage,
        input: CommitCheckpoint,
    ) -> Result<CheckpointMetadata> {
        self.validate_stage(stage)?;
        validate_commit_checkpoint(&stage.id, &input)?;
        if let Some(parent) = &input.parent {
            self.validated_chain_from(stage.sandbox_id, parent)?;
        }
        ensure_missing(&stage.final_path, "inspect publication target")?;
        validate_exact_entries(&stage.path, &REQUIRED_ARTIFACTS)?;

        let mut artifacts = Vec::with_capacity(REQUIRED_ARTIFACTS.len());
        for name in REQUIRED_ARTIFACTS {
            let path = require_contained_file(&stage.path, name)?;
            sync_file(&path)?;
            artifacts.push(hash_artifact(&path, name)?);
        }

        let metadata = CheckpointMetadata {
            format_version: CHECKPOINT_FORMAT_VERSION,
            id: stage.id.clone(),
            parent: input.parent,
            sandbox_id: stage.sandbox_id,
            policy_name: input.policy_name,
            image_digest: input.image_digest,
            backend: input.backend,
            backend_version: input.backend_version,
            created_at: Utc::now(),
            snapshot_kind: input.snapshot_kind,
            artifacts,
        };
        validate_checkpoint_manifest(&metadata, stage.sandbox_id, &stage.id)?;

        let metadata_path = stage.path.join(METADATA_FILE);
        write_json_new(&metadata_path, &metadata)?;
        sync_directory(&stage.path)?;
        rename_path(
            &stage.path,
            &stage.final_path,
            "publish checkpoint directory",
        )?;
        let sandbox_dir = stage
            .final_path
            .parent()
            .ok_or_else(|| invariant("published checkpoint has no sandbox parent"))?;
        checkpoint_store_failpoint("checkpoint-store-publish-after-rename", &stage.final_path)?;
        sync_directory(sandbox_dir)?;
        Ok(metadata)
    }

    /// Remove an unpublished stage owned by this process.
    pub fn abort(&self, stage: CheckpointStage) -> Result<()> {
        self.abort_staging(stage.sandbox_id, &stage.id)
    }

    /// Remove one unpublished stage if it still exists.
    ///
    /// The stage is first renamed to a tombstone so a process interruption
    /// cannot make it appear publishable again. Startup cleanup removes any
    /// residual tombstone.
    pub fn abort_staging(&self, sandbox_id: Uuid, checkpoint_id: &str) -> Result<()> {
        validate_checkpoint_id(checkpoint_id)?;
        let Some(sandbox_dir) = self.optional_sandbox_dir(sandbox_id)? else {
            return Ok(());
        };
        let stage = sandbox_dir.join(format!(".{checkpoint_id}{STAGING_SUFFIX}"));
        let Some(metadata) = optional_symlink_metadata(&stage, "inspect staging directory")? else {
            return Ok(());
        };
        require_plain_directory_metadata(&stage, &metadata, "checkpoint staging directory")?;
        require_direct_child(&sandbox_dir, &stage, "checkpoint staging directory")?;

        let tombstone = tombstone_path(&sandbox_dir, ABORT_TOMBSTONE_PREFIX, checkpoint_id);
        rename_path(&stage, &tombstone, "tombstone aborted checkpoint stage")?;
        sync_directory(&sandbox_dir)?;
        remove_directory(&tombstone, "remove aborted checkpoint tombstone")?;
        sync_directory(&sandbox_dir)
    }

    /// Read and validate one committed checkpoint and all artifact hashes.
    pub fn verify(&self, sandbox_id: Uuid, checkpoint_id: &str) -> Result<CheckpointMetadata> {
        let dir = self.committed_dir(sandbox_id, checkpoint_id)?;
        validate_exact_entries(
            &dir,
            &[
                REQUIRED_ARTIFACTS[0],
                REQUIRED_ARTIFACTS[1],
                REQUIRED_ARTIFACTS[2],
                METADATA_FILE,
            ],
        )?;
        let metadata_path = require_contained_file(&dir, METADATA_FILE)?;
        let bytes = read_file(&metadata_path, "read checkpoint metadata")?;
        let metadata: CheckpointMetadata =
            serde_json::from_slice(&bytes).map_err(|source| CheckpointStoreError::Json {
                path: metadata_path,
                source,
            })?;
        validate_checkpoint_manifest(&metadata, sandbox_id, checkpoint_id)?;

        for name in REQUIRED_ARTIFACTS {
            let expected = metadata
                .artifacts
                .iter()
                .find(|artifact| artifact.name == name)
                .ok_or_else(|| {
                    invariant(format!(
                        "validated checkpoint {checkpoint_id} has no record for {name}"
                    ))
                })?;
            let path = require_contained_file(&dir, name)?;
            let actual = hash_artifact(&path, name)?;
            if &actual != expected {
                return Err(invariant(format!(
                    "checkpoint {checkpoint_id} artifact {name} failed integrity validation"
                )));
            }
        }
        Ok(metadata)
    }

    /// Verify a restore target, its complete ancestry, and its artifact paths.
    pub fn verify_restore_target(
        &self,
        sandbox_id: Uuid,
        checkpoint_id: &str,
    ) -> Result<VerifiedCheckpoint> {
        let metadata = self.verify(sandbox_id, checkpoint_id)?;
        if let Some(parent) = metadata.parent.as_deref() {
            self.validated_chain_from(sandbox_id, parent)?;
        }
        let directory = self.committed_dir(sandbox_id, checkpoint_id)?;
        Ok(VerifiedCheckpoint {
            snapshot_path: require_contained_file(&directory, "vmstate.snap")?,
            memory_path: require_contained_file(&directory, "memory.snap")?,
            rootfs_path: require_contained_file(&directory, "rootfs.snap")?,
            metadata,
        })
    }

    /// List committed checkpoints and mark the lineage reachable from HEAD.
    pub fn list(&self, sandbox_id: Uuid) -> Result<Vec<CheckpointInfo>> {
        let Some(_) = self.optional_sandbox_dir(sandbox_id)? else {
            return Ok(Vec::new());
        };
        let catalog = self.load_catalog(sandbox_id)?;
        let head = self.read_head(sandbox_id)?;
        let on_head_chain = match head.as_deref() {
            Some(head) => lineage_from(&catalog, head)?,
            None => HashSet::new(),
        };

        let mut checkpoints = Vec::with_capacity(catalog.len());
        for metadata in catalog.into_values() {
            let size_bytes = metadata
                .artifacts
                .iter()
                .try_fold(0_u64, |total, artifact| {
                    total.checked_add(artifact.size_bytes)
                })
                .ok_or_else(|| {
                    invariant(format!(
                        "checkpoint {} artifact sizes overflow u64",
                        metadata.id
                    ))
                })?;
            checkpoints.push(CheckpointInfo {
                id: metadata.id.clone(),
                parent: metadata.parent,
                created_at: metadata.created_at,
                size_bytes,
                is_head: head.as_deref() == Some(metadata.id.as_str()),
                on_head_chain: on_head_chain.contains(&metadata.id),
            });
        }
        checkpoints.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(checkpoints)
    }

    /// Atomically move HEAD to an already committed, verified checkpoint.
    pub fn set_head(&self, sandbox_id: Uuid, checkpoint_id: &str) -> Result<()> {
        self.verify(sandbox_id, checkpoint_id)?;
        let sandbox_dir = self
            .optional_sandbox_dir(sandbox_id)?
            .ok_or_else(|| invariant(format!("checkpoint sandbox {sandbox_id} disappeared")))?;
        validate_existing_head_type(&sandbox_dir)?;

        let temporary = sandbox_dir.join(format!(".HEAD.{}{STAGING_SUFFIX}", Uuid::new_v4()));
        let outcome = (|| {
            let mut file = open_new_file(&temporary, "create temporary HEAD")?;
            write_all(&mut file, &temporary, checkpoint_id.as_bytes())?;
            write_all(&mut file, &temporary, b"\n")?;
            sync_open_file(&file, &temporary)?;
            rename_path(
                &temporary,
                &sandbox_dir.join(HEAD_FILE),
                "publish checkpoint HEAD",
            )?;
            checkpoint_store_failpoint(
                "checkpoint-store-head-after-rename",
                &sandbox_dir.join(HEAD_FILE),
            )?;
            sync_directory(&sandbox_dir)
        })();
        if outcome.is_err() {
            let _ = remove_file_if_exists(&temporary, "remove temporary HEAD");
        }
        outcome
    }

    /// Return the persisted HEAD, if present.
    pub fn read_head(&self, sandbox_id: Uuid) -> Result<Option<String>> {
        let Some(sandbox_dir) = self.optional_sandbox_dir(sandbox_id)? else {
            return Ok(None);
        };
        let path = sandbox_dir.join(HEAD_FILE);
        let Some(metadata) = optional_symlink_metadata(&path, "inspect checkpoint HEAD")? else {
            return Ok(None);
        };
        require_plain_file_metadata(&path, &metadata, "checkpoint HEAD")?;
        require_direct_child(&sandbox_dir, &path, "checkpoint HEAD")?;
        let bytes = read_file(&path, "read checkpoint HEAD")?;
        let raw = std::str::from_utf8(&bytes)
            .map_err(|error| invariant(format!("checkpoint HEAD is not UTF-8: {error}")))?;
        let checkpoint_id = raw
            .strip_suffix('\n')
            .filter(|value| !value.contains('\n') && !value.contains('\r'))
            .ok_or_else(|| invariant("checkpoint HEAD is not one canonical line"))?;
        validate_checkpoint_id(checkpoint_id)?;
        self.committed_dir(sandbox_id, checkpoint_id)?;
        Ok(Some(checkpoint_id.to_string()))
    }

    /// Remove incomplete stages, temporary HEAD files, and cleanup tombstones.
    ///
    /// Committed checkpoint directories and the published HEAD are retained.
    pub fn cleanup_transaction_artifacts(&self, sandbox_id: Uuid) -> Result<Vec<PathBuf>> {
        let scratch = self.transaction_artifacts(sandbox_id)?;
        let Some(sandbox_dir) = self.optional_sandbox_dir(sandbox_id)? else {
            return Ok(Vec::new());
        };

        let mut removed = Vec::with_capacity(scratch.len());
        for (path, kind) in scratch {
            match kind {
                ScratchKind::Directory => {
                    remove_directory(&path, "remove checkpoint scratch directory")?
                }
                ScratchKind::File => remove_file(&path, "remove checkpoint scratch file")?,
            }
            removed.push(path);
        }
        if !removed.is_empty() {
            sync_directory(&sandbox_dir)?;
        }
        Ok(removed)
    }

    fn transaction_artifacts(&self, sandbox_id: Uuid) -> Result<Vec<(PathBuf, ScratchKind)>> {
        let Some(sandbox_dir) = self.optional_sandbox_dir(sandbox_id)? else {
            return Ok(Vec::new());
        };
        let mut scratch = Vec::new();
        let entries = read_directory(&sandbox_dir, "scan checkpoint scratch")?;
        for entry in entries {
            let entry = entry
                .map_err(|source| io_error("read checkpoint scratch", &sandbox_dir, source))?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let Some(kind) = classify_scratch_name(name)? else {
                continue;
            };
            let path = entry.path();
            let file_type = entry
                .file_type()
                .map_err(|source| io_error("inspect checkpoint scratch", &path, source))?;
            match kind {
                ScratchKind::Directory if file_type.is_dir() && !file_type.is_symlink() => {
                    scratch.push((path, ScratchKind::Directory));
                }
                ScratchKind::File if file_type.is_file() && !file_type.is_symlink() => {
                    scratch.push((path, ScratchKind::File));
                }
                _ => {
                    return Err(invariant(format!(
                        "checkpoint scratch {} has an unexpected file type",
                        path.display()
                    )));
                }
            }
        }
        scratch.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(scratch)
    }

    fn validate_stage(&self, stage: &CheckpointStage) -> Result<()> {
        validate_checkpoint_id(&stage.id)?;
        let sandbox_dir = self
            .optional_sandbox_dir(stage.sandbox_id)?
            .ok_or_else(|| invariant("checkpoint staging sandbox does not exist"))?;
        let expected_path = sandbox_dir.join(format!(".{}{STAGING_SUFFIX}", stage.id));
        let expected_final_path = sandbox_dir.join(&stage.id);
        if stage.path != expected_path || stage.final_path != expected_final_path {
            return Err(invariant(
                "checkpoint stage paths do not match its frozen identity",
            ));
        }
        require_plain_directory(&stage.path, "checkpoint staging directory")?;
        require_direct_child(&sandbox_dir, &stage.path, "checkpoint staging directory")
    }

    fn validated_chain_from(&self, sandbox_id: Uuid, checkpoint_id: &str) -> Result<Vec<String>> {
        validate_checkpoint_id(checkpoint_id)?;
        let mut current = checkpoint_id.to_string();
        let mut lineage = Vec::new();
        let mut seen = HashSet::new();
        loop {
            if !seen.insert(current.clone()) {
                return Err(invariant(format!(
                    "checkpoint parent cycle reaches {current}"
                )));
            }
            let metadata = self.verify(sandbox_id, &current)?;
            lineage.push(current);
            let Some(parent) = metadata.parent else {
                break;
            };
            current = parent;
        }
        Ok(lineage)
    }

    fn load_catalog(&self, sandbox_id: Uuid) -> Result<HashMap<String, CheckpointMetadata>> {
        let Some(sandbox_dir) = self.optional_sandbox_dir(sandbox_id)? else {
            return Ok(HashMap::new());
        };
        let mut catalog = HashMap::new();
        let entries = read_directory(&sandbox_dir, "scan checkpoint catalog")?;
        for entry in entries {
            let entry = entry
                .map_err(|source| io_error("read checkpoint catalog", &sandbox_dir, source))?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if !name.starts_with("ckpt-") {
                continue;
            }
            validate_checkpoint_id(name)?;
            let file_type = entry
                .file_type()
                .map_err(|source| io_error("inspect checkpoint entry", entry.path(), source))?;
            if !file_type.is_dir() || file_type.is_symlink() {
                return Err(invariant(format!(
                    "checkpoint entry {} is not a plain directory",
                    entry.path().display()
                )));
            }
            let metadata = self.verify(sandbox_id, name)?;
            catalog.insert(name.to_string(), metadata);
        }
        Ok(catalog)
    }

    fn ensure_sandbox_dir(&self, sandbox_id: Uuid) -> Result<PathBuf> {
        let root_was_missing =
            optional_symlink_metadata(&self.root, "inspect checkpoint root")?.is_none();
        create_directories(&self.root, "create checkpoint root")?;
        require_plain_directory(&self.root, "checkpoint root")?;
        if root_was_missing
            && let Some(parent) = self
                .root
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
        {
            sync_directory(parent)?;
        }
        let sandbox_dir = self.sandbox_dir(sandbox_id);
        match fs::create_dir(&sandbox_dir) {
            Ok(()) => {
                sync_directory(&self.root)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(source) => {
                return Err(io_error(
                    "create checkpoint sandbox directory",
                    &sandbox_dir,
                    source,
                ));
            }
        }
        require_plain_directory(&sandbox_dir, "checkpoint sandbox directory")?;
        require_direct_child(&self.root, &sandbox_dir, "checkpoint sandbox directory")?;
        Ok(sandbox_dir)
    }

    fn optional_sandbox_dir(&self, sandbox_id: Uuid) -> Result<Option<PathBuf>> {
        let Some(root_metadata) = optional_symlink_metadata(&self.root, "inspect checkpoint root")?
        else {
            return Ok(None);
        };
        require_plain_directory_metadata(&self.root, &root_metadata, "checkpoint root")?;
        let sandbox_dir = self.sandbox_dir(sandbox_id);
        let Some(metadata) =
            optional_symlink_metadata(&sandbox_dir, "inspect checkpoint sandbox directory")?
        else {
            return Ok(None);
        };
        require_plain_directory_metadata(&sandbox_dir, &metadata, "checkpoint sandbox directory")?;
        require_direct_child(&self.root, &sandbox_dir, "checkpoint sandbox directory")?;
        Ok(Some(sandbox_dir))
    }

    fn committed_dir(&self, sandbox_id: Uuid, checkpoint_id: &str) -> Result<PathBuf> {
        validate_checkpoint_id(checkpoint_id)?;
        let sandbox_dir = self
            .optional_sandbox_dir(sandbox_id)?
            .ok_or_else(|| invariant(format!("checkpoint sandbox {sandbox_id} does not exist")))?;
        let path = sandbox_dir.join(checkpoint_id);
        require_plain_directory(&path, "committed checkpoint directory")?;
        require_direct_child(&sandbox_dir, &path, "committed checkpoint directory")?;
        Ok(path)
    }

    fn sandbox_dir(&self, sandbox_id: Uuid) -> PathBuf {
        self.root.join(sandbox_id.to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScratchKind {
    Directory,
    File,
}

fn lineage_from(
    catalog: &HashMap<String, CheckpointMetadata>,
    checkpoint_id: &str,
) -> Result<HashSet<String>> {
    validate_checkpoint_id(checkpoint_id)?;
    let mut current = checkpoint_id;
    let mut lineage = HashSet::new();
    loop {
        if !lineage.insert(current.to_string()) {
            return Err(invariant(format!(
                "checkpoint parent cycle reaches {current}"
            )));
        }
        let metadata = catalog.get(current).ok_or_else(|| {
            invariant(format!(
                "checkpoint lineage references missing checkpoint {current}"
            ))
        })?;
        let Some(parent) = metadata.parent.as_deref() else {
            break;
        };
        current = parent;
    }
    Ok(lineage)
}

fn validate_exact_entries(directory: &Path, expected: &[&str]) -> Result<()> {
    let expected = expected.iter().copied().collect::<HashSet<_>>();
    let mut observed = HashSet::new();
    let entries = read_directory(directory, "scan checkpoint directory")?;
    for entry in entries {
        let entry =
            entry.map_err(|source| io_error("read checkpoint directory", directory, source))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(invariant(format!(
                "checkpoint directory {} contains a non-UTF-8 entry",
                directory.display()
            )));
        };
        let file_type = entry
            .file_type()
            .map_err(|source| io_error("inspect checkpoint entry", entry.path(), source))?;
        if !expected.contains(name) || !file_type.is_file() || file_type.is_symlink() {
            return Err(invariant(format!(
                "checkpoint directory {} contains unexpected entry {name:?}",
                directory.display()
            )));
        }
        observed.insert(name.to_string());
    }
    if observed.len() != expected.len() || expected.iter().any(|name| !observed.contains(*name)) {
        return Err(invariant(format!(
            "checkpoint directory {} does not contain the exact required file set",
            directory.display()
        )));
    }
    Ok(())
}

fn hash_artifact(path: &Path, name: &str) -> Result<CheckpointArtifact> {
    let mut file = open_file(path, "open checkpoint artifact")?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 128 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|source| io_error("read checkpoint artifact", path, source))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let size_bytes = file
        .metadata()
        .map_err(|source| io_error("inspect checkpoint artifact", path, source))?
        .len();
    Ok(CheckpointArtifact {
        name: name.to_string(),
        size_bytes,
        sha256: format!("{:x}", hasher.finalize()),
    })
}

fn classify_scratch_name(name: &str) -> Result<Option<ScratchKind>> {
    if let Some(checkpoint_id) = name
        .strip_prefix('.')
        .and_then(|name| name.strip_suffix(STAGING_SUFFIX))
        .filter(|name| name.starts_with("ckpt-"))
    {
        validate_checkpoint_id(checkpoint_id)?;
        return Ok(Some(ScratchKind::Directory));
    }
    if let Some(nonce) = name
        .strip_prefix(".HEAD.")
        .and_then(|name| name.strip_suffix(STAGING_SUFFIX))
    {
        parse_uuid_component(nonce, "temporary HEAD")?;
        return Ok(Some(ScratchKind::File));
    }
    if let Some(body) = name
        .strip_prefix(ABORT_TOMBSTONE_PREFIX)
        .and_then(|name| name.strip_suffix(TOMBSTONE_SUFFIX))
    {
        let (checkpoint_id, nonce) = body
            .rsplit_once('.')
            .ok_or_else(|| invariant(format!("invalid checkpoint tombstone {name:?}")))?;
        validate_checkpoint_id(checkpoint_id)?;
        parse_uuid_component(nonce, "checkpoint tombstone")?;
        return Ok(Some(ScratchKind::Directory));
    }
    Ok(None)
}

fn parse_uuid_component(value: &str, label: &str) -> Result<Uuid> {
    let uuid = Uuid::parse_str(value)
        .map_err(|error| invariant(format!("invalid {label} identifier {value:?}: {error}")))?;
    if value != uuid.to_string() {
        return Err(invariant(format!(
            "{label} identifier {value:?} is not canonical"
        )));
    }
    Ok(uuid)
}

fn tombstone_path(directory: &Path, prefix: &str, checkpoint_id: &str) -> PathBuf {
    directory.join(format!(
        "{prefix}{checkpoint_id}.{}{TOMBSTONE_SUFFIX}",
        Uuid::new_v4()
    ))
}

fn validate_existing_head_type(sandbox_dir: &Path) -> Result<()> {
    let path = sandbox_dir.join(HEAD_FILE);
    let Some(metadata) = optional_symlink_metadata(&path, "inspect existing checkpoint HEAD")?
    else {
        return Ok(());
    };
    require_plain_file_metadata(&path, &metadata, "existing checkpoint HEAD")
}

fn require_plain_directory(path: &Path, label: &str) -> Result<()> {
    let metadata = symlink_metadata(path, "inspect directory")?;
    require_plain_directory_metadata(path, &metadata, label)
}

fn require_plain_directory_metadata(
    path: &Path,
    metadata: &fs::Metadata,
    label: &str,
) -> Result<()> {
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(invariant(format!(
            "{label} {} is not a plain directory",
            path.display()
        )));
    }
    Ok(())
}

fn require_plain_file_metadata(path: &Path, metadata: &fs::Metadata, label: &str) -> Result<()> {
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(invariant(format!(
            "{label} {} is not a plain file",
            path.display()
        )));
    }
    Ok(())
}

fn require_contained_file(directory: &Path, name: &str) -> Result<PathBuf> {
    if name != METADATA_FILE {
        validate_artifact_name(name)?;
    }
    let path = directory.join(name);
    let metadata = symlink_metadata(&path, "inspect checkpoint file")?;
    require_plain_file_metadata(&path, &metadata, "checkpoint file")?;
    require_direct_child(directory, &path, "checkpoint file")?;
    Ok(path)
}

fn require_direct_child(parent: &Path, child: &Path, label: &str) -> Result<()> {
    let canonical_parent = canonicalize(parent, "canonicalize checkpoint parent")?;
    let canonical_child = canonicalize(child, "canonicalize checkpoint child")?;
    if canonical_child.parent() != Some(canonical_parent.as_path()) {
        return Err(invariant(format!(
            "{label} {} is not directly contained by {}",
            child.display(),
            parent.display()
        )));
    }
    Ok(())
}

fn ensure_missing(path: &Path, operation: &'static str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => Err(invariant(format!(
            "checkpoint publication target {} already exists",
            path.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(io_error(operation, path, source)),
    }
}

fn optional_symlink_metadata(path: &Path, operation: &'static str) -> Result<Option<fs::Metadata>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(io_error(operation, path, source)),
    }
}

fn symlink_metadata(path: &Path, operation: &'static str) -> Result<fs::Metadata> {
    fs::symlink_metadata(path).map_err(|source| io_error(operation, path, source))
}

fn create_directories(path: &Path, operation: &'static str) -> Result<()> {
    fs::create_dir_all(path).map_err(|source| io_error(operation, path, source))
}

fn create_directory(path: &Path, operation: &'static str) -> Result<()> {
    fs::create_dir(path).map_err(|source| io_error(operation, path, source))
}

fn open_file(path: &Path, operation: &'static str) -> Result<File> {
    File::open(path).map_err(|source| io_error(operation, path, source))
}

fn open_new_file(path: &Path, operation: &'static str) -> Result<File> {
    fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|source| io_error(operation, path, source))
}

fn write_all(file: &mut File, path: &Path, bytes: &[u8]) -> Result<()> {
    file.write_all(bytes)
        .map_err(|source| io_error("write file", path, source))
}

fn sync_open_file(file: &File, path: &Path) -> Result<()> {
    file.sync_all()
        .map_err(|source| io_error("sync file", path, source))
}

fn write_json_new<T: serde::Serialize>(path: &Path, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value).map_err(|source| CheckpointStoreError::Json {
        path: path.to_path_buf(),
        source,
    })?;
    let mut file = open_new_file(path, "create checkpoint metadata")?;
    write_all(&mut file, path, &bytes)?;
    write_all(&mut file, path, b"\n")?;
    sync_open_file(&file, path)
}

fn read_file(path: &Path, operation: &'static str) -> Result<Vec<u8>> {
    fs::read(path).map_err(|source| io_error(operation, path, source))
}

fn read_directory(path: &Path, operation: &'static str) -> Result<fs::ReadDir> {
    fs::read_dir(path).map_err(|source| io_error(operation, path, source))
}

fn canonicalize(path: &Path, operation: &'static str) -> Result<PathBuf> {
    fs::canonicalize(path).map_err(|source| io_error(operation, path, source))
}

fn rename_path(source: &Path, target: &Path, operation: &'static str) -> Result<()> {
    fs::rename(source, target).map_err(|error| {
        io_error(
            operation,
            PathBuf::from(format!("{} -> {}", source_path(source), target.display())),
            error,
        )
    })
}

fn source_path(path: &Path) -> String {
    path.display().to_string()
}

fn sync_file(path: &Path) -> Result<()> {
    let file = open_file(path, "open file for sync")?;
    sync_open_file(&file, path)
}

fn sync_directory(path: &Path) -> Result<()> {
    let directory = open_file(path, "open directory for sync")?;
    directory
        .sync_all()
        .map_err(|source| io_error("sync directory", path, source))
}

fn remove_directory(path: &Path, operation: &'static str) -> Result<()> {
    fs::remove_dir_all(path).map_err(|source| io_error(operation, path, source))
}

fn remove_file(path: &Path, operation: &'static str) -> Result<()> {
    fs::remove_file(path).map_err(|source| io_error(operation, path, source))
}

fn remove_file_if_exists(path: &Path, operation: &'static str) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(io_error(operation, path, source)),
    }
}

fn io_error(
    operation: &'static str,
    path: impl AsRef<Path>,
    source: std::io::Error,
) -> CheckpointStoreError {
    CheckpointStoreError::Io {
        operation,
        path: path.as_ref().to_path_buf(),
        source,
    }
}

fn invariant(message: impl Into<String>) -> CheckpointStoreError {
    CheckpointStoreError::Invariant(message.into())
}

fn checkpoint_store_failpoint(name: &'static str, path: &Path) -> Result<()> {
    crate::failpoint::storage(name).map_err(|error| {
        io_error(
            "run checkpoint store failpoint",
            path,
            std::io::Error::other(error.to_string()),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use blaze_core::backend::{BackendKind, SnapshotKind};

    fn commit_input(parent: Option<String>) -> CommitCheckpoint {
        CommitCheckpoint {
            parent,
            policy_name: "default".to_string(),
            image_digest: "sha256:test".to_string(),
            backend: BackendKind::Mock,
            backend_version: Some("mock-v1".to_string()),
            snapshot_kind: SnapshotKind::Full,
        }
    }

    fn populate(stage: &CheckpointStage, suffix: &str) {
        for name in REQUIRED_ARTIFACTS {
            fs::write(stage.path.join(name), format!("{name}-{suffix}")).expect("write artifact");
        }
    }

    fn publish(
        store: &CheckpointStore,
        sandbox_id: Uuid,
        parent: Option<String>,
        move_head: bool,
    ) -> String {
        let stage = store.begin(sandbox_id).expect("begin checkpoint");
        let id = stage.id().to_string();
        populate(&stage, &id);
        store
            .publish(&stage, commit_input(parent))
            .expect("publish checkpoint");
        if move_head {
            store.set_head(sandbox_id, &id).expect("move HEAD");
        }
        id
    }

    #[test]
    fn publish_verify_and_list_preserve_the_head_boundary() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = CheckpointStore::new(temp.path().join("checkpoints"));
        let sandbox_id = Uuid::new_v4();
        let root = publish(&store, sandbox_id, None, true);
        let unreachable = publish(&store, sandbox_id, Some(root.clone()), false);

        assert_eq!(store.read_head(sandbox_id).expect("HEAD"), Some(root));
        store
            .verify(sandbox_id, &unreachable)
            .expect("published checkpoint");
        let listed = store.list(sandbox_id).expect("list checkpoints");
        assert_eq!(listed.len(), 2);
        assert_eq!(listed.iter().filter(|info| info.is_head).count(), 1);
        assert!(
            listed
                .iter()
                .any(|info| info.id == unreachable && !info.on_head_chain)
        );
    }

    #[test]
    fn verify_rejects_corrupted_artifact_content() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = CheckpointStore::new(temp.path().join("checkpoints"));
        let sandbox_id = Uuid::new_v4();
        let checkpoint_id = publish(&store, sandbox_id, None, true);
        fs::write(
            store
                .root
                .join(sandbox_id.to_string())
                .join(&checkpoint_id)
                .join("memory.snap"),
            b"corrupt",
        )
        .expect("corrupt artifact");

        assert!(store.verify(sandbox_id, &checkpoint_id).is_err());
    }

    #[test]
    fn publish_rejects_an_unexpected_stage_entry() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = CheckpointStore::new(temp.path().join("checkpoints"));
        let sandbox_id = Uuid::new_v4();
        let stage = store.begin(sandbox_id).expect("begin checkpoint");
        populate(&stage, "candidate");
        fs::write(stage.path.join("unexpected"), b"unexpected").expect("write extra entry");

        let error = store
            .publish(&stage, commit_input(None))
            .expect_err("unexpected entry must fail");
        assert!(error.to_string().contains("unexpected entry"));
        store
            .abort_staging(sandbox_id, stage.id())
            .expect("abort stage");
    }

    #[test]
    fn cleanup_removes_transaction_scratch_but_retains_committed_history() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = CheckpointStore::new(temp.path().join("checkpoints"));
        let sandbox_id = Uuid::new_v4();
        let committed = publish(&store, sandbox_id, None, true);
        let stage = store.begin(sandbox_id).expect("begin checkpoint");
        let sandbox_dir = store.root.join(sandbox_id.to_string());
        let temporary_head = sandbox_dir.join(format!(".HEAD.{}{STAGING_SUFFIX}", Uuid::new_v4()));
        fs::write(&temporary_head, b"temporary").expect("write temporary HEAD");
        let tombstone = tombstone_path(&sandbox_dir, ABORT_TOMBSTONE_PREFIX, &committed);
        fs::create_dir(&tombstone).expect("create abort tombstone");

        let removed = store
            .cleanup_transaction_artifacts(sandbox_id)
            .expect("cleanup scratch");

        assert_eq!(removed.len(), 3);
        assert!(!stage.path.exists());
        assert!(!temporary_head.exists());
        assert!(!tombstone.exists());
        assert!(store.verify(sandbox_id, &committed).is_ok());
        assert_eq!(store.read_head(sandbox_id).expect("HEAD"), Some(committed));
    }

    #[test]
    fn cleanup_recognizes_every_capture_transaction_artifact() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = CheckpointStore::new(temp.path().join("checkpoints"));
        let sandbox_id = Uuid::new_v4();
        let stage = store.begin(sandbox_id).expect("begin checkpoint");
        let sandbox_dir = store.root.join(sandbox_id.to_string());
        let temporary_head = sandbox_dir.join(format!(".HEAD.{}{STAGING_SUFFIX}", Uuid::new_v4()));
        fs::write(&temporary_head, b"temporary").expect("write temporary HEAD");
        let abort_tombstone = tombstone_path(&sandbox_dir, ABORT_TOMBSTONE_PREFIX, stage.id());
        fs::create_dir(&abort_tombstone).expect("create abort tombstone");

        let removed = store
            .cleanup_transaction_artifacts(sandbox_id)
            .expect("cleanup transaction artifacts");
        assert_eq!(removed.len(), 3);
        assert!(!stage.path.exists());
        assert!(!temporary_head.exists());
        assert!(!abort_tombstone.exists());
    }

    #[test]
    fn cleanup_reports_unsafe_layout_without_deleting_it() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = CheckpointStore::new(temp.path().join("checkpoints"));
        let sandbox_id = Uuid::new_v4();
        let stage = store.begin(sandbox_id).expect("begin checkpoint");
        fs::remove_dir(&stage.path).expect("remove staging directory");
        fs::write(&stage.path, b"not a directory").expect("replace stage with file");

        let cleanup_error = store
            .cleanup_transaction_artifacts(sandbox_id)
            .expect_err("cleanup must not delete an entry with the wrong type");
        assert!(cleanup_error.to_string().contains("unexpected file type"));
        assert!(stage.path.is_file());
    }

    #[test]
    fn abort_staging_is_idempotent() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = CheckpointStore::new(temp.path().join("checkpoints"));
        let sandbox_id = Uuid::new_v4();
        let stage = store.begin(sandbox_id).expect("begin checkpoint");
        let checkpoint_id = stage.id().to_string();

        store
            .abort_staging(sandbox_id, &checkpoint_id)
            .expect("abort stage");
        store
            .abort_staging(sandbox_id, &checkpoint_id)
            .expect("repeat abort");
        assert!(!stage.path.exists());
    }

    #[test]
    fn missing_catalog_has_no_head_or_entries() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = CheckpointStore::new(temp.path().join("missing"));
        let sandbox_id = Uuid::new_v4();
        assert_eq!(store.read_head(sandbox_id).expect("HEAD"), None);
        assert!(store.list(sandbox_id).expect("list").is_empty());
        assert!(
            store
                .cleanup_transaction_artifacts(sandbox_id)
                .expect("cleanup")
                .is_empty()
        );
    }

    #[cfg(unix)]
    #[test]
    fn verify_rejects_artifact_symlinks() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("tempdir");
        let store = CheckpointStore::new(temp.path().join("checkpoints"));
        let sandbox_id = Uuid::new_v4();
        let checkpoint_id = publish(&store, sandbox_id, None, true);
        let artifact = store
            .root
            .join(sandbox_id.to_string())
            .join(&checkpoint_id)
            .join("rootfs.snap");
        fs::remove_file(&artifact).expect("remove artifact");
        let outside = temp.path().join("outside");
        fs::write(&outside, b"outside").expect("write outside file");
        symlink(&outside, &artifact).expect("link artifact");

        assert!(store.verify(sandbox_id, &checkpoint_id).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn begin_rejects_a_symlinked_catalog_root() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("tempdir");
        let actual = temp.path().join("actual");
        fs::create_dir(&actual).expect("create actual root");
        let linked = temp.path().join("linked");
        symlink(&actual, &linked).expect("link root");
        let store = CheckpointStore::new(linked);

        assert!(store.begin(Uuid::new_v4()).is_err());
    }

    #[cfg(not(feature = "test-failpoints"))]
    #[test]
    fn production_checkpoint_store_boundary_hooks_are_inert() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = CheckpointStore::new(temp.path().join("checkpoints"));
        let sandbox_id = Uuid::new_v4();
        let root = publish(&store, sandbox_id, None, true);

        assert_eq!(store.read_head(sandbox_id).expect("HEAD"), Some(root));
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn publish_boundary_error_leaves_a_committed_unreachable_checkpoint() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = CheckpointStore::new(temp.path().join("checkpoints"));
        let sandbox_id = Uuid::new_v4();
        let stage = store.begin(sandbox_id).expect("begin checkpoint");
        let checkpoint_id = stage.id().to_string();
        let final_path = stage.final_path.clone();
        populate(&stage, "publish-boundary");
        let hook = crate::failpoint::TestFailpoint::new(&["checkpoint-store-publish-after-rename"]);

        let error = hook
            .run(async { store.publish(&stage, commit_input(None)) })
            .await
            .expect_err("publish boundary must return a store error");

        assert!(
            error
                .to_string()
                .contains("checkpoint-store-publish-after-rename")
        );
        assert!(!stage.path.exists());
        assert!(final_path.is_dir());
        store
            .verify(sandbox_id, &checkpoint_id)
            .expect("renamed checkpoint remains committed");
        assert_eq!(store.read_head(sandbox_id).expect("HEAD"), None);
        assert!(
            store
                .cleanup_transaction_artifacts(sandbox_id)
                .expect("inspect transaction artifacts")
                .is_empty()
        );
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn head_boundary_error_leaves_the_new_head_visible() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = CheckpointStore::new(temp.path().join("checkpoints"));
        let sandbox_id = Uuid::new_v4();
        let checkpoint_id = publish(&store, sandbox_id, None, false);
        let hook = crate::failpoint::TestFailpoint::new(&["checkpoint-store-head-after-rename"]);

        let error = hook
            .run(async { store.set_head(sandbox_id, &checkpoint_id) })
            .await
            .expect_err("HEAD boundary must return a store error");

        assert!(
            error
                .to_string()
                .contains("checkpoint-store-head-after-rename")
        );
        assert_eq!(
            store.read_head(sandbox_id).expect("HEAD"),
            Some(checkpoint_id)
        );
        assert!(
            store
                .cleanup_transaction_artifacts(sandbox_id)
                .expect("inspect transaction artifacts")
                .is_empty()
        );
    }
}
