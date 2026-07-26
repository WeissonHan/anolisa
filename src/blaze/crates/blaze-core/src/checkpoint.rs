// SPDX-License-Identifier: Apache-2.0
//! Crash-safe checkpoint metadata, integrity validation, lineage, and pruning.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::backend::{BackendKind, SnapshotKind};
use crate::error::{BlazeError, Result};

/// Current on-disk checkpoint metadata format.
pub const CHECKPOINT_FORMAT_VERSION: u32 = 1;

/// Names of artifacts required for every committed checkpoint.
pub const REQUIRED_ARTIFACTS: [&str; 3] = ["vmstate.snap", "mem.diff", "rootfs.diff"];

/// One content-addressed file recorded in checkpoint metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointArtifact {
    /// File name relative to the checkpoint directory.
    pub name: String,
    /// Logical file size in bytes.
    pub size_bytes: u64,
    /// Lowercase SHA-256 digest.
    pub sha256: String,
}

/// Durable checkpoint identity and integrity manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointMetadata {
    /// Metadata schema version.
    pub format_version: u32,
    /// Stable `ckpt-<uuid>` identifier.
    pub id: String,
    /// Previous checkpoint on this branch.
    #[serde(default)]
    pub parent: Option<String>,
    /// Sandbox that owns the checkpoint.
    pub sandbox_id: Uuid,
    /// User-visible template name, if present.
    #[serde(default)]
    pub template_name: String,
    /// Image identity selected by policy.
    pub image_digest: String,
    /// Backend that produced the VM state.
    pub backend: BackendKind,
    /// Firecracker version captured by the target environment.
    #[serde(default)]
    pub backend_version: Option<String>,
    /// UTC commit time.
    pub created_at: DateTime<Utc>,
    /// Full or diff snapshot semantics.
    pub snapshot_kind: SnapshotKind,
    /// Integrity records for all required artifacts.
    pub artifacts: Vec<CheckpointArtifact>,
}

/// Read-only API view of a checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointInfo {
    /// Checkpoint identifier.
    pub id: String,
    /// Parent checkpoint.
    pub parent: Option<String>,
    /// Commit time.
    pub created_at: DateTime<Utc>,
    /// Sum of logical artifact sizes.
    pub size_bytes: u64,
    /// Whether this checkpoint is the current HEAD.
    pub is_head: bool,
    /// Whether this checkpoint is reachable from HEAD.
    pub on_head_chain: bool,
}

/// Temporary checkpoint directory populated before an atomic commit.
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

    /// Temporary directory in which the backend writes artifacts.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Path for one artifact inside the staging directory.
    pub fn artifact_path(&self, name: &str) -> Result<PathBuf> {
        if !REQUIRED_ARTIFACTS.contains(&name) {
            return Err(checkpoint_error(format!(
                "artifact name {name:?} is not part of the frozen format"
            )));
        }
        Ok(self.path.join(name))
    }
}

/// Values needed to finalize a staged checkpoint.
#[derive(Debug, Clone)]
pub struct CommitCheckpoint {
    /// Parent checkpoint, if this branch already has a HEAD.
    pub parent: Option<String>,
    /// User-visible template name.
    pub template_name: String,
    /// Image identity selected by policy.
    pub image_digest: String,
    /// Backend that produced the artifacts.
    pub backend: BackendKind,
    /// Backend version captured by the caller.
    pub backend_version: Option<String>,
    /// Snapshot semantics.
    pub snapshot_kind: SnapshotKind,
}

/// Filesystem-backed checkpoint chain.
#[derive(Debug, Clone)]
pub struct CheckpointStore {
    root: PathBuf,
}

impl CheckpointStore {
    /// Create a store rooted at `root`.
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    /// Return the configured root.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Create a unique temporary directory for one checkpoint transaction.
    pub fn begin(&self, sandbox_id: Uuid) -> Result<CheckpointStage> {
        let id = format!("ckpt-{}", Uuid::new_v4());
        let sandbox_dir = self.sandbox_dir(sandbox_id);
        fs::create_dir_all(&sandbox_dir)?;
        let path = sandbox_dir.join(format!(".{id}.tmp"));
        fs::create_dir(&path).map_err(|source| BlazeError::CheckpointError {
            msg: format!("create checkpoint staging dir {}: {source}", path.display()),
        })?;
        let final_path = sandbox_dir.join(&id);
        Ok(CheckpointStage {
            id,
            sandbox_id,
            path,
            final_path,
        })
    }

    /// Hash, fsync, and atomically publish a populated checkpoint stage.
    pub fn commit(
        &self,
        stage: CheckpointStage,
        input: CommitCheckpoint,
    ) -> Result<CheckpointMetadata> {
        let outcome = self.publish(&stage, input);
        let metadata = match outcome {
            Ok(metadata) => metadata,
            Err(error) => {
                let _ = self.abort(stage);
                return Err(error);
            }
        };
        self.set_head(stage.sandbox_id, &stage.id)?;
        Ok(metadata)
    }

    /// Hash, fsync, and atomically publish a checkpoint without moving HEAD.
    ///
    /// Keeping publication separate from [`Self::set_head`] gives the daemon
    /// an explicit crash boundary: a published but unreachable checkpoint is
    /// safe to diagnose and later prune.
    pub fn publish(
        &self,
        stage: &CheckpointStage,
        input: CommitCheckpoint,
    ) -> Result<CheckpointMetadata> {
        if input.backend == BackendKind::Firecracker
            && input
                .backend_version
                .as_deref()
                .map(str::trim)
                .filter(|version| !version.is_empty())
                .is_none()
        {
            return Err(checkpoint_error(
                "Firecracker checkpoint metadata requires a backend version",
            ));
        }
        if let Some(parent) = &input.parent {
            self.validate_chain_from(stage.sandbox_id, parent)?;
        }
        let mut artifacts = Vec::with_capacity(REQUIRED_ARTIFACTS.len());
        for name in REQUIRED_ARTIFACTS {
            let path = stage.path.join(name);
            sync_file(&path)?;
            artifacts.push(hash_artifact(&path, name)?);
        }

        let metadata = CheckpointMetadata {
            format_version: CHECKPOINT_FORMAT_VERSION,
            id: stage.id.clone(),
            parent: input.parent,
            sandbox_id: stage.sandbox_id,
            template_name: input.template_name,
            image_digest: input.image_digest,
            backend: input.backend,
            backend_version: input.backend_version,
            created_at: Utc::now(),
            snapshot_kind: input.snapshot_kind,
            artifacts,
        };
        let metadata_path = stage.path.join("metadata.json");
        write_json_sync(&metadata_path, &metadata)?;
        sync_dir(&stage.path)?;
        fs::rename(&stage.path, &stage.final_path).map_err(|source| {
            BlazeError::CheckpointError {
                msg: format!(
                    "publish checkpoint {} -> {}: {source}",
                    stage.path.display(),
                    stage.final_path.display()
                ),
            }
        })?;
        sync_dir(
            stage
                .final_path
                .parent()
                .ok_or_else(|| checkpoint_error("checkpoint final path has no parent"))?,
        )?;
        Ok(metadata)
    }

    /// Delete an unpublished staging directory after a failed transaction.
    pub fn abort(&self, stage: CheckpointStage) -> Result<()> {
        self.abort_staging(stage.sandbox_id, &stage.id)
    }

    /// Delete one unpublished staging directory by its frozen identifier.
    pub fn abort_staging(&self, sandbox_id: Uuid, checkpoint_id: &str) -> Result<()> {
        validate_checkpoint_id(checkpoint_id)?;
        let path = self
            .sandbox_dir(sandbox_id)
            .join(format!(".{checkpoint_id}.tmp"));
        if remove_dir_all_if_exists(&path)? {
            sync_dir(
                path.parent()
                    .ok_or_else(|| checkpoint_error("checkpoint staging path has no parent"))?,
            )?;
        }
        Ok(())
    }

    /// Remove only transaction scratch after an explicit sandbox destroy.
    ///
    /// Committed checkpoint directories and HEAD remain durable history.
    pub fn cleanup_transaction_artifacts(&self, sandbox_id: Uuid) -> Result<Vec<PathBuf>> {
        let dir = self.sandbox_dir(sandbox_id);
        let mut removed = Vec::new();
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(removed),
            Err(error) => return Err(error.into()),
        };
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let path = entry.path();
            let checkpoint_staging = name.starts_with(".ckpt-") && name.ends_with(".tmp");
            let head_staging = name.starts_with(".HEAD.") && name.ends_with(".tmp");
            if checkpoint_staging && entry.file_type()?.is_dir() {
                fs::remove_dir_all(&path)?;
                removed.push(path);
            } else if head_staging && entry.file_type()?.is_file() {
                fs::remove_file(&path)?;
                removed.push(path);
            }
        }
        if !removed.is_empty() {
            sync_dir(&dir)?;
        }
        removed.sort();
        Ok(removed)
    }

    /// Read and validate one checkpoint and all artifact hashes.
    pub fn verify(&self, sandbox_id: Uuid, checkpoint_id: &str) -> Result<CheckpointMetadata> {
        validate_checkpoint_id(checkpoint_id)?;
        let dir = self.sandbox_dir(sandbox_id).join(checkpoint_id);
        let metadata: CheckpointMetadata =
            serde_json::from_slice(&fs::read(dir.join("metadata.json"))?)?;
        if metadata.format_version != CHECKPOINT_FORMAT_VERSION {
            return Err(checkpoint_error(format!(
                "checkpoint {checkpoint_id} has unsupported format {}",
                metadata.format_version
            )));
        }
        if metadata.id != checkpoint_id || metadata.sandbox_id != sandbox_id {
            return Err(checkpoint_error(format!(
                "checkpoint {checkpoint_id} identity does not match its path"
            )));
        }
        if let Some(parent) = &metadata.parent {
            validate_checkpoint_id(parent)?;
        }
        if metadata
            .backend_version
            .as_deref()
            .is_some_and(|version| version.trim().is_empty())
            || (metadata.backend == BackendKind::Firecracker && metadata.backend_version.is_none())
        {
            return Err(checkpoint_error(format!(
                "checkpoint {checkpoint_id} has an invalid backend version"
            )));
        }
        let artifact_names = metadata
            .artifacts
            .iter()
            .map(|artifact| artifact.name.as_str())
            .collect::<HashSet<_>>();
        if metadata.artifacts.len() != REQUIRED_ARTIFACTS.len()
            || artifact_names.len() != REQUIRED_ARTIFACTS.len()
            || REQUIRED_ARTIFACTS
                .iter()
                .any(|required| !artifact_names.contains(required))
        {
            return Err(checkpoint_error(format!(
                "checkpoint {checkpoint_id} has an invalid artifact manifest"
            )));
        }
        for required in REQUIRED_ARTIFACTS {
            let artifact = metadata
                .artifacts
                .iter()
                .find(|artifact| artifact.name == required)
                .ok_or_else(|| {
                    checkpoint_error(format!(
                        "checkpoint {checkpoint_id} is missing metadata for {required}"
                    ))
                })?;
            let actual = hash_artifact(&dir.join(required), required)?;
            if &actual != artifact {
                return Err(checkpoint_error(format!(
                    "checkpoint {checkpoint_id} artifact {required} failed integrity validation"
                )));
            }
        }
        Ok(metadata)
    }

    /// Validate the current HEAD chain and return IDs from HEAD to root.
    pub fn validate_head_chain(&self, sandbox_id: Uuid) -> Result<Vec<String>> {
        let Some(current) = self.read_head(sandbox_id)? else {
            return Ok(Vec::new());
        };
        self.validate_chain_from(sandbox_id, &current)
    }

    /// Validate one checkpoint and its complete parent chain.
    pub fn validate_chain_from(
        &self,
        sandbox_id: Uuid,
        checkpoint_id: &str,
    ) -> Result<Vec<String>> {
        validate_checkpoint_id(checkpoint_id)?;
        let mut current = checkpoint_id.to_string();
        let mut chain = Vec::new();
        let mut seen = HashSet::new();
        loop {
            if !seen.insert(current.clone()) {
                return Err(checkpoint_error(format!(
                    "checkpoint parent cycle at {current}"
                )));
            }
            let metadata = self.verify(sandbox_id, &current)?;
            chain.push(current);
            let Some(parent) = metadata.parent else {
                break;
            };
            current = parent;
        }
        Ok(chain)
    }

    /// Resolve one frozen artifact after validating its name and identifier.
    pub fn artifact_path(
        &self,
        sandbox_id: Uuid,
        checkpoint_id: &str,
        name: &str,
    ) -> Result<PathBuf> {
        validate_checkpoint_id(checkpoint_id)?;
        if !REQUIRED_ARTIFACTS.contains(&name) {
            return Err(checkpoint_error(format!(
                "artifact name {name:?} is not part of the frozen format"
            )));
        }
        Ok(self.sandbox_dir(sandbox_id).join(checkpoint_id).join(name))
    }

    /// List all committed checkpoints, marking HEAD reachability.
    pub fn list(&self, sandbox_id: Uuid) -> Result<Vec<CheckpointInfo>> {
        let dir = self.sandbox_dir(sandbox_id);
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let head = self.read_head(sandbox_id)?;
        let mut committed = HashMap::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let Some(id) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if validate_checkpoint_id(&id).is_err() {
                continue;
            }
            let metadata = self.verify(sandbox_id, &id)?;
            committed.insert(id, metadata);
        }
        let mut on_chain = HashSet::new();
        if let Some(mut current) = head.clone() {
            loop {
                if !on_chain.insert(current.clone()) {
                    return Err(checkpoint_error(format!(
                        "checkpoint parent cycle at {current}"
                    )));
                }
                let metadata = committed.get(&current).ok_or_else(|| {
                    checkpoint_error(format!(
                        "HEAD chain references missing checkpoint {current}"
                    ))
                })?;
                let Some(parent) = &metadata.parent else {
                    break;
                };
                current = parent.clone();
            }
        }
        let mut result = Vec::with_capacity(committed.len());
        for (id, metadata) in committed {
            result.push(CheckpointInfo {
                id: id.clone(),
                parent: metadata.parent,
                created_at: metadata.created_at,
                size_bytes: metadata
                    .artifacts
                    .iter()
                    .map(|artifact| artifact.size_bytes)
                    .sum(),
                is_head: head.as_deref() == Some(id.as_str()),
                on_head_chain: on_chain.contains(&id),
            });
        }
        result.sort_by_key(|info| info.created_at);
        Ok(result)
    }

    /// Delete committed checkpoints that are not reachable from HEAD.
    pub fn prune(&self, sandbox_id: Uuid) -> Result<Vec<String>> {
        let keep: HashSet<String> = self.validate_head_chain(sandbox_id)?.into_iter().collect();
        if keep.is_empty() {
            return Err(checkpoint_error(
                "no HEAD chain exists; refusing to prune every checkpoint",
            ));
        }
        let dir = self.sandbox_dir(sandbox_id);
        let mut candidates = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let Some(id) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if validate_checkpoint_id(&id).is_err() || keep.contains(&id) {
                continue;
            }
            self.verify(sandbox_id, &id)?;
            candidates.push((id, entry.path()));
        }
        candidates.sort_by(|left, right| left.0.cmp(&right.0));
        let mut removed = Vec::with_capacity(candidates.len());
        for (id, path) in candidates {
            fs::remove_dir_all(path)?;
            removed.push(id);
        }
        sync_dir(&dir)?;
        Ok(removed)
    }

    /// Atomically update HEAD after the referenced checkpoint is committed.
    pub fn set_head(&self, sandbox_id: Uuid, checkpoint_id: &str) -> Result<()> {
        validate_checkpoint_id(checkpoint_id)?;
        let dir = self.sandbox_dir(sandbox_id);
        if !dir.join(checkpoint_id).is_dir() {
            return Err(checkpoint_error(format!(
                "cannot set HEAD to missing checkpoint {checkpoint_id}"
            )));
        }
        fs::create_dir_all(&dir)?;
        let tmp = dir.join(format!(".HEAD.{}.tmp", Uuid::new_v4()));
        let outcome = (|| {
            {
                let mut file = File::create(&tmp)?;
                file.write_all(checkpoint_id.as_bytes())?;
                file.write_all(b"\n")?;
                file.sync_all()?;
            }
            fs::rename(&tmp, dir.join("HEAD"))?;
            sync_dir(&dir)?;
            Ok(())
        })();
        if outcome.is_err() && tmp.exists() {
            let _ = fs::remove_file(tmp);
        }
        outcome
    }

    /// Return the persisted HEAD, if present.
    pub fn read_head(&self, sandbox_id: Uuid) -> Result<Option<String>> {
        let path = self.sandbox_dir(sandbox_id).join("HEAD");
        let raw = match fs::read_to_string(path) {
            Ok(raw) => raw,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let id = raw.trim().to_string();
        validate_checkpoint_id(&id)?;
        Ok(Some(id))
    }

    fn sandbox_dir(&self, sandbox_id: Uuid) -> PathBuf {
        self.root.join(sandbox_id.to_string())
    }
}

/// Validate the frozen `ckpt-<uuid>` identifier format.
pub fn validate_checkpoint_id(checkpoint_id: &str) -> Result<Uuid> {
    let raw = checkpoint_id
        .strip_prefix("ckpt-")
        .ok_or_else(|| checkpoint_error(format!("invalid checkpoint id {checkpoint_id:?}")))?;
    if raw.contains('/') || raw.contains('\\') {
        return Err(checkpoint_error(format!(
            "invalid checkpoint id {checkpoint_id:?}"
        )));
    }
    Uuid::parse_str(raw).map_err(|error| {
        checkpoint_error(format!("invalid checkpoint id {checkpoint_id:?}: {error}"))
    })
}

fn hash_artifact(path: &Path, name: &str) -> Result<CheckpointArtifact> {
    let mut file = File::open(path).map_err(|source| BlazeError::CheckpointError {
        msg: format!("open checkpoint artifact {}: {source}", path.display()),
    })?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 128 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let size_bytes = file.metadata()?.len();
    Ok(CheckpointArtifact {
        name: name.to_string(),
        size_bytes,
        sha256: format!("{:x}", hasher.finalize()),
    })
}

fn write_json_sync<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value)?;
    let mut file = File::create(path)?;
    file.write_all(&bytes)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn sync_file(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn remove_dir_all_if_exists(path: &Path) -> Result<bool> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn checkpoint_error(msg: impl Into<String>) -> BlazeError {
    BlazeError::CheckpointError { msg: msg.into() }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn commit_input(parent: Option<String>) -> CommitCheckpoint {
        CommitCheckpoint {
            parent,
            template_name: "base".into(),
            image_digest: "sha256:test".into(),
            backend: BackendKind::Mock,
            backend_version: Some("mock-test".into()),
            snapshot_kind: SnapshotKind::Diff,
        }
    }

    fn populate(stage: &CheckpointStage, suffix: &str) {
        for artifact in REQUIRED_ARTIFACTS {
            fs::write(stage.path().join(artifact), format!("{artifact}-{suffix}")).expect("write");
        }
    }

    fn commit(store: &CheckpointStore, sandbox_id: Uuid, parent: Option<String>) -> String {
        let stage = store.begin(sandbox_id).expect("begin");
        populate(&stage, stage.id());
        let id = stage.id().to_string();
        store.commit(stage, commit_input(parent)).expect("commit");
        id
    }

    #[test]
    fn commit_verify_and_list() {
        let temp = tempfile::tempdir().expect("temp");
        let store = CheckpointStore::new(temp.path().to_path_buf());
        let sandbox_id = Uuid::new_v4();
        let first = commit(&store, sandbox_id, None);
        let second = commit(&store, sandbox_id, Some(first.clone()));

        assert_eq!(store.read_head(sandbox_id).expect("head"), Some(second));
        assert_eq!(
            store.validate_head_chain(sandbox_id).expect("chain").len(),
            2
        );
        let listed = store.list(sandbox_id).expect("list");
        assert_eq!(listed.len(), 2);
        assert_eq!(listed.iter().filter(|info| info.is_head).count(), 1);
    }

    #[test]
    fn corruption_is_rejected() {
        let temp = tempfile::tempdir().expect("temp");
        let store = CheckpointStore::new(temp.path().to_path_buf());
        let sandbox_id = Uuid::new_v4();
        let id = commit(&store, sandbox_id, None);
        fs::write(
            temp.path()
                .join(sandbox_id.to_string())
                .join(&id)
                .join("mem.diff"),
            b"corrupt",
        )
        .expect("corrupt");
        assert!(store.verify(sandbox_id, &id).is_err());
    }

    #[test]
    fn prune_removes_abandoned_branch() {
        let temp = tempfile::tempdir().expect("temp");
        let store = CheckpointStore::new(temp.path().to_path_buf());
        let sandbox_id = Uuid::new_v4();
        let root = commit(&store, sandbox_id, None);
        let abandoned = commit(&store, sandbox_id, Some(root.clone()));
        store.set_head(sandbox_id, &root).expect("rewind head");
        let kept = commit(&store, sandbox_id, Some(root));
        let removed = store.prune(sandbox_id).expect("prune");
        assert_eq!(removed, vec![abandoned]);
        assert!(store.verify(sandbox_id, &kept).is_ok());
    }

    #[test]
    fn publish_is_durable_before_head_moves() {
        let temp = tempfile::tempdir().expect("temp");
        let store = CheckpointStore::new(temp.path().to_path_buf());
        let sandbox_id = Uuid::new_v4();
        let stage = store.begin(sandbox_id).expect("begin");
        populate(&stage, "published");
        let id = stage.id().to_string();

        let metadata = store.publish(&stage, commit_input(None)).expect("publish");

        assert_eq!(metadata.id, id);
        assert_eq!(metadata.backend_version.as_deref(), Some("mock-test"));
        assert_eq!(store.read_head(sandbox_id).expect("read head"), None);
        assert_eq!(
            store.validate_chain_from(sandbox_id, &id).expect("chain"),
            vec![id.clone()]
        );
        store.set_head(sandbox_id, &id).expect("set head");
        assert_eq!(store.read_head(sandbox_id).expect("read head"), Some(id));
    }

    #[test]
    fn complete_parent_chain_is_validated() {
        let temp = tempfile::tempdir().expect("temp");
        let store = CheckpointStore::new(temp.path().to_path_buf());
        let sandbox_id = Uuid::new_v4();
        let root = commit(&store, sandbox_id, None);
        let middle = commit(&store, sandbox_id, Some(root.clone()));
        let leaf = commit(&store, sandbox_id, Some(middle));

        fs::write(
            temp.path()
                .join(sandbox_id.to_string())
                .join(&root)
                .join("mem.diff"),
            b"corrupt-root",
        )
        .expect("corrupt root");

        assert!(store.validate_chain_from(sandbox_id, &leaf).is_err());
        let stage = store.begin(sandbox_id).expect("begin");
        populate(&stage, "new-leaf");
        assert!(store.publish(&stage, commit_input(Some(leaf))).is_err());
        store.abort(stage).expect("abort");
    }

    #[test]
    fn parent_cycle_is_rejected() {
        let temp = tempfile::tempdir().expect("temp");
        let store = CheckpointStore::new(temp.path().to_path_buf());
        let sandbox_id = Uuid::new_v4();
        let root = commit(&store, sandbox_id, None);
        let metadata_path = temp
            .path()
            .join(sandbox_id.to_string())
            .join(&root)
            .join("metadata.json");
        let mut metadata: CheckpointMetadata =
            serde_json::from_slice(&fs::read(&metadata_path).expect("read metadata"))
                .expect("parse metadata");
        metadata.parent = Some(root.clone());
        write_json_sync(&metadata_path, &metadata).expect("write metadata");

        let error = store
            .validate_chain_from(sandbox_id, &root)
            .expect_err("cycle must fail");
        assert!(error.to_string().contains("parent cycle"));
    }

    #[test]
    fn non_exact_artifact_manifest_is_rejected() {
        let temp = tempfile::tempdir().expect("temp");
        let store = CheckpointStore::new(temp.path().to_path_buf());
        let sandbox_id = Uuid::new_v4();
        let id = commit(&store, sandbox_id, None);
        let metadata_path = temp
            .path()
            .join(sandbox_id.to_string())
            .join(&id)
            .join("metadata.json");
        let mut metadata: CheckpointMetadata =
            serde_json::from_slice(&fs::read(&metadata_path).expect("read metadata"))
                .expect("parse metadata");
        metadata.artifacts.push(metadata.artifacts[0].clone());
        write_json_sync(&metadata_path, &metadata).expect("write metadata");

        let error = store
            .verify(sandbox_id, &id)
            .expect_err("duplicate manifest entry must fail");
        assert!(error.to_string().contains("invalid artifact manifest"));
    }

    #[test]
    fn missing_head_target_preserves_previous_head() {
        let temp = tempfile::tempdir().expect("temp");
        let store = CheckpointStore::new(temp.path().to_path_buf());
        let sandbox_id = Uuid::new_v4();
        let root = commit(&store, sandbox_id, None);
        let missing = format!("ckpt-{}", Uuid::new_v4());

        assert!(store.set_head(sandbox_id, &missing).is_err());
        assert_eq!(store.read_head(sandbox_id).expect("read head"), Some(root));
    }

    #[test]
    fn transaction_cleanup_preserves_committed_history() {
        let temp = tempfile::tempdir().expect("temp");
        let store = CheckpointStore::new(temp.path().to_path_buf());
        let sandbox_id = Uuid::new_v4();
        let committed = commit(&store, sandbox_id, None);
        let stage = store.begin(sandbox_id).expect("begin");
        populate(&stage, "staging");
        let sandbox_dir = temp.path().join(sandbox_id.to_string());
        let head_tmp = sandbox_dir.join(format!(".HEAD.{}.tmp", Uuid::new_v4()));
        fs::write(&head_tmp, b"partial").expect("write head temp");
        let unrelated = sandbox_dir.join(".operator-note.tmp");
        fs::write(&unrelated, b"keep").expect("write unrelated temp");

        let removed = store
            .cleanup_transaction_artifacts(sandbox_id)
            .expect("cleanup transactions");

        assert_eq!(removed.len(), 2);
        assert!(!stage.path().exists());
        assert!(!head_tmp.exists());
        assert!(unrelated.exists());
        assert!(store.verify(sandbox_id, &committed).is_ok());
        assert_eq!(
            store.read_head(sandbox_id).expect("read head"),
            Some(committed)
        );
    }

    #[test]
    fn prune_preflights_all_unreachable_checkpoints() {
        let temp = tempfile::tempdir().expect("temp");
        let store = CheckpointStore::new(temp.path().to_path_buf());
        let sandbox_id = Uuid::new_v4();
        let root = commit(&store, sandbox_id, None);
        let corrupt = commit(&store, sandbox_id, Some(root.clone()));
        store.set_head(sandbox_id, &root).expect("rewind head");
        let valid = commit(&store, sandbox_id, Some(root.clone()));
        store.set_head(sandbox_id, &root).expect("rewind head");
        fs::write(
            temp.path()
                .join(sandbox_id.to_string())
                .join(&corrupt)
                .join("mem.diff"),
            b"corrupt",
        )
        .expect("corrupt unreachable checkpoint");

        assert!(store.prune(sandbox_id).is_err());
        assert!(
            temp.path()
                .join(sandbox_id.to_string())
                .join(corrupt)
                .exists()
        );
        assert!(store.verify(sandbox_id, &valid).is_ok());
    }

    #[test]
    fn invalid_parent_identifier_is_rejected() {
        let temp = tempfile::tempdir().expect("temp");
        let store = CheckpointStore::new(temp.path().to_path_buf());
        let sandbox_id = Uuid::new_v4();
        let id = commit(&store, sandbox_id, None);
        let metadata_path = temp
            .path()
            .join(sandbox_id.to_string())
            .join(&id)
            .join("metadata.json");
        let mut metadata: CheckpointMetadata =
            serde_json::from_slice(&fs::read(&metadata_path).expect("read metadata"))
                .expect("parse metadata");
        metadata.parent = Some("../outside".into());
        write_json_sync(&metadata_path, &metadata).expect("write metadata");

        assert!(store.verify(sandbox_id, &id).is_err());
    }

    #[test]
    fn firecracker_checkpoint_requires_backend_version() {
        let temp = tempfile::tempdir().expect("temp");
        let store = CheckpointStore::new(temp.path().to_path_buf());
        let sandbox_id = Uuid::new_v4();
        let stage = store.begin(sandbox_id).expect("begin");
        populate(&stage, "firecracker");
        let mut input = commit_input(None);
        input.backend = BackendKind::Firecracker;
        input.backend_version = None;

        let error = store
            .publish(&stage, input)
            .expect_err("missing Firecracker version must fail");
        assert!(error.to_string().contains("requires a backend version"));
        store.abort(stage).expect("abort");
    }
}
