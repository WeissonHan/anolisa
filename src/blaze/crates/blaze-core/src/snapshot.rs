// SPDX-License-Identifier: Apache-2.0
//! Durable snapshot store.
//!
//! A snapshot is a backend-written checkpoint payload plus store-owned
//! metadata. Snapshots deliberately outlive the instance that produced
//! them so a single image can be restored in place, restored repeatedly,
//! or used to hatch new instances after the source is destroyed.
//!
//! Unlike [`crate::template`] this store is persistent: metadata is
//! written with the same tmp-then-rename discipline as
//! [`crate::lifecycle::SandboxInstance::persist`].

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::backend::{BackendKind, SnapshotCompression};
use crate::error::{BlazeError, Result};
use crate::policy::WorkloadClass;

/// Directory under `state_dir` that holds every snapshot payload.
const SNAPSHOTS_DIR: &str = "snapshots";
const META_FILE: &str = "meta.json";

/// Durable write state. A payload is only usable once `Ready` is committed,
/// so a crash mid-checkpoint leaves a reapable `Writing` directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SnapshotStatus {
    Writing,
    Ready,
}

/// Durable description of one checkpoint image.
///
/// `workload_class`, `image_digest` and `policy_name` are what let a
/// snapshot be policy-evaluated afresh after the source instance's
/// `state.json` is gone, which is what makes hatching self-sufficient.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMeta {
    pub id: Uuid,
    pub status: SnapshotStatus,
    /// Backend that wrote the payload; only this kind can restore it.
    pub backend: BackendKind,
    /// Provenance only — the snapshot stays valid after this instance dies.
    pub source_instance: Uuid,
    pub workload_class: WorkloadClass,
    pub image_digest: String,
    /// Image reference the source rootfs came from. Hatching needs it because
    /// the new instance's run directory has no filesystem yet.
    #[serde(default)]
    pub image: Option<String>,
    pub policy_name: String,
    /// False when the source was hibernated into this image.
    pub left_running: bool,
    pub compression: SnapshotCompression,
    pub size_bytes: u64,
    pub created_at: DateTime<Utc>,
    /// Instances restored or hatched from this image, oldest first.
    #[serde(default)]
    pub restored_by: Vec<Uuid>,
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

impl SnapshotMeta {
    /// Build reservation metadata for a checkpoint that is about to be
    /// written. The caller commits it as [`SnapshotStatus::Ready`] once the
    /// backend reports success.
    pub fn reserving(
        backend: BackendKind,
        source_instance: Uuid,
        workload_class: WorkloadClass,
        image_digest: String,
        policy_name: String,
        compression: SnapshotCompression,
        labels: HashMap<String, String>,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            status: SnapshotStatus::Writing,
            backend,
            source_instance,
            workload_class,
            image_digest,
            image: None,
            policy_name,
            left_running: false,
            compression,
            size_bytes: 0,
            created_at: Utc::now(),
            restored_by: Vec::new(),
            labels,
        }
    }
}

/// Persistent index over `<state_dir>/snapshots`.
#[derive(Debug)]
pub struct SnapshotStore {
    root: PathBuf,
    index: HashMap<Uuid, SnapshotMeta>,
}

impl SnapshotStore {
    /// Open `<state_dir>/snapshots`, loading committed metadata.
    ///
    /// Payloads still marked [`SnapshotStatus::Writing`] are incomplete by
    /// definition and are reaped here. Unreadable entries are skipped with
    /// a warning rather than failing, mirroring instance rehydration.
    ///
    /// # Errors
    /// Returns an error only when the snapshots directory itself cannot be
    /// created or listed.
    pub fn open(state_dir: &Path) -> Result<Self> {
        let root = state_dir.join(SNAPSHOTS_DIR);
        fs::create_dir_all(&root)?;
        let mut index = HashMap::new();
        for entry in fs::read_dir(&root)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let dir = entry.path();
            let Some(id) = entry
                .file_name()
                .to_str()
                .and_then(|name| Uuid::parse_str(name).ok())
            else {
                continue;
            };
            match read_meta(&dir) {
                Ok(meta) if meta.status == SnapshotStatus::Ready => {
                    index.insert(id, meta);
                }
                Ok(_) => {
                    tracing::warn!(snapshot = %id, "reaping incomplete snapshot payload");
                    reap(&dir);
                }
                Err(error) => {
                    tracing::warn!(snapshot = %id, %error, "skipping unreadable snapshot metadata");
                }
            }
        }
        tracing::info!(snapshots = index.len(), dir = %root.display(), "opened snapshot store");
        Ok(Self { root, index })
    }

    /// Empty store used when [`Self::open`] fails, so daemon boot never
    /// depends on the snapshot directory being readable.
    pub fn empty(state_dir: &Path) -> Self {
        Self {
            root: state_dir.join(SNAPSHOTS_DIR),
            index: HashMap::new(),
        }
    }

    /// Payload root for one snapshot.
    pub fn dir(&self, id: Uuid) -> PathBuf {
        self.root.join(id.to_string())
    }

    /// Create the payload directory and commit `Writing` metadata, then
    /// return the directory the backend should write into.
    ///
    /// This is the write-ahead marker: the reservation is durable before
    /// the backend is invoked, so a crash leaves a reapable directory
    /// rather than an orphan payload with no owner.
    ///
    /// # Errors
    /// Returns an error when the directory or metadata cannot be written.
    pub fn reserve(&mut self, meta: SnapshotMeta) -> Result<PathBuf> {
        let dir = self.dir(meta.id);
        fs::create_dir_all(&dir)?;
        write_meta(&dir, &meta)?;
        let id = meta.id;
        self.index.insert(id, meta);
        Ok(dir)
    }

    /// Publish a completed payload and return the stored metadata.
    ///
    /// Returning it keeps callers from reporting their pre-commit copy, whose
    /// status still says [`SnapshotStatus::Writing`].
    ///
    /// # Errors
    /// Returns an error when the metadata cannot be persisted.
    pub fn commit(&mut self, mut meta: SnapshotMeta) -> Result<SnapshotMeta> {
        meta.status = SnapshotStatus::Ready;
        write_meta(&self.dir(meta.id), &meta)?;
        self.index.insert(meta.id, meta.clone());
        Ok(meta)
    }

    /// Metadata for one snapshot, regardless of status.
    pub fn get(&self, id: Uuid) -> Option<SnapshotMeta> {
        self.index.get(&id).cloned()
    }

    /// Every usable snapshot, newest first.
    pub fn list(&self) -> Vec<SnapshotMeta> {
        let mut ready: Vec<SnapshotMeta> = self
            .index
            .values()
            .filter(|meta| meta.status == SnapshotStatus::Ready)
            .cloned()
            .collect();
        ready.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        ready
    }

    /// Record that `instance` was restored or hatched from `id`.
    ///
    /// # Errors
    /// Returns an error when `id` is unknown or metadata cannot be written.
    pub fn record_restore(&mut self, id: Uuid, instance: Uuid) -> Result<()> {
        let meta = self.index.get_mut(&id).ok_or_else(|| unknown(id))?;
        if !meta.restored_by.contains(&instance) {
            meta.restored_by.push(instance);
        }
        write_meta(&self.root.join(id.to_string()), meta)
    }

    /// Drop the index entry and return the payload directory so the caller
    /// can delete it outside any lock — payloads can be gigabytes.
    ///
    /// # Errors
    /// Returns an error when `id` is unknown.
    pub fn forget(&mut self, id: Uuid) -> Result<PathBuf> {
        self.index.remove(&id).ok_or_else(|| unknown(id))?;
        Ok(self.dir(id))
    }
}

fn unknown(id: Uuid) -> BlazeError {
    BlazeError::StorageError {
        msg: format!("unknown snapshot {id}"),
    }
}

fn read_meta(dir: &Path) -> Result<SnapshotMeta> {
    let raw = fs::read(dir.join(META_FILE))?;
    Ok(serde_json::from_slice(&raw)?)
}

fn write_meta(dir: &Path, meta: &SnapshotMeta) -> Result<()> {
    fs::create_dir_all(dir)?;
    let tmp = dir.join("meta.json.tmp");
    fs::write(&tmp, serde_json::to_vec_pretty(meta)?)?;
    fs::rename(&tmp, dir.join(META_FILE))?;
    Ok(())
}

fn reap(dir: &Path) {
    if let Err(error) = fs::remove_dir_all(dir) {
        tracing::warn!(dir = %dir.display(), %error, "failed to reap snapshot payload");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(source: Uuid) -> SnapshotMeta {
        SnapshotMeta::reserving(
            BackendKind::Gvisor,
            source,
            WorkloadClass::AgentTool,
            "sha256:deadbeef".to_string(),
            "agent-tool-default".to_string(),
            SnapshotCompression::None,
            HashMap::new(),
        )
    }

    #[test]
    fn reserve_then_commit_publishes_a_ready_snapshot() {
        let temp = tempfile::tempdir().expect("temp");
        let mut store = SnapshotStore::open(temp.path()).expect("open");
        let mut pending = meta(Uuid::new_v4());
        let id = pending.id;

        let dir = store.reserve(pending.clone()).expect("reserve");
        assert!(dir.join(META_FILE).is_file());
        assert!(store.list().is_empty(), "writing payloads are not usable");

        pending.size_bytes = 4096;
        pending.left_running = true;
        pending.image = Some("docker.io/library/alpine:latest".to_string());
        store.commit(pending).expect("commit");

        let listed = store.list();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, id);
        assert_eq!(listed[0].status, SnapshotStatus::Ready);
        assert_eq!(listed[0].size_bytes, 4096);

        // Hatching reads the image reference back from disk, so it has to
        // survive a reopen rather than only living in the in-memory index.
        let reopened = SnapshotStore::open(temp.path()).expect("reopen");
        assert_eq!(
            reopened.list()[0].image.as_deref(),
            Some("docker.io/library/alpine:latest")
        );
    }

    #[test]
    fn open_reaps_incomplete_payloads_and_keeps_ready_ones() {
        let temp = tempfile::tempdir().expect("temp");
        let (ready_id, writing_dir) = {
            let mut store = SnapshotStore::open(temp.path()).expect("open");
            let ready = meta(Uuid::new_v4());
            let ready_id = ready.id;
            store.reserve(ready.clone()).expect("reserve ready");
            store.commit(ready).expect("commit");

            let writing = meta(Uuid::new_v4());
            let writing_dir = store.reserve(writing).expect("reserve writing");
            (ready_id, writing_dir)
        };

        let reopened = SnapshotStore::open(temp.path()).expect("reopen");
        assert_eq!(reopened.list().len(), 1);
        assert_eq!(reopened.list()[0].id, ready_id);
        assert!(!writing_dir.exists(), "incomplete payload must be reaped");
    }

    #[test]
    fn open_skips_non_uuid_and_metaless_directories() {
        let temp = tempfile::tempdir().expect("temp");
        let root = temp.path().join(SNAPSHOTS_DIR);
        fs::create_dir_all(root.join("not-a-uuid")).expect("mkdir");
        fs::create_dir_all(root.join(Uuid::new_v4().to_string())).expect("mkdir");

        let store = SnapshotStore::open(temp.path()).expect("open");
        assert!(store.list().is_empty());
        assert!(
            root.join("not-a-uuid").exists(),
            "foreign dirs are left alone"
        );
    }

    #[test]
    fn record_restore_appends_and_persists() {
        let temp = tempfile::tempdir().expect("temp");
        let first = Uuid::new_v4();
        let id = {
            let mut store = SnapshotStore::open(temp.path()).expect("open");
            let pending = meta(Uuid::new_v4());
            let id = pending.id;
            store.reserve(pending.clone()).expect("reserve");
            store.commit(pending).expect("commit");
            store.record_restore(id, first).expect("record");
            store
                .record_restore(id, first)
                .expect("idempotent re-record");
            id
        };

        let reopened = SnapshotStore::open(temp.path()).expect("reopen");
        assert_eq!(reopened.get(id).expect("meta").restored_by, vec![first]);
    }

    #[test]
    fn forget_returns_the_payload_dir_and_drops_the_index_entry() {
        let temp = tempfile::tempdir().expect("temp");
        let mut store = SnapshotStore::open(temp.path()).expect("open");
        let pending = meta(Uuid::new_v4());
        let id = pending.id;
        store.reserve(pending.clone()).expect("reserve");
        store.commit(pending).expect("commit");

        let dir = store.forget(id).expect("forget");
        assert_eq!(dir, store.dir(id));
        assert!(store.get(id).is_none());
        assert!(store.forget(id).is_err(), "forgetting twice is an error");
    }
}
