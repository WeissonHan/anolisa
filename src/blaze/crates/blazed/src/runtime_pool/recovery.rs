// SPDX-License-Identifier: Apache-2.0
//! Durable ownership records and startup recovery for unclaimed runtime slots.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::time::Duration;

use blaze_core::backend::BackendKind;
use blaze_core::lifecycle::{
    BackendOwnership, OperationKind, RuntimeLocation, SandboxInstance, SandboxState,
};
use blaze_core::storage::StorageProvider;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

use crate::error::{BlazeDaemonError, Result};
use crate::spawner::SpawnerRegistry;

const OWNERSHIP_FILE: &str = "ownership.json";
const OWNERSHIP_VERSION: u32 = 1;
const CLEANUP_NAMESPACE: &str = ".cleanup";
const DELETION_PROOF_NAMESPACE: &str = ".deletion-proofs";
const STARTUP_RECONCILE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case", tag = "kind")]
pub(super) enum RuntimeSlotPhase {
    Building,
    Ready,
    Handoff { token: Uuid },
    LifecycleOwned { token: Uuid },
    PoolCleanup,
    LifecycleCleanup { token: Uuid },
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RuntimeSlotOwnership {
    pub(super) version: u32,
    pub(super) instance_id: Uuid,
    pub(super) backend: BackendKind,
    pub(super) backend_ownership: BackendOwnership,
    pub(super) storage_owned: bool,
    pub(super) phase: RuntimeSlotPhase,
}

impl RuntimeSlotOwnership {
    pub(super) fn new(instance_id: Uuid, backend: BackendKind) -> Self {
        Self {
            version: OWNERSHIP_VERSION,
            instance_id,
            backend,
            backend_ownership: BackendOwnership::NotStarted,
            storage_owned: false,
            phase: RuntimeSlotPhase::Building,
        }
    }
}

/// Lifecycle identity used to keep transferred warm resources out of cleanup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DurableRuntimeOwner {
    instance_id: Uuid,
    runtime_location: RuntimeLocation,
    backend: BackendKind,
    backend_ownership: BackendOwnership,
    state: SandboxState,
    operation: Option<OperationKind>,
    runtime_owner_token: Option<Uuid>,
    clean_terminal: bool,
}

impl From<&SandboxInstance> for DurableRuntimeOwner {
    fn from(instance: &SandboxInstance) -> Self {
        Self {
            instance_id: instance.id,
            runtime_location: instance.runtime_location,
            backend: instance.backend,
            backend_ownership: instance.backend_ownership,
            state: instance.state,
            operation: instance.operation.as_ref().map(|operation| operation.kind),
            runtime_owner_token: instance.runtime_owner_token,
            clean_terminal: instance.is_clean_terminal(),
        }
    }
}

impl DurableRuntimeOwner {
    fn is_clean_terminal(&self) -> bool {
        self.clean_terminal
    }
}

/// Derive the only accepted backend runtime directory for an owned instance.
pub(crate) fn runtime_dir(
    state_dir: &Path,
    location: RuntimeLocation,
    instance_id: Uuid,
) -> PathBuf {
    match location {
        RuntimeLocation::Sandbox => state_dir.join(instance_id.to_string()),
        RuntimeLocation::WarmPool => state_dir.join("runtime-pool").join(instance_id.to_string()),
    }
}

/// Release runtime slots that were never adopted by durable lifecycle state.
///
/// A provider-only slot is safe to release by stable ID. Once a runtime
/// directory exists, its ownership record is required so restart recovery can
/// select the original backend instead of guessing from current policy.
pub(crate) async fn reconcile_runtime_slots(
    runtime_root: &Path,
    durable_owners: &HashMap<Uuid, DurableRuntimeOwner>,
    storage: &dyn StorageProvider,
    spawners: &SpawnerRegistry,
) -> Result<usize> {
    reconcile_runtime_slots_until(
        tokio::time::Instant::now() + STARTUP_RECONCILE_TIMEOUT,
        runtime_root,
        durable_owners,
        storage,
        spawners,
    )
    .await
}

async fn reconcile_runtime_slots_until(
    deadline: tokio::time::Instant,
    runtime_root: &Path,
    durable_owners: &HashMap<Uuid, DurableRuntimeOwner>,
    storage: &dyn StorageProvider,
    spawners: &SpawnerRegistry,
) -> Result<usize> {
    match tokio::time::timeout_at(
        deadline,
        reconcile_runtime_slots_inner(runtime_root, durable_owners, storage, spawners),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(recovery_error(
            "runtime slot reconciliation exceeded its shared startup deadline".to_string(),
        )),
    }
}

async fn reconcile_runtime_slots_inner(
    runtime_root: &Path,
    durable_owners: &HashMap<Uuid, DurableRuntimeOwner>,
    storage: &dyn StorageProvider,
    spawners: &SpawnerRegistry,
) -> Result<usize> {
    ensure_real_directory(runtime_root, "runtime slot root").await?;
    let cleanup_root = runtime_root.join(CLEANUP_NAMESPACE);
    ensure_real_directory(&cleanup_root, "runtime cleanup namespace").await?;
    let deletion_proof_root = cleanup_root.join(DELETION_PROOF_NAMESPACE);
    ensure_deletion_proof_root(&cleanup_root, &deletion_proof_root).await?;
    restore_deletion_proofs(&cleanup_root, &deletion_proof_root).await?;

    let (runtime_dirs, tombstones, root_errors) =
        scan_runtime_dirs(runtime_root, &cleanup_root, &deletion_proof_root).await?;
    if !root_errors.is_empty() {
        return Err(unresolved_items(root_errors));
    }
    let mut errors =
        validate_durable_owners(runtime_root, durable_owners, &runtime_dirs, &tombstones).await;
    if !errors.is_empty() {
        return Err(unresolved_items(errors));
    }
    let mut provider_ids = BTreeSet::new();
    let mut provider_errors = Vec::new();
    if storage.supports_runtime_pool_recovery() {
        match storage.list_owned_ids().await {
            Ok(ids) => {
                for owned_id in ids {
                    match parse_stable_id(&owned_id) {
                        Ok(id) => {
                            provider_ids.insert(id);
                        }
                        Err(error) => {
                            provider_errors
                                .push(format!("provider-owned slot {owned_id:?}: {error}"));
                        }
                    }
                }
            }
            Err(error) => {
                provider_errors.push(format!("read provider-owned slot inventory: {error}"));
            }
        }
    } else if !runtime_dirs.is_empty() {
        errors.push("storage provider cannot inventory slots for runtime recovery".to_string());
        return Err(unresolved_items(errors));
    }
    if !provider_errors.is_empty() {
        provider_errors.extend(errors);
        return Err(unresolved_items(provider_errors));
    }
    for instance_id in tombstones.keys() {
        if provider_ids.contains(instance_id) {
            errors.push(format!(
                "{instance_id}: cleanup tombstone conflicts with provider-owned storage"
            ));
        }
    }
    for (instance_id, tombstone) in &tombstones {
        if !durable_owners.contains_key(instance_id) {
            match read_ownership(tombstone, *instance_id).await {
                Ok(ownership) if ownership.phase == RuntimeSlotPhase::PoolCleanup => {}
                Ok(_) => errors.push(format!(
                    "{instance_id}: cleanup tombstone has no durable lifecycle owner and is not \
                     pool cleanup"
                )),
                Err(error) => errors.push(format!("{instance_id}: {error}")),
            }
        }
    }
    if !errors.is_empty() {
        return Err(unresolved_items(errors));
    }
    let candidates = runtime_dirs
        .keys()
        .chain(provider_ids.iter())
        .copied()
        .collect::<BTreeSet<_>>();
    let mut cleaned = 0;
    for (instance_id, tombstone) in tombstones {
        if durable_owners.get(&instance_id).is_some_and(|owner| {
            !owner.is_clean_terminal()
                && owner.runtime_location == RuntimeLocation::WarmPool
                && owner.state == SandboxState::Destroyed
                && owner.backend_ownership == BackendOwnership::Stopped
                && owner.operation == Some(OperationKind::Destroy)
        }) {
            continue;
        }
        let expected_owner = match durable_owners.get(&instance_id) {
            Some(owner) => match owner.runtime_owner_token {
                Some(token) => CleanupOwner::Lifecycle(token),
                None => {
                    errors.push(format!(
                        "{instance_id}: durable lifecycle owner has no runtime ownership token"
                    ));
                    continue;
                }
            },
            None => CleanupOwner::Pool,
        };
        match remove_owned_tombstone(runtime_root, instance_id, expected_owner).await {
            Ok(()) => cleaned += 1,
            Err(error) => errors.push(format!(
                "{instance_id}: continue runtime tombstone cleanup {}: {error}",
                tombstone.display()
            )),
        }
    }
    for instance_id in candidates {
        let run_dir = runtime_dirs.get(&instance_id);
        if durable_owners
            .get(&instance_id)
            .is_some_and(|owner| !owner.is_clean_terminal())
        {
            continue;
        }

        let Some(run_dir) = run_dir else {
            match storage.release_by_id(&instance_id.to_string()).await {
                Ok(()) => cleaned += 1,
                Err(error) => errors.push(format!(
                    "{instance_id}: release provider-only slot: {error}"
                )),
            }
            continue;
        };

        let ownership_path = run_dir.join(OWNERSHIP_FILE);
        let ownership_missing = match tokio::fs::symlink_metadata(&ownership_path).await {
            Ok(_) => false,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
            Err(error) => {
                errors.push(format!(
                    "{instance_id}: inspect ownership journal {}: {error}",
                    ownership_path.display()
                ));
                continue;
            }
        };
        if ownership_missing && !provider_ids.contains(&instance_id) {
            match tokio::fs::remove_dir(run_dir).await {
                Ok(()) => match sync_directory(runtime_root).await {
                    Ok(()) => {
                        cleaned += 1;
                        continue;
                    }
                    Err(error) => {
                        errors.push(format!(
                            "{instance_id}: sync runtime slot root after removing an empty \
                             unjournaled slot: {error}"
                        ));
                        continue;
                    }
                },
                Err(error) => {
                    errors.push(format!(
                        "{instance_id}: ownership journal {} is missing and the runtime directory \
                         is not safely removable as empty: {error}",
                        ownership_path.display()
                    ));
                    continue;
                }
            }
        }

        let mut ownership = match read_ownership(run_dir, instance_id).await {
            Ok(ownership) => ownership,
            Err(error) => {
                errors.push(format!("{instance_id}: {error}"));
                continue;
            }
        };
        if matches!(
            ownership.phase,
            RuntimeSlotPhase::LifecycleOwned { .. } | RuntimeSlotPhase::LifecycleCleanup { .. }
        ) {
            errors.push(format!(
                "{instance_id}: runtime slot records lifecycle ownership but durable lifecycle \
                 metadata is missing"
            ));
            continue;
        }
        if ownership.phase != RuntimeSlotPhase::PoolCleanup {
            ownership.phase = RuntimeSlotPhase::PoolCleanup;
            if let Err(error) = write_ownership(run_dir, &ownership).await {
                errors.push(format!(
                    "{instance_id}: record pool cleanup ownership: {error}"
                ));
                continue;
            }
        }

        if matches!(
            ownership.backend_ownership,
            BackendOwnership::Unknown | BackendOwnership::Starting | BackendOwnership::Running
        ) {
            let Some(spawner) = spawners.get(ownership.backend) else {
                errors.push(format!(
                    "{instance_id}: no recovery spawner registered for recorded backend {}",
                    ownership.backend
                ));
                continue;
            };
            if let Err(error) = spawner.cleanup_orphan(instance_id, run_dir).await {
                errors.push(format!(
                    "{instance_id}: clean recorded backend {}: {error}",
                    ownership.backend
                ));
                continue;
            }
            ownership.backend_ownership = BackendOwnership::Stopped;
            if let Err(error) = write_ownership(run_dir, &ownership).await {
                errors.push(format!(
                    "{instance_id}: record completed backend cleanup: {error}"
                ));
                continue;
            }
        }

        if ownership.storage_owned || provider_ids.contains(&instance_id) {
            if let Err(error) = storage.release_by_id(&instance_id.to_string()).await {
                errors.push(format!("{instance_id}: release recorded storage: {error}"));
                continue;
            }
            ownership.storage_owned = false;
            if let Err(error) = write_ownership(run_dir, &ownership).await {
                errors.push(format!(
                    "{instance_id}: record completed storage cleanup: {error}"
                ));
                continue;
            }
        }

        match tombstone_pool_slot(runtime_root, instance_id).await {
            Ok(()) => match remove_pool_tombstone(runtime_root, instance_id).await {
                Ok(()) => cleaned += 1,
                Err(error) => errors.push(format!(
                    "{instance_id}: remove runtime cleanup tombstone: {error}"
                )),
            },
            Err(error) => errors.push(format!(
                "{instance_id}: tombstone runtime directory {}: {error}",
                run_dir.display()
            )),
        }
    }

    if errors.is_empty() {
        Ok(cleaned)
    } else {
        Err(unresolved_items(errors))
    }
}

async fn validate_durable_owners(
    runtime_root: &Path,
    durable_owners: &HashMap<Uuid, DurableRuntimeOwner>,
    runtime_dirs: &BTreeMap<Uuid, PathBuf>,
    tombstones: &BTreeMap<Uuid, PathBuf>,
) -> Vec<String> {
    let mut errors = Vec::new();
    for (instance_id, owner) in durable_owners {
        if owner.is_clean_terminal() {
            if let Some(run_dir) = runtime_dirs.get(instance_id) {
                errors.push(format!(
                    "{instance_id}: clean terminal lifecycle state still has active runtime slot {}",
                    run_dir.display()
                ));
            }
            if let Some(tombstone) = tombstones.get(instance_id)
                && let Err(error) =
                    validate_transferred_run_dir(tombstone, *instance_id, owner, true).await
            {
                errors.push(format!("{instance_id}: {error}"));
            }
            continue;
        }
        if owner.instance_id != *instance_id {
            errors.push(format!(
                "{instance_id}: durable owner map records instance {}",
                owner.instance_id
            ));
            continue;
        }
        if let Some(tombstone) = tombstones.get(instance_id) {
            if owner.runtime_location != RuntimeLocation::WarmPool
                || owner.state != SandboxState::Destroyed
                || owner.backend_ownership != BackendOwnership::Stopped
                || owner.operation != Some(OperationKind::Destroy)
            {
                errors.push(format!(
                    "{instance_id}: cleanup tombstone {} collides with durable lifecycle owner",
                    tombstone.display()
                ));
            } else if let Err(error) =
                validate_transferred_run_dir(tombstone, *instance_id, owner, true).await
            {
                errors.push(format!("{instance_id}: {error}"));
            }
            continue;
        }
        match owner.runtime_location {
            RuntimeLocation::Sandbox => {
                if let Some(run_dir) = runtime_dirs.get(instance_id) {
                    errors.push(format!(
                        "{instance_id}: warm-slot directory {} collides with sandbox runtime owner \
                         in {} state",
                        run_dir.display(),
                        owner.state
                    ));
                }
            }
            RuntimeLocation::WarmPool => match runtime_dirs.get(instance_id) {
                Some(run_dir) => {
                    if let Err(error) =
                        validate_transferred_run_dir(run_dir, *instance_id, owner, false).await
                    {
                        errors.push(format!("{instance_id}: {error}"));
                    }
                }
                None => errors.push(format!(
                    "{instance_id}: durable warm-slot owner in {} state is missing canonical \
                     runtime directory {}",
                    owner.state,
                    runtime_root.join(instance_id.to_string()).display()
                )),
            },
        }
    }
    errors
}

async fn validate_transferred_run_dir(
    run_dir: &Path,
    instance_id: Uuid,
    owner: &DurableRuntimeOwner,
    tombstoned: bool,
) -> std::result::Result<(), String> {
    let ownership_path = run_dir.join(OWNERSHIP_FILE);
    match tokio::fs::symlink_metadata(&ownership_path).await {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(format!(
                "durable warm-slot owner is missing ownership journal {}",
                ownership_path.display()
            ));
        }
        Err(error) => {
            return Err(format!(
                "inspect transferred ownership {}: {error}",
                ownership_path.display()
            ));
        }
    }
    let ownership = read_ownership(run_dir, instance_id).await?;
    if ownership.backend != owner.backend {
        return Err(format!(
            "{} records backend {} but durable lifecycle owns {}",
            ownership_path.display(),
            ownership.backend,
            owner.backend
        ));
    }
    let Some(owner_token) = owner.runtime_owner_token else {
        return Err(format!(
            "durable warm-slot owner has no runtime ownership token for {}",
            ownership_path.display()
        ));
    };
    let current_phase = ownership.phase;
    match current_phase {
        RuntimeSlotPhase::Handoff { token } => {
            if tombstoned {
                return Err(format!(
                    "{} is tombstoned before its ownership handoff completed",
                    ownership_path.display()
                ));
            }
            if token != owner_token {
                return Err(format!(
                    "{} records handoff token {token} but durable lifecycle records {owner_token}",
                    ownership_path.display()
                ));
            }
            if ownership.backend_ownership != owner.backend_ownership {
                return Err(format!(
                    "{} records backend ownership {:?} but durable lifecycle records {:?}",
                    ownership_path.display(),
                    ownership.backend_ownership,
                    owner.backend_ownership
                ));
            }
            if !ownership.storage_owned {
                return Err(format!(
                    "{} records released storage during ownership handoff",
                    ownership_path.display()
                ));
            }
        }
        RuntimeSlotPhase::LifecycleOwned { token } => {
            if tombstoned {
                return Err(format!(
                    "{} is tombstoned without a lifecycle cleanup phase",
                    ownership_path.display()
                ));
            }
            if token != owner_token {
                return Err(format!(
                    "{} records lifecycle token {token} but durable lifecycle records {owner_token}",
                    ownership_path.display()
                ));
            }
        }
        RuntimeSlotPhase::LifecycleCleanup { token } => {
            if token != owner_token {
                return Err(format!(
                    "{} records cleanup token {token} but durable lifecycle records {owner_token}",
                    ownership_path.display()
                ));
            }
            if owner.operation != Some(OperationKind::Destroy) && !owner.is_clean_terminal() {
                return Err(format!(
                    "{} records lifecycle cleanup without a durable destroy operation",
                    ownership_path.display()
                ));
            }
        }
        RuntimeSlotPhase::Building | RuntimeSlotPhase::Ready | RuntimeSlotPhase::PoolCleanup => {
            return Err(format!(
                "{} remains pool-owned while durable lifecycle metadata exists",
                ownership_path.display()
            ));
        }
    }
    if matches!(
        current_phase,
        RuntimeSlotPhase::Handoff { .. } | RuntimeSlotPhase::LifecycleOwned { .. }
    ) && !ownership.storage_owned
    {
        return Err(format!(
            "{} records lifecycle ownership without owned storage",
            ownership_path.display()
        ));
    }
    Ok(())
}

async fn scan_runtime_dirs(
    runtime_root: &Path,
    cleanup_root: &Path,
    deletion_proof_root: &Path,
) -> Result<(
    BTreeMap<Uuid, PathBuf>,
    BTreeMap<Uuid, PathBuf>,
    Vec<String>,
)> {
    let mut directories = BTreeMap::new();
    let mut errors = Vec::new();
    let mut entries = tokio::fs::read_dir(runtime_root).await.map_err(|error| {
        recovery_error(format!(
            "read runtime slot root {}: {error}",
            runtime_root.display()
        ))
    })?;
    while let Some(entry) = entries.next_entry().await.map_err(|error| {
        recovery_error(format!(
            "read runtime slot entry under {}: {error}",
            runtime_root.display()
        ))
    })? {
        let path = entry.path();
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            errors.push(format!(
                "{}: runtime slot name is not UTF-8",
                path.display()
            ));
            continue;
        };
        let file_type = match entry.file_type().await {
            Ok(file_type) => file_type,
            Err(error) => {
                errors.push(format!("{}: inspect runtime slot: {error}", path.display()));
                continue;
            }
        };
        if name == CLEANUP_NAMESPACE {
            if !file_type.is_dir() {
                errors.push(format!(
                    "{}: runtime cleanup namespace is not a directory",
                    path.display()
                ));
            }
            continue;
        }
        if !file_type.is_dir() {
            errors.push(format!(
                "{}: unexpected non-directory runtime slot entry",
                path.display()
            ));
            continue;
        }
        match parse_stable_id(&name) {
            Ok(id) => {
                directories.insert(id, path);
            }
            Err(error) => errors.push(format!("{}: {error}", path.display())),
        }
    }
    let (tombstones, tombstone_errors) =
        scan_cleanup_dirs(cleanup_root, deletion_proof_root).await?;
    errors.extend(tombstone_errors);
    for instance_id in directories.keys() {
        if tombstones.contains_key(instance_id) {
            errors.push(format!(
                "{instance_id}: runtime slot exists in both active and cleanup namespaces"
            ));
        }
    }
    Ok((directories, tombstones, errors))
}

async fn scan_cleanup_dirs(
    cleanup_root: &Path,
    deletion_proof_root: &Path,
) -> Result<(BTreeMap<Uuid, PathBuf>, Vec<String>)> {
    let mut tombstones = BTreeMap::new();
    let mut errors = Vec::new();
    let mut entries = tokio::fs::read_dir(cleanup_root).await.map_err(|error| {
        recovery_error(format!(
            "read runtime cleanup namespace {}: {error}",
            cleanup_root.display()
        ))
    })?;
    while let Some(entry) = entries.next_entry().await.map_err(|error| {
        recovery_error(format!(
            "read runtime cleanup entry under {}: {error}",
            cleanup_root.display()
        ))
    })? {
        let path = entry.path();
        if path == deletion_proof_root {
            let file_type = entry.file_type().await.map_err(|error| {
                recovery_error(format!(
                    "inspect runtime deletion proof namespace {}: {error}",
                    path.display()
                ))
            })?;
            if !file_type.is_dir() {
                errors.push(format!(
                    "{}: runtime deletion proof namespace is not a directory",
                    path.display()
                ));
            }
            continue;
        }
        let file_type = match entry.file_type().await {
            Ok(file_type) => file_type,
            Err(error) => {
                errors.push(format!(
                    "{}: inspect runtime cleanup entry: {error}",
                    path.display()
                ));
                continue;
            }
        };
        if !file_type.is_dir() {
            errors.push(format!(
                "{}: unexpected non-directory runtime cleanup entry",
                path.display()
            ));
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            errors.push(format!(
                "{}: runtime cleanup name is not UTF-8",
                path.display()
            ));
            continue;
        };
        match parse_stable_id(&name) {
            Ok(id) => {
                tombstones.insert(id, path);
            }
            Err(error) => errors.push(format!("{}: {error}", path.display())),
        }
    }
    Ok((tombstones, errors))
}

struct DeletionProofRepair {
    instance_id: Uuid,
    proof: PathBuf,
    tombstone: PathBuf,
    create_tombstone: bool,
    restore_journal: bool,
}

async fn ensure_deletion_proof_root(cleanup_root: &Path, proof_root: &Path) -> Result<()> {
    ensure_real_directory(cleanup_root, "runtime cleanup namespace").await?;
    ensure_real_directory(proof_root, "runtime deletion proof namespace").await?;
    sync_directory(cleanup_root).await.map_err(|error| {
        recovery_error(format!(
            "sync runtime cleanup namespace {} after ensuring deletion proofs: {error}",
            cleanup_root.display()
        ))
    })
}

async fn restore_deletion_proofs(cleanup_root: &Path, proof_root: &Path) -> Result<()> {
    let (proofs, mut errors) = scan_deletion_proofs(proof_root).await?;
    let mut repairs = Vec::new();
    for (instance_id, proof) in proofs {
        let proof_ownership = match read_ownership_file(&proof, instance_id).await {
            Ok(ownership)
                if matches!(
                    ownership.phase,
                    RuntimeSlotPhase::PoolCleanup | RuntimeSlotPhase::LifecycleCleanup { .. }
                ) =>
            {
                ownership
            }
            Ok(ownership) => {
                errors.push(format!(
                    "{instance_id}: deletion proof {} records non-cleanup phase {:?}",
                    proof.display(),
                    ownership.phase
                ));
                continue;
            }
            Err(error) => {
                errors.push(format!("{instance_id}: {error}"));
                continue;
            }
        };
        let tombstone = cleanup_root.join(instance_id.to_string());
        let (create_tombstone, restore_journal) =
            match tokio::fs::symlink_metadata(&tombstone).await {
                Ok(metadata) if metadata.file_type().is_dir() => {
                    let journal = tombstone.join(OWNERSHIP_FILE);
                    match tokio::fs::symlink_metadata(&journal).await {
                        Ok(metadata) if metadata.file_type().is_file() => {
                            match read_ownership_file(&journal, instance_id).await {
                                Ok(ownership) if ownership == proof_ownership => (false, false),
                                Ok(_) => {
                                    errors.push(format!(
                                        "{instance_id}: deletion proof {} does not match {}",
                                        proof.display(),
                                        journal.display()
                                    ));
                                    continue;
                                }
                                Err(error) => {
                                    errors.push(format!("{instance_id}: {error}"));
                                    continue;
                                }
                            }
                        }
                        Ok(_) => {
                            errors.push(format!(
                                "{instance_id}: cleanup journal {} is not a regular file",
                                journal.display()
                            ));
                            continue;
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => (false, true),
                        Err(error) => {
                            errors.push(format!(
                                "{instance_id}: inspect cleanup journal {}: {error}",
                                journal.display()
                            ));
                            continue;
                        }
                    }
                }
                Ok(_) => {
                    errors.push(format!(
                        "{instance_id}: cleanup tombstone {} is not a real directory",
                        tombstone.display()
                    ));
                    continue;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => (true, true),
                Err(error) => {
                    errors.push(format!(
                        "{instance_id}: inspect cleanup tombstone {}: {error}",
                        tombstone.display()
                    ));
                    continue;
                }
            };
        repairs.push(DeletionProofRepair {
            instance_id,
            proof,
            tombstone,
            create_tombstone,
            restore_journal,
        });
    }
    if !errors.is_empty() {
        return Err(unresolved_items(errors));
    }

    for repair in repairs {
        if repair.create_tombstone {
            tokio::fs::create_dir(&repair.tombstone)
                .await
                .map_err(|error| {
                    recovery_error(format!(
                        "{}: recreate cleanup tombstone {} from deletion proof: {error}",
                        repair.instance_id,
                        repair.tombstone.display()
                    ))
                })?;
            sync_directory(cleanup_root).await.map_err(|error| {
                recovery_error(format!(
                    "{}: sync runtime cleanup namespace after recreating tombstone: {error}",
                    repair.instance_id
                ))
            })?;
        }
        if repair.restore_journal {
            let journal = repair.tombstone.join(OWNERSHIP_FILE);
            tokio::fs::hard_link(&repair.proof, &journal)
                .await
                .map_err(|error| {
                    recovery_error(format!(
                        "{}: restore cleanup journal {} from deletion proof {}: {error}",
                        repair.instance_id,
                        journal.display(),
                        repair.proof.display()
                    ))
                })?;
            sync_directory(&repair.tombstone).await.map_err(|error| {
                recovery_error(format!(
                    "{}: sync restored cleanup tombstone {}: {error}",
                    repair.instance_id,
                    repair.tombstone.display()
                ))
            })?;
        }
    }
    Ok(())
}

async fn scan_deletion_proofs(proof_root: &Path) -> Result<(BTreeMap<Uuid, PathBuf>, Vec<String>)> {
    let mut proofs = BTreeMap::new();
    let mut errors = Vec::new();
    let mut entries = tokio::fs::read_dir(proof_root).await.map_err(|error| {
        recovery_error(format!(
            "read runtime deletion proof namespace {}: {error}",
            proof_root.display()
        ))
    })?;
    while let Some(entry) = entries.next_entry().await.map_err(|error| {
        recovery_error(format!(
            "read runtime deletion proof entry under {}: {error}",
            proof_root.display()
        ))
    })? {
        let path = entry.path();
        let file_type = match entry.file_type().await {
            Ok(file_type) => file_type,
            Err(error) => {
                errors.push(format!(
                    "{}: inspect runtime deletion proof: {error}",
                    path.display()
                ));
                continue;
            }
        };
        if !file_type.is_file() {
            errors.push(format!(
                "{}: runtime deletion proof is not a regular file",
                path.display()
            ));
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            errors.push(format!(
                "{}: runtime deletion proof name is not UTF-8",
                path.display()
            ));
            continue;
        };
        match parse_stable_id(&name) {
            Ok(id) => {
                proofs.insert(id, path);
            }
            Err(error) => errors.push(format!("{}: {error}", path.display())),
        }
    }
    Ok((proofs, errors))
}

async fn ensure_real_directory(path: &Path, label: &str) -> Result<()> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.file_type().is_dir() => return Ok(()),
        Ok(_) => {
            return Err(recovery_error(format!(
                "{label} {} is not a real directory",
                path.display()
            )));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(recovery_error(format!(
                "inspect {label} {}: {error}",
                path.display()
            )));
        }
    }

    tokio::fs::create_dir_all(path)
        .await
        .map_err(|error| recovery_error(format!("create {label} {}: {error}", path.display())))?;
    let metadata = tokio::fs::symlink_metadata(path).await.map_err(|error| {
        recovery_error(format!(
            "inspect created {label} {}: {error}",
            path.display()
        ))
    })?;
    if !metadata.file_type().is_dir() {
        return Err(recovery_error(format!(
            "{label} {} is not a real directory",
            path.display()
        )));
    }
    Ok(())
}

async fn rename_to_tombstone(
    runtime_root: &Path,
    cleanup_root: &Path,
    run_dir: &Path,
    instance_id: Uuid,
) -> std::result::Result<(), String> {
    let tombstone = cleanup_root.join(instance_id.to_string());
    match tokio::fs::symlink_metadata(&tombstone).await {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Ok(_) => {
            return Err(format!(
                "cleanup tombstone {} already exists",
                tombstone.display()
            ));
        }
        Err(error) => {
            return Err(format!(
                "inspect cleanup tombstone {}: {error}",
                tombstone.display()
            ));
        }
    }

    tokio::fs::rename(run_dir, &tombstone)
        .await
        .map_err(|error| {
            format!(
                "move {} to {}: {error}",
                run_dir.display(),
                tombstone.display()
            )
        })?;
    sync_directory(runtime_root).await.map_err(|error| {
        format!(
            "sync runtime slot root {} after rename: {error}",
            runtime_root.display()
        )
    })?;
    sync_directory(cleanup_root).await.map_err(|error| {
        format!(
            "sync runtime cleanup namespace {} after rename: {error}",
            cleanup_root.display()
        )
    })?;
    Ok(())
}

#[derive(Clone, Copy)]
enum CleanupOwner {
    Pool,
    Lifecycle(Uuid),
}

impl CleanupOwner {
    fn phase(self) -> RuntimeSlotPhase {
        match self {
            Self::Pool => RuntimeSlotPhase::PoolCleanup,
            Self::Lifecycle(token) => RuntimeSlotPhase::LifecycleCleanup { token },
        }
    }
}

pub(super) async fn tombstone_pool_slot(
    runtime_root: &Path,
    instance_id: Uuid,
) -> std::result::Result<(), String> {
    tombstone_owned_slot(runtime_root, instance_id, CleanupOwner::Pool).await
}

pub(crate) async fn tombstone_lifecycle_slot(
    runtime_root: &Path,
    instance_id: Uuid,
    owner_token: Uuid,
) -> std::result::Result<(), String> {
    tombstone_owned_slot(
        runtime_root,
        instance_id,
        CleanupOwner::Lifecycle(owner_token),
    )
    .await
}

async fn tombstone_owned_slot(
    runtime_root: &Path,
    instance_id: Uuid,
    expected_owner: CleanupOwner,
) -> std::result::Result<(), String> {
    ensure_real_directory(runtime_root, "runtime slot root")
        .await
        .map_err(|error| error.to_string())?;
    let cleanup_root = runtime_root.join(CLEANUP_NAMESPACE);
    ensure_real_directory(&cleanup_root, "runtime cleanup namespace")
        .await
        .map_err(|error| error.to_string())?;
    let run_dir = runtime_root.join(instance_id.to_string());
    let tombstone = cleanup_root.join(instance_id.to_string());
    let run_dir_exists = real_directory_exists(&run_dir, "runtime slot").await?;
    let tombstone_exists = real_directory_exists(&tombstone, "cleanup tombstone").await?;
    match (run_dir_exists, tombstone_exists) {
        (true, false) => {
            require_cleanup_owner(&run_dir, instance_id, expected_owner).await?;
            rename_to_tombstone(runtime_root, &cleanup_root, &run_dir, instance_id).await
        }
        (false, true) => {
            require_cleanup_owner(&tombstone, instance_id, expected_owner).await?;
            Ok(())
        }
        (false, false) => match expected_owner {
            CleanupOwner::Pool => sync_directory(&cleanup_root).await.map_err(|error| {
                format!(
                    "{instance_id}: sync runtime cleanup namespace after prior pool removal: \
                     {error}"
                )
            }),
            CleanupOwner::Lifecycle(_) => Err(format!(
                "{instance_id}: runtime slot has neither an active directory nor a cleanup \
                 tombstone"
            )),
        },
        (true, true) => Err(format!(
            "{instance_id}: runtime slot exists in both active and cleanup namespaces"
        )),
    }
}

async fn require_cleanup_owner(
    directory: &Path,
    instance_id: Uuid,
    expected_owner: CleanupOwner,
) -> std::result::Result<RuntimeSlotOwnership, String> {
    let ownership = read_ownership(directory, instance_id).await?;
    if ownership.phase != expected_owner.phase() {
        return Err(format!(
            "{} records {:?} instead of the expected cleanup owner",
            directory.join(OWNERSHIP_FILE).display(),
            ownership.phase
        ));
    }
    Ok(ownership)
}

pub(super) async fn remove_pool_tombstone(
    runtime_root: &Path,
    instance_id: Uuid,
) -> std::result::Result<(), String> {
    remove_owned_tombstone(runtime_root, instance_id, CleanupOwner::Pool).await
}

pub(crate) async fn remove_lifecycle_tombstone(
    runtime_root: &Path,
    instance_id: Uuid,
    owner_token: Uuid,
) -> std::result::Result<(), String> {
    remove_owned_tombstone(
        runtime_root,
        instance_id,
        CleanupOwner::Lifecycle(owner_token),
    )
    .await
}

async fn remove_owned_tombstone(
    runtime_root: &Path,
    instance_id: Uuid,
    expected_owner: CleanupOwner,
) -> std::result::Result<(), String> {
    // Pool maintenance, lifecycle operation locks, and startup isolation each
    // serialize removal for one instance ID. The proof protocol relies on that
    // per-owner exclusion while it repairs or removes the canonical paths.
    let run_dir = runtime_root.join(instance_id.to_string());
    if real_directory_exists(&run_dir, "runtime slot").await? {
        return Err(format!(
            "{instance_id}: active runtime slot still exists before tombstone removal"
        ));
    }
    let cleanup_root = runtime_root.join(CLEANUP_NAMESPACE);
    let proof_root = cleanup_root.join(DELETION_PROOF_NAMESPACE);
    ensure_deletion_proof_root(&cleanup_root, &proof_root)
        .await
        .map_err(|error| error.to_string())?;
    let tombstone = cleanup_root.join(instance_id.to_string());
    let proof = proof_root.join(instance_id.to_string());

    let proof_exists = match tokio::fs::symlink_metadata(&proof).await {
        Ok(metadata) if metadata.file_type().is_file() => true,
        Ok(_) => {
            return Err(format!(
                "runtime deletion proof {} is not a regular file",
                proof.display()
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => {
            return Err(format!(
                "inspect runtime deletion proof {}: {error}",
                proof.display()
            ));
        }
    };

    if proof_exists {
        restore_one_deletion_proof(
            &cleanup_root,
            &proof_root,
            &proof,
            &tombstone,
            instance_id,
            expected_owner,
        )
        .await?;
    } else if real_directory_exists(&tombstone, "cleanup tombstone").await? {
        arm_deletion_proof(&proof_root, &tombstone, &proof, instance_id, expected_owner).await?;
    } else {
        sync_directory(&cleanup_root).await.map_err(|error| {
            format!(
                "{instance_id}: sync runtime cleanup namespace after prior tombstone removal: \
                 {error}"
            )
        })?;
        return Ok(());
    }

    remove_armed_tombstone(&cleanup_root, &proof_root, &tombstone, &proof, instance_id).await
}

async fn arm_deletion_proof(
    proof_root: &Path,
    tombstone: &Path,
    proof: &Path,
    instance_id: Uuid,
    expected_owner: CleanupOwner,
) -> std::result::Result<(), String> {
    let ownership = require_cleanup_owner(tombstone, instance_id, expected_owner).await?;
    let journal = tombstone.join(OWNERSHIP_FILE);
    tokio::fs::hard_link(&journal, proof)
        .await
        .map_err(|error| {
            format!(
                "{instance_id}: preserve cleanup ownership from {} in {}: {error}",
                journal.display(),
                proof.display()
            )
        })?;
    sync_directory(proof_root).await.map_err(|error| {
        format!(
            "{instance_id}: sync runtime deletion proof namespace {}: {error}",
            proof_root.display()
        )
    })?;
    let proof_ownership = read_ownership_file(proof, instance_id).await?;
    if proof_ownership != ownership {
        return Err(format!(
            "{instance_id}: deletion proof {} changed while it was persisted",
            proof.display()
        ));
    }
    Ok(())
}

async fn real_directory_exists(path: &Path, label: &str) -> std::result::Result<bool, String> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(true),
        Ok(_) => Err(format!(
            "{label} {} is not a real directory",
            path.display()
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(format!("inspect {label} {}: {error}", path.display())),
    }
}

async fn restore_one_deletion_proof(
    cleanup_root: &Path,
    proof_root: &Path,
    proof: &Path,
    tombstone: &Path,
    instance_id: Uuid,
    expected_owner: CleanupOwner,
) -> std::result::Result<(), String> {
    let proof_ownership = read_ownership_file(proof, instance_id).await?;
    if proof_ownership.phase != expected_owner.phase() {
        return Err(format!(
            "{} records {:?} instead of the expected cleanup owner",
            proof.display(),
            proof_ownership.phase
        ));
    }
    match tokio::fs::symlink_metadata(tombstone).await {
        Ok(metadata) if metadata.file_type().is_dir() => {}
        Ok(_) => {
            return Err(format!(
                "cleanup tombstone {} is not a real directory",
                tombstone.display()
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            tokio::fs::create_dir(tombstone).await.map_err(|error| {
                format!(
                    "{instance_id}: recreate cleanup tombstone {} from deletion proof: {error}",
                    tombstone.display()
                )
            })?;
            sync_directory(cleanup_root).await.map_err(|error| {
                format!(
                    "{instance_id}: sync runtime cleanup namespace after recreating tombstone: \
                     {error}"
                )
            })?;
        }
        Err(error) => {
            return Err(format!(
                "inspect cleanup tombstone {}: {error}",
                tombstone.display()
            ));
        }
    }
    let journal = tombstone.join(OWNERSHIP_FILE);
    match tokio::fs::symlink_metadata(&journal).await {
        Ok(metadata) if metadata.file_type().is_file() => {
            let ownership = read_ownership_file(&journal, instance_id).await?;
            if ownership != proof_ownership {
                return Err(format!(
                    "{instance_id}: deletion proof {} does not match {}",
                    proof.display(),
                    journal.display()
                ));
            }
        }
        Ok(_) => {
            return Err(format!(
                "{instance_id}: cleanup journal {} is not a regular file",
                journal.display()
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            tokio::fs::hard_link(proof, &journal)
                .await
                .map_err(|error| {
                    format!(
                        "{instance_id}: restore cleanup journal {} from deletion proof {}: {error}",
                        journal.display(),
                        proof.display()
                    )
                })?;
            sync_directory(tombstone).await.map_err(|error| {
                format!(
                    "{instance_id}: sync restored cleanup tombstone {}: {error}",
                    tombstone.display()
                )
            })?;
        }
        Err(error) => {
            return Err(format!(
                "{instance_id}: inspect cleanup journal {}: {error}",
                journal.display()
            ));
        }
    }
    sync_directory(proof_root).await.map_err(|error| {
        format!(
            "{instance_id}: sync runtime deletion proof namespace {}: {error}",
            proof_root.display()
        )
    })
}

async fn remove_armed_tombstone(
    cleanup_root: &Path,
    proof_root: &Path,
    tombstone: &Path,
    proof: &Path,
    instance_id: Uuid,
) -> std::result::Result<(), String> {
    let tombstone_exists = match tokio::fs::symlink_metadata(tombstone).await {
        Ok(metadata) if metadata.file_type().is_dir() => true,
        Ok(_) => {
            return Err(format!(
                "cleanup tombstone {} is not a real directory",
                tombstone.display()
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => {
            return Err(format!(
                "inspect cleanup tombstone {}: {error}",
                tombstone.display()
            ));
        }
    };
    if tombstone_exists {
        tokio::fs::remove_dir_all(tombstone)
            .await
            .map_err(|error| {
                format!("remove cleanup tombstone {}: {error}", tombstone.display())
            })?;
    }
    sync_directory(cleanup_root).await.map_err(|error| {
        format!(
            "sync runtime cleanup namespace {} after removal: {error}",
            cleanup_root.display()
        )
    })?;
    match tokio::fs::remove_file(proof).await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(format!(
                "{instance_id}: remove runtime deletion proof {}: {error}",
                proof.display()
            ));
        }
    }
    sync_directory(proof_root).await.map_err(|error| {
        format!(
            "{instance_id}: sync runtime deletion proof namespace {} after removal: {error}",
            proof_root.display()
        )
    })?;
    Ok(())
}

async fn sync_directory(path: &Path) -> std::io::Result<()> {
    tokio::fs::File::open(path).await?.sync_all().await
}

pub(super) async fn finish_ownership_handoff(
    run_dir: &Path,
    expected_id: Uuid,
    expected_backend: BackendKind,
    expected_ownership: BackendOwnership,
    expected_token: Uuid,
) -> std::result::Result<(), String> {
    let mut ownership = read_ownership(run_dir, expected_id).await?;
    let path = run_dir.join(OWNERSHIP_FILE);
    if ownership.backend != expected_backend {
        return Err(format!(
            "{} records backend {} instead of {expected_backend}",
            path.display(),
            ownership.backend
        ));
    }
    if ownership.backend_ownership != expected_ownership {
        return Err(format!(
            "{} records backend ownership {:?} instead of {:?}",
            path.display(),
            ownership.backend_ownership,
            expected_ownership
        ));
    }
    if !ownership.storage_owned {
        return Err(format!(
            "{} cannot transfer a slot without owned storage",
            path.display()
        ));
    }
    if ownership.phase
        == (RuntimeSlotPhase::LifecycleOwned {
            token: expected_token,
        })
    {
        return Ok(());
    }
    if ownership.phase
        != (RuntimeSlotPhase::Handoff {
            token: expected_token,
        })
    {
        return Err(format!(
            "{} does not record expected ownership handoff token {expected_token}",
            path.display()
        ));
    }
    ownership.phase = RuntimeSlotPhase::LifecycleOwned {
        token: expected_token,
    };
    write_ownership(run_dir, &ownership).await
}

/// Durably transfer a claimed slot into lifecycle cleanup before mutating
/// backend or storage ownership.
pub(crate) async fn begin_lifecycle_cleanup(
    runtime_root: &Path,
    instance_id: Uuid,
    backend: BackendKind,
    owner_token: Uuid,
) -> std::result::Result<(), String> {
    ensure_real_directory(runtime_root, "runtime slot root")
        .await
        .map_err(|error| error.to_string())?;
    let cleanup_root = runtime_root.join(CLEANUP_NAMESPACE);
    ensure_real_directory(&cleanup_root, "runtime cleanup namespace")
        .await
        .map_err(|error| error.to_string())?;
    let run_dir = runtime_root.join(instance_id.to_string());
    let tombstone = cleanup_root.join(instance_id.to_string());
    let run_dir_exists = real_directory_exists(&run_dir, "runtime slot").await?;
    let tombstone_exists = real_directory_exists(&tombstone, "cleanup tombstone").await?;
    let (directory, tombstoned) = match (run_dir_exists, tombstone_exists) {
        (true, false) => (run_dir, false),
        (false, true) => (tombstone, true),
        (false, false) => {
            return Err(format!(
                "{instance_id}: lifecycle-owned runtime slot has neither an active directory nor \
                 a cleanup tombstone"
            ));
        }
        (true, true) => {
            return Err(format!(
                "{instance_id}: lifecycle-owned runtime slot exists in both active and cleanup \
                 namespaces"
            ));
        }
    };
    let mut ownership = read_ownership(&directory, instance_id).await?;
    if ownership.backend != backend {
        return Err(format!(
            "{} records backend {} instead of lifecycle backend {backend}",
            directory.join(OWNERSHIP_FILE).display(),
            ownership.backend
        ));
    }
    let current_phase = ownership.phase;
    match current_phase {
        RuntimeSlotPhase::Handoff { token }
        | RuntimeSlotPhase::LifecycleOwned { token }
        | RuntimeSlotPhase::LifecycleCleanup { token }
            if token == owner_token => {}
        RuntimeSlotPhase::Handoff { token }
        | RuntimeSlotPhase::LifecycleOwned { token }
        | RuntimeSlotPhase::LifecycleCleanup { token } => {
            return Err(format!(
                "{} records lifecycle token {token} instead of {owner_token}",
                directory.join(OWNERSHIP_FILE).display()
            ));
        }
        phase => {
            return Err(format!(
                "{} records pool phase {phase:?} instead of lifecycle ownership",
                directory.join(OWNERSHIP_FILE).display()
            ));
        }
    }
    if matches!(
        current_phase,
        RuntimeSlotPhase::Handoff { .. } | RuntimeSlotPhase::LifecycleOwned { .. }
    ) && !ownership.storage_owned
    {
        return Err(format!(
            "{} records lifecycle ownership without owned storage",
            directory.join(OWNERSHIP_FILE).display()
        ));
    }
    if tombstoned {
        if ownership.phase != (RuntimeSlotPhase::LifecycleCleanup { token: owner_token }) {
            return Err(format!(
                "{} was tombstoned before lifecycle cleanup was committed",
                directory.join(OWNERSHIP_FILE).display()
            ));
        }
        return Ok(());
    }
    if ownership.phase == (RuntimeSlotPhase::LifecycleCleanup { token: owner_token }) {
        return Ok(());
    }
    ownership.phase = RuntimeSlotPhase::LifecycleCleanup { token: owner_token };
    write_ownership(&directory, &ownership).await
}

pub(super) async fn read_ownership(
    run_dir: &Path,
    expected_id: Uuid,
) -> std::result::Result<RuntimeSlotOwnership, String> {
    let path = run_dir.join(OWNERSHIP_FILE);
    read_ownership_file(&path, expected_id).await
}

async fn read_ownership_file(
    path: &Path,
    expected_id: Uuid,
) -> std::result::Result<RuntimeSlotOwnership, String> {
    let metadata = tokio::fs::symlink_metadata(&path)
        .await
        .map_err(|error| format!("inspect {}: {error}", path.display()))?;
    if !metadata.file_type().is_file() {
        return Err(format!("{} is not a regular file", path.display()));
    }
    let encoded = tokio::fs::read(&path)
        .await
        .map_err(|error| format!("read {}: {error}", path.display()))?;
    let ownership: RuntimeSlotOwnership = serde_json::from_slice(&encoded)
        .map_err(|error| format!("decode {}: {error}", path.display()))?;
    validate_ownership(path, &ownership, expected_id)?;
    Ok(ownership)
}

fn validate_ownership(
    path: &Path,
    ownership: &RuntimeSlotOwnership,
    expected_id: Uuid,
) -> std::result::Result<(), String> {
    if ownership.version != OWNERSHIP_VERSION {
        return Err(format!(
            "{} has unsupported ownership version {}",
            path.display(),
            ownership.version
        ));
    }
    if ownership.instance_id != expected_id {
        return Err(format!(
            "{} records instance {} instead of directory {expected_id}",
            path.display(),
            ownership.instance_id
        ));
    }
    Ok(())
}

pub(super) async fn write_ownership(
    run_dir: &Path,
    ownership: &RuntimeSlotOwnership,
) -> std::result::Result<(), String> {
    let encoded = serde_json::to_vec(ownership)
        .map_err(|error| format!("encode ownership record: {error}"))?;
    let path = run_dir.join(OWNERSHIP_FILE);
    let temporary = run_dir.join(format!(".ownership-{}.tmp", Uuid::new_v4()));
    let result = async {
        let mut file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .await?;
        file.write_all(&encoded).await?;
        file.sync_all().await?;
        drop(file);
        tokio::fs::rename(&temporary, &path).await?;
        tokio::fs::File::open(run_dir).await?.sync_all().await
    }
    .await;
    if let Err(error) = result {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(format!("persist {}: {error}", path.display()));
    }
    Ok(())
}

fn parse_stable_id(value: &str) -> std::result::Result<Uuid, String> {
    let id = Uuid::parse_str(value).map_err(|error| format!("slot ID is not a UUID: {error}"))?;
    if value != id.to_string() {
        return Err(format!("slot ID must use canonical UUID form {id}"));
    }
    Ok(id)
}

fn recovery_error(message: String) -> BlazeDaemonError {
    BlazeDaemonError::RecoveryRequired(message)
}

fn unresolved_items(errors: Vec<String>) -> BlazeDaemonError {
    recovery_error(format!(
        "runtime slot reconciliation found {} unresolved item(s): {}",
        errors.len(),
        errors.join("; ")
    ))
}

#[cfg(test)]
mod tests {
    use std::future;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use blaze_core::backend::SpawnRequest;
    use blaze_core::lifecycle::StartPath;
    use blaze_core::storage::{AcquireOpts, PoolStatus, StorageAcquireError, StorageSlot};
    use blaze_core::{BlazeError, Result as CoreResult};

    use super::*;
    use crate::spawner::{BackendSpawner, DynBackendInstance, DynSpawner, SpawnFailure};

    #[derive(Default)]
    struct RecordingStorage {
        owned: Mutex<BTreeSet<Uuid>>,
        fail_once: Mutex<BTreeSet<Uuid>>,
        events: Arc<Mutex<Vec<String>>>,
        supports_recovery: bool,
        inventory_error: Option<String>,
        inventory_override: Option<Vec<String>>,
        pending_inventory: bool,
        pending_release: bool,
    }

    impl RecordingStorage {
        fn with_ids(ids: impl IntoIterator<Item = Uuid>, events: Arc<Mutex<Vec<String>>>) -> Self {
            Self {
                owned: Mutex::new(ids.into_iter().collect()),
                fail_once: Mutex::new(BTreeSet::new()),
                events,
                supports_recovery: true,
                inventory_error: None,
                inventory_override: None,
                pending_inventory: false,
                pending_release: false,
            }
        }

        fn fail_next_release(&self, instance_id: Uuid) {
            self.fail_once
                .lock()
                .expect("release failures")
                .insert(instance_id);
        }
    }

    #[async_trait]
    impl StorageProvider for RecordingStorage {
        async fn probe(&self) -> CoreResult<bool> {
            Ok(true)
        }

        async fn acquire(
            &self,
            _opts: &AcquireOpts,
        ) -> std::result::Result<StorageSlot, StorageAcquireError> {
            Err(StorageAcquireError::clean(BlazeError::StorageError {
                msg: "test storage does not acquire slots".into(),
            }))
        }

        async fn release(&self, slot: StorageSlot) -> CoreResult<()> {
            self.release_by_id(&slot.id).await
        }

        async fn release_by_id(&self, instance_id: &str) -> CoreResult<()> {
            let id = Uuid::parse_str(instance_id).map_err(|error| BlazeError::StorageError {
                msg: format!("invalid test slot id {instance_id}: {error}"),
            })?;
            self.events
                .lock()
                .expect("events")
                .push(format!("storage:{id}"));
            if self.pending_release {
                future::pending::<()>().await;
            }
            if self.fail_once.lock().expect("release failures").remove(&id) {
                return Err(BlazeError::StorageError {
                    msg: format!("injected release failure for {id}"),
                });
            }
            self.owned.lock().expect("owned slots").remove(&id);
            Ok(())
        }

        async fn reconstruct(&self, instance_id: &str) -> CoreResult<StorageSlot> {
            Err(BlazeError::StorageError {
                msg: format!("test storage cannot reconstruct {instance_id}"),
            })
        }

        async fn flush_dirty(&self, _slot: &StorageSlot) -> CoreResult<()> {
            Ok(())
        }

        fn pool_status(&self) -> PoolStatus {
            PoolStatus::default()
        }

        async fn drain_pool(&self) -> CoreResult<usize> {
            Ok(0)
        }

        fn supports_runtime_pool_recovery(&self) -> bool {
            self.supports_recovery
        }

        async fn list_owned_ids(&self) -> CoreResult<Vec<String>> {
            if self.pending_inventory {
                future::pending::<()>().await;
            }
            if let Some(message) = &self.inventory_error {
                return Err(BlazeError::StorageError {
                    msg: message.clone(),
                });
            }
            if let Some(ids) = &self.inventory_override {
                return Ok(ids.clone());
            }
            Ok(self
                .owned
                .lock()
                .expect("owned slots")
                .iter()
                .map(Uuid::to_string)
                .collect())
        }
    }

    struct RecordingSpawner {
        events: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl BackendSpawner for RecordingSpawner {
        async fn spawn(
            &self,
            _request: SpawnRequest,
        ) -> std::result::Result<DynBackendInstance, SpawnFailure> {
            Err(SpawnFailure::clean(BlazeError::BackendError {
                msg: "test spawner does not spawn instances".into(),
            }))
        }

        async fn probe(&self, _binary_path: &Path) -> CoreResult<bool> {
            Ok(true)
        }

        async fn cleanup_orphan(&self, instance_id: Uuid, _run_dir: &Path) -> CoreResult<()> {
            self.events
                .lock()
                .expect("events")
                .push(format!("backend:{instance_id}"));
            Ok(())
        }
    }

    struct FailingSpawner {
        events: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl BackendSpawner for FailingSpawner {
        async fn spawn(
            &self,
            _request: SpawnRequest,
        ) -> std::result::Result<DynBackendInstance, SpawnFailure> {
            Err(SpawnFailure::clean(BlazeError::BackendError {
                msg: "test spawner does not spawn instances".into(),
            }))
        }

        async fn probe(&self, _binary_path: &Path) -> CoreResult<bool> {
            Ok(true)
        }

        async fn cleanup_orphan(&self, instance_id: Uuid, _run_dir: &Path) -> CoreResult<()> {
            self.events
                .lock()
                .expect("events")
                .push(format!("backend:{instance_id}"));
            Err(BlazeError::BackendError {
                msg: format!("injected backend cleanup failure for {instance_id}"),
            })
        }
    }

    struct PendingSpawner {
        events: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl BackendSpawner for PendingSpawner {
        async fn spawn(
            &self,
            _request: SpawnRequest,
        ) -> std::result::Result<DynBackendInstance, SpawnFailure> {
            Err(SpawnFailure::clean(BlazeError::BackendError {
                msg: "test spawner does not spawn instances".into(),
            }))
        }

        async fn probe(&self, _binary_path: &Path) -> CoreResult<bool> {
            Ok(true)
        }

        async fn cleanup_orphan(&self, instance_id: Uuid, _run_dir: &Path) -> CoreResult<()> {
            self.events
                .lock()
                .expect("events")
                .push(format!("backend:{instance_id}"));
            future::pending::<CoreResult<()>>().await
        }
    }

    struct DelayedSpawner {
        delay: Duration,
        events: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl BackendSpawner for DelayedSpawner {
        async fn spawn(
            &self,
            _request: SpawnRequest,
        ) -> std::result::Result<DynBackendInstance, SpawnFailure> {
            Err(SpawnFailure::clean(BlazeError::BackendError {
                msg: "test spawner does not spawn instances".into(),
            }))
        }

        async fn probe(&self, _binary_path: &Path) -> CoreResult<bool> {
            Ok(true)
        }

        async fn cleanup_orphan(&self, instance_id: Uuid, _run_dir: &Path) -> CoreResult<()> {
            self.events
                .lock()
                .expect("events")
                .push(format!("backend:{instance_id}"));
            tokio::time::sleep(self.delay).await;
            Ok(())
        }
    }

    fn registry(kind: BackendKind, spawner: DynSpawner) -> SpawnerRegistry {
        let mut registry = SpawnerRegistry::new();
        registry.insert(kind, spawner);
        registry
    }

    fn write_ownership(
        runtime_root: &Path,
        instance_id: Uuid,
        backend: BackendKind,
        backend_ownership: BackendOwnership,
        storage_owned: bool,
    ) -> PathBuf {
        write_ownership_with_phase(
            runtime_root,
            instance_id,
            backend,
            backend_ownership,
            storage_owned,
            RuntimeSlotPhase::Building,
        )
    }

    fn write_ownership_with_phase(
        runtime_root: &Path,
        instance_id: Uuid,
        backend: BackendKind,
        backend_ownership: BackendOwnership,
        storage_owned: bool,
        phase: RuntimeSlotPhase,
    ) -> PathBuf {
        let run_dir = runtime_root.join(instance_id.to_string());
        std::fs::create_dir_all(&run_dir).expect("run dir");
        let ownership = RuntimeSlotOwnership {
            version: OWNERSHIP_VERSION,
            instance_id,
            backend,
            backend_ownership,
            storage_owned,
            phase,
        };
        std::fs::write(
            run_dir.join(OWNERSHIP_FILE),
            serde_json::to_vec(&ownership).expect("serialize ownership"),
        )
        .expect("write ownership");
        run_dir
    }

    fn write_tombstone(runtime_root: &Path, instance_id: Uuid) -> PathBuf {
        let tombstone = runtime_root
            .join(CLEANUP_NAMESPACE)
            .join(instance_id.to_string());
        std::fs::create_dir_all(&tombstone).expect("tombstone");
        let ownership = RuntimeSlotOwnership {
            version: OWNERSHIP_VERSION,
            instance_id,
            backend: BackendKind::Mock,
            backend_ownership: BackendOwnership::Stopped,
            storage_owned: false,
            phase: RuntimeSlotPhase::PoolCleanup,
        };
        std::fs::write(
            tombstone.join(OWNERSHIP_FILE),
            serde_json::to_vec(&ownership).expect("serialize tombstone ownership"),
        )
        .expect("write tombstone ownership");
        tombstone
    }

    async fn arm_test_deletion_proof(
        runtime_root: &Path,
        instance_id: Uuid,
        expected_owner: CleanupOwner,
    ) -> PathBuf {
        let cleanup_root = runtime_root.join(CLEANUP_NAMESPACE);
        let proof_root = cleanup_root.join(DELETION_PROOF_NAMESPACE);
        ensure_deletion_proof_root(&cleanup_root, &proof_root)
            .await
            .expect("proof root");
        let tombstone = cleanup_root.join(instance_id.to_string());
        let proof = proof_root.join(instance_id.to_string());
        arm_deletion_proof(&proof_root, &tombstone, &proof, instance_id, expected_owner)
            .await
            .expect("arm deletion proof");
        proof
    }

    #[tokio::test]
    async fn reconcile_cleans_backend_before_storage_and_run_dir() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id = Uuid::new_v4();
        let run_dir = write_ownership(
            &runtime_root,
            instance_id,
            BackendKind::Mock,
            BackendOwnership::Running,
            true,
        );
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([instance_id], events.clone());
        let spawners = registry(
            BackendKind::Mock,
            Arc::new(RecordingSpawner {
                events: events.clone(),
            }),
        );

        let cleaned = reconcile_runtime_slots(&runtime_root, &HashMap::new(), &storage, &spawners)
            .await
            .expect("reconcile");

        assert_eq!(cleaned, 1);
        assert_eq!(
            *events.lock().expect("events"),
            vec![
                format!("backend:{instance_id}"),
                format!("storage:{instance_id}")
            ]
        );
        assert!(!run_dir.exists());
    }

    #[tokio::test]
    async fn reconcile_retains_storage_after_backend_cleanup_failure() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id = Uuid::new_v4();
        let run_dir = write_ownership(
            &runtime_root,
            instance_id,
            BackendKind::Mock,
            BackendOwnership::Running,
            true,
        );
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([instance_id], events.clone());
        let spawners = registry(
            BackendKind::Mock,
            Arc::new(FailingSpawner {
                events: events.clone(),
            }),
        );

        let error = reconcile_runtime_slots(&runtime_root, &HashMap::new(), &storage, &spawners)
            .await
            .expect_err("backend failure must retain later owners");

        assert!(
            error
                .to_string()
                .contains("injected backend cleanup failure")
        );
        assert_eq!(
            *events.lock().expect("events"),
            vec![format!("backend:{instance_id}")]
        );
        assert!(run_dir.exists());
        assert!(
            storage
                .owned
                .lock()
                .expect("owned slots")
                .contains(&instance_id)
        );
    }

    #[tokio::test]
    async fn reconcile_releases_provider_only_slot_by_id() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id = Uuid::new_v4();
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([instance_id], events.clone());

        let cleaned = reconcile_runtime_slots(
            &runtime_root,
            &HashMap::new(),
            &storage,
            &SpawnerRegistry::new(),
        )
        .await
        .expect("reconcile");

        assert_eq!(cleaned, 1);
        assert_eq!(
            *events.lock().expect("events"),
            vec![format!("storage:{instance_id}")]
        );
    }

    #[tokio::test]
    async fn reconcile_protects_durable_lifecycle_owner() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id = Uuid::new_v4();
        let owner_token = Uuid::new_v4();
        let run_dir = write_ownership_with_phase(
            &runtime_root,
            instance_id,
            BackendKind::Mock,
            BackendOwnership::Running,
            true,
            RuntimeSlotPhase::LifecycleOwned { token: owner_token },
        );
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([instance_id], events.clone());

        let owner = DurableRuntimeOwner {
            instance_id,
            runtime_location: RuntimeLocation::WarmPool,
            backend: BackendKind::Mock,
            backend_ownership: BackendOwnership::Running,
            state: SandboxState::Running,
            operation: None,
            runtime_owner_token: Some(owner_token),
            clean_terminal: false,
        };
        let cleaned = reconcile_runtime_slots(
            &runtime_root,
            &HashMap::from([(instance_id, owner)]),
            &storage,
            &SpawnerRegistry::new(),
        )
        .await
        .expect("durable owner must be excluded");

        assert_eq!(cleaned, 0);
        assert!(events.lock().expect("events").is_empty());
        assert!(run_dir.exists());
    }

    #[tokio::test]
    async fn reconcile_checks_live_warm_owner_missing_from_inventory() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id = Uuid::new_v4();
        let owner = DurableRuntimeOwner {
            instance_id,
            runtime_location: RuntimeLocation::WarmPool,
            backend: BackendKind::Mock,
            backend_ownership: BackendOwnership::Running,
            state: SandboxState::Running,
            operation: None,
            runtime_owner_token: None,
            clean_terminal: false,
        };
        let storage = RecordingStorage::default();

        let error = reconcile_runtime_slots(
            &runtime_root,
            &HashMap::from([(instance_id, owner)]),
            &storage,
            &SpawnerRegistry::new(),
        )
        .await
        .expect_err("live warm owner requires its canonical run dir");

        let message = error.to_string();
        assert!(message.contains(&instance_id.to_string()));
        assert!(message.contains("canonical runtime directory"));
        assert!(message.contains("is missing"));
    }

    #[tokio::test]
    async fn reconcile_rejects_warm_owner_without_run_dir() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id = Uuid::new_v4();
        let owner = DurableRuntimeOwner {
            instance_id,
            runtime_location: RuntimeLocation::WarmPool,
            backend: BackendKind::Mock,
            backend_ownership: BackendOwnership::NotStarted,
            state: SandboxState::Creating,
            operation: Some(OperationKind::Create),
            runtime_owner_token: Some(Uuid::new_v4()),
            clean_terminal: false,
        };
        let storage = RecordingStorage::default();

        let error = reconcile_runtime_slots(
            &runtime_root,
            &HashMap::from([(instance_id, owner)]),
            &storage,
            &SpawnerRegistry::new(),
        )
        .await
        .expect_err("durable warm owner must retain its canonical run dir");

        assert!(error.to_string().contains("missing canonical"));
    }

    #[test]
    fn runtime_location_survives_warm_reclassification_and_reload() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut sandbox = SandboxInstance::new(
            BackendKind::Mock,
            blaze_core::policy::WorkloadClass::AgentTool,
            "sha256:sandbox-location".into(),
            StartPath::Cold,
            "sandbox-location".into(),
        );
        sandbox
            .transition(SandboxState::Creating)
            .expect("creating");
        sandbox.transition(SandboxState::Running).expect("running");
        sandbox.transition(SandboxState::Reset).expect("reset");
        sandbox.transition(SandboxState::Warm).expect("warm");
        sandbox
            .transition(SandboxState::Creating)
            .expect("warm claim");
        sandbox.persist(temp.path()).expect("persist sandbox");
        let sandbox = SandboxInstance::load(temp.path(), sandbox.id).expect("reload sandbox");
        assert_eq!(sandbox.start_path, StartPath::Warm);
        assert_eq!(
            runtime_dir(temp.path(), sandbox.runtime_location, sandbox.id),
            temp.path().join(sandbox.id.to_string())
        );

        let mut warm_pool = SandboxInstance::new(
            BackendKind::Mock,
            blaze_core::policy::WorkloadClass::AgentTool,
            "sha256:warm-location".into(),
            StartPath::Warm,
            "warm-location".into(),
        );
        warm_pool.runtime_location = RuntimeLocation::WarmPool;
        warm_pool.persist(temp.path()).expect("persist warm owner");
        let warm_pool =
            SandboxInstance::load(temp.path(), warm_pool.id).expect("reload warm owner");
        assert_eq!(
            runtime_dir(temp.path(), warm_pool.runtime_location, warm_pool.id),
            temp.path()
                .join("runtime-pool")
                .join(warm_pool.id.to_string())
        );
    }

    #[tokio::test]
    async fn reconcile_rejects_cold_lifecycle_run_dir_collision() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id = Uuid::new_v4();
        let run_dir = write_ownership(
            &runtime_root,
            instance_id,
            BackendKind::Mock,
            BackendOwnership::Running,
            true,
        );
        let owner = DurableRuntimeOwner {
            instance_id,
            runtime_location: RuntimeLocation::Sandbox,
            backend: BackendKind::Mock,
            backend_ownership: BackendOwnership::Running,
            state: SandboxState::Running,
            operation: None,
            runtime_owner_token: None,
            clean_terminal: false,
        };
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([instance_id], events.clone());

        let error = reconcile_runtime_slots(
            &runtime_root,
            &HashMap::from([(instance_id, owner)]),
            &storage,
            &SpawnerRegistry::new(),
        )
        .await
        .expect_err("cold lifecycle must not adopt a pool run dir");

        assert!(error.to_string().contains("sandbox runtime owner"));
        assert!(events.lock().expect("events").is_empty());
        assert!(run_dir.exists());
    }

    #[tokio::test]
    async fn reconcile_rejects_transferred_backend_mismatch() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id = Uuid::new_v4();
        let run_dir = write_ownership(
            &runtime_root,
            instance_id,
            BackendKind::Mock,
            BackendOwnership::Running,
            true,
        );
        let owner = DurableRuntimeOwner {
            instance_id,
            runtime_location: RuntimeLocation::WarmPool,
            backend: BackendKind::Runc,
            backend_ownership: BackendOwnership::Running,
            state: SandboxState::Running,
            operation: None,
            runtime_owner_token: Some(Uuid::new_v4()),
            clean_terminal: false,
        };
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([instance_id], events.clone());

        let error = reconcile_runtime_slots(
            &runtime_root,
            &HashMap::from([(instance_id, owner)]),
            &storage,
            &SpawnerRegistry::new(),
        )
        .await
        .expect_err("backend mismatch must stop startup");

        let message = error.to_string();
        assert!(message.contains("records backend mock"));
        assert!(message.contains("durable lifecycle owns runc"));
        assert!(events.lock().expect("events").is_empty());
        assert!(run_dir.exists());
    }

    #[tokio::test]
    async fn reconcile_does_not_protect_destroyed_lifecycle_record() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id = Uuid::new_v4();
        let owner = DurableRuntimeOwner {
            instance_id,
            runtime_location: RuntimeLocation::Sandbox,
            backend: BackendKind::Mock,
            backend_ownership: BackendOwnership::Stopped,
            state: SandboxState::Destroyed,
            operation: None,
            runtime_owner_token: None,
            clean_terminal: true,
        };
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([instance_id], events.clone());

        let cleaned = reconcile_runtime_slots(
            &runtime_root,
            &HashMap::from([(instance_id, owner)]),
            &storage,
            &SpawnerRegistry::new(),
        )
        .await
        .expect("destroyed lifecycle record does not own runtime resources");

        assert_eq!(cleaned, 1);
        assert_eq!(
            *events.lock().expect("events"),
            vec![format!("storage:{instance_id}")]
        );
    }

    #[tokio::test]
    async fn reconcile_protects_nonterminal_destroyed_owners() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let live_id = Uuid::new_v4();
        let journal_id = Uuid::new_v4();
        let mut live = SandboxInstance::new(
            BackendKind::Mock,
            blaze_core::policy::WorkloadClass::AgentTool,
            "sha256:live-destroyed".into(),
            StartPath::Cold,
            "live-destroyed".into(),
        );
        live.id = live_id;
        live.transition(SandboxState::Destroyed).expect("destroyed");
        live.backend_ownership = BackendOwnership::Running;
        let mut journal = SandboxInstance::new(
            BackendKind::Mock,
            blaze_core::policy::WorkloadClass::AgentTool,
            "sha256:journal-destroyed".into(),
            StartPath::Cold,
            "journal-destroyed".into(),
        );
        journal.id = journal_id;
        journal
            .begin_operation(blaze_core::lifecycle::OperationKind::Destroy)
            .expect("destroy journal");
        journal
            .transition(SandboxState::Destroyed)
            .expect("destroyed");
        journal.backend_ownership = BackendOwnership::Stopped;
        let owners = HashMap::from([
            (live_id, DurableRuntimeOwner::from(&live)),
            (journal_id, DurableRuntimeOwner::from(&journal)),
        ]);
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([live_id, journal_id], events.clone());

        let cleaned =
            reconcile_runtime_slots(&runtime_root, &owners, &storage, &SpawnerRegistry::new())
                .await
                .expect("nonterminal lifecycle records retain cleanup ownership");

        assert_eq!(cleaned, 0);
        assert!(events.lock().expect("events").is_empty());
    }

    #[tokio::test]
    async fn reconcile_does_not_call_provider_without_recovery_contract() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id = Uuid::new_v4();
        let run_dir = write_ownership(
            &runtime_root,
            instance_id,
            BackendKind::Mock,
            BackendOwnership::Stopped,
            true,
        );
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage {
            owned: Mutex::new(BTreeSet::from([instance_id])),
            fail_once: Mutex::new(BTreeSet::new()),
            events: events.clone(),
            supports_recovery: false,
            ..RecordingStorage::default()
        };

        let error = reconcile_runtime_slots(
            &runtime_root,
            &HashMap::new(),
            &storage,
            &SpawnerRegistry::new(),
        )
        .await
        .expect_err("unsupported provider must stop before cleanup");

        assert!(
            error
                .to_string()
                .contains("cannot inventory slots for runtime recovery")
        );
        assert!(events.lock().expect("events").is_empty());
        assert!(run_dir.exists());
    }

    #[tokio::test]
    async fn reconcile_rejects_namespace_alias_before_cleanup() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id =
            Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").expect("fixed UUID");
        let alias = runtime_root.join(instance_id.to_string().to_uppercase());
        std::fs::create_dir_all(&alias).expect("alias run dir");
        let ownership = RuntimeSlotOwnership {
            version: OWNERSHIP_VERSION,
            instance_id,
            backend: BackendKind::Mock,
            backend_ownership: BackendOwnership::Running,
            storage_owned: true,
            phase: RuntimeSlotPhase::Building,
        };
        std::fs::write(
            alias.join(OWNERSHIP_FILE),
            serde_json::to_vec(&ownership).expect("ownership"),
        )
        .expect("alias ownership");
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([instance_id], events.clone());
        let spawners = registry(
            BackendKind::Mock,
            Arc::new(RecordingSpawner {
                events: events.clone(),
            }),
        );

        let error = reconcile_runtime_slots(&runtime_root, &HashMap::new(), &storage, &spawners)
            .await
            .expect_err("noncanonical alias must stop preflight");

        assert!(error.to_string().contains("canonical UUID"));
        assert!(events.lock().expect("events").is_empty());
        assert!(alias.exists());
        assert!(
            storage
                .owned
                .lock()
                .expect("owned slots")
                .contains(&instance_id)
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn reconcile_rejects_uuid_symlink_before_cleanup() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        std::fs::create_dir_all(&runtime_root).expect("runtime root");
        let instance_id = Uuid::new_v4();
        let target = tempfile::tempdir().expect("target");
        let alias = runtime_root.join(instance_id.to_string());
        symlink(target.path(), &alias).expect("runtime symlink");
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([instance_id], events.clone());

        let error = reconcile_runtime_slots(
            &runtime_root,
            &HashMap::new(),
            &storage,
            &SpawnerRegistry::new(),
        )
        .await
        .expect_err("runtime symlink must stop preflight");

        assert!(error.to_string().contains("non-directory runtime slot"));
        assert!(events.lock().expect("events").is_empty());
        assert!(
            std::fs::symlink_metadata(&alias)
                .expect("alias")
                .is_symlink()
        );
        assert!(target.path().is_dir());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn reconcile_rejects_symlinked_runtime_roots() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_target = tempfile::tempdir().expect("runtime target");
        let runtime_root = temp.path().join("runtime-pool");
        symlink(runtime_target.path(), &runtime_root).expect("runtime root symlink");
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([], events.clone());

        let runtime_error = reconcile_runtime_slots(
            &runtime_root,
            &HashMap::new(),
            &storage,
            &SpawnerRegistry::new(),
        )
        .await
        .expect_err("runtime root symlink must be rejected");

        assert!(runtime_error.to_string().contains("not a real directory"));
        assert!(events.lock().expect("events").is_empty());

        let real_runtime_root = temp.path().join("real-runtime-pool");
        std::fs::create_dir_all(&real_runtime_root).expect("real runtime root");
        let cleanup_target = tempfile::tempdir().expect("cleanup target");
        symlink(
            cleanup_target.path(),
            real_runtime_root.join(CLEANUP_NAMESPACE),
        )
        .expect("cleanup root symlink");

        let cleanup_error = reconcile_runtime_slots(
            &real_runtime_root,
            &HashMap::new(),
            &storage,
            &SpawnerRegistry::new(),
        )
        .await
        .expect_err("cleanup root symlink must be rejected");

        assert!(cleanup_error.to_string().contains("not a real directory"));
        assert!(events.lock().expect("events").is_empty());
        assert!(cleanup_target.path().is_dir());
    }

    #[tokio::test]
    async fn reconcile_rejects_inventory_error_before_backend_cleanup() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id = Uuid::new_v4();
        let run_dir = write_ownership(
            &runtime_root,
            instance_id,
            BackendKind::Mock,
            BackendOwnership::Running,
            true,
        );
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut storage = RecordingStorage::with_ids([instance_id], events.clone());
        storage.inventory_error = Some("injected inventory failure".to_string());
        let spawners = registry(
            BackendKind::Mock,
            Arc::new(RecordingSpawner {
                events: events.clone(),
            }),
        );

        let error = reconcile_runtime_slots(&runtime_root, &HashMap::new(), &storage, &spawners)
            .await
            .expect_err("inventory failure must stop preflight");

        assert!(error.to_string().contains("injected inventory failure"));
        assert!(events.lock().expect("events").is_empty());
        assert!(run_dir.exists());
    }

    #[tokio::test]
    async fn reconcile_rejects_noncanonical_provider_id_before_cleanup() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id =
            Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").expect("fixed UUID");
        let run_dir = write_ownership(
            &runtime_root,
            instance_id,
            BackendKind::Mock,
            BackendOwnership::Running,
            true,
        );
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut storage = RecordingStorage::with_ids([instance_id], events.clone());
        storage.inventory_override = Some(vec![instance_id.to_string().to_uppercase()]);
        let spawners = registry(
            BackendKind::Mock,
            Arc::new(RecordingSpawner {
                events: events.clone(),
            }),
        );

        let error = reconcile_runtime_slots(&runtime_root, &HashMap::new(), &storage, &spawners)
            .await
            .expect_err("provider alias must stop preflight");

        assert!(error.to_string().contains("canonical UUID"));
        assert!(events.lock().expect("events").is_empty());
        assert!(run_dir.exists());
    }

    #[tokio::test]
    async fn reconcile_resumes_tombstone_without_external_cleanup() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id = Uuid::new_v4();
        let tombstone = write_tombstone(&runtime_root, instance_id);
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([], events.clone());
        let spawners = registry(
            BackendKind::Mock,
            Arc::new(RecordingSpawner {
                events: events.clone(),
            }),
        );

        let cleaned = reconcile_runtime_slots(&runtime_root, &HashMap::new(), &storage, &spawners)
            .await
            .expect("resume tombstone");

        assert_eq!(cleaned, 1);
        assert!(events.lock().expect("events").is_empty());
        assert!(!tombstone.exists());
    }

    #[tokio::test]
    async fn reconcile_resumes_pool_tombstone_after_journal_unlink() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id = Uuid::new_v4();
        let tombstone = write_tombstone(&runtime_root, instance_id);
        let nested = tombstone.join("partially-removed");
        std::fs::create_dir_all(&nested).expect("partial directory");
        std::fs::write(nested.join("leftover"), b"data").expect("partial file");
        let proof = arm_test_deletion_proof(&runtime_root, instance_id, CleanupOwner::Pool).await;
        std::fs::remove_file(tombstone.join(OWNERSHIP_FILE)).expect("unlink journal");
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([], events.clone());

        let cleaned = reconcile_runtime_slots(
            &runtime_root,
            &HashMap::new(),
            &storage,
            &SpawnerRegistry::new(),
        )
        .await
        .expect("resume partial tombstone");

        assert_eq!(cleaned, 1);
        assert!(events.lock().expect("events").is_empty());
        assert!(!tombstone.exists());
        assert!(!proof.exists());
        assert_eq!(
            reconcile_runtime_slots(
                &runtime_root,
                &HashMap::new(),
                &storage,
                &SpawnerRegistry::new()
            )
            .await
            .expect("idempotent reconciliation"),
            0
        );
    }

    #[tokio::test]
    async fn reconcile_resumes_lifecycle_tombstone_after_journal_unlink() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id = Uuid::new_v4();
        let owner_token = Uuid::new_v4();
        let active = write_ownership_with_phase(
            &runtime_root,
            instance_id,
            BackendKind::Mock,
            BackendOwnership::Stopped,
            false,
            RuntimeSlotPhase::LifecycleCleanup { token: owner_token },
        );
        let cleanup_root = runtime_root.join(CLEANUP_NAMESPACE);
        std::fs::create_dir_all(&cleanup_root).expect("cleanup root");
        let tombstone = cleanup_root.join(instance_id.to_string());
        std::fs::rename(active, &tombstone).expect("tombstone lifecycle slot");
        let proof = arm_test_deletion_proof(
            &runtime_root,
            instance_id,
            CleanupOwner::Lifecycle(owner_token),
        )
        .await;
        std::fs::remove_file(tombstone.join(OWNERSHIP_FILE)).expect("unlink journal");
        let owner = DurableRuntimeOwner {
            instance_id,
            runtime_location: RuntimeLocation::WarmPool,
            backend: BackendKind::Mock,
            backend_ownership: BackendOwnership::Stopped,
            state: SandboxState::Destroyed,
            operation: None,
            runtime_owner_token: Some(owner_token),
            clean_terminal: true,
        };
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([], events.clone());

        let cleaned = reconcile_runtime_slots(
            &runtime_root,
            &HashMap::from([(instance_id, owner)]),
            &storage,
            &SpawnerRegistry::new(),
        )
        .await
        .expect("resume partial lifecycle tombstone");

        assert_eq!(cleaned, 1);
        assert!(events.lock().expect("events").is_empty());
        assert!(!tombstone.exists());
        assert!(!proof.exists());
    }

    #[tokio::test]
    async fn reconcile_finishes_deletion_when_only_the_proof_remains() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id = Uuid::new_v4();
        let tombstone = write_tombstone(&runtime_root, instance_id);
        let proof = arm_test_deletion_proof(&runtime_root, instance_id, CleanupOwner::Pool).await;
        std::fs::remove_dir_all(&tombstone).expect("simulate completed recursive deletion");
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([], events.clone());

        let cleaned = reconcile_runtime_slots(
            &runtime_root,
            &HashMap::new(),
            &storage,
            &SpawnerRegistry::new(),
        )
        .await
        .expect("finish deletion from proof");

        assert_eq!(cleaned, 1);
        assert!(events.lock().expect("events").is_empty());
        assert!(!tombstone.exists());
        assert!(!proof.exists());
    }

    #[tokio::test]
    async fn reconcile_rejects_partial_tombstone_without_deletion_proof() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id = Uuid::new_v4();
        let tombstone = write_tombstone(&runtime_root, instance_id);
        std::fs::remove_file(tombstone.join(OWNERSHIP_FILE)).expect("unlink journal");
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([], events.clone());

        let error = reconcile_runtime_slots(
            &runtime_root,
            &HashMap::new(),
            &storage,
            &SpawnerRegistry::new(),
        )
        .await
        .expect_err("missing deletion proof must stop recovery");

        assert!(error.to_string().contains("ownership.json"));
        assert!(events.lock().expect("events").is_empty());
        assert!(tombstone.exists());
    }

    #[tokio::test]
    async fn reconcile_rejects_mismatched_deletion_proof() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id = Uuid::new_v4();
        let other_id = Uuid::new_v4();
        let tombstone = write_tombstone(&runtime_root, instance_id);
        let other_tombstone = write_tombstone(&runtime_root, other_id);
        let cleanup_root = runtime_root.join(CLEANUP_NAMESPACE);
        let proof_root = cleanup_root.join(DELETION_PROOF_NAMESPACE);
        std::fs::create_dir_all(&proof_root).expect("proof root");
        let proof = proof_root.join(instance_id.to_string());
        std::fs::hard_link(other_tombstone.join(OWNERSHIP_FILE), &proof).expect("mismatched proof");
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([], events.clone());

        let error = reconcile_runtime_slots(
            &runtime_root,
            &HashMap::new(),
            &storage,
            &SpawnerRegistry::new(),
        )
        .await
        .expect_err("mismatched deletion proof must stop recovery");

        assert!(error.to_string().contains("records instance"));
        assert!(events.lock().expect("events").is_empty());
        assert!(tombstone.exists());
        assert!(proof.exists());
    }

    #[tokio::test]
    async fn reconcile_rejects_deletion_proof_that_differs_from_its_journal() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id = Uuid::new_v4();
        let tombstone = write_tombstone(&runtime_root, instance_id);
        let proof_root = runtime_root
            .join(CLEANUP_NAMESPACE)
            .join(DELETION_PROOF_NAMESPACE);
        std::fs::create_dir_all(&proof_root).expect("proof root");
        let proof = proof_root.join(instance_id.to_string());
        let mismatched = RuntimeSlotOwnership {
            version: OWNERSHIP_VERSION,
            instance_id,
            backend: BackendKind::Runc,
            backend_ownership: BackendOwnership::Stopped,
            storage_owned: false,
            phase: RuntimeSlotPhase::PoolCleanup,
        };
        std::fs::write(
            &proof,
            serde_json::to_vec(&mismatched).expect("proof ownership"),
        )
        .expect("mismatched proof");
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([], events.clone());

        let error = reconcile_runtime_slots(
            &runtime_root,
            &HashMap::new(),
            &storage,
            &SpawnerRegistry::new(),
        )
        .await
        .expect_err("proof and journal mismatch must stop recovery");

        assert!(error.to_string().contains("does not match"));
        assert!(events.lock().expect("events").is_empty());
        assert!(tombstone.exists());
        assert!(proof.exists());
    }

    #[tokio::test]
    async fn reconcile_rejects_partial_lifecycle_tombstone_with_wrong_token() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id = Uuid::new_v4();
        let proof_token = Uuid::new_v4();
        let durable_token = Uuid::new_v4();
        let active = write_ownership_with_phase(
            &runtime_root,
            instance_id,
            BackendKind::Mock,
            BackendOwnership::Stopped,
            false,
            RuntimeSlotPhase::LifecycleCleanup { token: proof_token },
        );
        let cleanup_root = runtime_root.join(CLEANUP_NAMESPACE);
        std::fs::create_dir_all(&cleanup_root).expect("cleanup root");
        let tombstone = cleanup_root.join(instance_id.to_string());
        std::fs::rename(active, &tombstone).expect("tombstone lifecycle slot");
        let proof = arm_test_deletion_proof(
            &runtime_root,
            instance_id,
            CleanupOwner::Lifecycle(proof_token),
        )
        .await;
        std::fs::remove_file(tombstone.join(OWNERSHIP_FILE)).expect("unlink journal");
        let owner = DurableRuntimeOwner {
            instance_id,
            runtime_location: RuntimeLocation::WarmPool,
            backend: BackendKind::Mock,
            backend_ownership: BackendOwnership::Stopped,
            state: SandboxState::Destroyed,
            operation: None,
            runtime_owner_token: Some(durable_token),
            clean_terminal: true,
        };
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([], events.clone());

        let error = reconcile_runtime_slots(
            &runtime_root,
            &HashMap::from([(instance_id, owner)]),
            &storage,
            &SpawnerRegistry::new(),
        )
        .await
        .expect_err("wrong lifecycle proof token must stop recovery");

        assert!(error.to_string().contains("cleanup token"));
        assert!(events.lock().expect("events").is_empty());
        assert!(tombstone.exists());
        assert!(proof.exists());
    }

    #[tokio::test]
    async fn reconcile_restores_but_defers_nonterminal_lifecycle_tombstone() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id = Uuid::new_v4();
        let owner_token = Uuid::new_v4();
        let active = write_ownership_with_phase(
            &runtime_root,
            instance_id,
            BackendKind::Mock,
            BackendOwnership::Stopped,
            false,
            RuntimeSlotPhase::LifecycleCleanup { token: owner_token },
        );
        let cleanup_root = runtime_root.join(CLEANUP_NAMESPACE);
        std::fs::create_dir_all(&cleanup_root).expect("cleanup root");
        let tombstone = cleanup_root.join(instance_id.to_string());
        std::fs::rename(active, &tombstone).expect("tombstone lifecycle slot");
        let proof = arm_test_deletion_proof(
            &runtime_root,
            instance_id,
            CleanupOwner::Lifecycle(owner_token),
        )
        .await;
        let journal = tombstone.join(OWNERSHIP_FILE);
        std::fs::remove_file(&journal).expect("unlink journal");
        let owner = DurableRuntimeOwner {
            instance_id,
            runtime_location: RuntimeLocation::WarmPool,
            backend: BackendKind::Mock,
            backend_ownership: BackendOwnership::Stopped,
            state: SandboxState::Destroyed,
            operation: Some(OperationKind::Destroy),
            runtime_owner_token: Some(owner_token),
            clean_terminal: false,
        };
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([], events.clone());

        let cleaned = reconcile_runtime_slots(
            &runtime_root,
            &HashMap::from([(instance_id, owner)]),
            &storage,
            &SpawnerRegistry::new(),
        )
        .await
        .expect("nonterminal lifecycle cleanup remains lifecycle-owned");

        assert_eq!(cleaned, 0);
        assert!(events.lock().expect("events").is_empty());
        assert!(tombstone.exists());
        assert!(proof.exists());
        assert!(journal.is_file());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn tombstone_removal_rejects_a_linked_cleanup_root() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        std::fs::create_dir_all(&runtime_root).expect("runtime root");
        let external = tempfile::tempdir().expect("external");
        let sentinel = external.path().join("keep");
        std::fs::write(&sentinel, b"keep").expect("sentinel");
        symlink(external.path(), runtime_root.join(CLEANUP_NAMESPACE)).expect("cleanup link");

        let error = remove_pool_tombstone(&runtime_root, Uuid::new_v4())
            .await
            .expect_err("linked cleanup root must be rejected");

        assert!(error.contains("not a real directory"));
        assert_eq!(std::fs::read(&sentinel).expect("sentinel remains"), b"keep");
        assert!(!external.path().join(DELETION_PROOF_NAMESPACE).exists());
    }

    #[tokio::test]
    async fn reconcile_resumes_tombstone_for_clean_terminal_owner() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id = Uuid::new_v4();
        let owner_token = Uuid::new_v4();
        let active = write_ownership_with_phase(
            &runtime_root,
            instance_id,
            BackendKind::Mock,
            BackendOwnership::Stopped,
            false,
            RuntimeSlotPhase::LifecycleCleanup { token: owner_token },
        );
        let cleanup_root = runtime_root.join(CLEANUP_NAMESPACE);
        std::fs::create_dir_all(&cleanup_root).expect("cleanup root");
        let tombstone = cleanup_root.join(instance_id.to_string());
        std::fs::rename(active, &tombstone).expect("tombstone lifecycle slot");
        let owner = DurableRuntimeOwner {
            instance_id,
            runtime_location: RuntimeLocation::WarmPool,
            backend: BackendKind::Mock,
            backend_ownership: BackendOwnership::Stopped,
            state: SandboxState::Destroyed,
            operation: None,
            runtime_owner_token: Some(owner_token),
            clean_terminal: true,
        };
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([], events.clone());

        let cleaned = reconcile_runtime_slots(
            &runtime_root,
            &HashMap::from([(instance_id, owner)]),
            &storage,
            &SpawnerRegistry::new(),
        )
        .await
        .expect("clean terminal owner no longer owns external resources");

        assert_eq!(cleaned, 1);
        assert!(events.lock().expect("events").is_empty());
        assert!(!tombstone.exists());
    }

    #[tokio::test]
    async fn reconcile_rejects_tombstone_alias_before_removal() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let cleanup_root = runtime_root.join(CLEANUP_NAMESPACE);
        let valid_id = Uuid::new_v4();
        let valid = write_tombstone(&runtime_root, valid_id);
        let alias_id = Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").expect("fixed UUID");
        let alias = cleanup_root.join(alias_id.to_string().to_uppercase());
        std::fs::create_dir_all(&alias).expect("alias tombstone");
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([], events.clone());

        let error = reconcile_runtime_slots(
            &runtime_root,
            &HashMap::new(),
            &storage,
            &SpawnerRegistry::new(),
        )
        .await
        .expect_err("tombstone alias must stop preflight");

        assert!(error.to_string().contains("canonical UUID"));
        assert!(events.lock().expect("events").is_empty());
        assert!(valid.exists());
        assert!(alias.exists());
    }

    #[tokio::test]
    async fn reconcile_preserves_tombstone_that_conflicts_with_storage() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id = Uuid::new_v4();
        let tombstone = write_tombstone(&runtime_root, instance_id);
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([instance_id], events.clone());

        let error = reconcile_runtime_slots(
            &runtime_root,
            &HashMap::new(),
            &storage,
            &SpawnerRegistry::new(),
        )
        .await
        .expect_err("storage and tombstone conflict must stop preflight");

        assert!(error.to_string().contains("tombstone conflicts"));
        assert!(events.lock().expect("events").is_empty());
        assert!(tombstone.exists());
    }

    #[tokio::test]
    async fn reconcile_preserves_tombstone_owned_by_lifecycle() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id = Uuid::new_v4();
        let tombstone = write_tombstone(&runtime_root, instance_id);
        let owner = DurableRuntimeOwner {
            instance_id,
            runtime_location: RuntimeLocation::WarmPool,
            backend: BackendKind::Mock,
            backend_ownership: BackendOwnership::Stopped,
            state: SandboxState::RecoveryRequired,
            operation: None,
            runtime_owner_token: Some(Uuid::new_v4()),
            clean_terminal: false,
        };
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([], events.clone());

        let error = reconcile_runtime_slots(
            &runtime_root,
            &HashMap::from([(instance_id, owner)]),
            &storage,
            &SpawnerRegistry::new(),
        )
        .await
        .expect_err("lifecycle and tombstone conflict must stop preflight");

        assert!(
            error
                .to_string()
                .contains("collides with durable lifecycle")
        );
        assert!(events.lock().expect("events").is_empty());
        assert!(tombstone.exists());
    }

    #[tokio::test(start_paused = true)]
    async fn reconcile_deadline_bounds_pending_inventory() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut storage = RecordingStorage::with_ids([], events.clone());
        storage.pending_inventory = true;
        let started = tokio::time::Instant::now();

        let error = reconcile_runtime_slots_until(
            started + Duration::from_secs(10),
            &runtime_root,
            &HashMap::new(),
            &storage,
            &SpawnerRegistry::new(),
        )
        .await
        .expect_err("pending inventory must time out");

        assert!(error.to_string().contains("shared startup deadline"));
        assert_eq!(started.elapsed(), Duration::from_secs(10));
        assert!(events.lock().expect("events").is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn reconcile_deadline_retains_storage_while_backend_is_pending() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id = Uuid::new_v4();
        let run_dir = write_ownership(
            &runtime_root,
            instance_id,
            BackendKind::Mock,
            BackendOwnership::Running,
            true,
        );
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([instance_id], events.clone());
        let spawners = registry(
            BackendKind::Mock,
            Arc::new(PendingSpawner {
                events: events.clone(),
            }),
        );
        let started = tokio::time::Instant::now();

        reconcile_runtime_slots_until(
            started + Duration::from_secs(10),
            &runtime_root,
            &HashMap::new(),
            &storage,
            &spawners,
        )
        .await
        .expect_err("pending backend must time out");

        assert_eq!(started.elapsed(), Duration::from_secs(10));
        assert_eq!(
            *events.lock().expect("events"),
            vec![format!("backend:{instance_id}")]
        );
        let ownership = read_ownership(&run_dir, instance_id)
            .await
            .expect("ownership retained");
        assert_eq!(ownership.backend_ownership, BackendOwnership::Running);
        assert!(ownership.storage_owned);
    }

    #[tokio::test(start_paused = true)]
    async fn reconcile_deadline_persists_backend_phase_before_pending_storage() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id = Uuid::new_v4();
        let run_dir = write_ownership(
            &runtime_root,
            instance_id,
            BackendKind::Mock,
            BackendOwnership::Running,
            true,
        );
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut storage = RecordingStorage::with_ids([instance_id], events.clone());
        storage.pending_release = true;
        let spawners = registry(
            BackendKind::Mock,
            Arc::new(RecordingSpawner {
                events: events.clone(),
            }),
        );
        let started = tokio::time::Instant::now();

        reconcile_runtime_slots_until(
            started + Duration::from_secs(10),
            &runtime_root,
            &HashMap::new(),
            &storage,
            &spawners,
        )
        .await
        .expect_err("pending storage must time out");

        assert_eq!(started.elapsed(), Duration::from_secs(10));
        assert_eq!(
            *events.lock().expect("events"),
            vec![
                format!("backend:{instance_id}"),
                format!("storage:{instance_id}")
            ]
        );
        let ownership = read_ownership(&run_dir, instance_id)
            .await
            .expect("ownership retained");
        assert_eq!(ownership.backend_ownership, BackendOwnership::Stopped);
        assert!(ownership.storage_owned);
    }

    #[tokio::test(start_paused = true)]
    async fn reconcile_uses_one_deadline_for_all_candidates() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let first = Uuid::parse_str("11111111-1111-4111-8111-111111111111").expect("first UUID");
        let second = Uuid::parse_str("22222222-2222-4222-8222-222222222222").expect("second UUID");
        let first_dir = write_ownership(
            &runtime_root,
            first,
            BackendKind::Mock,
            BackendOwnership::Running,
            false,
        );
        let second_dir = write_ownership(
            &runtime_root,
            second,
            BackendKind::Mock,
            BackendOwnership::Running,
            false,
        );
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([], events.clone());
        let spawners = registry(
            BackendKind::Mock,
            Arc::new(DelayedSpawner {
                delay: Duration::from_secs(6),
                events: events.clone(),
            }),
        );
        let started = tokio::time::Instant::now();

        reconcile_runtime_slots_until(
            started + Duration::from_secs(10),
            &runtime_root,
            &HashMap::new(),
            &storage,
            &spawners,
        )
        .await
        .expect_err("the second cleanup must share the first deadline");

        assert_eq!(started.elapsed(), Duration::from_secs(10));
        assert_eq!(
            *events.lock().expect("events"),
            vec![format!("backend:{first}"), format!("backend:{second}")]
        );
        assert!(!first_dir.exists());
        let second_ownership = read_ownership(&second_dir, second)
            .await
            .expect("second ownership retained");
        assert_eq!(
            second_ownership.backend_ownership,
            BackendOwnership::Running
        );
    }

    #[tokio::test]
    async fn reconcile_rejects_run_dir_without_ownership() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id = Uuid::new_v4();
        let run_dir = runtime_root.join(instance_id.to_string());
        std::fs::create_dir_all(&run_dir).expect("run dir");
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([instance_id], events.clone());

        let error = reconcile_runtime_slots(
            &runtime_root,
            &HashMap::new(),
            &storage,
            &SpawnerRegistry::new(),
        )
        .await
        .expect_err("missing ownership must stop startup");

        assert!(error.to_string().contains(&instance_id.to_string()));
        assert!(error.to_string().contains(OWNERSHIP_FILE));
        assert!(events.lock().expect("events").is_empty());
        assert!(run_dir.exists());
    }

    #[tokio::test]
    async fn reconcile_removes_empty_slot_left_before_first_journal() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id = Uuid::new_v4();
        let run_dir = runtime_root.join(instance_id.to_string());
        std::fs::create_dir_all(&run_dir).expect("run dir");
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([], events.clone());

        let cleaned = reconcile_runtime_slots(
            &runtime_root,
            &HashMap::new(),
            &storage,
            &SpawnerRegistry::new(),
        )
        .await
        .expect("empty pre-journal slot has no external owner");

        assert_eq!(cleaned, 1);
        assert!(events.lock().expect("events").is_empty());
        assert!(!run_dir.exists());
    }

    #[tokio::test]
    async fn reconcile_reports_corrupt_and_unregistered_owners() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let corrupt_id = Uuid::new_v4();
        let corrupt_dir = runtime_root.join(corrupt_id.to_string());
        std::fs::create_dir_all(&corrupt_dir).expect("corrupt run dir");
        std::fs::write(corrupt_dir.join(OWNERSHIP_FILE), b"{not-json").expect("corrupt ownership");
        let unregistered_id = Uuid::new_v4();
        write_ownership(
            &runtime_root,
            unregistered_id,
            BackendKind::Runc,
            BackendOwnership::Running,
            true,
        );
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([corrupt_id, unregistered_id], events.clone());

        let error = reconcile_runtime_slots(
            &runtime_root,
            &HashMap::new(),
            &storage,
            &SpawnerRegistry::new(),
        )
        .await
        .expect_err("ambiguous owners must stop startup");
        let message = error.to_string();

        assert!(message.contains(&corrupt_id.to_string()));
        assert!(message.contains("decode"));
        assert!(message.contains(&unregistered_id.to_string()));
        assert!(message.contains("no recovery spawner"));
        assert!(events.lock().expect("events").is_empty());
        assert!(corrupt_dir.exists());
    }

    #[tokio::test]
    async fn reconcile_persists_backend_cleanup_before_storage_retry() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id = Uuid::new_v4();
        let run_dir = write_ownership(
            &runtime_root,
            instance_id,
            BackendKind::Mock,
            BackendOwnership::Running,
            true,
        );
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([instance_id], events.clone());
        storage.fail_next_release(instance_id);
        let spawners = registry(
            BackendKind::Mock,
            Arc::new(RecordingSpawner {
                events: events.clone(),
            }),
        );

        reconcile_runtime_slots(&runtime_root, &HashMap::new(), &storage, &spawners)
            .await
            .expect_err("storage failure must retain the journal");

        assert_eq!(
            *events.lock().expect("events"),
            vec![
                format!("backend:{instance_id}"),
                format!("storage:{instance_id}")
            ]
        );
        let persisted = read_ownership(&run_dir, instance_id)
            .await
            .expect("persisted cleanup phase");
        assert_eq!(persisted.phase, RuntimeSlotPhase::PoolCleanup);
        assert_eq!(persisted.backend_ownership, BackendOwnership::Stopped);
        assert!(persisted.storage_owned);

        events.lock().expect("events").clear();
        let cleaned = reconcile_runtime_slots(
            &runtime_root,
            &HashMap::new(),
            &storage,
            &SpawnerRegistry::new(),
        )
        .await
        .expect("stopped backend needs no recovery spawner");

        assert_eq!(cleaned, 1);
        assert_eq!(
            *events.lock().expect("events"),
            vec![format!("storage:{instance_id}")]
        );
        assert!(!run_dir.exists());
    }

    #[tokio::test]
    async fn reconcile_continues_safe_cleanup_before_returning_error() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let good_id = Uuid::new_v4();
        let bad_id = Uuid::new_v4();
        let good_dir = write_ownership(
            &runtime_root,
            good_id,
            BackendKind::Mock,
            BackendOwnership::Stopped,
            true,
        );
        std::fs::create_dir_all(runtime_root.join(bad_id.to_string())).expect("bad run dir");
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([good_id, bad_id], events.clone());
        let spawners = registry(
            BackendKind::Mock,
            Arc::new(RecordingSpawner {
                events: events.clone(),
            }),
        );

        reconcile_runtime_slots(&runtime_root, &HashMap::new(), &storage, &spawners)
            .await
            .expect_err("one ambiguous slot must stop startup");

        assert_eq!(
            *events.lock().expect("events"),
            vec![format!("storage:{good_id}")]
        );
        assert!(!good_dir.exists());
        assert!(runtime_root.join(bad_id.to_string()).exists());
    }

    #[tokio::test]
    async fn reconcile_rejects_unknown_runtime_root_entry() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        std::fs::create_dir_all(&runtime_root).expect("runtime root");
        std::fs::write(runtime_root.join("unexpected"), b"not a slot").expect("entry");
        let storage = RecordingStorage::default();

        let error = reconcile_runtime_slots(
            &runtime_root,
            &HashMap::new(),
            &storage,
            &SpawnerRegistry::new(),
        )
        .await
        .expect_err("unknown entry must stop startup");

        assert!(error.to_string().contains("unexpected non-directory"));
    }

    #[tokio::test]
    async fn handoff_and_cleanup_transitions_are_idempotent_and_token_bound() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id = Uuid::new_v4();
        let owner_token = Uuid::new_v4();
        let run_dir = write_ownership_with_phase(
            &runtime_root,
            instance_id,
            BackendKind::Mock,
            BackendOwnership::NotStarted,
            true,
            RuntimeSlotPhase::Handoff { token: owner_token },
        );

        for _ in 0..2 {
            finish_ownership_handoff(
                &run_dir,
                instance_id,
                BackendKind::Mock,
                BackendOwnership::NotStarted,
                owner_token,
            )
            .await
            .expect("idempotent lifecycle handoff");
        }
        let wrong_token = Uuid::new_v4();
        begin_lifecycle_cleanup(&runtime_root, instance_id, BackendKind::Mock, wrong_token)
            .await
            .expect_err("wrong cleanup token");
        assert_eq!(
            read_ownership(&run_dir, instance_id)
                .await
                .expect("ownership after rejected token")
                .phase,
            RuntimeSlotPhase::LifecycleOwned { token: owner_token }
        );

        for _ in 0..2 {
            begin_lifecycle_cleanup(&runtime_root, instance_id, BackendKind::Mock, owner_token)
                .await
                .expect("idempotent lifecycle cleanup");
        }
        assert_eq!(
            read_ownership(&run_dir, instance_id)
                .await
                .expect("cleanup ownership")
                .phase,
            RuntimeSlotPhase::LifecycleCleanup { token: owner_token }
        );
    }

    #[tokio::test]
    async fn reconcile_cleans_an_uncommitted_handoff_as_pool_owned() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id = Uuid::new_v4();
        let run_dir = write_ownership_with_phase(
            &runtime_root,
            instance_id,
            BackendKind::Mock,
            BackendOwnership::NotStarted,
            true,
            RuntimeSlotPhase::Handoff {
                token: Uuid::new_v4(),
            },
        );
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([instance_id], events.clone());

        let cleaned = reconcile_runtime_slots(
            &runtime_root,
            &HashMap::new(),
            &storage,
            &SpawnerRegistry::new(),
        )
        .await
        .expect("uncommitted handoff remains pool-owned");

        assert_eq!(cleaned, 1);
        assert_eq!(
            *events.lock().expect("events"),
            vec![format!("storage:{instance_id}")]
        );
        assert!(!run_dir.exists());
    }

    #[tokio::test]
    async fn reconcile_rejects_lifecycle_marker_without_lifecycle_state() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let instance_id = Uuid::new_v4();
        let run_dir = write_ownership_with_phase(
            &runtime_root,
            instance_id,
            BackendKind::Mock,
            BackendOwnership::NotStarted,
            true,
            RuntimeSlotPhase::LifecycleOwned {
                token: Uuid::new_v4(),
            },
        );
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([instance_id], events.clone());

        let error = reconcile_runtime_slots(
            &runtime_root,
            &HashMap::new(),
            &storage,
            &SpawnerRegistry::new(),
        )
        .await
        .expect_err("lifecycle marker without state is ambiguous");

        assert!(error.to_string().contains("lifecycle ownership"));
        assert!(events.lock().expect("events").is_empty());
        assert!(run_dir.exists());
    }

    #[tokio::test]
    async fn reconcile_defers_a_valid_lifecycle_cleanup_tombstone() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime-pool");
        let cleanup_root = runtime_root.join(CLEANUP_NAMESPACE);
        let instance_id = Uuid::new_v4();
        let owner_token = Uuid::new_v4();
        let run_dir = write_ownership_with_phase(
            &runtime_root,
            instance_id,
            BackendKind::Mock,
            BackendOwnership::Stopped,
            false,
            RuntimeSlotPhase::LifecycleCleanup { token: owner_token },
        );
        std::fs::create_dir_all(&cleanup_root).expect("cleanup root");
        let tombstone = cleanup_root.join(instance_id.to_string());
        std::fs::rename(run_dir, &tombstone).expect("tombstone runtime");
        let owner = DurableRuntimeOwner {
            instance_id,
            runtime_location: RuntimeLocation::WarmPool,
            backend: BackendKind::Mock,
            backend_ownership: BackendOwnership::Stopped,
            state: SandboxState::Destroyed,
            operation: Some(OperationKind::Destroy),
            runtime_owner_token: Some(owner_token),
            clean_terminal: false,
        };
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = RecordingStorage::with_ids([], events.clone());

        let cleaned = reconcile_runtime_slots(
            &runtime_root,
            &HashMap::from([(instance_id, owner)]),
            &storage,
            &SpawnerRegistry::new(),
        )
        .await
        .expect("lifecycle must finish its own cleanup");

        assert_eq!(cleaned, 0);
        assert!(events.lock().expect("events").is_empty());
        assert!(tombstone.exists());
    }
}
