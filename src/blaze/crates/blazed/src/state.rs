// SPDX-License-Identifier: Apache-2.0
//! Daemon-wide shared state: configuration, policy engine, pool, template and
//! hook registries, plus the sandbox manager. API paths that change runtime
//! ownership enter through the manager so its per-instance lock spans every
//! asynchronous resource mutation.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

#[cfg(test)]
use std::path::PathBuf;

use blaze_core::backend::BackendKind;
use blaze_core::config::DaemonConfig;
use blaze_core::kernel::HookRegistry;
use blaze_core::lifecycle::SandboxInstance;
use blaze_core::policy::PolicyEngine;
use blaze_core::pool::PoolManager;
use blaze_core::storage::StorageProvider;
use blaze_core::template::TemplateRegistry;
use uuid::Uuid;

use crate::error::{BlazeDaemonError, Result};
use crate::metrics::Metrics;
use crate::sandbox::{SandboxManager, SandboxManagerInit};
use crate::spawner::SpawnerRegistry;

/// All daemon mutable state. Cloning is via `Arc` (see the `state.clone()`
/// idiom in `daemon.rs`); the struct itself is never `Clone`.
pub struct ServerState {
    pub config: Mutex<DaemonConfig>,
    pub policy: Mutex<PolicyEngine>,
    pub pool: Arc<Mutex<PoolManager>>,
    pub template: Mutex<TemplateRegistry>,
    pub hook: Mutex<HookRegistry>,
    #[cfg(test)]
    pub instances: Arc<Mutex<HashMap<Uuid, SandboxInstance>>>,
    pub manager: Arc<SandboxManager>,
    /// The backend kind that `build_spawner` actually probed and selected.
    /// API handlers use this to constrain availability to the single active
    /// backend rather than reporting all configured binaries.
    pub active_backend: BackendKind,
    pub storage: Arc<dyn StorageProvider>,
    #[cfg(test)]
    pub state_dir: PathBuf,
    pub metrics: Arc<Metrics>,
}

impl ServerState {
    /// Build a server state, scanning `state_dir` to repopulate the
    /// `instances` map from previous runs.
    #[allow(clippy::too_many_arguments)]
    pub fn build(
        config: DaemonConfig,
        policy: PolicyEngine,
        pool: PoolManager,
        template: TemplateRegistry,
        hook: HookRegistry,
        spawners: SpawnerRegistry,
        active_backend: BackendKind,
        storage: Arc<dyn StorageProvider>,
    ) -> Result<Self> {
        let state_dir = config.daemon.state_dir.clone();
        let instances = scan_state_dir(&state_dir)?;
        let (manager, resources) = SandboxManager::new(SandboxManagerInit {
            instances,
            pool,
            spawners,
            active_backend,
            storage: storage.clone(),
            state_dir: state_dir.clone(),
            rootfs_size: config.storage.rootfs_size,
            mem_size: config.storage.mem_size,
            pool_size: config.storage.pool_size,
            prefork: config.storage.prefork,
            default_warm_ttl: config.pool.default_warm_ttl.clone(),
            gc_interval: config.pool.gc_interval.clone(),
        })?;

        Ok(Self {
            config: Mutex::new(config),
            policy: Mutex::new(policy),
            pool: resources.pool,
            template: Mutex::new(template),
            hook: Mutex::new(hook),
            #[cfg(test)]
            instances: resources.instances,
            manager: Arc::new(manager),
            active_backend,
            storage,
            #[cfg(test)]
            state_dir,
            metrics: resources.metrics,
        })
    }

    /// Return the async operation lock that serializes one sandbox mutation.
    #[cfg(test)]
    pub fn operation_lock(&self, id: Uuid) -> Arc<tokio::sync::Mutex<()>> {
        self.manager.operation_lock(id)
    }
}

/// Walk `{state_dir}/<uuid>/state.json` and rebuild the instance map.
///
/// A valid UUID directory is owned lifecycle state. If its record cannot be
/// loaded, startup must stop rather than hide resources from later cleanup.
fn scan_state_dir(state_dir: &Path) -> Result<HashMap<Uuid, SandboxInstance>> {
    let mut out = HashMap::new();
    if !state_dir.exists() {
        return Ok(out);
    }
    for entry in std::fs::read_dir(state_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let Some(name_str) = name.to_str() else {
            continue;
        };
        let Ok(id) = Uuid::parse_str(name_str) else {
            continue;
        };
        let instance = SandboxInstance::load(state_dir, id).map_err(|error| {
            BlazeDaemonError::RecoveryRequired(format!(
                "cannot load persisted instance {id}: {error}"
            ))
        })?;
        if instance.id != id {
            return Err(BlazeDaemonError::RecoveryRequired(format!(
                "persisted instance id {} does not match owned directory {id}",
                instance.id
            )));
        }
        out.insert(id, instance);
    }
    tracing::info!(instances = out.len(), "rehydrated instances from state_dir");
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_ignores_runtime_pool_root() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_slot = temp
            .path()
            .join("runtime-pool")
            .join(Uuid::new_v4().to_string());
        std::fs::create_dir_all(&runtime_slot).expect("runtime slot");
        std::fs::write(runtime_slot.join("ownership.json"), b"{not-lifecycle-state")
            .expect("ownership marker");

        let instances = scan_state_dir(temp.path()).expect("scan state");

        assert!(instances.is_empty());
    }

    #[test]
    fn scan_rejects_corrupt_owned_state() {
        let temp = tempfile::tempdir().expect("tempdir");
        let id = Uuid::new_v4();
        let instance_dir = temp.path().join(id.to_string());
        std::fs::create_dir_all(&instance_dir).expect("instance dir");
        std::fs::write(instance_dir.join("state.json"), b"{not-json").expect("state");

        let error = scan_state_dir(temp.path()).expect_err("corrupt state must stop startup");

        assert!(matches!(
            error,
            BlazeDaemonError::RecoveryRequired(message)
                if message.contains(&id.to_string())
                    && message.contains("cannot load persisted instance")
        ));
    }
    #[test]
    fn scan_rejects_state_owned_by_a_different_directory() {
        let temp = tempfile::tempdir().expect("tempdir");
        let instance = SandboxInstance::new(
            BackendKind::Mock,
            blaze_core::policy::WorkloadClass::AgentTool,
            "sha256:mismatched-id".into(),
            blaze_core::lifecycle::StartPath::Cold,
            "test".into(),
        );
        instance.persist(temp.path()).expect("persist state");
        let directory_id = Uuid::new_v4();
        std::fs::rename(
            temp.path().join(instance.id.to_string()),
            temp.path().join(directory_id.to_string()),
        )
        .expect("rename directory");

        let error = scan_state_dir(temp.path()).expect_err("mismatched state must stop startup");

        assert!(matches!(
            error,
            BlazeDaemonError::RecoveryRequired(message)
                if message.contains(&instance.id.to_string())
                    && message.contains(&directory_id.to_string())
        ));
    }
}
