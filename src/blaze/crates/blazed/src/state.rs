// SPDX-License-Identifier: Apache-2.0
//! Daemon-wide shared state: configuration, policy engine, template and hook
//! registries, plus the sandbox runtime manager. All API handlers
//! receive an [`Arc<ServerState>`] and acquire the relevant `Mutex<...>`
//! lock just long enough to read or mutate the piece they need — locks
//! are never held across `.await` boundaries.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use blaze_core::backend::BackendKind;
use blaze_core::config::DaemonConfig;
use blaze_core::kernel::HookRegistry;
use blaze_core::lifecycle::SandboxInstance;
use blaze_core::policy::PolicyEngine;
use blaze_core::storage::StorageProvider;
use blaze_core::template::TemplateRegistry;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::error::Result;
use crate::metrics::Metrics;
use crate::sandbox::SandboxManager;
use crate::spawner::DynSpawner;

/// All daemon mutable state. Cloning is via `Arc` (see the `state.clone()`
/// idiom in `daemon.rs`); the struct itself is never `Clone`.
pub struct ServerState {
    pub config: Mutex<DaemonConfig>,
    pub policy: Mutex<PolicyEngine>,
    pub template: Mutex<TemplateRegistry>,
    pub hook: Mutex<HookRegistry>,
    pub manager: Arc<SandboxManager>,
    /// The backend kind that `build_spawner` actually probed and selected.
    /// API handlers use this to constrain availability to the single active
    /// backend rather than reporting all configured binaries.
    pub active_backend: BackendKind,
    pub metrics: Metrics,
}

impl ServerState {
    /// Build a server state, scanning `state_dir` to repopulate the
    /// `instances` map from previous runs. Corrupt UUID-owned state aborts
    /// startup so the daemon cannot silently orphan runtime resources.
    #[allow(clippy::too_many_arguments)]
    pub fn build(
        config: DaemonConfig,
        policy: PolicyEngine,
        template: TemplateRegistry,
        hook: HookRegistry,
        spawner: DynSpawner,
        active_backend: BackendKind,
        storage: Arc<dyn StorageProvider>,
    ) -> Result<Self> {
        let state_dir = config.daemon.state_dir.clone();
        let instances = scan_state_dir(&state_dir)?;

        let cancellation = CancellationToken::new();
        let manager = Arc::new(SandboxManager::new(
            config.clone(),
            instances,
            spawner,
            active_backend,
            storage,
            cancellation,
        )?);

        Ok(Self {
            config: Mutex::new(config),
            policy: Mutex::new(policy),
            template: Mutex::new(template),
            hook: Mutex::new(hook),
            manager,
            active_backend,
            metrics: Metrics::new(),
        })
    }
}

/// Walk `{state_dir}/<uuid>/state.json` and rebuild the instance map.
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
        let instance = SandboxInstance::load(state_dir, id)?;
        out.insert(id, instance);
    }
    tracing::info!(instances = out.len(), "rehydrated instances from state_dir");
    Ok(out)
}
