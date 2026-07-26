// SPDX-License-Identifier: Apache-2.0
//! UDS HTTP API server.
//!
//! Routing is a hand-rolled `match` on `(method, path-segments)` rather
//! than a router framework — the surface is small (~17 endpoints) and
//! the cost of a fresh dependency outweighs the readability win.

use std::collections::HashMap;
use std::convert::Infallible;
use std::str::FromStr;
use std::sync::Arc;

use blaze_core::backend::{
    BackendKind, BackendStatus, NetworkConfig, SpawnRequest, select_backend,
};
use blaze_core::kernel::HookKind;
use blaze_core::lifecycle::{SandboxInstance, SandboxState, StartPath};
use blaze_core::policy::{ImageMetadata, RuntimeDecision, WorkloadClass, parse_duration};
use blaze_core::pool::{PoolConfig, PoolKey};
use blaze_core::storage::AcquireOpts;
use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::header::CONTENT_TYPE;
use hyper::{Method, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::error::{BlazeDaemonError, Result};
use crate::state::ServerState;

/// Top-level request handler. Always returns `Ok(Response)`; internal
/// errors are turned into JSON error bodies so hyper never sees a panic.
pub async fn handle(
    req: Request<Incoming>,
    state: Arc<ServerState>,
) -> std::result::Result<Response<Full<Bytes>>, Infallible> {
    state.metrics.inc(&state.metrics.requests_total);

    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let query = req.uri().query().unwrap_or("").to_string();

    let response = match collect_body(req).await {
        Ok(body) => dispatch(&method, &path, &query, body, &state).await,
        Err(e) => Err(e),
    };

    let resp = match response {
        Ok(r) => r,
        Err(e) => error_response(&e),
    };
    Ok(resp)
}

async fn collect_body(req: Request<Incoming>) -> Result<Vec<u8>> {
    let collected = req.into_body().collect().await?;
    Ok(collected.to_bytes().to_vec())
}

async fn dispatch(
    method: &Method,
    path: &str,
    _query: &str,
    body: Vec<u8>,
    state: &Arc<ServerState>,
) -> Result<Response<Full<Bytes>>> {
    let parts: Vec<&str> = path
        .trim_start_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();
    let m = method.as_str();

    match (m, parts.as_slice()) {
        ("GET", ["v1", "health"]) => health(state),
        ("GET", ["v1", "instances"]) => list_instances(state),
        ("POST", ["v1", "instances"]) => create_instance(state, &body).await,
        ("GET", ["v1", "instances", id]) => get_instance(state, id),
        ("POST", ["v1", "instances", id, "checkpoint"]) => checkpoint(state, id),
        ("POST", ["v1", "instances", id, "reset"]) => reset_instance(state, id),
        ("POST", ["v1", "instances", id, "destroy"]) => destroy_instance(state, id).await,
        ("GET", ["v1", "pools"]) => list_pools(state),
        ("GET", ["v1", "pools", backend, class]) => pool_status(state, backend, class),
        ("POST", ["v1", "pools", backend, class, "drain"]) => drain_pool(state, backend, class),
        ("PUT", ["v1", "pools", backend, class, "sizing"]) => {
            resize_pool(state, backend, class, &body)
        }
        ("POST", ["v1", "templates", "gc"]) => gc_templates(state),
        ("GET", ["v1", "templates"]) => list_templates(state),
        ("GET", ["v1", "templates", id]) => inspect_template(state, id),
        ("GET", ["v1", "policies"]) => list_policies(state),
        ("GET", ["v1", "hooks"]) => list_hooks(state),
        ("GET", ["v1", "metrics"]) => metrics(state),
        ("POST", ["v1", "admin", "reload"]) => admin_reload(state),
        _ => Err(BlazeDaemonError::NotFound(format!("{method} {path}"))),
    }
}

// ---------------------------------------------------------------------------
// Health / metrics / admin
// ---------------------------------------------------------------------------

fn health(state: &Arc<ServerState>) -> Result<Response<Full<Bytes>>> {
    let pool_status = state.storage.pool_status();
    json_ok(&json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "storage_pool": pool_status,
    }))
}

fn metrics(state: &Arc<ServerState>) -> Result<Response<Full<Bytes>>> {
    let body = state.metrics.render();
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/plain; version=0.0.4")
        .body(Full::new(Bytes::from(body)))?)
}

fn admin_reload(state: &Arc<ServerState>) -> Result<Response<Full<Bytes>>> {
    let policy_dir = {
        let cfg = state
            .config
            .lock()
            .map_err(|_| BlazeDaemonError::Internal("config lock poisoned".into()))?;
        cfg.policy.dir.clone()
    };
    let new_engine = blaze_core::policy::PolicyEngine::load_dir(&policy_dir)?;
    let count = new_engine.policies().len();
    {
        let mut engine = state
            .policy
            .lock()
            .map_err(|_| BlazeDaemonError::Internal("policy lock poisoned".into()))?;
        *engine = new_engine;
    }
    tracing::info!(policies = count, "policy engine reloaded");
    json_ok(&json!({ "reloaded": true, "policies": count }))
}

// ---------------------------------------------------------------------------
// Instances
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct CreateInstanceReq {
    workload_class: WorkloadClass,
    image_digest: String,
    #[serde(default)]
    labels: HashMap<String, String>,
    #[serde(default)]
    kernel_version: Option<String>,
}

#[derive(Debug, Serialize)]
struct CreateInstanceResp {
    instance: SandboxInstance,
    decision: RuntimeDecision,
    start_path: StartPath,
    selected_backend: BackendKind,
}

fn list_instances(state: &Arc<ServerState>) -> Result<Response<Full<Bytes>>> {
    let map = state
        .instances
        .lock()
        .map_err(|_| BlazeDaemonError::Internal("instances lock poisoned".into()))?;
    let list: Vec<&SandboxInstance> = map.values().collect();
    json_ok(&list)
}

fn get_instance(state: &Arc<ServerState>, id: &str) -> Result<Response<Full<Bytes>>> {
    let uuid = parse_uuid(id)?;
    let map = state
        .instances
        .lock()
        .map_err(|_| BlazeDaemonError::Internal("instances lock poisoned".into()))?;
    let inst = map
        .get(&uuid)
        .ok_or_else(|| BlazeDaemonError::NotFound(format!("instance {uuid}")))?;
    json_ok(inst)
}

async fn create_instance(state: &Arc<ServerState>, body: &[u8]) -> Result<Response<Full<Bytes>>> {
    let req: CreateInstanceReq = serde_json::from_slice(body)
        .map_err(|e| BlazeDaemonError::BadRequest(format!("invalid create body: {e}")))?;

    let img = ImageMetadata {
        digest: req.image_digest.clone(),
        workload_class: Some(req.workload_class),
        kernel_version: req.kernel_version.clone(),
    };

    // 1. Policy evaluation.
    let decision = {
        let engine = state
            .policy
            .lock()
            .map_err(|_| BlazeDaemonError::Internal("policy lock poisoned".into()))?;
        match engine.evaluate(&req.labels, &img) {
            Ok(d) => d,
            Err(e) => {
                state.metrics.inc(&state.metrics.policy_eval_failures);
                return Err(e.into());
            }
        }
    };

    // 2. Backend selection. Constrain availability to the daemon's active
    // spawner — only the backend that was actually probed at boot can execute.
    let availability: Vec<BackendStatus> = {
        let cfg = state
            .config
            .lock()
            .map_err(|_| BlazeDaemonError::Internal("config lock poisoned".into()))?;
        decision
            .backend_priority
            .iter()
            .map(|kind| {
                let available = *kind == state.active_backend
                    && (state.active_backend == BackendKind::Mock
                        || cfg
                            .backends
                            .get(kind.as_str())
                            .map(|p| p.exists())
                            .unwrap_or(false));
                BackendStatus {
                    kind: *kind,
                    available,
                    version: None,
                }
            })
            .collect()
    };
    // Select backend from available options. If no match is found:
    // - Mock mode: fall back to the first policy entry (dev convenience)
    // - Real backend: propagate BackendUnavailable (policy does not permit
    //   the active backend, refusing to silently bypass policy)
    let backend = match select_backend(&decision.backend_priority, &availability) {
        Ok(b) => b,
        Err(e) => {
            if state.active_backend == BackendKind::Mock {
                *decision.backend_priority.first().ok_or_else(|| {
                    BlazeDaemonError::Internal("policy has empty backend_priority".into())
                })?
            } else {
                return Err(e.into());
            }
        }
    };

    // 3. Pool lookup.
    let pool_key = PoolKey::new(backend, decision.workload_class, req.image_digest.clone());
    let mut start_path = StartPath::Cold;
    let mut reused: Option<Uuid> = None;
    if decision.pool_eligible {
        let mut pool = state
            .pool
            .lock()
            .map_err(|_| BlazeDaemonError::Internal("pool lock poisoned".into()))?;
        if let Some(id) = pool.lookup(&pool_key) {
            reused = Some(id);
            start_path = StartPath::Warm;
            state.metrics.inc(&state.metrics.pool_hits);
        } else {
            state.metrics.inc(&state.metrics.pool_misses);
        }
    }

    // 4. A warm hit already owns its backend and storage. Claiming it from
    // the pool must not allocate another slot or start another backend.
    if let Some(id) = reused {
        let (instance, actual_backend) =
            activate_warm_instance(state, pool_key.clone(), id).await?;
        state.metrics.inc(&state.metrics.instances_created);
        return json_created(&CreateInstanceResp {
            instance,
            decision,
            start_path,
            selected_backend: actual_backend,
        });
    }

    let mut instance = SandboxInstance::new(
        backend,
        decision.workload_class,
        req.image_digest.clone(),
        StartPath::Cold,
        decision.policy_name.clone(),
    );
    instance.transition(SandboxState::Creating)?;

    // 5. Spawn the data-plane process via the BackendSpawner trait.
    //    The daemon picks LinuxSandboxSpawner when the configured
    //    backend binary exists; otherwise MockSpawner keeps the daemon
    //    usable on macOS dev hosts and in CI.
    let (binary_path, rootfs_size, mem_size) = {
        let cfg = state
            .config
            .lock()
            .map_err(|_| BlazeDaemonError::Internal("config lock poisoned".into()))?;
        (
            cfg.backends
                .get(state.active_backend.as_str())
                .cloned()
                .unwrap_or_else(std::path::PathBuf::new),
            cfg.storage.rootfs_size,
            cfg.storage.mem_size,
        )
    };
    let storage = state
        .storage
        .acquire(&AcquireOpts {
            instance_id: instance.id.to_string(),
            rootfs_size,
            mem_size,
        })
        .await?;
    let work_dir = state.state_dir.join(instance.id.to_string());
    let spawner = state.spawner.clone();
    let actual_backend = match spawner
        .spawn(SpawnRequest {
            instance_id: instance.id,
            run_dir: work_dir,
            binary_path,
            storage: storage.clone(),
            backend: decision.backend.clone(),
            vm: decision.vm.clone(),
            network: decision
                .backend
                .firecracker
                .as_ref()
                .map(|config| NetworkConfig {
                    enabled: config.enable_network,
                    ..NetworkConfig::default()
                }),
        })
        .await
    {
        Ok(backend_instance) => {
            let real_backend = backend_instance.backend();
            let mut backend_instance = Some(backend_instance);
            let registered = match state.backend_instances.lock() {
                Ok(mut instances) => {
                    instances.insert(
                        instance.id,
                        backend_instance
                            .take()
                            .expect("backend instance is present"),
                    );
                    true
                }
                Err(_) => false,
            };
            if !registered {
                let original = BlazeDaemonError::Internal("backend_instances lock poisoned".into());
                return Err(cleanup_failed_create(
                    state,
                    &mut instance,
                    storage,
                    backend_instance,
                    false,
                    original,
                )
                .await);
            }
            real_backend
        }
        Err(err) => {
            tracing::error!(instance = %instance.id, ?err, "spawn failed");
            return Err(cleanup_failed_create(
                state,
                &mut instance,
                storage,
                None,
                false,
                err.into(),
            )
            .await);
        }
    };
    if let Err(error) = instance.transition(SandboxState::Running) {
        return Err(
            cleanup_failed_create(state, &mut instance, storage, None, true, error.into()).await,
        );
    }
    if let Err(error) = instance.persist(&state.state_dir) {
        return Err(
            cleanup_failed_create(state, &mut instance, storage, None, true, error.into()).await,
        );
    }

    // 6. Done.
    let inserted = match state.instances.lock() {
        Ok(mut map) => {
            map.insert(instance.id, instance.clone());
            true
        }
        Err(_) => false,
    };
    if !inserted {
        let original = BlazeDaemonError::Internal("instances lock poisoned".into());
        return Err(
            cleanup_failed_create(state, &mut instance, storage, None, true, original).await,
        );
    }
    state.metrics.inc(&state.metrics.instances_created);

    json_created(&CreateInstanceResp {
        instance,
        decision,
        start_path,
        selected_backend: actual_backend,
    })
}

async fn activate_warm_instance(
    state: &Arc<ServerState>,
    pool_key: PoolKey,
    id: Uuid,
) -> Result<(SandboxInstance, BackendKind)> {
    let original = match state.instances.lock() {
        Ok(mut instances) => match instances.remove(&id) {
            Some(instance) => instance,
            None => {
                return Err(restore_warm_activation(
                    state,
                    pool_key,
                    id,
                    None,
                    BlazeDaemonError::RecoveryRequired(format!(
                        "warm instance {id} is missing lifecycle state"
                    )),
                ));
            }
        },
        Err(_) => {
            return Err(restore_warm_activation(
                state,
                pool_key,
                id,
                None,
                BlazeDaemonError::Internal("instances lock poisoned".into()),
            ));
        }
    };

    if original.state != SandboxState::Warm {
        let original_state = original.state;
        return Err(restore_warm_activation(
            state,
            pool_key,
            id,
            Some(original),
            BlazeDaemonError::RecoveryRequired(format!(
                "warm instance {id} has lifecycle state {original_state}"
            )),
        ));
    }

    let actual_backend = match state.backend_instances.lock() {
        Ok(instances) => match instances.get(&id) {
            Some(backend) => backend.backend(),
            None => {
                return Err(restore_warm_activation(
                    state,
                    pool_key,
                    id,
                    Some(original),
                    BlazeDaemonError::RecoveryRequired(format!(
                        "warm instance {id} is missing its backend owner"
                    )),
                ));
            }
        },
        Err(_) => {
            return Err(restore_warm_activation(
                state,
                pool_key,
                id,
                Some(original),
                BlazeDaemonError::Internal("backend_instances lock poisoned".into()),
            ));
        }
    };

    if let Err(error) = state.storage.reconstruct(&id.to_string()).await {
        return Err(restore_warm_activation(
            state,
            pool_key,
            id,
            Some(original),
            error.into(),
        ));
    }

    let mut activated = original.clone();
    if let Err(error) = activated.transition(SandboxState::Creating) {
        return Err(restore_warm_activation(
            state,
            pool_key,
            id,
            Some(original),
            error.into(),
        ));
    }
    if let Err(error) = activated.transition(SandboxState::Running) {
        return Err(restore_warm_activation(
            state,
            pool_key,
            id,
            Some(original),
            error.into(),
        ));
    }
    if let Err(error) = activated.persist(&state.state_dir) {
        return Err(restore_warm_activation(
            state,
            pool_key,
            id,
            Some(original),
            error.into(),
        ));
    }

    match state.instances.lock() {
        Ok(mut instances) => {
            instances.insert(id, activated.clone());
        }
        Err(_) => {
            return Err(restore_warm_activation(
                state,
                pool_key,
                id,
                Some(original),
                BlazeDaemonError::Internal("instances lock poisoned".into()),
            ));
        }
    }
    Ok((activated, actual_backend))
}

fn restore_warm_activation(
    state: &Arc<ServerState>,
    pool_key: PoolKey,
    id: Uuid,
    original: Option<SandboxInstance>,
    cause: BlazeDaemonError,
) -> BlazeDaemonError {
    let mut errors = Vec::new();
    if let Some(instance) = original {
        let instance_id = instance.id;
        if let Err(error) = instance.persist(&state.state_dir) {
            errors.push(format!("restore warm state persistence failed: {error}"));
        }
        match state.instances.lock() {
            Ok(mut instances) => {
                instances.insert(instance_id, instance);
            }
            Err(poisoned) => {
                poisoned.into_inner().insert(instance_id, instance);
                errors.push("instances map lock poisoned while restoring warm claim".to_string());
            }
        }
        match state.pool.lock() {
            Ok(mut pool) => pool.restore_lookup(pool_key, instance_id),
            Err(poisoned) => {
                poisoned.into_inner().restore_lookup(pool_key, instance_id);
                errors.push("pool lock poisoned while restoring warm claim".to_string());
            }
        }
    } else {
        match state.pool.lock() {
            Ok(mut pool) => pool.restore_lookup(pool_key, id),
            Err(poisoned) => {
                poisoned.into_inner().restore_lookup(pool_key, id);
                errors.push("pool lock poisoned while restoring warm claim".to_string());
            }
        }
    }
    let details = if errors.is_empty() {
        "warm claim restored".to_string()
    } else {
        errors.join("; ")
    };
    BlazeDaemonError::RecoveryRequired(format!("{cause}; {details}"))
}

async fn cleanup_failed_create(
    state: &Arc<ServerState>,
    instance: &mut SandboxInstance,
    storage: blaze_core::storage::StorageSlot,
    backend: Option<crate::spawner::DynBackendInstance>,
    registered: bool,
    original: BlazeDaemonError,
) -> BlazeDaemonError {
    let mut cleanup_errors = Vec::new();
    let backend = if registered {
        match state.backend_instances.lock() {
            Ok(mut instances) => instances.remove(&instance.id),
            Err(poisoned) => poisoned.into_inner().remove(&instance.id),
        }
    } else {
        backend
    };
    let mut backend_stopped = !registered || backend.is_some();
    if registered && backend.is_none() {
        backend_stopped = false;
        cleanup_errors.push("registered backend owner is missing".to_string());
    }
    if let Some(backend) = backend.as_ref()
        && let Err(error) = backend.kill().await
    {
        backend_stopped = false;
        cleanup_errors.push(format!("backend termination failed: {error}"));
    }

    let mut storage_released = false;
    if backend_stopped {
        match state.storage.release(storage).await {
            Ok(()) => storage_released = true,
            Err(error) => cleanup_errors.push(format!("storage release failed: {error}")),
        }
    } else {
        cleanup_errors.push("storage retained until backend termination succeeds".to_string());
    }

    if backend_stopped && storage_released {
        if let Err(error) = instance.transition(SandboxState::Destroyed) {
            let mut recovery_errors = vec![format!("lifecycle update failed: {error}")];
            if let Err(persist_error) = instance.persist(&state.state_dir) {
                recovery_errors.push(format!("state persistence failed: {persist_error}"));
            }
            if let Some(retain_error) = retain_instance_state(state, instance.clone()) {
                recovery_errors.push(retain_error);
            }
            return BlazeDaemonError::RecoveryRequired(format!(
                "{original}; cleanup completed but {}",
                recovery_errors.join("; ")
            ));
        }
        if let Err(error) = instance.persist(&state.state_dir) {
            let mut recovery_errors = vec![format!("state persistence failed: {error}")];
            if let Some(retain_error) = retain_instance_state(state, instance.clone()) {
                recovery_errors.push(retain_error);
            }
            return BlazeDaemonError::RecoveryRequired(format!(
                "{original}; cleanup completed but {}",
                recovery_errors.join("; ")
            ));
        }
        state.metrics.inc(&state.metrics.instances_destroyed);
        return original;
    }

    if let Some(backend) = backend
        && let Some(error) = retain_backend_owner(state, instance.id, backend)
    {
        cleanup_errors.push(error);
    }
    if let Err(error) = instance.persist(&state.state_dir) {
        cleanup_errors.push(format!("state persistence failed: {error}"));
    }
    if let Some(error) = retain_instance_state(state, instance.clone()) {
        cleanup_errors.push(error);
    }
    BlazeDaemonError::RecoveryRequired(format!(
        "{original}; cleanup incomplete: {}",
        cleanup_errors.join("; ")
    ))
}

fn retain_backend_owner(
    state: &Arc<ServerState>,
    id: Uuid,
    backend: crate::spawner::DynBackendInstance,
) -> Option<String> {
    match state.backend_instances.lock() {
        Ok(mut instances) => {
            instances.insert(id, backend);
            None
        }
        Err(poisoned) => {
            poisoned.into_inner().insert(id, backend);
            Some("backend owner retained in poisoned runtime map".to_string())
        }
    }
}

fn retain_instance_state(state: &Arc<ServerState>, instance: SandboxInstance) -> Option<String> {
    match state.instances.lock() {
        Ok(mut instances) => {
            instances.insert(instance.id, instance);
            None
        }
        Err(poisoned) => {
            poisoned.into_inner().insert(instance.id, instance);
            Some("instance state retained in poisoned lifecycle map".to_string())
        }
    }
}

fn checkpoint(state: &Arc<ServerState>, id: &str) -> Result<Response<Full<Bytes>>> {
    let uuid = parse_uuid(id)?;
    let mut map = state
        .instances
        .lock()
        .map_err(|_| BlazeDaemonError::Internal("instances lock poisoned".into()))?;
    let inst = map
        .get_mut(&uuid)
        .ok_or_else(|| BlazeDaemonError::NotFound(format!("instance {uuid}")))?;

    if inst.state == SandboxState::Running {
        inst.transition(SandboxState::Paused)?;
    }
    inst.transition(SandboxState::Checkpointed)?;
    inst.persist(&state.state_dir)?;

    let checkpoint_id = format!("ckpt-{}-{}", inst.id, chrono::Utc::now().timestamp());
    json_ok(&json!({
        "checkpoint_id": checkpoint_id,
        "instance_id": inst.id,
    }))
}

fn reset_instance(state: &Arc<ServerState>, id: &str) -> Result<Response<Full<Bytes>>> {
    let uuid = parse_uuid(id)?;
    let mut map = state
        .instances
        .lock()
        .map_err(|_| BlazeDaemonError::Internal("instances lock poisoned".into()))?;
    let inst = map
        .get_mut(&uuid)
        .ok_or_else(|| BlazeDaemonError::NotFound(format!("instance {uuid}")))?;
    // TODO(v0.2): perform actual data-plane reset (full-recreate or
    // mm-template rollback per policy reset_mode) before returning to
    // pool. Current implementation is control-plane state only.
    inst.transition(SandboxState::Reset)?;
    inst.transition(SandboxState::Warm)?;
    inst.persist(&state.state_dir)?;

    // return to pool keyed on (backend, class, image_digest)
    let key = PoolKey::new(inst.backend, inst.workload_class, inst.image_digest.clone());
    let inst_id = inst.id;
    let snapshot = inst.clone();
    drop(map);
    {
        let mut pool = state
            .pool
            .lock()
            .map_err(|_| BlazeDaemonError::Internal("pool lock poisoned".into()))?;
        pool.return_to_pool(key, inst_id);
    }
    state.metrics.inc(&state.metrics.instances_resets);
    json_ok(&snapshot)
}

async fn destroy_instance(state: &Arc<ServerState>, id: &str) -> Result<Response<Full<Bytes>>> {
    let uuid = parse_uuid(id)?;
    let original = state
        .instances
        .lock()
        .map_err(|_| BlazeDaemonError::Internal("instances lock poisoned".into()))?
        .get(&uuid)
        .cloned()
        .ok_or_else(|| BlazeDaemonError::NotFound(format!("instance {uuid}")))?;
    let backend = {
        let instances = state
            .backend_instances
            .lock()
            .map_err(|_| BlazeDaemonError::Internal("backend_instances lock poisoned".into()))?;
        instances.get(&uuid).cloned()
    };

    let stop_result = if let Some(backend) = backend.as_ref() {
        backend.kill().await
    } else {
        state
            .spawner
            .cleanup_orphan(uuid, &state.state_dir.join(uuid.to_string()))
            .await
    };
    if let Err(error) = stop_result {
        return Err(BlazeDaemonError::RecoveryRequired(format!(
            "destroy {uuid}: backend termination failed: {error}; owner and storage retained"
        )));
    }

    if let Err(error) = state.storage.release_by_id(&uuid.to_string()).await {
        return Err(BlazeDaemonError::RecoveryRequired(format!(
            "destroy {uuid}: backend stopped but storage release failed: {error}; owner retained for retry"
        )));
    }

    let mut destroyed = original;
    if destroyed.state != SandboxState::Destroyed
        && let Err(error) = destroyed.transition(SandboxState::Destroyed)
    {
        return Err(BlazeDaemonError::RecoveryRequired(format!(
            "destroy {uuid}: resources released but lifecycle update failed: {error}"
        )));
    }
    if let Err(error) = destroyed.persist(&state.state_dir) {
        return Err(BlazeDaemonError::RecoveryRequired(format!(
            "destroy {uuid}: resources released but state persistence failed: {error}"
        )));
    }

    let mut recovery_errors = Vec::new();
    match state.instances.lock() {
        Ok(mut instances) => {
            instances.insert(uuid, destroyed.clone());
        }
        Err(poisoned) => {
            poisoned.into_inner().insert(uuid, destroyed.clone());
            recovery_errors.push("lifecycle map lock poisoned".to_string());
        }
    }
    match state.backend_instances.lock() {
        Ok(mut instances) => {
            instances.remove(&uuid);
        }
        Err(poisoned) => {
            poisoned.into_inner().remove(&uuid);
            recovery_errors.push("runtime owner map lock poisoned".to_string());
        }
    }
    if !recovery_errors.is_empty() {
        return Err(BlazeDaemonError::RecoveryRequired(format!(
            "destroy {uuid}: resources released and state persisted but {}",
            recovery_errors.join("; ")
        )));
    }

    // Provider artifacts are released, while run_dir logs and configuration
    // remain available for post-mortem diagnostics.
    state.metrics.inc(&state.metrics.instances_destroyed);
    json_ok(&json!({
        "destroyed": true,
        "instance_id": destroyed.id,
    }))
}

// ---------------------------------------------------------------------------
// Pools
// ---------------------------------------------------------------------------

fn list_pools(state: &Arc<ServerState>) -> Result<Response<Full<Bytes>>> {
    let pool = state
        .pool
        .lock()
        .map_err(|_| BlazeDaemonError::Internal("pool lock poisoned".into()))?;
    let listed: Vec<_> = pool
        .list_pools()
        .into_iter()
        .map(|(k, s)| {
            json!({
                "key": {
                    "backend": k.backend.as_str(),
                    "workload_class": k.workload_class.as_str(),
                    "image_digest": k.image_digest,
                },
                "stats": s,
            })
        })
        .collect();
    json_ok(&listed)
}

fn pool_status(
    state: &Arc<ServerState>,
    backend: &str,
    class: &str,
) -> Result<Response<Full<Bytes>>> {
    let pool = state
        .pool
        .lock()
        .map_err(|_| BlazeDaemonError::Internal("pool lock poisoned".into()))?;
    let backend_kind = BackendKind::from_str(backend)
        .map_err(|e| BlazeDaemonError::BadRequest(format!("backend: {e}")))?;
    let class_kind = WorkloadClass::from_str(class)
        .map_err(|e| BlazeDaemonError::BadRequest(format!("class: {e}")))?;

    let listed: Vec<_> = pool
        .list_pools()
        .into_iter()
        .filter(|(k, _)| k.backend == backend_kind && k.workload_class == class_kind)
        .map(|(k, s)| {
            json!({
                "key": {
                    "backend": k.backend.as_str(),
                    "workload_class": k.workload_class.as_str(),
                    "image_digest": k.image_digest,
                },
                "stats": s,
            })
        })
        .collect();
    json_ok(&listed)
}

fn drain_pool(
    state: &Arc<ServerState>,
    backend: &str,
    class: &str,
) -> Result<Response<Full<Bytes>>> {
    let backend_kind = BackendKind::from_str(backend)
        .map_err(|e| BlazeDaemonError::BadRequest(format!("backend: {e}")))?;
    let class_kind = WorkloadClass::from_str(class)
        .map_err(|e| BlazeDaemonError::BadRequest(format!("class: {e}")))?;
    // TODO(v0.2): after removing instance IDs from the pool, walk
    // spawn_handles and kill the underlying processes so that drain
    // actually frees host resources.
    let drained = {
        let mut pool = state
            .pool
            .lock()
            .map_err(|_| BlazeDaemonError::Internal("pool lock poisoned".into()))?;
        pool.drain(backend_kind, class_kind)
    };
    json_ok(&json!({
        "drained": drained,
        "count": drained.len(),
    }))
}

#[derive(Debug, Deserialize)]
struct ResizeReq {
    #[serde(default)]
    enabled: Option<bool>,
    min: u32,
    target: u32,
    max: u32,
    #[serde(default)]
    image_digest: Option<String>,
    #[serde(default)]
    warm_ttl_secs: Option<u64>,
}

fn resize_pool(
    state: &Arc<ServerState>,
    backend: &str,
    class: &str,
    body: &[u8],
) -> Result<Response<Full<Bytes>>> {
    let req: ResizeReq = serde_json::from_slice(body)
        .map_err(|e| BlazeDaemonError::BadRequest(format!("invalid resize body: {e}")))?;
    let backend_kind = BackendKind::from_str(backend)
        .map_err(|e| BlazeDaemonError::BadRequest(format!("backend: {e}")))?;
    let class_kind = WorkloadClass::from_str(class)
        .map_err(|e| BlazeDaemonError::BadRequest(format!("class: {e}")))?;
    let key = PoolKey::new(
        backend_kind,
        class_kind,
        req.image_digest.clone().unwrap_or_default(),
    );
    let cfg = PoolConfig {
        enabled: req.enabled.unwrap_or(true),
        min: req.min,
        target: req.target,
        max: req.max,
        warm_ttl: std::time::Duration::from_secs(req.warm_ttl_secs.unwrap_or(30 * 60)),
        reset_mode: blaze_core::policy::ResetMode::default(),
    };
    {
        let mut pool = state
            .pool
            .lock()
            .map_err(|_| BlazeDaemonError::Internal("pool lock poisoned".into()))?;
        pool.resize(&key, cfg);
    }
    json_ok(&json!({
        "resized": true,
        "backend": backend,
        "class": class,
    }))
}

// ---------------------------------------------------------------------------
// Templates
// ---------------------------------------------------------------------------

fn list_templates(state: &Arc<ServerState>) -> Result<Response<Full<Bytes>>> {
    let reg = state
        .template
        .lock()
        .map_err(|_| BlazeDaemonError::Internal("template lock poisoned".into()))?;
    json_ok(&reg.list())
}

fn inspect_template(state: &Arc<ServerState>, id: &str) -> Result<Response<Full<Bytes>>> {
    let uuid = parse_uuid(id)?;
    let reg = state
        .template
        .lock()
        .map_err(|_| BlazeDaemonError::Internal("template lock poisoned".into()))?;
    let view = reg
        .inspect(uuid)
        .ok_or_else(|| BlazeDaemonError::NotFound(format!("template {uuid}")))?;
    json_ok(&view)
}

fn gc_templates(state: &Arc<ServerState>) -> Result<Response<Full<Bytes>>> {
    let idle_ttl = {
        let cfg = state
            .config
            .lock()
            .map_err(|_| BlazeDaemonError::Internal("config lock poisoned".into()))?;
        parse_duration(&cfg.template.idle_ttl).unwrap_or(std::time::Duration::from_secs(3600))
    };
    let collected = {
        let mut reg = state
            .template
            .lock()
            .map_err(|_| BlazeDaemonError::Internal("template lock poisoned".into()))?;
        reg.gc_unused(idle_ttl)
    };
    json_ok(&json!({
        "collected": collected,
        "count": collected.len(),
    }))
}

// ---------------------------------------------------------------------------
// Policies / hooks
// ---------------------------------------------------------------------------

fn list_policies(state: &Arc<ServerState>) -> Result<Response<Full<Bytes>>> {
    let engine = state
        .policy
        .lock()
        .map_err(|_| BlazeDaemonError::Internal("policy lock poisoned".into()))?;
    let names: Vec<_> = engine
        .policies()
        .iter()
        .map(|p| {
            json!({
                "name": p.policy_name,
                "priority": p.priority,
                "workload_class": p.match_.workload_class.as_str(),
            })
        })
        .collect();
    json_ok(&names)
}

fn list_hooks(state: &Arc<ServerState>) -> Result<Response<Full<Bytes>>> {
    let reg = state
        .hook
        .lock()
        .map_err(|_| BlazeDaemonError::Internal("hook lock poisoned".into()))?;
    json_ok(&reg.list())
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn parse_uuid(s: &str) -> Result<Uuid> {
    Uuid::parse_str(s).map_err(|e| BlazeDaemonError::BadRequest(format!("invalid uuid: {e}")))
}

fn json_ok<T: Serialize>(value: &T) -> Result<Response<Full<Bytes>>> {
    json_response(StatusCode::OK, value)
}

fn json_created<T: Serialize>(value: &T) -> Result<Response<Full<Bytes>>> {
    json_response(StatusCode::CREATED, value)
}

fn json_response<T: Serialize>(status: StatusCode, value: &T) -> Result<Response<Full<Bytes>>> {
    let body = serde_json::to_vec_pretty(value)?;
    Ok(Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(body)))?)
}

fn error_response(err: &BlazeDaemonError) -> Response<Full<Bytes>> {
    let status =
        StatusCode::from_u16(err.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let body = json!({
        "error": err.to_string(),
        "status": status.as_u16(),
    });
    let bytes = serde_json::to_vec_pretty(&body)
        .unwrap_or_else(|_| br#"{"error":"serialize_failed"}"#.to_vec());
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(bytes)))
        .unwrap_or_else(|_| {
            // Hyper's builder can fail on invalid header values; this branch
            // should be unreachable. Fall back to a status-only response.
            Response::new(Full::new(Bytes::from_static(b"{}")))
        })
}

// Keep the unused-import lint quiet when `HookKind` is gated behind
// future-only hook registration paths.
#[allow(dead_code)]
fn _hookkind_marker(_k: HookKind) {}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use blaze_core::backend::{
        BackendKind, FlushResult, SnapshotRequest, SnapshotResult, SpawnRequest,
    };
    use blaze_core::config::DaemonConfig;
    use blaze_core::kernel::HookRegistry;
    use blaze_core::policy::{
        BackendConfigs, FallbackOnMissingHook, PolicyEngine, PolicyFile, PolicyHooks, PolicyMatch,
        PolicyPool, PolicySelect, WorkloadClass,
    };
    use blaze_core::pool::PoolManager;
    use blaze_core::template::TemplateRegistry;
    use blaze_core::{BlazeError, Result as CoreResult};

    use crate::file_provider::FileStorageProvider;
    use crate::spawner::{
        BackendInstance, BackendSpawner, DynBackendInstance, MockSpawner, SpawnResult,
    };
    use crate::state::ServerState;

    use super::*;

    /// When multiple backend binaries exist on disk but the daemon probed
    /// Firecracker at boot, only Firecracker should be reported available
    /// and selected — even if policy prioritizes bubblewrap higher.
    #[tokio::test]
    async fn availability_constrained_to_active_backend() {
        // Create temp files to simulate both binaries existing.
        let tmp = std::env::temp_dir().join("blaze-test-active-backend");
        let _ = std::fs::create_dir_all(&tmp);
        let fc_bin = tmp.join("firecracker");
        let bwrap_bin = tmp.join("bwrap");
        std::fs::write(&fc_bin, b"fake-fc").unwrap();
        std::fs::write(&bwrap_bin, b"fake-bwrap").unwrap();

        // Minimal config with both backends present.
        let mut config = DaemonConfig::default();
        config.daemon.state_dir = tmp.join("state");
        config.storage.rootfs_size = 1024;
        config.storage.mem_size = 512;
        let _ = std::fs::create_dir_all(&config.daemon.state_dir);
        config.backends.insert("firecracker".into(), fc_bin.clone());
        config
            .backends
            .insert("bubblewrap".into(), bwrap_bin.clone());

        // Policy that prioritizes bubblewrap over firecracker.
        let policy_file = PolicyFile {
            manifest_version: 1,
            policy_name: "test-multi-backend".into(),
            priority: 100,
            match_: PolicyMatch {
                workload_class: WorkloadClass::AgentRl,
                image_labels: HashMap::new(),
            },
            select: PolicySelect {
                backend_priority: vec![BackendKind::Bubblewrap, BackendKind::Firecracker],
                kernel_hooks: vec![],
                templates: vec![],
                fallback_on_missing_hook: FallbackOnMissingHook::default(),
            },
            pool: None,
            checkpoint: None,
            quota: None,
            hooks: PolicyHooks::default(),
            backend: BackendConfigs::default(),
            vm: None,
        };
        let engine = PolicyEngine::with_policies(vec![policy_file]);

        // Build state with active_backend = Firecracker (simulating probe
        // selected FC at boot) but using MockSpawner for test portability.
        let spawner: crate::spawner::DynSpawner = Arc::new(MockSpawner);
        let storage_dir = tmp.join("storage");
        let _ = std::fs::create_dir_all(&storage_dir);
        let storage: Arc<dyn blaze_core::storage::StorageProvider> =
            Arc::new(FileStorageProvider::new(storage_dir));
        let state = Arc::new(ServerState::build(
            config,
            engine,
            PoolManager::new(),
            TemplateRegistry::new(),
            HookRegistry::new(),
            spawner,
            BackendKind::Firecracker,
            storage,
        ));

        // Create instance request for AgentRl workload.
        let req_body = serde_json::to_vec(&serde_json::json!({
            "workload_class": "agent-rl",
            "image_digest": "sha256:abc123",
        }))
        .unwrap();

        let resp = create_instance(&state, &req_body).await.unwrap();
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let resp_json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        // The instance should be created with backend = firecracker,
        // NOT bubblewrap (even though bwrap was higher priority in policy)
        // because only the active backend is reported as available.
        assert_eq!(
            resp_json["instance"]["backend"].as_str().unwrap(),
            "firecracker",
            "instance backend should be the active backend (firecracker), \
             not the higher-priority bubblewrap"
        );

        // Cleanup.
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn spawn_failure_releases_acquired_storage() {
        let temp = tempfile::tempdir().expect("temp");
        let state_dir = temp.path().join("state");
        let storage_dir = temp.path().join("storage");
        std::fs::create_dir_all(&state_dir).expect("state dir");
        std::fs::create_dir_all(&storage_dir).expect("storage dir");
        let state = test_state(
            &state_dir,
            &storage_dir,
            Arc::new(FailingSpawner),
            BackendKind::Mock,
        );

        let error = create_instance(&state, &create_request())
            .await
            .expect_err("spawn must fail");

        assert!(error.to_string().contains("injected spawn failure"));
        assert!(directory_is_empty(&storage_dir));
        let persisted = only_persisted_instance(&state_dir);
        assert_eq!(persisted.state, SandboxState::Destroyed);
    }

    #[tokio::test]
    async fn backend_registration_failure_cleans_backend_and_storage() {
        let temp = tempfile::tempdir().expect("temp");
        let state_dir = temp.path().join("state");
        let storage_dir = temp.path().join("storage");
        std::fs::create_dir_all(&state_dir).expect("state dir");
        std::fs::create_dir_all(&storage_dir).expect("storage dir");
        let state = test_state(
            &state_dir,
            &storage_dir,
            Arc::new(MockSpawner),
            BackendKind::Mock,
        );
        let poison_target = state.clone();
        std::thread::spawn(move || {
            let _guard = poison_target
                .backend_instances
                .lock()
                .expect("backend map lock");
            panic!("poison backend map");
        })
        .join()
        .expect_err("poison thread must panic");

        let error = create_instance(&state, &create_request())
            .await
            .expect_err("registration must fail");

        assert!(
            error
                .to_string()
                .contains("backend_instances lock poisoned")
        );
        assert!(directory_is_empty(&storage_dir));
        let persisted = only_persisted_instance(&state_dir);
        assert_eq!(persisted.state, SandboxState::Destroyed);
        assert!(
            !state_dir
                .join(persisted.id.to_string())
                .join("vsock.uds")
                .exists()
        );
    }

    #[tokio::test]
    async fn warm_reuse_preserves_backend_and_storage() {
        let temp = tempfile::tempdir().expect("temp");
        let state_dir = temp.path().join("state");
        let storage_dir = temp.path().join("storage");
        std::fs::create_dir_all(&state_dir).expect("state dir");
        std::fs::create_dir_all(&storage_dir).expect("storage dir");
        let spawner = ControlledSpawner::new(0, None);
        let state = test_state_with_policy(
            &state_dir,
            &storage_dir,
            Arc::new(spawner.clone()),
            BackendKind::Mock,
            test_pool_policy(),
        );

        let first = response_json(
            create_instance(&state, &create_request())
                .await
                .expect("cold create"),
        )
        .await;
        let id =
            Uuid::parse_str(first["instance"]["id"].as_str().expect("instance id")).expect("uuid");
        reset_instance(&state, &id.to_string()).expect("reset");
        assert_eq!(
            state
                .pool
                .lock()
                .expect("pool")
                .stats(&pool_key())
                .warm_count,
            1
        );

        let second = response_json(
            create_instance(&state, &create_request())
                .await
                .expect("warm create"),
        )
        .await;

        let id_string = id.to_string();
        assert_eq!(second["instance"]["id"].as_str(), Some(id_string.as_str()));
        assert_eq!(second["start_path"].as_str(), Some("warm"));
        assert_eq!(spawner.spawn_count(), 1);
        assert_eq!(state.backend_instances.lock().expect("backends").len(), 1);
        assert!(storage_dir.join(id.to_string()).is_dir());
        assert_eq!(
            state
                .pool
                .lock()
                .expect("pool")
                .stats(&pool_key())
                .warm_count,
            0
        );

        destroy_instance(&state, &id.to_string())
            .await
            .expect("destroy reused instance");
    }

    #[tokio::test]
    async fn create_map_failure_keeps_owner_and_storage_when_kill_fails() {
        let temp = tempfile::tempdir().expect("temp");
        let state_dir = temp.path().join("state");
        let storage_dir = temp.path().join("storage");
        std::fs::create_dir_all(&state_dir).expect("state dir");
        std::fs::create_dir_all(&storage_dir).expect("storage dir");
        let spawner = ControlledSpawner::new(1, None);
        let state = test_state(
            &state_dir,
            &storage_dir,
            Arc::new(spawner),
            BackendKind::Mock,
        );
        let poison_target = state.clone();
        std::thread::spawn(move || {
            let _guard = poison_target.instances.lock().expect("instances map lock");
            panic!("poison instances map");
        })
        .join()
        .expect_err("poison thread must panic");

        let error = create_instance(&state, &create_request())
            .await
            .expect_err("map update must fail");
        assert!(matches!(error, BlazeDaemonError::RecoveryRequired(_)));

        let id = {
            let retained = state
                .instances
                .lock()
                .expect_err("instances map remains poisoned")
                .into_inner();
            let (id, instance) = retained.iter().next().expect("retained instance");
            assert_eq!(instance.state, SandboxState::Running);
            assert!(storage_dir.join(id.to_string()).is_dir());
            assert!(
                state
                    .backend_instances
                    .lock()
                    .expect("backends")
                    .contains_key(id)
            );
            *id
        };
        state.instances.clear_poison();

        destroy_instance(&state, &id.to_string())
            .await
            .expect("retry destroy");
        assert!(!storage_dir.join(id.to_string()).exists());
    }

    #[tokio::test]
    async fn create_persist_failure_rolls_back_backend_and_storage() {
        let temp = tempfile::tempdir().expect("temp");
        let state_dir = temp.path().join("state");
        let storage_dir = temp.path().join("storage");
        std::fs::create_dir_all(&state_dir).expect("state dir");
        std::fs::create_dir_all(&storage_dir).expect("storage dir");
        let spawner = ControlledSpawner::new(0, Some(state_dir.clone()));
        let state = test_state(
            &state_dir,
            &storage_dir,
            Arc::new(spawner),
            BackendKind::Mock,
        );

        let error = create_instance(&state, &create_request())
            .await
            .expect_err("persist must fail");

        assert!(matches!(error, BlazeDaemonError::RecoveryRequired(_)));
        assert!(directory_is_empty(&storage_dir));
        assert!(state.backend_instances.lock().expect("backends").is_empty());
        let instances = state.instances.lock().expect("instances");
        assert_eq!(instances.len(), 1);
        assert_eq!(
            instances.values().next().expect("retained state").state,
            SandboxState::Destroyed
        );
    }

    #[tokio::test]
    async fn destroy_failure_retains_owner_state_and_storage_for_retry() {
        let temp = tempfile::tempdir().expect("temp");
        let state_dir = temp.path().join("state");
        let storage_dir = temp.path().join("storage");
        std::fs::create_dir_all(&state_dir).expect("state dir");
        std::fs::create_dir_all(&storage_dir).expect("storage dir");
        let spawner = ControlledSpawner::new(1, None);
        let state = test_state(
            &state_dir,
            &storage_dir,
            Arc::new(spawner),
            BackendKind::Mock,
        );
        let created = response_json(
            create_instance(&state, &create_request())
                .await
                .expect("create"),
        )
        .await;
        let id = Uuid::parse_str(created["instance"]["id"].as_str().expect("instance id"))
            .expect("uuid");

        let error = destroy_instance(&state, &id.to_string())
            .await
            .expect_err("first termination must fail");

        assert!(matches!(error, BlazeDaemonError::RecoveryRequired(_)));
        assert_eq!(
            state
                .instances
                .lock()
                .expect("instances")
                .get(&id)
                .expect("instance")
                .state,
            SandboxState::Running
        );
        assert!(
            state
                .backend_instances
                .lock()
                .expect("backends")
                .contains_key(&id)
        );
        assert!(storage_dir.join(id.to_string()).is_dir());

        destroy_instance(&state, &id.to_string())
            .await
            .expect("retry destroy");
        assert_eq!(
            state
                .instances
                .lock()
                .expect("instances")
                .get(&id)
                .expect("instance")
                .state,
            SandboxState::Destroyed
        );
        assert!(
            !state
                .backend_instances
                .lock()
                .expect("backends")
                .contains_key(&id)
        );
        assert!(!storage_dir.join(id.to_string()).exists());
    }

    struct FailingSpawner;

    #[async_trait]
    impl BackendSpawner for FailingSpawner {
        async fn spawn(&self, _request: SpawnRequest) -> CoreResult<DynBackendInstance> {
            Err(BlazeError::BackendError {
                msg: "injected spawn failure".to_string(),
            })
        }

        async fn probe(&self, _binary_path: &std::path::Path) -> CoreResult<bool> {
            Ok(true)
        }
    }

    #[derive(Clone)]
    struct ControlledSpawner {
        spawns: Arc<AtomicUsize>,
        kill_failures: Arc<AtomicUsize>,
        sabotage_state_dir: Option<PathBuf>,
    }

    impl ControlledSpawner {
        fn new(kill_failures: usize, sabotage_state_dir: Option<PathBuf>) -> Self {
            Self {
                spawns: Arc::new(AtomicUsize::new(0)),
                kill_failures: Arc::new(AtomicUsize::new(kill_failures)),
                sabotage_state_dir,
            }
        }

        fn spawn_count(&self) -> usize {
            self.spawns.load(Ordering::Acquire)
        }
    }

    #[async_trait]
    impl BackendSpawner for ControlledSpawner {
        async fn spawn(&self, request: SpawnRequest) -> CoreResult<DynBackendInstance> {
            self.spawns.fetch_add(1, Ordering::AcqRel);
            let backend = MockSpawner.spawn(request).await?;
            if let Some(state_dir) = self.sabotage_state_dir.as_ref() {
                tokio::fs::remove_dir_all(state_dir).await?;
                tokio::fs::write(state_dir, b"not a directory").await?;
            }
            Ok(Arc::new(ControlledInstance {
                backend,
                kill_failures: self.kill_failures.clone(),
            }))
        }

        async fn probe(&self, _binary_path: &Path) -> CoreResult<bool> {
            Ok(true)
        }
    }

    struct ControlledInstance {
        backend: DynBackendInstance,
        kill_failures: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl BackendInstance for ControlledInstance {
        fn instance_id(&self) -> Uuid {
            self.backend.instance_id()
        }

        fn backend(&self) -> BackendKind {
            self.backend.backend()
        }

        fn version(&self) -> Option<&str> {
            self.backend.version()
        }

        fn pid(&self) -> Option<u32> {
            self.backend.pid()
        }

        fn guest_socket_path(&self) -> &Path {
            self.backend.guest_socket_path()
        }

        async fn wait(&self) -> CoreResult<SpawnResult> {
            self.backend.wait().await
        }

        async fn pause(&self) -> CoreResult<()> {
            self.backend.pause().await
        }

        async fn resume(&self) -> CoreResult<()> {
            self.backend.resume().await
        }

        async fn snapshot(&self, request: SnapshotRequest) -> CoreResult<SnapshotResult> {
            self.backend.snapshot(request).await
        }

        async fn flush_dirty(&self) -> CoreResult<FlushResult> {
            self.backend.flush_dirty().await
        }

        async fn kill(&self) -> CoreResult<()> {
            let injected = self
                .kill_failures
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok();
            if injected {
                return Err(BlazeError::BackendError {
                    msg: "injected backend termination failure".to_string(),
                });
            }
            self.backend.kill().await
        }
    }

    fn test_state(
        state_dir: &std::path::Path,
        storage_dir: &std::path::Path,
        spawner: crate::spawner::DynSpawner,
        active_backend: BackendKind,
    ) -> Arc<ServerState> {
        test_state_with_policy(
            state_dir,
            storage_dir,
            spawner,
            active_backend,
            test_policy(),
        )
    }

    fn test_state_with_policy(
        state_dir: &std::path::Path,
        storage_dir: &std::path::Path,
        spawner: crate::spawner::DynSpawner,
        active_backend: BackendKind,
        policy: PolicyFile,
    ) -> Arc<ServerState> {
        let mut config = DaemonConfig::default();
        config.daemon.state_dir = state_dir.to_path_buf();
        config.storage.rootfs_size = 1024;
        config.storage.mem_size = 512;
        let storage: Arc<dyn blaze_core::storage::StorageProvider> =
            Arc::new(FileStorageProvider::new(storage_dir.to_path_buf()));
        Arc::new(ServerState::build(
            config,
            PolicyEngine::with_policies(vec![policy]),
            PoolManager::new(),
            TemplateRegistry::new(),
            HookRegistry::new(),
            spawner,
            active_backend,
            storage,
        ))
    }

    fn test_policy() -> PolicyFile {
        PolicyFile {
            manifest_version: 1,
            policy_name: "test-create-cleanup".into(),
            priority: 100,
            match_: PolicyMatch {
                workload_class: WorkloadClass::AgentRl,
                image_labels: HashMap::new(),
            },
            select: PolicySelect {
                backend_priority: vec![BackendKind::Mock],
                kernel_hooks: vec![],
                templates: vec![],
                fallback_on_missing_hook: FallbackOnMissingHook::default(),
            },
            pool: None,
            checkpoint: None,
            quota: None,
            hooks: PolicyHooks::default(),
            backend: BackendConfigs::default(),
            vm: None,
        }
    }

    fn test_pool_policy() -> PolicyFile {
        let mut policy = test_policy();
        policy.pool = Some(PolicyPool {
            enabled: true,
            min: 0,
            target: 1,
            max: 1,
            warm_ttl: "30m".to_string(),
            reset_mode: Default::default(),
        });
        policy
    }

    fn pool_key() -> PoolKey {
        PoolKey::new(
            BackendKind::Mock,
            WorkloadClass::AgentRl,
            "sha256:test".to_string(),
        )
    }

    fn create_request() -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "workload_class": "agent-rl",
            "image_digest": "sha256:test",
        }))
        .expect("request")
    }

    async fn response_json(response: Response<Full<Bytes>>) -> serde_json::Value {
        let body = response
            .into_body()
            .collect()
            .await
            .expect("collect response")
            .to_bytes();
        serde_json::from_slice(&body).expect("response json")
    }

    fn directory_is_empty(path: &std::path::Path) -> bool {
        std::fs::read_dir(path)
            .expect("read directory")
            .next()
            .is_none()
    }

    fn only_persisted_instance(state_dir: &std::path::Path) -> SandboxInstance {
        let mut entries = std::fs::read_dir(state_dir).expect("read state dir");
        let entry = entries
            .next()
            .expect("one persisted instance")
            .expect("state entry");
        assert!(entries.next().is_none());
        let id = Uuid::parse_str(entry.file_name().to_str().expect("utf-8 id")).expect("uuid");
        SandboxInstance::load(state_dir, id).expect("load state")
    }
}
