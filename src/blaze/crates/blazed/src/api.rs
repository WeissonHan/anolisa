// SPDX-License-Identifier: Apache-2.0
//! UDS HTTP API server.
//!
//! Routing is a hand-rolled `match` on `(method, path-segments)` rather
//! than a router framework — the surface is small (~17 endpoints) and
//! the cost of a fresh dependency outweighs the readability win.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::convert::Infallible;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use blaze_core::backend::{BackendKind, BackendStatus, select_backend};
use blaze_core::kernel::HookKind;
use blaze_core::lifecycle::{SandboxInstance, SandboxState, StartPath};
use blaze_core::policy::{ImageMetadata, RuntimeDecision, WorkloadClass, parse_duration};
use blaze_core::pool::{PoolConfig, PoolKey};
use http_body_util::Full;
use hyper::body::{Body, Bytes, Incoming};
use hyper::header::CONTENT_TYPE;
use hyper::{Method, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::task::JoinSet;
use tokio::time::Instant;
use uuid::Uuid;

use crate::error::{BlazeDaemonError, Result};
use crate::guest::MAX_GUEST_FILE_BYTES;
use crate::request_body;
use crate::sandbox::{
    CreateSandbox, HibernateSandbox, RestoreSandbox, RestoreSandboxResult, ResumeSandbox,
};
use crate::state::ServerState;

const MAX_EXEC_TIMEOUT_SECS: u32 = 20;

/// Top-level request handler. Always returns `Ok(Response)`; internal
/// errors are turned into JSON error bodies so hyper never sees a panic.
pub async fn handle(
    req: Request<Incoming>,
    state: Arc<ServerState>,
) -> std::result::Result<Response<Full<Bytes>>, Infallible> {
    handle_request(req, state).await
}

async fn handle_request<B>(
    req: Request<B>,
    state: Arc<ServerState>,
) -> std::result::Result<Response<Full<Bytes>>, Infallible>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: std::fmt::Display,
{
    state.metrics.inc(&state.metrics.requests_total);

    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let query = req.uri().query().unwrap_or("").to_string();

    let limit = state
        .config
        .lock()
        .map(|config| config.api.max_body_bytes)
        .map_err(|_| BlazeDaemonError::Internal("config lock poisoned".into()));
    let response = match limit {
        Ok(limit) => match request_body::collect(req, limit).await {
            Ok(body) => dispatch(&method, &path, &query, body, &state).await,
            Err(error) => Err(error),
        },
        Err(error) => Err(error),
    };

    let resp = match response {
        Ok(r) => r,
        Err(e) => error_response(&e),
    };
    Ok(resp)
}

const fn max_base64_len(decoded_bytes: usize) -> usize {
    decoded_bytes
        .saturating_add(2)
        .saturating_div(3)
        .saturating_mul(4)
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
        ("GET", ["v1", "instances"]) | ("GET", ["v1", "sandboxes"]) => list_instances(state),
        ("POST", ["v1", "instances"]) | ("POST", ["v1", "sandboxes"]) => {
            create_instance(state, &body).await
        }
        ("GET", ["v1", "instances", id]) | ("GET", ["v1", "sandboxes", id]) => {
            get_instance(state, id)
        }
        ("POST", ["v1", "sandboxes", id, "exec"]) | ("POST", ["v1", "instances", id, "exec"]) => {
            exec_instance(state, id, &body).await
        }
        ("POST", ["v1", "sandboxes", id, "read"]) | ("POST", ["v1", "instances", id, "read"]) => {
            read_instance_file(state, id, &body).await
        }
        ("POST", ["v1", "sandboxes", id, "write"]) | ("POST", ["v1", "instances", id, "write"]) => {
            write_instance_file(state, id, &body).await
        }
        ("POST", ["v1", "instances", id, "checkpoint"])
        | ("POST", ["v1", "sandboxes", id, "checkpoint"]) => checkpoint(state, id).await,
        ("GET", ["v1", "instances", id, "checkpoints"])
        | ("GET", ["v1", "sandboxes", id, "checkpoints"]) => list_checkpoints(state, id).await,
        ("POST", ["v1", "instances", id, "rollback", checkpoint_id])
        | ("POST", ["v1", "sandboxes", id, "rollback", checkpoint_id]) => {
            rollback(state, id, checkpoint_id).await
        }
        ("POST", ["v1", "instances", id, "hibernate"])
        | ("POST", ["v1", "sandboxes", id, "hibernate"]) => hibernate(state, id).await,
        ("POST", ["v1", "instances", id, "resume"])
        | ("POST", ["v1", "sandboxes", id, "resume"]) => resume(state, id).await,
        ("POST", ["v1", "instances", id, "checkpoints", "prune"])
        | ("POST", ["v1", "sandboxes", id, "checkpoints", "prune"]) => {
            prune_checkpoints(state, id).await
        }
        ("POST", ["v1", "instances", id, "reset"]) => reset_instance(state, id).await,
        ("DELETE", ["v1", "instances", id])
        | ("DELETE", ["v1", "sandboxes", id])
        | ("POST", ["v1", "instances", id, "destroy"]) => destroy_instance(state, id).await,
        ("GET", ["v1", "pools"]) => list_pools(state),
        ("GET", ["v1", "pools", backend, class]) => pool_status(state, backend, class),
        ("POST", ["v1", "pools", backend, class, "drain"]) => drain_pool(state, backend, class),
        ("PUT", ["v1", "pools", backend, class, "sizing"]) => {
            resize_pool(state, backend, class, &body)
        }
        ("POST", ["v1", "templates", "gc"]) => gc_templates(state),
        ("GET", ["v1", "templates"]) => list_templates(state),
        ("GET", ["v1", "templates", id]) => inspect_template(state, id),
        ("GET", ["v1", "runtime-templates"]) => list_runtime_templates(state).await,
        ("GET", ["v1", "runtime-templates", name]) => get_runtime_template(state, name).await,
        ("POST", ["v1", "runtime-templates", "import"]) => {
            import_runtime_template(state, &body).await
        }
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
    json_ok(&state.manager.list()?)
}

fn get_instance(state: &Arc<ServerState>, id: &str) -> Result<Response<Full<Bytes>>> {
    json_ok(&state.manager.get(parse_uuid(id)?)?)
}

async fn create_instance(state: &Arc<ServerState>, body: &[u8]) -> Result<Response<Full<Bytes>>> {
    let req: CreateInstanceReq = serde_json::from_slice(body)
        .map_err(|e| BlazeDaemonError::BadRequest(format!("invalid create body: {e}")))?;

    let image = ImageMetadata {
        digest: req.image_digest.clone(),
        workload_class: Some(req.workload_class),
        kernel_version: req.kernel_version.clone(),
    };
    let mut decision = {
        let engine = state
            .policy
            .lock()
            .map_err(|_| BlazeDaemonError::Internal("policy lock poisoned".into()))?;
        match engine.evaluate(&req.labels, &image) {
            Ok(decision) => decision,
            Err(error) => {
                state.metrics.inc(&state.metrics.policy_eval_failures);
                return Err(error.into());
            }
        }
    };
    if let Some(pool) = decision.pool.as_mut()
        && pool.warm_ttl.is_none()
    {
        let default_warm_ttl = state
            .config
            .lock()
            .map_err(|_| BlazeDaemonError::Internal("config lock poisoned".into()))?
            .pool
            .default_warm_ttl
            .clone();
        pool.warm_ttl = Some(default_warm_ttl);
    }

    // Constrain availability to the implementation selected at daemon boot.
    let availability: Vec<BackendStatus> = {
        let config = state
            .config
            .lock()
            .map_err(|_| BlazeDaemonError::Internal("config lock poisoned".into()))?;
        decision
            .backend_priority
            .iter()
            .map(|kind| {
                let available = *kind == state.active_backend
                    && (state.active_backend == BackendKind::Mock
                        || config
                            .backends
                            .get(kind.as_str())
                            .map(|path| path.exists())
                            .unwrap_or(false));
                BackendStatus {
                    kind: *kind,
                    available,
                    version: None,
                }
            })
            .collect()
    };
    let policy_backend = match select_backend(&decision.backend_priority, &availability) {
        Ok(backend) => backend,
        Err(_) if state.active_backend == BackendKind::Mock => {
            *decision.backend_priority.first().ok_or_else(|| {
                BlazeDaemonError::Internal("policy has empty backend_priority".into())
            })?
        }
        Err(error) => return Err(error.into()),
    };
    let runtime_backend = if state.active_backend == BackendKind::Mock {
        BackendKind::Mock
    } else {
        policy_backend
    };
    let binary_path = state
        .config
        .lock()
        .map_err(|_| BlazeDaemonError::Internal("config lock poisoned".into()))?
        .backends
        .get(state.active_backend.as_str())
        .cloned()
        .unwrap_or_default();

    let created = state
        .manager
        .create(CreateSandbox {
            decision: decision.clone(),
            image_digest: req.image_digest,
            runtime_backend,
            binary_path,
        })
        .await?;
    json_created(&CreateInstanceResp {
        start_path: created.instance.start_path,
        instance: created.instance,
        decision,
        selected_backend: created.selected_backend,
    })
}

async fn checkpoint(state: &Arc<ServerState>, id: &str) -> Result<Response<Full<Bytes>>> {
    let uuid = parse_uuid(id)?;
    json_ok(&state.manager.checkpoint(uuid).await?)
}

async fn list_checkpoints(state: &Arc<ServerState>, id: &str) -> Result<Response<Full<Bytes>>> {
    json_ok(&state.manager.list_checkpoints(parse_uuid(id)?).await?)
}

async fn rollback(
    state: &Arc<ServerState>,
    id: &str,
    checkpoint_id: &str,
) -> Result<Response<Full<Bytes>>> {
    let uuid = parse_uuid(id)?;
    let instance = state.manager.get(uuid)?;
    let binary_path = state
        .config
        .lock()
        .map_err(|_| BlazeDaemonError::Internal("config lock poisoned".into()))?
        .backends
        .get(instance.backend.as_str())
        .cloned()
        .unwrap_or_default();
    let restored: RestoreSandboxResult = state
        .manager
        .restore(
            uuid,
            RestoreSandbox {
                checkpoint_id: checkpoint_id.to_string(),
                binary_path,
            },
        )
        .await?;
    json_ok(&json!({
        "instance_id": restored.instance.id,
        "checkpoint_id": restored.checkpoint_id,
        "restored": true,
        "state": restored.instance.state,
    }))
}

async fn hibernate(state: &Arc<ServerState>, id: &str) -> Result<Response<Full<Bytes>>> {
    let uuid = parse_uuid(id)?;
    let instance = state.manager.get(uuid)?;
    let binary_path = configured_backend_path(state, instance.backend)?;
    json_ok(
        &state
            .manager
            .hibernate(uuid, HibernateSandbox { binary_path })
            .await?,
    )
}

async fn resume(state: &Arc<ServerState>, id: &str) -> Result<Response<Full<Bytes>>> {
    let uuid = parse_uuid(id)?;
    let instance = state.manager.get(uuid)?;
    let binary_path = configured_backend_path(state, instance.backend)?;
    json_ok(
        &state
            .manager
            .resume(uuid, ResumeSandbox { binary_path })
            .await?,
    )
}

fn configured_backend_path(
    state: &ServerState,
    backend: BackendKind,
) -> Result<std::path::PathBuf> {
    Ok(state
        .config
        .lock()
        .map_err(|_| BlazeDaemonError::Internal("config lock poisoned".into()))?
        .backends
        .get(backend.as_str())
        .cloned()
        .unwrap_or_default())
}

async fn prune_checkpoints(state: &Arc<ServerState>, id: &str) -> Result<Response<Full<Bytes>>> {
    let removed = state.manager.prune_checkpoints(parse_uuid(id)?).await?;
    json_ok(&json!({
        "removed": removed,
        "count": removed.len(),
    }))
}

async fn reset_instance(state: &Arc<ServerState>, id: &str) -> Result<Response<Full<Bytes>>> {
    let uuid = parse_uuid(id)?;
    let _operation = state
        .manager
        .lock_quiescent_state(uuid, SandboxState::Running)
        .await?;

    Err(BlazeDaemonError::UnsupportedOperation(format!(
        "instance {uuid} cannot be reset until its backend can reset runtime and storage state"
    )))
}

#[cfg(test)]
async fn return_to_pool_for_test(state: &Arc<ServerState>, id: &str) -> Result<()> {
    let uuid = parse_uuid(id)?;
    let operation_lock = state.operation_lock(uuid);
    let _operation = operation_lock.lock().await;
    let mut map = state
        .instances
        .lock()
        .map_err(|_| BlazeDaemonError::Internal("instances lock poisoned".into()))?;
    let inst = map
        .get_mut(&uuid)
        .ok_or_else(|| BlazeDaemonError::NotFound(format!("instance {uuid}")))?;
    inst.transition(SandboxState::Reset)?;
    inst.transition(SandboxState::Warm)?;
    inst.persist(&state.state_dir)?;

    // return to pool keyed on (backend, class, image_digest)
    let key = PoolKey::new(inst.backend, inst.workload_class, inst.image_digest.clone());
    let inst_id = inst.id;
    drop(map);
    {
        let mut pool = state
            .pool
            .lock()
            .map_err(|_| BlazeDaemonError::Internal("pool lock poisoned".into()))?;
        pool.return_to_pool(key, inst_id);
    }
    Ok(())
}

async fn destroy_instance(state: &Arc<ServerState>, id: &str) -> Result<Response<Full<Bytes>>> {
    let uuid = parse_uuid(id)?;
    state.manager.destroy(uuid).await?;
    json_ok(&json!({
        "destroyed": true,
        "instance_id": uuid,
    }))
}

#[derive(Debug, Deserialize)]
struct ExecRequest {
    cmd: String,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    env: Option<HashMap<String, String>>,
    #[serde(default)]
    timeout: Option<u32>,
}

async fn exec_instance(
    state: &Arc<ServerState>,
    id: &str,
    body: &[u8],
) -> Result<Response<Full<Bytes>>> {
    let request: ExecRequest = serde_json::from_slice(body)
        .map_err(|error| BlazeDaemonError::BadRequest(format!("invalid exec body: {error}")))?;
    if request.cmd.is_empty() {
        return Err(BlazeDaemonError::BadRequest(
            "exec command is required".to_string(),
        ));
    }
    let timeout = request.timeout.unwrap_or(MAX_EXEC_TIMEOUT_SECS);
    if timeout == 0 || timeout > MAX_EXEC_TIMEOUT_SECS {
        return Err(BlazeDaemonError::BadRequest(format!(
            "exec timeout must be between 1 and {MAX_EXEC_TIMEOUT_SECS} seconds"
        )));
    }
    let result = state
        .manager
        .exec(
            parse_uuid(id)?,
            request.cmd,
            request.cwd,
            request.env,
            timeout,
        )
        .await?;
    json_ok(&json!({
        "exit_code": result.exit_code,
        "stdout_b64": BASE64.encode(result.stdout),
        "stderr_b64": BASE64.encode(result.stderr),
    }))
}

#[derive(Debug, Deserialize)]
struct FileRequest {
    path: String,
    #[serde(default)]
    data_b64: Option<String>,
}

async fn read_instance_file(
    state: &Arc<ServerState>,
    id: &str,
    body: &[u8],
) -> Result<Response<Full<Bytes>>> {
    let request: FileRequest = serde_json::from_slice(body)
        .map_err(|error| BlazeDaemonError::BadRequest(format!("invalid read body: {error}")))?;
    let data = state
        .manager
        .read_file(parse_uuid(id)?, request.path)
        .await?;
    json_ok(&json!({"data_b64": BASE64.encode(data)}))
}

async fn write_instance_file(
    state: &Arc<ServerState>,
    id: &str,
    body: &[u8],
) -> Result<Response<Full<Bytes>>> {
    let request: FileRequest = serde_json::from_slice(body)
        .map_err(|error| BlazeDaemonError::BadRequest(format!("invalid write body: {error}")))?;
    let encoded = request
        .data_b64
        .ok_or_else(|| BlazeDaemonError::BadRequest("data_b64 is required".to_string()))?;
    let data = decode_guest_file(&encoded, MAX_GUEST_FILE_BYTES)?;
    state
        .manager
        .write_file(parse_uuid(id)?, request.path, &data)
        .await?;
    json_ok(&json!({"written": true, "bytes": data.len()}))
}

fn decode_guest_file(encoded: &str, limit: usize) -> Result<Vec<u8>> {
    let encoded_limit = max_base64_len(limit);
    if encoded.len() > encoded_limit {
        return Err(crate::guest::GuestError::PayloadTooLarge {
            actual: encoded.len(),
            limit: encoded_limit,
        }
        .into());
    }
    let data = BASE64
        .decode(encoded)
        .map_err(|error| BlazeDaemonError::BadRequest(format!("invalid base64: {error}")))?;
    if data.len() > limit {
        return Err(crate::guest::GuestError::PayloadTooLarge {
            actual: data.len(),
            limit,
        }
        .into());
    }
    Ok(data)
}

/// Stop every tracked sandbox after the daemon has stopped accepting work.
///
/// Cleanup starts concurrently for all known owners and shares one deadline.
/// This lets independent owners finish after another owner fails or stalls
/// without multiplying the daemon's shutdown time by the sandbox count.
/// Timed-out sandbox tasks are cancelled and joined. Runtime-pool shutdown
/// retains control of its nested worker and joins that worker itself.
pub(crate) async fn shutdown_instances(state: &Arc<ServerState>, budget: Duration) -> Result<()> {
    let ids = state.manager.owned_instance_ids()?;

    let deadline = Instant::now() + budget;
    let mut tasks = JoinSet::new();
    let mut task_owners = HashMap::new();
    for id in ids {
        let state = state.clone();
        let task = tasks.spawn(async move { state.manager.destroy(id).await.map(|_| ()) });
        task_owners.insert(task.id(), id.to_string());
    }
    let pool_state = state.clone();
    let mut pool_shutdown = Box::pin(async move {
        pool_state
            .manager
            .shutdown_runtime_pool_until(deadline)
            .await
    });
    let mut pool_pending = true;
    let mut deadline_sleep = Box::pin(tokio::time::sleep_until(deadline));

    let mut failures = BTreeMap::new();
    let mut deadline_expired = false;
    while !tasks.is_empty() || pool_pending {
        tokio::select! {
            result = &mut pool_shutdown, if pool_pending => {
                pool_pending = false;
                if let Err(error) = result {
                    failures.insert("runtime-pool".to_string(), error.to_string());
                }
            }
            task = tasks.join_next_with_id(), if !tasks.is_empty() => {
                match task {
                    Some(Ok((task_id, result))) => {
                        let Some(id) = task_owners.remove(&task_id) else {
                            failures.insert(
                                format!("task-{task_id}"),
                                "cleanup result had no tracked sandbox owner".to_string(),
                            );
                            continue;
                        };
                        if let Err(error) = result {
                            failures.insert(id, error.to_string());
                        }
                    }
                    Some(Err(error)) => {
                        let key = task_owners
                            .remove(&error.id())
                            .unwrap_or_else(|| format!("task-{}", error.id()));
                        failures.insert(key, format!("cleanup task failed: {error}"));
                    }
                    None => {}
                }
            }
            _ = &mut deadline_sleep => {
                deadline_expired = true;
                break;
            }
        }
    }

    if deadline_expired {
        let mut timed_out_owners = task_owners.values().cloned().collect::<BTreeSet<_>>();
        tasks.abort_all();
        while let Some(result) = tasks.join_next_with_id().await {
            match result {
                Ok((task_id, result)) => {
                    let Some(id) = task_owners.remove(&task_id) else {
                        failures.insert(
                            format!("task-{task_id}"),
                            "cleanup result had no tracked sandbox owner".to_string(),
                        );
                        continue;
                    };
                    timed_out_owners.remove(&id);
                    if let Err(error) = result {
                        failures.insert(id, error.to_string());
                    }
                }
                Err(error) => {
                    let owner = task_owners.remove(&error.id());
                    let key = owner
                        .clone()
                        .unwrap_or_else(|| format!("task-{}", error.id()));
                    if !error.is_cancelled() {
                        if let Some(id) = owner {
                            timed_out_owners.remove(&id);
                        }
                        failures.insert(key, format!("cleanup task failed: {error}"));
                    }
                }
            }
        }
        for id in timed_out_owners {
            failures.entry(id).or_insert_with(|| {
                format!("cleanup did not finish within the shared {budget:?} budget")
            });
        }
        if pool_pending {
            pool_pending = false;
            if let Err(error) = pool_shutdown.await {
                failures.insert("runtime-pool".to_string(), error.to_string());
            }
        }
    }
    debug_assert!(!pool_pending);

    if failures.is_empty() {
        Ok(())
    } else {
        let failures = failures
            .into_iter()
            .map(|(owner, error)| format!("{owner}: {error}"))
            .collect::<Vec<_>>();
        Err(BlazeDaemonError::RecoveryRequired(format!(
            "daemon shutdown left {} runtime cleanup operation(s) incomplete: {}",
            failures.len(),
            failures.join("; ")
        )))
    }
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

async fn list_runtime_templates(state: &Arc<ServerState>) -> Result<Response<Full<Bytes>>> {
    json_ok(&state.manager.list_runtime_templates().await?)
}

async fn get_runtime_template(
    state: &Arc<ServerState>,
    name: &str,
) -> Result<Response<Full<Bytes>>> {
    json_ok(&state.manager.get_runtime_template(name.to_string()).await?)
}

#[derive(Debug, Deserialize)]
struct ImportRuntimeTemplateRequest {
    name: String,
    source: PathBuf,
    #[serde(default)]
    description: String,
}

async fn import_runtime_template(
    state: &Arc<ServerState>,
    body: &[u8],
) -> Result<Response<Full<Bytes>>> {
    let request: ImportRuntimeTemplateRequest = serde_json::from_slice(body).map_err(|error| {
        BlazeDaemonError::BadRequest(format!("invalid runtime template import body: {error}"))
    })?;
    let imported = state
        .manager
        .import_runtime_template(request.name, request.source, request.description)
        .await?;
    json_response(StatusCode::CREATED, &imported)
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
    let mut body = json!({
        "error": err.to_string(),
        "status": status.as_u16(),
    });
    if let Some(code) = err.api_code() {
        body["code"] = json!(code);
    }
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
    use std::future;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;
    use blaze_core::BlazeError;
    use blaze_core::backend::{BackendKind, SpawnRequest};
    use blaze_core::config::DaemonConfig;
    use blaze_core::kernel::HookRegistry;
    #[cfg(feature = "test-failpoints")]
    use blaze_core::lifecycle::OperationPhase;
    use blaze_core::lifecycle::{BackendOwnership, OperationKind, RuntimeLocation};
    use blaze_core::policy::{
        BackendConfigs, FallbackOnMissingHook, PolicyEngine, PolicyFile, PolicyHooks, PolicyMatch,
        PolicyPool, PolicySelect, ResetMode, WorkloadClass,
    };
    use blaze_core::pool::PoolManager;
    use blaze_core::storage::{
        AcquireOpts, PoolStatus, StorageAcquireError, StorageProvider, StorageSlot,
    };
    use blaze_core::template::TemplateRegistry;
    use http_body_util::BodyExt;

    use crate::file_provider::FileStorageProvider;
    use crate::runtime_pool::PoolPrototype;
    #[cfg(target_os = "linux")]
    use crate::spawner::BubblewrapSpawner;
    use crate::spawner::{
        BackendInstance, BackendSpawner, DynBackendInstance, DynSpawner, MockSpawner, SpawnFailure,
        SpawnResult, SpawnerRegistry,
    };
    use crate::state::ServerState;

    use super::*;

    fn spawners(kind: BackendKind, spawner: DynSpawner) -> SpawnerRegistry {
        let mut registry = SpawnerRegistry::new();
        registry.insert(kind, spawner);
        registry
    }

    fn test_config(temp: &tempfile::TempDir) -> DaemonConfig {
        let mut config = DaemonConfig::default();
        config.daemon.state_dir = temp.path().join("state");
        config.storage.images_dir = temp.path().join("images");
        config.storage.instances_dir = temp.path().join("instances");
        config.runtime_templates.dir = temp.path().join("runtime-templates");
        std::fs::create_dir_all(&config.daemon.state_dir).expect("state");
        std::fs::create_dir_all(&config.storage.images_dir).expect("images");
        std::fs::create_dir_all(&config.storage.instances_dir).expect("instances");
        config
    }

    fn test_policy(kind: BackendKind, pooled: bool) -> PolicyFile {
        PolicyFile {
            manifest_version: 1,
            policy_name: "ownership-test".into(),
            priority: 100,
            match_: PolicyMatch {
                workload_class: WorkloadClass::AgentTool,
                image_labels: HashMap::new(),
            },
            select: PolicySelect {
                backend_priority: vec![kind],
                kernel_hooks: vec![],
                templates: vec![],
                fallback_on_missing_hook: FallbackOnMissingHook::default(),
            },
            pool: pooled.then_some(PolicyPool {
                enabled: true,
                min: 0,
                target: 0,
                max: 1,
                warm_ttl: Some("30m".into()),
                reset_mode: ResetMode::FullRecreate,
            }),
            checkpoint: None,
            quota: None,
            hooks: PolicyHooks::default(),
            backend: BackendConfigs::default(),
            vm: None,
        }
    }

    fn test_request() -> Vec<u8> {
        serde_json::to_vec(&json!({
            "workload_class": "agent-tool",
            "image_digest": "sha256:ownership-test"
        }))
        .expect("request")
    }

    fn build_test_state(
        config: DaemonConfig,
        policy: PolicyFile,
        registry: SpawnerRegistry,
        active_backend: BackendKind,
        storage: Arc<dyn StorageProvider>,
    ) -> Arc<ServerState> {
        Arc::new(
            ServerState::build(
                config,
                PolicyEngine::with_policies(vec![policy]),
                PoolManager::new(),
                TemplateRegistry::new(),
                HookRegistry::new(),
                registry,
                active_backend,
                storage,
            )
            .expect("build server state"),
        )
    }

    #[cfg(feature = "test-failpoints")]
    fn mock_state(temp: &tempfile::TempDir, pooled: bool) -> Arc<ServerState> {
        mock_state_from_config(test_config(temp), pooled)
    }

    #[cfg(feature = "test-failpoints")]
    fn mock_state_from_config(config: DaemonConfig, pooled: bool) -> Arc<ServerState> {
        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            config.storage.instances_dir.clone(),
        ));
        build_test_state(
            config,
            test_policy(BackendKind::Mock, pooled),
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            storage,
        )
    }

    async fn created_json(state: &Arc<ServerState>, request: &[u8]) -> serde_json::Value {
        let response = create_instance(state, request).await.expect("create");
        serde_json::from_slice(
            &response
                .into_body()
                .collect()
                .await
                .expect("body")
                .to_bytes(),
        )
        .expect("created json")
    }

    async fn wait_for_ready_runtime(state: &Arc<ServerState>) -> Uuid {
        let runtime_root = state.state_dir.join("runtime-pool");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if state.manager.runtime_pool_status().ready > 0
                    && let Ok(mut entries) = tokio::fs::read_dir(&runtime_root).await
                {
                    while let Ok(Some(entry)) = entries.next_entry().await {
                        let Ok(instance_id) = Uuid::parse_str(&entry.file_name().to_string_lossy())
                        else {
                            continue;
                        };
                        let Ok(raw) = tokio::fs::read(entry.path().join("ownership.json")).await
                        else {
                            continue;
                        };
                        let Ok(ownership) = serde_json::from_slice::<serde_json::Value>(&raw)
                        else {
                            continue;
                        };
                        if ownership["phase"]["kind"] == "ready" {
                            return instance_id;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("runtime pool produces a ready slot")
    }

    fn warm_runtime_state(
        temp: &tempfile::TempDir,
        prefork: bool,
        spawner: DynSpawner,
    ) -> (Arc<ServerState>, PathBuf) {
        let mut config = test_config(temp);
        config.storage.pool_size = 1;
        config.storage.prefork = prefork;
        std::fs::create_dir_all(config.daemon.state_dir.join("runtime-pool"))
            .expect("runtime pool root");
        let instances_dir = config.storage.instances_dir.clone();
        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            instances_dir.clone(),
        ));
        (
            build_test_state(
                config,
                test_policy(BackendKind::Mock, true),
                spawners(BackendKind::Mock, spawner),
                BackendKind::Mock,
                storage,
            ),
            instances_dir,
        )
    }

    async fn assert_warm_runtime_round_trip(prefork: bool) {
        let temp = tempfile::tempdir().expect("temp");
        let (state, instances_dir) = warm_runtime_state(&temp, prefork, Arc::new(MockSpawner));

        let _bootstrap = created_json(&state, &test_request()).await;
        let ready_id = wait_for_ready_runtime(&state).await;
        let claimed = created_json(&state, &test_request()).await;
        let claimed_id =
            Uuid::parse_str(claimed["instance"]["id"].as_str().expect("claimed ID")).expect("UUID");

        assert_eq!(claimed_id, ready_id);
        assert_eq!(claimed["start_path"], "warm");
        assert_eq!(claimed["instance"]["runtime_location"], "warm-pool");
        assert!(claimed["instance"]["runtime_owner_token"].is_string());

        state.manager.begin_shutdown();
        assert!(
            state
                .manager
                .destroy(claimed_id)
                .await
                .expect("destroy claim")
        );
        let terminal = state.manager.get(claimed_id).expect("terminal lifecycle");
        assert!(terminal.is_clean_terminal());
        assert!(!instances_dir.join(claimed_id.to_string()).exists());
        assert!(
            !state
                .state_dir
                .join("runtime-pool")
                .join(claimed_id.to_string())
                .exists()
        );
        assert!(
            !state
                .state_dir
                .join("runtime-pool")
                .join(".cleanup")
                .join(claimed_id.to_string())
                .exists()
        );
        shutdown_instances(&state, Duration::from_secs(1))
            .await
            .expect("shutdown remaining owners");
    }

    async fn write_checkpoint_fixture(state: &Arc<ServerState>, id: &str) -> StorageSlot {
        let slot = state.storage.reconstruct(id).await.expect("storage slot");
        tokio::fs::write(&slot.rootfs_path, b"checkpoint-rootfs")
            .await
            .expect("rootfs");
        slot
    }

    #[cfg(feature = "test-failpoints")]
    async fn cancel_checkpoint_at(state: &Arc<ServerState>, id: Uuid, failpoint: &'static str) {
        let hook = crate::failpoint::TestFailpoint::new(&[failpoint]);
        let capture_state = state.clone();
        let capture_hook = hook.clone();
        let capture =
            tokio::spawn(
                async move { capture_hook.run(capture_state.manager.checkpoint(id)).await },
            );
        hook.wait_until_paused().await;
        capture.abort();
        let cancelled = capture
            .await
            .expect_err("checkpoint task must be cancelled");
        assert!(cancelled.is_cancelled());
    }

    struct NoCheckpointStorage {
        inner: FileStorageProvider,
    }

    #[async_trait]
    impl StorageProvider for NoCheckpointStorage {
        async fn probe(&self) -> blaze_core::Result<bool> {
            self.inner.probe().await
        }

        async fn acquire(
            &self,
            opts: &AcquireOpts,
        ) -> std::result::Result<StorageSlot, StorageAcquireError> {
            self.inner.acquire(opts).await
        }

        async fn release(&self, slot: StorageSlot) -> blaze_core::Result<()> {
            self.inner.release(slot).await
        }

        async fn release_by_id(&self, instance_id: &str) -> blaze_core::Result<()> {
            self.inner.release_by_id(instance_id).await
        }

        async fn reconstruct(&self, instance_id: &str) -> blaze_core::Result<StorageSlot> {
            self.inner.reconstruct(instance_id).await
        }

        async fn flush_dirty(&self, slot: &StorageSlot) -> blaze_core::Result<()> {
            self.inner.flush_dirty(slot).await
        }

        fn pool_status(&self) -> PoolStatus {
            self.inner.pool_status()
        }

        async fn drain_pool(&self) -> blaze_core::Result<usize> {
            self.inner.drain_pool().await
        }
    }

    async fn dispatched_json(
        state: &Arc<ServerState>,
        method: Method,
        path: &str,
        body: Vec<u8>,
    ) -> (StatusCode, serde_json::Value) {
        let response = dispatch(&method, path, "", body, state)
            .await
            .expect("dispatch");
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("response body")
            .to_bytes();
        let value = serde_json::from_slice(&body).expect("response json");
        (status, value)
    }

    async fn handled_json(
        state: &Arc<ServerState>,
        method: Method,
        path: &str,
        body: Vec<u8>,
    ) -> (StatusCode, serde_json::Value) {
        let request = Request::builder()
            .method(method)
            .uri(path)
            .header(hyper::header::CONTENT_LENGTH, body.len())
            .body(Full::new(Bytes::from(body)))
            .expect("request");
        let response = handle_request(request, state.clone())
            .await
            .expect("infallible response");
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("response body")
            .to_bytes();
        let value = serde_json::from_slice(&body).expect("response json");
        (status, value)
    }

    struct TransientReconstructStorage {
        inner: FileStorageProvider,
        fail_reconstruct: AtomicBool,
    }

    impl TransientReconstructStorage {
        fn new(images_dir: std::path::PathBuf, instances_dir: std::path::PathBuf) -> Self {
            Self {
                inner: FileStorageProvider::with_images(images_dir, instances_dir),
                fail_reconstruct: AtomicBool::new(false),
            }
        }
    }

    #[async_trait]
    impl StorageProvider for TransientReconstructStorage {
        async fn probe(&self) -> blaze_core::Result<bool> {
            self.inner.probe().await
        }

        async fn acquire(
            &self,
            opts: &AcquireOpts,
        ) -> std::result::Result<StorageSlot, StorageAcquireError> {
            self.inner.acquire(opts).await
        }

        async fn release(&self, slot: StorageSlot) -> blaze_core::Result<()> {
            self.inner.release(slot).await
        }

        async fn release_by_id(&self, instance_id: &str) -> blaze_core::Result<()> {
            self.inner.release_by_id(instance_id).await
        }

        async fn reconstruct(&self, instance_id: &str) -> blaze_core::Result<StorageSlot> {
            if self.fail_reconstruct.load(Ordering::Acquire) {
                return Err(BlazeError::StorageError {
                    msg: "transient reconstruct failure".into(),
                });
            }
            self.inner.reconstruct(instance_id).await
        }

        async fn flush_dirty(&self, slot: &StorageSlot) -> blaze_core::Result<()> {
            self.inner.flush_dirty(slot).await
        }

        fn pool_status(&self) -> PoolStatus {
            self.inner.pool_status()
        }

        async fn drain_pool(&self) -> blaze_core::Result<usize> {
            self.inner.drain_pool().await
        }
    }

    struct OwnershipObservingStorage {
        inner: FileStorageProvider,
        state_dir: PathBuf,
        observed: Arc<AtomicBool>,
    }

    #[async_trait]
    impl StorageProvider for OwnershipObservingStorage {
        async fn probe(&self) -> blaze_core::Result<bool> {
            self.inner.probe().await
        }

        async fn acquire(
            &self,
            opts: &AcquireOpts,
        ) -> std::result::Result<StorageSlot, StorageAcquireError> {
            let id = Uuid::parse_str(&opts.instance_id).expect("stable instance ID");
            let instance = SandboxInstance::load(&self.state_dir, id).expect("ownership published");
            assert_eq!(instance.state, SandboxState::Creating);
            assert_eq!(instance.backend_ownership, BackendOwnership::NotStarted);
            assert_eq!(
                instance.operation.as_ref().map(|operation| operation.kind),
                Some(OperationKind::Create)
            );
            self.observed.store(true, Ordering::Release);
            self.inner.acquire(opts).await
        }

        async fn release(&self, slot: StorageSlot) -> blaze_core::Result<()> {
            self.inner.release(slot).await
        }

        async fn release_by_id(&self, instance_id: &str) -> blaze_core::Result<()> {
            self.inner.release_by_id(instance_id).await
        }

        async fn reconstruct(&self, instance_id: &str) -> blaze_core::Result<StorageSlot> {
            self.inner.reconstruct(instance_id).await
        }

        async fn flush_dirty(&self, slot: &StorageSlot) -> blaze_core::Result<()> {
            self.inner.flush_dirty(slot).await
        }

        fn pool_status(&self) -> PoolStatus {
            self.inner.pool_status()
        }

        async fn drain_pool(&self) -> blaze_core::Result<usize> {
            self.inner.drain_pool().await
        }
    }

    struct FailOnceOwner {
        instance_id: Uuid,
        attempts: AtomicUsize,
    }

    #[async_trait]
    impl BackendInstance for FailOnceOwner {
        fn backend(&self) -> BackendKind {
            BackendKind::Mock
        }

        async fn try_wait(&self) -> blaze_core::Result<Option<SpawnResult>> {
            Ok(None)
        }

        async fn kill(&self) -> blaze_core::Result<()> {
            if self.attempts.fetch_add(1, Ordering::AcqRel) == 0 {
                return Err(BlazeError::BackendError {
                    msg: format!("instance {} termination deferred", self.instance_id),
                });
            }
            Ok(())
        }
    }

    #[derive(Clone, Copy)]
    enum ShutdownBehavior {
        Complete,
        Fail,
        Stall,
    }

    struct ShutdownOwner {
        instance_id: Uuid,
        behavior: ShutdownBehavior,
        attempts: Arc<AtomicUsize>,
        active: Arc<AtomicUsize>,
    }

    struct ActiveCleanup(Arc<AtomicUsize>);

    impl ActiveCleanup {
        fn enter(active: Arc<AtomicUsize>) -> Self {
            active.fetch_add(1, Ordering::AcqRel);
            Self(active)
        }
    }

    impl Drop for ActiveCleanup {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::AcqRel);
        }
    }

    #[async_trait]
    impl BackendInstance for ShutdownOwner {
        fn backend(&self) -> BackendKind {
            BackendKind::Mock
        }

        async fn try_wait(&self) -> blaze_core::Result<Option<SpawnResult>> {
            Ok(None)
        }

        async fn kill(&self) -> blaze_core::Result<()> {
            self.attempts.fetch_add(1, Ordering::AcqRel);
            let _active = ActiveCleanup::enter(self.active.clone());
            match self.behavior {
                ShutdownBehavior::Complete => Ok(()),
                ShutdownBehavior::Fail => Err(BlazeError::BackendError {
                    msg: format!("instance {} termination failed", self.instance_id),
                }),
                ShutdownBehavior::Stall => future::pending().await,
            }
        }
    }

    async fn track_shutdown_owner(
        state: &Arc<ServerState>,
        behavior: ShutdownBehavior,
        attempts: Arc<AtomicUsize>,
        active: Arc<AtomicUsize>,
    ) -> Uuid {
        let mut instance = SandboxInstance::new(
            BackendKind::Mock,
            WorkloadClass::AgentTool,
            "sha256:shutdown-budget".into(),
            StartPath::Cold,
            "shutdown-budget-test".into(),
        );
        instance
            .transition(SandboxState::Creating)
            .expect("creating");
        instance.transition(SandboxState::Running).expect("running");
        instance.backend_ownership = BackendOwnership::Running;
        instance.persist(&state.state_dir).expect("persist");
        state
            .storage
            .acquire(&AcquireOpts {
                instance_id: instance.id.to_string(),
                rootfs_size: 4096,
                mem_size: 4096,
            })
            .await
            .expect("storage");

        let id = instance.id;
        state
            .instances
            .lock()
            .expect("instances")
            .insert(id, instance);
        state
            .manager
            .insert_backend_owner(
                id,
                Arc::new(ShutdownOwner {
                    instance_id: id,
                    behavior,
                    attempts,
                    active,
                }),
            )
            .expect("retain backend owner");
        id
    }

    struct PartialSpawnSpawner;

    #[async_trait]
    impl BackendSpawner for PartialSpawnSpawner {
        async fn spawn(
            &self,
            request: SpawnRequest,
        ) -> std::result::Result<DynBackendInstance, SpawnFailure> {
            let owner: DynBackendInstance = Arc::new(FailOnceOwner {
                instance_id: request.instance_id,
                attempts: AtomicUsize::new(0),
            });
            Err(SpawnFailure::with_owner(
                BlazeError::BackendError {
                    msg: "backend readiness failed".into(),
                },
                owner,
            ))
        }

        async fn probe(&self, _binary_path: &Path) -> blaze_core::Result<bool> {
            Ok(true)
        }

        async fn cleanup_orphan(
            &self,
            _instance_id: Uuid,
            _run_dir: &Path,
        ) -> blaze_core::Result<()> {
            Err(BlazeError::BackendError {
                msg: "partial owner must remain registered".into(),
            })
        }
    }

    struct RecordingSpawner {
        cleanup_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl BackendSpawner for RecordingSpawner {
        async fn spawn(
            &self,
            _request: SpawnRequest,
        ) -> std::result::Result<DynBackendInstance, SpawnFailure> {
            Err(SpawnFailure::clean(BlazeError::BackendError {
                msg: "spawn not used".into(),
            }))
        }

        async fn probe(&self, _binary_path: &Path) -> blaze_core::Result<bool> {
            Ok(true)
        }

        async fn cleanup_orphan(
            &self,
            _instance_id: Uuid,
            _run_dir: &Path,
        ) -> blaze_core::Result<()> {
            self.cleanup_count.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
    }

    struct SelectiveCleanupSpawner {
        failed_id: Uuid,
        cleanup_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl BackendSpawner for SelectiveCleanupSpawner {
        async fn spawn(
            &self,
            request: SpawnRequest,
        ) -> std::result::Result<DynBackendInstance, SpawnFailure> {
            MockSpawner.spawn(request).await
        }

        async fn probe(&self, _binary_path: &Path) -> blaze_core::Result<bool> {
            Ok(true)
        }

        async fn cleanup_orphan(
            &self,
            instance_id: Uuid,
            _run_dir: &Path,
        ) -> blaze_core::Result<()> {
            self.cleanup_count.fetch_add(1, Ordering::AcqRel);
            if instance_id == self.failed_id {
                return Err(BlazeError::BackendError {
                    msg: "cleanup deferred".into(),
                });
            }
            Ok(())
        }
    }

    struct CountingOwner {
        instance_id: Uuid,
        kill_count: Arc<AtomicUsize>,
        killed: AtomicBool,
    }

    #[async_trait]
    impl BackendInstance for CountingOwner {
        fn backend(&self) -> BackendKind {
            BackendKind::Mock
        }

        async fn try_wait(&self) -> blaze_core::Result<Option<SpawnResult>> {
            Ok(self.killed.load(Ordering::Acquire).then_some(SpawnResult {
                instance_id: self.instance_id,
                exit_code: Some(0),
                signal: None,
            }))
        }

        async fn kill(&self) -> blaze_core::Result<()> {
            if !self.killed.swap(true, Ordering::AcqRel) {
                self.kill_count.fetch_add(1, Ordering::AcqRel);
            }
            Ok(())
        }
    }

    struct CountingSpawner {
        kill_count: Arc<AtomicUsize>,
        orphan_cleanup_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl BackendSpawner for CountingSpawner {
        async fn spawn(
            &self,
            request: SpawnRequest,
        ) -> std::result::Result<DynBackendInstance, SpawnFailure> {
            Ok(Arc::new(CountingOwner {
                instance_id: request.instance_id,
                kill_count: self.kill_count.clone(),
                killed: AtomicBool::new(false),
            }))
        }

        async fn probe(&self, _binary_path: &Path) -> blaze_core::Result<bool> {
            Ok(true)
        }

        async fn cleanup_orphan(
            &self,
            _instance_id: Uuid,
            _run_dir: &Path,
        ) -> blaze_core::Result<()> {
            self.orphan_cleanup_count.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
    }

    struct CaptureOnlyMockSpawner;

    #[async_trait]
    impl BackendSpawner for CaptureOnlyMockSpawner {
        async fn spawn(
            &self,
            request: SpawnRequest,
        ) -> std::result::Result<DynBackendInstance, SpawnFailure> {
            MockSpawner.spawn(request).await
        }

        async fn probe(&self, _binary_path: &Path) -> blaze_core::Result<bool> {
            Ok(true)
        }

        async fn cleanup_orphan(
            &self,
            instance_id: Uuid,
            run_dir: &Path,
        ) -> blaze_core::Result<()> {
            MockSpawner.cleanup_orphan(instance_id, run_dir).await
        }
    }

    struct StalledGuestOwner {
        instance_id: Uuid,
        socket: PathBuf,
        kill_count: Arc<AtomicUsize>,
        killed: AtomicBool,
    }

    #[async_trait]
    impl BackendInstance for StalledGuestOwner {
        fn backend(&self) -> BackendKind {
            BackendKind::Mock
        }

        fn guest_socket_path(&self) -> &Path {
            &self.socket
        }

        async fn try_wait(&self) -> blaze_core::Result<Option<SpawnResult>> {
            Ok(self.killed.load(Ordering::Acquire).then_some(SpawnResult {
                instance_id: self.instance_id,
                exit_code: Some(0),
                signal: None,
            }))
        }

        async fn kill(&self) -> blaze_core::Result<()> {
            if !self.killed.swap(true, Ordering::AcqRel) {
                self.kill_count.fetch_add(1, Ordering::AcqRel);
            }
            Ok(())
        }
    }

    struct StalledGuestSpawner {
        spawned: Arc<tokio::sync::Notify>,
        kill_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl BackendSpawner for StalledGuestSpawner {
        async fn spawn(
            &self,
            request: SpawnRequest,
        ) -> std::result::Result<DynBackendInstance, SpawnFailure> {
            self.spawned.notify_one();
            Ok(Arc::new(StalledGuestOwner {
                instance_id: request.instance_id,
                socket: request.run_dir.join("missing-guest.uds"),
                kill_count: self.kill_count.clone(),
                killed: AtomicBool::new(false),
            }))
        }

        async fn probe(&self, _binary_path: &Path) -> blaze_core::Result<bool> {
            Ok(true)
        }

        async fn cleanup_orphan(
            &self,
            _instance_id: Uuid,
            _run_dir: &Path,
        ) -> blaze_core::Result<()> {
            Ok(())
        }
    }

    struct CountingStorage {
        inner: FileStorageProvider,
        release_count: Arc<AtomicUsize>,
    }

    struct PoolWorkerReleaseStorage {
        inner: FileStorageProvider,
        acquire_count: AtomicUsize,
        residual_attempt: usize,
        delayed_id: Mutex<Option<String>>,
        release_started: Arc<AtomicUsize>,
        release_active: Arc<AtomicUsize>,
        release_completed: Arc<AtomicUsize>,
        release_delay: Duration,
    }

    struct ActiveRelease {
        active: Arc<AtomicUsize>,
    }

    impl Drop for ActiveRelease {
        fn drop(&mut self) {
            self.active.fetch_sub(1, Ordering::AcqRel);
        }
    }

    #[async_trait]
    impl StorageProvider for PoolWorkerReleaseStorage {
        async fn probe(&self) -> blaze_core::Result<bool> {
            self.inner.probe().await
        }

        async fn acquire(
            &self,
            opts: &AcquireOpts,
        ) -> std::result::Result<StorageSlot, StorageAcquireError> {
            let attempt = self.acquire_count.fetch_add(1, Ordering::AcqRel) + 1;
            let slot = self.inner.acquire(opts).await?;
            if attempt == self.residual_attempt {
                *self.delayed_id.lock().expect("delayed ID") = Some(slot.id.clone());
                return Err(StorageAcquireError::with_residual(
                    BlazeError::StorageError {
                        msg: "injected pool build residual".to_string(),
                    },
                    slot,
                ));
            }
            Ok(slot)
        }

        async fn release(&self, slot: StorageSlot) -> blaze_core::Result<()> {
            self.release_by_id(&slot.id).await
        }

        async fn release_by_id(&self, instance_id: &str) -> blaze_core::Result<()> {
            let delayed =
                self.delayed_id.lock().expect("delayed ID").as_deref() == Some(instance_id);
            if delayed {
                self.release_active.fetch_add(1, Ordering::AcqRel);
                let _active = ActiveRelease {
                    active: self.release_active.clone(),
                };
                self.release_started.fetch_add(1, Ordering::AcqRel);
                tokio::time::sleep(self.release_delay).await;
                self.inner.release_by_id(instance_id).await?;
                self.release_completed.fetch_add(1, Ordering::AcqRel);
                return Ok(());
            }
            self.inner.release_by_id(instance_id).await
        }

        fn supports_runtime_pool_recovery(&self) -> bool {
            true
        }

        async fn list_owned_ids(&self) -> blaze_core::Result<Vec<String>> {
            self.inner.list_owned_ids().await
        }

        async fn reconstruct(&self, instance_id: &str) -> blaze_core::Result<StorageSlot> {
            self.inner.reconstruct(instance_id).await
        }

        async fn flush_dirty(&self, slot: &StorageSlot) -> blaze_core::Result<()> {
            self.inner.flush_dirty(slot).await
        }

        fn pool_status(&self) -> PoolStatus {
            self.inner.pool_status()
        }

        async fn drain_pool(&self) -> blaze_core::Result<usize> {
            self.inner.drain_pool().await
        }
    }

    #[async_trait]
    impl StorageProvider for CountingStorage {
        async fn probe(&self) -> blaze_core::Result<bool> {
            self.inner.probe().await
        }

        async fn acquire(
            &self,
            opts: &AcquireOpts,
        ) -> std::result::Result<StorageSlot, StorageAcquireError> {
            self.inner.acquire(opts).await
        }

        async fn release(&self, slot: StorageSlot) -> blaze_core::Result<()> {
            self.release_count.fetch_add(1, Ordering::AcqRel);
            self.inner.release(slot).await
        }

        async fn release_by_id(&self, instance_id: &str) -> blaze_core::Result<()> {
            self.release_count.fetch_add(1, Ordering::AcqRel);
            self.inner.release_by_id(instance_id).await
        }

        async fn reconstruct(&self, instance_id: &str) -> blaze_core::Result<StorageSlot> {
            self.inner.reconstruct(instance_id).await
        }

        async fn flush_dirty(&self, slot: &StorageSlot) -> blaze_core::Result<()> {
            self.inner.flush_dirty(slot).await
        }

        fn pool_status(&self) -> PoolStatus {
            self.inner.pool_status()
        }

        async fn drain_pool(&self) -> blaze_core::Result<usize> {
            self.inner.drain_pool().await
        }
    }

    struct SelectiveHangingStorage {
        inner: FileStorageProvider,
        stalled_id: Uuid,
    }

    #[async_trait]
    impl StorageProvider for SelectiveHangingStorage {
        async fn probe(&self) -> blaze_core::Result<bool> {
            self.inner.probe().await
        }

        async fn acquire(
            &self,
            opts: &AcquireOpts,
        ) -> std::result::Result<StorageSlot, StorageAcquireError> {
            self.inner.acquire(opts).await
        }

        async fn release(&self, slot: StorageSlot) -> blaze_core::Result<()> {
            self.inner.release(slot).await
        }

        async fn release_by_id(&self, instance_id: &str) -> blaze_core::Result<()> {
            if instance_id == self.stalled_id.to_string() {
                return std::future::pending().await;
            }
            self.inner.release_by_id(instance_id).await
        }

        async fn reconstruct(&self, instance_id: &str) -> blaze_core::Result<StorageSlot> {
            self.inner.reconstruct(instance_id).await
        }

        async fn flush_dirty(&self, slot: &StorageSlot) -> blaze_core::Result<()> {
            self.inner.flush_dirty(slot).await
        }

        fn pool_status(&self) -> PoolStatus {
            self.inner.pool_status()
        }

        async fn drain_pool(&self) -> blaze_core::Result<usize> {
            self.inner.drain_pool().await
        }
    }

    #[cfg(feature = "test-failpoints")]
    fn counting_state(
        temp: &tempfile::TempDir,
    ) -> (
        Arc<ServerState>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
    ) {
        let config = test_config(temp);
        let kill_count = Arc::new(AtomicUsize::new(0));
        let orphan_cleanup_count = Arc::new(AtomicUsize::new(0));
        let release_count = Arc::new(AtomicUsize::new(0));
        let storage: Arc<dyn StorageProvider> = Arc::new(CountingStorage {
            inner: FileStorageProvider::with_images(
                config.storage.images_dir.clone(),
                config.storage.instances_dir.clone(),
            ),
            release_count: release_count.clone(),
        });
        let state = build_test_state(
            config,
            test_policy(BackendKind::Mock, false),
            spawners(
                BackendKind::Mock,
                Arc::new(CountingSpawner {
                    kill_count: kill_count.clone(),
                    orphan_cleanup_count: orphan_cleanup_count.clone(),
                }),
            ),
            BackendKind::Mock,
            storage,
        );
        (state, kill_count, orphan_cleanup_count, release_count)
    }

    #[tokio::test]
    async fn sandbox_collection_and_item_routes_match_instance_routes() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            config.storage.instances_dir.clone(),
        ));
        let state = build_test_state(
            config,
            test_policy(BackendKind::Mock, false),
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            storage,
        );

        let (status, created) =
            dispatched_json(&state, Method::POST, "/v1/sandboxes", test_request()).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(created["instance"]["state"], "running");
        let id = created["instance"]["id"].as_str().expect("instance id");

        let (_, sandboxes) =
            dispatched_json(&state, Method::GET, "/v1/sandboxes", Vec::new()).await;
        let (_, instances) =
            dispatched_json(&state, Method::GET, "/v1/instances", Vec::new()).await;
        assert_eq!(sandboxes, instances);

        let (_, sandbox) = dispatched_json(
            &state,
            Method::GET,
            &format!("/v1/sandboxes/{id}"),
            Vec::new(),
        )
        .await;
        let (_, instance) = dispatched_json(
            &state,
            Method::GET,
            &format!("/v1/instances/{id}"),
            Vec::new(),
        )
        .await;
        assert_eq!(sandbox, instance);
    }

    #[tokio::test]
    async fn destroy_route_forms_share_managed_cleanup() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            config.storage.instances_dir.clone(),
        ));
        let state = build_test_state(
            config,
            test_policy(BackendKind::Mock, false),
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            storage,
        );

        let mut ids = Vec::new();
        for _ in 0..3 {
            let created = created_json(&state, &test_request()).await;
            ids.push(
                Uuid::parse_str(created["instance"]["id"].as_str().expect("instance id"))
                    .expect("uuid"),
            );
        }
        let routes = [
            (Method::DELETE, format!("/v1/sandboxes/{}", ids[0]), ids[0]),
            (Method::DELETE, format!("/v1/instances/{}", ids[1]), ids[1]),
            (
                Method::POST,
                format!("/v1/instances/{}/destroy", ids[2]),
                ids[2],
            ),
        ];

        for (method, path, id) in routes {
            let (status, response) = dispatched_json(&state, method, &path, Vec::new()).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(response["destroyed"], true);
            assert_eq!(response["instance_id"], id.to_string());
            assert_eq!(
                state.manager.get(id).expect("destroyed state").state,
                SandboxState::Destroyed
            );
        }
    }

    #[tokio::test]
    async fn non_prefork_runtime_claim_completes_create_and_destroy() {
        assert_warm_runtime_round_trip(false).await;
    }

    #[tokio::test]
    async fn prefork_runtime_claim_completes_create_and_destroy() {
        assert_warm_runtime_round_trip(true).await;
    }

    #[tokio::test]
    async fn omitted_policy_ttl_is_resolved_in_create_response() {
        let temp = tempfile::tempdir().expect("temp");
        let mut config = test_config(&temp);
        config.pool.default_warm_ttl = "1h".into();
        let instances_dir = config.storage.instances_dir.clone();
        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            instances_dir,
        ));
        let mut policy = test_policy(BackendKind::Mock, true);
        policy.pool.as_mut().expect("pool").warm_ttl = None;
        let state = build_test_state(
            config,
            policy,
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            storage,
        );

        let created = created_json(&state, &test_request()).await;
        assert_eq!(created["decision"]["pool"]["warm_ttl"], "1h");
        let id = Uuid::parse_str(created["instance"]["id"].as_str().expect("instance ID"))
            .expect("UUID");
        assert!(state.manager.destroy(id).await.expect("destroy instance"));
        shutdown_instances(&state, Duration::from_millis(100))
            .await
            .expect("no owners remain");
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn visible_lifecycle_publish_error_keeps_one_cleanup_owner() {
        let temp = tempfile::tempdir().expect("temp");
        let (state, instances_dir) = warm_runtime_state(&temp, false, Arc::new(MockSpawner));
        let _bootstrap = created_json(&state, &test_request()).await;
        let ready_id = wait_for_ready_runtime(&state).await;
        let hook = crate::failpoint::TestFailpoint::new(&["warm-runtime-lifecycle-publish-result"]);

        let error = hook
            .run(create_instance(&state, &test_request()))
            .await
            .expect_err("visible lifecycle publication must be compensated");

        assert!(error.to_string().contains("publication reported an error"));
        let terminal = state.manager.get(ready_id).expect("terminal lifecycle");
        assert!(terminal.is_clean_terminal());
        assert!(state.manager.backend_owner(ready_id).is_none());
        assert!(!instances_dir.join(ready_id.to_string()).exists());
        assert!(
            !state
                .state_dir
                .join("runtime-pool")
                .join(ready_id.to_string())
                .exists()
        );
        assert!(
            !state
                .state_dir
                .join("runtime-pool")
                .join(".cleanup")
                .join(ready_id.to_string())
                .exists()
        );

        state.manager.begin_shutdown();
        shutdown_instances(&state, Duration::from_secs(1))
            .await
            .expect("shutdown remaining owners");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn ambiguous_lifecycle_publish_remains_counted_until_restart() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("temp");
        let (state, instances_dir) = warm_runtime_state(
            &temp,
            false,
            Arc::new(CountingSpawner {
                kill_count: Arc::new(AtomicUsize::new(0)),
                orphan_cleanup_count: Arc::new(AtomicUsize::new(0)),
            }),
        );
        let _bootstrap = created_json(&state, &test_request()).await;
        let ready_id = wait_for_ready_runtime(&state).await;
        let external = tempfile::tempdir().expect("external lifecycle owner");
        symlink(external.path(), state.state_dir.join(ready_id.to_string()))
            .expect("linked lifecycle owner");

        let error = create_instance(&state, &test_request())
            .await
            .expect_err("ambiguous lifecycle publication must stop the claim");

        assert!(error.to_string().contains("publication was ambiguous"));
        let status = state.manager.runtime_pool_status();
        assert_eq!(status.ready, 0);
        assert_eq!(status.leased, 0);
        assert_eq!(status.unresolved, 1);
        assert_eq!(status.deficit, 0);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(state.manager.runtime_pool_status().unresolved, 1);
        assert!(instances_dir.join(ready_id.to_string()).exists());

        state.manager.begin_shutdown();
        let shutdown = shutdown_instances(&state, Duration::from_secs(1))
            .await
            .expect_err("shutdown must report the unresolved owner");
        assert!(shutdown.to_string().contains(&ready_id.to_string()));
        assert!(
            shutdown
                .to_string()
                .contains("unresolved lifecycle publication")
        );
        assert_eq!(state.manager.runtime_pool_status().unresolved, 1);
    }

    #[tokio::test]
    async fn failed_runtime_claim_retains_one_recoverable_owner() {
        let temp = tempfile::tempdir().expect("temp");
        let (state, instances_dir) =
            warm_runtime_state(&temp, false, Arc::new(PartialSpawnSpawner));

        let _bootstrap_error = create_instance(&state, &test_request())
            .await
            .expect_err("bootstrap spawn fails after pool configuration");
        let ready_id = wait_for_ready_runtime(&state).await;
        let error = create_instance(&state, &test_request())
            .await
            .expect_err("runtime claim spawn fails");

        assert!(error.to_string().contains("cleanup incomplete"));
        let retained = state.manager.get(ready_id).expect("retained lifecycle");
        assert_eq!(retained.runtime_location, RuntimeLocation::WarmPool);
        assert_eq!(
            retained.operation.as_ref().map(|operation| operation.kind),
            Some(OperationKind::Destroy)
        );
        assert!(!retained.is_clean_terminal());
        let ownership: serde_json::Value = serde_json::from_slice(
            &std::fs::read(
                state
                    .state_dir
                    .join("runtime-pool")
                    .join(ready_id.to_string())
                    .join("ownership.json"),
            )
            .expect("retained ownership"),
        )
        .expect("ownership JSON");
        assert_eq!(ownership["phase"]["kind"], "lifecycle-cleanup");

        assert!(
            state
                .manager
                .destroy(ready_id)
                .await
                .expect("retry retained cleanup")
        );
        assert!(
            state
                .manager
                .get(ready_id)
                .expect("terminal lifecycle")
                .is_clean_terminal()
        );
        assert!(!instances_dir.join(ready_id.to_string()).exists());
        assert!(
            !state
                .state_dir
                .join("runtime-pool")
                .join(ready_id.to_string())
                .exists()
        );

        state.manager.begin_shutdown();
        shutdown_instances(&state, Duration::from_secs(1))
            .await
            .expect("shutdown bootstrap owner");
    }

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
        config.runtime_templates.dir = tmp.join("runtime-templates");
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
        let spawner: DynSpawner = Arc::new(MockSpawner);
        let storage_dir = tmp.join("storage");
        let _ = std::fs::create_dir_all(&storage_dir);
        let storage: Arc<dyn blaze_core::storage::StorageProvider> =
            Arc::new(FileStorageProvider::new(storage_dir));
        let state = Arc::new(
            ServerState::build(
                config,
                engine,
                PoolManager::new(),
                TemplateRegistry::new(),
                HookRegistry::new(),
                spawners(BackendKind::Firecracker, spawner),
                BackendKind::Firecracker,
                storage,
            )
            .expect("build server state"),
        );

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
    async fn checkpoint_rejects_unsupported_storage_without_mutation() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let storage: Arc<dyn StorageProvider> = Arc::new(NoCheckpointStorage {
            inner: FileStorageProvider::with_images(
                config.storage.images_dir.clone(),
                config.storage.instances_dir.clone(),
            ),
        });
        let state = build_test_state(
            config,
            test_policy(BackendKind::Mock, false),
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            storage,
        );
        let request = test_request();
        let created = created_json(&state, &request).await;
        let id = created["instance"]["id"].as_str().expect("id");
        let uuid = Uuid::parse_str(id).expect("uuid");
        let state_path = state.state_dir.join(id).join("state.json");
        let persisted_before = std::fs::read(&state_path).expect("persisted state");

        let error = checkpoint(&state, id)
            .await
            .expect_err("checkpoint without backend and storage capture must fail closed");

        assert!(matches!(error, BlazeDaemonError::UnsupportedOperation(_)));
        assert_eq!(error.status_code(), 501);
        assert_eq!(
            state.instances.lock().expect("instances")[&uuid].state,
            SandboxState::Running
        );
        assert!(
            state.instances.lock().expect("instances")[&uuid]
                .operation
                .is_none()
        );
        assert_eq!(
            std::fs::read(state_path).expect("persisted state"),
            persisted_before
        );
        assert!(!state.state_dir.join("checkpoints").join(id).exists());
        assert!(state.manager.backend_owner(uuid).is_some());
    }

    #[tokio::test]
    async fn checkpoint_rejects_unsupported_backend_without_mutation() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            config.storage.instances_dir.clone(),
        ));
        let kill_count = Arc::new(AtomicUsize::new(0));
        let state = build_test_state(
            config,
            test_policy(BackendKind::Mock, false),
            spawners(
                BackendKind::Mock,
                Arc::new(CountingSpawner {
                    kill_count: kill_count.clone(),
                    orphan_cleanup_count: Arc::new(AtomicUsize::new(0)),
                }),
            ),
            BackendKind::Mock,
            storage,
        );
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id");
        let uuid = Uuid::parse_str(id).expect("uuid");
        let state_path = state.state_dir.join(id).join("state.json");
        let persisted_before = std::fs::read(&state_path).expect("persisted state");

        let error = checkpoint(&state, id)
            .await
            .expect_err("checkpoint without backend capture must fail closed");

        assert!(matches!(error, BlazeDaemonError::UnsupportedOperation(_)));
        assert_eq!(error.status_code(), 501);
        let lifecycle = state.manager.get(uuid).expect("lifecycle");
        assert_eq!(lifecycle.state, SandboxState::Running);
        assert!(lifecycle.operation.is_none());
        assert_eq!(
            std::fs::read(state_path).expect("persisted state"),
            persisted_before
        );
        assert!(!state.state_dir.join("checkpoints").join(id).exists());
        assert_eq!(kill_count.load(Ordering::Acquire), 0);
        assert!(state.manager.backend_owner(uuid).is_some());
    }

    #[tokio::test]
    async fn checkpoint_routes_capture_and_list_live_state() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            config.storage.instances_dir.clone(),
        ));
        let state = build_test_state(
            config,
            test_policy(BackendKind::Mock, false),
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            storage,
        );
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id");
        let uuid = Uuid::parse_str(id).expect("uuid");
        let slot = write_checkpoint_fixture(&state, id).await;

        let (status, checkpoint) = dispatched_json(
            &state,
            Method::POST,
            &format!("/v1/sandboxes/{id}/checkpoint"),
            Vec::new(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let checkpoint_id = checkpoint["id"].as_str().expect("checkpoint id");
        assert_eq!(checkpoint["snapshot_kind"], "full");
        assert_eq!(checkpoint["sandbox_id"], id);
        let captured_rootfs = state
            .state_dir
            .join("checkpoints")
            .join(id)
            .join(checkpoint_id)
            .join("rootfs.snap");
        assert_eq!(
            tokio::fs::read(&captured_rootfs)
                .await
                .expect("captured rootfs"),
            b"checkpoint-rootfs"
        );

        tokio::fs::write(&slot.rootfs_path, b"changed-after-checkpoint")
            .await
            .expect("mutate live rootfs");
        assert_eq!(
            tokio::fs::read(&captured_rootfs)
                .await
                .expect("independent captured rootfs"),
            b"checkpoint-rootfs"
        );
        let (status, checkpoints) = dispatched_json(
            &state,
            Method::GET,
            &format!("/v1/instances/{id}/checkpoints"),
            Vec::new(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(checkpoints.as_array().expect("checkpoint list").len(), 1);
        assert_eq!(checkpoints[0]["id"], checkpoint_id);
        assert_eq!(checkpoints[0]["is_head"], true);
        assert_eq!(checkpoints[0]["on_head_chain"], true);

        let lifecycle = state.manager.get(uuid).expect("lifecycle");
        assert_eq!(lifecycle.state, SandboxState::Running);
        assert!(lifecycle.operation.is_none());
        assert_eq!(lifecycle.last_checkpoint.as_deref(), Some(checkpoint_id));
        assert!(state.manager.backend_owner(uuid).is_some());
    }

    #[tokio::test]
    async fn hibernate_releases_the_backend_and_resume_survives_restart() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            config.storage.instances_dir.clone(),
        ));
        let state = build_test_state(
            config.clone(),
            test_policy(BackendKind::Mock, false),
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            storage,
        );
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id");
        let uuid = Uuid::parse_str(id).expect("uuid");
        state
            .manager
            .write_file(uuid, "/tmp/value".to_string(), b"hibernate-memory")
            .await
            .expect("write guest state");

        let (status, hibernated) = dispatched_json(
            &state,
            Method::POST,
            &format!("/v1/sandboxes/{id}/hibernate"),
            Vec::new(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(hibernated["state"], "hibernated");
        assert_eq!(hibernated["backend_ownership"], "stopped");
        assert!(state.manager.backend_owner(uuid).is_none());
        let hibernate_dir = config.daemon.state_dir.join(id).join("hibernate");
        for name in ["manifest.json", "memory.snap", "vmstate.snap"] {
            assert!(hibernate_dir.join(name).is_file(), "{name} is missing");
        }
        let report = state.manager.reconcile_startup().await;
        assert_eq!(report.attempted, 0);
        assert!(report.failures.is_empty());
        drop(state);

        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            config.storage.instances_dir.clone(),
        ));
        let restarted = build_test_state(
            config,
            test_policy(BackendKind::Mock, false),
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            storage,
        );
        assert_eq!(
            restarted.manager.get(uuid).expect("loaded state").state,
            SandboxState::Hibernated
        );
        let report = restarted.manager.reconcile_startup().await;
        assert_eq!(report.attempted, 0);
        assert!(report.failures.is_empty());

        let (status, resumed) = dispatched_json(
            &restarted,
            Method::POST,
            &format!("/v1/instances/{id}/resume"),
            Vec::new(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(resumed["state"], "running");
        assert_eq!(
            restarted
                .manager
                .read_file(uuid, "/tmp/value".to_string())
                .await
                .expect("read resumed guest state"),
            b"hibernate-memory"
        );
        assert!(
            hibernate_dir.is_dir(),
            "the last hibernation image remains available until replacement or destroy"
        );
        assert!(restarted.manager.destroy(uuid).await.expect("destroy"));
        assert!(!hibernate_dir.exists());
    }

    #[tokio::test]
    async fn hibernate_rejects_a_capture_only_backend_before_state_mutation() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            config.storage.instances_dir.clone(),
        ));
        let state = build_test_state(
            config,
            test_policy(BackendKind::Mock, false),
            spawners(BackendKind::Mock, Arc::new(CaptureOnlyMockSpawner)),
            BackendKind::Mock,
            storage,
        );
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id");
        let uuid = Uuid::parse_str(id).expect("uuid");
        let owner = state.manager.backend_owner(uuid).expect("owner");

        let error = state
            .manager
            .hibernate(
                uuid,
                HibernateSandbox {
                    binary_path: PathBuf::new(),
                },
            )
            .await
            .expect_err("resume capability is required");

        assert!(matches!(error, BlazeDaemonError::UnsupportedOperation(_)));
        let retained = state.manager.backend_owner(uuid).expect("retained owner");
        assert!(Arc::ptr_eq(&owner, &retained));
        let lifecycle = state.manager.get(uuid).expect("lifecycle");
        assert_eq!(lifecycle.state, SandboxState::Running);
        assert!(lifecycle.operation.is_none());
    }

    #[tokio::test]
    async fn resume_rejects_corrupted_hibernation_artifacts_without_starting_a_backend() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            config.storage.instances_dir.clone(),
        ));
        let state = build_test_state(
            config.clone(),
            test_policy(BackendKind::Mock, false),
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            storage,
        );
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id");
        let uuid = Uuid::parse_str(id).expect("uuid");
        state
            .manager
            .hibernate(
                uuid,
                HibernateSandbox {
                    binary_path: PathBuf::new(),
                },
            )
            .await
            .expect("hibernate");
        tokio::fs::write(
            config
                .daemon
                .state_dir
                .join(id)
                .join("hibernate/memory.snap"),
            b"corrupted",
        )
        .await
        .expect("corrupt artifact");

        let error = state
            .manager
            .resume(
                uuid,
                ResumeSandbox {
                    binary_path: PathBuf::new(),
                },
            )
            .await
            .expect_err("corrupted artifact must fail closed");

        assert!(matches!(error, BlazeDaemonError::RecoveryRequired(_)));
        assert!(state.manager.backend_owner(uuid).is_none());
        let lifecycle = state.manager.get(uuid).expect("lifecycle");
        assert_eq!(lifecycle.state, SandboxState::RecoveryRequired);
        assert!(lifecycle.operation.is_none());
        assert!(state.manager.destroy(uuid).await.expect("destroy"));
    }

    #[tokio::test]
    async fn startup_retains_an_interrupted_hibernation_for_explicit_cleanup() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            config.storage.instances_dir.clone(),
        ));
        let mut instance = SandboxInstance::new(
            BackendKind::Mock,
            WorkloadClass::AgentTool,
            "sha256:ownership-test".into(),
            StartPath::Cold,
            "ownership-test".into(),
        );
        instance
            .transition(SandboxState::Creating)
            .expect("creating");
        instance.transition(SandboxState::Running).expect("running");
        instance.backend_ownership = BackendOwnership::Running;
        instance
            .begin_hibernate_operation()
            .expect("begin hibernation");
        instance
            .transition(SandboxState::Hibernating)
            .expect("hibernating");
        instance.persist(&config.daemon.state_dir).expect("persist");
        storage
            .acquire(&AcquireOpts {
                instance_id: instance.id.to_string(),
                rootfs_size: 4096,
                mem_size: 4096,
            })
            .await
            .expect("storage");
        let id = instance.id;
        let state = build_test_state(
            config,
            test_policy(BackendKind::Mock, false),
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            storage,
        );

        let report = state.manager.reconcile_startup().await;
        assert_eq!(report.attempted, 0);
        assert!(report.failures.is_empty());
        let retained = state.manager.get(id).expect("retained lifecycle");
        assert_eq!(retained.state, SandboxState::RecoveryRequired);
        assert_eq!(
            retained.operation.as_ref().map(|operation| operation.kind),
            Some(OperationKind::Hibernate)
        );
        assert!(state.manager.destroy(id).await.expect("explicit destroy"));
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn hibernate_snapshot_failure_resumes_the_existing_backend() {
        let temp = tempfile::tempdir().expect("temp");
        let state = mock_state(&temp, false);
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id");
        let uuid = Uuid::parse_str(id).expect("uuid");
        let owner = state.manager.backend_owner(uuid).expect("owner");
        let hook = crate::failpoint::TestFailpoint::new(&["hibernate-snapshot"]);

        hook.run(state.manager.hibernate(
            uuid,
            HibernateSandbox {
                binary_path: PathBuf::new(),
            },
        ))
        .await
        .expect_err("snapshot failure");

        let retained = state.manager.backend_owner(uuid).expect("retained owner");
        assert!(Arc::ptr_eq(&owner, &retained));
        let lifecycle = state.manager.get(uuid).expect("lifecycle");
        assert_eq!(lifecycle.state, SandboxState::Running);
        assert_eq!(lifecycle.backend_ownership, BackendOwnership::Running);
        assert!(lifecycle.operation.is_none());
        let names = std::fs::read_dir(state.state_dir.join(id))
            .expect("instance directory")
            .map(|entry| entry.expect("entry").file_name())
            .collect::<Vec<_>>();
        assert!(
            names
                .iter()
                .all(|name| !name.to_string_lossy().starts_with(".hibernate."))
        );
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn hibernate_compensation_requires_guest_readiness() {
        let temp = tempfile::tempdir().expect("temp");
        let state = mock_state(&temp, false);
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id");
        let uuid = Uuid::parse_str(id).expect("uuid");
        let hook =
            crate::failpoint::TestFailpoint::new(&["hibernate-snapshot", "resume-guest-ready"]);

        let error = hook
            .run(state.manager.hibernate(
                uuid,
                HibernateSandbox {
                    binary_path: PathBuf::new(),
                },
            ))
            .await
            .expect_err("guest readiness must fail closed");

        assert!(matches!(error, BlazeDaemonError::RecoveryRequired(_)));
        assert!(state.manager.backend_owner(uuid).is_some());
        let lifecycle = state.manager.get(uuid).expect("lifecycle");
        assert_eq!(lifecycle.state, SandboxState::RecoveryRequired);
        assert_eq!(lifecycle.backend_ownership, BackendOwnership::Unknown);
        assert_eq!(
            lifecycle.operation.as_ref().map(|operation| operation.kind),
            Some(OperationKind::Hibernate)
        );
        assert!(state.manager.destroy(uuid).await.expect("destroy"));
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn uncertain_hibernate_stop_retains_the_existing_owner() {
        let temp = tempfile::tempdir().expect("temp");
        let state = mock_state(&temp, false);
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id");
        let uuid = Uuid::parse_str(id).expect("uuid");
        let owner = state.manager.backend_owner(uuid).expect("owner");
        let hook = crate::failpoint::TestFailpoint::new(&["hibernate-backend-stop"]);

        let error = hook
            .run(state.manager.hibernate(
                uuid,
                HibernateSandbox {
                    binary_path: PathBuf::new(),
                },
            ))
            .await
            .expect_err("uncertain stop must retain ownership");

        assert!(matches!(error, BlazeDaemonError::RecoveryRequired(_)));
        let retained = state.manager.backend_owner(uuid).expect("retained owner");
        assert!(Arc::ptr_eq(&owner, &retained));
        let lifecycle = state.manager.get(uuid).expect("lifecycle");
        assert_eq!(lifecycle.state, SandboxState::RecoveryRequired);
        assert_eq!(lifecycle.backend_ownership, BackendOwnership::Unknown);
        assert_eq!(
            lifecycle
                .operation
                .as_ref()
                .and_then(|operation| operation.phase),
            Some(OperationPhase::HibernateArtifactsSynced)
        );
        assert!(state.manager.destroy(uuid).await.expect("destroy"));
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn hibernate_publish_failure_retains_stopped_ownership_for_destroy() {
        let temp = tempfile::tempdir().expect("temp");
        let state = mock_state(&temp, false);
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id");
        let uuid = Uuid::parse_str(id).expect("uuid");
        let hook = crate::failpoint::TestFailpoint::new(&["hibernate-publish"]);

        let error = hook
            .run(state.manager.hibernate(
                uuid,
                HibernateSandbox {
                    binary_path: PathBuf::new(),
                },
            ))
            .await
            .expect_err("publish failure follows backend stop");

        assert!(matches!(error, BlazeDaemonError::RecoveryRequired(_)));
        assert!(state.manager.backend_owner(uuid).is_none());
        let lifecycle = state.manager.get(uuid).expect("lifecycle");
        assert_eq!(lifecycle.state, SandboxState::RecoveryRequired);
        assert_eq!(lifecycle.backend_ownership, BackendOwnership::Stopped);
        assert_eq!(
            lifecycle
                .operation
                .as_ref()
                .and_then(|operation| operation.phase),
            Some(OperationPhase::HibernateBackendStopped)
        );
        assert!(state.manager.destroy(uuid).await.expect("destroy"));
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn resume_start_failure_preserves_retryable_hibernation() {
        let temp = tempfile::tempdir().expect("temp");
        let state = mock_state(&temp, false);
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id");
        let uuid = Uuid::parse_str(id).expect("uuid");
        state
            .manager
            .hibernate(
                uuid,
                HibernateSandbox {
                    binary_path: PathBuf::new(),
                },
            )
            .await
            .expect("hibernate");
        let hook = crate::failpoint::TestFailpoint::new(&["resume-backend-start"]);

        hook.run(state.manager.resume(
            uuid,
            ResumeSandbox {
                binary_path: PathBuf::new(),
            },
        ))
        .await
        .expect_err("resume start failure");

        assert!(state.manager.backend_owner(uuid).is_none());
        let lifecycle = state.manager.get(uuid).expect("lifecycle");
        assert_eq!(lifecycle.state, SandboxState::Hibernated);
        assert_eq!(lifecycle.backend_ownership, BackendOwnership::Stopped);
        assert!(lifecycle.operation.is_none());
        state
            .manager
            .resume(
                uuid,
                ResumeSandbox {
                    binary_path: PathBuf::new(),
                },
            )
            .await
            .expect("retry resume");
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn resume_readiness_failure_cleans_the_replacement_backend() {
        let temp = tempfile::tempdir().expect("temp");
        let state = mock_state(&temp, false);
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id");
        let uuid = Uuid::parse_str(id).expect("uuid");
        state
            .manager
            .hibernate(
                uuid,
                HibernateSandbox {
                    binary_path: PathBuf::new(),
                },
            )
            .await
            .expect("hibernate");
        let hook = crate::failpoint::TestFailpoint::new(&["resume-guest-ready"]);

        hook.run(state.manager.resume(
            uuid,
            ResumeSandbox {
                binary_path: PathBuf::new(),
            },
        ))
        .await
        .expect_err("readiness failure");

        assert!(state.manager.backend_owner(uuid).is_none());
        let lifecycle = state.manager.get(uuid).expect("lifecycle");
        assert_eq!(lifecycle.state, SandboxState::Hibernated);
        assert_eq!(lifecycle.backend_ownership, BackendOwnership::Stopped);
        assert!(lifecycle.operation.is_none());
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn resume_cleanup_failure_retains_the_replacement_owner() {
        let temp = tempfile::tempdir().expect("temp");
        let state = mock_state(&temp, false);
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id");
        let uuid = Uuid::parse_str(id).expect("uuid");
        state
            .manager
            .hibernate(
                uuid,
                HibernateSandbox {
                    binary_path: PathBuf::new(),
                },
            )
            .await
            .expect("hibernate");
        let hook =
            crate::failpoint::TestFailpoint::new(&["resume-guest-ready", "resume-backend-stop"]);

        let error = hook
            .run(state.manager.resume(
                uuid,
                ResumeSandbox {
                    binary_path: PathBuf::new(),
                },
            ))
            .await
            .expect_err("failed cleanup must retain ownership");

        assert!(matches!(error, BlazeDaemonError::RecoveryRequired(_)));
        assert!(state.manager.backend_owner(uuid).is_some());
        let lifecycle = state.manager.get(uuid).expect("lifecycle");
        assert_eq!(lifecycle.state, SandboxState::RecoveryRequired);
        assert_eq!(lifecycle.backend_ownership, BackendOwnership::Unknown);
        assert_eq!(
            lifecycle.operation.as_ref().map(|operation| operation.kind),
            Some(OperationKind::Resume)
        );
        assert_eq!(
            lifecycle
                .operation
                .as_ref()
                .and_then(|operation| operation.phase),
            Some(OperationPhase::ResumeBackendStarted)
        );
        assert!(state.manager.destroy(uuid).await.expect("destroy"));
    }

    #[tokio::test]
    async fn rollback_replaces_runtime_state_without_rewriting_capture_history() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            config.storage.instances_dir.clone(),
        ));
        let state = build_test_state(
            config,
            test_policy(BackendKind::Mock, false),
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            storage,
        );
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id");
        let uuid = Uuid::parse_str(id).expect("uuid");
        let slot = state.storage.reconstruct(id).await.expect("storage slot");

        tokio::fs::write(&slot.rootfs_path, b"first-rootfs")
            .await
            .expect("first rootfs");
        state
            .manager
            .write_file(uuid, "/tmp/value".to_string(), b"first-memory")
            .await
            .expect("first guest state");
        let (_, first) = dispatched_json(
            &state,
            Method::POST,
            &format!("/v1/instances/{id}/checkpoint"),
            Vec::new(),
        )
        .await;
        let first_id = first["id"].as_str().expect("first checkpoint");

        tokio::fs::write(&slot.rootfs_path, b"second-rootfs")
            .await
            .expect("second rootfs");
        state
            .manager
            .write_file(uuid, "/tmp/value".to_string(), b"second-memory")
            .await
            .expect("second guest state");
        let (_, second) = dispatched_json(
            &state,
            Method::POST,
            &format!("/v1/sandboxes/{id}/checkpoint"),
            Vec::new(),
        )
        .await;
        let second_id = second["id"].as_str().expect("second checkpoint");

        tokio::fs::write(&slot.rootfs_path, b"third-rootfs")
            .await
            .expect("third rootfs");
        state
            .manager
            .write_file(uuid, "/tmp/value".to_string(), b"third-memory")
            .await
            .expect("third guest state");

        let (status, restored) = dispatched_json(
            &state,
            Method::POST,
            &format!("/v1/sandboxes/{id}/rollback/{first_id}"),
            Vec::new(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(restored["instance_id"], id);
        assert_eq!(restored["checkpoint_id"], first_id);
        assert_eq!(restored["restored"], true);
        assert_eq!(restored["state"], "running");
        assert_eq!(
            tokio::fs::read(&slot.rootfs_path)
                .await
                .expect("restored rootfs"),
            b"first-rootfs"
        );
        assert_eq!(
            state
                .manager
                .read_file(uuid, "/tmp/value".to_string())
                .await
                .expect("restored guest state"),
            b"first-memory"
        );

        let lifecycle = state.manager.get(uuid).expect("lifecycle");
        assert_eq!(lifecycle.state, SandboxState::Running);
        assert!(lifecycle.operation.is_none());
        assert_eq!(lifecycle.last_checkpoint.as_deref(), Some(second_id));
        assert_eq!(
            state
                .manager
                .list_checkpoints(uuid)
                .await
                .expect("checkpoint list")
                .iter()
                .find(|checkpoint| checkpoint.is_head)
                .map(|checkpoint| checkpoint.id.as_str()),
            Some(first_id)
        );
        assert!(state.manager.backend_owner(uuid).is_some());
        for name in [
            ".rootfs.restore-copying",
            ".rootfs.restore-staged",
            ".rootfs.restore-backup",
            ".rootfs.restore-discard",
            ".rootfs.restore.json",
            ".rootfs.restore-journal.tmp",
        ] {
            assert!(!slot.instance_dir.join(name).exists(), "{name} remains");
        }
    }

    #[tokio::test]
    async fn rollback_rejects_an_unavailable_adapter_before_mutation() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            config.storage.instances_dir.clone(),
        ));
        let state = build_test_state(
            config,
            test_policy(BackendKind::Mock, false),
            spawners(BackendKind::Mock, Arc::new(CaptureOnlyMockSpawner)),
            BackendKind::Mock,
            storage,
        );
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id");
        let uuid = Uuid::parse_str(id).expect("uuid");
        let slot = write_checkpoint_fixture(&state, id).await;
        let checkpoint = state.manager.checkpoint(uuid).await.expect("checkpoint");
        tokio::fs::write(&slot.rootfs_path, b"current-rootfs")
            .await
            .expect("current rootfs");
        let owner = state.manager.backend_owner(uuid).expect("backend owner");

        let error = state
            .manager
            .restore(
                uuid,
                RestoreSandbox {
                    checkpoint_id: checkpoint.id,
                    binary_path: PathBuf::new(),
                },
            )
            .await
            .expect_err("restore must require an adapter");

        assert!(matches!(error, BlazeDaemonError::UnsupportedOperation(_)));
        assert_eq!(
            tokio::fs::read(&slot.rootfs_path)
                .await
                .expect("unchanged rootfs"),
            b"current-rootfs"
        );
        let retained = state.manager.backend_owner(uuid).expect("retained owner");
        assert!(Arc::ptr_eq(&owner, &retained));
        let lifecycle = state.manager.get(uuid).expect("lifecycle");
        assert_eq!(lifecycle.state, SandboxState::Running);
        assert!(lifecycle.operation.is_none());
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn restore_stage_failure_keeps_the_current_runtime_running() {
        let temp = tempfile::tempdir().expect("temp");
        let state = mock_state(&temp, false);
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id");
        let uuid = Uuid::parse_str(id).expect("uuid");
        let slot = write_checkpoint_fixture(&state, id).await;
        let checkpoint = state.manager.checkpoint(uuid).await.expect("checkpoint");
        tokio::fs::write(&slot.rootfs_path, b"current-rootfs")
            .await
            .expect("current rootfs");
        let owner = state.manager.backend_owner(uuid).expect("backend owner");
        let hook = crate::failpoint::TestFailpoint::new(&["restore-storage-stage"]);

        hook.run(state.manager.restore(
            uuid,
            RestoreSandbox {
                checkpoint_id: checkpoint.id,
                binary_path: PathBuf::new(),
            },
        ))
        .await
        .expect_err("stage failure");

        let retained = state.manager.backend_owner(uuid).expect("retained owner");
        assert!(Arc::ptr_eq(&owner, &retained));
        assert_eq!(
            tokio::fs::read(&slot.rootfs_path)
                .await
                .expect("unchanged rootfs"),
            b"current-rootfs"
        );
        let lifecycle = state.manager.get(uuid).expect("lifecycle");
        assert_eq!(lifecycle.state, SandboxState::Running);
        assert!(lifecycle.operation.is_none());
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn uncertain_backend_stop_retains_the_current_owner_and_rootfs() {
        let temp = tempfile::tempdir().expect("temp");
        let state = mock_state(&temp, false);
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id");
        let uuid = Uuid::parse_str(id).expect("uuid");
        let slot = write_checkpoint_fixture(&state, id).await;
        let checkpoint = state.manager.checkpoint(uuid).await.expect("checkpoint");
        tokio::fs::write(&slot.rootfs_path, b"current-rootfs")
            .await
            .expect("current rootfs");
        let owner = state.manager.backend_owner(uuid).expect("backend owner");
        let hook = crate::failpoint::TestFailpoint::new(&["restore-backend-stop"]);

        let error = hook
            .run(state.manager.restore(
                uuid,
                RestoreSandbox {
                    checkpoint_id: checkpoint.id,
                    binary_path: PathBuf::new(),
                },
            ))
            .await
            .expect_err("backend stop outcome must require recovery");

        assert!(matches!(error, BlazeDaemonError::RecoveryRequired(_)));
        let retained = state.manager.backend_owner(uuid).expect("retained owner");
        assert!(Arc::ptr_eq(&owner, &retained));
        assert_eq!(
            tokio::fs::read(&slot.rootfs_path)
                .await
                .expect("unchanged rootfs"),
            b"current-rootfs"
        );
        let lifecycle = state.manager.get(uuid).expect("lifecycle");
        assert_eq!(lifecycle.state, SandboxState::RecoveryRequired);
        assert_eq!(lifecycle.backend_ownership, BackendOwnership::Unknown);
        assert_eq!(
            lifecycle
                .operation
                .as_ref()
                .and_then(|operation| operation.phase),
            Some(OperationPhase::RestoreStorageStaged)
        );
        for name in [
            ".rootfs.restore-staged",
            ".rootfs.restore-backup",
            ".rootfs.restore.json",
        ] {
            assert!(!slot.instance_dir.join(name).exists(), "{name} remains");
        }
        assert!(state.manager.destroy(uuid).await.expect("destroy"));
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn uncertain_head_update_retains_the_replacement_owner() {
        let temp = tempfile::tempdir().expect("temp");
        let state = mock_state(&temp, false);
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id");
        let uuid = Uuid::parse_str(id).expect("uuid");
        let slot = write_checkpoint_fixture(&state, id).await;
        let checkpoint = state.manager.checkpoint(uuid).await.expect("checkpoint");
        tokio::fs::write(&slot.rootfs_path, b"later-checkpoint-rootfs")
            .await
            .expect("later checkpoint rootfs");
        let latest = state
            .manager
            .checkpoint(uuid)
            .await
            .expect("later checkpoint");
        tokio::fs::write(&slot.rootfs_path, b"current-rootfs")
            .await
            .expect("current rootfs");
        let hook = crate::failpoint::TestFailpoint::new(&["checkpoint-store-head-after-rename"]);

        let error = hook
            .run(state.manager.restore(
                uuid,
                RestoreSandbox {
                    checkpoint_id: checkpoint.id.clone(),
                    binary_path: PathBuf::new(),
                },
            ))
            .await
            .expect_err("HEAD update must be reported");

        assert!(matches!(error, BlazeDaemonError::RecoveryRequired(_)));
        assert_eq!(
            tokio::fs::read(&slot.rootfs_path)
                .await
                .expect("selected rootfs"),
            b"checkpoint-rootfs"
        );
        assert!(state.manager.backend_owner(uuid).is_some());
        let lifecycle = state.manager.get(uuid).expect("lifecycle");
        assert_eq!(lifecycle.state, SandboxState::RecoveryRequired);
        assert_eq!(lifecycle.backend_ownership, BackendOwnership::Running);
        assert_eq!(
            lifecycle
                .operation
                .as_ref()
                .and_then(|operation| operation.phase),
            Some(OperationPhase::RestoreBackendStarted)
        );
        assert_eq!(
            lifecycle.last_checkpoint.as_deref(),
            Some(latest.id.as_str())
        );
        assert_eq!(
            state
                .manager
                .list_checkpoints(uuid)
                .await
                .expect("observable checkpoint catalog")
                .iter()
                .find(|item| item.is_head)
                .map(|item| item.id.as_str()),
            Some(checkpoint.id.as_str())
        );

        assert!(state.manager.destroy(uuid).await.expect("destroy"));
        assert_eq!(
            state.manager.get(uuid).expect("destroyed").state,
            SandboxState::Destroyed
        );
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn final_state_failure_keeps_the_committed_restore_journal() {
        let temp = tempfile::tempdir().expect("temp");
        let state = mock_state(&temp, false);
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id");
        let uuid = Uuid::parse_str(id).expect("uuid");
        let slot = write_checkpoint_fixture(&state, id).await;
        let checkpoint = state.manager.checkpoint(uuid).await.expect("checkpoint");
        tokio::fs::write(&slot.rootfs_path, b"current-rootfs")
            .await
            .expect("current rootfs");
        let hook = crate::failpoint::TestFailpoint::new(&["restore-final-state"]);

        let error = hook
            .run(state.manager.restore(
                uuid,
                RestoreSandbox {
                    checkpoint_id: checkpoint.id.clone(),
                    binary_path: PathBuf::new(),
                },
            ))
            .await
            .expect_err("final state failure");

        assert!(matches!(error, BlazeDaemonError::RecoveryRequired(_)));
        assert_eq!(
            tokio::fs::read(&slot.rootfs_path)
                .await
                .expect("committed rootfs"),
            b"checkpoint-rootfs"
        );
        let lifecycle = state.manager.get(uuid).expect("lifecycle");
        assert_eq!(lifecycle.state, SandboxState::RecoveryRequired);
        assert_eq!(lifecycle.backend_ownership, BackendOwnership::Running);
        assert_eq!(
            lifecycle
                .operation
                .as_ref()
                .map(|operation| (operation.checkpoint_id.as_deref(), operation.phase)),
            Some((
                Some(checkpoint.id.as_str()),
                Some(OperationPhase::RestoreStorageCommitted)
            ))
        );
        assert_eq!(
            state
                .manager
                .list_checkpoints(uuid)
                .await
                .expect("checkpoint list")
                .iter()
                .find(|item| item.is_head)
                .map(|item| item.id.as_str()),
            Some(checkpoint.id.as_str())
        );
        assert!(state.manager.backend_owner(uuid).is_some());
        assert!(state.manager.destroy(uuid).await.expect("destroy"));
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn cancelled_restore_after_head_is_destroyable() {
        let temp = tempfile::tempdir().expect("temp");
        let state = mock_state(&temp, false);
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id").to_string();
        let uuid = Uuid::parse_str(&id).expect("uuid");
        write_checkpoint_fixture(&state, &id).await;
        let checkpoint = state.manager.checkpoint(uuid).await.expect("checkpoint");
        let hook = crate::failpoint::TestFailpoint::new(&["restore-after-head"]);
        let restore_state = state.clone();
        let restore_hook = hook.clone();
        let restore = tokio::spawn(async move {
            restore_hook
                .run(restore_state.manager.restore(
                    uuid,
                    RestoreSandbox {
                        checkpoint_id: checkpoint.id,
                        binary_path: PathBuf::new(),
                    },
                ))
                .await
        });
        hook.wait_until_paused().await;

        let persisted =
            SandboxInstance::load(&state.state_dir, uuid).expect("persisted restore journal");
        assert_eq!(persisted.state, SandboxState::Restoring);
        assert_eq!(
            persisted.operation.and_then(|operation| operation.phase),
            Some(OperationPhase::RestoreHeadUpdated)
        );
        assert_eq!(persisted.backend_ownership, BackendOwnership::Running);
        assert!(state.manager.backend_owner(uuid).is_some());

        restore.abort();
        assert!(restore.await.expect_err("cancelled restore").is_cancelled());
        assert!(state.manager.destroy(uuid).await.expect("destroy"));
        assert_eq!(
            state.manager.get(uuid).expect("destroyed").state,
            SandboxState::Destroyed
        );
        assert!(
            !state
                .config
                .lock()
                .expect("config")
                .storage
                .instances_dir
                .join(id)
                .exists()
        );
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn checkpoint_snapshot_failure_resumes_and_clears_the_journal() {
        let temp = tempfile::tempdir().expect("temp");
        let state = mock_state(&temp, false);
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id");
        let uuid = Uuid::parse_str(id).expect("uuid");
        let hook = crate::failpoint::TestFailpoint::new(&["checkpoint-snapshot"]);

        let error = hook
            .run(state.manager.checkpoint(uuid))
            .await
            .expect_err("snapshot failure");

        assert!(matches!(
            error,
            BlazeDaemonError::Core(BlazeError::BackendError { .. })
        ));
        let lifecycle = state.manager.get(uuid).expect("lifecycle");
        assert_eq!(lifecycle.state, SandboxState::Running);
        assert!(lifecycle.operation.is_none());
        assert_eq!(
            SandboxInstance::load(&state.state_dir, uuid)
                .expect("persisted lifecycle")
                .operation,
            None
        );
        let checkpoint_dir = state.state_dir.join("checkpoints").join(id);
        let staging = std::fs::read_dir(checkpoint_dir)
            .expect("checkpoint directory")
            .filter_map(std::result::Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().starts_with(".ckpt-"))
            .count();
        assert_eq!(staging, 0);
        assert!(state.manager.backend_owner(uuid).is_some());
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn checkpoint_prepublication_failure_discards_the_stage() {
        let temp = tempfile::tempdir().expect("temp");
        let state = mock_state(&temp, false);
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id");
        let uuid = Uuid::parse_str(id).expect("uuid");
        write_checkpoint_fixture(&state, id).await;
        let hook = crate::failpoint::TestFailpoint::new(&["checkpoint-publish"]);

        let error = hook
            .run(state.manager.checkpoint(uuid))
            .await
            .expect_err("publication must fail before the store call");

        assert!(matches!(
            error,
            BlazeDaemonError::Core(BlazeError::StorageError { .. })
        ));
        let lifecycle = state.manager.get(uuid).expect("lifecycle");
        assert_eq!(lifecycle.state, SandboxState::Running);
        assert!(lifecycle.operation.is_none());
        assert!(
            state
                .manager
                .list_checkpoints(uuid)
                .await
                .expect("checkpoint catalog")
                .is_empty()
        );
        assert!(state.manager.backend_owner(uuid).is_some());
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn checkpoint_state_failures_retain_the_reached_durable_phase() {
        for (failpoint, expected_phase, expected_head) in [
            (
                "checkpoint-published-state",
                OperationPhase::CheckpointPublished,
                false,
            ),
            (
                "checkpoint-head-state",
                OperationPhase::CheckpointHeadUpdated,
                true,
            ),
        ] {
            let temp = tempfile::tempdir().expect("temp");
            let state = mock_state(&temp, false);
            let created = created_json(&state, &test_request()).await;
            let id = created["instance"]["id"].as_str().expect("id");
            let uuid = Uuid::parse_str(id).expect("uuid");
            write_checkpoint_fixture(&state, id).await;
            let hook = crate::failpoint::TestFailpoint::new(&[failpoint]);

            let error = hook
                .run(state.manager.checkpoint(uuid))
                .await
                .expect_err("state commit must fail");

            assert!(matches!(error, BlazeDaemonError::RecoveryRequired(_)));
            let lifecycle = state.manager.get(uuid).expect("lifecycle");
            assert_eq!(lifecycle.state, SandboxState::RecoveryRequired);
            assert_eq!(
                lifecycle
                    .operation
                    .as_ref()
                    .and_then(|journal| journal.phase),
                Some(expected_phase)
            );
            let checkpoints = state
                .manager
                .list_checkpoints(uuid)
                .await
                .expect("published checkpoint");
            assert_eq!(checkpoints.len(), 1);
            assert_eq!(checkpoints[0].is_head, expected_head);
            assert!(state.manager.backend_owner(uuid).is_some());
        }
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn checkpoint_store_boundary_failures_preserve_observable_catalog_truth() {
        for (failpoint, expected_phase, expected_head) in [
            (
                "checkpoint-store-publish-after-rename",
                OperationPhase::CheckpointPaused,
                false,
            ),
            (
                "checkpoint-store-head-after-rename",
                OperationPhase::CheckpointPublished,
                true,
            ),
        ] {
            let temp = tempfile::tempdir().expect("temp");
            let state = mock_state(&temp, false);
            let created = created_json(&state, &test_request()).await;
            let id = created["instance"]["id"].as_str().expect("id");
            let uuid = Uuid::parse_str(id).expect("uuid");
            write_checkpoint_fixture(&state, id).await;
            let hook = crate::failpoint::TestFailpoint::new(&[failpoint]);

            let error = hook
                .run(state.manager.checkpoint(uuid))
                .await
                .expect_err("durability boundary must report an uncertain result");

            assert!(matches!(error, BlazeDaemonError::RecoveryRequired(_)));
            let lifecycle = state.manager.get(uuid).expect("lifecycle");
            assert_eq!(lifecycle.state, SandboxState::RecoveryRequired);
            assert_eq!(
                lifecycle
                    .operation
                    .as_ref()
                    .and_then(|journal| journal.phase),
                Some(expected_phase)
            );
            let checkpoints = state
                .manager
                .list_checkpoints(uuid)
                .await
                .expect("observable checkpoint catalog");
            assert_eq!(checkpoints.len(), 1);
            assert_eq!(checkpoints[0].is_head, expected_head);
            assert!(state.manager.backend_owner(uuid).is_some());
        }
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn startup_cleans_a_terminal_checkpoint_prune_tombstone() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            config.storage.instances_dir.clone(),
        ));
        let state = build_test_state(
            config.clone(),
            test_policy(BackendKind::Mock, false),
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            storage,
        );
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id").to_string();
        let uuid = Uuid::parse_str(&id).expect("uuid");
        write_checkpoint_fixture(&state, &id).await;
        let head_hook = crate::failpoint::TestFailpoint::new(&["checkpoint-head-update"]);
        head_hook
            .run(state.manager.checkpoint(uuid))
            .await
            .expect_err("checkpoint must remain published but unreachable");
        state.manager.destroy(uuid).await.expect("destroy sandbox");

        let prune_hook =
            crate::failpoint::TestFailpoint::new(&["checkpoint-prune-after-tombstone"]);
        prune_hook
            .run(state.manager.prune_checkpoints(uuid))
            .await
            .expect_err("prune must stop after the durable tombstone");
        let checkpoint_dir = state.state_dir.join("checkpoints").join(&id);
        assert!(
            std::fs::read_dir(&checkpoint_dir)
                .expect("checkpoint catalog")
                .filter_map(std::result::Result::ok)
                .any(|entry| entry.file_name().to_string_lossy().starts_with(".prune."))
        );
        drop(state);

        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            config.storage.instances_dir.clone(),
        ));
        let restarted = build_test_state(
            config,
            test_policy(BackendKind::Mock, false),
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            storage,
        );
        let report = restarted.manager.reconcile_startup().await;
        assert_eq!(report.attempted, 1);
        assert_eq!(report.completed, 1);
        assert!(report.failures.is_empty());
        assert!(
            !std::fs::read_dir(&checkpoint_dir)
                .expect("checkpoint catalog")
                .filter_map(std::result::Result::ok)
                .any(|entry| entry.file_name().to_string_lossy().starts_with(".prune."))
        );
        assert!(
            restarted
                .manager
                .list_checkpoints(uuid)
                .await
                .expect("empty checkpoint catalog")
                .is_empty()
        );
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn checkpoint_prune_removes_an_unreachable_publication() {
        let temp = tempfile::tempdir().expect("temp");
        let state = mock_state(&temp, false);
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id");
        let uuid = Uuid::parse_str(id).expect("uuid");
        write_checkpoint_fixture(&state, id).await;
        let head_hook = crate::failpoint::TestFailpoint::new(&["checkpoint-head-update"]);
        head_hook
            .run(state.manager.checkpoint(uuid))
            .await
            .expect_err("checkpoint must remain published but unreachable");
        let checkpoint_id = state
            .manager
            .list_checkpoints(uuid)
            .await
            .expect("unreachable checkpoint")
            .pop()
            .expect("checkpoint")
            .id;

        let (status, pruned) = dispatched_json(
            &state,
            Method::POST,
            &format!("/v1/sandboxes/{id}/checkpoints/prune"),
            Vec::new(),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(pruned["count"], 1);
        assert_eq!(pruned["removed"], json!([checkpoint_id]));
        assert!(
            state
                .manager
                .list_checkpoints(uuid)
                .await
                .expect("empty checkpoint catalog")
                .is_empty()
        );
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn prune_retry_cleans_a_prior_checkpoint_tombstone() {
        let temp = tempfile::tempdir().expect("temp");
        let state = mock_state(&temp, false);
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id").to_string();
        let uuid = Uuid::parse_str(&id).expect("uuid");
        write_checkpoint_fixture(&state, &id).await;
        let head_hook = crate::failpoint::TestFailpoint::new(&["checkpoint-head-update"]);
        head_hook
            .run(state.manager.checkpoint(uuid))
            .await
            .expect_err("checkpoint must remain published but unreachable");
        let prune_hook =
            crate::failpoint::TestFailpoint::new(&["checkpoint-prune-after-tombstone"]);
        prune_hook
            .run(state.manager.prune_checkpoints(uuid))
            .await
            .expect_err("first prune must stop after the durable tombstone");

        assert!(
            state
                .manager
                .prune_checkpoints(uuid)
                .await
                .expect("retry prune")
                .is_empty()
        );
        assert!(
            state
                .manager
                .list_checkpoints(uuid)
                .await
                .expect("empty checkpoint catalog")
                .is_empty()
        );
        assert!(
            !std::fs::read_dir(state.state_dir.join("checkpoints").join(&id))
                .expect("checkpoint catalog")
                .filter_map(std::result::Result::ok)
                .any(|entry| entry.file_name().to_string_lossy().starts_with(".prune."))
        );
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn checkpoint_resume_failure_keeps_head_and_runtime_ownership() {
        let temp = tempfile::tempdir().expect("temp");
        let state = mock_state(&temp, false);
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id");
        let uuid = Uuid::parse_str(id).expect("uuid");
        write_checkpoint_fixture(&state, id).await;
        let hook = crate::failpoint::TestFailpoint::new(&["checkpoint-resume"]);

        let error = hook
            .run(state.manager.checkpoint(uuid))
            .await
            .expect_err("resume failure");

        assert!(matches!(error, BlazeDaemonError::RecoveryRequired(_)));
        let lifecycle = state.manager.get(uuid).expect("lifecycle");
        assert_eq!(lifecycle.state, SandboxState::RecoveryRequired);
        assert_eq!(
            lifecycle
                .operation
                .as_ref()
                .and_then(|journal| journal.phase),
            Some(OperationPhase::CheckpointHeadUpdated)
        );
        assert!(state.manager.backend_owner(uuid).is_some());
        let checkpoints = state
            .manager
            .list_checkpoints(uuid)
            .await
            .expect("committed checkpoint");
        assert_eq!(checkpoints.len(), 1);
        assert!(checkpoints[0].is_head);

        state.manager.destroy(uuid).await.expect("destroy retry");
        assert_eq!(
            state.manager.get(uuid).expect("destroyed").state,
            SandboxState::Destroyed
        );
        assert_eq!(
            state
                .manager
                .list_checkpoints(uuid)
                .await
                .expect("durable checkpoint history")
                .len(),
            1
        );
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn published_checkpoint_holds_the_operation_lock_until_head_commit() {
        let temp = tempfile::tempdir().expect("temp");
        let state = mock_state(&temp, false);
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id");
        let uuid = Uuid::parse_str(id).expect("uuid");
        write_checkpoint_fixture(&state, id).await;
        let hook = crate::failpoint::TestFailpoint::new(&["checkpoint-after-publish-before-head"]);
        let capture_state = state.clone();
        let capture_hook = hook.clone();
        let capture = tokio::spawn(async move {
            capture_hook
                .run(capture_state.manager.checkpoint(uuid))
                .await
        });
        hook.wait_until_paused().await;

        let persisted =
            SandboxInstance::load(&state.state_dir, uuid).expect("persisted checkpoint journal");
        assert_eq!(persisted.state, SandboxState::Paused);
        assert_eq!(
            persisted.operation.and_then(|journal| journal.phase),
            Some(OperationPhase::CheckpointPublished)
        );
        let list_state = state.clone();
        let mut list = tokio::spawn(async move { list_state.manager.list_checkpoints(uuid).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut list)
                .await
                .is_err(),
            "checkpoint listing must wait for a consistent catalog boundary"
        );
        let destroy_state = state.clone();
        let mut destroy = tokio::spawn(async move { destroy_state.manager.destroy(uuid).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut destroy)
                .await
                .is_err(),
            "destroy must wait for checkpoint ownership"
        );

        hook.release();
        capture
            .await
            .expect("capture task")
            .expect("checkpoint capture");
        let checkpoints = list.await.expect("list task").expect("checkpoint list");
        assert_eq!(checkpoints.len(), 1);
        assert!(checkpoints[0].is_head);
        assert!(destroy.await.expect("destroy task").expect("destroy"));
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn cancelled_published_checkpoint_is_destroyable() {
        let temp = tempfile::tempdir().expect("temp");
        let state = mock_state(&temp, false);
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id").to_string();
        let uuid = Uuid::parse_str(&id).expect("uuid");
        write_checkpoint_fixture(&state, &id).await;
        let hook = crate::failpoint::TestFailpoint::new(&["checkpoint-after-publish-before-head"]);
        let capture_state = state.clone();
        let capture_hook = hook.clone();
        let capture = tokio::spawn(async move {
            capture_hook
                .run(capture_state.manager.checkpoint(uuid))
                .await
        });
        hook.wait_until_paused().await;
        capture.abort();
        let _ = capture.await;

        let interrupted = state.manager.get(uuid).expect("interrupted lifecycle");
        assert_eq!(interrupted.state, SandboxState::Paused);
        assert_eq!(
            interrupted.operation.and_then(|journal| journal.phase),
            Some(OperationPhase::CheckpointPublished)
        );
        assert!(
            !state
                .state_dir
                .join("checkpoints")
                .join(&id)
                .join("HEAD")
                .exists()
        );

        state
            .manager
            .destroy(uuid)
            .await
            .expect("destroy interrupted capture");
        let checkpoints = state
            .manager
            .list_checkpoints(uuid)
            .await
            .expect("unreachable checkpoint");
        assert_eq!(checkpoints.len(), 1);
        assert!(!checkpoints[0].is_head);
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn cancelled_checkpoint_phases_are_destroyable_before_and_after_restart() {
        for (failpoint, expected_state, expected_phase, checkpoint_count) in [
            (
                "checkpoint-after-begin",
                SandboxState::Running,
                OperationPhase::CheckpointPreparing,
                0,
            ),
            (
                "checkpoint-after-pause",
                SandboxState::Paused,
                OperationPhase::CheckpointPaused,
                0,
            ),
            (
                "checkpoint-after-head",
                SandboxState::Paused,
                OperationPhase::CheckpointHeadUpdated,
                1,
            ),
        ] {
            let temp = tempfile::tempdir().expect("temp");
            let state = mock_state(&temp, false);
            let created = created_json(&state, &test_request()).await;
            let id = created["instance"]["id"].as_str().expect("id").to_string();
            let uuid = Uuid::parse_str(&id).expect("uuid");
            write_checkpoint_fixture(&state, &id).await;
            cancel_checkpoint_at(&state, uuid, failpoint).await;
            let interrupted = state.manager.get(uuid).expect("interrupted lifecycle");
            assert_eq!(interrupted.state, expected_state);
            assert_eq!(
                interrupted.operation.and_then(|journal| journal.phase),
                Some(expected_phase)
            );

            state
                .manager
                .destroy(uuid)
                .await
                .expect("same-process destroy");
            let destroyed = state.manager.get(uuid).expect("destroyed lifecycle");
            assert_eq!(destroyed.state, SandboxState::Destroyed);
            assert!(destroyed.operation.is_none());
            assert_eq!(
                state
                    .manager
                    .list_checkpoints(uuid)
                    .await
                    .expect("checkpoint history")
                    .len(),
                checkpoint_count
            );

            let restart_temp = tempfile::tempdir().expect("restart temp");
            let config = test_config(&restart_temp);
            let restart_state = mock_state_from_config(config.clone(), false);
            let created = created_json(&restart_state, &test_request()).await;
            let restart_id = created["instance"]["id"]
                .as_str()
                .expect("restart id")
                .to_string();
            let restart_uuid = Uuid::parse_str(&restart_id).expect("restart uuid");
            write_checkpoint_fixture(&restart_state, &restart_id).await;
            cancel_checkpoint_at(&restart_state, restart_uuid, failpoint).await;
            restart_state
                .manager
                .backend_owner(restart_uuid)
                .expect("backend owner")
                .kill()
                .await
                .expect("simulate daemon exit");
            drop(restart_state);

            let restarted = mock_state_from_config(config, false);
            let report = restarted.manager.reconcile_startup().await;
            assert_eq!(report.attempted, 1);
            assert_eq!(report.completed, 1);
            assert!(report.failures.is_empty());
            let destroyed = restarted
                .manager
                .get(restart_uuid)
                .expect("reconciled lifecycle");
            assert_eq!(destroyed.state, SandboxState::Destroyed);
            assert!(destroyed.operation.is_none());
            assert_eq!(
                restarted
                    .manager
                    .list_checkpoints(restart_uuid)
                    .await
                    .expect("reconciled checkpoint history")
                    .len(),
                checkpoint_count
            );
            let checkpoint_dir = restarted.state_dir.join("checkpoints").join(&restart_id);
            if checkpoint_dir.exists() {
                assert!(
                    !std::fs::read_dir(checkpoint_dir)
                        .expect("checkpoint catalog")
                        .filter_map(std::result::Result::ok)
                        .any(|entry| entry.file_name().to_string_lossy().starts_with('.'))
                );
            }
        }
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn guest_operations_wait_for_checkpoint_publication() {
        let temp = tempfile::tempdir().expect("temp");
        let state = mock_state(&temp, false);
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id");
        let uuid = Uuid::parse_str(id).expect("uuid");
        write_checkpoint_fixture(&state, id).await;
        state
            .manager
            .write_file(uuid, "/tmp/existing".into(), b"before")
            .await
            .expect("seed guest file");
        let hook = crate::failpoint::TestFailpoint::new(&["checkpoint-after-publish-before-head"]);
        let capture_state = state.clone();
        let capture_hook = hook.clone();
        let capture = tokio::spawn(async move {
            capture_hook
                .run(capture_state.manager.checkpoint(uuid))
                .await
        });
        hook.wait_until_paused().await;

        let exec_state = state.clone();
        let mut exec = tokio::spawn(async move {
            exec_state
                .manager
                .exec(uuid, "printf locked".into(), None, None, 5)
                .await
        });
        let read_state = state.clone();
        let mut read = tokio::spawn(async move {
            read_state
                .manager
                .read_file(uuid, "/tmp/existing".into())
                .await
        });
        let write_state = state.clone();
        let mut write = tokio::spawn(async move {
            write_state
                .manager
                .write_file(uuid, "/tmp/after".into(), b"after")
                .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut exec)
                .await
                .is_err(),
            "guest exec must wait for checkpoint ownership"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut read)
                .await
                .is_err(),
            "guest read must wait for checkpoint ownership"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut write)
                .await
                .is_err(),
            "guest write must wait for checkpoint ownership"
        );

        hook.release();
        capture
            .await
            .expect("capture task")
            .expect("checkpoint capture");
        assert_eq!(
            exec.await.expect("exec task").expect("guest exec").stdout,
            b"printf locked"
        );
        assert_eq!(
            read.await.expect("read task").expect("guest read"),
            b"before"
        );
        write.await.expect("write task").expect("guest write");
    }

    #[tokio::test]
    async fn reset_rejects_state_only_pool_return() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            config.storage.instances_dir.clone(),
        ));
        let state = build_test_state(
            config,
            test_policy(BackendKind::Mock, true),
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            storage,
        );
        let request = test_request();
        let created = created_json(&state, &request).await;
        let id = created["instance"]["id"].as_str().expect("id");
        let uuid = Uuid::parse_str(id).expect("uuid");

        let error = reset_instance(&state, id)
            .await
            .expect_err("reset without a runtime implementation must fail closed");

        assert!(matches!(error, BlazeDaemonError::UnsupportedOperation(_)));
        assert_eq!(error.status_code(), 501);
        assert_eq!(
            state.instances.lock().expect("instances")[&uuid].state,
            SandboxState::Running
        );
        let key = PoolKey::new(
            BackendKind::Mock,
            WorkloadClass::AgentTool,
            "sha256:ownership-test".into(),
        );
        assert_eq!(state.pool.lock().expect("pool").stats(&key).warm_count, 0);
        assert!(state.manager.backend_owner(uuid).is_some());
    }

    #[tokio::test]
    async fn checkpoint_rejects_an_unfinished_lifecycle_journal() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            config.storage.instances_dir.clone(),
        ));
        let state = build_test_state(
            config,
            test_policy(BackendKind::Mock, false),
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            storage,
        );
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id");
        let uuid = Uuid::parse_str(id).expect("uuid");
        let journal = {
            let mut instances = state.instances.lock().expect("instances");
            let instance = instances.get_mut(&uuid).expect("instance");
            instance
                .begin_operation(OperationKind::Create)
                .expect("begin unfinished operation");
            instance.persist(&state.state_dir).expect("persist journal");
            instance.operation.clone().expect("journal")
        };

        let error = checkpoint(&state, id)
            .await
            .expect_err("unfinished lifecycle work must fail closed");

        assert!(matches!(error, BlazeDaemonError::RecoveryRequired(_)));
        assert_eq!(
            state.instances.lock().expect("instances")[&uuid].operation,
            Some(journal)
        );
        assert_eq!(
            SandboxInstance::load(&state.state_dir, uuid)
                .expect("persisted instance")
                .operation,
            state.instances.lock().expect("instances")[&uuid].operation
        );
    }

    #[tokio::test]
    async fn checkpoint_rejects_a_non_running_lifecycle_state() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            config.storage.instances_dir.clone(),
        ));
        let state = build_test_state(
            config,
            test_policy(BackendKind::Mock, false),
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            storage,
        );
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id");
        let uuid = Uuid::parse_str(id).expect("uuid");
        state.manager.destroy(uuid).await.expect("destroy");

        let error = checkpoint(&state, id)
            .await
            .expect_err("checkpoint must require a running instance");

        assert!(matches!(error, BlazeDaemonError::Conflict(_)));
        assert_eq!(error.status_code(), 409);
    }

    #[tokio::test]
    async fn quiescent_state_guard_serializes_later_destroy() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            config.storage.instances_dir.clone(),
        ));
        let state = build_test_state(
            config,
            test_policy(BackendKind::Mock, false),
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            storage,
        );
        let created = created_json(&state, &test_request()).await;
        let id = Uuid::parse_str(created["instance"]["id"].as_str().expect("id")).expect("uuid");
        let guard = state
            .manager
            .lock_quiescent_state(id, SandboxState::Running)
            .await
            .expect("quiescent running state");
        let destroy_state = state.clone();
        let mut destroy = tokio::spawn(async move { destroy_state.manager.destroy(id).await });

        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut destroy)
                .await
                .is_err(),
            "destroy must wait while a lifecycle operation holds the guard"
        );
        drop(guard);

        assert!(destroy.await.expect("destroy task").expect("destroy"));
        assert_eq!(
            state.manager.get(id).expect("instance").state,
            SandboxState::Destroyed
        );
    }

    #[tokio::test]
    async fn guest_manager_waits_for_the_lifecycle_lock() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            config.storage.instances_dir.clone(),
        ));
        let state = build_test_state(
            config,
            test_policy(BackendKind::Mock, false),
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            storage,
        );
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("instance id");
        let uuid = Uuid::parse_str(id).expect("uuid");

        let operation = state.manager.operation_lock(uuid).lock_owned().await;
        let exec_state = state.clone();
        let mut pending_exec = tokio::spawn(async move {
            exec_state
                .manager
                .exec(uuid, "printf guest-lock".into(), None, None, 5)
                .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut pending_exec)
                .await
                .is_err(),
            "guest operation bypassed the lifecycle lock"
        );
        drop(operation);

        let exec = pending_exec
            .await
            .expect("exec task")
            .expect("managed exec");
        assert_eq!(exec.stdout, b"printf guest-lock");
    }

    #[tokio::test]
    async fn manager_cleanup_releases_tracked_runtime_resources() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let instances_dir = config.storage.instances_dir.clone();
        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            instances_dir.clone(),
        ));
        let state = build_test_state(
            config,
            test_policy(BackendKind::Mock, false),
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            storage,
        );
        let created = created_json(&state, &test_request()).await;
        let id = Uuid::parse_str(created["instance"]["id"].as_str().expect("instance id"))
            .expect("uuid");
        {
            let mut instances = state.instances.lock().expect("instances");
            let instance = instances.get_mut(&id).expect("instance");
            instance
                .transition(SandboxState::Destroyed)
                .expect("persisted terminal state");
            instance.backend_ownership = BackendOwnership::Stopped;
            instance.finish_operation();
            instance.persist(&state.state_dir).expect("persist");
        }

        let report = state
            .manager
            .cleanup_owned_instances_with_timeout(Duration::from_secs(1))
            .await;

        assert!(report.failures.is_empty());
        assert_eq!(
            state.manager.get(id).expect("instance").state,
            SandboxState::Destroyed
        );
        assert!(state.manager.backend_owner(id).is_none());
        assert!(!instances_dir.join(id.to_string()).exists());
    }

    #[tokio::test]
    async fn warm_claim_validates_runtime_and_quarantines_dead_owner() {
        let temp = tempfile::tempdir().expect("temp");
        let mut config = DaemonConfig::default();
        config.daemon.state_dir = temp.path().join("state");
        config.storage.images_dir = temp.path().join("images");
        config.storage.instances_dir = temp.path().join("instances");
        config.runtime_templates.dir = temp.path().join("runtime-templates");
        std::fs::create_dir_all(&config.daemon.state_dir).expect("state");
        std::fs::create_dir_all(&config.storage.images_dir).expect("images");
        std::fs::create_dir_all(&config.storage.instances_dir).expect("instances");

        let policy = PolicyFile {
            manifest_version: 1,
            policy_name: "warm-validation".into(),
            priority: 100,
            match_: PolicyMatch {
                workload_class: WorkloadClass::AgentTool,
                image_labels: HashMap::new(),
            },
            select: PolicySelect {
                backend_priority: vec![BackendKind::Mock],
                kernel_hooks: vec![],
                templates: vec![],
                fallback_on_missing_hook: FallbackOnMissingHook::default(),
            },
            pool: Some(PolicyPool {
                enabled: true,
                min: 0,
                target: 0,
                max: 1,
                warm_ttl: Some("30m".into()),
                reset_mode: ResetMode::FullRecreate,
            }),
            checkpoint: None,
            quota: None,
            hooks: PolicyHooks::default(),
            backend: BackendConfigs::default(),
            vm: None,
        };
        let storage: Arc<dyn blaze_core::storage::StorageProvider> =
            Arc::new(FileStorageProvider::with_images(
                config.storage.images_dir.clone(),
                config.storage.instances_dir.clone(),
            ));
        let state = Arc::new(
            ServerState::build(
                config,
                PolicyEngine::with_policies(vec![policy]),
                PoolManager::new(),
                TemplateRegistry::new(),
                HookRegistry::new(),
                spawners(BackendKind::Mock, Arc::new(MockSpawner)),
                BackendKind::Mock,
                storage,
            )
            .expect("build server state"),
        );
        let request = serde_json::to_vec(&json!({
            "workload_class": "agent-tool",
            "image_digest": "sha256:warm-validation"
        }))
        .expect("request");

        let cold = create_instance(&state, &request)
            .await
            .expect("cold create");
        let cold: serde_json::Value =
            serde_json::from_slice(&cold.into_body().collect().await.expect("body").to_bytes())
                .expect("cold json");
        let id = cold["instance"]["id"].as_str().expect("id").to_string();
        return_to_pool_for_test(&state, &id)
            .await
            .expect("return to pool");

        let warm = create_instance(&state, &request)
            .await
            .expect("warm create");
        let warm: serde_json::Value =
            serde_json::from_slice(&warm.into_body().collect().await.expect("body").to_bytes())
                .expect("warm json");
        assert_eq!(warm["instance"]["id"], id);
        assert_eq!(warm["start_path"], "warm");

        return_to_pool_for_test(&state, &id)
            .await
            .expect("return live owner");
        let owner = state
            .manager
            .backend_owner(Uuid::parse_str(&id).expect("uuid"))
            .expect("owner");
        owner.kill().await.expect("simulate backend exit");

        let replacement = create_instance(&state, &request)
            .await
            .expect("cold fallback");
        let replacement: serde_json::Value = serde_json::from_slice(
            &replacement
                .into_body()
                .collect()
                .await
                .expect("body")
                .to_bytes(),
        )
        .expect("replacement json");
        assert_ne!(replacement["instance"]["id"], id);
        assert_eq!(replacement["start_path"], "cold");
        let key = PoolKey::new(
            BackendKind::Mock,
            WorkloadClass::AgentTool,
            "sha256:warm-validation".into(),
        );
        assert_eq!(
            state
                .pool
                .lock()
                .expect("pool")
                .stats(&key)
                .quarantine_count,
            1
        );
    }

    #[tokio::test]
    async fn sandbox_guest_routes_use_owned_runtime() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            config.storage.instances_dir.clone(),
        ));
        let state = build_test_state(
            config,
            test_policy(BackendKind::Mock, false),
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            storage,
        );
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("instance id");

        let (status, exec) = dispatched_json(
            &state,
            Method::POST,
            &format!("/v1/sandboxes/{id}/exec"),
            serde_json::to_vec(&json!({
                "cmd": "printf routed",
                "timeout": 5,
            }))
            .expect("exec request"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(exec["exit_code"], 0);
        assert_eq!(exec["stdout_b64"], BASE64.encode(b"printf routed"));

        let encoded = "AAEC/2d1ZXN0";
        let (status, written) = dispatched_json(
            &state,
            Method::POST,
            &format!("/v1/instances/{id}/write"),
            serde_json::to_vec(&json!({
                "path": "/tmp/value",
                "data_b64": encoded,
            }))
            .expect("write request"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(written["bytes"], 9);

        let (status, read) = dispatched_json(
            &state,
            Method::POST,
            &format!("/v1/sandboxes/{id}/read"),
            serde_json::to_vec(&json!({"path": "/tmp/value"})).expect("read request"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(read["data_b64"], encoded);

        let invalid_timeout = dispatch(
            &Method::POST,
            &format!("/v1/sandboxes/{id}/exec"),
            "",
            serde_json::to_vec(&json!({
                "cmd": "true",
                "timeout": MAX_EXEC_TIMEOUT_SECS + 1,
            }))
            .expect("invalid request"),
            &state,
        )
        .await
        .expect_err("timeout above the API limit must fail");
        assert!(matches!(invalid_timeout, BlazeDaemonError::BadRequest(_)));

        assert_eq!(
            decode_guest_file(&BASE64.encode(b"1234"), 4).expect("boundary"),
            b"1234"
        );
        assert!(matches!(
            decode_guest_file(&BASE64.encode(b"12345"), 4),
            Err(BlazeDaemonError::Guest(
                crate::guest::GuestError::PayloadTooLarge { .. }
            ))
        ));
        assert!(matches!(
            decode_guest_file("not/base64!", 16),
            Err(BlazeDaemonError::BadRequest(_))
        ));

        let (status, destroyed) = dispatched_json(
            &state,
            Method::DELETE,
            &format!("/v1/sandboxes/{id}"),
            Vec::new(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(destroyed["destroyed"], true);
    }

    #[tokio::test]
    async fn guest_write_respects_http_and_decoded_limits() {
        const EXTENDED_BODY_LIMIT: usize = 22 * 1024 * 1024;

        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let default_limit = config.api.max_body_bytes;
        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            config.storage.instances_dir.clone(),
        ));
        let state = build_test_state(
            config,
            test_policy(BackendKind::Mock, false),
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            storage,
        );
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("instance id");
        let uuid = Uuid::parse_str(id).expect("uuid");
        let path = format!("/v1/sandboxes/{id}/write");

        let envelope_payload = vec![b'y'; 800 * 1024];
        let envelope_body = serde_json::to_vec(&json!({
            "path": "/tmp/http-envelope",
            "data_b64": BASE64.encode(&envelope_payload),
        }))
        .expect("write request above the default HTTP limit");
        assert!(envelope_body.len() > default_limit);
        let (status, error) =
            handled_json(&state, Method::POST, &path, envelope_body.clone()).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(error["status"], 413);

        state.config.lock().expect("config").api.max_body_bytes = EXTENDED_BODY_LIMIT;
        let (status, written) = handled_json(&state, Method::POST, &path, envelope_body).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(written["bytes"], envelope_payload.len());
        assert_eq!(
            state
                .manager
                .read_file(uuid, "/tmp/http-envelope".into())
                .await
                .expect("read envelope payload"),
            envelope_payload
        );

        let mut payload = vec![b'z'; MAX_GUEST_FILE_BYTES];
        let body = serde_json::to_vec(&json!({
            "path": "/tmp/max-size",
            "data_b64": BASE64.encode(&payload),
        }))
        .expect("write request");
        assert!(body.len() <= EXTENDED_BODY_LIMIT);

        let (status, written) = handled_json(&state, Method::POST, &path, body).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(written["bytes"], MAX_GUEST_FILE_BYTES);
        let readback = state
            .manager
            .read_file(uuid, "/tmp/max-size".into())
            .await
            .expect("read maximum file");
        assert_eq!(readback, payload);
        drop(readback);

        payload.push(b'z');
        let oversized = serde_json::to_vec(&json!({
            "path": "/tmp/too-large",
            "data_b64": BASE64.encode(&payload),
        }))
        .expect("oversized write request");
        assert!(oversized.len() <= EXTENDED_BODY_LIMIT);
        let (status, error) = handled_json(&state, Method::POST, &path, oversized).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(error["status"], 413);
    }

    #[tokio::test]
    async fn write_route_reports_unknown_after_delivery_failure() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            config.storage.instances_dir.clone(),
        ));
        let state = build_test_state(
            config,
            test_policy(BackendKind::Mock, false),
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            storage,
        );
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("instance id");
        let uuid = Uuid::parse_str(id).expect("uuid");
        state
            .manager
            .backend_owner(uuid)
            .expect("mock owner")
            .kill()
            .await
            .expect("stop mock guest");

        let socket = temp.path().join("uncertain.uds");
        let listener = tokio::net::UnixListener::bind(&socket).expect("bind guest endpoint");
        state
            .manager
            .insert_backend_owner(
                uuid,
                Arc::new(StalledGuestOwner {
                    instance_id: uuid,
                    socket,
                    kill_count: Arc::new(AtomicUsize::new(0)),
                    killed: AtomicBool::new(false),
                }),
            )
            .expect("replace backend owner");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept guest request");
            let mut reader = tokio::io::BufReader::new(stream);
            let mut connect = String::new();
            reader.read_line(&mut connect).await.expect("read connect");
            assert_eq!(connect, "CONNECT 5000\n");
            reader
                .get_mut()
                .write_all(b"OK 5000\n")
                .await
                .expect("write handshake");
            let mut request = String::new();
            reader
                .read_line(&mut request)
                .await
                .expect("read guest request");
            let request: serde_json::Value =
                serde_json::from_str(&request).expect("parse guest request");
            assert_eq!(request["op"], "write");
        });

        let body = serde_json::to_vec(&json!({
            "path": "/tmp/value",
            "data_b64": BASE64.encode(b"value"),
        }))
        .expect("write request");
        let (status, error) = handled_json(
            &state,
            Method::POST,
            &format!("/v1/sandboxes/{id}/write"),
            body,
        )
        .await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(error["code"], "guest_outcome_unknown");
        server.await.expect("guest server");
    }

    #[tokio::test]
    async fn unknown_guest_outcome_has_stable_api_code() {
        let response = error_response(&BlazeDaemonError::Guest(
            crate::guest::GuestError::OutcomeUnknown("response lost".into()),
        ));
        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("response body")
            .to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&body).expect("error json");
        assert_eq!(value["code"], "guest_outcome_unknown");
        assert_eq!(value["status"], 504);

        let response = error_response(&BlazeDaemonError::Guest(
            crate::guest::GuestError::ResponseTooLarge {
                actual: 5,
                limit: 4,
            },
        ));
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("response body")
            .to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&body).expect("error json");
        assert_eq!(value["code"], "guest_response_too_large");

        let response = error_response(&BlazeDaemonError::Guest(crate::guest::GuestError::Timeout(
            "connect stalled".into(),
        )));
        let body = response
            .into_body()
            .collect()
            .await
            .expect("response body")
            .to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&body).expect("error json");
        assert_eq!(value["code"], "guest_timeout");
    }

    #[tokio::test]
    async fn create_publishes_ownership_before_provider_acquire() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let observed = Arc::new(AtomicBool::new(false));
        let storage: Arc<dyn StorageProvider> = Arc::new(OwnershipObservingStorage {
            inner: FileStorageProvider::with_images(
                config.storage.images_dir.clone(),
                config.storage.instances_dir.clone(),
            ),
            state_dir: config.daemon.state_dir.clone(),
            observed: observed.clone(),
        });
        let state = build_test_state(
            config,
            test_policy(BackendKind::Mock, false),
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            storage,
        );

        created_json(&state, &test_request()).await;
        assert!(observed.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn daemon_shutdown_releases_tracked_runtime_resources() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let instances_dir = config.storage.instances_dir.clone();
        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            instances_dir.clone(),
        ));
        let state = build_test_state(
            config,
            test_policy(BackendKind::Mock, false),
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            storage,
        );
        let created = created_json(&state, &test_request()).await;
        let id = Uuid::parse_str(created["instance"]["id"].as_str().expect("instance id"))
            .expect("uuid");

        shutdown_instances(&state, Duration::from_secs(1))
            .await
            .expect("shutdown cleanup");

        assert_eq!(
            state.manager.get(id).expect("instance").state,
            SandboxState::Destroyed
        );
        assert!(state.manager.backend_owner(id).is_none());
        assert!(!instances_dir.join(id.to_string()).exists());
    }

    #[tokio::test]
    async fn shutdown_cancels_readiness_and_completes_create_compensation() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let instances_dir = config.storage.instances_dir.clone();
        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            instances_dir.clone(),
        ));
        let spawned = Arc::new(tokio::sync::Notify::new());
        let kill_count = Arc::new(AtomicUsize::new(0));
        let state = build_test_state(
            config,
            test_policy(BackendKind::Mock, false),
            spawners(
                BackendKind::Mock,
                Arc::new(StalledGuestSpawner {
                    spawned: spawned.clone(),
                    kill_count: kill_count.clone(),
                }),
            ),
            BackendKind::Mock,
            storage,
        );
        let create_state = state.clone();
        let create =
            tokio::spawn(async move { create_instance(&create_state, &test_request()).await });
        tokio::time::timeout(Duration::from_secs(1), spawned.notified())
            .await
            .expect("backend started");

        state.manager.begin_shutdown();
        let error = tokio::time::timeout(Duration::from_secs(1), create)
            .await
            .expect("create cancellation")
            .expect("create task")
            .expect_err("readiness must be cancelled");
        assert!(matches!(
            error,
            BlazeDaemonError::Guest(crate::guest::GuestError::Cancelled)
        ));

        let instance = state
            .manager
            .list()
            .expect("instances")
            .into_iter()
            .next()
            .expect("cancelled create record");
        assert_eq!(instance.state, SandboxState::Destroyed);
        assert!(instance.operation.is_none());
        assert_eq!(instance.backend_ownership, BackendOwnership::Stopped);
        assert!(state.manager.backend_owner(instance.id).is_none());
        assert_eq!(kill_count.load(Ordering::Acquire), 1);
        assert!(!instances_dir.join(instance.id.to_string()).exists());
        shutdown_instances(&state, Duration::from_millis(100))
            .await
            .expect("no ownership remains");
    }

    #[tokio::test]
    async fn daemon_shutdown_joins_an_active_pool_worker_before_returning() {
        let temp = tempfile::tempdir().expect("temp");
        let mut config = test_config(&temp);
        config.storage.pool_size = 1;
        config.storage.prefork = false;
        std::fs::create_dir_all(config.daemon.state_dir.join("runtime-pool"))
            .expect("runtime pool root");
        let release_started = Arc::new(AtomicUsize::new(0));
        let release_active = Arc::new(AtomicUsize::new(0));
        let release_completed = Arc::new(AtomicUsize::new(0));
        let storage = Arc::new(PoolWorkerReleaseStorage {
            inner: FileStorageProvider::with_images(
                config.storage.images_dir.clone(),
                config.storage.instances_dir.clone(),
            ),
            acquire_count: AtomicUsize::new(0),
            residual_attempt: 1,
            delayed_id: Mutex::new(None),
            release_started: release_started.clone(),
            release_active: release_active.clone(),
            release_completed: release_completed.clone(),
            release_delay: Duration::from_secs(1),
        });
        let state = build_test_state(
            config,
            test_policy(BackendKind::Mock, true),
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            storage,
        );
        state
            .manager
            .configure_runtime_pool_for_test(PoolPrototype {
                image_digest: "sha256:pool-shutdown".to_string(),
                policy_name: "pool-shutdown".to_string(),
                workload_class: WorkloadClass::AgentTool,
                templates: Vec::new(),
                kernel_hooks: Vec::new(),
                binary_path: PathBuf::from("/unused"),
                runtime_backend: BackendKind::Mock,
                backend: BackendConfigs::default(),
                vm: None,
                warm_ttl: Duration::from_secs(60),
            })
            .expect("configure runtime pool");
        tokio::time::timeout(Duration::from_secs(2), async {
            while release_active.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("pool worker starts delayed cleanup");

        let error = shutdown_instances(&state, Duration::from_millis(40))
            .await
            .expect_err("pool cleanup exceeds the shared budget");

        assert!(error.to_string().contains("runtime-pool"));
        assert_eq!(release_started.load(Ordering::Acquire), 1);
        assert_eq!(release_completed.load(Ordering::Acquire), 0);
        assert_eq!(
            release_active.load(Ordering::Acquire),
            0,
            "shutdown returned while the pool worker was still running"
        );
        assert!(!state.manager.runtime_pool_has_tracked_worker());
        let status = state.manager.runtime_pool_status();
        assert_eq!(status.cleanup_pending, 0);
        assert_eq!(status.quarantined, 1);

        state
            .manager
            .shutdown_runtime_pool_until(Instant::now() + Duration::from_secs(2))
            .await
            .expect("retry releases the retained pool owner");
        assert_eq!(release_completed.load(Ordering::Acquire), 1);
        assert_eq!(state.manager.runtime_pool_status().quarantined, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn daemon_shutdown_joins_all_owners_within_one_budget() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let instances_dir = config.storage.instances_dir.clone();
        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            instances_dir.clone(),
        ));
        let state = build_test_state(
            config,
            test_policy(BackendKind::Mock, false),
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            storage,
        );
        let attempts = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let complete = track_shutdown_owner(
            &state,
            ShutdownBehavior::Complete,
            attempts.clone(),
            active.clone(),
        )
        .await;
        let failed = track_shutdown_owner(
            &state,
            ShutdownBehavior::Fail,
            attempts.clone(),
            active.clone(),
        )
        .await;
        let stalled_a = track_shutdown_owner(
            &state,
            ShutdownBehavior::Stall,
            attempts.clone(),
            active.clone(),
        )
        .await;
        let stalled_b = track_shutdown_owner(
            &state,
            ShutdownBehavior::Stall,
            attempts.clone(),
            active.clone(),
        )
        .await;

        let started = Instant::now();
        let error = shutdown_instances(&state, Duration::from_millis(40))
            .await
            .expect_err("stalled cleanup must exhaust the shared budget");

        assert!(
            started.elapsed() < Duration::from_millis(400),
            "cleanup did not quiesce promptly after the test deadline"
        );
        assert_eq!(
            attempts.load(Ordering::Acquire),
            4,
            "every owner must receive a cleanup attempt"
        );
        assert_eq!(
            active.load(Ordering::Acquire),
            0,
            "shutdown returned before every cleanup task became quiescent"
        );
        let message = error.to_string();
        for id in [failed, stalled_a, stalled_b] {
            assert!(message.contains(&id.to_string()));
            assert!(state.manager.backend_owner(id).is_some());
            assert!(instances_dir.join(id.to_string()).is_dir());
        }
        assert_eq!(
            state
                .instances
                .lock()
                .expect("instances")
                .get(&complete)
                .expect("complete instance")
                .state,
            SandboxState::Destroyed
        );
        assert!(state.manager.backend_owner(complete).is_none());
        assert!(!instances_dir.join(complete.to_string()).exists());
    }

    #[tokio::test]
    async fn mock_fallback_uses_runtime_backend_for_warm_reuse() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            config.storage.instances_dir.clone(),
        ));
        let state = build_test_state(
            config,
            test_policy(BackendKind::Firecracker, true),
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            storage,
        );
        let request = test_request();

        let cold = created_json(&state, &request).await;
        let id = cold["instance"]["id"].as_str().expect("id").to_string();
        assert_eq!(cold["instance"]["backend"], "mock");
        assert_eq!(cold["selected_backend"], "mock");
        assert!(cold["instance"]["operation"].is_null());

        return_to_pool_for_test(&state, &id)
            .await
            .expect("return to pool");
        let warm = created_json(&state, &request).await;
        assert_eq!(warm["instance"]["id"], id);
        assert_eq!(warm["instance"]["backend"], "mock");
        assert_eq!(warm["selected_backend"], "mock");
        assert_eq!(warm["start_path"], "warm");
        assert!(warm["instance"]["operation"].is_null());
    }

    #[tokio::test]
    async fn partial_spawn_failure_retains_owner_and_storage_for_destroy() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let instances_dir = config.storage.instances_dir.clone();
        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            instances_dir.clone(),
        ));
        let state = build_test_state(
            config,
            test_policy(BackendKind::Mock, false),
            spawners(BackendKind::Mock, Arc::new(PartialSpawnSpawner)),
            BackendKind::Mock,
            storage,
        );

        let error = create_instance(&state, &test_request())
            .await
            .expect_err("partial spawn must require recovery");
        assert!(matches!(error, BlazeDaemonError::RecoveryRequired(_)));
        let instance = state
            .instances
            .lock()
            .expect("instances")
            .values()
            .next()
            .cloned()
            .expect("retained lifecycle");
        assert_eq!(instance.state, SandboxState::RecoveryRequired);
        assert_eq!(instance.backend_ownership, BackendOwnership::Running);
        assert_eq!(
            instance.operation.as_ref().map(|operation| operation.kind),
            Some(OperationKind::Create)
        );
        assert!(instances_dir.join(instance.id.to_string()).is_dir());
        assert!(state.manager.backend_owner(instance.id).is_some());

        destroy_instance(&state, &instance.id.to_string())
            .await
            .expect("retry destroy");
        assert!(!instances_dir.join(instance.id.to_string()).exists());
        assert_eq!(
            state.instances.lock().expect("instances")[&instance.id].state,
            SandboxState::Destroyed
        );
    }

    #[tokio::test]
    async fn restart_destroy_uses_the_persisted_backend_spawner() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let storage = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            config.storage.instances_dir.clone(),
        ));
        let mut instance = SandboxInstance::new(
            BackendKind::Bubblewrap,
            WorkloadClass::AgentTool,
            "sha256:recovery".into(),
            StartPath::Cold,
            "recovery-test".into(),
        );
        instance
            .transition(SandboxState::Creating)
            .expect("creating");
        instance.transition(SandboxState::Running).expect("running");
        instance.backend_ownership = BackendOwnership::Running;
        instance.persist(&config.daemon.state_dir).expect("persist");
        storage
            .acquire(&AcquireOpts {
                instance_id: instance.id.to_string(),
                rootfs_size: 4096,
                mem_size: 4096,
            })
            .await
            .expect("storage");

        let active_cleanups = Arc::new(AtomicUsize::new(0));
        let persisted_cleanups = Arc::new(AtomicUsize::new(0));
        let mut registry = SpawnerRegistry::new();
        registry.insert(
            BackendKind::Mock,
            Arc::new(RecordingSpawner {
                cleanup_count: active_cleanups.clone(),
            }),
        );
        registry.insert(
            BackendKind::Bubblewrap,
            Arc::new(RecordingSpawner {
                cleanup_count: persisted_cleanups.clone(),
            }),
        );
        let state = build_test_state(
            config,
            test_policy(BackendKind::Mock, false),
            registry,
            BackendKind::Mock,
            storage,
        );

        destroy_instance(&state, &instance.id.to_string())
            .await
            .expect("destroy recovered instance");
        assert_eq!(persisted_cleanups.load(Ordering::Acquire), 1);
        assert_eq!(active_cleanups.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn mock_fallback_restart_destroy_uses_mock_spawner() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let instances_dir = config.storage.instances_dir.clone();
        let initial_storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            instances_dir.clone(),
        ));
        let initial_state = build_test_state(
            config.clone(),
            test_policy(BackendKind::Firecracker, false),
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            initial_storage,
        );
        let created = created_json(&initial_state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id").to_string();
        assert_eq!(created["instance"]["backend"], "mock");
        drop(initial_state);

        let mock_cleanups = Arc::new(AtomicUsize::new(0));
        let policy_cleanups = Arc::new(AtomicUsize::new(0));
        let mut registry = SpawnerRegistry::new();
        registry.insert(
            BackendKind::Mock,
            Arc::new(RecordingSpawner {
                cleanup_count: mock_cleanups.clone(),
            }),
        );
        registry.insert(
            BackendKind::Firecracker,
            Arc::new(RecordingSpawner {
                cleanup_count: policy_cleanups.clone(),
            }),
        );
        let restarted_storage: Arc<dyn StorageProvider> =
            Arc::new(FileStorageProvider::with_images(
                config.storage.images_dir.clone(),
                instances_dir.clone(),
            ));
        let restarted = build_test_state(
            config,
            test_policy(BackendKind::Firecracker, false),
            registry,
            BackendKind::Mock,
            restarted_storage,
        );

        destroy_instance(&restarted, &id)
            .await
            .expect("destroy recovered mock instance");
        assert_eq!(mock_cleanups.load(Ordering::Acquire), 1);
        assert_eq!(policy_cleanups.load(Ordering::Acquire), 0);
        assert!(!instances_dir.join(id).exists());
    }

    #[tokio::test]
    async fn write_ahead_create_without_slot_is_destroyable_after_restart() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let instances_dir = config.storage.instances_dir.clone();
        let mut instance = SandboxInstance::new(
            BackendKind::Mock,
            WorkloadClass::AgentTool,
            "sha256:write-ahead".into(),
            StartPath::Cold,
            "write-ahead-test".into(),
        );
        instance
            .transition(SandboxState::Creating)
            .expect("creating");
        instance
            .persist(&config.daemon.state_dir)
            .expect("write-ahead state");
        let id = instance.id;
        assert!(!instances_dir.join(id.to_string()).exists());

        let cleanup_count = Arc::new(AtomicUsize::new(0));
        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            instances_dir.clone(),
        ));
        let restarted = build_test_state(
            config,
            test_policy(BackendKind::Mock, false),
            spawners(
                BackendKind::Mock,
                Arc::new(RecordingSpawner {
                    cleanup_count: cleanup_count.clone(),
                }),
            ),
            BackendKind::Mock,
            storage,
        );

        destroy_instance(&restarted, &id.to_string())
            .await
            .expect("destroy state without slot");
        assert_eq!(cleanup_count.load(Ordering::Acquire), 0);
        assert_eq!(
            restarted.instances.lock().expect("instances")[&id].state,
            SandboxState::Destroyed
        );
        assert!(!instances_dir.join(id.to_string()).exists());
    }

    #[tokio::test]
    async fn warm_reconstruct_restores_transient_failure_for_retry() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let storage = Arc::new(TransientReconstructStorage::new(
            config.storage.images_dir.clone(),
            config.storage.instances_dir.clone(),
        ));
        let state = build_test_state(
            config,
            test_policy(BackendKind::Mock, true),
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            storage.clone(),
        );
        let request = test_request();
        let cold = created_json(&state, &request).await;
        let id = cold["instance"]["id"].as_str().expect("id").to_string();
        return_to_pool_for_test(&state, &id).await.expect("warm");

        storage.fail_reconstruct.store(true, Ordering::Release);
        let error = create_instance(&state, &request)
            .await
            .expect_err("transient error must preserve claim");
        assert!(matches!(error, BlazeDaemonError::RecoveryRequired(_)));
        let uuid = Uuid::parse_str(&id).expect("uuid");
        assert_eq!(
            state.instances.lock().expect("instances")[&uuid].state,
            SandboxState::Warm
        );
        let key = PoolKey::new(
            BackendKind::Mock,
            WorkloadClass::AgentTool,
            "sha256:ownership-test".into(),
        );
        assert_eq!(state.pool.lock().expect("pool").stats(&key).warm_count, 1);

        storage.fail_reconstruct.store(false, Ordering::Release);
        let retried = created_json(&state, &request).await;
        assert_eq!(retried["instance"]["id"], id);
        assert_eq!(retried["start_path"], "warm");
    }

    #[tokio::test]
    async fn warm_reconstruct_quarantines_an_incomplete_slot() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let instances_dir = config.storage.instances_dir.clone();
        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            instances_dir.clone(),
        ));
        let state = build_test_state(
            config,
            test_policy(BackendKind::Mock, true),
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            storage,
        );
        let request = test_request();
        let cold = created_json(&state, &request).await;
        let id = cold["instance"]["id"].as_str().expect("id").to_string();
        return_to_pool_for_test(&state, &id).await.expect("warm");
        std::fs::remove_file(instances_dir.join(&id).join("mem.bin")).expect("remove artifact");

        let replacement = created_json(&state, &request).await;
        assert_ne!(replacement["instance"]["id"], id);
        assert_eq!(replacement["start_path"], "cold");
        let uuid = Uuid::parse_str(&id).expect("uuid");
        assert_eq!(
            state.instances.lock().expect("instances")[&uuid].state,
            SandboxState::Destroyed
        );
    }

    #[cfg(feature = "test-failpoints")]
    async fn assert_warm_state_commit_failure_restores_claim(failpoint: &'static str) {
        let temp = tempfile::tempdir().expect("temp");
        let state = mock_state(&temp, true);
        let request = test_request();
        let cold = created_json(&state, &request).await;
        let id = cold["instance"]["id"].as_str().expect("id").to_string();
        let uuid = Uuid::parse_str(&id).expect("uuid");
        return_to_pool_for_test(&state, &id).await.expect("warm");
        let owner = state.manager.backend_owner(uuid).expect("backend owner");
        let key = PoolKey::new(
            BackendKind::Mock,
            WorkloadClass::AgentTool,
            "sha256:ownership-test".into(),
        );

        let hook = crate::failpoint::TestFailpoint::new(&[failpoint]);
        let error = hook
            .run(create_instance(&state, &request))
            .await
            .expect_err("state commit failure");

        assert!(matches!(error, BlazeDaemonError::RecoveryRequired(_)));
        let restored = state.instances.lock().expect("instances")[&uuid].clone();
        assert_eq!(restored.state, SandboxState::Warm);
        assert!(restored.operation.is_none());
        let persisted =
            SandboxInstance::load(&state.state_dir, uuid).expect("persisted warm state");
        assert_eq!(persisted.state, SandboxState::Warm);
        assert_eq!(persisted.backend_ownership, BackendOwnership::Running);
        assert!(persisted.operation.is_none());
        let retained_owner = state.manager.backend_owner(uuid).expect("retained owner");
        assert!(Arc::ptr_eq(&owner, &retained_owner));
        assert_eq!(state.pool.lock().expect("pool").stats(&key).warm_count, 1);
        assert!(retained_owner.try_wait().await.expect("liveness").is_none());

        let retried = created_json(&state, &request).await;
        assert_eq!(retried["instance"]["id"], id);
        assert_eq!(retried["start_path"], "warm");
        let persisted =
            SandboxInstance::load(&state.state_dir, uuid).expect("persisted running state");
        assert_eq!(persisted.state, SandboxState::Running);
        assert_eq!(persisted.backend_ownership, BackendOwnership::Running);
        assert!(persisted.operation.is_none());
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn warm_intent_commit_failure_restores_the_claim() {
        assert_warm_state_commit_failure_restores_claim("warm-intent-state-commit").await;
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn warm_final_commit_failure_restores_the_claim() {
        assert_warm_state_commit_failure_restores_claim("warm-final-state-commit").await;
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn guest_readiness_failure_compensates_owned_resources() {
        let request = test_request();
        let temp = tempfile::tempdir().expect("temp");
        let state = mock_state(&temp, false);
        let hook = crate::failpoint::TestFailpoint::new(&["create-guest-ready"]);

        hook.run(create_instance(&state, &request))
            .await
            .expect_err("guest readiness failure");

        let instance = state
            .instances
            .lock()
            .expect("instances")
            .values()
            .next()
            .cloned()
            .expect("destroyed create");
        assert_eq!(instance.state, SandboxState::Destroyed);
        assert!(state.manager.backend_owner(instance.id).is_none());
        assert!(
            !temp
                .path()
                .join("instances")
                .join(instance.id.to_string())
                .exists()
        );
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn failure_hooks_drive_create_and_destroy_compensation() {
        let request = test_request();

        let spawn_temp = tempfile::tempdir().expect("temp");
        let spawn_state = mock_state(&spawn_temp, false);
        let spawn_hook = crate::failpoint::TestFailpoint::new(&["create-spawn"]);
        spawn_hook
            .run(create_instance(&spawn_state, &request))
            .await
            .expect_err("spawn failure");
        let spawn_instance = spawn_state
            .instances
            .lock()
            .expect("instances")
            .values()
            .next()
            .cloned()
            .expect("destroyed create");
        assert_eq!(spawn_instance.state, SandboxState::Destroyed);

        let commit_temp = tempfile::tempdir().expect("temp");
        let commit_state = mock_state(&commit_temp, false);
        let commit_hook = crate::failpoint::TestFailpoint::new(&["create-state-commit"]);
        commit_hook
            .run(create_instance(&commit_state, &request))
            .await
            .expect_err("state commit failure");
        let commit_instance = commit_state
            .instances
            .lock()
            .expect("instances")
            .values()
            .next()
            .cloned()
            .expect("destroyed create");
        assert_eq!(commit_instance.state, SandboxState::Destroyed);
        assert!(
            commit_state
                .manager
                .backend_owner(commit_instance.id)
                .is_none()
        );

        let destroy_temp = tempfile::tempdir().expect("temp");
        let destroy_state = mock_state(&destroy_temp, false);
        let created = created_json(&destroy_state, &request).await;
        let id = created["instance"]["id"].as_str().expect("id").to_string();
        let kill_hook = crate::failpoint::TestFailpoint::new(&["destroy-kill"]);
        kill_hook
            .run(destroy_instance(&destroy_state, &id))
            .await
            .expect_err("kill boundary");
        let uuid = Uuid::parse_str(&id).expect("uuid");
        let failed_destroy = destroy_state.instances.lock().expect("instances")[&uuid].clone();
        assert_eq!(failed_destroy.state, SandboxState::RecoveryRequired);
        assert_eq!(
            failed_destroy
                .operation
                .as_ref()
                .map(|operation| operation.kind),
            Some(OperationKind::Destroy)
        );
        assert!(destroy_state.manager.backend_owner(uuid).is_some());
        destroy_instance(&destroy_state, &id)
            .await
            .expect("destroy retry");

        let release_temp = tempfile::tempdir().expect("temp");
        let release_state = mock_state(&release_temp, false);
        let created = created_json(&release_state, &request).await;
        let id = created["instance"]["id"].as_str().expect("id").to_string();
        let release_hook = crate::failpoint::TestFailpoint::new(&["storage-release"]);
        release_hook
            .run(destroy_instance(&release_state, &id))
            .await
            .expect_err("release boundary");
        let uuid = Uuid::parse_str(&id).expect("uuid");
        assert_eq!(
            release_state.instances.lock().expect("instances")[&uuid].backend_ownership,
            BackendOwnership::Stopped
        );
        assert_eq!(
            release_state.instances.lock().expect("instances")[&uuid]
                .operation
                .as_ref()
                .map(|operation| operation.kind),
            Some(OperationKind::Destroy)
        );
        destroy_instance(&release_state, &id)
            .await
            .expect("release retry");
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn destroy_intent_failure_does_not_touch_owned_resources() {
        let temp = tempfile::tempdir().expect("temp");
        let (state, kill_count, orphan_cleanup_count, release_count) = counting_state(&temp);
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id").to_string();
        let uuid = Uuid::parse_str(&id).expect("uuid");
        let hook = crate::failpoint::TestFailpoint::new(&["destroy-intent-state-commit"]);

        let error = hook
            .run(destroy_instance(&state, &id))
            .await
            .expect_err("intent failure");

        assert!(matches!(error, BlazeDaemonError::RecoveryRequired(_)));
        assert_eq!(kill_count.load(Ordering::Acquire), 0);
        assert_eq!(orphan_cleanup_count.load(Ordering::Acquire), 0);
        assert_eq!(release_count.load(Ordering::Acquire), 0);
        let retained = state.instances.lock().expect("instances")[&uuid].clone();
        assert_eq!(retained.state, SandboxState::RecoveryRequired);
        assert!(retained.operation.is_none());
        let persisted =
            SandboxInstance::load(&state.state_dir, uuid).expect("persisted recovery state");
        assert_eq!(persisted.state, SandboxState::RecoveryRequired);
        assert_eq!(persisted.backend_ownership, BackendOwnership::Running);
        assert!(persisted.operation.is_none());
        assert!(temp.path().join("instances").join(&id).is_dir());

        destroy_instance(&state, &id).await.expect("destroy retry");
        assert_eq!(kill_count.load(Ordering::Acquire), 1);
        assert_eq!(release_count.load(Ordering::Acquire), 1);
        assert_eq!(
            state.instances.lock().expect("instances")[&uuid].state,
            SandboxState::Destroyed
        );
        let persisted =
            SandboxInstance::load(&state.state_dir, uuid).expect("persisted destroyed state");
        assert_eq!(persisted.state, SandboxState::Destroyed);
        assert!(persisted.operation.is_none());
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn destroy_stop_commit_failure_retains_storage_for_retry() {
        let temp = tempfile::tempdir().expect("temp");
        let (state, kill_count, orphan_cleanup_count, release_count) = counting_state(&temp);
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id").to_string();
        let uuid = Uuid::parse_str(&id).expect("uuid");
        let hook = crate::failpoint::TestFailpoint::new(&["destroy-stop-state-commit"]);

        let error = hook
            .run(destroy_instance(&state, &id))
            .await
            .expect_err("stop commit failure");

        assert!(matches!(error, BlazeDaemonError::RecoveryRequired(_)));
        assert_eq!(kill_count.load(Ordering::Acquire), 1);
        assert_eq!(orphan_cleanup_count.load(Ordering::Acquire), 0);
        assert_eq!(release_count.load(Ordering::Acquire), 0);
        let retained = state.instances.lock().expect("instances")[&uuid].clone();
        assert_eq!(retained.state, SandboxState::RecoveryRequired);
        assert_eq!(retained.backend_ownership, BackendOwnership::Stopped);
        assert_eq!(
            retained.operation.as_ref().map(|operation| operation.kind),
            Some(OperationKind::Destroy)
        );
        let persisted =
            SandboxInstance::load(&state.state_dir, uuid).expect("persisted recovery state");
        assert_eq!(persisted.state, SandboxState::RecoveryRequired);
        assert_eq!(persisted.backend_ownership, BackendOwnership::Stopped);
        assert_eq!(
            persisted.operation.as_ref().map(|operation| operation.kind),
            Some(OperationKind::Destroy)
        );
        assert!(temp.path().join("instances").join(&id).is_dir());

        destroy_instance(&state, &id).await.expect("destroy retry");
        assert_eq!(kill_count.load(Ordering::Acquire), 1);
        assert_eq!(release_count.load(Ordering::Acquire), 1);
        let persisted =
            SandboxInstance::load(&state.state_dir, uuid).expect("persisted destroyed state");
        assert_eq!(persisted.state, SandboxState::Destroyed);
        assert!(persisted.operation.is_none());
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn destroy_final_commit_failure_retains_retryable_metadata() {
        let temp = tempfile::tempdir().expect("temp");
        let (state, kill_count, orphan_cleanup_count, release_count) = counting_state(&temp);
        let created = created_json(&state, &test_request()).await;
        let id = created["instance"]["id"].as_str().expect("id").to_string();
        let uuid = Uuid::parse_str(&id).expect("uuid");
        let hook = crate::failpoint::TestFailpoint::new(&["destroy-final-state-commit"]);

        let error = hook
            .run(destroy_instance(&state, &id))
            .await
            .expect_err("final commit failure");

        assert!(matches!(error, BlazeDaemonError::RecoveryRequired(_)));
        assert_eq!(kill_count.load(Ordering::Acquire), 1);
        assert_eq!(orphan_cleanup_count.load(Ordering::Acquire), 0);
        assert_eq!(release_count.load(Ordering::Acquire), 1);
        assert!(!temp.path().join("instances").join(&id).exists());
        let retained = state.instances.lock().expect("instances")[&uuid].clone();
        assert_eq!(retained.state, SandboxState::RecoveryRequired);
        assert_eq!(retained.backend_ownership, BackendOwnership::Stopped);
        assert_eq!(
            retained.operation.as_ref().map(|operation| operation.kind),
            Some(OperationKind::Destroy)
        );
        let persisted =
            SandboxInstance::load(&state.state_dir, uuid).expect("persisted recovery state");
        assert_eq!(persisted.state, SandboxState::RecoveryRequired);
        assert_eq!(persisted.backend_ownership, BackendOwnership::Stopped);
        assert_eq!(
            persisted.operation.as_ref().map(|operation| operation.kind),
            Some(OperationKind::Destroy)
        );

        destroy_instance(&state, &id).await.expect("destroy retry");
        assert_eq!(kill_count.load(Ordering::Acquire), 1);
        assert_eq!(release_count.load(Ordering::Acquire), 2);
        let destroyed = state.instances.lock().expect("instances")[&uuid].clone();
        assert_eq!(destroyed.state, SandboxState::Destroyed);
        assert!(destroyed.operation.is_none());
        let persisted =
            SandboxInstance::load(&state.state_dir, uuid).expect("persisted destroyed state");
        assert_eq!(persisted.state, SandboxState::Destroyed);
        assert!(persisted.operation.is_none());
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn acquire_rollback_failure_retains_a_destroyable_record() {
        let temp = tempfile::tempdir().expect("temp");
        let state = mock_state(&temp, false);
        let acquire_hook = crate::failpoint::TestFailpoint::new(&[
            "storage-acquire-artifacts",
            "storage-acquire-rollback",
        ]);
        let error = acquire_hook
            .run(create_instance(&state, &test_request()))
            .await
            .expect_err("residual slot must require recovery");
        assert!(matches!(error, BlazeDaemonError::RecoveryRequired(_)));

        let instance = state
            .instances
            .lock()
            .expect("instances")
            .values()
            .next()
            .cloned()
            .expect("recovery record");
        assert_eq!(instance.state, SandboxState::RecoveryRequired);
        assert_eq!(instance.backend_ownership, BackendOwnership::NotStarted);
        assert_eq!(
            instance.operation.as_ref().map(|operation| operation.kind),
            Some(OperationKind::Create)
        );
        assert!(
            temp.path()
                .join("instances")
                .join(instance.id.to_string())
                .is_dir()
        );
        destroy_instance(&state, &instance.id.to_string())
            .await
            .expect("destroy residual slot");
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn acquired_slot_is_destroyable_after_restart_before_start_commit() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let instances_dir = config.storage.instances_dir.clone();
        let initial_storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            instances_dir.clone(),
        ));
        let initial_state = build_test_state(
            config.clone(),
            test_policy(BackendKind::Mock, false),
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            initial_storage,
        );
        let pause_hook = crate::failpoint::TestFailpoint::new(&["create-after-storage-acquire"]);
        let create_state = initial_state.clone();
        let create_hook = pause_hook.clone();
        let create = tokio::spawn(async move {
            create_hook
                .run(create_instance(&create_state, &test_request()))
                .await
        });
        pause_hook.wait_until_paused().await;

        let instance = initial_state
            .instances
            .lock()
            .expect("instances")
            .values()
            .next()
            .cloned()
            .expect("write-ahead instance");
        let id = instance.id;
        assert_eq!(instance.state, SandboxState::Creating);
        assert_eq!(instance.backend_ownership, BackendOwnership::NotStarted);
        assert_eq!(
            instance.operation.as_ref().map(|operation| operation.kind),
            Some(OperationKind::Create)
        );
        assert!(
            config
                .daemon
                .state_dir
                .join(id.to_string())
                .join("state.json")
                .is_file()
        );
        assert!(instances_dir.join(id.to_string()).is_dir());

        create.abort();
        assert!(
            create
                .await
                .expect_err("create task aborted")
                .is_cancelled()
        );
        drop(initial_state);

        let cleanup_count = Arc::new(AtomicUsize::new(0));
        let restarted_storage: Arc<dyn StorageProvider> =
            Arc::new(FileStorageProvider::with_images(
                config.storage.images_dir.clone(),
                instances_dir.clone(),
            ));
        let restarted = build_test_state(
            config,
            test_policy(BackendKind::Mock, false),
            spawners(
                BackendKind::Mock,
                Arc::new(RecordingSpawner {
                    cleanup_count: cleanup_count.clone(),
                }),
            ),
            BackendKind::Mock,
            restarted_storage,
        );
        assert!(
            restarted
                .instances
                .lock()
                .expect("instances")
                .contains_key(&id)
        );

        destroy_instance(&restarted, &id.to_string())
            .await
            .expect("destroy acquired slot after restart");
        assert_eq!(cleanup_count.load(Ordering::Acquire), 0);
        assert_eq!(
            restarted.instances.lock().expect("instances")[&id].state,
            SandboxState::Destroyed
        );
        assert!(!instances_dir.join(id.to_string()).exists());
    }

    #[cfg(feature = "test-failpoints")]
    #[tokio::test]
    async fn warm_activation_and_destroy_are_serialized_per_instance() {
        let temp = tempfile::tempdir().expect("temp");
        let state = mock_state(&temp, true);
        let request = test_request();
        let cold = created_json(&state, &request).await;
        let id = cold["instance"]["id"].as_str().expect("id").to_string();
        return_to_pool_for_test(&state, &id).await.expect("warm");

        let pause_hook = crate::failpoint::TestFailpoint::new(&["warm-before-state-commit"]);
        let create_state = state.clone();
        let create_request = request.clone();
        let activation_hook = pause_hook.clone();
        let activation = tokio::spawn(async move {
            activation_hook
                .run(create_instance(&create_state, &create_request))
                .await
        });
        pause_hook.wait_until_paused().await;
        let uuid = Uuid::parse_str(&id).expect("uuid");
        assert_eq!(
            state.instances.lock().expect("instances")[&uuid]
                .operation
                .as_ref()
                .map(|operation| operation.kind),
            Some(OperationKind::Create)
        );

        let destroy_state = state.clone();
        let destroy_id = id.clone();
        let destroy =
            tokio::spawn(async move { destroy_instance(&destroy_state, &destroy_id).await });
        tokio::task::yield_now().await;
        assert!(!destroy.is_finished(), "destroy must wait for activation");

        pause_hook.release();
        activation
            .await
            .expect("activation task")
            .expect("activation");
        destroy.await.expect("destroy task").expect("destroy");
        assert_eq!(
            state.instances.lock().expect("instances")[&uuid].state,
            SandboxState::Destroyed
        );
    }

    #[tokio::test]
    async fn startup_reconciliation_repairs_anomalous_destroyed_records() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let cleanup_count = Arc::new(AtomicUsize::new(0));
        let release_count = Arc::new(AtomicUsize::new(0));
        let storage = Arc::new(CountingStorage {
            inner: FileStorageProvider::with_images(
                config.storage.images_dir.clone(),
                config.storage.instances_dir.clone(),
            ),
            release_count: release_count.clone(),
        });

        let clean_stopped_id = Uuid::new_v4();
        let clean_not_started_id = Uuid::new_v4();
        let legacy_id = Uuid::new_v4();
        let active_id = Uuid::new_v4();
        for (id, ownership, active_operation) in [
            (clean_stopped_id, BackendOwnership::Stopped, false),
            (clean_not_started_id, BackendOwnership::NotStarted, false),
            (legacy_id, BackendOwnership::Unknown, false),
            (active_id, BackendOwnership::Running, true),
        ] {
            let mut instance = SandboxInstance::new(
                BackendKind::Mock,
                WorkloadClass::AgentTool,
                "sha256:destroyed-reconcile".into(),
                StartPath::Cold,
                "destroyed-reconcile-test".into(),
            );
            instance.id = id;
            if active_operation {
                instance
                    .begin_operation(OperationKind::Create)
                    .expect("begin interrupted create");
            }
            instance
                .transition(SandboxState::Destroyed)
                .expect("destroyed");
            instance.backend_ownership = ownership;
            instance.persist(&config.daemon.state_dir).expect("persist");

            if id == legacy_id || id == active_id {
                storage
                    .acquire(&AcquireOpts {
                        instance_id: id.to_string(),
                        rootfs_size: 64,
                        mem_size: 32,
                    })
                    .await
                    .expect("storage");
            }
        }
        let state = build_test_state(
            config.clone(),
            test_policy(BackendKind::Mock, false),
            spawners(
                BackendKind::Mock,
                Arc::new(SelectiveCleanupSpawner {
                    failed_id: legacy_id,
                    cleanup_count: cleanup_count.clone(),
                }),
            ),
            BackendKind::Mock,
            storage.clone(),
        );

        let report = state.manager.reconcile_startup().await;

        assert_eq!(report.attempted, 2);
        assert_eq!(report.completed, 1);
        assert_eq!(report.failures.len(), 1);
        assert_eq!(report.failures[0].instance_id, legacy_id);
        assert_eq!(cleanup_count.load(Ordering::Acquire), 2);
        assert_eq!(release_count.load(Ordering::Acquire), 1);
        let retryable = state.manager.get(legacy_id).expect("retryable instance");
        assert_eq!(retryable.state, SandboxState::Destroyed);
        assert_eq!(retryable.backend_ownership, BackendOwnership::Unknown);
        assert_eq!(
            retryable.operation.as_ref().map(|operation| operation.kind),
            Some(OperationKind::Destroy)
        );
        assert!(
            config
                .storage
                .instances_dir
                .join(legacy_id.to_string())
                .is_dir()
        );

        drop(state);
        let retry_cleanup_count = Arc::new(AtomicUsize::new(0));
        let restarted = build_test_state(
            config.clone(),
            test_policy(BackendKind::Mock, false),
            spawners(
                BackendKind::Mock,
                Arc::new(RecordingSpawner {
                    cleanup_count: retry_cleanup_count.clone(),
                }),
            ),
            BackendKind::Mock,
            storage,
        );
        let retry_report = restarted.manager.reconcile_startup().await;

        assert_eq!(retry_report.attempted, 1);
        assert_eq!(retry_report.completed, 1);
        assert!(retry_report.failures.is_empty());
        assert_eq!(retry_cleanup_count.load(Ordering::Acquire), 1);
        assert_eq!(release_count.load(Ordering::Acquire), 2);
        for id in [clean_stopped_id, legacy_id, active_id] {
            let instance = restarted.manager.get(id).expect("instance");
            assert_eq!(instance.state, SandboxState::Destroyed);
            assert_eq!(instance.backend_ownership, BackendOwnership::Stopped);
            assert!(instance.operation.is_none());
        }
        let clean_not_started = restarted
            .manager
            .get(clean_not_started_id)
            .expect("clean not-started instance");
        assert_eq!(clean_not_started.state, SandboxState::Destroyed);
        assert_eq!(
            clean_not_started.backend_ownership,
            BackendOwnership::NotStarted
        );
        assert!(clean_not_started.operation.is_none());
        assert!(
            !config
                .storage
                .instances_dir
                .join(legacy_id.to_string())
                .exists()
        );
        assert!(
            !config
                .storage
                .instances_dir
                .join(active_id.to_string())
                .exists()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn startup_reconciliation_times_out_one_record_and_continues() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let stalled_id = Uuid::new_v4();
        let completed_id = Uuid::new_v4();
        let inner = FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            config.storage.instances_dir.clone(),
        );
        for id in [stalled_id, completed_id] {
            let mut instance = SandboxInstance::new(
                BackendKind::Mock,
                WorkloadClass::AgentTool,
                "sha256:bounded-reconcile".into(),
                StartPath::Cold,
                "bounded-reconcile-test".into(),
            );
            instance.id = id;
            instance
                .transition(SandboxState::Creating)
                .expect("creating");
            instance.persist(&config.daemon.state_dir).expect("persist");
            inner
                .acquire(&AcquireOpts {
                    instance_id: id.to_string(),
                    rootfs_size: 64,
                    mem_size: 32,
                })
                .await
                .expect("storage");
        }
        let storage: Arc<dyn StorageProvider> =
            Arc::new(SelectiveHangingStorage { inner, stalled_id });
        let state = build_test_state(
            config.clone(),
            test_policy(BackendKind::Mock, false),
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            storage,
        );

        let report = state
            .manager
            .cleanup_owned_instances_with_timeout(Duration::from_millis(20))
            .await;

        assert_eq!(report.attempted, 2);
        assert_eq!(report.completed, 1);
        assert_eq!(report.failures.len(), 1);
        assert_eq!(report.failures[0].instance_id, stalled_id);
        assert!(report.failures[0].error.contains("20 ms"));
        let stalled = state.manager.get(stalled_id).expect("stalled record");
        assert_eq!(stalled.state, SandboxState::RecoveryRequired);
        assert_eq!(stalled.backend_ownership, BackendOwnership::Stopped);
        assert_eq!(
            stalled.operation.as_ref().map(|operation| operation.kind),
            Some(OperationKind::Destroy)
        );
        assert_eq!(
            state.manager.get(completed_id).expect("completed").state,
            SandboxState::Destroyed
        );
        assert!(
            config
                .storage
                .instances_dir
                .join(stalled_id.to_string())
                .is_dir()
        );
        assert!(
            !config
                .storage
                .instances_dir
                .join(completed_id.to_string())
                .exists()
        );

        drop(state);
        let retry_storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            config.storage.instances_dir.clone(),
        ));
        let restarted = build_test_state(
            config,
            test_policy(BackendKind::Mock, false),
            spawners(BackendKind::Mock, Arc::new(MockSpawner)),
            BackendKind::Mock,
            retry_storage,
        );
        let retry_report = restarted.manager.reconcile_startup().await;

        assert_eq!(retry_report.attempted, 1);
        assert_eq!(retry_report.completed, 1);
        assert!(retry_report.failures.is_empty());
        let recovered = restarted.manager.get(stalled_id).expect("recovered record");
        assert_eq!(recovered.state, SandboxState::Destroyed);
        assert_eq!(recovered.backend_ownership, BackendOwnership::Stopped);
        assert!(recovered.operation.is_none());
    }

    #[tokio::test]
    async fn startup_reconciliation_continues_after_one_cleanup_failure() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let storage = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            config.storage.instances_dir.clone(),
        ));
        let failed_id = Uuid::new_v4();
        let completed_id = Uuid::new_v4();
        for id in [failed_id, completed_id] {
            let mut instance = SandboxInstance::new(
                BackendKind::Mock,
                WorkloadClass::AgentTool,
                "sha256:reconcile".into(),
                StartPath::Cold,
                "reconcile-test".into(),
            );
            instance.id = id;
            instance
                .transition(SandboxState::Creating)
                .expect("creating");
            instance.transition(SandboxState::Running).expect("running");
            instance.backend_ownership = BackendOwnership::Running;
            instance.persist(&config.daemon.state_dir).expect("persist");
            storage
                .acquire(&AcquireOpts {
                    instance_id: id.to_string(),
                    rootfs_size: 64,
                    mem_size: 32,
                })
                .await
                .expect("storage");
        }
        let cleanup_count = Arc::new(AtomicUsize::new(0));
        let state = build_test_state(
            config.clone(),
            test_policy(BackendKind::Mock, false),
            spawners(
                BackendKind::Mock,
                Arc::new(SelectiveCleanupSpawner {
                    failed_id,
                    cleanup_count: cleanup_count.clone(),
                }),
            ),
            BackendKind::Mock,
            storage,
        );

        let report = state.manager.reconcile_startup().await;

        assert_eq!(report.attempted, 2);
        assert_eq!(report.completed, 1);
        assert_eq!(report.failures.len(), 1);
        assert_eq!(report.failures[0].instance_id, failed_id);
        assert_eq!(cleanup_count.load(Ordering::Acquire), 2);
        assert_eq!(
            state.instances.lock().expect("instances")[&failed_id].state,
            SandboxState::RecoveryRequired
        );
        assert_eq!(
            state.instances.lock().expect("instances")[&completed_id].state,
            SandboxState::Destroyed
        );
        assert!(
            config
                .storage
                .instances_dir
                .join(failed_id.to_string())
                .is_dir()
        );
        assert!(
            !config
                .storage
                .instances_dir
                .join(completed_id.to_string())
                .exists()
        );
        let created = created_json(&state, &test_request()).await;
        assert_eq!(created["instance"]["state"], "running");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn startup_reconciliation_cleans_pre_spawn_backend_ownership() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let storage: Arc<dyn StorageProvider> = Arc::new(FileStorageProvider::with_images(
            config.storage.images_dir.clone(),
            config.storage.instances_dir.clone(),
        ));
        let mut instance = SandboxInstance::new(
            BackendKind::Bubblewrap,
            WorkloadClass::AgentTool,
            "sha256:pre-spawn".into(),
            StartPath::Cold,
            "reconcile-test".into(),
        );
        instance
            .transition(SandboxState::Creating)
            .expect("creating");
        let id = instance.id;
        let run_dir = config.daemon.state_dir.join(id.to_string());
        let spawner = Arc::new(BubblewrapSpawner);
        spawner
            .prepare_spawn(&run_dir)
            .await
            .expect("persist pre-spawn handoff");
        instance
            .begin_operation(OperationKind::Create)
            .expect("begin create");
        instance.backend_ownership = BackendOwnership::Starting;
        instance
            .persist(&config.daemon.state_dir)
            .expect("persist starting ownership");
        storage
            .acquire(&AcquireOpts {
                instance_id: instance.id.to_string(),
                rootfs_size: 64,
                mem_size: 32,
            })
            .await
            .expect("storage");
        assert!(
            std::fs::read_to_string(run_dir.join("backend.pid"))
                .expect("pre-spawn handoff")
                .is_empty(),
            "crash occurred after the empty handoff was persisted but before spawn"
        );

        let state = build_test_state(
            config.clone(),
            test_policy(BackendKind::Bubblewrap, false),
            spawners(BackendKind::Bubblewrap, spawner),
            BackendKind::Bubblewrap,
            storage,
        );

        let report = state.manager.reconcile_startup().await;

        assert_eq!(report.attempted, 1);
        assert_eq!(report.completed, 1);
        assert!(report.failures.is_empty());
        assert_eq!(
            state.instances.lock().expect("instances")[&id].state,
            SandboxState::Destroyed
        );
        assert!(!config.storage.instances_dir.join(id.to_string()).exists());
        assert!(run_dir.join("backend.stopped").is_file());
    }

    #[tokio::test]
    async fn startup_reconciliation_skips_cleanup_for_known_stopped_states() {
        let temp = tempfile::tempdir().expect("temp");
        let config = test_config(&temp);
        let release_count = Arc::new(AtomicUsize::new(0));
        let storage: Arc<dyn StorageProvider> = Arc::new(CountingStorage {
            inner: FileStorageProvider::with_images(
                config.storage.images_dir.clone(),
                config.storage.instances_dir.clone(),
            ),
            release_count: release_count.clone(),
        });
        let not_started_id = Uuid::new_v4();
        let stopped_id = Uuid::new_v4();

        let mut not_started = SandboxInstance::new(
            BackendKind::Mock,
            WorkloadClass::AgentTool,
            "sha256:not-started".into(),
            StartPath::Cold,
            "reconcile-test".into(),
        );
        not_started.id = not_started_id;
        not_started
            .transition(SandboxState::Creating)
            .expect("creating");
        not_started
            .persist(&config.daemon.state_dir)
            .expect("persist");

        let mut stopped = SandboxInstance::new(
            BackendKind::Mock,
            WorkloadClass::AgentTool,
            "sha256:stopped".into(),
            StartPath::Cold,
            "reconcile-test".into(),
        );
        stopped.id = stopped_id;
        stopped
            .transition(SandboxState::Creating)
            .expect("creating");
        stopped.transition(SandboxState::Running).expect("running");
        stopped.backend_ownership = BackendOwnership::Stopped;
        stopped.persist(&config.daemon.state_dir).expect("persist");

        for id in [not_started_id, stopped_id] {
            storage
                .acquire(&AcquireOpts {
                    instance_id: id.to_string(),
                    rootfs_size: 64,
                    mem_size: 32,
                })
                .await
                .expect("storage");
        }
        let kill_count = Arc::new(AtomicUsize::new(0));
        let orphan_cleanup_count = Arc::new(AtomicUsize::new(0));
        let state = build_test_state(
            config,
            test_policy(BackendKind::Mock, false),
            spawners(
                BackendKind::Mock,
                Arc::new(CountingSpawner {
                    kill_count: kill_count.clone(),
                    orphan_cleanup_count: orphan_cleanup_count.clone(),
                }),
            ),
            BackendKind::Mock,
            storage,
        );

        let report = state.manager.reconcile_startup().await;

        assert_eq!(report.attempted, 2);
        assert_eq!(report.completed, 2);
        assert!(report.failures.is_empty());
        assert_eq!(kill_count.load(Ordering::Acquire), 0);
        assert_eq!(orphan_cleanup_count.load(Ordering::Acquire), 0);
        assert_eq!(release_count.load(Ordering::Acquire), 2);
        assert_eq!(
            state.instances.lock().expect("instances")[&not_started_id].state,
            SandboxState::Destroyed
        );
        assert_eq!(
            state.instances.lock().expect("instances")[&stopped_id].state,
            SandboxState::Destroyed
        );
    }

    #[tokio::test]
    async fn runtime_template_routes_import_list_and_get_published_artifacts() {
        let temp = tempfile::tempdir().expect("temp");
        let import_root = temp.path().join("imports");
        let source = import_root.join("source");
        std::fs::create_dir(&import_root).expect("import root");
        std::fs::create_dir(&source).expect("source");
        std::fs::write(source.join("vmstate.snap"), b"snapshot").expect("snapshot");
        std::fs::write(source.join("mem.bin"), b"memory").expect("memory");
        std::fs::write(source.join("rootfs.ext4"), b"rootfs").expect("rootfs");

        let mut config = DaemonConfig::default();
        config.daemon.state_dir = temp.path().join("state");
        config.storage.images_dir = temp.path().join("images");
        config.storage.instances_dir = temp.path().join("instances");
        config.template.dir = temp.path().join("templates");
        config.runtime_templates.dir = temp.path().join("runtime-templates");
        config.runtime_templates.import_root = Some(import_root);
        for directory in [
            &config.daemon.state_dir,
            &config.storage.images_dir,
            &config.storage.instances_dir,
            &config.template.dir,
            &config.runtime_templates.dir,
        ] {
            std::fs::create_dir_all(directory).expect("directory");
        }
        let storage: Arc<dyn blaze_core::storage::StorageProvider> =
            Arc::new(FileStorageProvider::with_images(
                config.storage.images_dir.clone(),
                config.storage.instances_dir.clone(),
            ));
        let state = Arc::new(
            ServerState::build(
                config,
                PolicyEngine::with_policies(Vec::new()),
                PoolManager::new(),
                TemplateRegistry::new(),
                HookRegistry::new(),
                spawners(BackendKind::Mock, Arc::new(MockSpawner)),
                BackendKind::Mock,
                storage,
            )
            .expect("state"),
        );

        let request = serde_json::to_vec(&json!({
            "name": "runtime-base",
            "source": "source",
            "description": "reusable runtime",
        }))
        .expect("request");
        let imported = dispatch(
            &Method::POST,
            "/v1/runtime-templates/import",
            "",
            request.clone(),
            &state,
        )
        .await
        .expect("import");
        assert_eq!(imported.status(), StatusCode::CREATED);
        let imported = serde_json::from_slice::<serde_json::Value>(
            &imported
                .into_body()
                .collect()
                .await
                .expect("body")
                .to_bytes(),
        )
        .expect("json");
        assert_eq!(imported["name"], "runtime-base");
        assert_eq!(imported["description"], "reusable runtime");

        let listed = dispatch(
            &Method::GET,
            "/v1/runtime-templates",
            "",
            Vec::new(),
            &state,
        )
        .await
        .expect("list");
        let listed = serde_json::from_slice::<serde_json::Value>(
            &listed.into_body().collect().await.expect("body").to_bytes(),
        )
        .expect("json");
        assert_eq!(listed.as_array().expect("templates").len(), 1);
        assert_eq!(listed[0]["name"], "runtime-base");

        let fetched = dispatch(
            &Method::GET,
            "/v1/runtime-templates/runtime-base",
            "",
            Vec::new(),
            &state,
        )
        .await
        .expect("get");
        let fetched = serde_json::from_slice::<serde_json::Value>(
            &fetched
                .into_body()
                .collect()
                .await
                .expect("body")
                .to_bytes(),
        )
        .expect("json");
        assert_eq!(fetched, imported);

        let duplicate = dispatch(
            &Method::POST,
            "/v1/runtime-templates/import",
            "",
            request,
            &state,
        )
        .await
        .expect_err("duplicate");
        assert!(matches!(duplicate, BlazeDaemonError::Conflict(_)));

        let legacy = dispatch(&Method::GET, "/v1/templates", "", Vec::new(), &state)
            .await
            .expect("legacy template registry");
        let legacy = serde_json::from_slice::<serde_json::Value>(
            &legacy.into_body().collect().await.expect("body").to_bytes(),
        )
        .expect("json");
        assert_eq!(legacy, json!([]));
    }
}
