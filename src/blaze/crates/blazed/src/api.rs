// SPDX-License-Identifier: Apache-2.0
//! UDS/TCP HTTP API with canonical sandbox routes and compatibility aliases.

use std::collections::HashMap;
use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use blaze_core::backend::{BackendKind, BackendStatus, select_backend};
use blaze_core::lifecycle::SandboxInstance;
use blaze_core::policy::{ImageMetadata, RuntimeDecision, WorkloadClass, parse_duration};
use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::header::CONTENT_TYPE;
use hyper::{Method, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::error::{BlazeDaemonError, Result};
use crate::sandbox::CreateSandbox;
use crate::state::ServerState;

/// Top-level request handler. Internal errors always become JSON responses.
pub async fn handle(
    req: Request<Incoming>,
    state: Arc<ServerState>,
) -> std::result::Result<Response<Full<Bytes>>, Infallible> {
    state.metrics.inc(&state.metrics.requests_total);
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let query = req.uri().query().unwrap_or("").to_string();
    let limit = state
        .config
        .lock()
        .map(|config| config.api.max_body_bytes)
        .unwrap_or(1024 * 1024);
    let response = match collect_body(req, limit).await {
        Ok(body) => dispatch(&method, &path, &query, body, &state).await,
        Err(error) => Err(error),
    };
    Ok(match response {
        Ok(response) => response,
        Err(error) => error_response(&error, &method, &path),
    })
}

async fn collect_body(req: Request<Incoming>, limit: usize) -> Result<Vec<u8>> {
    let mut body = req.into_body();
    let mut collected = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame?;
        if let Ok(data) = frame.into_data() {
            let next = collected.len().saturating_add(data.len());
            if next > limit {
                return Err(BlazeDaemonError::PayloadTooLarge {
                    actual: next,
                    limit,
                });
            }
            collected.extend_from_slice(&data);
        }
    }
    Ok(collected)
}

async fn dispatch(
    method: &Method,
    path: &str,
    _query: &str,
    body: Vec<u8>,
    state: &Arc<ServerState>,
) -> Result<Response<Full<Bytes>>> {
    let parts = path
        .trim_start_matches('/')
        .split('/')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    match (method.as_str(), parts.as_slice()) {
        ("GET", ["v1", "health"]) => health(state),

        ("GET", ["v1", "sandboxes"]) | ("GET", ["v1", "instances"]) => list_sandboxes(state),
        ("POST", ["v1", "sandboxes"]) | ("POST", ["v1", "instances"]) => {
            create_sandbox(state, &body).await
        }
        ("GET", ["v1", "sandboxes", id]) | ("GET", ["v1", "instances", id]) => {
            get_sandbox(state, id)
        }
        ("DELETE", ["v1", "sandboxes", id]) => destroy_sandbox(state, id, true).await,
        ("DELETE", ["v1", "instances", id]) | ("POST", ["v1", "instances", id, "destroy"]) => {
            destroy_sandbox(state, id, false).await
        }
        ("POST", ["v1", "sandboxes", id, "exec"]) | ("POST", ["v1", "instances", id, "exec"]) => {
            exec_sandbox(state, id, &body).await
        }
        ("POST", ["v1", "sandboxes", id, "read"]) | ("POST", ["v1", "instances", id, "read"]) => {
            read_file(state, id, &body).await
        }
        ("POST", ["v1", "sandboxes", id, "write"]) | ("POST", ["v1", "instances", id, "write"]) => {
            write_file(state, id, &body).await
        }
        ("POST", ["v1", "sandboxes", id, "checkpoint"])
        | ("POST", ["v1", "instances", id, "checkpoint"]) => checkpoint(state, id).await,
        ("GET", ["v1", "sandboxes", id, "checkpoints"]) => list_checkpoints(state, id).await,
        ("POST", ["v1", "sandboxes", id, "checkpoints", "prune"]) => {
            prune_checkpoints(state, id).await
        }
        ("POST", ["v1", "sandboxes", id, "rollback", checkpoint_id]) => {
            rollback(state, id, checkpoint_id).await
        }
        ("POST", ["v1", "instances", id, "reset"]) => reset_to_head(state, id).await,
        ("POST", ["v1", "sandboxes", id, "hibernate"]) => hibernate(state, id).await,
        ("POST", ["v1", "sandboxes", id, "resume"]) => resume(state, id).await,

        ("GET", ["v1", "pool", "status"]) => pool_status(state),
        ("POST", ["v1", "pool", "cleanup"]) => cleanup_pool(state).await,
        ("GET", ["v1", "pools"]) => pool_status(state),
        ("GET", ["v1", "pools", _, _]) => pool_status(state),
        ("POST", ["v1", "pools", _, _, "drain"]) => cleanup_pool(state).await,

        ("GET", ["v1", "templates"]) => list_templates(state),
        ("GET", ["v1", "templates", id]) => get_template(state, id),
        ("POST", ["v1", "templates", "import"]) => import_template(state, &body).await,
        ("POST", ["v1", "templates", "gc"]) => gc_templates(state),

        ("GET", ["v1", "policies"]) => list_policies(state),
        ("GET", ["v1", "hooks"]) => list_hooks(state),
        ("GET", ["v1", "metrics"]) => metrics(state),
        ("POST", ["v1", "admin", "reload"]) => admin_reload(state),
        _ => Err(BlazeDaemonError::NotFound(format!("{method} {path}"))),
    }
}

#[derive(Debug, Deserialize)]
struct CreateRequest {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    template: Option<String>,
    #[serde(default)]
    workload_class: Option<WorkloadClass>,
    #[serde(default)]
    image_digest: Option<String>,
    #[serde(default)]
    labels: HashMap<String, String>,
    #[serde(default)]
    kernel_version: Option<String>,
}

#[derive(Debug, Serialize)]
struct CreateResponse {
    id: Uuid,
    status: String,
    template: String,
    instance: SandboxInstance,
    decision: RuntimeDecision,
    selected_backend: BackendKind,
    existing: bool,
}

async fn create_sandbox(state: &Arc<ServerState>, body: &[u8]) -> Result<Response<Full<Bytes>>> {
    let request: CreateRequest = if body.is_empty() {
        CreateRequest {
            id: None,
            template: None,
            workload_class: None,
            image_digest: None,
            labels: HashMap::new(),
            kernel_version: None,
        }
    } else {
        serde_json::from_slice(body).map_err(|error| {
            BlazeDaemonError::BadRequest(format!("invalid create body: {error}"))
        })?
    };
    let requested_id = request.id.as_deref().map(parse_uuid).transpose()?;
    let template = request.template.unwrap_or_default();
    validate_optional_name(&template, "template")?;
    let workload_class = request.workload_class.unwrap_or(WorkloadClass::AgentTool);
    let image_digest = request
        .image_digest
        .unwrap_or_else(|| format!("template:{}", default_if_empty(&template, "default")));
    let decision = evaluate_request(
        state,
        workload_class,
        &image_digest,
        &request.labels,
        request.kernel_version,
    )?;
    let binary_path = state
        .config
        .lock()
        .map_err(|_| internal_lock("config"))?
        .backends
        .get(state.active_backend.as_str())
        .cloned()
        .unwrap_or_default();
    let created = state
        .manager
        .create(CreateSandbox {
            requested_id,
            decision: decision.clone(),
            image_digest,
            template_name: template.clone(),
            binary_path,
        })
        .await?;
    if !created.existing {
        state.metrics.inc(&state.metrics.instances_created);
        if created.instance.start_path == blaze_core::lifecycle::StartPath::Warm {
            state.metrics.inc(&state.metrics.pool_hits);
        } else {
            state.metrics.inc(&state.metrics.pool_misses);
        }
    }
    let response = CreateResponse {
        id: created.instance.id,
        status: created.instance.state.to_string(),
        template,
        instance: created.instance,
        decision,
        selected_backend: created.selected_backend,
        existing: created.existing,
    };
    json_response(
        if response.existing {
            StatusCode::OK
        } else {
            StatusCode::CREATED
        },
        &response,
    )
}

fn evaluate_request(
    state: &Arc<ServerState>,
    workload_class: WorkloadClass,
    image_digest: &str,
    labels: &HashMap<String, String>,
    kernel_version: Option<String>,
) -> Result<RuntimeDecision> {
    let image = ImageMetadata {
        digest: image_digest.to_string(),
        workload_class: Some(workload_class),
        kernel_version,
    };
    let decision = state
        .policy
        .lock()
        .map_err(|_| internal_lock("policy"))?
        .evaluate(labels, &image)
        .map_err(|error| {
            state.metrics.inc(&state.metrics.policy_eval_failures);
            BlazeDaemonError::from(error)
        })?;
    let availability = decision
        .backend_priority
        .iter()
        .map(|kind| BackendStatus {
            kind: *kind,
            available: *kind == state.active_backend || state.active_backend == BackendKind::Mock,
            version: None,
        })
        .collect::<Vec<_>>();
    if select_backend(&decision.backend_priority, &availability).is_err()
        && state.active_backend != BackendKind::Mock
    {
        return Err(blaze_core::BlazeError::BackendUnavailable {
            requested: decision
                .backend_priority
                .iter()
                .map(ToString::to_string)
                .collect(),
            available: vec![state.active_backend.to_string()],
        }
        .into());
    }
    Ok(decision)
}

fn list_sandboxes(state: &Arc<ServerState>) -> Result<Response<Full<Bytes>>> {
    json_ok(&state.manager.list()?)
}

fn get_sandbox(state: &Arc<ServerState>, id: &str) -> Result<Response<Full<Bytes>>> {
    json_ok(&state.manager.get(parse_uuid(id)?)?)
}

async fn destroy_sandbox(
    state: &Arc<ServerState>,
    id: &str,
    no_content: bool,
) -> Result<Response<Full<Bytes>>> {
    let id = parse_uuid(id)?;
    let changed = state.manager.destroy(id).await?;
    if changed {
        state.metrics.inc(&state.metrics.instances_destroyed);
    }
    if no_content {
        empty_response(StatusCode::NO_CONTENT)
    } else {
        json_ok(&json!({"destroyed": true, "instance_id": id, "existing": !changed}))
    }
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

async fn exec_sandbox(
    state: &Arc<ServerState>,
    id: &str,
    body: &[u8],
) -> Result<Response<Full<Bytes>>> {
    let request: ExecRequest = parse_body(body, "exec")?;
    if request.cmd.is_empty() {
        return Err(BlazeDaemonError::BadRequest("cmd is required".to_string()));
    }
    let configured = state
        .config
        .lock()
        .map_err(|_| internal_lock("config"))?
        .api
        .request_timeout
        .clone();
    let max_secs = parse_duration(&configured)
        .map(|duration| {
            duration
                .as_secs()
                .saturating_sub(10)
                .min(u64::from(u32::MAX)) as u32
        })
        .unwrap_or(30);
    let timeout = request.timeout.unwrap_or(max_secs);
    if timeout == 0 || timeout > max_secs {
        return Err(BlazeDaemonError::BadRequest(format!(
            "timeout must be between 1 and {max_secs} seconds"
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
        "stdout": String::from_utf8_lossy(&result.stdout),
        "stderr": String::from_utf8_lossy(&result.stderr)
    }))
}

#[derive(Debug, Deserialize)]
struct FileRequest {
    path: String,
    #[serde(default)]
    data_b64: Option<String>,
}

async fn read_file(
    state: &Arc<ServerState>,
    id: &str,
    body: &[u8],
) -> Result<Response<Full<Bytes>>> {
    let request: FileRequest = parse_body(body, "read")?;
    validate_guest_path(&request.path)?;
    let data = state
        .manager
        .read_file(parse_uuid(id)?, request.path)
        .await?;
    json_ok(&json!({"data_b64": BASE64.encode(data)}))
}

async fn write_file(
    state: &Arc<ServerState>,
    id: &str,
    body: &[u8],
) -> Result<Response<Full<Bytes>>> {
    let request: FileRequest = parse_body(body, "write")?;
    validate_guest_path(&request.path)?;
    let encoded = request
        .data_b64
        .ok_or_else(|| BlazeDaemonError::BadRequest("data_b64 is required".to_string()))?;
    let data = BASE64
        .decode(encoded)
        .map_err(|error| BlazeDaemonError::BadRequest(format!("invalid base64: {error}")))?;
    state
        .manager
        .write_file(parse_uuid(id)?, request.path, &data)
        .await?;
    empty_response(StatusCode::NO_CONTENT)
}

async fn checkpoint(state: &Arc<ServerState>, id: &str) -> Result<Response<Full<Bytes>>> {
    let checkpoint = state.manager.checkpoint(parse_uuid(id)?).await?;
    json_ok(&json!({
        "status": "checkpointed",
        "checkpoint": &checkpoint.id,
        "checkpoint_id": &checkpoint.id
    }))
}

async fn list_checkpoints(state: &Arc<ServerState>, id: &str) -> Result<Response<Full<Bytes>>> {
    json_ok(&json!({
        "checkpoints": state.manager.list_checkpoints(parse_uuid(id)?).await?
    }))
}

async fn prune_checkpoints(state: &Arc<ServerState>, id: &str) -> Result<Response<Full<Bytes>>> {
    let removed = state.manager.prune_checkpoints(parse_uuid(id)?).await?;
    json_ok(&json!({
        "status": "pruned",
        "removed_count": removed.len(),
        "removed": removed
    }))
}

async fn rollback(
    state: &Arc<ServerState>,
    id: &str,
    checkpoint_id: &str,
) -> Result<Response<Full<Bytes>>> {
    blaze_core::checkpoint::validate_checkpoint_id(checkpoint_id).map_err(|error| {
        BlazeDaemonError::BadRequest(format!("invalid checkpoint identifier: {error}"))
    })?;
    state
        .manager
        .rollback(parse_uuid(id)?, checkpoint_id)
        .await?;
    json_ok(&json!({"status": "rolledback", "checkpoint": checkpoint_id}))
}

async fn reset_to_head(state: &Arc<ServerState>, id: &str) -> Result<Response<Full<Bytes>>> {
    let id = parse_uuid(id)?;
    let checkpoint = state.manager.reset_to_head(id).await?;
    state.metrics.inc(&state.metrics.instances_resets);
    json_ok(&json!({"status": "running", "checkpoint": checkpoint}))
}

async fn hibernate(state: &Arc<ServerState>, id: &str) -> Result<Response<Full<Bytes>>> {
    state.manager.hibernate(parse_uuid(id)?).await?;
    json_ok(&json!({"status": "hibernated"}))
}

async fn resume(state: &Arc<ServerState>, id: &str) -> Result<Response<Full<Bytes>>> {
    state.manager.resume(parse_uuid(id)?).await?;
    json_ok(&json!({"status": "running"}))
}

fn health(state: &Arc<ServerState>) -> Result<Response<Full<Bytes>>> {
    json_ok(&json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "backend": state.active_backend,
        "storage_pool": state.manager.pool_status()
    }))
}

fn pool_status(state: &Arc<ServerState>) -> Result<Response<Full<Bytes>>> {
    let status = state.manager.pool_status();
    json_ok(&json!({
        "ready": status.ready,
        "capacity": status.capacity,
        "pending": status.pending,
        "quarantined": status.quarantined,
        "pool_ready": status.ready,
        "pool_size": status.capacity
    }))
}

async fn cleanup_pool(state: &Arc<ServerState>) -> Result<Response<Full<Bytes>>> {
    let destroyed = state.manager.drain_pool().await?;
    json_ok(&json!({"destroyed": destroyed, "message": "warm pool drained"}))
}

fn metrics(state: &Arc<ServerState>) -> Result<Response<Full<Bytes>>> {
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/plain; version=0.0.4")
        .body(Full::new(Bytes::from(state.metrics.render())))?)
}

fn admin_reload(state: &Arc<ServerState>) -> Result<Response<Full<Bytes>>> {
    let policy_dir = state
        .config
        .lock()
        .map_err(|_| internal_lock("config"))?
        .policy
        .dir
        .clone();
    let engine = blaze_core::policy::PolicyEngine::load_dir(&policy_dir)?;
    let count = engine.policies().len();
    *state.policy.lock().map_err(|_| internal_lock("policy"))? = engine;
    json_ok(&json!({"reloaded": true, "policies": count}))
}

fn list_policies(state: &Arc<ServerState>) -> Result<Response<Full<Bytes>>> {
    let engine = state.policy.lock().map_err(|_| internal_lock("policy"))?;
    json_ok(
        &engine
            .policies()
            .iter()
            .map(|policy| {
                json!({
                    "name": policy.policy_name,
                    "priority": policy.priority,
                    "workload_class": policy.match_.workload_class
                })
            })
            .collect::<Vec<_>>(),
    )
}

fn list_hooks(state: &Arc<ServerState>) -> Result<Response<Full<Bytes>>> {
    json_ok(&state.hook.lock().map_err(|_| internal_lock("hook"))?.list())
}

fn list_templates(state: &Arc<ServerState>) -> Result<Response<Full<Bytes>>> {
    json_ok(&state.manager.list_templates()?)
}

fn get_template(state: &Arc<ServerState>, name: &str) -> Result<Response<Full<Bytes>>> {
    validate_name(name, "template")?;
    json_ok(&state.manager.get_template(name)?)
}

#[derive(Debug, Deserialize)]
struct ImportTemplateRequest {
    name: String,
    source_dir: PathBuf,
    #[serde(default)]
    description: String,
}

async fn import_template(state: &Arc<ServerState>, body: &[u8]) -> Result<Response<Full<Bytes>>> {
    let request: ImportTemplateRequest = parse_body(body, "template import")?;
    validate_name(&request.name, "template")?;
    let imported = state
        .manager
        .import_template(request.name, request.source_dir, request.description)
        .await?;
    json_response(StatusCode::CREATED, &imported)
}

fn gc_templates(state: &Arc<ServerState>) -> Result<Response<Full<Bytes>>> {
    let idle_ttl = state
        .config
        .lock()
        .map_err(|_| internal_lock("config"))?
        .template
        .idle_ttl
        .clone();
    let collected = state
        .template
        .lock()
        .map_err(|_| internal_lock("template"))?
        .gc_unused(parse_duration(&idle_ttl).unwrap_or(std::time::Duration::from_secs(3600)));
    json_ok(&json!({"collected": collected, "count": collected.len()}))
}

fn parse_body<T: for<'de> Deserialize<'de>>(body: &[u8], operation: &str) -> Result<T> {
    serde_json::from_slice(body)
        .map_err(|error| BlazeDaemonError::BadRequest(format!("invalid {operation} body: {error}")))
}

fn parse_uuid(value: &str) -> Result<Uuid> {
    Uuid::parse_str(value)
        .map_err(|error| BlazeDaemonError::BadRequest(format!("invalid sandbox UUID: {error}")))
}

fn validate_optional_name(value: &str, label: &str) -> Result<()> {
    if value.is_empty() {
        return Ok(());
    }
    validate_name(value, label)
}

fn validate_name(value: &str, label: &str) -> Result<()> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
    {
        return Err(BlazeDaemonError::BadRequest(format!(
            "{label} must be one non-empty path component"
        )));
    }
    Ok(())
}

fn validate_guest_path(path: &str) -> Result<()> {
    if path.is_empty() || !path.starts_with('/') || path.len() > 4096 || path.contains('\0') {
        return Err(BlazeDaemonError::BadRequest(
            "guest file path must be absolute, NUL-free, and at most 4096 bytes".to_string(),
        ));
    }
    Ok(())
}

fn default_if_empty<'a>(value: &'a str, default: &'a str) -> &'a str {
    if value.is_empty() { default } else { value }
}

fn json_ok<T: Serialize>(value: &T) -> Result<Response<Full<Bytes>>> {
    json_response(StatusCode::OK, value)
}

fn json_response<T: Serialize>(status: StatusCode, value: &T) -> Result<Response<Full<Bytes>>> {
    Ok(Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(serde_json::to_vec_pretty(value)?)))?)
}

fn empty_response(status: StatusCode) -> Result<Response<Full<Bytes>>> {
    Ok(Response::builder()
        .status(status)
        .body(Full::new(Bytes::new()))?)
}

fn error_response(error: &BlazeDaemonError, method: &Method, path: &str) -> Response<Full<Bytes>> {
    let status =
        StatusCode::from_u16(error.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let sandbox_id = path
        .split('/')
        .find(|segment| Uuid::parse_str(segment).is_ok())
        .map(str::to_string);
    let operation = format!("{} {}", method.as_str(), path);
    let body = json!({
        "code": error.code(),
        "message": error.to_string(),
        "operation": operation,
        "sandbox_id": sandbox_id
    });
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(
            serde_json::to_vec_pretty(&body)
                .unwrap_or_else(|_| br#"{"code":"internal_error"}"#.to_vec()),
        )))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::from_static(b"{}"))))
}

fn internal_lock(name: &str) -> BlazeDaemonError {
    BlazeDaemonError::Internal(format!("{name} lock poisoned"))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use blaze_core::config::DaemonConfig;
    use blaze_core::kernel::HookRegistry;
    use blaze_core::policy::{
        BackendConfigs, FallbackOnMissingHook, PolicyEngine, PolicyFile, PolicyHooks, PolicyMatch,
        PolicySelect,
    };
    use blaze_core::template::TemplateRegistry;

    use crate::file_provider::FileStorageProvider;
    use crate::spawner::MockSpawner;

    use super::*;

    fn state(temp: &std::path::Path) -> Arc<ServerState> {
        let images = temp.join("images");
        let instances = temp.join("instances");
        let policy_dir = temp.join("policies");
        let template_dir = temp.join("templates");
        for directory in [&images, &instances, &policy_dir, &template_dir] {
            std::fs::create_dir_all(directory).expect("dir");
        }
        let mut config = DaemonConfig::default();
        config.daemon.state_dir = temp.join("state");
        config.storage.images_dir = images.clone();
        config.storage.instances_dir = instances.clone();
        config.storage.rootfs_size = 64;
        config.storage.mem_size = 32;
        config.policy.dir = policy_dir;
        config.template.dir = template_dir;
        std::fs::create_dir_all(&config.daemon.state_dir).expect("state");
        let policy = PolicyFile {
            manifest_version: 1,
            policy_name: "test".into(),
            priority: 1,
            match_: PolicyMatch {
                workload_class: WorkloadClass::AgentTool,
                image_labels: HashMap::new(),
            },
            select: PolicySelect {
                backend_priority: vec![BackendKind::Mock],
                kernel_hooks: Vec::new(),
                templates: Vec::new(),
                fallback_on_missing_hook: FallbackOnMissingHook::Fail,
            },
            pool: None,
            checkpoint: None,
            quota: None,
            hooks: PolicyHooks::default(),
            backend: BackendConfigs::default(),
            vm: None,
        };
        Arc::new(
            ServerState::build(
                config,
                PolicyEngine::with_policies(vec![policy]),
                TemplateRegistry::new(),
                HookRegistry::new(),
                Arc::new(MockSpawner),
                BackendKind::Mock,
                Arc::new(FileStorageProvider::with_images(images, instances)),
            )
            .expect("state"),
        )
    }

    async fn route_status(
        state: &Arc<ServerState>,
        method: Method,
        path: &str,
        body: &[u8],
    ) -> StatusCode {
        match dispatch(&method, path, "", body.to_vec(), state).await {
            Ok(response) => response.status(),
            Err(error) => StatusCode::from_u16(error.status_code()).expect("error status"),
        }
    }

    async fn response_json(response: Response<Full<Bytes>>) -> serde_json::Value {
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        serde_json::from_slice(&body).expect("json")
    }

    #[tokio::test]
    async fn every_sandbox_route_reaches_its_registered_handler() {
        let temp = tempfile::tempdir().expect("temp");
        let state = state(temp.path());
        let checkpoint_id = "ckpt-00000000-0000-4000-8000-000000000000";
        let cases = [
            (Method::POST, "/v1/sandboxes", b"{".as_slice(), 400),
            (Method::GET, "/v1/sandboxes", b"".as_slice(), 200),
            (Method::GET, "/v1/sandboxes/not-a-uuid", b"", 400),
            (Method::DELETE, "/v1/sandboxes/not-a-uuid", b"", 400),
            (
                Method::POST,
                "/v1/sandboxes/not-a-uuid/exec",
                br#"{"cmd":"true"}"#,
                400,
            ),
            (Method::POST, "/v1/sandboxes/not-a-uuid/hibernate", b"", 400),
            (
                Method::POST,
                "/v1/sandboxes/not-a-uuid/checkpoint",
                b"",
                400,
            ),
            (
                Method::GET,
                "/v1/sandboxes/not-a-uuid/checkpoints",
                b"",
                400,
            ),
            (
                Method::POST,
                "/v1/sandboxes/not-a-uuid/checkpoints/prune",
                b"",
                400,
            ),
            (
                Method::POST,
                &format!("/v1/sandboxes/not-a-uuid/rollback/{checkpoint_id}"),
                b"",
                400,
            ),
            (Method::POST, "/v1/sandboxes/not-a-uuid/resume", b"", 400),
            (
                Method::POST,
                "/v1/sandboxes/not-a-uuid/read",
                br#"{"path":"/tmp/x"}"#,
                400,
            ),
            (
                Method::POST,
                "/v1/sandboxes/not-a-uuid/write",
                br#"{"path":"/tmp/x","data_b64":""}"#,
                400,
            ),
            (Method::GET, "/v1/templates", b"", 200),
            (Method::GET, "/v1/templates/missing", b"", 404),
            (Method::POST, "/v1/templates/import", b"{}", 400),
            (Method::GET, "/v1/health", b"", 200),
            (Method::POST, "/v1/pool/cleanup", b"", 200),
            (Method::GET, "/v1/pool/status", b"", 200),
        ];
        for (method, path, body, expected) in cases {
            assert_eq!(
                route_status(&state, method.clone(), path, body).await,
                StatusCode::from_u16(expected).expect("expected status"),
                "{method} {path}"
            );
        }
    }

    #[tokio::test]
    async fn compatibility_and_documented_extension_routes_are_registered() {
        let temp = tempfile::tempdir().expect("temp");
        let state = state(temp.path());
        let cases = [
            (Method::GET, "/v1/instances", b"".as_slice(), 200),
            (Method::POST, "/v1/instances", b"{".as_slice(), 400),
            (Method::GET, "/v1/instances/not-a-uuid", b"", 400),
            (Method::DELETE, "/v1/instances/not-a-uuid", b"", 400),
            (Method::POST, "/v1/instances/not-a-uuid/destroy", b"", 400),
            (
                Method::POST,
                "/v1/instances/not-a-uuid/exec",
                br#"{"cmd":"true"}"#,
                400,
            ),
            (
                Method::POST,
                "/v1/instances/not-a-uuid/read",
                br#"{"path":"/tmp/x"}"#,
                400,
            ),
            (
                Method::POST,
                "/v1/instances/not-a-uuid/write",
                br#"{"path":"/tmp/x","data_b64":""}"#,
                400,
            ),
            (
                Method::POST,
                "/v1/instances/not-a-uuid/checkpoint",
                b"",
                400,
            ),
            (Method::POST, "/v1/instances/not-a-uuid/reset", b"", 400),
            (Method::GET, "/v1/pools", b"", 200),
            (Method::GET, "/v1/pools/mock/agent-tool", b"", 200),
            (Method::POST, "/v1/pools/mock/agent-tool/drain", b"", 200),
            (Method::POST, "/v1/templates/gc", b"", 200),
            (Method::GET, "/v1/policies", b"", 200),
            (Method::GET, "/v1/hooks", b"", 200),
            (Method::GET, "/v1/metrics", b"", 200),
            (Method::POST, "/v1/admin/reload", b"", 200),
        ];
        for (method, path, body, expected) in cases {
            assert_eq!(
                route_status(&state, method.clone(), path, body).await,
                StatusCode::from_u16(expected).expect("expected status"),
                "{method} {path}"
            );
        }
    }

    #[tokio::test]
    async fn canonical_and_compatibility_create_share_manager() {
        let temp = tempfile::tempdir().expect("temp");
        let state = state(temp.path());
        let id = Uuid::new_v4();
        let body = serde_json::to_vec(&json!({
            "id": id,
            "workload_class": "agent-tool",
            "image_digest": "sha256:test"
        }))
        .expect("body");
        let created = dispatch(&Method::POST, "/v1/sandboxes", "", body.clone(), &state)
            .await
            .expect("canonical");
        assert_eq!(created.status(), StatusCode::CREATED);
        let repeated = dispatch(&Method::POST, "/v1/instances", "", body, &state)
            .await
            .expect("compatibility");
        assert_eq!(repeated.status(), StatusCode::OK);
        assert_eq!(state.manager.list().expect("list").len(), 1);
    }

    #[tokio::test]
    async fn lifecycle_retry_semantics_are_explicit() {
        let temp = tempfile::tempdir().expect("temp");
        let state = state(temp.path());
        let id = Uuid::new_v4();
        let body = serde_json::to_vec(&json!({
            "id": id,
            "workload_class": "agent-tool",
            "image_digest": "sha256:idempotency"
        }))
        .expect("body");
        assert_eq!(
            dispatch(&Method::POST, "/v1/sandboxes", "", body.clone(), &state)
                .await
                .expect("create")
                .status(),
            StatusCode::CREATED
        );
        assert_eq!(
            dispatch(&Method::POST, "/v1/sandboxes", "", body, &state)
                .await
                .expect("idempotent create")
                .status(),
            StatusCode::OK
        );

        let checkpoint_path = format!("/v1/sandboxes/{id}/checkpoint");
        let first = dispatch(&Method::POST, &checkpoint_path, "", Vec::new(), &state)
            .await
            .expect("first checkpoint");
        let first_id = response_json(first).await["checkpoint_id"]
            .as_str()
            .expect("first id")
            .to_string();
        let second = dispatch(&Method::POST, &checkpoint_path, "", Vec::new(), &state)
            .await
            .expect("second checkpoint");
        let second_id = response_json(second).await["checkpoint_id"]
            .as_str()
            .expect("second id")
            .to_string();
        assert_ne!(first_id, second_id, "checkpoint POST is non-idempotent");

        let rollback_path = format!("/v1/sandboxes/{id}/rollback/{first_id}");
        for _ in 0..2 {
            assert_eq!(
                dispatch(&Method::POST, &rollback_path, "", Vec::new(), &state)
                    .await
                    .expect("repeatable rollback")
                    .status(),
                StatusCode::OK
            );
        }

        let hibernate_path = format!("/v1/sandboxes/{id}/hibernate");
        assert_eq!(
            dispatch(&Method::POST, &hibernate_path, "", Vec::new(), &state)
                .await
                .expect("hibernate")
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            route_status(&state, Method::POST, &hibernate_path, b"").await,
            StatusCode::CONFLICT
        );
        let resume_path = format!("/v1/sandboxes/{id}/resume");
        assert_eq!(
            dispatch(&Method::POST, &resume_path, "", Vec::new(), &state)
                .await
                .expect("resume")
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            route_status(&state, Method::POST, &resume_path, b"").await,
            StatusCode::CONFLICT
        );
        assert_eq!(
            dispatch(
                &Method::DELETE,
                &format!("/v1/sandboxes/{id}"),
                "",
                Vec::new(),
                &state,
            )
            .await
            .expect("destroy")
            .status(),
            StatusCode::NO_CONTENT
        );
    }

    #[tokio::test]
    async fn errors_use_frozen_shape() {
        let response = error_response(
            &BlazeDaemonError::NotFound("sandbox".into()),
            &Method::GET,
            "/v1/sandboxes/00000000-0000-0000-0000-000000000000",
        );
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).expect("content type"),
            "application/json"
        );
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(value["code"], "not_found");
        assert_eq!(value["message"], "not found: sandbox");
        assert_eq!(
            value["operation"],
            "GET /v1/sandboxes/00000000-0000-0000-0000-000000000000"
        );
        assert_eq!(value["sandbox_id"], "00000000-0000-0000-0000-000000000000");
        assert_eq!(value.as_object().expect("object").len(), 4);
    }
}
