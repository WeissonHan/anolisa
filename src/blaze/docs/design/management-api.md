# Sandbox Management API

[中文版](management-api_zh.md)

Blaze exposes sandbox operations through the daemon HTTP service. The canonical
resource is `/v1/sandboxes`; existing `/v1/instances` routes are compatibility
aliases that call the same `SandboxManager`. This preserves one owner for
persisted state, provider slots, and backend handles.

## Request boundary

The daemon accepts requests over its Unix domain socket and, when configured,
an optional TCP listener. It collects at most `api.max_body_bytes` per request.
Guest read and write payloads are additionally limited by
`api.max_file_bytes`. `api.request_timeout` bounds a guest operation and must
be at least 11 seconds so command execution retains time for protocol
completion.

Errors are JSON objects with four stable fields:

```json
{
  "code": "not_found",
  "message": "not found: sandbox",
  "operation": "GET /v1/sandboxes/00000000-0000-0000-0000-000000000000",
  "sandbox_id": "00000000-0000-0000-0000-000000000000"
}
```

`sandbox_id` is `null` when the route does not contain a valid UUID.

## Sandbox operations

`POST /v1/sandboxes` accepts `workload_class`, `image_digest`, labels, an
optional template name, and an optional caller-selected UUID. Policy evaluation
chooses the backend and runtime settings before the manager creates or claims
resources.

Guest command execution accepts `cmd`, optional `cwd` and environment values,
and an optional timeout in seconds. Read requests contain an absolute `path`.
Write requests contain an absolute `path` and standard-base64 `data_b64`.

Checkpoint, rollback, hibernate, and resume use the durable transactions
described in [Recovery Transactions](recovery-transactions.md). The API does
not duplicate their ownership or keep a second lifecycle map.

## Retry behavior

- Repeating create with the same caller-selected UUID and identical immutable
  parameters returns the existing running sandbox. A mismatch returns `409`.
- Destroy is idempotent after the sandbox reaches its terminal state.
- Each successful checkpoint request creates a new checkpoint ID.
- Rollback to the same valid checkpoint may be repeated.
- Hibernate and resume require their source state. Repeating either after a
  successful transition returns a conflict.

The service does not interpret an idempotency-key header. Callers that need a
stable create retry key should supply a UUID.

## Template import

`POST /v1/templates/import` accepts:

```json
{
  "name": "runtime-base",
  "source_dir": "/var/lib/blaze/import/runtime-base",
  "description": "base runtime"
}
```

The source directory must contain `vmstate.snap`, `mem.bin`, and
`rootfs.ext4`. `template.json` is optional; the importer creates it when
absent, normalizes the name, and supplies default `rootfs_size` and
`memory_size` values. Files are copied into a unique staging directory and
published by rename only after validation. Nested directories and symbolic
links are ignored.

## Ownership and shutdown

The daemon reconciles persisted records before accepting requests. All
management routes share one manager, so compatibility aliases cannot diverge
from canonical state. On shutdown, listeners stop first and the manager then
drains warm resources and cleans up owned runtimes.

Client command-line tooling is outside this API service.
