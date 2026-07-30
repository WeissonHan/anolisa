# ANOLISA Blaze

[中文版](README_zh.md)

Per-host sandbox orchestrator daemon for AI Agent workloads.

Blaze manages sandbox instance lifecycles via HTTP API with policy-driven
backend selection. It supports warm-pool pre-allocation, multi-backend
fallback (Firecracker → Bubblewrap → Mock), and Prometheus metrics export.
Designed as the per-host agent for E2B-style orchestrator platforms.

## Features

- **HTTP API** — Unix domain socket (`/run/blaze/api.sock`) + TCP (`:14159`)
- **Policy-driven backend selection** — workload class → backend priority list
- **Lifecycle state machine** — 9 states: Pending, Creating, Running, Paused,
  Checkpointed, RecoveryRequired, Reset, Warm, and Destroyed
- **Guest operations** — bounded command execution and file transfer for
  running backends that expose a guest endpoint
- **Runtime slot capacity** — independent storage slots with optional backend
  prefork and TTL-based cleanup
- **Template registry** — in-memory template tracking with idle eviction
- **Kernel hook registry** — state tracking for pre/post hooks
- **Prometheus metrics** — request counts, instance gauges, pool sizes
- **Spawners** — FirecrackerSpawner, BubblewrapSpawner, MockSpawner

## Installation

Blaze is a Labs component. This source tree contains its ANOLISA component
manifest and RPM packaging, but not every configured component repository
publishes a `blaze` candidate. Preview repository resolution before applying
the system installation:

```bash
sudo anolisa --install-mode system --dry-run install blaze
sudo anolisa --install-mode system install blaze
```

On an RPM repository that publishes Blaze:

```bash
sudo yum install blaze
```

For a developer source build:

```bash
cd src/blaze
cargo build --release --locked
```

## Quick Start

```bash
# Choose one startup method; do not run both at the same time.
# Packaged installation
sudo systemctl enable --now blazed

# Source build alternative (override policy.dir to use local examples)
sudo ./target/release/blazed daemon start --config examples/config.toml
# Note: the default config sets policy.dir = /etc/anolisa/blaze/policies.
# For source-checkout testing, create a symlink or override:
#   sudo mkdir -p /etc/anolisa/blaze
#   sudo ln -s $(pwd)/examples/policies /etc/anolisa/blaze/policies

# Health check
curl --unix-socket /run/blaze/api.sock http://localhost/v1/health

# Create a sandbox
curl -X POST --unix-socket /run/blaze/api.sock http://localhost/v1/sandboxes \
  -H 'Content-Type: application/json' \
  -d '{"workload_class":"agent-tool","image_digest":"sha256:..."}'
```

The quick-start request uses an example policy with Firecracker guest transport
disabled, so an image without the compatible guest agent does not wait for guest
readiness. Enable the transport only for images that run that agent.

## Configuration

The daemon reads a TOML config file (default: `/etc/anolisa/blaze/config.toml`)
and a policies directory containing per-workload-class policy files.

```
/etc/anolisa/blaze/
├── config.toml
└── policies/
    ├── agent-rl.toml
    └── agent-tool.toml
```

See `src/blaze/examples/` for annotated sample configurations.

### API Request Limits

The daemon accepts request bodies up to 1 MiB by default. It checks both
declared `Content-Length` values and streamed body frames, and returns HTTP
413 when the configured limit is exceeded. Override the limit with a positive
byte count:

```toml
[api]
max_body_bytes = 1048576
```

Guest files are limited to 16 MiB after base64 decoding. A full-size write is
larger on the wire because JSON and base64 add overhead, so the default 1 MiB
request limit intentionally rejects it. Set at least 22 MiB when callers need
the full decoded limit:

```toml
[api]
max_body_bytes = 23068672
```

The daemon checks both the HTTP request size and the decoded file size.

### VM Resource Configuration

Blaze resolves vCPU and memory settings using a three-layer fallback chain:

1. **Backend-specific** (`[backend.firecracker].vcpus` / `.memory`) — highest priority
2. **Policy-level** (`[vm].vcpus` / `[vm].memory`) — shared across backends
3. **Code default** (1 vCPU, 256 MiB) — fallback when unspecified

Example in a policy file:

```toml
[vm]
vcpus = 2
memory = "512Mi"

[backend.firecracker]
vcpus = 4        # overrides [vm].vcpus for Firecracker only
memory = "1Gi"   # overrides [vm].memory for Firecracker only
```

### Storage Configuration

The `[storage]` section controls the sandbox storage backend:

```toml
[storage]
provider = "file"       # Storage provider selection. Currently supported: "file", "auto".
                        # "auto" probes available providers in priority order (currently equivalent to "file").
                        # Other values will log a warning and fall back to file.
images_dir = "/var/lib/blaze/images"
pool_size = 0            # Background runtime slots; zero disables construction
prefork = false          # Start the backend before a slot becomes ready
# flush_interval = "30s"  # [Reserved] Dirty data flush period (not yet active)

[pool]
default_warm_ttl = "30m" # Used when an eligible policy omits warm_ttl
gc_interval = "5m"       # Expiry and capacity maintenance interval
```

The `file` provider uses standard filesystem operations for sandbox storage. The `auto` provider probes available backends in priority order (currently equivalent to `file`). Unrecognized values will log a warning and fall back to `file`.

When `pool_size` is non-zero, the first eligible create request fixes one
compatible build shape and starts background construction. Every slot owns
storage; a slot owns a running backend only when `prefork` is enabled.
`pool_size` limits pool-owned and in-flight slots, not sandboxes that have
already completed lifecycle handoff. Incompatible requests continue through
the existing create flow. A policy is eligible only when its `[pool]` section
sets `enabled = true`; its optional `warm_ttl` overrides `default_warm_ttl`.

On restart, the daemon cleans unclaimed slot journals before serving requests;
it does not restore old slots to the ready queue. That cleanup uses the
currently configured provider and storage/runtime roots, so those settings
must continue to identify the same owned directories across a restart. The
`/v1/pools` endpoints below expose a separate lifecycle recycling-pool
contract and do not expose this background runtime capacity. Public reset
currently returns `501`, so no production path returns a used sandbox to that
pool.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/health` | Health check |
| GET | `/v1/sandboxes` | List all sandboxes |
| POST | `/v1/sandboxes` | Create a sandbox |
| GET | `/v1/sandboxes/{id}` | Get sandbox details |
| DELETE | `/v1/sandboxes/{id}` | Destroy a sandbox |
| POST | `/v1/sandboxes/{id}/exec` | Execute a guest command |
| POST | `/v1/sandboxes/{id}/read` | Read a guest file |
| POST | `/v1/sandboxes/{id}/write` | Replace a guest file |
| GET | `/v1/instances` | Alias for listing sandboxes |
| POST | `/v1/instances` | Alias for creating a sandbox |
| GET | `/v1/instances/{id}` | Alias for sandbox details |
| DELETE | `/v1/instances/{id}` | Alias for destroying a sandbox |
| POST | `/v1/instances/{id}/destroy` | Compatible destroy action |
| POST | `/v1/instances/{id}/exec` | Compatible guest command action |
| POST | `/v1/instances/{id}/read` | Compatible guest file read action |
| POST | `/v1/instances/{id}/write` | Compatible guest file write action |
| POST | `/v1/instances/{id}/checkpoint` | Reserved; returns `501` until backend and storage capture is implemented |
| POST | `/v1/instances/{id}/reset` | Reserved; returns `501` until runtime reset is implemented |
| GET | `/v1/pools` | List lifecycle recycling pools |
| GET | `/v1/pools/{backend}/{class}` | Get lifecycle recycling-pool status |
| POST | `/v1/pools/{backend}/{class}/drain` | Drain a lifecycle recycling pool |
| PUT | `/v1/pools/{backend}/{class}/sizing` | Resize a lifecycle recycling pool |
| GET | `/v1/templates` | List templates |
| GET | `/v1/templates/{id}` | Inspect a template |
| POST | `/v1/templates/gc` | Trigger template GC |
| GET | `/v1/policies` | List loaded policies |
| GET | `/v1/hooks` | List kernel hooks |
| GET | `/v1/metrics` | Prometheus metrics |
| POST | `/v1/admin/reload` | Hot-reload policies |

### Managed lifecycle and recovery

Create and destroy record their operation before changing storage or backend
resources. A successful create finishes in `Running`; a successful destroy
finishes in `Destroyed`. If compensation cannot release every owned resource,
the sandbox remains visible as `RecoveryRequired` so destroy can be retried.

Runtime-slot reconciliation completes before this lifecycle pass. An
inventory, journal, or runtime cleanup error stops startup. After it succeeds,
the daemon reconciles each non-terminal sandbox independently. Failure to
clean up one sandbox does not prevent the remaining records from being
processed or the API from starting.

During graceful shutdown, the daemon first stops accepting work and drains
accepted connections. It then attempts bounded cleanup for every persisted
record and retained backend owner. One cleanup failure does not skip the
remaining sandboxes, and all unresolved records are reported.

The operation journal records the operation and start time, not completion of
each resource step. An interrupted create is cleaned up rather than resumed,
and an existing backend process is not adopted after restart. Failed recovery
does not run in a background retry loop. Checkpoint and reset validate that the
sandbox is running with no active lifecycle operation, then return `501`
without changing runtime or persisted state. Their backend and storage
operations are not implemented here.

### Guest operations

Guest operations are available only while a sandbox is `Running` and its
backend reports a guest endpoint. A cold create that reports such an endpoint
waits for the guest agent to answer before publishing `Running`. Backends with
guest support disabled skip that wait, and later guest-operation requests
return HTTP 409. A prefork runtime waits for guest readiness before its slot
becomes ready when the backend exposes a guest endpoint. Claim checks backend
liveness without repeating guest readiness. A storage-only slot waits after it
starts its backend.

Guest operations and lifecycle changes use the same per-sandbox operation
lock. A request may wait for an earlier lifecycle action. After it obtains the
lock, the manager checks `Running` again; if destroy or another state change
won the race, the guest request fails without contacting the old runtime.

The endpoints accept JSON:

```json
{"cmd":"uname -a","cwd":"/","env":{"LANG":"C"},"timeout":10}
```

```json
{"path":"/tmp/input","data_b64":"aGVsbG8="}
```

`read` takes only `path`; successful file reads and command output use standard
base64 in the response. Exec timeouts must be from 1 through 20 seconds. Guest
files are limited to 16 MiB after decoding, and response frames are bounded.

An exec or write failure before request delivery is an ordinary transport
failure. A bounded wait that expires before delivery uses
`"code": "guest_timeout"`. If delivery began but the daemon cannot determine
the result, the API returns HTTP 504 with
`"code": "guest_outcome_unknown"`; callers must reconcile state instead of
automatically replaying the operation. Reads do not change guest state and
remain safe for caller-directed retry. An oversized read response returns
HTTP 502 with `"code": "guest_response_too_large"`. For exec or write after
delivery starts, an oversized or otherwise untrusted response instead leaves
the outcome unknown. An oversized caller request returns HTTP 413.

Each request is fully buffered. The limits bound one request, not the sum of
concurrent requests, so callers should also bound guest-operation concurrency.
Streaming files, interactive terminals, and session reuse are not supported.

#### Health Check

`GET /v1/health` returns daemon status including provider storage-pool
readiness:

```json
{
  "status": "ok",
  "version": "0.3.0",
  "storage_pool": { "ready": 0, "capacity": 0, "pending": 0 }
}
```

The `storage_pool` object does not report background runtime-slot capacity.

## Documentation

- [Runtime slot user guide](../../docs/user-guide/en/runtime/blaze/QUICKSTART.md)
- [Runtime slot ownership design](docs/design/runtime-slot-ownership.md)

## Project Layout

```
src/blaze/
├── crates/
│   ├── blaze-core/   # Library: policy, lifecycle, pool, template, kernel, config
│   └── blazed/       # Binary: daemon, API server, spawners, metrics
├── docs/design/       # Component design documents
├── examples/         # config.toml, policies/
├── dist/             # blazed.service, blaze.spec, tmpfiles
└── manifests/        # Component metadata
```

## Requirements

- Rust 1.88+ (see `src/blaze/rust-toolchain.toml`)
- Linux host with root privileges for sandbox backends

## License
