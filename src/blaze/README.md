# ANOLISA Blaze

[中文版](README_zh.md)

Per-host sandbox orchestrator daemon for AI Agent workloads.

Blaze manages sandbox lifecycles through a daemon-only HTTP API with
policy-driven backend selection. It owns backend processes, guest operations,
warm runtime capacity, checkpoint and hibernate transactions, and template
imports behind backend-neutral contracts.

## Features

- **HTTP API** — Unix domain socket (`/run/blaze/api.sock`) + optional TCP
- **Policy-driven backend selection** — workload class → backend priority list
- **Lifecycle transactions** — persisted operation markers and recoverable ownership
- **Runtime warm pool** — storage-only or pre-started backend capacity with asynchronous refill
- **Guest operations** — bounded readiness, command execution, and file transfer
- **Checkpoint and hibernate** — verified snapshots, rollback, prune, hibernate, and resume
- **Template APIs** — transactional import of self-contained template artifacts
- **Kernel hook registry** — state tracking for pre/post hooks
- **Prometheus metrics** — request counts, instance gauges, pool sizes
- **Spawners** — FirecrackerSpawner, BubblewrapSpawner, MockSpawner

## Quick Start

```bash
# Build
cd src/blaze
cargo build --release

# Run daemon (dev: override policy.dir to use local examples)
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
  -d '{"workload_class":"agent-rl","image_digest":"sha256:..."}'
```

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
instances_dir = "/var/lib/blaze/instances"
pool_size = 0            # Ready slot target
prefork = false          # Pre-start a backend for each ready slot
flush_interval = "30s"   # Synchronize running provider slots
rootfs_size = 8589934592
mem_size = 4294967296

[api]
max_body_bytes = 1048576
max_file_bytes = 16777216
request_timeout = "30s"
```

The `file` provider gives each instance independent root filesystem and memory
files. `images_dir` and `instances_dir` must be disjoint; equal or nested paths
are rejected. Independent copies use more capacity and add create latency, but
remain valid without another instance's files. The `StorageProvider` interface
allows other implementations to optimize this tradeoff. The `auto` provider
currently resolves to `file`; unrecognized values log a warning and fall back
to it. The runtime pool is initialized from the first eligible policy;
requests with a different runtime prototype use the normal allocation path.
The daemon synchronizes each running slot on the configured interval. One
provider failure is reported without stopping the remaining sweep, and
shutdown joins the loop before runtime teardown.

### Backend Host Requirements

Firecracker requires Linux, root privileges, and the `ip` and `unshare`
executables. When a policy enables VM networking, Blaze creates a dedicated
namespace and link pair. Host routing, forwarding policy, DNS, and upstream
connectivity remain operator-managed.

See [Runtime Foundations](docs/design/runtime-foundations.md) for process
ownership, storage, networking, and recovery behavior.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/health` | Health check |
| GET, POST | `/v1/sandboxes` | List or create sandboxes |
| GET, DELETE | `/v1/sandboxes/{id}` | Inspect or idempotently destroy a sandbox |
| POST | `/v1/sandboxes/{id}/exec` | Execute a guest command |
| POST | `/v1/sandboxes/{id}/read` | Read a guest file as standard base64 |
| POST | `/v1/sandboxes/{id}/write` | Write a standard-base64 guest file |
| POST | `/v1/sandboxes/{id}/checkpoint` | Commit a checkpoint |
| GET | `/v1/sandboxes/{id}/checkpoints` | List checkpoints and HEAD reachability |
| POST | `/v1/sandboxes/{id}/checkpoints/prune` | Remove branches unreachable from HEAD |
| POST | `/v1/sandboxes/{id}/rollback/{checkpoint}` | Verify and restore a checkpoint |
| POST | `/v1/sandboxes/{id}/hibernate` | Snapshot and stop the backend |
| POST | `/v1/sandboxes/{id}/resume` | Restore a hibernated backend |
| GET | `/v1/pool/status` | Get current ready, capacity, and pending values |
| POST | `/v1/pool/cleanup` | Drain ready resources and trigger refill |
| GET | `/v1/templates` | List templates |
| GET | `/v1/templates/{id}` | Inspect a template |
| POST | `/v1/templates/import` | Transactionally import a template directory |
| POST | `/v1/templates/gc` | Trigger template GC |
| GET | `/v1/policies` | List loaded policies |
| GET | `/v1/hooks` | List kernel hooks |
| GET | `/v1/metrics` | Prometheus metrics |
| POST | `/v1/admin/reload` | Hot-reload policies |

`/v1/instances` and the existing instance operations remain compatibility
aliases and call the same sandbox manager. API errors contain `code`,
`message`, `operation`, and `sandbox_id`.

Create accepts an optional UUID. Repeating a successful create with that UUID
returns the existing running sandbox when its immutable parameters match, and
returns `409` when they differ. Destroy is idempotent. Checkpoint creation
commits a new ID on every success; rollback to the same checkpoint is
repeatable. Repeating hibernate or resume after success returns a state
conflict because its source-state precondition no longer holds.

See [Sandbox Management API](docs/design/management-api.md) for request
contracts, limits, template layout, and retry behavior.
See [Storage Synchronization](docs/design/storage-synchronization.md) for the
periodic provider contract and shutdown ordering.

#### Health Check

`GET /v1/health` returns daemon status including storage pool readiness:

```json
{
  "status": "ok",
  "version": "0.4.0",
  "backend": "mock",
  "storage_pool": { "ready": 0, "capacity": 0, "pending": 0 }
}
```

## Project Layout

```
src/blaze/
├── crates/
│   ├── blaze-core/   # Contracts: policy, lifecycle, checkpoint, guest, storage
│   └── blazed/       # Daemon: API, manager, pool, guest client, spawners
├── examples/         # config.toml, policies/
├── dist/             # blazed.service, blaze.spec, tmpfiles
└── manifests/        # Component metadata
```

## Requirements

- Rust 1.88+ (see `src/blaze/rust-toolchain.toml`)
- Linux host with root privileges for sandbox backends

## License
