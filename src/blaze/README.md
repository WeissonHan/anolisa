# ANOLISA Blaze

[中文版](README_zh.md)

Per-host sandbox orchestrator daemon for AI Agent workloads.

Blaze manages sandbox lifecycles through a daemon-only HTTP API with
policy-driven backend selection. It provides owned Firecracker processes,
guest-agent I/O, an asynchronous runtime warm pool, recoverable
hibernate/resume and checkpoint/rollback transactions, and background
storage synchronization.

## Features

- **HTTP API** — Unix domain socket (`/run/blaze/api.sock`) + optional TCP
  (TCP has no built-in authentication or TLS and must stay on a trusted network)
- **Policy-driven backend selection** — workload class → backend priority list
- **Lifecycle transactions** — 13 states plus a persisted operation journal and `RecoveryRequired`
- **Runtime warm pool** — storage-only or running/unassigned VM pre-fork slots with async refill and real resource drain
- **Firecracker ownership** — API readiness, process supervision, pause/resume, snapshot/restore, optional netns/tap/NAT
- **Guest agent** — bounded Firecracker CONNECT + JSON-line ping/exec/read/write
- **Checkpoint and hibernate** — SHA-256 artifact manifests, HEAD lineage, rollback, prune, hibernate/resume
- **Template APIs** — transactional import of sandbox template artifacts
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
instances_dir = "/var/lib/blaze/instances" # must be disjoint from images_dir
pool_size = 0            # ready slot target
prefork = false          # true also starts a ready, unassigned VM per slot
flush_interval = "30s"   # periodically fsync provider-owned slot artifacts
rootfs_size = 8589934592 # sparse fallback size in bytes
mem_size = 4294967296

[api]
max_body_bytes = 1048576
max_file_bytes = 16777216
request_timeout = "30s"
```

The file provider copies `images_dir/rootfs.ext4` and `images_dir/mem.bin`
when present, otherwise creates sparse files. Mutable slots never live below
`images_dir`. The runtime pool is initialized from the first eligible policy;
requests with a different runtime prototype take the cold path.

Firecracker policy options `enable_vsock` and `enable_network` activate the
guest-agent and isolated-network data planes respectively.

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
| GET | `/v1/pool/status` | Get actual ready/capacity/pending values |
| POST | `/v1/pool/cleanup` | Drain resources and trigger refill |
| GET | `/v1/templates` | List templates |
| GET | `/v1/templates/{id}` | Inspect a template |
| POST | `/v1/templates/import` | Transactionally import a template directory |
| POST | `/v1/templates/gc` | Trigger template GC |
| GET | `/v1/policies` | List loaded policies |
| GET | `/v1/hooks` | List kernel hooks |
| GET | `/v1/metrics` | Prometheus metrics |
| POST | `/v1/admin/reload` | Hot-reload policies |

`/v1/instances` and the legacy instance operations remain compatibility aliases
and call the same sandbox manager. API errors always contain
`code`, `message`, `operation`, and `sandbox_id`. A client-supplied create ID
must be a UUID; repeating a successful UUID create returns the existing
running sandbox when its immutable parameters match, and returns `409` when
they differ. Destroy is idempotent. Checkpoint creation is intentionally
non-idempotent and commits a new checkpoint ID on every success. Repeating a
rollback to the same checkpoint is allowed and returns the same resulting
state. Repeating hibernate or resume after success returns a state conflict
because its source-state precondition no longer holds. The API does not
interpret an idempotency-key header.

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
- Linux host only; do not build or test Blaze on macOS/Windows
- Root plus KVM, mount namespace, netns, tap, and iptables capabilities for Firecracker acceptance

## License
