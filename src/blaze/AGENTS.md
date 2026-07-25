# AGENTS.md — blaze

> Common Rust conventions (comments, module layout, dependency management, error handling, pre-commit checks, commit conventions) are defined in the [root AGENTS.md](../../AGENTS.md). This file documents **blaze-specific** additions only.

## Architecture

blaze is a **daemon-only** per-host sandbox orchestrator. All sandbox management is exposed via HTTP API; the binary only handles daemon lifecycle (start / reload / doctor).

Two-crate workspace:

- **blaze-core** (library): policy engine, lifecycle and operation journal, backend/storage/guest contracts, checkpoint integrity and lineage, template and kernel-hook registries, config schema. I/O is limited to bounded local filesystem persistence.
- **blazed** (binary): daemon HTTP server (UDS + TCP), sandbox manager, guest client, runtime warm pool, Firecracker/network process ownership, metrics, and daemon lifecycle commands.

Dependency direction: `blazed` → `blaze-core`. No reverse dependency.

## Build & Test

```bash
cd src/blaze
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets
```

Platform: Linux (x86_64 + aarch64) only. Do not build or test Blaze on macOS or Windows; use a Linux runner. `MockSpawner` provides data-plane-independent API and lifecycle tests on Linux.

## Key Design Constraints

- **Daemon-only API model**: No CLI client for sandbox operations. All instance/pool/template management is done via HTTP endpoints on UDS (`/run/blaze/api.sock`) or TCP (`:14159`). The CLI subcommands (`daemon start`, `daemon reload`, `daemon doctor`) only manage daemon lifecycle.
- **Backend ownership**: `BackendSpawner::spawn/restore` returns `Arc<dyn BackendInstance>`. The instance owns its child process and implements `wait/pause/resume/snapshot/flush_dirty/kill`; `kill` is retryable and idempotent.
- **Policy-driven backend selection**: Workload class → policy file → prioritized backend list. The daemon probes backends at startup and selects the first available. Never hardcode backend preference in application logic.
- **Lifecycle state machine**: 13 states, including hibernate/resume, rollback, and `RecoveryRequired`. Multi-step mutations persist an operation journal before touching the data plane. State transitions are enforced by `blaze_core::lifecycle`; do not bypass them.
- **Runtime locking**: Persisted metadata and non-serializable runtime handles remain separate. Every sandbox has an async mutex; never hold a global map lock across `.await`.
- **Background flush**: The standard loop calls `StorageProvider::flush_dirty` for Running sandboxes. Provider-specific dirty-page APIs are separate future capabilities, not implicit fallbacks.
- **Recovery over optimism**: If cleanup or final persistence fails, retain reconstructable resources and enter `RecoveryRequired`. Never report a successful control-plane state after a failed data-plane step.
- **MockSpawner fallback**: When the configured backend binary is missing or fails `probe()`, the daemon auto-downgrades to `MockSpawner` with a warning. This keeps API/integration tests functional without a real backend.

## Adding a New Backend

1. Add a variant to `BackendKind` in `blaze-core/src/backend.rs`
2. Implement `BackendSpawner` and its owned `BackendInstance` under `blazed/src/spawner/`
3. Register the new spawner in `daemon::build_spawner()` priority logic
4. Add a corresponding `[backends.<name>]` section in config schema (`blaze-core/src/config.rs`)
5. Add policy support: allow the new backend kind in policy `backends` priority lists
6. Add unit tests for `probe()` and `spawn()` (use mock paths for CI)

## Configuration

Runtime config: `/etc/anolisa/blaze/config.toml` + `/etc/anolisa/blaze/policies/*.toml`

Development config: `src/blaze/examples/config.toml` + `src/blaze/examples/policies/`

When modifying config schema, update both the Rust struct in `config.rs` and the example files.

## Commit Scope

Use scope `blaze` for all changes under `src/blaze/`. Examples:

```
feat(blaze): add snapshot backend
fix(blaze): handle missing rootfs gracefully
```

## Verification

Before committing:

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo doc --workspace --no-deps   # ensure no broken intra-doc links
```

Also run `cargo fmt --all -- --check`. Firecracker acceptance additionally requires a Linux/KVM environment with mount namespace, netns, tap, and iptables privileges.
