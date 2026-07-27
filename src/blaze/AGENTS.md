# AGENTS.md — blaze

> Common Rust conventions (comments, module layout, dependency management, error handling, pre-commit checks, commit conventions) are defined in the [root AGENTS.md](../../AGENTS.md). This file documents **blaze-specific** additions only.

## Architecture

Blaze is a per-host sandbox orchestrator with a daemon-only server binary and
a separate HTTP client. `blazed` owns daemon lifecycle and all orchestration
state; `blazectl` only maps its frozen command surface to the daemon HTTP API.

Three-crate workspace:

- **blaze-core** (library): policy engine, lifecycle state machine, backend selector, pool manager, template registry, kernel hook registry, config schema. Zero I/O beyond local TOML/JSON parsing.
- **blazed** (binary): daemon HTTP server (UDS + TCP), spawner implementations, metrics endpoint, CLI for daemon lifecycle commands.
- **blazectl** (binary + library): bounded UDS/TCP HTTP client, command grammar,
  local wire DTOs, deterministic text/JSON output, binary-safe guest file I/O,
  and the 14 supported remote operations plus local version output.

Dependency direction: `blazed` → `blaze-core`. `blazectl` does not depend on
either crate and must not duplicate daemon state transitions or data-plane
logic. No reverse dependency is allowed.

## Build & Test

```bash
cd src/blaze
cargo build --workspace --locked
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test -p blazectl --features integration --locked
```

Platform: Linux (x86_64 + aarch64) only. Do not build or test Blaze on macOS
or Windows; use a Linux runner. `MockSpawner` provides data-plane-independent
API and lifecycle tests on Linux.

## Key Design Constraints

- **Binary boundary**: `blazed` remains daemon-only; it handles only daemon
  lifecycle and serves the HTTP API. `blazectl` is an HTTP client and never
  imports the `blazed` binary crate, starts the daemon, or performs data-plane
  work locally.
- **Frozen client surface**: `blazectl` exposes exactly `create`, `exec`,
  `list`, `kill`, `hibernate`, `checkpoint`, `rollback`, `checkpoints`,
  `prune-checkpoints`, `resume`, `cleanup-devices`, `pool-status`, `read`, and
  `write`, plus local `version`/`--version`. Do not add template, policy,
  metrics, hook, admin, or daemon-lifecycle commands without a new contract.
- **Client safety**: The default endpoint is `/run/blaze/api.sock`; explicit
  TCP is plain HTTP and must not be widened implicitly. `exec` passes an opaque
  command string to the guest and never invokes a local shell. File operations
  remain binary-safe. `kill --all` attempts every listed target with at most 50
  requests in flight and returns nonzero on any failed or unfinished target.
- **BackendSpawner trait**: All backend-specific process management is behind `BackendSpawner`. Adding a new backend means implementing `spawn()`, `wait()`, `kill()`, `probe()` and registering it in `daemon::build_spawner()`.
- **Policy-driven backend selection**: Workload class → policy file → prioritized backend list. The daemon probes backends at startup and selects the first available. Never hardcode backend preference in application logic.
- **Lifecycle state machine**: 13 states, including hibernate/resume, rollback,
  and `RecoveryRequired`. Multi-step mutations persist an operation journal
  before touching the data plane. State transitions are enforced by
  `blaze_core::lifecycle`; do not bypass them.
- **MockSpawner fallback**: When the configured backend binary is missing or fails `probe()`, the daemon auto-downgrades to `MockSpawner` with a warning. This keeps API/integration tests functional without a real backend.

## Adding a New Backend

1. Add a variant to `BackendKind` in `blaze-core/src/backend.rs`
2. Implement `BackendSpawner` in `blazed/src/spawner.rs`
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
cargo fmt --all -- --check
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo doc --workspace --no-deps --locked
```
