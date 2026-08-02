# Changelog

[中文版](CHANGELOG_zh.md)

All notable changes to ANOLISA Blaze will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - 2026-08-01

### Added

- Pause and resume a running sandbox: `POST /v1/instances/{id}/pause` and `/resume`.
- Snapshot a sandbox into a durable store: `POST /v1/instances/{id}/snapshot` keeps
  the sandbox running by default, or hibernates it with `{"leave_running": false}`.
- Restore a hibernated sandbox in place with `POST /v1/instances/{id}/restore`, and
  hatch a brand-new instance from any snapshot with `POST /v1/snapshots/{id}/restore`.
  Snapshots outlive the instance that produced them, so one image can be restored
  repeatedly and still used after the source is destroyed.
- Browse and reclaim snapshots via `GET /v1/snapshots`, `GET /v1/snapshots/{id}` and
  `DELETE /v1/snapshots/{id}`. Deleting an image a hibernated sandbox still needs is
  refused instead of stranding it.
- gVisor backend implements all four operations; `[checkpoint].enabled = false` in a
  policy now actually refuses snapshots.
- Optional `[containerd]` configuration section. When set, the gVisor backend roots
  each sandbox in an ordinary OCI image with its own writable layer instead of the
  shared read-only directory at `<images_dir>/gvisor-rootfs`, which had to be
  assembled by hand. Only containerd's image and snapshot services are used, so blaze
  keeps owning the sandbox process and the whole lifecycle. Leaving the section out
  keeps the shared base image.
- Create requests accept an `image` reference, e.g.
  `{"image": "docker.io/library/alpine:latest"}`. `image_digest` remains the workload
  identity used for policy matching and warm-pool keying; `image` is the locator the
  backend provisions from. Snapshots record it so hatching can build an identical
  filesystem in a brand-new run directory.
- New counters: `blaze_instances_paused_total`, `blaze_instances_resumed_total`,
  `blaze_instances_restored_total`, `blaze_instances_hatched_total`,
  `blaze_snapshots_created_total`, `blaze_snapshots_failed_total`,
  `blaze_snapshots_deleted_total`.

### Changed

- **BREAKING** `POST /v1/instances/{id}/checkpoint` now performs real work instead of
  only moving the state machine. It still hibernates the instance, but can now fail
  (501 when the backend cannot snapshot, 409 when the daemon holds no backend owner,
  500 when the save fails), and its `checkpoint_id` changes from `ckpt-<uuid>-<ts>` to
  a snapshot uuid resolvable through `/v1/snapshots`.
- `start_path` gains a third value, `restored`, for instances brought up from a
  snapshot. Instance state written after a restore cannot be read by 0.3.x.

### Fixed

- gVisor sandboxes are now addressed through an explicit runsc state root under
  `state_dir`, so a daemon restarted with a different environment can still manage
  and reclaim the sandboxes it started.
- Creating a gVisor instance no longer reports it as running before the sandbox is
  ready, which made an immediately following operation fail spuriously.

## [0.3.0] - 2026-07-22

### Added

- Generic `StorageProvider` trait with pluggable backend architecture.
- `FileStorageProvider`: default file-based storage backend for development and standard deployments.
- `[storage]` config section: `provider`, `pool_size`, `prefork`, `flush_interval` fields with backward-compatible defaults.
- `GET /v1/health` now includes `storage_pool` status (ready/capacity/pending).

## [0.2.1] - 2026-07-21

### Changed

- **Rebrand**: Component renamed from Anvil to Blaze. Binary: `blazed`, config path: `/etc/anolisa/blaze/`, state: `/var/lib/blaze/`.
- Firecracker vCPU configuration now validated against upper bound (1–32).

### Added

- Component registered in project manifests (root README, AGENTS.md, PR template).
- VM resource configuration fallback chain documented in README.

## [0.2.0] - 2026-06-30

### Added

- FirecrackerSpawner: Firecracker microVM backend, daemon auto-detects and selects strongest isolation at startup.
- TCP remote API: configurable `[listen].http_addr` enables TCP listener (port 14159) for platform calls.
- Prioritized backend selection: `build_spawner()` auto-selects by firecracker → linux-sandbox → mock priority.
- Storage section: `[storage].images_dir` unifies vmlinux/rootfs lookup path.
- Packaging skeleton: `dist/anvil.service` (systemd unit) + `anvil.spec` (RPM) + `tmpfiles-anvil.conf`.
- `[backends]` config section for direct backend binary path mapping.

## [0.1.3] - 2026-06-24

### Changed

- Sandbox processes now run with full namespace isolation (PID, network, filesystem).

## [0.1.2] - 2026-06-22

### Added

- Sandbox processes are now managed by the daemon: auto-spawn on create, auto-kill on destroy.
- Daemon gracefully degrades when backend binary is unavailable (useful for dev environments).

## [0.1.1] - 2026-06-20

### Added

- Policy validation rejects unsafe configurations before sandbox starts.
- Safe coordination with `osbase sandbox uninstall` (prevents removing in-use backends).

## [0.1.0] - 2026-06-18

Initial scaffold of ANOLISA Anvil per-host sandbox daemon.

### Added

- Create, list, inspect, checkpoint (state-only), reset, and destroy sandboxes via HTTP API.
- Policy-driven backend selection: assign workload class → get the right sandbox type automatically.
- Warm pool: pre-created sandboxes ready for instant allocation, configurable min/target/max.
- Template sharing: multiple sandboxes share one base memory image, reducing per-instance cost.
- Prometheus metrics endpoint for monitoring.

