# Build-time Data-plane Providers

[中文版](build-time-data-plane-providers_zh.md)

## Purpose

Blaze exposes a source-level data-plane provider contract so downstream
developers can compose custom resource implementations with the daemon without
patching the Blaze source tree. The provider is selected when the final binary
is built. It is not discovered from a plugin directory, chosen by a tenant
request, or switched by the standard daemon configuration.

The standard `blazed` binary always uses the existing file implementation. A
downstream binary may depend on `blazed` as a library, implement
`DataPlaneProvider` in an extension crate, and pass that value to
`BlazeDaemonBuilder`.

## Compatibility boundary

This contract is a Rust source interface, not a stable dynamic-library ABI. A
provider binary must pin compatible revisions of:

- `blaze-provider-api`;
- `blaze-provider-conformance`;
- `blazed` and `blaze-core`;
- the Rust toolchain; and
- the dependency lock used for the final build.

`ProviderDescriptor.contract_version` protects the runtime boundary from an
obvious contract mismatch, but it does not replace source and dependency
pinning.

## Reproducible and privacy-preserving builds

Rust toolchains may retain absolute source paths in diagnostics and metadata.
For reproducible distribution builds, use a stable source root or remap build
paths with rustc's `--remap-path-prefix`. Apply the same rule to Cargo dependency
paths. Package only tracked source and declared release artifacts; do not copy
`.git`, `target`, local configuration, or generated test output.

These checks protect reproducibility, developer privacy, and deployment
secrets; they are not a substitute for documenting shipped behavior. Before
publication, compare the archive manifest with the declared release inputs and
inspect printable binary strings for unexpected local paths, host identifiers,
credentials, and undeclared configuration. Document intentionally shipped
product identifiers and settings as part of the extension release.

## Lifecycle contract

The first contract revision covers sandbox creation and deletion. Every
mutation is bound to a preselected sandbox, request, operation, lease, and
generation. A successful lifecycle follows this sequence:

| Operation | Required meaning |
|---|---|
| `probe` | Check prerequisites without allocating sandbox resources. |
| `prepare` | Create one provider-owned lease and return path-backed or opened restore resources. |
| `inspect` | Observe the exact state of a known lease without mutating it. |
| `commit` | Accept that the backend reached readiness before public state is published. |
| `finalize` | Close the handoff after the matching public state transition is durable. |
| `stop` | Record that backend use ended while retaining cleanup ownership. |
| `release` | Prove that all resources owned by the stopped lease are absent. |
| `abort` | Compensate a prepared or committed lease that has no durable public owner. |

Each successful transition keeps the same provider, request, operation, and
lease identities and increments the generation exactly once. An operation with
an uncertain result returns `OutcomeUnknown`; Blaze then uses `inspect` before
deciding whether compensation is safe.

Preparation may return either:

- `PreparedResources::PathBacked`, which preserves the existing file storage
  layout; or
- `PreparedResources::OpenedRestore`, which transfers typed root-drive and
  guest-memory descriptors for a validated template restore.

Opened resources are accepted only when the provider declares the corresponding
capability. Blaze validates the returned lease and resource shape before a
backend is started. If the resource shape is invalid but the binding is safe to
identify, Blaze calls `abort`; it does not compensate through an untrusted
binding.

## Composition

An extension crate implements `DataPlaneProvider`, defines its resource
configuration and durable state, and maps implementation errors to the
provider-independent `ProviderError` categories. A purpose-built command
binary is the composition root. The repository contains two executable
examples:

- [`minimal_provider.rs`](../../crates/blaze-provider-conformance/examples/minimal_provider.rs)
  runs the reusable create-and-delete exercise against a complete base-trait
  implementation;
- [`custom_provider_daemon.rs`](../../crates/blazed/examples/custom_provider_daemon.rs)
  passes that provider to `BlazeDaemonBuilder` and starts the daemon.

Build and inspect both entry points from `src/blaze`:

```bash
cargo run -p blaze-provider-conformance --example minimal_provider --locked
cargo run -p blazed --example custom_provider_daemon --locked -- --help
```

The daemon example accepts `--daemon-config <path>` and
`--resource-root <absolute-directory>` directly; it does not use the standard
binary's `daemon start` subcommand. The shared `ExampleFileProvider` creates
real sparse file resources, but deliberately omits persistence and all optional
extensions. Production providers must additionally test their real backend,
compensation, concurrency, and failure recovery.

The builder validates the provider descriptor and runs `probe` before Blaze
creates daemon-owned directories. A probe failure stops startup. It never
constructs the standard file provider as a replacement for a failed build-time
provider.

## Extension configuration

`DaemonConfig` remains provider-independent and does not select a build-time
extension at runtime. Each extension defines the configuration and resource
mapping it needs in its crate and composition binary. Values returned across
the Blaze contract are limited to public, implementation-neutral types and the
stable `ProviderError` categories; Blaze does not ingest arbitrary provider
error strings. Extension code may emit additional diagnostics according to the
deployer's logging policy.

Names returned through the management interface are part of the extension's
versioned product contract and should be documented by its maintainer.
Transport endpoints, resource mappings, and provider-specific settings stay in
the provider package and its own operator documentation so the Blaze
configuration remains portable across independent implementations.

Blaze uses an explicit management representation for sandboxes. It contains
the documented lifecycle, policy, and backend fields, but not data-plane lease
or recovery records. Selecting a provider at build time therefore does not
silently extend the HTTP response schema.

Contributions to Blaze itself follow the same reusable-contract rule. Public
types, comments, examples, fixtures, and diagnostics describe observable
roles, capabilities, lifecycle results, and stable error categories. Examples
must be understandable and executable from the tracked Blaze sources alone.
Provider-specific resource topology and configuration are defined and
documented by the provider that owns them.

The existing `[storage]` file directories remain required in the first
revision. Blaze still uses the file `StorageProvider` for legacy checkpoint,
hibernation, and periodic synchronization paths. Injecting a primary provider
changes create and delete only; it does not silently extend those legacy paths
to provider-owned sandboxes.

## Supported and deferred behavior

| Scenario | Current status and prerequisites |
|---|---|
| Ordinary image creation with path-backed resources | Supported |
| Template creation with path-backed resources | Supported |
| Template creation with opened root-drive and guest-memory resources | Supported when declared |
| Ordinary image creation with opened restore resources | Rejected before backend start |
| Failed compiled-provider probe | Startup fails; no file fallback |
| Provider lease adoption after daemon restart | Not supported |
| Provider-owned checkpoint and rollback | Not supported |
| Provider-owned hibernation and resume | Not supported |
| Provider capacity and reusable-resource pools | Not supported |
| Runtime dynamic-library or process plugin discovery | Not supported |

A provider that does not support restart adoption must not be presented as
production-ready for persistent workloads. Restart reconciliation, checkpoint,
hibernation, and capacity require separate optional contracts rather than
unused methods in the initial interface.

## Verification

At minimum, an extension should:

1. run `blaze-provider-conformance::exercise_create_delete` against isolated
   resources;
2. test every uncertain-result and compensation branch;
3. prove that unsupported sources fail before side effects and do not select a
   different provider;
4. verify that a real backend consumes the exact resources returned by the
   lease;
5. verify that deletion leaves no active backend, attachment, provider resource,
   or claimable lease; durable idempotency and tombstone records may remain only
   when they no longer own resources; and
6. verify that Blaze-facing APIs, logs, metrics, and conformance evidence follow
   the public contract, while documenting the extension's own resource model in
   its developer and operator materials.

The conformance crate validates the provider-independent state and resource
shape. A concrete extension still has to prove its own correctness and
production readiness.
