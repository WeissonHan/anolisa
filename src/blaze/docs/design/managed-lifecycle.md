# Managed Sandbox Lifecycle

[中文版](managed-lifecycle_zh.md)

Blaze uses one `SandboxManager` to own persisted metadata, provider slots,
backend handles, and warm-pool claims. Backend-specific work remains behind
`BackendSpawner` and `BackendInstance`; provider-specific allocation and cleanup
remain behind `StorageProvider`. This keeps lifecycle compensation independent
from a particular runtime or storage implementation.

The manager is an internal service. HTTP routing and client-facing request
schemas are separate concerns and are not defined by this component.

## Durable operation boundary

Every multi-step mutation records an operation before changing runtime
resources. State is written to a temporary file, synchronized, atomically
renamed, and followed by a parent-directory sync. A successful state write is
therefore the commit point for the control-plane transition.

Create follows this order:

1. Validate an optional requested UUID and any existing committed record.
2. Persist `Creating` with a `Create` operation marker.
3. Claim a compatible warm slot or allocate a new provider slot.
4. Start or reuse a backend and wait for guest readiness when enabled.
5. Store the live runtime handle, persist `Running`, and clear the marker.

Destroy is idempotent after `Destroyed` is committed. Before that point, the
manager serializes access to the runtime, stops the backend, removes runtime
artifacts, releases the provider slot, and only then commits the terminal state.

If a later step fails, cleanup runs in reverse ownership order. A cleanup
failure does not discard the remaining handle or slot: the manager retains it
and commits `RecoveryRequired`, allowing a later retry to identify the resource
that still needs cleanup.

## Runtime ownership and concurrency

Serializable `SandboxInstance` records and non-serializable runtime handles use
separate maps. Global map locks are held only for short lookups and updates;
they are never held across `.await`. Each live runtime has its own asynchronous
mutex, so operations on one sandbox are ordered without blocking unrelated
sandboxes.

A supervisor waits on each backend instance. An unexpected exit is applied only
when the observed handle is still the current running handle. The sandbox is
then marked `RecoveryRequired`; a replaced or already-destroyed runtime is left
unchanged.

At startup, records that cannot be proven terminal or independently
reconstructable are marked for recovery because process-local handles were
lost. Reconciliation asks the spawner to clean a matching orphan and asks the
provider to reconstruct paths from the stable sandbox ID.

## Readiness contract

Guest readiness uses a small JSON-line protocol over the backend's socket
proxy. Each attempt opens a new connection, sends `CONNECT 5000`, requires an
`OK` response with a numeric peer identifier, and sends a correlated `ping`
request. Response lines are bounded and must carry the request ID.

Readiness polling has a total deadline, short per-attempt deadlines,
exponential backoff, and shutdown cancellation. A stalled or malformed
connection cannot poison the next attempt. Command execution and guest file
operations are intentionally outside this contract.

## Warm-pool rules

The asynchronous runtime pool can prepare storage-only slots or pre-start an
unassigned backend. It publishes a pre-started slot only after the readiness
contract succeeds.

The first eligible request pins one immutable runtime prototype: executable,
backend settings, VM settings, and network settings. A request with a different
prototype takes the normal allocation path instead of consuming an incompatible
slot. Concurrent refill never exceeds the configured target, and drain or
shutdown waits for pending builders before releasing ready resources.

Failed builders release resources when possible. If release also fails, the
slot is quarantined rather than reported as ready. This favors explicit
capacity loss over reusing a resource with uncertain ownership.

## Current boundaries

The manager in this design owns create, list, inspect, destroy, pool status,
pool cleanup, startup reconciliation, and shutdown cleanup. The persisted state
vocabulary also reserves transactional states used by checkpoint and
hibernation services; their operation sequences are specified in
[Recovery Transactions](recovery-transactions.md).

Only one warm-pool prototype is active per manager. The file provider uses
self-contained slot files, so warm allocation trades additional capacity and
copy time for independent cleanup and restore correctness.
