# Runtime Foundations

[中文版](runtime-foundations_zh.md)

Blaze owns each sandbox through backend-neutral runtime contracts. The daemon
selects a `BackendSpawner`, receives an owned `BackendInstance`, and stores that
handle until the sandbox is destroyed or the process exits. Backend-specific
startup, pause, snapshot, restore, and cleanup behavior remains behind those
traits so the lifecycle layer can compensate completed steps when a later step
fails.

## Process ownership and recovery

Real backend processes receive `BLAZE_INSTANCE_ID` and write a backend-specific
PID file under the instance run directory. Normal shutdown requests `SIGTERM`,
allows a five-second grace period, and then uses `SIGKILL` only if the process
does not exit. Restart cleanup reads the recorded process environment and acts
only when the instance identifier matches. If that environment cannot be read,
cleanup fails closed and leaves the process for operator inspection.

Backend sockets and PID metadata are removed after successful cleanup. Serial
logs rotate at 16 MiB. The run directory and its remaining logs and
configuration are retained by the foundational instance API for diagnostics.
When the sandbox management API owns the lifecycle, destroy removes the run
directory transactionally after backend cleanup.

## File storage provider

The file provider keeps immutable images and mutable instance slots in disjoint
directories. Configuration loading rejects equal or nested roots, and daemon
startup repeats the check after path canonicalization.

Each acquired slot owns independent root filesystem and memory files. When base
artifacts exist, the provider copies them into the slot. This full-copy model
uses more capacity and adds create latency, but every slot and restored snapshot
remains self-contained: deleting or changing another slot cannot invalidate it.
The `StorageProvider` trait is the extension point for a future provider that
offers copy-on-write or content-addressed sharing while preserving the same
independent restore contract.

Persisted lifecycle records contain stable instance identifiers, not provider
paths. After restart, the configured provider reconstructs every path beneath
its own root.

## Optional VM networking

VM networking is disabled unless a policy enables it. Enabling it requires the
`ip` and `unshare` executables and sufficient host privileges. Blaze allocates a
dedicated namespace and link pair, skipping names already present on the host.
The requested interface identifier is used consistently in the VM
configuration and boot arguments.

The namespace setup provides guest outbound connectivity through the host. Host
routing, forwarding policy, DNS, and upstream connectivity remain operator
responsibilities; Blaze does not alter global host forwarding settings.

Setup is compensating: a failed step removes only resources created by that
attempt. Existing namespaces are never pre-deleted, and a concurrent allocation
is detected and retried with another slot.
