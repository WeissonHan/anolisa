# Recovery Transactions

[中文版](recovery-transactions_zh.md)

Blaze treats checkpoint, rollback, hibernate, and resume as durable
transactions owned by `SandboxManager`. Backend-specific pause, snapshot,
restore, and termination remain behind `BackendInstance` and
`BackendSpawner`. Provider-specific data synchronization remains behind
`StorageProvider`.

Each operation records intent in the sandbox state before changing runtime
resources. A success response is possible only after the final state and
operation-marker removal are persisted. If compensation cannot restore the
previous state, Blaze retains every resource it can still identify and commits
`RecoveryRequired`.

## Checkpoint store

`CheckpointStore` keeps one directory per sandbox and one immutable directory
per committed checkpoint. A checkpoint contains:

- `vmstate.snap`
- `mem.diff`
- `rootfs.diff`
- `metadata.json`

The metadata records the sandbox identity, template and image identity, backend
and backend version, parent checkpoint, snapshot kind, logical size, and
SHA-256 digest for every required artifact.

Checkpoint creation starts in a unique hidden staging directory. Commit
synchronizes each artifact and metadata file, synchronizes the staging
directory, atomically renames it to the final checkpoint ID, and synchronizes
the sandbox checkpoint directory. A separate atomic HEAD file identifies the
selected lineage.

Publication and HEAD movement are separate commit points. An interruption
between them leaves an immutable, unreachable checkpoint. That checkpoint is
not selected for restore and can be inspected or removed by prune.

Verification rejects malformed IDs, path traversal, missing or extra identity
changes, changed sizes or digests, missing backend-version data where required,
parent cycles, cross-sandbox parents, and chains that do not resolve to a valid
root.

## Checkpoint transaction

A checkpoint transaction:

1. Persists a `Checkpoint` marker.
2. Pauses the current backend and persists `Paused`.
3. Writes backend state into the checkpoint staging directory.
4. Flushes provider-owned data and copies the root filesystem artifact.
5. Publishes the verified checkpoint and moves HEAD.
6. Resumes the original backend and rechecks guest readiness when enabled.
7. Persists `Running`, the selected checkpoint ID, and a cleared marker.

Before HEAD moves, a failure aborts staging and attempts to resume the original
backend. After publication, a failure may leave an unreachable checkpoint.
After HEAD moves, failure recovery keeps HEAD and the operation marker
diagnosable rather than claiming that the transaction never happened.

## Rollback transaction

Rollback verifies the selected checkpoint and its identity before modifying the
current runtime. It then persists `RollingBack`, stops the current backend, and
keeps a provider-local backup of the current root filesystem. Only after the
checkpoint root filesystem is installed does it restore the backend snapshot.

The restored backend must pass readiness before HEAD and the sandbox state are
committed. The previous root filesystem backup remains until that final commit.
If cleanup of a failed replacement backend also fails, the new handle is
retained and the sandbox becomes `RecoveryRequired`.

Checkpoint creation always produces a new ID. Repeating rollback to the same
valid checkpoint is allowed and produces the same selected lineage.

## Hibernate and resume

Hibernate retains the provider slot but releases the live backend:

1. Persist `Hibernating`.
2. Pause and snapshot into a unique staging directory.
3. Flush provider data and synchronize snapshot artifacts.
4. Terminate the backend.
5. Atomically publish the hibernate directory.
6. Persist `Hibernated` and clear the operation marker.

Resume reconstructs the provider slot when needed, persists `Resuming`,
restores the backend from the published hibernate artifacts, waits for
readiness, and finally commits `Running`.

A failed hibernate resumes the original backend when possible. A failed resume
terminates the replacement backend and restores `Hibernated` when possible.
Failure of those compensations enters `RecoveryRequired`. Destroy removes
published or staged hibernation artifacts before releasing the provider slot.

## File-provider tradeoff

The file provider records a complete root filesystem artifact for each
checkpoint and keeps hibernated storage in its independently owned slot. This
uses more capacity and takes longer than shared copy-on-write layers, but a
checkpoint does not depend on another sandbox's mutable files. Deleting or
changing another slot cannot invalidate restore.

## Concurrency and current boundaries

Checkpoint, rollback, hibernate, resume, prune, and destroy serialize on the
sandbox runtime mutex. Filesystem hashing and lineage traversal run on blocking
workers so they do not stall the asynchronous executor.

These services operate on manager methods and backend/provider traits. HTTP
routing, guest command execution, and guest file transfer are outside their
responsibility.
