# Storage Synchronization

[中文版](storage-synchronization_zh.md)

Blaze can periodically ask the configured `StorageProvider` to synchronize
data owned by running sandboxes. This closes the gap between a provider that
can synchronize one slot and a daemon that schedules the operation safely
across all eligible sandboxes.

Periodic synchronization is disabled by default. Set
`storage.flush_interval` to a positive duration to enable it.
`storage.flush_timeout` is a positive duration that bounds each provider call
and defaults to 30 seconds.

## Which sandboxes are synchronized

At the beginning of a sweep, the manager selects records whose lifecycle state
is `Running`. Before it calls the provider for one sandbox, it enters the same
operation lock used by create and destroy and checks the record again.

The provider call runs only when all of these conditions still hold:

- the lifecycle state is `Running`;
- there is no unfinished lifecycle operation;
- metadata says the backend is running and the daemon still owns that backend;
- the provider can reconstruct a complete slot from the sandbox ID.

A sandbox that changed state while waiting for the operation lock is skipped.
An inconsistent Running record is reported as a failed item instead of being
silently omitted. The remaining sandboxes in the sweep still run.

The first sweep starts after one complete interval. Missed ticks are skipped
instead of queued, so a slow sweep cannot create an unbounded backlog.

## Failure and retry behavior

Each provider call has its own deadline. A failure or timeout leaves the slot
owned by the sandbox and does not change lifecycle state. A later sweep or
destroy can therefore retry the provider operation.

`StorageProvider::flush_dirty` is the provider-specific persistence boundary.
Implementations must leave a cancelled call safe to retry or release. The file
provider synchronizes the canonical files in its independent sandbox slot.
Other providers can use a different mechanism while preserving the same
ownership and cancellation contract.

Storage synchronization does not save VM memory or device state. It is not a
substitute for saving and restoring a complete runtime.

## Daemon shutdown

The daemon supervises the periodic worker while serving requests. If the
worker exits unexpectedly, the daemon stops accepting work and follows the
normal coordinated shutdown path.

During normal shutdown, the daemon performs these steps in order:

1. stop accepting new connections;
2. cancel and join the synchronization worker;
3. drain accepted connections;
4. release owned runtime and storage resources.

The worker is gone before destroy starts, so a periodic provider call cannot
race with teardown of the same sandbox. If both the worker and a later
shutdown stage fail, the daemon reports both failures.
