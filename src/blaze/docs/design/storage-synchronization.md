# Storage Synchronization

[中文版](storage-synchronization_zh.md)

Blaze periodically asks the configured `StorageProvider` to synchronize data
owned by each running sandbox. The manager does not encode provider-specific
file or device operations; `StorageProvider::flush_dirty` remains the only
persistence boundary.

## Sweep behavior

`storage.flush_interval` is a positive duration and defaults to 30 seconds.
The first sweep starts after one complete interval. Missed ticks are skipped
instead of queued, preventing a slow provider from building an unbounded
backlog.

Each sweep snapshots references for sandboxes that are `Running` before the
first provider call. It then acquires each sandbox operation lock and checks
the state again. A sandbox that changed state while waiting is skipped. This
uses the same per-sandbox serialization as checkpoint, rollback, hibernate,
resume, and destroy.

Provider failures are isolated per sandbox. The sweep records selected,
flushed, skipped, and failed counts and continues with remaining sandboxes.
Failure does not change lifecycle state because the provider still owns the
slot and a later sweep can retry it.

## Shutdown ordering

After the API listeners stop, the daemon cancels and joins the periodic task.
Only then does manager shutdown terminate owned backends and drain warm
resources. No sweep can overlap runtime teardown after the task has joined.

The file provider synchronizes its independent slot artifacts. Other providers
may implement a different persistence mechanism while preserving the same
manager ordering and retry behavior.
