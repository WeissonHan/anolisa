# Opened Restore Resources

[中文版](opened-restore-resources_zh.md)

## Context

Firecracker normally restores a captured virtual machine by reopening root-drive
and guest-memory paths. Reopening a path can select a different object after a
rename or replacement, and it prevents a storage implementation from handing
Blaze an object that has already been opened and verified.

Blaze therefore accepts an optional collection of typed, opened attachments on
the restore request. The ordinary file-backed path remains the default and is
unchanged when no attachment collection is supplied.

## Contract

Each collection is bound to one sandbox, one nonzero lease identifier, and one
positive lease generation. Each attachment declares:

- a unique backend role: root drive or guest memory;
- read-only or read-write access;
- exclusive or shared-read-only ownership;
- regular-file, character-device, or block-device object kind;
- a nonzero, page-aligned logical extent; and
- a pre-provisioned consumer path when the captured root drive requires one.

Before starting Firecracker, Blaze compares every declaration with facts read
from the opened descriptor. It rejects duplicate roles, stale sandbox bindings,
descriptor aliasing between roles, access or object-kind mismatches, invalid
logical extents, and writable shared attachments.

On Linux, Blaze preserves only the approved descriptors across `exec`. The root
drive is bound to its captured path inside the child process's isolated mount
namespace, while the guest-memory descriptor is passed to Firecracker as
`/proc/self/fd/<number>`.
Blaze retains the attachment collection for the lifetime of the backend owner,
so cleanup cannot close a descriptor while Firecracker still depends on it.

## Scope

This contract changes only Firecracker restore input and ownership. Provider
selection and resource leases are defined by
[Build-time Data-plane Providers](build-time-data-plane-providers.md). Restart
adoption, checkpoint ownership, suspension, and reusable capacity are separate
optional contracts described in [Provider Reconciliation](provider-reconciliation.md),
[Provider-owned Checkpoints](provider-checkpoints.md),
[Provider-owned Suspension](provider-suspension.md), and
[Provider Data-plane Capacity](provider-capacity.md).
