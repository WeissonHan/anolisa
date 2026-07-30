// SPDX-License-Identifier: Apache-2.0
//! Runtime slot ownership, recovery, and capacity management.

mod pool;
mod recovery;

#[cfg(test)]
pub(crate) use pool::RuntimePoolStatus;
pub(crate) use pool::{PoolPrototype, RuntimePoolLease, RuntimeWarmPool};
pub(crate) use recovery::{
    DurableRuntimeOwner, begin_lifecycle_cleanup, reconcile_runtime_slots,
    remove_lifecycle_tombstone, runtime_dir, tombstone_lifecycle_slot,
};
