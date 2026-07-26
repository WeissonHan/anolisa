// SPDX-License-Identifier: Apache-2.0
//! blaze-core: shared contracts and persistence primitives for the
//! blaze sandbox-orchestration daemon.
//!
//! This crate performs bounded local filesystem I/O for configuration,
//! lifecycle metadata, and checkpoint manifests. Process, network, and UDS
//! surfaces are implemented in the `blazed` daemon crate.
//!
//! - [`config`]: daemon TOML configuration
//! - [`policy`]: workload class + policy file schema
//! - [`backend`]: backend kinds + selection / fallback
//! - [`lifecycle`]: sandbox state machine + JSON persistence
//! - [`checkpoint`]: checkpoint metadata, integrity, lineage, and pruning
//! - [`guest_protocol`]: guest-agent wire DTOs
//! - [`storage`]: provider and slot contracts
//! - [`pool`]: warm-pool key/stat/manager
//! - [`template`]: template registry + refcnt + GC
//! - [`kernel`]: kernel hook registry, per-hook mutex
//! - [`error`]: unified [`BlazeError`] error enum

pub mod backend;
pub mod checkpoint;
pub mod config;
pub mod error;
pub mod guest_protocol;
pub mod kernel;
pub mod lifecycle;
pub mod policy;
pub mod pool;
pub mod storage;
pub mod template;

pub use error::{BlazeError, Result};
