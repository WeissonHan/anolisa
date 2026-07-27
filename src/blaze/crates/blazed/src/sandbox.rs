// SPDX-License-Identifier: Apache-2.0
//! Sandbox runtime manager and lifecycle transactions.

mod checkpoint;
mod hibernate;
mod manager;
mod template;

pub use manager::{CreateSandbox, SandboxManager};
