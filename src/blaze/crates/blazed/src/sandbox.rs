// SPDX-License-Identifier: Apache-2.0
//! Sandbox runtime manager and lifecycle transactions.

#![allow(dead_code, unused_imports)] // Activated by daemon API wiring.

mod checkpoint;
mod hibernate;
mod manager;
mod template;

pub use manager::{CreateSandbox, SandboxManager};
