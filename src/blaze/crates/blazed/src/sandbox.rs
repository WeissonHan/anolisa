// SPDX-License-Identifier: Apache-2.0
//! Managed sandbox lifecycle and runtime ownership.

mod checkpoint;
mod flush;
mod hibernate;
mod manager;
mod restore;

pub(crate) use flush::FlushLoop;
pub use hibernate::{HibernateSandbox, ResumeSandbox};
pub use manager::{CreateSandbox, SandboxManager, SandboxManagerInit};
pub use restore::{RestoreSandbox, RestoreSandboxResult};
