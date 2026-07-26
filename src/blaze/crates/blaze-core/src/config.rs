// SPDX-License-Identifier: Apache-2.0
//! Daemon configuration (`/etc/anolisa/blaze/config.toml`).

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{BlazeError, ConfigErrorSource, Result};
use crate::policy::parse_duration;

/// Top-level daemon configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DaemonConfig {
    #[serde(default)]
    pub daemon: DaemonSection,
    #[serde(default)]
    pub listen: ListenSection,
    /// Backend name → binary path mapping (e.g. `firecracker = "/usr/bin/firecracker"`).
    #[serde(default)]
    pub backends: HashMap<String, PathBuf>,
    #[serde(default)]
    pub policy: PolicySection,
    #[serde(default)]
    pub storage: StorageSection,
    #[serde(default)]
    pub pool: PoolSection,
    #[serde(default)]
    pub template: TemplateSection,
    #[serde(default)]
    pub metrics: MetricsSection,
    #[serde(default)]
    pub api: ApiSection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonSection {
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default = "default_state_dir")]
    pub state_dir: PathBuf,
    #[serde(default = "default_socket")]
    pub socket: PathBuf,
}

impl Default for DaemonSection {
    fn default() -> Self {
        Self {
            log_level: default_log_level(),
            state_dir: default_state_dir(),
            socket: default_socket(),
        }
    }
}

/// Remote API listener configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ListenSection {
    /// TCP address for remote HTTP API (e.g. "0.0.0.0:14159").
    /// Empty string or absent means remote API is disabled.
    #[serde(default)]
    pub http_addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicySection {
    #[serde(default = "default_policy_dir")]
    pub dir: PathBuf,
    #[serde(default = "default_on_load_error")]
    pub on_load_error: PolicyLoadErrorMode,
}

impl Default for PolicySection {
    fn default() -> Self {
        Self {
            dir: default_policy_dir(),
            on_load_error: default_on_load_error(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PolicyLoadErrorMode {
    Fail,
    Warn,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolSection {
    #[serde(default = "default_pool_warm_ttl")]
    pub default_warm_ttl: String,
    #[serde(default = "default_pool_gc_interval")]
    pub gc_interval: String,
}

impl Default for PoolSection {
    fn default() -> Self {
        Self {
            default_warm_ttl: default_pool_warm_ttl(),
            gc_interval: default_pool_gc_interval(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateSection {
    #[serde(default = "default_template_dir")]
    pub dir: PathBuf,
    #[serde(default = "default_template_gc_interval")]
    pub gc_interval: String,
    #[serde(default = "default_template_idle_ttl")]
    pub idle_ttl: String,
}

impl Default for TemplateSection {
    fn default() -> Self {
        Self {
            dir: default_template_dir(),
            gc_interval: default_template_gc_interval(),
            idle_ttl: default_template_idle_ttl(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSection {
    #[serde(default = "default_prometheus_socket")]
    pub prometheus_socket: PathBuf,
}

impl Default for MetricsSection {
    fn default() -> Self {
        Self {
            prometheus_socket: default_prometheus_socket(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageSection {
    /// Primary directory for vmlinux, rootfs base images, memfile bases.
    /// All runtime image files are looked up here by default.
    #[serde(default = "default_images_dir")]
    pub images_dir: PathBuf,

    /// Provider-owned runtime slots. This must not be the image directory.
    #[serde(default = "default_instances_dir")]
    pub instances_dir: PathBuf,

    /// Storage provider backend name (e.g. "file", "btrfs", "zfs").
    #[serde(default = "default_storage_provider")]
    pub provider: String,

    /// Warm pool target size (0 = no pool).
    /// NOTE: Reserved for future use. Not yet wired into runtime.
    #[serde(default)]
    pub pool_size: usize,

    /// Whether to pre-start VMs in pool slots.
    /// NOTE: Reserved for future use. Not yet wired into runtime.
    #[serde(default)]
    pub prefork: bool,

    /// Interval for flushing dirty data.
    /// NOTE: Reserved for future use. Not yet wired into runtime.
    #[serde(default = "default_flush_interval")]
    pub flush_interval: String,

    /// Logical size of file-provider root filesystem slots.
    #[serde(default = "default_rootfs_size")]
    pub rootfs_size: u64,

    /// Logical size of file-provider guest memory slots.
    #[serde(default = "default_mem_size")]
    pub mem_size: u64,
}

impl Default for StorageSection {
    fn default() -> Self {
        Self {
            images_dir: default_images_dir(),
            instances_dir: default_instances_dir(),
            provider: default_storage_provider(),
            pool_size: 0,
            prefork: false,
            flush_interval: default_flush_interval(),
            rootfs_size: default_rootfs_size(),
            mem_size: default_mem_size(),
        }
    }
}

/// HTTP and guest-I/O safety limits.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiSection {
    /// Maximum accepted HTTP request body.
    #[serde(default = "default_max_body_bytes")]
    pub max_body_bytes: usize,
    /// Maximum decoded payload for guest read/write operations.
    #[serde(default = "default_max_file_bytes")]
    pub max_file_bytes: usize,
    /// Default upper bound for one API operation.
    #[serde(default = "default_request_timeout")]
    pub request_timeout: String,
}

impl Default for ApiSection {
    fn default() -> Self {
        Self {
            max_body_bytes: default_max_body_bytes(),
            max_file_bytes: default_max_file_bytes(),
            request_timeout: default_request_timeout(),
        }
    }
}

impl DaemonConfig {
    /// Load and parse a daemon configuration file at `path`.
    pub fn load(path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path)?;
        let cfg: DaemonConfig = toml::from_str(&raw)?;
        cfg.validate()?;
        tracing::info!(path = %path.display(), "loaded blaze daemon config");
        Ok(cfg)
    }

    /// Reject configurations that make lifecycle operations unsafe or timers invalid.
    pub fn validate(&self) -> Result<()> {
        validate_storage_paths(&self.storage.images_dir, &self.storage.instances_dir)?;
        if self.storage.rootfs_size == 0 || self.storage.mem_size == 0 {
            return Err(invalid_config(
                "storage.rootfs_size and storage.mem_size must be greater than zero",
            ));
        }
        let flush_interval =
            validate_duration("storage.flush_interval", &self.storage.flush_interval, 1)?;
        if std::time::Instant::now()
            .checked_add(flush_interval)
            .is_none()
        {
            return Err(invalid_config(
                "storage.flush_interval is too large for the platform timer",
            ));
        }
        validate_duration("api.request_timeout", &self.api.request_timeout, 11)?;
        if self.api.max_body_bytes == 0 || self.api.max_file_bytes == 0 {
            return Err(invalid_config(
                "api.max_body_bytes and api.max_file_bytes must be greater than zero",
            ));
        }
        Ok(())
    }
}

/// Reject storage roots whose ownership domains overlap.
pub fn validate_storage_paths(images_dir: &Path, instances_dir: &Path) -> Result<()> {
    if images_dir == instances_dir
        || images_dir.starts_with(instances_dir)
        || instances_dir.starts_with(images_dir)
    {
        return Err(invalid_config(format!(
            "storage.images_dir ({}) and storage.instances_dir ({}) must be disjoint",
            images_dir.display(),
            instances_dir.display()
        )));
    }
    Ok(())
}

fn validate_duration(name: &str, value: &str, minimum_secs: u64) -> Result<std::time::Duration> {
    let duration = parse_duration(value)
        .ok_or_else(|| invalid_config(format!("{name} must be a positive duration")))?;
    if duration.as_secs() < minimum_secs {
        return Err(invalid_config(format!(
            "{name} must be at least {minimum_secs}s"
        )));
    }
    Ok(duration)
}

fn invalid_config(message: impl Into<String>) -> BlazeError {
    BlazeError::ConfigError {
        source: ConfigErrorSource::InvalidValue(message.into()),
    }
}

// ----- defaults -----

fn default_log_level() -> String {
    "info".to_string()
}
fn default_state_dir() -> PathBuf {
    PathBuf::from("/var/lib/blaze")
}
fn default_socket() -> PathBuf {
    PathBuf::from("/run/blaze/api.sock")
}
fn default_policy_dir() -> PathBuf {
    PathBuf::from("/etc/anolisa/blaze/policies")
}
fn default_on_load_error() -> PolicyLoadErrorMode {
    PolicyLoadErrorMode::Fail
}
fn default_pool_warm_ttl() -> String {
    "30m".to_string()
}
fn default_pool_gc_interval() -> String {
    "5m".to_string()
}
fn default_template_dir() -> PathBuf {
    PathBuf::from("/var/lib/blaze/templates")
}
fn default_template_gc_interval() -> String {
    "10m".to_string()
}
fn default_template_idle_ttl() -> String {
    "1h".to_string()
}
fn default_prometheus_socket() -> PathBuf {
    PathBuf::from("/run/blaze/metrics.sock")
}
fn default_images_dir() -> PathBuf {
    PathBuf::from("/var/lib/blaze/images")
}
fn default_instances_dir() -> PathBuf {
    PathBuf::from("/var/lib/blaze/instances")
}
fn default_storage_provider() -> String {
    "file".to_string()
}
fn default_flush_interval() -> String {
    "30s".to_string()
}
fn default_rootfs_size() -> u64 {
    8 * 1024 * 1024 * 1024
}
fn default_mem_size() -> u64 {
    4 * 1024 * 1024 * 1024
}
fn default_max_body_bytes() -> usize {
    1024 * 1024
}
fn default_max_file_bytes() -> usize {
    16 * 1024 * 1024
}
fn default_request_timeout() -> String {
    "30s".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_round_trip() {
        let cfg: DaemonConfig = toml::from_str("").expect("empty parses to defaults");
        assert_eq!(cfg.daemon.log_level, "info");
        assert_eq!(cfg.policy.on_load_error, PolicyLoadErrorMode::Fail);
        assert!(cfg.backends.is_empty());
        assert_ne!(cfg.storage.images_dir, cfg.storage.instances_dir);
        assert_eq!(cfg.api.max_body_bytes, 1024 * 1024);
    }

    #[test]
    fn parses_full_example() {
        let toml_str = r#"
            [daemon]
            log_level = "debug"
            state_dir = "/tmp/blaze"
            socket = "/tmp/blaze/api.sock"

            [backends]
            firecracker = "/usr/bin/firecracker"
            linux-sandbox = "/usr/bin/anolisa-linux-sandbox"

            [policy]
            dir = "/etc/anolisa/blaze/policies"
            on_load_error = "warn"
        "#;
        let cfg: DaemonConfig = toml::from_str(toml_str).expect("parses");
        assert_eq!(cfg.daemon.log_level, "debug");
        assert_eq!(cfg.policy.on_load_error, PolicyLoadErrorMode::Warn);
        assert_eq!(cfg.backends.len(), 2);
    }

    #[test]
    fn rejects_equal_or_nested_storage_roots() {
        for (images, instances) in [
            ("/var/lib/blaze/data", "/var/lib/blaze/data"),
            ("/var/lib/blaze/data", "/var/lib/blaze/data/instances"),
            ("/var/lib/blaze/images/base", "/var/lib/blaze/images"),
        ] {
            let mut cfg = DaemonConfig::default();
            cfg.storage.images_dir = PathBuf::from(images);
            cfg.storage.instances_dir = PathBuf::from(instances);
            let error = cfg.validate().expect_err("overlapping paths");
            assert!(error.to_string().contains("must be disjoint"));
        }
    }

    #[test]
    fn validation_rejects_short_timeout() {
        let mut cfg = DaemonConfig::default();
        cfg.api.request_timeout = "10s".into();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validation_rejects_invalid_or_subsecond_flush_interval() {
        for invalid in ["", "0s", "500ms", "30", "later", "18446744073709551615s"] {
            let mut cfg = DaemonConfig::default();
            cfg.storage.flush_interval = invalid.to_string();
            let error = cfg.validate().expect_err("invalid flush interval");
            assert!(
                error.to_string().contains("storage.flush_interval"),
                "unexpected error for {invalid:?}: {error}"
            );
        }

        let mut cfg = DaemonConfig::default();
        cfg.storage.flush_interval = "1s".into();
        cfg.validate().expect("one second is the minimum");
    }
}
