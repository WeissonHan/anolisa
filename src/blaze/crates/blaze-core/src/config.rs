// SPDX-License-Identifier: Apache-2.0
//! Daemon configuration (`/etc/anolisa/blaze/config.toml`).

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

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
    #[serde(default)]
    pub api: ApiSection,
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

/// HTTP API request limits.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiSection {
    /// Maximum number of bytes collected from one HTTP request body.
    #[serde(default = "default_max_body_bytes")]
    pub max_body_bytes: usize,
}

impl Default for ApiSection {
    fn default() -> Self {
        Self {
            max_body_bytes: default_max_body_bytes(),
        }
    }
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

    /// Warm runtime target size (0 disables background construction).
    #[serde(default)]
    pub pool_size: usize,

    /// Whether to pre-start backends in warm runtime slots.
    #[serde(default)]
    pub prefork: bool,

    /// Interval for synchronizing provider-owned runtime data.
    ///
    /// The literal `disabled` turns off periodic synchronization.
    #[serde(default = "default_flush_interval")]
    pub flush_interval: String,

    /// Maximum duration of one provider synchronization attempt.
    #[serde(default = "default_flush_timeout")]
    pub flush_timeout: String,

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
            flush_timeout: default_flush_timeout(),
            rootfs_size: default_rootfs_size(),
            mem_size: default_mem_size(),
        }
    }
}

/// Parsed periodic storage synchronization policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageFlushSchedule {
    /// Do not run periodic synchronization.
    Disabled,
    /// Run one sweep after every configured interval.
    Every(Duration),
}

impl StorageSection {
    /// Parse the periodic synchronization setting.
    pub fn flush_schedule(&self) -> Result<StorageFlushSchedule> {
        if self.flush_interval == "disabled" {
            return Ok(StorageFlushSchedule::Disabled);
        }
        parse_duration(&self.flush_interval)
            .map(StorageFlushSchedule::Every)
            .ok_or_else(|| invalid_storage_duration("flush_interval", &self.flush_interval, true))
    }

    /// Parse the maximum duration of one provider synchronization attempt.
    pub fn flush_timeout_duration(&self) -> Result<Duration> {
        parse_duration(&self.flush_timeout)
            .ok_or_else(|| invalid_storage_duration("flush_timeout", &self.flush_timeout, false))
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

    /// Validate cross-field invariants that serde cannot express.
    pub fn validate(&self) -> Result<()> {
        if self.api.max_body_bytes == 0 {
            return Err(BlazeError::ConfigError {
                source: ConfigErrorSource::InvalidValue(
                    "api.max_body_bytes must be greater than zero".to_string(),
                ),
            });
        }
        for (name, value) in [
            ("pool.default_warm_ttl", self.pool.default_warm_ttl.as_str()),
            ("pool.gc_interval", self.pool.gc_interval.as_str()),
        ] {
            if parse_duration(value).is_none() {
                return Err(BlazeError::ConfigError {
                    source: ConfigErrorSource::InvalidValue(format!(
                        "{name} must be a positive duration with an s, m, h, or d suffix, got \
                         {value:?}"
                    )),
                });
            }
        }
        validate_runtime_storage_paths(
            &self.daemon.state_dir.join("runtime-pool"),
            &self.storage.images_dir,
            &self.storage.instances_dir,
        )?;
        self.storage.flush_schedule()?;
        self.storage.flush_timeout_duration()?;
        Ok(())
    }
}

fn invalid_storage_duration(name: &str, value: &str, allow_disabled: bool) -> BlazeError {
    let expected = if allow_disabled {
        "a positive duration or \"disabled\""
    } else {
        "a positive duration"
    };
    BlazeError::ConfigError {
        source: ConfigErrorSource::InvalidValue(format!(
            "storage.{name} ({value:?}) must be {expected}"
        )),
    }
}

/// Reject storage roots whose ownership domains overlap.
pub fn validate_storage_paths(images_dir: &Path, instances_dir: &Path) -> Result<()> {
    if paths_overlap(images_dir, instances_dir) {
        return Err(BlazeError::ConfigError {
            source: ConfigErrorSource::InvalidValue(format!(
                "storage.images_dir ({}) and storage.instances_dir ({}) must be disjoint",
                images_dir.display(),
                instances_dir.display()
            )),
        });
    }
    Ok(())
}

/// Reject runtime and storage roots whose ownership domains overlap.
///
/// Callers should invoke this once with configured paths and again with
/// canonical paths after creating the roots. The second check catches aliases
/// introduced by symbolic links.
pub fn validate_runtime_storage_paths(
    runtime_root: &Path,
    images_dir: &Path,
    instances_dir: &Path,
) -> Result<()> {
    validate_storage_paths(images_dir, instances_dir)?;
    for (name, storage_root) in [
        ("storage.images_dir", images_dir),
        ("storage.instances_dir", instances_dir),
    ] {
        if paths_overlap(runtime_root, storage_root) {
            return Err(BlazeError::ConfigError {
                source: ConfigErrorSource::InvalidValue(format!(
                    "runtime slot root ({}) and {name} ({}) must be disjoint",
                    runtime_root.display(),
                    storage_root.display()
                )),
            });
        }
    }
    Ok(())
}

fn paths_overlap(left: &Path, right: &Path) -> bool {
    left == right || left.starts_with(right) || right.starts_with(left)
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
fn default_max_body_bytes() -> usize {
    1024 * 1024
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
    "disabled".to_string()
}
fn default_flush_timeout() -> String {
    "30s".to_string()
}
fn default_rootfs_size() -> u64 {
    8 * 1024 * 1024 * 1024
}
fn default_mem_size() -> u64 {
    4 * 1024 * 1024 * 1024
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_round_trip() {
        let cfg: DaemonConfig = toml::from_str("").expect("empty parses to defaults");
        assert_eq!(cfg.daemon.log_level, "info");
        assert_eq!(cfg.policy.on_load_error, PolicyLoadErrorMode::Fail);
        assert_eq!(cfg.api.max_body_bytes, 1024 * 1024);
        assert!(cfg.backends.is_empty());
        assert_ne!(cfg.storage.images_dir, cfg.storage.instances_dir);
        assert_eq!(
            cfg.storage.flush_schedule().expect("flush schedule"),
            StorageFlushSchedule::Disabled
        );
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
    fn parses_api_body_limit() {
        let cfg: DaemonConfig = toml::from_str(
            r#"
                [api]
                max_body_bytes = 4096
            "#,
        )
        .expect("api config");
        assert_eq!(cfg.api.max_body_bytes, 4096);
        cfg.validate().expect("positive body limit");
    }

    #[test]
    fn rejects_zero_api_body_limit() {
        let cfg: DaemonConfig = toml::from_str(
            r#"
                [api]
                max_body_bytes = 0
            "#,
        )
        .expect("api config");
        let error = cfg.validate().expect_err("zero body limit");
        assert!(
            error
                .to_string()
                .contains("api.max_body_bytes must be greater than zero")
        );
    }

    #[test]
    fn rejects_invalid_runtime_pool_durations() {
        for (field, value) in [
            ("pool.default_warm_ttl", "30"),
            ("pool.default_warm_ttl", "0s"),
            ("pool.gc_interval", "soon"),
            ("pool.gc_interval", "0m"),
        ] {
            let mut cfg = DaemonConfig::default();
            if field == "pool.default_warm_ttl" {
                cfg.pool.default_warm_ttl = value.to_string();
            } else {
                cfg.pool.gc_interval = value.to_string();
            }

            let error = cfg.validate().expect_err("invalid pool duration");

            assert!(error.to_string().contains(field));
            assert!(error.to_string().contains(value));
        }
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
    fn accepts_sibling_runtime_and_storage_roots() {
        validate_runtime_storage_paths(
            Path::new("/var/lib/blaze/runtime-pool"),
            Path::new("/var/lib/blaze/images"),
            Path::new("/var/lib/blaze/instances"),
        )
        .expect("sibling ownership roots are disjoint");
    }

    #[test]
    fn rejects_equal_or_nested_runtime_and_storage_roots() {
        for (runtime, images, instances) in [
            (
                "/var/lib/blaze/images",
                "/var/lib/blaze/images",
                "/var/lib/blaze/instances",
            ),
            (
                "/var/lib/blaze",
                "/var/lib/blaze/images",
                "/var/lib/blaze/instances",
            ),
            (
                "/var/lib/blaze/images/runtime",
                "/var/lib/blaze/images",
                "/var/lib/blaze/instances",
            ),
            (
                "/var/lib/blaze/instances/runtime",
                "/var/lib/blaze/images",
                "/var/lib/blaze/instances",
            ),
        ] {
            let error = validate_runtime_storage_paths(
                Path::new(runtime),
                Path::new(images),
                Path::new(instances),
            )
            .expect_err("overlapping runtime and storage roots");
            assert!(matches!(error, BlazeError::ConfigError { .. }));
            assert!(error.to_string().contains("must be disjoint"));
        }
    }

    #[test]
    fn storage_flush_schedule_accepts_disabled_or_positive_duration() {
        let mut cfg = DaemonConfig::default();
        cfg.storage.flush_interval = "disabled".into();
        cfg.validate().expect("disabled schedule");
        assert_eq!(
            cfg.storage.flush_schedule().expect("schedule"),
            StorageFlushSchedule::Disabled
        );

        cfg.storage.flush_interval = "15s".into();
        cfg.validate().expect("positive schedule");
        assert_eq!(
            cfg.storage.flush_schedule().expect("schedule"),
            StorageFlushSchedule::Every(Duration::from_secs(15))
        );
    }

    #[test]
    fn storage_flush_schedule_rejects_invalid_values() {
        for interval in ["0s", "not-a-duration"] {
            let mut cfg = DaemonConfig::default();
            cfg.storage.flush_interval = interval.into();
            let error = cfg.validate().expect_err("invalid flush interval");
            assert!(
                error.to_string().contains("storage.flush_interval"),
                "{error}"
            );
        }

        for timeout in ["0s", "disabled", "not-a-duration"] {
            let mut cfg = DaemonConfig::default();
            cfg.storage.flush_timeout = timeout.into();
            let error = cfg.validate().expect_err("invalid flush timeout");
            assert!(
                error.to_string().contains("storage.flush_timeout"),
                "{error}"
            );
        }
    }
}
