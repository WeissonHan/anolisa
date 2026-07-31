// SPDX-License-Identifier: Apache-2.0
//! Daemon configuration (`/etc/anolisa/blaze/config.toml`).

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{BlazeError, ConfigErrorSource, Result};

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
    pub runtime_templates: RuntimeTemplateSection,
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

/// Published runtime artifact catalog and its local import boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeTemplateSection {
    /// Directory containing atomically published runtime artifact sets.
    #[serde(default = "default_runtime_template_dir")]
    pub dir: PathBuf,
    /// Optional root containing operator-prepared import sources.
    ///
    /// Imports are disabled when this value is absent. API callers provide a
    /// relative path below this root rather than an arbitrary daemon path.
    #[serde(default)]
    pub import_root: Option<PathBuf>,
    /// Maximum number of regular files accepted from one source directory.
    #[serde(default = "default_runtime_template_max_files")]
    pub max_files: usize,
    /// Maximum final artifact and generated metadata bytes for one import.
    #[serde(default = "default_runtime_template_max_bytes")]
    pub max_bytes: u64,
    /// Maximum serialized size of one published `template.json`.
    #[serde(default = "default_runtime_template_max_metadata_bytes")]
    pub max_metadata_bytes: u64,
    /// Maximum aggregate bytes retained by the published catalog.
    #[serde(default = "default_runtime_template_max_total_bytes")]
    pub max_total_bytes: u64,
}

impl Default for RuntimeTemplateSection {
    fn default() -> Self {
        Self {
            dir: default_runtime_template_dir(),
            import_root: None,
            max_files: default_runtime_template_max_files(),
            max_bytes: default_runtime_template_max_bytes(),
            max_metadata_bytes: default_runtime_template_max_metadata_bytes(),
            max_total_bytes: default_runtime_template_max_total_bytes(),
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
        validate_storage_paths(&self.storage.images_dir, &self.storage.instances_dir)?;
        validate_runtime_template_paths(
            &self.runtime_templates.dir,
            self.runtime_templates.import_root.as_deref(),
            &self.storage.images_dir,
            &self.storage.instances_dir,
            &self.template.dir,
        )?;
        if self.runtime_templates.max_files == 0 {
            return Err(BlazeError::ConfigError {
                source: ConfigErrorSource::InvalidValue(
                    "runtime_templates.max_files must be greater than zero".to_string(),
                ),
            });
        }
        if self.runtime_templates.max_bytes == 0 {
            return Err(BlazeError::ConfigError {
                source: ConfigErrorSource::InvalidValue(
                    "runtime_templates.max_bytes must be greater than zero".to_string(),
                ),
            });
        }
        if self.runtime_templates.max_metadata_bytes == 0
            || self.runtime_templates.max_metadata_bytes > self.runtime_templates.max_bytes
        {
            return Err(BlazeError::ConfigError {
                source: ConfigErrorSource::InvalidValue(
                    "runtime_templates.max_metadata_bytes must be greater than zero and no \
                     larger than max_bytes"
                        .to_string(),
                ),
            });
        }
        if self.runtime_templates.max_total_bytes == 0 {
            return Err(BlazeError::ConfigError {
                source: ConfigErrorSource::InvalidValue(
                    "runtime_templates.max_total_bytes must be greater than zero".to_string(),
                ),
            });
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

fn validate_runtime_template_paths(
    dir: &Path,
    import_root: Option<&Path>,
    images_dir: &Path,
    instances_dir: &Path,
    template_dir: &Path,
) -> Result<()> {
    validate_absolute_root(dir, "runtime_templates.dir")?;
    if let Some(import_root) = import_root {
        validate_absolute_root(import_root, "runtime_templates.import_root")?;
    }

    let mut roots = vec![
        ("storage.images_dir", images_dir),
        ("storage.instances_dir", instances_dir),
        ("template.dir", template_dir),
    ];
    if let Some(import_root) = import_root {
        roots.push(("runtime_templates.import_root", import_root));
    }
    for (label, root) in roots {
        if paths_overlap(dir, root) {
            return Err(BlazeError::ConfigError {
                source: ConfigErrorSource::InvalidValue(format!(
                    "runtime_templates.dir ({}) and {label} ({}) must be disjoint",
                    dir.display(),
                    root.display()
                )),
            });
        }
    }

    if let Some(import_root) = import_root {
        for (label, root) in [
            ("storage.images_dir", images_dir),
            ("storage.instances_dir", instances_dir),
            ("template.dir", template_dir),
        ] {
            if paths_overlap(import_root, root) {
                return Err(BlazeError::ConfigError {
                    source: ConfigErrorSource::InvalidValue(format!(
                        "runtime_templates.import_root ({}) and {label} ({}) must be disjoint",
                        import_root.display(),
                        root.display()
                    )),
                });
            }
        }
    }
    Ok(())
}

fn validate_absolute_root(path: &Path, label: &str) -> Result<()> {
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(BlazeError::ConfigError {
            source: ConfigErrorSource::InvalidValue(format!(
                "{label} ({}) must be an absolute path without parent components",
                path.display()
            )),
        });
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
fn default_runtime_template_dir() -> PathBuf {
    PathBuf::from("/var/lib/blaze/runtime-templates")
}
fn default_runtime_template_max_files() -> usize {
    32
}
fn default_runtime_template_max_bytes() -> u64 {
    256 * 1024 * 1024 * 1024
}
fn default_runtime_template_max_metadata_bytes() -> u64 {
    1024 * 1024
}
fn default_runtime_template_max_total_bytes() -> u64 {
    1024 * 1024 * 1024 * 1024
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
        assert!(cfg.runtime_templates.import_root.is_none());
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
    fn rejects_unsafe_runtime_template_boundaries() {
        let mut relative = DaemonConfig::default();
        relative.runtime_templates.dir = PathBuf::from("runtime-templates");
        assert!(relative.validate().is_err());

        let mut parent = DaemonConfig::default();
        parent.runtime_templates.dir = PathBuf::from("/var/lib/blaze/../runtime-templates");
        assert!(parent.validate().is_err());

        let mut overlapping = DaemonConfig::default();
        overlapping.runtime_templates.import_root =
            Some(PathBuf::from("/var/lib/blaze/runtime-templates/imports"));
        assert!(overlapping.validate().is_err());

        for owned_root in [
            "/var/lib/blaze/images/catalog",
            "/var/lib/blaze/instances/catalog",
            "/var/lib/blaze/templates/catalog",
        ] {
            let mut config = DaemonConfig::default();
            config.runtime_templates.dir = PathBuf::from(owned_root);
            assert!(config.validate().is_err());
        }

        let mut source_overlap = DaemonConfig::default();
        source_overlap.runtime_templates.import_root =
            Some(PathBuf::from("/var/lib/blaze/images/imports"));
        assert!(source_overlap.validate().is_err());

        let mut unbounded = DaemonConfig::default();
        unbounded.runtime_templates.max_files = 0;
        assert!(unbounded.validate().is_err());

        let mut metadata = DaemonConfig::default();
        metadata.runtime_templates.max_metadata_bytes = metadata.runtime_templates.max_bytes + 1;
        assert!(metadata.validate().is_err());

        let mut total = DaemonConfig::default();
        total.runtime_templates.max_total_bytes = 0;
        assert!(total.validate().is_err());
    }
}
