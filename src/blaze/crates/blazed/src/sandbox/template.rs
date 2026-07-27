// SPDX-License-Identifier: Apache-2.0
//! Template catalog operations owned by the manager service.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::json;
use uuid::Uuid;

use crate::error::{BlazeDaemonError, Result};

use super::manager::SandboxManager;

impl SandboxManager {
    /// List transactionally imported templates.
    pub fn list_templates(&self) -> Result<Vec<serde_json::Value>> {
        let directory = &self.config.template.dir;
        let mut templates = Vec::new();
        if directory.exists() {
            for entry in std::fs::read_dir(directory)? {
                let entry = entry?;
                if !entry.file_type()?.is_dir() {
                    continue;
                }
                let metadata = entry.path().join("template.json");
                if let Ok(value) = read_json(&metadata) {
                    templates.push(value);
                }
            }
        }
        Ok(templates)
    }

    /// Read one imported template by name.
    pub fn get_template(&self, name: &str) -> Result<serde_json::Value> {
        validate_name(name, "template")?;
        let path = self.config.template.dir.join(name).join("template.json");
        if !path.is_file() {
            return Err(BlazeDaemonError::NotFound(format!("template {name}")));
        }
        read_json(&path)
    }

    /// Copy and atomically publish a template directory.
    pub async fn import_template(
        self: &Arc<Self>,
        name: String,
        source_dir: PathBuf,
        description: String,
    ) -> Result<serde_json::Value> {
        validate_name(&name, "template")?;
        let template_root = self.config.template.dir.clone();
        tokio::task::spawn_blocking(move || {
            copy_template_transactional(&source_dir, &template_root, &name, &description)
        })
        .await
        .map_err(|error| BlazeDaemonError::Internal(format!("template import task: {error}")))?
    }
}

fn copy_template_transactional(
    source: &Path,
    root: &Path,
    name: &str,
    description: &str,
) -> Result<serde_json::Value> {
    if !source.is_dir() {
        return Err(BlazeDaemonError::BadRequest(format!(
            "template source {} is not a directory",
            source.display()
        )));
    }
    std::fs::create_dir_all(root)?;
    let destination = root.join(name);
    if destination.exists() {
        return Err(BlazeDaemonError::Conflict(format!(
            "template {name} already exists"
        )));
    }
    let staging = root.join(format!(".{name}.{}.tmp", Uuid::new_v4()));
    std::fs::create_dir(&staging)?;
    let outcome = (|| {
        for entry in std::fs::read_dir(source)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_symlink() || file_type.is_dir() {
                continue;
            }
            std::fs::copy(entry.path(), staging.join(entry.file_name()))?;
        }
        let template_json = staging.join("template.json");
        let mut value = if template_json.is_file() {
            read_json(&template_json)?
        } else {
            json!({"name": name})
        };
        if !value.is_object() {
            return Err(BlazeDaemonError::BadRequest(
                "template.json must contain a JSON object".to_string(),
            ));
        }
        value["name"] = json!(name);
        if !description.is_empty() {
            value["description"] = json!(description);
        }
        if value
            .get("rootfs_size")
            .and_then(serde_json::Value::as_u64)
            .is_none()
        {
            value["rootfs_size"] = json!(8_u64 * 1024 * 1024 * 1024);
        }
        if value
            .get("memory_size")
            .and_then(serde_json::Value::as_u64)
            .is_none()
        {
            value["memory_size"] = json!(4_u64 * 1024 * 1024 * 1024);
        }
        std::fs::write(&template_json, serde_json::to_vec_pretty(&value)?)?;
        for required in ["vmstate.snap", "mem.bin", "rootfs.ext4"] {
            if !staging.join(required).is_file() {
                return Err(BlazeDaemonError::BadRequest(format!(
                    "template source is missing {required}"
                )));
            }
        }
        std::fs::rename(&staging, &destination)?;
        Ok(value)
    })();
    if outcome.is_err() {
        let _ = std::fs::remove_dir_all(staging);
    }
    outcome
}

fn read_json(path: &Path) -> Result<serde_json::Value> {
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}

fn validate_name(value: &str, label: &str) -> Result<()> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
    {
        return Err(BlazeDaemonError::BadRequest(format!(
            "{label} must be one non-empty path component"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn import_publishes_canonical_artifacts_and_defaults() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source = temp.path().join("source");
        let root = temp.path().join("templates");
        std::fs::create_dir(&source).expect("source directory");
        std::fs::write(source.join("vmstate.snap"), b"snapshot").expect("snapshot");
        std::fs::write(source.join("mem.bin"), b"memory").expect("memory");
        std::fs::write(source.join("rootfs.ext4"), b"rootfs").expect("rootfs");

        let metadata =
            copy_template_transactional(&source, &root, "runtime-base", "base runtime template")
                .expect("import template");
        let destination = root.join("runtime-base");

        assert_eq!(metadata["name"], "runtime-base");
        assert_eq!(metadata["description"], "base runtime template");
        assert_eq!(metadata["rootfs_size"], 8_u64 * 1024 * 1024 * 1024);
        assert_eq!(metadata["memory_size"], 4_u64 * 1024 * 1024 * 1024);
        assert_eq!(
            std::fs::read(destination.join("mem.bin")).expect("canonical memory"),
            b"memory"
        );
        assert_eq!(
            std::fs::read(destination.join("rootfs.ext4")).expect("canonical rootfs"),
            b"rootfs"
        );
        assert_eq!(
            read_json(&destination.join("template.json")).expect("published metadata"),
            metadata
        );
    }

    #[test]
    fn import_rejects_path_components_and_cleans_staging() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source = temp.path().join("source");
        let root = temp.path().join("templates");
        std::fs::create_dir(&source).expect("source directory");
        std::fs::write(source.join("vmstate.snap"), b"snapshot").expect("snapshot");
        std::fs::write(source.join("mem.bin"), b"memory").expect("memory");

        assert!(validate_name("../runtime", "template").is_err());
        assert!(copy_template_transactional(&source, &root, "runtime", "").is_err());
        assert!(!root.join("runtime").exists());
        assert_eq!(std::fs::read_dir(&root).expect("template root").count(), 0);
    }
}
