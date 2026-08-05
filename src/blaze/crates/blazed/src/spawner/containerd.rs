// SPDX-License-Identifier: Apache-2.0
//! Image-provided sandbox filesystems via containerd.
//!
//! Only containerd's image and snapshot services are used. blaze still
//! launches and owns the sandbox process itself, which is what keeps the
//! lifecycle (pause, resume, checkpoint, and in-place restore) under blaze's
//! control rather than a shim's.
//!
//! containerd is driven through `ctr` rather than its gRPC API: the three
//! operations needed here are one command each, and `ctr images mount`
//! already combines "prepare a writable snapshot" with "mount it", which is
//! otherwise a snapshotter-specific assembly job.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use blaze_core::config::ContainerdSection;
use blaze_core::{BlazeError, Result};
use tokio::process::Command;

/// Registry pulls are the only unbounded operation here.
const PULL_TIMEOUT: Duration = Duration::from_secs(300);
const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(60);
/// OCI reference grammar caps repository plus tag well below this; the limit
/// exists so an absurd argument fails before reaching containerd.
const MAX_REF_LEN: usize = 255;

/// Image and snapshot operations against one containerd namespace.
#[derive(Debug, Clone)]
pub struct ContainerdImages {
    ctr_path: PathBuf,
    address: String,
    namespace: String,
    snapshotter: String,
}

impl ContainerdImages {
    /// Build a client from configuration, or `None` when containerd is not
    /// configured and backends should keep using their static base image.
    pub fn from_config(section: &ContainerdSection) -> Option<Self> {
        if !section.enabled() {
            return None;
        }
        Some(Self {
            ctr_path: section.ctr_path.clone(),
            address: section.address.clone(),
            namespace: section.namespace.clone(),
            snapshotter: section.snapshotter.clone(),
        })
    }

    fn base_argv(&self) -> Vec<OsString> {
        vec![
            OsString::from("--address"),
            OsString::from(&self.address),
            OsString::from("--namespace"),
            OsString::from(&self.namespace),
        ]
    }

    fn snapshotter_argv(&self) -> Vec<OsString> {
        if self.snapshotter.is_empty() {
            Vec::new()
        } else {
            vec![
                OsString::from("--snapshotter"),
                OsString::from(&self.snapshotter),
            ]
        }
    }

    fn list_argv(&self, image: &str) -> Vec<OsString> {
        let mut argv = self.base_argv();
        argv.push(OsString::from("images"));
        argv.push(OsString::from("ls"));
        argv.push(OsString::from("-q"));
        argv.push(OsString::from(format!("name=={image}")));
        argv
    }

    fn pull_argv(&self, image: &str) -> Vec<OsString> {
        let mut argv = self.base_argv();
        argv.push(OsString::from("images"));
        argv.push(OsString::from("pull"));
        argv.extend(self.snapshotter_argv());
        argv.push(OsString::from(image));
        argv
    }

    fn mount_argv(&self, image: &str, target: &Path) -> Vec<OsString> {
        let mut argv = self.base_argv();
        argv.push(OsString::from("images"));
        argv.push(OsString::from("mount"));
        argv.extend(self.snapshotter_argv());
        argv.push(OsString::from("--rw"));
        argv.push(OsString::from(image));
        argv.push(target.as_os_str().to_owned());
        argv
    }

    fn unmount_argv(&self, target: &Path) -> Vec<OsString> {
        let mut argv = self.base_argv();
        argv.push(OsString::from("images"));
        argv.push(OsString::from("unmount"));
        argv.push(target.as_os_str().to_owned());
        argv
    }

    /// `ctr images mount` names the snapshot after its mount target, so the
    /// same path addresses the snapshot when unmounting left it behind.
    fn snapshot_rm_argv(&self, target: &Path) -> Vec<OsString> {
        let mut argv = self.base_argv();
        argv.push(OsString::from("snapshots"));
        argv.extend(self.snapshotter_argv());
        argv.push(OsString::from("rm"));
        argv.push(target.as_os_str().to_owned());
        argv
    }

    /// Pull `image` unless the namespace already has it.
    pub async fn ensure_image(&self, image: &str) -> Result<()> {
        validate_ref(image)?;
        let listed = self
            .run(self.list_argv(image), Duration::from_secs(30), "images ls")
            .await?;
        if !String::from_utf8_lossy(&listed).trim().is_empty() {
            return Ok(());
        }
        self.run(self.pull_argv(image), PULL_TIMEOUT, "images pull")
            .await?;
        Ok(())
    }

    /// Mount a writable snapshot of `image` at `target`.
    pub async fn prepare_rootfs(&self, image: &str, target: &Path) -> Result<()> {
        validate_ref(image)?;
        tokio::fs::create_dir_all(target).await?;
        self.run(
            self.mount_argv(image, target),
            SNAPSHOT_TIMEOUT,
            "images mount",
        )
        .await?;
        Ok(())
    }

    /// Unmount `target` and drop the snapshot behind it.
    ///
    /// Idempotent by design: both steps are best-effort so a repeated or
    /// partially completed release still converges instead of blocking the
    /// destroy that called it.
    pub async fn release_rootfs(&self, target: &Path) {
        for (argv, what) in [
            (self.unmount_argv(target), "images unmount"),
            (self.snapshot_rm_argv(target), "snapshots rm"),
        ] {
            if let Err(error) = self.run(argv, SNAPSHOT_TIMEOUT, what).await {
                tracing::debug!(%error, target = %target.display(), "containerd {what} found nothing to release");
            }
        }
    }

    async fn run(&self, argv: Vec<OsString>, timeout: Duration, what: &str) -> Result<Vec<u8>> {
        let output = tokio::time::timeout(
            timeout,
            Command::new(&self.ctr_path)
                .args(&argv)
                .stdin(Stdio::null())
                .output(),
        )
        .await
        .map_err(|_| BlazeError::BackendError {
            msg: format!("ctr {what} timed out after {timeout:?}"),
        })??;
        if !output.status.success() {
            return Err(BlazeError::BackendError {
                msg: format!(
                    "ctr {what} failed with {}: {}",
                    output.status,
                    String::from_utf8_lossy(&output.stderr).trim()
                ),
            });
        }
        Ok(output.stdout)
    }
}

/// Reject references that `ctr` would misread.
///
/// The reference arrives from an API request body. It is passed as a single
/// argv element and never through a shell, so there is no command injection
/// surface; the real hazard is a value beginning with `-`, which `ctr` would
/// parse as one of its own flags.
pub fn validate_ref(image: &str) -> Result<()> {
    let reject = |reason: &str| {
        Err(BlazeError::BackendError {
            msg: format!("invalid image reference {image:?}: {reason}"),
        })
    };
    if image.is_empty() {
        return reject("empty");
    }
    if image.len() > MAX_REF_LEN {
        return reject("longer than 255 bytes");
    }
    if image.starts_with('-') {
        return reject("would be parsed as a ctr flag");
    }
    if !image
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b"._:/@-".contains(&b))
    {
        return reject("contains characters outside the OCI reference grammar");
    }
    Ok(())
}

/// Rendered argv, for tests that assert flag placement.
#[cfg(test)]
fn rendered(argv: &[OsString]) -> Vec<String> {
    argv.iter()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client(snapshotter: &str) -> ContainerdImages {
        ContainerdImages::from_config(&ContainerdSection {
            address: "/run/containerd/containerd.sock".to_string(),
            namespace: "blaze".to_string(),
            ctr_path: PathBuf::from("/usr/bin/ctr"),
            snapshotter: snapshotter.to_string(),
        })
        .expect("configured address enables containerd")
    }

    #[test]
    fn an_empty_address_opts_out_of_containerd() {
        assert!(
            ContainerdImages::from_config(&ContainerdSection::default()).is_some(),
            "containerd is on by default"
        );
        assert!(
            ContainerdImages::from_config(&ContainerdSection {
                address: String::new(),
                ..Default::default()
            })
            .is_none(),
            "an empty address is the opt-out"
        );
    }

    #[test]
    fn every_command_is_scoped_to_the_configured_namespace() {
        let client = client("");
        for argv in [
            client.list_argv("alpine"),
            client.pull_argv("alpine"),
            client.mount_argv("alpine", Path::new("/run/target")),
            client.unmount_argv(Path::new("/run/target")),
            client.snapshot_rm_argv(Path::new("/run/target")),
        ] {
            let argv = rendered(&argv);
            assert_eq!(argv[0], "--address");
            assert_eq!(argv[1], "/run/containerd/containerd.sock");
            assert_eq!(argv[2], "--namespace");
            assert_eq!(argv[3], "blaze");
        }
    }

    #[test]
    fn mounting_asks_for_a_writable_layer_at_the_target() {
        let argv = rendered(&client("").mount_argv("alpine:3", Path::new("/run/x/bundle/rootfs")));
        assert!(argv.contains(&"--rw".to_string()));
        assert_eq!(argv[argv.len() - 2], "alpine:3");
        assert_eq!(argv[argv.len() - 1], "/run/x/bundle/rootfs");
        assert!(
            !argv.contains(&"--snapshotter".to_string()),
            "an empty snapshotter must leave containerd on its own default"
        );
    }

    #[test]
    fn an_explicit_snapshotter_is_forwarded() {
        let argv = rendered(&client("overlayfs").mount_argv("alpine", Path::new("/run/target")));
        let flag = argv
            .iter()
            .position(|arg| arg == "--snapshotter")
            .expect("snapshotter flag");
        assert_eq!(argv[flag + 1], "overlayfs");
    }

    /// The snapshot key is the mount target, so releasing must address the
    /// same path it mounted.
    #[test]
    fn releasing_addresses_the_snapshot_by_its_mount_target() {
        let target = Path::new("/var/lib/blaze/abc/bundle/rootfs");
        let argv = rendered(&client("").snapshot_rm_argv(target));
        assert_eq!(argv[argv.len() - 2], "rm");
        assert_eq!(argv[argv.len() - 1], target.to_string_lossy());
    }

    #[test]
    fn references_that_ctr_would_misread_are_rejected() {
        for bad in [
            "",
            "-rm",
            "alpine latest",
            "alpine;rm -rf /",
            "alpine$(id)",
            "alpine|tee",
        ] {
            let error = validate_ref(bad).expect_err(bad);
            assert!(error.to_string().contains("invalid image reference"));
        }
        assert!(validate_ref(&"a".repeat(MAX_REF_LEN + 1)).is_err());
    }

    #[test]
    fn ordinary_references_are_accepted() {
        for good in [
            "alpine",
            "alpine:3.20",
            "docker.io/library/alpine:latest",
            "registry.example.com:5000/team/app:v1.2.3",
            "docker.io/library/alpine@sha256:0123456789abcdef",
        ] {
            validate_ref(good).expect(good);
        }
    }
}
