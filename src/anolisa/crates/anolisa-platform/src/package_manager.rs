//! Package manager abstraction (dnf/apt/zypper).

use std::process::Command;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum PkgError {
    #[error("package manager command failed: {0}")]
    CommandFailed(String),
    #[error("unsupported package base: {0}")]
    Unsupported(String),
}

/// Abstraction over system package managers.
pub trait PackageManager {
    fn install(&self, packages: &[&str]) -> Result<(), PkgError>;
    fn remove(&self, packages: &[&str]) -> Result<(), PkgError>;
    fn is_installed(&self, package: &str) -> bool;
}

/// DNF/YUM backend for RPM-based distros (Anolis, ALINUX, RHEL, Fedora).
pub struct DnfBackend;

/// APT backend for DEB-based distros (Ubuntu, Debian).
pub struct AptBackend;

impl PackageManager for DnfBackend {
    fn install(&self, packages: &[&str]) -> Result<(), PkgError> {
        if packages.is_empty() {
            return Ok(());
        }
        let status = Command::new("dnf")
            .args(["install", "-y", "--setopt=install_weak_deps=False"])
            .args(packages)
            .status()
            .map_err(|e| PkgError::CommandFailed(format!("failed to spawn dnf: {e}")))?;
        if !status.success() {
            return Err(PkgError::CommandFailed(format!(
                "dnf install exited with {status}"
            )));
        }
        Ok(())
    }

    fn remove(&self, packages: &[&str]) -> Result<(), PkgError> {
        if packages.is_empty() {
            return Ok(());
        }
        let status = Command::new("dnf")
            .args(["remove", "-y"])
            .args(packages)
            .status()
            .map_err(|e| PkgError::CommandFailed(format!("failed to spawn dnf: {e}")))?;
        if !status.success() {
            return Err(PkgError::CommandFailed(format!(
                "dnf remove exited with {status}"
            )));
        }
        Ok(())
    }

    fn is_installed(&self, package: &str) -> bool {
        Command::new("rpm")
            .args(["-q", package])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }
}

impl PackageManager for AptBackend {
    fn install(&self, packages: &[&str]) -> Result<(), PkgError> {
        if packages.is_empty() {
            return Ok(());
        }
        let status = Command::new("apt-get")
            .args(["install", "-y", "--no-install-recommends"])
            .args(packages)
            .env("DEBIAN_FRONTEND", "noninteractive")
            .status()
            .map_err(|e| PkgError::CommandFailed(format!("failed to spawn apt-get: {e}")))?;
        if !status.success() {
            return Err(PkgError::CommandFailed(format!(
                "apt-get install exited with {status}"
            )));
        }
        Ok(())
    }

    fn remove(&self, packages: &[&str]) -> Result<(), PkgError> {
        if packages.is_empty() {
            return Ok(());
        }
        let status = Command::new("apt-get")
            .args(["remove", "-y"])
            .args(packages)
            .env("DEBIAN_FRONTEND", "noninteractive")
            .status()
            .map_err(|e| PkgError::CommandFailed(format!("failed to spawn apt-get: {e}")))?;
        if !status.success() {
            return Err(PkgError::CommandFailed(format!(
                "apt-get remove exited with {status}"
            )));
        }
        Ok(())
    }

    fn is_installed(&self, package: &str) -> bool {
        Command::new("dpkg")
            .args(["-s", package])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }
}

/// Detect the appropriate package manager for the current system.
///
/// Uses `pkg_base` from `EnvFacts` to select the backend. Falls back to
/// checking binary availability if `pkg_base` is `None`.
pub fn detect_package_manager(pkg_base: Option<&str>) -> Result<Box<dyn PackageManager>, PkgError> {
    match pkg_base {
        Some(base) if base.starts_with("anolis") || base.starts_with("alinux") => {
            Ok(Box::new(DnfBackend))
        }
        Some(base)
            if base.starts_with("rhel")
                || base.starts_with("centos")
                || base.starts_with("fedora") =>
        {
            Ok(Box::new(DnfBackend))
        }
        Some(base) if base.starts_with("ubuntu") || base.starts_with("debian") => {
            Ok(Box::new(AptBackend))
        }
        Some(base) => {
            // Fallback: try to detect from binary availability
            if command_exists("dnf") || command_exists("yum") {
                Ok(Box::new(DnfBackend))
            } else if command_exists("apt-get") {
                Ok(Box::new(AptBackend))
            } else {
                Err(PkgError::Unsupported(base.to_string()))
            }
        }
        None => {
            // No pkg_base info; probe binaries
            if command_exists("dnf") || command_exists("yum") {
                Ok(Box::new(DnfBackend))
            } else if command_exists("apt-get") {
                Ok(Box::new(AptBackend))
            } else {
                Err(PkgError::Unsupported("unknown".to_string()))
            }
        }
    }
}

fn command_exists(cmd: &str) -> bool {
    Command::new("which")
        .arg(cmd)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}
