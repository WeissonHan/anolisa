// SPDX-License-Identifier: Apache-2.0

const SPEC: &str = include_str!("../../../dist/blaze.spec");
const SERVICE: &str = include_str!("../../../dist/blazed.service");
const TMPFILES: &str = include_str!("../../../dist/tmpfiles-blaze.conf");
const DAEMON: &str = include_str!("../../blazed/src/daemon.rs");
const COMPONENT_MANIFEST: &str = include_str!("../../../.anolisa/component.toml");
const RELEASE_MANIFEST: &str = include_str!("../../../manifests/blaze.toml");
const README: &str = include_str!("../../../README.md");
const README_ZH: &str = include_str!("../../../README_zh.md");

#[test]
fn rpm_builds_and_owns_the_daemon_and_client_contract() {
    let version_line = format!("Version:        {}", env!("CARGO_PKG_VERSION"));
    assert!(SPEC.lines().any(|line| line == version_line.as_str()));
    assert!(SPEC.contains("cargo build --workspace --release --offline --locked"));

    assert_eq!(
        line_count("install -Dm755 target/release/blazed "),
        1,
        "blazed must have one install source"
    );
    assert_eq!(
        line_count("install -Dm755 target/release/blazectl "),
        1,
        "blazectl must have one install source"
    );
    assert!(SPEC.contains(
        "install -Dm755 target/release/blazed %{buildroot}%{_libexecdir}/anolisa/blazed"
    ));
    assert!(
        SPEC.contains("install -Dm755 target/release/blazectl %{buildroot}%{_bindir}/blazectl")
    );

    assert!(SPEC.contains("%attr(0755,root,root) %{_libexecdir}/anolisa/blazed"));
    assert!(SPEC.contains("%attr(0755,root,root) %{_bindir}/blazectl"));
    assert_eq!(
        line_count("%attr(0755,root,root) %{_bindir}/blazectl"),
        1,
        "the RPM file list must own exactly one client path"
    );
}

#[test]
fn rpm_build_uses_a_vendored_offline_dependency_source() {
    assert!(SPEC.contains("Source1:        %{name}-%{version}-vendor.tar.gz"));
    assert!(SPEC.contains("%setup -q -T -D -a 1"));
    assert!(SPEC.contains("[source.crates-io]"));
    assert!(SPEC.contains("replace-with = \"vendored-sources\""));
    assert!(SPEC.contains("[source.vendored-sources]"));
    assert!(SPEC.contains("directory = \"vendor\""));
    assert!(SPEC.contains("cargo build --workspace --release --offline --locked"));
}

#[test]
fn package_preserves_daemon_and_state_boundaries() {
    assert!(SERVICE.contains(
        "ExecStart=/usr/libexec/anolisa/blazed daemon start --config /etc/anolisa/blaze/config.toml"
    ));
    assert!(!SERVICE.contains("blazectl"));
    assert!(SPEC.contains("BuildRequires:  systemd-rpm-macros"));
    assert!(SPEC.contains("%systemd_post blazed.service"));
    assert!(SPEC.contains("%systemd_preun blazed.service"));
    assert!(SPEC.contains("%systemd_postun blazed.service"));

    assert!(SPEC.contains("%config(noreplace) %{_sysconfdir}/anolisa/blaze/config.toml"));
    assert!(SPEC.contains("%dir /var/lib/blaze"));
    assert_eq!(
        TMPFILES
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .collect::<Vec<_>>(),
        vec!["d /run/blaze 0755 root root -"]
    );
    assert!(SPEC.contains("install -d -m 0755 %{buildroot}%{_tmpfilesdir}"));
    assert!(SPEC.contains(
        "install -Dm644 dist/tmpfiles-blaze.conf %{buildroot}%{_tmpfilesdir}/blaze.conf"
    ));
    assert!(
        SPEC.lines()
            .any(|line| line == "%{_tmpfilesdir}/blaze.conf")
    );
    assert!(SPEC.contains("%tmpfiles_create %{_tmpfilesdir}/blaze.conf"));
    assert!(!SPEC.lines().any(|line| line == "%dir /run/blaze"));
    assert!(DAEMON.contains("Permissions::from_mode(0o660)"));
    for line in SPEC
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("rm "))
    {
        assert_eq!(line, "rm -rf $RPM_BUILD_ROOT");
    }
}

#[test]
fn release_metadata_matches_the_client_version() {
    let version_assignment = format!("version = \"{}\"", env!("CARGO_PKG_VERSION"));
    let health_version = format!("\"version\": \"{}\"", env!("CARGO_PKG_VERSION"));

    assert!(
        COMPONENT_MANIFEST
            .lines()
            .any(|line| line == version_assignment.as_str())
    );
    assert!(
        RELEASE_MANIFEST
            .lines()
            .any(|line| line == version_assignment.as_str())
    );
    assert!(README.contains(&health_version));
    assert!(README_ZH.contains(&health_version));
}

#[test]
fn package_metadata_contains_no_identity_changelog() {
    assert!(SPEC.contains("Packager:        Blaze Package Builder"));
    assert!(!SPEC.contains("%changelog"));
    assert!(!SPEC.contains('@'));
}

fn line_count(needle: &str) -> usize {
    SPEC.lines().filter(|line| line.contains(needle)).count()
}
