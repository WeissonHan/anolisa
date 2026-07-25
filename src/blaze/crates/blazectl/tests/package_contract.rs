// SPDX-License-Identifier: Apache-2.0

const SPEC: &str = include_str!("../../../dist/blaze.spec");
const TMPFILES: &str = include_str!("../../../dist/tmpfiles-blaze.conf");

#[test]
fn package_recreates_the_volatile_runtime_directory() {
    assert!(SPEC.contains("BuildRequires:  systemd-rpm-macros"));
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
    assert!(SPEC.contains("%dir /var/lib/blaze"));
    assert!(SPEC.contains("%config(noreplace) %{_sysconfdir}/anolisa/blaze/config.toml"));
}
