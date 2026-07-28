// SPDX-License-Identifier: Apache-2.0

const RUNNER: &str = include_str!("../../../scripts/verify-blazectl-rpm.sh");

#[test]
fn rpm_runner_freezes_build_install_upgrade_and_uninstall_contract() {
    for marker in [
        "CONTRACT: candidate source-rpm binary-rpm release blazed blazectl",
        "CONTRACT: runtime build-only x86_64 aarch64",
        "CONTRACT: fresh-install help version uds owner group mode service",
        "CONTRACT: upgrade config-preserved binary-version-match",
        "CONTRACT: uninstall cli-removed user-state-preserved",
        "CONTRACT: raw-scan publish-byte-match hashes finalize-last",
    ] {
        assert!(RUNNER.contains(marker), "missing RPM marker: {marker}");
    }

    for requirement in [
        "BLAZE_RPM_DISPOSABLE_HOST=YES",
        "real RPM package acceptance requires Linux",
        "runtime mode requires root",
        "candidate does not match source HEAD",
        "source worktree is not clean",
        "candidate must be a full lowercase SHA",
        "runtime mode requires a reviewed previous-rpm",
        "no blaze package may be installed before runtime mode",
        "git -C \"$SOURCE_ROOT\" archive",
        "gzip -n",
        "cargo vendor --quiet --locked",
        "blaze-$VERSION-vendor.tar.gz",
        "CARGO_NET_OFFLINE=true",
        "source RPM vendor archive differs from staged dependencies",
        "vendor_archive_sha256",
        "rpmbuild -ba",
        "--define \"_buildhost build.invalid\"",
        "--define \"_packager Blaze Package Builder\"",
        "rpm -qip",
        "rpm -qp --scripts",
        "rpm -K",
        "rpm -ivh",
        "rpm -Uvh",
        "rpm -e blaze",
        "systemctl start blazed.service",
        "systemctl stop blazed.service",
        "systemd-tmpfiles --create /usr/lib/tmpfiles.d/blaze.conf",
        "/usr/bin/blazectl --help",
        "/usr/bin/blazectl --version",
        "/usr/bin/blazectl version",
        "/usr/libexec/anolisa/blazed --version",
        "/run/blaze/api.sock",
        "config-preservation-sentinel",
        "user-state-preservation-sentinel",
        "restricted-patterns",
        "grep -aEif",
        "verification-summary.json",
        "SHA256SUMS",
        "evidence_level: \"package-build-only\"",
        "evidence_level: \"package-runtime\"",
    ] {
        assert!(
            RUNNER.contains(requirement),
            "missing RPM preflight/runtime/evidence requirement: {requirement}"
        );
    }

    for forbidden in [
        "--nodeps",
        "eval ",
        "dnf ",
        "yum ",
        "package_complete: true",
    ] {
        assert!(
            !RUNNER.contains(forbidden),
            "RPM runner contains forbidden behavior: {forbidden}"
        );
    }

    let restricted_scan = RUNNER
        .rfind("reviewed restricted pattern matched package/runtime/evidence output")
        .expect("final restricted scan");
    let evidence_publish = RUNNER
        .rfind("install -m 0600 \"$CONFIDENTIALITY_SUMMARY\"")
        .expect("evidence publication");
    let checksum_verify = RUNNER
        .rfind("sha256sum -c SHA256SUMS")
        .expect("evidence checksum verification");
    let final_cleanup = RUNNER
        .rfind("cleanup_runner_state || die \"final disposable-host cleanup failed\"")
        .expect("final disposable-host cleanup");
    let finalized = RUNNER.rfind("FINALIZED=1").expect("finalization marker");
    assert!(
        restricted_scan < evidence_publish
            && evidence_publish < checksum_verify
            && checksum_verify < final_cleanup
            && final_cleanup < finalized,
        "RPM PASS evidence must be scanned, published, checksummed and only then finalized"
    );
}

#[test]
fn rpm_runner_scans_complete_binary_and_source_package_headers() {
    for requirement in [
        "MAX_PACKAGE_METADATA_BYTES",
        "rpm -qp --xml <candidate-rpm>",
        "rpm -qp --xml <source-rpm>",
        "binary RPM complete header metadata is available for confidentiality review",
        "source RPM complete header metadata is available for confidentiality review",
        "complete_header_metadata_scanned: true",
        "binary_rpm_header_scanned: true",
        "source_rpm_header_scanned: true",
        "package_metadata_scanned: true",
    ] {
        assert!(
            RUNNER.contains(requirement),
            "missing complete package metadata scan requirement: {requirement}"
        );
    }
}

#[test]
fn rpm_runner_identifies_source_packages_portably() {
    assert!(RUNNER.contains("'%{SOURCEPACKAGE}'"));
    assert!(RUNNER.contains("source RPM header is not marked as a source package"));
    assert!(RUNNER.contains("src | \"$ARCH\""));
    assert!(!RUNNER.contains("source RPM architecture differs\""));
}

#[test]
fn rpm_runner_keeps_awk_boolean_operators_on_the_previous_rule_line() {
    for path in ["/usr/bin/blazectl", "/usr/libexec/anolisa/blazed"] {
        let portable_rule = format!("$1 == \"{path}\" &&\n    $2 ==");
        assert!(
            RUNNER.contains(&portable_rule),
            "ownership rule for {path} must parse with portable awk"
        );
    }
    assert!(!RUNNER.contains("\n    && $2 =="));
}
