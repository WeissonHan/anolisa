// SPDX-License-Identifier: Apache-2.0

const RUNNER: &str = include_str!("../../../scripts/verify-blazectl-firecracker.sh");

#[test]
fn firecracker_firecracker_listener_filter_keeps_boolean_operator_on_the_awk_rule_line() {
    let filter = RUNNER
        .split("BLAZE_Firecracker_TCP_PORT=\"$TCP_PORT\"")
        .nth(1)
        .and_then(|tail| tail.split("sort >\"$RAW/$name.listeners\"").next())
        .expect("listener filter");
    assert!(
        !filter
            .lines()
            .any(|line| line.trim_start().starts_with("||")),
        "the host awk rejects a rule whose continuation starts with ||"
    );
    assert!(
        filter.contains("ENVIRON[\"BLAZE_Firecracker_WORK_ROOT\"]) ||"),
        "the listener alternatives must remain one awk rule"
    );
}

#[test]
fn firecracker_firecracker_open_fd_inventory_is_redirected_before_in_place_sort() {
    let inventory = RUNNER
        .split("while IFS= read -r pid; do")
        .nth(1)
        .and_then(|tail| tail.split("LC_ALL=C sort -o \"$RAW/$name.fds\"").next())
        .expect("open-FD inventory");
    assert!(
        inventory
            .contains("done >\"$RAW/$name.fds\" < <(awk '{print $1}' \"$RAW/$name.processes\")"),
        "open-FD rows must be captured instead of disclosed on runner stdout"
    );
}

#[test]
fn firecracker_firecracker_captures_nat_inside_blaze_network_namespaces() {
    let capture = RUNNER
        .split("capture_global_resources()")
        .nth(1)
        .and_then(|tail| tail.split("assert_live_resources()").next())
        .expect("global resource capture");
    assert!(
        capture.contains("ip netns exec \"$namespace\" iptables-save"),
        "NAT rules are owned by the Blaze network namespace"
    );
    assert!(
        !capture.contains("iptables-save >\"$RAW/$name.nat\""),
        "volatile host-global NAT must not stand in for namespaced Blaze NAT"
    );
    assert!(
        !capture.contains("-v namespace="),
        "gawk reserves namespace as a builtin keyword"
    );
    assert!(
        capture.contains("-v ns_label=\"$namespace\""),
        "namespaced NAT rows must use a portable awk label variable"
    );
}

#[test]
fn firecracker_firecracker_runner_freezes_the_full_release_cli_contract() {
    for marker in [
        "CONTRACT: backend=firecracker mock=false",
        "CONTRACT: create list exec write read checkpoint rollback checkpoints",
        "CONTRACT: prune-checkpoints hibernate resume pool-status cleanup-devices kill kill-all",
        "CONTRACT: uds tcp text json binary guest-nonzero daemon-error connection-unavailable",
        "CONTRACT: firecracker netns tap-veth nat uds storage runtime cli-process open-fd listener delta",
        "CONTRACT: candidate binary runtime-input command exit assertion confidentiality hashes",
    ] {
        assert!(
            RUNNER.contains(marker),
            "missing Firecracker marker: {marker}"
        );
    }

    for command in [
        "create",
        "list",
        "exec",
        "write",
        "read",
        "checkpoint",
        "rollback",
        "checkpoints",
        "prune-checkpoints",
        "hibernate",
        "resume",
        "pool-status",
        "cleanup-devices",
        "kill",
        "kill --all",
    ] {
        assert!(
            RUNNER.contains(&format!("record_assertion {command:?}")),
            "missing asserted Firecracker command: {command}"
        );
    }

    for requirement in [
        "/dev/kvm",
        "must run as root",
        "diff --quiet",
        "diff --cached --quiet",
        "sha256sum",
        "iptables-save",
        "ip netns list",
        "ip -o link show",
        "/proc/$pid/fd",
        "live listener delta",
        "final open-FD delta is nonzero",
        "verification-summary.json",
        "SHA256SUMS",
        "EXPECTED_COMMAND_ASSERTIONS=51",
        "MAX_DAEMON_LOG_BYTES=16777216",
        "restricted-patterns",
        "restricted_patterns_sha256",
        "grep -aEif",
        "CONFIDENTIALITY_SUMMARY=\"$WORK_ROOT/confidentiality-summary.json\"",
        "VERIFICATION_SUMMARY=\"$WORK_ROOT/verification-summary.json\"",
        "SHA256_SUMS=\"$WORK_ROOT/SHA256SUMS\"",
        "SCANNED_FILES=$((SCANNED_FILES + 3))",
    ] {
        assert!(
            RUNNER.contains(requirement),
            "missing Firecracker preflight/evidence requirement: {requirement}"
        );
    }

    assert!(
        !RUNNER.contains("eval "),
        "Firecracker runner must pass guest commands as argv data without local eval"
    );

    let restricted_scan = RUNNER
        .rfind("reviewed restricted pattern matched runtime/evidence output")
        .expect("final restricted scan");
    let evidence_publish = RUNNER
        .rfind("install -m 0600 \"$CONFIDENTIALITY_SUMMARY\"")
        .expect("evidence publication");
    let checksum_verify = RUNNER
        .rfind("sha256sum -c SHA256SUMS")
        .expect("evidence checksum verification");
    let finalized = RUNNER.rfind("FINALIZED=1").expect("finalization marker");
    assert!(
        restricted_scan < evidence_publish
            && evidence_publish < checksum_verify
            && checksum_verify < finalized,
        "Firecracker PASS evidence must be scanned, published, checksummed and only then finalized"
    );
}
