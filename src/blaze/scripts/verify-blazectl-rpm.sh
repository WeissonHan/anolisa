#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Explicit Linux RPM acceptance for the final Blaze source candidate.
# Runtime mode mutates package state and therefore requires an empty,
# disposable system explicitly acknowledged by the caller.
#
# CONTRACT: candidate source-rpm binary-rpm release blazed blazectl
# CONTRACT: runtime build-only x86_64 aarch64
# CONTRACT: fresh-install help version uds owner group mode service
# CONTRACT: upgrade config-preserved binary-version-match
# CONTRACT: uninstall cli-removed user-state-preserved
# CONTRACT: raw-scan publish-byte-match hashes finalize-last

set -euo pipefail
umask 077

MAX_BUILD_LOG_BYTES=33554432
MAX_PACKAGE_METADATA_BYTES=8388608
MAX_RUNTIME_LOG_BYTES=8388608

usage() {
    cat >&2 <<'EOF'
usage: verify-blazectl-rpm.sh \
  <source-root> <candidate-sha> <runtime|build-only> <previous-rpm|-> \
  <restricted-patterns> <evidence-root>

Runtime mode requires a reviewed previous RPM for upgrade acceptance, root,
systemd as the active service manager, an initially uninstalled Blaze package,
empty package-owned paths and BLAZE_RPM_DISPOSABLE_HOST=YES.

Build-only mode accepts "-" for previous-rpm and proves only the source/binary
RPM build and package payload on the current supported architecture. It never
claims cross-architecture runtime acceptance. Runtime mode proves the install
lifecycle on one architecture; complete package acceptance still requires
x86_64/aarch64 evidence aggregation.
EOF
    exit 2
}

die() {
    printf 'blazectl RPM: %s\n' "$1" >&2
    exit 1
}

require_command() {
    command -v "$1" >/dev/null 2>&1 \
        || die "required command is unavailable: $1"
}

hash_file() {
    sha256sum "$1" | awk '{print $1}'
}

[[ $# -eq 6 ]] || usage

SOURCE_ROOT_ARG=$1
CANDIDATE=$2
MODE=$3
PREVIOUS_RPM_ARG=$4
RESTRICTED_PATTERNS_ARG=$5
EVIDENCE_ARG=$6

for command in awk basename cargo cat cmp cpio cut dirname env find git grep \
    gzip id install jq mktemp realpath rm rmdir rpm rpm2cpio rpmbuild \
    sha256sum sleep sort stat tar tr uname uniq wc; do
    require_command "$command"
done

[[ "$(uname -s)" == "Linux" ]] \
    || die "real RPM package acceptance requires Linux"
ARCH=$(uname -m)
case "$ARCH" in
    x86_64 | aarch64) ;;
    *) die "package architecture must be x86_64 or aarch64" ;;
esac
case "$MODE" in
    runtime | build-only) ;;
    *) die "mode must be runtime or build-only" ;;
esac

[[ "$CANDIDATE" =~ ^[0-9a-f]{40}$ ]] \
    || die "candidate must be a full lowercase SHA"
SOURCE_ROOT=$(realpath "$SOURCE_ROOT_ARG")
RESTRICTED_PATTERNS=$(realpath "$RESTRICTED_PATTERNS_ARG")
[[ -d "$SOURCE_ROOT/.git" || -f "$SOURCE_ROOT/.git" ]] \
    || die "source-root is not a Git worktree"
[[ "$(git -C "$SOURCE_ROOT" rev-parse HEAD)" == "$CANDIDATE" ]] \
    || die "candidate does not match source HEAD"
git -C "$SOURCE_ROOT" diff --quiet \
    || die "source worktree has unstaged changes"
git -C "$SOURCE_ROOT" diff --cached --quiet \
    || die "source worktree has staged changes"
[[ -z "$(git -C "$SOURCE_ROOT" status --porcelain=v1)" ]] \
    || die "source worktree is not clean"
[[ -f "$RESTRICTED_PATTERNS" && -s "$RESTRICTED_PATTERNS" ]] \
    || die "restricted-patterns must contain reviewed expressions"

SPEC_SOURCE="$SOURCE_ROOT/src/blaze/dist/blaze.spec"
[[ -f "$SPEC_SOURCE" ]] || die "candidate RPM spec is missing"
VERSION_LINES=$(awk '$1 == "Version:" {count++} END {print count+0}' "$SPEC_SOURCE")
[[ "$VERSION_LINES" -eq 1 ]] || die "RPM spec must contain exactly one Version field"
VERSION=$(awk '$1 == "Version:" {print $2}' "$SPEC_SOURCE")
[[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] \
    || die "RPM version must be a stable semantic version"

PREVIOUS_RPM=
if [[ "$MODE" == "runtime" ]]; then
    for command in journalctl seq systemctl systemd-tmpfiles; do
        require_command "$command"
    done
    [[ "$(id -u)" -eq 0 ]] || die "runtime mode requires root"
    [[ "${BLAZE_RPM_DISPOSABLE_HOST:-}" == "YES" ]] \
        || die "runtime mode requires BLAZE_RPM_DISPOSABLE_HOST=YES"
    [[ "$PREVIOUS_RPM_ARG" != "-" ]] \
        || die "runtime mode requires a reviewed previous-rpm"
    PREVIOUS_RPM=$(realpath "$PREVIOUS_RPM_ARG")
    [[ -f "$PREVIOUS_RPM" ]] \
        || die "runtime mode requires a reviewed previous-rpm"
    [[ -d /run/systemd/system ]] \
        || die "runtime mode requires systemd as the active service manager"
    if rpm -q blaze >/dev/null 2>&1; then
        die "no blaze package may be installed before runtime mode"
    fi
    if systemctl is-active --quiet blazed.service; then
        die "blazed.service must be inactive before runtime mode"
    fi
    for path in \
        /usr/bin/blazectl \
        /usr/libexec/anolisa/blazed \
        /etc/anolisa/blaze \
        /var/lib/blaze \
        /run/blaze \
        /usr/lib/systemd/system/blazed.service \
        /usr/lib/tmpfiles.d/blaze.conf \
        /etc/systemd/system/multi-user.target.wants/blazed.service; do
        [[ ! -e "$path" ]] \
            || die "runtime mode requires initially absent package paths"
    done
else
    [[ "$PREVIOUS_RPM_ARG" == "-" ]] \
        || die "build-only mode requires '-' for previous-rpm"
fi

EVIDENCE_PARENT=$(realpath "$(dirname "$EVIDENCE_ARG")")
EVIDENCE_NAME=$(basename "$EVIDENCE_ARG")
[[ "$EVIDENCE_NAME" =~ ^[A-Za-z0-9._-]+$ ]] \
    || die "evidence directory name must be non-sensitive and portable"
EVIDENCE_ROOT="$EVIDENCE_PARENT/$EVIDENCE_NAME"
[[ ! -e "$EVIDENCE_ROOT" ]] || die "evidence root already exists"

WORK_ROOT=$(mktemp -d /tmp/blaze-rpm.XXXXXX)
[[ "$WORK_ROOT" == /tmp/blaze-rpm.* ]] \
    || die "unexpected temporary work root"
: >"$WORK_ROOT/.blaze-rpm-owned"

RAW="$WORK_ROOT/raw"
TOPDIR="$WORK_ROOT/rpmbuild"
EXTRACT="$WORK_ROOT/extracted"
SOURCE_EXTRACT="$WORK_ROOT/source-rpm"
VENDOR_STAGE="$WORK_ROOT/vendor-stage"
ACTIVE_PATTERNS="$WORK_ROOT/restricted.patterns"
SOURCE_ARCHIVE="$TOPDIR/SOURCES/blaze-$VERSION.tar.gz"
VENDOR_ARCHIVE="$TOPDIR/SOURCES/blaze-$VERSION-vendor.tar.gz"
SPEC_COPY="$TOPDIR/SPECS/blaze.spec"
COMMAND_MATRIX="$EVIDENCE_ROOT/command-matrix.tsv"
PACKAGE_FILES="$EVIDENCE_ROOT/package-files.tsv"
CONFIG_PATH=/etc/anolisa/blaze/config.toml
STATE_SENTINEL=/var/lib/blaze/.blaze-rpm-user-state-preservation-sentinel
CONFIG_SENTINEL="# blaze-rpm-config-preservation-sentinel"
SENTINEL_EXPECTED="$WORK_ROOT/user-state.expected"
FINALIZED=0
ACTION_INDEX=0
RUN_STDOUT=
RUN_STDERR=
RUN_EXIT=
CANDIDATE_RPM=
SOURCE_RPM=
PACKAGED_CONFIG=
PACKAGED_BLAZECTL_SHA=
PACKAGED_BLAZED_SHA=

cleanup_known_sidecars() {
    local sidecar
    for sidecar in "$CONFIG_PATH.rpmnew" "$CONFIG_PATH.rpmsave"; do
        [[ -e "$sidecar" ]] || continue
        if [[ -n "$PACKAGED_CONFIG" && -f "$PACKAGED_CONFIG" ]] \
            && cmp -s "$sidecar" "$PACKAGED_CONFIG"; then
            rm -f -- "$sidecar"
        elif grep -F "$CONFIG_SENTINEL" "$sidecar" >/dev/null 2>&1; then
            rm -f -- "$sidecar"
        else
            return 1
        fi
    done
}

cleanup_runner_state() {
    local cleanup_status=0
    local path
    if [[ "$MODE" == "runtime" ]]; then
        systemctl stop blazed.service >/dev/null 2>&1 || true
        systemctl disable blazed.service >/dev/null 2>&1 || true
        if [[ -n "$PACKAGED_CONFIG" && -f "$PACKAGED_CONFIG" \
            && -f "$CONFIG_PATH" ]]; then
            install -m 0644 "$PACKAGED_CONFIG" "$CONFIG_PATH" \
                >/dev/null 2>&1 || cleanup_status=1
        fi
        cleanup_known_sidecars || cleanup_status=1
        if rpm -q blaze >/dev/null 2>&1; then
            rpm -e blaze >/dev/null 2>&1 || cleanup_status=1
        fi
        if [[ -f "$STATE_SENTINEL" ]]; then
            if cmp -s "$STATE_SENTINEL" "$SENTINEL_EXPECTED"; then
                rm -f -- "$STATE_SENTINEL"
            else
                cleanup_status=1
            fi
        fi
        cleanup_known_sidecars || cleanup_status=1
        for path in /etc/anolisa/blaze /var/lib/blaze /run/blaze; do
            if [[ -d "$path" ]]; then
                find "$path" -depth -type d -empty -delete \
                    >/dev/null 2>&1 || cleanup_status=1
            fi
            [[ ! -e "$path" ]] || cleanup_status=1
        done
        systemctl daemon-reload >/dev/null 2>&1 || cleanup_status=1
    fi
    return "$cleanup_status"
}

cleanup() {
    local exit_code=$?
    local failed_line=${BASH_LINENO[0]:-0}
    local cleanup_status=0
    set +e
    cleanup_runner_state || cleanup_status=1
    if [[ "$exit_code" -eq 0 && "$cleanup_status" -ne 0 ]]; then
        exit_code=1
        FINALIZED=0
    fi
    if [[ "$FINALIZED" -eq 0 ]]; then
        jq -n \
            --arg candidate "$CANDIDATE" \
            --arg mode "$MODE" \
            --argjson failed_line "$failed_line" \
            '{
              result: "FAIL",
              candidate: $candidate,
              mode: $mode,
              failed_line: $failed_line
            }' >"$EVIDENCE_ROOT/verification-summary.json"
    fi
    if [[ "$WORK_ROOT" == /tmp/blaze-rpm.* \
        && -d "$WORK_ROOT" && ! -L "$WORK_ROOT" \
        && -f "$WORK_ROOT/.blaze-rpm-owned" ]]; then
        rm -rf -- "$WORK_ROOT"
    fi
    trap - EXIT INT TERM
    exit "$exit_code"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

install -d -m 0700 "$EVIDENCE_ROOT" "$RAW" "$TOPDIR/BUILD" \
    "$TOPDIR/BUILDROOT" "$TOPDIR/RPMS" "$TOPDIR/SOURCES" "$TOPDIR/SPECS" \
    "$TOPDIR/SRPMS" "$EXTRACT" "$SOURCE_EXTRACT" "$VENDOR_STAGE"
awk 'NF && $0 !~ /^[[:space:]]*#/' "$RESTRICTED_PATTERNS" \
    >"$ACTIVE_PATTERNS"
[[ -s "$ACTIVE_PATTERNS" ]] \
    || die "restricted-patterns has no active reviewed expressions"
set +e
grep -Eif "$ACTIVE_PATTERNS" /dev/null >/dev/null 2>&1
PATTERN_STATUS=$?
set -e
[[ "$PATTERN_STATUS" -eq 1 ]] \
    || die "restricted-patterns contains an invalid expression"
printf 'user-state-preservation-sentinel\n' >"$SENTINEL_EXPECTED"

printf 'index\toperation\tredacted_argv\texit\tassertion\n' >"$COMMAND_MATRIX"

record_action() {
    local operation=$1
    local safe_argv=$2
    local assertion=$3
    ACTION_INDEX=$((ACTION_INDEX + 1))
    printf '%s\t%s\t%s\t%s\t%s\n' \
        "$ACTION_INDEX" "$operation" "$safe_argv" "$RUN_EXIT" "$assertion" \
        >>"$COMMAND_MATRIX"
}

run_action() {
    local expected_exit=$1
    local operation=$2
    local safe_argv=$3
    local assertion=$4
    shift 4
    local sequence
    sequence=$(printf '%03d' "$((ACTION_INDEX + 1))")
    RUN_STDOUT="$RAW/action-$sequence.out"
    RUN_STDERR="$RAW/action-$sequence.err"
    set +e
    "$@" >"$RUN_STDOUT" 2>"$RUN_STDERR"
    RUN_EXIT=$?
    set -e
    [[ "$RUN_EXIT" -eq "$expected_exit" ]] \
        || die "$operation returned exit $RUN_EXIT, expected $expected_exit"
    record_action "$operation" "$safe_argv" "$assertion"
}

assert_empty_stderr() {
    [[ ! -s "$RUN_STDERR" ]] || die "$1 wrote unexpected stderr"
}

capture_complete_rpm_header() {
    local tag=$1
    local package_file=$2
    local safe_argv=$3
    local assertion=$4

    run_action 0 "$tag-rpm-header" "$safe_argv" "$assertion" \
        rpm -qp --xml "$package_file"
    assert_empty_stderr "$tag RPM header query"
    [[ -s "$RUN_STDOUT" ]] \
        || die "$tag RPM complete header metadata is empty"
    [[ "$(stat -c '%s' "$RUN_STDOUT")" -le "$MAX_PACKAGE_METADATA_BYTES" ]] \
        || die "$tag RPM complete header metadata exceeded the review bound"
}

assert_absent_runtime_paths() {
    local package_group
    local package_mode
    local package_owner
    local package_path
    local path
    rpm -q blaze >/dev/null 2>&1 \
        && die "blaze package remains installed after uninstall"
    systemctl is-active --quiet blazed.service \
        && die "blazed.service remains active after uninstall"
    systemctl is-enabled --quiet blazed.service \
        && die "blazed.service remains enabled after uninstall"
    while IFS=$'\t' read -r package_path package_mode package_owner package_group; do
        case "$package_mode" in
            d*) ;;
            *)
                [[ ! -e "$package_path" && ! -L "$package_path" ]] \
                    || die "package-owned non-directory remains after uninstall"
                ;;
        esac
    done <"$PACKAGE_FILES"
    for path in \
        /usr/bin/blazectl \
        /usr/libexec/anolisa/blazed \
        /usr/lib/systemd/system/blazed.service \
        /usr/lib/tmpfiles.d/blaze.conf \
        /etc/systemd/system/multi-user.target.wants/blazed.service; do
        [[ ! -e "$path" ]] || die "package-owned path remains after uninstall"
    done
}

cleanup_verified_state() {
    local path
    [[ -f "$STATE_SENTINEL" ]] \
        || die "user-state-preservation-sentinel disappeared during uninstall"
    cmp -s "$STATE_SENTINEL" "$SENTINEL_EXPECTED" \
        || die "preserved user state bytes differ"
    rm -f -- "$STATE_SENTINEL"
    cleanup_known_sidecars \
        || die "unexpected package config sidecar remains"
    for path in /etc/anolisa/blaze /var/lib/blaze /run/blaze; do
        if [[ -d "$path" ]]; then
            find "$path" -depth -type d -empty -delete
        fi
        [[ ! -e "$path" ]] \
            || die "runner-owned package/runtime residue remains"
    done
    systemctl daemon-reload >/dev/null
}

verify_installed_binaries() {
    local tag=$1
    local help_command
    local help_commands
    local version

    [[ -x /usr/bin/blazectl ]] || die "installed blazectl is not executable"
    [[ -x /usr/libexec/anolisa/blazed ]] \
        || die "installed blazed is not executable"
    [[ "$(stat -c '%U:%G:%a' /usr/bin/blazectl)" == "root:root:755" ]] \
        || die "installed blazectl owner/group/mode differs"
    [[ "$(stat -c '%U:%G:%a' /usr/libexec/anolisa/blazed)" == "root:root:755" ]] \
        || die "installed blazed owner/group/mode differs"
    [[ "$(hash_file /usr/bin/blazectl)" == "$PACKAGED_BLAZECTL_SHA" ]] \
        || die "installed blazectl bytes differ from candidate RPM"
    [[ "$(hash_file /usr/libexec/anolisa/blazed)" == "$PACKAGED_BLAZED_SHA" ]] \
        || die "installed blazed bytes differ from candidate RPM"

    run_action 0 "$tag-help" "/usr/bin/blazectl --help" \
        "installed client help succeeds before daemon start" \
        env -i PATH="$PATH" LC_ALL=C LANG=C NO_COLOR=1 CLICOLOR=0 \
        CLICOLOR_FORCE=0 TERM=dumb /usr/bin/blazectl --help
    assert_empty_stderr "$tag help"
    help_commands=$(awk '
      /^Commands:$/ {inside=1; next}
      /^Options:$/ {inside=0}
      inside && $0 ~ /^  [a-z0-9-]+[[:space:]]/ {count++}
      END {print count+0}
    ' "$RUN_STDOUT")
    [[ "$help_commands" -eq 15 ]] \
        || die "installed help does not contain exactly 14 remote plus version"
    for help_command in create exec list kill hibernate checkpoint rollback \
        checkpoints prune-checkpoints resume cleanup-devices pool-status read \
        write version; do
        grep -E "^  $help_command[[:space:]]" "$RUN_STDOUT" >/dev/null \
            || die "installed help omits an approved command"
    done

    run_action 0 "$tag-version-flag" "/usr/bin/blazectl --version" \
        "local client version flag succeeds before daemon start" \
        env -i PATH="$PATH" LC_ALL=C LANG=C /usr/bin/blazectl --version
    assert_empty_stderr "$tag version flag"
    version=$(awk 'NR == 1 {print $2}' "$RUN_STDOUT")
    [[ "$version" == "$VERSION" ]] \
        || die "installed blazectl --version differs from package version"

    run_action 0 "$tag-version-command" "/usr/bin/blazectl version" \
        "local client version command succeeds before daemon start" \
        env -i PATH="$PATH" LC_ALL=C LANG=C /usr/bin/blazectl version
    assert_empty_stderr "$tag version command"
    version=$(awk 'NR == 1 {print $2}' "$RUN_STDOUT")
    [[ "$version" == "$VERSION" ]] \
        || die "installed blazectl version differs from package version"

    run_action 0 "$tag-daemon-version" \
        "/usr/libexec/anolisa/blazed --version" \
        "installed daemon version matches client package" \
        env -i PATH="$PATH" LC_ALL=C LANG=C \
        /usr/libexec/anolisa/blazed --version
    assert_empty_stderr "$tag daemon version"
    version=$(awk 'NR == 1 {print $2}' "$RUN_STDOUT")
    [[ "$version" == "$VERSION" ]] \
        || die "installed blazed version differs from package version"
}

verify_tmpfiles_recreation() {
    local tag=$1
    [[ -d /run/blaze ]] \
        || die "package post-install did not create the runtime directory"
    [[ "$(stat -c '%U:%G:%a' /run/blaze)" == "root:root:755" ]] \
        || die "package post-install runtime directory mode differs"
    rmdir -- /run/blaze
    run_action 0 "$tag-tmpfiles-recreate" \
        "systemd-tmpfiles --create /usr/lib/tmpfiles.d/blaze.conf" \
        "tmpfiles recreates the empty runtime directory after tmpfs loss" \
        systemd-tmpfiles --create /usr/lib/tmpfiles.d/blaze.conf
    [[ "$(stat -c '%U:%G:%a' /run/blaze)" == "root:root:755" ]] \
        || die "recreated runtime directory owner/group/mode differs"
}

verify_service_and_uds() {
    local tag=$1
    local healthy=0
    local invocation
    local main_pid
    local sequence

    run_action 0 "$tag-service-start" "systemctl start blazed.service" \
        "packaged service starts the daemon" \
        systemctl start blazed.service

    sequence=$(printf '%03d' "$((ACTION_INDEX + 1))")
    RUN_STDOUT="$RAW/action-$sequence.out"
    RUN_STDERR="$RAW/action-$sequence.err"
    for _ in $(seq 1 240); do
        set +e
        env -i PATH="$PATH" LC_ALL=C LANG=C NO_COLOR=1 CLICOLOR=0 \
            CLICOLOR_FORCE=0 TERM=dumb \
            /usr/bin/blazectl --output json list \
            >"$RUN_STDOUT" 2>"$RUN_STDERR"
        RUN_EXIT=$?
        set -e
        if [[ "$RUN_EXIT" -eq 0 ]] \
            && jq -e 'type == "array"' "$RUN_STDOUT" >/dev/null; then
            healthy=1
            break
        fi
        systemctl is-active --quiet blazed.service \
            || die "packaged service exited before UDS became ready"
        sleep 0.25
    done
    [[ "$healthy" -eq 1 ]] || die "packaged UDS was not ready within 60 seconds"
    record_action "$tag-uds-list" \
        "/usr/bin/blazectl --output json list" \
        "approved root caller reaches the packaged default UDS"
    [[ -S /run/blaze/api.sock ]] || die "packaged API UDS is missing"
    [[ "$(stat -c '%U:%G:%a' /run/blaze/api.sock)" == "root:root:660" ]] \
        || die "packaged API UDS owner/group/mode differs"
    [[ "$(stat -c '%U:%G:%a' /run/blaze)" == "root:root:755" ]] \
        || die "packaged runtime directory owner/group/mode differs"

    systemctl show -p ExecStart --value blazed.service \
        >"$RAW/$tag-exec-start.txt"
    grep -F "/usr/libexec/anolisa/blazed daemon start" \
        "$RAW/$tag-exec-start.txt" >/dev/null \
        || die "packaged service does not start blazed"
    if grep -F "blazectl" "$RAW/$tag-exec-start.txt" >/dev/null; then
        die "packaged service starts blazectl"
    fi
    main_pid=$(systemctl show -p MainPID --value blazed.service)
    [[ "$main_pid" =~ ^[1-9][0-9]*$ ]] || die "packaged service has no live PID"
    invocation=$(systemctl show -p InvocationID --value blazed.service)
    [[ "$invocation" =~ ^[0-9a-f]{32}$ ]] \
        || die "packaged service invocation ID is unavailable"

    run_action 0 "$tag-service-stop" "systemctl stop blazed.service" \
        "packaged service stops cleanly" \
        systemctl stop blazed.service
    for _ in $(seq 1 120); do
        if ! systemctl is-active --quiet blazed.service \
            && [[ ! -S /run/blaze/api.sock ]]; then
            break
        fi
        sleep 0.25
    done
    systemctl is-active --quiet blazed.service \
        && die "packaged service remains active after stop"
    [[ ! -S /run/blaze/api.sock ]] \
        || die "packaged API UDS remains after service stop"
    [[ "$(systemctl show -p MainPID --value blazed.service)" == "0" ]] \
        || die "packaged daemon PID remains after service stop"
    journalctl "_SYSTEMD_INVOCATION_ID=$invocation" --no-pager \
        --output=short-monotonic >"$RAW/$tag-journal.log"
    [[ "$(stat -c '%s' "$RAW/$tag-journal.log")" -le "$MAX_RUNTIME_LOG_BYTES" ]] \
        || die "packaged service journal exceeded the review bound"
}

TREE=$(git -C "$SOURCE_ROOT" rev-parse HEAD^{tree})
git -C "$SOURCE_ROOT" archive --format=tar \
    --prefix="blaze-$VERSION/" "$CANDIDATE:src/blaze" \
    | gzip -n >"$SOURCE_ARCHIVE"
git -C "$SOURCE_ROOT" show "$CANDIDATE:src/blaze/dist/blaze.spec" \
    >"$SPEC_COPY"
cmp -s "$SPEC_COPY" "$SPEC_SOURCE" \
    || die "archived RPM spec differs from clean candidate source"
SOURCE_ARCHIVE_SHA=$(hash_file "$SOURCE_ARCHIVE")
RESTRICTED_PATTERNS_SHA=$(hash_file "$RESTRICTED_PATTERNS")

set +e
env CARGO_NET_OFFLINE=true cargo vendor --quiet --locked \
    --manifest-path "$SOURCE_ROOT/src/blaze/Cargo.toml" \
    "$VENDOR_STAGE/vendor" \
    >"$RAW/cargo-vendor.out" 2>"$RAW/cargo-vendor.err"
RUN_EXIT=$?
set -e
[[ "$RUN_EXIT" -eq 0 ]] \
    || die "cargo vendor --quiet --locked failed with the available package-builder cache"
record_action "dependency-vendor" \
    "CARGO_NET_OFFLINE=true cargo vendor --quiet --locked <temporary>/vendor" \
    "locked third-party sources are staged without network access"
tar --sort=name --mtime='@0' --owner=0 --group=0 --numeric-owner \
    -cf - -C "$VENDOR_STAGE" vendor | gzip -n >"$VENDOR_ARCHIVE"
VENDOR_ARCHIVE_SHA=$(hash_file "$VENDOR_ARCHIVE")

set +e
env CARGO_HOME="$WORK_ROOT/cargo-home" \
    RUSTFLAGS="--remap-path-prefix=$WORK_ROOT=/usr/src/blaze" \
    rpmbuild -ba --define "_topdir $TOPDIR" \
    --define "_buildhost build.invalid" \
    --define "_packager Blaze Package Builder" "$SPEC_COPY" \
    >"$RAW/rpmbuild.out" 2>"$RAW/rpmbuild.err"
RUN_EXIT=$?
set -e
[[ "$RUN_EXIT" -eq 0 ]] || die "rpmbuild -ba failed"
record_action "package-build" \
    "rpmbuild -ba --define _topdir <temporary> --define _buildhost build.invalid blaze.spec" \
    "source and binary RPM build succeeds from candidate archive"
for build_log in "$RAW/rpmbuild.out" "$RAW/rpmbuild.err"; do
    [[ "$(stat -c '%s' "$build_log")" -le "$MAX_BUILD_LOG_BYTES" ]] \
        || die "rpmbuild log exceeded the review bound"
done

mapfile -t BINARY_RPMS < <(
    find "$TOPDIR/RPMS" -type f -name '*.rpm' \
        ! -name '*debuginfo*' ! -name '*debugsource*' | LC_ALL=C sort
)
mapfile -t SOURCE_RPMS < <(
    find "$TOPDIR/SRPMS" -type f -name '*.src.rpm' | LC_ALL=C sort
)
[[ "${#BINARY_RPMS[@]}" -eq 1 ]] \
    || die "package build must produce exactly one binary RPM"
[[ "${#SOURCE_RPMS[@]}" -eq 1 ]] \
    || die "package build must produce exactly one source RPM"
CANDIDATE_RPM=${BINARY_RPMS[0]}
SOURCE_RPM=${SOURCE_RPMS[0]}

[[ "$(rpm -qp --qf '%{NAME}' "$CANDIDATE_RPM")" == "blaze" ]] \
    || die "binary RPM name differs"
[[ "$(rpm -qp --qf '%{VERSION}' "$CANDIDATE_RPM")" == "$VERSION" ]] \
    || die "binary RPM version differs"
[[ "$(rpm -qp --qf '%{ARCH}' "$CANDIDATE_RPM")" == "$ARCH" ]] \
    || die "binary RPM architecture differs from builder"
[[ "$(rpm -qp --qf '%{NAME}' "$SOURCE_RPM")" == "blaze" ]] \
    || die "source RPM name differs"
[[ "$(rpm -qp --qf '%{VERSION}' "$SOURCE_RPM")" == "$VERSION" ]] \
    || die "source RPM version differs"
[[ "$(rpm -qp --qf '%{SOURCEPACKAGE}' "$SOURCE_RPM")" == "1" ]] \
    || die "source RPM header is not marked as a source package"
SOURCE_RPM_ARCH=$(rpm -qp --qf '%{ARCH}' "$SOURCE_RPM")
case "$SOURCE_RPM_ARCH" in
    src | "$ARCH") ;;
    *) die "source RPM architecture differs from the source/build architecture" ;;
esac
for package_file in "$CANDIDATE_RPM" "$SOURCE_RPM"; do
    [[ "$(rpm -qp --qf '%{BUILDHOST}' "$package_file")" == "build.invalid" ]] \
        || die "RPM header contains a non-generic build host"
    [[ "$(rpm -qp --qf '%{PACKAGER}' "$package_file")" \
        == "Blaze Package Builder" ]] \
        || die "RPM header contains a non-generic packager"
done

run_action 0 "binary-rpm-digest" "rpm -K <candidate-rpm>" \
    "binary RPM payload digest verifies" rpm -K "$CANDIDATE_RPM"
run_action 0 "source-rpm-digest" "rpm -K <source-rpm>" \
    "source RPM payload digest verifies" rpm -K "$SOURCE_RPM"
run_action 0 "binary-rpm-metadata" "rpm -qip <candidate-rpm>" \
    "binary RPM metadata is available for confidentiality review" \
    rpm -qip "$CANDIDATE_RPM"
run_action 0 "source-rpm-metadata" "rpm -qip <source-rpm>" \
    "source RPM metadata is available for confidentiality review" \
    rpm -qip "$SOURCE_RPM"
capture_complete_rpm_header binary "$CANDIDATE_RPM" \
    "rpm -qp --xml <candidate-rpm>" \
    "binary RPM complete header metadata is available for confidentiality review"
capture_complete_rpm_header source "$SOURCE_RPM" \
    "rpm -qp --xml <source-rpm>" \
    "source RPM complete header metadata is available for confidentiality review"
run_action 0 "binary-rpm-scriptlets" "rpm -qp --scripts <candidate-rpm>" \
    "binary RPM scriptlets are available for confidentiality review" \
    rpm -qp --scripts "$CANDIDATE_RPM"

(
    cd "$SOURCE_EXTRACT"
    rpm2cpio "$SOURCE_RPM" \
        | cpio -idm --quiet --no-absolute-filenames
)
cmp -s "$SOURCE_EXTRACT/blaze-$VERSION.tar.gz" "$SOURCE_ARCHIVE" \
    || die "source RPM archive differs from candidate archive"
cmp -s "$SOURCE_EXTRACT/blaze-$VERSION-vendor.tar.gz" "$VENDOR_ARCHIVE" \
    || die "source RPM vendor archive differs from staged dependencies"
cmp -s "$SOURCE_EXTRACT/blaze.spec" "$SPEC_COPY" \
    || die "source RPM spec differs from candidate spec"

rpm -qp --qf \
    '[%{FILENAMES}\t%{FILEMODES:perms}\t%{FILEUSERNAME}\t%{FILEGROUPNAME}\n]' \
    "$CANDIDATE_RPM" | LC_ALL=C sort >"$PACKAGE_FILES"
[[ "$(awk -F '\t' '$1 == "/usr/bin/blazectl" {count++} END {print count+0}' \
    "$PACKAGE_FILES")" -eq 1 ]] \
    || die "binary RPM must own exactly one blazectl path"
awk -F '\t' '
  $1 == "/usr/bin/blazectl" &&
    $2 == "-rwxr-xr-x" && $3 == "root" && $4 == "root" {found=1}
  END {exit found ? 0 : 1}
' "$PACKAGE_FILES" || die "binary RPM blazectl owner/group/mode differs"
awk -F '\t' '
  $1 == "/usr/libexec/anolisa/blazed" &&
    $2 == "-rwxr-xr-x" && $3 == "root" && $4 == "root" {found=1}
  END {exit found ? 0 : 1}
' "$PACKAGE_FILES" || die "binary RPM blazed owner/group/mode differs"
[[ -z "$(cut -f1 "$PACKAGE_FILES" | LC_ALL=C sort | uniq -d)" ]] \
    || die "binary RPM contains duplicate package paths"
for required_path in \
    /etc/anolisa/blaze/config.toml \
    /usr/lib/systemd/system/blazed.service \
    /usr/lib/tmpfiles.d/blaze.conf \
    /usr/share/anolisa/components/blaze/component.toml \
    /usr/share/doc/blaze/README.md \
    /usr/share/doc/blaze/README_zh.md; do
    [[ "$(awk -F '\t' -v path="$required_path" \
        '$1 == path {count++} END {print count+0}' "$PACKAGE_FILES")" -eq 1 ]] \
        || die "binary RPM payload is missing a required public path"
done
if [[ "$MODE" == "runtime" ]]; then
    while IFS=$'\t' read -r package_path package_mode package_owner package_group; do
        case "$package_mode" in
            d*) ;;
            *)
                [[ ! -e "$package_path" && ! -L "$package_path" ]] \
                    || die "candidate RPM would overwrite an existing path"
                if rpm -qf "$package_path" >/dev/null 2>&1; then
                    die "candidate RPM path is already owned by another package"
                fi
                ;;
        esac
    done <"$PACKAGE_FILES"
fi

(
    cd "$EXTRACT"
    rpm2cpio "$CANDIDATE_RPM" \
        | cpio -idm --quiet --no-absolute-filenames
)
[[ -x "$EXTRACT/usr/bin/blazectl" ]] \
    || die "extracted RPM is missing executable blazectl"
[[ -x "$EXTRACT/usr/libexec/anolisa/blazed" ]] \
    || die "extracted RPM is missing executable blazed"
PACKAGED_BLAZECTL_SHA=$(hash_file "$EXTRACT/usr/bin/blazectl")
PACKAGED_BLAZED_SHA=$(hash_file "$EXTRACT/usr/libexec/anolisa/blazed")
PACKAGED_CONFIG="$EXTRACT/etc/anolisa/blaze/config.toml"
[[ -f "$PACKAGED_CONFIG" ]] || die "extracted RPM is missing daemon config"
cmp -s "$PACKAGED_CONFIG" "$SOURCE_ROOT/src/blaze/examples/config.toml" \
    || die "packaged daemon config differs from candidate source"
cmp -s "$EXTRACT/usr/share/anolisa/components/blaze/component.toml" \
    "$SOURCE_ROOT/src/blaze/.anolisa/component.toml" \
    || die "packaged component manifest differs from candidate source"
cmp -s "$EXTRACT/usr/share/doc/blaze/README.md" \
    "$SOURCE_ROOT/src/blaze/README.md" \
    || die "packaged README differs from candidate source"
cmp -s "$EXTRACT/usr/share/doc/blaze/README_zh.md" \
    "$SOURCE_ROOT/src/blaze/README_zh.md" \
    || die "packaged translated README differs from candidate source"
SERVICE_FILE="$EXTRACT/usr/lib/systemd/system/blazed.service"
TMPFILES_FILE="$EXTRACT/usr/lib/tmpfiles.d/blaze.conf"
[[ -f "$SERVICE_FILE" && -f "$TMPFILES_FILE" ]] \
    || die "extracted RPM is missing service or tmpfiles contract"
grep -F \
    "ExecStart=/usr/libexec/anolisa/blazed daemon start --config /etc/anolisa/blaze/config.toml" \
    "$SERVICE_FILE" >/dev/null \
    || die "packaged service path differs"
if grep -F "blazectl" "$SERVICE_FILE" >/dev/null; then
    die "packaged service references blazectl"
fi
[[ "$(awk '
  NF && $0 !~ /^[[:space:]]*#/ {print}
' "$TMPFILES_FILE")" == "d /run/blaze 0755 root root -" ]] \
    || die "packaged tmpfiles contract differs"

CANDIDATE_RPM_SHA=$(hash_file "$CANDIDATE_RPM")
SOURCE_RPM_SHA=$(hash_file "$SOURCE_RPM")
PREVIOUS_RPM_SHA=
if [[ "$MODE" == "runtime" ]]; then
    [[ "$(rpm -qp --qf '%{NAME}' "$PREVIOUS_RPM")" == "blaze" ]] \
        || die "previous-rpm package name differs"
    PREVIOUS_ARCH=$(rpm -qp --qf '%{ARCH}' "$PREVIOUS_RPM")
    [[ "$PREVIOUS_ARCH" == "$ARCH" ]] \
        || die "previous-rpm architecture differs from builder"
    PREVIOUS_EVR=$(rpm -qp --qf '%{EPOCHNUM}:%{VERSION}-%{RELEASE}' "$PREVIOUS_RPM")
    CANDIDATE_EVR=$(rpm -qp --qf '%{EPOCHNUM}:%{VERSION}-%{RELEASE}' "$CANDIDATE_RPM")
    [[ "$PREVIOUS_EVR" != "$CANDIDATE_EVR" ]] \
        || die "previous-rpm must differ from the candidate RPM"
    run_action 0 "previous-rpm-digest" "rpm -K <previous-rpm>" \
        "reviewed previous RPM payload digest verifies" rpm -K "$PREVIOUS_RPM"
    PREVIOUS_RPM_SHA=$(hash_file "$PREVIOUS_RPM")
fi

jq -n \
    --arg candidate "$CANDIDATE" \
    --arg tree "$TREE" \
    --arg mode "$MODE" \
    --arg arch "$ARCH" \
    --arg version "$VERSION" \
    --arg source_archive_sha256 "$SOURCE_ARCHIVE_SHA" \
    --arg vendor_archive_sha256 "$VENDOR_ARCHIVE_SHA" \
    --arg binary_rpm_sha256 "$CANDIDATE_RPM_SHA" \
    --arg source_rpm_sha256 "$SOURCE_RPM_SHA" \
    --arg blazectl_sha256 "$PACKAGED_BLAZECTL_SHA" \
    --arg blazed_sha256 "$PACKAGED_BLAZED_SHA" \
    --arg previous_rpm_sha256 "$PREVIOUS_RPM_SHA" \
    --arg restricted_patterns_sha256 "$RESTRICTED_PATTERNS_SHA" \
    '{
      candidate: $candidate,
      tree: $tree,
      dirty: false,
      os_family: "linux",
      arch: $arch,
      mode: $mode,
      package: "blaze",
      version: $version,
      source_archive_sha256: $source_archive_sha256,
      vendor_archive_sha256: $vendor_archive_sha256,
      binary_rpm_sha256: $binary_rpm_sha256,
      source_rpm_sha256: $source_rpm_sha256,
      blazectl_sha256: $blazectl_sha256,
      blazed_sha256: $blazed_sha256,
      previous_rpm_sha256:
        (if $previous_rpm_sha256 == "" then null else $previous_rpm_sha256 end),
      restricted_patterns_sha256: $restricted_patterns_sha256
    }' >"$EVIDENCE_ROOT/candidate.json"

FRESH_INSTALL=false
UPGRADE=false
CONFIG_PRESERVED=false
VERSION_MATCH=false
UDS_ACCESS=false
UNINSTALL_STATE_PRESERVED=false
TMPFILES_RECREATED=false

if [[ "$MODE" == "runtime" ]]; then
    run_action 0 "fresh-install" "rpm -ivh <candidate-rpm>" \
        "candidate RPM installs onto an empty disposable host" \
        rpm -ivh "$CANDIDATE_RPM"
    FRESH_INSTALL=true
    verify_tmpfiles_recreation fresh
    TMPFILES_RECREATED=true
    verify_installed_binaries fresh
    verify_service_and_uds fresh
    UDS_ACCESS=true
    VERSION_MATCH=true

    install -d -m 0755 /var/lib/blaze
    install -m 0600 "$SENTINEL_EXPECTED" "$STATE_SENTINEL"
    run_action 0 "fresh-uninstall" "rpm -e blaze" \
        "fresh package uninstall succeeds" rpm -e blaze
    assert_absent_runtime_paths
    cleanup_verified_state
    UNINSTALL_STATE_PRESERVED=true

    run_action 0 "previous-install" "rpm -ivh <previous-rpm>" \
        "reviewed previous RPM installs for upgrade acceptance" \
        rpm -ivh "$PREVIOUS_RPM"
    install -m 0644 "$PACKAGED_CONFIG" "$CONFIG_PATH"
    printf '\n%s\n' "$CONFIG_SENTINEL" >>"$CONFIG_PATH"
    CONFIG_HASH_BEFORE=$(hash_file "$CONFIG_PATH")
    install -d -m 0755 /var/lib/blaze
    install -m 0600 "$SENTINEL_EXPECTED" "$STATE_SENTINEL"

    run_action 0 "candidate-upgrade" "rpm -Uvh <candidate-rpm>" \
        "candidate RPM upgrades the reviewed previous package" \
        rpm -Uvh "$CANDIDATE_RPM"
    UPGRADE=true
    [[ "$(rpm -q --qf '%{VERSION}' blaze)" == "$VERSION" ]] \
        || die "installed RPM version differs after upgrade"
    [[ "$(hash_file "$CONFIG_PATH")" == "$CONFIG_HASH_BEFORE" ]] \
        || die "upgrade did not preserve daemon config bytes"
    grep -F "$CONFIG_SENTINEL" "$CONFIG_PATH" >/dev/null \
        || die "upgrade did not preserve config-preservation-sentinel"
    CONFIG_PRESERVED=true
    verify_tmpfiles_recreation upgrade
    verify_installed_binaries upgrade
    verify_service_and_uds upgrade

    if [[ -e "$CONFIG_PATH.rpmnew" ]]; then
        cmp -s "$CONFIG_PATH.rpmnew" "$PACKAGED_CONFIG" \
            || die "upgrade rpmnew differs from candidate config"
        rm -f -- "$CONFIG_PATH.rpmnew"
    fi
    install -m 0644 "$PACKAGED_CONFIG" "$CONFIG_PATH"
    run_action 0 "upgrade-uninstall" "rpm -e blaze" \
        "upgraded package uninstall succeeds" rpm -e blaze
    assert_absent_runtime_paths
    cleanup_verified_state
fi

jq -n \
    --arg mode "$MODE" \
    --arg arch "$ARCH" \
    --argjson source_rpm true \
    --argjson binary_rpm true \
    --argjson fresh_install "$FRESH_INSTALL" \
    --argjson upgrade "$UPGRADE" \
    --argjson config_preserved "$CONFIG_PRESERVED" \
    --argjson version_match "$VERSION_MATCH" \
    --argjson uds_access "$UDS_ACCESS" \
    --argjson uninstall_state_preserved "$UNINSTALL_STATE_PRESERVED" \
    --argjson tmpfiles_recreated "$TMPFILES_RECREATED" \
    '{
      mode: $mode,
      arch: $arch,
      source_rpm: $source_rpm,
      binary_rpm: $binary_rpm,
      package_complete: false,
      requires_cross_arch_aggregation: true,
      payload_owner_mode: true,
      package_header_buildhost: "build.invalid",
      package_header_packager: "Blaze Package Builder",
      complete_header_metadata_scanned: true,
      package_path_overlap_preflight: ($mode == "runtime"),
      service_executes_blazed_only: true,
      fresh_install: $fresh_install,
      upgrade: $upgrade,
      config_preserved: $config_preserved,
      daemon_client_version_match: $version_match,
      approved_root_uds_access: $uds_access,
      tmpfiles_recreated_after_removal: $tmpfiles_recreated,
      uninstall_cli_removed: $fresh_install,
      uninstall_user_state_preserved: $uninstall_state_preserved
    }' >"$EVIDENCE_ROOT/package-summary.json"

jq -n \
    --arg arch "$ARCH" \
    --arg mode "$MODE" \
    --argjson disposable_runtime "$([[ "$MODE" == "runtime" ]] && printf true || printf false)" \
    '{
      os_family: "linux",
      arch: $arch,
      mode: $mode,
      supported_architecture: true,
      disposable_runtime: $disposable_runtime,
      public_environment_values_only: true
    }' >"$EVIDENCE_ROOT/environment-summary.json"

set +e
git -C "$SOURCE_ROOT" grep -I -E -f "$ACTIVE_PATTERNS" \
    "$CANDIDATE" -- src/blaze >/dev/null
SOURCE_SCAN_STATUS=$?
set -e
case "$SOURCE_SCAN_STATUS" in
    1) ;;
    0) die "reviewed restricted pattern matched candidate source" ;;
    *) die "candidate source confidentiality scan failed" ;;
esac

CONFIDENTIALITY_SUMMARY="$WORK_ROOT/confidentiality-summary.json"
VERIFICATION_SUMMARY="$WORK_ROOT/verification-summary.json"
SHA256_SUMS="$WORK_ROOT/SHA256SUMS"
SCANNED_FILES=$(find "$RAW" "$EVIDENCE_ROOT" "$EXTRACT" -type f \
    | wc -l | tr -d ' ')
SCANNED_FILES=$((SCANNED_FILES + 3))

jq -n \
    --argjson scanned_files "$SCANNED_FILES" \
    '{
      deny: 0,
      scanned_files: $scanned_files,
      candidate_source_scanned: true,
      extracted_binary_rpm_scanned: true,
      binary_rpm_header_scanned: true,
      source_rpm_header_scanned: true,
      raw_build_and_runtime_scanned: true,
      final_evidence_bytes_scanned_before_publish: true,
      reviewed_restricted_pattern_hits: 0,
      raw_output_copied_to_evidence: false,
      public_values: "public package paths, versions, architecture, hashes and redacted argv only"
    }' >"$CONFIDENTIALITY_SUMMARY"

if [[ "$MODE" == "build-only" ]]; then
    jq -n \
        --arg candidate "$CANDIDATE" \
        --arg tree "$TREE" \
        --arg arch "$ARCH" \
        --argjson actions "$ACTION_INDEX" \
        '{
          result: "PASS",
          evidence_level: "package-build-only",
          package_complete: false,
          candidate: $candidate,
          tree: $tree,
          dirty: false,
          arch: $arch,
          source_rpm: true,
          binary_rpm: true,
          package_metadata_scanned: true,
          package_runtime: false,
          confidentiality_deny: 0,
          command_assertions: $actions
        }' >"$VERIFICATION_SUMMARY"
else
    jq -n \
        --arg candidate "$CANDIDATE" \
        --arg tree "$TREE" \
        --arg arch "$ARCH" \
        --argjson actions "$ACTION_INDEX" \
        '{
          result: "PASS",
          evidence_level: "package-runtime",
          package_complete: false,
          runtime_contract_complete: true,
          requires_cross_arch_aggregation: true,
          candidate: $candidate,
          tree: $tree,
          dirty: false,
          arch: $arch,
          source_rpm: true,
          binary_rpm: true,
          package_metadata_scanned: true,
          fresh_install: true,
          upgrade: true,
          config_preserved: true,
          daemon_client_version_match: true,
          approved_root_uds_access: true,
          tmpfiles_recreated_after_removal: true,
          uninstall_cli_removed: true,
          uninstall_user_state_preserved: true,
          confidentiality_deny: 0,
          command_assertions: $actions
        }' >"$VERIFICATION_SUMMARY"
fi

(
    cd "$EVIDENCE_ROOT"
    sha256sum candidate.json command-matrix.tsv environment-summary.json \
        package-files.tsv package-summary.json
    printf '%s  confidentiality-summary.json\n' \
        "$(hash_file "$CONFIDENTIALITY_SUMMARY")"
    printf '%s  verification-summary.json\n' \
        "$(hash_file "$VERIFICATION_SUMMARY")"
) >"$SHA256_SUMS"

ACTUAL_SCANNED_FILES=0
while IFS= read -r -d '' scan_file; do
    ACTUAL_SCANNED_FILES=$((ACTUAL_SCANNED_FILES + 1))
    if grep -aEif "$ACTIVE_PATTERNS" "$scan_file" >/dev/null; then
        die "reviewed restricted pattern matched package/runtime/evidence output"
    fi
done < <(
    find "$RAW" "$EVIDENCE_ROOT" "$EXTRACT" -type f -print0
    printf '%s\0' "$CONFIDENTIALITY_SUMMARY" "$VERIFICATION_SUMMARY" "$SHA256_SUMS"
)
[[ "$ACTUAL_SCANNED_FILES" -eq "$SCANNED_FILES" ]] \
    || die "confidentiality scan input accounting differs"

install -m 0600 "$CONFIDENTIALITY_SUMMARY" \
    "$EVIDENCE_ROOT/confidentiality-summary.json"
install -m 0600 "$VERIFICATION_SUMMARY" \
    "$EVIDENCE_ROOT/verification-summary.json"
install -m 0600 "$SHA256_SUMS" "$EVIDENCE_ROOT/SHA256SUMS"
cmp -s "$CONFIDENTIALITY_SUMMARY" \
    "$EVIDENCE_ROOT/confidentiality-summary.json" \
    || die "published confidentiality summary differs from scanned bytes"
cmp -s "$VERIFICATION_SUMMARY" "$EVIDENCE_ROOT/verification-summary.json" \
    || die "published verification summary differs from scanned bytes"
cmp -s "$SHA256_SUMS" "$EVIDENCE_ROOT/SHA256SUMS" \
    || die "published checksum manifest differs from scanned bytes"
(
    cd "$EVIDENCE_ROOT"
    sha256sum -c SHA256SUMS >/dev/null
)

cleanup_runner_state || die "final disposable-host cleanup failed"
[[ "$WORK_ROOT" == /tmp/blaze-rpm.* \
    && -d "$WORK_ROOT" && ! -L "$WORK_ROOT" \
    && -f "$WORK_ROOT/.blaze-rpm-owned" ]] \
    || die "final temporary cleanup ownership check failed"
rm -rf -- "$WORK_ROOT"
[[ ! -e "$WORK_ROOT" ]] || die "final temporary cleanup failed"

FINALIZED=1
trap - EXIT INT TERM
printf 'PASS candidate=%s evidence=%s mode=%s arch=%s\n' \
    "$CANDIDATE" \
    "$([[ "$MODE" == "runtime" ]] && printf package-runtime || printf package-build-only)" \
    "$MODE" "$ARCH"
