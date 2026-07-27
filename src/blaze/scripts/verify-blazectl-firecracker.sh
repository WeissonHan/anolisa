#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Explicit Linux/KVM acceptance for the final release blazed and blazectl.
# This runner never treats a missing fixture or privilege as a successful run.
#
# CONTRACT: backend=firecracker mock=false
# CONTRACT: create list exec write read checkpoint rollback checkpoints
# CONTRACT: prune-checkpoints hibernate resume pool-status cleanup-devices kill kill-all
# CONTRACT: uds tcp text json binary guest-nonzero daemon-error connection-unavailable
# CONTRACT: firecracker netns tap-veth nat uds storage runtime cli-process open-fd listener delta
# CONTRACT: candidate binary runtime-input command exit assertion confidentiality hashes

set -euo pipefail
umask 077

# 2 local version surfaces, 2 x 19 lifecycle assertions, 9 error/cleanup
# assertions and 2 unavailable-daemon assertions.
EXPECTED_COMMAND_ASSERTIONS=51
MAX_DAEMON_LOG_BYTES=16777216

usage() {
    cat >&2 <<'EOF'
usage: verify-blazectl-firecracker.sh \
  <source-root> <candidate-sha> <release-blazed> <release-blazectl> \
  <firecracker> <vmlinux> <rootfs.ext4> <restricted-patterns> \
  <evidence-root> [tcp-port]

The rootfs fixture must boot a guest agent compatible with the frozen Blaze
guest protocol and support the fixed commands used by this runner. The
restricted-patterns file must contain the complete reviewed confidentiality
deny expressions for source, runtime and evidence surfaces.
EOF
    exit 2
}

die() {
    printf 'blazectl Firecracker: %s\n' "$1" >&2
    exit 1
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "required command is unavailable: $1"
}

hash_file() {
    sha256sum "$1" | awk '{print $1}'
}

[[ $# -eq 9 || $# -eq 10 ]] || usage

SOURCE_ROOT_ARG=$1
CANDIDATE=$2
BLAZED_ARG=$3
BLAZECTL_ARG=$4
FIRECRACKER_ARG=$5
VMLINUX_ARG=$6
ROOTFS_ARG=$7
RESTRICTED_PATTERNS_ARG=$8
EVIDENCE_ARG=$9
TCP_PORT=${10:-14159}

for command in awk base64 basename cat cmp curl diff dirname env find git grep \
    id install ip iptables iptables-save jq kill ln mktemp ps realpath rm sed \
    readlink seq setsid sha256sum sleep sort ss stat tr uname unshare wc; do
    require_command "$command"
done

[[ "$(uname -s)" == "Linux" ]] || die "real Firecracker acceptance requires Linux"
[[ "$(id -u)" -eq 0 ]] || die "real Firecracker acceptance must run as root"
[[ -r /dev/kvm && -w /dev/kvm ]] || die "/dev/kvm is not readable and writable"
[[ "$CANDIDATE" =~ ^[0-9a-f]{40}$ ]] || die "candidate must be a full lowercase SHA"
[[ "$TCP_PORT" =~ ^[0-9]+$ ]] || die "tcp-port must be numeric"
((TCP_PORT >= 1024 && TCP_PORT <= 65535)) || die "tcp-port is outside 1024..65535"

SOURCE_ROOT=$(realpath "$SOURCE_ROOT_ARG")
BLAZED=$(realpath "$BLAZED_ARG")
BLAZECTL=$(realpath "$BLAZECTL_ARG")
FIRECRACKER=$(realpath "$FIRECRACKER_ARG")
VMLINUX=$(realpath "$VMLINUX_ARG")
ROOTFS=$(realpath "$ROOTFS_ARG")
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

for executable in "$BLAZED" "$BLAZECTL" "$FIRECRACKER"; do
    [[ -f "$executable" && -x "$executable" ]] \
        || die "required release/runtime executable is missing"
done
for input in "$VMLINUX" "$ROOTFS" "$RESTRICTED_PATTERNS"; do
    [[ -f "$input" ]] || die "required runtime input is missing"
done
[[ -s "$RESTRICTED_PATTERNS" ]] \
    || die "restricted-patterns must contain at least one reviewed pattern"

EVIDENCE_PARENT=$(realpath "$(dirname "$EVIDENCE_ARG")")
EVIDENCE_NAME=$(basename "$EVIDENCE_ARG")
[[ "$EVIDENCE_NAME" =~ ^[A-Za-z0-9._-]+$ ]] \
    || die "evidence directory name must be non-sensitive and portable"
EVIDENCE_ROOT="$EVIDENCE_PARENT/$EVIDENCE_NAME"
[[ ! -e "$EVIDENCE_ROOT" ]] || die "evidence root already exists"

if ss -H -ltn | awk -v suffix=":$TCP_PORT" \
    '$4 ~ suffix "$" {found=1} END {exit found ? 0 : 1}'; then
    die "requested loopback TCP port is already listening"
fi

WORK_ROOT=$(mktemp -d /tmp/blaze-firecracker.XXXXXX)
[[ "$WORK_ROOT" == /tmp/blaze-firecracker.* ]] || die "unexpected temporary work root"
: >"$WORK_ROOT/.blaze-firecracker-owned"

early_cleanup() {
    local exit_code=$?
    set +e
    if [[ -d "$EVIDENCE_ROOT" \
        && ! -e "$EVIDENCE_ROOT/verification-summary.json" ]]; then
        jq -n \
            --arg candidate "$CANDIDATE" \
            '{
              result: "FAIL",
              candidate: $candidate,
              stage: "runner-setup",
              backend: "firecracker",
              mock: false
            }' >"$EVIDENCE_ROOT/verification-summary.json"
    fi
    if [[ "$WORK_ROOT" == /tmp/blaze-firecracker.* \
        && -d "$WORK_ROOT" && ! -L "$WORK_ROOT" \
        && -f "$WORK_ROOT/.blaze-firecracker-owned" ]]; then
        rm -rf -- "$WORK_ROOT"
    fi
    trap - EXIT INT TERM
    exit "$exit_code"
}
trap early_cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
install -d -m 0700 "$EVIDENCE_ROOT"

RAW="$WORK_ROOT/raw"
RUN_DIR="$WORK_ROOT/run"
STATE_DIR="$WORK_ROOT/state"
IMAGES_DIR="$WORK_ROOT/images"
INSTANCES_DIR="$WORK_ROOT/instances"
POLICY_DIR="$WORK_ROOT/policies"
TEMPLATE_DIR="$WORK_ROOT/templates"
BIN_DIR="$WORK_ROOT/bin"
CONFIG="$WORK_ROOT/config.toml"
POLICY="$POLICY_DIR/firecracker.toml"
SOCKET="$RUN_DIR/api.sock"
METRICS_SOCKET="$RUN_DIR/metrics.sock"
TCP_URL="http://127.0.0.1:$TCP_PORT"
DAEMON_LOG="$RAW/daemon.log"
COMMAND_MATRIX="$EVIDENCE_ROOT/command-matrix.tsv"

install -d -m 0700 "$RAW" "$RUN_DIR" "$STATE_DIR" "$IMAGES_DIR" \
    "$INSTANCES_DIR" "$POLICY_DIR" "$TEMPLATE_DIR" "$BIN_DIR"
ln -s "$FIRECRACKER" "$BIN_DIR/firecracker"
ln -s "$VMLINUX" "$IMAGES_DIR/vmlinux"
ln -s "$ROOTFS" "$IMAGES_DIR/rootfs.ext4"

cat >"$CONFIG" <<EOF
[daemon]
log_level = "info"
state_dir = "$STATE_DIR"
socket = "$SOCKET"

[listen]
http_addr = "127.0.0.1:$TCP_PORT"

[backends]
firecracker = "$BIN_DIR/firecracker"

[policy]
dir = "$POLICY_DIR"
on_load_error = "fail"

[storage]
images_dir = "$IMAGES_DIR"
instances_dir = "$INSTANCES_DIR"
provider = "file"
pool_size = 0
prefork = false
flush_interval = "30s"
rootfs_size = 536870912
mem_size = 536870912

[api]
max_body_bytes = 1048576
max_file_bytes = 16777216
request_timeout = "60s"

[pool]
default_warm_ttl = "30m"
gc_interval = "5m"

[template]
dir = "$TEMPLATE_DIR"
gc_interval = "10m"
idle_ttl = "1h"

[metrics]
prometheus_socket = "$METRICS_SOCKET"
EOF

cat >"$POLICY" <<'EOF'
manifest_version = 1
policy_name = "blazectl-firecracker"
priority = 100

[match]
workload_class = "agent-tool"

[select]
backend_priority = ["firecracker"]
kernel_hooks = []
templates = []
fallback_on_missing_hook = "fail"

[pool]
enabled = false
min = 0
target = 0
max = 0
warm_ttl = "30m"
reset_mode = "full-recreate"

[vm]
vcpus = 1
memory = "512Mi"

[backend.firecracker]
boot_args = "console=ttyS0 reboot=k panic=1 pci=off"
enable_vsock = true
enable_network = true
memory = "512Mi"
vcpus = 1
serial_log = false
EOF

DAEMON_PID=
FINALIZED=0
COMMAND_INDEX=0
RUN_STDOUT=
RUN_STDERR=
RUN_EXIT=
RUN_SAFE_ARGV=

stop_daemon() {
    [[ -n "$DAEMON_PID" ]] || return 0
    local forced=0
    local wait_status=0
    if kill -0 "$DAEMON_PID" 2>/dev/null; then
        kill -TERM "$DAEMON_PID" 2>/dev/null || true
        for _ in $(seq 1 120); do
            kill -0 "$DAEMON_PID" 2>/dev/null || break
            sleep 0.25
        done
        if kill -0 "$DAEMON_PID" 2>/dev/null; then
            forced=1
            kill -KILL -- "-$DAEMON_PID" 2>/dev/null \
                || kill -KILL "$DAEMON_PID" 2>/dev/null \
                || true
        fi
    fi
    if wait "$DAEMON_PID" 2>/dev/null; then
        wait_status=0
    else
        wait_status=$?
    fi
    DAEMON_PID=
    [[ "$forced" -eq 0 && "$wait_status" -eq 0 ]]
}

cleanup() {
    local exit_code=$?
    local failed_line=${BASH_LINENO[0]:-0}
    set +e
    stop_daemon
    if [[ "$FINALIZED" -eq 0 ]]; then
        jq -n \
            --arg candidate "$CANDIDATE" \
            --argjson failed_line "$failed_line" \
            '{
              result: "FAIL",
              candidate: $candidate,
              failed_line: $failed_line,
              backend: "firecracker",
              mock: false
            }' >"$EVIDENCE_ROOT/verification-summary.json"
    fi
    if [[ "$WORK_ROOT" == /tmp/blaze-firecracker.* \
        && -d "$WORK_ROOT" && ! -L "$WORK_ROOT" \
        && -f "$WORK_ROOT/.blaze-firecracker-owned" ]]; then
        rm -rf -- "$WORK_ROOT"
    fi
    trap - EXIT INT TERM
    exit "$exit_code"
}
trap - EXIT
trap cleanup EXIT

printf 'index\toperation\tlabel\ttransport\tredacted_argv\texit\tassertion\n' \
    >"$COMMAND_MATRIX"

record_assertion() {
    local operation=$1
    local label=$2
    local transport=$3
    local safe_argv=$4
    local assertion=$5
    COMMAND_INDEX=$((COMMAND_INDEX + 1))
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$COMMAND_INDEX" "$operation" "$label" "$transport" \
        "$safe_argv" "$RUN_EXIT" "$assertion" >>"$COMMAND_MATRIX"
}

run_cli() {
    local expected_exit=$1
    local transport=$2
    local mode=$3
    local stdin_file=$4
    local label=$5
    local safe_argv=$6
    shift 6
    [[ "$1" == "--" ]] || die "runner command separator is missing"
    shift

    local sequence
    sequence=$(printf '%03d' "$((COMMAND_INDEX + 1))")
    RUN_STDOUT="$RAW/cli-$sequence.out"
    RUN_STDERR="$RAW/cli-$sequence.err"
    RUN_SAFE_ARGV=$safe_argv

    local endpoint=()
    case "$transport" in
        uds)
            endpoint=(--socket "$SOCKET")
            ;;
        tcp)
            endpoint=(--url "$TCP_URL")
            ;;
        none)
            endpoint=()
            ;;
        *)
            die "unknown runner transport"
            ;;
    esac

    set +e
    if [[ "$stdin_file" == "-" ]]; then
        env -i PATH="$PATH" LC_ALL=C LANG=C NO_COLOR=1 CLICOLOR=0 \
            CLICOLOR_FORCE=0 TERM=dumb \
            "$BLAZECTL" "${endpoint[@]}" --output "$mode" "$@" \
            </dev/null >"$RUN_STDOUT" 2>"$RUN_STDERR"
    else
        env -i PATH="$PATH" LC_ALL=C LANG=C NO_COLOR=1 CLICOLOR=0 \
            CLICOLOR_FORCE=0 TERM=dumb \
            "$BLAZECTL" "${endpoint[@]}" --output "$mode" "$@" \
            <"$stdin_file" >"$RUN_STDOUT" 2>"$RUN_STDERR"
    fi
    RUN_EXIT=$?
    set -e
    [[ "$RUN_EXIT" -eq "$expected_exit" ]] \
        || die "$label returned exit $RUN_EXIT, expected $expected_exit"
}

assert_empty_stderr() {
    [[ ! -s "$RUN_STDERR" ]] || die "$1 wrote unexpected stderr"
}

assert_empty_stdout() {
    [[ ! -s "$RUN_STDOUT" ]] || die "$1 wrote unexpected stdout"
}

assert_text_field() {
    local label=$1
    local value=$2
    awk -F '\t' -v label="$label" -v value="$value" \
        '$1 == label && $2 == value {found=1} END {exit found ? 0 : 1}' \
        "$RUN_STDOUT" || die "missing text field $label"
}

capture_global_resources() {
    local name=$1
    local fd
    local namespace
    local pid
    local target
    ip netns list | awk '{print $1}' | LC_ALL=C sort \
        >"$RAW/$name.netns"
    ip -o link show | awk -F ': ' '{print $2}' | sed 's/@.*//' \
        | LC_ALL=C sort >"$RAW/$name.links"
    : >"$RAW/$name.nat"
    while IFS= read -r namespace; do
        [[ "$namespace" =~ ^blz-ns-[0-9]+$ ]] || continue
        ip netns exec "$namespace" iptables-save \
            | awk -v ns_label="$namespace" \
                '{print ns_label "\t" $0}' >>"$RAW/$name.nat"
    done < <(ip netns list | awk '{print $1}' | LC_ALL=C sort)
    LC_ALL=C sort -o "$RAW/$name.nat" "$RAW/$name.nat"
    ps -eo pid=,ppid=,comm=,args= \
        | BLAZE_Firecracker_WORK_ROOT="$WORK_ROOT" \
            awk 'index($0, ENVIRON["BLAZE_Firecracker_WORK_ROOT"]) {print}' \
        | LC_ALL=C sort >"$RAW/$name.processes"
    {
        ss -H -ltnp
        ss -H -lxnp
    } | BLAZE_Firecracker_WORK_ROOT="$WORK_ROOT" BLAZE_Firecracker_TCP_PORT="$TCP_PORT" \
        awk 'index($0, ENVIRON["BLAZE_Firecracker_WORK_ROOT"]) ||
          $0 ~ (":" ENVIRON["BLAZE_Firecracker_TCP_PORT"] "([^0-9]|$)")' \
        | LC_ALL=C sort >"$RAW/$name.listeners"
    while IFS= read -r pid; do
        [[ "$pid" =~ ^[0-9]+$ ]] || continue
        for fd in "/proc/$pid/fd/"*; do
            [[ -e "$fd" || -L "$fd" ]] || continue
            target=$(readlink "$fd" 2>/dev/null) || continue
            printf '%s\t%s\t%s\n' "$pid" "${fd##*/}" "$target"
        done
    done >"$RAW/$name.fds" < <(awk '{print $1}' "$RAW/$name.processes")
    LC_ALL=C sort -o "$RAW/$name.fds" "$RAW/$name.fds"
}

assert_live_resources() {
    capture_global_resources live
    ! cmp -s "$RAW/before.netns" "$RAW/live.netns" \
        || die "live Firecracker run created no netns delta"
    ! cmp -s "$RAW/before.links" "$RAW/live.links" \
        || die "live Firecracker run created no tap-veth delta"
    ! cmp -s "$RAW/before.nat" "$RAW/live.nat" \
        || die "live Firecracker run created no NAT delta"
    [[ -s "$RAW/live.processes" ]] \
        || die "live Firecracker process inventory is empty"
    [[ -s "$RAW/live.listeners" ]] \
        || die "live listener delta is empty"
    ! cmp -s "$RAW/before.listeners" "$RAW/live.listeners" \
        || die "live listener delta is zero"
    [[ -s "$RAW/live.fds" ]] \
        || die "live open-FD inventory is empty"
    local socket_count
    socket_count=$(find "$WORK_ROOT" -type s | wc -l | tr -d ' ')
    ((socket_count >= 3)) || die "live UDS inventory is incomplete"
    [[ -n "$(find "$INSTANCES_DIR" -mindepth 1 -maxdepth 1 -print -quit)" ]] \
        || die "live storage inventory is empty"
}

assert_final_resource_delta() {
    capture_global_resources after
    for kind in netns links nat listeners; do
        if ! cmp -s "$RAW/before.$kind" "$RAW/after.$kind"; then
            diff -u "$RAW/before.$kind" "$RAW/after.$kind" \
                >"$RAW/$kind.diff" || true
            die "final $kind resource delta is nonzero"
        fi
    done
    cmp -s "$RAW/before.fds" "$RAW/after.fds" \
        || die "final open-FD delta is nonzero"
    [[ ! -s "$RAW/after.processes" ]] \
        || die "final Firecracker/daemon/CLI process delta is nonzero"
    [[ -z "$(find "$RUN_DIR" -type s -print -quit)" ]] \
        || die "final UDS delta is nonzero"
    [[ -z "$(find "$INSTANCES_DIR" -mindepth 1 -print -quit)" ]] \
        || die "final storage delta is nonzero"
    if [[ -d "$STATE_DIR/runtime" ]]; then
        [[ -z "$(find "$STATE_DIR/runtime" -mindepth 1 -print -quit)" ]] \
            || die "final runtime delta is nonzero"
    fi
}

assert_guest_file() {
    local mode=$1
    local transport=$2
    local sandbox_id=$3
    local guest_path=$4
    local expected_file=$5
    local tag=$6

    run_cli 0 "$transport" "$mode" - "$tag read" \
        "blazectl --$transport <endpoint> --output $mode read $sandbox_id $guest_path" \
        -- read "$sandbox_id" "$guest_path"
    assert_empty_stderr "$tag read"
    if [[ "$mode" == "json" ]]; then
        jq -er '.data_b64' "$RUN_STDOUT" \
            | base64 --decode >"$RAW/$tag-decoded.bin"
        cmp -s "$RAW/$tag-decoded.bin" "$expected_file" \
            || die "$tag JSON read bytes differ"
    else
        cmp -s "$RUN_STDOUT" "$expected_file" \
            || die "$tag text read bytes differ"
    fi
    record_assertion "read" "$tag read" "$transport" "$RUN_SAFE_ARGV" \
        "exit=0; binary payload exactly matches the fixed input"
}

LIVE_CAPTURED=0
LIFECYCLE_CHECKPOINTS=()

run_lifecycle() {
    local mode=$1
    local transport=$2
    local sandbox_id=$3
    local tag=$4
    local guest_path="/tmp/blaze-firecracker-$tag.bin"
    local original="$RAW/$tag-original.bin"
    local mutated="$RAW/$tag-mutated.bin"
    local checkpoint

    printf '\000\001\177\200\377BLAZE-Firecracker-ORIGINAL\012' >"$original"
    printf '\377\000BLAZE-Firecracker-MUTATED\200\012' >"$mutated"

    run_cli 0 "$transport" "$mode" - "$tag create" \
        "blazectl --$transport <endpoint> --output $mode create $sandbox_id" \
        -- create "$sandbox_id"
    assert_empty_stderr "$tag create"
    if [[ "$mode" == "json" ]]; then
        jq -e --arg id "$sandbox_id" \
            '.id == $id and .status == "running"' "$RUN_STDOUT" >/dev/null
    else
        assert_text_field ID "$sandbox_id"
        assert_text_field STATUS running
    fi
    record_assertion "create" "$tag create" "$transport" "$RUN_SAFE_ARGV" \
        "exit=0; id matches and status is running"

    run_cli 0 "$transport" "$mode" - "$tag list" \
        "blazectl --$transport <endpoint> --output $mode list" \
        -- list
    assert_empty_stderr "$tag list"
    if [[ "$mode" == "json" ]]; then
        jq -e --arg id "$sandbox_id" \
            'any(.[]; .id == $id)' "$RUN_STDOUT" >/dev/null
    else
        grep -F "$sandbox_id" "$RUN_STDOUT" >/dev/null
    fi
    record_assertion "list" "$tag list" "$transport" "$RUN_SAFE_ARGV" \
        "exit=0; active sandbox is present"

    run_cli 0 "$transport" "$mode" - "$tag exec" \
        "blazectl --$transport <endpoint> --output $mode exec $sandbox_id <fixed-command>" \
        -- exec "$sandbox_id" "printf blaze-firecracker-exec-ok"
    assert_empty_stderr "$tag exec"
    if [[ "$mode" == "json" ]]; then
        jq -e \
            '.exit_code == 0 and .stdout == "blaze-firecracker-exec-ok" and .stderr == ""' \
            "$RUN_STDOUT" >/dev/null
    else
        cmp -s "$RUN_STDOUT" <(printf 'blaze-firecracker-exec-ok') \
            || die "$tag exec stdout differs"
    fi
    record_assertion "exec" "$tag exec" "$transport" "$RUN_SAFE_ARGV" \
        "exit=0; fixed guest command output matches"

    if [[ "$LIVE_CAPTURED" -eq 0 ]]; then
        assert_live_resources
        LIVE_CAPTURED=1
    fi

    run_cli 0 "$transport" "$mode" - "$tag write" \
        "blazectl --$transport <endpoint> --output $mode write $sandbox_id $guest_path --file <input>" \
        -- write "$sandbox_id" "$guest_path" --file "$original"
    assert_empty_stderr "$tag write"
    if [[ "$mode" == "json" ]]; then
        jq -e '.status == "ok"' "$RUN_STDOUT" >/dev/null
    else
        assert_text_field STATUS ok
    fi
    record_assertion "write" "$tag write" "$transport" "$RUN_SAFE_ARGV" \
        "exit=0; binary host file accepted"

    assert_guest_file "$mode" "$transport" "$sandbox_id" "$guest_path" \
        "$original" "$tag-original"

    run_cli 0 "$transport" "$mode" - "$tag checkpoint" \
        "blazectl --$transport <endpoint> --output $mode checkpoint $sandbox_id" \
        -- checkpoint "$sandbox_id"
    assert_empty_stderr "$tag checkpoint"
    if [[ "$mode" == "json" ]]; then
        checkpoint=$(jq -er \
            'select(.status == "checkpointed") | .checkpoint_id' "$RUN_STDOUT")
    else
        assert_text_field STATUS checkpointed
        checkpoint=$(awk -F '\t' '$1 == "CHECKPOINT" {print $2}' "$RUN_STDOUT")
    fi
    [[ "$checkpoint" =~ ^ckpt-[0-9a-f-]{36}$ ]] \
        || die "$tag checkpoint identifier is invalid"
    LIFECYCLE_CHECKPOINTS+=("$sandbox_id:$checkpoint")
    record_assertion "checkpoint" "$tag checkpoint" "$transport" "$RUN_SAFE_ARGV" \
        "exit=0; status=checkpointed and checkpoint id is valid"

    run_cli 0 "$transport" "$mode" - "$tag checkpoints" \
        "blazectl --$transport <endpoint> --output $mode checkpoints $sandbox_id" \
        -- checkpoints "$sandbox_id"
    assert_empty_stderr "$tag checkpoints"
    grep -F "$checkpoint" "$RUN_STDOUT" >/dev/null \
        || die "$tag checkpoint list omits the committed checkpoint"
    record_assertion "checkpoints" "$tag checkpoints" "$transport" "$RUN_SAFE_ARGV" \
        "exit=0; committed checkpoint is listed"

    run_cli 0 "$transport" "$mode" "$mutated" "$tag mutate" \
        "blazectl --$transport <endpoint> --output $mode write $sandbox_id $guest_path --file -" \
        -- write "$sandbox_id" "$guest_path" --file -
    assert_empty_stderr "$tag mutate"
    if [[ "$mode" == "json" ]]; then
        jq -e '.status == "ok"' "$RUN_STDOUT" >/dev/null
    else
        assert_text_field STATUS ok
    fi
    record_assertion "write" "$tag mutate" "$transport" "$RUN_SAFE_ARGV" \
        "exit=0; binary stdin mutation accepted"
    assert_guest_file "$mode" "$transport" "$sandbox_id" "$guest_path" \
        "$mutated" "$tag-mutated"

    run_cli 0 "$transport" "$mode" - "$tag rollback" \
        "blazectl --$transport <endpoint> --output $mode rollback $sandbox_id $checkpoint" \
        -- rollback "$sandbox_id" "$checkpoint"
    assert_empty_stderr "$tag rollback"
    if [[ "$mode" == "json" ]]; then
        jq -e --arg checkpoint "$checkpoint" \
            '.status == "rolledback" and .checkpoint == $checkpoint' \
            "$RUN_STDOUT" >/dev/null
    else
        assert_text_field STATUS rolledback
        assert_text_field CHECKPOINT "$checkpoint"
    fi
    record_assertion "rollback" "$tag rollback" "$transport" "$RUN_SAFE_ARGV" \
        "exit=0; rollback selects the committed checkpoint"
    assert_guest_file "$mode" "$transport" "$sandbox_id" "$guest_path" \
        "$original" "$tag-rollback"

    run_cli 0 "$transport" "$mode" - "$tag prune" \
        "blazectl --$transport <endpoint> --output $mode prune-checkpoints $sandbox_id" \
        -- prune-checkpoints "$sandbox_id"
    assert_empty_stderr "$tag prune"
    if [[ "$mode" == "json" ]]; then
        jq -e '.status == "pruned"' "$RUN_STDOUT" >/dev/null
    else
        assert_text_field STATUS pruned
    fi
    record_assertion "prune-checkpoints" "$tag prune" "$transport" "$RUN_SAFE_ARGV" \
        "exit=0; checkpoint pruning reports pruned"

    run_cli 0 "$transport" "$mode" - "$tag hibernate" \
        "blazectl --$transport <endpoint> --output $mode hibernate $sandbox_id" \
        -- hibernate "$sandbox_id"
    assert_empty_stderr "$tag hibernate"
    if [[ "$mode" == "json" ]]; then
        jq -e '.status == "hibernated"' "$RUN_STDOUT" >/dev/null
    else
        assert_text_field STATUS hibernated
    fi
    record_assertion "hibernate" "$tag hibernate" "$transport" "$RUN_SAFE_ARGV" \
        "exit=0; status=hibernated"

    run_cli 0 "$transport" "$mode" - "$tag resume" \
        "blazectl --$transport <endpoint> --output $mode resume $sandbox_id" \
        -- resume "$sandbox_id"
    assert_empty_stderr "$tag resume"
    if [[ "$mode" == "json" ]]; then
        jq -e '.status == "running"' "$RUN_STDOUT" >/dev/null
    else
        assert_text_field STATUS running
    fi
    record_assertion "resume" "$tag resume" "$transport" "$RUN_SAFE_ARGV" \
        "exit=0; status=running"

    run_cli 0 "$transport" "$mode" - "$tag resumed-exec" \
        "blazectl --$transport <endpoint> --output $mode exec $sandbox_id <fixed-command>" \
        -- exec "$sandbox_id" "printf blaze-firecracker-exec-ok"
    assert_empty_stderr "$tag resumed-exec"
    if [[ "$mode" == "json" ]]; then
        jq -e \
            '.exit_code == 0 and .stdout == "blaze-firecracker-exec-ok" and .stderr == ""' \
            "$RUN_STDOUT" >/dev/null
    else
        cmp -s "$RUN_STDOUT" <(printf 'blaze-firecracker-exec-ok') \
            || die "$tag resumed exec stdout differs"
    fi
    record_assertion "exec" "$tag resumed-exec" "$transport" "$RUN_SAFE_ARGV" \
        "exit=0; guest command succeeds after resume"

    run_cli 0 "$transport" "$mode" - "$tag pool-status" \
        "blazectl --$transport <endpoint> --output $mode pool-status" \
        -- pool-status
    assert_empty_stderr "$tag pool-status"
    [[ -s "$RUN_STDOUT" ]] || die "$tag pool status is empty"
    record_assertion "pool-status" "$tag pool-status" "$transport" "$RUN_SAFE_ARGV" \
        "exit=0; bounded pool status is present"

    run_cli 0 "$transport" "$mode" - "$tag cleanup-devices" \
        "blazectl --$transport <endpoint> --output $mode cleanup-devices" \
        -- cleanup-devices
    assert_empty_stderr "$tag cleanup-devices"
    if [[ "$mode" == "json" ]]; then
        jq -e '.destroyed >= 0' "$RUN_STDOUT" >/dev/null
    else
        awk -F '\t' '$1 == "DESTROYED" {found=1} END {exit found ? 0 : 1}' \
            "$RUN_STDOUT"
    fi
    record_assertion "cleanup-devices" "$tag cleanup-devices" "$transport" \
        "$RUN_SAFE_ARGV" "exit=0; cleanup result is structured"

    run_cli 0 "$transport" "$mode" - "$tag kill" \
        "blazectl --$transport <endpoint> --output $mode kill $sandbox_id" \
        -- kill "$sandbox_id"
    assert_empty_stderr "$tag kill"
    if [[ "$mode" == "json" ]]; then
        jq -e '.status == "ok"' "$RUN_STDOUT" >/dev/null
    else
        assert_text_field STATUS ok
    fi
    record_assertion "kill" "$tag kill" "$transport" "$RUN_SAFE_ARGV" \
        "exit=0; sandbox is destroyed"

    run_cli 0 "$transport" "$mode" - "$tag final-list" \
        "blazectl --$transport <endpoint> --output $mode list" \
        -- list
    assert_empty_stderr "$tag final-list"
    if grep -F "$sandbox_id" "$RUN_STDOUT" >/dev/null; then
        die "$tag destroyed sandbox remains in public list"
    fi
    record_assertion "list" "$tag final-list" "$transport" "$RUN_SAFE_ARGV" \
        "exit=0; destroyed sandbox is absent"
}

TREE=$(git -C "$SOURCE_ROOT" rev-parse HEAD^{tree})
SOURCE_ARCHIVE_SHA=$(git -C "$SOURCE_ROOT" archive "$CANDIDATE" \
    | sha256sum | awk '{print $1}')
BLAZED_SHA=$(hash_file "$BLAZED")
BLAZECTL_SHA=$(hash_file "$BLAZECTL")
FIRECRACKER_SHA=$(hash_file "$FIRECRACKER")
VMLINUX_SHA=$(hash_file "$VMLINUX")
ROOTFS_SHA=$(hash_file "$ROOTFS")
RESTRICTED_PATTERNS_SHA=$(hash_file "$RESTRICTED_PATTERNS")

set +e
env -i PATH="$PATH" LC_ALL=C LANG=C BLAZED_URL="$TCP_URL" \
    "$BLAZECTL" --version >"$RAW/version-flag.out" 2>"$RAW/version-flag.err"
RUN_EXIT=$?
set -e
[[ "$RUN_EXIT" -eq 0 && ! -s "$RAW/version-flag.err" ]] \
    || die "blazectl --version failed before daemon startup"
RUN_STDOUT="$RAW/version-flag.out"
RUN_STDERR="$RAW/version-flag.err"
record_assertion "version" "version flag before daemon" none \
    "blazectl --version" "exit=0; no daemon is available"

set +e
env -i PATH="$PATH" LC_ALL=C LANG=C BLAZED_URL="$TCP_URL" \
    "$BLAZECTL" version >"$RAW/version-command.out" 2>"$RAW/version-command.err"
RUN_EXIT=$?
set -e
[[ "$RUN_EXIT" -eq 0 && ! -s "$RAW/version-command.err" ]] \
    || die "blazectl version failed before daemon startup"
RUN_STDOUT="$RAW/version-command.out"
RUN_STDERR="$RAW/version-command.err"
record_assertion "version" "version command before daemon" none \
    "blazectl version" "exit=0; no daemon is available"

BLAZED_VERSION=$(env -i PATH="$PATH" LC_ALL=C LANG=C "$BLAZED" --version \
    | awk '{print $2}')
BLAZECTL_VERSION=$(awk '{print $2}' "$RAW/version-flag.out")
[[ -n "$BLAZED_VERSION" && "$BLAZED_VERSION" == "$BLAZECTL_VERSION" ]] \
    || die "release blazed and blazectl versions differ"

jq -n \
    --arg candidate "$CANDIDATE" \
    --arg tree "$TREE" \
    --arg arch "$(uname -m)" \
    --arg version "$BLAZECTL_VERSION" \
    --arg source_archive_sha256 "$SOURCE_ARCHIVE_SHA" \
    --arg blazed_sha256 "$BLAZED_SHA" \
    --arg blazectl_sha256 "$BLAZECTL_SHA" \
    --arg firecracker_sha256 "$FIRECRACKER_SHA" \
    --arg vmlinux_sha256 "$VMLINUX_SHA" \
    --arg rootfs_sha256 "$ROOTFS_SHA" \
    --arg restricted_patterns_sha256 "$RESTRICTED_PATTERNS_SHA" \
    '{
      candidate: $candidate,
      tree: $tree,
      dirty: false,
      os_family: "linux",
      arch: $arch,
      version: $version,
      source_archive_sha256: $source_archive_sha256,
      blazed_sha256: $blazed_sha256,
      blazectl_sha256: $blazectl_sha256,
      firecracker_sha256: $firecracker_sha256,
      vmlinux_sha256: $vmlinux_sha256,
      rootfs_sha256: $rootfs_sha256,
      restricted_patterns_sha256: $restricted_patterns_sha256,
      backend: "firecracker",
      mock: false,
      transports: ["uds", "tcp"]
    }' >"$EVIDENCE_ROOT/candidate.json"

capture_global_resources before

env -i PATH="$PATH" LC_ALL=C LANG=C NO_COLOR=1 CLICOLOR=0 \
    CLICOLOR_FORCE=0 TERM=dumb RUST_LOG=info \
    setsid "$BLAZED" daemon start --config "$CONFIG" \
    >"$DAEMON_LOG" 2>&1 &
DAEMON_PID=$!

UDS_HEALTH="$RAW/health-uds.json"
TCP_HEALTH="$RAW/health-tcp.json"
healthy=0
for _ in $(seq 1 240); do
    if curl --silent --show-error --max-time 1 --unix-socket "$SOCKET" \
        http://localhost/v1/health >"$UDS_HEALTH" 2>/dev/null \
        && curl --silent --show-error --max-time 1 \
            "$TCP_URL/v1/health" >"$TCP_HEALTH" 2>/dev/null \
        && jq -e --arg version "$BLAZECTL_VERSION" \
            '.status == "ok" and .backend == "firecracker" and .version == $version' \
            "$UDS_HEALTH" >/dev/null \
        && jq -e --arg version "$BLAZECTL_VERSION" \
            '.status == "ok" and .backend == "firecracker" and .version == $version' \
            "$TCP_HEALTH" >/dev/null; then
        healthy=1
        break
    fi
    kill -0 "$DAEMON_PID" 2>/dev/null \
        || die "blazed exited before Firecracker health became ready"
    sleep 0.25
done
[[ "$healthy" -eq 1 ]] || die "Firecracker health was not ready within 60 seconds"

UDS_ID=00000000-0000-4000-8000-000000000401
TCP_ID=00000000-0000-4000-8000-000000000402
NONZERO_ID=00000000-0000-4000-8000-000000000403
MISSING_ID=00000000-0000-4000-8000-000000000404
KILL_ALL_IDS=(
    00000000-0000-4000-8000-000000000405
    00000000-0000-4000-8000-000000000406
    00000000-0000-4000-8000-000000000407
)

run_lifecycle json uds "$UDS_ID" uds-json
run_lifecycle text tcp "$TCP_ID" tcp-text

run_cli 0 uds json - "nonzero create" \
    "blazectl --socket <uds> --output json create $NONZERO_ID" \
    -- create "$NONZERO_ID"
assert_empty_stderr "nonzero create"
jq -e --arg id "$NONZERO_ID" '.id == $id and .status == "running"' \
    "$RUN_STDOUT" >/dev/null
record_assertion "create" "nonzero create" uds "$RUN_SAFE_ARGV" \
    "exit=0; nonzero fixture sandbox is running"

run_cli 7 uds json - "guest nonzero" \
    "blazectl --socket <uds> --output json exec $NONZERO_ID <fixed-nonzero-command>" \
    -- exec "$NONZERO_ID" "exit 7"
assert_empty_stderr "guest nonzero"
jq -e '.exit_code == 7' "$RUN_STDOUT" >/dev/null
record_assertion "exec" "guest nonzero" uds "$RUN_SAFE_ARGV" \
    "exit=7; daemon guest exit is preserved"

run_cli 0 uds json - "nonzero kill" \
    "blazectl --socket <uds> --output json kill $NONZERO_ID" \
    -- kill "$NONZERO_ID"
assert_empty_stderr "nonzero kill"
jq -e '.status == "ok"' "$RUN_STDOUT" >/dev/null
record_assertion "kill" "nonzero kill" uds "$RUN_SAFE_ARGV" \
    "exit=0; nonzero fixture sandbox is destroyed"

run_cli 1 uds json - "structured daemon error" \
    "blazectl --socket <uds> --output json kill $MISSING_ID" \
    -- kill "$MISSING_ID"
assert_empty_stdout "structured daemon error"
jq -e --arg id "$MISSING_ID" \
    '.code == "not_found" and .sandbox_id == $id' "$RUN_STDERR" >/dev/null
record_assertion "kill" "structured daemon error" uds "$RUN_SAFE_ARGV" \
    "exit=1; stderr is one structured not_found diagnostic"

for id in "${KILL_ALL_IDS[@]}"; do
    run_cli 0 tcp json - "kill-all create" \
        "blazectl --url <tcp-loopback> --output json create $id" \
        -- create "$id"
    assert_empty_stderr "kill-all create"
    jq -e --arg id "$id" '.id == $id and .status == "running"' \
        "$RUN_STDOUT" >/dev/null
    record_assertion "create" "kill-all create" tcp "$RUN_SAFE_ARGV" \
        "exit=0; kill-all fixture sandbox is running"
done

run_cli 0 tcp json - "kill all three" \
    "blazectl --url <tcp-loopback> --output json kill --all" \
    -- kill --all
assert_empty_stderr "kill all three"
jq -e \
    --arg first "${KILL_ALL_IDS[0]}" \
    --arg second "${KILL_ALL_IDS[1]}" \
    --arg third "${KILL_ALL_IDS[2]}" \
    '.total == 3
      and ((.succeeded | sort) == ([$first, $second, $third] | sort))
      and (.failed | length) == 0
      and (.unfinished | length) == 0' \
    "$RUN_STDOUT" >/dev/null
record_assertion "kill --all" "kill all three" tcp "$RUN_SAFE_ARGV" \
    "exit=0; all three targets are attempted and destroyed"

run_cli 0 tcp json - "kill-all final list" \
    "blazectl --url <tcp-loopback> --output json list" \
    -- list
assert_empty_stderr "kill-all final list"
jq -e 'length == 0' "$RUN_STDOUT" >/dev/null
record_assertion "list" "kill-all final list" tcp "$RUN_SAFE_ARGV" \
    "exit=0; public list is empty"

stop_daemon || die "blazed did not stop cleanly"

run_cli 1 uds json - "UDS unavailable after stop" \
    "blazectl --socket <uds> --output json list" \
    -- list
assert_empty_stdout "UDS unavailable after stop"
jq -e '.code == "connect_error"' "$RUN_STDERR" >/dev/null
record_assertion "list" "UDS unavailable after stop" uds "$RUN_SAFE_ARGV" \
    "exit=1; structured connect_error after daemon reap"

run_cli 1 tcp json - "TCP unavailable after stop" \
    "blazectl --url <tcp-loopback> --output json list" \
    -- list
assert_empty_stdout "TCP unavailable after stop"
jq -e '.code == "connect_error"' "$RUN_STDERR" >/dev/null
record_assertion "list" "TCP unavailable after stop" tcp "$RUN_SAFE_ARGV" \
    "exit=1; structured connect_error after daemon reap"

assert_final_resource_delta

EXPECTED_IDS=("$UDS_ID" "$TCP_ID" "$NONZERO_ID" "${KILL_ALL_IDS[@]}")
for id in "${EXPECTED_IDS[@]}"; do
    state_file="$STATE_DIR/$id/state.json"
    [[ -f "$state_file" ]] || die "expected destroyed tombstone is missing"
    jq -e --arg id "$id" \
        '.id == $id and .state == "destroyed" and .operation == null' \
        "$state_file" >/dev/null
done
[[ ! -e "$STATE_DIR/$MISSING_ID" ]] \
    || die "not-found request created persistent metadata"
STATE_FILES=$(find "$STATE_DIR" -name state.json -type f | wc -l | tr -d ' ')
[[ "$STATE_FILES" -eq "${#EXPECTED_IDS[@]}" ]] \
    || die "persistent sandbox metadata contains unexpected entries"

for entry in "${LIFECYCLE_CHECKPOINTS[@]}"; do
    id=${entry%%:*}
    checkpoint=${entry#*:}
    [[ -d "$STATE_DIR/checkpoints/$id/$checkpoint" ]] \
        || die "committed checkpoint metadata is missing"
    jq -e --arg id "$id" --arg checkpoint "$checkpoint" \
        '.id == $checkpoint and .sandbox_id == $id and .backend == "firecracker"' \
        "$STATE_DIR/checkpoints/$id/$checkpoint/metadata.json" >/dev/null \
        || die "checkpoint metadata is not bound to the Firecracker sandbox"
done

[[ "$COMMAND_INDEX" -eq "$EXPECTED_COMMAND_ASSERTIONS" ]] \
    || die "CLI assertion count does not match the frozen Firecracker matrix"

(
    cd "$STATE_DIR"
    while IFS= read -r relative; do
        hash=$(sha256sum "$relative" | awk '{print $1}')
        printf '%s  %s\n' "$hash" "$relative"
    done < <(find . -type f -printf '%P\n' | LC_ALL=C sort)
) >"$EVIDENCE_ROOT/persistent-metadata.sha256"

DAEMON_LOG_BYTES=$(stat -c '%s' "$DAEMON_LOG")
((DAEMON_LOG_BYTES <= MAX_DAEMON_LOG_BYTES)) \
    || die "daemon log exceeded the Firecracker review bound"
for sensitive_path in "$SOURCE_ROOT" "$BLAZED" "$BLAZECTL" "$FIRECRACKER" \
    "$VMLINUX" "$ROOTFS" "$RESTRICTED_PATTERNS" "$EVIDENCE_ROOT"; do
    if grep -aF "$sensitive_path" "$DAEMON_LOG" >/dev/null; then
        die "daemon log disclosed an input/source/evidence path"
    fi
done
for guest_value in BLAZE-Firecracker-ORIGINAL BLAZE-Firecracker-MUTATED \
    blaze-firecracker-exec-ok /tmp/blaze-firecracker-uds-json.bin /tmp/blaze-firecracker-tcp-text.bin; do
    if grep -aF "$guest_value" "$DAEMON_LOG" >/dev/null; then
        die "daemon log disclosed fixed guest content"
    fi
done

jq -n \
    --argjson live_firecracker true \
    --argjson live_netns true \
    --argjson live_tap_veth true \
    --argjson live_nat true \
    --argjson live_uds true \
    --argjson live_listener true \
    --argjson live_open_fds true \
    --argjson final_process_delta 0 \
    --argjson final_netns_delta 0 \
    --argjson final_tap_veth_delta 0 \
    --argjson final_nat_delta 0 \
    --argjson final_uds_delta 0 \
    --argjson final_listener_delta 0 \
    --argjson final_open_fd_delta 0 \
    --argjson final_storage_delta 0 \
    --argjson final_runtime_delta 0 \
    --argjson persistent_sandboxes "$STATE_FILES" \
    '{
      live: {
        firecracker: $live_firecracker,
        netns: $live_netns,
        tap_veth: $live_tap_veth,
        nat: $live_nat,
        uds: $live_uds,
        listener: $live_listener,
        open_fds: $live_open_fds
      },
      final_delta: {
        process: $final_process_delta,
        netns: $final_netns_delta,
        tap_veth: $final_tap_veth_delta,
        nat: $final_nat_delta,
        uds: $final_uds_delta,
        listener: $final_listener_delta,
        open_fds: $final_open_fd_delta,
        storage: $final_storage_delta,
        runtime: $final_runtime_delta
      },
      expected_persistent_metadata: {
        destroyed_sandboxes: $persistent_sandboxes,
        lifecycle_checkpoint_roots: 2
      }
    }' >"$EVIDENCE_ROOT/resource-summary.json"

CONFIDENTIALITY_SUMMARY="$WORK_ROOT/confidentiality-summary.json"
VERIFICATION_SUMMARY="$WORK_ROOT/verification-summary.json"
SHA256_SUMS="$WORK_ROOT/SHA256SUMS"
SCANNED_FILES=$(find "$RAW" "$EVIDENCE_ROOT" -type f | wc -l | tr -d ' ')
SCANNED_FILES=$((SCANNED_FILES + 3))

jq -n \
    --argjson scanned_files "$SCANNED_FILES" \
    '{
      deny: 0,
      scanned_files: $scanned_files,
      reviewed_restricted_pattern_hits: 0,
      raw_runtime_output_copied_to_evidence: false,
      source_or_input_path_in_daemon_log: 0,
      guest_content_in_daemon_log: 0,
      public_values: "fixed synthetic identifiers, redacted argv and hashes only"
    }' >"$CONFIDENTIALITY_SUMMARY"

jq -n \
    --arg candidate "$CANDIDATE" \
    --arg tree "$TREE" \
    --argjson commands "$COMMAND_INDEX" \
    '{
      result: "PASS",
      evidence_level: "Firecracker",
      candidate: $candidate,
      tree: $tree,
      dirty: false,
      backend: "firecracker",
      mock: false,
      transports: ["uds", "tcp"],
      output_modes: ["json", "text"],
      binary_io: true,
      guest_nonzero: true,
      daemon_structured_error: true,
      connection_unavailable: true,
      resource_delta_zero: true,
      listener_delta_zero: true,
      open_fd_delta_zero: true,
      confidentiality_deny: 0,
      command_assertions: $commands
    }' >"$VERIFICATION_SUMMARY"

(
    cd "$EVIDENCE_ROOT"
    sha256sum candidate.json command-matrix.tsv
    printf '%s  confidentiality-summary.json\n' \
        "$(hash_file "$CONFIDENTIALITY_SUMMARY")"
    sha256sum persistent-metadata.sha256 resource-summary.json
    printf '%s  verification-summary.json\n' \
        "$(hash_file "$VERIFICATION_SUMMARY")"
) >"$SHA256_SUMS"

ACTUAL_SCANNED_FILES=0
while IFS= read -r -d '' scan_file; do
    ACTUAL_SCANNED_FILES=$((ACTUAL_SCANNED_FILES + 1))
    if grep -aEif "$RESTRICTED_PATTERNS" "$scan_file" >/dev/null; then
        die "reviewed restricted pattern matched runtime/evidence output"
    fi
done < <(
    find "$RAW" "$EVIDENCE_ROOT" -type f -print0
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

FINALIZED=1
printf 'PASS candidate=%s evidence=Firecracker backend=firecracker mock=false\n' \
    "$CANDIDATE"
