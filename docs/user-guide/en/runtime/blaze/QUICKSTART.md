# Blaze CLI User Guide

[中文版](../../../zh/runtime/blaze/QUICKSTART.md)

Blaze gives administrators one local control surface for sandbox lifecycle,
guest I/O, checkpoints, and warm-pool maintenance. The `blazed` binary owns
all orchestration state and data-plane resources; `blazectl` is a separate
HTTP client that only performs the documented remote operations.

## Installation

Blaze is a Linux system component. The initial access boundary is the service
administrator/root boundary because the default Unix socket is not
world-accessible.

### ANOLISA component manager

When the configured component catalog contains Blaze, use system mode:

```bash
sudo anolisa --install-mode system install blaze
sudo systemctl enable --now blazed.service
```

Catalog availability depends on the configured Linux distribution repository;
the source tree does not publish a package by itself.

### RPM package

On a supported RPM-based distribution whose repository contains Blaze:

```bash
sudo yum install blaze
sudo systemctl enable --now blazed.service
```

### Build the client from source

Builds must run on Linux:

```bash
cd src/blaze
cargo build -p blazectl --release --locked
./target/release/blazectl version
```

Use the source-built client with the installed daemon:

```bash
sudo ./target/release/blazectl --socket /run/blaze/api.sock list
```

Do not build or test Blaze on macOS or Windows.

## Endpoint Selection

`blazectl` uses this precedence:

| Priority | Selection | Result |
|---------:|-----------|--------|
| 1 | `--socket <absolute-path>` | HTTP over the selected Unix socket |
| 1 | `--url <http-origin>` | HTTP over explicit TCP |
| 2 | `BLAZED_URL=<http-origin>` | TCP when neither endpoint flag is present |
| 3 | No endpoint option | `/run/blaze/api.sock` |

`--socket` and `--url` conflict. An endpoint flag overrides `BLAZED_URL`.
The socket path must be absolute. A TCP URL must be a plain `http` origin with
no user information, path, query, or fragment. Selecting `--url` does not
start or reconfigure the daemon TCP listener.

```bash
sudo blazectl --socket /run/blaze/api.sock list
blazectl --url http://127.0.0.1:14159 list
BLAZED_URL=http://127.0.0.1:14159 blazectl list
```

TCP has no built-in authentication or TLS. Bind it only to an approved trusted
management network and enforce access outside Blaze.

Client bounds are fixed: connection establishment has a 5 seconds deadline,
the complete request has a 30 seconds deadline, and a collected response may
not exceed 32 MiB. Mutation requests are not automatically retried.

## Output and Streams

Success output resolves as `--output`, then `BLAZECTL_OUTPUT`, then `text`.
Only `text` and `json` are accepted.

```bash
sudo blazectl --output text list
sudo blazectl --output json list
sudo env BLAZECTL_OUTPUT=json blazectl list
```

- Text success uses stable fields and deterministic ordering.
- JSON success writes exactly one JSON value plus a newline to stdout.
- Runtime diagnostics go only to stderr and do not contaminate stdout.
- In text mode, `exec` forwards guest stdout to stdout and guest stderr to
  stderr. In JSON mode, it writes one response object to stdout and leaves
  stderr empty on success.
- Text `read` writes raw decoded bytes to stdout without UTF-8 conversion or
  an added newline.
- JSON `read` returns `data_b64`; it does not place arbitrary bytes in a JSON
  string.
- Errors do not reflect endpoint values, local input paths, oversized response
  bodies, or unstructured daemon response bodies.

## Command Reference

The remote surface contains exactly 14 canonical commands:

| Command | HTTP request | Behavior |
|---------|--------------|----------|
| `blazectl create [ID] [--template NAME]` | `POST /v1/sandboxes` | Creates a sandbox; `ID` is an optional UUID |
| `blazectl exec <ID> <CMD> [--cwd PATH]` | `POST /v1/sandboxes/{id}/exec` | Passes one opaque command string to the guest agent |
| `blazectl list` | `GET /v1/sandboxes` | Lists sandboxes in stable ID order; alias: `ls` |
| `blazectl kill <ID>`<br>`blazectl kill --all` | `DELETE /v1/sandboxes/{id}`; `--all` first uses `GET /v1/sandboxes` | Destroys one or all listed sandboxes; alias: `rm` for one ID |
| `blazectl hibernate <ID>` | `POST /v1/sandboxes/{id}/hibernate` | Snapshots and stops the selected backend |
| `blazectl checkpoint <ID>` | `POST /v1/sandboxes/{id}/checkpoint` | Commits a new checkpoint |
| `blazectl rollback <ID> <CHECKPOINT>` | `POST /v1/sandboxes/{id}/rollback/{checkpoint}` | Restores a `ckpt-<uuid>` checkpoint |
| `blazectl checkpoints <ID>` | `GET /v1/sandboxes/{id}/checkpoints` | Lists checkpoints and HEAD reachability |
| `blazectl prune-checkpoints <ID>` | `POST /v1/sandboxes/{id}/checkpoints/prune` | Removes branches unreachable from HEAD |
| `blazectl resume <ID>` | `POST /v1/sandboxes/{id}/resume` | Restores a hibernated backend |
| `blazectl cleanup-devices` | `POST /v1/pool/cleanup` | Drains pool resources and triggers refill |
| `blazectl pool-status` | `GET /v1/pool/status` | Shows ready, capacity, and pending pool values |
| `blazectl read <ID> <PATH>` | `POST /v1/sandboxes/{id}/read` | Reads one absolute guest path |
| `blazectl write <ID> <PATH> [--file PATH\|-]` | `POST /v1/sandboxes/{id}/write` | Writes binary data to one absolute guest path |

Local version surfaces:

| Command | Daemon access | Result |
|---------|---------------|--------|
| `blazectl version` | None | Stable text or JSON client version |
| `blazectl --version` | None | Standard local version line |

There are no `blazectl` template, policy, hook, metrics, admin, or
daemon-lifecycle commands.

### Guest exec and binary file I/O

`exec` never invokes a local shell. The caller supplies one `CMD` argument,
which the client carries as JSON data to the guest agent. Quote it in the
calling shell when it contains spaces.

`write` selects its input as follows:

| Invocation | Input |
|------------|-------|
| `--file <path>` | Binary bytes from that local file |
| `--file -` | Binary bytes from stdin |
| No `--file`, non-terminal stdin | Binary bytes from stdin |
| No `--file`, terminal stdin | Immediate error without contacting the daemon |

Empty input is valid. Input larger than 16 MiB is rejected before transport.
`read` and `write` remain binary-safe in both output modes.

```bash
ID=00000000-0000-4000-8000-000000000001
sudo blazectl create "$ID"
sudo blazectl exec "$ID" "printf sentinel"
printf '\000\001\002' > /tmp/blaze-sentinel.bin
sudo blazectl write "$ID" /tmp/sentinel.bin --file /tmp/blaze-sentinel.bin
sudo blazectl read "$ID" /tmp/sentinel.bin > /tmp/blaze-sentinel.out
cmp /tmp/blaze-sentinel.bin /tmp/blaze-sentinel.out
printf sentinel | sudo blazectl write "$ID" /tmp/sentinel.txt --file -
sudo blazectl kill "$ID"
```

### kill --all

`kill --all` first obtains a stable sandbox ID set, then attempts every target
with no more than 50 delete requests in flight. A failed target does not stop
the remaining attempts. The summary deterministically separates `succeeded`,
`failed`, `unfinished`, and `total`; any failed or unfinished target returns
exit 1. A fully successful summary goes to stdout; a partial-failure summary
goes to stderr and leaves stdout empty.

## Exit Codes

| Exit | Meaning | Stream contract |
|-----:|---------|-----------------|
| 0 | Successful remote operation or local version output | Result on stdout; stderr empty |
| 1 | Endpoint, connection, protocol, daemon, input, cancellation, output, or batch failure | No false success on stdout; bounded diagnostic on stderr |
| 2 | Invalid or conflicting CLI arguments | Clap usage diagnostic on stderr |
| 1–125 | Guest command exited nonzero | Text preserves guest streams; JSON emits one response value; process exit matches the guest code |

Guest exit codes outside 1–125 are mapped to exit 1 with a stable diagnostic.
For `kill --all`, any failed or unfinished target returns exit 1 after all
possible targets have been attempted.

## Limitations and Security

- Blaze supports Linux x86_64 and aarch64 only.
- The default `/run/blaze/api.sock` is mode `0660` with service ownership.
  Initial use therefore requires the service administrator/root boundary.
- `blazectl` does not start `blazed`, alter daemon configuration, change socket
  permissions, or enable a TCP listener.
- TCP is plain HTTP without built-in authentication or TLS.
- Firecracker operations require a suitable Linux/KVM host and the daemon's
  configured mount, network namespace, tap, and firewall privileges.
- The client intentionally excludes template, policy, hook, metrics, admin,
  and daemon-lifecycle operations even though the daemon HTTP API may expose
  additional endpoints.

## Troubleshooting

- For a missing socket or connection failure, verify
  `systemctl status blazed.service` and the selected endpoint.
- For UDS permission errors, run within the approved administrator/root
  boundary; do not make the socket world-writable.
- For machine processing, select `--output json` and parse stdout only.
- Treat any exit other than 0 as failure; for `exec`, also interpret the
  documented guest exit-code range.
