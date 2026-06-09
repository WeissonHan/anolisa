# scan-prompt daemon protocol

This document defines the response contract for the daemon `scan-prompt` method.
It is the method-level contract for callers such as the CLI, Cosh hooks, and
other subprocess clients.

## Response layers

`scan-prompt` responses have three layers. Callers must handle them in this
order.

### 1. Transport failure: no `DaemonResponse`

The client did not receive a valid daemon response.

Examples:

- daemon socket does not exist
- daemon connection or request timeout
- daemon process exits before writing a response
- daemon response is not valid protocol data
- daemon response exceeds the configured size limit
- daemon runtime path cannot be resolved, for example when `XDG_RUNTIME_DIR` is
  not set and no explicit daemon socket path is provided

Caller behavior:

```python
try:
    response = client.call("scan-prompt", params=params)
except (DaemonClientError, DaemonRuntimePathError):
    # Daemon is unreachable, the protocol is broken, or the local runtime
    # socket path cannot be resolved.
    exit(1)
```

### 2. Daemon failure: `ok=false`

The daemon received the request, but the method could not be dispatched or
executed at the daemon/method boundary.

`ok=false` responses are not scan results. Callers must not parse `data` or
`stdout` as an action result.

Expected shape:

```json
{
  "id": "req-1",
  "ok": false,
  "data": {},
  "stdout": "",
  "stderr": "prompt scanner is not ready: status=loading",
  "exit_code": 1,
  "error": {
    "code": "unavailable",
    "message": "prompt scanner is not ready: status=loading"
  }
}
```

`scan-prompt` daemon failures include:

- unknown daemon method
- malformed daemon request
- prompt scanner runtime unavailable, including preload states such as
  `pending`, `downloading`, `loading`, or `degraded`
- daemon method timeout
- unexpected handler crash

Unexpected handler crashes should be logged by the daemon and returned as
`internal_error` without exposing arbitrary exception details to callers.

Caller behavior:

```python
if not response.ok:
    echo_error(response.stderr or response.error["message"])
    exit(response.exit_code or 1)
```

### 3. Action result: `ok=true`

The daemon successfully dispatched `scan-prompt`, and the handler returned a
scan action result.

For `ok=true`, `exit_code` is the action/CLI semantic exit code. It may be
non-zero even though daemon dispatch succeeded.

Expected successful scan shape:

```json
{
  "id": "req-1",
  "ok": true,
  "data": {
    "ok": true,
    "verdict": "pass"
  },
  "stdout": "{...same scan result as JSON...}",
  "stderr": "",
  "exit_code": 0
}
```

Expected scanner error result shape:

```json
{
  "id": "req-1",
  "ok": true,
  "data": {
    "ok": false,
    "verdict": "error",
    "summary": "Scanner error: model exploded"
  },
  "stdout": "{...same error verdict as JSON...}",
  "stderr": "Scanner error: model exploded",
  "exit_code": 1
}
```

`scan-prompt` action results include:

- `PASS`, `WARN`, and `DENY` scan verdicts: `ok=true`, `exit_code=0`
- backend validation failures, such as missing/empty `text` or invalid `mode`:
  `ok=true`, `exit_code=1`, with `stderr` describing the validation error
- scanner-produced `ERROR` verdicts: `ok=true`, `exit_code=1`, with structured
  error verdict data
- scanner domain exceptions that can be converted to an error verdict:
  `ok=true`, `exit_code=1`, with structured error verdict data

Caller behavior:

```python
if response.ok:
    rendered = render_action_output_if_present(response)
    if response.exit_code != 0:
        if not rendered:
            echo_error(response.stderr or "scan-prompt failed")
        exit(response.exit_code)
    exit(0)
```

Callers should render structured action output before exiting with a non-zero
action `exit_code`, so JSON consumers can still parse the error verdict. If an
action failure has no structured output, callers should display `stderr`.

## Request parameters

`scan-prompt` request params:

```json
{
  "text": "prompt text to scan",
  "mode": "fast|standard|strict",
  "source": "optional input source label"
}
```

Rules:

- `text` is required and must contain non-whitespace content.
- `mode` is optional and defaults to `standard`.
- `mode` must be one of `fast`, `standard`, or `strict`.
- `source` is optional and defaults to an empty string.

Invalid `text` or `mode` is handled by the prompt scan backend and returned as
an action failure: `ok=true`, `exit_code=1`.
