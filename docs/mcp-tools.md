# MCP tool reference

The RustDV Verilator MCP server exposes live VPI inspection, asynchronous
simulation control, watches, and FST-backed history through streamable HTTP.

The endpoint is printed by `McpServer::endpoint()` and normally looks like
`http://127.0.0.1:9393/mcp`. The server accepts loopback addresses only.

## Request and response envelope

A raw tool call uses this JSON-RPC shape:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": {
    "name": "read_signal",
    "arguments": {"path": "count"}
  }
}
```

Set both HTTP headers:

```text
Content-Type: application/json
Accept: application/json, text/event-stream
```

The domain response is JSON encoded inside `result.content[0].text`. Raw
clients therefore parse the HTTP JSON and then parse the text field. An MCP
SDK normally handles the outer envelope.

MCP clients discover the available schemas with `tools/list`. A raw request is:

```json
{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}
```

Successful neutral-debug responses use a tagged shape:

```json
{
  "response": "signal",
  "value": {"path": "debug_probe.count", "width": 4, "binary": "0011"}
}
```

FST responses use the same `response`/`value` convention. A domain error is:

```json
{"message":"recording 'failure_window' does not exist"}
```

Signal values are binary strings. A Verilator build is two-state in normal
operation; other simulator adapters may return four-state digits.

## Live inspection

### `simulation_status`

Arguments: none.

Returns the DUT root, current simulation time, service poll count, watch and
recording counts, and control status. When the FST backend is enabled, the
`value` object also contains `trace`, with capture capability and configured
limits.

```json
{"name":"simulation_status","arguments":{}}
```

### `list_hierarchy`

| Argument | Type | Default | Meaning |
|---|---:|---:|---|
| `path` | string | `""` | Scope relative to the DUT root |
| `max_depth` | integer | `1` | Maximum hierarchy depth |
| `max_results` | integer | `256` | Requested response limit, clamped by server configuration |

Returns `response: "hierarchy"` with entries containing `path`, `kind`
(`scope` or `logic`), and optional `width`. Live hierarchy depth is capped at
eight by the neutral debug service.

```json
{
  "name": "list_hierarchy",
  "arguments": {"path": "", "max_depth": 3, "max_results": 100}
}
```

### `read_signal`

| Argument | Type | Required | Meaning |
|---|---:|---:|---|
| `path` | string | yes | Live VPI path relative to the DUT root |

Returns `response: "signal"` with `path`, `width`, and `binary`.

```json
{"name":"read_signal","arguments":{"path":"count"}}
```

Live inspection can see only signals exposed through VPI by the selected
Verilator build policy. Use recorded hierarchy queries to inspect an internal
signal that was traced but not exposed through VPI.

## Watches

A watch is evaluated once per stable simulation time. It is a hit only when
every bit of every `all_high` signal is `1` and every bit of every `all_low`
signal is `0`.

### `add_watch`

| Argument | Type | Default | Meaning |
|---|---:|---:|---|
| `name` | string | required | Watch identifier; adding an existing name replaces that watch |
| `all_high` | string array | `[]` | Signals required to be all ones |
| `all_low` | string array | `[]` | Signals required to be all zeros |

At least one signal is required. Returns `response: "watch"` with sample,
hit, run-length, time, and read-error counters.

```json
{
  "name": "add_watch",
  "arguments": {
    "name": "idle_cycles",
    "all_high": ["ready"],
    "all_low": ["valid"]
  }
}
```

### `list_watches`

Arguments: none. Returns `response: "watches"` with all current statistics.

### `remove_watch`

Argument: `name` string. Returns `response: "removed"` with a boolean.

## Recording lifecycle

In the standard Verilator integration these tools use runtime-gated FST. In a
transport without the FST channel, the same five tools fall back to
`rustdv-debug`'s targeted provider recording.

### `start_recording`

| Argument | Type | Default | Meaning |
|---|---:|---:|---|
| `name` | string | required | Unique recording name |
| `signals` | string array | `[]` | Optional default projection for `get_recording` |
| `capacity` | integer | `4096` | Maximum retained change-only projection samples |

In FST mode the physical capture always includes every signal retained by the
trace-capable model. `signals` affects only the legacy projection returned by
`get_recording`; it does not narrow the FST capture.

Returns `response: "recording_status"`. FAST and INSPECT builds return a
structured unsupported-capability error.

```json
{
  "name": "start_recording",
  "arguments": {
    "name": "failure_window",
    "signals": ["count", "done"],
    "capacity": 512
  }
}
```

### `recording_status`

Argument: `name` string.

Returns activity, retained/dropped projection samples, next cursor, read
errors, capture times, bytes, signal and segment counts, and any automatic
limit reason. While capture is active, projection counters are exact only
through `projection_through_time_steps`; status polling refreshes file bytes
but deliberately does not rotate the active segment. A stopped recording is
materialized through its exact end time. `signal_count` can remain zero until
the first segment has been closed and indexed.

### `get_recording`

| Argument | Type | Default | Meaning |
|---|---:|---:|---|
| `name` | string | required | Recording name |
| `cursor` | integer or null | oldest retained | First monotonic sample cursor requested |
| `limit` | integer | `256` | Page size, clamped by configuration |

Returns `response: "recording"` with `samples`, `next_cursor`, `dropped`, and
`truncated`. Each sample has a monotonic cursor, absolute simulation time, and
projected values. If the requested cursor has already fallen out of the ring,
`truncated` is true and the page begins at the oldest retained sample.

### `stop_recording`

Argument: `name` string. Stops at the current settled ReadOnly point and
retains history. No later samples are appended.

### `remove_recording`

Argument: `name` string. Removes status/history and deletes private FST
segments. Returns `response: "removed"` with a boolean.

## FST history queries

These tools require a Verilator debug session constructed with the trace
backend. They query private closed FST segments. Successful tool results do not
contain filesystem paths; a filesystem-cleanup error can include the affected
private path for diagnosis.

### `recording_hierarchy`

| Argument | Type | Default | Meaning |
|---|---:|---:|---|
| `name` | string | required | Recording name |
| `scope` | string | `""` | Recorded scope path |
| `max_depth` | integer | `1` | Maximum hierarchy depth |
| `max_results` | integer | `256` | Requested result limit |

Returns recorded scopes and signals. Use the returned full path in subsequent
history queries. Recorded hierarchy depth is capped at 32.

```json
{
  "name": "recording_hierarchy",
  "arguments": {
    "name": "failure_window",
    "scope": "",
    "max_depth": 8,
    "max_results": 1000
  }
}
```

### `recording_value_at`

| Argument | Type | Required | Meaning |
|---|---:|---:|---|
| `name` | string | yes | Recording name |
| `signal` | string | yes | Recorded signal path or unambiguous alias |
| `time_steps` | integer | yes | Absolute simulation time |

Returns the most recent captured value at or before the requested time and
reports both requested and sampled times.

```json
{
  "name": "recording_value_at",
  "arguments": {
    "name": "failure_window",
    "signal": "TOP.debug_probe.hidden_state",
    "time_steps": 1250
  }
}
```

### `recording_changes`

| Argument | Type | Default | Meaning |
|---|---:|---:|---|
| `name` | string | required | Recording name |
| `signal` | string | required | Recorded signal path |
| `start_time_steps` | integer or null | capture start | Inclusive absolute start |
| `end_time_steps` | integer or null | capture end | Inclusive absolute end |
| `cursor` | integer or null | `0` | Offset into this filtered change sequence |
| `limit` | integer | `256` | Page size |

Returns `changes`, `next_cursor`, and `truncated`. Reuse the same name, signal,
and time range when following `next_cursor`. The cursor is an offset into that
filtered result, not a persistent recording cursor. On a continuation page,
`truncated: true` means earlier matching changes were intentionally omitted by
the nonzero cursor; it does not mean the FST lost them.

### `recording_snapshot`

| Argument | Type | Default | Meaning |
|---|---:|---:|---|
| `name` | string | required | Recording name |
| `scope` | string | `""` | Recorded scope |
| `time_steps` | integer | required | Absolute simulation time |
| `max_signals` | integer | `256` | Requested signal limit |

Returns the most recent value for each selected signal at or before the
requested time. `truncated` reports that more signals existed in the scope.

### `recording_backend_status`

Arguments: none. Returns FST availability, active recording, retained bytes,
and the capture/query limits exposed by `TraceSummary`.

## Simulation control

### `control_status`

Arguments: none. Returns:

- `mode`: `running`, `paused`, `run_until_time`, `run_until_predicate`, or
  `terminated`
- current absolute `simulation_time_steps`
- active target/predicate/timeout
- `pause_reason`
- `last_error`

Pause reasons include `requested`, `target_reached`, `predicate_matched`,
`predicate_timeout`, `predicate_read_error`, `deadline_missed`,
`lease_expired`, and `controller_disconnected`.

### `pause_simulation`

Arguments: none. Pauses at the current settled ReadOnly stable point and
returns control status.

### `resume_simulation`

Arguments: none. Releases a held pause and returns control status.

### `run_until_time`

Argument: required absolute `target_time_steps` integer. A target in the past
is rejected or reported as a missed deadline; this is not a relative delay.
The command releases a held pause itself.

```json
{
  "name": "run_until_time",
  "arguments": {"target_time_steps": 5000}
}
```

### `run_until_predicate`

| Argument | Type | Default | Meaning |
|---|---:|---:|---|
| `all_high` | string array | `[]` | Signals required to be all ones |
| `all_low` | string array | `[]` | Signals required to be all zeros |
| `timeout_steps` | integer | required | Relative simulation-time timeout |

At least one signal is required. The command releases a held pause and stops
on a match, exact timeout, or signal-read error.

```json
{
  "name": "run_until_predicate",
  "arguments": {
    "all_high": ["done"],
    "all_low": [],
    "timeout_steps": 1000
  }
}
```

### `terminate_simulation`

Arguments: none. Sets sticky termination state. The opted-in service loop
returns `DebugServiceExit::Terminated`, closes any active trace, and lets the
RustDV test finish normally.
