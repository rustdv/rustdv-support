# Recording and history

The Verilator adapter uses a private FST as the primary MCP history store. It
captures every signal retained by the trace-capable model over a short,
runtime-selected window without reading thousands of VPI handles on every
simulation step.

## Choose RECORD, not DEBUG

Use `RUSTDV_VERILATOR_MODE=record` for MCP history:

- trace instrumentation is compiled into the model;
- no FST is opened before `start_recording`;
- capture begins and ends at settled ReadOnly points;
- private FST segments are queried through MCP and then deleted.

`debug` is the conventional waveform mode: it writes a user-visible FST from
time zero. `fast` and `inspect` are deliberately uninstrumented and reject MCP
recording.

## A practical failure-window workflow

Suppose a long test normally fails around time 1,000,000:

1. Run quickly without an active trace.
2. Pause shortly before the suspect region.
3. Arm an all-signal capture.
4. Run through the failure by time or predicate.
5. Stop capture.
6. Discover and query signals without rerunning.

### Position the simulation

`run_until_time` takes an absolute simulation time:

```json
{
  "name": "run_until_time",
  "arguments": {"target_time_steps": 990000}
}
```

Poll `control_status` until `mode` is `paused` and `pause_reason` is
`target_reached`.

### Start full-design capture

With no `signals`, capture is still all-signal but no legacy projection is
created. Use the recorded hierarchy/value/change/snapshot tools:

```json
{
  "name": "start_recording",
  "arguments": {"name": "failure_window"}
}
```

If an existing client expects `get_recording`, provide a small projection:

```json
{
  "name": "start_recording",
  "arguments": {
    "name": "failure_window",
    "signals": ["input_valid", "input_ready", "output_valid", "output_ready"],
    "capacity": 2048
  }
}
```

The projection list does not restrict the FST. An internal signal that was not
named here can still be discovered and queried later.

Only one FST recording may be active at a time. Multiple stopped recordings
can be retained up to the configured limit.

### Run through the event of interest

Arm a predicate directly from the pause:

```json
{
  "name": "run_until_predicate",
  "arguments": {
    "all_high": ["error_seen"],
    "all_low": [],
    "timeout_steps": 50000
  }
}
```

For buses, `all_high` means every bit is one. If the desired condition is
“non-zero,” expose a one-bit predicate in the RTL or use an appropriate status
bit.

### Stop at the settled boundary

```json
{
  "name": "stop_recording",
  "arguments": {"name": "failure_window"}
}
```

The returned status gives exact start/end times. No sample after the stop-time
ReadOnly point is included.

## Discover an internal signal after capture

First list the recorded hierarchy:

```json
{
  "name": "recording_hierarchy",
  "arguments": {
    "name": "failure_window",
    "scope": "TOP.codec",
    "max_depth": 6,
    "max_results": 2000
  }
}
```

Use the returned signal path exactly. Recorded paths and live VPI paths are
related but not guaranteed to use the same root spelling.

Read its value at the stop time:

```json
{
  "name": "recording_value_at",
  "arguments": {
    "name": "failure_window",
    "signal": "TOP.codec.entropy_state",
    "time_steps": 1017420
  }
}
```

The response distinguishes `requested_time_steps` from
`sampled_time_steps`. A signal retains its most recent earlier value when it
did not change at the requested time.

## Query transitions

```json
{
  "name": "recording_changes",
  "arguments": {
    "name": "failure_window",
    "signal": "TOP.codec.entropy_state",
    "start_time_steps": 1017000,
    "end_time_steps": 1017420,
    "limit": 256
  }
}
```

If `next_cursor` is not null, repeat the same query with that cursor. Keep the
same signal and time range across pages.

## Query a scope snapshot

```json
{
  "name": "recording_snapshot",
  "arguments": {
    "name": "failure_window",
    "scope": "TOP.codec.entropy_decoder",
    "time_steps": 1017412,
    "max_signals": 512
  }
}
```

Snapshots are useful when the likely failing block is known but the relevant
register is not. If `truncated` is true, narrow the scope or raise the requested
limit within the server-configured maximum.

## Legacy projection and cursors

The optional projection is a fixed-capacity change-only ring:

- the first sample is the value at arm time;
- a new sample is added only when a selected value changes;
- cursors are monotonic and are not reused;
- when capacity is exceeded, the oldest sample is dropped;
- `dropped` counts evicted samples;
- requesting an evicted cursor sets `truncated` and starts at the oldest
  retained sample.

Example page request:

```json
{
  "name": "get_recording",
  "arguments": {
    "name": "failure_window",
    "cursor": 4096,
    "limit": 128
  }
}
```

## Active queries and private segments

An active history query must read a closed FST. The service therefore closes
the current private segment and immediately starts another at the same settled
time. This does not create a history gap, but every rotation consumes one of
the configured segment slots. Status polling does not rotate a segment, so an
active recording's projection counters are current only through its reported
`projection_through_time_steps`. Stopping materializes them through the exact
capture end.

For long captures, avoid repeatedly querying full history while the simulation
is still running. Stop first, then investigate freely.

## Resource limits

`TraceRecordingConfig` bounds:

- recordings and active segments;
- explicit projection signals and ring capacity;
- capture duration;
- indexed hierarchy entries and path length;
- hierarchy/snapshot/change response sizes;
- total filtered-query change scans and decoded bytes;
- each value retained by the streaming projection.

`max_duration_steps` is an exact simulator-time deadline, even if the RTL has
no nearer event. `max_segments_per_recording` is a hard rotation limit.

`max_bytes_per_recording` is an early stop threshold checked after an FST
flush, not a filesystem quota. A segment can overshoot it by the trace data
emitted since the previous check. Status reports observed bytes,
`limit_reached`, and `limit_reason`.

## Cleanup and privacy

Private FST paths are not part of successful MCP results. They live in a
restricted temporary directory and are deleted when:

- `remove_recording` succeeds;
- the Verilator debug session is dropped.

Termination or raw-channel disconnection first stops an active capture; the
private directory is removed when the session is subsequently dropped.

Explicit removal reports filesystem deletion failures instead of silently
forgetting retained bytes; that diagnostic error can include the affected
private path. Use `recording_backend_status` to confirm that `recordings` and
`retained_bytes` return to zero.
