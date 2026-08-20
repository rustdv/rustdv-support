# Troubleshooting

## Recording says the model was not built with `--trace-fst`

Cause: the executable was built in `fast` or `inspect` mode.

Fix: rebuild with:

```sh
RUSTDV_VERILATOR_MODE=record
```

This is intentional. FAST remains entirely uninstrumented; recording does not
fall back to reading every VPI signal.

## A live signal is missing, but it appears in recorded hierarchy

Live tools (`read_signal`, watches, predicates) use VPI visibility. Recorded
history uses every signal retained by the FST-capable model. These are
different visibility surfaces.

Fix one of the following:

- query it through `recording_value_at`, `recording_changes`, or
  `recording_snapshot` after capture;
- expose it deliberately in a Verilator `.vlt` control file for live VPI use;
- keep it as a top-level port when it is part of the DUT interface.

Do not enable blanket `--public-flat-rw` on a large performance build merely
to inspect one internal signal.

## A recorded internal signal is missing

“All signals” means all signals retained and instrumented by Verilator. A
signal optimized away during elaboration cannot be recovered from FST.

Fix: preserve/select the signal using an appropriate Verilator control file,
then rebuild the RECORD model.

## The simulation passed the target before the command arrived

Cause: `resume_simulation` and `run_until_*` were sent as separate requests to
a very fast simulation.

Fix: while paused, issue `run_until_time` or `run_until_predicate` directly.
Both commands release the pause themselves and arm the stop atomically at one
settled service point.

## `run_until_time` stopped with `deadline_missed`

`target_time_steps` is an absolute time, not a relative delay. A target behind
the current time cannot be reached exactly. A target equal to the current time
pauses immediately with `target_reached`.

Fix: call `control_status`, read `simulation_time_steps`, and choose a larger
absolute target.

## A predicate timed out unexpectedly

Check these details:

- `timeout_steps` is relative to the time at which the predicate is armed;
- every bit of an `all_high` signal must be `1`;
- every bit of an `all_low` signal must be `0`;
- read failures pause with `predicate_read_error`, not ordinary timeout;
- the signal must be visible through VPI.

For “bus is non-zero,” provide a one-bit RTL status predicate instead of
placing the whole bus in `all_high`.

## The simulation resumed while I was paused

Held pauses have a wall-clock inactivity lease. The default is 30 seconds.
Requests such as `control_status` renew the lease. If the transport becomes
inactive, expiry releases the pause so a simulation cannot remain wedged
indefinitely. Dropping every raw `DebugClient` is a separate channel-disconnect
case and also fails open.

Fix: keep the MCP client active, poll status while thinking, or increase
`DebugSessionConfig::pause_inactivity_timeout` for interactive work.

## The MCP request timed out

Possible causes:

- the simulator has not reached another stable point;
- the debug task or simulator has exited.

A full bounded queue returns an immediate `debug request queue is full` error
rather than a timeout.

The service can process requests while holding a pause. While running, it
needs RTL events, VPI timers, control deadlines, or trace deadlines to reach
future service points. A minimal interactive DUT should provide a clock or
another event source.

`McpServerConfig::request_timeout` is the deadline for the simulator to claim a
request, not a hard execution limit. Increasing it can help when stable points
are infrequent. Once a request is claimed, it completes atomically and the HTTP
worker waits for the result; narrow an expensive history query rather than
expecting this timeout to interrupt it.

## The server refuses its bind address

The MCP server is unauthenticated and accepts loopback addresses only. Use
`127.0.0.1`, `::1`, or port zero on a loopback address:

```rust
let bind = "127.0.0.1:0".parse().unwrap();
```

Do not expose this endpoint through a proxy or port-forward.

## A raw HTTP response looks double-encoded

The outer object is the MCP/JSON-RPC response. `result.content[0].text` is a
JSON string containing the typed RustDV response. Parse both layers.

MCP SDKs normally return the content item directly; raw `curl`, `reqwest`, or
similar clients must decode the text themselves.

## `get_recording` says there is no default projection

Cause: `start_recording` was called without a `signals` list.

The all-signal FST still exists. Use:

- `recording_hierarchy`
- `recording_value_at`
- `recording_changes`
- `recording_snapshot`

If backward-compatible pages are required, start a new recording with a small
explicit `signals` list.

## Recording history is truncated

For `get_recording`, the fixed-capacity projection ring evicted samples older
than the requested cursor. `dropped` reports the total evictions and
`truncated: true` reports that the requested beginning is unavailable.

Fix: increase projection `capacity`, request pages more frequently, or query
the FST directly with a time range.

For snapshots, `truncated` means the requested response limit was smaller than
the number of matching signals. Hierarchy responses are plain arrays without a
truncation flag; if the result reaches `max_results`, narrow the scope or
increase the requested limit within configured bounds.

## A history query exceeds scan or decoded-byte limits

Narrow the signal, scope, or time range and paginate changes. Limits protect
the simulator thread from unbounded filtered queries and protect the MCP
response from uncontrolled growth.

The legacy projection uses a separate streaming path: it scans each newly
closed segment once, retains only its fixed-capacity ring, and independently
bounds the width of any retained value.

## The recording exceeded its byte threshold

`max_bytes_per_recording` is checked after flushing, so it is a stop threshold
rather than a filesystem quota. Bytes written since the previous check can
cause an overshoot. Exact duration and segment limits still guarantee capture
termination.

Inspect `recording_status.limit_reached`, `limit_reason`, `bytes`, and
`end_time_steps`. Reduce the duration or select a smaller Verilator trace set
when disk use matters more than complete internal visibility.

## Too many active-query segments

Every active history query rotates the current private FST at the same settled
time. Repeated active queries can reach `max_segments_per_recording`.

Fix: let the short capture finish, call `stop_recording`, and then perform the
bulk of the investigation on the closed history.

## Direct runtime trace control reports the wrong phase

The low-level `rustdv::sim::verilator_trace` operations must run on the
simulator thread at settled ReadOnly. The MCP adapter already enforces this.

For custom code, wrap the operation:

```rust
let status = rustdv::service_read_only(|| {
    rustdv::sim::verilator_trace::status()
}).await;
```

Do not call trace start/flush/stop from an HTTP or MCP worker thread.

## FST startup reports that the destination cannot be created

For conventional DEBUG mode, ensure the parent directory exists and is
writable. RustDV validates the exact destination before handing it to
Verilator, because Verilator 5.050's `isOpen()` does not prove its underlying
file stream opened successfully.

MCP RECORD mode manages private temporary paths automatically.

## The native library fails to link on macOS

The simulator-loaded `cdylib` must leave VPI symbols unresolved. Add the
macOS `dynamic_lookup` rustflags shown in [Getting started](getting-started.md).

Keep `rustdv-vpi-stubs` as a dev-dependency only. Linking the panicking stubs
into the release `cdylib` prevents it from resolving real VPI symbols from the
simulator.

## Values do not show X propagation

Verilator is a two-state simulator. X randomization can expose some
initialization mistakes, but it does not provide four-state propagation.

Use Icarus or another four-state simulator as the framework/reference backend
for X/Z-sensitive behavior. Use Verilator as the fast functional backend.

## Shutdown did not call `final()`

Finish the interactive service with `terminate_simulation`. The service stops
an active capture, returns `DebugServiceExit::Terminated`, and allows the
RustDV test and Verilator host to shut down normally.

Killing the process externally cannot provide the same cleanup guarantee.
