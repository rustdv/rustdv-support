# RustDV Support

Reusable debugging and simulator-integration crates for
[RustDV](https://github.com/rustdv/rustdv).

This repository holds host-side support code. Protocol and interface models
belong in [`rustdv-ip`](https://github.com/rustdv/rustdv-ip), while the core
executor, phases, and simulator backends remain in `rustdv` itself.

## Crates

- [`rustdv-debug`](debug/) is the simulator-neutral request, recording, watch,
  and asynchronous simulation-control layer. It has no RustDV or VPI
  dependency.
- [`rustdv-mcp`](mcp/) is the simulator-neutral testbench and MCP entry point.
  Its VPI service runs unchanged on Icarus or Verilator and selects a trace
  adapter only when the host advertises its output format.
- [`rustdv-mcp-fst`](mcp-fst/) owns Verilator's runtime-gated FST capture,
  decoding, and historical queries. It is separate from `rustdv-debug`.
- [`rustdv-mcp-verilator`](mcp-verilator/) preserves the previous import path
  for existing testbenches.

The MCP transport never owns or touches a VPI handle. Worker threads submit
bounded requests; the simulator thread services them at RustDV's settled
ReadOnly point. RustDV remains the only simulation scheduler.

## Documentation

- [Getting started](docs/getting-started.md) — dependencies, Rust testbench
  setup, Verilator RECORD mode, first MCP call, and a neutral-backend example
- [Backend-neutral debug API](docs/debug-api.md) — `SignalProvider`,
  owner-thread polling, requests, watches, recordings, and control integration
- [MCP tool reference](docs/mcp-tools.md) — every tool, argument, default,
  response, cursor, and control state
- [Recording and history](docs/recording-and-history.md) — full-design capture,
  internal-signal discovery, historical queries, limits, and cleanup
- [Troubleshooting](docs/troubleshooting.md) — visibility, timing, leases,
  linking, FST, query limits, and shutdown
- [Standalone MCP debug testbench](examples/mcp-debug-testbench/) — a small
  documentation-only Rust/SystemVerilog example

## MCP tools

The MCP server exposes:

- `simulation_status`, `list_hierarchy`, and `read_signal`
- `add_watch`, `list_watches`, and `remove_watch`
- `start_recording`, `recording_status`, `get_recording`,
  `stop_recording`, and `remove_recording`
- `recording_hierarchy`, `recording_value_at`, `recording_changes`,
  `recording_snapshot`, and `recording_backend_status`
- `control_status`, `pause_simulation`, `resume_simulation`,
  `run_until_time`, `run_until_predicate`, and `terminate_simulation`

`rustdv-debug` remains simulator- and trace-format-neutral: it contains no
Verilator, VPI, FST, or temporary-file types. Watches and run-until predicates
use VPI and share one provider snapshot per stable simulation time. Paused
services use bounded wall-clock waits, fail open if every client disconnects,
and also have a configurable inactivity lease.

## Verilator FST recording

The existing recording tools now control a private all-signal FST capture in a
trace-capable Verilator build. An optional `signals` list defines the
backward-compatible change-only projection returned by `get_recording`; it
does not limit what is physically captured. The additional history tools query
the same captured trace for hierarchy, values, changes, and snapshots.

The FST is an internal backing store rather than a user waveform artifact. It
is created in a restricted temporary directory only when recording starts,
and its path is not part of successful MCP responses. It is deleted by
`remove_recording` and removed with the session. A filesystem-cleanup error
can include the affected private path for diagnosis. Active history queries
rotate private segments at the same settled time; status polling does not
rotate or consume a segment.

`TraceRecordingConfig` bounds recordings, projected signals, indexed
hierarchy, decoded events and bytes, response sizes, capture duration, and
active-query segments. `max_bytes_per_recording` is a stop threshold checked
after flushing: a segment can exceed it by the bytes Verilator emitted since
the preceding check. Exact duration and segment limits provide the hard
termination bounds; the byte threshold provides an earlier size-triggered
stop, not a filesystem quota. The legacy projection streams each newly closed
segment once into its fixed-capacity ring, so its retained memory and response
size are bounded independently of the number of captured changes. Filtered
history queries separately enforce total change-scan and decoded-byte budgets.
Limit-triggered stops and reasons are reported through status. Start, flush,
stop, and automatic deadlines are serviced synchronously at RustDV's settled
ReadOnly point.

The simulator must be built in RustDV `record` mode (`--trace-fst`). `fast`
mode remains completely uninstrumented; recording calls return a structured
unsupported-capability error instead of silently falling back to thousands of
VPI reads.

On Icarus, the same Rust testbench uses the selected-signal recording ring in
`rustdv-debug`; it does not claim to provide an all-signal trace. Live reads,
watches, pause/resume, and run-until remain available through the shared VPI
service. A future Icarus-native trace provider can implement the same history
operations without changing the testbench's MCP entry point.

The server is intentionally unauthenticated and therefore rejects non-loopback
bind addresses.

## Verification

The normal workspace gate is:

```sh
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps --locked
```

The real integration gate requires Verilator 5.050, Icarus, and a RustDV checkout with
the settled service and runtime trace APIs:

```sh
RUSTDV_ROOT=/path/to/rustdv \
  bash tests/debug-control-verilator/run.sh
```

It first proves that a FAST executable rejects recording, then drives real
HTTP/MCP calls against a trace-capable build. The test covers exact time and
predicate stops, explicit-signal projection, an internal signal unavailable
through VPI, automatic duration limiting, history boundaries after stop,
pause/resume/terminate, private-file cleanup, and lockfile preservation. The
same portable debug test also runs against Icarus with selected-signal history.

## Dependency status

The workspace temporarily pins an exact commit on `teabone113/rustdv:master`
that includes the simulator-neutral trace API proposed in
[`rustdv/rustdv#10`](https://github.com/rustdv/rustdv/pull/10). The pin keeps
the support crates reproducible while that API is under upstream review.
Once the core API is merged and released, switch to the released RustDV
version before publishing the support crates to crates.io.

See [CONTRIBUTING.md](CONTRIBUTING.md) for repository boundaries and test
requirements.
