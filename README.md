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
- [`rustdv-mcp-verilator`](mcp-verilator/) exposes those operations through a
  loopback-only streamable-HTTP MCP server, adapts Verilator VPI handles to
  `rustdv-debug`, and provides runtime-gated all-signal FST history.

The MCP transport never owns or touches a VPI handle. Worker threads submit
bounded requests; the simulator thread services them at RustDV's settled
ReadOnly point. RustDV remains the only simulation scheduler.

## MCP tools

The Verilator server exposes:

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
never returned to an MCP client, deleted by `remove_recording`, and removed
with the session. Active history queries rotate private segments at the same
settled time; status polling does not rotate or consume a segment.

`TraceRecordingConfig` bounds recordings, projected signals, indexed
hierarchy, decoded events and bytes, response sizes, capture duration, and
active-query segments. `max_bytes_per_recording` is a stop threshold checked
after flushing: a segment can exceed it by the bytes Verilator emitted since
the preceding check. Limit-triggered stops and reasons are reported through
status. Start, flush, stop, and automatic deadlines are serviced synchronously
at RustDV's settled ReadOnly point.

The simulator must be built in RustDV `record` mode (`--trace-fst`). `fast`
mode remains completely uninstrumented; recording calls return a structured
unsupported-capability error instead of silently falling back to thousands of
VPI reads.

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

The real integration gate requires Verilator 5.050 and a RustDV checkout with
the settled service and runtime trace APIs:

```sh
RUSTDV_ROOT=/path/to/rustdv \
  bash tests/debug-control-verilator/run.sh
```

It first proves that a FAST executable rejects recording, then drives 33 real
HTTP/MCP calls against a trace-capable build. The test covers exact time and
predicate stops, explicit-signal projection, an internal signal unavailable
through VPI, automatic duration limiting, history boundaries after stop,
pause/resume/terminate, private-file cleanup, and lockfile preservation.

## Dependency status

The workspace temporarily pins the exact head of
[`rustdv/rustdv#10`](https://github.com/rustdv/rustdv/pull/10) because the
required core API is not in a tagged release yet. Once that change is merged
and released, this repository should switch to the released RustDV version
before publishing `rustdv-mcp-verilator` to crates.io.

See [CONTRIBUTING.md](CONTRIBUTING.md) for repository boundaries and test
requirements.
