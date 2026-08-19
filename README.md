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
  loopback-only streamable-HTTP MCP server and adapts Verilator VPI handles to
  `rustdv-debug`.

The MCP transport never owns or touches a VPI handle. Worker threads submit
bounded requests; the simulator thread services them at RustDV's settled
ReadOnly point. RustDV remains the only simulation scheduler.

## MCP tools

The Verilator server exposes:

- `simulation_status`, `list_hierarchy`, and `read_signal`
- `add_watch`, `list_watches`, and `remove_watch`
- `start_recording`, `recording_status`, `get_recording`,
  `stop_recording`, and `remove_recording`
- `control_status`, `pause_simulation`, `resume_simulation`,
  `run_until_time`, `run_until_predicate`, and `terminate_simulation`

Watch count, signals per watch, recording count, signals per recording, ring
capacity, request work, and response sizes are configurable and bounded.
Recordings are change-only rings with simulation timestamps and monotonic
cursors. Watches, recordings, and run-until predicates share one provider
snapshot per stable simulation time. Paused services use bounded wall-clock
waits, fail open if every client disconnects, and also have a configurable
inactivity lease.

The server is intentionally unauthenticated and therefore rejects non-loopback
bind addresses.

## Verification

The normal workspace gate is:

```sh
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps
```

The real integration gate requires Verilator 5.050 and a RustDV checkout with
the settled service API and INSPECT mode:

```sh
RUSTDV_ROOT=/path/to/rustdv \
  bash tests/debug-control-verilator/run.sh
```

It drives the complete controller flow through actual HTTP/MCP calls, checks
exact time and predicate stops, recording history, pause/resume/terminate, and
proves that INSPECT mode emits no FST.

## Dependency status

The workspace temporarily pins an exact commit from the RustDV contribution
branch because the required core API is not in a tagged release yet. Once the
core change is merged and released, this repository should switch to the
released RustDV version before publishing `rustdv-mcp-verilator` to crates.io.

See [CONTRIBUTING.md](CONTRIBUTING.md) for repository boundaries and test
requirements.
