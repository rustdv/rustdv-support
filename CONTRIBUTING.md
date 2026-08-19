# Contributing

Thank you for helping improve RustDV's reusable support tooling.

## Repository boundary

Contributions belong here when they provide host-side debugging, inspection,
transport, reporting, or simulator integration that can be reused across DUTs.

- Simulation phases, scheduling, and simulator backends belong in `rustdv`.
- Protocol and interface models belong in `rustdv-ip`.
- Product-specific register maps, media formats, and test scenarios belong in
  the consuming project.

Support integrations must not add a second simulation scheduler. Simulator API
access, including all VPI handle access, must remain on the simulator thread.
Worker communication and paused waits must be bounded and cancellation-aware.

## Layout

Each public crate has its own top-level directory, documentation, license
files, unit tests, and package metadata. Simulator-level fixtures live under
`tests/` and exercise the public API rather than private test hooks.

## Pull requests

Before requesting review, run:

```sh
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps
```

Changes to `rustdv-mcp-verilator`, debug control, or simulator-thread
boundaries must also pass the real Verilator smoke:

```sh
RUSTDV_ROOT=/path/to/rustdv \
  bash tests/debug-control-verilator/run.sh
```

New public behavior needs focused unit coverage and, where scheduling or VPI is
involved, an end-to-end simulator regression.
