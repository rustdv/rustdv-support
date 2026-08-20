# MCP debug testbench example

This is a documentation-only, standalone example. It is deliberately not a
member of the repository workspace and is not part of CI.

It shows the minimum pieces needed to:

- load a RustDV `cdylib` into Verilator;
- start a loopback MCP server;
- keep the simulator servicing requests at settled ReadOnly points;
- enable runtime-gated FST recording in RECORD mode; and
- terminate cleanly through MCP.

## Files

- `Cargo.toml` — standalone testbench crate
- `.cargo/config.toml` — macOS unresolved-VPI linker policy
- `src/lib.rs` — RustDV test and MCP service
- `probe.sv` — small DUT with traced internal state
- `inspect.vlt` — deliberate live VPI visibility for
  `selected_debug_state`; `hidden_state` remains FST-only
- `run.sh` — build and launch wrapper

## Run

Set `RUSTDV_ROOT` to a compatible RustDV checkout, then:

```sh
RUSTDV_ROOT=/path/to/rustdv bash run.sh
```

The test prints an endpoint such as:

```text
RustDV MCP endpoint: http://127.0.0.1:54321/mcp
```

In another terminal, set that value and pause the simulation:

```sh
export RUSTDV_MCP_ENDPOINT=http://127.0.0.1:54321/mcp

curl -sS "$RUSTDV_MCP_ENDPOINT" \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{
    "jsonrpc":"2.0",
    "id":1,
    "method":"tools/call",
    "params":{"name":"pause_simulation","arguments":{}}
  }'
```

The control file makes `selected_debug_state` available to live VPI tools:

```json
{"name":"read_signal","arguments":{"path":"selected_debug_state"}}
```

`hidden_state` is intentionally absent from that VPI selection. Start a
recording and use `recording_hierarchy` to discover it in the all-signal FST,
then query it with `recording_value_at` or `recording_changes`.

Follow the workflow in
[Recording and history](../../docs/recording-and-history.md), then call
`terminate_simulation` to let the RustDV test and Verilator `final` block
finish normally.

The dependency declarations track repository default branches for readability.
For reproducible work, replace them with compatible released versions or exact
reviewed Git revisions.
