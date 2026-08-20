# Getting started

This guide adds a local MCP debug server to a RustDV testbench, runs it with a
trace-capable Verilator model, and controls the simulation from an MCP client.

The server is deliberately loopback-only and unauthenticated. Do not proxy it
or bind it to a non-loopback address.

## Prerequisites

- Rust 1.89 or newer
- Verilator 5.050 or newer
- RustDV with `service_read_only` and runtime Verilator trace control
- `pkg-config` and LZ4 development files for an FST-capable build
- a Unix host; the supplied build and CI paths cover Linux and macOS

While the RustDV and support changes are under review, pin both Git
dependencies to reviewed commits. Once compatible releases exist, prefer
normal Cargo version requirements.

## 1. Configure the testbench crate

RustDV testbenches are loaded by the simulator as a native library, so include
`cdylib` in the crate types:

```toml
[lib]
crate-type = ["cdylib", "rlib"]

[dependencies]
rustdv = { git = "https://github.com/rustdv/rustdv.git" }
rustdv-mcp-verilator = { git = "https://github.com/rustdv/rustdv-support.git" }

[dev-dependencies]
# Unit-test builds need VPI symbols, but the simulator-loaded cdylib must
# resolve the real VPI symbols from Verilator instead of linking these stubs.
rustdv-vpi-stubs = { git = "https://github.com/rustdv/rustdv.git" }
```

On macOS the native library must leave VPI symbols unresolved until Verilator
loads it. Put this in `.cargo/config.toml`:

```toml
[target.aarch64-apple-darwin]
rustflags = ["-C", "link-arg=-undefined", "-C", "link-arg=dynamic_lookup"]

[target.x86_64-apple-darwin]
rustflags = ["-C", "link-arg=-undefined", "-C", "link-arg=dynamic_lookup"]
```

## 2. Start the debug service from RustDV

The simulator thread owns every VPI handle. `McpServer` runs HTTP on worker
threads, but workers submit requests through bounded channels; the RustDV task
services those requests only at settled ReadOnly points.

```rust
use rustdv::prelude::*;
use rustdv_mcp_verilator::{
    run_verilator_debug_service, verilator_debug_session_with_configs,
    DebugServiceExit, DebugSessionConfig, McpServer, McpServerConfig,
    TraceRecordingConfig,
};
use std::time::Duration;

#[cfg(test)]
use rustdv_vpi_stubs as _;

rustdv::vpi_bootstrap!();

#[rustdv::test]
async fn interactive_debug(ctx: RustdvCtx) -> Result<(), TestError> {
    // A clock or another RTL/VPI event source lets the service reach future
    // stable points while the simulation is running.
    let clk = ctx.dut().signal("clk")?;
    Clock::new(&clk, SimDuration::ns(2)).start();

    let debug_config = DebugSessionConfig {
        pause_inactivity_timeout: Duration::from_secs(30),
        ..DebugSessionConfig::default()
    };
    let trace_config = TraceRecordingConfig {
        max_duration_steps: 100_000,
        ..TraceRecordingConfig::default()
    };
    let (mut session, client) = verilator_debug_session_with_configs(
        ctx.dut(),
        debug_config,
        trace_config,
    )
    .map_err(|error| TestError::new(error.to_string()))?;

    // Port 0 asks the OS for an unused loopback port.
    let server = McpServer::start(
        client,
        McpServerConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            request_timeout: Duration::from_secs(5),
        },
    )
    .map_err(|error| TestError::new(error.to_string()))?;
    println!("RustDV MCP endpoint: {}", server.endpoint());

    match run_verilator_debug_service(&mut session).await {
        DebugServiceExit::Terminated => println!("debug session terminated"),
        DebugServiceExit::ControllerDisconnected => {
            println!("debug controller disconnected; simulation released")
        }
    }

    drop(server);
    Ok(())
}
```

Keep `McpServer` alive for as long as the service is running. Dropping it stops
the HTTP worker. Call `terminate_simulation` for a normal interactive shutdown.

## 3. Select the Verilator build mode

RustDV's Verilator host has four normal policies:

| Mode | VPI visibility | FST behavior | MCP recording |
|---|---|---|---|
| `fast` | Top-level ports | No instrumentation | Returns an unsupported-capability error |
| `inspect` | Ports plus selected `.vlt` signals | No instrumentation | Returns an unsupported-capability error |
| `record` | Ports plus selected `.vlt` signals | Runtime-gated instrumentation | Recommended for MCP history |
| `debug` | Ports plus selected `.vlt` signals | Writes an FST from time zero | Conventional waveform debugging |

Build the Rust testbench, then pass its native library, top module, build
directory, and HDL sources to RustDV's Verilator wrapper:

```sh
cargo build --release

RUSTDV_ROOT=/path/to/rustdv
RUSTDV_VERILATOR_MODE=record \
RUSTDV_VERILATOR_CONTROL_FILE="$PWD/inspect.vlt" \
  "$RUSTDV_ROOT/sim/run_verilator.sh" \
  "$PWD/target/release/librustdv_mcp_debug_example.so" \
  debug_probe \
  "$PWD/build/verilator" \
  "$PWD/probe.sv"
```

On macOS the library suffix is `.dylib`. The documentation sample includes a
portable wrapper that selects `.so` or `.dylib`.

`record` compiles tracing capability into the model but creates no FST until
`start_recording` is called. “All signals” means all signals retained and
instrumented by Verilator; an optimized-away signal cannot be recovered.

## 4. Call the MCP endpoint

Most MCP clients discover the tool schemas automatically. A raw HTTP client
can call a tool with JSON-RPC:

```sh
curl -sS "$RUSTDV_MCP_ENDPOINT" \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/call",
    "params": {
      "name": "pause_simulation",
      "arguments": {}
    }
  }'
```

The MCP result contains a text content item. That text is itself the typed
RustDV JSON response, for example:

```json
{
  "response": "control",
  "value": {
    "mode": "paused",
    "simulation_time_steps": 42,
    "target_time_steps": null,
    "predicate": null,
    "timeout_time_steps": null,
    "pause_reason": "requested",
    "last_error": null
  }
}
```

Domain failures such as an unknown signal are also returned in the text item:

```json
{"message":"no object named 'missing' in scope 'debug_probe'"}
```

See [MCP tool reference](mcp-tools.md) for every argument and response.

## 5. Use a race-free control sequence

The safest interactive pattern is:

1. `pause_simulation`
2. inspect live state or start a recording
3. arm `run_until_time` or `run_until_predicate` directly from the pause
4. poll `control_status` until it reports `paused`
5. inspect or stop recording
6. `terminate_simulation`

Do not issue `resume_simulation` followed by a separate run-until command on a
very fast DUT. The simulation may pass the desired point before the second
HTTP request is serviced. A run-until command already resumes a held pause.

## Using `rustdv-debug` without Verilator

`rustdv-debug` contains no RustDV, VPI, HTTP, Verilator, or FST dependency. A
different simulator adapter implements `SignalProvider`, owns `DebugSession`
on its simulator thread, and gives a cloned `DebugClient` to its transport:

```rust
use rustdv_debug::{
    DebugRequest, DebugResponse, DebugSession, SignalInfo, SignalProvider,
    SignalValue,
};

struct MySimulatorProvider;

impl SignalProvider for MySimulatorProvider {
    fn root_name(&self) -> String {
        "dut".to_owned()
    }

    fn list_hierarchy(
        &mut self,
        _path: &str,
        _max_depth: usize,
    ) -> Result<Vec<SignalInfo>, rustdv_debug::DebugError> {
        // Translate the simulator's hierarchy API into SignalInfo values.
        todo!()
    }

    fn read_signal(
        &mut self,
        _path: &str,
    ) -> Result<SignalValue, rustdv_debug::DebugError> {
        // Read through the simulator API on this owner thread.
        todo!()
    }
}

let (mut session, client) = DebugSession::new(MySimulatorProvider);

// A transport worker submits owned requests and never receives simulator
// handles. The call blocks until the owner thread services it.
let worker = std::thread::spawn(move || {
    client.request(DebugRequest::ReadSignal {
        path: "dut.ready".to_owned(),
    })
});

// Simulator callback / owner thread, after the model has settled. A bounded
// wait removes the race between spawning this illustrative worker and sending
// its first request.
let directive = session.poll_wait(
    simulation_time_steps,
    std::time::Duration::from_millis(100),
);

let response = worker.join().expect("debug worker panicked")?;
if let DebugResponse::Signal(value) = response {
    println!("{} = {}", value.path, value.binary);
}
```

The backend is responsible for mapping `DebugDirective::Advance`, `Hold`, and
`Terminate` into its simulator's scheduler without moving simulator handles to
worker threads. See [Backend-neutral debug API](debug-api.md) for the complete
adapter contract, bounded held-pause loop, and control/recording examples.
