# Backend-neutral debug API

`rustdv-debug` contains the simulator-independent state machine behind live
inspection, watches, bounded recordings, and simulation control. It has no
dependency on RustDV, VPI, Verilator, FST, HTTP, or MCP.

Use this crate when implementing another simulator adapter or another
transport. If you are using the supplied Verilator MCP adapter, start with
[Getting started](getting-started.md) instead; it already implements the
owner-thread and scheduling rules described here.

## Architecture

There are three roles:

1. A `SignalProvider` translates neutral hierarchy and value requests into the
   simulator's native API.
2. A `DebugSession<P>` owns that provider on the simulator thread and is
   polled only at settled, read-only stable points.
3. Cloneable `DebugClient` handles are given to HTTP, MCP, CLI, or other worker
   threads. They exchange owned request/response values through a bounded
   channel and never receive simulator handles.

```text
transport worker          bounded channel          simulator thread
DebugClient  ------------------------------------>  DebugSession<Provider>
             <------------------------------------  DebugResponse / DebugError
                                                      |
                                                      +--> simulator API
```

## Implement `SignalProvider`

Paths are strings in the adapter's chosen namespace. Keep returned paths
stable because watches, predicates, and recordings use them as identifiers.

```rust
use rustdv_debug::{
    DebugError, SignalInfo, SignalKind, SignalProvider, SignalValue,
};
use std::collections::BTreeMap;

struct ExampleProvider {
    values: BTreeMap<String, String>,
}

impl SignalProvider for ExampleProvider {
    fn root_name(&self) -> String {
        "dut".to_owned()
    }

    fn list_hierarchy(
        &mut self,
        path: &str,
        _max_depth: usize,
    ) -> Result<Vec<SignalInfo>, DebugError> {
        Ok(self
            .values
            .iter()
            .filter(|(name, _)| path.is_empty() || name.starts_with(path))
            .map(|(name, binary)| SignalInfo {
                path: name.clone(),
                kind: SignalKind::Logic,
                width: Some(binary.len() as u32),
            })
            .collect())
    }

    fn read_signal(&mut self, path: &str) -> Result<SignalValue, DebugError> {
        let binary = self
            .values
            .get(path)
            .cloned()
            .ok_or_else(|| DebugError::new(format!("unknown signal {path}")))?;
        Ok(SignalValue {
            path: path.to_owned(),
            width: binary.len() as u32,
            binary,
        })
    }
}
```

The provider methods are called only from `DebugSession::poll` or
`poll_wait`. They may use thread-affine simulator handles internally, but
those handles must never be placed in a `DebugClient`, request, or response.

## Create the session and client

```rust
use rustdv_debug::{DebugSession, DebugSessionConfig};
use std::{collections::BTreeMap, time::Duration};

let provider = ExampleProvider {
    values: BTreeMap::new(),
};
let config = DebugSessionConfig {
    request_queue_capacity: 64,
    max_requests_per_poll: 16,
    pause_inactivity_timeout: Duration::from_secs(30),
    ..DebugSessionConfig::default()
};

let (mut session, client) =
    DebugSession::with_config(provider, config)?;
let transport_client = client.clone();
```

Configuration bounds queued work, watches, signals per watch, retained
recordings, projection capacity, response page size, and hierarchy results.
Invalid zero limits are rejected when the session is created.

## Service one stable point

Call `poll` after the simulator has evaluated and settled the model. When it
returns `Hold`, keep that same stable point open and use bounded wall-clock
`poll_wait` calls so control requests can be serviced without advancing
simulation time.

```rust
use rustdv_debug::{DebugDirective, DebugSession, SignalProvider};
use std::time::Duration;

fn service_stable_point<P: SignalProvider>(
    session: &mut DebugSession<P>,
    simulation_time_steps: u64,
) -> DebugDirective {
    let mut directive = session.poll(simulation_time_steps);
    while directive == DebugDirective::Hold {
        directive = session.poll_wait(
            simulation_time_steps,
            Duration::from_millis(10),
        );
    }
    directive
}
```

Interpret the result as follows:

| Directive | Adapter action |
|---|---|
| `Advance` | Return from the stable point and continue simulator scheduling |
| `Hold` | Stay at the same stable point and call `poll_wait` with a bounded wall-clock timeout |
| `Terminate` | Stop the adapter service and let the simulator/testbench shut down normally |

For `run_until_time`, schedule a simulator callback at the exact absolute
`target_time_steps`. For `run_until_predicate`, schedule a callback at the
absolute `timeout_time_steps` as well as servicing normal simulator events.
The current deadlines are available from `session.control_status()`. If an
adapter jumps over a deadline, the session reports `deadline_missed` rather
than pretending it stopped exactly.

Active watches, provider-backed recordings, and a predicate share one cached
set of signal reads per simulation time. Repeated `poll_wait` calls at a held
time therefore do not resample the simulator.

## Send requests from a worker

`DebugClient::request` gives an unclaimed request up to two seconds by default.
`request_timeout` accepts an explicit wall-clock claim deadline. If the
simulator has not claimed the request by then, the request is cancelled. Once
the simulator claims it, the operation completes atomically and the worker
waits for that result even if it takes longer than the requested timeout. The
request queue is bounded; a full queue returns an error instead of growing
without limit.

```rust
use rustdv_debug::{DebugRequest, DebugResponse};
use std::{thread, time::Duration};

// `client` is the handle returned when the session was created.
let worker = thread::spawn(move || {
    client.request_timeout(
        DebugRequest::ReadSignal {
            path: "dut.ready".to_owned(),
        },
        Duration::from_secs(5),
    )
});

// Meanwhile, the simulator thread continues calling poll/poll_wait.
let response = worker.join().expect("debug worker panicked")?;
if let DebugResponse::Signal(value) = response {
    println!("{} = {}", value.path, value.binary);
}
```

Dropping every `DebugClient` disconnects the raw channel. A held session then
fails open instead of wedging the simulator. A transport such as the MCP
server normally retains its client; in that case the wall-clock pause lease is
the protection against an abandoned held pause.

## Watches

A watch counts stable-time samples for which all selected conditions hold:

```rust
use rustdv_debug::{DebugRequest, WatchSpec};

let request = DebugRequest::AddWatch {
    spec: WatchSpec {
        name: "idle_cycles".to_owned(),
        all_high: vec!["dut.ready".to_owned()],
        all_low: vec!["dut.valid".to_owned()],
    },
};
```

`all_high` means every bit of the value is `1`; `all_low` means every bit is
`0`. `WatchStats` reports complete samples, hits, consecutive-run statistics,
first/last hit times, and provider read errors. Use `ListWatches` and
`RemoveWatch` to inspect or remove watches.

## Provider-backed recordings

The neutral recorder samples an explicit signal list and retains only changes
in a fixed-capacity ring:

```rust
use rustdv_debug::DebugRequest;

let start = DebugRequest::StartRecording {
    name: "handshake".to_owned(),
    signals: vec!["dut.valid".to_owned(), "dut.ready".to_owned()],
    capacity: 1024,
};
```

The initial values are captured immediately when the request is serviced. A
new sample is appended only when the selected values change. Cursors are
monotonic; `RecordingPage::truncated` is set if a requested cursor has already
been evicted. `RecordingStatus`, `GetRecording`, `StopRecording`, and
`RemoveRecording` complete the lifecycle.

This recorder is intentionally simulator-neutral. The Verilator adapter maps
the same MCP recording tools to runtime-gated all-signal FST capture instead;
see [Recording and history](recording-and-history.md).

## Simulation control

Control requests are asynchronous state changes, not a second scheduler:

```rust
use rustdv_debug::{DebugRequest, PredicateSpec};

let pause = DebugRequest::Pause;
let exact_stop = DebugRequest::RunUntilTime {
    target_time_steps: 50_000,
};
let predicate_stop = DebugRequest::RunUntilPredicate {
    predicate: PredicateSpec {
        all_high: vec!["dut.done".to_owned()],
        all_low: vec!["dut.error".to_owned()],
    },
    timeout_steps: 10_000,
};
let terminate = DebugRequest::Terminate;
```

Run-until time is absolute. Predicate timeout is relative to the arm time.
Both commands release a held pause themselves. Inspect `ControlStatus.mode`,
`pause_reason`, and `last_error` rather than inferring completion from a
transport response alone.

## Request and response summary

| Area | Requests | Responses |
|---|---|---|
| Status | `Status`, `ControlStatus` | `Status`, `Control` |
| Hierarchy/value | `ListHierarchy`, `ReadSignal` | `Hierarchy`, `Signal` |
| Watches | `AddWatch`, `ListWatches`, `RemoveWatch` | `Watch`, `Watches`, `Removed` |
| Recordings | `StartRecording`, `RecordingStatus`, `GetRecording`, `StopRecording`, `RemoveRecording` | `RecordingStatus`, `Recording`, `Removed` |
| Control | `Pause`, `Resume`, `RunUntilTime`, `RunUntilPredicate`, `Terminate` | `Control` |

All public request, response, status, and error types derive Serde
serialization. A transport can expose these neutral shapes directly or wrap
them in its own protocol envelope.
