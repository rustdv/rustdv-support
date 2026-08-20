# rustdv-debug

Backend-neutral live debugging and asynchronous control primitives for RustDV
simulations.

The crate defines bounded request queues, signal providers, watches,
change-only recording rings, pause/resume, exact absolute-time stops, bounded
signal predicates, and termination state. Request queues, per-poll work,
watches, signals per watch, recordings, history, and response sizes all have
configuration limits. The crate deliberately has no dependency on RustDV,
Verilator, VPI, HTTP, or MCP.

A backend owns `DebugSession` on its simulator thread and gives transport
workers cloned `DebugClient` handles. All provider reads occur while polling
the session on that owner thread.

See [Backend-neutral debug API](../docs/debug-api.md) for `SignalProvider`,
owner-thread polling, scheduler responsibilities, requests, watches,
recordings, control, and worker-thread examples. The shorter
[getting-started example](../docs/getting-started.md#using-rustdv-debug-without-verilator)
shows the basic boundary, and the complete request/response types are
documented by the public Rust API in `src/lib.rs`.
