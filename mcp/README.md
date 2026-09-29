# rustdv-mcp

Simulator-neutral MCP inspection and control for RustDV testbenches. The same
testbench source can run with Icarus or Verilator. Live reads, watches, pause,
resume, and run-until use the shared RustDV VPI and settled ReadOnly service.

The separate `rustdv-mcp-fst` adapter optionally adds runtime-gated FST capture
and historical queries when the simulator host advertises FST output. A host
with another trace format is never sent through the FST decoder. On Icarus, ordinary
recording tools retain bounded selected-signal samples through `rustdv-debug`;
all-signal trace queries report that no trace backend is available. Recording
capabilities are explicit; no simulator silently claims to have captured an
all-signal trace.

The former `rustdv-mcp-verilator` import path remains available for existing
testbenches. New testbenches should use the `simulator_debug_session_with_config`
and `run_debug_service` entry points exported here.
