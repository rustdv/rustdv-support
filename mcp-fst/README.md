# rustdv-mcp-fst

Verilator FST recording adapter for the RustDV MCP service. This crate owns
the runtime-gated capture state, private files, FST decoding, history queries,
and resource limits. It is selected only when RustDV's simulator host
advertises FST output. It is not part of the simulator-neutral `rustdv-debug`
request and control crate.
