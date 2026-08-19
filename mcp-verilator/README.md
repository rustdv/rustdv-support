# rustdv-mcp-verilator

Loopback MCP inspection and control for RustDV simulations running on
Verilator.

This crate adapts Verilator VPI hierarchy and signal handles to
`rustdv-debug`, serves the public debug operations over streamable HTTP, and
provides an opt-in service loop composed from RustDV's existing ReadOnly and
time triggers. It does not create another simulation scheduler.

Use RustDV's Verilator `inspect` mode with a deliberately scoped Verilator
control file when live visibility is required without waveform generation.
The unauthenticated server accepts loopback bind addresses only.
