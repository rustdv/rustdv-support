# rustdv-mcp-verilator

Loopback MCP inspection and control for RustDV simulations running on
Verilator.

This crate adapts Verilator VPI hierarchy and signal handles to
`rustdv-debug`, serves the public debug operations over streamable HTTP, and
provides an opt-in service loop composed from RustDV's existing ReadOnly and
time triggers. It does not create another simulation scheduler.

Live reads, watches, and run-until predicates use VPI. Recording uses a
runtime-gated, private all-signal FST in RustDV's Verilator `record` mode; the
existing recording tools transparently project history from that trace, while
the trace-query tools expose bounded hierarchy, value, change, and snapshot
queries. `fast` mode remains uninstrumented and rejects recording with a
structured unsupported-capability error.

`rustdv-debug` remains independent of Verilator and FST. All trace control and
decoding stays in this adapter and runs at settled ReadOnly through RustDV's
service API. The unauthenticated server accepts loopback bind addresses only.
