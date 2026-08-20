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

Recording and query resources are bounded. The configured recording byte
limit is a post-flush stop threshold, so the active segment may overshoot it by
the trace data emitted between checks; exact duration and segment limits still
terminate capture independently. Status reports observed bytes and the
limit-triggered stop reason. The legacy change-only projection folds each
closed segment into a fixed-capacity ring with a per-value width bound;
filtered value/change/snapshot queries use separate total scan and decoded-byte
budgets.

User guides:

- [Getting started](../docs/getting-started.md)
- [Backend-neutral debug API](../docs/debug-api.md)
- [MCP tool reference](../docs/mcp-tools.md)
- [Recording and history](../docs/recording-and-history.md)
- [Troubleshooting](../docs/troubleshooting.md)
- [Standalone example](../examples/mcp-debug-testbench/)
