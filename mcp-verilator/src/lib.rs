//! Compatibility import path for the original Verilator MCP package.
//!
//! New testbenches should use `rustdv-mcp`, whose setup chooses the active
//! simulator's recording capability without Verilator names in testbench code.

pub use rustdv_mcp::*;
