#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
RUSTDV_ROOT="${RUSTDV_ROOT:?set RUSTDV_ROOT to a compatible RustDV checkout}"
TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}"

cargo build --release --manifest-path "$ROOT/Cargo.toml"

LIB="$TARGET_DIR/release/librustdv_mcp_debug_example.so"
if [ ! -f "$LIB" ]; then
    LIB="$TARGET_DIR/release/librustdv_mcp_debug_example.dylib"
fi
if [ ! -f "$LIB" ]; then
    echo "cannot find the built RustDV testbench library" >&2
    exit 1
fi

RUSTDV_VERILATOR_MODE=record \
RUSTDV_VERILATOR_CONTROL_FILE="$ROOT/inspect.vlt" \
    "$RUSTDV_ROOT/sim/run_verilator.sh" \
    "$LIB" \
    debug_probe \
    "$ROOT/build/verilator" \
    "$ROOT/probe.sv"

