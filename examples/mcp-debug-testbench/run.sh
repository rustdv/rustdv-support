#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
RUSTDV_ROOT="${RUSTDV_ROOT:?set RUSTDV_ROOT to a compatible RustDV checkout}"
SIM="${SIM:-verilator}"
TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/rustdv-support-$(id -u)/example-target}"
BUILD="${SIM_BUILD_DIR:-/tmp/rustdv-support-$(id -u)/mcp-debug-example}"
export CARGO_TARGET_DIR="$TARGET_DIR"
mkdir -p "$BUILD"

cargo build --release --manifest-path "$ROOT/Cargo.toml"

LIB="$TARGET_DIR/release/librustdv_mcp_debug_example.so"
if [ ! -f "$LIB" ]; then
    LIB="$TARGET_DIR/release/librustdv_mcp_debug_example.dylib"
fi
if [ ! -f "$LIB" ]; then
    echo "cannot find the built RustDV testbench library" >&2
    exit 1
fi

case "$SIM" in
    verilator)
        RUSTDV_VERILATOR_MODE=record \
        RUSTDV_VERILATOR_CONTROL_FILE="$ROOT/inspect.vlt" \
            "$RUSTDV_ROOT/sim/run_verilator.sh" \
            "$LIB" debug_probe "$BUILD/verilator" "$ROOT/probe.sv"
        ;;
    icarus)
        cp "$LIB" "$BUILD/rustdv_mcp_debug_example.vpi"
        iverilog -g2012 -o "$BUILD/debug_probe.vvp" -s debug_probe "$ROOT/probe.sv"
        RUSTDV_TOP=debug_probe vvp -M "$BUILD" -m rustdv_mcp_debug_example \
            "$BUILD/debug_probe.vvp"
        ;;
    *)
        echo "unknown simulator: $SIM (use icarus|verilator)" >&2
        exit 2
        ;;
esac
