#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

RUSTDV_ROOT="${RUSTDV_ROOT:?set RUSTDV_ROOT to the pinned RustDV checkout}"
BUILD="$(mktemp -d /tmp/rustdv-support-debug-control.XXXXXX)"
LOCKFILE="$PWD/Cargo.lock"
LOCKFILE_BACKUP="$(mktemp /tmp/rustdv-support-cargo-lock.XXXXXX)"
cp "$LOCKFILE" "$LOCKFILE_BACKUP"

restore_lockfile() {
    if ! cmp -s "$LOCKFILE_BACKUP" "$LOCKFILE"; then
        cp "$LOCKFILE_BACKUP" "$LOCKFILE"
    fi
}

cleanup() {
    status=$?
    restore_lockfile
    rm -rf "$BUILD"
    rm -f "$LOCKFILE_BACKUP"
    exit "$status"
}
trap cleanup EXIT

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/rustdv-support-$(id -u)/target}"
cargo \
    --config "patch.\"https://github.com/teabone113/rustdv.git\".rustdv.path=\"$RUSTDV_ROOT/rustdv\"" \
    --config "patch.\"https://github.com/teabone113/rustdv.git\".rustdv-vpi-stubs.path=\"$RUSTDV_ROOT/rustdv/rustdv-vpi-stubs\"" \
    build --release -p rustdv-debug-control-verilator-test --quiet

LIB="$CARGO_TARGET_DIR/release/librustdv_debug_control_verilator_test.so"
[ -f "$LIB" ] || LIB="$CARGO_TARGET_DIR/release/librustdv_debug_control_verilator_test.dylib"
RUSTDV_VERILATOR_MODE=inspect \
RUSTDV_VERILATOR_CONTROL_FILE="$PWD/tests/debug-control-verilator/inspect.vlt" \
    "$RUSTDV_ROOT/sim/run_verilator.sh" "$LIB" debug_control_probe "$BUILD/verilator" \
    "$PWD/tests/debug-control-verilator/probe.sv"

FST="$(find "$BUILD" -type f -name '*.fst' -print -quit)"
if [ -n "$FST" ]; then
    echo "DEBUG CONTROL NO FST: FAIL — emitted $FST" >&2
    exit 1
fi
echo "DEBUG CONTROL NO FST: PASS"

restore_lockfile
cmp -s "$LOCKFILE_BACKUP" "$LOCKFILE"
echo "DEBUG CONTROL LOCKFILE: PASS"
trap - EXIT
rm -rf "$BUILD"
rm -f "$LOCKFILE_BACKUP"
