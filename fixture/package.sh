#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(dirname "$0")

# rustflags for wasm32 target are in .cargo/config.toml
cargo build --manifest-path "$SCRIPT_DIR/Cargo.toml" --release --target wasm32-unknown-unknown -p integrity -p coordinator1 -p coordinator2 -p dht_util

pushd "$SCRIPT_DIR/package/dna1" || exit 1
hc dna pack .
popd || exit 1

pushd "$SCRIPT_DIR/package/dna2" || exit 1
hc dna pack .
popd || exit 1

pushd "$SCRIPT_DIR/package/happ1" || exit 1
hc app pack .
popd || exit 1

pushd "$SCRIPT_DIR/package/happ2" || exit 1
hc app pack .
popd || exit 1

