#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Velo soak — PR smoke gate.
#
# Goal: in <2 min, exercise messenger / streaming / rendezvous correctness on
# both TCP and gRPC, with sized-down loads. Failure of this script is a
# blocking signal: either a regression landed or a previously-known flake
# escaped its guardrail.
#
# What runs:
#   * messenger M1..M3, M5  — sustained AM, unary, mixed, drain-on-server-drop
#   * streaming  S1, S4, S6 — single-stream / rapid create / large items
#   * rendezvous R1..R4     — inline + chunked + refcount + gauge
#
# What is NOT run here (run in nightly instead):
#   * S2 (concurrent multi-stream) — known intermittent SenderDropped under
#     cross-stream contention; tracked as a follow-up.
#   * --faults — handler panics dump to stderr in the smoke output and the
#     fault-mode scenarios add several seconds; nightly is the right place.

set -euo pipefail

cd "$(dirname "$0")/.."

ROUNDS=(tcp grpc)
SCENARIOS="M1,M2,M3,M5,S1,S4,S6,R1,R2,R3,R4"

cargo build --release --example soak \
    --all-features \
    --manifest-path examples/Cargo.toml

for transport in "${ROUNDS[@]}"; do
    echo
    echo "=========================================================="
    echo " soak smoke — transport=${transport}"
    echo "=========================================================="
    RUST_LOG="${RUST_LOG:-error}" \
        examples/target/release/examples/soak all \
            --transport "${transport}" \
            --tier ci \
            --scenarios "${SCENARIOS}"
done

echo
echo "[soak-smoke] all rounds passed"
