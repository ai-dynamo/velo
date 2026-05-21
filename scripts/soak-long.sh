#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Long-form / on-demand soak. Operator-driven; not part of any CI cron.
#
# Usage:
#   scripts/soak-long.sh <area> [--transport tcp|grpc] [--duration <dur>] [--faults]
#
# Where <area> is one of: messenger | stream | rendezvous | all.
#
# Defaults to `--tier long` so the in-binary tier defaults are used; pass
# `--duration` to override.

set -euo pipefail
cd "$(dirname "$0")/.."

if [[ $# -lt 1 ]]; then
    echo "usage: $0 <messenger|stream|rendezvous|all> [extra soak args...]" >&2
    exit 2
fi

area="$1"; shift

cargo build --release --example soak --features grpc \
    --manifest-path examples/Cargo.toml

exec examples/target/release/examples/soak "${area}" --tier long "$@"
