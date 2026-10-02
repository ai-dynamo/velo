// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Configured and eager batch limits, including small transport budgets.

use crate::streaming::messenger_mux::peer_batcher::writer::{MIN_BATCH_CAP, batch_cap};

#[test]
fn the_configured_cap_binds_when_it_is_the_smallest() {
    assert_eq!(batch_cap(4096, usize::MAX), 4096);
}

#[test]
fn a_larger_configured_cap_is_not_limited_by_tcp_coalescing() {
    assert_eq!(batch_cap(120 * 1024, usize::MAX), 120 * 1024);
    assert_eq!(batch_cap(1 << 20, 256 * 1024), 256 * 1024);
}

#[test]
fn a_transport_with_a_tight_eager_budget_bounds_the_batch() {
    assert_eq!(batch_cap(60 * 1024, 8192), 8192);
    assert_eq!(batch_cap(1 << 20, 8192), 8192);
}

#[test]
fn the_floor_survives_every_ceiling() {
    // A budget this tight would otherwise clamp to nothing and route even the
    // 13-byte control records through rendezvous, so the batcher would never
    // make progress. Records that do not fit above the floor still take the
    // singleton path, which is the right answer for them.
    assert_eq!(batch_cap(60 * 1024, 1), MIN_BATCH_CAP);
    assert_eq!(batch_cap(0, usize::MAX), MIN_BATCH_CAP);
}
