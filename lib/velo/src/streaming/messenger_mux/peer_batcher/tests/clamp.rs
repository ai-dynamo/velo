// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The three-way batch clamp, at the level where all three terms are reachable.
//!
//! Two of them bind end to end and have wire tests over in [`super::egress`].
//! The eager budget does not and cannot in process: every messenger transport
//! reports either the 256 KiB rendezvous staging threshold or its own smaller
//! ceiling — NATS' ~1 MiB message limit loses to the threshold — and both are
//! far above the 64 KiB coalescing threshold, so the eager term is never the
//! minimum on a loopback pair. It would bind on a transport with a message
//! limit under 64 KiB, and this is where that node is expressible.

use crate::streaming::messenger_mux::peer_batcher::writer::{MIN_BATCH_CAP, batch_cap};
use crate::transports::tcp::framing::COALESCE_THRESHOLD;

#[test]
fn the_configured_cap_binds_when_it_is_the_smallest() {
    assert_eq!(batch_cap(4096, usize::MAX), 4096);
}

#[test]
fn the_coalescing_threshold_binds_over_a_larger_configured_cap() {
    assert_eq!(batch_cap(1 << 20, usize::MAX), COALESCE_THRESHOLD);
}

#[test]
fn a_transport_with_a_tight_eager_budget_binds_over_both() {
    // The arm no in-process pair reaches: a peer served by a transport whose
    // message limit lands under the coalescing threshold. Exceeding it does not
    // fail the flush — it silently stages the batch through rendezvous, paying a
    // round trip on behalf of every slot packed into it.
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
