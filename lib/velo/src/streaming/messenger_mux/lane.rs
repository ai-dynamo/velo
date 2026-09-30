// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The key every piece of per-peer mux state is stored under.
//!
//! A mux lane is one ordered channel of batches between two nodes. Order holds
//! per (peer, lane), not per peer, so everything that depends on order is kept
//! per lane: the egress batcher, and on the receive side the epoch,
//! `batch_seq` and the slot table. Slot ids are unique only within one batcher,
//! so a table shared by two lanes would let one lane retire the other's slots.
//!
//! Named `LaneIndex` rather than `Lane` because "lane" already means two other
//! things here: the messenger's ordered dispatch for one sender, and the
//! reject lane in the batcher's control state.

use velo_ext::WorkerId;

/// Which mux lane to a peer. Lane 0 is the only one used today.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct LaneIndex(u16);

impl LaneIndex {
    /// The lane every stream uses until lanes are chosen per stream.
    pub(crate) const ZERO: Self = Self(0);
}

impl std::fmt::Display for LaneIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// One (peer, lane): the key of a batcher and of a receive-side slot table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct PeerLane {
    pub(crate) peer: WorkerId,
    pub(crate) lane: LaneIndex,
}

impl PeerLane {
    pub(crate) const fn new(peer: WorkerId, lane: LaneIndex) -> Self {
        Self { peer, lane }
    }
}
