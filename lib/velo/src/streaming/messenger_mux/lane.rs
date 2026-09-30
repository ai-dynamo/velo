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

use std::num::NonZeroU16;

use velo_ext::WorkerId;

/// Most mux lanes one node keeps to one peer.
///
/// Every node registers one batch handler per lane at build, so this bounds
/// the handler table, and the batch header carries the lane in four bits.
/// A transport with more lanes than this still carries only this many mux
/// lanes; the rest stay free for other traffic.
pub(crate) const MAX_LANES: u16 = 16;

/// The batch handler of each lane, by index.
///
/// Lane 0 keeps the name the mux has always used, so a peer that predates
/// lanes still sends and receives every batch on it. Static strings rather
/// than `format!` at each send, because the writer names the handler on every
/// batch.
const STREAM_BATCH_HANDLERS: [&str; MAX_LANES as usize] = [
    "_stream_batch",
    "_stream_batch.1",
    "_stream_batch.2",
    "_stream_batch.3",
    "_stream_batch.4",
    "_stream_batch.5",
    "_stream_batch.6",
    "_stream_batch.7",
    "_stream_batch.8",
    "_stream_batch.9",
    "_stream_batch.10",
    "_stream_batch.11",
    "_stream_batch.12",
    "_stream_batch.13",
    "_stream_batch.14",
    "_stream_batch.15",
];

/// How many mux lanes to keep to a peer whose transport keeps `transport`.
///
/// Mux lane k rides transport lane k, so the mux never uses more lanes than
/// the transport has, and never more than [`MAX_LANES`].
pub(crate) fn mux_lanes(transport: NonZeroU16) -> NonZeroU16 {
    transport.min(NonZeroU16::new(MAX_LANES).expect("MAX_LANES is not zero"))
}

/// Whether `name` is one of the mux's batch handlers.
///
/// Read when a handler's metrics are bound, not per batch.
pub(crate) fn is_batch_handler(name: &str) -> bool {
    STREAM_BATCH_HANDLERS.contains(&name)
}

/// Which mux lane to a peer.
///
/// Always below [`MAX_LANES`]: the only way to make one from a number is
/// [`LaneIndex::clamped`], so indexing the handler table cannot go out of range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct LaneIndex(u16);

impl LaneIndex {
    /// The lane of every stream whose consumer did not choose another, and of
    /// every peer that predates lanes.
    pub(crate) const ZERO: Self = Self(0);

    /// `lane % lanes`, with `lanes` itself capped at [`MAX_LANES`].
    ///
    /// How a sender takes the lane its consumer named: the consumer may name a
    /// lane this node does not keep to it, and the stream then rides the lane
    /// it maps to. Any lane is correct for any stream, because the consumer
    /// registers a handler for every lane and replies on the arrival lane;
    /// only the spread changes.
    pub(crate) fn clamped(lane: u16, lanes: NonZeroU16) -> Self {
        Self(lane % mux_lanes(lanes).get())
    }

    /// Every lane, in order. One batch handler is registered per entry.
    pub(crate) fn all() -> impl Iterator<Item = Self> {
        (0..MAX_LANES).map(Self)
    }

    /// The lane as it travels in attach responses and tickets.
    pub(crate) const fn get(self) -> u16 {
        self.0
    }

    /// The active-message handler this lane's batches travel through.
    pub(crate) fn handler_name(self) -> &'static str {
        STREAM_BATCH_HANDLERS[usize::from(self.0)]
    }

    #[cfg(test)]
    pub(crate) const fn new(index: u16) -> Self {
        assert!(index < MAX_LANES);
        Self(index)
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Lane 0 must keep the one name the mux has always used: a peer built
    /// before lanes registers only that handler and sends only to it.
    #[test]
    fn lane_zero_keeps_the_old_handler_name_and_the_rest_are_distinct() {
        assert_eq!(LaneIndex::ZERO.handler_name(), "_stream_batch");
        let names: std::collections::HashSet<_> =
            LaneIndex::all().map(LaneIndex::handler_name).collect();
        assert_eq!(names.len(), usize::from(MAX_LANES));
        for lane in LaneIndex::all().skip(1) {
            assert_eq!(lane.handler_name(), format!("_stream_batch.{lane}"));
        }
    }

    #[test]
    fn a_lane_clamps_to_the_lanes_this_node_keeps() {
        let one = NonZeroU16::MIN;
        let four = NonZeroU16::new(4).unwrap();
        let many = NonZeroU16::new(64).unwrap();
        assert_eq!(LaneIndex::clamped(3, one), LaneIndex::ZERO);
        assert_eq!(LaneIndex::clamped(3, four), LaneIndex::new(3));
        assert_eq!(LaneIndex::clamped(6, four), LaneIndex::new(2));
        // A transport with more lanes than the mux keeps still maps into the
        // handler table.
        assert_eq!(LaneIndex::clamped(17, many), LaneIndex::new(1));
        assert_eq!(mux_lanes(many).get(), MAX_LANES);
    }
}
