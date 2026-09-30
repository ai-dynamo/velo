// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Which lane a stream bound on this node is placed on.
//!
//! The consumer chooses, once, when it binds the slot: on attach, or when it
//! pre-binds and mints a ticket. The sender then opens on that lane, clamped
//! to the lanes it keeps itself.
//!
//! - With a key from the caller, the lane is `stable_hash(key) % lanes`, so
//!   streams with one key share a lane on every node and in every build.
//! - Without a key, the lane is the one with the least load, ties to the
//!   lowest index. On attach the peer is known and the load of lane k is that
//!   peer's live slots on k plus its binds on k that no `OpenSlot` has claimed
//!   yet. On pre-bind no peer is known and the load of lane k is this node's
//!   live slots on k from every peer, plus the pre-binds on k that are not yet
//!   claimed, released or expired.
//!
//! Unclaimed binds count because an `OpenSlot` arrives only with the sender's
//! first batch. Attaches answered before any of their senders sent would all
//! see zero live slots and all land on lane 0.
//!
//! Live slots count for pre-binds because a frontend's tickets are claimed
//! within milliseconds and their streams then live for seconds. Counting only
//! unclaimed pre-binds would show every lane empty at almost every choice, and
//! nearly every stream would go to lane 0.
//!
//! Every count is an atomic that the bind or the slot itself holds, so an
//! unkeyed choice reads no slot table. The ordered batch handler holds its
//! table's mutex through a whole batch, and a choice that locked tables would
//! wait behind that batch.

use std::num::NonZeroU16;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use dashmap::DashMap;
use velo_ext::WorkerId;

use super::lane::{LaneIndex, MAX_LANES, PeerLane, mux_lanes};

/// Mixes a caller's lane key before it is reduced to a lane.
///
/// The splitmix64 finalizer, written out so the result can never change with
/// the Rust version or the process: `std`'s `DefaultHasher` makes no promise
/// across releases, and a key must land on the same lane on every node that
/// runs a different build. The mix matters because callers pass ids that are
/// often sequential or share low bits (multiples of 16, say); a plain
/// `key % lanes` would put all of those on one lane.
pub(crate) const fn stable_hash(key: u64) -> u64 {
    let mut z = key.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// The lane `key` is placed on when this node keeps `lanes` mux lanes.
pub(crate) fn keyed_lane(key: u64, lanes: NonZeroU16) -> LaneIndex {
    let lanes = mux_lanes(lanes);
    // Below `lanes`, so it fits a `u16` and `clamped` leaves it as it is.
    let lane = stable_hash(key) % u64::from(lanes.get());
    LaneIndex::clamped(lane as u16, lanes)
}

/// The lane with the least `load` among the first `lanes`, ties to the lowest.
fn least_loaded(lanes: NonZeroU16, load: impl Fn(LaneIndex) -> usize) -> LaneIndex {
    LaneIndex::all()
        .take(usize::from(mux_lanes(lanes).get()))
        .min_by_key(|lane| (load(*lane), *lane))
        .unwrap_or(LaneIndex::ZERO)
}

/// One bind's or one live slot's claim on a lane, counted until its holder
/// goes.
///
/// Held by the bind or the slot itself, so every way it can leave (a bind
/// claimed by an `OpenSlot`, released by its owner, expired by the accept
/// window, cleared at shutdown; a slot closed, retired with its epoch, or torn
/// down) gives the count back through `Drop`, and none can forget to. `Drop`
/// only decrements an atomic: binds and slots are dropped while a slot
/// table's mutex is held, so taking any lock here risks a lock-order deadlock.
#[derive(Debug)]
pub(crate) struct LaneReservation {
    lane: LaneIndex,
    count: Arc<AtomicUsize>,
}

impl LaneReservation {
    /// The lane this bind was placed on.
    pub(crate) fn lane(&self) -> LaneIndex {
        self.lane
    }

    /// A reservation counted nowhere, for tests that register binds by hand.
    #[cfg(test)]
    pub(crate) fn uncounted(lane: LaneIndex) -> Self {
        Self::take(lane, Arc::new(AtomicUsize::new(0)))
    }

    pub(crate) fn take(lane: LaneIndex, count: Arc<AtomicUsize>) -> Self {
        count.fetch_add(1, Ordering::Relaxed);
        Self { lane, count }
    }
}

impl Drop for LaneReservation {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::Relaxed);
    }
}

/// A count per lane, shared with the reservations taken on it.
#[derive(Default)]
pub(crate) struct LaneCounts([Arc<AtomicUsize>; MAX_LANES as usize]);

impl LaneCounts {
    /// Count one more on `lane` until the returned reservation drops.
    pub(crate) fn take(&self, lane: LaneIndex) -> LaneReservation {
        LaneReservation::take(lane, Arc::clone(&self.0[usize::from(lane.get())]))
    }

    /// The reservations on `lane` not yet dropped.
    pub(crate) fn get(&self, lane: LaneIndex) -> usize {
        self.0[usize::from(lane.get())].load(Ordering::Relaxed)
    }
}

/// A count per (peer, lane), shared with the reservations taken on it.
///
/// Grows with distinct (peer, lane)s and is never pruned, like the ingress
/// registry's own per-(peer, lane) state: a reservation holds its counter, so
/// removing an entry while one is out would split the count in two.
#[derive(Default)]
pub(crate) struct PeerLaneCounts(DashMap<PeerLane, Arc<AtomicUsize>>);

impl PeerLaneCounts {
    /// The counter for `key`, created on first use.
    pub(crate) fn counter(&self, key: PeerLane) -> Arc<AtomicUsize> {
        if let Some(count) = self.0.get(&key) {
            return Arc::clone(count.value());
        }
        Arc::clone(self.0.entry(key).or_default().value())
    }

    /// Count one more on `key` until the returned reservation drops.
    pub(crate) fn take(&self, key: PeerLane) -> LaneReservation {
        LaneReservation::take(key.lane, self.counter(key))
    }

    /// The reservations on `key` not yet dropped.
    pub(crate) fn get(&self, key: PeerLane) -> usize {
        self.0
            .get(&key)
            .map_or(0, |count| count.load(Ordering::Relaxed))
    }
}

/// Unclaimed binds per lane, the part of lane load that no slot table shows.
#[derive(Default)]
pub(crate) struct LaneLoad {
    /// Attach binds not yet claimed, per (peer, lane).
    attach: PeerLaneCounts,
    /// Binds with no peer (pre-binds, and binds through the bare
    /// `FrameTransport::bind`) not yet claimed, released or expired, per lane.
    local: LaneCounts,
    /// Makes reading the loads and taking a reservation one step for the
    /// attach binds of one peer, so two chosen at once cannot both read the
    /// same lane as least loaded. Without it, attaches from one peer answered
    /// together all land on one lane. Per peer, because an attach reads only
    /// its own peer's counts, so attaches from different peers have nothing to
    /// race over. Never taken in `LaneReservation::drop`.
    ///
    /// Grows with distinct peers and is never pruned, as `attach` above.
    choosing_attach: DashMap<WorkerId, Arc<Mutex<()>>>,
    /// The same for binds with no peer. One for the node, because every
    /// pre-bind reads the same node-wide counts.
    choosing_local: Mutex<()>,
}

impl LaneLoad {
    /// Place one bind and count it on its lane.
    ///
    /// `peer` is the sender when an attach names it, `None` for a pre-bind.
    /// `lanes` is how many mux lanes this node keeps to `peer` (or to any peer,
    /// for a pre-bind). `live(peer, lane)` is the live slots on `lane` from
    /// `peer`, or from every peer when `peer` is `None`; it is not read for a
    /// keyed bind.
    pub(crate) fn reserve(
        &self,
        peer: Option<WorkerId>,
        key: Option<u64>,
        lanes: NonZeroU16,
        live: impl Fn(Option<WorkerId>, LaneIndex) -> usize,
    ) -> LaneReservation {
        if let Some(key) = key {
            return self.reserve_on(peer, keyed_lane(key, lanes));
        }
        // Cloned out of the map before it is locked, so the map's shard lock
        // is not held through the choice.
        let per_peer =
            peer.map(|peer| Arc::clone(self.choosing_attach.entry(peer).or_default().value()));
        let choosing = per_peer.as_deref().unwrap_or(&self.choosing_local);
        let _choosing = choosing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let lane = least_loaded(lanes, |lane| live(peer, lane) + self.pending(peer, lane));
        self.reserve_on(peer, lane)
    }

    /// Count one bind on a lane already decided.
    pub(crate) fn reserve_on(&self, peer: Option<WorkerId>, lane: LaneIndex) -> LaneReservation {
        match peer {
            Some(peer) => self.attach.take(PeerLane::new(peer, lane)),
            None => self.local.take(lane),
        }
    }

    /// Binds on `lane` that are counted and not yet claimed, released or
    /// expired, for `peer` (attach) or with no peer (pre-bind).
    pub(crate) fn pending(&self, peer: Option<WorkerId>, lane: LaneIndex) -> usize {
        match peer {
            Some(peer) => self.attach.get(PeerLane::new(peer, lane)),
            None => self.local.get(lane),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lanes(n: u16) -> NonZeroU16 {
        NonZeroU16::new(n).expect("non-zero")
    }

    /// The hash is part of the wire contract in effect: two nodes running
    /// different builds must put one key on one lane. These values pin it, so
    /// a change to the mix shows up here and not as streams quietly moving.
    #[test]
    fn a_key_lands_on_the_same_lane_in_every_build() {
        assert_eq!(stable_hash(0), 0xE220_A839_7B1D_CDAF);
        assert_eq!(stable_hash(1), 0x910A_2DEC_8902_5CC1);
        let at_four: Vec<u16> = (0..8).map(|key| keyed_lane(key, lanes(4)).get()).collect();
        assert_eq!(at_four, GOLDEN_AT_FOUR);
    }

    /// `keyed_lane(0..8, 4)` as the mix above gives it.
    const GOLDEN_AT_FOUR: [u16; 8] = [3, 1, 2, 1, 2, 2, 0, 3];

    #[test]
    fn one_key_one_lane_and_many_keys_spread() {
        for key in [0, 7, 1 << 40, u64::MAX] {
            assert_eq!(keyed_lane(key, lanes(8)), keyed_lane(key, lanes(8)));
        }
        // Keys that share their low bits, which `key % lanes` would pile on
        // lane 0, still reach every lane.
        let mut per_lane = [0usize; 4];
        for key in (0..256u64).map(|n| n * 16) {
            per_lane[usize::from(keyed_lane(key, lanes(4)).get())] += 1;
        }
        for count in per_lane {
            assert!((40..=90).contains(&count), "spread {per_lane:?}");
        }
    }

    #[test]
    fn one_lane_places_every_key_on_lane_zero() {
        for key in 0..64 {
            assert_eq!(keyed_lane(key, lanes(1)), LaneIndex::ZERO);
        }
        let load = LaneLoad::default();
        let held: Vec<_> = (0..8)
            .map(|_| load.reserve(None, None, lanes(1), |_, _| 0))
            .collect();
        assert!(held.iter().all(|r| r.lane() == LaneIndex::ZERO));
    }

    /// Unkeyed pre-binds fill lanes evenly, and one given back is reused next.
    #[test]
    fn pre_binds_go_to_the_least_loaded_lane_and_give_it_back_on_drop() {
        let load = LaneLoad::default();
        let mut held: Vec<_> = (0..8)
            .map(|_| load.reserve(None, None, lanes(4), |_, _| 0))
            .collect();
        let placed: Vec<u16> = held.iter().map(|r| r.lane().get()).collect();
        assert_eq!(placed, [0, 1, 2, 3, 0, 1, 2, 3]);

        // Give back one on lane 2.
        drop(held.remove(2));
        assert_eq!(load.pending(None, LaneIndex::new(2)), 1);
        let next = load.reserve(None, None, lanes(4), |_, _| 0);
        assert_eq!(next.lane(), LaneIndex::new(2));

        drop(held);
        drop(next);
        for lane in LaneIndex::all() {
            assert_eq!(load.pending(None, lane), 0);
        }
    }

    /// On attach, a peer's live slots and its unclaimed binds both count.
    #[test]
    fn attach_counts_live_slots_and_unclaimed_binds_of_that_peer() {
        let load = LaneLoad::default();
        let peer = WorkerId::from_u64(7);
        let other = WorkerId::from_u64(8);
        // Lane 0 has two live slots from `peer`.
        let live = |from: Option<WorkerId>, lane: LaneIndex| {
            usize::from(from == Some(peer) && lane == LaneIndex::ZERO) * 2
        };
        // Another peer's binds do not move this peer's choice.
        let _others: Vec<_> = (0..4)
            .map(|_| load.reserve(Some(other), Some(3), lanes(4), live))
            .collect();

        let first = load.reserve(Some(peer), None, lanes(4), live);
        let second = load.reserve(Some(peer), None, lanes(4), live);
        let third = load.reserve(Some(peer), None, lanes(4), live);
        let fourth = load.reserve(Some(peer), None, lanes(4), live);
        assert_eq!(
            [first.lane(), second.lane(), third.lane(), fourth.lane()].map(LaneIndex::get),
            [1, 2, 3, 1]
        );
    }

    /// On pre-bind, the node's live slots on a lane and its unclaimed
    /// pre-binds both count.
    ///
    /// A claimed pre-bind stops counting as pending, and its stream counts as
    /// a live slot instead for as long as it lives. Without the live term a
    /// node whose pre-binds are all claimed would read every lane as empty.
    #[test]
    fn pre_binds_count_the_nodes_live_slots_and_unclaimed_pre_binds() {
        let load = LaneLoad::default();
        // Live slots on the node, from any peer: lanes 0 and 1 hold two each,
        // lane 2 holds one.
        let live = |from: Option<WorkerId>, lane: LaneIndex| {
            assert_eq!(from, None, "a pre-bind reads the node's count");
            [2, 2, 1, 0][usize::from(lane.get())]
        };
        let first = load.reserve(None, None, lanes(4), live);
        let second = load.reserve(None, None, lanes(4), live);
        let third = load.reserve(None, None, lanes(4), live);
        assert_eq!(
            [first.lane(), second.lane(), third.lane()].map(LaneIndex::get),
            [3, 2, 3]
        );
    }

    /// A keyed bind is counted too, so unkeyed binds route around it.
    #[test]
    fn a_keyed_bind_counts_toward_its_lane() {
        let load = LaneLoad::default();
        let keyed = load.reserve(None, Some(3), lanes(4), |_, _| 0);
        assert_eq!(keyed.lane(), keyed_lane(3, lanes(4)));
        let unkeyed = load.reserve(None, None, lanes(4), |_, _| 0);
        assert_ne!(unkeyed.lane(), keyed.lane());
    }
}
