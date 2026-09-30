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
//!   yet. On pre-bind no peer is known and the load of lane k is the pre-binds
//!   on k that are not yet claimed, released or expired.
//!
//! Unclaimed binds count because an `OpenSlot` arrives only with the sender's
//! first batch. Attaches answered before any of their senders sent would all
//! see zero live slots and all land on lane 0.

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

/// One bind's claim on a lane, counted until the bind leaves the table.
///
/// Held by the bind itself, so every way a bind can leave (claimed by an
/// `OpenSlot`, released by its owner, expired by the accept window, cleared at
/// shutdown) gives the count back through `Drop`, and none can forget to.
/// `Drop` only decrements an atomic. The claim path drops a bind while it
/// holds a slot table's mutex, and the choice reads slot tables, so taking a
/// lock here could deadlock against it.
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

    fn take(lane: LaneIndex, count: Arc<AtomicUsize>) -> Self {
        count.fetch_add(1, Ordering::Relaxed);
        Self { lane, count }
    }
}

impl Drop for LaneReservation {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Unclaimed binds per lane, the part of lane load that no slot table shows.
#[derive(Default)]
pub(crate) struct LaneLoad {
    /// Attach binds not yet claimed, per (peer, lane).
    ///
    /// Grows with distinct (peer, lane)s and is never pruned, like the ingress
    /// registry's own per-(peer, lane) state: a reservation holds its counter,
    /// so removing an entry while a bind is pending would split the count in
    /// two.
    attach: DashMap<PeerLane, Arc<AtomicUsize>>,
    /// Binds with no peer (pre-binds, and binds through the bare
    /// `FrameTransport::bind`) not yet claimed, released or expired, per lane.
    local: [Arc<AtomicUsize>; MAX_LANES as usize],
    /// Makes reading the loads and taking a reservation one step, so two
    /// binds chosen at once cannot both read the same lane as least loaded.
    /// Never taken in `LaneReservation::drop`.
    choosing: Mutex<()>,
}

impl LaneLoad {
    /// Place one bind and count it on its lane.
    ///
    /// `peer` is the sender when an attach names it, `None` for a pre-bind.
    /// `lanes` is how many mux lanes this node keeps to `peer` (or to any peer,
    /// for a pre-bind). `live` is `peer`'s live slots on a lane; it is not
    /// read for a keyed bind or when `peer` is `None`.
    pub(crate) fn reserve(
        &self,
        peer: Option<WorkerId>,
        key: Option<u64>,
        lanes: NonZeroU16,
        live: impl Fn(PeerLane) -> usize,
    ) -> LaneReservation {
        if let Some(key) = key {
            return self.reserve_on(peer, keyed_lane(key, lanes));
        }
        let _choosing = self
            .choosing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let lane = least_loaded(lanes, |lane| match peer {
            Some(peer) => {
                let key = PeerLane::new(peer, lane);
                live(key) + self.pending(Some(peer), lane)
            }
            None => self.pending(None, lane),
        });
        LaneReservation::take(lane, self.counter(peer, lane))
    }

    /// Count one bind on a lane already decided.
    pub(crate) fn reserve_on(&self, peer: Option<WorkerId>, lane: LaneIndex) -> LaneReservation {
        LaneReservation::take(lane, self.counter(peer, lane))
    }

    /// Binds on `lane` that are counted and not yet claimed, released or
    /// expired, for `peer` (attach) or with no peer (pre-bind).
    pub(crate) fn pending(&self, peer: Option<WorkerId>, lane: LaneIndex) -> usize {
        match peer {
            Some(peer) => self
                .attach
                .get(&PeerLane::new(peer, lane))
                .map_or(0, |count| count.load(Ordering::Relaxed)),
            None => self.local[usize::from(lane.get())].load(Ordering::Relaxed),
        }
    }

    fn counter(&self, peer: Option<WorkerId>, lane: LaneIndex) -> Arc<AtomicUsize> {
        match peer {
            Some(peer) => Arc::clone(
                self.attach
                    .entry(PeerLane::new(peer, lane))
                    .or_default()
                    .value(),
            ),
            None => Arc::clone(&self.local[usize::from(lane.get())]),
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
            .map(|_| load.reserve(None, None, lanes(1), |_| 0))
            .collect();
        assert!(held.iter().all(|r| r.lane() == LaneIndex::ZERO));
    }

    /// Unkeyed pre-binds fill lanes evenly, and one given back is reused next.
    #[test]
    fn pre_binds_go_to_the_least_loaded_lane_and_give_it_back_on_drop() {
        let load = LaneLoad::default();
        let mut held: Vec<_> = (0..8)
            .map(|_| load.reserve(None, None, lanes(4), |_| 0))
            .collect();
        let placed: Vec<u16> = held.iter().map(|r| r.lane().get()).collect();
        assert_eq!(placed, [0, 1, 2, 3, 0, 1, 2, 3]);

        // Give back one on lane 2.
        drop(held.remove(2));
        assert_eq!(load.pending(None, LaneIndex::new(2)), 1);
        let next = load.reserve(None, None, lanes(4), |_| 0);
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
        let live = |key: PeerLane| usize::from(key.peer == peer && key.lane == LaneIndex::ZERO) * 2;
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

    /// A keyed bind is counted too, so unkeyed binds route around it.
    #[test]
    fn a_keyed_bind_counts_toward_its_lane() {
        let load = LaneLoad::default();
        let keyed = load.reserve(None, Some(3), lanes(4), |_| 0);
        assert_eq!(keyed.lane(), keyed_lane(3, lanes(4)));
        let unkeyed = load.reserve(None, None, lanes(4), |_| 0);
        assert_ne!(unkeyed.lane(), keyed.lane());
    }
}
