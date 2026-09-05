// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Coalesced control state for one peer batcher.
//!
//! Control used to be messages on an unbounded channel, which is fine right up
//! until the batcher stops draining it. It stops whenever a flush parks on
//! admission — the peer is congested, which is exactly when its ingress lane is
//! busiest returning credit — and a stalled batcher facing a peer that keeps
//! sending grows that queue without bound. Unbounded *and* unread is the shape
//! `BATCHING.md` warns about for lane channels, reproduced one layer up.
//!
//! So control is state, not a queue. Every entry is keyed by slot and merged in
//! place:
//!
//! - **Credit accumulates.** Ten grants of one are a `u32` add, not ten
//!   messages. Nothing is lost, because the batcher only ever wanted the sum.
//! - **Close dominates credit.** Dominance is about *state size*, not about
//!   discarding credit: a slot's entry never becomes two, and the first close is
//!   the one that ended it, so a later reason adds nothing. Credit that arrived
//!   alongside is still carried and still emitted — the batcher sends the stored
//!   `CreditUpdate` before the `CloseSlot`, which is the order they were owed in
//!   and costs nothing, since a peer that has already stopped simply ignores a
//!   window it will not use.
//! - **A failed singleton dominates a successful one.** It is epoch death, and
//!   coalescing it away would leave slots alive with an unclosable `frame_seq`
//!   gap.
//! - **A flush kick is a bit.** An application calling `flush_batch` while the
//!   batcher is parked on admission asks for the same thing however many times
//!   it asks, so a thousand kicks are one `bool` rather than a thousand queued
//!   commands. This is why the flush entry point is coalesced control and not a
//!   message: a queued one would be unbounded exactly when it matters, since a
//!   producer loop keeps flushing every pass whether or not the last batch has
//!   been admitted.
//!
//! The result is O(live slots) whatever the arrival rate, and the batcher is
//! woken rather than fed: one [`tokio::sync::Notify`] permit stands in for any
//! number of pending changes.

use std::collections::HashMap;
use std::sync::Mutex;

use tokio::sync::Notify;

use super::super::protocol::{CloseReason, SlotId};
use crate::observability::MuxMetricsHandle;

// What bounds the two slot maps, now that nothing caps them by size.
//
// A fixed cap (4,096 entries, until 2026-09-06) was sized for a peer with
// about a thousand live slots and refused the 4,097th key. Legitimate keys
// are bounded by live slots on one peer, and a router in front of a worker
// that is carrying thousands of streams is an ordinary deployment: `t3-iso1`
// measured one peer at 4,000 to 6,700 and the cap refused its credit grants,
// its closes and, under `MuxConfig::async_open_ack`, the admission answers
// that lift a fenced slot. A refused grant is credit lost for good — the
// receiver zeroed its `ungranted` the moment it sent the `CreditUpdate` — so
// a size cap is the wrong shape for what it guards against, which is a peer
// naming slot ids that were never alive.
//
// The bound is now the one thing that distinguishes a legitimate key from a
// bogus one. `mine` holds control the *peer* sends about slots this batcher
// owns, and this batcher knows exactly which indices it has handed out:
// `ControlState::allocated`, published by the batcher on every allocation.
// A key at or past it names a slot that never existed and is refused, which
// is what `velo_streaming_mux_control_refused_total` now counts. Below it the
// map is bounded by the indices in use. `peers` holds control this side's own
// ingress writes about the peer's slots, which it only does for slots it
// holds in a table bounded by its own slot limit, so nothing a peer sends can
// grow it and it refuses nothing. Resolutions keep their own map (see
// `ControlState::resolutions`) for the ordering reason given there.
//

/// Coalesced control for one slot **this** batcher owns.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct OwnedControl {
    /// Credit granted since the batcher last looked.
    pub(super) credit: u32,
    /// The receiver asked us to abandon the slot.
    pub(super) close: Option<CloseReason>,
    /// A singleton (rendezvous, or an `OpenSlot` under
    /// `MuxConfig::async_open_ack`) resolved; `false` is a failed admission.
    pub(super) singleton: Option<bool>,
}

/// Coalesced control to send back for one slot the **peer** owns.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct PeerControl {
    /// Credit to advertise.
    pub(super) credit: u32,
    /// A close to send.
    pub(super) close: Option<CloseReason>,
}

/// Everything pending for a batcher that is not a data record or an open.
#[derive(Debug, Default)]
struct ControlState {
    /// The sweep evicted this batcher from the registry.
    pub(super) retire: bool,
    /// The application asked for whatever is staged to go now.
    pub(super) flush: bool,
    mine: HashMap<u32, OwnedControl>,
    peers: HashMap<u32, PeerControl>,
    /// Singleton resolutions owed to this side's own fenced slots, kept apart
    /// from `mine` so that this lane's growth can never be what refuses
    /// somebody else's grant, and so a resolution is never refused at all: it
    /// is this side's own answer to a fence it raised, at most one per fenced
    /// slot. Merged into `mine` at [`drain`] time, which takes both maps out
    /// of the state a writer could still be growing.
    ///
    /// [`drain`]: Self::drain
    resolutions: HashMap<u32, bool>,
    /// One past the highest slot index this batcher has ever allocated. A
    /// key into `mine` at or past it names a slot that never existed.
    allocated: u32,
    /// Entries refused because their index was never allocated.
    refused: u64,
}

impl ControlState {
    /// Whether the batcher has anything to do.
    fn is_idle(&self) -> bool {
        !self.retire
            && !self.flush
            && self.mine.is_empty()
            && self.peers.is_empty()
            && self.resolutions.is_empty()
    }

    /// The sweep evicted this batcher from the registry.
    fn retire(&mut self) {
        self.retire = true;
    }

    /// The application asked for a flush.
    fn kick_flush(&mut self) {
        self.flush = true;
    }

    /// Pending entries across every map, for the bound to be asserted on.
    ///
    /// The two flags are deliberately not counted: they are `bool`s, so they
    /// bound themselves and cannot be what a flood grows.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.mine.len() + self.peers.len() + self.resolutions.len()
    }

    /// Take everything pending, leaving the state empty.
    ///
    /// `resolutions` merges into `mine` here rather than living there all
    /// along, so a slot with both a grant and a resolution pending still
    /// reaches `on_owned_control` as the one `OwnedControl` it has always
    /// been — coalescing a failed admission over a successful one exactly as
    /// [`ControlInbox::singleton_resolved`] does, since this is the same rule
    /// applied at the boundary instead of at write time.
    fn drain(&mut self) -> DrainedControl {
        let mut mine = std::mem::take(&mut self.mine);
        for (raw, admitted) in std::mem::take(&mut self.resolutions) {
            let entry = mine.entry(raw).or_default();
            entry.singleton = Some(entry.singleton.unwrap_or(true) && admitted);
        }
        DrainedControl {
            retire: std::mem::take(&mut self.retire),
            flush: std::mem::take(&mut self.flush),
            mine,
            peers: std::mem::take(&mut self.peers),
        }
    }

    /// The entry for control the peer sent about a slot this side owns.
    ///
    /// `None`, counted as a refusal, when the index was never allocated here:
    /// the one case a size cap was guarding against, answered exactly. Both
    /// maps key by the whole [`SlotId`], generation included: keying by index
    /// alone would let a grant meant for a retired generation land in the
    /// live one's entry and hand it credit it was never given. A stale entry
    /// is harmless; the batcher's generation check rejects it on the next
    /// wake and the entry goes with the drain.
    fn entry_mine(&mut self, slot: SlotId) -> Option<&mut OwnedControl> {
        if slot.index() >= self.allocated {
            self.refused = self.refused.saturating_add(1);
            return None;
        }
        Some(self.mine.entry(slot.raw()).or_default())
    }

    /// The entry for an answer this batcher owes one of its own fenced slots.
    ///
    /// Its own map rather than one more key into `mine`: a refused
    /// resolution is a leak rather than a dropped message, and keeping the
    /// lane apart is what makes that statement hold whatever else is pending.
    fn entry_mine_owed(&mut self, slot: SlotId) -> &mut bool {
        self.resolutions.entry(slot.raw()).or_insert(true)
    }

    /// The entry for control this side sends back about a slot the peer owns.
    ///
    /// Never refused: the ingress writes these only for slots it holds, and
    /// its table is bounded by its own slot limit, so the map is bounded by
    /// construction and nothing the peer sends can grow it.
    fn entry_peer(&mut self, slot: SlotId) -> &mut PeerControl {
        self.peers.entry(slot.raw()).or_default()
    }
}

/// One drain's worth of control, owned by the batcher task.
pub(super) struct DrainedControl {
    pub(super) retire: bool,
    pub(super) flush: bool,
    pub(super) mine: HashMap<u32, OwnedControl>,
    pub(super) peers: HashMap<u32, PeerControl>,
}

/// The state plus the wakeup that tells the batcher to look at it.
///
/// `Notify` rather than a channel because a permit is exactly what is wanted:
/// it coalesces, it costs nothing to leave set, and a writer never waits.
#[derive(Default)]
pub(super) struct ControlInbox {
    state: Mutex<ControlState>,
    notify: Notify,
    metrics: Option<MuxMetricsHandle>,
}

impl ControlInbox {
    /// An inbox that reports refusals into `metrics`.
    pub(super) fn new(metrics: Option<MuxMetricsHandle>) -> Self {
        Self {
            state: Mutex::new(ControlState::default()),
            notify: Notify::new(),
            metrics,
        }
    }

    /// Wait until there is something to drain.
    pub(super) async fn wait(&self) {
        loop {
            // Register before the check: a notification landing between the two
            // is held as a permit, so the ordering costs a spurious wake at
            // worst and never a missed one.
            let notified = self.notify.notified();
            if !self.lock().is_idle() {
                return;
            }
            notified.await;
        }
    }

    /// Take everything pending, or `None` when there is nothing.
    pub(super) fn take(&self) -> Option<DrainedControl> {
        let mut state = self.lock();
        if state.is_idle() {
            return None;
        }
        Some(state.drain())
    }

    /// Pending entries, for the bound the stalled-admission test asserts.
    #[cfg(test)]
    pub(super) fn pending_len(&self) -> usize {
        self.lock().len()
    }

    /// Entries refused because their index was never allocated. The series
    /// `velo_streaming_mux_control_refused_total` is the operator-facing view of
    /// the same number; this one exists so a test can read it without a
    /// registry.
    #[cfg(test)]
    pub(super) fn refused(&self) -> u64 {
        self.lock().refused
    }

    /// The batcher handed out slot `index`; keys up to it are now legitimate.
    ///
    /// Not a wake: nothing became pending. Taken under the lock so a grant
    /// that races the open it answers is judged against the bound the open
    /// just raised, never against the one before it.
    pub(super) fn note_allocated(&self, index: u32) {
        let mut state = self.lock();
        state.allocated = state.allocated.max(index.saturating_add(1));
    }

    /// An inbound `CreditUpdate` for a slot we own.
    pub(super) fn grant(&self, slot: SlotId, delta: u32) {
        self.mutate(|state| {
            if let Some(entry) = state.entry_mine(slot) {
                entry.credit = entry.credit.saturating_add(delta);
            }
        });
    }

    /// The receiver asked us to abandon a slot we own.
    pub(super) fn peer_closed(&self, slot: SlotId, reason: CloseReason) {
        self.mutate(|state| {
            if let Some(entry) = state.entry_mine(slot) {
                entry.close.get_or_insert(reason);
            }
        });
    }

    /// A singleton (rendezvous, or an `OpenSlot` under
    /// `MuxConfig::async_open_ack`) finished resolving its admission.
    pub(super) fn singleton_resolved(&self, slot: SlotId, admitted: bool) {
        self.mutate(|state| {
            let entry = state.entry_mine_owed(slot);
            // A failed admission is epoch death and must survive any number
            // of successful resolutions coalescing over it.
            *entry = *entry && admitted;
        });
    }

    /// Credit to advertise back for a slot the peer owns.
    pub(super) fn reply_credit(&self, slot: SlotId, delta: u32) {
        self.mutate(|state| {
            let entry = state.entry_peer(slot);
            entry.credit = entry.credit.saturating_add(delta);
        });
    }

    /// A close to send back for a slot the peer owns.
    pub(super) fn reply_close(&self, slot: SlotId, reason: CloseReason) {
        self.mutate(|state| {
            state.entry_peer(slot).close.get_or_insert(reason);
        });
    }

    /// The sweep evicted this batcher.
    pub(super) fn retire(&self) {
        self.mutate(ControlState::retire);
    }

    /// The application asked for whatever is staged to go now.
    ///
    /// Sync and non-blocking, because the producer calling it is a serving loop
    /// with a forward pass to get back to: it sets a bit and leaves. Waiting for
    /// the write is admission's job, not the caller's.
    pub(super) fn kick_flush(&self) {
        self.mutate(ControlState::kick_flush);
    }

    fn mutate(&self, apply: impl FnOnce(&mut ControlState)) {
        let refused = {
            let mut state = self.lock();
            let before = state.refused;
            apply(&mut state);
            state.refused - before
        };
        // Reported outside the lock: a prometheus counter is cheap, but nothing
        // that can be moved out of a critical section belongs inside one.
        if refused > 0
            && let Some(metrics) = &self.metrics
        {
            for _ in 0..refused {
                metrics.control_refused();
            }
        }
        self.notify.notify_one();
    }

    /// Take the lock, ignoring poisoning.
    ///
    /// The critical section is a map insert with no user code in it, so a
    /// poisoned lock means a panic elsewhere rather than torn state; propagating
    /// it would strand every slot on the peer.
    fn lock(&self) -> std::sync::MutexGuard<'_, ControlState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slot(index: u32, generation: u8) -> SlotId {
        SlotId::new(index, generation).expect("index fits u24")
    }

    /// The size cap the bound replaced, kept only to name what the tests
    /// below prove no longer applies.
    const OLD_CAP: u32 = 4096;

    /// An inbox whose batcher has allocated `allocated` slot indices.
    fn inbox_with(allocated: u32) -> ControlInbox {
        let inbox = ControlInbox::default();
        inbox.note_allocated(allocated - 1);
        inbox
    }

    #[test]
    fn credit_accumulates_into_one_entry() {
        let inbox = inbox_with(8);
        let id = slot(3, 0);
        for _ in 0..10_000 {
            inbox.grant(id, 1);
        }
        assert_eq!(inbox.pending_len(), 1, "ten thousand grants, one entry");

        let drained = inbox.take().expect("something pending");
        assert_eq!(drained.mine[&id.raw()].credit, 10_000);
        assert!(inbox.take().is_none(), "the drain leaves nothing behind");
    }

    #[test]
    fn a_close_dominates_and_the_first_reason_wins() {
        let inbox = inbox_with(8);
        let id = slot(1, 0);
        inbox.grant(id, 5);
        inbox.peer_closed(id, CloseReason::UnknownSlot);
        inbox.peer_closed(id, CloseReason::ProtocolError);
        inbox.grant(id, 5);

        let drained = inbox.take().expect("something pending");
        let entry = drained.mine[&id.raw()];
        assert_eq!(entry.close, Some(CloseReason::UnknownSlot));
        assert_eq!(
            entry.credit, 10,
            "credit still merges; the batcher discards it with the slot"
        );
    }

    #[test]
    fn a_failed_singleton_survives_successful_ones() {
        let inbox = ControlInbox::default();
        let id = slot(2, 7);
        inbox.singleton_resolved(id, true);
        inbox.singleton_resolved(id, false);
        inbox.singleton_resolved(id, true);

        let drained = inbox.take().expect("something pending");
        assert_eq!(
            drained.mine[&id.raw()].singleton,
            Some(false),
            "a failed admission is epoch death and must not coalesce away"
        );
    }

    #[test]
    fn generations_do_not_share_an_entry() {
        let inbox = inbox_with(8);
        inbox.grant(slot(4, 0), 1);
        inbox.grant(slot(4, 1), 2);
        assert_eq!(
            inbox.pending_len(),
            2,
            "a grant for a retired generation must not credit the live one"
        );
    }

    /// A key naming a slot this batcher never allocated is refused; every
    /// key below the bound is kept, however many there are.
    #[test]
    fn a_grant_for_an_index_never_allocated_is_refused() {
        let inbox = inbox_with(10);
        for index in 0..10 {
            inbox.grant(slot(index, 0), 1);
        }
        inbox.grant(slot(10, 0), 1);
        inbox.grant(slot(4_999, 0), 1);
        assert_eq!(
            inbox.pending_len(),
            10,
            "ten allocated indices, ten entries"
        );
        assert_eq!(inbox.refused(), 2, "and two keys that name no slot of ours");

        // Keys already present still merge.
        inbox.grant(slot(0, 0), 41);
        let drained = inbox.take().expect("something pending");
        assert_eq!(drained.mine[&slot(0, 0).raw()].credit, 42);
    }

    /// A peer with more live slots than the old size cap loses no grant.
    ///
    /// This is the case `t3-iso1` hit: one router-side batcher owning 4,000
    /// to 6,700 slots on a worker, every one of them owed credit. The bound
    /// is the batcher's own allocation, so the count of live slots is not
    /// something the map can be too small for.
    #[test]
    fn a_peer_with_more_live_slots_than_the_old_cap_loses_no_grant() {
        let inbox = inbox_with(6_000);
        for index in 0..(OLD_CAP + 904) {
            inbox.grant(slot(index, 0), 1);
        }
        assert_eq!(inbox.refused(), 0, "every grant names a slot we allocated");
        assert_eq!(inbox.pending_len(), (OLD_CAP + 904) as usize);
        let drained = inbox.take().expect("something pending");
        assert_eq!(drained.mine[&slot(OLD_CAP + 903, 0).raw()].credit, 1);
    }

    /// Replies are this side's own writes about the peer's slots, bounded by
    /// the ingress table that produces them; nothing refuses one.
    #[test]
    fn replies_are_never_refused() {
        let inbox = ControlInbox::default();
        for index in 0..(OLD_CAP + 5_904) {
            inbox.reply_credit(slot(index, 0), 1);
        }
        assert_eq!(inbox.refused(), 0);
        assert_eq!(inbox.pending_len(), (OLD_CAP + 5_904) as usize);
        inbox.reply_close(slot(OLD_CAP + 5_903, 0), CloseReason::UnknownSlot);
        let drained = inbox.take().expect("something pending");
        assert_eq!(
            drained.peers[&slot(OLD_CAP + 5_903, 0).raw()].close,
            Some(CloseReason::UnknownSlot)
        );
    }

    /// The answer a fenced slot is waiting for is never refused at the cap.
    ///
    /// A peer flooding bogus ids can fill the map, and under `async_open_ack`
    /// a peer with more live slots than the cap fills it legitimately. Either
    /// way the resolution of a slot's own `OpenSlot` or over-budget record has
    /// to land, because nothing else lifts that slot's fence.
    #[test]
    fn a_singleton_resolution_is_never_refused_at_the_cap() {
        let inbox = inbox_with(OLD_CAP + 2);
        for index in 0..OLD_CAP {
            inbox.grant(slot(index, 0), 1);
        }
        assert_eq!(inbox.pending_len(), OLD_CAP as usize);

        let fenced = slot(OLD_CAP + 1, 0);
        inbox.singleton_resolved(fenced, true);
        assert_eq!(
            inbox.refused(),
            0,
            "a resolution is owed to a fence this side raised; refusing it leaks the slot"
        );

        let drained = inbox.take().expect("something pending");
        assert_eq!(
            drained
                .mine
                .get(&fenced.raw())
                .and_then(|entry| entry.singleton),
            Some(true),
            "the resolution reaches the batcher past a full map"
        );
    }

    /// Resolutions never crowd out grants.
    ///
    /// Every open under `MuxConfig::async_open_ack` resolves through
    /// `entry_mine_owed`, so a peer holding thousands of live slots —
    /// `t3-iso1` measured 4,000 to 6,700 on one peer — generates that many
    /// resolutions with no grant or peer-close among them at all. Under the
    /// size cap this file had then, resolutions sharing `mine` with
    /// `entry_mine` would have pushed it past the cap and refused every grant
    /// behind it. A refused grant is unrecoverable: the receiver has already
    /// zeroed `ungranted` for the delta the moment it sent the
    /// `CreditUpdate`, so nothing about the flood a peer's resolutions cause
    /// may be allowed to starve credit for the peer's other slots.
    #[test]
    fn resolutions_alone_must_not_exhaust_the_grant_lane() {
        let inbox = inbox_with(OLD_CAP + 1_000);
        for index in 0..(OLD_CAP + 500) {
            inbox.singleton_resolved(slot(index, 0), true);
        }
        assert_eq!(
            inbox.refused(),
            0,
            "the exempt lane must never itself trip the refusal counter"
        );

        // An ordinary credit grant, for a slot the resolution flood above
        // never touched, must still land.
        let untouched = slot(OLD_CAP + 999, 0);
        inbox.grant(untouched, 7);
        assert_eq!(
            inbox.refused(),
            0,
            "a grant for an untouched slot must not be refused merely because \
             open-ack resolutions filled the shared map"
        );

        let drained = inbox.take().expect("something pending");
        assert_eq!(
            drained.mine.get(&untouched.raw()).map(|entry| entry.credit),
            Some(7),
            "the grant must reach the batcher, not be silently dropped at the cap"
        );
    }

    #[test]
    fn a_thousand_flush_kicks_are_one_bit() {
        let inbox = ControlInbox::default();
        for _ in 0..1_000 {
            inbox.kick_flush();
        }
        assert_eq!(
            inbox.pending_len(),
            0,
            "a kick is a flag, so it never grows the slot maps the cap protects"
        );

        let drained = inbox.take().expect("something pending");
        assert!(drained.flush, "the drain carries the kick");
        assert!(
            inbox.take().is_none(),
            "and takes it, so one kick is not served twice"
        );
    }

    #[tokio::test]
    async fn a_flush_kick_wakes_a_parked_batcher() {
        let inbox = ControlInbox::default();
        inbox.kick_flush();
        tokio::time::timeout(std::time::Duration::from_secs(5), inbox.wait())
            .await
            .expect("a kick must wake the batcher like any other control");
    }

    #[tokio::test]
    async fn wait_returns_for_a_change_made_before_it_was_called() {
        let inbox = ControlInbox::default();
        inbox.retire();
        tokio::time::timeout(std::time::Duration::from_secs(5), inbox.wait())
            .await
            .expect("a permit set before the wait must still wake it");
    }
}
