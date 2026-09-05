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

/// Entries either map may hold before it starts refusing new keys.
///
/// The cap exists for one case: a peer naming slot ids that were never alive,
/// which would otherwise grow the maps by one entry per bogus record. It was
/// sized against a decode engine's 1024 concurrent streams to one router — an
/// order of magnitude of headroom against *that* shape. That headroom is not
/// universal: `t3-iso1` measured one peer holding 4,000 to 6,700 live slots at
/// once (`agent-docs/w4a-async-open-ack-status.md`), an order of magnitude
/// above the sizing assumption, and at that shape it is legitimate entries —
/// not bogus ones — the cap refuses.
///
/// It applies to what the *peer* names. A singleton resolution is exempt: it
/// is this side's own answer to a fence it raised, there is at most one
/// outstanding per fenced slot, and nothing sends it twice. Refusing one leaves
/// the slot fenced with no second answer coming, so every record it ever
/// queues is withheld until the consumer's heartbeat watchdog gives up on it.
/// That is what happened on a peer with more live slots than this cap under
/// [`MuxConfig::async_open_ack`], where `t3-iso1` measured every open landing
/// on a peer congested enough to fence it (`fire_singleton` fenced
/// unconditionally at the time; it now fences only when the admission is not
/// already behind it, but a peer that congested still fences most opens).
///
/// The exemption lives in its own map (see [`ControlState::resolutions`])
/// rather than as an uncapped key into `mine`, precisely so it cannot make
/// `entry_mine`'s own problem worse. `entry_mine`'s credit grants and
/// peer-initiated closes still go through the capped path and still refuse
/// once *`mine`'s own* entries — grants and closes alone, with nothing from
/// the exemption in them — reach this cap, which at a peer's live-slot count
/// above it is the same legitimate-entries case described above, not the
/// bogus-id case the cap was sized for. That refusal is still real and still
/// unrecoverable: the receiver has already zeroed the credit it sent by the
/// time its `CreditUpdate` reached us. A singleton resolution sharing `mine`
/// with grants and closes would only add to that pressure — a peer with more
/// live slots than this cap generates that many resolutions too, and every
/// one of them would have pushed `mine` closer to refusing the grant behind
/// it. The separate map removes that particular contributor; it does not
/// close the underlying gap. A cap keyed to live slots, or one that refuses
/// only keys naming no live slot, is the follow-up this leaves open
/// (`agent-docs/w4a-async-open-ack-status.md`).
///
/// [`MuxConfig::async_open_ack`]: super::super::MuxConfig::async_open_ack
pub(super) const MAX_PENDING_CONTROL: usize = 4096;

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
    /// Singleton resolutions owed to this side's own fenced slots — see
    /// [`MAX_PENDING_CONTROL`]'s doc for why this is a separate, uncapped map
    /// rather than one more key into `mine`. Merged into `mine` at [`drain`]
    /// time, once the cap has nothing left to protect: draining takes both
    /// maps out of the state a writer could still be growing, so nothing
    /// about the merge can trip a refusal.
    ///
    /// [`drain`]: Self::drain
    resolutions: HashMap<u32, bool>,
    /// Entries refused because a map was at [`MAX_PENDING_CONTROL`].
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

    fn entry_mine(&mut self, slot: SlotId) -> Option<&mut OwnedControl> {
        Self::slot_entry(&mut self.mine, &mut self.refused, slot)
    }

    /// The entry for an answer this batcher owes one of its own fenced slots.
    ///
    /// Not subject to [`MAX_PENDING_CONTROL`] — it does not touch `mine` at
    /// all until [`Self::drain`] — for why see the cap's own doc: a refused
    /// resolution is a leak rather than a dropped message, and the whole point
    /// of a separate map is that this lane's growth can never be what refuses
    /// somebody else's grant.
    fn entry_mine_owed(&mut self, slot: SlotId) -> &mut bool {
        self.resolutions.entry(slot.raw()).or_insert(true)
    }

    fn entry_peer(&mut self, slot: SlotId) -> Option<&mut PeerControl> {
        Self::slot_entry(&mut self.peers, &mut self.refused, slot)
    }

    /// Key by the whole [`SlotId`], generation included.
    ///
    /// Keying by index alone would let a grant meant for a retired generation
    /// land in the live one's entry and hand it credit it was never given. A
    /// stale entry is harmless: the batcher's generation check rejects it on the
    /// next wake and the entry goes with the drain.
    fn slot_entry<'a, T: Default>(
        map: &'a mut HashMap<u32, T>,
        refused: &mut u64,
        slot: SlotId,
    ) -> Option<&'a mut T> {
        let key = slot.raw();
        if !map.contains_key(&key) && map.len() >= MAX_PENDING_CONTROL {
            *refused = refused.saturating_add(1);
            return None;
        }
        Some(map.entry(key).or_default())
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

    /// Entries refused at the cap. The series
    /// `velo_streaming_mux_control_refused_total` is the operator-facing view of
    /// the same number; this one exists so a test can read it without a
    /// registry.
    #[cfg(test)]
    pub(super) fn refused(&self) -> u64 {
        self.lock().refused
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
            if let Some(entry) = state.entry_peer(slot) {
                entry.credit = entry.credit.saturating_add(delta);
            }
        });
    }

    /// A close to send back for a slot the peer owns.
    pub(super) fn reply_close(&self, slot: SlotId, reason: CloseReason) {
        self.mutate(|state| {
            if let Some(entry) = state.entry_peer(slot) {
                entry.close.get_or_insert(reason);
            }
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

    #[test]
    fn credit_accumulates_into_one_entry() {
        let inbox = ControlInbox::default();
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
        let inbox = ControlInbox::default();
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
        let inbox = ControlInbox::default();
        inbox.grant(slot(4, 0), 1);
        inbox.grant(slot(4, 1), 2);
        assert_eq!(
            inbox.pending_len(),
            2,
            "a grant for a retired generation must not credit the live one"
        );
    }

    #[test]
    fn the_cap_refuses_new_keys_rather_than_growing() {
        let inbox = ControlInbox::default();
        for index in 0..(MAX_PENDING_CONTROL as u32 + 500) {
            inbox.grant(slot(index, 0), 1);
        }
        assert_eq!(inbox.pending_len(), MAX_PENDING_CONTROL);
        assert_eq!(inbox.refused(), 500);

        // Keys already present still merge — a live slot's credit is never lost
        // to a flood of bogus ids that arrived first.
        inbox.grant(slot(0, 0), 41);
        let drained = inbox.take().expect("something pending");
        assert_eq!(drained.mine[&slot(0, 0).raw()].credit, 42);
    }

    /// The answer a fenced slot is waiting for is never refused at the cap.
    ///
    /// A peer flooding bogus ids can fill the map, and under `async_open_ack`
    /// a peer with more live slots than the cap fills it legitimately. Either
    /// way the resolution of a slot's own `OpenSlot` or over-budget record has
    /// to land, because nothing else lifts that slot's fence.
    #[test]
    fn a_singleton_resolution_is_never_refused_at_the_cap() {
        let inbox = ControlInbox::default();
        for index in 0..(MAX_PENDING_CONTROL as u32) {
            inbox.grant(slot(index, 0), 1);
        }
        assert_eq!(inbox.pending_len(), MAX_PENDING_CONTROL);

        let fenced = slot(MAX_PENDING_CONTROL as u32 + 1, 0);
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

    /// The exemption must not spend the capped lane's own budget.
    ///
    /// Every open under `MuxConfig::async_open_ack` resolves through
    /// `entry_mine_owed`, so a peer holding more live slots than
    /// `MAX_PENDING_CONTROL` — `t3-iso1` measured 4,000-6,700 against 4,096 —
    /// generates that many resolutions with no grant or peer-close among them
    /// at all. If those resolutions shared `mine` with `entry_mine`, that
    /// alone would push `mine` past the cap and start refusing every grant
    /// behind it. A refused grant is unrecoverable: the receiver has already
    /// zeroed `ungranted` for the delta the moment it sent the
    /// `CreditUpdate`, so nothing about the flood a peer's resolutions cause
    /// may be allowed to starve credit for the peer's other slots.
    #[test]
    fn resolutions_alone_must_not_exhaust_the_grant_lane() {
        let inbox = ControlInbox::default();
        for index in 0..(MAX_PENDING_CONTROL as u32 + 500) {
            inbox.singleton_resolved(slot(index, 0), true);
        }
        assert_eq!(
            inbox.refused(),
            0,
            "the exempt lane must never itself trip the refusal counter"
        );

        // An ordinary credit grant, for a slot the resolution flood above
        // never touched, must still land.
        let untouched = slot(MAX_PENDING_CONTROL as u32 + 999, 0);
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
