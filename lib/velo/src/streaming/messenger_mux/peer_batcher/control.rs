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
//! A fixed constant bound belongs only to `rejects`, the small capped lane
//! for a peer's bogus `OpenSlot`s — `OpenSlot`s the ingress never admitted,
//! so no live-slot count applies to them either (the id itself can still
//! name a slot admitted under a different, already-live `OpenSlot`; see
//! `ControlState::reject`). `mine`, `peers` and `resolutions` carry no size
//! cap: each is keyed by slot id, and `ControlState`'s field docs give the
//! exact (index, generation) bound each one carries. `drain` (below) caps
//! the accumulation window in practice, taking every map under one lock
//! hold. What answers the "unbounded and unread" hazard above, for
//! `entry_peer`'s own writers (`collect_grants` and `fail_slot`), is that a
//! new key never comes free there: it costs the peer a full
//! open/record/close cycle and consumes a locally registered bind, so
//! growth tracks stream lifecycles rather than arrival rate. The up to
//! [`MAX_PENDING_REJECTS`] reject-derived keys `drain` merges into `peers`
//! are bounded by that cap instead — see its doc for why capping them,
//! unlike `entry_peer`'s own keys, costs nothing. The batcher is woken
//! rather than fed either way: one [`tokio::sync::Notify`] permit stands in
//! for any number of pending changes.

use std::collections::HashMap;
use std::sync::Mutex;

use tokio::sync::Notify;

use super::super::protocol::{CloseReason, SlotId};
use crate::observability::MuxMetricsHandle;

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

/// Ceiling on `ControlState::rejects`, the lane for `OpenSlot`s the ingress
/// rejected outright.
///
/// Capped rather than bounded by any table, because the ingress's own
/// `MAX_INGRESS_SLOTS_PER_PEER` (65,536 indices) caps none of what lands
/// here: every index at or above it is out of range by definition, and the
/// `SlotId` index space (`u24`) holds roughly 16.7 million of them for a
/// peer to name — plus any in-range index whose bind lookup misses or whose
/// id collides with a live incumbent. Repeats are free regardless: `reject`
/// early-returns on a key already pending and never counts it, so what
/// actually grows this lane is distinct bogus ids, not repetition of one. A
/// rejection is safe to drop because it carries no credit: the sender's
/// slot just keeps streaming into one the receiver already discarded
/// (`MuxDropReason::ClosedSlot`) until its own producer finishes, and no
/// state leaks on either side. A dropped `CreditUpdate` is not recoverable
/// the same way — the receiver zeroed `ungranted` for that delta the moment
/// it sent the grant — which is why this cap belongs only here and never to
/// `entry_peer`.
pub(super) const MAX_PENDING_REJECTS: usize = 8_192;

/// Everything pending for a batcher that is not a data record or an open.
///
/// `mine` and `peers` used to share one fixed-size cap (4,096 entries) that
/// refused whichever key arrived once the map was full — including the
/// 4,097th live slot's credit grant, which is unrecoverable: the receiver
/// zeroed its `ungranted` the moment it sent the `CreditUpdate`, so a refused
/// grant is credit lost for good. `t3-iso1` measured one peer at 4,000 to
/// 6,700 live slots and hit exactly that. A size cap is the wrong shape for
/// what it was guarding against, which is a peer naming slot ids that were
/// never alive — so each map now has a bound built from what actually
/// distinguishes a legitimate key from a bogus one, and neither bound is a
/// cap on live traffic:
///
/// - **`mine`** holds control the *peer* sends about slots this batcher
///   owns, keyed by the whole [`SlotId`] — generation included, because
///   keying by index alone would let a grant meant for a retired generation
///   land in the live one's entry and hand it credit it was never given.
///   [`ControlInbox::note_allocated`] publishes each index's live generation
///   the moment this batcher opens it, before the peer can possibly have
///   learned the id, so [`ControlState::entry_mine`] can tell a legitimate
///   key from a bogus one without a separate size limit: an index this
///   batcher never allocated is refused and counted (`refused`, the operator
///   view is `velo_streaming_mux_control_refused_total`); an index it did
///   allocate but at a generation other than the live one names a generation
///   that is not current — either it retired because the index was since
///   reopened, or the peer simply guessed a generation that index never had
///   — and either way the key is stale and is dropped silently, uncounted
///   (without the check a hostile peer could pin one entry per generation it
///   cares to name, up to 256 per index, rather than none). The check only
///   gates *future* writes, though — it does not reach back and remove
///   whatever a reopen's predecessor generation already left in `mine` — so
///   the map's real bound between two drains is one entry per index at its
///   live generation, plus one stale leftover per reopen that index went
///   through since the last drain, up to the same 256-per-index ceiling the
///   check exists to keep a bogus peer from reaching on its own. A slot that
///   is closed but **not yet reopened** still matches its live generation,
///   so it is accepted here the same as any other grant and dies one hop
///   later instead, at apply time, when the batcher's own generation check
///   against the live `EgressSlots` table finds no slot there to credit.
/// - **`peers`** holds control this side's own ingress writes about the
///   peer's slots, keyed by the whole [`SlotId`] for the same reason `mine`
///   is: a credit or close reply names the generation the ingress table held
///   at the time it was admitted, and `fail_slot` pushes its `CloseSlot`
///   reply after `finish_close` has already removed that row, so the key can
///   outlive the table entry that produced it. Most of it — the credit and
///   close replies `collect_grants` and `fail_slot` produce — names a slot
///   the ingress table actually admitted, and the table never holds more
///   than one live generation per index at a time, so none of it is ever
///   refused. But the table's own slot limit does not cap `peers`: a peer
///   that closes and reopens the same index repeatedly leaves one entry
///   behind per generation the ingress admitted for it since the last
///   drain — up to 256, the width of the generation — because nothing here
///   is removed except by [`drain`]. Separately, `open_slot` rejecting an
///   `OpenSlot` outright — out of range, a collision, or a bind that never
///   existed — was never admitted (one of those rejections can still name an
///   id a *different*, admitted `OpenSlot` holds live right now; what bounds
///   it is that nothing here is waiting on it, not that the id is absent
///   from the table). Those go through `ControlState::reject` into `rejects`
///   instead, a lane capped at [`MAX_PENDING_REJECTS`] for the reason given
///   there: dropping one costs no credit, unlike the credit and closes the
///   rest of `peers` carries. `rejects` merges into `peers` at [`drain`]
///   time, same as `resolutions` merges into `mine`.
///
/// Resolutions keep their own map (see `ControlState::resolutions`) for the
/// ordering reason given there.
///
/// [`drain`]: Self::drain
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
    /// is this side's own answer to every singleton this batcher dispatched.
    /// `fire_singleton`'s spawn that inserts here runs unconditionally
    /// whether or not the fence was actually raised, so the bound is one
    /// entry per slot id that dispatched a singleton since the last drain,
    /// not one per *fenced* slot. Merged into `mine` at [`drain`] time,
    /// which takes both maps out of the state a writer could still be
    /// growing.
    ///
    /// [`drain`]: Self::drain
    resolutions: HashMap<u32, bool>,
    /// `OpenSlot`s the ingress rejected without admitting, capped at
    /// [`MAX_PENDING_REJECTS`] for the reason given there — unlike
    /// everything else in `peers`, dropping one costs no credit. Merged
    /// into `peers` at [`drain`] time.
    ///
    /// [`drain`]: Self::drain
    rejects: HashMap<u32, CloseReason>,
    /// Live generation of each index this batcher has ever allocated,
    /// published by [`ControlInbox::note_allocated`] on every open —
    /// including a reopen, which is what keeps this current across a
    /// close-then-reopen rather than only at first allocation. A `Vec`
    /// rather than a map: `EgressSlots::allocate` only ever reuses a freed
    /// index or pushes at its own length, so the allocated index set is
    /// always exactly `0..len` with no gaps, and position already encodes
    /// "never allocated" as `index >= len`. An index absent (past the end,
    /// or `None` within it — `note_allocated` never leaves a hole, but
    /// `resize` fills forward with `None` rather than assume it never will)
    /// was never allocated; present and matching the incoming key's
    /// generation means the slot is either still open or closed and not yet
    /// reopened, and either way the key is accepted; present but not
    /// matching means the key names a stale generation — one this index has
    /// since moved past via a reopen, or one it never had at all.
    live_generations: Vec<Option<u8>>,
    /// Entries refused: an index `mine` never allocated, or a `rejects` key
    /// past its cap. An ordinary stale-generation race is not counted here —
    /// see `entry_mine`.
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
            && self.rejects.is_empty()
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
    /// bound themselves and cannot be what a flood grows. `live_generations`
    /// is excluded for a different reason: it is not pending state — nothing
    /// drains it, and it never shrinks, so it is bounded by this batcher's
    /// own allocation history rather than by anything a peer can grow.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.mine.len() + self.peers.len() + self.resolutions.len() + self.rejects.len()
    }

    /// Take everything pending, leaving the state empty.
    ///
    /// `resolutions` merges into `mine` here rather than living there all
    /// along, so a slot with both a grant and a resolution pending still
    /// reaches `on_owned_control` as the one `OwnedControl` it has always
    /// been. No coalescing happens at this boundary: `mine`'s own writers
    /// (`entry_mine`, reached from `grant` and `peer_closed`) never touch
    /// `singleton`, so an entry taken from `mine` here is always fresh, and
    /// the failed-admission-wins rule lives entirely in
    /// [`ControlInbox::singleton_resolved`], the one place that writes
    /// `resolutions`.
    fn drain(&mut self) -> DrainedControl {
        let mut mine = std::mem::take(&mut self.mine);
        for (raw, admitted) in std::mem::take(&mut self.resolutions) {
            mine.entry(raw).or_default().singleton = Some(admitted);
        }
        let mut peers = std::mem::take(&mut self.peers);
        for (raw, reason) in std::mem::take(&mut self.rejects) {
            peers.entry(raw).or_default().close.get_or_insert(reason);
        }
        DrainedControl {
            retire: std::mem::take(&mut self.retire),
            flush: std::mem::take(&mut self.flush),
            mine,
            peers,
        }
    }

    /// The entry for control the peer sent about a slot this side owns.
    ///
    /// `None` for an index this side never allocated — counted as a refusal
    /// — or for a generation that is not the one currently live at an index
    /// it did allocate, whether that generation retired via a reopen or the
    /// peer simply named one that index never had; either way it is an
    /// ordinary stale key and is dropped without counting. A slot that is
    /// closed but not yet reopened still matches its live generation and is
    /// accepted here, not dropped — it dies one hop later, at apply time,
    /// against the live `EgressSlots` table. See `ControlState`'s struct doc
    /// for what the stale-key drop costs `mine`'s bound between drains.
    fn entry_mine(&mut self, slot: SlotId) -> Option<&mut OwnedControl> {
        match self
            .live_generations
            .get(slot.index() as usize)
            .copied()
            .flatten()
        {
            None => {
                self.refused = self.refused.saturating_add(1);
                None
            }
            Some(live) if live != slot.generation() => None,
            Some(_) => Some(self.mine.entry(slot.raw()).or_default()),
        }
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
    /// Never refused: `collect_grants` and `fail_slot`, the only writers,
    /// name a slot the ingress table actually admitted. See `ControlState`'s
    /// struct doc for `peers`'s real bound between drains and why leaving it
    /// unrefused is still safe. An `OpenSlot` the ingress rejects outright
    /// never comes through here, whether or not its id happens to match a
    /// slot admitted under a different `OpenSlot` — see `ControlState::reject`.
    fn entry_peer(&mut self, slot: SlotId) -> &mut PeerControl {
        self.peers.entry(slot.raw()).or_default()
    }

    /// Record an `OpenSlot` the ingress rejected without admitting it: out of
    /// range, a collision, or a bind that never existed.
    ///
    /// The rejected id can still coincide with a slot this side holds live
    /// under a different, admitted `OpenSlot` — a duplicate whose bind lookup
    /// missed is one way that happens. What makes dropping it safe is that
    /// this particular `OpenSlot` was never admitted, so nothing is waiting
    /// on its answer, not that the id is absent from the table.
    ///
    /// A repeat of a key already pending changes nothing — first reason wins,
    /// same as `entry_peer` — so it never counts against the cap. A genuinely
    /// new key past [`MAX_PENDING_REJECTS`] is refused and counted: dropping
    /// it is safe because it carries no credit, which is not true of
    /// anything `entry_peer` carries — see [`MAX_PENDING_REJECTS`] for why.
    fn reject(&mut self, slot: SlotId, reason: CloseReason) {
        let raw = slot.raw();
        if self.rejects.contains_key(&raw) {
            return;
        }
        if self.rejects.len() >= MAX_PENDING_REJECTS {
            self.refused = self.refused.saturating_add(1);
            return;
        }
        self.rejects.insert(raw, reason);
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

    /// Entries refused: an index `mine` never allocated, or a `rejects` key
    /// past its cap. The series `velo_streaming_mux_control_refused_total` is
    /// the operator-facing view of the same number; this one exists so a
    /// test can read it without a registry.
    #[cfg(test)]
    pub(super) fn refused(&self) -> u64 {
        self.lock().refused
    }

    /// The batcher opened `slot`; its generation is now the live one for its
    /// index, whether this is a first allocation or a reopen.
    ///
    /// Not a wake: nothing became pending. Taken under the lock so a grant
    /// that races the open it answers is judged against the generation the
    /// open just published, never against the one before it. Called before
    /// the peer can possibly have learned `slot`'s id, so by the time a
    /// legitimate grant for it can arrive, this has always already run.
    pub(super) fn note_allocated(&self, slot: SlotId) {
        let mut state = self.lock();
        let index = slot.index() as usize;
        if index >= state.live_generations.len() {
            state.live_generations.resize(index + 1, None);
        }
        state.live_generations[index] = Some(slot.generation());
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

    /// An `OpenSlot` the ingress rejected without ever holding `slot`.
    pub(super) fn reject_slot(&self, slot: SlotId, reason: CloseReason) {
        self.mutate(|state| state.reject(slot, reason));
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
    use crate::streaming::messenger_mux::ingress::MAX_INGRESS_SLOTS_PER_PEER;

    fn slot(index: u32, generation: u8) -> SlotId {
        SlotId::new(index, generation).expect("index fits u24")
    }

    /// The size cap the bound replaced, kept only to name what the tests
    /// below prove no longer applies.
    const OLD_CAP: u32 = 4096;

    /// An inbox whose batcher has allocated indices `0..allocated`, each at
    /// generation 0 — one `note_allocated` call per index, exactly as a real
    /// batcher's `on_open_slot` makes one per open.
    fn inbox_with(allocated: u32) -> ControlInbox {
        let inbox = ControlInbox::default();
        for index in 0..allocated {
            inbox.note_allocated(slot(index, 0));
        }
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

    /// A grant for a retired generation must not credit the live one.
    ///
    /// Before the generation check moved to write time, both entries were
    /// kept — the raw key already kept them apart — and the stale one was
    /// only dropped later, at apply time. Dropping it here instead is a
    /// tighter bound (`mine` no longer holds one entry per generation a peer
    /// cares to name, up to 256 per index), not a different outcome for the
    /// live entry, which is what this test still pins.
    #[test]
    fn a_stale_generation_is_dropped_and_never_credits_the_live_entry() {
        let inbox = inbox_with(8);
        inbox.grant(slot(4, 0), 1);
        inbox.grant(slot(4, 1), 2);
        assert_eq!(
            inbox.pending_len(),
            1,
            "the stale generation names no live slot and is dropped, not kept"
        );

        let drained = inbox.take().expect("something pending");
        assert_eq!(
            drained.mine[&slot(4, 0).raw()].credit,
            1,
            "the live entry is untouched by the stale grant"
        );
    }

    /// A slot closed but not yet reopened is accepted here, not dropped.
    ///
    /// `note_allocated` is the only writer of `live_generations`, and it only
    /// runs on open — `EgressSlots::close` never touches it. So from this
    /// inbox's own state, a slot this batcher has since closed is
    /// indistinguishable from one still open: both leave the index's live
    /// generation exactly where the last open set it. A grant naming that
    /// generation is accepted here either way; the real answer comes one hop
    /// later, when the batcher applies it against the live `EgressSlots`
    /// table and finds no slot to credit.
    #[test]
    fn a_grant_for_a_closed_but_not_reopened_slot_is_accepted_here() {
        let inbox = inbox_with(8);
        // No reopen happens — this inbox has no way to represent one without
        // `note_allocated`, and that is exactly the point: closing a slot
        // alone changes nothing this map can see.
        inbox.grant(slot(4, 0), 1);

        let drained = inbox.take().expect("something pending");
        assert_eq!(
            drained
                .mine
                .get(&slot(4, 0).raw())
                .map(|entry| entry.credit),
            Some(1),
            "a grant at the still-live generation is accepted, whether or \
             not the batcher has already closed that slot locally"
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

    /// Credit and close replies for slots the ingress currently holds are
    /// this side's own writes, and nothing here checks a size limit for
    /// them — see `ControlState`'s field doc for what actually bounds
    /// `peers` across drains. (An `OpenSlot` the ingress rejects outright is
    /// a different lane — see `a_flood_of_bogus_open_rejections_is_capped`.)
    #[test]
    fn replies_for_held_slots_are_never_refused() {
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

    /// `peers` is not bounded by the ingress table's own slot limit: one
    /// index can hold at most one live ingress slot at a time, but a peer
    /// that closes and reopens that index keeps minting a new key here, one
    /// per generation the ingress admitted for it, because nothing removes
    /// an entry except [`ControlState::drain`]. The ceiling is the width of
    /// the generation, not the ingress table's index cap — this pins the
    /// 256-per-index half of that claim directly, without needing 65,536
    /// indices' worth of churn to demonstrate it.
    #[test]
    fn one_index_reopened_through_every_generation_leaves_256_entries_in_peers() {
        let inbox = ControlInbox::default();
        let index = 0;
        for generation in 0..=u8::MAX {
            inbox.reply_credit(slot(index, generation), 1);
        }
        assert_eq!(
            inbox.refused(),
            0,
            "every reply names a generation the ingress table could plausibly \
             have admitted; entry_peer never checks live_generations at all"
        );
        assert_eq!(
            inbox.pending_len(),
            256,
            "one index, 256 generations, 256 keys — not the one entry an \
             \"O(live slots)\" bound would predict"
        );

        let drained = inbox.take().expect("something pending");
        assert_eq!(
            drained.peers.len(),
            256,
            "the 256 keys are in `peers` specifically, not spread across the \
             other maps `pending_len` also sums"
        );
    }

    /// A flood of `OpenSlot` rejections is capped; credit and close replies
    /// for slots the ingress actually holds are not.
    ///
    /// `replies_for_held_slots_are_never_refused` pins the half of the old
    /// "refuses nothing" claim that is still true. This pins the half that
    /// was not: a peer can name an unbounded number of slots it will never
    /// hold — an out-of-range index, a collision, a bind that expired — and
    /// each one produces a rejection here on the ingress task, not on the
    /// peer's own accounting. Dropping one past the cap costs no credit
    /// (see [`MAX_PENDING_REJECTS`]), which is why this lane is capped
    /// instead of grown, and the two lanes are independent: filling this
    /// one must not touch the other.
    #[test]
    fn a_flood_of_bogus_open_rejections_is_capped() {
        let inbox = ControlInbox::default();
        let flood = MAX_PENDING_REJECTS as u32 + 10_000;
        for index in 0..flood {
            inbox.reject_slot(
                slot(MAX_INGRESS_SLOTS_PER_PEER as u32 + index, 0),
                CloseReason::ProtocolError,
            );
        }
        assert_eq!(
            inbox.pending_len(),
            MAX_PENDING_REJECTS,
            "the reject lane stops growing at its cap, unlike the credit lane it merges into"
        );
        assert_eq!(
            inbox.refused(),
            u64::from(flood) - MAX_PENDING_REJECTS as u64,
            "entries past the cap are refused and counted, not silently dropped uncounted"
        );

        // A repeat of a key already pending changes neither count: first
        // reason wins, and it was never new.
        let refused_before = inbox.refused();
        inbox.reject_slot(
            slot(MAX_INGRESS_SLOTS_PER_PEER as u32, 0),
            CloseReason::UnknownSlot,
        );
        assert_eq!(inbox.pending_len(), MAX_PENDING_REJECTS);
        assert_eq!(inbox.refused(), refused_before);

        // The credit lane for a slot the ingress actually holds is a
        // different map on the same inbox and does not feel the flood.
        inbox.note_allocated(slot(0, 0));
        inbox.grant(slot(0, 0), 1);
        assert_eq!(
            inbox.refused(),
            refused_before,
            "a full reject lane must not refuse an ordinary grant"
        );
        let drained = inbox.take().expect("something pending");
        assert_eq!(
            drained
                .mine
                .get(&slot(0, 0).raw())
                .map(|entry| entry.credit),
            Some(1),
            "the grant reaches the batcher past a full reject lane"
        );

        // The defining property of the split: a `RejectSlot` must come out
        // the wire the same way a `CloseSlot` does, which only happens if
        // `drain` actually merges `rejects` into `peers` rather than
        // dropping them once the cap has done its counting.
        assert_eq!(
            drained.peers.len(),
            MAX_PENDING_REJECTS,
            "every capped rejection must reach the batcher's peers map, not \
             just be counted and discarded"
        );
        assert_eq!(
            drained
                .peers
                .get(&slot(MAX_INGRESS_SLOTS_PER_PEER as u32, 0).raw())
                .and_then(|entry| entry.close),
            Some(CloseReason::ProtocolError),
            "the merged entry carries the close reason through, so the peer \
             actually gets told to abandon the slot it opened"
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
