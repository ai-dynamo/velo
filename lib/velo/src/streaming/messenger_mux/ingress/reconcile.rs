// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The reconcile visitors: which of a peer's slots a pass visits, and the
//! `CreditUpdate` each visit stages.
//!
//! Three visitors share one rule and differ only in which slots they name.
//! The rule is that the quantity lives on the slot's own
//! [`DrainSignal`](super::DrainSignal), never on the list that named the
//! slot, so a redundant visit finds a count of zero where a delta would
//! double-count.

use super::super::peer_batcher::ReplyRecord;
use super::slot::IngressSlot;
use super::{MAX_INGRESS_SLOTS_PER_PEER, PeerIngress};

/// Reconcile every slot of this peer. The periodic tick's walk, and the
/// backstop for a slot whose drain could not reach [`collect_touched_grants`]
/// because the dirty lane was full when its pump tried to list it.
///
/// Unlike the other two visitors, this one does not route through
/// [`list_drained_slots`], so it is the one that must drain the lane itself:
/// every live slot below is reconciled and cleared unconditionally. Draining
/// the lane first narrows this function's own race window rather than
/// closing it — a drain landing after the drain loop below but before the
/// walk reaches that slot still relists it, leaving one entry behind for the
/// *next* call to discard. That entry is redundant, not harmful: its slot
/// cannot be granted credit for it twice, because the quantity a reconcile
/// reads lives in the slot's own [`DrainSignal`](super::DrainSignal), not in the lane, and the
/// entry itself does not survive this function's next call, which drains the
/// lane unconditionally before it reconciles again. Discarded rather than run
/// through `mark_touched`: nothing downstream of this walk needs a
/// touched-list entry, since every live slot is about to be visited
/// regardless of what named it.
pub(super) fn collect_grants(state: &mut PeerIngress, replies: &mut Vec<ReplyRecord>) {
    for _ in 0..MAX_INGRESS_SLOTS_PER_PEER {
        let Ok(_index) = state.drained_rx.try_recv() else {
            break;
        };
    }
    #[cfg(test)]
    let visits = &mut state.reconcile_visits;
    for entry in &mut state.slots {
        let Some(slot) = entry.as_mut() else {
            continue;
        };
        #[cfg(test)]
        {
            *visits += 1;
        }
        // This walk visits every live slot unconditionally, so clearing here
        // is what keeps `touched`'s meaning scoped to a pass
        // (`IngressSlot::touched`'s doc) without depending on a list entry to
        // find the slot.
        slot.clear_touched();
        reconcile_slot(slot, replies);
    }
}

/// Move the dirty lane's entries onto the pass's reconcile list.
///
/// Drained with `try_recv` and bounded by the lane's own capacity rather than
/// by "until empty": a pump listing a slot while this runs would otherwise be
/// able to hold the pass here, under the peer mutex the batch path needs. An
/// entry that arrives after the bound is not lost — it is the next pass's, and
/// the count that entitles it to credit lives in the slot's own signal.
///
/// Dedup is [`IngressSlot::mark_touched`], the same flag the batch's own
/// deliveries use, so a slot that both received and drained is visited once.
///
/// An entry naming an index whose slot is gone lists nothing, and one naming an
/// index a *different* slot has since taken costs that slot one visit that
/// reads that slot's own count, whatever it is. Neither can misplace credit:
/// the lane carries an index and no quantity, and the quantity lives in the
/// [`DrainSignal`](super::DrainSignal) the slot itself holds.
pub(super) fn list_drained_slots(state: &mut PeerIngress) {
    for _ in 0..MAX_INGRESS_SLOTS_PER_PEER {
        let Ok(index) = state.drained_rx.try_recv() else {
            break;
        };
        let Some(slot) = state.slots.get_mut(index as usize).and_then(Option::as_mut) else {
            continue;
        };
        if slot.mark_touched() {
            state.touched.push(index);
        }
    }
}

/// Reconcile the slots this pass listed, and clear the list it built.
///
/// An index whose slot is gone is skipped: a later record of the same batch
/// closed it, and a retired slot has nobody left to grant credit to — which is
/// what the whole-table walk did with it too, since it ran after every record
/// of the batch had been applied.
///
/// An index whose slot was *replaced* — closed and reopened at the same dense
/// index in one pass — is not skipped: the lookup finds the new occupant and
/// reconciles it, even though the entry that listed the index belonged to the
/// slot that is now gone. That visit is spurious but cannot mint credit the
/// replacement did not earn, because the count a reconcile reads belongs to
/// the slot rather than to the index: `bind` makes one [`DrainSignal`](super::DrainSignal) per
/// bind and `open_slot` claims it, so the replacement reads its own count —
/// zero unless its own pump has already drained something, and either way
/// its own credit, never the retired slot's. If the replacement is listed
/// again later in the same pass, it is visited twice — the first visit takes
/// the whole count and the pending grant with it, so the second finds zero of
/// each and stages nothing.
pub(super) fn collect_touched_grants(state: &mut PeerIngress, replies: &mut Vec<ReplyRecord>) {
    #[cfg(test)]
    let visits = &mut state.reconcile_visits;
    for index in &state.touched {
        let Some(slot) = state
            .slots
            .get_mut(*index as usize)
            .and_then(Option::as_mut)
        else {
            continue;
        };
        #[cfg(test)]
        {
            *visits += 1;
        }
        slot.clear_touched();
        reconcile_slot(slot, replies);
    }
    state.touched.clear();
}

/// Reconcile one slot and stage the `CreditUpdate` it earned, if any.
fn reconcile_slot(slot: &mut IngressSlot, replies: &mut Vec<ReplyRecord>) {
    slot.reconcile();
    if let Some(delta) = slot.take_grant() {
        replies.push(ReplyRecord::CreditUpdate {
            slot: slot.id,
            delta,
        });
    }
}
