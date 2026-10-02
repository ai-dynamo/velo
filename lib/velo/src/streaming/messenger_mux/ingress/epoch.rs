// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The epoch and `batch_seq` checks a batch passes before its records are
//! applied, and the retirement of a dying epoch's slots.

use super::super::protocol::{BatchHeader, batch_seq_gap, batch_seq_is_newer};
use super::{BatchOutcome, PeerIngress};
use crate::observability::{MuxDropReason, MuxMetricsHandle};

/// Decide what to do with a batch's epoch. `false` means discard the batch.
pub(super) fn accept_epoch(
    state: &mut PeerIngress,
    header: &BatchHeader,
    metrics: Option<&MuxMetricsHandle>,
    outcome: &mut BatchOutcome,
) -> bool {
    match state.epoch {
        // First batch from this peer: adopt whatever epoch it names.
        None => state.epoch = Some(header.peer_epoch),
        Some(current) if header.peer_epoch < current => {
            // Discarded wholesale by header inspection rather than drained
            // record by record against state that has moved on.
            if let Some(metrics) = metrics {
                metrics.records_dropped(MuxDropReason::StaleEpoch, u64::from(header.record_count));
            }
            return false;
        }
        Some(current) if header.peer_epoch > current => {
            // The reconnect, seen from the receive side. Egress learns of epoch
            // death from a failed admission; the receiver's only signal is this
            // header, and without acting on it the old epoch's slots leak for
            // the life of the process and `live_slots` never returns to zero.
            outcome.closed += retire_epoch(state, metrics);
            state.epoch = Some(header.peer_epoch);
            state.last_batch_seq = None;
        }
        Some(_) => {}
    }
    true
}

/// Meter the batch's sequence against the newest one seen from this peer.
///
/// The mark only moves forward. A batch behind it — a duplicate, or one that
/// arrived after its successor — is not a gap and does not move the mark.
/// Metering it would add the wrapped difference, near `u32::MAX`, to a counter
/// that means "batches missing", and moving the mark back would count its
/// successor's gap a second time when the sequence resumes past it. A detached
/// open under `MuxConfig::async_open_ack` can invert a pair this way (see
/// `Batcher::open_detached` in `peer_batcher`); what the meter reports for
/// one is the single batch its later half looked like when it arrived first,
/// and nothing more.
pub(super) fn note_batch_seq(
    state: &mut PeerIngress,
    header: &BatchHeader,
    metrics: Option<&MuxMetricsHandle>,
) {
    let received = header.batch_seq;
    if let Some(last) = state.last_batch_seq {
        if !batch_seq_is_newer(received, last) {
            return;
        }
        let gap = batch_seq_gap(last.wrapping_add(1), received);
        if gap > 0
            && let Some(metrics) = metrics
        {
            metrics.batch_seq_gap(gap);
        }
    }
    state.last_batch_seq = Some(received);
}

/// Retire every slot of a dying epoch, injecting exactly one `Dropped` each.
pub(super) fn retire_epoch(state: &mut PeerIngress, metrics: Option<&MuxMetricsHandle>) -> usize {
    let mut closed = 0;
    for index in 0..state.slots.len() {
        if let Some(mut slot) = state.slots[index].take() {
            // Only this slot's holds go back: the budget is shared with the
            // peer's other lanes, whose epochs live on.
            state.peer_bytes.release(slot.hold_bytes_used() as usize);
            if let Some(metrics) = metrics
                && slot.held() > 0
            {
                metrics.held_records_delta(-(slot.held() as i64));
            }
            slot.inject_dropped();
            closed += 1;
        }
    }
    state.slots.clear();
    // The entries here name slots of the epoch being retired. On the ordinary
    // path the list is already empty at this point — the epoch check runs
    // before any record is applied, and `shutdown` runs with no batch in
    // flight — so this clears the poison path `touched`'s own doc names (a
    // panic between a push and the drain), plus any future caller that
    // retires mid-batch. The table is cleared and regrows from index zero, so
    // a left-behind entry would send the next pass to whatever slot takes that
    // index back — a reconcile of a slot neither the batch nor a consumer named.
    // Clearing keeps every entry meaning what the pass assumes it means.
    //
    // The dirty set gets no matching clear. A retired index's own consumer can
    // still list it — draining what was already in the C + 1 buffer at
    // close — and that listing outlives this function with nothing here to
    // name it. Left alone, it costs whatever visits the index next one empty visit
    // (nothing, if the index stays closed) or one spurious visit of a
    // replacement (harmless per `collect_touched_grants`'s doc: the visit
    // reads the replacement's own count, whatever its own consumer has drained
    // since, and that count is always its own — `bind` makes one
    // `DrainSignal` per bind and `open_slot` claims it, so it can never be the
    // retired slot's). `collect_grants`'s periodic walk also takes the whole
    // set, so such a listing cannot outlive one tick.
    state.touched.clear();
    closed
}
