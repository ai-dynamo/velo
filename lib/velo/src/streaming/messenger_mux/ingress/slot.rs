// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Receive-side per-slot state: the mux-owned buffer, its credit account, and
//! the ahead-of-sequence hold.
//!
//! Everything here is synchronous and non-blocking, because the `_stream_batch`
//! handler runs on the peer's ordering lane and holds it for the duration. An
//! `await` into an anchor channel would stall every slot from that peer behind
//! one slow consumer — head-of-line blocking with a worse failure mode than a
//! socket's, since lane channels are unbounded and blocking converts
//! backpressure into unbounded memory growth.
//!
//! > **Invariant.** A slot never has more than `C` records outstanding against a
//! > `C + 1`-deep buffer, so [`IngressSlot::apply_data`] only `try_send`s into
//! > space credit already reserved. `velo_streaming_mux_reader_stall_total > 0`
//! > is a bug, not a tuning signal.
//!
//! Credit is issued against *this* buffer and never against the anchor's
//! `frame_tx`, which has writers other than the mux — the local same-worker
//! attach path, detach and finalize, `reader_pump`'s watchdog injection, and
//! decisively M concurrent MPSC senders. Any "C credits against a C-deep
//! channel" proof collapses the moment a second writer exists.

use std::collections::{BTreeMap, VecDeque};

use super::super::flow_control::{ByteBudget, CreditClass, SlotCreditAccount, try_reserve_pair};
use super::super::protocol::{CloseReason, RecordType, SlotId};
use crate::streaming::sender::{cached_dropped, cached_heartbeat, is_terminal_sentinel};

/// What applying a record did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Applied {
    /// Handed to the consumer (possibly along with records the arrival released
    /// from the hold).
    Delivered,
    /// Parked ahead of its sequence, waiting for the gap to close.
    Held,
    /// Behind the slot's sequence: already seen, and dropped.
    Duplicate,
    /// `try_send` failed on space credit had already reserved. Always a bug;
    /// metered as such, then treated as a protocol fault so the slot does not
    /// silently swallow records.
    ReaderStall,
    /// The slot must be closed with this reason. Scoped to this slot — the
    /// lane and the peer's other slots are untouched.
    Fault(CloseReason),
}

/// One receive-side slot.
pub(super) struct IngressSlot {
    /// Index and generation this slot answers to.
    pub(super) id: SlotId,
    /// The mux-owned `C + 1`-deep buffer handed to the anchor by `bind`.
    frame_tx: flume::Sender<Vec<u8>>,
    account: SlotCreditAccount,
    /// Encoded sizes of the records currently sitting in `frame_tx`, oldest
    /// first. Popped as the reconcile step observes them drain, which is how
    /// byte occupancy stays exact without a per-slot drain task.
    sizes: VecDeque<u32>,
    buffered_bytes: u64,
    /// Occupancy above which credit stops being advertised.
    byte_watermark: u64,
    next_seq: u32,
    hold: BTreeMap<u32, Vec<u8>>,
    hold_bytes: ByteBudget,
    /// A `CloseSlot` that arrived ahead of records still in the hold.
    ///
    /// `CloseSlot{TerminalSent}` is position-ordered behind its terminal, and
    /// that terminal can be sitting in the hold — a rendezvous singleton
    /// resolves outside the ordered lane, so eager successors overtake it.
    /// Applying the close on arrival would drop the mux-side sender early and
    /// hand the consumer `Dropped` where it should have seen `Finalized`.
    pending_close: Option<(u32, CloseReason)>,
}

/// Why a record could not be handed to the consumer.
enum DeliverFault {
    /// The peer overspent the credit it was granted.
    Overspend,
    /// The `C + 1` buffer was full. Breaks the invariant; always a bug.
    ReaderStall,
    /// The anchor side dropped its receiver.
    ConsumerGone,
}

impl IngressSlot {
    /// Open a slot against the buffer `bind` created, granting `initial_credit`.
    pub(super) fn new(
        id: SlotId,
        frame_tx: flume::Sender<Vec<u8>>,
        initial_credit: u32,
        slot_byte_budget: u32,
        first_seq: u32,
    ) -> Self {
        Self {
            id,
            frame_tx,
            account: SlotCreditAccount::new(initial_credit),
            sizes: VecDeque::new(),
            buffered_bytes: 0,
            byte_watermark: u64::from(slot_byte_budget),
            next_seq: first_seq,
            hold: BTreeMap::new(),
            hold_bytes: ByteBudget::new(u64::from(slot_byte_budget)),
            pending_close: None,
        }
    }

    /// Records currently parked ahead of sequence.
    pub(super) fn held(&self) -> usize {
        self.hold.len()
    }

    /// The window this slot opened holding: data credit, then byte watermark.
    ///
    /// The one read that says what `IngressSlot::new` was actually handed. A
    /// ticket quotes those two numbers to a sender long before this slot
    /// exists, and the only thing keeping the two reads equal is the config
    /// normalisation between them.
    #[cfg(test)]
    pub(super) fn open_terms(&self) -> (u32, u64) {
        (self.account.limit(), self.byte_watermark)
    }

    /// Apply a `Data` or `SlotHeartbeat` record.
    ///
    /// `frame_seq` is compared with plain ordering rather than modulo: it is
    /// per slot, `BATCHING.md` calls `u32` unreachable there, and the egress
    /// counter saturates instead of wrapping — so a value below `next_seq` is a
    /// duplicate and never a wrap.
    pub(super) fn apply_data(
        &mut self,
        frame_seq: u32,
        body: Vec<u8>,
        peer_bytes: &mut ByteBudget,
    ) -> Applied {
        if frame_seq < self.next_seq {
            return Applied::Duplicate;
        }
        if frame_seq > self.next_seq {
            return self.park(frame_seq, body, peer_bytes);
        }

        let class = classify(&body);
        if let Err(fault) = self.admit(class) {
            return fault_reason(&fault);
        }
        if let Err(fault) = self.deliver(body) {
            return fault_reason(&fault);
        }
        self.next_seq = self.next_seq.saturating_add(1);
        self.release_hold(peer_bytes)
    }

    /// Apply a `CloseSlot` sent by the slot's owner.
    ///
    /// Returns `true` when the close takes effect now; `false` when it has been
    /// deferred behind records still in the hold.
    pub(super) fn apply_close(&mut self, frame_seq: u32, reason: CloseReason) -> bool {
        if frame_seq > self.next_seq {
            self.pending_close = Some((frame_seq, reason));
            return false;
        }
        true
    }

    /// The close deferred behind the hold, if the hold has since drained past
    /// its sequence.
    pub(super) fn due_close(&mut self) -> Option<CloseReason> {
        let (seq, reason) = self.pending_close?;
        (seq <= self.next_seq).then(|| {
            self.pending_close = None;
            reason
        })
    }

    /// Account for records the consumer has drained and report the credit now
    /// waiting to be advertised.
    ///
    /// Occupancy is *reconciled* rather than observed at the drain point:
    /// `flume` has no consumed-callback, and a per-slot drain task would
    /// reintroduce exactly the per-stream tasks the mux exists to delete. The
    /// count is exact at every sample — only its timing is sampled — and the
    /// sweep in [`super`] is what guarantees a slot with no further arrivals
    /// still gets its credit back.
    pub(super) fn reconcile(&mut self) {
        let in_channel = self.frame_tx.len() as u32;
        let resident = in_channel.saturating_add(self.hold.len() as u32);
        let drained = self.account.buffered().saturating_sub(resident);
        if drained == 0 {
            return;
        }
        for _ in 0..drained {
            let Some(size) = self.sizes.pop_front() else {
                break;
            };
            self.buffered_bytes = self.buffered_bytes.saturating_sub(u64::from(size));
        }
        self.account.release(drained);
    }

    /// Credit to advertise back to the sender, if any.
    ///
    /// Withheld while the slot is over its byte watermark. Frame credit gives
    /// the no-head-of-line-blocking proof and byte credit the memory bound;
    /// where the two grants disagree — `C` records of a megabyte each against a
    /// one-megabyte slot cap — it is the byte side that has to win, and it wins
    /// by throttling the next grant rather than by refusing a record whose
    /// frame credit was already given.
    pub(super) fn take_grant(&mut self) -> Option<u32> {
        if self.buffered_bytes >= self.byte_watermark {
            return None;
        }
        self.account.take_pending_grant()
    }

    /// Inject the `Dropped` sentinel a consumer sees when its sender dies.
    ///
    /// Deliberately `Dropped` and not `TransportError`: this reproduces
    /// `pump_frames`' existing behaviour and keeps the consumer-visible
    /// `StreamError::SenderDropped` byte-identical. `TransportError` stays
    /// reserved for protocol violations.
    ///
    /// Spends the terminal reserve, which is also the guard against a second
    /// one: a slot that already delivered a terminal has none left, so a close
    /// arriving behind a `Finalized` cannot append a spurious `Dropped`.
    pub(super) fn inject_dropped(&mut self) -> bool {
        if self.account.admit(CreditClass::Terminal).is_err() {
            return false;
        }
        self.frame_tx.try_send(cached_dropped().clone()).is_ok()
    }

    /// Bytes this slot has reserved from the peer budget, for release on close.
    pub(super) fn hold_bytes_used(&self) -> u64 {
        self.hold_bytes.used()
    }

    fn park(&mut self, frame_seq: u32, body: Vec<u8>, peer_bytes: &mut ByteBudget) -> Applied {
        let class = classify(&body);
        if let Err(fault) = self.admit(class) {
            return fault_reason(&fault);
        }
        if try_reserve_pair(peer_bytes, &mut self.hold_bytes, body.len()).is_err() {
            return Applied::Fault(CloseReason::ProtocolError);
        }
        self.hold.insert(frame_seq, body);
        Applied::Held
    }

    fn release_hold(&mut self, peer_bytes: &mut ByteBudget) -> Applied {
        while let Some(body) = self.hold.remove(&self.next_seq) {
            let len = body.len();
            if let Err(fault) = self.deliver(body) {
                super::super::flow_control::release_pair(peer_bytes, &mut self.hold_bytes, len);
                return fault_reason(&fault);
            }
            super::super::flow_control::release_pair(peer_bytes, &mut self.hold_bytes, len);
            self.next_seq = self.next_seq.saturating_add(1);
        }
        Applied::Delivered
    }

    fn admit(&mut self, class: CreditClass) -> Result<(), DeliverFault> {
        self.account
            .admit(class)
            .map_err(|_| DeliverFault::Overspend)
    }

    /// Hand one record to the consumer. Never blocks — see the module docs.
    fn deliver(&mut self, body: Vec<u8>) -> Result<(), DeliverFault> {
        let len = body.len() as u32;
        match self.frame_tx.try_send(body) {
            Ok(()) => {
                self.sizes.push_back(len);
                self.buffered_bytes = self.buffered_bytes.saturating_add(u64::from(len));
                Ok(())
            }
            Err(flume::TrySendError::Full(_)) => Err(DeliverFault::ReaderStall),
            Err(flume::TrySendError::Disconnected(_)) => Err(DeliverFault::ConsumerGone),
        }
    }
}

/// A record's credit class, from bytes the sender encoded.
///
/// `is_terminal_sentinel` works unchanged on record bodies — that is the whole
/// reason `Data` carries the existing `rmp_serde` encoding byte-for-byte — so
/// terminal handling gains no new code path and no new place to diverge.
fn classify(body: &[u8]) -> CreditClass {
    CreditClass::of(RecordType::Data, is_terminal_sentinel(body))
}

fn fault_reason(fault: &DeliverFault) -> Applied {
    match fault {
        // The peer spent credit it was not granted, or sent a second terminal.
        DeliverFault::Overspend => Applied::Fault(CloseReason::ProtocolError),
        DeliverFault::ReaderStall => Applied::ReaderStall,
        // The consumer went away. Nothing is wrong with the peer; this side
        // simply has nowhere left to put the records, which is the same answer
        // it gives for a slot it never had a binding for.
        DeliverFault::ConsumerGone => Applied::Fault(CloseReason::UnknownSlot),
    }
}

/// The bytes a `SlotHeartbeat` record turns into on the way to the consumer.
///
/// A heartbeat is a `Data`-class record on purpose: dropping one under
/// saturation *is* the per-slot saturation signal `reader_pump`'s
/// `DETECTION_MULTIPLIER` watches for, and it is the only thing a streaming beat
/// still uniquely carries now that the Messenger detects process, host and
/// connection death itself.
pub(super) fn heartbeat_frame() -> Vec<u8> {
    cached_heartbeat().clone()
}
