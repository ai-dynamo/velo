// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! What happens to a producer's records — the batcher's data path.
//!
//! Split out of [`super`] rather than living beside the lifecycle it runs
//! inside, for the same reason [`control`](super::control) and
//! [`writer`](super::writer) are: the file was full. What is here is one
//! coherent question — *this record arrived on this slot's inlet; where does it
//! go?* — and the four answers to it:
//!
//! - into the staged batch, when the slot has credit and the record fits;
//! - into the slot's withheld queue, when it does not have credit, which is
//!   what lets the inlet be drained unconditionally so a synchronous `finalize`,
//!   `detach` or `Drop` can never block on a full channel;
//! - out alone through rendezvous, when it is larger than any eager batch to
//!   this peer, fencing its slot until the admission resolves;
//! - or nowhere, when the producer outran the byte cap on a slot nobody is
//!   draining and the slot is closed under it.
//!
//! The slot lifecycle these reach for — `close_local`, `epoch_death`, the batch
//! clamps — stays in [`super`], because those are decisions about the peer
//! rather than about a record.

use std::sync::Arc;

use super::*;

impl Batcher {
    pub(super) async fn on_frame(&mut self, index: u32, bytes: Vec<u8>) {
        let Some(slot) = self.slots.get_mut(index) else {
            // The slot closed between the stream yielding and this dispatch —
            // a terminal in the same drain, or a peer-side close. Today's
            // egress semantics: frames queued behind a terminal are discarded.
            return;
        };

        // Terminal-ness costs an `rmp_serde` decode attempt on anything that is
        // not one of the three cached sentinels, so it is asked only where the
        // answer changes what happens. On the fast path a record with data
        // credit behind it is sent either way. On the starved path it decides
        // whether the reserve applies, and the whole reason the reserve exists
        // is that a terminal must not wait on credit a stalled consumer will
        // never return.
        if slot.must_withhold(CreditClass::Data) {
            let terminal = is_terminal_sentinel(&bytes);
            // The reserve applies to the *head* of the stream, not to any
            // terminal: predecessors still queued go first, and a fence means a
            // predecessor is on the wire. Otherwise the terminal would overtake
            // records the consumer is owed.
            if terminal
                && slot.withheld.is_empty()
                && !slot.is_fenced()
                && slot.credit.can_spend(CreditClass::Terminal)
            {
                self.emit_data(index, bytes, true).await;
                return;
            }

            let starved = slot.credit.data_available() == 0;
            match slot.withheld.push(bytes) {
                Ok(()) => {
                    if let Some(metrics) = &self.metrics {
                        metrics.withheld_records_delta(1);
                        if starved && slot.note_starved() {
                            metrics.credit_exhausted();
                        }
                    }
                }
                Err(error) => self.overflow_kill(index, error).await,
            }
            return;
        }

        let terminal = is_terminal_sentinel(&bytes);
        self.emit_data(index, bytes, terminal).await;
    }

    /// The producer ran past the byte cap on a slot that cannot send.
    ///
    /// This is the per-slot slow-consumer kill, and it is deliberately *not* the
    /// heartbeat watchdog: the slot dies, its consumer sees `Dropped` through the
    /// `CloseSlot{PeerGone}` sent here, and the peer's other slots never notice.
    /// Anything withheld goes with it — including a queued terminal, so a
    /// consumer that would have seen `Finalized` sees `Dropped` instead. That is
    /// the cost of not blocking the producer's synchronous terminal send, and it
    /// is bounded by a megabyte of run-ahead on a stream nobody is draining.
    async fn overflow_kill(&mut self, index: u32, error: slot_stream::WithheldOverflow) {
        let Some(slot) = self.slots.get_mut(index) else {
            return;
        };
        let id = slot.id;
        let seq = slot.take_seq();
        let discarded = slot.withheld.len();
        slot.withheld.clear();
        tracing::warn!(
            slot = ?id,
            %error,
            discarded,
            "messenger mux: producer outran a starved slot's byte cap; closing the slot"
        );
        if let Some(metrics) = &self.metrics {
            metrics.records_dropped(MuxDropReason::WithheldOverflow, discarded as u64 + 1);
            metrics.withheld_records_delta(-(discarded as i64));
        }
        self.push_close(id, seq, CloseReason::PeerGone).await;
        self.close_local(index);
    }

    /// Append a `CloseSlot` for a slot this side owns, cutting the batch first
    /// if it will not fit.
    async fn push_close(&mut self, id: SlotId, seq: u32, reason: CloseReason) {
        let needed = record_encoded_len(1).unwrap_or(usize::MAX);
        self.ensure_batch();
        if !self.fits(needed, 1) {
            self.flush().await;
            self.ensure_batch();
        }
        if let Some(encoder) = self.writer.encoder() {
            let _ = encoder.push_close_slot(id, seq, reason);
            // A close is a liveness record: the consumer behind it is waiting
            // for a terminal or a `Dropped` that only this tells it about, so it
            // does not wait on an application's flush.
            self.gate.stage_urgent(1);
        }
    }

    /// Every producer handle for a slot has gone without a terminal going out.
    ///
    /// The consumer has to be told, or it waits out its heartbeat watchdog for a
    /// sender that no longer exists. `CloseSlot{PeerGone}` is that telling, and
    /// it is what makes the receive side inject `Dropped` — reproducing, per
    /// slot, what the TCP receive pump does on an EOF with no terminal behind
    /// it.
    pub(super) async fn on_inlet_closed(&mut self, index: u32) {
        let Some(slot) = self.slots.get_mut(index) else {
            return;
        };
        slot.inlet_closed = true;
        if !slot.withheld.is_empty() {
            // Records the producer enqueued before it went are still owed to the
            // consumer. The close waits behind them; `release_withheld` fires it
            // when the queue empties.
            return;
        }
        self.finish_inlet_close(index).await;
    }

    /// Emit the `CloseSlot{PeerGone}` a departed producer owes its consumer.
    async fn finish_inlet_close(&mut self, index: u32) {
        let Some(slot) = self.slots.get_mut(index) else {
            return;
        };
        let id = slot.id;
        let seq = slot.take_seq();
        self.push_close(id, seq, CloseReason::PeerGone).await;
        self.close_local(index);
    }

    /// Pack one data record, cutting or bypassing the batch as its size demands.
    async fn emit_data(&mut self, index: u32, bytes: Vec<u8>, terminal: bool) {
        let record_len = record_encoded_len(bytes.len()).unwrap_or(usize::MAX);
        // A terminal and its `CloseSlot` are atomic: same batch, adjacent. Room
        // is therefore checked for the pair, never for the terminal alone —
        // splitting them across batches is a silent protocol violation that
        // only shows up at a cap boundary.
        let close_len = record_encoded_len(1).unwrap_or(usize::MAX);
        let needed = if terminal {
            record_len.saturating_add(close_len)
        } else {
            record_len
        };
        let records = if terminal { 2 } else { 1 };

        let mut cap = self.ensure_batch();
        if BATCH_HEADER_LEN.saturating_add(needed) > cap {
            // Larger than any eager batch to this peer. Flush first so the
            // record keeps its place in the peer's send order, then send it
            // alone and let the messenger stage it through rendezvous.
            self.flush().await;
            self.send_singleton(index, bytes, terminal).await;
            return;
        }
        if !self.fits(needed, records) {
            self.flush().await;
            cap = self.ensure_batch();
            if BATCH_HEADER_LEN.saturating_add(needed) > cap {
                self.send_singleton(index, bytes, terminal).await;
                return;
            }
        }

        let class = if terminal {
            CreditClass::Terminal
        } else {
            CreditClass::Data
        };
        let Some(slot) = self.slots.get_mut(index) else {
            return;
        };
        if slot.credit.try_spend(class).is_err() {
            return;
        }
        let id = slot.id;
        let seq = slot.take_seq();
        let close_seq = terminal.then(|| slot.take_seq());

        if let Some(encoder) = self.writer.encoder() {
            if let Err(error) = encoder.push_data(id, seq, &bytes) {
                tracing::error!(slot = ?id, %error, "messenger mux: dropping unencodable record");
                return;
            }
            match close_seq {
                Some(close_seq) => {
                    let _ = encoder.push_close_slot(id, close_seq, CloseReason::TerminalSent);
                    // The terminal ends a stream, so it and its close travel
                    // like the other liveness records rather than waiting for a
                    // flush the finalizing producer may never make.
                    self.gate.stage_urgent(2);
                }
                None => self.gate.stage(1),
            }
        }

        if terminal {
            self.close_local(index);
        }
    }

    /// Send one record alone, over the eager budget and therefore through
    /// rendezvous, and fence its slot until the admission resolves.
    async fn send_singleton(&mut self, index: u32, bytes: Vec<u8>, terminal: bool) {
        let class = if terminal {
            CreditClass::Terminal
        } else {
            CreditClass::Data
        };
        let Some(slot) = self.slots.get_mut(index) else {
            return;
        };
        if slot.credit.try_spend(class).is_err() {
            return;
        }
        let id = slot.id;
        let seq = slot.take_seq();
        let close_seq = terminal.then(|| slot.take_seq());

        let fire = self.writer.dispatch_singleton(|encoder| {
            encoder.push_data(id, seq, &bytes)?;
            match close_seq {
                Some(close_seq) => {
                    encoder.push_close_slot(id, close_seq, CloseReason::TerminalSent)
                }
                None => Ok(()),
            }
        });
        let Some(fire) = fire else {
            self.epoch_death();
            return;
        };

        if terminal {
            self.close_local(index);
        } else if let Some(slot) = self.slots.get_mut(index) {
            slot.fence();
        }

        // Deliberately not awaited here: fencing is per slot, so every other
        // slot on this peer keeps flowing while the staged transfer is in
        // flight. The resolution comes back as coalesced control.
        let control = Arc::clone(&self.control);
        tokio::spawn(async move {
            control.singleton_resolved(id, fire.await.is_ok());
        });
    }

    /// Drain a slot's withheld queue as far as credit and the fence allow.
    ///
    /// Loops rather than releasing one record, because a grant may cover several
    /// and the queue is the only thing holding `frame_seq` order. Re-reads the
    /// slot each turn: `emit_data` can close it (a terminal) or fence it (an
    /// oversized singleton).
    ///
    /// The terminal reserve does **not** apply here, and that is deliberate: it
    /// buys a terminal past an *empty* queue, not past records the consumer is
    /// still owed. A terminal behind starved predecessors therefore waits with
    /// them, and what ends such a stream is one of the two mechanisms that
    /// already exist for a consumer that stopped draining — the byte cap, if the
    /// producer keeps sending, or `reader_pump`'s heartbeat watchdog if it does
    /// not.
    pub(super) async fn release_withheld(&mut self, index: u32) {
        loop {
            let next = {
                let Some(slot) = self.slots.get_mut(index) else {
                    return;
                };
                if slot.is_fenced() {
                    return;
                }
                match slot.withheld.front() {
                    Some(front) => {
                        let terminal = is_terminal_sentinel(front);
                        let class = if terminal {
                            CreditClass::Terminal
                        } else {
                            CreditClass::Data
                        };
                        if !slot.credit.can_spend(class) {
                            return;
                        }
                        let popped = slot.withheld.pop();
                        if popped.is_some()
                            && let Some(metrics) = &self.metrics
                        {
                            metrics.withheld_records_delta(-1);
                        }
                        popped.map(|bytes| (bytes, terminal))
                    }
                    None => {
                        slot.note_flowing();
                        None
                    }
                }
            };
            match next {
                Some((bytes, terminal)) => self.emit_data(index, bytes, terminal).await,
                // The queue is empty. A producer that left while records were
                // still owed gets its close now.
                None => {
                    if self
                        .slots
                        .get_mut(index)
                        .is_some_and(|slot| slot.inlet_closed)
                    {
                        self.finish_inlet_close(index).await;
                    }
                    return;
                }
            }
        }
    }
}
