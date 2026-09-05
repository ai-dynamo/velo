// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Batch assembly and the send that ends it.
//!
//! Everything between "there is a record to put on the wire" and "the messenger
//! has it" lives here: the staging buffer, the three clamps that decide how big
//! a batch may get, the sequence numbering, and the two ways a batch leaves — a
//! packed flush that parks on admission, and a singleton that does not.
//!
//! It is a separate component from the batcher because it knows nothing about
//! slots. It cannot spend credit, close a slot or fail an epoch; it reports what
//! happened and the batcher decides. That split is what keeps "a failed
//! admission is epoch death" a statement made in exactly one place.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use velo_ext::{InstanceId, WorkerId};

use super::super::MuxConfig;
use super::super::protocol::{BATCH_HEADER_LEN, BatchEncoder, EncodeError, MAX_RECORDS_PER_BATCH};
use crate::messenger::{FireResult, Messenger};
use crate::observability::{MuxDirection, MuxMetricsHandle};
use crate::streaming::messenger_mux::STREAM_BATCH_HANDLER;
use crate::transports::tcp::framing::COALESCE_THRESHOLD;

/// Smallest batch a clamp may produce: the header plus one empty record.
///
/// A transport that reports a tiny eager budget must not clamp the cap to
/// nothing, or the writer would route every record — including the 13-byte
/// control ones — through rendezvous and never make progress. Records that do
/// not fit above this floor still take the singleton path, which is the correct
/// answer for them.
pub(super) const MIN_BATCH_CAP: usize = BATCH_HEADER_LEN + 13;

/// `min(configured cap, effective eager budget, COALESCE_THRESHOLD)`, floored
/// at [`MIN_BATCH_CAP`].
///
/// The threshold is the packing *target*: the shared coalescing writer stages a
/// frame into one buffered `write_all` only while it fits, so a batch above it
/// gives back what batching bought. The eager budget is the ceiling above it —
/// exceed it and the batch quietly becomes a rendezvous transfer, paying a round
/// trip on behalf of every slot packed into it.
///
/// Split out from the caller because the eager term is the one an in-process
/// pair cannot make bind: every messenger transport's budget is the 256 KiB
/// rendezvous threshold or its own smaller limit, both far above the 64 KiB
/// coalescing threshold, so end to end the other two terms always win. The
/// arithmetic is where that arm is reachable.
pub(super) const fn batch_cap(configured: usize, eager: usize) -> usize {
    let clamped = if configured < eager {
        configured
    } else {
        eager
    };
    let clamped = if clamped < COALESCE_THRESHOLD {
        clamped
    } else {
        COALESCE_THRESHOLD
    };
    // Not `clamp`: the floor is applied *after* the three ceilings, and a
    // configured cap below the floor is a legitimate (if useless) setting rather
    // than the panic `clamp` would give it.
    if clamped > MIN_BATCH_CAP {
        clamped
    } else {
        MIN_BATCH_CAP
    }
}

/// A batch was handed to the messenger and never admitted.
///
/// The writer reports it rather than acting on it: what it means — that every
/// slot packed into that batch now has a `frame_seq` gap the mux cannot close —
/// is the batcher's knowledge, not the writer's.
#[derive(Debug)]
pub(super) struct FlushFailed(pub(super) anyhow::Error);

/// Staging and dispatch for one peer's batches.
pub(super) struct BatchWriter {
    messenger: Arc<Messenger>,
    peer: WorkerId,
    peer_instance: Option<InstanceId>,
    config: MuxConfig,
    metrics: Option<MuxMetricsHandle>,
    epoch: u64,
    next_batch_seq: u32,
    cap: usize,
    encoder: Option<BatchEncoder>,
    buffer: BytesMut,
}

impl BatchWriter {
    pub(super) fn new(
        messenger: Arc<Messenger>,
        peer: WorkerId,
        config: MuxConfig,
        metrics: Option<MuxMetricsHandle>,
        epoch: u64,
    ) -> Self {
        Self {
            messenger,
            peer,
            peer_instance: None,
            config,
            metrics,
            epoch,
            next_batch_seq: 0,
            cap: MIN_BATCH_CAP,
            encoder: None,
            buffer: BytesMut::new(),
        }
    }

    /// The epoch every batch this writer opens is stamped with.
    pub(super) const fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Start again under `epoch`, discarding anything staged.
    ///
    /// The staged batch goes because its records belong to slots that are about
    /// to be failed, and its sequence goes because sequences are scoped by the
    /// epoch above them.
    pub(super) fn reset_epoch(&mut self, epoch: u64) {
        self.epoch = epoch;
        self.next_batch_seq = 0;
        self.encoder = None;
    }

    /// Open a batch if none is staged, and report the byte cap it must respect.
    pub(super) fn ensure_batch(&mut self) -> usize {
        if self.encoder.is_none() {
            self.cap = self.compute_cap();
            let batch_seq = self.take_batch_seq();
            let buffer = std::mem::take(&mut self.buffer);
            self.encoder = Some(BatchEncoder::with_buffer(buffer, self.epoch, batch_seq));
        }
        self.cap
    }

    /// Whether the staged batch can take `bytes` more in `records` more records.
    pub(super) fn fits(&self, bytes: usize, records: u16) -> bool {
        let Some(encoder) = self.encoder.as_ref() else {
            return true;
        };
        encoder.encoded_len().saturating_add(bytes) <= self.cap
            && u32::from(encoder.record_count()) + u32::from(records)
                <= u32::from(MAX_RECORDS_PER_BATCH)
    }

    /// The staged batch, for a caller with a record to append.
    pub(super) fn encoder(&mut self) -> Option<&mut BatchEncoder> {
        self.encoder.as_mut()
    }

    /// The cap for the next batch to this peer.
    ///
    /// The eager budget is asked here, on the batcher's task, because
    /// `effective_eager_payload` accounts for the ambient trace context the send
    /// will inject. An unresolved peer costs the conservative clamp rather than
    /// a failed flush.
    fn compute_cap(&mut self) -> usize {
        let eager = self.peer_instance().map_or(usize::MAX, |instance| {
            self.messenger
                .effective_eager_payload(instance, STREAM_BATCH_HANDLER, None)
        });
        batch_cap(self.config.max_batch_bytes, eager)
    }

    fn peer_instance(&mut self) -> Option<InstanceId> {
        if self.peer_instance.is_none() {
            self.peer_instance = self
                .messenger
                .backend()
                .try_translate_worker_id(self.peer)
                .ok();
        }
        self.peer_instance
    }

    fn take_batch_seq(&mut self) -> u32 {
        let seq = self.next_batch_seq;
        self.next_batch_seq = self.next_batch_seq.wrapping_add(1);
        seq
    }

    /// Write the staged batch, parking on admission until it is the transport's
    /// problem.
    ///
    /// Awaited rather than fired and forgotten because admission is the only
    /// ordered per-target congestion signal a messenger user has: parking here
    /// parks the batcher, in order, instead of a runtime worker.
    pub(super) async fn flush(&mut self) -> Result<(), FlushFailed> {
        let Some(encoder) = self.encoder.take() else {
            return Ok(());
        };
        if encoder.is_empty() {
            self.refund_batch_seq();
            self.buffer = encoder.finish();
            return Ok(());
        }
        let records = usize::from(encoder.record_count());
        let mut finished = encoder.finish();
        let payload = finished.split().freeze();
        self.buffer = finished;

        if let Some(metrics) = &self.metrics {
            metrics.batch(MuxDirection::Sent, records);
        }
        self.dispatch(payload).await.map_err(FlushFailed)
    }

    /// Build and hand off a batch of its own, without parking on admission.
    ///
    /// Returns the [`FireResult`] rather than awaiting it, for the two records
    /// that must not charge a whole peer for their own round trip: an
    /// over-budget record, which rides rendezvous, and an `OpenSlot`, whose ack
    /// is what a producer waits on before it may send anything at all. The
    /// caller fences the one slot involved and watches the admission from a
    /// detached task.
    ///
    /// Both ways out without a send give the reserved sequence back, so this
    /// call's own reservation never leaks — whatever a caller does with the
    /// `Option<FireResult>` it gets back. That does not, by itself, keep the
    /// wire contiguous against a *separate* reservation a caller holds open
    /// across this call: `emit_data`'s clamp-retry path calls `ensure_batch`
    /// (reserving a sequence for a fresh, still-empty encoder) and can in
    /// principle fall through to this method without flushing it first, which
    /// would leave that encoder's sequence to be refunded later out of order
    /// and read at the receiver as a hole followed by a duplicate. It cannot
    /// today: that fallthrough needs [`Self::compute_cap`] to shrink between
    /// the two `ensure_batch` calls in one `emit_data` invocation, and
    /// [`batch_cap`]'s own doc records that the eager term never binds
    /// end-to-end for any transport in this workspace. So this is a caller
    /// discipline the writer cannot enforce by itself — held today by that
    /// arithmetic fact about `emit_data`, not by construction here.
    ///
    /// Every caller flushes immediately before reaching here, so `self.buffer`
    /// is free capacity the last flush handed back — take it the way
    /// `ensure_batch` does, rather than allocating a fresh `BytesMut`, so the
    /// path this flag adds costs no more per open than the awaited one did.
    /// The one caller that does not arrive with `self.buffer` free is
    /// `emit_data`'s clamp-retry arm, which re-opens an encoder (and so
    /// re-takes the buffer into it) before learning the record still does not
    /// fit; a staged `self.encoder` is how that case is told apart, and it
    /// falls back to a fresh allocation because the buffer is already spoken
    /// for.
    pub(super) fn dispatch_singleton(
        &mut self,
        write: impl FnOnce(&mut BatchEncoder) -> Result<(), EncodeError>,
    ) -> Option<FireResult> {
        let batch_seq = self.take_batch_seq();
        let mut encoder = if self.encoder.is_none() {
            BatchEncoder::with_buffer(std::mem::take(&mut self.buffer), self.epoch, batch_seq)
        } else {
            BatchEncoder::new(self.epoch, batch_seq)
        };
        if let Err(error) = write(&mut encoder) {
            tracing::error!(%error, "messenger mux: dropping unencodable singleton");
            self.refund_batch_seq();
            self.buffer = encoder.finish();
            return None;
        }
        let records = usize::from(encoder.record_count());
        // Split rather than freeze whole, so the tail capacity comes back to
        // `self.buffer` exactly as `flush` returns its own — the reuse this
        // method exists to give the next call is worthless if this one never
        // gives the buffer back.
        let mut finished = encoder.finish();
        let payload = finished.split().freeze();
        self.buffer = finished;

        match self.messenger.am_send_streaming(STREAM_BATCH_HANDLER) {
            Ok(builder) => {
                if let Some(metrics) = &self.metrics {
                    metrics.batch(MuxDirection::Sent, records);
                }
                Some(builder.raw_payload(payload).worker(self.peer).send())
            }
            Err(error) => {
                tracing::error!(%error, "messenger mux: could not build a singleton send");
                self.refund_batch_seq();
                None
            }
        }
    }

    /// Give back a sequence a dispatch reserved and then did not send under.
    ///
    /// Nothing went out, so the sequence is not a gap — but a receiver reading
    /// the next batch would meter one: `note_batch_seq` measures the distance
    /// between the `batch_seq` it expected and the one that arrived, and cannot
    /// tell a batch that was lost from one that was never built. Contiguity is
    /// therefore the writer's own invariant, held at the writer's own seam. It
    /// is not covering for a reachable defect: the batcher fails the epoch on
    /// every arm that returns without sending, and that resets the counter.
    fn refund_batch_seq(&mut self) {
        self.next_batch_seq = self.next_batch_seq.wrapping_sub(1);
    }

    async fn dispatch(&self, payload: Bytes) -> anyhow::Result<()> {
        self.messenger
            .am_send_streaming(STREAM_BATCH_HANDLER)?
            .raw_payload(payload)
            .worker(self.peer)
            .send()
            .await
    }
}
