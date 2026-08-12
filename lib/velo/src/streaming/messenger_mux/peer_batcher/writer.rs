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

    /// `min(configured cap, effective eager budget, COALESCE_THRESHOLD)`.
    ///
    /// The threshold is the packing *target*: the shared coalescing writer
    /// stages a frame into one buffered `write_all` only while it fits, so a
    /// batch above it gives back what batching bought. The eager budget is the
    /// ceiling above it — exceed it and the batch quietly becomes a rendezvous
    /// transfer, paying a round trip on behalf of every slot packed into it.
    ///
    /// Asked here, on the batcher's task, because `effective_eager_payload`
    /// accounts for the ambient trace context the send will inject. An
    /// unresolved peer costs the conservative clamp rather than a failed flush.
    fn compute_cap(&mut self) -> usize {
        let eager = self.peer_instance().map_or(usize::MAX, |instance| {
            self.messenger
                .effective_eager_payload(instance, STREAM_BATCH_HANDLER, None)
        });
        let clamped = self
            .config
            .max_batch_bytes
            .min(eager)
            .min(COALESCE_THRESHOLD);
        // Not `clamp`: the floor is applied *after* the three ceilings, and a
        // configured cap below the floor is a legitimate (if useless) setting
        // rather than the panic `clamp` would give it.
        clamped.max(MIN_BATCH_CAP)
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
            // Nothing went out, so the sequence it reserved is not a gap.
            self.next_batch_seq = self.next_batch_seq.wrapping_sub(1);
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

    /// Build and hand off a one- or two-record batch of its own.
    ///
    /// Returns the [`FireResult`] rather than awaiting it: an over-budget record
    /// rides rendezvous, and charging every other slot on the peer for that
    /// round trip is exactly what the singleton path exists to avoid. The
    /// caller fences the one slot involved and watches the admission from a
    /// detached task.
    pub(super) fn dispatch_singleton(
        &mut self,
        write: impl FnOnce(&mut BatchEncoder) -> Result<(), EncodeError>,
    ) -> Option<FireResult> {
        let batch_seq = self.take_batch_seq();
        let mut encoder = BatchEncoder::new(self.epoch, batch_seq);
        if let Err(error) = write(&mut encoder) {
            tracing::error!(%error, "messenger mux: dropping unencodable singleton");
            return None;
        }
        let records = usize::from(encoder.record_count());
        let payload = encoder.finish().freeze();

        if let Some(metrics) = &self.metrics {
            metrics.rendezvous_singleton();
            metrics.batch(MuxDirection::Sent, records);
        }
        match self.messenger.am_send_streaming(STREAM_BATCH_HANDLER) {
            Ok(builder) => Some(builder.raw_payload(payload).worker(self.peer).send()),
            Err(error) => {
                tracing::error!(%error, "messenger mux: could not build a singleton send");
                None
            }
        }
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
