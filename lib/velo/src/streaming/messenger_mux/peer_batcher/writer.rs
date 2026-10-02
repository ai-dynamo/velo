// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Batch assembly and the send that ends it.
//!
//! Everything between "there is a record to put on the wire" and "the messenger
//! has it" lives here: the staging buffer, the two clamps that decide how big
//! a batch may get, the sequence numbering, and the two ways a batch leaves — a
//! packed flush that parks on admission, and a singleton that does not.
//!
//! It is a separate component from the batcher because it knows nothing about
//! slots. It cannot spend credit, close a slot or fail an epoch; it reports what
//! happened and the batcher decides. That split is what keeps "a failed
//! admission is epoch death" a statement made in exactly one place.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use velo_ext::InstanceId;

use super::super::protocol::{BATCH_HEADER_LEN, BatchEncoder, EncodeError, MAX_RECORDS_PER_BATCH};
use super::super::{MuxConfig, PeerLane};
use crate::messenger::{FireResult, Messenger};
use crate::observability::MuxMetricsHandle;
use crate::transports::tcp::framing::COALESCE_THRESHOLD;

/// Smallest batch a clamp may produce: the header plus one empty record.
///
/// A transport that reports a tiny eager budget must not clamp the cap to
/// nothing, or the writer would route every record — including the 13-byte
/// control ones — through rendezvous and never make progress. Records that do
/// not fit above this floor still take the singleton path, which is the correct
/// answer for them.
pub(super) const MIN_BATCH_CAP: usize = BATCH_HEADER_LEN + 13;

/// `min(configured cap, effective eager budget)`, floored at [`MIN_BATCH_CAP`].
///
/// The eager budget prevents a packed batch from becoming a rendezvous
/// transfer. TCP's coalescing threshold is not a message limit: larger frames
/// use its direct write path. Let the caller choose that tradeoff, also for
/// transports that do not use TCP's coalescing writer.
pub(super) const fn batch_cap(configured: usize, eager: usize) -> usize {
    let clamped = if configured < eager {
        configured
    } else {
        eager
    };
    // Not `clamp`: the floor is applied *after* the two ceilings, and a
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

/// Staging and dispatch for one (peer, lane)'s batches.
pub(super) struct BatchWriter {
    messenger: Arc<Messenger>,
    /// Where batches go: to `key.peer`, through the lane's own batch handler,
    /// on the transport lane of the same index. One handler on one ordered
    /// connection is what keeps the lane's batches in order.
    key: PeerLane,
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
        key: PeerLane,
        config: MuxConfig,
        metrics: Option<MuxMetricsHandle>,
        epoch: u64,
    ) -> Self {
        Self {
            messenger,
            key,
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

    pub(super) async fn check_peer_health(&mut self) -> anyhow::Result<()> {
        let peer = self
            .peer_instance()
            .ok_or_else(|| anyhow::anyhow!("peer is not registered"))?;
        match self
            .messenger
            .backend()
            .check_peer_health(peer, std::time::Duration::from_secs(5))
            .await
        {
            Ok(()) | Err(crate::transports::HealthCheckError::NeverConnected) => Ok(()),
            Err(error) => Err(error.into()),
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
            self.encoder = Some(BatchEncoder::with_buffer(
                buffer,
                self.epoch,
                batch_seq,
                self.key.lane,
            ));
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
        let eager = self.peer_instance().map_or(COALESCE_THRESHOLD, |instance| {
            self.messenger
                .effective_eager_payload(instance, self.key.lane.handler_name(), None)
        });
        batch_cap(self.config.max_batch_bytes, eager)
    }

    fn peer_instance(&mut self) -> Option<InstanceId> {
        if self.peer_instance.is_none() {
            self.peer_instance = self
                .messenger
                .backend()
                .try_translate_worker_id(self.key.peer)
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
        let by_type = encoder.record_type_counts();
        let mut finished = encoder.finish();
        let payload = finished.split().freeze();
        self.buffer = finished;

        if let Some(metrics) = &self.metrics {
            metrics.batch_sent(by_type);
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
    /// Both ways out without a send give this call's reserved sequence back.
    /// Callers must flush any staged batch first, including an empty batch
    /// opened before a refreshed eager budget routes a record here. Otherwise
    /// its reserved sequence would become a gap ahead of this singleton.
    ///
    /// The previous flush returns spare buffer capacity for this write. The
    /// fallback allocation keeps this helper from taking a live encoder's
    /// buffer if another internal caller is added later.
    pub(super) fn dispatch_singleton(
        &mut self,
        write: impl FnOnce(&mut BatchEncoder) -> Result<(), EncodeError>,
    ) -> Option<FireResult> {
        let batch_seq = self.take_batch_seq();
        let mut encoder = if self.encoder.is_none() {
            BatchEncoder::with_buffer(
                std::mem::take(&mut self.buffer),
                self.epoch,
                batch_seq,
                self.key.lane,
            )
        } else {
            BatchEncoder::new(self.epoch, batch_seq, self.key.lane)
        };
        if let Err(error) = write(&mut encoder) {
            tracing::error!(%error, "messenger mux: dropping unencodable singleton");
            self.refund_batch_seq();
            self.buffer = encoder.finish();
            return None;
        }
        let by_type = encoder.record_type_counts();
        // Split rather than freeze whole, so the tail capacity comes back to
        // `self.buffer` exactly as `flush` returns its own — the reuse this
        // method exists to give the next call is worthless if this one never
        // gives the buffer back.
        let mut finished = encoder.finish();
        let payload = finished.split().freeze();
        self.buffer = finished;

        match self
            .messenger
            .am_send_streaming(self.key.lane.handler_name())
        {
            Ok(builder) => {
                if let Some(metrics) = &self.metrics {
                    metrics.batch_sent(by_type);
                }
                Some(
                    builder
                        .raw_payload(payload)
                        .worker(self.key.peer)
                        .lane(self.key.lane.get())
                        .send(),
                )
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
            .am_send_streaming(self.key.lane.handler_name())?
            .raw_payload(payload)
            .worker(self.key.peer)
            .lane(self.key.lane.get())
            .send()
            .await
    }
}
