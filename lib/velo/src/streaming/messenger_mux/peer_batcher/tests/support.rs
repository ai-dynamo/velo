// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The harness the batcher tests are driven through, and the fixtures too large
//! to keep beside the tests that use them.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::super::*;
use crate::messenger::{Context, Handler};
use crate::observability::VeloMetrics;
use crate::streaming::messenger_mux::STREAM_BATCH_HANDLER;
use crate::streaming::messenger_mux::flow_control::SlotCredit;
use crate::streaming::messenger_mux::protocol::{
    BatchDecoder, BatchHeader, RecordBody, RecordType,
};
use crate::streaming::messenger_mux::test_support::{StallingTransport, stalling_address};
use crate::transports::tcp::TcpTransportBuilder;

pub(super) const RECV_TIMEOUT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Owned mirrors of the borrowed decoder types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct OwnedRecord {
    pub(super) slot: SlotId,
    pub(super) frame_seq: u32,
    pub(super) kind: RecordType,
    pub(super) data: Vec<u8>,
    /// The delta a `CreditUpdate` carried, so a test can assert the *value* a
    /// coalescing merge produced rather than merely that one arrived.
    pub(super) credit: u32,
}

#[derive(Debug, Clone)]
pub(super) struct OwnedBatch {
    pub(super) header: BatchHeader,
    pub(super) encoded_len: usize,
    pub(super) records: Vec<OwnedRecord>,
}

impl OwnedBatch {
    pub(super) fn decode(payload: &Bytes) -> Self {
        let decoder = BatchDecoder::new(payload).expect("decodable batch");
        let header = decoder.header();
        let records = decoder
            .map(|record| {
                let record = record.expect("well-formed record");
                OwnedRecord {
                    slot: record.slot,
                    frame_seq: record.frame_seq,
                    kind: record.record_type(),
                    data: match record.body {
                        RecordBody::Data(body) => body.to_vec(),
                        _ => Vec::new(),
                    },
                    credit: match record.body {
                        RecordBody::CreditUpdate { delta } => delta,
                        _ => 0,
                    },
                }
            })
            .collect();
        Self {
            header,
            encoded_len: payload.len(),
            records,
        }
    }

    pub(super) fn slots(&self) -> std::collections::BTreeSet<u32> {
        self.records.iter().map(|r| r.slot.index()).collect()
    }
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

pub(super) struct Harness {
    pub(super) handle: Arc<BatcherHandle>,
    /// The batcher's own configuration, so [`Harness::open`] can hand a slot
    /// the byte budget the batcher was built with.
    config: MuxConfig,
    pub(super) batches: flume::Receiver<Bytes>,
    pub(super) registry: prometheus::Registry,
    pub(super) cancel: CancellationToken,
    // Held so the messengers outlive the batcher.
    _sender: Arc<Messenger>,
    _capture: Arc<Messenger>,
}

pub(super) fn tcp_transport() -> Arc<crate::transports::tcp::TcpTransport> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    Arc::new(
        TcpTransportBuilder::new()
            .from_listener(listener)
            .expect("from_listener")
            .build()
            .expect("build transport"),
    )
}

pub(super) async fn harness(config: MuxConfig) -> Harness {
    harness_with_hooks(config, None).await
}

/// A sender messenger and a peer capturing every `_stream_batch` payload it is
/// sent.
///
/// The far end of every egress test: a real messenger pair over loopback TCP,
/// so the assertions read wire bytes rather than an accounting mirror.
pub(super) async fn capture_pair() -> (Arc<Messenger>, Arc<Messenger>, flume::Receiver<Bytes>) {
    let sender = Messenger::builder()
        .add_transport(tcp_transport())
        .build()
        .await
        .expect("sender messenger");
    let capture = Messenger::builder()
        .add_transport(tcp_transport())
        .build()
        .await
        .expect("capture messenger");
    sender
        .register_peer(capture.peer_info())
        .expect("register capture");
    capture
        .register_peer(sender.peer_info())
        .expect("register sender");

    let (batch_tx, batches) = flume::unbounded::<Bytes>();
    let handler = Handler::am_handler_async(STREAM_BATCH_HANDLER, move |ctx: Context| {
        let batch_tx = batch_tx.clone();
        async move {
            let _ = batch_tx.send(ctx.payload);
            Ok(())
        }
    })
    // Same dispatch mode the mux uses, so captured order is arrival order.
    .ordered()
    .build();
    capture
        .register_streaming_handler(handler)
        .expect("register capture handler");

    // Let the TCP connections settle so the first send takes the direct path.
    tokio::time::sleep(Duration::from_millis(200)).await;
    (sender, capture, batches)
}

/// As [`harness`], with a barrier installed in the batcher's run loop.
pub(super) async fn harness_with_hooks(
    config: MuxConfig,
    hooks: Option<Arc<super::super::test_hooks::TestHooks>>,
) -> Harness {
    let (sender, capture, batches) = capture_pair().await;

    let registry = prometheus::Registry::new();
    let metrics = Arc::new(VeloMetrics::register(&registry).expect("register metrics"));
    let cancel = CancellationToken::new();
    let peer = capture.instance_id().worker_id();
    let handle = spawn(
        peer,
        BatcherContext {
            messenger: Arc::clone(&sender),
            config: config.clone(),
            metrics: Some(metrics.bind_mux()),
            epochs: Arc::new(AtomicU64::new(1)),
            batchers: Arc::new(DashMap::new()),
            cancel: cancel.clone(),
            hooks,
        },
    );

    Harness {
        handle,
        config,
        batches,
        registry,
        cancel,
        _sender: sender,
        _capture: capture,
    }
}

impl Harness {
    /// Open a slot and return its producer-side inlet plus the id the batcher
    /// allocated, read back off the `OpenSlot` record it eagerly flushed.
    pub(super) async fn open(
        &self,
        anchor_id: u64,
        session_id: u64,
    ) -> (flume::Sender<Vec<u8>>, SlotId) {
        let (inlet, (slot, _)) = self.open_with_header(anchor_id, session_id).await;
        (inlet, slot)
    }

    /// As [`Self::open`], but also yields the header of the eager `OpenSlot`
    /// batch, which is where the epoch and batch sequence are observable.
    pub(super) async fn open_with_header(
        &self,
        anchor_id: u64,
        session_id: u64,
    ) -> (flume::Sender<Vec<u8>>, (SlotId, BatchHeader)) {
        // Deep enough that a test can queue more than one batch's worth on a
        // parked slot before granting credit.
        self.open_with_inlet(anchor_id, session_id, 512).await
    }

    /// As [`Self::open`], but with the slot already holding `credit`.
    ///
    /// The starved open below is what most of these tests want, because they
    /// are about withholding. The flush-policy tests are the opposite case:
    /// they need records to reach the *staged batch*, and a slot with no credit
    /// never gets one there — every record goes to the withheld queue instead
    /// and the test would assert on a batch that was never going to exist.
    pub(super) async fn open_credited(
        &self,
        anchor_id: u64,
        session_id: u64,
        credit: u32,
    ) -> (flume::Sender<Vec<u8>>, SlotId) {
        let (inlet, (slot, _)) = self
            .open_inner(anchor_id, session_id, 512, SlotCredit::new(credit))
            .await;
        (inlet, slot)
    }

    /// As [`Self::open_with_header`], with a caller-chosen inlet depth — the
    /// knob that decides how soon a producer meets a full channel.
    pub(super) async fn open_with_inlet(
        &self,
        anchor_id: u64,
        session_id: u64,
        depth: usize,
    ) -> (flume::Sender<Vec<u8>>, (SlotId, BatchHeader)) {
        self.open_inner(anchor_id, session_id, depth, SlotCredit::new(0))
            .await
    }

    async fn open_inner(
        &self,
        anchor_id: u64,
        session_id: u64,
        depth: usize,
        credit: SlotCredit,
    ) -> (flume::Sender<Vec<u8>>, (SlotId, BatchHeader)) {
        let (inlet_tx, inlet_rx) = flume::bounded::<Vec<u8>>(depth);
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        self.handle
            .open_slot(OpenSlotRequest {
                anchor_id,
                session_id,
                inlet: inlet_rx,
                // The default is deliberately starved. On the attach path a
                // slot opens holding the window its peer advertised, but most
                // arms below are about what the batcher does once a slot has
                // none — withholding, fairness between a parked slot and a
                // flowing one, the reserved terminal — and opening at zero is
                // how a test reaches that state without first spending a
                // window. [`Harness::open_credited`] is for the tests that
                // need the other case.
                credit,
                slot_byte_budget: self.config.slot_byte_budget,
                ack: ack_tx,
            })
            .await
            .expect("queue OpenSlot");
        ack_rx
            .await
            .expect("ack delivered")
            .expect("slot allocated");

        let batch = self.next_batch().await;
        assert_eq!(batch.records.len(), 1, "OpenSlot is flushed on its own");
        assert_eq!(batch.records[0].kind, RecordType::OpenSlot);
        (inlet_tx, (batch.records[0].slot, batch.header))
    }

    pub(super) async fn next_batch(&self) -> OwnedBatch {
        let payload = tokio::time::timeout(RECV_TIMEOUT, self.batches.recv_async())
            .await
            .expect("timed out waiting for a batch")
            .expect("capture channel closed");
        OwnedBatch::decode(&payload)
    }

    pub(super) fn try_next_batch(&self) -> Option<OwnedBatch> {
        self.batches.try_recv().ok().map(|p| OwnedBatch::decode(&p))
    }

    pub(super) fn grant(&self, slot: SlotId, delta: u32) {
        self.handle.grant(slot, delta);
    }

    pub(super) fn snapshot(&self) -> crate::observability::test_helpers::MetricSnapshot {
        crate::observability::test_helpers::MetricSnapshot::from_registry(&self.registry)
    }

    /// Records the batcher has pulled from inlets and parked.
    pub(super) fn withheld(&self) -> f64 {
        self.snapshot()
            .gauge("velo_streaming_mux_withheld_records", &[])
    }

    /// Wait until exactly `count` records are parked.
    ///
    /// A positive fact to wait for, unlike "no batch has arrived yet" — which
    /// is true before the batcher has run at all and so proves nothing.
    pub(super) async fn await_withheld(&self, count: usize) {
        eventually(|| (self.withheld() - count as f64).abs() < f64::EPSILON).await;
    }

    /// Records packed into a batch the writer has open but has not written.
    pub(super) fn staged(&self) -> f64 {
        self.snapshot()
            .gauge("velo_streaming_mux_staged_records", &[])
    }

    /// Wait until exactly `count` records are staged.
    ///
    /// The positive fact the manual-policy tests need. "The inlet is empty"
    /// would not do: it proves the batcher *pulled* the records, which is also
    /// true when it withheld them, and staging is the thing being asserted.
    pub(super) async fn await_staged(&self, count: usize) {
        eventually(|| (self.staged() - count as f64).abs() < f64::EPSILON).await;
    }

    /// An application flush, as `Velo::flush_batch` delivers it.
    pub(super) fn flush_batch(&self) {
        self.handle.kick_flush();
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

pub(super) fn item(n: u32) -> Vec<u8> {
    rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::Item(n)).expect("encode item")
}

/// Wait until `predicate` holds, polling the batcher's observable state.
pub(super) async fn eventually(mut predicate: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + RECV_TIMEOUT;
    while tokio::time::Instant::now() < deadline {
        if predicate() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("condition never held within {RECV_TIMEOUT:?}");
}

// ---------------------------------------------------------------------------
// A batcher whose peer never takes a second frame
// ---------------------------------------------------------------------------

/// A batcher over a [`StallingTransport`], plus the wire nobody drains.
///
/// The congested peer, made deterministic: the gate holds exactly one frame, so
/// the first send lands and every send after it parks until a test takes that
/// one out. Separate from [`Harness`] because there is no capture messenger and
/// no far end at all — the admission gate is the whole of the peer.
pub(super) struct StalledHarness {
    pub(super) handle: Arc<BatcherHandle>,
    /// The batcher's own configuration, so [`StalledHarness::open`] hands a slot
    /// the byte budget the batcher was built with.
    config: MuxConfig,
    /// Frames the gate has admitted. Draining one place lets the next in.
    pub(super) wire: flume::Receiver<(Bytes, Bytes)>,
    pub(super) registry: prometheus::Registry,
    pub(super) cancel: CancellationToken,
    // Held so the messenger outlives the batcher.
    _sender: Arc<Messenger>,
}

pub(super) async fn stalled_harness(config: MuxConfig) -> StalledHarness {
    stalled_harness_with_hooks(config, None).await
}

pub(super) async fn stalled_harness_with_hooks(
    config: MuxConfig,
    hooks: Option<Arc<super::super::test_hooks::TestHooks>>,
) -> StalledHarness {
    let (transport, wire) = StallingTransport::new(tokio::runtime::Handle::current());
    let sender = Messenger::builder()
        .add_transport(transport)
        .build()
        .await
        .expect("sender messenger");
    // A peer id the transport accepts. Nothing ever reads the far end; the
    // gate is the only thing under test.
    let peer_instance = velo_ext::InstanceId::new_v4();
    sender
        .register_peer(velo_ext::PeerInfo::new(peer_instance, stalling_address()))
        .expect("register peer");

    let registry = prometheus::Registry::new();
    let metrics = Arc::new(VeloMetrics::register(&registry).expect("register metrics"));
    let cancel = CancellationToken::new();
    let handle = spawn(
        peer_instance.worker_id(),
        BatcherContext {
            messenger: Arc::clone(&sender),
            config: config.clone(),
            metrics: Some(metrics.bind_mux()),
            epochs: Arc::new(AtomicU64::new(1)),
            batchers: Arc::new(DashMap::new()),
            cancel: cancel.clone(),
            hooks,
        },
    );

    StalledHarness {
        handle,
        config,
        wire,
        registry,
        cancel,
        _sender: sender,
    }
}

impl StalledHarness {
    /// Queue an `OpenSlot` and hand back the producer inlet and the ack channel.
    ///
    /// The ack is deliberately **not** awaited here: whether it arrives before
    /// the `OpenSlot` is admitted is the property these tests are about, so a
    /// helper that waited for it would decide the question it is used to ask.
    pub(super) async fn open(
        &self,
        anchor_id: u64,
        session_id: u64,
        credit: u32,
    ) -> (
        flume::Sender<Vec<u8>>,
        oneshot::Receiver<Result<(), OpenRejected>>,
    ) {
        // Deep enough that a producer never meets a full channel while the
        // batcher is parked on admission and therefore not draining it.
        let (inlet_tx, inlet_rx) = flume::bounded::<Vec<u8>>(512);
        let (ack_tx, ack_rx) = oneshot::channel();
        self.handle
            .open_slot(OpenSlotRequest {
                anchor_id,
                session_id,
                inlet: inlet_rx,
                credit: SlotCredit::new(credit),
                slot_byte_budget: self.config.slot_byte_budget,
                ack: ack_tx,
            })
            .await
            .expect("queue OpenSlot");
        (inlet_tx, ack_rx)
    }

    /// Take one admitted frame off the gate, freeing its place.
    pub(super) async fn next_wire_batch(&self) -> OwnedBatch {
        let (_, payload) = tokio::time::timeout(RECV_TIMEOUT, self.wire.recv_async())
            .await
            .expect("timed out waiting for an admitted frame")
            .expect("wire closed");
        OwnedBatch::decode(&payload)
    }

    /// Drop the receiver behind the gate, so a frame parked in it fails to
    /// admit instead of merely waiting.
    ///
    /// `StallingTransport`'s gate is a bounded(1) `flume` channel over `wire`;
    /// dropping this end disconnects it, and the gate's driver task resolves
    /// every queued ticket `Err(AdmissionError::ChannelClosed)` rather than
    /// leaving it parked. That is the one way this fixture can produce a real
    /// failed admission rather than an admission a test injects by calling
    /// `singleton_resolved` directly.
    pub(super) fn disconnect_wire(&mut self) {
        let (_tx, unused) = flume::bounded(0);
        drop(std::mem::replace(&mut self.wire, unused));
    }

    pub(super) fn snapshot(&self) -> crate::observability::test_helpers::MetricSnapshot {
        crate::observability::test_helpers::MetricSnapshot::from_registry(&self.registry)
    }

    /// Records packed into a batch the writer has open but has not written.
    pub(super) fn staged(&self) -> f64 {
        self.snapshot()
            .gauge("velo_streaming_mux_staged_records", &[])
    }

    /// Records the producer ran past a starved slot's byte cap and lost.
    pub(super) fn overflow_dropped(&self) -> f64 {
        self.snapshot().counter(
            "velo_streaming_mux_records_dropped_total",
            &[("reason", "withheld_overflow")],
        )
    }

    /// Wait until exactly `count` records are parked — for want of credit, or
    /// behind a fence.
    ///
    /// A positive fact, unlike "nothing has reached the wire yet", which is
    /// true before the batcher has run at all and so proves nothing.
    pub(super) async fn await_withheld(&self, count: usize) {
        eventually(|| {
            let withheld = self
                .snapshot()
                .gauge("velo_streaming_mux_withheld_records", &[]);
            (withheld - count as f64).abs() < f64::EPSILON
        })
        .await;
    }
}

impl Drop for StalledHarness {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
