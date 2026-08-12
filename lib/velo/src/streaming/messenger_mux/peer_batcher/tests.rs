// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Egress tests.
//!
//! The batcher is driven directly rather than through a `MessengerMuxTransport`,
//! because every property worth pinning here is about what it *packs*, and
//! commands are the only input it has. The peer is a real messenger with a
//! capture handler registered on `_stream_batch`, so the assertions read the
//! actual wire bytes rather than an internal accounting mirror.
//!
//! Nothing here races the batcher for timing. Where a test needs several
//! records in one batch it queues them on parked slots first and then grants
//! credit, which the batcher drains in a single wake — the opportunistic policy
//! taking everything that is *already* queued, exactly as it does under a
//! forward pass.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use tokio_util::sync::CancellationToken;

use super::test_support::StallingTransport;
use super::*;
use crate::messenger::{Context, Handler};
use crate::observability::VeloMetrics;
use crate::streaming::messenger_mux::STREAM_BATCH_HANDLER;
use crate::streaming::messenger_mux::protocol::{
    BatchDecoder, BatchHeader, RecordBody, RecordType,
};
use crate::streaming::sender::cached_finalized;
use crate::transports::tcp::TcpTransportBuilder;
use crate::transports::tcp::framing::COALESCE_THRESHOLD;

const RECV_TIMEOUT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Owned mirrors of the borrowed decoder types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
struct OwnedRecord {
    slot: SlotId,
    frame_seq: u32,
    kind: RecordType,
    data: Vec<u8>,
    /// The delta a `CreditUpdate` carried, so a test can assert the *value* a
    /// coalescing merge produced rather than merely that one arrived.
    credit: u32,
}

#[derive(Debug, Clone)]
struct OwnedBatch {
    header: BatchHeader,
    encoded_len: usize,
    records: Vec<OwnedRecord>,
}

impl OwnedBatch {
    fn decode(payload: &Bytes) -> Self {
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

    fn slots(&self) -> std::collections::BTreeSet<u32> {
        self.records.iter().map(|r| r.slot.index()).collect()
    }
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

struct Harness {
    handle: Arc<BatcherHandle>,
    batches: flume::Receiver<Bytes>,
    registry: prometheus::Registry,
    cancel: CancellationToken,
    // Held so the messengers outlive the batcher.
    _sender: Arc<Messenger>,
    _capture: Arc<Messenger>,
}

fn tcp_transport() -> Arc<crate::transports::tcp::TcpTransport> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    Arc::new(
        TcpTransportBuilder::new()
            .from_listener(listener)
            .expect("from_listener")
            .build()
            .expect("build transport"),
    )
}

async fn harness(config: MuxConfig) -> Harness {
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

    let registry = prometheus::Registry::new();
    let metrics = Arc::new(VeloMetrics::register(&registry).expect("register metrics"));
    let cancel = CancellationToken::new();
    let peer = capture.instance_id().worker_id();
    let handle = spawn(
        peer,
        BatcherContext {
            messenger: Arc::clone(&sender),
            config,
            metrics: Some(metrics.bind_mux()),
            epochs: Arc::new(AtomicU64::new(1)),
            batchers: Arc::new(DashMap::new()),
            cancel: cancel.clone(),
        },
    );

    Harness {
        handle,
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
    async fn open(&self, anchor_id: u64, session_id: u64) -> (flume::Sender<Vec<u8>>, SlotId) {
        let (inlet, (slot, _)) = self.open_with_header(anchor_id, session_id).await;
        (inlet, slot)
    }

    /// As [`Self::open`], but also yields the header of the eager `OpenSlot`
    /// batch, which is where the epoch and batch sequence are observable.
    async fn open_with_header(
        &self,
        anchor_id: u64,
        session_id: u64,
    ) -> (flume::Sender<Vec<u8>>, (SlotId, BatchHeader)) {
        // Deep enough that a test can queue more than one batch's worth on a
        // parked slot before granting credit.
        self.open_with_inlet(anchor_id, session_id, 512).await
    }

    /// As [`Self::open_with_header`], with a caller-chosen inlet depth — the
    /// knob that decides how soon a producer meets a full channel.
    async fn open_with_inlet(
        &self,
        anchor_id: u64,
        session_id: u64,
        depth: usize,
    ) -> (flume::Sender<Vec<u8>>, (SlotId, BatchHeader)) {
        let (inlet_tx, inlet_rx) = flume::bounded::<Vec<u8>>(depth);
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        self.handle
            .open_slot(OpenSlotRequest {
                anchor_id,
                session_id,
                inlet: inlet_rx,
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

    async fn next_batch(&self) -> OwnedBatch {
        let payload = tokio::time::timeout(RECV_TIMEOUT, self.batches.recv_async())
            .await
            .expect("timed out waiting for a batch")
            .expect("capture channel closed");
        OwnedBatch::decode(&payload)
    }

    fn try_next_batch(&self) -> Option<OwnedBatch> {
        self.batches.try_recv().ok().map(|p| OwnedBatch::decode(&p))
    }

    fn grant(&self, slot: SlotId, delta: u32) {
        self.handle.grant(slot, delta);
    }

    fn snapshot(&self) -> crate::observability::test_helpers::MetricSnapshot {
        crate::observability::test_helpers::MetricSnapshot::from_registry(&self.registry)
    }

    /// Records the batcher has pulled from inlets and parked.
    fn withheld(&self) -> f64 {
        self.snapshot()
            .gauge("velo_streaming_mux_withheld_records", &[])
    }

    /// Wait until exactly `count` records are parked.
    ///
    /// A positive fact to wait for, unlike "no batch has arrived yet" — which
    /// is true before the batcher has run at all and so proves nothing.
    async fn await_withheld(&self, count: usize) {
        eventually(|| (self.withheld() - count as f64).abs() < f64::EPSILON).await;
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

fn item(n: u32) -> Vec<u8> {
    rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::Item(n)).expect("encode item")
}

/// Wait until `predicate` holds, polling the batcher's observable state.
async fn eventually(mut predicate: impl FnMut() -> bool) {
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
// Packing
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn one_batch_carries_records_from_several_slots() {
    let harness = harness(MuxConfig::default()).await;

    let mut slots = Vec::new();
    for session in 0..3u64 {
        slots.push(harness.open(1, session).await);
    }

    // Queue on parked slots: with no credit the batcher pulls one record per
    // slot, withholds it and stops draining. Everything else sits in the inlet.
    for (inlet, _) in &slots {
        for n in 0..4u32 {
            inlet.send(item(n)).expect("queue record");
        }
    }
    eventually(|| harness.try_next_batch().is_none()).await;

    // One grant per slot, all queued before the batcher can flush the first:
    // the opportunistic drain takes all three plus every queued record.
    for (_, id) in &slots {
        harness.grant(*id, 8);
    }

    let mut seen = std::collections::BTreeMap::<u32, usize>::new();
    let mut multi_slot_batches = 0;
    while seen.values().sum::<usize>() < 12 {
        let batch = harness.next_batch().await;
        if batch.slots().len() > 1 {
            multi_slot_batches += 1;
        }
        for record in &batch.records {
            if record.kind == RecordType::Data {
                *seen.entry(record.slot.index()).or_default() += 1;
            }
        }
    }

    assert_eq!(seen.len(), 3, "every slot must have been drained");
    assert!(
        seen.values().all(|count| *count == 4),
        "records lost or duplicated: {seen:?}"
    );
    assert!(
        multi_slot_batches > 0,
        "the point of bucketing by destination is that one batch carries several streams"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_configured_cap_bounds_every_batch() {
    // Small enough that a handful of records fills it, large enough that a
    // single record still fits eagerly.
    let cap = 128;
    let harness = harness(MuxConfig {
        max_batch_bytes: cap,
        ..MuxConfig::default()
    })
    .await;

    let (inlet, id) = harness.open(1, 1).await;
    for n in 0..12u32 {
        inlet.send(item(n)).expect("queue record");
    }
    eventually(|| harness.try_next_batch().is_none()).await;
    harness.grant(id, 32);

    let mut delivered = 0;
    while delivered < 12 {
        let batch = harness.next_batch().await;
        assert!(
            batch.encoded_len <= cap,
            "batch of {} bytes exceeds the {cap}-byte cap",
            batch.encoded_len
        );
        delivered += batch.records.len();
    }
    assert_eq!(delivered, 12);
}

/// The coalescing threshold is the clamp that binds when nothing else does.
///
/// It is the packing *target*, not merely a ceiling: the shared coalescing
/// writer stages a frame into one buffered `write_all` only while
/// `header + payload` fits under it, so a batch above the threshold gives back
/// exactly what batching bought. With a configured cap far above it and a
/// transport reporting megabytes of eager budget, this is the arm that has to
/// hold — and the default configuration sits just under the threshold, which
/// would hide a regression here forever.
#[tokio::test(flavor = "multi_thread")]
async fn the_coalescing_threshold_bounds_a_batch_when_the_configured_cap_does_not() {
    let harness = harness(MuxConfig {
        max_batch_bytes: 1 << 20,
        ..MuxConfig::default()
    })
    .await;

    let (inlet, id) = harness.open(1, 1).await;
    let payload = rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::Item(vec![7u8; 1000]))
        .expect("encode payload");
    const RECORDS: usize = 100;
    for _ in 0..RECORDS {
        inlet.send(payload.clone()).expect("queue record");
    }
    eventually(|| harness.try_next_batch().is_none()).await;
    harness.grant(id, 256);

    let mut delivered = 0;
    let mut batches = 0;
    let mut largest = 0;
    while delivered < RECORDS {
        let batch = harness.next_batch().await;
        assert!(
            batch.encoded_len <= COALESCE_THRESHOLD,
            "batch of {} bytes is over the {COALESCE_THRESHOLD}-byte coalescing threshold",
            batch.encoded_len
        );
        largest = largest.max(batch.encoded_len);
        batches += 1;
        delivered += batch.records.len();
    }
    assert_eq!(delivered, RECORDS);
    assert!(
        batches > 1,
        "100 KiB of records has to be cut into more than one batch"
    );
    assert!(
        largest > COALESCE_THRESHOLD / 2,
        "the threshold, not some smaller clamp, is what bound these batches: \
         largest was {largest} bytes"
    );
}

// ---------------------------------------------------------------------------
// The inlet is drained unconditionally
// ---------------------------------------------------------------------------

/// A starved slot must not be able to block a producer's *synchronous* send.
///
/// `finalize`, `detach` and `Drop` reach the inlet through `flume::Sender::send`,
/// which blocks on a full channel — and `Drop` does it from inside async
/// context, on a runtime worker thread. Under TCP a full channel drains at
/// socket speed, so the block is transient. Under mux a slot with no credit
/// would never drain at all, so it would be permanent. The withheld queue is
/// what keeps the channel moving.
#[tokio::test(flavor = "multi_thread")]
async fn a_starved_slot_keeps_draining_so_a_synchronous_terminal_never_blocks() {
    let harness = harness(MuxConfig::default()).await;
    // A shallow inlet, so a producer meets the channel's limit almost at once.
    let (inlet, (id, _)) = harness.open_with_inlet(1, 1, 4).await;

    // No credit is ever granted, so nothing this slot holds may be sent.
    const RECORDS: u32 = 32;
    for n in 0..RECORDS {
        tokio::time::timeout(RECV_TIMEOUT, inlet.send_async(item(n)))
            .await
            .expect("a starved slot must not stall its producer")
            .expect("inlet open");
    }

    // The blocking call the hazard is about. On a blocking pool so that a
    // regression stalls this test rather than the whole runtime.
    let terminal_inlet = inlet.clone();
    let sent = tokio::time::timeout(
        RECV_TIMEOUT,
        tokio::task::spawn_blocking(move || terminal_inlet.send(cached_finalized().clone())),
    )
    .await
    .expect("a synchronous terminal send must not block on a starved slot")
    .expect("blocking task");
    assert!(sent.is_ok());

    assert!(
        harness.try_next_batch().is_none(),
        "nothing may reach the wire without credit"
    );

    // Everything the producer handed over is still there, in order, and comes
    // out the moment credit does.
    harness.grant(id, 64);
    let mut records = Vec::new();
    while records.len() < RECORDS as usize + 2 {
        records.extend(harness.next_batch().await.records);
    }
    for (n, record) in records.iter().take(RECORDS as usize).enumerate() {
        assert_eq!(record.data, item(n as u32), "record {n} out of order");
    }
    assert_eq!(records[RECORDS as usize].data, *cached_finalized());
    assert_eq!(records[RECORDS as usize + 1].kind, RecordType::CloseSlot);
}

/// Run-ahead past the byte cap kills that slot, and only that slot.
///
/// This is the per-slot slow-consumer kill `BATCHING.md` prefers to the
/// heartbeat watchdog: deterministic, scoped, and metered.
#[tokio::test(flavor = "multi_thread")]
async fn withheld_overflow_closes_the_starved_slot_and_leaves_the_others_alone() {
    let harness = harness(MuxConfig {
        slot_byte_budget: 256,
        ..MuxConfig::default()
    })
    .await;

    let (starved_inlet, (starved, _)) = harness.open_with_inlet(1, 1, 256).await;
    let (flowing_inlet, (flowing, _)) = harness.open_with_inlet(1, 2, 256).await;
    harness.grant(flowing, 64);

    // Never granted, so everything piles into the withheld queue until the cap.
    for n in 0..64u32 {
        let _ = starved_inlet.send(item(n));
    }
    eventually(|| starved_inlet.is_disconnected()).await;

    let mut closed = None;
    let mut flowing_seen = 0;
    for n in 0..4u32 {
        flowing_inlet.send(item(100 + n)).expect("flowing send");
    }
    while closed.is_none() || flowing_seen < 4 {
        let batch = harness.next_batch().await;
        for record in batch.records {
            if record.slot == starved && record.kind == RecordType::CloseSlot {
                closed = Some(record);
            } else if record.slot == flowing {
                flowing_seen += 1;
            }
        }
    }
    assert!(
        closed.is_some(),
        "the consumer has to be told, or it waits out its heartbeat watchdog"
    );
    assert!(
        !flowing_inlet.is_disconnected(),
        "the peer's other slots are untouched"
    );
    assert!(
        harness.snapshot().counter(
            "velo_streaming_mux_records_dropped_total",
            &[("reason", "withheld_overflow")]
        ) > 0.0
    );
}

/// A producer that goes while records are still withheld owes them first.
#[tokio::test(flavor = "multi_thread")]
async fn a_departed_producer_s_withheld_records_go_before_its_close() {
    let harness = harness(MuxConfig::default()).await;
    let (inlet, (id, _)) = harness.open_with_inlet(1, 1, 64).await;

    for n in 0..4u32 {
        inlet.send(item(n)).expect("queue record");
    }
    drop(inlet);
    // The records are parked, not merely un-sent: the inlet drained even though
    // the slot has no credit, which is the property the close has to wait on.
    harness.await_withheld(4).await;
    assert!(harness.try_next_batch().is_none());

    harness.grant(id, 64);
    let mut records = Vec::new();
    while records.len() < 5 {
        records.extend(harness.next_batch().await.records);
    }
    for (n, record) in records.iter().take(4).enumerate() {
        assert_eq!(record.data, item(n as u32), "record {n} out of order");
    }
    assert_eq!(
        records[4].kind,
        RecordType::CloseSlot,
        "the close a departed producer owes waits behind what it enqueued"
    );
}

/// A terminal already in the queue closes the slot; no `PeerGone` follows it.
#[tokio::test(flavor = "multi_thread")]
async fn a_withheld_terminal_closes_the_slot_without_a_second_close() {
    let harness = harness(MuxConfig::default()).await;
    let (inlet, (id, _)) = harness.open_with_inlet(1, 1, 64).await;

    inlet.send(item(1)).expect("queue item");
    inlet
        .send(cached_finalized().clone())
        .expect("queue terminal");
    drop(inlet);
    // Both parked: the terminal is behind a predecessor, so its reserve does
    // not apply and it waits its turn.
    harness.await_withheld(2).await;
    assert!(harness.try_next_batch().is_none());

    harness.grant(id, 64);
    let mut records = Vec::new();
    while records.len() < 3 {
        records.extend(harness.next_batch().await.records);
    }
    assert_eq!(records[0].data, item(1));
    assert_eq!(records[1].data, *cached_finalized());
    assert_eq!(records[2].kind, RecordType::CloseSlot);
    assert_eq!(
        records.len(),
        3,
        "the terminal already closed the slot, so the departed inlet adds nothing"
    );
    // Nothing further: a second close would tell the consumer to inject
    // `Dropped` behind a `Finalized` it has already seen.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(harness.try_next_batch().is_none());
}

/// A terminal at the head of the queue spends the reserve without a grant.
///
/// The reserve exists for exactly this: a consumer that has stopped draining
/// will never return credit, so a terminal that waited for one would wait
/// forever and the stream would end on the heartbeat watchdog instead of on the
/// `Finalized` its producer actually sent.
#[tokio::test(flavor = "multi_thread")]
async fn a_terminal_spends_the_reserve_when_data_credit_is_gone() {
    let harness = harness(MuxConfig::default()).await;
    let (inlet, id) = harness.open(1, 1).await;

    // Exactly one data credit, spent by the first record.
    harness.grant(id, 1);
    inlet.send(item(0)).expect("queue item");
    let first = harness.next_batch().await;
    assert_eq!(first.records[0].data, item(0));

    // No predecessors queued, no further grant coming: the terminal must go out
    // on the reserve alone.
    inlet
        .send(cached_finalized().clone())
        .expect("queue terminal");
    let mut records = Vec::new();
    while records.len() < 2 {
        records.extend(harness.next_batch().await.records);
    }
    assert_eq!(records[0].data, *cached_finalized());
    assert_eq!(
        records[1].kind,
        RecordType::CloseSlot,
        "the terminal's close still rides the same batch"
    );
    assert_eq!(harness.withheld(), 0.0, "the terminal was sent, not parked");
}

/// A grant that covers only part of the queue drains only that part, in order.
///
/// The path this pins is `release_withheld`'s peek-then-pop: a record inspected
/// and found unaffordable must stay where it is. Popping it and putting it back
/// would send it to the *back* of the queue, which is a reordering the wire has
/// no way to detect.
#[tokio::test(flavor = "multi_thread")]
async fn a_partial_grant_drains_the_queue_head_and_leaves_the_rest_in_order() {
    let harness = harness(MuxConfig::default()).await;
    let (inlet, id) = harness.open_with_inlet(1, 1, 64).await;
    let id = id.0;

    const RECORDS: u32 = 8;
    for n in 0..RECORDS {
        inlet.send(item(n)).expect("queue record");
    }
    harness.await_withheld(RECORDS as usize).await;

    // Three credits against eight records: the fourth peek misses and the
    // remaining five have to stay put.
    harness.grant(id, 3);
    let mut records = Vec::new();
    while records.len() < 3 {
        records.extend(harness.next_batch().await.records);
    }
    harness.await_withheld((RECORDS - 3) as usize).await;
    assert!(
        harness.try_next_batch().is_none(),
        "a partial grant must not drain past what it paid for"
    );

    harness.grant(id, 16);
    while records.len() < RECORDS as usize {
        records.extend(harness.next_batch().await.records);
    }
    for (n, record) in records.iter().enumerate() {
        assert_eq!(
            record.data,
            item(n as u32),
            "record {n} out of order across the credit miss"
        );
    }
}

// ---------------------------------------------------------------------------
// Credit
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_starved_slot_is_withheld_while_the_others_keep_flowing() {
    let harness = harness(MuxConfig::default()).await;

    let (starved_inlet, starved) = harness.open(1, 1).await;
    let (flowing_inlet, flowing) = harness.open(1, 2).await;

    for n in 0..4u32 {
        starved_inlet.send(item(n)).expect("queue starved");
        flowing_inlet.send(item(100 + n)).expect("queue flowing");
    }

    // Only the second slot gets credit. The starved slot's records are pulled
    // all the same — they wait in its withheld queue, not in its inlet.
    harness.grant(flowing, 8);

    let mut flowing_records = 0;
    while flowing_records < 4 {
        let batch = harness.next_batch().await;
        for record in &batch.records {
            assert_eq!(
                record.slot, flowing,
                "a slot with no credit must not put anything on the wire"
            );
            flowing_records += 1;
        }
    }

    assert!(
        harness.try_next_batch().is_none(),
        "the starved slot has nothing admissible"
    );
    assert!(
        harness
            .snapshot()
            .counter("velo_streaming_slot_credit_exhausted_total", &[])
            > 0.0,
        "starvation is a per-slot event worth an operator's attention"
    );

    // The grant is the un-park: the withheld record and everything behind it
    // flow without the producer having done anything.
    harness.grant(starved, 8);
    let mut starved_records = 0;
    while starved_records < 4 {
        let batch = harness.next_batch().await;
        starved_records += batch
            .records
            .iter()
            .filter(|record| record.slot == starved)
            .count();
    }
}

// ---------------------------------------------------------------------------
// Terminals
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_terminal_and_its_close_ride_the_same_batch() {
    let harness = harness(MuxConfig::default()).await;
    let (inlet, id) = harness.open(1, 1).await;

    inlet.send(item(1)).expect("queue item");
    inlet
        .send(cached_finalized().clone())
        .expect("queue terminal");
    // Frames queued behind a terminal are discarded — today's semantics,
    // correctly scoped to the one slot.
    inlet.send(item(2)).expect("queue after terminal");
    harness.grant(id, 8);

    let mut records = Vec::new();
    while !records
        .iter()
        .any(|r: &OwnedRecord| r.kind == RecordType::CloseSlot)
    {
        records.extend(harness.next_batch().await.records);
    }

    let close_at = records
        .iter()
        .position(|r| r.kind == RecordType::CloseSlot)
        .expect("close present");
    assert_eq!(
        records[close_at - 1].data,
        *cached_finalized(),
        "the close must sit immediately behind its terminal"
    );
    assert_eq!(records[close_at].slot, id);
    assert!(
        !records[close_at + 1..]
            .iter()
            .any(|r| r.slot == id && r.kind == RecordType::Data),
        "nothing queued behind the terminal may reach the wire"
    );

    eventually(|| inlet.is_disconnected()).await;
    assert_eq!(
        harness
            .snapshot()
            .gauge("velo_streaming_mux_live_slots", &[]),
        0.0,
        "a terminal frees its slot"
    );
}

// ---------------------------------------------------------------------------
// Oversized records
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn an_oversized_record_goes_alone_and_fences_only_its_slot() {
    let cap = 256;
    let harness = harness(MuxConfig {
        max_batch_bytes: cap,
        ..MuxConfig::default()
    })
    .await;

    let (big_inlet, big) = harness.open(1, 1).await;
    let (small_inlet, small) = harness.open(1, 2).await;
    harness.grant(big, 8);
    harness.grant(small, 8);

    let oversized = rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::Item(vec![
        7u8;
        cap * 2
    ]))
    .expect("encode oversized");
    big_inlet.send(oversized.clone()).expect("queue oversized");
    for n in 0..4u32 {
        small_inlet.send(item(n)).expect("queue small");
    }

    let mut small_seen = 0;
    let mut singleton_seen = false;
    while small_seen < 4 || !singleton_seen {
        let batch = harness.next_batch().await;
        if batch.records.iter().any(|r| r.slot == big) {
            assert_eq!(
                batch.records.len(),
                1,
                "an over-budget record travels alone so the rendezvous round trip \
                 is not charged to unrelated slots"
            );
            assert_eq!(batch.records[0].data, oversized);
            singleton_seen = true;
        }
        small_seen += batch.records.iter().filter(|r| r.slot == small).count();
    }

    assert_eq!(
        harness
            .snapshot()
            .counter("velo_streaming_mux_rendezvous_singletons_total", &[]),
        1.0
    );

    // The fence lifts once the singleton's admission resolves, and the slot's
    // later records follow it in order.
    big_inlet.send(item(9)).expect("queue successor");
    let mut successor = None;
    while successor.is_none() {
        let batch = harness.next_batch().await;
        successor = batch.records.into_iter().find(|r| r.slot == big);
    }
    let successor = successor.expect("successor delivered");
    assert_eq!(successor.data, item(9));
    assert_eq!(
        successor.frame_seq, 2,
        "frame_seq carries the order proof past a resolve that is not lane-ordered"
    );
}

// ---------------------------------------------------------------------------
// Control is state, not a queue
// ---------------------------------------------------------------------------

/// A batcher stalled on admission must not grow with what arrives behind it.
///
/// The stall is not exotic: a flush parks whenever the peer is congested, and a
/// congested peer is exactly when its ingress lane is busiest returning credit.
/// An unbounded control queue in that window is unbounded memory. Coalesced
/// state is O(live slots) whatever the arrival rate, and the deltas it merged
/// still deliver once the peer un-parks.
#[tokio::test(flavor = "multi_thread")]
async fn a_stalled_batcher_coalesces_control_instead_of_queueing_it() {
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
        .register_peer(velo_ext::PeerInfo::new(
            peer_instance,
            velo_ext::WorkerAddress::from_encoded(
                rmp_serde::to_vec(&std::collections::HashMap::from([(
                    "stalling".to_string(),
                    b"stalling".to_vec(),
                )]))
                .expect("encode"),
            ),
        ))
        .expect("register peer");

    let registry = prometheus::Registry::new();
    let metrics = Arc::new(VeloMetrics::register(&registry).expect("register metrics"));
    let cancel = CancellationToken::new();
    let handle = spawn(
        peer_instance.worker_id(),
        BatcherContext {
            messenger: Arc::clone(&sender),
            config: MuxConfig::default(),
            metrics: Some(metrics.bind_mux()),
            epochs: Arc::new(AtomicU64::new(1)),
            batchers: Arc::new(DashMap::new()),
            cancel: cancel.clone(),
        },
    );

    // Open a slot. Its eager `OpenSlot` flush takes the gate's one free place.
    //
    // The inlet is deep because a batcher parked on admission is not draining
    // it — admission parking suspends the whole task, including the inlet
    // drain that credit starvation cannot suspend. That park is bounded by the
    // transport's own progress, which is the situation a socket was always in;
    // credit starvation is the unbounded one, and that is the one the withheld
    // queue exists for.
    let (inlet, inlet_rx) = flume::bounded::<Vec<u8>>(512);
    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
    handle
        .open_slot(OpenSlotRequest {
            anchor_id: 1,
            session_id: 1,
            inlet: inlet_rx,
            ack: ack_tx,
        })
        .await
        .expect("queue OpenSlot");
    let opened = tokio::time::timeout(RECV_TIMEOUT, ack_rx)
        .await
        .expect("ack")
        .expect("ack delivered");
    assert!(opened.is_ok());

    let open_batch = OwnedBatch::decode(&{
        let (_, payload) = tokio::time::timeout(RECV_TIMEOUT, wire.recv_async())
            .await
            .expect("the OpenSlot flush must reach the wire")
            .expect("wire open");
        payload
    });
    let id = open_batch.records[0].slot;

    // Fill the gate's one place and leave it filled. Nothing drains `wire`
    // until the release phase, so every flush after this one parks.
    handle.grant(id, 1);
    inlet.send(item(0)).expect("queue record");
    eventually(|| wire.is_full()).await;

    // A control write the batcher can act on, so the flush it triggers is the
    // one that parks. After this the batcher is inside `flush().await` and
    // cannot take anything else off the control state.
    handle.reply(&[ReplyRecord::CloseSlot {
        slot: SlotId::from_raw(u32::MAX),
        reason: CloseReason::UnknownSlot,
    }]);
    let batches = |registry: &prometheus::Registry| {
        crate::observability::test_helpers::MetricSnapshot::from_registry(registry)
            .counter("velo_streaming_mux_batches_total", &[("direction", "sent")])
    };
    eventually(|| batches(&registry) >= 3.0).await;
    let parked_at = batches(&registry);

    // More records than the one credit already granted, so the merged grant is
    // what decides whether they flow.
    const QUEUED: u32 = 100;
    for n in 1..QUEUED {
        inlet.send(item(n)).expect("queue record");
    }

    // Ten thousand grants and ten thousand replies while it is stuck there.
    const MERGED: u32 = 10_000;
    let mut peak_pending = 0;
    for _ in 0..MERGED {
        handle.grant(id, 1);
        handle.reply(&[ReplyRecord::CreditUpdate { slot: id, delta: 1 }]);
        peak_pending = peak_pending.max(handle.pending_control());
    }
    // The stall was real: a batcher keeping up would have packed some of those
    // twenty thousand writes into batches by now.
    assert_eq!(
        batches(&registry),
        parked_at,
        "nothing may reach the messenger while the peer's gate is full"
    );
    assert!(
        peak_pending <= 3,
        "control must coalesce per slot, not queue: peaked at {peak_pending} entries \
         against 20 000 writes"
    );

    // Un-park the peer, one place at a time — the gate holds exactly one, so
    // the batcher flushes, parks again, and the test keeps freeing it. The
    // merged credit is applied as one grant and the merged reply goes out as
    // one record; what is asserted is that both arrive and that nothing is
    // left holding state behind them.
    let mut records: Vec<OwnedRecord> = Vec::new();
    let deadline = tokio::time::Instant::now() + RECV_TIMEOUT;
    let settled = loop {
        if tokio::time::Instant::now() >= deadline {
            break false;
        }
        match wire.try_recv() {
            Ok((_, payload)) => records.extend(OwnedBatch::decode(&payload).records),
            Err(_) => tokio::time::sleep(Duration::from_millis(2)).await,
        }
        if handle.pending_control() == 0
            && records
                .iter()
                .filter(|r| r.kind == RecordType::Data)
                .count()
                == QUEUED as usize
            && records.iter().any(|r| r.kind == RecordType::CreditUpdate)
        {
            break true;
        }
    };
    assert!(
        settled,
        "the coalesced control must deliver once the peer un-parks: \
         {} entries still pending, {} of {QUEUED} records out",
        handle.pending_control(),
        records
            .iter()
            .filter(|r| r.kind == RecordType::Data)
            .count()
    );

    // The merged *values*, not merely their arrival.
    //
    // Asserted as a sum rather than as a single record of 10 000, because the
    // batcher is free to drain more than once — each freed place in the gate
    // lets it flush and look again. What must hold either way is conservation:
    // every delta written is delivered exactly once, in far fewer records than
    // it was written in. A merge that dropped or double-counted would move the
    // sum; a merge that did not happen would move the count.
    let credit: Vec<u32> = records
        .iter()
        .filter(|r| r.kind == RecordType::CreditUpdate)
        .map(|r| r.credit)
        .collect();
    assert_eq!(
        credit.iter().sum::<u32>(),
        MERGED,
        "coalescing must neither drop nor duplicate a delta"
    );
    assert!(
        credit.len() < MERGED as usize / 4,
        "ten thousand replies arrived as {} records — that is not coalescing",
        credit.len()
    );
    assert!(
        records
            .iter()
            .any(|r| r.kind == RecordType::CloseSlot && r.slot.raw() == u32::MAX),
        "the control written while parked has to survive the park"
    );
    for (n, record) in records
        .iter()
        .filter(|r| r.kind == RecordType::Data)
        .enumerate()
    {
        assert_eq!(record.data, item(n as u32), "record {n} out of order");
    }
    cancel.cancel();
}

// ---------------------------------------------------------------------------
// Epoch death
// ---------------------------------------------------------------------------

/// A singleton's failure resolving after its slot closed must not fail the
/// epoch.
///
/// The resolution carries the `SlotId` it was sent under, and a close-then-open
/// recycles that dense index under a new generation while the answer is still in
/// flight. Acting on it would take down every live slot on the peer over a
/// stream that ended cleanly before the answer arrived — and there is no gap to
/// protect, because the slot is gone. A connection-level failure is not lost
/// either: the next batch to this peer meets the same failure and fails the
/// epoch then.
#[tokio::test(flavor = "multi_thread")]
async fn a_singleton_failing_after_its_slot_closed_does_not_fail_the_epoch() {
    let harness = harness(MuxConfig::default()).await;

    let (inlet, stale) = harness.open(1, 1).await;
    harness.grant(stale, 8);
    inlet
        .send(cached_finalized().clone())
        .expect("queue terminal");
    eventually(|| inlet.is_disconnected()).await;
    // Drain the terminal's batch so the next assertions read a quiet wire.
    while harness.try_next_batch().is_some() {}

    // The index comes back under a new generation.
    let (reopened_inlet, reopened) = harness.open(1, 2).await;
    assert_eq!(reopened.index(), stale.index());
    assert_ne!(reopened.generation(), stale.generation());

    // The old slot's singleton finally answers, and answers badly.
    harness.handle.control.singleton_resolved(stale, false);
    eventually(|| {
        harness.snapshot().counter(
            "velo_streaming_mux_records_dropped_total",
            &[("reason", "stale_singleton")],
        ) > 0.0
    })
    .await;

    assert!(
        !reopened_inlet.is_disconnected(),
        "the slot that reused the index must survive its predecessor's answer"
    );
    assert_eq!(
        harness
            .snapshot()
            .counter("velo_streaming_mux_epoch_deaths_total", &[]),
        0.0,
        "a stale answer is not evidence about the connection this epoch has"
    );

    // And the reopened slot still works.
    harness.grant(reopened, 8);
    reopened_inlet.send(item(7)).expect("send on reopened slot");
    let mut seen = None;
    while seen.is_none() {
        seen = harness
            .next_batch()
            .await
            .records
            .into_iter()
            .find(|r| r.kind == RecordType::Data);
    }
    assert_eq!(seen.expect("record").data, item(7));
}

#[tokio::test(flavor = "multi_thread")]
async fn epoch_death_fails_every_live_slot_exactly_once() {
    let harness = harness(MuxConfig::default()).await;

    let mut inlets = Vec::new();
    let mut ids = Vec::new();
    for session in 0..3u64 {
        let (inlet, id) = harness.open(1, session).await;
        harness.grant(id, 8);
        inlets.push(inlet);
        ids.push(id);
    }
    assert_eq!(
        harness
            .snapshot()
            .gauge("velo_streaming_mux_live_slots", &[]),
        3.0
    );

    // A singleton whose admission never resolved. The batch it carried is gone,
    // so every slot packed into that epoch has a frame_seq gap that can never
    // close — which is why any failed admission fails the whole epoch.
    harness.handle.control.singleton_resolved(ids[0], false);

    for inlet in &inlets {
        eventually(|| inlet.is_disconnected()).await;
    }
    let snapshot = harness.snapshot();
    assert_eq!(
        snapshot.counter("velo_streaming_mux_epoch_deaths_total", &[]),
        1.0,
        "one death, not one per slot"
    );
    assert_eq!(
        snapshot.gauge("velo_streaming_mux_live_slots", &[]),
        0.0,
        "slots do not survive an epoch"
    );

    // The batcher stays usable: a reconnect is a fresh epoch, not a fresh task.
    let (_inlet, id) = harness.open(1, 99).await;
    assert!(
        ids.iter().any(|prior| prior.index() == id.index()),
        "the freed dense indices are reused, which is what the generation tag exists for"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_epoch_restarts_batch_sequences_and_bumps_generations() {
    let harness = harness(MuxConfig::default()).await;
    let (_inlet, first) = harness.open_with_header(1, 1).await;
    let (first_slot, first_header) = first;

    harness.handle.control.singleton_resolved(first_slot, false);
    eventually(|| harness.handle.live_slots.load(Ordering::Relaxed) == 0).await;

    let (_inlet, (second_slot, second_header)) = harness.open_with_header(1, 2).await;

    assert_eq!(second_slot.index(), first_slot.index());
    assert_eq!(
        second_slot.generation(),
        first_slot.generation().wrapping_add(1),
        "reuse of a dense index has to be distinguishable from the original"
    );
    assert!(
        second_header.peer_epoch > first_header.peer_epoch,
        "a reconnect is a new epoch, so the peer can discard the old one's \
         batches by header inspection"
    );
    assert_eq!(
        second_header.batch_seq, 0,
        "batch sequences are scoped by the epoch above them"
    );
}

// ---------------------------------------------------------------------------
// Teardown
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn cancelling_the_transport_closes_every_producer_channel() {
    let harness = harness(MuxConfig::default()).await;
    let (inlet_a, _) = harness.open(1, 1).await;
    let (inlet_b, _) = harness.open(1, 2).await;

    harness.cancel.cancel();

    eventually(|| inlet_a.is_disconnected() && inlet_b.is_disconnected()).await;
    assert_eq!(
        harness
            .snapshot()
            .gauge("velo_streaming_mux_live_slots", &[]),
        0.0
    );
}
