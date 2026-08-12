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

use super::*;
use crate::messenger::{Context, Handler};
use crate::observability::VeloMetrics;
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
        let (inlet_tx, inlet_rx) = flume::bounded::<Vec<u8>>(512);
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        self.handle
            .send(Command::OpenSlot {
                anchor_id,
                session_id,
                inlet: inlet_rx,
                ack: ack_tx,
            })
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
        self.handle
            .send(Command::Grant { slot, delta })
            .expect("queue grant");
    }

    fn snapshot(&self) -> crate::observability::test_helpers::MetricSnapshot {
        crate::observability::test_helpers::MetricSnapshot::from_registry(&self.registry)
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

    // Only the second slot gets credit.
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
// Epoch death
// ---------------------------------------------------------------------------

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
    harness
        .handle
        .send(Command::SingletonResolved {
            slot: ids[0],
            admitted: false,
        })
        .expect("queue resolution");

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

    harness
        .handle
        .send(Command::SingletonResolved {
            slot: first_slot,
            admitted: false,
        })
        .expect("queue resolution");
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
