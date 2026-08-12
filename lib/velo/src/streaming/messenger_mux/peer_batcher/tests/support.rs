// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The harness the batcher tests are driven through, and the fixtures too large
//! to keep beside the tests that use them.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use tokio_util::sync::CancellationToken;

use super::super::*;
use crate::messenger::{Context, Handler};
use crate::observability::VeloMetrics;
use crate::streaming::messenger_mux::STREAM_BATCH_HANDLER;
use crate::streaming::messenger_mux::protocol::{
    BatchDecoder, BatchHeader, RecordBody, RecordType,
};
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

    /// As [`Self::open_with_header`], with a caller-chosen inlet depth — the
    /// knob that decides how soon a producer meets a full channel.
    pub(super) async fn open_with_inlet(
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

/// A transport whose per-target send channel this test owns.
///
/// One admission gate over a `bounded(1)` channel nobody drains: the first send
/// takes the fast path, every send after it parks in the gate. That is the shape
/// of a congested peer, produced deterministically instead of waited for.
pub(super) struct StallingTransport {
    key: velo_ext::TransportKey,
    address: velo_ext::WorkerAddress,
    gate: velo_ext::AdmissionGate<(Bytes, Bytes)>,
    peers: std::sync::Mutex<std::collections::HashSet<velo_ext::InstanceId>>,
}

impl StallingTransport {
    pub(super) fn new(rt: tokio::runtime::Handle) -> (Arc<Self>, flume::Receiver<(Bytes, Bytes)>) {
        let (tx, rx) = flume::bounded::<(Bytes, Bytes)>(1);
        let key = velo_ext::TransportKey::new("stalling");
        let mut entries = std::collections::HashMap::<String, Vec<u8>>::new();
        entries.insert(key.as_str().to_string(), b"stalling".to_vec());
        let address =
            velo_ext::WorkerAddress::from_encoded(rmp_serde::to_vec(&entries).expect("encode"));
        let transport = Arc::new(Self {
            key,
            address,
            gate: velo_ext::AdmissionGate::new(tx, rt),
            peers: std::sync::Mutex::new(std::collections::HashSet::new()),
        });
        (transport, rx)
    }
}

impl velo_ext::Transport for StallingTransport {
    fn key(&self) -> velo_ext::TransportKey {
        self.key.clone()
    }

    fn address(&self) -> velo_ext::WorkerAddress {
        self.address.clone()
    }

    fn register(&self, peer_info: velo_ext::PeerInfo) -> Result<(), velo_ext::TransportError> {
        self.peers
            .lock()
            .expect("peer set poisoned")
            .insert(peer_info.instance_id());
        Ok(())
    }

    fn send_message(
        &self,
        _instance_id: velo_ext::InstanceId,
        header: Bytes,
        payload: Bytes,
        _message_type: velo_ext::MessageType,
        _on_error: Arc<dyn velo_ext::TransportErrorHandler>,
    ) -> velo_ext::SendOutcome {
        self.gate.send((header, payload))
    }

    fn start(
        &self,
        _instance_id: velo_ext::InstanceId,
        _channels: velo_ext::TransportAdapter,
        _rt: tokio::runtime::Handle,
    ) -> futures::future::BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn shutdown(&self) {}

    fn check_health(
        &self,
        _instance_id: velo_ext::InstanceId,
        _timeout: Duration,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), velo_ext::HealthCheckError>> + Send + '_>,
    > {
        Box::pin(async { Ok(()) })
    }
}
