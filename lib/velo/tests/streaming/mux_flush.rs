// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! `Velo::flush_batch()` over real nodes — the public contract, end to end.
//!
//! The unit tests beside the batcher drive `BatcherHandle::kick_flush()`, and
//! the transport-level one drives `MessengerMuxTransport::flush_batches()`.
//! Neither touches the delegation an application actually calls through:
//!
//! ```text
//! Velo::flush_batch  ->  AnchorManager::flush_mux_batches  ->  MessengerMuxTransport::flush_batches
//! ```
//!
//! Break any link in that chain — an early return, a mux handle never
//! installed, a method wired to the wrong thing — and every one of those tests
//! still passes while no application can flush anything. This file is the one
//! that would fail, so it starts from `Velo` and asserts on delivered frames.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use prometheus::Registry;
use velo::observability::VeloMetrics;
use velo::observability::test_helpers::MetricSnapshot;
use velo::streaming::{
    AutoFlush, FlushPolicy, MuxConfig, StreamAnchor, StreamAnchorHandle, StreamFrame,
};
use velo::transports::tcp::TcpTransportBuilder;
use velo::{Velo, VeloBuilder};

const PATIENCE: Duration = Duration::from_secs(30);

/// Long enough that nothing in this file can reach it, so the only thing that
/// can produce a batch under `Auto` is the flush being tested.
const UNREACHABLE_WINDOW: Duration = Duration::from_secs(600);

/// Long enough that a policy holding records really is holding them, rather
/// than merely being slow.
const SETTLE: Duration = Duration::from_millis(300);

fn tcp_transport() -> Arc<velo::transports::tcp::TcpTransport> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    Arc::new(
        TcpTransportBuilder::new()
            .from_listener(listener)
            .expect("from_listener")
            .build()
            .expect("build transport"),
    )
}

struct Node {
    velo: Arc<Velo>,
    registry: Registry,
}

impl Node {
    /// Batches this node's egress batchers handed to the messenger.
    ///
    /// Per-node registry, so this counts *this* node's writes. That is what
    /// makes "one batch per peer" assertable: the producer's count moves by the
    /// number of peers it flushed, and by nothing else.
    fn batches_sent(&self) -> f64 {
        MetricSnapshot::from_registry(&self.registry)
            .counter("velo_streaming_mux_batches_total", &[("direction", "sent")])
    }
}

async fn node(mux: Option<MuxConfig>) -> Node {
    let registry = Registry::new();
    let metrics = Arc::new(VeloMetrics::register(&registry).expect("register metrics"));
    let mut builder: VeloBuilder = Velo::builder()
        .add_transport(tcp_transport())
        .stream_bind_addr(std::net::Ipv4Addr::LOCALHOST.into())
        .metrics(metrics);
    if let Some(config) = mux {
        builder = builder.messenger_mux(config).expect("install mux");
    }
    Node {
        velo: builder.build().await.expect("build velo"),
        registry,
    }
}

fn mux(policy: FlushPolicy) -> MuxConfig {
    MuxConfig {
        enabled: true,
        flush_policy: policy,
        ..MuxConfig::default()
    }
}

/// Cross-register and wait until each side can see the other's attach handler.
async fn introduce(a: &Node, b: &Node) {
    a.velo
        .register_peer(b.velo.peer_info())
        .expect("register b on a");
    b.velo
        .register_peer(a.velo.peer_info())
        .expect("register a on b");
    for (from, to) in [(a, b), (b, a)] {
        tokio::time::timeout(
            PATIENCE,
            from.velo
                .wait_for_handler(to.velo.instance_id(), "_anchor_attach"),
        )
        .await
        .expect("timed out waiting for the peer's control plane")
        .expect("peer never advertised the handler");
    }
}

/// The handle as it would cross an RPC to the producer.
fn transfer(handle: StreamAnchorHandle) -> StreamAnchorHandle {
    StreamAnchorHandle::from_u128(handle.as_u128())
}

/// Take one item, or report that nothing arrived within `window`.
async fn next_item(anchor: &mut StreamAnchor<u32>, window: Duration) -> Option<u32> {
    match tokio::time::timeout(window, anchor.next()).await {
        Ok(Some(frame)) => match frame.expect("no stream error") {
            StreamFrame::Item(value) => Some(value),
            other => panic!("unexpected frame: {other:?}"),
        },
        Ok(None) => panic!("the anchor closed early"),
        Err(_) => None,
    }
}

// ---------------------------------------------------------------------------
// Manual, across two peers
// ---------------------------------------------------------------------------

/// One `Velo::flush_batch()` writes to every peer, and writes nothing before.
///
/// The shape a serving loop produces: one producer holding streams on two
/// different consumers, a round of sends across both, then one flush. What must
/// come out is exactly one batch per peer — not one per stream, not only the
/// first peer, and nothing at all until the call.
#[tokio::test(flavor = "multi_thread")]
async fn flush_batch_writes_one_batch_to_every_peer_under_manual() {
    const STREAMS_PER_PEER: u32 = 2;

    let producer = node(Some(mux(FlushPolicy::Manual))).await;
    let consumers = [
        node(Some(mux(FlushPolicy::Manual))).await,
        node(Some(mux(FlushPolicy::Manual))).await,
    ];
    for consumer in &consumers {
        introduce(&producer, consumer).await;
    }

    // Attach every stream first. Each `OpenSlot` is flushed eagerly and on its
    // own, so once the attaches have returned the counter is at a known place
    // and everything after it is the round being tested.
    let mut anchors = Vec::new();
    let mut senders = Vec::new();
    for consumer in &consumers {
        for _ in 0..STREAMS_PER_PEER {
            let anchor = consumer.velo.create_anchor::<u32>();
            let handle = transfer(anchor.handle());
            senders.push(
                producer
                    .velo
                    .attach_anchor::<u32>(handle)
                    .await
                    .expect("remote attach"),
            );
            anchors.push(anchor);
        }
    }
    let after_opens = producer.batches_sent();
    assert_eq!(
        after_opens,
        f64::from(STREAMS_PER_PEER) * consumers.len() as f64,
        "one eager batch per OpenSlot"
    );

    // One round: an item on every stream, back to back, nothing awaited between
    // them beyond the send itself.
    for (n, sender) in senders.iter().enumerate() {
        sender.send(n as u32).await.expect("send item");
    }

    // Manual holds it. Nothing on the wire and nothing at either consumer.
    tokio::time::sleep(SETTLE).await;
    assert_eq!(
        producer.batches_sent(),
        after_opens,
        "manual must hold the round until the application flushes it"
    );
    for anchor in &mut anchors {
        assert_eq!(
            next_item(anchor, Duration::from_millis(50)).await,
            None,
            "no item may arrive before the flush"
        );
    }

    // The public call, and the only thing that has changed.
    producer.velo.flush_batch();

    for (n, anchor) in anchors.iter_mut().enumerate() {
        assert_eq!(
            next_item(anchor, PATIENCE).await,
            Some(n as u32),
            "stream {n} did not receive the item its peer's flush carried"
        );
    }
    assert_eq!(
        producer.batches_sent(),
        after_opens + consumers.len() as f64,
        "one flush, one batch per peer — not one per stream, and not only the \
         first peer the producer happened to resolve"
    );
}

// ---------------------------------------------------------------------------
// Auto
// ---------------------------------------------------------------------------

/// The public call is valid under `Auto` and forces a write ahead of the
/// conditions the batcher was going to wait for.
///
/// The policy is the windowed one — no end-of-wake write, and a window no test
/// will outlive — so nothing but `flush_batch()` can produce a batch here.
#[tokio::test(flavor = "multi_thread")]
async fn flush_batch_forces_a_write_under_auto() {
    let policy = FlushPolicy::Auto(AutoFlush {
        on_admission: false,
        max_linger: Some(UNREACHABLE_WINDOW),
    });
    let producer = node(Some(mux(policy))).await;
    let consumer = node(Some(mux(policy))).await;
    introduce(&producer, &consumer).await;

    let mut anchor = consumer.velo.create_anchor::<u32>();
    let handle = transfer(anchor.handle());
    let sender = producer
        .velo
        .attach_anchor::<u32>(handle)
        .await
        .expect("remote attach");
    let after_open = producer.batches_sent();

    for n in 0..4u32 {
        sender.send(n).await.expect("send item");
    }
    tokio::time::sleep(SETTLE).await;
    assert_eq!(
        producer.batches_sent(),
        after_open,
        "neither condition has fired, so the batcher is holding the records"
    );
    assert_eq!(
        next_item(&mut anchor, Duration::from_millis(50)).await,
        None
    );

    producer.velo.flush_batch();

    for n in 0..4u32 {
        assert_eq!(
            next_item(&mut anchor, PATIENCE).await,
            Some(n),
            "item {n} did not arrive after the flush forced the write"
        );
    }
}

// ---------------------------------------------------------------------------
// No mux
// ---------------------------------------------------------------------------

/// On a node with no mux the call is a no-op, not an error and not a panic.
///
/// The legacy per-stream transports have nothing staged to write — their egress
/// pumps hand every frame straight to a socket — so a call site does not have
/// to know how the node it holds was configured. Worth pinning because the
/// delegation walks through an `Option` that is `None` here, and the streaming
/// that follows proves the call left the node working.
#[tokio::test(flavor = "multi_thread")]
async fn flush_batch_is_a_no_op_on_a_node_without_a_mux() {
    let producer = node(None).await;
    let consumer = node(None).await;
    introduce(&producer, &consumer).await;

    // Before anything exists to flush.
    producer.velo.flush_batch();

    let mut anchor = consumer.velo.create_anchor::<u32>();
    let handle = transfer(anchor.handle());
    let sender = producer
        .velo
        .attach_anchor::<u32>(handle)
        .await
        .expect("remote attach");

    for n in 0..4u32 {
        sender.send(n).await.expect("send item");
    }
    // Repeatedly, mid-stream, on the node that has no batchers at all.
    for _ in 0..10 {
        producer.velo.flush_batch();
    }

    for n in 0..4u32 {
        assert_eq!(
            next_item(&mut anchor, PATIENCE).await,
            Some(n),
            "the legacy path must be undisturbed by a flush it has no use for"
        );
    }
    assert_eq!(
        producer.batches_sent(),
        0.0,
        "and no mux batch was invented for a node that has no mux"
    );
}
