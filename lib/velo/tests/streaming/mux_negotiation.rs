// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Attach-time negotiation of `messenger-mux-v1`, over two real `Velo` nodes.
//!
//! The unit tests beside `streaming/negotiation.rs` pin the decision table; this
//! file checks that two nodes actually reach those verdicts across a wire, and
//! that the streams they agree on then work. The interesting half is the mixed
//! deployment — the guarantee is that a node with the mux switched on keeps
//! serving every peer that does not have it, which is a claim about pairs and
//! not about either node alone.
//!
//! Peers that predate negotiation are simulated by sending payloads that lack
//! the new fields, so `#[serde(default)]` is doing the real work rather than a
//! constructor passing an empty vector. A node built without the mux is the
//! other half of the same story and is used where a whole stream has to flow.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use prometheus::Registry;
use serde::Serialize;
use velo::observability::VeloMetrics;
use velo::observability::test_helpers::MetricSnapshot;
use velo::streaming::control::{AnchorAttachResponse, StreamCancelHandle};
use velo::streaming::mpsc::MpscAnchorAttachResponse;
use velo::streaming::{MpscFrame, MuxConfig, StreamAnchorHandle, StreamFrame};
use velo::transports::tcp::TcpTransportBuilder;
use velo::{Velo, VeloBuilder};
use velo_ext::WorkerId;

/// The streaming transport a `Velo` node runs when nothing else is configured.
const LEGACY_KEY: &str = "tcp-stream";
const MUX_KEY: &str = "messenger-mux-v1";

const SETTLE: Duration = Duration::from_millis(200);
const PATIENCE: Duration = Duration::from_secs(30);

/// A window far smaller than the traffic every test pushes through it.
///
/// Deliberate: this is the first stage in which `reader_pump` drains a mux slot
/// buffer at all, so the credit the pump returns by reconciliation is newly
/// load-bearing. At the default 256 the window never empties and none of that is
/// exercised; at 8 it empties constantly and only the return path can refill it.
fn mux_config() -> MuxConfig {
    MuxConfig {
        enabled: true,
        initial_credit: 8,
        credit_sweep_interval: Duration::from_millis(1),
        ..MuxConfig::default()
    }
}

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

/// One node plus the registry its own collectors were installed into.
struct Node {
    velo: Arc<Velo>,
    registry: Registry,
}

impl Node {
    fn worker_id(&self) -> WorkerId {
        self.velo.instance_id().worker_id()
    }

    fn snapshot(&self) -> MetricSnapshot {
        MetricSnapshot::from_registry(&self.registry)
    }

    /// Successful attaches this node answered, by the transport it chose.
    ///
    /// The attach handler labels the counter with the key it settled on, which
    /// makes it the one place the negotiated outcome is observable from outside.
    fn attaches_over(&self, transport: &str) -> f64 {
        self.snapshot().counter(
            "velo_streaming_anchor_operations_total",
            &[
                ("operation", "attach"),
                ("outcome", "success"),
                ("transport_scheme", transport),
            ],
        )
    }

    fn mux_live_slots(&self) -> f64 {
        self.snapshot().gauge("velo_streaming_mux_live_slots", &[])
    }

    /// Batches this node's ingress lane decoded — one per `_stream_batch`
    /// active message, however many streams' records rode in it.
    fn mux_batches_received(&self) -> f64 {
        self.snapshot().counter(
            "velo_streaming_mux_batches_total",
            &[("direction", "received")],
        )
    }

    /// Batches this node's egress batchers handed to the messenger. Zero is
    /// proof no slot was ever opened from here, which is what a negative
    /// negotiation arm needs and what the `live_slots` gauge cannot give — that
    /// gauge returns to zero on the mux path too.
    fn mux_batches_sent(&self) -> f64 {
        self.snapshot()
            .counter("velo_streaming_mux_batches_total", &[("direction", "sent")])
    }

    fn mux_records_received(&self) -> f64 {
        self.snapshot()
            .histogram_sum("velo_streaming_mux_records_per_batch", &[])
    }

    /// The applier never met a full slot buffer. Non-zero is a broken credit
    /// invariant, and nothing else in a test would show it.
    fn assert_no_reader_stall(&self) {
        assert_eq!(
            self.snapshot()
                .counter("velo_streaming_mux_reader_stall_total", &[]),
            0.0,
            "the applier hit a full slot buffer: credit and buffer depth disagree"
        );
    }

    fn registers_mux(&self) -> bool {
        self.velo
            .anchor_manager()
            .transport_registry
            .contains_key(MUX_KEY)
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
    let velo = builder.build().await.expect("build velo");
    Node { velo, registry }
}

/// Two cross-registered nodes: `consumer` owns the anchors, `producer` attaches.
async fn pair(consumer: Option<MuxConfig>, producer: Option<MuxConfig>) -> (Node, Node) {
    let consumer = node(consumer).await;
    let producer = node(producer).await;
    consumer
        .velo
        .register_peer(producer.velo.peer_info())
        .expect("register producer on consumer");
    producer
        .velo
        .register_peer(consumer.velo.peer_info())
        .expect("register consumer on producer");
    tokio::time::sleep(SETTLE).await;
    (consumer, producer)
}

fn transfer(handle: StreamAnchorHandle) -> StreamAnchorHandle {
    StreamAnchorHandle::from_u128(handle.as_u128())
}

async fn eventually(mut predicate: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + PATIENCE;
    while tokio::time::Instant::now() < deadline {
        if predicate() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("condition never held within {PATIENCE:?}");
}

/// Stream `count` items and a `Finalized` from `producer` to `consumer`'s
/// anchor, asserting every item arrives exactly once and in order.
async fn stream_spsc(consumer: &Node, producer: &Node, count: u32) {
    let mut anchor = consumer.velo.create_anchor::<u32>();
    let handle = transfer(anchor.handle());

    let sender = producer
        .velo
        .attach_anchor::<u32>(handle)
        .await
        .expect("remote attach");

    let send = tokio::spawn(async move {
        for n in 0..count {
            sender.send(n).await.expect("send item");
        }
        sender.finalize().expect("finalize");
    });

    let collect = async {
        let mut items = Vec::with_capacity(count as usize);
        while let Some(frame) = anchor.next().await {
            match frame.expect("no stream error") {
                StreamFrame::Item(value) => items.push(value),
                StreamFrame::Finalized => break,
                other => panic!("unexpected frame: {other:?}"),
            }
        }
        items
    };

    let items = tokio::time::timeout(PATIENCE, collect)
        .await
        .expect("timed out collecting items");
    send.await.expect("send task");
    assert_eq!(
        items,
        (0..count).collect::<Vec<_>>(),
        "frames lost, duplicated or reordered"
    );
}

// ---------------------------------------------------------------------------
// Both sides new
// ---------------------------------------------------------------------------

/// (a) Two mux nodes negotiate the mux, and a stream runs the whole way.
#[tokio::test(flavor = "multi_thread")]
async fn two_mux_nodes_negotiate_the_mux_and_stream_through_it() {
    const FRAMES: u32 = 400;
    let (consumer, producer) = pair(Some(mux_config()), Some(mux_config())).await;
    assert!(consumer.registers_mux() && producer.registers_mux());

    stream_spsc(&consumer, &producer, FRAMES).await;

    assert_eq!(
        consumer.attaches_over(MUX_KEY),
        1.0,
        "both sides advertised {MUX_KEY}, so the attach must have settled on it"
    );
    assert_eq!(consumer.attaches_over(LEGACY_KEY), 0.0);
    // Far more frames than the 8-credit window, so the run is only possible if
    // credit came back — repeatedly — through `reader_pump`'s reconciliation.
    assert!(consumer.mux_records_received() >= f64::from(FRAMES));
    consumer.assert_no_reader_stall();
    eventually(|| consumer.mux_live_slots() == 0.0).await;
    eventually(|| producer.mux_live_slots() == 0.0).await;
}

/// (b) The MPSC anchor kind negotiates in the same version, for every producer.
#[tokio::test(flavor = "multi_thread")]
async fn an_mpsc_anchor_negotiates_the_mux_for_all_of_its_producers() {
    const SENDERS: u32 = 3;
    const PER_SENDER: u32 = 60;
    let (consumer, producer) = pair(Some(mux_config()), Some(mux_config())).await;

    let mut anchor = consumer.velo.create_mpsc_anchor::<u32>();
    let handle = transfer(anchor.handle());

    let mut senders = Vec::new();
    for _ in 0..SENDERS {
        senders.push(
            producer
                .velo
                .attach_mpsc_anchor::<u32>(handle)
                .await
                .expect("remote mpsc attach"),
        );
    }

    let send = tokio::spawn(async move {
        for n in 0..PER_SENDER {
            for sender in &senders {
                sender.send(n).await.expect("send item");
            }
        }
        for sender in senders {
            sender.detach().await.expect("detach");
        }
    });

    // Per-sender ordering is the MPSC guarantee — the interleaving between
    // senders is not, and the mux packs all three into shared batches.
    let mut next: std::collections::BTreeMap<u64, u32> = std::collections::BTreeMap::new();
    let mut finished = 0;
    let collect = async {
        while finished < SENDERS {
            let Some(frame) = anchor.next().await else {
                break;
            };
            match frame.expect("no stream error") {
                (sender_id, MpscFrame::Item(value)) => {
                    let expected = next.entry(sender_id.0).or_default();
                    assert_eq!(value, *expected, "sender {sender_id} out of order");
                    *expected += 1;
                }
                (_, MpscFrame::Detached) => finished += 1,
                (_, MpscFrame::Dropped(reason)) => panic!("sender dropped: {reason:?}"),
                (_, MpscFrame::SenderError(message)) => panic!("sender error: {message}"),
            }
        }
    };
    tokio::time::timeout(PATIENCE, collect)
        .await
        .expect("timed out collecting mpsc frames");
    send.await.expect("send task");

    assert_eq!(next.len(), SENDERS as usize, "every sender must have sent");
    assert!(next.values().all(|delivered| *delivered == PER_SENDER));

    // The MPSC attach handler does not label the operations counter, so the
    // negotiated outcome is read off the ingress lane instead: nothing but a
    // `_stream_batch` from the producer can move this, and every assertion
    // above would hold just as well over TCP.
    let batches = consumer.mux_batches_received();
    assert!(batches > 0.0, "the attach did not settle on {MUX_KEY}");
    assert!(
        batches < f64::from(SENDERS * PER_SENDER),
        "{batches} batches for {} records — the producers did not share any",
        SENDERS * PER_SENDER
    );
    consumer.assert_no_reader_stall();
    eventually(|| consumer.mux_live_slots() == 0.0).await;
}

/// (h) Concurrent streams to one peer share one `_stream_batch` flow, and each
/// keeps its own order inside it.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_streams_to_one_peer_share_the_batch_flow() {
    const STREAMS: u32 = 6;
    const FRAMES: u32 = 100;
    let (consumer, producer) = pair(Some(mux_config()), Some(mux_config())).await;

    let mut anchors = Vec::new();
    let mut senders = Vec::new();
    for _ in 0..STREAMS {
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

    let send = tokio::spawn(async move {
        for n in 0..FRAMES {
            for sender in &senders {
                sender.send(n).await.expect("send item");
            }
        }
        for sender in senders {
            sender.finalize().expect("finalize");
        }
    });

    let collectors = anchors.into_iter().map(|mut anchor| async move {
        let mut items = Vec::with_capacity(FRAMES as usize);
        while let Some(frame) = anchor.next().await {
            match frame.expect("no stream error") {
                StreamFrame::Item(value) => items.push(value),
                StreamFrame::Finalized => break,
                other => panic!("unexpected frame: {other:?}"),
            }
        }
        items
    });
    let collected = tokio::time::timeout(PATIENCE, futures::future::join_all(collectors))
        .await
        .expect("timed out collecting items");
    send.await.expect("send task");

    for items in &collected {
        assert_eq!(
            items,
            &(0..FRAMES).collect::<Vec<_>>(),
            "per-stream order must hold even though the streams share batches"
        );
    }
    assert_eq!(consumer.attaches_over(MUX_KEY), f64::from(STREAMS));
    // The whole point of the mux: far fewer active messages than records, which
    // can only happen if records from several slots travelled together.
    let records = consumer.mux_records_received();
    let batches = consumer.mux_batches_received();
    assert!(
        records > batches,
        "{records} records arrived in {batches} batches — nothing was multiplexed"
    );
    consumer.assert_no_reader_stall();
    eventually(|| consumer.mux_live_slots() == 0.0).await;
}

// ---------------------------------------------------------------------------
// Mixed deployments
// ---------------------------------------------------------------------------

/// The attach request as it looked before negotiation: no key list at all.
#[derive(Serialize)]
struct PreNegotiationAttachRequest {
    handle: StreamAnchorHandle,
    session_id: u64,
    stream_cancel_handle: StreamCancelHandle,
}

/// (c) A sender whose request has no key list gets the legacy transport.
///
/// The field is absent from the payload rather than present and empty, so the
/// `#[serde(default)]` on the receiver is what produces the empty list — the
/// actual mechanism an older sender relies on.
#[tokio::test(flavor = "multi_thread")]
async fn a_request_from_before_negotiation_is_answered_with_the_legacy_key() {
    let (consumer, producer) = pair(Some(mux_config()), Some(mux_config())).await;
    let anchor = consumer.velo.create_anchor::<u32>();
    let handle = transfer(anchor.handle());

    let request = PreNegotiationAttachRequest {
        handle,
        session_id: 1,
        stream_cancel_handle: StreamCancelHandle::pack(producer.worker_id(), 1),
    };
    let response: AnchorAttachResponse = producer
        .velo
        .messenger()
        .typed_unary_streaming::<AnchorAttachResponse>("_anchor_attach")
        .payload(&request)
        .expect("payload")
        .worker(consumer.worker_id())
        .send()
        .await
        .expect("attach round trip");

    match response {
        AnchorAttachResponse::Ok {
            streaming_transport_key,
            initial_credit,
            slot_byte_budget,
            ..
        } => {
            assert_eq!(
                streaming_transport_key.as_str(),
                LEGACY_KEY,
                "a receiver that answered {MUX_KEY} here would break a sender \
                 whose resolve hard-errors on a key it does not know"
            );
            assert_eq!(initial_credit, 0, "no mux was offered, so no window is");
            assert_eq!(slot_byte_budget, 0);
        }
        AnchorAttachResponse::Err { reason } => panic!("attach rejected: {reason}"),
    }
}

/// (c) The same case with a whole stream behind it: a node built without the
/// mux talks to one built with it, and nothing about the stream changes.
#[tokio::test(flavor = "multi_thread")]
async fn a_sender_without_a_mux_streams_over_the_legacy_path() {
    let (consumer, producer) = pair(Some(mux_config()), None).await;
    assert!(consumer.registers_mux() && !producer.registers_mux());

    stream_spsc(&consumer, &producer, 200).await;

    assert_eq!(consumer.attaches_over(LEGACY_KEY), 1.0);
    assert_eq!(consumer.attaches_over(MUX_KEY), 0.0);
    assert_eq!(
        consumer.mux_live_slots(),
        0.0,
        "the mux is installed on the consumer but nothing may reach it"
    );
}

/// (d) A mux sender against a receiver without one: SPSC.
#[tokio::test(flavor = "multi_thread")]
async fn a_mux_sender_falls_back_when_the_receiver_has_no_mux() {
    let (consumer, producer) = pair(None, Some(mux_config())).await;

    stream_spsc(&consumer, &producer, 200).await;

    assert_eq!(consumer.attaches_over(LEGACY_KEY), 1.0);
    assert_eq!(
        producer.mux_live_slots(),
        0.0,
        "the sender has a mux and advertised it, but the answer named another \
         transport and nothing may open a slot"
    );
}

/// (d) The same, for the MPSC anchor kind.
#[tokio::test(flavor = "multi_thread")]
async fn a_mux_mpsc_sender_falls_back_when_the_receiver_has_no_mux() {
    let (consumer, producer) = pair(None, Some(mux_config())).await;

    let mut anchor = consumer.velo.create_mpsc_anchor::<u32>();
    let handle = transfer(anchor.handle());
    let sender = producer
        .velo
        .attach_mpsc_anchor::<u32>(handle)
        .await
        .expect("remote mpsc attach");

    for n in 0..50u32 {
        sender.send(n).await.expect("send item");
    }
    sender.detach().await.expect("detach");

    let collect = async {
        let mut items = Vec::new();
        while let Some(frame) = anchor.next().await {
            match frame.expect("no stream error") {
                (_, MpscFrame::Item(value)) => items.push(value),
                (_, MpscFrame::Detached) => break,
                (_, other) => panic!("unexpected frame: {other:?}"),
            }
        }
        items
    };
    let items = tokio::time::timeout(PATIENCE, collect)
        .await
        .expect("timed out collecting items");

    assert_eq!(items, (0..50).collect::<Vec<_>>());
    assert_eq!(
        producer.mux_batches_sent(),
        0.0,
        "the sender has a mux and advertised it, but the answer named another \
         transport, so no slot may have been opened and no batch sent"
    );
}

/// (d) The response half of the same story, at the wire: an MPSC response from
/// before negotiation carries no credit fields, and defaults to offering none.
#[tokio::test(flavor = "multi_thread")]
async fn an_mpsc_response_from_before_negotiation_offers_no_window() {
    let (consumer, producer) = pair(None, Some(mux_config())).await;
    let anchor = consumer.velo.create_mpsc_anchor::<u32>();
    let handle = transfer(anchor.handle());

    let request = PreNegotiationAttachRequest {
        handle,
        session_id: 1,
        stream_cancel_handle: StreamCancelHandle::pack(producer.worker_id(), 1),
    };
    let response: MpscAnchorAttachResponse = producer
        .velo
        .messenger()
        .typed_unary_streaming::<MpscAnchorAttachResponse>("_mpsc_anchor_attach")
        .payload(&request)
        .expect("payload")
        .worker(consumer.worker_id())
        .send()
        .await
        .expect("attach round trip");

    match response {
        MpscAnchorAttachResponse::Ok {
            streaming_transport_key,
            initial_credit,
            ..
        } => {
            assert_eq!(streaming_transport_key.as_str(), LEGACY_KEY);
            assert_eq!(initial_credit, 0);
        }
        MpscAnchorAttachResponse::Err { reason } => panic!("attach rejected: {reason}"),
    }
}

// ---------------------------------------------------------------------------
// The switch
// ---------------------------------------------------------------------------

/// (e) A mux configured but disabled is not installed, so it cannot be
/// advertised and cannot be selected.
#[tokio::test(flavor = "multi_thread")]
async fn a_disabled_mux_is_never_registered_advertised_or_selected() {
    let disabled = MuxConfig {
        enabled: false,
        ..mux_config()
    };
    let (consumer, producer) = pair(Some(mux_config()), Some(disabled)).await;
    assert!(
        !producer.registers_mux(),
        "a disabled mux must not reach the transport registry"
    );

    stream_spsc(&consumer, &producer, 100).await;

    // The consumer has a mux and would have chosen it had the producer named
    // it, so this is the advertisement being absent, observed from the outside.
    assert_eq!(consumer.attaches_over(LEGACY_KEY), 1.0);
    assert_eq!(consumer.attaches_over(MUX_KEY), 0.0);
}

/// (g) Rollback: the same pair with the flag turned off negotiates the legacy
/// path again.
///
/// `MuxConfig` is read at build time, so this is a rebuild rather than a live
/// toggle — which is exactly what a rollback is in production too, since the
/// switch is deployment configuration. What it pins is that nothing else has to
/// change with it: same code, same peers, same anchors, no wire change.
#[tokio::test(flavor = "multi_thread")]
async fn flipping_the_switch_off_returns_the_next_attach_to_the_legacy_path() {
    let (consumer, producer) = pair(Some(mux_config()), Some(mux_config())).await;
    stream_spsc(&consumer, &producer, 50).await;
    assert_eq!(consumer.attaches_over(MUX_KEY), 1.0);
    drop((consumer, producer));

    let rolled_back = MuxConfig {
        enabled: false,
        ..mux_config()
    };
    let (consumer, producer) = pair(Some(rolled_back.clone()), Some(rolled_back)).await;
    stream_spsc(&consumer, &producer, 50).await;
    assert_eq!(consumer.attaches_over(LEGACY_KEY), 1.0);
    assert_eq!(consumer.attaches_over(MUX_KEY), 0.0);
}

/// One mux per instance, refused where the mistake is made rather than at the
/// second attach.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_mux_is_refused_at_build_time() {
    let error = Velo::builder()
        .add_transport(tcp_transport())
        .messenger_mux(mux_config())
        .expect("first mux accepted")
        .messenger_mux(mux_config())
        .err()
        .expect("a second mux must be refused");
    assert!(
        error.to_string().contains("more than once"),
        "unhelpful error: {error}"
    );
}

/// A zero window is the wire encoding of "not offering the mux", so a node
/// cannot be configured to install one and then tell every peer to ignore it.
#[tokio::test(flavor = "multi_thread")]
async fn a_zero_credit_window_is_refused_at_build_time() {
    let error = Velo::builder()
        .add_transport(tcp_transport())
        .stream_bind_addr(std::net::Ipv4Addr::LOCALHOST.into())
        .messenger_mux(MuxConfig {
            initial_credit: 0,
            ..mux_config()
        })
        .expect("accepted by the builder")
        .build()
        .await
        .err()
        .expect("a zero credit window must fail the build");
    assert!(
        error.to_string().contains("initial_credit = 0"),
        "unhelpful error: {error}"
    );
}
