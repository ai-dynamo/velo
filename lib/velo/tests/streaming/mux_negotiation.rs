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
use velo::streaming::mpsc::{MpscAnchorAttachRequest, MpscAnchorAttachResponse};
use velo::streaming::{FrameTransport, MpscFrame, MuxConfig, StreamAnchorHandle, StreamFrame};
use velo::transports::tcp::TcpTransportBuilder;
use velo::{Velo, VeloBuilder};
use velo_ext::WorkerId;

/// The streaming transport a `Velo` node runs when nothing else is configured.
const LEGACY_KEY: &str = "tcp-stream";
/// Aliased rather than re-typed: the literal belongs to negotiation, and a
/// second copy of it here could agree with the first for a whole release.
const MUX_KEY: &str = velo::streaming::MESSENGER_MUX_KEY;

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
    // Wait on the handler the attach actually needs rather than on a fixed
    // sleep: under a loaded machine a fixed settle is a flake, and every arm
    // here begins with an attach.
    ready(&producer, consumer.velo.instance_id(), "_anchor_attach").await;
    ready(&consumer, producer.velo.instance_id(), "_anchor_attach").await;
    (consumer, producer)
}

/// Block until `node` can see `handler` on `peer`.
async fn ready(node: &Node, peer: velo_ext::InstanceId, handler: &str) {
    tokio::time::timeout(PATIENCE, node.velo.wait_for_handler(peer, handler))
        .await
        .expect("timed out waiting for the peer's control plane")
        .expect("peer never advertised the handler");
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

/// (d) The MPSC *request* half at the wire: a request with no key list is
/// answered with the legacy key and no window.
///
/// Deliberately named for what it covers. The response here is produced by the
/// current handler, so it carries the credit fields (both zero); the arm below
/// is the one that exercises a response genuinely lacking them.
#[tokio::test(flavor = "multi_thread")]
async fn an_mpsc_request_from_before_negotiation_is_answered_with_no_window() {
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
// A receiver that answers without the credit fields at all
// ---------------------------------------------------------------------------

/// The MPSC attach response as it looked before negotiation.
///
/// Externally tagged, like the real enum, so the bytes on the wire are what an
/// older receiver would have written — with `initial_credit` and
/// `slot_byte_budget` simply absent rather than present and zero. Serialising
/// the real type could not produce this: it always writes both.
#[derive(Serialize)]
enum PreNegotiationMpscAttachResponse {
    Ok {
        streaming_transport_key: velo_ext::TransportKey,
        heartbeat_interval_ms: u64,
        sender_id: u64,
        routing_session_id: u64,
    },
}

/// A worker that answers `_mpsc_anchor_attach` the way one did before
/// negotiation, and really binds its legacy transport behind the answer.
///
/// The key it answers with is the caller's choice, because that is what
/// separates the two arms below: naming the legacy transport is the ordinary
/// mixed-deployment case, and naming the mux with the same field-less payload is
/// the only way to reach the branch where the *absent credit fields* are what
/// decides.
struct LegacyReceiver {
    messenger: Arc<velo::messenger::Messenger>,
    /// Carries only the `tcp-stream` entry, for the producer's frame transport.
    stream_peer: velo_ext::PeerInfo,
    /// Frames the bound legacy transport delivered.
    frames: flume::Receiver<Vec<u8>>,
    _stream: Arc<velo::streaming::TcpFrameTransport>,
}

async fn legacy_mpsc_receiver(answers_with: &str) -> LegacyReceiver {
    let messenger = velo::messenger::Messenger::builder()
        .add_transport(tcp_transport())
        .build()
        .await
        .expect("legacy receiver messenger");
    let stream = velo::streaming::TcpFrameTransport::new(std::net::Ipv4Addr::LOCALHOST.into())
        .await
        .expect("legacy streaming listener");
    let stream_peer = velo_ext::PeerInfo::new(messenger.instance_id(), stream.address());

    let (frame_tx, frames) = flume::unbounded::<Vec<u8>>();
    let bind_stream = Arc::clone(&stream);
    let answered_key = velo_ext::TransportKey::new(answers_with);
    let handler = velo::messenger::Handler::typed_unary_async(
        "_mpsc_anchor_attach",
        move |ctx: velo::messenger::TypedContext<MpscAnchorAttachRequest>| {
            let stream = Arc::clone(&bind_stream);
            let frame_tx = frame_tx.clone();
            let answered_key = answered_key.clone();
            async move {
                let (_, local_id) = ctx.input.handle.unpack();
                let routing_session_id = 1;
                let rx = stream.bind(local_id, routing_session_id).await?;
                tokio::spawn(async move {
                    while let Ok(bytes) = rx.recv_async().await {
                        if frame_tx.send_async(bytes).await.is_err() {
                            break;
                        }
                    }
                });
                Ok(PreNegotiationMpscAttachResponse::Ok {
                    streaming_transport_key: answered_key,
                    heartbeat_interval_ms: 5_000,
                    sender_id: 1,
                    routing_session_id,
                })
            }
        },
    )
    .spawn()
    .build();
    // The same door the real control plane uses: `register_handler` refuses
    // names beginning with `_`.
    messenger
        .register_streaming_handler(handler)
        .expect("register legacy attach handler");

    LegacyReceiver {
        messenger,
        stream_peer,
        frames,
        _stream: stream,
    }
}

/// Cross-register a `Velo` producer with a stand-in receiver.
///
/// The control plane both ways, then the producer's frame transport is told
/// where the receiver's streaming listener is. `Velo::register_peer` does both
/// at once for a `Velo` peer; this peer is not one, and its messenger
/// `PeerInfo` carries no `tcp-stream` entry to fan out.
async fn wire_up(producer: &Node, receiver: &LegacyReceiver) {
    producer
        .velo
        .register_peer(receiver.messenger.peer_info())
        .expect("register receiver on producer");
    receiver
        .messenger
        .register_peer(producer.velo.peer_info())
        .expect("register producer on receiver");
    producer
        .velo
        .anchor_manager()
        .transport_registry
        .get(LEGACY_KEY)
        .expect("the producer runs the legacy transport too")
        .register(&receiver.stream_peer)
        .expect("register the receiver's streaming endpoint");
    ready(
        producer,
        receiver.messenger.instance_id(),
        "_mpsc_anchor_attach",
    )
    .await;
}

/// (d) A response that genuinely omits the credit fields keeps a mux sender on
/// the legacy path — and the stream still runs.
///
/// The other direction of the compatibility claim, and the one no pair of
/// current nodes can produce: every receiver in this build writes both fields,
/// so the only way to exercise the sender's decode of a response without them
/// is to have something else answer. `#[serde(default)]` filling the absent
/// window with zero is what makes the sender refuse to open a mux slot, and the
/// batches-sent counter is what fails if it opened one anyway.
#[tokio::test(flavor = "multi_thread")]
async fn a_response_from_before_negotiation_keeps_an_mpsc_sender_on_the_legacy_path() {
    const FRAMES: u32 = 20;
    let producer = node(Some(mux_config())).await;
    let receiver = legacy_mpsc_receiver(LEGACY_KEY).await;

    wire_up(&producer, &receiver).await;

    let handle = StreamAnchorHandle::pack_mpsc(
        receiver.messenger.instance_id().worker_id(),
        7, // any id: the stand-in receiver keeps no anchor registry
    );
    let sender = producer
        .velo
        .attach_mpsc_anchor::<u32>(handle)
        .await
        .expect("remote mpsc attach");

    for n in 0..FRAMES {
        sender.send(n).await.expect("send item");
    }

    let collect = async {
        let mut items = Vec::with_capacity(FRAMES as usize);
        while items.len() < FRAMES as usize {
            let bytes = receiver.frames.recv_async().await.expect("frame channel");
            match rmp_serde::from_slice::<StreamFrame<u32>>(&bytes).expect("decode frame") {
                StreamFrame::Item(value) => items.push(value),
                StreamFrame::Heartbeat => {}
                other => panic!("unexpected frame: {other:?}"),
            }
        }
        items
    };
    let items = tokio::time::timeout(PATIENCE, collect)
        .await
        .expect("timed out collecting items");

    assert_eq!(
        items,
        (0..FRAMES).collect::<Vec<_>>(),
        "the legacy transport must carry the stream unchanged"
    );
    assert_eq!(
        producer.mux_batches_sent(),
        0.0,
        "a response with no window makes the mux unusable, so no slot may be \
         opened and no batch sent"
    );
}

/// (d) The arm where the *absent credit fields* are what decides.
///
/// Its sibling above proves the sender still streams when a field-less response
/// names the legacy transport — but there the key alone settles it, so the
/// batches-sent assertion would hold even if an absent window were wrongly read
/// as a usable one. Here the key matches, so nothing but the window is left to
/// decide, and `#[serde(default)]` turning "no field" into "no window" is the
/// whole difference between failing here and opening a slot into a peer that
/// never sized a buffer for it.
///
/// It fails loudly rather than retrying elsewhere: a peer naming the mux has
/// bound a mux receiver, so connecting over any other transport would reach
/// nothing it is listening on and hang until the anchor's watchdog fires.
#[tokio::test(flavor = "multi_thread")]
async fn a_mux_key_answered_without_a_window_fails_the_attach() {
    let producer = node(Some(mux_config())).await;
    let receiver = legacy_mpsc_receiver(MUX_KEY).await;
    wire_up(&producer, &receiver).await;

    let handle = StreamAnchorHandle::pack_mpsc(receiver.messenger.instance_id().worker_id(), 7);
    let Err(error) = producer.velo.attach_mpsc_anchor::<u32>(handle).await else {
        panic!("an attach naming the mux with no window must fail");
    };
    let message = error.to_string();
    assert!(
        message.contains(MUX_KEY) && message.contains("initial_credit = 0"),
        "the error must name both the key and why it could not be honoured: {message}"
    );

    assert_eq!(
        producer.mux_batches_sent(),
        0.0,
        "the attach failed, so no slot was opened and nothing went out"
    );
    assert!(
        receiver.frames.is_empty(),
        "a failed attach must not have streamed over the legacy transport either"
    );
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

// ---------------------------------------------------------------------------
// The outcome, from the sending side
// ---------------------------------------------------------------------------

/// The sender is told which transport its attach settled on, and it is told the
/// same thing the receiver recorded.
///
/// Both arms assert the pair, not the accessor alone: the receiver labels its
/// attach counter with the key it answered with, so a sender-side report that
/// disagreed with it would be a report of something else. Reading only the
/// sender would pass just as well against a hardcoded key.
#[tokio::test(flavor = "multi_thread")]
async fn a_sender_is_told_which_transport_the_attach_settled_on() {
    // Both sides have the mux, so the attach settles on it.
    let (consumer, producer) = pair(Some(mux_config()), Some(mux_config())).await;
    let anchor = consumer.velo.create_anchor::<u32>();
    let sender = producer
        .velo
        .attach_anchor::<u32>(transfer(anchor.handle()))
        .await
        .expect("remote attach");

    assert_eq!(
        sender.negotiated_transport().map(|key| key.as_str()),
        Some(MUX_KEY)
    );
    assert_eq!(
        consumer.attaches_over(MUX_KEY),
        1.0,
        "the sender named {MUX_KEY}, so that is what the receiver must have answered"
    );
    drop((sender, anchor, consumer, producer));

    // The receiver has no mux, so the same sender settles on the legacy path.
    let (consumer, producer) = pair(None, Some(mux_config())).await;
    let anchor = consumer.velo.create_anchor::<u32>();
    let sender = producer
        .velo
        .attach_anchor::<u32>(transfer(anchor.handle()))
        .await
        .expect("remote attach");

    assert_eq!(
        sender.negotiated_transport().map(|key| key.as_str()),
        Some(LEGACY_KEY),
        "the sender advertised the mux, but the answer is the receiver's to give"
    );
    assert_eq!(consumer.attaches_over(LEGACY_KEY), 1.0);
}

/// A same-worker attach settles on nothing, because nothing was negotiated.
///
/// `None` here is the honest answer rather than a missing one: the frames go
/// straight into the anchor's channel and never reach a transport, so no key
/// would describe them — including this node's own mux, which is installed.
#[tokio::test(flavor = "multi_thread")]
async fn a_same_worker_attach_negotiates_no_transport() {
    let node = node(Some(mux_config())).await;
    let anchor = node.velo.create_anchor::<u32>();
    let sender = node
        .velo
        .attach_anchor::<u32>(anchor.handle())
        .await
        .expect("local attach");

    assert!(node.registers_mux());
    assert_eq!(sender.negotiated_transport(), None);
    assert_eq!(
        node.attaches_over(MUX_KEY) + node.attaches_over(LEGACY_KEY),
        0.0,
        "a local attach never reaches the attach handler that labels that counter"
    );
}
