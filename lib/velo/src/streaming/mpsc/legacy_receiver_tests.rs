// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! A receiver that answers an MPSC attach without the credit fields at all,
//! as one did before negotiation. Its stand-in binds a frame transport by
//! hand, so these are unit tests: the trait is crate-private. The rest of the
//! negotiation cases are integration tests (`tests/streaming/mux_negotiation`).

use std::sync::Arc;
use std::time::Duration;

use prometheus::Registry;
use serde::Serialize;
use velo_ext::{PeerInfo, TransportKey};

use crate::Velo;
use crate::observability::VeloMetrics;
use crate::observability::test_helpers::MetricSnapshot;
use crate::streaming::mpsc::control::MpscAnchorAttachRequest;
use crate::streaming::tcp_transport::TCP_STREAM_KEY as LEGACY_KEY;
use crate::streaming::transport::FrameTransport;
use crate::streaming::{MESSENGER_MUX_KEY as MUX_KEY, MuxConfig};
use crate::{StreamAnchorHandle, StreamFrame};

const PATIENCE: Duration = Duration::from_secs(30);

fn mux_config() -> MuxConfig {
    MuxConfig {
        enabled: true,
        initial_credit: 8,
        credit_sweep_interval: Duration::from_millis(1),
        ..MuxConfig::default()
    }
}

fn tcp_transport() -> Arc<crate::transports::tcp::TcpTransport> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    Arc::new(
        crate::transports::tcp::TcpTransportBuilder::new()
            .from_listener(listener)
            .expect("from_listener")
            .build()
            .expect("build transport"),
    )
}

/// A producer that runs the mux and the per-stream TCP transport, with its
/// own metrics registry.
struct Node {
    velo: Velo,
    registry: Registry,
}

impl Node {
    async fn new() -> Self {
        let registry = Registry::new();
        let metrics = Arc::new(VeloMetrics::register(&registry).expect("register metrics"));
        let velo = Velo::builder()
            .add_transport(tcp_transport())
            .stream_bind_addr(std::net::Ipv4Addr::LOCALHOST.into())
            .metrics(metrics)
            .messenger_mux(mux_config())
            .expect("configure mux")
            .build()
            .await
            .expect("build velo");
        Self { velo, registry }
    }

    fn mux_batches_sent(&self) -> f64 {
        MetricSnapshot::from_registry(&self.registry)
            .counter("velo_streaming_mux_batches_total", &[("direction", "sent")])
    }
}

async fn ready(node: &Node, peer: velo_ext::InstanceId, handler: &str) {
    tokio::time::timeout(PATIENCE, node.velo.wait_for_handler(peer, handler))
        .await
        .expect("timed out waiting for the peer's control plane")
        .expect("peer never advertised the handler");
}

/// The MPSC attach response as it looked before negotiation.
///
/// Externally tagged, like the real enum, so the bytes on the wire are what an
/// older receiver would have written — with `initial_credit` and
/// `slot_byte_budget` simply absent rather than present and zero. Serialising
/// the real type could not produce this: it always writes both.
#[derive(Serialize)]
enum PreNegotiationMpscAttachResponse {
    Ok {
        streaming_transport_key: TransportKey,
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
    messenger: Arc<crate::messenger::Messenger>,
    /// The node the messenger belongs to; its own attach handler is replaced.
    node: Velo,
    /// Carries only the `tcp-stream` entry, for the producer's frame transport.
    stream_peer: PeerInfo,
    /// Frames the bound legacy transport delivered.
    frames: flume::Receiver<Vec<u8>>,
    _stream: Arc<crate::streaming::TcpFrameTransport>,
}

async fn legacy_mpsc_receiver(answers_with: &str) -> LegacyReceiver {
    // A mux-only node, so it advertises no stream endpoint of its own; the
    // stand-in listener below is the only one the producer learns.
    let node = Velo::builder()
        .add_transport(tcp_transport())
        .mux_only()
        .build()
        .await
        .expect("legacy receiver node");
    let messenger = Arc::clone(node.messenger());
    let stream = crate::streaming::TcpFrameTransport::new(std::net::Ipv4Addr::LOCALHOST.into())
        .await
        .expect("legacy streaming listener");
    let stream_peer = PeerInfo::new(messenger.instance_id(), stream.address());

    let (frame_tx, frames) = flume::unbounded::<Vec<u8>>();
    let bind_stream = Arc::clone(&stream);
    let answered_key = TransportKey::new(answers_with);
    let handler = crate::Handler::typed_unary_async(
        "_mpsc_anchor_attach",
        move |ctx: crate::TypedContext<MpscAnchorAttachRequest>| {
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
    // names beginning with `_`. It replaces the node's own attach handler.
    messenger
        .register_streaming_handler(handler)
        .expect("register legacy attach handler");

    LegacyReceiver {
        messenger,
        node,
        stream_peer,
        frames,
        _stream: stream,
    }
}

/// Cross-register a `Velo` producer with a stand-in receiver.
///
/// The control plane both ways, then the producer's frame transport is told
/// where the receiver's streaming listener is. The receiver node is mux-only,
/// so its `PeerInfo` carries no `tcp-stream` entry for `Velo::register_peer`
/// to fan out.
async fn wire_up(producer: &Node, receiver: &LegacyReceiver) {
    producer
        .velo
        .register_peer(receiver.messenger.peer_info())
        .expect("register receiver on producer");
    receiver
        .node
        .register_peer(producer.velo.peer_info())
        .expect("register producer on receiver");
    producer
        .velo
        .inner
        .anchor_manager
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
    let producer = Node::new().await;
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
    let producer = Node::new().await;
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
