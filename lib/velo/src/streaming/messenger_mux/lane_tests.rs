// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Streams on a lane other than 0, end to end over QUIC.
//!
//! Two `Velo` nodes on loopback QUIC, where each transport lane is its own
//! connection. The consumer's mux is told to place every stream on lane 3
//! ([`MessengerMuxTransport::force_lane`]), which is the only way to reach a
//! lane other than 0 until lanes are chosen per stream. What these tests pin is
//! that the lane the consumer names is the lane every hop uses: the producer's
//! batches, the consumer's ingress table, and the credit that comes back.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;

use super::{LaneIndex, MessengerMuxTransport, PeerLane};
use crate::Velo;
use crate::streaming::{StreamAnchor, StreamAnchorHandle, StreamFrame};
use crate::transports::quic::{QuicTransport, QuicTransportBuilder};

const BOUND: Duration = Duration::from_secs(20);

/// More than the default credit window of 32, so a stream cannot finish
/// unless credit comes back.
const ITEMS: u32 = 200;

const LANE: u16 = 3;

struct Node {
    velo: Arc<Velo>,
    quic: Arc<QuicTransport>,
}

impl Node {
    async fn new(lanes: u16) -> Self {
        let quic = Arc::new(
            QuicTransportBuilder::new()
                .bind_addr("127.0.0.1:0".parse().expect("loopback"))
                .lanes(lanes)
                .build()
                .expect("quic transport"),
        );
        let velo = Velo::builder()
            .add_transport(Arc::clone(&quic) as Arc<dyn crate::Transport>)
            .stream_bind_addr(std::net::Ipv4Addr::LOCALHOST.into())
            .build()
            .await
            .expect("build velo");
        Self { velo, quic }
    }

    fn mux(&self) -> Arc<MessengerMuxTransport> {
        self.velo
            .anchor_manager()
            .mux_handle()
            .and_then(|mux| mux.upgrade())
            .expect("velo installs the mux by default")
    }

    fn worker(&self) -> velo_ext::WorkerId {
        self.velo.instance_id().worker_id()
    }
}

/// A consumer whose mux places every stream on [`LANE`], and a producer, with
/// `consumer_lanes` and `producer_lanes` QUIC lanes.
async fn pair(consumer_lanes: u16, producer_lanes: u16) -> (Node, Node) {
    let consumer = Node::new(consumer_lanes).await;
    let producer = Node::new(producer_lanes).await;
    consumer.mux().force_lane(LaneIndex::new(LANE));
    consumer
        .velo
        .register_peer(producer.velo.peer_info())
        .expect("register producer on consumer");
    producer
        .velo
        .register_peer(consumer.velo.peer_info())
        .expect("register consumer on producer");
    for (node, peer) in [
        (&producer, consumer.velo.instance_id()),
        (&consumer, producer.velo.instance_id()),
    ] {
        tokio::time::timeout(BOUND, node.velo.wait_for_handler(peer, "_anchor_attach"))
            .await
            .expect("timed out waiting for the peer's control plane")
            .expect("peer never advertised the handler");
    }
    (consumer, producer)
}

fn transfer(handle: StreamAnchorHandle) -> StreamAnchorHandle {
    StreamAnchorHandle::from_u128(handle.as_u128())
}

async fn eventually(what: &str, mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(BOUND, async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

/// Send [`ITEMS`] items and a finalize, and read them back in order.
async fn stream_through(sender: crate::streaming::StreamSender<u32>, anchor: StreamAnchor<u32>) {
    let send = tokio::spawn(async move {
        for n in 0..ITEMS {
            sender.send(n).await.expect("send");
        }
        sender.finalize().expect("finalize");
    });
    let mut anchor = anchor;
    for n in 0..ITEMS {
        let frame = tokio::time::timeout(BOUND, anchor.next())
            .await
            .unwrap_or_else(|_| panic!("timed out at item {n}; did credit come back?"))
            .expect("stream ended early")
            .expect("no stream error");
        assert!(
            matches!(frame, StreamFrame::Item(m) if m == n),
            "expected item {n} in order, got {frame:?}"
        );
    }
    let last = tokio::time::timeout(BOUND, anchor.next())
        .await
        .expect("timed out waiting for the finalize")
        .expect("stream ended before the finalize")
        .expect("no stream error");
    assert!(matches!(last, StreamFrame::Finalized), "got {last:?}");
    send.await.expect("send task");
}

/// A stream the consumer placed on lane 3 rides lane 3 at every hop.
///
/// The producer's batches arrive on lane 3's handler (the consumer's ingress
/// table for (producer, 3) holds the slot, lane 0's holds nothing), and they
/// travel on the producer's QUIC lane 3. The consumer's credit goes back
/// through its (producer, 3) batcher, which is the only sender on the
/// consumer's QUIC lane 3 to the producer, so that connection existing is the
/// credit riding lane 3. The item count is past the credit window, so the
/// stream finishing in order proves the credit arrived and was applied to the
/// right slot.
#[tokio::test(flavor = "multi_thread")]
async fn a_stream_on_lane_three_rides_lane_three_both_ways() {
    let (consumer, producer) = pair(4, 4).await;
    let on_lane = PeerLane::new(producer.worker(), LaneIndex::new(LANE));
    let on_zero = PeerLane::new(producer.worker(), LaneIndex::ZERO);

    let anchor = consumer.velo.create_anchor::<u32>();
    let sender = producer
        .velo
        .attach_anchor::<u32>(transfer(anchor.handle()))
        .await
        .expect("remote attach");
    sender.send(u32::MAX).await.expect("first send");
    let consumer_mux = consumer.mux();
    eventually("the slot to open on lane 3", || {
        consumer_mux.live_ingress_slots(on_lane) == 1
    })
    .await;
    assert_eq!(consumer_mux.live_ingress_slots(on_zero), 0);

    let mut anchor = anchor;
    let first = tokio::time::timeout(BOUND, anchor.next())
        .await
        .expect("first item")
        .expect("open")
        .expect("no error");
    assert!(matches!(first, StreamFrame::Item(u32::MAX)));
    stream_through(sender, anchor).await;

    assert!(
        producer
            .quic
            .has_lane_connection(consumer.velo.instance_id(), LANE),
        "the producer's batches must travel on its QUIC lane 3"
    );
    assert!(
        consumer
            .quic
            .has_lane_connection(producer.velo.instance_id(), LANE),
        "the consumer's credit must travel on its QUIC lane 3"
    );
}

/// A ticket minted on lane 3 quotes it, survives the envelope, and opens the
/// slot on lane 3 with no attach.
#[tokio::test(flavor = "multi_thread")]
async fn a_ticket_opens_its_slot_on_the_lane_it_quotes() {
    let (consumer, producer) = pair(4, 4).await;
    let on_lane = PeerLane::new(producer.worker(), LaneIndex::new(LANE));

    let anchor = consumer.velo.create_anchor::<u32>();
    let ticket = consumer
        .velo
        .prebind_anchor(anchor.handle())
        .expect("ticket");
    let ticket: crate::streaming::control::StreamOpenTicket =
        serde_json::from_slice(&serde_json::to_vec(&ticket).expect("encode")).expect("decode");
    assert_eq!(ticket.lane, LANE);

    let sender = producer
        .velo
        .open_anchor_stream::<u32>(transfer(anchor.handle()), ticket)
        .await
        .expect("open from ticket");
    sender.send(u32::MAX).await.expect("first send");
    let consumer_mux = consumer.mux();
    eventually("the pre-bound slot to open on lane 3", || {
        consumer_mux.live_ingress_slots(on_lane) == 1
    })
    .await;

    let mut anchor = anchor;
    let first = tokio::time::timeout(BOUND, anchor.next())
        .await
        .expect("first item")
        .expect("open")
        .expect("no error");
    assert!(matches!(first, StreamFrame::Item(u32::MAX)));
    stream_through(sender, anchor).await;
}

/// A producer with one transport lane, told lane 3, clamps to lane 0 and the
/// stream works there.
///
/// "Works" alone would pass without the clamp: QUIC maps any lane past its
/// count onto one it has, and the consumer takes batches on every lane. So
/// the placement is what is asserted: the slot opens on (producer, 0) and
/// nothing opens on (producer, 3).
#[tokio::test(flavor = "multi_thread")]
async fn a_one_lane_producer_clamps_a_named_lane_to_zero() {
    let (consumer, producer) = pair(4, 1).await;
    let on_lane = PeerLane::new(producer.worker(), LaneIndex::new(LANE));
    let on_zero = PeerLane::new(producer.worker(), LaneIndex::ZERO);

    let anchor = consumer.velo.create_anchor::<u32>();
    let sender = producer
        .velo
        .attach_anchor::<u32>(transfer(anchor.handle()))
        .await
        .expect("remote attach");
    sender.send(u32::MAX).await.expect("first send");
    let consumer_mux = consumer.mux();
    eventually("the slot to open on lane 0", || {
        consumer_mux.live_ingress_slots(on_zero) == 1
    })
    .await;
    assert_eq!(
        consumer_mux.live_ingress_slots(on_lane),
        0,
        "a one-lane producer must not send on lane 3"
    );

    let mut anchor = anchor;
    let first = tokio::time::timeout(BOUND, anchor.next())
        .await
        .expect("first item")
        .expect("open")
        .expect("no error");
    assert!(matches!(first, StreamFrame::Item(u32::MAX)));
    stream_through(sender, anchor).await;
}
