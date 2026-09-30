// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Lane placement and lanes other than 0, end to end over QUIC.
//!
//! Two `Velo` nodes on loopback QUIC, where each transport lane is its own
//! connection. The first tests place a stream on lane 3 through a key that
//! hashes there, and pin that the lane the consumer names is the lane every
//! hop uses: the producer's batches, the consumer's ingress table, and the
//! credit that comes back. The rest pin how the consumer places streams: by
//! key, by the least-used lane on attach, by the least-used lane on pre-bind,
//! and always lane 0 when its transport keeps one lane.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;

use super::lane_choice::keyed_lane;
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

/// A key that hashes to `lane` when the consumer keeps `lanes` lanes.
///
/// Searched rather than written down, so these tests do not depend on the
/// hash's values; the golden values are pinned in `lane_choice`.
fn key_for(lane: u16, lanes: u16) -> u64 {
    let lanes = std::num::NonZeroU16::new(lanes).expect("non-zero");
    (0..1024)
        .find(|key| keyed_lane(*key, lanes).get() == lane)
        .unwrap_or_else(|| panic!("no key below 1024 hashes to lane {lane} of {lanes}"))
}

/// Live slots from `peer` on each of the first `lanes` lanes of `mux`.
fn live_per_lane(mux: &MessengerMuxTransport, peer: velo_ext::WorkerId, lanes: u16) -> Vec<usize> {
    (0..lanes)
        .map(|lane| mux.live_ingress_slots(PeerLane::new(peer, LaneIndex::new(lane))))
        .collect()
}

/// A consumer and a producer, with `consumer_lanes` and `producer_lanes` QUIC
/// lanes.
async fn pair(consumer_lanes: u16, producer_lanes: u16) -> (Node, Node) {
    let consumer = Node::new(consumer_lanes).await;
    let producer = Node::new(producer_lanes).await;
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
        .attach_anchor_keyed::<u32>(transfer(anchor.handle()), key_for(LANE, 4))
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
        .prebind_anchor_keyed(anchor.handle(), key_for(LANE, 4))
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
        .attach_anchor_keyed::<u32>(transfer(anchor.handle()), key_for(LANE, 4))
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

/// An MPSC sender attached with a key opens on the lane the key hashes to.
///
/// The MPSC attach carries its own request and builds its own ticket from the
/// response, apart from the SPSC path, so a key or a lane dropped on either
/// leg would silently put the sender on lane 0.
#[tokio::test(flavor = "multi_thread")]
async fn an_mpsc_sender_opens_on_the_lane_its_attach_names() {
    let (consumer, producer) = pair(4, 4).await;
    let on_lane = PeerLane::new(producer.worker(), LaneIndex::new(LANE));
    let on_zero = PeerLane::new(producer.worker(), LaneIndex::ZERO);

    let anchor = consumer.velo.create_mpsc_anchor::<u32>();
    let sender = producer
        .velo
        .attach_mpsc_anchor_keyed::<u32>(transfer(anchor.handle()), key_for(LANE, 4))
        .await
        .expect("remote mpsc attach");
    sender.send(7).await.expect("send");
    let consumer_mux = consumer.mux();
    eventually("the mpsc slot to open on lane 3", || {
        consumer_mux.live_ingress_slots(on_lane) == 1
    })
    .await;
    assert_eq!(consumer_mux.live_ingress_slots(on_zero), 0);
    drop(sender);
    drop(anchor);
}

/// Open `count` unkeyed attaches from `producer` at once, all answered before
/// any of them sends.
async fn attach_all(
    consumer: &Node,
    producer: &Node,
    count: usize,
) -> Vec<(crate::streaming::StreamSender<u32>, StreamAnchor<u32>)> {
    let anchors: Vec<StreamAnchor<u32>> = (0..count)
        .map(|_| consumer.velo.create_anchor::<u32>())
        .collect();
    let senders = futures::future::join_all(anchors.iter().map(|anchor| {
        producer
            .velo
            .attach_anchor::<u32>(transfer(anchor.handle()))
    }))
    .await;
    senders
        .into_iter()
        .map(|sender| sender.expect("remote attach"))
        .zip(anchors)
        .collect()
}

/// Unkeyed attaches from one peer spread evenly over the lanes, even when all
/// of them are answered before any sender sends.
///
/// An `OpenSlot` arrives only with a sender's first batch, so while these
/// attaches are answered no slot is live yet. The consumer must count the
/// binds it has answered but not seen claimed, or all eight land on lane 0.
/// Once the slots open, those counts must fall back to zero: a claimed bind
/// is a live slot, and counting it twice would skew every later choice.
#[tokio::test(flavor = "multi_thread")]
async fn unkeyed_attaches_from_one_peer_spread_over_the_lanes() {
    let (consumer, producer) = pair(4, 4).await;
    let peer = producer.worker();
    let streams = attach_all(&consumer, &producer, 8).await;
    for (sender, _) in &streams {
        sender.send(u32::MAX).await.expect("first send");
    }
    let consumer_mux = consumer.mux();
    eventually("all eight slots to open", || {
        live_per_lane(&consumer_mux, peer, 4).iter().sum::<usize>() == 8
    })
    .await;
    assert_eq!(live_per_lane(&consumer_mux, peer, 4), [2, 2, 2, 2]);
    for lane in 0..4 {
        assert_eq!(
            consumer_mux.pending_binds_on(Some(peer), LaneIndex::new(lane)),
            0,
            "a claimed bind must stop counting as pending on lane {lane}"
        );
    }

    let finished =
        futures::future::join_all(streams.into_iter().map(|(sender, anchor)| async move {
            stream_through(sender, drain_first(anchor).await).await
        }));
    tokio::time::timeout(BOUND, finished)
        .await
        .expect("streams finished");
}

/// Read the `u32::MAX` a test sent to open the slot, and hand the anchor back.
async fn drain_first(mut anchor: StreamAnchor<u32>) -> StreamAnchor<u32> {
    let first = tokio::time::timeout(BOUND, anchor.next())
        .await
        .expect("first item")
        .expect("open")
        .expect("no error");
    assert!(matches!(first, StreamFrame::Item(u32::MAX)));
    anchor
}

/// A lane freed by a finished stream is the one the next attach takes.
///
/// Four attaches one after another land on lanes 0 to 3. The stream on lane 2
/// finishes and its slot retires. With every other lane holding one stream,
/// the next attach must go to lane 2; a count that did not fall would leave
/// all four lanes equal and send it to lane 0.
#[tokio::test(flavor = "multi_thread")]
async fn a_finished_stream_frees_its_lane_for_the_next_attach() {
    let (consumer, producer) = pair(4, 4).await;
    let peer = producer.worker();
    let consumer_mux = consumer.mux();

    let mut streams = Vec::new();
    for lane in 0..4 {
        let (sender, anchor) = attach_all(&consumer, &producer, 1)
            .await
            .pop()
            .expect("one stream");
        sender.send(u32::MAX).await.expect("first send");
        eventually("the slot to open", || {
            consumer_mux.live_ingress_slots(PeerLane::new(peer, LaneIndex::new(lane))) == 1
        })
        .await;
        streams.push((sender, anchor));
    }
    assert_eq!(live_per_lane(&consumer_mux, peer, 4), [1, 1, 1, 1]);

    let (sender, anchor) = streams.remove(2);
    stream_through(sender, drain_first(anchor).await).await;
    eventually("lane 2's slot to retire", || {
        live_per_lane(&consumer_mux, peer, 4) == [1, 1, 0, 1]
    })
    .await;

    let (sender, anchor) = attach_all(&consumer, &producer, 1)
        .await
        .pop()
        .expect("one stream");
    sender.send(u32::MAX).await.expect("first send");
    eventually("the new slot to open", || {
        live_per_lane(&consumer_mux, peer, 4).iter().sum::<usize>() == 4
    })
    .await;
    assert_eq!(live_per_lane(&consumer_mux, peer, 4), [1, 1, 1, 1]);
    streams.push((sender, anchor));

    for (sender, anchor) in streams {
        stream_through(sender, drain_first(anchor).await).await;
    }
}

/// Pre-binds spread by this node's own unclaimed pre-binds, and a lane's
/// count falls when its bind is claimed, released or expired.
///
/// No peer is known at pre-bind, so the count is the only input. Each step
/// frees one lane while the others stay held, so the next pre-bind landing
/// there proves that exit gave the count back.
#[tokio::test(flavor = "multi_thread")]
async fn pre_binds_spread_and_give_their_lane_back_on_claim_release_and_expiry() {
    let (consumer, producer) = pair(4, 4).await;
    let consumer_mux = consumer.mux();
    let pending = |lane: u16| consumer_mux.pending_binds_on(None, LaneIndex::new(lane));

    let mut anchors: Vec<StreamAnchor<u32>> = (0..4)
        .map(|_| consumer.velo.create_anchor::<u32>())
        .collect();
    let tickets: Vec<_> = anchors
        .iter()
        .map(|anchor| {
            consumer
                .velo
                .prebind_anchor(anchor.handle())
                .expect("ticket")
        })
        .collect();
    assert_eq!(
        tickets.iter().map(|ticket| ticket.lane).collect::<Vec<_>>(),
        [0, 1, 2, 3]
    );

    // Claim: the producer opens the ticket on lane 2 and sends.
    let claimed = anchors.remove(2);
    let sender = producer
        .velo
        .open_anchor_stream::<u32>(transfer(claimed.handle()), tickets[2].clone())
        .await
        .expect("open from ticket");
    sender.send(u32::MAX).await.expect("first send");
    eventually("the ticket's slot to open", || {
        consumer_mux.live_ingress_slots(PeerLane::new(producer.worker(), LaneIndex::new(2))) == 1
    })
    .await;
    assert_eq!(pending(2), 0, "a claimed pre-bind must stop counting");
    let after_claim = consumer.velo.create_anchor::<u32>();
    let ticket = consumer
        .velo
        .prebind_anchor(after_claim.handle())
        .expect("ticket");
    assert_eq!(ticket.lane, 2);
    anchors.push(after_claim);

    // Release: the anchor pre-bound on lane 1 is dropped before any sender
    // opens it.
    drop(anchors.remove(1));
    eventually("lane 1's pre-bind to be released", || pending(1) == 0).await;
    let after_release = consumer.velo.create_anchor::<u32>();
    let ticket = consumer
        .velo
        .prebind_anchor(after_release.handle())
        .expect("ticket");
    assert_eq!(ticket.lane, 1);
    anchors.push(after_release);

    // Expiry: the accept window closes on every unclaimed pre-bind.
    assert_eq!((0..4).map(pending).collect::<Vec<_>>(), [1, 1, 1, 1]);
    consumer_mux.expire_all_binds();
    assert_eq!((0..4).map(pending).collect::<Vec<_>>(), [0, 0, 0, 0]);

    stream_through(sender, drain_first(claimed).await).await;
}

/// Streams with one key share a lane; streams with keys on different lanes
/// each ride their own, and every one arrives complete and in order.
///
/// Four keys that hash to lanes 0 to 3 and a fifth stream with lane 1's key,
/// all streaming at once.
#[tokio::test(flavor = "multi_thread")]
async fn keyed_streams_ride_their_lanes_and_all_arrive_in_order() {
    let (consumer, producer) = pair(4, 4).await;
    let peer = producer.worker();
    let keys = [
        key_for(0, 4),
        key_for(1, 4),
        key_for(2, 4),
        key_for(3, 4),
        key_for(1, 4),
    ];
    let anchors: Vec<StreamAnchor<u32>> = keys
        .iter()
        .map(|_| consumer.velo.create_anchor::<u32>())
        .collect();
    let senders = futures::future::join_all(anchors.iter().zip(keys).map(|(anchor, key)| {
        producer
            .velo
            .attach_anchor_keyed::<u32>(transfer(anchor.handle()), key)
    }))
    .await;
    let senders: Vec<_> = senders
        .into_iter()
        .map(|sender| sender.expect("remote attach"))
        .collect();
    for sender in &senders {
        sender.send(u32::MAX).await.expect("first send");
    }
    let consumer_mux = consumer.mux();
    eventually("all five slots to open", || {
        live_per_lane(&consumer_mux, peer, 4).iter().sum::<usize>() == 5
    })
    .await;
    assert_eq!(live_per_lane(&consumer_mux, peer, 4), [1, 2, 1, 1]);

    let finished = futures::future::join_all(senders.into_iter().zip(anchors).map(
        |(sender, anchor)| async move { stream_through(sender, drain_first(anchor).await).await },
    ));
    tokio::time::timeout(BOUND, finished)
        .await
        .expect("streams finished");
    for lane in 0..4 {
        assert!(
            producer
                .quic
                .has_lane_connection(consumer.velo.instance_id(), lane),
            "the producer's batches must travel on its QUIC lane {lane}"
        );
    }
}

/// A consumer whose transport keeps one lane answers lane 0 to every key and
/// every unkeyed stream, even to a producer that keeps four.
///
/// This is what keeps a default deployment's attach answers and tickets the
/// same bytes as before lanes, which a worker built before lanes needs.
#[tokio::test(flavor = "multi_thread")]
async fn a_one_lane_consumer_places_every_stream_on_lane_zero() {
    let (consumer, producer) = pair(1, 4).await;
    let peer = producer.worker();

    let anchors: Vec<StreamAnchor<u32>> = (0..8)
        .map(|_| consumer.velo.create_anchor::<u32>())
        .collect();
    for (n, anchor) in anchors.iter().enumerate() {
        let ticket = if n % 2 == 0 {
            consumer
                .velo
                .prebind_anchor_keyed(anchor.handle(), key_for((n / 2) as u16, 4))
        } else {
            consumer.velo.prebind_anchor(anchor.handle())
        };
        assert_eq!(ticket.expect("ticket").lane, 0);
    }

    // The producer keeps four lanes, so it would send on lane 3 if the
    // consumer answered it.
    let anchor = consumer.velo.create_anchor::<u32>();
    let sender = producer
        .velo
        .attach_anchor_keyed::<u32>(transfer(anchor.handle()), key_for(LANE, 4))
        .await
        .expect("remote attach");
    sender.send(u32::MAX).await.expect("first send");
    let consumer_mux = consumer.mux();
    eventually("the slot to open", || {
        live_per_lane(&consumer_mux, peer, 4).iter().sum::<usize>() == 1
    })
    .await;
    assert_eq!(live_per_lane(&consumer_mux, peer, 4), [1, 0, 0, 0]);
    stream_through(sender, drain_first(anchor).await).await;
}
