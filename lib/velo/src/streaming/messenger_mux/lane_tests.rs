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
    connect(&consumer, &producer).await;
    (consumer, producer)
}

/// Make `a` and `b` peers of each other and wait for their control planes.
async fn connect(a: &Node, b: &Node) {
    a.velo
        .register_peer(b.velo.peer_info())
        .expect("register b on a");
    b.velo
        .register_peer(a.velo.peer_info())
        .expect("register a on b");
    for (node, peer) in [(b, a.velo.instance_id()), (a, b.velo.instance_id())] {
        tokio::time::timeout(BOUND, node.velo.wait_for_handler(peer, "_anchor_attach"))
            .await
            .expect("timed out waiting for the peer's control plane")
            .expect("peer never advertised the handler");
    }
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

/// Unkeyed attaches spread over every lane of a producer that keeps fewer
/// lanes than the consumer.
///
/// The consumer keeps 8 lanes and the producer 4, so a stream the consumer
/// places on lane k arrives on lane k % 4. A stream's load must stay on the
/// lane the consumer chose, from its bind until its slot retires. Counted on
/// the lane it arrived on instead, lanes 4 to 7 hold no load once their binds
/// are claimed: every attach after the fourth goes to lane 4 and rides the
/// producer's lane 0, and nothing reports it. Each attach here waits for its
/// stream's first record before the next one, so no bind is pending when the
/// next attach chooses and only the live slots decide.
#[tokio::test(flavor = "multi_thread")]
async fn unkeyed_attaches_spread_over_a_producer_with_fewer_lanes() {
    let (consumer, producer) = pair(8, 4).await;
    let peer = producer.worker();
    let consumer_mux = consumer.mux();
    assert_eq!(
        consumer_mux.core.transport_lanes(peer).get(),
        8,
        "the consumer must keep 8 lanes to the producer, or this proves nothing"
    );

    let mut streams = Vec::new();
    for _ in 0..8 {
        let (sender, anchor) = attach_all(&consumer, &producer, 1)
            .await
            .pop()
            .expect("one stream");
        sender.send(u32::MAX).await.expect("first send");
        streams.push((sender, drain_first(anchor).await));
    }
    assert_eq!(
        live_per_lane(&consumer_mux, peer, 8),
        [2, 2, 2, 2, 0, 0, 0, 0],
        "eight streams must arrive two on each of the producer's four lanes"
    );

    let finished = futures::future::join_all(
        streams
            .into_iter()
            .map(|(sender, anchor)| async move { stream_through(sender, anchor).await }),
    );
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

/// Pre-binds spread by this node's unclaimed pre-binds and live slots. A
/// claim moves a stream's count from pending to its live slot, and a release
/// or an expiry gives the count back.
///
/// No peer is known at pre-bind, so these counts are the only input. The
/// release step frees one lane while the others stay held, so the next
/// pre-bind landing there proves the release gave the count back.
#[tokio::test(flavor = "multi_thread")]
async fn pre_binds_spread_and_give_their_lane_back_on_release_and_expiry() {
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
    assert_eq!(
        pending(2),
        0,
        "a claimed pre-bind must stop counting as pending"
    );
    // Lane 2 is still counted by its live slot, so all four lanes hold one
    // and the tie goes to lane 0.
    let after_claim = consumer.velo.create_anchor::<u32>();
    let ticket = consumer
        .velo
        .prebind_anchor(after_claim.handle())
        .expect("ticket");
    assert_eq!(ticket.lane, 0, "a live slot must keep its lane counted");
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
    assert_eq!((0..4).map(pending).collect::<Vec<_>>(), [2, 1, 0, 1]);
    consumer_mux.expire_all_binds();
    assert_eq!((0..4).map(pending).collect::<Vec<_>>(), [0, 0, 0, 0]);

    stream_through(sender, drain_first(claimed).await).await;
}

/// Pre-bound streams that are claimed and still live keep their lane counted,
/// so later unkeyed pre-binds go to other lanes.
///
/// This is the frontend's pattern: it mints a ticket per request, a worker
/// opens it within milliseconds, and the stream then lives for seconds. A
/// count that fell at the claim would show every lane empty at almost every
/// pre-bind, and nearly all streams would pile onto lane 0. Here each ticket
/// is opened and its slot is live before the next is minted, so no pre-bind is
/// ever pending when the next one chooses.
///
/// The tickets are opened by two producers in turn. A pre-bind does not know
/// which peer will open it, so the count is the node's live slots on the lane
/// from every peer, not one peer's.
#[tokio::test(flavor = "multi_thread")]
async fn claimed_pre_binds_keep_their_lane_counted_while_their_streams_live() {
    let consumer = Node::new(4).await;
    let producers = [Node::new(4).await, Node::new(4).await];
    for producer in &producers {
        connect(&consumer, producer).await;
    }
    let consumer_mux = consumer.mux();
    let live_on_node = || -> Vec<usize> {
        (0..4)
            .map(|lane| {
                producers
                    .iter()
                    .map(|producer| {
                        consumer_mux.live_ingress_slots(PeerLane::new(
                            producer.worker(),
                            LaneIndex::new(lane),
                        ))
                    })
                    .sum()
            })
            .collect()
    };

    let mut streams = Vec::new();
    for n in 0..8 {
        let anchor = consumer.velo.create_anchor::<u32>();
        let ticket = consumer
            .velo
            .prebind_anchor(anchor.handle())
            .expect("ticket");
        let lane = ticket.lane;
        let sender = producers[n % 2]
            .velo
            .open_anchor_stream::<u32>(transfer(anchor.handle()), ticket)
            .await
            .expect("open from ticket");
        sender.send(u32::MAX).await.expect("first send");
        eventually("the ticket's slot to open", || {
            live_on_node().iter().sum::<usize>() == n + 1
        })
        .await;
        streams.push((lane, sender, anchor));
    }
    assert_eq!(
        live_on_node(),
        [2, 2, 2, 2],
        "pre-binds must spread by the node's live slots, not only its pending pre-binds"
    );

    // A finished stream gives its lane back: the next pre-bind takes it.
    let finished = streams
        .iter()
        .position(|(lane, _, _)| *lane == 2)
        .expect("a stream on lane 2");
    let (_, sender, anchor) = streams.remove(finished);
    stream_through(sender, drain_first(anchor).await).await;
    eventually("lane 2's slot to retire", || live_on_node() == [2, 2, 1, 2]).await;
    let next = consumer.velo.create_anchor::<u32>();
    let ticket = consumer.velo.prebind_anchor(next.handle()).expect("ticket");
    assert_eq!(ticket.lane, 2);

    let finished =
        futures::future::join_all(streams.into_iter().map(|(_, sender, anchor)| async move {
            stream_through(sender, drain_first(anchor).await).await
        }));
    tokio::time::timeout(BOUND, finished)
        .await
        .expect("streams finished");
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

/// A one-lane producer puts every stream on lane 0 of a 16-lane consumer, so
/// that one table must take slot indices past 65,536 / 16.
///
/// The sender picks the lane it rides, clamped to its own lane count, so the
/// consumer cannot size a table's slot range from its own lane count: that
/// gave (producer, 0) 4,096 indices here, and a producer with more live
/// streams than that had its `OpenSlot`s refused as protocol errors. The
/// `OpenSlot` is built by hand and fed to the consumer's receive path, because
/// opening 4,097 real streams would test the same thing slower.
#[tokio::test(flavor = "multi_thread")]
async fn a_one_lane_producer_opens_past_a_sixteenth_of_the_slot_ceiling() {
    use super::protocol::{BatchEncoder, SlotId};

    let (consumer, producer) = pair(16, 1).await;
    let mux = consumer.mux();
    let peer = producer.worker();
    assert_eq!(
        mux.core.transport_lanes(peer).get(),
        16,
        "the consumer must keep 16 lanes to the producer, or this proves nothing"
    );
    let key = PeerLane::new(peer, LaneIndex::ZERO);
    let sixteenth = super::ingress::MAX_INGRESS_SLOTS_PER_PEER / 16;
    let last = super::ingress::MAX_INGRESS_SLOTS_PER_PEER - 1;
    let mut receivers = Vec::new();
    for (session, index) in [(1u64, sixteenth), (2, last)] {
        let anchor_id = u64::MAX - session;
        let lane = mux.core.lane_load.reserve_on(Some(peer), LaneIndex::ZERO);
        receivers.push(mux.bind_on_lane(anchor_id, session, lane).unwrap());
        let mut encoder = BatchEncoder::new(1, session as u32, LaneIndex::ZERO);
        encoder
            .push_open_slot(
                SlotId::new(index as u32, 0).expect("index fits u24"),
                0,
                anchor_id,
                session,
            )
            .expect("encode");
        mux.core.deliver_batch(key, &encoder.finish().freeze());
    }
    assert_eq!(
        mux.live_ingress_slots(key),
        2,
        "slot indices {sixteenth} and {last} on lane 0 must open"
    );
}

/// An unkeyed attach chooses its lane without taking any ingress table's
/// mutex.
///
/// The ordered batch handler holds its table's mutex through a whole batch's
/// decode. A choice that read live slots by locking each table would wait
/// behind that batch, and every unkeyed attach on the node would wait behind
/// the choice. Here another thread holds the (producer, 0) table's mutex, and
/// the choice must finish anyway. The table must exist: with none, reading its
/// live slots takes no lock and the test would pass for nothing.
#[tokio::test(flavor = "multi_thread")]
async fn an_unkeyed_attach_choice_takes_no_ingress_table_lock() {
    let (consumer, producer) = pair(4, 4).await;
    let peer = producer.worker();
    let on_zero = PeerLane::new(peer, LaneIndex::ZERO);
    let anchor = consumer.velo.create_anchor::<u32>();
    let sender = producer
        .velo
        .attach_anchor_keyed::<u32>(transfer(anchor.handle()), key_for(0, 4))
        .await
        .expect("remote attach");
    sender.send(u32::MAX).await.expect("first send");
    let mux = consumer.mux();
    eventually("the slot to open on lane 0", || {
        mux.live_ingress_slots(on_zero) == 1
    })
    .await;

    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let holder = {
        let mux = Arc::clone(&mux);
        std::thread::spawn(move || {
            mux.core
                .ingress
                .with_table_locked(on_zero, || {
                    locked_tx.send(()).expect("test thread waits");
                    let _ = release_rx.recv();
                })
                .expect("the (producer, 0) table exists");
        })
    };
    locked_rx
        .recv_timeout(BOUND)
        .expect("the holder took the table's mutex");
    let (chosen_tx, chosen_rx) = std::sync::mpsc::channel();
    let chooser = {
        let mux = Arc::clone(&mux);
        std::thread::spawn(move || {
            let _ = chosen_tx.send(mux.choose_lane(Some(peer), None).lane());
        })
    };
    let chosen = chosen_rx.recv_timeout(Duration::from_secs(2));
    // Release before asserting, so a failure here ends as a failure and not
    // as a hung test.
    let _ = release_tx.send(());
    holder.join().expect("holder thread");
    chooser.join().expect("chooser thread");
    let lane = chosen.expect("an unkeyed attach choice must not wait on an ingress table's mutex");
    assert_ne!(lane, LaneIndex::ZERO, "lane 0 holds a live slot");

    stream_through(sender, drain_first(anchor).await).await;
}

/// An attach that fails after its bind gives the bind and its lane back at
/// once, on both attach handlers.
///
/// The failure arms after the bind (the anchor removed, attached, or
/// pre-bound while the handler bound) are races: the mux bind does not await,
/// so another thread must land in between. The bind hook plays that thread
/// and removes the anchor. Left to the accept window, the bind would count on
/// its lane for 60 s and skew every unkeyed choice for that peer meanwhile.
#[tokio::test(flavor = "multi_thread")]
async fn an_attach_that_fails_after_its_bind_gives_its_lane_back() {
    let (consumer, producer) = pair(4, 4).await;
    let mux = consumer.mux();
    let peer = producer.worker();
    let manager = consumer.velo.anchor_manager();
    let spsc = Arc::clone(&manager.registry);
    let mpsc = Arc::clone(&manager.mpsc_registry);
    assert!(
        mux.core
            .bind_hook
            .set(Box::new(move |anchor_id| {
                spsc.remove(&anchor_id);
                mpsc.remove(&anchor_id);
            }))
            .is_ok()
    );
    let counted = || {
        (
            (0..4)
                .map(|lane| mux.pending_binds_on(Some(peer), LaneIndex::new(lane)))
                .sum::<usize>(),
            mux.pending_binds(),
            mux.parked_drains(),
        )
    };

    let anchor = consumer.velo.create_anchor::<u32>();
    let attached = producer
        .velo
        .attach_anchor::<u32>(transfer(anchor.handle()))
        .await;
    assert!(attached.is_err(), "the anchor was removed during the bind");
    assert_eq!(
        counted(),
        (0, 0, 0),
        "a failed SPSC attach must give back its lane count, its bind and its drain signal"
    );

    let anchor = consumer.velo.create_mpsc_anchor::<u32>();
    let attached = producer
        .velo
        .attach_mpsc_anchor::<u32>(transfer(anchor.handle()))
        .await;
    assert!(attached.is_err(), "the anchor was removed during the bind");
    assert_eq!(
        counted(),
        (0, 0, 0),
        "a failed MPSC attach must give back its lane count, its bind and its drain signal"
    );
}

/// A transport that refuses admission on one lane once told to, and passes
/// everything else to QUIC.
///
/// A refused admission is the only send failure the mux batcher sees, and it
/// fails the epoch of that (peer, lane). A connection close refuses only the
/// batches still waiting for admission (`ChannelClosed` or
/// `ConnectionReplaced`). It is not
/// reported for batches already admitted: QUIC and TCP dial the lane again on
/// the next send, and the slots that lost records end through their stream
/// watchdogs. So a refusal is the failure to inject here.
struct LaneFailing {
    inner: Arc<QuicTransport>,
    /// The lane to refuse, or `u16::MAX` for none.
    failed: std::sync::atomic::AtomicU16,
    /// A gate over a channel with no receiver: every send is refused.
    dead: velo_ext::AdmissionGate<()>,
}

impl LaneFailing {
    fn new(inner: Arc<QuicTransport>) -> Self {
        let (tx, rx) = flume::bounded(1);
        drop(rx);
        Self {
            inner,
            failed: std::sync::atomic::AtomicU16::new(u16::MAX),
            dead: velo_ext::AdmissionGate::new(tx, tokio::runtime::Handle::current()),
        }
    }

    fn fail(&self, lane: u16) {
        self.failed
            .store(lane, std::sync::atomic::Ordering::Release);
    }
}

impl velo_ext::Transport for LaneFailing {
    fn key(&self) -> velo_ext::TransportKey {
        self.inner.key()
    }

    fn address(&self) -> velo_ext::WorkerAddress {
        self.inner.address()
    }

    fn register(&self, peer_info: velo_ext::PeerInfo) -> Result<(), velo_ext::TransportError> {
        self.inner.register(peer_info)
    }

    fn send_message(
        &self,
        instance_id: velo_ext::InstanceId,
        header: bytes::Bytes,
        payload: bytes::Bytes,
        message_type: velo_ext::MessageType,
        on_error: Arc<dyn velo_ext::TransportErrorHandler>,
    ) -> velo_ext::SendOutcome {
        self.send_message_on_lane(instance_id, 0, header, payload, message_type, on_error)
    }

    fn lanes(&self, target: velo_ext::InstanceId) -> std::num::NonZeroU16 {
        self.inner.lanes(target)
    }

    fn send_message_on_lane(
        &self,
        instance_id: velo_ext::InstanceId,
        lane: u16,
        header: bytes::Bytes,
        payload: bytes::Bytes,
        message_type: velo_ext::MessageType,
        on_error: Arc<dyn velo_ext::TransportErrorHandler>,
    ) -> velo_ext::SendOutcome {
        if lane == self.failed.load(std::sync::atomic::Ordering::Acquire) {
            return self.dead.send(());
        }
        self.inner
            .send_message_on_lane(instance_id, lane, header, payload, message_type, on_error)
    }

    fn max_message_size(&self, target: velo_ext::InstanceId) -> Option<usize> {
        self.inner.max_message_size(target)
    }

    fn start(
        &self,
        instance_id: velo_ext::InstanceId,
        channels: velo_ext::TransportAdapter,
        rt: tokio::runtime::Handle,
    ) -> futures::future::BoxFuture<'_, anyhow::Result<()>> {
        self.inner.start(instance_id, channels, rt)
    }

    fn shutdown(&self) {
        self.inner.shutdown();
    }

    fn closed(&self) -> futures::future::BoxFuture<'_, ()> {
        self.inner.closed()
    }

    fn set_observability(&self, observability: Arc<dyn velo_ext::TransportObservability>) {
        self.inner.set_observability(observability);
    }

    fn begin_drain(&self) {
        self.inner.begin_drain();
    }

    fn check_health(
        &self,
        instance_id: velo_ext::InstanceId,
        timeout: Duration,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), velo_ext::HealthCheckError>> + Send + '_>,
    > {
        self.inner.check_health(instance_id, timeout)
    }
}

/// The heartbeat window of the stream on the refusing lane, short so its
/// consumer's watchdog fires within the test's bound.
const FAILED_HEARTBEAT: Duration = Duration::from_secs(1);

/// A lane whose transport refuses admission fails the streams on that lane
/// and no others.
///
/// Each lane has its own batcher and its own epoch, so a refused batch kills
/// only the slots of its (peer, lane) on the producer. This drives the mux over
/// real QUIC lanes and injects the failure at the transport's admission, on
/// the producer's lane 2 only; it does not close a QUIC connection, which the
/// transport redials on the next send without refusing admission.
///
/// The consumer of the failed stream sees `SenderDropped`, but not from the
/// epoch death: that happens on the producer, and the refusing lane carries
/// nothing more to the consumer. Its stream watchdog ends the stream once
/// `DETECTION_MULTIPLIER` heartbeat windows pass with no arrival, so the
/// `Dropped` comes at least a window after the failure. Its anchor uses a short
/// window so that wait fits the test's bound.
#[tokio::test(flavor = "multi_thread")]
async fn a_lane_that_refuses_admission_fails_only_its_own_streams() {
    let consumer = Node::new(4).await;
    let quic = Arc::new(
        QuicTransportBuilder::new()
            .bind_addr("127.0.0.1:0".parse().expect("loopback"))
            .lanes(4)
            .build()
            .expect("quic transport"),
    );
    let failing = Arc::new(LaneFailing::new(Arc::clone(&quic)));
    let velo = Velo::builder()
        .add_transport(Arc::clone(&failing) as Arc<dyn crate::Transport>)
        .stream_bind_addr(std::net::Ipv4Addr::LOCALHOST.into())
        .build()
        .await
        .expect("build velo");
    let producer = Node { velo, quic };
    connect(&consumer, &producer).await;
    let peer = producer.worker();
    let consumer_mux = consumer.mux();

    const FAILED: u16 = 2;
    let mut streams = Vec::new();
    for lane in 0..4 {
        let anchor = if lane == FAILED {
            consumer
                .velo
                .anchor_manager()
                .create_anchor_with_config::<u32>(crate::streaming::AnchorConfig {
                    heartbeat_interval: Some(FAILED_HEARTBEAT),
                    ..Default::default()
                })
        } else {
            consumer.velo.create_anchor::<u32>()
        };
        let sender = producer
            .velo
            .attach_anchor_keyed::<u32>(transfer(anchor.handle()), key_for(lane, 4))
            .await
            .expect("remote attach");
        sender.send(u32::MAX).await.expect("first send");
        streams.push((sender, drain_first(anchor).await));
    }
    assert_eq!(
        live_per_lane(&consumer_mux, peer, 4),
        [1, 1, 1, 1],
        "one stream per lane, or a failure on lane 2 proves nothing about the others"
    );

    failing.fail(FAILED);
    let failed_at = tokio::time::Instant::now();
    let (failed_sender, mut failed_anchor) = streams.remove(usize::from(FAILED));
    tokio::time::timeout(BOUND, async {
        let mut n = 0u32;
        while failed_sender.send(n).await.is_ok() {
            n += 1;
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the stream on the refusing lane must fail");

    let end = tokio::time::timeout(BOUND, async {
        loop {
            match failed_anchor.next().await {
                Some(Ok(StreamFrame::Item(_))) => continue,
                other => return other,
            }
        }
    })
    .await
    .expect("the consumer on the refusing lane must see its stream end");
    assert!(
        matches!(end, Some(Err(crate::streaming::StreamError::SenderDropped))),
        "the consumer on the refusing lane must see SenderDropped, got {end:?}"
    );
    assert!(
        failed_at.elapsed() >= FAILED_HEARTBEAT,
        "the Dropped must come from the stream watchdog, a window or more after \
         the failure, not from the lane: {:?}",
        failed_at.elapsed()
    );

    let finished = futures::future::join_all(
        streams
            .into_iter()
            .map(|(sender, anchor)| async move { stream_through(sender, anchor).await }),
    );
    tokio::time::timeout(BOUND, finished)
        .await
        .expect("the streams on the other lanes finish in order");
}

/// A pre-bind that races shutdown fails, and is counted as a failed pre-bind;
/// it does not panic.
///
/// `prebind_anchor` is a public sync call an application can make on any
/// thread. Mux shutdown clears the parked drain signals, so one can vanish
/// between the pre-bind's bind and its take. The bind hook plays that
/// shutdown.
#[tokio::test(flavor = "multi_thread")]
async fn a_prebind_racing_shutdown_returns_none() {
    use crate::observability::test_helpers::MetricSnapshot;

    let registry = prometheus::Registry::new();
    let metrics = Arc::new(crate::observability::VeloMetrics::register(&registry).unwrap());
    let velo = Velo::builder()
        .add_transport(Arc::new(
            QuicTransportBuilder::new()
                .bind_addr("127.0.0.1:0".parse().expect("loopback"))
                .build()
                .expect("quic transport"),
        ) as Arc<dyn crate::Transport>)
        .stream_bind_addr(std::net::Ipv4Addr::LOCALHOST.into())
        .metrics(metrics)
        .build()
        .await
        .expect("build velo");
    let mux = velo
        .anchor_manager()
        .mux_handle()
        .and_then(|mux| mux.upgrade())
        .expect("velo installs the mux by default");
    let core = Arc::downgrade(&mux.core);
    assert!(
        mux.core
            .bind_hook
            .set(Box::new(move |_| {
                if let Some(core) = core.upgrade() {
                    core.drains.clear();
                }
            }))
            .is_ok()
    );
    let anchor = velo.create_anchor::<u32>();
    assert!(velo.prebind_anchor(anchor.handle()).is_none());
    assert_eq!(
        MetricSnapshot::from_registry(&registry).counter(
            "velo_streaming_anchor_operations_total",
            &[("operation", "prebind"), ("outcome", "error")],
        ),
        1.0
    );
}
