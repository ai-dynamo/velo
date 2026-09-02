// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Transport-level tests: two `MessengerMuxTransport`s over a real in-process
//! messenger pair.
//!
//! These drive `bind` / `connect` end to end, so they exercise the egress
//! batcher, the `_stream_batch` handler and the credit loop together — the
//! interlock, rather than either half. The full two-node matrix (fairness,
//! reconnect, slow-consumer memory bounds) belongs to the integration stage that
//! follows; what is pinned here is that the pieces compose.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use velo_ext::WorkerId;

use super::*;
use crate::observability::test_helpers::MetricSnapshot;
use crate::streaming::sender::{cached_dropped, cached_finalized};
use crate::streaming::{AnchorManagerBuilder, StreamAnchorHandle, StreamFrame};
use crate::transports::tcp::TcpTransportBuilder;

const RECV_TIMEOUT: Duration = Duration::from_secs(10);

fn test_config() -> MuxConfig {
    MuxConfig {
        // Fast enough that a parked sender resumes inside a test's patience.
        credit_sweep_interval: Duration::from_millis(1),
        ..MuxConfig::default()
    }
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

async fn messenger_pair() -> (Arc<Messenger>, Arc<Messenger>) {
    let a = Messenger::builder()
        .add_transport(tcp_transport())
        .build()
        .await
        .expect("messenger a");
    let b = Messenger::builder()
        .add_transport(tcp_transport())
        .build()
        .await
        .expect("messenger b");
    a.register_peer(b.peer_info()).expect("register b on a");
    b.register_peer(a.peer_info()).expect("register a on b");
    tokio::time::sleep(Duration::from_millis(200)).await;
    (a, b)
}

/// Two mux transports, each on its own messenger, plus a shared metrics
/// registry so slot bookkeeping is observable from either side.
struct Pair {
    consumer: Arc<MessengerMuxTransport>,
    producer: Arc<MessengerMuxTransport>,
    consumer_worker: WorkerId,
    registry: prometheus::Registry,
    _messengers: (Arc<Messenger>, Arc<Messenger>),
}

async fn mux_pair(config: MuxConfig) -> Pair {
    let (m_consumer, m_producer) = messenger_pair().await;
    let registry = prometheus::Registry::new();
    let metrics = Arc::new(VeloMetrics::register(&registry).expect("register metrics"));
    let consumer = MessengerMuxTransport::new(
        Arc::clone(&m_consumer),
        config.clone(),
        Some(Arc::clone(&metrics)),
    )
    .expect("consumer mux");
    let producer =
        MessengerMuxTransport::new(Arc::clone(&m_producer), config, Some(Arc::clone(&metrics)))
            .expect("producer mux");
    let consumer_worker = m_consumer.instance_id().worker_id();
    Pair {
        consumer,
        producer,
        consumer_worker,
        registry,
        _messengers: (m_consumer, m_producer),
    }
}

impl Pair {
    fn snapshot(&self) -> MetricSnapshot {
        MetricSnapshot::from_registry(&self.registry)
    }

    fn live_slots(&self) -> f64 {
        self.snapshot().gauge("velo_streaming_mux_live_slots", &[])
    }

    /// The applier's `try_send` never failed on space credit had reserved.
    ///
    /// The `C + 1` buffer against `C` data credits plus one terminal is what
    /// makes the ingress lane nonblocking, and this counter is the only way a
    /// break in it is visible from outside. Every test that pushes on the credit
    /// loop checks it, because a stall would otherwise show up as nothing at all
    /// — the records still arrive, they just came through a path that was
    /// supposed to be unreachable.
    fn assert_no_reader_stall(&self) {
        assert_eq!(
            self.snapshot()
                .counter("velo_streaming_mux_reader_stall_total", &[]),
            0.0,
            "the credit invariant broke: the applier hit a full slot buffer"
        );
    }
}

fn item(n: u32) -> Vec<u8> {
    rmp_serde::to_vec(&StreamFrame::Item(n)).expect("encode item")
}

async fn recv(rx: &flume::Receiver<Vec<u8>>) -> Vec<u8> {
    tokio::time::timeout(RECV_TIMEOUT, rx.recv_async())
        .await
        .expect("timed out waiting for a frame")
        .expect("frame channel closed")
}

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
// Identity
// ---------------------------------------------------------------------------

/// The key is a wire constant, so its *value* is pinned here rather than only
/// its uses.
///
/// Everything else in the tree reads the constant, which is what keeps the
/// copies from drifting — and is exactly why nothing else would notice it being
/// renamed. A rename is a silent interop break: negotiation matches on this
/// string, so a peer built either side of it simply never selects the mux and
/// falls back to the legacy path, with no error anywhere to say why.
#[test]
fn the_negotiated_key_is_the_string_that_shipped() {
    assert_eq!(MESSENGER_MUX_KEY, "messenger-mux-v1");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_transport_answers_to_the_negotiated_key_and_advertises_no_endpoint() {
    let pair = mux_pair(test_config()).await;
    assert_eq!(pair.consumer.key().as_str(), MESSENGER_MUX_KEY);
    assert!(
        pair.consumer
            .address()
            .available_transports()
            .expect("decodable address")
            .is_empty(),
        "the mux piggybacks on the messenger, so it has no listener to advertise \
         and the Velo builder has nothing to merge into the local PeerInfo"
    );
    // The trait default is the right `register`: the messenger already tracks
    // the peer, so there is no endpoint cache to fill.
    let peer = velo_ext::PeerInfo::new(
        velo_ext::InstanceId::new_v4(),
        velo_ext::WorkerAddress::empty(),
    );
    assert!(pair.consumer.register(&peer).is_ok());
}

// ---------------------------------------------------------------------------
// Round trips
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_stream_round_trips_and_ends_on_its_terminal() {
    let pair = mux_pair(test_config()).await;

    let rx = pair.consumer.bind(1, 1).await.expect("bind");
    let tx = pair
        .producer
        .connect(pair.consumer_worker, 1, 1)
        .await
        .expect("connect");

    for n in 0..8u32 {
        tx.send_async(item(n)).await.expect("send item");
    }
    tx.send_async(cached_finalized().clone())
        .await
        .expect("send terminal");

    for n in 0..8u32 {
        assert_eq!(recv(&rx).await, item(n), "frame {n} out of order");
    }
    assert_eq!(recv(&rx).await, *cached_finalized());
    eventually(|| rx.is_disconnected()).await;
    assert_eq!(
        pair.live_slots(),
        0.0,
        "both sides free the slot once the terminal has landed"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn credit_returns_let_a_producer_outrun_its_window() {
    // A window far smaller than the traffic, so the sender parks repeatedly and
    // only the credit loop can un-park it.
    let pair = mux_pair(MuxConfig {
        initial_credit: 4,
        ..test_config()
    })
    .await;

    let rx = pair.consumer.bind(2, 2).await.expect("bind");
    let tx = pair
        .producer
        .connect(pair.consumer_worker, 2, 2)
        .await
        .expect("connect");

    const FRAMES: u32 = 200;
    let producer = tokio::spawn(async move {
        for n in 0..FRAMES {
            tx.send_async(item(n)).await.expect("send item");
        }
        tx.send_async(cached_finalized().clone())
            .await
            .expect("send terminal");
    });

    for n in 0..FRAMES {
        assert_eq!(recv(&rx).await, item(n), "frame {n} out of order");
    }
    assert_eq!(recv(&rx).await, *cached_finalized());
    producer.await.expect("producer task");
    pair.assert_no_reader_stall();
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_sessions_on_one_anchor_stay_separate() {
    let pair = mux_pair(test_config()).await;

    let rx_a = pair.consumer.bind(5, 1).await.expect("bind a");
    let rx_b = pair.consumer.bind(5, 2).await.expect("bind b");
    let tx_a = pair
        .producer
        .connect(pair.consumer_worker, 5, 1)
        .await
        .expect("connect a");
    let tx_b = pair
        .producer
        .connect(pair.consumer_worker, 5, 2)
        .await
        .expect("connect b");

    for n in 0..16u32 {
        tx_a.send_async(item(n)).await.expect("send a");
        tx_b.send_async(item(1000 + n)).await.expect("send b");
    }

    for n in 0..16u32 {
        assert_eq!(recv(&rx_a).await, item(n));
        assert_eq!(recv(&rx_b).await, item(1000 + n));
    }
    pair.assert_no_reader_stall();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_session_nobody_bound_is_rejected_without_disturbing_a_live_one() {
    let pair = mux_pair(test_config()).await;

    let rx = pair.consumer.bind(9, 1).await.expect("bind");
    let live = pair
        .producer
        .connect(pair.consumer_worker, 9, 1)
        .await
        .expect("connect live");
    let orphan = pair
        .producer
        .connect(pair.consumer_worker, 9, 999)
        .await
        .expect("connect orphan");

    // The reverse race: the receiver replies `CloseSlot{UnknownSlot}` and the
    // sender's channel closes. The peer itself is never failed.
    eventually(|| orphan.is_disconnected()).await;

    live.send_async(item(1)).await.expect("live send");
    assert_eq!(recv(&rx).await, item(1));
    assert!(!live.is_disconnected());
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_a_producer_without_a_terminal_injects_dropped() {
    let pair = mux_pair(test_config()).await;

    let rx = pair.consumer.bind(11, 1).await.expect("bind");
    let tx = pair
        .producer
        .connect(pair.consumer_worker, 11, 1)
        .await
        .expect("connect");

    tx.send_async(item(1)).await.expect("send item");
    assert_eq!(recv(&rx).await, item(1));
    drop(tx);

    assert_eq!(
        recv(&rx).await,
        *cached_dropped(),
        "a stream that ends without a terminal is `Dropped`, never `TransportError`"
    );
    eventually(|| pair.live_slots() == 0.0).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn live_slots_returns_to_zero_when_the_producers_go_away() {
    let pair = mux_pair(test_config()).await;

    let mut receivers = Vec::new();
    let mut senders = Vec::new();
    for session in 0..4u64 {
        receivers.push(pair.consumer.bind(20, session).await.expect("bind"));
        senders.push(
            pair.producer
                .connect(pair.consumer_worker, 20, session)
                .await
                .expect("connect"),
        );
    }
    for tx in &senders {
        tx.send_async(item(1)).await.expect("send");
    }
    for rx in &receivers {
        assert_eq!(recv(rx).await, item(1));
    }
    eventually(|| pair.live_slots() == 8.0).await;

    drop(senders);
    for rx in &receivers {
        assert_eq!(recv(rx).await, *cached_dropped());
    }
    eventually(|| pair.live_slots() == 0.0).await;
    pair.assert_no_reader_stall();
}

/// Dropping the transports themselves has to reach zero too, and promptly.
///
/// The failure this guards against is a strong reference held across `bind`'s
/// 60-second accept window: the transport would keep every slot, batcher task
/// and ingress entry alive for a minute after its last owner let go, and the
/// gauge would come back only when the timers did.
#[tokio::test(flavor = "multi_thread")]
async fn dropping_the_transports_tears_every_slot_down_promptly() {
    let pair = mux_pair(test_config()).await;

    let mut receivers = Vec::new();
    let mut senders = Vec::new();
    for session in 0..4u64 {
        receivers.push(pair.consumer.bind(30, session).await.expect("bind"));
        senders.push(
            pair.producer
                .connect(pair.consumer_worker, 30, session)
                .await
                .expect("connect"),
        );
    }
    // One bind with no `connect` behind it, so an accept window really is open.
    let _pending = pair.consumer.bind(30, 99).await.expect("pending bind");
    for tx in &senders {
        tx.send_async(item(1)).await.expect("send");
    }
    for rx in &receivers {
        assert_eq!(recv(rx).await, item(1));
    }
    eventually(|| pair.live_slots() == 8.0).await;

    // The producer channels and consumer receivers stay alive on purpose: the
    // teardown under test is the transport's, not the channels'.
    let registry = pair.registry.clone();
    drop(pair);

    for rx in &receivers {
        assert_eq!(
            recv(rx).await,
            *cached_dropped(),
            "a consumer must not wait out its heartbeat watchdog for a sender \
             that has already been dismantled"
        );
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if MetricSnapshot::from_registry(&registry).gauge("velo_streaming_mux_live_slots", &[])
            == 0.0
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("live_slots never returned to zero after the transports were dropped");
}

// ---------------------------------------------------------------------------
// End to end through the anchor layer
// ---------------------------------------------------------------------------

/// Fast remote sender, slow consumer, over the mux.
///
/// Ported from the deleted `VeloFrameTransport` ordering regression. The hazard
/// it caught was real and is not gone — the AM dispatcher still spawns a task
/// per inbound message by default — it has moved: the mux registers
/// `_stream_batch` with ordered per-sender dispatch and stamps a per-slot
/// `frame_seq`, so ordering is now a protocol obligation this test holds it to.
#[tokio::test(flavor = "multi_thread")]
async fn a_remote_stream_preserves_send_order_under_a_slow_consumer() {
    const FRAMES: u32 = 200;

    let (m_consumer, m_producer) = messenger_pair().await;
    let consumer_worker = m_consumer.instance_id().worker_id();
    let producer_worker = m_producer.instance_id().worker_id();

    let mux_consumer =
        MessengerMuxTransport::new(Arc::clone(&m_consumer), test_config(), None).expect("mux a");
    let mux_producer =
        MessengerMuxTransport::new(Arc::clone(&m_producer), test_config(), None).expect("mux b");

    let am_consumer = Arc::new(
        AnchorManagerBuilder::default()
            .worker_id(consumer_worker)
            .transport(Arc::clone(&mux_consumer) as Arc<dyn FrameTransport>)
            .build()
            .expect("consumer anchor manager"),
    );
    let am_producer = Arc::new(
        AnchorManagerBuilder::default()
            .worker_id(producer_worker)
            .transport(Arc::clone(&mux_producer) as Arc<dyn FrameTransport>)
            .build()
            .expect("producer anchor manager"),
    );
    // Installed, not merely wired in as the default transport. Negotiation is
    // what carries the credit window now, and only an installed mux has a
    // window to advertise or a `connect_negotiated` to open a slot through.
    am_consumer
        .install_mux(mux_consumer)
        .expect("install consumer mux");
    am_producer
        .install_mux(mux_producer)
        .expect("install producer mux");
    am_consumer
        .register_handlers(Arc::clone(&m_consumer))
        .expect("consumer handlers");
    am_producer
        .register_handlers(Arc::clone(&m_producer))
        .expect("producer handlers");

    let mut anchor = am_consumer.create_anchor::<u32>();
    let handle = StreamAnchorHandle::from_u128(anchor.handle().as_u128());

    let sender = am_producer
        .attach_stream_anchor::<u32>(handle)
        .await
        .expect("remote attach");

    let send_task = tokio::spawn(async move {
        for n in 0..FRAMES {
            sender.send(n).await.expect("send item");
        }
        sender.finalize().expect("finalize");
    });

    let collect = async {
        let mut items = Vec::with_capacity(FRAMES as usize);
        while let Some(frame) = anchor.next().await {
            match frame.expect("no stream error") {
                StreamFrame::Item(value) => {
                    items.push(value);
                    // Yield so inbound batches accumulate behind the consumer.
                    tokio::task::yield_now().await;
                }
                StreamFrame::Finalized => break,
                other => panic!("unexpected frame: {other:?}"),
            }
        }
        items
    };

    let items = tokio::time::timeout(Duration::from_secs(30), collect)
        .await
        .expect("timed out collecting items");
    send_task.await.expect("send task");

    assert_eq!(
        items,
        (0..FRAMES).collect::<Vec<_>>(),
        "frames out of order"
    );
}

// ---------------------------------------------------------------------------
// Flush fan-out
// ---------------------------------------------------------------------------

/// One `flush_batch()` reaches every peer this node has staged records for.
///
/// The batcher-level tests pin what a kick does to one batcher. This pins the
/// property the *public* call has to have, and the reason it takes no argument:
/// a producer holds `StreamSender`s and cannot know which batcher each one
/// feeds, so a flush that reached only some peers would be a call whose correct
/// use requires knowing something the API hides. Two consumers, a round of
/// sends spread across both, one flush — and exactly one batch to each.
#[tokio::test(flavor = "multi_thread")]
async fn one_flush_reaches_every_peer_batcher() {
    const PEERS: usize = 2;
    const SLOTS_PER_PEER: u64 = 3;

    let producer_messenger = Messenger::builder()
        .add_transport(tcp_transport())
        .build()
        .await
        .expect("producer messenger");
    let mut consumers = Vec::with_capacity(PEERS);
    for _ in 0..PEERS {
        let m = Messenger::builder()
            .add_transport(tcp_transport())
            .build()
            .await
            .expect("consumer messenger");
        producer_messenger
            .register_peer(m.peer_info())
            .expect("register consumer");
        m.register_peer(producer_messenger.peer_info())
            .expect("register producer");
        consumers.push(m);
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The producer gets its own registry, so the batch counter below is its
    // writes and nobody else's.
    let registry = prometheus::Registry::new();
    let metrics = Arc::new(VeloMetrics::register(&registry).expect("register metrics"));
    let config = MuxConfig {
        flush_policy: crate::streaming::FlushPolicy::Manual,
        ..test_config()
    };
    let producer = MessengerMuxTransport::new(
        Arc::clone(&producer_messenger),
        config.clone(),
        Some(Arc::clone(&metrics)),
    )
    .expect("producer mux");

    let mut receivers = Vec::new();
    let mut senders = Vec::new();
    let mut consumer_muxes = Vec::new();
    for (peer, messenger) in consumers.iter().enumerate() {
        let mux = MessengerMuxTransport::new(Arc::clone(messenger), config.clone(), None)
            .expect("consumer mux");
        let worker = messenger.instance_id().worker_id();
        for slot in 0..SLOTS_PER_PEER {
            let id = (peer as u64 + 1) * 100 + slot;
            receivers.push(mux.bind(id, id).await.expect("bind"));
            senders.push(producer.connect(worker, id, id).await.expect("connect"));
        }
        consumer_muxes.push(mux);
    }

    let sent_batches = || {
        MetricSnapshot::from_registry(&registry)
            .counter("velo_streaming_mux_batches_total", &[("direction", "sent")])
    };
    // Every `OpenSlot` was flushed eagerly and on its own, so the opens are
    // behind us and the count below starts from a known place.
    let after_opens = sent_batches();
    assert_eq!(
        after_opens,
        (PEERS as u64 * SLOTS_PER_PEER) as f64,
        "one eager batch per OpenSlot"
    );

    // One round: a record on every slot, spread across both peers.
    for (n, tx) in senders.iter().enumerate() {
        tx.send_async(item(n as u32)).await.expect("send item");
    }
    // The records are staged, not written — that is the policy under test.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        sent_batches(),
        after_opens,
        "manual holds the round until the application flushes it"
    );

    producer.flush_batches();

    for (n, rx) in receivers.iter().enumerate() {
        assert_eq!(
            recv(rx).await,
            item(n as u32),
            "slot {n} did not receive the record its peer's flush carried"
        );
    }
    assert_eq!(
        sent_batches(),
        after_opens + PEERS as f64,
        "one flush, one batch per peer — not one per slot and not only the first peer"
    );
}

// ---------------------------------------------------------------------------
// The doorbell's per-peer floor
// ---------------------------------------------------------------------------

/// The scheduling half of the floor, taken away from the runtime.
///
/// `mux_credit.rs` observes the rate over a real pair of nodes, which is the
/// claim that matters and also the slower, noisier way to find out that a
/// boundary case is wrong. These pin the boundary itself: what `admit` does on
/// each side of the floor, that a deferred peer is handed back exactly once,
/// and that the re-check inside `due` cannot spin.
mod drain_visit_floor {
    use super::super::DrainVisits;
    use std::time::Duration;
    use tokio::time::Instant;
    use velo_ext::WorkerId;

    const FLOOR: Duration = Duration::from_millis(2);

    fn peer(id: u64) -> WorkerId {
        WorkerId::from_u64(id)
    }

    #[test]
    fn a_first_wake_is_walked_immediately() {
        let mut visits = DrainVisits::new(FLOOR);
        let now = Instant::now();
        assert_eq!(
            visits.admit(peer(1), now),
            Some(peer(1)),
            "a peer with no visit behind it has nothing to wait for"
        );
        assert!(
            visits.next_due().is_none(),
            "an admitted wake is walked, not queued"
        );
    }

    #[test]
    fn a_wake_inside_the_floor_is_deferred_to_the_floor() {
        let mut visits = DrainVisits::new(FLOOR);
        let start = Instant::now();
        assert_eq!(visits.admit(peer(1), start), Some(peer(1)));

        assert_eq!(
            visits.admit(peer(1), start + FLOOR / 2),
            None,
            "the floor has not elapsed, so this wake must not walk"
        );
        assert_eq!(
            visits.next_due(),
            Some(start + FLOOR),
            "the deferred visit is due one floor after the last walk, not one floor from now — \
             the second form would let a stream of wakes push it out indefinitely"
        );
    }

    #[test]
    fn a_wake_past_the_floor_is_walked_immediately() {
        let mut visits = DrainVisits::new(FLOOR);
        let start = Instant::now();
        assert_eq!(visits.admit(peer(1), start), Some(peer(1)));
        assert_eq!(
            visits.admit(peer(1), start + FLOOR),
            Some(peer(1)),
            "exactly one floor later is late enough: the ceiling is one visit per floor"
        );
    }

    #[test]
    fn a_deferred_peer_comes_back_once_when_due() {
        let mut visits = DrainVisits::new(FLOOR);
        let start = Instant::now();
        assert_eq!(visits.admit(peer(1), start), Some(peer(1)));
        assert_eq!(visits.admit(peer(1), start + FLOOR / 2), None);

        assert!(
            visits.due(start + FLOOR / 2).is_empty(),
            "nothing is due before the floor elapses"
        );
        assert_eq!(
            visits.due(start + FLOOR),
            vec![peer(1)],
            "the deferred visit comes back exactly once"
        );
        assert!(
            visits.next_due().is_none(),
            "and is not left in the queue behind it"
        );
    }

    /// A wake landing while a walk is already queued coalesces into it.
    ///
    /// The queued walk is the authoritative next one, so `admit` refuses to
    /// walk a scheduled peer even once the floor has elapsed. Racing it instead
    /// is what leaves the queued entry behind as residue, and residue is what
    /// the ratchet below is made of.
    #[test]
    fn a_wake_while_a_walk_is_queued_coalesces_into_it() {
        let mut visits = DrainVisits::new(FLOOR);
        let start = Instant::now();
        assert_eq!(visits.admit(peer(1), start), Some(peer(1)));
        assert_eq!(visits.admit(peer(1), start + FLOOR / 2), None);

        assert_eq!(
            visits.admit(peer(1), start + FLOOR),
            None,
            "the queued walk answers this wake; walking it here would strand that entry"
        );
        assert_eq!(visits.queued(), 1, "and buys no second entry");
        assert_eq!(
            visits.due(start + FLOOR),
            vec![peer(1)],
            "the queued walk is what serves both wakes"
        );
        assert_eq!(visits.queued(), 0);
    }

    /// Two wakes inside one floor queue one walk, and leave nothing behind.
    ///
    /// This is the ratchet in miniature. The periodic tick calls `sweep_peer` on
    /// every peer, including one already sitting in the deferred queue, and that
    /// clears its wake; the consumer's next drain re-arms and posts a second
    /// wake inside the same floor. If that wake queues an entry of its own, the
    /// pair falls due together, one walk is taken and the other entry is put
    /// back — permanent residue, one entry per tick, with the sweep task's queue
    /// work growing with it.
    #[test]
    fn a_peer_wakened_twice_inside_the_floor_is_queued_once() {
        let mut visits = DrainVisits::new(FLOOR);
        let start = Instant::now();
        assert_eq!(visits.admit(peer(1), start), Some(peer(1)));
        assert_eq!(visits.admit(peer(1), start + FLOOR / 4), None);
        assert_eq!(visits.admit(peer(1), start + FLOOR / 2), None);
        assert_eq!(
            visits.queued(),
            1,
            "one peer, one queued walk — the second wake has nothing to add to it"
        );

        assert_eq!(
            visits.due(start + FLOOR),
            vec![peer(1)],
            "one walk serves both wakes"
        );
        assert_eq!(
            visits.queued(),
            0,
            "and the queue is empty behind it: nothing was put back"
        );
        assert_eq!(visits.next_due(), None);
    }

    /// The ratchet, run forward.
    ///
    /// Each round is one periodic tick clearing an armed-deferred peer's flag
    /// and the drain that follows it, then the walk falling due. Before the
    /// bound, every round left one more entry in the queue and it never came
    /// back down — CPU in the sweep task before memory anywhere.
    #[test]
    fn rounds_of_tick_cleared_wakes_do_not_grow_the_queue() {
        const ROUNDS: usize = 64;

        let mut visits = DrainVisits::new(FLOOR);
        let mut now = Instant::now();
        assert_eq!(visits.admit(peer(1), now), Some(peer(1)));

        for round in 0..ROUNDS {
            // The drain that re-armed after the walk, and the one that re-armed
            // after a tick cleared the flag underneath the queued walk.
            assert_eq!(visits.admit(peer(1), now + FLOOR / 4), None);
            assert_eq!(visits.admit(peer(1), now + FLOOR / 2), None);
            assert!(
                visits.queued() <= 1,
                "round {round}: the queue holds {} entries for one peer",
                visits.queued()
            );

            now += FLOOR;
            assert_eq!(visits.due(now), vec![peer(1)], "round {round}");
        }
        assert_eq!(
            visits.queued(),
            0,
            "after the last walk the queue is empty, not {ROUNDS} entries deep"
        );
    }

    /// The prune must not orphan a queued walk.
    ///
    /// `forget_stale` drops per-peer state whose last walk is older than the
    /// floor, which is exactly the state that says "this peer already has a walk
    /// queued". Dropping it lets the next wake walk immediately and queue a
    /// second entry behind the one still sitting there — the same residue by
    /// another route.
    #[test]
    fn a_queued_walk_survives_the_periodic_prune() {
        let mut visits = DrainVisits::new(FLOOR);
        let start = Instant::now();
        assert_eq!(visits.admit(peer(1), start), Some(peer(1)));
        assert_eq!(visits.admit(peer(1), start + FLOOR / 2), None);

        // The tick's prune runs while the deferred walk is still queued, at a
        // point where the peer's last walk is already a floor old.
        visits.forget_stale(start + FLOOR);

        assert_eq!(
            visits.admit(peer(1), start + FLOOR),
            None,
            "the queued walk is still the authoritative next one"
        );
        assert_eq!(
            visits.queued(),
            1,
            "the prune must not have let a second entry in behind the queued one"
        );
        assert_eq!(visits.due(start + FLOOR), vec![peer(1)]);
        assert_eq!(visits.queued(), 0);
    }

    /// An absurd floor is clamped rather than left to overflow the deadline.
    ///
    /// `drain_visit_floor` is an operator-set `Duration` and the deferral
    /// deadline is `last + floor`, which panics the sweep task on overflow.
    /// A floor of hours already means "the doorbell is off"; every larger value
    /// means the same thing, so clamping loses nothing and keeps the arithmetic
    /// total.
    #[test]
    fn an_absurd_floor_is_clamped_rather_than_overflowing_the_deadline() {
        let mut visits = DrainVisits::new(Duration::MAX);
        let start = Instant::now();
        assert_eq!(visits.admit(peer(1), start), Some(peer(1)));
        assert_eq!(
            visits.admit(peer(1), start),
            None,
            "still inside the floor, so still deferred — and computing when to \
             is what would have panicked"
        );
        assert_eq!(visits.queued(), 1);
    }

    #[test]
    fn peers_are_independent() {
        let mut visits = DrainVisits::new(FLOOR);
        let start = Instant::now();
        assert_eq!(visits.admit(peer(1), start), Some(peer(1)));
        assert_eq!(
            visits.admit(peer(2), start),
            Some(peer(2)),
            "one peer's floor must not hold back another's — the walk is per peer"
        );
    }

    /// The map holds the peers currently draining, not every peer ever seen.
    #[test]
    fn stale_stamps_are_forgotten_and_change_no_decision() {
        let mut visits = DrainVisits::new(FLOOR);
        let start = Instant::now();
        assert_eq!(visits.admit(peer(1), start), Some(peer(1)));
        assert_eq!(visits.admit(peer(2), start + FLOOR), Some(peer(2)));

        visits.forget_stale(start + FLOOR);
        assert_eq!(
            visits.admit(peer(1), start + FLOOR),
            Some(peer(1)),
            "the forgotten stamp was already past the floor, so the answer is unchanged"
        );
        assert_eq!(
            visits.admit(peer(2), start + FLOOR),
            None,
            "a stamp still inside the floor is kept"
        );
    }
}
