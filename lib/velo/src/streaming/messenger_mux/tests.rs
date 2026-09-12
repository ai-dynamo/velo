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

use super::peer_batcher::test_hooks::TestHooks;
use super::test_support::stalled_producer;
use super::*;
use crate::observability::test_helpers::MetricSnapshot;
use crate::streaming::sender::{cached_dropped, cached_finalized};
use crate::streaming::{
    AnchorConfig, AnchorManager, AnchorManagerBuilder, StreamAnchorHandle, StreamFrame,
};
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
    producer_worker: WorkerId,
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
    let producer_worker = m_producer.instance_id().worker_id();
    Pair {
        consumer,
        producer,
        consumer_worker,
        producer_worker,
        registry,
        _messengers: (m_consumer, m_producer),
    }
}

impl Pair {
    /// `bind` on the consumer mux, with the drain signal the attach path would
    /// hand the pump it spawns.
    async fn bind(&self, anchor_id: u64, session_id: u64) -> BoundSlot {
        bind_slot(&self.consumer, anchor_id, session_id).await
    }

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

/// One bound slot's consumer side, standing in for `reader_pump`.
///
/// The attach path hands the pump both the receiver `bind` returned and the
/// drain signal the mux parked for that pair, and the pump counts every record
/// it takes out on that signal. Credit is returned against that count, so a
/// test taking records straight from the receiver would look to the ledger like
/// a stream whose pump had died — and the first thing that reaches is
/// `credit_returns_let_a_producer_outrun_its_window`, whose producer would park
/// after four records and never be woken.
struct BoundSlot {
    rx: flume::Receiver<Vec<u8>>,
    drain: Arc<ingress::DrainSignal>,
}

impl BoundSlot {
    /// Take one record, counting it the way `reader_pump` does.
    async fn recv(&self) -> Vec<u8> {
        let frame = recv(&self.rx).await;
        self.drain.drained();
        frame
    }

    fn is_disconnected(&self) -> bool {
        self.rx.is_disconnected()
    }
}

/// `bind` on `mux`, with the drain signal the attach path would hand the pump.
async fn bind_slot(mux: &MessengerMuxTransport, anchor_id: u64, session_id: u64) -> BoundSlot {
    let rx = mux.bind(anchor_id, session_id).await.expect("bind");
    let drain = mux
        .take_drain_signal(anchor_id, session_id)
        .expect("bind parks a drain signal for the attach path to take");
    BoundSlot { rx, drain }
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
// Configuration
// ---------------------------------------------------------------------------

/// A zero sweep interval is refused where the mux is built, not on its first
/// tick.
///
/// The sweep runs on a `tokio::time::interval`, which panics on a zero
/// period. That panic landed inside the spawned sweep task, so `new` returned
/// `Ok` and only the sweep died — and with it the periodic credit backstop,
/// batcher eviction, and the drain doorbell, whose only reader is that task.
/// Zero is a plausible thing to write here because the two neighbouring
/// knobs, `drain_visit_floor` and `reply_linger`, accept `Duration::ZERO` as
/// "off".
///
/// The control below builds the same messenger with a positive interval, so
/// the refusal is about the interval and comes before the mux takes the
/// messenger's one `_stream_batch` handler slot.
#[tokio::test(flavor = "multi_thread")]
async fn a_zero_sweep_interval_is_refused_when_the_mux_is_built() {
    let messenger = Messenger::builder()
        .add_transport(tcp_transport())
        .build()
        .await
        .expect("messenger");

    let error = MessengerMuxTransport::new(
        Arc::clone(&messenger),
        MuxConfig {
            credit_sweep_interval: Duration::ZERO,
            ..MuxConfig::default()
        },
        None,
    )
    .err()
    .expect("a zero sweep interval cannot tick, so the build must say so");
    assert!(
        error.to_string().contains("credit_sweep_interval"),
        "the refusal names the field: {error}"
    );

    MessengerMuxTransport::new(
        messenger,
        MuxConfig {
            credit_sweep_interval: Duration::from_millis(1),
            ..MuxConfig::default()
        },
        None,
    )
    .expect("the same messenger takes a mux with a positive interval");
}

// ---------------------------------------------------------------------------
// Round trips
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_stream_round_trips_and_ends_on_its_terminal() {
    let pair = mux_pair(test_config()).await;

    let rx = pair.bind(1, 1).await;
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
        assert_eq!(rx.recv().await, item(n), "frame {n} out of order");
    }
    assert_eq!(rx.recv().await, *cached_finalized());
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

    let rx = pair.bind(2, 2).await;
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
        assert_eq!(rx.recv().await, item(n), "frame {n} out of order");
    }
    assert_eq!(rx.recv().await, *cached_finalized());
    producer.await.expect("producer task");
    pair.assert_no_reader_stall();
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_sessions_on_one_anchor_stay_separate() {
    let pair = mux_pair(test_config()).await;

    let rx_a = pair.bind(5, 1).await;
    let rx_b = pair.bind(5, 2).await;
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
        assert_eq!(rx_a.recv().await, item(n));
        assert_eq!(rx_b.recv().await, item(1000 + n));
    }
    pair.assert_no_reader_stall();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_session_nobody_bound_is_rejected_without_disturbing_a_live_one() {
    let pair = mux_pair(test_config()).await;

    let rx = pair.bind(9, 1).await;
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
    assert_eq!(rx.recv().await, item(1));
    assert!(!live.is_disconnected());
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_a_producer_without_a_terminal_injects_dropped() {
    let pair = mux_pair(test_config()).await;

    let rx = pair.bind(11, 1).await;
    let tx = pair
        .producer
        .connect(pair.consumer_worker, 11, 1)
        .await
        .expect("connect");

    tx.send_async(item(1)).await.expect("send item");
    assert_eq!(rx.recv().await, item(1));
    drop(tx);

    assert_eq!(
        rx.recv().await,
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
        receivers.push(pair.bind(20, session).await);
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
        assert_eq!(rx.recv().await, item(1));
    }
    eventually(|| pair.live_slots() == 8.0).await;

    drop(senders);
    for rx in &receivers {
        assert_eq!(rx.recv().await, *cached_dropped());
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
        receivers.push(pair.bind(30, session).await);
        senders.push(
            pair.producer
                .connect(pair.consumer_worker, 30, session)
                .await
                .expect("connect"),
        );
    }
    // One bind with no `connect` behind it, so an accept window really is open.
    let _pending = pair.bind(30, 99).await;
    for tx in &senders {
        tx.send_async(item(1)).await.expect("send");
    }
    for rx in &receivers {
        assert_eq!(rx.recv().await, item(1));
    }
    eventually(|| pair.live_slots() == 8.0).await;

    // The producer channels and consumer receivers stay alive on purpose: the
    // teardown under test is the transport's, not the channels'.
    let registry = pair.registry.clone();
    drop(pair);

    for rx in &receivers {
        assert_eq!(
            rx.recv().await,
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
            receivers.push(bind_slot(&mux, id, id).await);
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
            rx.recv().await,
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
// Ordered-lane observability
// ---------------------------------------------------------------------------

/// The ordered lane is observed once per *batch*, never once per record.
///
/// This is what makes `velo_messenger_ordered_lane_wait_seconds` readable as a
/// queue measurement rather than a throughput one: a `_stream_batch` carrying N
/// records costs exactly one lane observation, so the wait it reports is the
/// wait of the batch, and the records that batch carried are counted separately
/// by `velo_streaming_mux_records_per_batch`. Were the dispatcher ever to
/// observe per record the two series would move together and the wait would
/// read N times too heavy — the same series, silently measuring nothing.
///
/// The consumer messenger gets a registry of its own on purpose: credit rides
/// home to the producer on `_stream_batch` too, so one registry shared by both
/// messengers would count that return traffic as ingress here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_ordered_lane_is_observed_once_per_batch_not_once_per_record() {
    const RECORDS: u32 = 8;

    let consumer_reg = prometheus::Registry::new();
    let consumer_metrics =
        Arc::new(VeloMetrics::register(&consumer_reg).expect("register metrics"));

    let producer_messenger = Messenger::builder()
        .add_transport(tcp_transport())
        .build()
        .await
        .expect("producer messenger");
    let consumer_messenger = Messenger::builder()
        .add_transport(tcp_transport())
        .metrics(Arc::clone(&consumer_metrics))
        .build()
        .await
        .expect("consumer messenger");
    producer_messenger
        .register_peer(consumer_messenger.peer_info())
        .expect("register consumer");
    consumer_messenger
        .register_peer(producer_messenger.peer_info())
        .expect("register producer");
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Manual flush so "one batch" is something the test states rather than
    // something it hopes the flush timer happened to produce.
    let config = MuxConfig {
        flush_policy: crate::streaming::FlushPolicy::Manual,
        ..test_config()
    };
    let consumer = MessengerMuxTransport::new(
        Arc::clone(&consumer_messenger),
        config.clone(),
        Some(Arc::clone(&consumer_metrics)),
    )
    .expect("consumer mux");
    let producer = MessengerMuxTransport::new(Arc::clone(&producer_messenger), config, None)
        .expect("producer mux");

    let rx = consumer.bind(1, 1).await.expect("bind");
    let tx = producer
        .connect(consumer_messenger.instance_id().worker_id(), 1, 1)
        .await
        .expect("connect");

    let snapshot = || MetricSnapshot::from_registry(&consumer_reg);
    let waits = || {
        snapshot().histogram_count(
            "velo_messenger_ordered_lane_wait_seconds",
            &[("handler", STREAM_BATCH_HANDLER)],
        )
    };
    let records = || {
        snapshot().histogram_sum(
            "velo_streaming_mux_records_per_batch",
            &[("direction", "received")],
        )
    };

    // `connect` flushes its OpenSlot eagerly, so one batch has already crossed.
    // Wait on that batch's *record* count rather than its lane observation: the
    // receive path stamps the records inside the handler, after the lane wait
    // for the same batch, so `records() >= 1` is the only one of the two that
    // proves no batch is still in flight when the baseline is taken.
    eventually(|| records() >= 1.0).await;
    let waits_before = waits();
    let records_before = records();
    assert_eq!(
        waits_before, 1,
        "the OpenSlot batch must have been observed on the ordered lane — it is \
         not, while the `_` prefix of `_stream_batch` keeps it off the allowlist"
    );

    for n in 0..RECORDS {
        tx.send_async(item(n)).await.expect("send item");
    }
    producer.flush_batches();
    for n in 0..RECORDS {
        assert_eq!(recv(&rx).await, item(n), "record {n} out of order");
    }

    assert_eq!(
        waits() - waits_before,
        1,
        "one flush is one batch is one lane observation"
    );
    assert_eq!(
        records() - records_before,
        f64::from(RECORDS),
        "the batch that cost one lane observation carried every record"
    );
    assert!(
        snapshot().gauge(
            "velo_messenger_ordered_lanes",
            &[("handler", STREAM_BATCH_HANDLER)]
        ) >= 1.0,
        "the sending peer's lane must be live while its stream is"
    );
}

// ---------------------------------------------------------------------------
// Opening a slot into a congested peer
// ---------------------------------------------------------------------------

/// How long a `connect` may take before it counts as having waited.
///
/// Far longer than an open costs when it does not wait, and far shorter than
/// the peer here ever un-parks — which is never. Nothing in between is a
/// judgement call this test has to make.
const ACK_PATIENCE: Duration = Duration::from_secs(2);

/// A stream may open while the peer's send channel is full.
///
/// The measured shape: at the published concurrency every mocker process sits
/// with its per-connection channel full, and a worker cannot start generating
/// until its stream is open. Today the ack waits for the `OpenSlot`'s own
/// admission, so opening a stream costs a place in exactly the queue that is
/// full — the wait is not the network, it is the batch already in front of it.
/// The `OpenSlot` still goes to the transport before the ack; what stops is
/// waiting for the transport to take it.
#[tokio::test(flavor = "multi_thread")]
async fn connect_returns_while_the_data_channel_is_full() {
    let stalled = stalled_producer(MuxConfig {
        async_open_ack: true,
        ..test_config()
    })
    .await;

    // The first open takes the gate's one free place, so it is admitted and
    // says nothing about the property. It is how the channel gets full.
    //
    // Its inlet is held for the rest of the test on purpose: dropping it is the
    // producer going away, which queues a `CloseSlot{PeerGone}` and parks the
    // batcher on *that* write instead — a stall that has nothing to do with the
    // open being measured.
    let _first_inlet = stalled
        .connect(1, RECV_TIMEOUT)
        .await
        .expect("the first open is admitted immediately");
    eventually(|| stalled.wire.is_full()).await;

    // The second one's `OpenSlot` parks behind it.
    assert!(
        stalled.connect(2, ACK_PATIENCE).await.is_some(),
        "connect must return while the peer's send channel is full"
    );
}

/// With the gate off, `connect` waits for the admission exactly as it always
/// did.
///
/// This is what makes the bound above mean something. A fixture whose channel
/// never actually filled would let the fast return pass for the wrong reason;
/// here the same fixture, the same peer and the same full channel do not answer
/// at all.
#[tokio::test(flavor = "multi_thread")]
async fn gate_off_reproduces_the_awaited_ack() {
    let stalled = stalled_producer(test_config()).await;
    assert!(
        !MuxConfig::default().async_open_ack,
        "the awaited ack is the default"
    );

    let _first_inlet = stalled
        .connect(1, RECV_TIMEOUT)
        .await
        .expect("the first open is admitted immediately");
    eventually(|| stalled.wire.is_full()).await;

    assert!(
        stalled.connect(2, ACK_PATIENCE).await.is_none(),
        "the default must keep waiting for the OpenSlot's own admission"
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
    use super::super::sweep::DrainVisits;
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

// ---------------------------------------------------------------------------
// Zero-RTT pre-binds
// ---------------------------------------------------------------------------

/// One node that can pre-bind: a messenger, the mux installed on it, and an
/// anchor manager that knows about both.
///
/// `prebind_anchor` needs all three at once — the mux for the bind, the manager
/// for the registry entry the `PreBind` lives in — so every test below would
/// otherwise open with the same twelve lines.
struct PrebindingNode {
    messenger: Arc<Messenger>,
    mux: Arc<MessengerMuxTransport>,
    manager: AnchorManager,
}

async fn prebinding_node(
    config: MuxConfig,
    unattached_timeout: Option<Duration>,
) -> PrebindingNode {
    let messenger = Messenger::builder()
        .add_transport(tcp_transport())
        .build()
        .await
        .expect("messenger");
    let mux = MessengerMuxTransport::new(Arc::clone(&messenger), config, None).expect("mux");
    let mut builder = AnchorManagerBuilder::default()
        .worker_id(messenger.instance_id().worker_id())
        .transport(Arc::clone(&mux) as Arc<dyn crate::streaming::transport::FrameTransport>);
    if let Some(timeout) = unattached_timeout {
        builder = builder.default_unattached_timeout(timeout);
    }
    let manager = builder.build().expect("anchor manager");
    manager.install_mux(Arc::clone(&mux)).expect("install mux");
    PrebindingNode {
        messenger,
        mux,
        manager,
    }
}

/// A bind nobody claimed goes back when its anchor dies, not when the accept
/// window closes.
///
/// Zero-RTT setup binds a slot at request registration, so a request that dies
/// before its first token — a client that hangs up, a prompt that is refused —
/// leaves one behind. [`ACCEPT_TIMEOUT`] would collect it eventually and stays
/// as the backstop, but at the rate a frontend registers requests, a minute of
/// leaked bind, drain signal and reader pump per abandoned one is not a
/// reclamation policy.
///
/// The assertion runs with no `.await` between it and the drop, which is what
/// pins the claim: nothing asynchronous — no timer, no sweep, no spawned task —
/// can have run in between, so the reclamation is the drop and nothing else.
/// That is stronger than pausing the clock and advancing it a little, and it
/// avoids the trap in the weaker version: under `start_paused` an idle runtime
/// auto-advances to the next deadline, which is exactly the 60 s accept window
/// this test exists to prove is not involved.
#[tokio::test(flavor = "multi_thread")]
async fn unclaimed_bind_is_reclaimed_on_anchor_death_without_the_timer() {
    let node = prebinding_node(test_config(), None).await;
    let (messenger, mux, manager) = (&node.messenger, &node.mux, &node.manager);

    let anchor = manager.create_anchor::<u32>();
    let ticket = manager
        .prebind_anchor(anchor.handle())
        .expect("a mux is installed, so a ticket is minted");
    assert!(ticket.routing_session_id > 0);
    assert_eq!(mux.pending_binds(), 1, "the pre-bind registered a bind");
    assert_eq!(
        mux.parked_drains(),
        0,
        "`prebind_anchor` collected the drain signal inline, before spawning the pump, so no \
         later attach can find one parked here"
    );

    drop(anchor);

    assert_eq!(
        mux.pending_binds(),
        0,
        "the anchor died unclaimed, so its bind must be gone with it"
    );
    assert_eq!(mux.parked_drains(), 0);
    assert_eq!(
        mux.live_ingress_slots(messenger.instance_id().worker_id()),
        0
    );
}

/// Closing a claimed slot twice closes it once, and telling nobody is the
/// answer for a slot that is not there.
///
/// A pre-bind's drop is not the only thing that retires a slot whose consumer
/// has gone — the arrival path reaches the same verdict whenever a record
/// follows the consumer's death — so the two run in whichever order the traffic
/// decides. The second one must find nothing to do rather than close whatever
/// now holds that dense index.
#[tokio::test(flavor = "multi_thread")]
async fn close_claimed_slot_is_idempotent() {
    let pair = mux_pair(test_config()).await;

    // A slot this side never opened: nothing to close, and nothing to say.
    pair.consumer
        .close_claimed_slot(pair.producer_worker, protocol::SlotId::from_raw(0));

    let rx = pair.consumer.bind(1, 1).await.expect("bind");
    let tx = pair
        .producer
        .connect(pair.consumer_worker, 1, 1)
        .await
        .expect("connect");
    tx.send_async(item(0)).await.expect("send item");
    assert_eq!(recv(&rx).await, item(0));

    let ids = pair.consumer.live_slot_ids(pair.producer_worker);
    assert_eq!(ids.len(), 1, "one stream, one receive-side slot");
    let slot = ids[0];

    pair.consumer.close_claimed_slot(pair.producer_worker, slot);
    assert_eq!(
        pair.consumer.live_ingress_slots(pair.producer_worker),
        0,
        "the close must retire the slot here, not only tell the peer"
    );
    // The peer is told, so its producer stops rather than waiting for a record
    // it will never be asked for.
    eventually(|| tx.is_disconnected()).await;

    // Second close: same slot, already gone.
    pair.consumer.close_claimed_slot(pair.producer_worker, slot);
    assert_eq!(pair.consumer.live_ingress_slots(pair.producer_worker), 0);
}

/// The prompt close survives its peer's batcher retiring underneath it.
///
/// `close_claimed_slot` retires the last ingress slot for the peer -- exactly
/// what makes that peer's batcher evictable -- and only then posts the
/// `CloseSlot` the idle producer needs. The sweep can claim the batcher in
/// between, and the batcher's own last drain can precede the post. A reply
/// landing there used to be applied by nobody, and the producer never learned:
/// its later records are dropped on this side as `ClosedSlot`, with no reply.
///
/// Reproduced by hand at the seam every reply path shares, `send_replies`: the
/// handle is resolved as `close_claimed_slot` resolves it, the sweep's
/// claim-and-post runs, the batcher is held past the drain that carried
/// `retire`, and only then is the close posted. The batcher's own harness pins
/// the mechanism (`a_reply_posted_after_the_retire_drain_rides_the_final_flush`);
/// this pins the consequence, on the producer.
#[tokio::test(flavor = "multi_thread")]
async fn a_close_posted_past_the_batchers_last_drain_still_reaches_the_producer() {
    let hooks = Arc::new(TestHooks::default());
    let pair = mux_pair(test_config()).await;
    assert!(
        pair.consumer.core.hooks.set(Arc::clone(&hooks)).is_ok(),
        "installed before any batcher exists, so every one of them carries it"
    );

    let _rx = pair.bind(1, 1).await;
    let tx = pair
        .producer
        .connect(pair.consumer_worker, 1, 1)
        .await
        .expect("connect");
    eventually(|| pair.consumer.live_ingress_slots(pair.producer_worker) == 1).await;
    let slot = pair.consumer.live_slot_ids(pair.producer_worker)[0];

    // Resolved first, as `close_claimed_slot` resolves it. Nothing has been
    // drained, so no credit is owed and the batcher is idle from birth — which
    // is what makes `retire` its first wake and the barrier below a park past
    // the drain that carried it, rather than past some earlier one.
    assert!(
        !pair
            .consumer
            .core
            .batchers
            .contains_key(&pair.producer_worker),
        "an admitted OpenSlot owes no reply, so no batcher exists yet"
    );
    let batcher = pair.consumer.core.batcher(pair.producer_worker);

    // The sweep's eviction, by hand, in the window after its live-slot check
    // passed: claim under the registry lock, then post.
    hooks.pause();
    let (_, evicted) = pair
        .consumer
        .core
        .batchers
        .remove_if(&pair.producer_worker, |_, handle| handle.try_retire(0))
        .expect("an idle batcher with no egress slots is evictable");
    evicted.retire();
    // Parked past the drain that carried `retire`, one step from exiting.
    hooks.wait_until_parked().await;

    pair.consumer.core.send_replies(
        &batcher,
        pair.producer_worker,
        &[peer_batcher::ReplyRecord::CloseSlot {
            slot,
            reason: protocol::CloseReason::UnknownSlot,
        }],
    );
    hooks.release();

    // The producer learned: its egress slot is closed and its sender sees it.
    eventually(|| tx.is_disconnected()).await;
}

/// A reply the retired batcher refuses reaches the producer through the
/// batcher that replaces it.
///
/// The other half of the test above, which parks the batcher before its last
/// read so the reply rides the final flush and `send_replies`' re-resolve loop
/// never turns. Here the batcher is left to exit: its inbox is closed by the
/// time the reply is posted, so the first `reply` is refused and the loop has
/// to resolve a fresh batcher for the peer. What is pinned is that the
/// producer learns through that one — and that the fresh batcher is what the
/// refusal produced, not the retired one handed back.
#[tokio::test(flavor = "multi_thread")]
async fn a_close_refused_by_a_retired_batcher_reaches_the_producer_through_its_replacement() {
    let pair = mux_pair(test_config()).await;

    let _rx = pair.bind(1, 1).await;
    let tx = pair
        .producer
        .connect(pair.consumer_worker, 1, 1)
        .await
        .expect("connect");
    eventually(|| pair.consumer.live_ingress_slots(pair.producer_worker) == 1).await;
    let slot = pair.consumer.live_slot_ids(pair.producer_worker)[0];

    // Resolved first, as `close_claimed_slot` resolves it, then evicted by
    // hand the way the sweep does it: claim under the registry lock, post
    // `retire`, and this time let the task run all the way out.
    let batcher = pair.consumer.core.batcher(pair.producer_worker);
    let (_, evicted) = pair
        .consumer
        .core
        .batchers
        .remove_if(&pair.producer_worker, |_, handle| handle.try_retire(0))
        .expect("an idle batcher with no egress slots is evictable");
    evicted.retire();
    eventually(|| batcher.is_closed()).await;

    pair.consumer.core.send_replies(
        &batcher,
        pair.producer_worker,
        &[peer_batcher::ReplyRecord::CloseSlot {
            slot,
            reason: protocol::CloseReason::UnknownSlot,
        }],
    );

    // The producer learned, and through a batcher the refusal resolved.
    eventually(|| tx.is_disconnected()).await;
    let replacement = pair
        .consumer
        .core
        .batchers
        .get(&pair.producer_worker)
        .expect("the refused reply resolved a fresh batcher for the peer");
    assert!(
        !Arc::ptr_eq(replacement.value(), &batcher),
        "the registered batcher is the replacement, not the retired one"
    );
}

/// A `close_claimed_slot` with no runtime under it must retire nothing.
///
/// `PreBind::drop` is the only caller with no guarantee of a runtime -- it can
/// run wherever a `StreamAnchor` happens to be dropped, the same reason
/// `StreamController::cancel`'s own `_stream_cancel` spawn guards itself with
/// `Handle::try_current()` a few lines above it (`streaming/anchor.rs`).
/// Retiring the slot before checking for a runtime to post the reply on
/// leaves the producer strictly worse off than doing nothing: the reactive
/// `ConsumerGone` fault a live slot would otherwise raise on its next record
/// can no longer find a slot to raise it on.
#[tokio::test(flavor = "multi_thread")]
async fn close_claimed_slot_without_a_runtime_leaves_the_slot_in_place() {
    let pair = mux_pair(test_config()).await;

    let rx = pair.consumer.bind(1, 1).await.expect("bind");
    let tx = pair
        .producer
        .connect(pair.consumer_worker, 1, 1)
        .await
        .expect("connect");
    tx.send_async(item(0)).await.expect("send item");
    assert_eq!(recv(&rx).await, item(0));

    let ids = pair.consumer.live_slot_ids(pair.producer_worker);
    assert_eq!(ids.len(), 1, "one stream, one receive-side slot");
    let slot = ids[0];

    let consumer = Arc::clone(&pair.consumer);
    let peer = pair.producer_worker;
    // A bare OS thread carries no tokio context -- exactly the condition
    // `PreBind::drop` can hit and `close_claimed_slot`'s own runtime check
    // exists for.
    std::thread::spawn(move || consumer.close_claimed_slot(peer, slot))
        .join()
        .expect("close_claimed_slot must not panic off a runtime");

    assert_eq!(
        pair.consumer.live_ingress_slots(pair.producer_worker),
        1,
        "with no runtime to post the close, the slot must stay in the table for \
         the reactive ConsumerGone path to find -- retiring it here strands the \
         producer with neither a proactive close nor a reactive one"
    );
}

/// A pre-bind refused for a key mismatch gives the anchor its unattached timer
/// back.
///
/// `prebind_anchor` cancels that timer on purpose: the timer measures "no
/// sender attached", and a pre-bound anchor has a slot bound and pumped with a
/// sender on its way to it. A key mismatch takes the pre-bind away again — the
/// sender has been told the attach failed, so the `OpenSlot` that would have
/// claimed it never comes — and the anchor is plainly unattached once more.
/// Without a re-arm it is unattached with nothing left to reap it, and the
/// entry, its frame channel and its place in `velo_streaming_active_anchors`
/// outlive the request for the life of the process.
///
/// The mismatch is not a contrived shape: a worker running with
/// `MuxConfig::enabled = false` — the per-node rollback — advertises exactly
/// this, so the refusal is the rollback's own path.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_prebind_gives_the_unattached_timer_back() {
    /// Long enough that no plausible scheduling delay lets it fire between
    /// `create_anchor` and `prebind_anchor`, short enough to wait out twice.
    const UNATTACHED: Duration = Duration::from_secs(2);

    let node = prebinding_node(test_config(), Some(UNATTACHED)).await;
    let anchor = node.manager.create_anchor::<u32>();
    let handle = anchor.handle();
    let (_, local_id) = handle.unpack();
    node.manager.prebind_anchor(handle).expect("ticket");

    let request = crate::streaming::control::AnchorAttachRequest {
        handle,
        session_id: 1,
        stream_cancel_handle: crate::streaming::control::StreamCancelHandle::pack(
            WorkerId::from_u64(7),
            1,
        ),
        supported_transport_keys: vec![velo_ext::TransportKey::new(
            crate::streaming::tcp_transport::TCP_STREAM_KEY,
        )],
    };
    assert!(
        matches!(
            node.manager.adopt_prebind(local_id, &request),
            crate::streaming::anchor::PrebindAdoption::Refused(_)
        ),
        "a sender that cannot open the pre-bound key must be refused"
    );

    eventually(|| !node.manager.registry.contains_key(&local_id)).await;
    assert_eq!(
        node.mux.pending_binds(),
        0,
        "the refusal released the bind, and the reaped anchor left nothing behind"
    );
}

/// Adoption is the one transition from "no sender yet" to "a sender exists",
/// and the pre-bind's pump has to learn it immediately -- not only once the
/// adopting sender's own `OpenSlot` lands.
///
/// An older worker (or one whose envelope carried no ticket) can still attach
/// the ordinary way onto a slot this node already pre-bound. `adopt_prebind`
/// answers it `Ok` on the pre-bind's own terms, but the adopting sender has
/// not yet opened that slot -- nothing has claimed the pre-bind's drain --
/// which is exactly the window a sender that dies right after attaching (a
/// worker crash before its first record) sits in. If the pump still believes
/// no sender exists, that death is invisible to the heartbeat watchdog and
/// waits for the mux's 60 s accept window instead of the usual
/// `DETECTION_MULTIPLIER * heartbeat_interval`.
#[tokio::test(flavor = "multi_thread")]
async fn an_adopted_prebind_is_reaped_on_heartbeat_silence_before_its_open_slot() {
    let heartbeat = Duration::from_millis(50);
    let node = prebinding_node(test_config(), None).await;
    let anchor = node.manager.create_anchor_with_config::<u32>(AnchorConfig {
        heartbeat_interval: Some(heartbeat),
        ..Default::default()
    });
    let handle = anchor.handle();
    let (_, local_id) = handle.unpack();
    node.manager
        .prebind_anchor(handle)
        .expect("a mux is installed, so a ticket is minted");

    let request = crate::streaming::control::AnchorAttachRequest {
        handle,
        session_id: 1,
        stream_cancel_handle: crate::streaming::control::StreamCancelHandle::pack(
            WorkerId::from_u64(7),
            1,
        ),
        supported_transport_keys: vec![velo_ext::TransportKey::new(MESSENGER_MUX_KEY)],
    };
    assert!(
        matches!(
            node.manager.adopt_prebind(local_id, &request),
            crate::streaming::anchor::PrebindAdoption::Adopted(_)
        ),
        "the pre-bind's own key is offered, so this attach adopts it"
    );

    // The adopting sender has not opened its slot yet -- nothing has claimed
    // the pre-bind's drain -- so the pump is still deciding purely on
    // `PumpContext::prebound`, which adoption must have cleared.
    eventually(|| !node.manager.registry.contains_key(&local_id)).await;

    drop(anchor);
}

/// End-to-end version of the `control::reader_pump` reap-on-reclaim fix,
/// driven through the real `AnchorManager` / mux stack rather than the
/// synthetic pump fixture: at a heartbeat interval the watchdog alone cannot
/// beat the fixed 60 s accept window with (`>= 20 s` -- `DETECTION_MULTIPLIER`
/// is 3, so the watchdog's earliest fire from a fresh window is `3 *
/// heartbeat`), an adopted pre-bind whose sender never delivers its
/// `OpenSlot` must still be reaped once the accept window closes, and the
/// consumer must see `SenderDropped` rather than wedge on `Poll::Pending`
/// forever.
///
/// The sibling above uses a 50 ms heartbeat, so its watchdog always wins the
/// race against the 60 s window and this path never runs there -- this test
/// is the one that actually exercises the accept window as the reaper.
#[tokio::test]
async fn an_adopted_prebind_with_a_slow_heartbeat_is_reaped_by_the_accept_window() {
    tokio::time::pause();
    let heartbeat = Duration::from_secs(25);
    let node = prebinding_node(test_config(), None).await;
    let mut anchor = node.manager.create_anchor_with_config::<u32>(AnchorConfig {
        heartbeat_interval: Some(heartbeat),
        ..Default::default()
    });
    let handle = anchor.handle();
    let (_, local_id) = handle.unpack();
    node.manager
        .prebind_anchor(handle)
        .expect("a mux is installed, so a ticket is minted");

    let request = crate::streaming::control::AnchorAttachRequest {
        handle,
        session_id: 1,
        stream_cancel_handle: crate::streaming::control::StreamCancelHandle::pack(
            WorkerId::from_u64(7),
            1,
        ),
        supported_transport_keys: vec![velo_ext::TransportKey::new(MESSENGER_MUX_KEY)],
    };
    assert!(
        matches!(
            node.manager.adopt_prebind(local_id, &request),
            crate::streaming::anchor::PrebindAdoption::Adopted(_)
        ),
        "the pre-bind's own key is offered, so this attach adopts it"
    );

    // The watchdog alone would not fire until 25 s * (0 + DETECTION_MULTIPLIER)
    // = 75 s from adoption -- after the fixed 60 s accept window, not before.
    // Advance well past both so whichever fires first has already run.
    tokio::time::sleep(Duration::from_secs(65)).await;

    assert!(
        !node.manager.registry.contains_key(&local_id),
        "the accept window closing an unclaimed bind must reap the entry \
         even though the watchdog has not yet reached its own threshold"
    );

    let next = tokio::time::timeout(Duration::from_millis(50), anchor.next())
        .await
        .expect("the pump already exited on the accept window; the consumer must not block");
    assert!(
        matches!(
            next,
            Some(Err(crate::streaming::StreamError::SenderDropped))
        ),
        "expected SenderDropped, got {next:?}"
    );
}

/// Arming an unattached timeout on a pre-bound anchor stores it without
/// starting it.
///
/// Same reason `prebind_anchor` cancels the one already running: the anchor is
/// spoken for. Starting a reaper here would remove a stream that is about to
/// run, and under zero-RTT nothing would ever cancel it — there is no attach.
/// The duration is still stored, which is what lets a later release re-arm it.
#[tokio::test(flavor = "multi_thread")]
async fn set_timeout_on_a_pre_bound_anchor_arms_nothing() {
    const UNATTACHED: Duration = Duration::from_millis(50);

    let node = prebinding_node(test_config(), None).await;
    let anchor = node.manager.create_anchor::<u32>();
    let handle = anchor.handle();
    let (_, local_id) = handle.unpack();
    node.manager.prebind_anchor(handle).expect("ticket");

    anchor.set_timeout(Some(UNATTACHED));
    tokio::time::sleep(UNATTACHED * 6).await;

    assert!(
        node.manager.registry.contains_key(&local_id),
        "a pre-bound anchor has a slot waiting for a sender; nothing may reap it"
    );
    assert_eq!(
        node.manager
            .registry
            .get(&local_id)
            .expect("entry")
            .unattached_timeout,
        Some(UNATTACHED),
        "the duration is stored even while it is not running, so a released \
         pre-bind has something to re-arm"
    );
}

/// A pre-bound slot opens holding exactly what its ticket quoted.
///
/// The two numbers are read from different places — the ticket from
/// `NegotiatedLimits`, the slot from `MuxConfig` — and agree only because
/// `MessengerMuxTransport::new` normalises the config from the same limits. The
/// run in `mux_credit.rs` proves the sender and the buffer agree; nothing there
/// compares the ticket with what `IngressSlot::new` was actually handed, so a
/// change that gave `open_slot` its own budget source would split them
/// silently.
///
/// `slot_byte_budget: 0` is the field where they can differ: zero on the wire
/// means *use the default*, so a ticket built by copying the config field would
/// quote a zero the slot never opens at.
#[tokio::test(flavor = "multi_thread")]
async fn a_prebound_slot_opens_on_the_terms_its_ticket_quotes() {
    let config = MuxConfig {
        // Distinctive, so the assertion cannot pass on a shared default.
        initial_credit: 5,
        slot_byte_budget: 0,
        ..test_config()
    };
    let pair = mux_pair(config).await;
    let manager = AnchorManagerBuilder::default()
        .worker_id(pair.consumer_worker)
        .transport(
            Arc::clone(&pair.consumer) as Arc<dyn crate::streaming::transport::FrameTransport>
        )
        .build()
        .expect("anchor manager");
    manager
        .install_mux(Arc::clone(&pair.consumer))
        .expect("install mux");

    let anchor = manager.create_anchor::<u32>();
    let (_, local_id) = anchor.handle().unpack();
    let ticket = manager.prebind_anchor(anchor.handle()).expect("ticket");

    let tx = pair
        .producer
        .connect(pair.consumer_worker, local_id, ticket.routing_session_id)
        .await
        .expect("connect");
    tx.send_async(item(0)).await.expect("send item");
    eventually(|| pair.consumer.live_ingress_slots(pair.producer_worker) == 1).await;

    let id = pair.consumer.live_slot_ids(pair.producer_worker)[0];
    let (credit, byte_budget) = pair
        .consumer
        .slot_open_terms(pair.producer_worker, id)
        .expect("the claimed slot is live");
    assert_eq!(
        ticket.initial_credit, credit,
        "the sender opens holding what the ticket quoted; the slot must have reserved the same"
    );
    assert_eq!(
        u64::from(ticket.slot_byte_budget),
        byte_budget,
        "a configured zero resolves to the default in both reads, or neither"
    );

    drop(anchor);
}

/// Finding: `open_anchor_stream` has no same-worker guard, unlike its twin
/// `attach_stream_anchor`. `prebind_anchor` only mints for a handle this node
/// owns, but says nothing about which worker ends up opening it -- so a
/// ticket that is opened on the node that minted it (a single-process
/// application, or a test) took the network path instead of failing fast or
/// taking the co-located one.
///
/// Confirmed by running it before this guard existed: `open_anchor_stream`
/// returned `Ok(sender)`, and the very first `send` on that sender failed
/// with `ChannelClosed` -- a caller cannot tell the two apart from the
/// `Ok(sender)` alone, and has already lost the item it tried to send. The
/// fix is not "take the co-located path": zero-RTT deliberately never sets
/// `attachment` for a claimed slot (see `BATCHING.md`), so there is no
/// existing claim representation a co-located write could reuse without
/// making `attachment` mean two different things depending on where the
/// producer happened to land. Nothing in this PR claims same-worker ticket
/// open is supported -- every zero-RTT test pairs two nodes, and the README
/// example mints on one and opens on the other -- so the fix is to fail
/// immediately with a clear error instead of a confusing deferred one.
#[tokio::test(flavor = "multi_thread")]
async fn open_anchor_stream_on_the_minting_worker_fails_fast() {
    let node = prebinding_node(test_config(), None).await;
    let anchor = node.manager.create_anchor::<u32>();
    let handle = anchor.handle();
    let ticket = node.manager.prebind_anchor(handle).expect("ticket");

    let err = tokio::time::timeout(
        Duration::from_secs(3),
        node.manager.open_anchor_stream::<u32>(handle, ticket),
    )
    .await
    .expect("must fail immediately, not hang")
    .expect_err("a ticket opened on the worker that minted it must be refused");

    assert!(
        matches!(err, crate::streaming::AttachError::TransportError(_)),
        "expected TransportError naming the misuse, got {err:?}"
    );
    assert!(
        err.to_string().contains("attach_stream_anchor"),
        "the error must point at the co-located path a same-worker sender \
         should use instead, got: {err}"
    );

    drop(anchor);
}
