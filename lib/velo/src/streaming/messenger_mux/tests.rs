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
            .transport(mux_consumer as Arc<dyn FrameTransport>)
            .build()
            .expect("consumer anchor manager"),
    );
    let am_producer = Arc::new(
        AnchorManagerBuilder::default()
            .worker_id(producer_worker)
            .transport(mux_producer as Arc<dyn FrameTransport>)
            .build()
            .expect("producer anchor manager"),
    );
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
