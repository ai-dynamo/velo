// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! An application task that holds its tokio worker thread right after it woke
//! a velo task, on each runtime velo's tasks can run on.
//!
//! | Row | The application task | wakes | which then waits for the hold on the application's runtime |
//! |---|---|---|---|
//! | 1 | a producer calls stream `send` | the peer's batcher | the record |
//! | 2 | a caller makes a unary call | the connection's writer | the request |
//! | 3 | an ordered handler returns its response, and the next call on its lane blocks | the connection's writer | the response |
//! | 4 | a consumer reads a record already waiting | the node-wide credit sweep | the credit, so the next record |
//! | 5 | every application worker is held | nothing: a peer sends | the transport's read of the frames, and the mux's delivery of records |
//!
//! Rows 1 to 4 stall on the application's runtime when tokio has put the
//! woken velo task in the held worker's LIFO slot, which no other worker
//! steals from. Row 5 stalls there because the transport's reader and its
//! socket belong to the held runtime.
//!
//! On the default, [`TaskRuntime::Dedicated`] with one thread, every row is a
//! live test: the woken velo task and the reader run on velo's own runtime,
//! which the application cannot hold, so no run may wait for the hold. The
//! live rows build with [`TaskRuntime::default`], so they also guard the
//! default.
//!
//! On [`TaskRuntime::Application`], the opt-out, every row is an ignored
//! demonstration. A
//! test cannot force a tokio scheduling outcome by arranging wakes: another
//! wake can reach the velo task first (book, "Coding gotchas"). Run them with
//! `--ignored` to see the stall.
//!
//! A run "waited for the hold" when the woken work took at least half of it.
//! A run that waited took the whole hold, so the threshold leaves half the
//! hold for scheduling noise on a loaded machine.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::StreamExt;
use parking_lot::Mutex;
use prometheus::Registry;
use tokio::sync::Notify;
use velo::observability::VeloMetrics;
use velo::observability::test_helpers::MetricSnapshot;
use velo::streaming::{AnchorConfig, MuxConfig};
use velo::transports::tcp::TcpTransportBuilder;
use velo::{Context, Handler, StreamFrame, TaskRuntime, Velo};

const PATIENCE: Duration = Duration::from_secs(10);
const HOLD: Duration = Duration::from_millis(300);
/// Long enough that no watchdog fires during a hold.
const HEARTBEAT: Duration = Duration::from_secs(5);
const SETTLE: Duration = Duration::from_millis(20);
/// Runs per row and test. Loop the binary for more.
const RUNS: usize = 3;

fn waited(delay: Duration) -> bool {
    delay >= HOLD / 2
}

/// Every run kept clear of the hold.
fn assert_none_waited(row: &str, delays: &[Duration]) {
    assert!(
        !delays.iter().copied().any(waited),
        "{row}: velo's task waited for a hold on the application's runtime: {delays:?}"
    );
}

/// Every run waited for the hold.
fn assert_all_waited(row: &str, delays: &[Duration]) {
    assert!(
        delays.iter().copied().all(waited),
        "{row}: no stall this time (tokio's scheduling decides): {delays:?}"
    );
}

fn tcp() -> Arc<velo::transports::tcp::TcpTransport> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    Arc::new(
        TcpTransportBuilder::new()
            .from_listener(listener)
            .expect("from_listener")
            .build()
            .expect("build transport"),
    )
}

/// A loopback QUIC transport. QUIC builds its endpoint inside `start`, where
/// a TCP transport binds inside a task spawned on the runtime it is given, so
/// only QUIC shows whether `start` runs in velo's context.
#[cfg(feature = "quic")]
fn quic() -> Arc<dyn velo::Transport> {
    Arc::new(
        velo::transports::quic::QuicTransportBuilder::new()
            .bind_addr("127.0.0.1:0".parse().unwrap())
            .build()
            .expect("build transport"),
    )
}

fn tcp_dyn() -> Arc<dyn velo::Transport> {
    tcp()
}

async fn node(runtime: TaskRuntime, credit: Option<u32>, registry: Option<&Registry>) -> Velo {
    node_on(tcp(), runtime, credit, registry).await
}

async fn node_on(
    transport: Arc<dyn velo::Transport>,
    runtime: TaskRuntime,
    credit: Option<u32>,
    registry: Option<&Registry>,
) -> Velo {
    let mut builder = Velo::builder()
        .add_transport(transport)
        .mux_only()
        .task_runtime(runtime);
    if let Some(credit) = credit {
        builder = builder
            .messenger_mux(MuxConfig {
                initial_credit: credit,
                ..MuxConfig::default()
            })
            .expect("mux config");
    }
    if let Some(registry) = registry {
        builder = builder.metrics(Arc::new(VeloMetrics::register(registry).expect("metrics")));
    }
    builder.build().await.expect("build velo")
}

async fn introduce(a: &Velo, b: &Velo) {
    a.register_peer(b.peer_info()).expect("register");
    b.register_peer(a.peer_info()).expect("register");
    for (x, y) in [(a, b), (b, a)] {
        tokio::time::timeout(
            PATIENCE,
            x.wait_for_handler(y.instance_id(), "_anchor_attach"),
        )
        .await
        .expect("handshake timed out")
        .expect("handshake");
    }
}

/// Row 1: a stream `send` wakes the peer's batcher; the producer then holds.
/// The delay is from the send to the record's arrival at the consumer.
async fn stream_send(runtime: TaskRuntime) -> Vec<Duration> {
    let producer = node(runtime, None, None).await;
    let consumer = node(runtime, None, None).await;
    introduce(&producer, &consumer).await;
    let mut delays = Vec::new();
    for _ in 0..RUNS {
        let mut anchor = consumer.create_anchor_with_config::<u32>(AnchorConfig {
            heartbeat_interval: Some(HEARTBEAT),
            ..AnchorConfig::default()
        });
        let ticket = consumer.prebind_anchor(anchor.handle()).expect("ticket");
        let sender = producer
            .open_anchor_stream::<u32>(anchor.handle(), ticket)
            .await
            .expect("open stream");
        let go = Arc::new(Notify::new());
        let sent_at = Arc::new(Mutex::new(None::<Instant>));
        let task = tokio::spawn({
            let go = Arc::clone(&go);
            let sent_at = Arc::clone(&sent_at);
            async move {
                sender.send(0).await.expect("send");
                // Item 0 has arrived, so the batcher has drained and parked.
                go.notified().await;
                // Resume from the run queue: the LIFO slot is on again.
                tokio::task::yield_now().await;
                *sent_at.lock() = Some(Instant::now());
                sender.send(1).await.expect("send");
                std::thread::sleep(HOLD);
                sender.finalize().expect("finalize");
            }
        });
        let mut arrived = None;
        loop {
            match tokio::time::timeout(PATIENCE, anchor.next()).await {
                Ok(Some(Ok(StreamFrame::Item(0)))) => go.notify_one(),
                Ok(Some(Ok(StreamFrame::Item(1)))) => arrived = Some(Instant::now()),
                Ok(Some(Ok(StreamFrame::Finalized))) => break,
                other => panic!("row 1: unexpected {other:?}"),
            }
        }
        task.await.expect("producer");
        let sent = sent_at.lock().expect("sent");
        delays.push(arrived.expect("item 1").duration_since(sent));
        tokio::time::sleep(SETTLE).await;
    }
    producer.shutdown(velo::ShutdownPolicy::WaitForever).await;
    consumer.shutdown(velo::ShutdownPolicy::WaitForever).await;
    delays
}

/// Row 2: a unary call wakes the connection's writer (the call hands its
/// frame to the transport inside `send()`); the caller then holds before it
/// awaits the answer. The delay is from the call to the handler running on
/// the server.
async fn unary_call(runtime: TaskRuntime) -> Vec<Duration> {
    let client = node(runtime, None, None).await;
    let server = node(runtime, None, None).await;
    let (tx, rx) = flume::unbounded::<Instant>();
    server
        .register_handler(
            Handler::unary_handler("stamp", move |ctx: Context| {
                let _ = tx.send(Instant::now());
                Ok(Some(ctx.payload))
            })
            .build(),
        )
        .expect("handler");
    introduce(&client, &server).await;
    let target = server.instance_id();
    // Warm the connection.
    client
        .unary("stamp")
        .unwrap()
        .raw_payload(Bytes::from_static(b"w"))
        .instance(target)
        .send()
        .await
        .expect("warm");
    let _ = rx.recv_async().await;
    let mut delays = Vec::new();
    for _ in 0..RUNS {
        let go = Arc::new(Notify::new());
        let task = tokio::spawn({
            let go = Arc::clone(&go);
            let client = client.clone();
            async move {
                go.notified().await;
                tokio::task::yield_now().await;
                let t0 = Instant::now();
                let call = client
                    .unary("stamp")
                    .unwrap()
                    .raw_payload(Bytes::from_static(b"x"))
                    .instance(target)
                    .send();
                std::thread::sleep(HOLD);
                call.await.expect("unary");
                t0
            }
        });
        tokio::time::sleep(SETTLE).await;
        go.notify_one();
        let ran = tokio::time::timeout(PATIENCE, rx.recv_async())
            .await
            .expect("handler ran")
            .expect("stamp");
        let t0 = task.await.expect("caller");
        delays.push(ran.saturating_duration_since(t0));
    }
    client.shutdown(velo::ShutdownPolicy::WaitForever).await;
    server.shutdown(velo::ShutdownPolicy::WaitForever).await;
    delays
}

/// Row 3: an ordered handler returns its response, which wakes the
/// connection's writer; the lane then runs the next call, which blocks. The
/// delay is the first call's round trip, less the 2 ms its handler takes.
async fn handler_response(runtime: TaskRuntime) -> Vec<Duration> {
    const WORK: Duration = Duration::from_millis(2);
    let client = node(runtime, None, None).await;
    let server = node(runtime, None, None).await;
    server
        .register_handler(
            Handler::unary_handler("lane", |ctx: Context| {
                if ctx.payload.first() == Some(&1) {
                    std::thread::sleep(HOLD);
                } else {
                    // Long enough that the second call is queued on the lane
                    // when this one answers.
                    std::thread::sleep(WORK);
                }
                Ok(Some(ctx.payload))
            })
            .ordered()
            .build(),
        )
        .expect("handler");
    introduce(&client, &server).await;
    let target = server.instance_id();
    let call = |b: u8| {
        client
            .unary("lane")
            .unwrap()
            .raw_payload(Bytes::from(vec![b]))
            .instance(target)
            .send()
    };
    call(0).await.expect("warm");
    let mut delays = Vec::new();
    for _ in 0..RUNS {
        let t0 = Instant::now();
        let first = call(0);
        let second = call(1);
        let first = tokio::spawn(async move {
            first.await.expect("first");
            Instant::now()
        });
        let answered = tokio::time::timeout(PATIENCE, first)
            .await
            .expect("first answered")
            .expect("join");
        second.await.expect("second");
        delays.push(answered.duration_since(t0).saturating_sub(WORK));
        tokio::time::sleep(SETTLE).await;
    }
    client.shutdown(velo::ShutdownPolicy::WaitForever).await;
    server.shutdown(velo::ShutdownPolicy::WaitForever).await;
    delays
}

/// Row 4: a consumer comes back to a record already waiting and reads it,
/// which posts a drain to the credit sweep; the consumer then holds. With one
/// credit, the next record leaves the producer only once the sweep returns
/// that credit. The delay is from the read to the next record's arrival at
/// the consumer's node, seen by a thread outside tokio that watches the
/// node's received-record count.
///
/// A consumer that reads as soon as its record is delivered is usually the
/// third task in a chain of LIFO wakes (connection reader, inbound loop,
/// batch lane), and tokio turns the LIFO slot off after three LIFO polls in a
/// row. One that comes back to a waiting record has no such chain, so this
/// row reads after a sleep.
async fn buffered_read(runtime: TaskRuntime) -> Vec<Duration> {
    let registry = Arc::new(Registry::new());
    let producer = node(runtime, None, None).await;
    let consumer = node(runtime, Some(1), Some(&registry)).await;
    introduce(&producer, &consumer).await;
    let received = |r: &Registry| {
        MetricSnapshot::from_registry(r).histogram_sum(
            "velo_streaming_mux_records_per_batch",
            &[("direction", "received")],
        )
    };
    let mut delays = Vec::new();
    for _ in 0..RUNS {
        let mut anchor = consumer.create_anchor_with_config::<u32>(AnchorConfig {
            heartbeat_interval: Some(HEARTBEAT),
            ..AnchorConfig::default()
        });
        let ticket = consumer.prebind_anchor(anchor.handle()).expect("ticket");
        let sender = producer
            .open_anchor_stream::<u32>(anchor.handle(), ticket)
            .await
            .expect("open stream");
        // Item 1 waits at the producer for the credit item 0's read returns.
        let (mark_tx, mark_rx) = std::sync::mpsc::channel::<(Instant, f64)>();
        let watcher = std::thread::spawn({
            let registry = Arc::clone(&registry);
            move || {
                let (read_at, base) = mark_rx.recv().expect("mark");
                let deadline = read_at + PATIENCE;
                while Instant::now() < deadline {
                    if received(&registry) > base {
                        return Instant::now().duration_since(read_at);
                    }
                    std::thread::sleep(Duration::from_micros(100));
                }
                panic!("row 4: item 1 never arrived");
            }
        });
        let reader = tokio::spawn({
            let registry = Arc::clone(&registry);
            async move {
                // Item 0 arrives while this task sleeps.
                tokio::time::sleep(Duration::from_millis(5)).await;
                tokio::task::yield_now().await;
                match anchor.next().await {
                    Some(Ok(StreamFrame::Item(0))) => {}
                    other => panic!("row 4: item 0: {other:?}"),
                }
                // The read posted the drain. Take the count now: item 1 has
                // not left the producer without that credit.
                let base = received(&registry);
                mark_tx.send((Instant::now(), base)).expect("mark");
                std::thread::sleep(HOLD);
                loop {
                    match anchor.next().await {
                        Some(Ok(StreamFrame::Item(1))) => {}
                        Some(Ok(StreamFrame::Finalized)) | None => break,
                        other => panic!("row 4: {other:?}"),
                    }
                }
            }
        });
        sender.send(0).await.expect("send");
        sender.send(1).await.expect("send");
        sender.finalize().expect("finalize");
        tokio::time::timeout(PATIENCE, reader)
            .await
            .expect("reader")
            .expect("join");
        delays.push(watcher.join().expect("watcher"));
        tokio::time::sleep(SETTLE).await;
    }
    producer.shutdown(velo::ShutdownPolicy::WaitForever).await;
    consumer.shutdown(velo::ShutdownPolicy::WaitForever).await;
    delays
}

/// Row 5: every worker of the application's runtime is held while a peer on
/// another runtime sends to the node. The delay is from the first send to the
/// node's transport counting an inbound message, seen by this thread, which
/// is not a worker. The peer's own runtime, on its own thread, is never held.
///
/// Rows 1 to 4 wake writers, the batcher and the sweep; none of them reads.
/// This row checks the read side: a socket created on the application's
/// runtime would wait for it however velo's tasks were spawned.
async fn inbound_read(
    transport: fn() -> Arc<dyn velo::Transport>,
    runtime: TaskRuntime,
    workers: usize,
    inbound: Inbound,
) -> Duration {
    let registry = Registry::new();
    let node = node_on(transport(), runtime, None, Some(&registry)).await;
    node.register_handler(Handler::am_handler("sink", |_ctx: Context| Ok(())).build())
        .expect("handler");
    let accepted = move |r: &Registry| {
        let snapshot = MetricSnapshot::from_registry(r);
        match inbound {
            Inbound::Message => snapshot.counter_sum(
                "velo_transport_frames_total",
                &[
                    ("direction", "inbound"),
                    ("message_type", "message"),
                    ("outcome", "accepted"),
                ],
            ),
            // Observed by the mux's batch lane as it delivers a batch.
            Inbound::Record => snapshot.histogram_sum(
                "velo_streaming_mux_records_per_batch",
                &[("direction", "received")],
            ),
        }
    };
    // Kept, unread, to the end: the records wait in it.
    let anchor = node.create_anchor_with_config::<u32>(AnchorConfig {
        heartbeat_interval: Some(HEARTBEAT),
        ..AnchorConfig::default()
    });
    let handle = anchor.handle();
    let ticket = node.prebind_anchor(handle).expect("ticket");

    let node_info = node.peer_info();
    let target = node.instance_id();
    let (info_tx, info_rx) = tokio::sync::oneshot::channel();
    let (go_tx, go_rx) = tokio::sync::oneshot::channel::<()>();
    let (sent_tx, sent_rx) = std::sync::mpsc::channel::<Instant>();
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
    let peer = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("peer runtime");
        rt.block_on(async move {
            let peer = Velo::builder()
                .add_transport(transport())
                .mux_only()
                .build()
                .await
                .expect("peer");
            peer.register_peer(node_info).expect("register node");
            info_tx.send(peer.peer_info()).expect("peer info");
            let send = |peer: &Velo| {
                peer.am_send("sink")
                    .unwrap()
                    .raw_payload(Bytes::from_static(b"x"))
                    .instance(target)
                    .send()
            };
            // Warm the connection, and open the stream, while the node can
            // still answer.
            for handler in ["sink", "_anchor_attach"] {
                tokio::time::timeout(PATIENCE, peer.wait_for_handler(target, handler))
                    .await
                    .expect("handshake timed out")
                    .expect("handshake");
            }
            let sender = match inbound {
                Inbound::Message => {
                    send(&peer).await.expect("warm");
                    None
                }
                Inbound::Record => {
                    let sender = peer
                        .open_anchor_stream::<u32>(handle, ticket)
                        .await
                        .expect("open stream");
                    sender.send(0).await.expect("warm");
                    Some(sender)
                }
            };
            go_rx.await.expect("go");
            sent_tx.send(Instant::now()).expect("sent");
            for i in 1..=8 {
                match &sender {
                    None => send(&peer).await.expect("send"),
                    Some(sender) => sender.send(i).await.expect("send"),
                }
            }
            let _ = done_rx.await;
            drop(sender);
            peer.shutdown(velo::ShutdownPolicy::Timeout(Duration::from_secs(1)))
                .await;
        });
    });
    node.register_peer(info_rx.await.expect("peer info"))
        .expect("register peer");
    // The warm message has been read.
    tokio::time::timeout(PATIENCE, async {
        while accepted(&registry) < 1.0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("warm message never arrived");

    // Hold every worker. Spawned from this thread, which is not a worker, so
    // the tasks go to the shared queue and each idle worker takes one.
    let started = Arc::new(AtomicUsize::new(0));
    let holders: Vec<_> = (0..workers)
        .map(|_| {
            let started = Arc::clone(&started);
            tokio::spawn(async move {
                started.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(HOLD);
            })
        })
        .collect();
    // Blocking waits from here on: an await would hand this thread back to
    // the runtime, which cannot run anything while its workers are held.
    let deadline = Instant::now() + PATIENCE;
    while started.load(Ordering::SeqCst) < workers {
        assert!(Instant::now() < deadline, "the holders never started");
        std::thread::sleep(Duration::from_micros(100));
    }
    let base = accepted(&registry);
    go_tx.send(()).expect("go");
    let sent = sent_rx.recv_timeout(PATIENCE).expect("peer sent");
    let delay = loop {
        if accepted(&registry) > base {
            break sent.elapsed();
        }
        assert!(sent.elapsed() < PATIENCE, "row 5: no message arrived");
        std::thread::sleep(Duration::from_micros(100));
    };
    for holder in holders {
        holder.await.expect("holder");
    }
    let _ = done_tx.send(());
    tokio::task::spawn_blocking(move || peer.join().expect("peer thread"))
        .await
        .expect("join");
    node.shutdown(velo::ShutdownPolicy::Timeout(Duration::from_secs(1)))
        .await;
    drop(anchor);
    delay
}

/// What the peer sends in row 5.
#[derive(Clone, Copy)]
enum Inbound {
    /// Fire-and-forget messages, counted by the node's transport as it
    /// admits them.
    Message,
    /// Stream records, counted by the node's mux as its batch lane delivers
    /// them.
    Record,
}

// --- On the default, a dedicated runtime: live ----------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dedicated_stream_send_never_waits_for_a_held_thread() {
    assert_none_waited("row 1", &stream_send(TaskRuntime::default()).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dedicated_unary_call_never_waits_for_a_held_thread() {
    assert_none_waited("row 2", &unary_call(TaskRuntime::default()).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dedicated_handler_response_never_waits_for_a_held_thread() {
    assert_none_waited("row 3", &handler_response(TaskRuntime::default()).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dedicated_credit_sweep_never_waits_for_a_held_thread() {
    assert_none_waited("row 4", &buffered_read(TaskRuntime::default()).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dedicated_transport_reads_while_every_worker_is_held() {
    let delay = inbound_read(tcp_dyn, TaskRuntime::default(), 2, Inbound::Message).await;
    assert_none_waited("row 5", &[delay]);
}

/// The mux's batch lanes are velo's ingress and run on its runtime too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dedicated_mux_delivers_while_every_worker_is_held() {
    let delay = inbound_read(tcp_dyn, TaskRuntime::default(), 2, Inbound::Record).await;
    assert_none_waited("row 5, records", &[delay]);
}

#[cfg(feature = "quic")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dedicated_quic_transport_reads_while_every_worker_is_held() {
    let delay = inbound_read(quic, TaskRuntime::default(), 2, Inbound::Message).await;
    assert_none_waited("row 5, QUIC", &[delay]);
}

/// Two threads take the same path as one; a check that the count is wired
/// through.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dedicated_two_threads_stream_send_never_waits_for_a_held_thread() {
    assert_none_waited(
        "row 1, 2 threads",
        &stream_send(TaskRuntime::Dedicated { threads: 2 }).await,
    );
}

// --- On TaskRuntime::Application, the opt-out: ignored demonstrations ------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "demonstration: tokio's scheduling decides whether the batcher is in the held worker's LIFO slot (book, Coding gotchas); run with --ignored to see the stall"]
async fn application_stream_send_waits_for_a_held_thread() {
    assert_all_waited("row 1", &stream_send(TaskRuntime::Application).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "demonstration: tokio's scheduling decides whether the writer is in the held worker's LIFO slot (book, Coding gotchas); run with --ignored to see the stall"]
async fn application_unary_call_waits_for_a_held_thread() {
    assert_all_waited("row 2", &unary_call(TaskRuntime::Application).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "demonstration: tokio's scheduling decides whether the writer is in the held worker's LIFO slot (book, Coding gotchas); run with --ignored to see the stall"]
async fn application_handler_response_waits_for_a_held_thread() {
    assert_all_waited("row 3", &handler_response(TaskRuntime::Application).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "demonstration: tokio's scheduling decides whether the sweep is in the held worker's LIFO slot (book, Coding gotchas); run with --ignored to see the stall"]
async fn application_credit_sweep_waits_for_a_held_thread() {
    assert_all_waited("row 4", &buffered_read(TaskRuntime::Application).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "demonstration: on the application's runtime the reader waits for any hold of every worker; run with --ignored to see it"]
async fn application_transport_waits_while_every_worker_is_held() {
    let delay = inbound_read(tcp_dyn, TaskRuntime::Application, 2, Inbound::Message).await;
    assert_all_waited("row 5", &[delay]);
}
