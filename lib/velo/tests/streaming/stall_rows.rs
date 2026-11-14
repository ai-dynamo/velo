// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Scratch, for measurement: the four wakes an application task can stall by
//! holding its worker thread right after it made them.
//!
//! | Row | The application task | wakes | which then waits for the hold when it sits in the task's LIFO slot |
//! |---|---|---|---|
//! | 1 | a producer calls stream `send` | the peer's batcher | the record |
//! | 2 | a caller makes a unary call | the connection's writer | the request |
//! | 3 | an ordered handler returns its response, and the next call on its lane blocks | the connection's writer | the response |
//! | 4 | a consumer reads a record | the node-wide credit sweep (the drain doorbell) | the credit, so the next record |
//!
//! A test cannot force a tokio scheduling outcome by arranging wakes (book,
//! "Coding gotchas"). So each row runs `STALL_N` times (default 50) and
//! prints one `STALL,...` line: how often the woken work waited for the hold,
//! and for how long. It asserts nothing. `VELO_ARM` picks the arm (see
//! `velo::scratch_arm`). Run with `--ignored --test-threads=1 --nocapture`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::StreamExt;
use parking_lot::Mutex;
use prometheus::Registry;
use tokio::sync::Notify;
use velo::observability::VeloMetrics;
use velo::observability::test_helpers::MetricSnapshot;
use velo::streaming::{AnchorConfig, MuxConfig, StreamFrame};
use velo::transports::tcp::TcpTransportBuilder;
use velo::{Context, Handler, Velo};

const PATIENCE: Duration = Duration::from_secs(10);
/// The hold. A delivery that took at least half of it waited for it.
const HOLD: Duration = Duration::from_millis(100);
/// Long enough that no watchdog fires during a hold.
const HEARTBEAT: Duration = Duration::from_secs(2);
const SETTLE: Duration = Duration::from_millis(20);

fn runs() -> usize {
    std::env::var("STALL_N")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50)
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

async fn node(credit: Option<(u32, bool)>, registry: Option<&Registry>) -> Velo {
    let mut builder = Velo::builder().add_transport(tcp()).mux_only();
    if let Some((credit, immediate)) = credit {
        let mut config = MuxConfig {
            initial_credit: credit,
            ..MuxConfig::default()
        };
        if immediate {
            // Walk every doorbell wake at once and send the return at once,
            // so nothing but the wake decides when the credit leaves.
            config.drain_visit_floor = Duration::ZERO;
            config.reply_linger = Duration::ZERO;
        }
        builder = builder.messenger_mux(config).expect("mux config");
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

fn arm() -> String {
    velo::scratch_arm::RuntimeArm::from_env().label()
}

/// Print one row's result. `delays` are how long the woken work took, in µs.
fn report(row: u32, what: &str, delays: &[u64]) {
    let n = delays.len();
    let threshold = HOLD.as_micros() as u64 / 2;
    let mut all = delays.to_vec();
    all.sort_unstable();
    let mut stalled: Vec<u64> = all.iter().copied().filter(|&d| d >= threshold).collect();
    stalled.sort_unstable();
    let pct = |v: &[u64], q: f64| -> u64 {
        if v.is_empty() {
            0
        } else {
            v[((v.len() - 1) as f64 * q).round() as usize]
        }
    };
    println!(
        "STALL,row={row},what={what},arm={},n={n},stalls={},rate={:.2},all_p50_us={},all_max_us={},stall_p50_us={},stall_max_us={}",
        arm(),
        stalled.len(),
        stalled.len() as f64 / n.max(1) as f64,
        pct(&all, 0.5),
        pct(&all, 1.0),
        pct(&stalled, 0.5),
        pct(&stalled, 1.0),
    );
}

/// Row 1: a stream `send` wakes the peer's batcher; the producer then holds.
/// The delay is from the send to the record's arrival at the consumer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement: prints a stall rate"]
async fn row1_stream_send_wakes_the_batcher() {
    let producer = node(None, None).await;
    let consumer = node(None, None).await;
    introduce(&producer, &consumer).await;
    let mut delays = Vec::new();
    for _ in 0..runs() {
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
        delays.push(arrived.expect("item 1").duration_since(sent).as_micros() as u64);
        tokio::time::sleep(SETTLE).await;
    }
    report(1, "stream_send", &delays);
}

/// Row 2: a unary call wakes the connection's writer (the fast path sends
/// inside `send()`); the caller then holds before it awaits the answer. The
/// delay is from the call to the handler running on the server.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement: prints a stall rate"]
async fn row2_unary_call_wakes_the_writer() {
    let client = node(None, None).await;
    let server = node(None, None).await;
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
    // Warm the connection and the handshake.
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
    for _ in 0..runs() {
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
        delays.push(ran.saturating_duration_since(t0).as_micros() as u64);
    }
    report(2, "unary_call", &delays);
}

/// Row 3: an ordered handler returns its response, which wakes the
/// connection's writer; the lane then runs the next call, which blocks. The
/// delay is the first call's round trip, less the 2 ms its handler takes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement: prints a stall rate"]
async fn row3_handler_response_wakes_the_writer() {
    let client = node(None, None).await;
    let server = node(None, None).await;
    server
        .register_handler(
            Handler::unary_handler("lane", |ctx: Context| {
                if ctx.payload.first() == Some(&1) {
                    std::thread::sleep(HOLD);
                } else {
                    // Long enough that the second call is queued on the lane
                    // when this one answers.
                    std::thread::sleep(Duration::from_millis(2));
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
    for _ in 0..runs() {
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
        let rtt = answered.duration_since(t0).as_micros() as u64;
        delays.push(rtt.saturating_sub(2_000));
        tokio::time::sleep(SETTLE).await;
    }
    report(3, "handler_response", &delays);
}

/// Row 4: a consumer reads a record, which posts a drain to the credit sweep;
/// the consumer then holds. With one credit, the next record leaves the
/// producer only once the sweep returns that credit. The delay is from the
/// read to the next record's arrival at the consumer's node, seen by a thread
/// outside tokio that watches the node's received-record count.
///
/// At the default `drain_visit_floor` (2 ms) and `reply_linger` (1 ms) a
/// visit that comes too soon after the last one is put off to a timer, and
/// the timer fires on whichever worker drives the clock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement: prints a stall rate"]
async fn row4_consumer_read_wakes_the_credit_sweep() {
    row4(false, false).await;
}

/// Row 4 with `drain_visit_floor` and `reply_linger` at zero: the doorbell
/// wake alone decides when the credit leaves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement: prints a stall rate"]
async fn row4z_consumer_read_wakes_the_credit_sweep_at_once() {
    row4(true, false).await;
}

/// Row 4 where the consumer reads a record that is already buffered, after
/// it resumed from the run queue, at the default floor and linger.
///
/// A consumer that reads as soon as its record is delivered is usually the
/// third task in a chain of LIFO wakes (connection reader, inbound loop, batch
/// lane, consumer), and tokio turns the LIFO slot off after three LIFO polls
/// in a row, so its doorbell wake goes to the stealable run queue. A consumer
/// that comes back to records already waiting has no such chain.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement: prints a stall rate"]
async fn row4b_buffered_read_wakes_the_credit_sweep() {
    row4(false, true).await;
}

/// Row 4b with `drain_visit_floor` and `reply_linger` at zero.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement: prints a stall rate"]
async fn row4bz_buffered_read_wakes_the_credit_sweep_at_once() {
    row4(true, true).await;
}

async fn row4(immediate: bool, buffered: bool) {
    let registry = Registry::new();
    let producer = node(None, None).await;
    let consumer = node(Some((1, immediate)), Some(&registry)).await;
    introduce(&producer, &consumer).await;
    let received = |r: &Registry| {
        MetricSnapshot::from_registry(r).histogram_sum(
            "velo_streaming_mux_records_per_batch",
            &[("direction", "received")],
        )
    };
    let registry = Arc::new(registry);
    let mut delays = Vec::new();
    for _ in 0..runs() {
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
                if buffered {
                    // Item 0 arrives while this task sleeps.
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    tokio::task::yield_now().await;
                }
                match anchor.next().await {
                    Some(Ok(StreamFrame::Item(0))) => {}
                    other => panic!("row 4: item 0: {other:?}"),
                }
                // The read posted the drain. Take the count now: item 1 has
                // not left the producer without that credit.
                let base = received(&registry);
                let wakes =
                    velo::scratch_arm::DRAIN_WAKES.load(std::sync::atomic::Ordering::Relaxed);
                let me = std::thread::current().name().unwrap_or("?").to_string();
                let read_at = Instant::now();
                mark_tx.send((read_at, base)).expect("mark");
                if std::env::var("STALL_DEBUG").is_ok() {
                    eprintln!("row4 reader on {me:?}, drain wakes so far {wakes}");
                }
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
        let delay = watcher.join().expect("watcher");
        if std::env::var("STALL_DEBUG").is_ok() {
            let v = std::mem::take(&mut *velo::scratch_arm::VISITS.lock());
            eprintln!(
                "row4 visits: {:?}",
                v.iter()
                    .map(|(n, t)| (n.clone(), t.elapsed().as_micros()))
                    .collect::<Vec<_>>()
            );
        }
        delays.push(delay.as_micros() as u64);
        tokio::time::sleep(SETTLE).await;
    }
    report(
        4,
        match (immediate, buffered) {
            (false, false) => "consumer_read",
            (true, false) => "consumer_read_floor0",
            (false, true) => "buffered_read",
            (true, true) => "buffered_read_floor0",
        },
        &delays,
    );
}
