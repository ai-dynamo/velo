// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! A task that holds its tokio worker thread right after it wakes a velo task
//! stalls that velo task until it lets go.
//!
//! On a multi-thread runtime, tokio puts a parked task woken from a worker
//! thread in that worker's LIFO slot (while the slot is enabled; tokio turns
//! it off for a tick after three LIFO polls in a row), and no other worker
//! steals from the slot. A stream `send` wakes a parked peer batcher this way.
//! So a producer that sends and then blocks (a `std::thread::sleep`, a
//! blocking call, a long encode) can keep the batcher from running: no record
//! and no heartbeat leaves on that peer's lane, and the consumer's watchdog
//! ends the stream as if the producer had died. The same holds for other velo
//! tasks the application wakes from its own worker, such as a connection's
//! writer after a unary call.
//!
//! `tokio::task::block_in_place` avoids it: before it blocks, tokio hands the
//! worker's queued tasks, the LIFO slot included, to another worker. The
//! book's "Known limitations" appendix states the rule.
//!
//! The second test is the guarantee: the hold inside `block_in_place` keeps
//! the stream whole. The first test is a demonstration, so it is ignored: it
//! makes the stall likely (the batcher has drained, and the producer resumes
//! from its run queue so the slot is on) but cannot force it. A heartbeat or
//! a credit reply can wake the batcher on another worker first, and if that
//! poll is still running when the producer sends, tokio only marks the task
//! notified and the batcher carries on elsewhere. Run it with `--ignored` to
//! see the stall.

use std::sync::Arc;

use std::time::Duration;
use tokio::sync::Notify;

use futures::StreamExt;
use velo::Velo;
use velo::streaming::{AnchorConfig, StreamAnchor, StreamFrame};
use velo::transports::tcp::TcpTransportBuilder;

const PATIENCE: Duration = Duration::from_secs(5);
const HEARTBEAT: Duration = Duration::from_millis(50);
/// Twelve heartbeat windows. The watchdog ends a silent stream that holds
/// data credit after three to four windows.
const HOLD: Duration = Duration::from_millis(600);

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

async fn node() -> Velo {
    Velo::builder()
        .add_transport(tcp())
        .mux_only()
        .build()
        .await
        .expect("build velo")
}

/// A producer and a consumer that know each other.
async fn pair() -> (Velo, Velo) {
    let producer = node().await;
    let consumer = node().await;
    producer
        .register_peer(consumer.peer_info())
        .expect("register consumer");
    consumer
        .register_peer(producer.peer_info())
        .expect("register producer");
    for (a, b) in [(&producer, &consumer), (&consumer, &producer)] {
        tokio::time::timeout(
            PATIENCE,
            a.wait_for_handler(b.instance_id(), "_anchor_attach"),
        )
        .await
        .expect("handshake timed out")
        .expect("handshake");
    }
    (producer, consumer)
}

/// A new anchor on `consumer` with a short heartbeat, and a stream to it from
/// `producer`. The sender sends item 0, waits for `go`, yields once, sends
/// item 1, and then runs `hold` before it finalizes.
// Until `Velo` has its own call for a single-consumer anchor with a config,
// `anchor_manager()` is the way to give one a heartbeat interval.
#[allow(deprecated)]
async fn stream(
    producer: &Velo,
    consumer: &Velo,
    go: Arc<Notify>,
    hold: fn(),
) -> StreamAnchor<u32> {
    let anchor = consumer
        .anchor_manager()
        .create_anchor_with_config::<u32>(AnchorConfig {
            heartbeat_interval: Some(HEARTBEAT),
            ..AnchorConfig::default()
        });
    let ticket = consumer.prebind_anchor(anchor.handle()).expect("ticket");
    let sender = producer
        .open_anchor_stream::<u32>(anchor.handle(), ticket)
        .await
        .expect("open stream");
    tokio::spawn(async move {
        sender.send(0).await.expect("send");
        // Item 0 has arrived, so the batcher has drained and parked.
        go.notified().await;
        // Resume from the run queue: tokio turns the LIFO slot back on.
        tokio::task::yield_now().await;
        // This send wakes the parked batcher into this worker's LIFO slot.
        let _ = sender.send(1).await;
        hold();
        // A cut stream refuses this; the reader reports it.
        let _ = sender.finalize();
    });
    anchor
}

/// Every frame of `anchor` to its end. Tells `go` once item 0 has arrived.
async fn read(anchor: &mut StreamAnchor<u32>, go: &Notify) -> Vec<String> {
    let mut frames = Vec::new();
    loop {
        match tokio::time::timeout(HOLD + PATIENCE, anchor.next()).await {
            Ok(Some(Ok(StreamFrame::Item(v)))) => {
                frames.push(format!("item {v}"));
                if v == 0 {
                    go.notify_one();
                }
            }
            Ok(Some(Ok(StreamFrame::Finalized))) => {
                frames.push("finalized".to_string());
                return frames;
            }
            Ok(Some(other)) => {
                frames.push(format!("{other:?}"));
                return frames;
            }
            Ok(None) => return frames,
            Err(_) => panic!("the stream did not end: {frames:?}"),
        }
    }
}

fn whole() -> Vec<String> {
    ["item 0", "item 1", "finalized"].map(String::from).to_vec()
}

/// A producer that holds its worker after a send keeps the peer's batcher
/// from running, and the consumer's watchdog ends the stream as `Dropped`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "demonstration: tokio's scheduling decides whether the batcher is in the held worker's LIFO slot; run with --ignored to see the stall"]
async fn a_held_worker_thread_cuts_the_stream() {
    let (producer, consumer) = pair().await;
    let go = Arc::new(Notify::new());
    let mut anchor = stream(&producer, &consumer, go.clone(), || {
        std::thread::sleep(HOLD)
    })
    .await;
    let frames = read(&mut anchor, &go).await;
    assert_eq!(frames.first().map(String::as_str), Some("item 0"));
    assert_ne!(frames, whole(), "the stream survived the hold");
    assert!(
        frames.last().is_some_and(|f| f.contains("Dropped")),
        "the watchdog ended the stream: {frames:?}"
    );
}

/// The same hold inside `block_in_place`: the batcher runs on another worker,
/// and the stream arrives whole.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn block_in_place_keeps_the_stream() {
    let (producer, consumer) = pair().await;
    let go = Arc::new(Notify::new());
    let mut anchor = stream(&producer, &consumer, go.clone(), || {
        tokio::task::block_in_place(|| std::thread::sleep(HOLD))
    })
    .await;
    assert_eq!(read(&mut anchor, &go).await, whole());
}
