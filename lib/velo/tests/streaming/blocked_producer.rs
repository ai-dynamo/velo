// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! A producer that holds its thread between two sends, over real nodes.
//!
//! A remote sender's heartbeats come from the manager's sweep task, and under
//! the mux one batcher task per peer writes them with every stream's records.
//! Neither is the producer's task. But the producer's `send` wakes the batcher
//! from the producer's own worker thread, and tokio puts a task woken there
//! in that worker's LIFO slot, which no other worker steals and for which no
//! idle worker is woken (tokio-rs/tokio#4941). When the producer then holds
//! its thread, with a long encode, a render, or a blocking call, the batcher
//! waits in that slot until the producer lets go. So every stream from the
//! producer's node to that peer stalls, and the consumer's watchdog ends the
//! producer's own stream after `DETECTION_MULTIPLIER` heartbeat windows, as
//! if the producer were dead.
//!
//! With [`VeloBuilder::mux_runtime`](velo::VeloBuilder::mux_runtime) the mux
//! runs on a runtime of its own, so a producer's wake reaches the batcher
//! through that runtime's remote queue, and an idle worker there runs it.
//!
//! The producer runs in a spawned task: the body of a `#[tokio::test]` runs
//! on the thread that blocks on it, which is not a worker and has no LIFO
//! slot. The heartbeat is 50 ms, so the watchdog fires within about 200 ms of
//! silence, and the producer holds its thread for 600 ms.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::StreamExt;
use velo::Velo;
use velo::streaming::{AnchorConfig, StreamAnchor, StreamFrame};
use velo::transports::tcp::TcpTransportBuilder;

const PATIENCE: Duration = Duration::from_secs(5);
const HEARTBEAT: Duration = Duration::from_millis(50);
/// Twelve heartbeat windows: four times what the watchdog allows.
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

/// A mux-only node, with its mux on `mux` when given.
async fn node(mux: Option<tokio::runtime::Handle>) -> Velo {
    let mut builder = Velo::builder().add_transport(tcp()).mux_only();
    if let Some(mux) = mux {
        builder = builder.mux_runtime(mux);
    }
    builder.build().await.expect("build velo")
}

/// A producer and a consumer that know each other.
async fn pair(mux: Option<tokio::runtime::Handle>) -> (Velo, Velo) {
    let producer = node(mux.clone()).await;
    let consumer = node(mux).await;
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

/// A runtime for the mux, apart from the one the producer runs on. Shut down
/// with `shutdown_background`: a runtime may not drop inside another.
fn mux_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("velo-mux")
        .enable_all()
        .build()
        .expect("mux runtime")
}

/// Open a stream from `producer` to a new anchor on `consumer`, with a short
/// heartbeat, and send it two items on a spawned task. With `hold`, the task
/// holds its thread between the two, and sets `released` after.
async fn stream(
    producer: &Velo,
    consumer: &Velo,
    hold: bool,
    released: Arc<AtomicBool>,
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
        if hold {
            std::thread::sleep(HOLD);
            released.store(true, Ordering::SeqCst);
        }
        // A cut stream refuses these; the reader reports it.
        let _ = sender.send(1).await;
        let _ = sender.finalize();
    });
    anchor
}

/// Every frame of `anchor` to its end.
async fn read(anchor: &mut StreamAnchor<u32>) -> Vec<String> {
    let mut frames = Vec::new();
    loop {
        match tokio::time::timeout(HOLD + PATIENCE, anchor.next()).await {
            Ok(Some(Ok(StreamFrame::Item(v)))) => frames.push(format!("item {v}")),
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

/// A producer that holds its thread for many heartbeat windows keeps its
/// stream when the mux has a runtime of its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_producer_that_holds_its_thread_keeps_its_stream() {
    let mux = mux_runtime();
    let (producer, consumer) = pair(Some(mux.handle().clone())).await;
    let mut anchor = stream(&producer, &consumer, true, Arc::default()).await;
    assert_eq!(read(&mut anchor).await, whole());
    mux.shutdown_background();
}

/// Another stream from the same node to the same peer, opened while a
/// producer holds its thread, does not wait for it to let go. Unstalled it
/// takes a few milliseconds; the check is on the holder's own record of when
/// it let go, not on a clock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn another_stream_to_the_peer_does_not_wait_for_a_held_thread() {
    let mux = mux_runtime();
    let (producer, consumer) = pair(Some(mux.handle().clone())).await;
    let released = Arc::new(AtomicBool::new(false));
    let mut held = stream(&producer, &consumer, true, Arc::clone(&released)).await;
    // The holder's first item is out, so it has sent and holds its thread.
    match tokio::time::timeout(PATIENCE, held.next()).await {
        Ok(Some(Ok(StreamFrame::Item(0)))) => {}
        other => panic!("the holder's first item: {other:?}"),
    }
    let mut other = stream(&producer, &consumer, false, Arc::default()).await;
    assert_eq!(read(&mut other).await, whole());
    assert!(
        !released.load(Ordering::SeqCst),
        "the other stream ended only after the holder let go of its thread"
    );
    mux.shutdown_background();
}

/// The control: with the mux on the producer's runtime, the same hold cuts
/// the stream. This pins the defect `mux_runtime` exists for. If tokio ever
/// lets other workers steal the LIFO slot, this test fails, and the default
/// can be revisited.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_the_producers_runtime_a_held_thread_cuts_the_stream() {
    let (producer, consumer) = pair(None).await;
    let mut anchor = stream(&producer, &consumer, true, Arc::default()).await;
    let frames = read(&mut anchor).await;
    assert_ne!(frames, whole(), "the stream survived the hold");
    assert!(
        frames.last().is_some_and(|f| f.contains("Dropped")),
        "the watchdog ended the stream: {frames:?}"
    );
}
