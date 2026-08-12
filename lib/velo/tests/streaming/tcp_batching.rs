// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! End-to-end coverage for the coalescing egress pump in `TcpFrameTransport`.
//!
//! The streaming write path packs whatever is already queued into a single
//! `write_all` (see `lib/velo/src/streaming/BATCHING.md`). These tests pin the
//! three properties that make that safe:
//!
//! 1. **Ordering** survives coalescing — frames arrive in send order.
//! 2. **Terminal sentinels** still terminate the stream, and nothing sent
//!    before one is lost.
//! 3. **Coalescing actually happens** under load, verified through the
//!    `velo_streaming_{frames_written,socket_writes}_total` ratio.
//!
//! Property 3 is the one that would silently rot: an ordering-only test passes
//! just as happily when every frame gets its own syscall, which is the
//! behaviour this work exists to remove.
//!
//! These go through `Velo::builder()` rather than assembling an `AnchorManager`
//! by hand, because `Velo::register_peer` is what fans peer registration out to
//! the streaming transport — registering only on the `Messenger` leaves
//! `TcpFrameTransport::connect` unable to resolve the peer.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use prometheus::Registry;
use velo::observability::VeloMetrics;
use velo::observability::test_helpers::MetricSnapshot;
use velo::streaming::StreamFrame;
use velo::transports::tcp::TcpTransportBuilder;
use velo::{StreamConfig, TcpConfig, Velo};

/// Build a `Velo` node on TCP loopback with a TCP streaming data plane and its
/// own Prometheus registry.
async fn make_node() -> (Arc<Velo>, Registry) {
    let registry = Registry::new();
    let metrics = Arc::new(VeloMetrics::register(&registry).expect("metrics"));

    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let transport = Arc::new(
        TcpTransportBuilder::new()
            .from_listener(listener)
            .expect("from_listener")
            .build()
            .expect("build tcp transport"),
    );

    let node = Velo::builder()
        .add_transport(transport)
        .stream_config(StreamConfig::Tcp(Some(TcpConfig {
            bind_addr: std::net::Ipv4Addr::LOCALHOST.into(),
        })))
        .expect("stream_config")
        .metrics(Arc::clone(&metrics))
        .build()
        .await
        .expect("build velo");

    (node, registry)
}

/// Two nodes registered with each other. Returns `(consumer, producer,
/// producer_registry)` — the producer's registry is where the egress-side
/// batching counters land.
async fn make_pair() -> (Arc<Velo>, Arc<Velo>, Registry) {
    let (consumer, _consumer_registry) = make_node().await;
    let (producer, producer_registry) = make_node().await;

    // `Velo::register_peer` (not `Messenger::register_peer`) also registers the
    // peer on the streaming transport, which `connect()` needs.
    consumer
        .register_peer(producer.peer_info())
        .expect("register producer on consumer");
    producer
        .register_peer(consumer.peer_info())
        .expect("register consumer on producer");

    tokio::time::sleep(Duration::from_millis(200)).await;

    (consumer, producer, producer_registry)
}

/// Drain an anchor until `Finalized`, returning the items in arrival order.
async fn collect_items(
    anchor: &mut velo::streaming::StreamAnchor<u32>,
    expected: usize,
) -> Vec<u32> {
    let mut items = Vec::with_capacity(expected);
    let drain = async {
        while let Some(frame) = anchor.next().await {
            match frame.expect("no stream error") {
                StreamFrame::Item(v) => items.push(v),
                StreamFrame::Finalized => break,
                other => panic!("unexpected frame: {other:?}"),
            }
        }
        items
    };
    tokio::time::timeout(Duration::from_secs(30), drain)
        .await
        .expect("timed out draining anchor")
}

/// A burst of frames sent back-to-back must arrive complete and in order.
///
/// This is the shape the batching work targets: a producer emitting many frames
/// with no `.await` on anything else between them, so they are all sitting in
/// the channel by the time the egress pump wakes.
#[tokio::test(flavor = "multi_thread")]
async fn burst_preserves_order_and_completeness() {
    const N: u32 = 5_000;

    let (consumer, producer, _reg) = make_pair().await;

    let mut anchor = consumer.create_anchor::<u32>();
    let handle = anchor.handle();

    let sender = producer
        .attach_anchor::<u32>(handle)
        .await
        .expect("remote attach over tcp-stream");

    tokio::spawn(async move {
        for i in 0..N {
            sender.send(i).await.expect("send");
        }
        sender.finalize().expect("finalize");
    });

    let items = collect_items(&mut anchor, N as usize).await;

    assert_eq!(items.len(), N as usize, "every frame must arrive");
    assert!(
        items.iter().copied().eq(0..N),
        "coalescing must preserve send order"
    );
}

/// Under a back-to-back burst the pump must actually coalesce — otherwise the
/// change is inert and the ordering test above would still pass.
///
/// The assertion is deliberately weak (>1.5 frames per write rather than a
/// specific ratio): the exact packing depends on scheduler timing, and a test
/// that pins it would be flaky. What must not happen is a ratio of ~1.0, which
/// means every frame still costs its own syscall.
#[tokio::test(flavor = "multi_thread")]
async fn burst_coalesces_writes() {
    const N: u32 = 20_000;

    let (consumer, producer, producer_registry) = make_pair().await;

    let mut anchor = consumer.create_anchor::<u32>();
    let handle = anchor.handle();

    let sender = producer
        .attach_anchor::<u32>(handle)
        .await
        .expect("remote attach over tcp-stream");

    tokio::spawn(async move {
        for i in 0..N {
            sender.send(i).await.expect("send");
        }
        sender.finalize().expect("finalize");
    });

    let items = collect_items(&mut anchor, N as usize).await;
    assert_eq!(items.len(), N as usize);

    let snap = MetricSnapshot::from_registry(&producer_registry);
    let frames = snap.counter("velo_streaming_frames_written_total", &[]);
    let writes = snap.counter("velo_streaming_socket_writes_total", &[]);

    assert!(writes > 0.0, "producer must have issued socket writes");
    assert!(
        frames >= N as f64,
        "expected at least {N} frames written, saw {frames}"
    );

    let ratio = frames / writes;
    // Printed so `--nocapture` reports the achieved packing, which is the
    // number worth watching as the workload shape changes.
    eprintln!("batching ratio: {frames} frames / {writes} writes = {ratio:.2} frames per syscall");
    assert!(
        ratio > 1.5,
        "expected write coalescing under a back-to-back burst, but got \
         {frames} frames in {writes} writes (ratio {ratio:.2}). A ratio near \
         1.0 means every frame still costs its own syscall."
    );
}

/// A terminal sentinel that lands in the same batch as data frames must still
/// terminate the stream, and must not swallow the frames queued ahead of it.
///
/// This is the case the pump's terminal handling has to get right: it appends
/// the terminal to the current batch, flushes, and only then stops — rather
/// than stopping with staged frames still unwritten.
#[tokio::test(flavor = "multi_thread")]
async fn terminal_in_same_batch_flushes_preceding_frames() {
    const N: u32 = 512;

    let (consumer, producer, _reg) = make_pair().await;

    let mut anchor = consumer.create_anchor::<u32>();
    let handle = anchor.handle();

    let sender = producer
        .attach_anchor::<u32>(handle)
        .await
        .expect("remote attach over tcp-stream");

    // No await between the sends and the finalize, so the terminal is very
    // likely to be staged alongside the data in one batch.
    tokio::spawn(async move {
        for i in 0..N {
            sender.send(i).await.expect("send");
        }
        sender.finalize().expect("finalize");
    });

    let items = collect_items(&mut anchor, N as usize).await;

    assert_eq!(
        items.len(),
        N as usize,
        "frames staged ahead of the terminal must not be discarded"
    );
    assert!(items.iter().copied().eq(0..N), "order preserved");
}

/// The LLM forward-pass shape: X concurrent streams, each emitting exactly one
/// frame per pass. This is the workload the batching work exists for, and it is
/// the case per-stream coalescing **cannot** help with.
///
/// Per-stream coalescing only packs frames that are queued on the *same*
/// stream. A forward pass puts one frame on each of X different streams, so
/// each stream's egress pump wakes with exactly one frame and the ratio stays
/// near 1.0 — one syscall and one TCP segment per token, which is precisely the
/// cost the multiplexed protocol removes by bucketing on destination worker
/// rather than on stream.
///
/// This test asserts the *limitation*, not a win. It exists so the boundary
/// between what P2 (coalescing) and P4 (multiplexing) each buy is measured
/// rather than assumed — and so it fails loudly if someone later claims
/// coalescing alone solved this. See `streaming/BATCHING.md`.
#[tokio::test(flavor = "multi_thread")]
async fn forward_pass_shape_does_not_coalesce_per_stream() {
    const STREAMS: usize = 32;
    const PASSES: u32 = 100;

    let (consumer, producer, producer_registry) = make_pair().await;

    let mut anchors = Vec::with_capacity(STREAMS);
    let mut senders = Vec::with_capacity(STREAMS);
    for _ in 0..STREAMS {
        let anchor = consumer.create_anchor::<u32>();
        let handle = anchor.handle();
        let sender = producer
            .attach_anchor::<u32>(handle)
            .await
            .expect("remote attach");
        anchors.push(anchor);
        senders.push(sender);
    }

    // One token per stream per pass, exactly as a decode step emits them.
    let drive = tokio::spawn(async move {
        for pass in 0..PASSES {
            for sender in &senders {
                sender.send(pass).await.expect("send");
            }
            // Yield between passes so the egress pumps drain, mirroring the
            // real gap between forward passes.
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        for sender in senders {
            sender.finalize().expect("finalize");
        }
    });

    for (idx, anchor) in anchors.iter_mut().enumerate() {
        let items = collect_items(anchor, PASSES as usize).await;
        assert_eq!(items.len(), PASSES as usize, "stream {idx} lost frames");
    }
    drive.await.expect("driver");

    let snap = MetricSnapshot::from_registry(&producer_registry);
    let frames = snap.counter("velo_streaming_frames_written_total", &[]);
    let writes = snap.counter("velo_streaming_socket_writes_total", &[]);
    let ratio = frames / writes;
    eprintln!(
        "forward-pass shape: {frames} frames / {writes} writes = {ratio:.2} \
         frames per syscall across {STREAMS} streams"
    );

    // The point of the test: coalescing does essentially nothing here, because
    // the frames are spread across streams rather than queued on one. If this
    // ever climbs materially above ~1, the send path changed shape and the
    // multiplexing rationale should be re-measured.
    assert!(
        ratio < 1.5,
        "expected per-stream coalescing to be ineffective for the \
         one-frame-per-stream-per-pass shape, but got {ratio:.2}. If this is \
         genuinely higher now, re-evaluate whether multiplexing is still needed."
    );
}

/// Many concurrent streams between the same pair of nodes. Each still gets its
/// own connection today; this pins that per-stream ordering is independent and
/// that nothing is cross-delivered between streams.
///
/// It is also the fixture the multiplexed protocol will reuse — under mux these
/// streams share one connection, and this test is what proves frames still land
/// on the right anchor.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_streams_stay_independent() {
    const STREAMS: usize = 24;
    const N: u32 = 200;

    let (consumer, producer, _reg) = make_pair().await;

    let mut anchors = Vec::with_capacity(STREAMS);
    let mut senders = Vec::with_capacity(STREAMS);
    for _ in 0..STREAMS {
        let anchor = consumer.create_anchor::<u32>();
        let handle = anchor.handle();
        let sender = producer
            .attach_anchor::<u32>(handle)
            .await
            .expect("remote attach");
        anchors.push(anchor);
        senders.push(sender);
    }

    // Each stream sends a disjoint range so a cross-delivery is detectable.
    for (idx, sender) in senders.into_iter().enumerate() {
        let base = (idx as u32) * 1_000;
        tokio::spawn(async move {
            for i in 0..N {
                sender.send(base + i).await.expect("send");
            }
            sender.finalize().expect("finalize");
        });
    }

    for (idx, anchor) in anchors.iter_mut().enumerate() {
        let base = (idx as u32) * 1_000;
        let items = collect_items(anchor, N as usize).await;
        assert_eq!(items.len(), N as usize, "stream {idx} lost frames");
        assert!(
            items.iter().copied().eq(base..base + N),
            "stream {idx} received out-of-order or cross-delivered frames"
        );
    }
}
