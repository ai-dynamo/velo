// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Streaming saturation observability tests.
//!
//! Verifies that the cascade of backpressure counters added in the
//! observability commit actually ticks when a producer outpaces a
//! consumer. Without this test, a regression that detached a counter
//! from its hot-path call site would silently make the saturation
//! dashboard go dark — exactly the failure mode this metric set was
//! created to prevent.
//!
//! The cascade we care about:
//!
//!   producer → connect-side tx (4096) → TCP → bind-side tx (4096) →
//!   reader_pump → anchor_frame_tx (256) → consumer
//!
//! When the consumer stops reading, the smallest channel (256-deep
//! anchor_frame_tx) fills first. `reader_pump`'s `try_send` returns
//! `Full` and we tick `velo_streaming_reader_pump_backpressure_total` —
//! the leading indicator. With enough sustained load the cascade walks
//! upward and the server-pump and producer-send counters tick too.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use prometheus::Registry;
use velo::observability::VeloMetrics;
use velo::observability::test_helpers::MetricSnapshot;
use velo::transports::tcp::TcpTransportBuilder;
use velo::*;

fn new_messenger_transport() -> Arc<velo::transports::tcp::TcpTransport> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    Arc::new(
        TcpTransportBuilder::new()
            .from_listener(listener)
            .unwrap()
            .build()
            .unwrap(),
    )
}

async fn make_pair() -> (Arc<Velo>, Registry, Arc<Velo>, Registry) {
    let producer_reg = Registry::new();
    let producer_metrics = Arc::new(VeloMetrics::register(&producer_reg).unwrap());
    let consumer_reg = Registry::new();
    let consumer_metrics = Arc::new(VeloMetrics::register(&consumer_reg).unwrap());

    let producer = Velo::builder()
        .add_transport(new_messenger_transport())
        .metrics(producer_metrics)
        .build()
        .await
        .unwrap();
    let consumer = Velo::builder()
        .add_transport(new_messenger_transport())
        .metrics(consumer_metrics)
        .build()
        .await
        .unwrap();

    producer.register_peer(consumer.peer_info()).unwrap();
    consumer.register_peer(producer.peer_info()).unwrap();

    // Let the messenger TCP connections settle.
    tokio::time::sleep(Duration::from_millis(150)).await;

    (producer, producer_reg, consumer, consumer_reg)
}

/// Saturation reproduction: hold the consumer hostage and pump items at
/// the producer until the leading-indicator counter ticks.
///
/// The consumer creates an anchor but never advances its `next()` past
/// the first frame. The 256-deep anchor channel fills first; the reader
/// pump's try_send fails with `Full` and ticks
/// `velo_streaming_reader_pump_backpressure_total`. Asserts that counter
/// is > 0 on the consumer registry within a short window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reader_pump_backpressure_ticks_under_consumer_starvation() {
    let (producer, _producer_reg, consumer, consumer_reg) = make_pair().await;

    // Consumer side: create the anchor, transfer the handle to the producer,
    // and stall — never call .next(). This pins the per-anchor 256-deep
    // channel so reader_pump fills it on the very first burst from the
    // producer side.
    let mut anchor = consumer.create_anchor::<u64>();
    let handle = anchor.handle();

    // Producer side: attach and pump as fast as possible.
    let sender = producer
        .attach_anchor::<u64>(handle)
        .await
        .expect("attach must succeed when both peers are registered");

    // Pump 8192 items — well past the 256 anchor channel + 4096 transport
    // channel + 4096 connect channel ceiling. The producer side's
    // send_async naturally rate-limits us to whatever the cascade allows,
    // so this loop won't fill memory unboundedly; it just keeps pressure on.
    let producer_task = tokio::spawn(async move {
        for i in 0u64..8192 {
            if sender.send(i).await.is_err() {
                break;
            }
        }
        // Hold the sender alive so the anchor doesn't see Dropped during
        // the assertion window.
        tokio::time::sleep(Duration::from_millis(500)).await;
        sender.finalize().ok();
    });

    // Poll for the counter rather than sleeping a fixed window: slow CI runners
    // may need longer than 300ms for the cascade (producer -> transport ->
    // reader_pump) to fill the bounded(256) anchor channel and tick this counter.
    // Cap at 2s, which is still well under the 15s heartbeat watchdog window
    // so this test never trips the watchdog.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let mut reader_pump_bp = 0.0;
    let mut snap = MetricSnapshot::from_registry(&consumer_reg);
    while std::time::Instant::now() < deadline {
        snap = MetricSnapshot::from_registry(&consumer_reg);
        reader_pump_bp = snap.counter("velo_streaming_reader_pump_backpressure_total", &[]);
        if reader_pump_bp > 0.0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        reader_pump_bp > 0.0,
        "reader_pump backpressure counter must tick when consumer can't keep up within 2s; got {reader_pump_bp}"
    );

    // Watchdog must NOT have fired yet (we slept 300ms; deadline is 15s).
    let watchdog = snap.counter("velo_streaming_heartbeat_watchdog_firings_total", &[]);
    assert_eq!(
        watchdog, 0.0,
        "watchdog should not fire within 300ms (heartbeat default is 5s × 3 = 15s); got {watchdog}"
    );

    // Drain the anchor so the producer task finalizes cleanly.
    while let Some(frame) = anchor.next().await {
        if matches!(frame, Ok(streaming::StreamFrame::Finalized) | Err(_)) {
            break;
        }
    }
    let _ = producer_task.await;
}

/// Sanity check: when neither side is saturating, the new counters stay
/// at zero. This guards against a misconfigured try_send that ticked the
/// counter on the fast path too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_backpressure_counters_in_steady_state() {
    let (producer, producer_reg, consumer, consumer_reg) = make_pair().await;

    let mut anchor = consumer.create_anchor::<u64>();
    let handle = anchor.handle();
    let sender = producer.attach_anchor::<u64>(handle).await.unwrap();

    // Drive a tiny, well-paced exchange: 16 items with a small gap between
    // sends. None of the channels should fill.
    let consumer_task = tokio::spawn(async move {
        let mut got = 0;
        while let Some(frame) = anchor.next().await {
            match frame.unwrap() {
                streaming::StreamFrame::Item(_) => got += 1,
                streaming::StreamFrame::Finalized => break,
                _ => {}
            }
        }
        got
    });

    for i in 0u64..16 {
        sender.send(i).await.unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    sender.finalize().unwrap();

    let got = tokio::time::timeout(Duration::from_secs(5), consumer_task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, 16);

    for (label, reg) in [("producer", &producer_reg), ("consumer", &consumer_reg)] {
        let snap = MetricSnapshot::from_registry(reg);
        for metric in [
            "velo_streaming_reader_pump_backpressure_total",
            "velo_streaming_server_pump_backpressure_total",
            "velo_streaming_producer_send_backpressure_total",
            "velo_streaming_heartbeat_watchdog_firings_total",
        ] {
            let v = snap.counter(metric, &[]);
            assert_eq!(
                v, 0.0,
                "{label}: {metric} must stay 0 in steady state; got {v}"
            );
        }
    }
}
