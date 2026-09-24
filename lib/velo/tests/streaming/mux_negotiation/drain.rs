// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! A stream attached before `begin_drain` keeps flowing through the drain.
//!
//! The drain gate refuses new inbound requests so the node can finish the work
//! it already accepted. A stream that is already open is accepted work: the
//! generation behind it is the in-flight request a rolling restart waits for.
//! The per-stream transport never passes the messenger gate, so it keeps
//! flowing by construction. The mux carries both its records and its credit
//! grants as active messages, so the gate has to let them through by handler
//! name. These tests pin that a drain on either end does not cut an open mux
//! stream.

use super::*;

/// Attach over whatever `consumer` and `producer` negotiate, put `draining`
/// into its drain, then stream `count` items — far more than the window, so
/// the credit return path is exercised too — and require every one to arrive.
async fn stream_across_drain(consumer: &Node, producer: &Node, draining: &Node, count: u32) {
    let mut anchor = consumer.velo.create_anchor::<u32>();
    let sender = producer
        .velo
        .attach_anchor::<u32>(transfer(anchor.handle()))
        .await
        .expect("remote attach");

    draining.velo.begin_drain();

    let send = tokio::spawn(async move {
        for n in 0..count {
            sender.send(n).await.expect("send item");
        }
        sender.finalize().expect("finalize");
    });
    let collect = async {
        let mut items = Vec::with_capacity(count as usize);
        while let Some(frame) = anchor.next().await {
            match frame.expect("no stream error") {
                StreamFrame::Item(value) => items.push(value),
                StreamFrame::Finalized => break,
                other => panic!("unexpected frame: {other:?}"),
            }
        }
        items
    };
    let items = tokio::time::timeout(Duration::from_secs(10), collect)
        .await
        .expect("the stream stopped during the drain");
    send.await.expect("send task");
    assert_eq!(items, (0..count).collect::<Vec<_>>());
}

/// Control: the per-stream transport does not pass the messenger gate.
#[tokio::test(flavor = "multi_thread")]
async fn a_per_stream_transport_flows_through_the_consumers_drain() {
    let (consumer, producer) = pair(None, None).await;
    stream_across_drain(&consumer, &producer, &consumer, 64).await;
    assert_eq!(consumer.attaches_over(LEGACY_KEY), 1.0);
}

/// Records reach a consumer that is draining.
#[tokio::test(flavor = "multi_thread")]
async fn a_mux_stream_flows_through_the_consumers_drain() {
    let (consumer, producer) = pair(Some(mux_config()), Some(mux_config())).await;
    stream_across_drain(&consumer, &producer, &consumer, 64).await;
    assert_eq!(consumer.attaches_over(MUX_KEY), 1.0);
}

/// Credit reaches a producer that is draining.
#[tokio::test(flavor = "multi_thread")]
async fn a_mux_stream_flows_through_the_producers_drain() {
    let (consumer, producer) = pair(Some(mux_config()), Some(mux_config())).await;
    stream_across_drain(&consumer, &producer, &producer, 64).await;
    assert_eq!(consumer.attaches_over(MUX_KEY), 1.0);
}

/// A record too large for a batch is staged through rendezvous, and the
/// consumer pulls it from the producer with `_rv_*` messages. Those serve a
/// record the producer already sent, so they pass a producer's drain too.
#[tokio::test(flavor = "multi_thread")]
async fn a_large_mux_record_flows_through_the_producers_drain() {
    large_records(true).await;
}

/// Control: the same large records with no drain.
#[tokio::test(flavor = "multi_thread")]
async fn a_large_mux_record_flows_without_a_drain() {
    large_records(false).await;
}

/// Each record is above the messenger's 256 KiB staging threshold, and all of
/// them fit the 1 MiB slot byte budget: a producer that outruns that budget
/// loses the slot by design, drain or not.
async fn large_records(drain: bool) {
    const ITEMS: usize = 3;
    const SIZE: usize = 300 * 1024;
    let (consumer, producer) = pair(Some(mux_config()), Some(mux_config())).await;
    let mut anchor = consumer.velo.create_anchor::<Vec<u8>>();
    let sender = producer
        .velo
        .attach_anchor::<Vec<u8>>(transfer(anchor.handle()))
        .await
        .expect("remote attach");
    if drain {
        producer.velo.begin_drain();
    }

    let send = tokio::spawn(async move {
        for n in 0..ITEMS {
            sender.send(vec![n as u8; SIZE]).await.expect("send item");
        }
        sender.finalize().expect("finalize");
    });
    let collect = async {
        let mut items = Vec::new();
        while let Some(frame) = anchor.next().await {
            match frame.expect("no stream error") {
                StreamFrame::Item(value) => items.push(value),
                StreamFrame::Finalized => break,
                other => panic!("unexpected frame: {other:?}"),
            }
        }
        items
    };
    let items = tokio::time::timeout(Duration::from_secs(10), collect)
        .await
        .expect("a large record stopped the stream during the drain");
    send.await.expect("send task");
    assert_eq!(items.len(), ITEMS);
    for (n, item) in items.iter().enumerate() {
        assert_eq!(item.len(), SIZE);
        assert!(item.iter().all(|b| *b == n as u8));
    }
}

/// MPSC streams negotiate the mux too, so the control messages that serve an
/// open MPSC stream pass the drain like the SPSC ones. No client sends
/// `_mpsc_anchor_detach` today, so the test sends it through the messenger. It
/// shows that the gate passes the message; the handler is idempotent and
/// answers the same for a sender that never attached.
#[tokio::test(flavor = "multi_thread")]
async fn an_mpsc_detach_reaches_a_draining_consumer() {
    let (consumer, producer) = pair(Some(mux_config()), Some(mux_config())).await;
    let anchor = consumer.velo.create_mpsc_anchor::<u32>();
    let handle = transfer(anchor.handle());
    consumer.velo.begin_drain();

    let request = velo::streaming::mpsc::MpscAnchorDetachRequest {
        handle,
        sender_id: 1,
    };
    tokio::time::timeout(
        PATIENCE,
        producer
            .velo
            .messenger()
            .typed_unary_streaming::<()>("_mpsc_anchor_detach")
            .payload(&request)
            .expect("payload")
            .worker(consumer.worker_id())
            .send(),
    )
    .await
    .expect("the detach answered")
    .expect("a draining consumer refused the detach of an open stream");
}

/// The drain counts each admitted stream message while its handler runs, so a
/// producer that keeps sending keeps the drain waiting. Under
/// `ShutdownPolicy::Timeout` the call is still bounded.
#[tokio::test(flavor = "multi_thread")]
async fn a_timed_shutdown_is_bounded_while_a_stream_flows() {
    let (consumer, producer) = pair(Some(mux_config()), Some(mux_config())).await;
    let mut anchor = consumer.velo.create_anchor::<u32>();
    let sender = producer
        .velo
        .attach_anchor::<u32>(transfer(anchor.handle()))
        .await
        .expect("remote attach");
    let stop = tokio_util::sync::CancellationToken::new();
    let producing = {
        let stop = stop.clone();
        tokio::spawn(async move {
            let mut n = 0u32;
            while !stop.is_cancelled() {
                if sender.send(n).await.is_err() {
                    break;
                }
                n = n.wrapping_add(1);
            }
        })
    };
    let draining = tokio::spawn(async move { while anchor.next().await.is_some() {} });
    tokio::time::sleep(Duration::from_millis(200)).await;

    tokio::time::timeout(
        Duration::from_secs(5),
        consumer
            .velo
            .graceful_shutdown(velo::ShutdownPolicy::Timeout(Duration::from_millis(500))),
    )
    .await
    .expect("graceful shutdown under a timeout policy did not return");
    stop.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), producing).await;
    draining.abort();
}

/// A consumer that cancels its anchor tells the producer with `_stream_cancel`.
/// A draining producer must still hear it; otherwise it keeps producing for a
/// consumer that left, until teardown.
#[tokio::test(flavor = "multi_thread")]
async fn a_draining_producer_hears_the_consumers_cancel() {
    let (consumer, producer) = pair(Some(mux_config()), Some(mux_config())).await;
    let anchor = consumer.velo.create_anchor::<u32>();
    let sender = producer
        .velo
        .attach_anchor::<u32>(transfer(anchor.handle()))
        .await
        .expect("remote attach");
    let cancelled = sender.cancellation_token();
    producer.velo.begin_drain();
    let _controller = anchor.cancel();
    tokio::time::timeout(PATIENCE, cancelled.cancelled())
        .await
        .expect("a draining producer never heard the consumer's cancel");
}
