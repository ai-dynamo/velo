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
