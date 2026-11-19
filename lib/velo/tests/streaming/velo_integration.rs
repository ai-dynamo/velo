// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the full control-plane AM dispatch path (TEST-02).
//!
//! Two `Velo` nodes on TCP loopback, with the per-stream TCP transport as the
//! data plane. What is under test is the control plane, which is
//! transport-agnostic:
//! - Worker A creates an anchor.
//! - The handle is transferred to Worker B as a raw u128 (simulating cross-worker serialization).
//! - Worker B attaches with the transferred handle (worker_id mismatch triggers
//!   the remote path): sends `_anchor_attach` AM to Worker A, Worker A's handler binds the transport
//!   and spawns reader_pump, returns the stream endpoint, Worker B connects and receives StreamSender.
//! - Worker B sends N items + finalize via StreamSender.
//! - Worker A's StreamAnchor yields all N items then None.
//!
//! This validates the full stack: remote attach -> _anchor_attach AM dispatch
//! -> reader_pump -> StreamAnchor receives remote frames.

mod common;

use std::time::Duration;

use velo::streaming::TCP_STREAM_KEY;
use velo::{StreamAnchorHandle, StreamFrame};
use velo_ext::WorkerId;

// ---------------------------------------------------------------------------
// TEST-02: Two-worker full AM dispatch integration test
// ---------------------------------------------------------------------------
//
// Validates the full control-plane path:
//   1. Worker A creates an anchor.
//   2. The handle is transferred as u128 to Worker B (simulates cross-worker serialization).
//   3. Worker B calls attach_anchor (remote path fires):
//      - sends _anchor_attach AM to Worker A
//      - Worker A's handler binds transport + spawns reader_pump, returns endpoint
//      - Worker B connects transport and receives StreamSender
//   4. Worker B sends 5 items + finalize via StreamSender.
//   5. Worker A's StreamAnchor yields all 5 items then None.

#[tokio::test(flavor = "multi_thread")]
async fn test_02_remote_attach() {
    let (node_a, node_b) = common::tcp_stream_pair().await;
    let worker_id_a = node_a.instance_id().worker_id();

    let mut anchor_stream = node_a.create_anchor::<u32>();
    let handle = anchor_stream.handle();

    // Simulate cross-worker handle transfer (u128 round-trip)
    let handle_raw: u128 = handle.as_u128();
    let hi = (handle_raw >> 64) as u64;
    let lo = handle_raw as u64;
    let handle_transferred = StreamAnchorHandle::pack(WorkerId::from_u64(hi), lo);

    // Verify transfer
    let (recovered_worker, _) = handle_transferred.unpack();
    assert_eq!(recovered_worker, worker_id_a);

    // Worker B: create the streaming transport and AnchorManager.
    //
    // `Messenger::register_peer` only covers the control plane; the streaming
    // transport keeps its own WorkerId -> SocketAddr cache, so the two sides
    // have to be cross-registered before `connect()` can resolve a peer.
    // `Velo::register_peer` fans this out automatically; this test builds the
    // AnchorManager directly and so does it by hand.
    let sender = node_b
        .attach_anchor::<u32>(handle_transferred)
        .await
        .expect("remote attach must succeed");
    assert_eq!(
        sender.negotiated_transport().map(|key| key.as_str()),
        Some(TCP_STREAM_KEY)
    );

    // Worker B: send 5 items
    for i in 0u32..5 {
        sender.send(i).await.expect("send item");
    }

    sender.finalize().expect("finalize");

    // Worker A: collect items from StreamAnchor until None
    use futures::StreamExt;
    let mut items = Vec::new();
    let collect = async {
        while let Some(frame) = anchor_stream.next().await {
            match frame.expect("no stream error") {
                StreamFrame::Item(v) => items.push(v),
                StreamFrame::Finalized => break,
                other => panic!("unexpected frame: {:?}", other),
            }
        }
        items
    };

    let items = tokio::time::timeout(Duration::from_secs(10), collect)
        .await
        .expect("timed out waiting for Worker A to receive all items");

    assert_eq!(
        items,
        vec![0u32, 1, 2, 3, 4],
        "Worker A must receive all 5 items from Worker B in send order"
    );
}

/// A remote stream that sends nothing for many heartbeat intervals stays
/// alive: the producer's heartbeats keep the consumer's watchdog fed.
///
/// The watchdog ends a stream after `DETECTION_MULTIPLIER` (3) silent
/// intervals. Heartbeats come from one sweep per manager, not from a task
/// per sender, and this test fails if the sweep stops beating an idle
/// remote sender.
#[tokio::test(flavor = "multi_thread")]
async fn a_silent_remote_stream_outlives_the_watchdog_window() {
    use futures::StreamExt;
    use velo::streaming::AnchorConfig;

    let (node_a, node_b) = common::tcp_stream_pair().await;

    let interval = Duration::from_millis(100);
    let mut anchor = node_a.create_anchor_with_config::<u32>(AnchorConfig {
        heartbeat_interval: Some(interval),
        ..Default::default()
    });
    let sender = node_b
        .attach_anchor::<u32>(anchor.handle())
        .await
        .expect("remote attach");

    tokio::time::sleep(interval * 10).await;
    sender.send(7).await.expect("the stream is still open");
    sender.finalize().expect("finalize");

    let frames = tokio::time::timeout(Duration::from_secs(10), async {
        let mut frames = Vec::new();
        while let Some(frame) = anchor.next().await {
            let done = matches!(frame, Ok(StreamFrame::Finalized) | Err(_));
            frames.push(format!("{frame:?}"));
            if done {
                break;
            }
        }
        frames
    })
    .await
    .expect("bounded read");
    assert_eq!(frames, vec!["Ok(Item(7))", "Ok(Finalized)"]);
}
