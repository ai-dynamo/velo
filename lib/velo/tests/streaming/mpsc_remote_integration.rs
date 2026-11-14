// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Cross-worker integration tests for `velo::streaming::mpsc`.
//!
//! Validates the remote attach path: `_mpsc_anchor_attach` AM round-trip,
//! transport bind/connect, per-sender `mpsc_reader_pump`, and the
//! `(sender_id, bytes)` tagging at the anchor side.
//!
//! The nodes carry their streams on the per-stream TCP transport. What is
//! under test is the MPSC control plane, which is transport-agnostic; the
//! transport only has to be a real remote one so the frames genuinely cross a
//! wire.

mod common;

use std::time::Duration;

use futures::StreamExt;
use velo::streaming::{MpscAnchorConfig, MpscFrame, SenderId};
use velo::test_helpers::{
    MpscAnchorAttachRequest, MpscAnchorAttachResponse, MpscAnchorCancelRequest,
    MpscAnchorDetachRequest, StreamCancelHandle,
};
use velo::{AttachError, StreamAnchorHandle};
use velo_ext::WorkerId;

fn roundtrip_handle(handle: StreamAnchorHandle) -> StreamAnchorHandle {
    // Simulate opaque cross-worker transfer via the raw u128 (preserves the
    // MPSC kind bit). `pack` is kind-validated; `from_u128` is not.
    StreamAnchorHandle::from_u128(handle.as_u128())
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_sender_detach_and_drop_preserve_unrelated_local_slots() {
    let (node_a, node_b) = common::tcp_stream_pair().await;
    let remote_anchor = node_a.create_mpsc_anchor::<u32>();
    let local_anchor = node_b.create_mpsc_anchor_with_config::<u32>(MpscAnchorConfig {
        max_senders: Some(2),
        ..Default::default()
    });

    let local_one = node_b
        .attach_mpsc_anchor::<u32>(local_anchor.handle())
        .await
        .unwrap();
    let local_two = node_b
        .attach_mpsc_anchor::<u32>(local_anchor.handle())
        .await
        .unwrap();
    let remote_one = node_b
        .attach_mpsc_anchor::<u32>(remote_anchor.handle())
        .await
        .unwrap();
    let remote_two = node_b
        .attach_mpsc_anchor::<u32>(remote_anchor.handle())
        .await
        .unwrap();
    assert_eq!(local_one.sender_id(), remote_one.sender_id());
    assert_eq!(local_two.sender_id(), remote_two.sender_id());

    remote_one.detach().await.unwrap();
    drop(remote_two);
    assert!(matches!(
        node_b
            .attach_mpsc_anchor::<u32>(local_anchor.handle())
            .await,
        Err(AttachError::MaxSendersReached { .. })
    ));
    local_anchor.cancel();
    remote_anchor.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_sender_cancel_preserves_an_unrelated_local_sender() {
    let (node_a, node_b) = common::tcp_stream_pair().await;
    let remote_anchor = node_a.create_mpsc_anchor::<u32>();
    let mut local_anchor = node_a.create_mpsc_anchor::<u32>();
    let local_sender = node_a
        .attach_mpsc_anchor::<u32>(local_anchor.handle())
        .await
        .unwrap();
    let remote_sender = node_b
        .attach_mpsc_anchor::<u32>(remote_anchor.handle())
        .await
        .unwrap();
    // Sender IDs belong to workers, so both first senders have the same
    // registry key, each in its own worker's registry.
    assert_eq!(local_sender.sender_id(), remote_sender.sender_id());

    remote_anchor.cancel();
    tokio::time::timeout(
        Duration::from_secs(3),
        remote_sender.cancellation_token().cancelled(),
    )
    .await
    .expect("remote sender was not cancelled");
    assert!(!local_sender.cancellation_token().is_cancelled());
    local_sender.send(42).await.unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), local_anchor.next())
            .await
            .unwrap(),
        Some(Ok((_, MpscFrame::Item(42))))
    ));
    local_anchor.cancel();
}

/// Two remote senders attach to one MPSC anchor on a separate worker and
/// deliver items. Each sender gets a distinct `SenderId`, and the anchor
/// reports both.
#[tokio::test(flavor = "multi_thread")]
async fn test_mpsc_remote_multi_attach() {
    let (node_a, node_b) = common::tcp_stream_pair().await;

    let mut anchor = node_a.create_mpsc_anchor::<u32>();
    let handle = anchor.handle();
    let transferred = roundtrip_handle(handle);

    let s1 = node_b
        .attach_mpsc_anchor::<u32>(transferred)
        .await
        .expect("remote attach s1");
    let s2 = node_b
        .attach_mpsc_anchor::<u32>(transferred)
        .await
        .expect("remote attach s2");

    // Sender IDs are anchor-local and start at 1.
    assert_eq!(s1.sender_id(), SenderId(1));
    assert_eq!(s2.sender_id(), SenderId(2));

    for i in 0u32..5 {
        s1.send(i).await.expect("s1 send");
        s2.send(100 + i).await.expect("s2 send");
    }

    // Give concurrent AM dispatch a beat to settle before we start consuming.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut s1_count = 0;
    let mut s2_count = 0;
    let collect = async {
        while s1_count < 5 || s2_count < 5 {
            let Some(frame) = anchor.next().await else {
                break;
            };
            match frame.expect("no stream error") {
                (SenderId(1), MpscFrame::Item(_)) => s1_count += 1,
                (SenderId(2), MpscFrame::Item(_)) => s2_count += 1,
                (sid, MpscFrame::Item(_)) => panic!("unknown sender {sid}"),
                (_, MpscFrame::SenderError(m)) => panic!("sender error: {m}"),
                (_, MpscFrame::Detached | MpscFrame::Dropped(_)) => {}
            }
        }
    };

    tokio::time::timeout(Duration::from_secs(10), collect)
        .await
        .expect("consumer timed out");

    assert_eq!(s1_count, 5);
    assert_eq!(s2_count, 5);

    // Clean up: detach senders, then cancel the anchor.
    let _ = s1.detach().await;
    let _ = s2.detach().await;
    anchor.cancel();
}

/// When one remote sender's pump hits heartbeat timeout (simulated here by
/// dropping the sender without detach/finalize), the consumer sees a
/// `Dropped` event for that sender while the other keeps flowing.
#[tokio::test(flavor = "multi_thread")]
async fn test_mpsc_heartbeat_timeout_per_sender() {
    let (node_a, node_b) = common::tcp_stream_pair().await;

    let mut anchor = node_a.create_mpsc_anchor::<u32>();
    let handle = anchor.handle();
    let transferred = roundtrip_handle(handle);

    let s1 = node_b
        .attach_mpsc_anchor::<u32>(transferred)
        .await
        .expect("attach s1");
    let s2 = node_b
        .attach_mpsc_anchor::<u32>(transferred)
        .await
        .expect("attach s2");

    s1.send(1).await.unwrap();
    s2.send(2).await.unwrap();

    // Give cross-worker AM delivery a moment.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Drop s1 without detach: Drop impl sends a `Dropped` sentinel through
    // the transport; the anchor-side pump forwards it and removes the slot.
    drop(s1);

    // s2 must still work.
    s2.send(3).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut items_s1 = Vec::new();
    let mut items_s2 = Vec::new();
    let mut dropped_s1 = false;
    let collect = async {
        while let Some(frame) = anchor.next().await {
            match frame.expect("stream error") {
                (SenderId(1), MpscFrame::Item(v)) => items_s1.push(v),
                (SenderId(2), MpscFrame::Item(v)) => items_s2.push(v),
                (SenderId(1), MpscFrame::Dropped(_)) => {
                    dropped_s1 = true;
                }
                (SenderId(2), MpscFrame::Dropped(_) | MpscFrame::Detached) => break,
                _ => {}
            }
            if dropped_s1 && items_s2.len() >= 2 {
                break;
            }
        }
    };

    tokio::time::timeout(Duration::from_secs(10), collect)
        .await
        .expect("consumer timed out");

    assert_eq!(items_s1, vec![1u32]);
    assert!(items_s2.contains(&2u32));
    assert!(items_s2.contains(&3u32));
    assert!(dropped_s1, "must see Dropped for sender 1");

    let _ = s2.detach().await;
    anchor.cancel();
}

/// Regression: two senders living on *different* worker_ids both attach to the
/// same MPSC anchor. Each worker's local `next_sender_stream_id` starts at 1,
/// so before the receiver-side `routing_session_id` allocation lands, both
/// `transport.bind`/`transport.connect` calls would collide on the same
/// `(anchor_id, session_id)` routing key — the second bind would silently
/// overwrite the first and one sender's frames would route to the wrong
/// consumer slot (cross-talk) or be lost.
///
/// With the fix, the receiver allocates a unique `routing_session_id` per
/// attach. Each sender's items must reach the anchor under its own `SenderId`,
/// with no crossover.
#[tokio::test(flavor = "multi_thread")]
async fn test_mpsc_remote_two_workers_no_routing_collision() {
    // Three nodes: A hosts the anchor; B and C are two distinct
    // sender-side workers. Each gets its own streaming stack and a fresh
    // `next_sender_stream_id` starting at 0 -- the first attach on each would,
    // without the fix, reuse session_id == 1 on A's transport.
    let [node_a, node_b, node_c] = common::tcp_stream_nodes::<3>().await;

    let mut anchor = node_a.create_mpsc_anchor::<u32>();
    let handle = anchor.handle();
    let transferred = roundtrip_handle(handle);

    let s_b = node_b
        .attach_mpsc_anchor::<u32>(transferred)
        .await
        .expect("worker B attach");
    let s_c = node_c
        .attach_mpsc_anchor::<u32>(transferred)
        .await
        .expect("worker C attach");

    // Per-worker sender_stream_id counters are independent: both senders'
    // local counters yield 1. The regression manifests as one bind clobbering
    // the other in the receiver's transport routing table.
    const N: u32 = 50;
    for i in 0..N {
        s_b.send(i).await.expect("worker B send");
        s_c.send(1000 + i).await.expect("worker C send");
    }

    let mut b_items: Vec<u32> = Vec::new();
    let mut c_items: Vec<u32> = Vec::new();
    let collect = async {
        while b_items.len() < N as usize || c_items.len() < N as usize {
            let Some(frame) = anchor.next().await else {
                break;
            };
            match frame.expect("no stream error") {
                (sid, MpscFrame::Item(v)) => {
                    // The two senders must produce disjoint value ranges --
                    // worker B's items are 0..N, worker C's are 1000..(1000+N).
                    // SenderId is anchor-local, so we cannot test which worker
                    // produced an item by sid alone; the value ranges are the
                    // ground truth, and the SenderId mapping must be 1:1 with
                    // those ranges (no cross-talk).
                    if v < 1000 {
                        b_items.push(v);
                        assert_eq!(sid, SenderId(1), "B's items must all carry SenderId(1)");
                    } else {
                        c_items.push(v);
                        assert_eq!(sid, SenderId(2), "C's items must all carry SenderId(2)");
                    }
                }
                (_, MpscFrame::SenderError(m)) => panic!("sender error: {m}"),
                (_, MpscFrame::Detached | MpscFrame::Dropped(_)) => {}
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(10), collect)
        .await
        .expect("consumer timed out -- routing collision likely lost frames");

    assert_eq!(b_items.len(), N as usize, "all B items must arrive");
    assert_eq!(c_items.len(), N as usize, "all C items must arrive");
    // Sorted comparison: ordering within a single sender is preserved by the
    // pump, but cross-sender ordering is up to AM interleaving.
    b_items.sort_unstable();
    c_items.sort_unstable();
    assert_eq!(b_items, (0..N).collect::<Vec<_>>());
    assert_eq!(c_items, (1000..1000 + N).collect::<Vec<_>>());

    let _ = s_b.detach().await;
    let _ = s_c.detach().await;
    anchor.cancel();
}

/// Remote detach must surface exactly one `Detached` event and release
/// `max_senders` capacity immediately.
#[tokio::test(flavor = "multi_thread")]
async fn test_mpsc_remote_detach_is_exact_once() {
    let (node_a, node_b) = common::tcp_stream_pair().await;

    let config = MpscAnchorConfig {
        max_senders: Some(1),
        ..Default::default()
    };
    let mut anchor = node_a.create_mpsc_anchor_with_config::<u32>(config);
    let handle = roundtrip_handle(anchor.handle());

    let sender = node_b.attach_mpsc_anchor::<u32>(handle).await.unwrap();
    assert_eq!(sender.sender_id(), SenderId(1));
    let returned = sender.detach().await.expect("detach");
    assert_eq!(returned, handle);

    match tokio::time::timeout(Duration::from_secs(2), anchor.next())
        .await
        .expect("detach frame timeout")
    {
        Some(Ok((SenderId(1), MpscFrame::Detached))) => {}
        other => panic!(
            "expected exactly one Detached for sender 1, got {:?}",
            other
        ),
    }

    let s2 = node_b.attach_mpsc_anchor::<u32>(handle).await.unwrap();
    assert_eq!(s2.sender_id(), SenderId(2));
    s2.send(42).await.unwrap();

    match tokio::time::timeout(Duration::from_secs(2), anchor.next())
        .await
        .expect("item frame timeout")
    {
        Some(Ok((SenderId(2), MpscFrame::Item(42)))) => {}
        other => panic!("expected SenderId(2) Item(42), got {:?}", other),
    }

    match tokio::time::timeout(Duration::from_millis(200), anchor.next()).await {
        Err(_) => {}
        other => panic!(
            "unexpected extra frame after clean remote detach: {:?}",
            other
        ),
    }

    let _ = s2.detach().await;
    anchor.cancel();
}

/// Remote drop must surface exactly one `Dropped` event and release
/// `max_senders` capacity immediately.
#[tokio::test(flavor = "multi_thread")]
async fn test_mpsc_remote_drop_is_exact_once() {
    let (node_a, node_b) = common::tcp_stream_pair().await;

    let config = MpscAnchorConfig {
        max_senders: Some(1),
        ..Default::default()
    };
    let mut anchor = node_a.create_mpsc_anchor_with_config::<u32>(config);
    let handle = roundtrip_handle(anchor.handle());

    let sender = node_b.attach_mpsc_anchor::<u32>(handle).await.unwrap();
    assert_eq!(sender.sender_id(), SenderId(1));
    drop(sender);

    match tokio::time::timeout(Duration::from_secs(2), anchor.next())
        .await
        .expect("dropped frame timeout")
    {
        Some(Ok((SenderId(1), MpscFrame::Dropped(None)))) => {}
        other => panic!(
            "expected exactly one Dropped(None) for sender 1, got {:?}",
            other
        ),
    }

    let s2 = node_b.attach_mpsc_anchor::<u32>(handle).await.unwrap();
    assert_eq!(s2.sender_id(), SenderId(2));
    s2.send(99).await.unwrap();

    match tokio::time::timeout(Duration::from_secs(2), anchor.next())
        .await
        .expect("item frame timeout")
    {
        Some(Ok((SenderId(2), MpscFrame::Item(99)))) => {}
        other => panic!("expected SenderId(2) Item(99), got {:?}", other),
    }

    match tokio::time::timeout(Duration::from_millis(200), anchor.next()).await {
        Err(_) => {}
        other => panic!("unexpected extra frame after remote drop: {:?}", other),
    }

    let _ = s2.detach().await;
    anchor.cancel();
}

// ---------------------------------------------------------------------------
// AM handler branch coverage — `_mpsc_anchor_attach` error paths, and the
// currently-client-unused `_mpsc_anchor_detach` / `_mpsc_anchor_cancel`
// handlers. These tests invoke the messenger directly to drive handler
// branches that the public `attach_mpsc_stream_anchor` API protects callers
// from reaching.
// ---------------------------------------------------------------------------

/// The `_mpsc_anchor_attach` handler rejects SPSC handles with a clear
/// reason even if a misbehaving client skips the `attach_mpsc_stream_anchor`
/// client-side check. Exercises the defence-in-depth branch.
#[tokio::test(flavor = "multi_thread")]
async fn test_mpsc_remote_handler_rejects_spsc_handle() {
    let (node_a, node_b) = common::tcp_stream_pair().await;

    // Create an SPSC anchor on A.
    let spsc_anchor = node_a.create_anchor::<u32>();
    let spsc_handle = StreamAnchorHandle::from_u128(spsc_anchor.handle().as_u128());
    let worker_a = spsc_handle.unpack().0;

    // Worker B crafts an MPSC attach request pointing at the SPSC handle and
    // sends it directly via the messenger, bypassing the client-side check.
    let req = MpscAnchorAttachRequest {
        handle: spsc_handle,
        session_id: 9999,
        stream_cancel_handle: StreamCancelHandle::pack(WorkerId::from_u64(99), 9999),
        supported_transport_keys: Vec::new(),
        lane_key: None,
    };
    let response: MpscAnchorAttachResponse = node_b
        .messenger()
        .typed_unary_streaming::<MpscAnchorAttachResponse>("_mpsc_anchor_attach")
        .payload(&req)
        .expect("payload build")
        .worker(worker_a)
        .send()
        .await
        .expect("AM send");

    match response {
        MpscAnchorAttachResponse::Err { reason } => {
            assert!(
                reason.contains("spsc"),
                "rejection must mention kind, got: {reason}"
            );
        }
        MpscAnchorAttachResponse::Ok { .. } => panic!("handler must reject SPSC handle"),
    }
}

/// The `_mpsc_anchor_attach` handler returns `Err` when no MPSC anchor
/// exists at the requested `local_id`.
#[tokio::test(flavor = "multi_thread")]
async fn test_mpsc_remote_handler_anchor_not_found() {
    let (node_a, node_b) = common::tcp_stream_pair().await;
    let worker_a = node_a.instance_id().worker_id();

    // Fabricate an MPSC handle pointing at an id that will never exist.
    let fake = StreamAnchorHandle::pack_mpsc(worker_a, 0xDEAD_BEEF);
    let req = MpscAnchorAttachRequest {
        handle: fake,
        session_id: 1,
        stream_cancel_handle: StreamCancelHandle::pack(WorkerId::from_u64(99), 1),
        supported_transport_keys: Vec::new(),
        lane_key: None,
    };
    let response: MpscAnchorAttachResponse = node_b
        .messenger()
        .typed_unary_streaming::<MpscAnchorAttachResponse>("_mpsc_anchor_attach")
        .payload(&req)
        .expect("payload build")
        .worker(worker_a)
        .send()
        .await
        .expect("AM send");

    match response {
        MpscAnchorAttachResponse::Err { reason } => {
            assert!(reason.contains("not found"), "reason: {reason}");
        }
        MpscAnchorAttachResponse::Ok { .. } => panic!("handler must reject unknown anchor"),
    }
}

/// The `_mpsc_anchor_attach` handler enforces `max_senders` — the remote
/// attach path returns `AttachError::TransportError` wrapping the reason.
#[tokio::test(flavor = "multi_thread")]
async fn test_mpsc_remote_max_senders_enforced_by_handler() {
    let (node_a, node_b) = common::tcp_stream_pair().await;

    let config = MpscAnchorConfig {
        max_senders: Some(1),
        ..Default::default()
    };
    let anchor = node_a.create_mpsc_anchor_with_config::<u32>(config);
    let handle = roundtrip_handle(anchor.handle());

    // First remote attach succeeds.
    let s1 = node_b
        .attach_mpsc_anchor::<u32>(handle)
        .await
        .expect("first attach");
    // Second must hit the max_senders guard inside the handler and surface
    // as TransportError (the remote error path wraps the string reason).
    let result = node_b.attach_mpsc_anchor::<u32>(handle).await;
    match result {
        Err(AttachError::TransportError(e)) => {
            assert!(
                e.to_string().contains("max_senders"),
                "error must mention max_senders, got: {e}"
            );
        }
        other => panic!("expected TransportError(max_senders…), got {other:?}"),
    }

    drop(s1);
    drop(anchor);
}

/// The `_mpsc_anchor_detach` handler removes a specific sender slot from an
/// anchor entry on request. Exercised by sending the AM directly so the
/// currently client-unused handler gets coverage.
#[tokio::test(flavor = "multi_thread")]
async fn test_mpsc_remote_detach_handler() {
    let (node_a, node_b) = common::tcp_stream_pair().await;

    let mut anchor = node_a.create_mpsc_anchor::<u32>();
    let handle = roundtrip_handle(anchor.handle());

    let sender = node_b
        .attach_mpsc_anchor::<u32>(handle)
        .await
        .expect("attach");
    assert_eq!(sender.sender_id(), SenderId(1));
    sender.send(7).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send the detach AM directly (the client-level detach() uses the
    // in-band Detached sentinel instead; this tests the AM handler).
    let req = MpscAnchorDetachRequest {
        handle,
        sender_id: 1,
    };
    let _: () = node_b
        .messenger()
        .typed_unary_streaming::<()>("_mpsc_anchor_detach")
        .payload(&req)
        .expect("payload build")
        .worker(node_a.instance_id().worker_id())
        .send()
        .await
        .expect("AM send");

    // Give the handler a moment to process.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // The sender's slot on the anchor side is gone; a fresh attach must
    // allocate SenderId(2) — proving the previous slot was dropped.
    let s2 = node_b.attach_mpsc_anchor::<u32>(handle).await.unwrap();
    assert_eq!(s2.sender_id(), SenderId(2));
    drop(s2);

    // Drain any pending items + cancel.
    while tokio::time::timeout(Duration::from_millis(50), anchor.next())
        .await
        .is_ok()
    {}
    drop(sender);
    anchor.cancel();
}

/// The `_mpsc_anchor_cancel` handler removes the whole anchor from the
/// registry.
#[tokio::test(flavor = "multi_thread")]
async fn test_mpsc_remote_cancel_handler() {
    let (node_a, node_b) = common::tcp_stream_pair().await;

    let anchor = node_a.create_mpsc_anchor::<u32>();
    let handle = roundtrip_handle(anchor.handle());

    let req = MpscAnchorCancelRequest { handle };
    let _: () = node_b
        .messenger()
        .typed_unary_streaming::<()>("_mpsc_anchor_cancel")
        .payload(&req)
        .expect("payload build")
        .worker(node_a.instance_id().worker_id())
        .send()
        .await
        .expect("AM send");
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Anchor is gone — a fresh attach must get AnchorNotFound.
    let result = node_a.attach_mpsc_anchor::<u32>(handle).await;
    assert!(
        matches!(result, Err(AttachError::AnchorNotFound { .. })),
        "anchor must be removed after cancel AM, got {result:?}"
    );
    drop(anchor);
}

/// `MpscStreamController::cancel` fires `_stream_cancel` AM to each remote
/// sender's worker. The remote senders see their `send` calls fail with
/// `ChannelClosed` shortly after the cancel.
#[tokio::test(flavor = "multi_thread")]
async fn test_mpsc_controller_cancel_propagates_cross_worker() {
    let (node_a, node_b) = common::tcp_stream_pair().await;

    let anchor = node_a.create_mpsc_anchor::<u32>();
    let handle = roundtrip_handle(anchor.handle());
    let controller = anchor.controller();

    let sender = node_b.attach_mpsc_anchor::<u32>(handle).await.unwrap();
    sender.send(1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // A plain thread must still send `_stream_cancel` through A's runtime.
    std::thread::spawn(move || controller.cancel())
        .join()
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(3),
        sender.cancellation_token().cancelled(),
    )
    .await
    .expect("off-runtime cancel must reach the remote sender");

    // The remote sender's cancellation token should be set by now.
    let result = sender.send(2).await;
    assert!(
        matches!(result, Err(velo::SendError::ChannelClosed)),
        "remote sender must see ChannelClosed after cancel, got {result:?}"
    );

    drop(sender);
    drop(anchor);
}

/// A pending cross-worker `anchor.next()` must resolve to `None` promptly when
/// the consumer cancels, even before the remote sender is dropped.
#[tokio::test(flavor = "multi_thread")]
async fn test_mpsc_pending_next_wakes_on_cross_worker_cancel() {
    let (node_a, node_b) = common::tcp_stream_pair().await;

    let anchor = node_a.create_mpsc_anchor::<u32>();
    let handle = roundtrip_handle(anchor.handle());
    let controller = anchor.controller();
    let sender = node_b.attach_mpsc_anchor::<u32>(handle).await.unwrap();

    let next_task = tokio::spawn(async move {
        let mut anchor = anchor;
        anchor.next().await
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    controller.cancel();

    let result = tokio::time::timeout(Duration::from_secs(2), next_task)
        .await
        .expect("pending next() must wake after cross-worker cancel")
        .expect("next task join");
    assert!(
        result.is_none(),
        "cancelled cross-worker anchor.next() must resolve to None, got {result:?}"
    );

    drop(sender);
}
