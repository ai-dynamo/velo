// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Cross-worker integration tests for `velo_streaming::mpsc`.
//!
//! Validates the remote attach path: `_mpsc_anchor_attach` AM round-trip,
//! transport bind/connect, per-sender `mpsc_reader_pump`, and the
//! `(sender_id, bytes)` tagging at the anchor side.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use velo_common::WorkerId;
use velo_messenger::Messenger;
use velo_streaming::control::StreamCancelHandle;
use velo_streaming::mpsc::{
    MpscAnchorAttachRequest, MpscAnchorAttachResponse, MpscAnchorCancelRequest,
    MpscAnchorDetachRequest,
};
use velo_streaming::velo_transport::VeloFrameTransport;
use velo_streaming::{
    AnchorManager, AnchorManagerBuilder, AttachError, FrameTransport, MpscAnchorConfig, MpscFrame,
    SenderId, StreamAnchorHandle,
};
use velo_transports::tcp::TcpTransportBuilder;

fn new_tcp_transport() -> Arc<velo_transports::tcp::TcpTransport> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    Arc::new(
        TcpTransportBuilder::new()
            .from_listener(listener)
            .unwrap()
            .build()
            .unwrap(),
    )
}

async fn make_two_messengers() -> (Arc<Messenger>, Arc<Messenger>) {
    let t1 = new_tcp_transport();
    let t2 = new_tcp_transport();

    let m1 = Messenger::builder()
        .add_transport(t1)
        .build()
        .await
        .expect("create messenger 1");
    let m2 = Messenger::builder()
        .add_transport(t2)
        .build()
        .await
        .expect("create messenger 2");

    let p1 = m1.peer_info();
    let p2 = m2.peer_info();
    m2.register_peer(p1).expect("register m1 on m2");
    m1.register_peer(p2).expect("register m2 on m1");
    tokio::time::sleep(Duration::from_millis(200)).await;

    (m1, m2)
}

async fn make_am(messenger: Arc<Messenger>) -> Arc<AnchorManager> {
    let worker_id = messenger.instance_id().worker_id();
    let vft =
        Arc::new(VeloFrameTransport::new(Arc::clone(&messenger), worker_id, None).expect("VFT"));
    let am: Arc<AnchorManager> = Arc::new(
        AnchorManagerBuilder::default()
            .worker_id(worker_id)
            .transport(Arc::clone(&vft) as Arc<dyn FrameTransport>)
            // Cross-worker cancel AMs need the messenger on the builder (the
            // Velo facade does this at velo/src/lib.rs under "Step 5").
            .messenger(Some(Arc::clone(&messenger)))
            .build()
            .expect("AM build"),
    );
    am.register_handlers(Arc::clone(&messenger))
        .expect("register_handlers");
    am
}

fn roundtrip_handle(handle: StreamAnchorHandle) -> StreamAnchorHandle {
    // Simulate opaque cross-worker transfer via the raw u128 (preserves the
    // MPSC kind bit). `pack` is kind-validated; `from_u128` is not.
    StreamAnchorHandle::from_u128(handle.as_u128())
}

/// Two remote senders attach to one MPSC anchor on a separate worker and
/// deliver items. Each sender gets a distinct `SenderId`, and the anchor
/// reports both.
#[tokio::test(flavor = "multi_thread")]
async fn test_mpsc_remote_multi_attach() {
    let (messenger_a, messenger_b) = make_two_messengers().await;

    let am_a = make_am(messenger_a).await;
    let am_b = make_am(messenger_b).await;

    let mut anchor = am_a.create_mpsc_anchor::<u32>();
    let handle = anchor.handle();
    let transferred = roundtrip_handle(handle);

    let s1 = am_b
        .attach_mpsc_stream_anchor::<u32>(transferred)
        .await
        .expect("remote attach s1");
    let s2 = am_b
        .attach_mpsc_stream_anchor::<u32>(transferred)
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
    let (messenger_a, messenger_b) = make_two_messengers().await;
    let am_a = make_am(messenger_a).await;
    let am_b = make_am(messenger_b).await;

    let mut anchor = am_a.create_mpsc_anchor::<u32>();
    let handle = anchor.handle();
    let transferred = roundtrip_handle(handle);

    let s1 = am_b
        .attach_mpsc_stream_anchor::<u32>(transferred)
        .await
        .expect("attach s1");
    let s2 = am_b
        .attach_mpsc_stream_anchor::<u32>(transferred)
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
        loop {
            let Some(frame) = anchor.next().await else {
                break;
            };
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
    let (messenger_a, messenger_b) = make_two_messengers().await;
    let am_a = make_am(messenger_a.clone()).await;
    let _am_b = make_am(messenger_b.clone()).await;

    // Create an SPSC anchor on A.
    let spsc_anchor = am_a.create_anchor::<u32>();
    let spsc_handle = StreamAnchorHandle::from_u128(spsc_anchor.handle().as_u128());
    let worker_a = spsc_handle.unpack().0;

    // Worker B crafts an MPSC attach request pointing at the SPSC handle and
    // sends it directly via the messenger, bypassing the client-side check.
    let req = MpscAnchorAttachRequest {
        handle: spsc_handle,
        session_id: 9999,
        stream_cancel_handle: StreamCancelHandle::pack(WorkerId::from_u64(99), 9999),
    };
    let response: MpscAnchorAttachResponse = messenger_b
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
    let (messenger_a, messenger_b) = make_two_messengers().await;
    let _am_a = make_am(messenger_a.clone()).await;
    let _am_b = make_am(messenger_b.clone()).await;
    let worker_a = messenger_a.instance_id().worker_id();

    // Fabricate an MPSC handle pointing at an id that will never exist.
    let fake = StreamAnchorHandle::pack_mpsc(worker_a, 0xDEAD_BEEF);
    let req = MpscAnchorAttachRequest {
        handle: fake,
        session_id: 1,
        stream_cancel_handle: StreamCancelHandle::pack(WorkerId::from_u64(99), 1),
    };
    let response: MpscAnchorAttachResponse = messenger_b
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
    let (messenger_a, messenger_b) = make_two_messengers().await;
    let am_a = make_am(messenger_a.clone()).await;
    let am_b = make_am(messenger_b.clone()).await;

    let config = MpscAnchorConfig {
        max_senders: Some(1),
        ..Default::default()
    };
    let anchor = am_a.create_mpsc_anchor_with_config::<u32>(config);
    let handle = roundtrip_handle(anchor.handle());

    // First remote attach succeeds.
    let s1 = am_b
        .attach_mpsc_stream_anchor::<u32>(handle)
        .await
        .expect("first attach");
    // Second must hit the max_senders guard inside the handler and surface
    // as TransportError (the remote error path wraps the string reason).
    let result = am_b.attach_mpsc_stream_anchor::<u32>(handle).await;
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
    let (messenger_a, messenger_b) = make_two_messengers().await;
    let am_a = make_am(messenger_a.clone()).await;
    let am_b = make_am(messenger_b.clone()).await;

    let mut anchor = am_a.create_mpsc_anchor::<u32>();
    let handle = roundtrip_handle(anchor.handle());

    let sender = am_b
        .attach_mpsc_stream_anchor::<u32>(handle)
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
    let _: () = messenger_b
        .typed_unary_streaming::<()>("_mpsc_anchor_detach")
        .payload(&req)
        .expect("payload build")
        .worker(messenger_a.instance_id().worker_id())
        .send()
        .await
        .expect("AM send");

    // Give the handler a moment to process.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // The sender's slot on the anchor side is gone; a fresh attach must
    // allocate SenderId(2) — proving the previous slot was dropped.
    let s2 = am_b.attach_mpsc_stream_anchor::<u32>(handle).await.unwrap();
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
    let (messenger_a, messenger_b) = make_two_messengers().await;
    let am_a = make_am(messenger_a.clone()).await;
    let _am_b = make_am(messenger_b.clone()).await;

    let anchor = am_a.create_mpsc_anchor::<u32>();
    let handle = roundtrip_handle(anchor.handle());

    let req = MpscAnchorCancelRequest { handle };
    let _: () = messenger_b
        .typed_unary_streaming::<()>("_mpsc_anchor_cancel")
        .payload(&req)
        .expect("payload build")
        .worker(messenger_a.instance_id().worker_id())
        .send()
        .await
        .expect("AM send");
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Anchor is gone — a fresh attach must get AnchorNotFound.
    let result = am_a.attach_mpsc_stream_anchor::<u32>(handle).await;
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
    let (messenger_a, messenger_b) = make_two_messengers().await;
    let am_a = make_am(messenger_a.clone()).await;
    let am_b = make_am(messenger_b.clone()).await;

    let anchor = am_a.create_mpsc_anchor::<u32>();
    let handle = roundtrip_handle(anchor.handle());
    let controller = anchor.controller();

    let sender = am_b.attach_mpsc_stream_anchor::<u32>(handle).await.unwrap();
    sender.send(1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Cancel on A fires `_stream_cancel` AM to B.
    controller.cancel();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Remote sender's poison channel should be disconnected by now.
    let result = sender.send(2).await;
    assert!(
        matches!(result, Err(velo_streaming::SendError::ChannelClosed)),
        "remote sender must see ChannelClosed after cancel, got {result:?}"
    );

    drop(sender);
    drop(anchor);
}
