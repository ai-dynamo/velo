// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Local integration tests for `velo::streaming::mpsc`.
//!
//! No messenger required — every attach is same-worker, so the AnchorManager
//! can be driven entirely through `create_mpsc_anchor*` / `attach_mpsc_stream_anchor`.

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common::MockFrameTransport;
use futures::StreamExt;
use velo::streaming::{
    AnchorKind, AnchorManager, AttachError, MpscAnchorConfig, MpscFrame, MpscStreamAnchor, SenderId,
};
use velo_ext::WorkerId;

fn make_manager() -> Arc<AnchorManager> {
    Arc::new(AnchorManager::new(
        WorkerId::from_u64(1),
        Arc::new(MockFrameTransport::new()),
    ))
}

/// Pull frames with a hard timeout so tests never hang when we've miscounted
/// expected events.
async fn next_frame<T: serde::de::DeserializeOwned>(
    anchor: &mut MpscStreamAnchor<T>,
) -> Option<Result<(SenderId, MpscFrame<T>), velo::streaming::StreamError>> {
    match tokio::time::timeout(Duration::from_secs(3), anchor.next()).await {
        Ok(v) => v,
        Err(_) => panic!("stream stalled: no frame in 3s"),
    }
}

/// Test 1: two interleaved senders — both sender_ids appear with correct attribution.
#[tokio::test(flavor = "multi_thread")]
async fn test_mpsc_two_local_senders_interleaved() {
    let mgr = make_manager();
    let mut anchor = mgr.create_mpsc_anchor::<u32>();
    let handle = anchor.handle();

    let s1 = mgr
        .attach_mpsc_stream_anchor::<u32>(handle)
        .await
        .expect("attach s1");
    let s2 = mgr
        .attach_mpsc_stream_anchor::<u32>(handle)
        .await
        .expect("attach s2");
    assert_eq!(s1.sender_id(), SenderId(1));
    assert_eq!(s2.sender_id(), SenderId(2));

    for i in 0u32..5 {
        s1.send(i).await.expect("s1 send");
        s2.send(100 + i).await.expect("s2 send");
    }

    let mut s1_items = Vec::new();
    let mut s2_items = Vec::new();
    let mut remaining = 10;
    while remaining > 0 {
        match next_frame(&mut anchor).await.expect("frame") {
            Ok((SenderId(1), MpscFrame::Item(v))) => {
                s1_items.push(v);
                remaining -= 1;
            }
            Ok((SenderId(2), MpscFrame::Item(v))) => {
                s2_items.push(v);
                remaining -= 1;
            }
            Ok((sid, MpscFrame::Item(v))) => panic!("unexpected sender {sid} item {v}"),
            Ok((sid, MpscFrame::Detached | MpscFrame::Dropped(_))) => {
                panic!("unexpected early exit from {sid}");
            }
            Ok((_, MpscFrame::SenderError(m))) => panic!("unexpected sender error: {m}"),
            Err(e) => panic!("stream error: {e}"),
        }
    }
    assert_eq!(s1_items, (0u32..5).collect::<Vec<_>>());
    assert_eq!(s2_items, (100u32..105).collect::<Vec<_>>());

    anchor.cancel();
}

/// Test 2: dropping one of two senders is non-terminal — the other keeps flowing.
#[tokio::test(flavor = "multi_thread")]
async fn test_mpsc_sender_drop_not_terminal() {
    let mgr = make_manager();
    let mut anchor = mgr.create_mpsc_anchor::<u32>();
    let handle = anchor.handle();

    let s1 = mgr.attach_mpsc_stream_anchor::<u32>(handle).await.unwrap();
    let s2 = mgr.attach_mpsc_stream_anchor::<u32>(handle).await.unwrap();

    s1.send(1).await.unwrap();
    s2.send(2).await.unwrap();

    // Drop s1 → produces a Dropped event for SenderId(1). Anchor stays alive.
    drop(s1);

    // s2 is still usable.
    s2.send(3).await.unwrap();
    s2.send(4).await.unwrap();

    // Consume 4 items + 1 Dropped event for SenderId(1).
    let mut items_by_sender: HashMap<SenderId, Vec<u32>> = HashMap::new();
    let mut dropped_sids: Vec<SenderId> = Vec::new();
    let mut items_seen = 0;
    while items_seen < 4 || dropped_sids.is_empty() {
        match next_frame(&mut anchor).await.expect("frame") {
            Ok((sid, MpscFrame::Item(v))) => {
                items_by_sender.entry(sid).or_default().push(v);
                items_seen += 1;
            }
            Ok((sid, MpscFrame::Dropped(_))) => dropped_sids.push(sid),
            Ok((sid, MpscFrame::Detached)) => panic!("unexpected Detached from {sid}"),
            Ok((sid, MpscFrame::SenderError(m))) => panic!("sender error {sid}: {m}"),
            Err(e) => panic!("stream error: {e}"),
        }
    }
    assert_eq!(
        items_by_sender.get(&SenderId(1)).map(|v| v.as_slice()),
        Some(&[1u32][..])
    );
    assert_eq!(
        items_by_sender.get(&SenderId(2)).map(|v| v.as_slice()),
        Some(&[2u32, 3, 4][..])
    );
    assert_eq!(dropped_sids, vec![SenderId(1)]);

    // Anchor is still alive; new senders can attach.
    let s3 = mgr.attach_mpsc_stream_anchor::<u32>(handle).await.unwrap();
    assert_eq!(s3.sender_id(), SenderId(3));
    drop(s3);
    drop(s2);
    anchor.cancel();
}

/// Test 3: detach returns handle; a fresh attach allocates a new SenderId.
#[tokio::test(flavor = "multi_thread")]
async fn test_mpsc_sender_detach_reattach() {
    let mgr = make_manager();
    let mut anchor = mgr.create_mpsc_anchor::<u32>();
    let handle = anchor.handle();

    let s1 = mgr.attach_mpsc_stream_anchor::<u32>(handle).await.unwrap();
    assert_eq!(s1.sender_id(), SenderId(1));
    s1.send(10).await.unwrap();
    let returned = s1.detach().await.expect("detach");
    assert_eq!(returned, handle);

    let s2 = mgr
        .attach_mpsc_stream_anchor::<u32>(returned)
        .await
        .unwrap();
    assert_eq!(
        s2.sender_id(),
        SenderId(2),
        "reattach must allocate fresh id"
    );
    s2.send(20).await.unwrap();
    drop(s2);

    // Expect: Item(SenderId(1), 10), Detached(SenderId(1)), Item(SenderId(2), 20),
    // Dropped(SenderId(2)). Order of the first two may vary.
    let mut got_item1 = false;
    let mut got_item2 = false;
    let mut got_detached1 = false;
    let mut got_dropped2 = false;
    while !(got_item1 && got_item2 && got_detached1 && got_dropped2) {
        match next_frame(&mut anchor).await.expect("frame") {
            Ok((SenderId(1), MpscFrame::Item(10))) => got_item1 = true,
            Ok((SenderId(2), MpscFrame::Item(20))) => got_item2 = true,
            Ok((SenderId(1), MpscFrame::Detached)) => got_detached1 = true,
            Ok((SenderId(2), MpscFrame::Dropped(_))) => got_dropped2 = true,
            Ok(other) => panic!("unexpected frame: {:?}", other),
            Err(e) => panic!("stream error: {e}"),
        }
    }
    anchor.cancel();
}

/// Test 4: all senders gone + unattached timeout ⇒ stream eventually ends.
#[tokio::test(flavor = "multi_thread")]
async fn test_mpsc_all_senders_gone_timeout() {
    let mgr = make_manager();
    let config = MpscAnchorConfig {
        unattached_timeout: Some(Duration::from_millis(100)),
        ..Default::default()
    };
    let mut anchor = mgr.create_mpsc_anchor_with_config::<u32>(config);
    let handle = anchor.handle();

    let s1 = mgr.attach_mpsc_stream_anchor::<u32>(handle).await.unwrap();
    s1.send(42).await.unwrap();
    drop(s1);

    // Drain whatever the sender produced; expect the stream to eventually
    // yield None after the unattached-timeout auto-removes the anchor.
    let mut items = Vec::new();
    let mut terminated = false;
    for _ in 0..20 {
        match tokio::time::timeout(Duration::from_secs(2), anchor.next()).await {
            Ok(Some(Ok((_sid, MpscFrame::Item(v))))) => items.push(v),
            Ok(Some(Ok((_, MpscFrame::Dropped(_))))) => {}
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(e))) => panic!("stream error: {e}"),
            Ok(None) => {
                terminated = true;
                break;
            }
            Err(_) => panic!("stream did not terminate after unattached_timeout"),
        }
    }
    assert!(terminated, "stream must have yielded None");
    assert_eq!(items, vec![42u32]);
}

/// Test 5: max_senders cap enforced.
#[tokio::test(flavor = "multi_thread")]
async fn test_mpsc_max_senders() {
    let mgr = make_manager();
    let config = MpscAnchorConfig {
        max_senders: Some(2),
        ..Default::default()
    };
    let anchor = mgr.create_mpsc_anchor_with_config::<u32>(config);
    let handle = anchor.handle();

    let _s1 = mgr.attach_mpsc_stream_anchor::<u32>(handle).await.unwrap();
    let _s2 = mgr.attach_mpsc_stream_anchor::<u32>(handle).await.unwrap();
    let result = mgr.attach_mpsc_stream_anchor::<u32>(handle).await;
    assert!(
        matches!(result, Err(AttachError::MaxSendersReached { limit: 2, .. })),
        "expected MaxSendersReached, got {result:?}"
    );
    assert_eq!(mgr.sender_registry.senders.len(), 2);
}

/// Test 6: controller.cancel() cancels every attached sender.
#[tokio::test(flavor = "multi_thread")]
async fn test_mpsc_controller_cancel_propagates() {
    let mgr = make_manager();
    let anchor = mgr.create_mpsc_anchor::<u32>();
    let handle = anchor.handle();
    let controller = anchor.controller();

    let s1 = mgr.attach_mpsc_stream_anchor::<u32>(handle).await.unwrap();
    let s2 = mgr.attach_mpsc_stream_anchor::<u32>(handle).await.unwrap();

    controller.cancel();

    // Give the cancellation a beat to propagate.
    tokio::time::sleep(Duration::from_millis(20)).await;

    assert!(
        matches!(
            s1.send(1).await,
            Err(velo::streaming::SendError::ChannelClosed)
        ),
        "s1 send must fail after cancel"
    );
    assert!(
        matches!(
            s2.send(2).await,
            Err(velo::streaming::SendError::ChannelClosed)
        ),
        "s2 send must fail after cancel"
    );

    // Re-attach must fail (anchor removed from registry).
    let result = mgr.attach_mpsc_stream_anchor::<u32>(handle).await;
    assert!(
        matches!(result, Err(AttachError::AnchorNotFound { .. })),
        "reattach after cancel must be AnchorNotFound, got {result:?}"
    );

    drop(anchor);
}

/// Local drop must still surface one `Dropped` event even when the shared
/// frame channel is full.
#[tokio::test(flavor = "multi_thread")]
async fn test_mpsc_local_drop_preserved_under_backpressure() {
    let mgr = make_manager();
    let config = MpscAnchorConfig {
        channel_capacity: Some(1),
        ..Default::default()
    };
    let mut anchor = mgr.create_mpsc_anchor_with_config::<u32>(config);
    let handle = anchor.handle();

    let sender = mgr.attach_mpsc_stream_anchor::<u32>(handle).await.unwrap();
    let sid = sender.sender_id();
    sender.send(1).await.unwrap();

    let drop_task = tokio::spawn(async move {
        drop(sender);
    });
    match tokio::time::timeout(Duration::from_secs(2), drop_task).await {
        Ok(result) => result.expect("drop task join"),
        Err(_) => {
            // Unblock an old synchronous Drop before failing the test.
            drop(anchor);
            panic!("drop must return before the consumer drains the queue");
        }
    }

    match next_frame(&mut anchor).await.expect("frame") {
        Ok((got_sid, MpscFrame::Item(1))) => assert_eq!(got_sid, sid),
        other => panic!("expected Item(1), got {:?}", other),
    }

    match next_frame(&mut anchor).await.expect("frame") {
        Ok((got_sid, MpscFrame::Dropped(None))) => assert_eq!(got_sid, sid),
        other => panic!("expected Dropped(None), got {:?}", other),
    }

    anchor.cancel();
}

/// A polled detach has committed its terminal event. Abandoning the caller
/// must neither cancel that event nor add a second Dropped event.
#[tokio::test(flavor = "multi_thread")]
async fn abandoning_mpsc_detach_completes_or_stops_on_consumer_cancel() {
    for (cancel_consumer, exit_observed) in [(false, false), (false, true), (true, false)] {
        let mgr = make_manager();
        let mut anchor = mgr.create_mpsc_anchor_with_config::<u32>(MpscAnchorConfig {
            channel_capacity: Some(1),
            max_senders: Some(1),
            ..Default::default()
        });
        let sender = mgr
            .attach_mpsc_stream_anchor::<u32>(anchor.handle())
            .await
            .unwrap();
        let sid = sender.sender_id();
        sender.send(1).await.unwrap();
        let mut detach = Box::pin(sender.detach());
        assert!(futures::poll!(&mut detach).is_pending());
        if exit_observed {
            // The terminal has reached the consumer, but the caller has not
            // polled the detach future again to observe its completion.
            assert!(
                matches!(next_frame(&mut anchor).await, Some(Ok((id, MpscFrame::Item(1)))) if id == sid)
            );
            assert!(
                matches!(next_frame(&mut anchor).await, Some(Ok((id, MpscFrame::Detached))) if id == sid)
            );
        }
        drop(detach);
        if cancel_consumer {
            anchor.controller().cancel();
        } else {
            if !exit_observed {
                assert!(
                    matches!(next_frame(&mut anchor).await, Some(Ok((id, MpscFrame::Item(1)))) if id == sid)
                );
                assert!(
                    matches!(next_frame(&mut anchor).await, Some(Ok((id, MpscFrame::Detached))) if id == sid)
                );
            }
            let replacement = mgr
                .attach_mpsc_stream_anchor::<u32>(anchor.handle())
                .await
                .expect("completed detach must release max_senders capacity");
            replacement.send(2).await.unwrap();
            assert!(matches!(
                next_frame(&mut anchor).await,
                Some(Ok((_, MpscFrame::Item(2))))
            ));
            anchor.controller().cancel();
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while !mgr.sender_registry.senders.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("abandoned detach retained its sender registry entry");
    }
}

/// A sender can outlive the runtime it was attached on. Detaching it from
/// another runtime must still deliver `Detached`: work spawned on the stopped
/// runtime is dropped, so the consumer would see no terminal at all.
#[tokio::test(flavor = "multi_thread")]
async fn mpsc_detach_after_attaching_runtime_stops() {
    let mgr = make_manager();
    let mut anchor = mgr.create_mpsc_anchor::<u32>();
    let handle = anchor.handle();
    let attach_mgr = Arc::clone(&mgr);
    let sender = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(attach_mgr.attach_mpsc_stream_anchor::<u32>(handle))
        // `runtime` drops here and shuts down.
    })
    .join()
    .unwrap()
    .expect("attach");
    let sid = sender.sender_id();
    let returned = tokio::time::timeout(Duration::from_secs(2), sender.detach())
        .await
        .expect("detach stalled")
        .expect("detach after the attaching runtime stopped");
    assert_eq!(returned, handle);
    assert!(
        matches!(next_frame(&mut anchor).await, Some(Ok((id, MpscFrame::Detached))) if id == sid)
    );
    anchor.cancel();
}

/// A sender kept in a `thread_local!` (a language binding's thread) is
/// dropped while that thread's locals are torn down. Tokio's own context can
/// already be gone then, and `Handle::enter` panics on a destroyed context; a
/// panic in a thread-local destructor aborts the process. The drop must
/// degrade instead.
#[tokio::test(flavor = "multi_thread")]
async fn mpsc_sender_dropped_during_thread_local_teardown() {
    use std::cell::RefCell;

    thread_local! {
        static HELD: RefCell<Option<velo::streaming::MpscStreamSender<u32>>> =
            const { RefCell::new(None) };
    }

    // With an unattached timeout, the last sender leaving re-arms it, which
    // spawns: that spawn must degrade the same way.
    let mgr = make_manager();
    let mut anchor = mgr.create_mpsc_anchor_with_config::<u32>(MpscAnchorConfig {
        unattached_timeout: Some(Duration::from_secs(60)),
        ..Default::default()
    });
    let sender = mgr
        .attach_mpsc_stream_anchor::<u32>(anchor.handle())
        .await
        .unwrap();
    let sid = sender.sender_id();
    std::thread::spawn(move || {
        HELD.with(|held| *held.borrow_mut() = Some(sender));
        // Touch tokio's context after `HELD`, so its destructor is registered
        // later and runs first.
        assert!(tokio::runtime::Handle::try_current().is_err());
    })
    .join()
    .expect("dropping the sender during thread-local teardown panicked");
    assert!(
        matches!(next_frame(&mut anchor).await, Some(Ok((id, MpscFrame::Dropped(None)))) if id == sid)
    );
    anchor.cancel();
}

/// A pending `anchor.next()` must terminate promptly when the controller
/// cancels, even if sender handles are still alive.
#[tokio::test(flavor = "multi_thread")]
async fn test_mpsc_pending_next_wakes_on_controller_cancel() {
    let mgr = make_manager();
    let anchor = mgr.create_mpsc_anchor::<u32>();
    let handle = anchor.handle();
    let controller = anchor.controller();
    let sender = mgr.attach_mpsc_stream_anchor::<u32>(handle).await.unwrap();

    let next_task = tokio::spawn(async move {
        let mut anchor = anchor;
        anchor.next().await
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    controller.cancel();

    let result = tokio::time::timeout(Duration::from_secs(2), next_task)
        .await
        .expect("pending next() must wake after cancel")
        .expect("next task join");
    assert!(
        result.is_none(),
        "cancelled anchor.next() must resolve to None, got {result:?}"
    );

    drop(sender);
}

/// An MPSC handle passed to the SPSC attach path is rejected client-side
/// with [`AttachError::WrongHandleKind`] — no AM round-trip required.
#[tokio::test(flavor = "multi_thread")]
async fn test_mpsc_handle_rejected_by_spsc_attach() {
    let mgr = make_manager();
    let anchor = mgr.create_mpsc_anchor::<u32>();
    let handle = anchor.handle();
    assert!(handle.is_mpsc_stream());
    assert_eq!(handle.kind(), AnchorKind::Mpsc);

    let result = mgr.attach_stream_anchor::<u32>(handle).await;
    match result {
        Err(AttachError::WrongHandleKind {
            handle: h,
            expected,
        }) => {
            assert_eq!(h, handle);
            assert_eq!(expected, AnchorKind::Spsc);
        }
        other => panic!("expected WrongHandleKind(Spsc), got {other:?}"),
    }

    // Sanity: the MPSC anchor is still usable.
    let s = mgr.attach_mpsc_stream_anchor::<u32>(handle).await.unwrap();
    drop(s);
    drop(anchor);
}

/// An SPSC handle passed to the MPSC attach path is rejected client-side.
#[tokio::test(flavor = "multi_thread")]
async fn test_spsc_handle_rejected_by_mpsc_attach() {
    let mgr = make_manager();
    let anchor = mgr.create_anchor::<u32>();
    let handle = anchor.handle();
    assert!(handle.is_spsc_stream());
    assert_eq!(handle.kind(), AnchorKind::Spsc);

    let result = mgr.attach_mpsc_stream_anchor::<u32>(handle).await;
    match result {
        Err(AttachError::WrongHandleKind {
            handle: h,
            expected,
        }) => {
            assert_eq!(h, handle);
            assert_eq!(expected, AnchorKind::Mpsc);
        }
        other => panic!("expected WrongHandleKind(Mpsc), got {other:?}"),
    }

    // Sanity: the SPSC anchor still accepts an SPSC attach.
    let s = mgr.attach_stream_anchor::<u32>(handle).await.unwrap();
    drop(s);
    drop(anchor);
}

/// SPSC and MPSC handles for the same worker have distinct encoded forms.
#[tokio::test(flavor = "multi_thread")]
async fn test_handle_kind_roundtrip_via_manager() {
    let mgr = make_manager();
    let spsc_anchor = mgr.create_anchor::<u32>();
    let mpsc_anchor = mgr.create_mpsc_anchor::<u32>();

    let spsc_handle = spsc_anchor.handle();
    let mpsc_handle = mpsc_anchor.handle();

    assert!(spsc_handle.is_spsc_stream());
    assert!(mpsc_handle.is_mpsc_stream());
    assert_ne!(
        spsc_handle, mpsc_handle,
        "SPSC and MPSC handles must not collide"
    );

    drop(spsc_anchor);
    drop(mpsc_anchor);
}

/// `send_err` surfaces a non-terminal `SenderError` to the consumer. The
/// sender stays attached and can continue sending items afterwards.
#[tokio::test(flavor = "multi_thread")]
async fn test_mpsc_send_err_is_non_terminal() {
    let mgr = make_manager();
    let mut anchor = mgr.create_mpsc_anchor::<u32>();
    let handle = anchor.handle();
    let sender = mgr.attach_mpsc_stream_anchor::<u32>(handle).await.unwrap();

    sender.send(1).await.unwrap();
    sender.send_err("transient glitch").await.unwrap();
    sender.send(2).await.unwrap();

    let mut items = Vec::new();
    let mut got_err = false;
    while items.len() < 2 || !got_err {
        match next_frame(&mut anchor).await.expect("frame") {
            Ok((_, MpscFrame::Item(v))) => items.push(v),
            Ok((_, MpscFrame::SenderError(msg))) => {
                assert_eq!(msg, "transient glitch");
                got_err = true;
            }
            Ok((sid, MpscFrame::Detached | MpscFrame::Dropped(_))) => {
                panic!("unexpected exit from {sid}")
            }
            Err(e) => panic!("stream error: {e}"),
        }
    }
    assert_eq!(items, vec![1u32, 2]);

    drop(sender);
    anchor.cancel();
}

/// `MpscStreamSender::cancellation_token()` fires when the consumer cancels.
#[tokio::test(flavor = "multi_thread")]
async fn test_mpsc_cancellation_token_fires_on_cancel() {
    let mgr = make_manager();
    let anchor = mgr.create_mpsc_anchor::<u32>();
    let handle = anchor.handle();
    let controller = anchor.controller();
    let sender = mgr.attach_mpsc_stream_anchor::<u32>(handle).await.unwrap();
    let token = sender.cancellation_token();
    assert!(!token.is_cancelled());

    // Cancel from the consumer side.
    controller.cancel();

    // Poison propagation should fire the user-facing token too.
    tokio::time::timeout(Duration::from_secs(2), token.cancelled())
        .await
        .expect("cancellation_token must fire after controller.cancel()");

    drop(sender);
    drop(anchor);
}

/// `MpscStreamAnchor::cancel(self)` consume method (distinct from
/// `controller.cancel()`) cleans up the registry and blocks reattach.
#[tokio::test(flavor = "multi_thread")]
async fn test_mpsc_anchor_consume_cancel() {
    let mgr = make_manager();
    let anchor = mgr.create_mpsc_anchor::<u32>();
    let handle = anchor.handle();
    let _controller = anchor.cancel(); // consume
    // Wait a beat for cancel propagation.
    tokio::time::sleep(Duration::from_millis(20)).await;

    let result = mgr.attach_mpsc_stream_anchor::<u32>(handle).await;
    assert!(
        matches!(result, Err(AttachError::AnchorNotFound { .. })),
        "anchor must be gone after consume-cancel, got {result:?}"
    );
}

/// Debug impl of `MpscStreamSender` is stable and includes sender_id + handle.
#[tokio::test(flavor = "multi_thread")]
async fn test_mpsc_sender_debug_impl() {
    let mgr = make_manager();
    let anchor = mgr.create_mpsc_anchor::<u32>();
    let handle = anchor.handle();
    let sender = mgr.attach_mpsc_stream_anchor::<u32>(handle).await.unwrap();
    let dbg = format!("{sender:?}");
    assert!(dbg.contains("MpscStreamSender"));
    assert!(dbg.contains("SenderId(1)"));
    assert!(dbg.contains(&format!("{handle:?}")));
    drop(sender);
    drop(anchor);
}

/// `cancel()` via [`MpscStreamController`] is idempotent — multiple calls
/// short-circuit after the first one wins the AtomicBool gate.
#[tokio::test(flavor = "multi_thread")]
async fn test_mpsc_controller_cancel_idempotent() {
    let mgr = make_manager();
    let anchor = mgr.create_mpsc_anchor::<u32>();
    let controller = anchor.controller();
    let c2 = controller.clone();

    controller.cancel();
    c2.cancel(); // second call must not panic or double-remove
    drop(anchor);
}

/// A local MPSC sender parked on a full anchor channel wakes with an error when
/// the consumer cancels, even though the consumer still holds the anchor.
///
/// Holding the anchor keeps its channel open, so only the cancel can end the
/// wait. The task hands the sender back rather than dropping it: a local MPSC
/// sender's `Drop` blocks while the channel is full, which is a separate, known
/// limit, and the anchor is dropped first so that `Drop` finds it closed.
#[tokio::test(flavor = "multi_thread")]
async fn a_local_sender_parked_on_a_full_channel_wakes_on_cancel() {
    let mgr = make_manager();
    let anchor = mgr.create_mpsc_anchor_with_config::<u32>(velo::streaming::MpscAnchorConfig {
        channel_capacity: Some(1),
        ..Default::default()
    });
    let sender = mgr
        .attach_mpsc_stream_anchor::<u32>(anchor.handle())
        .await
        .expect("attach");
    sender.send(0).await.expect("fill the channel");
    let parked = tokio::spawn(async move {
        let result = sender.send(1).await;
        (result, sender)
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(!parked.is_finished(), "the channel is full, so send waits");

    anchor.controller().cancel();
    let (result, sender) = tokio::time::timeout(std::time::Duration::from_secs(5), parked)
        .await
        .expect("send never returned after the cancel")
        .expect("sender task panicked");
    assert!(result.is_err(), "a cancelled stream refuses the send");
    drop(anchor);
    drop(sender);
}
