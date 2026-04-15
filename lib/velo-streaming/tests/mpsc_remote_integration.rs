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
use velo_messenger::Messenger;
use velo_streaming::velo_transport::VeloFrameTransport;
use velo_streaming::{
    AnchorManager, AnchorManagerBuilder, FrameTransport, MpscFrame, SenderId, StreamAnchorHandle,
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
    let _ = s1.detach();
    let _ = s2.detach();
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

    let _ = s2.detach();
    anchor.cancel();
}
