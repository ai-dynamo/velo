// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Receiver-side ordering regression test.
//!
//! Reproduces out-of-order delivery on a remote `StreamSender` driven through
//! `VeloFrameTransport`. The send path is sequential per-sender, and every
//! transport preserves per-(src,dest) FIFO on the wire, so any reordering must
//! originate at AM dispatch on the receiver — where `SpawnedDispatcher` spawns
//! one tokio task per AM and the tokio scheduler does not guarantee they begin
//! in spawn order.
//!
//! Pre-fix: this test fails (sometimes after a handful of frames, sometimes
//! after several hundred) because consecutive `_stream_data` AMs land on the
//! consumer in scheduler order rather than send order.
//!
//! Post-fix (sender-stamped seq + receiver reorder buffer in `_stream_data`):
//! this test passes deterministically.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use velo_common::WorkerId;
use velo_messenger::Messenger;
use velo_streaming::velo_transport::VeloFrameTransport;
use velo_streaming::{AnchorManagerBuilder, FrameTransport, StreamAnchorHandle, StreamFrame};
use velo_transports::tcp::TcpTransportBuilder;

const FRAMES: u32 = 200;

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

/// Fast remote sender + slow consumer over `VeloFrameTransport`/TCP.
///
/// The consumer yields between frames so that pending `_stream_data` AMs
/// build up in flight, exposing the spawn-order race.
#[tokio::test(flavor = "multi_thread")]
async fn test_remote_stream_preserves_send_order() {
    let (messenger_a, messenger_b) = make_two_messengers().await;
    let worker_id_a = messenger_a.instance_id().worker_id();
    let worker_id_b = messenger_b.instance_id().worker_id();

    // Worker A: receiver
    let vft_a = Arc::new(
        VeloFrameTransport::new(Arc::clone(&messenger_a), worker_id_a, None).expect("VFT worker A"),
    );
    let am_a: Arc<velo_streaming::AnchorManager> = Arc::new(
        AnchorManagerBuilder::default()
            .worker_id(worker_id_a)
            .transport(Arc::clone(&vft_a) as Arc<dyn FrameTransport>)
            .build()
            .expect("AM worker A"),
    );
    am_a.register_handlers(Arc::clone(&messenger_a))
        .expect("register_handlers on worker A");

    let mut anchor_stream = am_a.create_anchor::<u32>();
    let handle = anchor_stream.handle();

    // Cross-worker handle transfer.
    let handle_raw: u128 = handle.as_u128();
    let hi = (handle_raw >> 64) as u64;
    let lo = handle_raw as u64;
    let handle_transferred = StreamAnchorHandle::pack(WorkerId::from_u64(hi), lo);

    // Worker B: sender
    let vft_b = Arc::new(
        VeloFrameTransport::new(Arc::clone(&messenger_b), worker_id_b, None).expect("VFT worker B"),
    );
    let am_b: Arc<velo_streaming::AnchorManager> = Arc::new(
        AnchorManagerBuilder::default()
            .worker_id(worker_id_b)
            .transport(Arc::clone(&vft_b) as Arc<dyn FrameTransport>)
            .build()
            .expect("AM worker B"),
    );
    am_b.register_handlers(Arc::clone(&messenger_b))
        .expect("register_handlers on worker B");

    let sender = am_b
        .attach_stream_anchor::<u32>(handle_transferred)
        .await
        .expect("remote attach must succeed");

    // Sender task: pushes FRAMES items as fast as it can, then finalizes.
    let send_task = tokio::spawn(async move {
        for i in 0u32..FRAMES {
            sender.send(i).await.expect("send item");
        }
        // Brief barrier to ensure all data AMs land on the receiver before the
        // Finalized sentinel — Finalized arrives on the same flume channel as
        // data and is therefore subject to the same dispatcher race we're
        // probing, but assertion is on *data* ordering.
        tokio::time::sleep(Duration::from_millis(50)).await;
        sender.finalize().expect("finalize");
    });

    // Slow consumer: yields between frames so AMs queue up in the dispatcher.
    let collect = async {
        let mut items = Vec::with_capacity(FRAMES as usize);
        while let Some(frame) = anchor_stream.next().await {
            match frame.expect("no stream error") {
                StreamFrame::Item(v) => {
                    items.push(v);
                    // Yield to let inbound AMs accumulate behind us.
                    tokio::task::yield_now().await;
                }
                StreamFrame::Finalized => break,
                other => panic!("unexpected frame: {:?}", other),
            }
        }
        items
    };

    let items = tokio::time::timeout(Duration::from_secs(15), collect)
        .await
        .expect("timed out waiting for items");

    send_task.await.expect("send task panicked");

    assert_eq!(
        items.len(),
        FRAMES as usize,
        "expected exactly {} items, got {}",
        FRAMES,
        items.len()
    );

    // The actual ordering check. Pre-fix, this fails in the typical run.
    let expected: Vec<u32> = (0..FRAMES).collect();
    if items != expected {
        let first_oos = items
            .iter()
            .zip(expected.iter())
            .position(|(got, want)| got != want)
            .unwrap_or(items.len());
        panic!(
            "frames out of order at index {}: got {:?}..., want {:?}...",
            first_oos,
            &items[first_oos..(first_oos + 5).min(items.len())],
            &expected[first_oos..(first_oos + 5).min(expected.len())],
        );
    }
}
