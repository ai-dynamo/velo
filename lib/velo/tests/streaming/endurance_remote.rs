// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

#![cfg(velo_endurance)]

//! Real-transport endurance tests for velo-streaming.
//!
//! Parallels the local endurance suite in `endurance.rs` but runs the
//! data-flow scenarios end-to-end across two `AnchorManager`s wired through:
//!   * **raw TCP** (`TcpFrameTransport`), and
//!   * **UDS velo-messenger streaming** (`VeloFrameTransport` over a UDS `Messenger`).
//!
//! Gated behind the `velo_endurance` rustc cfg (NOT a Cargo feature) so CI's
//! `cargo test --all-features --all-targets` never compiles or runs them.
//! Opt in locally with:
//!
//! ```text
//! RUSTFLAGS="--cfg velo_endurance" cargo test -p velo-streaming --test endurance_remote
//! ```
//!
//! The *consumer* side creates the anchor; the *producer* side calls
//! `attach_stream_anchor(handle)` — worker-id mismatch forces the remote path,
//! so frames cross the wire via the transport under test.
//!
//! Scope: E-01, E-02, E-05, E-06 × {TCP, UDS-velo}.
//! E-03/E-04/E-07 exercise local registry / Drop / cancel-gate semantics and
//! are covered by the local suite only.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use velo::messenger::Messenger;
use velo::streaming::velo_transport::VeloFrameTransport;
use velo::streaming::{
    AnchorManager, AnchorManagerBuilder, FrameTransport, StreamFrame, TcpFrameTransport,
};
use velo::transports::tcp::TcpTransportBuilder;
use velo::transports::uds::UdsTransportBuilder;
use velo_ext::WorkerId;

// ---------------------------------------------------------------------------
// Two-node fixture
// ---------------------------------------------------------------------------

/// Producer attaches; consumer owns the anchor and receives frames across the wire.
struct RemotePair {
    producer: Arc<AnchorManager>,
    consumer: Arc<AnchorManager>,
    // Keep messengers alive for the lifetime of the test.
    _msgr_producer: Arc<Messenger>,
    _msgr_consumer: Arc<Messenger>,
}

async fn make_two_tcp_messengers() -> (Arc<Messenger>, Arc<Messenger>) {
    let bind = |_| {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        Arc::new(
            TcpTransportBuilder::new()
                .from_listener(listener)
                .unwrap()
                .build()
                .unwrap(),
        )
    };
    let t1 = bind(());
    let t2 = bind(());

    let m1 = Messenger::builder()
        .add_transport(t1)
        .build()
        .await
        .expect("build messenger 1");
    let m2 = Messenger::builder()
        .add_transport(t2)
        .build()
        .await
        .expect("build messenger 2");

    m2.register_peer(m1.peer_info()).expect("register m1 on m2");
    m1.register_peer(m2.peer_info()).expect("register m2 on m1");
    tokio::time::sleep(Duration::from_millis(200)).await;
    (m1, m2)
}

async fn make_two_uds_messengers() -> (Arc<Messenger>, Arc<Messenger>) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let pid = std::process::id();
    let n = COUNTER.fetch_add(2, Ordering::Relaxed);
    let sock_a = std::env::temp_dir().join(format!("velo-endurance-{pid}-{n}-a.sock"));
    let sock_b = std::env::temp_dir().join(format!("velo-endurance-{pid}-{}-b.sock", n + 1));

    let t1 = Arc::new(
        UdsTransportBuilder::new()
            .socket_path(&sock_a)
            .build()
            .expect("build UDS transport 1"),
    );
    let t2 = Arc::new(
        UdsTransportBuilder::new()
            .socket_path(&sock_b)
            .build()
            .expect("build UDS transport 2"),
    );

    let m1 = Messenger::builder()
        .add_transport(t1)
        .build()
        .await
        .expect("build messenger 1");
    let m2 = Messenger::builder()
        .add_transport(t2)
        .build()
        .await
        .expect("build messenger 2");

    m2.register_peer(m1.peer_info()).expect("register m1 on m2");
    m1.register_peer(m2.peer_info()).expect("register m2 on m1");
    tokio::time::sleep(Duration::from_millis(200)).await;
    (m1, m2)
}

/// Consumer side owns the anchor; producer side attaches remotely.
/// Uses raw `TcpFrameTransport` as data plane on both sides, with a
/// `transport_registry` entry so the remote-attach handshake can resolve the scheme.
async fn make_remote_pair_tcp() -> RemotePair {
    let (m_consumer, m_producer) = make_two_tcp_messengers().await;
    let worker_consumer = m_consumer.instance_id().worker_id();
    let worker_producer = m_producer.instance_id().worker_id();

    async fn make_side(worker_id: WorkerId) -> Arc<velo::streaming::AnchorManager> {
        let tcp = TcpFrameTransport::new(std::net::Ipv4Addr::LOCALHOST.into())
            .await
            .unwrap();
        let mut registry = HashMap::new();
        registry.insert("tcp".to_string(), tcp.clone() as Arc<dyn FrameTransport>);
        Arc::new(
            AnchorManagerBuilder::default()
                .worker_id(worker_id)
                .transport(tcp as Arc<dyn FrameTransport>)
                .transport_registry(Arc::new(registry))
                .build()
                .expect("build AM"),
        )
    }

    let consumer = make_side(worker_consumer).await;
    let producer = make_side(worker_producer).await;

    consumer
        .register_handlers(Arc::clone(&m_consumer))
        .expect("consumer register_handlers");
    producer
        .register_handlers(Arc::clone(&m_producer))
        .expect("producer register_handlers");

    RemotePair {
        producer,
        consumer,
        _msgr_producer: m_producer,
        _msgr_consumer: m_consumer,
    }
}

/// Consumer side owns the anchor; producer side attaches remotely.
/// Uses `VeloFrameTransport` as data plane with a UDS-backed `Messenger` underneath.
async fn make_remote_pair_uds() -> RemotePair {
    let (m_consumer, m_producer) = make_two_uds_messengers().await;
    let worker_consumer = m_consumer.instance_id().worker_id();
    let worker_producer = m_producer.instance_id().worker_id();

    let vft_consumer = Arc::new(
        VeloFrameTransport::new(Arc::clone(&m_consumer), worker_consumer, None)
            .expect("VFT consumer"),
    );
    let vft_producer = Arc::new(
        VeloFrameTransport::new(Arc::clone(&m_producer), worker_producer, None)
            .expect("VFT producer"),
    );

    let consumer = Arc::new(
        AnchorManagerBuilder::default()
            .worker_id(worker_consumer)
            .transport(Arc::clone(&vft_consumer) as Arc<dyn FrameTransport>)
            .build()
            .expect("build consumer AM"),
    );
    let producer = Arc::new(
        AnchorManagerBuilder::default()
            .worker_id(worker_producer)
            .transport(Arc::clone(&vft_producer) as Arc<dyn FrameTransport>)
            .build()
            .expect("build producer AM"),
    );

    consumer
        .register_handlers(Arc::clone(&m_consumer))
        .expect("consumer register_handlers");
    producer
        .register_handlers(Arc::clone(&m_producer))
        .expect("producer register_handlers");

    RemotePair {
        producer,
        consumer,
        _msgr_producer: m_producer,
        _msgr_consumer: m_consumer,
    }
}

// ---------------------------------------------------------------------------
// Generic test bodies — parameterized over the `RemotePair` fixture.
// Per-frame AM dispatch (VeloFrameTransport) is concurrent, so tests that
// care about order collect items into a set and compare by multiset.
// TCP preserves order on a single sender; use exact-sequence comparisons there.
// ---------------------------------------------------------------------------

/// E-01 body: N frames on a single remote anchor, zero loss.
/// `ordered = true` asserts exact sequence; `ordered = false` asserts set equality.
async fn run_e01(pair: &RemotePair, total: u64, ordered: bool) {
    let mut anchor = pair.consumer.create_anchor::<u64>();
    let handle = anchor.handle();
    let sender = pair
        .producer
        .attach_stream_anchor::<u64>(handle)
        .await
        .expect("remote attach");

    let send = tokio::spawn(async move {
        for i in 0..total {
            sender.send(i).await.expect("send");
        }
        // Allow in-flight AMs to drain before finalize on VeloFrameTransport.
        tokio::time::sleep(Duration::from_millis(50)).await;
        sender.finalize().expect("finalize");
    });

    let mut received: Vec<u64> = Vec::with_capacity(total as usize);
    let collect = async {
        while let Some(frame) = anchor.next().await {
            match frame {
                Ok(StreamFrame::Item(v)) => received.push(v),
                Ok(StreamFrame::Finalized) => break,
                other => panic!("unexpected: {other:?}"),
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(120), collect)
        .await
        .expect("e01: timed out");
    send.await.expect("send task");

    assert_eq!(received.len(), total as usize, "zero frame loss");
    if ordered {
        assert_eq!(received, (0..total).collect::<Vec<_>>(), "strict order");
    } else {
        received.sort_unstable();
        assert_eq!(
            received,
            (0..total).collect::<Vec<_>>(),
            "all frames present"
        );
    }
    assert!(anchor.next().await.is_none());
}

/// E-02 body: STREAMS anchors × ITEMS frames each, all correctly routed.
async fn run_e02(pair: &RemotePair, streams: usize, items: u32, ordered: bool) {
    let mut anchors = Vec::with_capacity(streams);
    let mut senders = Vec::with_capacity(streams);
    for _ in 0..streams {
        let anchor = pair.consumer.create_anchor::<u32>();
        let handle = anchor.handle();
        let sender = pair
            .producer
            .attach_stream_anchor::<u32>(handle)
            .await
            .expect("attach");
        anchors.push(anchor);
        senders.push(sender);
    }

    let send_tasks: Vec<_> = senders
        .into_iter()
        .enumerate()
        .map(|(idx, sender)| {
            tokio::spawn(async move {
                let base = idx as u32 * 1000;
                for j in 0..items {
                    sender.send(base + j).await.expect("send");
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
                sender.finalize().expect("finalize");
            })
        })
        .collect();

    let recv_tasks: Vec<_> = anchors
        .into_iter()
        .enumerate()
        .map(|(idx, mut anchor)| {
            tokio::spawn(async move {
                let base = idx as u32 * 1000;
                let mut got: Vec<u32> = Vec::new();
                let collect = async {
                    while let Some(frame) = anchor.next().await {
                        match frame {
                            Ok(StreamFrame::Item(v)) => got.push(v),
                            Ok(StreamFrame::Finalized) => break,
                            other => panic!("stream {idx}: unexpected {other:?}"),
                        }
                    }
                };
                tokio::time::timeout(Duration::from_secs(120), collect)
                    .await
                    .expect("e02: timed out");
                (idx, got, base)
            })
        })
        .collect();

    for t in send_tasks {
        t.await.expect("send task");
    }
    for t in recv_tasks {
        let (idx, mut got, base) = t.await.expect("recv task");
        let expected: Vec<u32> = (0..items).map(|j| base + j).collect();
        if ordered {
            assert_eq!(got, expected, "stream {idx}: received wrong items");
        } else {
            got.sort_unstable();
            assert_eq!(got, expected, "stream {idx}: missing or duplicate items");
        }
    }

    assert_eq!(
        pair.consumer.active_anchor_count(),
        0,
        "all anchors must be removed after finalize"
    );
}

/// E-05 body: consumer sleeps 1 ms per item while producer saturates the wire.
async fn run_e05(pair: &RemotePair, total: u32, ordered: bool) {
    let mut anchor = pair.consumer.create_anchor::<u32>();
    let handle = anchor.handle();
    let sender = pair
        .producer
        .attach_stream_anchor::<u32>(handle)
        .await
        .expect("attach");

    let send = tokio::spawn(async move {
        for i in 0..total {
            sender.send(i).await.expect("send");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        sender.finalize().expect("finalize");
    });

    let mut items: Vec<u32> = Vec::with_capacity(total as usize);
    let collect = async {
        while let Some(frame) = anchor.next().await {
            match frame {
                Ok(StreamFrame::Item(v)) => {
                    items.push(v);
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                Ok(StreamFrame::Finalized) => break,
                other => panic!("unexpected: {other:?}"),
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(180), collect)
        .await
        .expect("e05: timed out");
    send.await.expect("send task");

    assert_eq!(items.len(), total as usize, "zero loss under slow consumer");
    if ordered {
        assert_eq!(items, (0..total).collect::<Vec<_>>(), "order preserved");
    } else {
        items.sort_unstable();
        assert_eq!(items, (0..total).collect::<Vec<_>>(), "all items present");
    }
    assert!(anchor.next().await.is_none());
}

/// E-06 body: 5 detach/re-attach cycles × BATCH items + 1 final finalize batch.
async fn run_e06(pair: &RemotePair, cycles: u32, batch: u32, ordered: bool) {
    let mut anchor = pair.consumer.create_anchor::<u32>();
    let mut current_handle = anchor.handle();
    let mut all_items: Vec<u32> = Vec::new();
    let mut detached_count: u32 = 0;
    let mut offset: u32 = 0;

    for cycle in 0..cycles {
        let sender = pair
            .producer
            .attach_stream_anchor::<u32>(current_handle)
            .await
            .unwrap_or_else(|e| panic!("cycle {cycle} attach: {e}"));
        for j in 0..batch {
            sender.send(offset + j).await.expect("send");
        }
        offset += batch;
        // Let in-flight AMs drain before detach so the cycle's Detached sentinel
        // orders after its items on VeloFrameTransport.
        tokio::time::sleep(Duration::from_millis(50)).await;
        current_handle = sender.detach().expect("detach");

        loop {
            let f = tokio::time::timeout(Duration::from_secs(30), anchor.next())
                .await
                .expect("cycle timeout")
                .expect("stream open");
            match f {
                Ok(StreamFrame::Item(v)) => all_items.push(v),
                Ok(StreamFrame::Detached) => {
                    detached_count += 1;
                    break;
                }
                other => panic!("cycle {cycle}: unexpected {other:?}"),
            }
        }
    }

    let last_sender = pair
        .producer
        .attach_stream_anchor::<u32>(current_handle)
        .await
        .expect("final attach");
    for j in 0..batch {
        last_sender.send(offset + j).await.expect("send");
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    last_sender.finalize().expect("finalize");

    loop {
        let f = tokio::time::timeout(Duration::from_secs(30), anchor.next())
            .await
            .expect("final timeout")
            .expect("stream open");
        match f {
            Ok(StreamFrame::Item(v)) => all_items.push(v),
            Ok(StreamFrame::Finalized) => break,
            other => panic!("final batch: unexpected {other:?}"),
        }
    }
    assert!(anchor.next().await.is_none());

    let total = (cycles + 1) * batch;
    assert_eq!(all_items.len(), total as usize, "must receive all items");
    if ordered {
        assert_eq!(all_items, (0..total).collect::<Vec<_>>(), "strict order");
    } else {
        all_items.sort_unstable();
        assert_eq!(all_items, (0..total).collect::<Vec<_>>(), "all present");
    }
    assert_eq!(
        detached_count, cycles,
        "must see exactly {cycles} Detached sentinels"
    );
}

// ===========================================================================
// TCP variants — ordered assertions (single-connection per anchor preserves order)
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_e01_tcp_sustained_10k_frames() {
    let pair = make_remote_pair_tcp().await;
    run_e01(&pair, 10_000, true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e02_tcp_concurrent_100_streams() {
    let pair = make_remote_pair_tcp().await;
    run_e02(&pair, 100, 100, true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e05_tcp_consumer_slow_sustained() {
    let pair = make_remote_pair_tcp().await;
    run_e05(&pair, 1_000, true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e06_tcp_five_cycle_reattach() {
    let pair = make_remote_pair_tcp().await;
    run_e06(&pair, 5, 100, true).await;
}

// ===========================================================================
// UDS velo-messenger variants — per-frame AM dispatch is concurrent.
// Order-by-multiset is the Phase 09-02 decision documented in velo_integration.rs.
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_e01_uds_velo_sustained_10k_frames() {
    let pair = make_remote_pair_uds().await;
    run_e01(&pair, 10_000, false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e02_uds_velo_concurrent_100_streams() {
    let pair = make_remote_pair_uds().await;
    run_e02(&pair, 100, 100, false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e05_uds_velo_consumer_slow_sustained() {
    let pair = make_remote_pair_uds().await;
    run_e05(&pair, 1_000, false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e06_uds_velo_five_cycle_reattach() {
    let pair = make_remote_pair_uds().await;
    run_e06(&pair, 5, 100, false).await;
}
