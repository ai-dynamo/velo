// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Stream items over the staging threshold, end to end.
//!
//! The mux sends an item larger than any eager batch alone, as a
//! fire-and-forget message, and the messenger stages its payload once it is
//! over 256 KiB. The receiver fetches it in a task of its own, outside the
//! lane's order, so the eager batches sent after it can arrive first. These
//! tests run two `Velo` nodes over TCP and stall that fetch at the producer,
//! so the order on the wire is the one a slow transfer gives.

#[path = "../support/staged_fetch.rs"]
mod staged_fetch;

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use prometheus::Registry;
use staged_fetch::{Stall, stall_fetches};
use velo::observability::VeloMetrics;
use velo::observability::test_helpers::MetricSnapshot;
use velo::streaming::MuxConfig;
use velo::transports::tcp::TcpTransportBuilder;
use velo::{StreamAnchor, StreamAnchorHandle, StreamFrame};
use velo::{Velo, VeloBuilder};

const PATIENCE: Duration = Duration::from_secs(10);
/// Over the staging threshold, so the item is staged.
const STAGED: usize = 512 * 1024;
/// One key for every stream, so they share one (peer, lane) and its slot
/// indexes.
const LANE_KEY: u64 = 7;

struct Node {
    velo: Velo,
    registry: Registry,
}

impl Node {
    /// Records waiting in the reorder hold of this node's ingress slots.
    fn held_records(&self) -> f64 {
        MetricSnapshot::from_registry(&self.registry).gauge("velo_streaming_mux_held_records", &[])
    }
}

async fn node(builder: VeloBuilder) -> Node {
    let registry = Registry::new();
    let metrics = Arc::new(VeloMetrics::register(&registry).unwrap());
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let transport = TcpTransportBuilder::new()
        .from_listener(listener)
        .unwrap()
        .build()
        .unwrap();
    let velo = builder
        .add_transport(Arc::new(transport))
        .stream_bind_addr(std::net::Ipv4Addr::LOCALHOST.into())
        .metrics(metrics)
        .messenger_mux(MuxConfig {
            enabled: true,
            ..MuxConfig::default()
        })
        .unwrap()
        .build()
        .await
        .unwrap();
    Node { velo, registry }
}

async fn introduce(a: &Node, b: &Node) {
    a.velo.register_peer(b.velo.peer_info()).unwrap();
    b.velo.register_peer(a.velo.peer_info()).unwrap();
    for (from, to) in [(a, b), (b, a)] {
        tokio::time::timeout(
            PATIENCE,
            from.velo
                .wait_for_handler(to.velo.instance_id(), "_anchor_attach"),
        )
        .await
        .expect("the peer's control plane came up")
        .unwrap();
    }
}

/// The handle as it would cross an RPC to the producer.
fn transfer(handle: StreamAnchorHandle) -> StreamAnchorHandle {
    StreamAnchorHandle::from_u128(handle.as_u128())
}

/// The next item, `None` at the end of the stream, or why neither came.
async fn next(anchor: &mut StreamAnchor<Vec<u8>>) -> Result<Option<Vec<u8>>, String> {
    match tokio::time::timeout(PATIENCE, anchor.next()).await {
        Err(_) => Err(format!("nothing arrived within {PATIENCE:?}")),
        Ok(None) | Ok(Some(Ok(StreamFrame::Finalized))) => Ok(None),
        Ok(Some(Ok(StreamFrame::Item(item)))) => Ok(Some(item)),
        Ok(Some(Ok(other))) => Err(format!("unexpected frame {other:?}")),
        Ok(Some(Err(error))) => Err(format!("{error:?}")),
    }
}

/// Wait until `node` holds a record in a reorder hold.
async fn until_held(node: &Node) {
    tokio::time::timeout(PATIENCE, async {
        while node.held_records() < 1.0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("a record overtook the staged item and waits in the hold");
}

/// A stream opened while the previous stream's staged item is still being
/// fetched delivers, and the previous stream then ends in full.
///
/// Stream A sends one staged item and ends. Its terminal and `CloseSlot` go
/// in an eager batch, overtake the stalled fetch, and wait in the consumer's
/// hold. The producer frees A's slot index once it writes that close, and
/// stream B, on the same lane, opens at the same index. The consumer used to
/// reject B's `OpenSlot` as a slot collision, so B never delivered. Responses
/// of a few megabytes, one stream after another on one lane, hit this in
/// production; one stream after one stalled item is enough.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stream_opened_behind_a_stalled_staged_item_delivers() {
    let consumer = node(Velo::builder()).await;
    let producer = node(Velo::builder()).await;
    introduce(&producer, &consumer).await;
    let stall = Stall::new();
    stall_fetches(&producer.velo, &stall);

    let mut a = consumer.velo.create_anchor::<Vec<u8>>();
    let a_tx = producer
        .velo
        .attach_anchor_keyed::<Vec<u8>>(transfer(a.handle()), LANE_KEY)
        .await
        .expect("attach A");
    a_tx.send(vec![0x5a; STAGED]).await.expect("send A's item");
    a_tx.finalize().expect("finalize A");
    tokio::time::timeout(PATIENCE, stall.entered.notified())
        .await
        .expect("the consumer began to fetch A's item");
    until_held(&consumer).await;

    let mut b = consumer.velo.create_anchor::<Vec<u8>>();
    let b_tx = producer
        .velo
        .attach_anchor_keyed::<Vec<u8>>(transfer(b.handle()), LANE_KEY)
        .await
        .expect("attach B");
    b_tx.send(vec![1, 2, 3]).await.expect("send B's item");
    b_tx.finalize().expect("finalize B");
    let b_item = next(&mut b).await;
    let b_end = next(&mut b).await;

    stall.gate.add_permits(1);
    let a_item = next(&mut a).await;
    let a_end = next(&mut a).await;

    assert_eq!(
        b_item,
        Ok(Some(vec![1, 2, 3])),
        "stream B, opened behind A's stalled fetch, did not deliver"
    );
    assert_eq!(b_end, Ok(None), "stream B did not end cleanly");
    assert_eq!(
        a_item.map(|item| item.map(|item| item.len())),
        Ok(Some(STAGED)),
        "stream A lost its staged item"
    );
    assert_eq!(a_end, Ok(None), "stream A did not end cleanly");
}

/// The receiver's own fetch bound does not drop a stream item.
///
/// `VeloBuilder::staged_fetch_timeout` ends a fetch that runs out, with no
/// reply. A stream item is a fire-and-forget batch, so the mux never learned
/// that the item was gone: the records after it waited in the hold, and the
/// consumer got `SenderDropped` from the stream watchdog about 15 s later,
/// while its producer was still attached. The producer also kept the item
/// staged. A slow item is late, not lost, and the stream watchdog already
/// decides when a stream is dead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_receivers_fetch_bound_does_not_drop_a_stream_item() {
    const BOUND: Duration = Duration::from_millis(100);
    let consumer = node(Velo::builder().staged_fetch_timeout(BOUND)).await;
    let producer = node(Velo::builder()).await;
    introduce(&producer, &consumer).await;
    let stall = Stall::new();
    stall_fetches(&producer.velo, &stall);

    let mut anchor = consumer.velo.create_anchor::<Vec<u8>>();
    let tx = producer
        .velo
        .attach_anchor::<Vec<u8>>(transfer(anchor.handle()))
        .await
        .expect("attach");
    tx.send(vec![0x5a; STAGED])
        .await
        .expect("send the staged item");
    tx.send(vec![1, 2, 3]).await.expect("send the next item");
    tx.finalize().expect("finalize");
    tokio::time::timeout(PATIENCE, stall.entered.notified())
        .await
        .expect("the consumer began to fetch the staged item");
    // Past the receiver's bound, on purpose.
    tokio::time::sleep(BOUND * 3).await;
    stall.gate.add_permits(1);

    assert_eq!(
        next(&mut anchor)
            .await
            .map(|item| item.map(|item| item.len())),
        Ok(Some(STAGED)),
        "the staged item was dropped at the receiver's fetch bound"
    );
    assert_eq!(next(&mut anchor).await, Ok(Some(vec![1, 2, 3])));
    assert_eq!(
        next(&mut anchor).await,
        Ok(None),
        "the stream did not end cleanly"
    );
}
