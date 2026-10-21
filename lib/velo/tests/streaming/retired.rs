// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! `StreamSender::retired`: when velo on the producer has handed on all of a
//! stream's output, over real nodes.
//!
//! A finished stream's output is not gone when `finalize` returns. Under the
//! messenger mux, records wait on the producer for the consumer's credit, so a
//! slow consumer can hold them there for as long as it does not read. A
//! producer that shuts its node down in that window cuts the stream short, and
//! nothing told it so. These tests pin what `retired()` reports in each case,
//! and `shutting_down_after_sent_keeps_the_tail` shows the cut it exists to
//! prevent.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{FutureExt, StreamExt};
use prometheus::Registry;
use velo::observability::VeloMetrics;
use velo::observability::test_helpers::MetricSnapshot;
use velo::streaming::{AnchorConfig, MuxConfig, Retirement, StreamAnchor, StreamFrame};
use velo::transports::tcp::TcpTransportBuilder;
use velo::{ShutdownPolicy, Velo};

const PATIENCE: Duration = Duration::from_secs(5);

/// Items each stream sends: far more than the small window below, so most of
/// them wait on the producer for credit.
const ITEMS: u32 = 64;

/// A credit window small enough that a consumer which does not read leaves
/// most of a stream withheld on the producer.
const SMALL_WINDOW: u32 = 4;

fn tcp() -> Arc<velo::transports::tcp::TcpTransport> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    Arc::new(
        TcpTransportBuilder::new()
            .from_listener(listener)
            .expect("from_listener")
            .build()
            .expect("build transport"),
    )
}

struct Node {
    velo: Velo,
    registry: Registry,
}

impl Node {
    /// Records the producer's batchers hold for want of credit.
    fn withheld(&self) -> f64 {
        MetricSnapshot::from_registry(&self.registry)
            .gauge("velo_streaming_mux_withheld_records", &[])
    }
}

async fn node(mux: MuxConfig) -> Node {
    let registry = Registry::new();
    let metrics = Arc::new(VeloMetrics::register(&registry).expect("metrics"));
    let velo = Velo::builder()
        .add_transport(tcp())
        .stream_bind_addr(std::net::Ipv4Addr::LOCALHOST.into())
        .metrics(metrics)
        .messenger_mux(mux)
        .expect("mux config")
        .build()
        .await
        .expect("build velo");
    Node { velo, registry }
}

fn window(credit: u32) -> MuxConfig {
    MuxConfig {
        initial_credit: credit,
        ..MuxConfig::default()
    }
}

/// A producer and a consumer that know each other.
async fn pair(mux: MuxConfig) -> (Node, Node) {
    let producer = node(mux.clone()).await;
    let consumer = node(mux).await;
    producer
        .velo
        .register_peer(consumer.velo.peer_info())
        .expect("register consumer");
    consumer
        .velo
        .register_peer(producer.velo.peer_info())
        .expect("register producer");
    for (a, b) in [(&producer, &consumer), (&consumer, &producer)] {
        tokio::time::timeout(
            PATIENCE,
            a.velo
                .wait_for_handler(b.velo.instance_id(), "_anchor_attach"),
        )
        .await
        .expect("handshake timed out")
        .expect("handshake");
    }
    (producer, consumer)
}

/// Open a stream from `producer` to an anchor on `consumer`, and send it
/// `ITEMS` items.
async fn stream_of_items(
    producer: &Node,
    consumer: &Node,
) -> (StreamAnchor<u32>, velo::streaming::StreamSender<u32>) {
    let anchor = consumer.velo.create_anchor::<u32>();
    let ticket = consumer
        .velo
        .prebind_anchor(anchor.handle())
        .expect("ticket");
    let sender = producer
        .velo
        .open_anchor_stream::<u32>(anchor.handle(), ticket)
        .await
        .expect("open stream");
    for i in 0..ITEMS {
        sender.send(i).await.expect("send");
    }
    (anchor, sender)
}

/// Read a whole stream: every item in order, then `Finalized`.
async fn read_to_the_end(anchor: &mut StreamAnchor<u32>) {
    for i in 0..ITEMS {
        match tokio::time::timeout(PATIENCE, anchor.next()).await {
            Ok(Some(Ok(StreamFrame::Item(v)))) if v == i => {}
            other => panic!("item {i}: {other:?}"),
        }
    }
    match tokio::time::timeout(PATIENCE, anchor.next()).await {
        Ok(Some(Ok(StreamFrame::Finalized))) => {}
        other => panic!("expected Finalized: {other:?}"),
    }
}

/// A consumer with room for the whole stream: the terminal goes out with the
/// records, and the stream retires as sent once that batch is admitted,
/// without any reply from the consumer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_short_stream_retires_without_a_reply() {
    let (producer, consumer) = pair(window(256)).await;
    let (mut anchor, sender) = stream_of_items(&producer, &consumer).await;
    let retired = sender.retired();
    sender.finalize().expect("finalize");
    // Before the consumer reads anything.
    assert_eq!(
        tokio::time::timeout(PATIENCE, retired).await,
        Ok(Retirement::Sent)
    );
    read_to_the_end(&mut anchor).await;
}

/// Output withheld for a consumer that has not read keeps the stream
/// unretired; once the consumer reads it all, the stream retires as sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slow_consumer_holds_retirement_until_it_reads() {
    let (producer, consumer) = pair(window(SMALL_WINDOW)).await;
    let (mut anchor, sender) = stream_of_items(&producer, &consumer).await;
    let mut retired = sender.retired();
    sender.finalize().expect("finalize");

    // The producer holds records for want of credit: a positive fact, and the
    // terminal is behind them, so nothing may have retired the stream.
    let deadline = tokio::time::Instant::now() + PATIENCE;
    while producer.withheld() < 1.0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the producer never withheld anything"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!((&mut retired).now_or_never(), None);

    read_to_the_end(&mut anchor).await;
    assert_eq!(
        tokio::time::timeout(PATIENCE, retired).await,
        Ok(Retirement::Sent)
    );
}

/// A consumer that drops its anchor with output still withheld retires the
/// stream as discarded. The close the consumer already sends is the signal;
/// nothing new crosses the wire.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_consumer_that_leaves_retires_the_stream_as_discarded() {
    let (producer, consumer) = pair(window(SMALL_WINDOW)).await;
    let (mut anchor, sender) = stream_of_items(&producer, &consumer).await;
    let retired = sender.retired();
    sender.finalize().expect("finalize");
    assert!(matches!(
        tokio::time::timeout(PATIENCE, anchor.next()).await,
        Ok(Some(Ok(StreamFrame::Item(0))))
    ));
    drop(anchor);
    assert_eq!(
        tokio::time::timeout(PATIENCE, retired).await,
        Ok(Retirement::Discarded)
    );
}

/// A producer that shuts down while it still withholds a stream's output
/// throws that output away, and `retired()` says so.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn producer_shutdown_discards_withheld_output() {
    let (producer, consumer) = pair(window(SMALL_WINDOW)).await;
    let (_anchor, sender) = stream_of_items(&producer, &consumer).await;
    let retired = sender.retired();
    sender.finalize().expect("finalize");
    producer
        .velo
        .graceful_shutdown(ShutdownPolicy::Timeout(Duration::from_secs(1)))
        .await;
    assert_eq!(
        tokio::time::timeout(PATIENCE, retired).await,
        Ok(Retirement::Discarded)
    );
}

/// The hazard `retired()` exists for, both ways round.
///
/// Shut the producer down right after `finalize`, while its node still holds
/// the stream's tail, and the consumer never sees the end of the stream. Wait
/// for `retired()` to say `Sent` first, and the whole stream arrives, even
/// though the shutdown follows at once: what velo had handed on, the
/// transport writes before it closes.
///
/// The second half runs several trials, because what it guards against is a
/// shutdown that lands in the moment between the batch's admission and its
/// write. Each trial builds its own two nodes, since the shutdown ends one; a
/// trial takes a few milliseconds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutting_down_after_sent_keeps_the_tail() {
    const TRIALS: usize = 10;

    // Right after `finalize`: the tail is still withheld when the producer
    // goes, so the consumer reads at most the window and then a broken
    // stream.
    {
        let (producer, consumer) = pair(window(SMALL_WINDOW)).await;
        // A short heartbeat, so the consumer's watchdog ends the broken stream
        // in a fraction of a second rather than the default seconds.
        let mut anchor = consumer
            .velo
            .anchor_manager()
            .create_anchor_with_config::<u32>(AnchorConfig {
                heartbeat_interval: Some(Duration::from_millis(50)),
                ..AnchorConfig::default()
            });
        let ticket = consumer
            .velo
            .prebind_anchor(anchor.handle())
            .expect("ticket");
        let sender = producer
            .velo
            .open_anchor_stream::<u32>(anchor.handle(), ticket)
            .await
            .expect("open stream");
        for i in 0..ITEMS {
            sender.send(i).await.expect("send");
        }
        let retired = sender.retired();
        sender.finalize().expect("finalize");
        producer
            .velo
            .graceful_shutdown(ShutdownPolicy::Timeout(Duration::from_millis(100)))
            .await;
        drop(producer);
        let mut items = 0;
        let end = loop {
            match tokio::time::timeout(PATIENCE, anchor.next()).await {
                Ok(Some(Ok(StreamFrame::Item(_)))) => items += 1,
                other => break other,
            }
        };
        assert!(
            items < ITEMS && !matches!(end, Ok(Some(Ok(StreamFrame::Finalized)))),
            "the tail survived a shutdown that cut it: {items} items, then {end:?}"
        );
        assert_eq!(retired.now_or_never(), Some(Retirement::Discarded));
    }

    // After `Sent`: every trial reads the whole stream.
    for trial in 0..TRIALS {
        let (producer, consumer) = pair(window(SMALL_WINDOW)).await;
        let (mut anchor, sender) = stream_of_items(&producer, &consumer).await;
        let reader = tokio::spawn(async move {
            read_to_the_end(&mut anchor).await;
        });
        let retired = sender.retired();
        sender.finalize().expect("finalize");
        assert_eq!(
            tokio::time::timeout(PATIENCE, retired).await,
            Ok(Retirement::Sent),
            "trial {trial}"
        );
        producer
            .velo
            .graceful_shutdown(ShutdownPolicy::Timeout(Duration::from_millis(100)))
            .await;
        drop(producer);
        tokio::time::timeout(PATIENCE, reader)
            .await
            .unwrap_or_else(|_| panic!("trial {trial}: the reader hung"))
            .unwrap_or_else(|error| panic!("trial {trial}: {error}"));
    }
}

/// Detaching hands the anchor on with `Detached`, a terminal: the stream
/// retires as sent once it goes out. Dropping a sender sends `Dropped`, also
/// a terminal, and retires the same way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn detach_and_drop_retire_as_sent() {
    let (producer, consumer) = pair(window(256)).await;

    let (mut detached, sender) = stream_of_items(&producer, &consumer).await;
    let retired = sender.retired();
    sender.detach().expect("detach");
    assert_eq!(
        tokio::time::timeout(PATIENCE, retired).await,
        Ok(Retirement::Sent)
    );
    for _ in 0..ITEMS {
        assert!(matches!(
            detached.next().await,
            Some(Ok(StreamFrame::Item(_)))
        ));
    }
    assert!(matches!(
        detached.next().await,
        Some(Ok(StreamFrame::Detached))
    ));

    let (_dropped, sender) = stream_of_items(&producer, &consumer).await;
    let retired = sender.retired();
    drop(sender);
    assert_eq!(
        tokio::time::timeout(PATIENCE, retired).await,
        Ok(Retirement::Sent)
    );
}

/// On the same worker the terminal goes straight into the anchor's channel,
/// and that is the retirement: there is nothing further on this node to hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_same_worker_stream_retires_when_its_terminal_is_queued() {
    let node = node(window(256)).await;
    let mut anchor = node.velo.create_anchor::<u32>();
    let sender = node
        .velo
        .attach_anchor::<u32>(anchor.handle())
        .await
        .expect("attach");
    sender.send(1).await.expect("send");
    let retired = sender.retired();
    sender.finalize().expect("finalize");
    assert_eq!(retired.now_or_never(), Some(Retirement::Sent));
    assert!(matches!(
        anchor.next().await,
        Some(Ok(StreamFrame::Item(1)))
    ));

    // The consumer already gone: the terminal has nowhere to go.
    let anchor = node.velo.create_anchor::<u32>();
    let sender = node
        .velo
        .attach_anchor::<u32>(anchor.handle())
        .await
        .expect("attach");
    let retired = sender.retired();
    drop(anchor);
    let _ = sender.finalize();
    assert_eq!(
        tokio::time::timeout(PATIENCE, retired).await,
        Ok(Retirement::Discarded)
    );
}

/// A per-stream TCP transport (the mux turned off): the stream retires once
/// its own pump has written the terminal to the socket. The pump reports it;
/// the sender does not, because the terminal is still on this node while it
/// waits in the egress channel.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_per_stream_transport_retires_once_its_terminal_is_written() {
    let (producer, consumer) = pair(MuxConfig {
        enabled: false,
        ..MuxConfig::default()
    })
    .await;
    // A ticket is the mux's; a per-stream transport attaches.
    let mut anchor = consumer.velo.create_anchor::<u32>();
    let sender = producer
        .velo
        .attach_anchor::<u32>(anchor.handle())
        .await
        .expect("attach");
    assert_ne!(
        sender.negotiated_transport().map(|key| key.as_str()),
        Some(velo::streaming::MESSENGER_MUX_KEY),
        "the stream must not ride the mux"
    );
    for i in 0..ITEMS {
        sender.send(i).await.expect("send");
    }
    let retired = sender.retired();
    sender.finalize().expect("finalize");
    assert_eq!(
        tokio::time::timeout(PATIENCE, retired).await,
        Ok(Retirement::Sent)
    );
    read_to_the_end(&mut anchor).await;
}

/// An item larger than the eager budget rides the mux as a handle, and its
/// bytes stay staged on the producer until the consumer pulls them. `Sent`
/// does not wait for that pull: it resolves when the handle's batch is
/// admitted, while the producer still holds the item. This is the documented
/// limitation; a shutdown right after `Sent` can cut such an item, because
/// the drain does not wait for the pull either. A later fix shows up here as
/// a change to what this test asserts.
///
/// The consumer never registers the producer, so it cannot pull: the staged
/// handle is still held when `Sent` resolves, with nothing racing it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sent_resolves_while_a_staged_item_is_still_held() {
    /// Well over the default eager budget (256 KiB), so the item is staged.
    const STAGED: usize = 1 << 20;

    let producer = node(window(256)).await;
    let consumer = node(window(256)).await;
    producer
        .velo
        .register_peer(consumer.velo.peer_info())
        .expect("register consumer");
    tokio::time::timeout(
        PATIENCE,
        producer
            .velo
            .wait_for_handler(consumer.velo.instance_id(), "_anchor_attach"),
    )
    .await
    .expect("handshake timed out")
    .expect("handshake");

    let anchor = consumer.velo.create_anchor::<Vec<u8>>();
    let ticket = consumer
        .velo
        .prebind_anchor(anchor.handle())
        .expect("ticket");
    let sender = producer
        .velo
        .open_anchor_stream::<Vec<u8>>(anchor.handle(), ticket)
        .await
        .expect("open stream");
    sender.send(vec![7u8; STAGED]).await.expect("send");
    let retired = sender.retired();
    sender.finalize().expect("finalize");
    assert_eq!(
        tokio::time::timeout(PATIENCE, retired).await,
        Ok(Retirement::Sent)
    );

    // The first slot the producer staged: the item, still here.
    let handle = velo::DataHandle::pack(producer.velo.instance_id().worker_id(), 1);
    let staged = producer
        .velo
        .metadata(handle)
        .await
        .expect("the staged item is still held when Sent resolves");
    assert!(
        staged.total_len as usize >= STAGED,
        "the staged slot is the item: {staged:?}"
    );
    drop(anchor);
}

/// A node whose streams ride per-stream gRPC.
#[cfg(feature = "grpc")]
async fn grpc_node() -> Node {
    let registry = Registry::new();
    let metrics = Arc::new(VeloMetrics::register(&registry).expect("metrics"));
    let velo = Velo::builder()
        .add_transport(tcp())
        .stream_config(velo::StreamConfig::Grpc(Some(velo::GrpcConfig {
            bind_addr: "127.0.0.1:0".parse().expect("loopback"),
        })))
        .expect("stream config")
        .metrics(metrics)
        .messenger_mux(MuxConfig {
            enabled: false,
            ..MuxConfig::default()
        })
        .expect("mux config")
        .build()
        .await
        .expect("build velo");
    Node { velo, registry }
}

/// A (producer, consumer) pair over per-stream gRPC that know each other.
#[cfg(feature = "grpc")]
async fn grpc_pair() -> (Node, Node) {
    let producer = grpc_node().await;
    let consumer = grpc_node().await;
    producer
        .velo
        .register_peer(consumer.velo.peer_info())
        .expect("register consumer");
    consumer
        .velo
        .register_peer(producer.velo.peer_info())
        .expect("register producer");
    for (a, b) in [(&producer, &consumer), (&consumer, &producer)] {
        tokio::time::timeout(
            PATIENCE,
            a.velo
                .wait_for_handler(b.velo.instance_id(), "_anchor_attach"),
        )
        .await
        .expect("handshake timed out")
        .expect("handshake");
    }
    (producer, consumer)
}

/// Over gRPC, the per-stream pump reports `Sent` once the server has taken the
/// whole request stream, terminal included: the call's response ends only
/// then.
#[cfg(feature = "grpc")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_per_stream_grpc_transport_retires_once_its_terminal_is_taken() {
    let (producer, consumer) = grpc_pair().await;
    let mut anchor = consumer.velo.create_anchor::<u32>();
    let sender = producer
        .velo
        .attach_anchor::<u32>(anchor.handle())
        .await
        .expect("attach");
    assert_eq!(
        sender.negotiated_transport().map(|key| key.as_str()),
        Some("grpc-stream"),
        "the stream must ride gRPC"
    );
    for i in 0..ITEMS {
        sender.send(i).await.expect("send");
    }
    let retired = sender.retired();
    sender.finalize().expect("finalize");
    assert_eq!(
        tokio::time::timeout(PATIENCE, retired).await,
        Ok(Retirement::Sent)
    );
    read_to_the_end(&mut anchor).await;
}

/// Items of a few bytes under and over the per-stream frame limit, as byte
/// items: a msgpack `bin` is one copy, where 16 million serde elements would
/// take seconds in a debug build.
const FRAME_LIMIT: usize = velo::streaming::MAX_ITEM_BYTES;
/// A payload of the limit's size encodes a few bytes over it.
const OVER_THE_LIMIT: usize = FRAME_LIMIT;
/// One 64 bytes shorter encodes under it.
const UNDER_THE_LIMIT: usize = FRAME_LIMIT - 64;

/// A per-stream sender refuses an item larger than its transport carries in
/// one frame at `send`, before anything leaves the producer, and the stream
/// stays open; one just under the limit crosses. The limit is one value on
/// every per-stream transport of this crate, and the gRPC server decodes
/// with it, so a frame the producer lets through is never one the far end
/// refuses.
async fn an_item_over_the_limit_fails_at_send(producer: &Node, consumer: &Node) {
    use velo::streaming::SendError;
    let mut anchor = consumer.velo.create_anchor::<Bytes>();
    let sender = producer
        .velo
        .attach_anchor::<Bytes>(anchor.handle())
        .await
        .expect("attach");
    let refused = sender.send_bytes(vec![0u8; OVER_THE_LIMIT]).await;
    assert!(
        matches!(refused, Err(SendError::ItemTooLarge { limit, .. }) if limit == FRAME_LIMIT),
        "{refused:?}"
    );
    sender
        .send_bytes(vec![1u8; UNDER_THE_LIMIT])
        .await
        .expect("an item under the limit");
    let retired = sender.retired();
    sender.finalize().expect("finalize");
    // A 16 MiB item over loopback: a wait wider than PATIENCE, for a slow
    // runner.
    let wide = Duration::from_secs(30);
    assert_eq!(
        tokio::time::timeout(wide, retired).await,
        Ok(Retirement::Sent)
    );
    match tokio::time::timeout(wide, anchor.next_bytes()).await {
        Ok(Some(Ok(StreamFrame::Item(item)))) if item.len() == UNDER_THE_LIMIT => {}
        other => panic!("the item under the limit: {other:?}"),
    }
    match tokio::time::timeout(wide, anchor.next_bytes()).await {
        Ok(Some(Ok(StreamFrame::Finalized))) => {}
        other => panic!("expected Finalized: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_item_over_the_tcp_limit_fails_at_send() {
    let (producer, consumer) = pair(MuxConfig {
        enabled: false,
        ..MuxConfig::default()
    })
    .await;
    an_item_over_the_limit_fails_at_send(&producer, &consumer).await;
}

#[cfg(feature = "grpc")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_item_over_the_grpc_limit_fails_at_send() {
    let (producer, consumer) = grpc_pair().await;
    an_item_over_the_limit_fails_at_send(&producer, &consumer).await;
}

/// An MPSC sender on a per-stream transport refuses an oversize item the same
/// way, and keeps sending after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_mpsc_item_over_the_tcp_limit_fails_at_send() {
    use velo::streaming::{MpscFrame, SendError};
    let (producer, consumer) = pair(MuxConfig {
        enabled: false,
        ..MuxConfig::default()
    })
    .await;
    let mut anchor = consumer.velo.create_mpsc_anchor::<Vec<u8>>();
    let sender = producer
        .velo
        .attach_mpsc_anchor::<Vec<u8>>(anchor.handle())
        .await
        .expect("attach");
    let refused = sender.send(vec![0u8; OVER_THE_LIMIT]).await;
    assert!(
        matches!(refused, Err(SendError::ItemTooLarge { limit, .. }) if limit == FRAME_LIMIT),
        "{refused:?}"
    );
    sender.send(vec![1u8; 16]).await.expect("a small item");
    drop(sender);
    match tokio::time::timeout(PATIENCE, anchor.next()).await {
        Ok(Some(Ok((_, MpscFrame::Item(item))))) if item.len() == 16 => {}
        other => panic!("the small item: {other:?}"),
    }
}

/// On the same worker a cancel through a `StreamController`, with the anchor
/// kept, leaves the terminal in the anchor's channel: that is `Sent`, as the
/// docs say; only the anchor's drop gives `Discarded` there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_same_worker_cancel_through_a_controller_leaves_the_terminal_queued() {
    let node = node(window(256)).await;
    let anchor = node.velo.create_anchor::<u32>();
    let sender = node
        .velo
        .attach_anchor::<u32>(anchor.handle())
        .await
        .expect("attach");
    let retired = sender.retired();
    anchor.controller().cancel();
    // The cancel reached the sender: its token fired, and a send is refused.
    tokio::time::timeout(PATIENCE, sender.cancellation_token().cancelled())
        .await
        .expect("the cancel reaches the sender");
    assert!(matches!(
        sender.send(2).await,
        Err(velo::streaming::SendError::ChannelClosed)
    ));
    let _ = sender.finalize();
    assert_eq!(
        tokio::time::timeout(PATIENCE, retired).await,
        Ok(Retirement::Sent)
    );
    drop(anchor);
}
