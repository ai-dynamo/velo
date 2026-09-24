// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for VeloBuilder + StreamConfig.
//!
//! These tests exercise the full Velo facade with streaming transport
//! configuration (TCP and gRPC).

use std::sync::Arc;

// ---------------------------------------------------------------------------
// Test: VeloBuilder with TCP transport (TCP-09 / GRPC-08)
// ---------------------------------------------------------------------------

/// Validates that VeloBuilder.stream_config(StreamConfig::Tcp(None)) creates a
/// TcpFrameTransport and registers it under `tcp-stream`, beside the mux that
/// the builder installs by default. This is the canonical backward-compat
/// test for GRPC-08: StreamConfig::Tcp(None) must produce identical
/// AnchorManager setup as the old stream_bind_addr(0.0.0.0) call.
#[tokio::test(flavor = "multi_thread")]
async fn test_velo_builder_tcp_transport() {
    use velo::StreamConfig;

    let transport = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        Arc::new(
            velo::transports::tcp::TcpTransportBuilder::new()
                .from_listener(listener)
                .unwrap()
                .build()
                .unwrap(),
        )
    };

    let velo = velo::Velo::builder()
        .add_transport(transport)
        .stream_config(StreamConfig::Tcp(None))
        .expect("stream_config should succeed on first call")
        .build()
        .await
        .unwrap();

    // The StreamConfig branch and the mux, which the builder installs by
    // default beside it.
    let registry = &velo.anchor_manager().transport_registry;
    assert!(
        registry.contains_key("tcp-stream"),
        "transport_registry should contain 'tcp-stream' key"
    );
    assert!(registry.contains_key(velo::streaming::MESSENGER_MUX_KEY));
    assert_eq!(
        registry.len(),
        2,
        "transport_registry should hold the StreamConfig transport and the mux"
    );

    // Create an anchor to verify the setup works end-to-end
    let _anchor = velo.create_anchor::<String>();
}

// ---------------------------------------------------------------------------
// Test: VeloBuilder with gRPC transport (GRPC-06)
// ---------------------------------------------------------------------------

/// Validates that VeloBuilder.stream_config(StreamConfig::Grpc(None)) creates a
/// GrpcFrameTransport and registers it under `grpc-stream`, beside the mux that
/// the builder installs by default.
#[cfg(feature = "grpc")]
#[tokio::test(flavor = "multi_thread")]
async fn test_velo_builder_grpc_transport() {
    use velo::StreamConfig;

    let transport = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        Arc::new(
            velo::transports::tcp::TcpTransportBuilder::new()
                .from_listener(listener)
                .unwrap()
                .build()
                .unwrap(),
        )
    };

    let velo = velo::Velo::builder()
        .add_transport(transport)
        .stream_config(StreamConfig::Grpc(None))
        .expect("stream_config should succeed on first call")
        .build()
        .await
        .expect("VeloBuilder with Grpc config should build successfully");

    // The chosen streaming transport, and the mux beside it (the default).
    let registry = &velo.anchor_manager().transport_registry;
    assert!(
        registry.contains_key("grpc-stream"),
        "transport_registry should contain 'grpc-stream' key"
    );
    assert!(registry.contains_key(velo::streaming::MESSENGER_MUX_KEY));
    assert_eq!(
        registry.len(),
        2,
        "transport_registry should hold the StreamConfig transport and the mux"
    );

    let _anchor = velo.create_anchor::<String>();
}

// ---------------------------------------------------------------------------
// Test: Velo facade MPSC smoke test
// ---------------------------------------------------------------------------

/// Validates that the Velo top-level facade exposes MPSC anchors via
/// `Velo::create_mpsc_anchor` / `Velo::attach_mpsc_anchor`, that the
/// `velo::streaming::mpsc::*` namespace re-exports resolve, and that two
/// local senders on one anchor get distinct `SenderId`s with correct
/// per-sender item delivery.
#[tokio::test(flavor = "multi_thread")]
async fn test_velo_facade_mpsc_create_and_attach() {
    use futures::StreamExt;
    use velo::StreamConfig;
    use velo::streaming::mpsc::{MpscFrame, SenderId};

    let transport = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        Arc::new(
            velo::transports::tcp::TcpTransportBuilder::new()
                .from_listener(listener)
                .unwrap()
                .build()
                .unwrap(),
        )
    };

    let velo = velo::Velo::builder()
        .add_transport(transport)
        .stream_config(StreamConfig::Tcp(None))
        .expect("stream_config")
        .build()
        .await
        .unwrap();

    let mut anchor = velo.create_mpsc_anchor::<u32>();
    let handle = anchor.handle();

    let s1 = velo
        .attach_mpsc_anchor::<u32>(handle)
        .await
        .expect("attach s1");
    let s2 = velo
        .attach_mpsc_anchor::<u32>(handle)
        .await
        .expect("attach s2");
    assert_eq!(s1.sender_id(), SenderId(1));
    assert_eq!(s2.sender_id(), SenderId(2));

    s1.send(10).await.expect("s1 send");
    s2.send(20).await.expect("s2 send");

    let mut s1_items = Vec::new();
    let mut s2_items = Vec::new();
    while s1_items.is_empty() || s2_items.is_empty() {
        let frame = tokio::time::timeout(std::time::Duration::from_secs(3), anchor.next())
            .await
            .expect("no stall")
            .expect("frame")
            .expect("stream ok");
        match frame {
            (SenderId(1), MpscFrame::Item(v)) => s1_items.push(v),
            (SenderId(2), MpscFrame::Item(v)) => s2_items.push(v),
            (_, MpscFrame::SenderError(m)) => panic!("sender error: {m}"),
            _ => {}
        }
    }
    assert_eq!(s1_items, vec![10]);
    assert_eq!(s2_items, vec![20]);

    anchor.cancel();
}

/// Exercises `Velo::create_mpsc_anchor_with_config` — the per-anchor config
/// path that the minimal smoke test above doesn't hit. Verifies that
/// `max_senders` and `heartbeat_interval` are plumbed through the facade.
#[tokio::test(flavor = "multi_thread")]
async fn test_velo_facade_mpsc_with_config() {
    use velo::StreamConfig;
    use velo::streaming::mpsc::MpscAnchorConfig;

    let transport = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        Arc::new(
            velo::transports::tcp::TcpTransportBuilder::new()
                .from_listener(listener)
                .unwrap()
                .build()
                .unwrap(),
        )
    };

    let velo = velo::Velo::builder()
        .add_transport(transport)
        .stream_config(StreamConfig::Tcp(None))
        .expect("stream_config")
        .build()
        .await
        .unwrap();

    let config = MpscAnchorConfig {
        max_senders: Some(2),
        heartbeat_interval: Some(std::time::Duration::from_millis(250)),
        unattached_timeout: Some(std::time::Duration::from_secs(1)),
        channel_capacity: Some(64),
    };
    let anchor = velo.create_mpsc_anchor_with_config::<u32>(config);
    let handle = anchor.handle();

    // First two attaches succeed …
    let s1 = velo
        .attach_mpsc_anchor::<u32>(handle)
        .await
        .expect("attach s1");
    let s2 = velo
        .attach_mpsc_anchor::<u32>(handle)
        .await
        .expect("attach s2");

    // … third must hit the max_senders cap.
    let third = velo.attach_mpsc_anchor::<u32>(handle).await;
    assert!(
        matches!(
            third,
            Err(velo::AttachError::MaxSendersReached { limit: 2, .. })
        ),
        "third attach must be MaxSendersReached, got {third:?}"
    );

    drop(s1);
    drop(s2);
    anchor.cancel();
}

// ---------------------------------------------------------------------------
// Default-config coverage
// ---------------------------------------------------------------------------

/// `Velo::builder().add_transport(t).build()` — no `.stream_config()` and no
/// `.messenger_mux()` call — must wire a TCP streaming transport under the
/// `tcp-stream` key and the mux under `messenger-mux-v1`, and register both.
/// The default-config path otherwise has zero coverage: every other test in
/// this file calls `.stream_config(...)` explicitly, so a regression that
/// swapped either builder default would slip through CI.
#[tokio::test(flavor = "multi_thread")]
async fn test_velo_builder_default_stream_config_is_tcp() {
    let transport = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        Arc::new(
            velo::transports::tcp::TcpTransportBuilder::new()
                .from_listener(listener)
                .unwrap()
                .build()
                .unwrap(),
        )
    };

    let velo = velo::Velo::builder()
        .add_transport(transport)
        .build()
        .await
        .unwrap();

    let registry = &velo.anchor_manager().transport_registry;
    assert!(
        registry.contains_key("tcp-stream"),
        "default StreamConfig must wire TcpFrameTransport (key='tcp-stream'); registry: {:?}",
        registry.keys().collect::<Vec<_>>()
    );
    assert!(
        registry.contains_key(velo::streaming::MESSENGER_MUX_KEY),
        "the builder must install the mux by default; registry: {:?}",
        registry.keys().collect::<Vec<_>>()
    );
    assert_eq!(registry.len(), 2);

    // The local PeerInfo's WorkerAddress must include the streaming entry too,
    // so peers can resolve our streaming endpoint via PeerDiscovery.
    let advertised = velo
        .peer_info()
        .worker_address()
        .available_transports()
        .expect("decode advertised transports");
    assert!(
        advertised.iter().any(|k| k.as_str() == "tcp-stream"),
        "default-config peer_info must advertise 'tcp-stream' alongside messenger keys; got {advertised:?}"
    );
}

// ---------------------------------------------------------------------------
// discover_and_register_peer fan-out (Item A regression)
// ---------------------------------------------------------------------------

/// Regression: `Velo::discover_and_register_peer` must fan out to the
/// streaming transport. If it only registers the peer with the messenger,
/// the next `attach_anchor` for that peer fails with "TCP streaming: peer
/// not registered" — a silent production breakage for any caller using a
/// PeerDiscovery backend (etcd / NATS / filesystem).
///
/// The test wires two Velos through a `FilesystemPeerDiscovery`, registers
/// each with discovery, has worker B resolve worker A via
/// `discover_and_register_peer`, and then drives a full attach + send cycle
/// across the streaming transport. A pre-fix Velo fails at the attach step.
#[tokio::test(flavor = "multi_thread")]
async fn test_discover_and_register_peer_fans_out_to_streaming() {
    use futures::StreamExt;
    use velo::PeerDiscovery;
    use velo::discovery::FilesystemPeerDiscovery;
    use velo::streaming::StreamFrame;

    let tmp = tempfile::tempdir().unwrap();
    let discovery = Arc::new(FilesystemPeerDiscovery::new(tmp.path().join("peers.json")).unwrap());

    let mk = || async {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let transport = Arc::new(
            velo::transports::tcp::TcpTransportBuilder::new()
                .from_listener(listener)
                .unwrap()
                .build()
                .unwrap(),
        );
        // The mux is off: the mux never consults the per-stream peer table,
        // so under it the fan-out this test guards would go unexercised.
        velo::Velo::builder()
            .add_transport(transport)
            .discovery(discovery.clone() as Arc<dyn PeerDiscovery>)
            .messenger_mux(velo::streaming::MuxConfig {
                enabled: false,
                ..velo::streaming::MuxConfig::default()
            })
            .unwrap()
            .build()
            .await
            .unwrap()
    };
    let a = mk().await;
    let b = mk().await;

    // Both register themselves into discovery using the *merged* address
    // (messenger + streaming), so the streaming entry is visible to the
    // discovering side. Velo doesn't auto-publish — that's the caller's job.
    let _guard_a = discovery.register_peer_info(&a.peer_info()).await.unwrap();
    let _guard_b = discovery.register_peer_info(&b.peer_info()).await.unwrap();

    // A discovers B through PeerDiscovery. The streaming transport on A must
    // see B via this call — otherwise the attach below fails.
    a.discover_and_register_peer(b.instance_id()).await.unwrap();

    // B must also know about A so the messenger-side _anchor_attach AM and
    // the reverse data path resolve. Both directions matter here because
    // attach_anchor sends a control AM from A to B, and the bound TCP socket
    // is reached from A's side using B's streaming endpoint.
    b.discover_and_register_peer(a.instance_id()).await.unwrap();

    // Drive a full attach + send + finalize cycle to confirm the streaming
    // path is actually usable, not just that register() didn't error.
    let mut anchor = b.create_anchor::<u32>();
    let handle = anchor.handle();
    let sender = a.attach_anchor::<u32>(handle).await.expect(
        "attach_anchor must succeed when discover_and_register_peer fanned out to streaming",
    );
    assert_eq!(
        sender.negotiated_transport().map(|k| k.as_str()),
        Some("tcp-stream"),
        "the stream must ride the per-stream transport the fan-out feeds"
    );

    sender.send(7).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    sender.finalize().unwrap();

    let frame = tokio::time::timeout(std::time::Duration::from_secs(5), anchor.next())
        .await
        .expect("no stall")
        .expect("frame")
        .expect("stream ok");
    assert!(matches!(frame, StreamFrame::Item(7)));
}

// ---------------------------------------------------------------------------
// The mux is the default
// ---------------------------------------------------------------------------

async fn default_pair(
    mux: Option<velo::streaming::MuxConfig>,
) -> (Arc<velo::Velo>, Arc<velo::Velo>) {
    let mk = || async {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let transport = Arc::new(
            velo::transports::tcp::TcpTransportBuilder::new()
                .from_listener(listener)
                .unwrap()
                .build()
                .unwrap(),
        );
        let mut builder = velo::Velo::builder().add_transport(transport);
        if let Some(config) = mux.clone() {
            builder = builder.messenger_mux(config).unwrap();
        }
        builder.build().await.unwrap()
    };
    let a = mk().await;
    let b = mk().await;
    a.register_peer(b.peer_info()).unwrap();
    b.register_peer(a.peer_info()).unwrap();
    (a, b)
}

/// The key a remote attach negotiates between two nodes built with `mux`
/// (`None` is the builder default), after one item has crossed.
async fn negotiated_key(mux: Option<velo::streaming::MuxConfig>) -> String {
    use futures::StreamExt;
    use velo::streaming::StreamFrame;

    let (producer, consumer) = default_pair(mux).await;
    let mut anchor = consumer.create_anchor::<u32>();
    let sender = producer
        .attach_anchor::<u32>(anchor.handle())
        .await
        .expect("remote attach");
    let key = sender
        .negotiated_transport()
        .expect("a remote attach negotiates a transport")
        .as_str()
        .to_string();
    sender.send(7).await.unwrap();
    sender.finalize().unwrap();
    let frame = tokio::time::timeout(std::time::Duration::from_secs(5), anchor.next())
        .await
        .expect("no stall")
        .expect("frame")
        .expect("stream ok");
    assert!(matches!(frame, StreamFrame::Item(7)));
    key
}

/// Two nodes built with no mux configuration carry their streams on the mux.
/// This is the default the per-stream path lost: a connection per stream runs
/// out of local ports at a few thousand new streams a second.
#[tokio::test(flavor = "multi_thread")]
async fn two_default_nodes_stream_over_the_mux() {
    assert_eq!(
        negotiated_key(None).await,
        velo::streaming::MESSENGER_MUX_KEY
    );
}

/// `enabled: false` on both nodes is the rollback: the attach negotiates the
/// per-stream transport, and the stream still flows.
#[tokio::test(flavor = "multi_thread")]
async fn turning_the_mux_off_falls_back_to_the_per_stream_transport() {
    let off = velo::streaming::MuxConfig {
        enabled: false,
        ..velo::streaming::MuxConfig::default()
    };
    assert_eq!(negotiated_key(Some(off)).await, "tcp-stream");
}
