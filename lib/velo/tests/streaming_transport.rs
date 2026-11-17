// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for VeloBuilder + StreamConfig.
//!
//! These tests exercise the full Velo facade with streaming transport
//! configuration (TCP and gRPC).

use std::sync::Arc;

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
    use velo::streaming::{MpscFrame, SenderId};

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
    use velo::streaming::MpscAnchorConfig;

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

/// `Velo::builder().add_transport(t).build()`, with no `.stream_config()`
/// and no `.messenger_mux()` call, installs the mux as its only stream
/// transport. It opens no per-stream listener and advertises no stream
/// endpoint. A per-stream listener is something a node asks for.
#[tokio::test(flavor = "multi_thread")]
async fn the_default_builder_installs_only_the_mux() {
    let velo = tcp_node(velo::Velo::builder()).await;

    assert_eq!(
        velo.stream_transport_keys(),
        [velo::streaming::MESSENGER_MUX_KEY],
        "a default node installs the mux and nothing else"
    );
    assert_eq!(
        velo.default_stream_transport_key(),
        velo::streaming::MESSENGER_MUX_KEY,
        "the mux is the default node's stream transport"
    );
    let advertised = velo
        .peer_info()
        .worker_address()
        .available_transports()
        .expect("decode advertised transports");
    assert!(
        !advertised
            .iter()
            .any(|k| k.as_str() == "tcp-stream" || k.as_str() == "grpc-stream"),
        "a default node advertises no stream endpoint; got {advertised:?}"
    );
    velo.shutdown(velo::ShutdownPolicy::WaitForever).await;
}

/// `stream_config` and `stream_bind_addr` still install the per-stream
/// transport beside the mux, register both, and advertise the stream
/// endpoint, so a node that opts in serves peers without the mux.
#[tokio::test(flavor = "multi_thread")]
async fn an_explicit_stream_config_installs_the_per_stream_transport() {
    let opted_in = [
        (
            velo::Velo::builder()
                .stream_config(velo::StreamConfig::Tcp(None))
                .unwrap(),
            "tcp-stream",
        ),
        (
            velo::Velo::builder().stream_bind_addr(std::net::Ipv4Addr::LOCALHOST.into()),
            "tcp-stream",
        ),
        #[cfg(feature = "grpc")]
        (
            velo::Velo::builder()
                .stream_config(velo::StreamConfig::Grpc(Some(velo::GrpcConfig {
                    bind_addr: "127.0.0.1:0".parse().unwrap(),
                })))
                .unwrap(),
            "grpc-stream",
        ),
    ];
    for (builder, key) in opted_in {
        let velo = tcp_node(builder).await;
        let keys = velo.stream_transport_keys();
        let mut expected = vec![
            key.to_string(),
            velo::streaming::MESSENGER_MUX_KEY.to_string(),
        ];
        expected.sort();
        assert_eq!(
            keys, expected,
            "{key}: the per-stream transport and the mux"
        );
        assert_eq!(velo.default_stream_transport_key(), key);
        let advertised = velo
            .peer_info()
            .worker_address()
            .available_transports()
            .expect("decode advertised transports");
        assert!(
            advertised.iter().any(|k| k.as_str() == key),
            "an opted-in node advertises {key}; got {advertised:?}"
        );
        velo.shutdown(velo::ShutdownPolicy::WaitForever).await;
    }
}

/// With the mux off and no stream config a node would have no remote stream
/// transport at all. `build` refuses it, and the error names the two calls
/// that opt in to the per-stream path, so the fix is in the message. That the
/// refusal comes before any transport starts is pinned by
/// `a_disabled_mux_without_a_stream_config_fails_before_a_transport_starts`
/// in the lib tests, which can see a mock transport's state.
#[tokio::test]
async fn a_disabled_mux_without_a_stream_config_fails_the_build() {
    let error = match velo::Velo::builder()
        .messenger_mux(velo::streaming::MuxConfig {
            enabled: false,
            ..Default::default()
        })
        .unwrap()
        .build()
        .await
    {
        Ok(_) => panic!("a node with no stream transport was built"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("stream_config"), "{error}");
    assert!(error.contains("stream_bind_addr"), "{error}");
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
/// The test wires two Velos through an in-memory PeerDiscovery, registers
/// each with discovery, has worker B resolve worker A via
/// `discover_and_register_peer`, and then drives a full attach + send cycle
/// across the streaming transport. A pre-fix Velo fails at the attach step.
#[tokio::test(flavor = "multi_thread")]
async fn test_discover_and_register_peer_fans_out_to_streaming() {
    use futures::StreamExt;
    use velo::StreamFrame;
    use velo::discovery::PeerDiscovery;

    #[derive(Default)]
    struct MemoryDiscovery(std::sync::Mutex<Vec<velo::PeerInfo>>);
    impl PeerDiscovery for MemoryDiscovery {
        fn discover_by_worker_id(
            &self,
            id: velo::WorkerId,
        ) -> futures::future::BoxFuture<'_, anyhow::Result<velo::PeerInfo>> {
            Box::pin(async move {
                self.0
                    .lock()
                    .unwrap()
                    .iter()
                    .find(|peer| peer.worker_id() == id)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("unknown worker"))
            })
        }
        fn discover_by_instance_id(
            &self,
            id: velo::InstanceId,
        ) -> futures::future::BoxFuture<'_, anyhow::Result<velo::PeerInfo>> {
            Box::pin(async move {
                self.0
                    .lock()
                    .unwrap()
                    .iter()
                    .find(|peer| peer.instance_id == id)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("unknown instance"))
            })
        }
    }
    let discovery = Arc::new(MemoryDiscovery::default());
    let mk = || async {
        // Disable the mux to exercise the per-stream peer-registration path.
        tcp_node(
            velo::Velo::builder()
                .discovery(discovery.clone() as Arc<dyn PeerDiscovery>)
                .stream_bind_addr(std::net::Ipv4Addr::LOCALHOST.into())
                .messenger_mux(velo::streaming::MuxConfig {
                    enabled: false,
                    ..Default::default()
                })
                .unwrap(),
        )
        .await
    };
    let a = mk().await;
    let b = mk().await;

    // Both register themselves into discovery using the *merged* address
    // (messenger + streaming), so the streaming entry is visible to the
    // discovering side. Velo doesn't auto-publish — that's the caller's job.
    discovery
        .0
        .lock()
        .unwrap()
        .extend([a.peer_info(), b.peer_info()]);

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

/// Two nodes on the default builder, or, with `mux`, two that set the mux
/// config in code and opt in to the per-stream path, which a disabled mux
/// needs.
async fn default_pair(mux: Option<velo::streaming::MuxConfig>) -> (velo::Velo, velo::Velo) {
    let mk = || async {
        let mut builder = velo::Velo::builder();
        if let Some(config) = mux.clone() {
            builder = builder
                .stream_bind_addr(std::net::Ipv4Addr::LOCALHOST.into())
                .messenger_mux(config)
                .unwrap();
        }
        tcp_node(builder).await
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
    use velo::StreamFrame;

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

/// `enabled: false` on both nodes that opted in to the per-stream path is
/// the rollback: the attach negotiates the per-stream transport, and the
/// stream still flows.
#[tokio::test(flavor = "multi_thread")]
async fn turning_the_mux_off_falls_back_to_the_per_stream_transport() {
    let off = velo::streaming::MuxConfig {
        enabled: false,
        ..velo::streaming::MuxConfig::default()
    };
    assert_eq!(negotiated_key(Some(off)).await, "tcp-stream");
}

async fn tcp_node(builder: velo::VeloBuilder) -> velo::Velo {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let transport = velo::transports::tcp::TcpTransportBuilder::new()
        .from_listener(listener)
        .unwrap()
        .build()
        .unwrap();
    builder
        .add_transport(Arc::new(transport))
        .build()
        .await
        .unwrap()
}

/// All stream forms and transparent large-message transfers work without an
/// extra stream listener, including when the services feature is absent.
#[tokio::test(flavor = "multi_thread")]
async fn mux_only_carries_streams_and_large_messages() {
    use bytes::Bytes;
    use futures::StreamExt;
    use velo::StreamFrame;
    use velo::streaming::{MESSENGER_MUX_KEY, MpscFrame};

    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let producer = tcp_node(velo::Velo::builder().mux_only()).await;
        let consumer = tcp_node(velo::Velo::builder().mux_only()).await;
        producer.register_peer(consumer.peer_info()).unwrap();
        consumer.register_peer(producer.peer_info()).unwrap();
        for node in [&producer, &consumer] {
            assert_eq!(node.stream_transport_keys(), [MESSENGER_MUX_KEY]);
            assert!(
                node.peer_info()
                    .worker_address
                    .get_entry("tcp-stream")
                    .unwrap()
                    .is_none()
            );
        }

        let payloads = [vec![1, 2, 3], vec![7; 512 * 1024]];
        for prebound in [false, true] {
            let mut anchor = consumer.create_anchor::<Vec<u8>>();
            let sender = if prebound {
                let ticket = consumer.prebind_anchor(anchor.handle()).unwrap();
                producer
                    .open_anchor_stream(anchor.handle(), ticket)
                    .await
                    .unwrap()
            } else {
                producer.attach_anchor(anchor.handle()).await.unwrap()
            };
            for payload in &payloads {
                sender.send(payload.clone()).await.unwrap();
            }
            sender.finalize().unwrap();
            let mut received = Vec::new();
            while let Some(frame) = anchor.next().await {
                if let StreamFrame::Item(payload) = frame.unwrap() {
                    received.push(payload);
                }
            }
            assert_eq!(received, payloads);
        }

        let mut anchor = consumer.create_mpsc_anchor::<u32>();
        let s1 = producer
            .attach_mpsc_anchor::<u32>(anchor.handle())
            .await
            .unwrap();
        let s2 = producer
            .attach_mpsc_anchor::<u32>(anchor.handle())
            .await
            .unwrap();
        s1.send(10).await.unwrap();
        s2.send(20).await.unwrap();
        let mut received = Vec::new();
        while received.len() < 2 {
            if let (id, MpscFrame::Item(value)) = anchor.next().await.unwrap().unwrap() {
                received.push((id, value));
            }
        }
        assert!(received.contains(&(s1.sender_id(), 10)));
        assert!(received.contains(&(s2.sender_id(), 20)));
        anchor.cancel();
        drop((s1, s2));

        consumer
            .register_handler(
                velo::Handler::unary_handler("echo", |ctx| Ok(Some(ctx.payload))).build(),
            )
            .unwrap();
        let large = Bytes::from(payloads[1].clone());
        let response = producer
            .unary("echo")
            .unwrap()
            .raw_payload(large.clone())
            .instance(consumer.instance_id())
            .send()
            .await
            .unwrap();
        assert_eq!(response, large);
        producer.shutdown(velo::ShutdownPolicy::WaitForever).await;
        consumer.shutdown(velo::ShutdownPolicy::WaitForever).await;
    })
    .await
    .expect("mux-only operations must finish");
}

/// A node with the mux as its only stream transport refuses an attach from a
/// peer that does not offer the mux, for SPSC and MPSC, and the refusal
/// names the calls that would let it serve that peer. The reverse attach
/// fails on the peer, which finds no transport in common.
async fn refuses_peers_that_need_a_stream_listener(receiver: velo::VeloBuilder) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mux = tcp_node(receiver).await;
        let legacy = tcp_node(
            velo::Velo::builder()
                .stream_bind_addr(std::net::Ipv4Addr::LOCALHOST.into())
                .messenger_mux(velo::streaming::MuxConfig {
                    enabled: false,
                    ..Default::default()
                })
                .unwrap(),
        )
        .await;
        mux.register_peer(legacy.peer_info()).unwrap();
        legacy.register_peer(mux.peer_info()).unwrap();
        let refused = |error: String| {
            assert!(
                error.contains("the sender does not offer the messenger mux"),
                "{error}"
            );
            assert!(
                error.contains("the anchor's node has no per-stream transport"),
                "{error}"
            );
            assert!(!error.contains("this instance"), "{error}");
            assert!(error.contains("stream_config"), "{error}");
            assert!(error.contains("stream_bind_addr"), "{error}");
        };
        let anchor = mux.create_anchor::<u32>();
        refused(
            legacy
                .attach_anchor::<u32>(anchor.handle())
                .await
                .unwrap_err()
                .to_string(),
        );
        let mpsc = mux.create_mpsc_anchor::<u32>();
        refused(
            legacy
                .attach_mpsc_anchor::<u32>(mpsc.handle())
                .await
                .unwrap_err()
                .to_string(),
        );
        let legacy_anchor = legacy.create_anchor::<u32>();
        let error = mux
            .attach_anchor::<u32>(legacy_anchor.handle())
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("no common streaming transport"),
            "{error}"
        );
        drop((anchor, mpsc, legacy_anchor));
        mux.shutdown(velo::ShutdownPolicy::WaitForever).await;
        legacy.shutdown(velo::ShutdownPolicy::WaitForever).await;
    })
    .await
    .expect("incompatible peers must fail promptly");
}

/// A default node has no per-stream listener, so it refuses a peer without
/// the mux. A `mux_only()` node builds the same node, because `mux_only` is
/// read only to validate the builder.
#[tokio::test(flavor = "multi_thread")]
async fn a_default_node_rejects_peers_that_need_a_stream_listener() {
    refuses_peers_that_need_a_stream_listener(velo::Velo::builder()).await;
}

/// A mux rollback needs the *same* per-stream transport on both ends, not
/// just some per-stream transport on each. A TCP node and a gRPC node attach
/// over the mux; with the mux off on both, each offers only its own key
/// (`tcp-stream`, `grpc-stream`), and the receiver selects only its own, so
/// the attach is refused in both directions. The tuning guide's rollback
/// steps rest on this.
#[cfg(feature = "grpc")]
#[tokio::test(flavor = "multi_thread")]
async fn a_mux_rollback_needs_the_same_per_stream_transport_on_both_ends() {
    fn mux(enabled: bool) -> velo::streaming::MuxConfig {
        velo::streaming::MuxConfig {
            enabled,
            ..Default::default()
        }
    }
    async fn pair(mux_on: bool) -> (velo::Velo, velo::Velo) {
        let tcp = tcp_node(
            velo::Velo::builder()
                .stream_bind_addr(std::net::Ipv4Addr::LOCALHOST.into())
                .messenger_mux(mux(mux_on))
                .unwrap(),
        )
        .await;
        let grpc = tcp_node(
            velo::Velo::builder()
                .stream_config(velo::StreamConfig::Grpc(Some(velo::GrpcConfig {
                    bind_addr: "127.0.0.1:0".parse().unwrap(),
                })))
                .unwrap()
                .messenger_mux(mux(mux_on))
                .unwrap(),
        )
        .await;
        tcp.register_peer(grpc.peer_info()).unwrap();
        grpc.register_peer(tcp.peer_info()).unwrap();
        (tcp, grpc)
    }

    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        // Control: with the mux on, the two attach over it.
        let (tcp, grpc) = pair(true).await;
        for (producer, consumer) in [(&tcp, &grpc), (&grpc, &tcp)] {
            let anchor = consumer.create_anchor::<u32>();
            let sender = producer
                .attach_anchor::<u32>(anchor.handle())
                .await
                .expect("a TCP node and a gRPC node share the mux");
            sender.finalize().unwrap();
            drop(anchor);
        }
        tcp.shutdown(velo::ShutdownPolicy::WaitForever).await;
        grpc.shutdown(velo::ShutdownPolicy::WaitForever).await;

        // With the mux off on both, no key is shared.
        let (tcp, grpc) = pair(false).await;
        for (producer, consumer, missing) in
            [(&tcp, &grpc, "grpc-stream"), (&grpc, &tcp, "tcp-stream")]
        {
            let anchor = consumer.create_anchor::<u32>();
            let error = producer
                .attach_anchor::<u32>(anchor.handle())
                .await
                .unwrap_err()
                .to_string();
            assert!(error.contains("no common streaming transport"), "{error}");
            assert!(error.contains(missing), "{error}");
            drop(anchor);
        }
        tcp.shutdown(velo::ShutdownPolicy::WaitForever).await;
        grpc.shutdown(velo::ShutdownPolicy::WaitForever).await;
    })
    .await
    .expect("the rollback check must finish promptly");
}

#[tokio::test]
async fn mux_only_rejects_conflicting_configuration_before_startup() {
    for builder in [
        velo::Velo::builder()
            .mux_only()
            .stream_config(velo::StreamConfig::Tcp(None))
            .unwrap(),
        velo::Velo::builder()
            .stream_bind_addr("127.0.0.1".parse().unwrap())
            .mux_only(),
        velo::Velo::builder()
            .mux_only()
            .messenger_mux(velo::streaming::MuxConfig {
                enabled: false,
                ..Default::default()
            })
            .unwrap(),
    ] {
        let error = match builder.build().await {
            Ok(_) => panic!("conflicting mux-only configuration was accepted"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("mux_only"), "{error}");
    }
}
