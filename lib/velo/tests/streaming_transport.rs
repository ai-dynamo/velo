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
/// TcpFrameTransport and populates the transport_registry with both "tcp" and
/// "velo" schemes. This is the canonical backward-compat test for GRPC-08:
/// StreamConfig::Tcp(None) must produce identical AnchorManager setup as the
/// old stream_bind_addr(0.0.0.0) call.
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

    // Verify transport_registry contains both schemes
    let registry = &velo.anchor_manager().transport_registry;
    assert!(
        registry.contains_key("tcp"),
        "transport_registry should contain 'tcp' scheme"
    );
    assert!(
        registry.contains_key("velo"),
        "transport_registry should contain 'velo' scheme"
    );
    assert_eq!(
        registry.len(),
        2,
        "transport_registry should have exactly 2 entries"
    );

    // Create an anchor to verify the setup works end-to-end
    let _anchor = velo.create_anchor::<String>();
}

// ---------------------------------------------------------------------------
// Test: VeloBuilder with gRPC transport (GRPC-06)
// ---------------------------------------------------------------------------

/// Validates that VeloBuilder.stream_config(StreamConfig::Grpc(None)) creates a
/// GrpcFrameTransport and populates the transport_registry with both "grpc" and
/// "velo" schemes.
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

    // Verify transport_registry contains "grpc" and "velo"
    let registry = &velo.anchor_manager().transport_registry;
    assert!(
        registry.contains_key("grpc"),
        "transport_registry should contain 'grpc' scheme"
    );
    assert!(
        registry.contains_key("velo"),
        "transport_registry should contain 'velo' scheme"
    );
    assert_eq!(
        registry.len(),
        2,
        "transport_registry should have exactly 2 entries"
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
