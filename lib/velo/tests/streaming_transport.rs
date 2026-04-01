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
            velo_transports::tcp::TcpTransportBuilder::new()
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
            velo_transports::tcp::TcpTransportBuilder::new()
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
