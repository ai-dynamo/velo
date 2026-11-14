// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Shared setup for the streaming integration tests.

use std::sync::Arc;

/// A TCP messenger transport on an ephemeral loopback port.
#[allow(dead_code)]
pub fn loopback_tcp() -> Arc<velo::transports::tcp::TcpTransport> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    Arc::new(
        velo::transports::tcp::TcpTransportBuilder::new()
            .from_listener(listener)
            .expect("from_listener")
            .build()
            .expect("build tcp transport"),
    )
}

/// One default `Velo` node on loopback, for tests whose sender and anchor
/// share the node. A local attach writes straight into the anchor's channel,
/// so no transport carries the frames.
#[allow(dead_code)]
pub async fn local_node() -> velo::Velo {
    velo::Velo::builder()
        .add_transport(loopback_tcp())
        .build()
        .await
        .expect("build velo")
}

/// `N` nodes that all know each other and carry their streams on the
/// per-stream TCP transport: each opens a stream listener on loopback, and
/// the mux is off, so an attach between them negotiates `tcp-stream`.
#[allow(dead_code)]
pub async fn tcp_stream_nodes<const N: usize>() -> [velo::Velo; N] {
    let mut nodes = Vec::with_capacity(N);
    for _ in 0..N {
        nodes.push(
            velo::Velo::builder()
                .add_transport(loopback_tcp())
                .stream_bind_addr(std::net::Ipv4Addr::LOCALHOST.into())
                .messenger_mux(velo::streaming::MuxConfig {
                    enabled: false,
                    ..velo::streaming::MuxConfig::default()
                })
                .expect("mux config")
                .build()
                .await
                .expect("build velo"),
        );
    }
    for a in &nodes {
        for b in &nodes {
            if a.instance_id() != b.instance_id() {
                a.register_peer(b.peer_info()).expect("register peer");
            }
        }
    }
    nodes.try_into().ok().expect("N nodes")
}

/// Two nodes from [`tcp_stream_nodes`].
#[allow(dead_code)]
pub async fn tcp_stream_pair() -> (velo::Velo, velo::Velo) {
    let [a, b] = tcp_stream_nodes::<2>().await;
    (a, b)
}
