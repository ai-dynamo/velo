// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! `VELO_MESSENGER_MUX_DISABLE` turns the default mux off.
//!
//! A test binary of its own, with one test, because it sets a process-wide
//! environment variable: in a shared binary it would switch the mux off for
//! every other test building a `Velo` at the same time. The parsing rule is
//! pinned by `the_kill_switches_read_only_affirmatives` in `lib.rs`.

use std::sync::Arc;

use velo::Velo;

async fn default_node() -> Arc<Velo> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let transport = Arc::new(
        velo::transports::tcp::TcpTransportBuilder::new()
            .from_listener(listener)
            .unwrap()
            .build()
            .unwrap(),
    );
    Velo::builder()
        .add_transport(transport)
        .build()
        .await
        .unwrap()
}

/// Two nodes that never call `messenger_mux()` would stream over the mux; with
/// the variable set, neither installs it, and the attach negotiates the
/// per-stream transport. This is the rollback for an application that does not
/// expose `MuxConfig::enabled`.
#[tokio::test]
async fn the_kill_switch_turns_the_default_mux_off() {
    // SAFETY: this binary has one test, and it sets the variable before it
    // builds any node, on a current-thread runtime.
    unsafe { std::env::set_var("VELO_MESSENGER_MUX_DISABLE", "1") };

    let consumer = default_node().await;
    let producer = default_node().await;
    assert!(
        !consumer
            .anchor_manager()
            .transport_registry
            .contains_key(velo::streaming::MESSENGER_MUX_KEY),
        "the kill switch must keep the mux out of the registry"
    );
    consumer.register_peer(producer.peer_info()).unwrap();
    producer.register_peer(consumer.peer_info()).unwrap();

    let anchor = consumer.create_anchor::<u32>();
    let sender = producer
        .attach_anchor::<u32>(anchor.handle())
        .await
        .expect("remote attach");
    assert_eq!(
        sender.negotiated_transport().map(|k| k.as_str()),
        Some("tcp-stream")
    );
}
