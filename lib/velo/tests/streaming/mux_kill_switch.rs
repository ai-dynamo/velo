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
use velo::streaming::MuxConfig;

/// A node on the default, or, with `mux`, one that sets the mux config in code.
async fn node(mux: Option<MuxConfig>) -> Arc<Velo> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let transport = Arc::new(
        velo::transports::tcp::TcpTransportBuilder::new()
            .from_listener(listener)
            .unwrap()
            .build()
            .unwrap(),
    );
    let mut builder = Velo::builder().add_transport(transport);
    if let Some(config) = mux {
        builder = builder.messenger_mux(config).unwrap();
    }
    builder.build().await.unwrap()
}

/// Two mux nodes would stream over the mux; with the variable set, neither
/// installs it, and the attach negotiates the per-stream transport. This is the
/// rollback for an application that does not expose `MuxConfig::enabled`.
///
/// The consumer is on the default and the producer sets `enabled: true` in
/// code, so the one test pins both halves of the rule: the variable turns the
/// default off, and it wins over an explicit `enabled: true`.
#[tokio::test]
async fn the_kill_switch_turns_the_default_mux_off() {
    // SAFETY: this binary has one test, and it sets the variable before any
    // `Velo` exists, so no other thread reads the environment concurrently.
    unsafe { std::env::set_var("VELO_MESSENGER_MUX_DISABLE", "1") };

    let consumer = node(None).await;
    let producer = node(Some(MuxConfig {
        enabled: true,
        ..MuxConfig::default()
    }))
    .await;
    // Both nodes, not just one: negotiation picks the per-stream key whenever
    // either side lacks the mux, so the attach below alone would pass with a
    // producer that kept it.
    for node in [&consumer, &producer] {
        assert!(
            !node
                .anchor_manager()
                .transport_registry
                .contains_key(velo::streaming::MESSENGER_MUX_KEY),
            "the kill switch must keep the mux out of every registry"
        );
    }
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
