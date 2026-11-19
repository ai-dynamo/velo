// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! `VELO_MESSENGER_MUX_DISABLE` turns the default mux off, and a node needs
//! a stream config to run without it.
//!
//! A test binary of its own, with one test, because it sets a process-wide
//! environment variable: in a shared binary it would switch the mux off for
//! every other test building a `Velo` at the same time. The parsing rule is
//! pinned by `the_kill_switches_read_only_affirmatives` in `lib.rs`.
//!
//! The tests that expect the default mux, such as
//! `two_default_nodes_stream_over_the_mux`, fail when the variable is set in
//! the environment that runs them. That is the switch working.

use std::sync::Arc;

use velo::Velo;
use velo::streaming::MuxConfig;

/// A builder with a loopback messenger transport, on the default, or, with
/// `mux`, one that sets the mux config in code.
fn builder(mux: Option<MuxConfig>) -> velo::VeloBuilder {
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
    builder
}

/// A node that opted in to the per-stream path.
async fn per_stream_node(mux: Option<MuxConfig>) -> Velo {
    builder(mux)
        .stream_bind_addr(std::net::Ipv4Addr::LOCALHOST.into())
        .build()
        .await
        .unwrap()
}

/// The variable turns the mux off, and what that leaves depends on whether
/// the node opted in to the per-stream path.
///
/// A default node has no per-stream transport, so with the mux off it would
/// have no remote stream transport at all. Its `build` fails and names the
/// calls that opt in, so the rollback needs a code change and the operator
/// learns which one from the error.
///
/// Two nodes that opted in would stream over the mux; with the variable set,
/// neither installs it, and the attach negotiates the per-stream transport.
/// The consumer is on the default mux config and the producer sets
/// `enabled: true` in code, so the one test pins both halves of the rule: the
/// variable turns the default off, and it wins over an explicit
/// `enabled: true`.
///
/// One test function, not one per case: the binary must set the variable
/// once, before any `Velo` exists.
#[tokio::test]
async fn the_kill_switch_turns_the_default_mux_off() {
    // SAFETY: this binary has one test, and it sets the variable before any
    // `Velo` exists, so no other thread reads the environment concurrently.
    unsafe { std::env::set_var("VELO_MESSENGER_MUX_DISABLE", "1") };

    for mux in [
        None,
        Some(MuxConfig {
            enabled: true,
            ..MuxConfig::default()
        }),
    ] {
        let error = match builder(mux).build().await {
            Ok(_) => panic!("a node with the mux off and no stream config was built"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("stream_config"), "{error}");
        assert!(error.contains("stream_bind_addr"), "{error}");
        assert!(error.contains("VELO_MESSENGER_MUX_DISABLE"), "{error}");
    }

    let consumer = per_stream_node(None).await;
    let producer = per_stream_node(Some(MuxConfig {
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
                .stream_transport_keys()
                .iter()
                .any(|key| key == velo::streaming::MESSENGER_MUX_KEY),
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
