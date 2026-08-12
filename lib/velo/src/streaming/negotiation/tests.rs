// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The negotiation decision table, exercised without a wire.
//!
//! Every arm a mixed deployment can produce is expressible here as three
//! numbers, so this is where the compatibility matrix is pinned; the
//! integration suite then checks that two real nodes reach the same verdicts.

use std::sync::Arc;

use futures::future::BoxFuture;
use velo_ext::{TransportKey, WorkerAddress, WorkerId};

use super::*;
use crate::streaming::messenger_mux::MuxConfig;

/// A stand-in for whichever legacy transport a node is configured with.
struct LegacyTransport(&'static str);

impl FrameTransport for LegacyTransport {
    fn key(&self) -> TransportKey {
        TransportKey::new(self.0)
    }

    fn address(&self) -> WorkerAddress {
        WorkerAddress::empty()
    }

    fn bind(
        &self,
        _anchor_id: u64,
        _session_id: u64,
    ) -> BoxFuture<'_, anyhow::Result<flume::Receiver<Vec<u8>>>> {
        Box::pin(async { Ok(flume::bounded::<Vec<u8>>(1).1) })
    }

    fn connect(
        &self,
        _peer: WorkerId,
        _anchor_id: u64,
        _session_id: u64,
    ) -> BoxFuture<'_, anyhow::Result<flume::Sender<Vec<u8>>>> {
        Box::pin(async { Ok(flume::bounded::<Vec<u8>>(1).0) })
    }
}

fn legacy() -> Arc<dyn FrameTransport> {
    Arc::new(LegacyTransport("tcp-stream"))
}

fn registry(keys: &[&str]) -> TransportRegistry {
    keys.iter()
        .map(|key| {
            (
                (*key).to_string(),
                Arc::new(LegacyTransport(Box::leak(Box::new((*key).to_string()))))
                    as Arc<dyn FrameTransport>,
            )
        })
        .collect()
}

/// A mux over a real messenger, which is what `advertised_limits` reads from.
async fn mux(config: MuxConfig) -> Arc<MessengerMuxTransport> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    let transport = Arc::new(
        crate::transports::tcp::TcpTransportBuilder::new()
            .from_listener(listener)
            .expect("from_listener")
            .build()
            .expect("build transport"),
    );
    let messenger = crate::messenger::Messenger::builder()
        .add_transport(transport)
        .build()
        .await
        .expect("messenger");
    MessengerMuxTransport::new(messenger, config, None).expect("mux")
}

fn keys(names: &[&str]) -> Vec<TransportKey> {
    names.iter().map(|name| TransportKey::new(*name)).collect()
}

fn names(keys: &[TransportKey]) -> Vec<&str> {
    keys.iter().map(TransportKey::as_str).collect()
}

// ---------------------------------------------------------------------------
// What a sender advertises
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_installed_mux_is_advertised_alongside_the_legacy_transport() {
    let mux = mux(MuxConfig::default()).await;
    let advertised = advertised_keys(
        &registry(&["tcp-stream", MESSENGER_MUX_KEY]),
        &legacy(),
        Some(&mux),
    );
    assert_eq!(names(&advertised), [MESSENGER_MUX_KEY, "tcp-stream"]);
}

#[test]
fn a_node_without_a_mux_never_advertises_one() {
    let advertised = advertised_keys(&registry(&["tcp-stream"]), &legacy(), None);
    assert_eq!(
        names(&advertised),
        ["tcp-stream"],
        "a key never advertised is a key never selected, which is what makes \
         flipping the switch off a complete rollback"
    );
}

#[test]
fn an_unpopulated_registry_advertises_the_default_transport() {
    // `resolve_transport` falls back to the default transport when no registry
    // was populated; the advertisement has to describe the same node.
    let advertised = advertised_keys(&TransportRegistry::new(), &legacy(), None);
    assert_eq!(names(&advertised), ["tcp-stream"]);
}

#[tokio::test]
async fn an_installed_mux_is_advertised_even_from_an_unpopulated_registry() {
    let mux = mux(MuxConfig::default()).await;
    let advertised = advertised_keys(&TransportRegistry::new(), &legacy(), Some(&mux));
    assert_eq!(names(&advertised), [MESSENGER_MUX_KEY, "tcp-stream"]);
}

// ---------------------------------------------------------------------------
// What a receiver selects
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_mux_is_selected_only_when_both_sides_named_it() {
    let mux = mux(MuxConfig {
        initial_credit: 64,
        slot_byte_budget: 4096,
        ..MuxConfig::default()
    })
    .await;

    let selected = select(
        &keys(&[MESSENGER_MUX_KEY, "tcp-stream"]),
        Some(&mux),
        &legacy(),
    );
    assert_eq!(selected.key.as_str(), MESSENGER_MUX_KEY);
    assert_eq!(selected.transport.key().as_str(), MESSENGER_MUX_KEY);
    assert_eq!(selected.initial_credit, 64);
    assert_eq!(selected.slot_byte_budget, 4096);
}

#[tokio::test]
async fn a_sender_that_did_not_name_the_mux_gets_the_legacy_transport() {
    let mux = mux(MuxConfig::default()).await;
    let selected = select(&keys(&["tcp-stream"]), Some(&mux), &legacy());
    assert_eq!(selected.key.as_str(), "tcp-stream");
    assert_eq!(selected.initial_credit, 0);
}

#[tokio::test]
async fn a_sender_from_before_negotiation_gets_the_legacy_transport() {
    // The whole compatibility claim in one line: an older sender omits the
    // field, `#[serde(default)]` makes that an empty list, and an empty list
    // cannot intersect.
    let mux = mux(MuxConfig::default()).await;
    let selected = select(&[], Some(&mux), &legacy());
    assert_eq!(selected.key.as_str(), "tcp-stream");
    assert_eq!(selected.initial_credit, 0);
    assert_eq!(selected.slot_byte_budget, 0);
}

#[test]
fn a_receiver_without_a_mux_never_answers_with_one() {
    let selected = select(&keys(&[MESSENGER_MUX_KEY]), None, &legacy());
    assert_eq!(
        selected.key.as_str(),
        "tcp-stream",
        "answering a key this node has not installed would break the sender's \
         resolve, which hard-errors on an unknown key"
    );
    assert_eq!(selected.initial_credit, 0);
}

// ---------------------------------------------------------------------------
// What a sender does with the answer
// ---------------------------------------------------------------------------

#[test]
fn a_legacy_key_takes_the_legacy_path_whatever_the_credit_fields_say() {
    assert_eq!(
        choose(&TransportKey::new("tcp-stream"), 0, 0),
        Ok(Connect::Legacy)
    );
    // Not a shape any peer emits, but the key is what decides — the credit
    // fields describe the mux and nothing else.
    assert_eq!(
        choose(&TransportKey::new("tcp-stream"), 256, 4096),
        Ok(Connect::Legacy)
    );
}

#[test]
fn a_negotiated_mux_opens_at_the_advertised_window() {
    let Ok(Connect::Mux(limits)) = choose(&TransportKey::new(MESSENGER_MUX_KEY), 64, 4096) else {
        panic!("a mux key with a window must negotiate the mux");
    };
    assert_eq!(limits.initial_credit(), 64);
    assert_eq!(limits.slot_byte_budget(), 4096);
    assert_eq!(
        limits.slot_buffer_depth(),
        65,
        "C + 1, terminal reserve included"
    );
}

#[test]
fn a_zero_byte_cap_means_the_default_rather_than_a_refusal() {
    // The half of the zero split that is easy to get backwards: this peer *is*
    // offering the mux, it just has no opinion on the byte cap.
    let Ok(Connect::Mux(limits)) = choose(&TransportKey::new(MESSENGER_MUX_KEY), 64, 0) else {
        panic!("a zero byte cap must not fall back to the legacy path");
    };
    assert_eq!(limits.initial_credit(), 64);
    assert_eq!(
        limits.slot_byte_budget(),
        crate::streaming::messenger_mux::flow_control::DEFAULT_SLOT_BYTE_BUDGET
    );
}

#[test]
fn a_zero_window_makes_the_mux_unusable_even_though_the_key_matched() {
    assert_eq!(
        choose(&TransportKey::new(MESSENGER_MUX_KEY), 0, 4096),
        Err(NegotiationError::LegacyPeer)
    );
    assert_eq!(
        choose(&TransportKey::new(MESSENGER_MUX_KEY), 0, 0),
        Err(NegotiationError::LegacyPeer)
    );
}
