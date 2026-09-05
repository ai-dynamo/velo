// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the UCX transport.
//!
//! Runs over `UCX_TLS=tcp` — the identical code path as RDMA lanes, with no
//! hardware requirement — so this suite runs on stock CI runners.

#![cfg(all(target_os = "linux", feature = "ucx"))]

#[macro_use]
mod common;

use common::{OUTER_TEST_TIMEOUT, UcxFactory, scenarios};

transport_integration_tests!(UcxFactory);

/// UCX records the inbound frames it receives, like every other transport.
///
/// Two properties, and they fail differently.
///
/// `velo_transport_frames_total{direction="inbound",message_type="message",
/// outcome="accepted"}` is the numerator of the messenger's inbound-queue depth
/// (`accepted - velo_messenger_inbound_dequeued_total`), and that subtraction is
/// sound only while every transport contributes to it. A transport that admits
/// without recording reads as a queue draining faster than it fills — a
/// negative depth, reported as a healthy zero.
///
/// The other inbound types carry no such identity, and are asserted anyway
/// because `bind_transport` pre-creates a child for every direction x
/// message_type at zero: a family where only `message` ever moves does not read
/// as "responses are uninstrumented", it reads as "no responses arrived". That
/// is a worse failure than a family that is dark throughout.
///
/// Observability is set after `start()` deliberately, because a hand-driven
/// transport such as this one can — the `Transport` trait fixes no order
/// between the two, so the AM receive callback has to read the handle at call
/// time rather than capture it at construction. (The `Velo` builder happens to
/// set it *before* `start()`; the callback must not depend on either.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbound_frames_are_recorded_on_every_message_type() {
    use std::sync::Arc;
    use std::time::Duration;

    use prometheus::Registry;
    use velo::observability::VeloMetrics;
    use velo::transports::{MessageType, Transport};

    use crate::common::TransportFactory;

    let sender = UcxFactory::create().await.expect("ucx sender");
    let receiver = UcxFactory::create().await.expect("ucx receiver");

    let registry = Registry::new();
    let metrics = VeloMetrics::register(&registry).expect("register metrics");
    let observability: Arc<dyn velo_ext::TransportObservability> =
        Arc::new(metrics.bind_transport("ucx"));
    receiver.transport.set_observability(observability);

    sender.register_peer(&receiver).expect("register receiver");

    let (header, payload) = common::test_message(1);
    sender.send(receiver.instance_id, header, payload, MessageType::Message);
    receiver
        .recv_message(Duration::from_secs(5))
        .await
        .expect("the message must arrive");

    await_frame(&registry, "message", 1.0, "an admitted inbound Message").await;

    // A `Response` frame takes the other exit out of the same callback. It is
    // sent raw rather than as a reply to a real request because the frame's
    // *type* is what routes it here; nothing in this path reads its header.
    let (header, payload) = common::test_message(2);
    sender.send(receiver.instance_id, header, payload, MessageType::Response);
    receiver
        .recv_response(Duration::from_secs(5))
        .await
        .expect("the response must arrive");
    await_frame(&registry, "response", 1.0, "an inbound Response").await;

    // `Event` shares its exit with `Ack`, so one of the two covers both.
    let (header, payload) = common::test_message(3);
    sender.send(receiver.instance_id, header, payload, MessageType::Event);
    receiver
        .recv_event(Duration::from_secs(5))
        .await
        .expect("the event must arrive");
    await_frame(&registry, "event", 1.0, "an inbound Event").await;

    sender.shutdown();
    receiver.shutdown();
}

/// Poll one `message_type` child of the inbound accepted-frame counter until it
/// reaches `want`, or fail naming what stalled.
///
/// Polled rather than read once: a receive returns from inside the adapter
/// handoff, while the record runs after that handoff on the progress thread —
/// so the frame can be in hand a moment before the counter moves.
async fn await_frame(registry: &prometheus::Registry, message_type: &str, want: f64, why: &str) {
    use std::time::Duration;
    use velo::observability::test_helpers::MetricSnapshot;

    let read = || {
        MetricSnapshot::from_registry(registry).counter(
            "velo_transport_frames_total",
            &[
                ("transport", "ucx"),
                ("direction", "inbound"),
                ("message_type", message_type),
                ("outcome", "accepted"),
            ],
        )
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while read() < want {
        assert!(
            std::time::Instant::now() < deadline,
            "{why} must be recorded; velo_transport_frames_total\
             {{message_type=\"{message_type}\"}} stayed at {}",
            read()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
