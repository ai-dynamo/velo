// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Admission-aware send results, over a real transport.
//!
//! The unit tests in `messenger/client/builders/tests.rs` pin the state machine
//! branch by branch against a hand-built admission gate. What needs a wire is
//! the pair of claims that machinery exists to make:
//!
//! - a fire-and-forget result nobody polls still delivers its frame, and
//! - `admitted()` finishes while a unary response is genuinely outstanding.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use velo::transports::tcp::{TcpTransport, TcpTransportBuilder};
use velo::*;

fn new_transport() -> Arc<TcpTransport> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    Arc::new(
        TcpTransportBuilder::new()
            .from_listener(listener)
            .unwrap()
            .build()
            .unwrap(),
    )
}

/// Two peers that know about each other, with `handler` registered on the
/// server. Returns `(client, server_instance)`.
async fn connected_pair(handler: Handler) -> (Arc<Velo>, Arc<Velo>) {
    let server = Velo::builder()
        .add_transport(new_transport())
        .build()
        .await
        .unwrap();
    server.register_handler(handler).unwrap();

    let client = Velo::builder()
        .add_transport(new_transport())
        .build()
        .await
        .unwrap();

    client.register_peer(server.peer_info()).unwrap();
    server.register_peer(client.peer_info()).unwrap();

    // Handshake once so later sends take the direct path rather than the
    // detached one; that is the path where admission is interesting.
    client
        .messenger()
        .available_handlers(server.instance_id())
        .await
        .unwrap();

    (client, server)
}

/// Poll `condition` until it holds, failing the test rather than hanging.
async fn wait_until(label: &str, mut condition: impl FnMut() -> bool) {
    let waited = tokio::time::timeout(Duration::from_secs(10), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(waited.is_ok(), "timed out waiting for {label}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unpolled_fire_result_still_delivers() {
    // The inversion this whole stack exists for: the send is issued when
    // `send()` returns, so dropping the result on the floor — the canonical
    // fire-and-forget shape — must not withdraw the frame.
    let received = Arc::new(AtomicUsize::new(0));
    let counter = received.clone();
    let handler = Handler::am_handler("sink", move |_ctx| {
        counter.fetch_add(1, Ordering::Release);
        Ok(())
    })
    .build();
    let (client, server) = connected_pair(handler).await;

    let sends = 8;
    for _ in 0..sends {
        // Deliberately not awaited, and dropped immediately.
        drop(
            client
                .am_send("sink")
                .unwrap()
                .raw_payload(Bytes::from_static(b"x"))
                .instance(server.instance_id())
                .send(),
        );
    }

    wait_until("every unpolled fire send to arrive", || {
        received.load(Ordering::Acquire) >= sends
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fire_result_reports_admission() {
    let handler = Handler::am_handler("sink", |_ctx| Ok(())).build();
    let (client, server) = connected_pair(handler).await;

    let mut result = client
        .am_send("sink")
        .unwrap()
        .raw_payload(Bytes::from_static(b"x"))
        .instance(server.instance_id())
        .send();

    // An established TCP connection with room in its send channel admits
    // synchronously; a busy one queues. Either way the wait resolves, and it
    // resolves without consuming the result.
    tokio::time::timeout(Duration::from_secs(5), result.admitted())
        .await
        .expect("the admission resolves")
        .expect("the frame is admitted");
    assert_eq!(result.admission_state(), AdmissionState::Admitted);
    result.await.expect("awaiting the result agrees");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admitted_finishes_while_a_unary_response_is_outstanding() {
    // The handler sits on the request, so the response cannot arrive for a
    // while. `admitted()` is about the frame leaving, not about the answer
    // coming back, so it must resolve long before the result does.
    let reply_delay = Duration::from_millis(500);
    let handler = Handler::unary_handler_async("slow_echo", move |ctx| async move {
        tokio::time::sleep(reply_delay).await;
        Ok(Some(ctx.payload))
    })
    .build();
    let (client, server) = connected_pair(handler).await;

    let mut result = client
        .unary("slow_echo")
        .unwrap()
        .raw_payload(Bytes::from_static(b"ping"))
        .instance(server.instance_id())
        .send();

    let started = std::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(5), result.admitted())
        .await
        .expect("the admission resolves")
        .expect("the frame is admitted");
    let admitted_after = started.elapsed();
    assert!(
        admitted_after < reply_delay,
        "admission waited for the response: {admitted_after:?}"
    );
    assert_eq!(result.admission_state(), AdmissionState::Admitted);

    // And the result is still good for the response afterwards.
    let response = tokio::time::timeout(Duration::from_secs(5), result)
        .await
        .expect("the unary result resolves")
        .expect("the unary succeeds");
    assert_eq!(response, Bytes::from_static(b"ping"));
    assert!(
        started.elapsed() >= reply_delay,
        "the response should have taken at least the handler's delay"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_send_to_an_unknown_peer_fails_its_admission() {
    let handler = Handler::am_handler("sink", |_ctx| Ok(())).build();
    let (client, _server) = connected_pair(handler).await;

    let mut result = client
        .am_send("sink")
        .unwrap()
        .raw_payload(Bytes::from_static(b"x"))
        .instance(InstanceId::new_v4())
        .send();

    let err = tokio::time::timeout(Duration::from_secs(5), result.admitted())
        .await
        .expect("an unroutable send resolves")
        .expect_err("an unregistered peer cannot be sent to");
    assert!(
        err.to_string().contains("not registered"),
        "unexpected error: {err}"
    );
    assert_eq!(result.admission_state(), AdmissionState::Failed);
}
