// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Graceful-shutdown behavior as peers observe it, driven through the public
//! `Velo::begin_drain` / `Velo::graceful_shutdown` API.
//!
//! Drain rejection chain under test: the server's listener rejects a `Message`
//! frame during drain by echoing its header in a `ShuttingDown` frame on the
//! socket the client dialed; the client's dialed-connection reader routes the
//! echo onto its `shutdown_stream`; the messenger's dedicated shutdown
//! handler recovers the response id from the request-format header and fails
//! the awaiter.
//!
//! Phase-2 chain under test: the dispatcher acquires an in-flight guard per
//! handler invocation, so `graceful_shutdown` waits for a running handler
//! instead of tearing down under it.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use velo::transports::Transport;
use velo::transports::tcp::TcpTransportBuilder;
use velo::*;

fn new_tcp_transport() -> Arc<dyn Transport> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    Arc::new(
        TcpTransportBuilder::new()
            .from_listener(listener)
            .unwrap()
            .build()
            .unwrap(),
    )
}

#[cfg(unix)]
fn new_uds_transport() -> Arc<dyn Transport> {
    let dir = std::env::temp_dir().join(format!("velo-drain-test-{}", InstanceId::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    Arc::new(
        velo::transports::uds::UdsTransportBuilder::new()
            .socket_path(dir.join("velo.sock"))
            .build()
            .unwrap(),
    )
}

/// A unary request to a peer that called `begin_drain` must fail fast with
/// the drain rejection instead of hanging until the response timeout.
async fn unary_to_draining_peer_fails_fast(make: fn() -> Arc<dyn Transport>) {
    let server = Velo::builder().add_transport(make()).build().await.unwrap();
    let ping = Handler::unary_handler("ping", |ctx| Ok(Some(ctx.payload))).build();
    server.register_handler(ping).unwrap();

    let client = Velo::builder().add_transport(make()).build().await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    client.register_peer(server.peer_info()).unwrap();
    server.register_peer(client.peer_info()).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Warm-up while the server is healthy: handshake completes and the
    // client's connection is established.
    let echoed: Bytes = client
        .unary("ping")
        .unwrap()
        .raw_payload(Bytes::from_static(b"warmup"))
        .instance(server.instance_id())
        .send()
        .await
        .unwrap();
    assert_eq!(echoed, Bytes::from_static(b"warmup"));

    // Instance-level gate through the public API; the server's listener now
    // rejects Message frames.
    server.begin_drain();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let result = tokio::time::timeout(
        Duration::from_secs(2),
        client
            .unary("ping")
            .unwrap()
            .raw_payload(Bytes::from_static(b"rejected"))
            .instance(server.instance_id())
            .send(),
    )
    .await;

    let err = result
        .expect("drain rejection must complete the request promptly, not hang until the response timeout")
        .expect_err("request during drain must fail");
    assert!(
        err.to_string().contains("shutting down"),
        "unexpected error: {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_unary_to_draining_peer_fails_fast() {
    unary_to_draining_peer_fails_fast(new_tcp_transport).await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uds_unary_to_draining_peer_fails_fast() {
    unary_to_draining_peer_fails_fast(new_uds_transport).await;
}

/// Phase 2 of `graceful_shutdown` must wait for a handler invocation that is
/// already running, and complete promptly once it finishes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graceful_shutdown_waits_for_in_flight_handlers() {
    let server = Velo::builder()
        .add_transport(new_tcp_transport())
        .build()
        .await
        .unwrap();

    // `entered` signals the handler has started (its in-flight guard is
    // held); `release` lets the test decide when it finishes.
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let (entered_h, release_h) = (entered.clone(), release.clone());
    let slow = Handler::unary_handler_async("slow", move |ctx| {
        let entered = entered_h.clone();
        let release = release_h.clone();
        async move {
            entered.add_permits(1);
            let _permit = release.acquire().await.expect("release semaphore closed");
            Ok(Some(ctx.payload))
        }
    })
    .build();
    server.register_handler(slow).unwrap();

    let client = Velo::builder()
        .add_transport(new_tcp_transport())
        .build()
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    client.register_peer(server.peer_info()).unwrap();
    server.register_peer(client.peer_info()).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let pending = tokio::spawn(
        client
            .unary("slow")
            .unwrap()
            .raw_payload(Bytes::from_static(b"x"))
            .instance(server.instance_id())
            .send(),
    );

    // Handler is now parked inside its invocation, guard held.
    tokio::time::timeout(Duration::from_secs(2), entered.acquire())
        .await
        .expect("handler never started")
        .expect("entered semaphore closed")
        .forget();

    let shutdown = tokio::spawn(async move {
        server.graceful_shutdown(ShutdownPolicy::WaitForever).await;
    });

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !shutdown.is_finished(),
        "graceful_shutdown must wait for the in-flight handler"
    );

    release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), shutdown)
        .await
        .expect("graceful_shutdown must complete once the handler finishes")
        .unwrap();

    // Whether the response beat the teardown is a separate race; just make
    // sure the client task winds down rather than asserting its outcome.
    let _ = tokio::time::timeout(Duration::from_millis(500), pending).await;
}
