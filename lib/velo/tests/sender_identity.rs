// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Connection-sourced sender identity, over a real TCP wire.
//!
//! Every dialed connection opens with an `Identify` frame, so a handler can
//! read who the transport says the sender is — one identity per connection,
//! never a per-message claim — and the exact `PeerInfo` that identity
//! registered. A `_hello` claiming a different identity than its connection
//! announced is rejected and registers nothing.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use velo::transports::tcp::TcpTransportBuilder;
use velo::{Handler, ShutdownPolicy, Velo};

const BOUND: Duration = Duration::from_secs(10);

async fn tcp_node() -> Arc<Velo> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    Velo::builder()
        .add_transport(Arc::new(
            TcpTransportBuilder::new()
                .from_listener(listener)
                .unwrap()
                .build()
                .unwrap(),
        ))
        .mux_only()
        .build()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_message_carries_its_connections_identity() {
    let server = tcp_node().await;
    let seen = Arc::new(std::sync::Mutex::new(None));
    let recorded = Arc::clone(&seen);
    server
        .register_handler(
            Handler::unary_handler("who", move |ctx| {
                *recorded.lock().unwrap() = Some((ctx.sender_identity(), ctx.sender_peer_info()));
                Ok(Some(Bytes::from_static(b"ok")))
            })
            .build(),
        )
        .unwrap();
    let client = tcp_node().await;
    client.register_peer(server.peer_info()).unwrap();
    server.register_peer(client.peer_info()).unwrap();
    // Handshake, so the client takes the direct send path and the server has
    // the client's _hello registration when the message lands.
    client
        .messenger()
        .available_handlers(server.instance_id())
        .await
        .unwrap();

    let echoed = tokio::time::timeout(
        BOUND,
        client
            .unary("who")
            .unwrap()
            .raw_payload(Bytes::from_static(b"x"))
            .instance(server.instance_id())
            .send(),
    )
    .await
    .expect("the send resolves")
    .expect("the send succeeds");
    assert_eq!(echoed, Bytes::from_static(b"ok"));

    let (identity, peer_info) = seen.lock().unwrap().clone().expect("the handler ran");
    assert_eq!(
        identity.instance_id(),
        Some(client.instance_id()),
        "the connection announced the client"
    );
    assert_eq!(
        identity,
        velo::ext::SenderIdentity::AnnouncedPeer(client.instance_id()),
        "an accepted connection attributes as announced, not dialed"
    );
    assert_eq!(
        peer_info.expect("the client registered at _hello"),
        client.peer_info(),
        "the retained record is the client's own advertised peer info"
    );

    client
        .shutdown(ShutdownPolicy::Timeout(Duration::from_secs(1)))
        .await;
    server
        .shutdown(ShutdownPolicy::Timeout(Duration::from_secs(1)))
        .await;
}

/// A `_hello` whose claimed identity differs from its connection's
/// announcement is rejected and registers nothing: the announcement owns the
/// claim, so a payload cannot retarget an identity to another address.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hello_cannot_claim_another_identity_than_its_connection() {
    let server = tcp_node().await;
    server
        .register_handler(Handler::unary_handler("probe", |ctx| Ok(Some(ctx.payload))).build())
        .unwrap();
    let client = tcp_node().await;
    client.register_peer(server.peer_info()).unwrap();
    server.register_peer(client.peer_info()).unwrap();
    client
        .messenger()
        .available_handlers(server.instance_id())
        .await
        .unwrap();

    // Forge a hello claiming a different instance over the client's
    // identified connection.
    let bogus = velo::PeerInfo::new(
        velo::InstanceId::new_v4(),
        client.peer_info().worker_address().clone(),
    );
    let payload = serde_json::to_vec(&serde_json::json!({ "peer_info": bogus })).unwrap();
    let error = tokio::time::timeout(
        BOUND,
        client
            .messenger()
            .unary_streaming("_hello")
            .raw_payload(Bytes::from(payload))
            .instance(server.instance_id())
            .send(),
    )
    .await
    .expect("the rejection answers, not times out")
    .expect_err("a claim that outruns its connection must be rejected");
    assert!(
        error.to_string().contains("claimed"),
        "unexpected error: {error}"
    );
    assert!(
        server
            .messenger()
            .registered_peer_info(bogus.instance_id())
            .is_none(),
        "a rejected hello must not register"
    );
    // The honest registration from the real hello still stands.
    assert_eq!(
        server
            .messenger()
            .registered_peer_info(client.instance_id())
            .map(|info| info.instance_id()),
        Some(client.instance_id())
    );

    client
        .shutdown(ShutdownPolicy::Timeout(Duration::from_secs(1)))
        .await;
    server
        .shutdown(ShutdownPolicy::Timeout(Duration::from_secs(1)))
        .await;
}
