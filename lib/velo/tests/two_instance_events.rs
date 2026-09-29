// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Two-instance distributed event integration tests.
//!
//! Tests that events created on one Velo instance can be subscribed to, awaited,
//! triggered, and poisoned from another instance.

use std::sync::Arc;
use std::time::Duration;

use velo::discovery::FilesystemPeerDiscovery;
use velo::transports::tcp::TcpTransportBuilder;
use velo::*;

async fn poll_until(timeout: Duration, mut condition: impl FnMut() -> bool) {
    let start = tokio::time::Instant::now();
    let mut interval = Duration::from_millis(5);
    while !condition() {
        if start.elapsed() > timeout {
            panic!("poll_until timed out after {:?}", timeout);
        }
        tokio::time::sleep(interval).await;
        interval = (interval * 2).min(Duration::from_millis(100));
    }
}

fn new_transport() -> Arc<velo::transports::tcp::TcpTransport> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    Arc::new(
        TcpTransportBuilder::new()
            .from_listener(listener)
            .unwrap()
            .build()
            .unwrap(),
    )
}

async fn make_pair() -> (Arc<Velo>, Arc<Velo>, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let discovery_file = tmp.path().join("peers.json");
    let discovery = Arc::new(FilesystemPeerDiscovery::new(&discovery_file).unwrap());

    let a = Velo::builder()
        .add_transport(new_transport())
        .discovery(discovery.clone() as Arc<dyn PeerDiscovery>)
        .build()
        .await
        .unwrap();

    let b = Velo::builder()
        .add_transport(new_transport())
        .discovery(discovery.clone() as Arc<dyn PeerDiscovery>)
        .build()
        .await
        .unwrap();

    a.register_peer(b.peer_info()).unwrap();
    b.register_peer(a.peer_info()).unwrap();

    // Verify bidirectional connectivity via handshake
    a.available_handlers(b.instance_id()).await.unwrap();
    b.available_handlers(a.instance_id()).await.unwrap();

    (a, b, tmp)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_event_trigger() {
    let (a, _b, _tmp) = make_pair().await;

    let em = a.event_manager();
    let event = em.new_event().unwrap();
    let handle = event.handle();

    let awaiter = em.awaiter(handle).unwrap();
    em.trigger(handle).unwrap();

    let result = tokio::time::timeout(Duration::from_secs(2), awaiter)
        .await
        .expect("Local event trigger timed out");
    result.expect("Local event trigger should resolve with Ok");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_event_poison() {
    let (a, _b, _tmp) = make_pair().await;

    let em = a.event_manager();
    let event = em.new_event().unwrap();
    let handle = event.handle();

    let awaiter = em.awaiter(handle).unwrap();
    em.poison(handle, "test failure").unwrap();

    let result = tokio::time::timeout(Duration::from_secs(2), awaiter)
        .await
        .expect("Local event poison timed out");
    assert!(
        result.is_err(),
        "Local event poison should resolve with Err"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_event_subscribe_and_trigger() {
    let (a, b, _tmp) = make_pair().await;

    // Create event on instance A
    let em_a = a.event_manager();
    let event = em_a.new_event().unwrap();
    let handle = event.handle();

    // Subscribe from instance B
    let em_b = b.event_manager();
    let awaiter = em_b.awaiter(handle).unwrap();

    // Wait for subscription to propagate
    let a_ref = a.clone();
    let b_id = b.instance_id();
    poll_until(Duration::from_secs(5), move || {
        a_ref.has_event_subscriber(handle, b_id)
    })
    .await;

    // Trigger on instance A
    em_a.trigger(handle).unwrap();

    // Instance B's awaiter should resolve
    let result = tokio::time::timeout(Duration::from_secs(5), awaiter)
        .await
        .expect("Remote event trigger timed out");
    result.expect("Remote event trigger should resolve subscriber's awaiter with Ok");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_event_poison() {
    let (a, b, _tmp) = make_pair().await;

    // Create event on A
    let em_a = a.event_manager();
    let event = em_a.new_event().unwrap();
    let handle = event.handle();

    // Subscribe from B
    let em_b = b.event_manager();
    let awaiter = em_b.awaiter(handle).unwrap();

    // Wait for subscription to propagate
    let a_ref = a.clone();
    let b_id = b.instance_id();
    poll_until(Duration::from_secs(5), move || {
        a_ref.has_event_subscriber(handle, b_id)
    })
    .await;

    // Poison on A
    em_a.poison(handle, "test error").unwrap();

    // B's awaiter should resolve (with poison)
    let result = tokio::time::timeout(Duration::from_secs(5), awaiter)
        .await
        .expect("Remote event poison timed out");
    assert!(
        result.is_err(),
        "Remote event poison should resolve subscriber's awaiter with Err"
    );
}

/// A trigger from an event's owner completes an awaiter on a node that is
/// draining. The awaiter belongs to work that node already accepted; if the
/// trigger is refused, that work never completes and a `WaitForever` drain
/// never ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_remote_trigger_reaches_a_draining_subscriber() {
    let (a, b, _tmp) = make_pair().await;
    let em_a = a.event_manager();
    let event = em_a.new_event().unwrap();
    let handle = event.handle();
    let awaiter = b.event_manager().awaiter(handle).unwrap();
    let a_ref = a.clone();
    let b_id = b.instance_id();
    poll_until(Duration::from_secs(5), move || {
        a_ref.has_event_subscriber(handle, b_id)
    })
    .await;

    b.begin_drain();
    em_a.trigger(handle).unwrap();
    tokio::time::timeout(Duration::from_secs(5), awaiter)
        .await
        .expect("the trigger never reached the draining subscriber")
        .expect("the event resolved with an error");
}

/// A remote trigger of a pending event reaches an owner that is draining.
///
/// The requester already holds the event's handle, so the trigger completes
/// work the owner accepted: its own awaiters, and the requester's ack. The
/// request is sent fire-and-forget, so a refusal echo cannot find the ack's
/// awaiter and the requester waits forever; the owner's awaiters never resolve
/// either, and a `WaitForever` drain waits on them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_remote_trigger_request_reaches_a_draining_owner() {
    let (a, b, _tmp) = make_pair().await;
    let handle = a.event_manager().new_event().unwrap().into_handle();
    let awaiter = a.event_manager().awaiter(handle).unwrap();

    a.begin_drain();
    tokio::time::timeout(Duration::from_secs(5), b.events().trigger(handle))
        .await
        .expect("the draining owner never acknowledged the trigger request")
        .expect("the trigger request failed");
    tokio::time::timeout(Duration::from_secs(5), awaiter)
        .await
        .expect("the owner's own awaiter never resolved")
        .expect("the event resolved with an error");
}

/// A subscriber reaches an owner that is draining, for an event the owner
/// completed before the drain. The owner answers with the completion; nothing
/// new starts. A refusal echo cannot find the subscriber's waiter, and the
/// waiter's pending mark stops any retry, so a refused subscribe hangs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_subscribe_reaches_a_draining_owner_of_a_completed_event() {
    let (a, b, _tmp) = make_pair().await;
    let handle = a.event_manager().new_event().unwrap().into_handle();
    a.event_manager().trigger(handle).unwrap();

    a.begin_drain();
    tokio::time::timeout(
        Duration::from_secs(5),
        b.event_manager().awaiter(handle).unwrap(),
    )
    .await
    .expect("the draining owner never answered the subscribe")
    .expect("the event resolved with an error");
}

/// A subscriber reaches an owner that is draining, for an event still pending.
/// The owner records one subscription and sends the completion when the event
/// fires, which here is after the drain began.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_subscribe_reaches_a_draining_owner_of_a_pending_event() {
    let (a, b, _tmp) = make_pair().await;
    let handle = a.event_manager().new_event().unwrap().into_handle();

    a.begin_drain();
    let awaiter = b.event_manager().awaiter(handle).unwrap();
    let a_ref = a.clone();
    let b_id = b.instance_id();
    poll_until(Duration::from_secs(5), move || {
        a_ref.has_event_subscriber(handle, b_id)
    })
    .await;
    a.event_manager().trigger(handle).unwrap();
    tokio::time::timeout(Duration::from_secs(5), awaiter)
        .await
        .expect("the trigger never reached the subscriber")
        .expect("the event resolved with an error");
}
