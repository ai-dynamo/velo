// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The client's service set and the hub's stay in agreement: through a
//! re-registration in flight, a refused request, a gateway in front of the
//! hub, and a guard dropped and made again at once.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use common::*;
use velo_hub::Hub;
use velo_hub::client::{ClientError, HubClient};
use velo_hub::store::testing::FaultStore;
use velo_hub::velo::InstanceId;
use velo_hub::velo::discovery::{ServiceDiscovery, ServiceRegistrationGuard};

fn client_for(hub: &Hub) -> HubClient {
    HubClient::builder()
        .host("127.0.0.1")
        .public_port(hub.public_addr().port())
        .control_port(hub.control_addr().port())
        .build()
        .unwrap()
}

async fn hub_instances(hub: &Hub, name: &str) -> Vec<InstanceId> {
    client_for(hub)
        .services()
        .get_instances(name)
        .await
        .unwrap()
}

/// An extension whose `on_registered` sleeps 700 ms when the registrant
/// opts in: a conforming slow registration (the contract allows up to
/// `step_timeout` = 5 s per extension).
#[derive(Default)]
struct Slow;

impl velo_hub::Extension for Slow {
    const NAME: &'static str = "slow";
    type Registration = ();
    type Staged = ();
    async fn prepare(
        &self,
        _: &velo_hub::Txn<'_>,
        _: Option<()>,
    ) -> Result<(), velo_hub::Rejection> {
        Ok(())
    }
    fn commit(&self, _: (), _: &velo_hub::Committed<'_>) {}
    async fn on_registered(&self, reg: &velo_hub::Committed<'_>) {
        if reg.extensions.contains_key("slow") {
            tokio::time::sleep(Duration::from_millis(700)).await;
        }
    }
    fn on_removed(&self, _: velo_hub::store::Removal) {}
}

fn slow_opt_in() -> BTreeMap<String, serde_json::Value> {
    let mut m = BTreeMap::new();
    m.insert("slow".to_string(), serde_json::Value::Null);
    m
}

/// A guard dropped while a re-registration is in flight (the transaction is
/// in `on_registered`, its credential already rotated): the drop's sync
/// waits for the registration, and the registration carries the client's
/// names, so the hub does not keep a name the client stopped serving.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_guard_dropped_during_a_re_registration_leaves_no_entry() {
    let hub = builder().extension(Slow).serve().await.unwrap();
    let client = client_for(&hub);
    let peer = make_peer();
    client
        .register_peer(peer.clone(), Default::default())
        .await
        .unwrap();
    let guard = client.register_service("x").await.unwrap();
    assert_eq!(hub_instances(&hub, "x").await, [peer.instance_id()]);

    let again = {
        let client = client.clone();
        let peer = peer.clone();
        tokio::spawn(async move { client.register_peer(peer, slow_opt_in()).await })
    };
    // The transaction is in on_registered: credential committed, no answer yet.
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(guard);
    again.await.unwrap().expect("the re-registration succeeds");
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert!(
        client.is_registered(),
        "control: the client holds the new registration"
    );
    assert!(
        hub_instances(&hub, "x").await.is_empty(),
        "the hub still lists the instance under a name the client stopped serving"
    );
    // The name was given back: serving it again works, and the hub lists it.
    let _second = client.register_service("x").await.unwrap();
    assert_eq!(hub_instances(&hub, "x").await, [peer.instance_id()]);
}

/// The same window through the guard's `unregister`: when it answers Ok,
/// the hub no longer lists the name.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unregister_during_a_re_registration_removes_the_entry() {
    let hub = builder().extension(Slow).serve().await.unwrap();
    let client = client_for(&hub);
    let peer = make_peer();
    client
        .register_peer(peer.clone(), Default::default())
        .await
        .unwrap();
    let mut guard = client.register_service("x").await.unwrap();

    let again = {
        let client = client.clone();
        let peer = peer.clone();
        tokio::spawn(async move { client.register_peer(peer, slow_opt_in()).await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    let answer = guard.unregister().await;
    again.await.unwrap().expect("the re-registration succeeds");
    tokio::time::sleep(Duration::from_millis(300)).await;
    let listed = hub_instances(&hub, "x").await;
    assert!(
        answer.is_err() || listed.is_empty(),
        "unregister answered {answer:?} but the hub lists {listed:?}"
    );
}

/// An `unregister` of a guard while the owner's registration holds the
/// store put: the sync waits for the registration, then removes the entry;
/// a retry is safe and the entry does not stay.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unregister_beside_a_registration_removes_the_entry() {
    let store = Arc::new(FaultStore::default());
    let hub = builder().store(store.clone()).serve().await.unwrap();
    let client = client_for(&hub);
    let peer = make_peer();
    client
        .register_peer(peer.clone(), Default::default())
        .await
        .unwrap();
    let mut guard = client.register_service("x").await.unwrap();

    let gate = store.gate_next_put();
    let again = {
        let client = client.clone();
        let peer = peer.clone();
        tokio::spawn(async move { client.register_peer(peer, Default::default()).await })
    };
    gate.entered().await;
    // The unregister waits for the registration, which holds the client's
    // operation lock, then removes the entry under the new credential.
    let unregister = tokio::spawn(async move { guard.unregister().await.map(|()| guard) });
    tokio::time::sleep(Duration::from_millis(100)).await;
    gate.release();
    again.await.unwrap().expect("the re-registration succeeds");
    let mut guard = unregister.await.unwrap().expect("the unregister succeeds");
    assert!(
        hub_instances(&hub, "x").await.is_empty(),
        "the hub still lists the instance after unregister"
    );
    // A retry is safe.
    guard.unregister().await.unwrap();
}

/// A one-shot proxy in front of the hub's control listener: forwards every
/// request, and when `fail` is set, waits for the hub's answer, discards
/// it, and answers 504 with no body, like a gateway whose upstream timeout
/// fired.
async fn proxy_to(upstream: String, fail: Arc<AtomicBool>) -> std::net::SocketAddr {
    use velo_hub::axum::body::{Body, to_bytes};
    use velo_hub::axum::extract::Request;
    use velo_hub::axum::response::Response;
    let http = reqwest::Client::new();
    let app = velo_hub::axum::Router::new().fallback(move |req: Request| {
        let http = http.clone();
        let upstream = upstream.clone();
        let fail = fail.clone();
        async move {
            let (parts, body) = req.into_parts();
            let bytes = to_bytes(body, usize::MAX).await.unwrap();
            let path = parts
                .uri
                .path_and_query()
                .map(|p| p.as_str().to_string())
                .unwrap_or_else(|| "/".to_string());
            let method = reqwest::Method::from_bytes(parts.method.as_str().as_bytes()).unwrap();
            let mut request = http.request(method, format!("{upstream}{path}"));
            for (name, value) in &parts.headers {
                if name != "host" && name != "content-length" {
                    request = request.header(name.as_str(), value.as_bytes());
                }
            }
            let answer = request.body(bytes).send().await.unwrap();
            let status = answer.status().as_u16();
            let body = answer.bytes().await.unwrap();
            if fail.swap(false, Ordering::SeqCst) {
                return Response::builder().status(504).body(Body::empty()).unwrap();
            }
            Response::builder()
                .status(status)
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap()
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { velo_hub::axum::serve(listener, app).await.unwrap() });
    addr
}

/// A re-registration the hub committed but a gateway answered 504 for is no
/// answer from the hub: the client fails closed, so it does not hold the
/// credential the hub rotated away (its handler would answer the hub's
/// probes ok for a registration nobody can use).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gateway_504_on_a_re_registration_fails_closed() {
    let hub = start().await;
    let fail = Arc::new(AtomicBool::new(false));
    let proxy = proxy_to(format!("http://{}", hub.control_addr()), fail.clone()).await;
    let client = HubClient::builder()
        .host("127.0.0.1")
        .public_port(hub.public_addr().port())
        .control_port(proxy.port())
        .build()
        .unwrap();
    let peer = make_peer();
    let first = client
        .register_peer(peer.clone(), Default::default())
        .await
        .unwrap();

    fail.store(true, Ordering::SeqCst);
    let answer = client.register_peer(peer.clone(), Default::default()).await;
    assert!(
        matches!(answer, Err(ClientError::Unexpected { status, .. }) if status.as_u16() == 504),
        "{answer:?}"
    );
    // The hub committed and rotated: the credential the client held is dead.
    assert!(
        hub.authority()
            .authorize(peer.instance_id(), Some(&first.credential))
            .is_err(),
        "control: the hub rotated the credential"
    );
    assert!(
        !client.is_registered(),
        "the client holds a credential the hub rotated away; a re-registration with no answer must fail closed"
    );
}

/// Drop a guard and serve the same name again at once, 40 times: one writer
/// sends the client's requests in order, so the old guard's removal never
/// lands after the new guard's add.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_a_guard_and_serving_the_name_again_keeps_the_entry() {
    let hub = start().await;
    let client = client_for(&hub);
    let peer = make_peer();
    client
        .register_peer(peer.clone(), Default::default())
        .await
        .unwrap();
    let mut lost = 0;
    for _ in 0..40 {
        let first = client.register_service("x").await.unwrap();
        drop(first);
        let second = client.register_service("x").await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        if hub_instances(&hub, "x").await.is_empty() {
            lost += 1;
        }
        let mut second = second;
        second.unregister().await.unwrap();
    }
    assert_eq!(
        lost, 0,
        "a live guard's entry was deleted by the previous guard's detached DELETE in {lost} of 40 rounds"
    );
}

/// A proxy that fails the next request of one method with 504.
async fn proxy_failing(
    upstream: String,
    fail: Arc<AtomicBool>,
    method_to_fail: &'static str,
) -> std::net::SocketAddr {
    use velo_hub::axum::body::{Body, to_bytes};
    use velo_hub::axum::extract::Request;
    use velo_hub::axum::response::Response;
    let http = reqwest::Client::new();
    let app = velo_hub::axum::Router::new().fallback(move |req: Request| {
        let http = http.clone();
        let upstream = upstream.clone();
        let fail = fail.clone();
        async move {
            let (parts, body) = req.into_parts();
            let bytes = to_bytes(body, usize::MAX).await.unwrap();
            let path = parts
                .uri
                .path_and_query()
                .map(|p| p.as_str().to_string())
                .unwrap_or_else(|| "/".to_string());
            let method = reqwest::Method::from_bytes(parts.method.as_str().as_bytes()).unwrap();
            let is_target = parts.method.as_str() == method_to_fail;
            let mut request = http.request(method, format!("{upstream}{path}"));
            for (name, value) in &parts.headers {
                if name != "host" && name != "content-length" {
                    request = request.header(name.as_str(), value.as_bytes());
                }
            }
            let answer = request.body(bytes).send().await.unwrap();
            let status = answer.status().as_u16();
            let body = answer.bytes().await.unwrap();
            if is_target && fail.swap(false, Ordering::SeqCst) {
                return Response::builder().status(504).body(Body::empty()).unwrap();
            }
            Response::builder()
                .status(status)
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap()
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { velo_hub::axum::serve(listener, app).await.unwrap() });
    addr
}

/// `register` answers Ok once the registration is held, whatever happens to
/// a service request after it, so the answer and `is_registered()` agree.
/// The registration carries the names, so the hub lists them after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn register_answers_ok_once_the_registration_is_held() {
    let hub = start().await;
    let fail = Arc::new(AtomicBool::new(false));
    let proxy = proxy_failing(
        format!("http://{}", hub.control_addr()),
        fail.clone(),
        "PUT",
    )
    .await;
    let client = HubClient::builder()
        .host("127.0.0.1")
        .public_port(hub.public_addr().port())
        .control_port(proxy.port())
        .build()
        .unwrap();
    let peer = make_peer();
    client
        .register_peer(peer.clone(), Default::default())
        .await
        .unwrap();
    let _guard = client.register_service("x").await.unwrap();
    fail.store(true, Ordering::SeqCst);
    let answer = client.register_peer(peer.clone(), Default::default()).await;
    assert!(
        answer.is_ok() || !client.is_registered(),
        "register_peer answered {answer:?} while the client holds the registration"
    );
    assert_eq!(hub_instances(&hub, "x").await, [peer.instance_id()]);
}
