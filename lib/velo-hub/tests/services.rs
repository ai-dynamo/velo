// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Service discovery through the hub: entries owned by a registration, the
//! routes, the SSE watch, and the client's `ServiceDiscovery`.

mod common;

use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use common::*;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use velo_hub::Hub;
use velo_hub::client::{ClientError, HubClient};
use velo_hub::protocol::{CREDENTIAL_HEADER, ErrorCode, service_instance};
use velo_hub::store::testing::FaultStore;
use velo_hub::velo::discovery::{ServiceDiscovery, ServiceEvent, ServiceRegistrationGuard};
use velo_hub::velo::{Handler, InstanceId};

fn client_for(hub: &Hub) -> HubClient {
    HubClient::builder()
        .host("127.0.0.1")
        .public_port(hub.public_addr().port())
        .control_port(hub.control_addr().port())
        .build()
        .unwrap()
}

fn none() -> BTreeMap<String, serde_json::Value> {
    BTreeMap::new()
}

async fn registered(hub: &Hub) -> (HubClient, InstanceId) {
    let client = client_for(hub);
    let peer = make_peer();
    client.register_peer(peer.clone(), none()).await.unwrap();
    (client, peer.instance_id())
}

async fn next(stream: &mut (impl futures::Stream<Item = ServiceEvent> + Unpin)) -> ServiceEvent {
    tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("no watch event within 5 s")
        .expect("the watch ended")
}

#[tokio::test]
async fn a_registered_instance_serves_and_is_listed() {
    let hub = start().await;
    let (client, id) = registered(&hub).await;
    let services = client.services();
    assert!(services.list_services().await.unwrap().is_empty());
    let _guard = client.register_service("router").await.unwrap();
    assert_eq!(services.list_services().await.unwrap(), ["router"]);
    assert_eq!(services.get_instances("router").await.unwrap(), [id]);
    assert!(services.get_instances("nobody").await.unwrap().is_empty());
}

/// Adding an instance needs that instance's credential: none is 401, and
/// one that a re-registration replaced is 401 too.
#[tokio::test]
async fn adding_an_instance_needs_its_credential() {
    let hub = start().await;
    let (client, id) = registered(&hub).await;
    let old = client.registration().unwrap().credential;
    let url = control(&hub, &service_instance("router", id));
    assert_eq!(http().put(&url).send().await.unwrap().status(), 401);
    let peer = velo_hub::velo::PeerInfo::new(id, make_peer().worker_address().clone());
    client.register_peer(peer, none()).await.unwrap();
    let status = http()
        .put(&url)
        .header(CREDENTIAL_HEADER, old.to_header_value())
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status, 401);
    assert!(hub_instances(&hub, "router").await.is_empty());
}

async fn hub_instances(hub: &Hub, name: &str) -> Vec<InstanceId> {
    client_for(hub)
        .services()
        .get_instances(name)
        .await
        .unwrap()
}

/// An instance's entries go with its registration: unregister, and expiry.
#[tokio::test]
async fn entries_die_with_their_owner() {
    let store = std::sync::Arc::new(FaultStore::default());
    let hub = builder().store(store.clone()).serve().await.unwrap();

    let (client, _) = registered(&hub).await;
    let _guard = client.register_service("a").await.unwrap();
    client.unregister().await.unwrap();
    assert!(hub_instances(&hub, "a").await.is_empty());

    let (client, id) = registered(&hub).await;
    let _guard = client.register_service("b").await.unwrap();
    store.expire(id);
    let hub_ref = &hub;
    let gone = async {
        for _ in 0..100 {
            if hub_instances(hub_ref, "b").await.is_empty() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        false
    }
    .await;
    assert!(gone, "an expired owner kept its service entry");
    assert!(
        client_for(&hub)
            .services()
            .list_services()
            .await
            .unwrap()
            .is_empty()
    );
}

/// A re-registration with the credential keeps the owner's services (the
/// peer still serves them); a fresh registration without one starts empty.
#[tokio::test]
async fn a_re_registration_keeps_services_and_a_fresh_one_does_not() {
    let hub = start().await;
    let client = client_for(&hub);
    let peer = make_peer();
    client.register_peer(peer.clone(), none()).await.unwrap();
    let guard = client.register_service("kept").await.unwrap();
    client.register_peer(peer.clone(), none()).await.unwrap();
    assert_eq!(hub_instances(&hub, "kept").await, [peer.instance_id()]);
    std::mem::forget(guard);

    // A fresh registration of the same instance, without the credential:
    // the old one is deleted around the client, then registered again.
    let credential = client.registration().unwrap().credential;
    assert_eq!(
        unregister_status(&hub, peer.instance_id(), Some(&credential)).await,
        204
    );
    assert!(hub_instances(&hub, "kept").await.is_empty());
    let fresh = client_for(&hub);
    fresh.register_peer(peer.clone(), none()).await.unwrap();
    assert!(hub_instances(&hub, "kept").await.is_empty());
}

/// A watch starts with every current instance, then reports each change
/// once, in order.
#[tokio::test]
async fn a_watch_reports_initial_then_changes() {
    let hub = start().await;
    let (first, first_id) = registered(&hub).await;
    let _first = first.register_service("svc").await.unwrap();
    let mut watch = client_for(&hub)
        .services()
        .watch_instances("svc")
        .await
        .unwrap();
    assert_eq!(
        next(&mut watch).await,
        ServiceEvent::Initial(vec![first_id])
    );

    let (second, second_id) = registered(&hub).await;
    let mut guard = second.register_service("svc").await.unwrap();
    assert_eq!(next(&mut watch).await, ServiceEvent::Added(second_id));
    guard.unregister().await.unwrap();
    assert_eq!(next(&mut watch).await, ServiceEvent::Removed(second_id));
    first.unregister().await.unwrap();
    assert_eq!(next(&mut watch).await, ServiceEvent::Removed(first_id));
}

/// The snapshot and the subscription are one step: while instances join
/// concurrently, the set a watcher builds from `initial` plus the deltas is
/// the hub's set, with no instance added twice and none missed. The race
/// is narrow, so this catches a split snapshot and subscription only some
/// of the time (2 runs in 3 with a 2 ms gap between them).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_watch_misses_nothing_while_instances_join() {
    let hub = start().await;
    let mut clients = Vec::new();
    for _ in 0..40 {
        clients.push(registered(&hub).await);
    }
    let joins: Vec<_> = clients
        .iter()
        .map(|(client, _)| {
            let client = client.clone();
            tokio::spawn(async move { client.register_service("busy").await.unwrap() })
        })
        .collect();
    let mut watch = client_for(&hub)
        .services()
        .watch_instances("busy")
        .await
        .unwrap();
    let mut seen: HashSet<InstanceId> = match next(&mut watch).await {
        ServiceEvent::Initial(ids) => ids.into_iter().collect(),
        other => panic!("first event {other:?}"),
    };
    let mut guards = Vec::new();
    for join in joins {
        guards.push(join.await.unwrap());
    }
    while seen.len() < clients.len() {
        match next(&mut watch).await {
            ServiceEvent::Added(id) => assert!(seen.insert(id), "{id} added twice"),
            other => panic!("unexpected {other:?}"),
        }
    }
    let expected: HashSet<InstanceId> = clients.iter().map(|(_, id)| *id).collect();
    assert_eq!(seen, expected);
    assert_eq!(hub_instances(&hub, "busy").await.len(), clients.len());
}

/// A watch ends when the hub stops, with `Disconnected`, and an open watch
/// does not hold the listener drain.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_watch_ends_at_shutdown_without_delaying_it() {
    let mut config = config();
    config.server.shutdown_timeout = Duration::from_secs(5);
    let hub = Hub::builder(config).serve().await.unwrap();
    let mut watch = client_for(&hub)
        .services()
        .watch_instances("svc")
        .await
        .unwrap();
    assert_eq!(next(&mut watch).await, ServiceEvent::Initial(vec![]));
    let started = std::time::Instant::now();
    hub.shutdown().await.unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "an open watch held shutdown for {:?}",
        started.elapsed()
    );
    assert_eq!(next(&mut watch).await, ServiceEvent::Disconnected);
    assert!(watch.next().await.is_none());
}

#[tokio::test]
async fn invalid_service_names_are_rejected() {
    let hub = start().await;
    for path in ["/v1/services/a%20b", "/v1/services/a%2Fb/watch"] {
        let response = http().get(public(&hub, path)).send().await.unwrap();
        assert_eq!(response.status(), 400, "{path}");
        assert_eq!(error_code(response).await, ErrorCode::BadRequest);
    }
    let (client, _) = registered(&hub).await;
    assert!(client.register_service("a b").await.is_err());
}

/// Dropping the guard removes the entry; once the client is gone, the
/// registration's end removes it instead.
#[tokio::test]
async fn dropping_the_guard_removes_the_entry() {
    let hub = start().await;
    let (client, _) = registered(&hub).await;
    let guard = client.register_service("dropped").await.unwrap();
    drop(guard);
    let hub_ref = &hub;
    let mut gone = false;
    for _ in 0..100 {
        if hub_instances(hub_ref, "dropped").await.is_empty() {
            gone = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(gone);
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Ping {
    value: u64,
}

/// Two velos: one serves `echo`; the other finds it by service name through
/// the hub, then reaches it by instance id through the hub's discovery.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_found_by_service_name_answers() {
    let hub = builder().velo(velo_builder()).serve().await.unwrap();
    let (a, b) = (client_for(&hub), client_for(&hub));
    let velo_a = a
        .velo_builder()
        .add_transport(tcp_transport())
        .build()
        .await
        .unwrap();
    let velo_b = b
        .velo_builder()
        .add_transport(tcp_transport())
        .build()
        .await
        .unwrap();
    velo_b
        .register_handler(
            Handler::typed_unary_async::<Ping, Ping, _, _>("echo", |ctx| async move {
                Ok(Ping {
                    value: ctx.input.value * 2,
                })
            })
            .build(),
        )
        .unwrap();
    a.register(&velo_a, none()).await.unwrap();
    b.register(&velo_b, none()).await.unwrap();
    let _serving = b.register_service("echo").await.unwrap();

    let found = a.services().get_instances("echo").await.unwrap();
    assert_eq!(found, [velo_b.instance_id()]);
    velo_a.discover_and_register_peer(found[0]).await.unwrap();
    let answer: Ping = velo_a
        .typed_unary::<Ping>("echo")
        .unwrap()
        .payload(&Ping { value: 21 })
        .unwrap()
        .instance(found[0])
        .send()
        .await
        .unwrap();
    assert_eq!(answer, Ping { value: 42 });
    a.unregister().await.unwrap();
    b.unregister().await.unwrap();
    hub.shutdown().await.unwrap();
}

/// A guard outlives its registration. The client holds one guard per name
/// and puts every name again after a registration, so after a fresh
/// registration (which the hub starts with no services) the entry is back
/// without a second `register_service`, and the one guard still removes it.
#[tokio::test]
async fn a_guard_outlives_a_fresh_registration_and_stays_the_only_one() {
    let hub = start().await;
    let client = client_for(&hub);
    let peer = make_peer();
    client
        .register_peer(peer.clone(), Default::default())
        .await
        .unwrap();
    let guard = client.register_service("r").await.unwrap();
    client.unregister().await.unwrap();
    assert!(hub_instances(&hub, "r").await.is_empty());
    client
        .register_peer(peer.clone(), Default::default())
        .await
        .unwrap();
    assert_eq!(hub_instances(&hub, "r").await, [peer.instance_id()]);
    assert!(client.register_service("r").await.is_err());
    drop(guard);
    let hub_ref = &hub;
    let mut gone = false;
    for _ in 0..100 {
        if hub_instances(hub_ref, "r").await.is_empty() {
            gone = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(gone, "the one guard did not remove its entry");
}

/// One guard per name: a second `register_service` of a name the client
/// serves is refused, as velo's own backends refuse a duplicate, so no
/// second guard can remove the first one's entry.
#[tokio::test]
async fn a_second_guard_for_one_name_is_refused() {
    let hub = start().await;
    let (client, id) = registered(&hub).await;
    let first = client.register_service("r").await.unwrap();
    let second = client.register_service("r").await;
    assert!(
        matches!(second, Err(ClientError::AlreadyServing(_))),
        "{second:?}"
    );
    assert_eq!(hub_instances(&hub, "r").await, [id]);
    drop(first);
}
