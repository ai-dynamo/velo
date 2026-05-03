// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Etcd service discovery integration tests.
//!
//! Requires a running etcd server at http://127.0.0.1:2379.
//! Locally: `docker run -d -p 2379:2379 -e ALLOW_NONE_AUTHENTICATION=yes bitnami/etcd:latest`

#![cfg(feature = "etcd")]

use std::time::Duration;

use futures::StreamExt;
use velo::discovery::etcd::EtcdServiceDiscoveryBuilder;
use velo::discovery::{ServiceDiscovery, ServiceEvent, ServiceRegistrationGuard};
use velo_ext::InstanceId;

async fn make_discovery() -> velo::discovery::etcd::EtcdServiceDiscovery {
    let client = etcd_client::Client::connect(["http://127.0.0.1:2379"], None)
        .await
        .expect("Failed to connect to etcd — is it running on 127.0.0.1:2379?");
    // Use a unique cluster_id per test to avoid cross-test interference
    let cluster_id = format!("test-{}", InstanceId::new_v4());
    EtcdServiceDiscoveryBuilder::new(client, cluster_id)
        .lease_ttl(10)
        .build()
}

#[tokio::test]
async fn test_register_and_get_instances() {
    let discovery = make_discovery().await;
    let id1 = InstanceId::new_v4();
    let id2 = InstanceId::new_v4();

    let _g1 = discovery.register_service("my-service", id1).await.unwrap();
    let _g2 = discovery.register_service("my-service", id2).await.unwrap();

    let mut instances = discovery.get_instances("my-service").await.unwrap();
    instances.sort_by_key(|id| id.as_u128());
    assert_eq!(instances.len(), 2);
    assert!(instances.contains(&id1));
    assert!(instances.contains(&id2));
}

#[tokio::test]
async fn test_list_services() {
    let discovery = make_discovery().await;
    let _g1 = discovery
        .register_service("svc-alpha", InstanceId::new_v4())
        .await
        .unwrap();
    let _g2 = discovery
        .register_service("svc-beta", InstanceId::new_v4())
        .await
        .unwrap();

    let mut services = discovery.list_services().await.unwrap();
    services.sort();
    assert_eq!(services, vec!["svc-alpha", "svc-beta"]);
}

#[tokio::test]
async fn test_unregister_removes_instance() {
    let discovery = make_discovery().await;
    let id = InstanceId::new_v4();

    let mut guard = discovery.register_service("svc", id).await.unwrap();
    assert_eq!(discovery.get_instances("svc").await.unwrap().len(), 1);

    guard.unregister().await.unwrap();

    let instances = discovery.get_instances("svc").await.unwrap();
    assert!(
        instances.is_empty(),
        "Instance should be removed after unregister, got: {instances:?}"
    );
}

#[tokio::test]
async fn test_empty_service_returns_empty() {
    let discovery = make_discovery().await;
    let instances = discovery.get_instances("nonexistent").await.unwrap();
    assert!(instances.is_empty());
}

#[tokio::test]
async fn test_watch_initial_and_added() {
    let discovery = make_discovery().await;
    let id = InstanceId::new_v4();
    let _g = discovery.register_service("watch-svc", id).await.unwrap();

    let mut stream = discovery.watch_instances("watch-svc").await.unwrap();

    // Should get Initial with the registered instance
    let event = stream.next().await.unwrap();
    assert_eq!(event, ServiceEvent::Initial(vec![id]));

    // Register another instance → should see Added
    let id2 = InstanceId::new_v4();
    let _g2 = discovery.register_service("watch-svc", id2).await.unwrap();

    let event = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("Timed out waiting for Added event")
        .unwrap();
    assert_eq!(event, ServiceEvent::Added(id2));
}

#[tokio::test]
async fn test_watch_removed_on_unregister() {
    let discovery = make_discovery().await;
    let id = InstanceId::new_v4();
    let mut guard = discovery.register_service("watch-rm", id).await.unwrap();

    let mut stream = discovery.watch_instances("watch-rm").await.unwrap();

    // Consume Initial
    let event = stream.next().await.unwrap();
    assert_eq!(event, ServiceEvent::Initial(vec![id]));

    // Unregister → should see Removed
    guard.unregister().await.unwrap();

    let event = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("Timed out waiting for Removed event")
        .unwrap();
    assert_eq!(event, ServiceEvent::Removed(id));
}

#[tokio::test]
async fn test_cluster_isolation() {
    let client = etcd_client::Client::connect(["http://127.0.0.1:2379"], None)
        .await
        .unwrap();

    let discovery_a = EtcdServiceDiscoveryBuilder::new(client.clone(), "cluster-a").build();
    let discovery_b = EtcdServiceDiscoveryBuilder::new(client, "cluster-b").build();

    let id = InstanceId::new_v4();
    let _g = discovery_a
        .register_service("shared-svc", id)
        .await
        .unwrap();

    // Cluster B should not see cluster A's registration
    let instances = discovery_b.get_instances("shared-svc").await.unwrap();
    assert!(
        instances.is_empty(),
        "Cluster B should not see cluster A's instances, got: {instances:?}"
    );

    // Cluster A should see it
    let instances = discovery_a.get_instances("shared-svc").await.unwrap();
    assert_eq!(instances, vec![id]);
}
