// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! NATS peer discovery integration tests.
//!
//! Requires a running NATS server at nats://127.0.0.1:4222.
//! Locally: `docker run -d -p 4222:4222 nats:latest`

#![cfg(feature = "nats")]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use velo_common::{InstanceId, PeerInfo, WorkerAddress};
use velo_discovery::nats::NatsPeerDiscoveryBuilder;
use velo_discovery::{PeerDiscovery, PeerRegistrationGuard};

/// Build a test PeerInfo with a NATS inbound subject as the address.
fn make_test_peer_info() -> PeerInfo {
    let instance_id = InstanceId::new_v4();
    let subject = format!("velo.test.{}.inbound", instance_id);
    let map: HashMap<String, Vec<u8>> = [("nats".to_string(), subject.into_bytes())]
        .into_iter()
        .collect();
    let encoded = rmp_serde::to_vec(&map).unwrap();
    let address = WorkerAddress::from_encoded(encoded);
    PeerInfo::new(instance_id, address)
}

/// TEST-03: Discover a registered peer by instance_id.
#[tokio::test]
async fn test_discover_by_instance_id() {
    let client = Arc::new(async_nats::connect("nats://127.0.0.1:4222").await.unwrap());
    let cluster_id = format!("test-{}", InstanceId::new_v4());
    let discovery = NatsPeerDiscoveryBuilder::new(client.clone(), &cluster_id).build();

    let peer_info = make_test_peer_info();
    let _guard = discovery.register(&peer_info).await.unwrap();

    // Give the responder task a moment to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    let found = discovery
        .discover_by_instance_id(peer_info.instance_id())
        .await
        .unwrap();

    assert_eq!(
        found.instance_id(),
        peer_info.instance_id(),
        "Discovered instance_id must match registered peer"
    );
    assert_eq!(
        found.worker_address(),
        peer_info.worker_address(),
        "Discovered worker_address must match registered peer"
    );
}

/// TEST-03: Discover a registered peer by worker_id.
#[tokio::test]
async fn test_discover_by_worker_id() {
    let client = Arc::new(async_nats::connect("nats://127.0.0.1:4222").await.unwrap());
    let cluster_id = format!("test-{}", InstanceId::new_v4());
    let discovery = NatsPeerDiscoveryBuilder::new(client.clone(), &cluster_id).build();

    let peer_info = make_test_peer_info();
    let _guard = discovery.register(&peer_info).await.unwrap();

    // Give the responder task a moment to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    let found = discovery
        .discover_by_worker_id(peer_info.worker_id())
        .await
        .unwrap();

    assert_eq!(
        found.instance_id(),
        peer_info.instance_id(),
        "Discovered instance_id must match registered peer"
    );
    assert_eq!(
        found.worker_id(),
        peer_info.worker_id(),
        "Discovered worker_id must match registered peer"
    );
}

/// Verify that discovering an unregistered peer returns an error.
#[tokio::test]
async fn test_discover_not_found() {
    let client = Arc::new(async_nats::connect("nats://127.0.0.1:4222").await.unwrap());
    let cluster_id = format!("test-{}", InstanceId::new_v4());
    let discovery = NatsPeerDiscoveryBuilder::new(client.clone(), &cluster_id).build();

    // Do NOT register any peer — discovery should fail with a "not found" error
    let random_id = InstanceId::new_v4();
    let result = discovery.discover_by_instance_id(random_id).await;

    assert!(
        result.is_err(),
        "Discovering unregistered peer must return Err"
    );
    let err_msg = result.unwrap_err().to_string();
    let has_expected_msg = err_msg.to_lowercase().contains("not found")
        || err_msg.to_lowercase().contains("no responder")
        || err_msg.to_lowercase().contains("timeout");
    assert!(
        has_expected_msg,
        "Error message must indicate peer not found, got: {}",
        err_msg
    );
}

/// Verify that calling unregister() stops discovery responses.
#[tokio::test]
async fn test_guard_unregister_stops_discovery() {
    let client = Arc::new(async_nats::connect("nats://127.0.0.1:4222").await.unwrap());
    let cluster_id = format!("test-{}", InstanceId::new_v4());
    let discovery = NatsPeerDiscoveryBuilder::new(client.clone(), &cluster_id).build();

    let peer_info = make_test_peer_info();
    let mut guard = discovery.register(&peer_info).await.unwrap();

    // Give the responder task a moment to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify discovery works before unregistering
    let found = discovery
        .discover_by_instance_id(peer_info.instance_id())
        .await;
    assert!(
        found.is_ok(),
        "Discovery must succeed before unregister, got: {:?}",
        found
    );

    // Unregister the peer
    guard.unregister().await.unwrap();

    // Wait for the NATS unsubscribe to propagate
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Discovery should now fail because the responder has unsubscribed
    let result = discovery
        .discover_by_instance_id(peer_info.instance_id())
        .await;
    assert!(
        result.is_err(),
        "Discovery must fail after unregister, but succeeded with: {:?}",
        result
    );
}
