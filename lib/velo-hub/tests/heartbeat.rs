// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Ported from kvbm-hub `tests/heartbeat_integration.rs`: the hub probes
//! real velo peers. kvbm registered through its HubClient (whose drop
//! guard unregisters); here registration is a raw POST, so nothing
//! unregisters behind the test's back.

mod common;

use std::collections::HashSet;
use std::time::Duration;

use common::*;
use velo_hub::Hub;
use velo_hub::velo::InstanceId;

async fn start_hub_with_heartbeat(interval: Duration, max_failures: u32) -> Hub {
    let mut config = config();
    config.server.heartbeat_interval = interval;
    config.server.heartbeat_max_failures = max_failures;
    // Long enough that the reaper never preempts the heartbeat path.
    config.server.ttl = Duration::from_secs(60);
    config.server.prune_interval = Duration::from_secs(30);
    Hub::builder(config)
        .velo(velo_builder())
        .serve()
        .await
        .expect("start hub")
}

fn registered_ids(hub: &Hub) -> HashSet<InstanceId> {
    hub.store()
        .list()
        .iter()
        .map(|stored| stored.instance_id())
        .collect()
}

/// A probe that succeeds refreshes liveness; a responsive peer stays.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn responsive_peer_stays_registered() {
    init_tracing();
    let hub = start_hub_with_heartbeat(Duration::from_millis(200), 3).await;
    let peer = new_velo().await;
    install_heartbeat(&peer);
    wire_mutual(&hub, &peer).await;

    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(registered_ids(&hub).contains(&peer.instance_id()));
    hub.shutdown().await.unwrap();
}

/// A wedged peer is removed after `max_failures` consecutive timeouts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wedged_peer_pruned_after_max_failures() {
    init_tracing();
    let interval = Duration::from_secs(1);
    let max_failures: u32 = 2;
    let hub = start_hub_with_heartbeat(interval, max_failures).await;
    let peer = new_velo().await;
    install_wedged_heartbeat(&peer, Duration::from_secs(60));
    wire_mutual(&hub, &peer).await;
    assert!(registered_ids(&hub).contains(&peer.instance_id()));

    // Worst case: one interval before the first tick, then one timeout per
    // probe. Budget (N + 2) intervals plus scheduling slack.
    let budget = interval * (max_failures + 2) + Duration::from_secs(2);
    assert!(
        eventually(budget, || !registered_ids(&hub)
            .contains(&peer.instance_id()))
        .await,
        "wedged peer should have been pruned after {max_failures} timeouts"
    );
    hub.shutdown().await.unwrap();
}

/// A wedged peer in the same tick as a healthy one does not delay the
/// healthy one's refresh: probes run concurrently.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wedged_peer_does_not_block_healthy_peer() {
    let hub = start_hub_with_heartbeat(Duration::from_millis(200), 100).await;
    let healthy = new_velo().await;
    install_heartbeat(&healthy);
    wire_mutual(&hub, &healthy).await;
    let wedged = new_velo().await;
    install_wedged_heartbeat(&wedged, Duration::from_secs(60));
    wire_mutual(&hub, &wedged).await;

    tokio::time::sleep(Duration::from_secs(2)).await;
    let ids = registered_ids(&hub);
    assert!(
        ids.contains(&healthy.instance_id()),
        "healthy peer must stay registered"
    );
    assert!(
        ids.contains(&wedged.instance_id()),
        "max_failures=100 keeps the wedged one"
    );
    hub.shutdown().await.unwrap();
}

/// A hub without velo does not probe; liveness is the store's TTL.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn discovery_only_hub_does_not_probe() {
    let mut config = config();
    config.server.heartbeat_interval = Duration::from_millis(200);
    config.server.heartbeat_max_failures = 2;
    let hub = Hub::builder(config).serve().await.unwrap();
    assert!(hub.velo().is_none(), "a discovery-only hub exposes no velo");
    let peer = make_peer();
    register_plain(&hub, &peer, None).await;
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert!(
        registered_ids(&hub).contains(&peer.instance_id()),
        "nothing probed it away"
    );
    hub.shutdown().await.unwrap();
}

/// The hub never probes its own pinned entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hub_does_not_probe_self() {
    let hub = start_hub_with_heartbeat(Duration::from_millis(50), 2).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let ids = registered_ids(&hub);
    assert_eq!(ids.len(), 1, "only the hub itself is registered");
    assert!(ids.contains(&hub.velo().unwrap().instance_id()));
    hub.shutdown().await.unwrap();
}

/// A successful probe refreshes liveness. With a TTL far shorter than the
/// test, a responsive peer stays only because each probe touches its
/// entry; a peer that never answers, with failures set too high to evict
/// it, is expired by the TTL in the same time, which shows the TTL bites.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_successful_probe_refreshes_liveness() {
    let mut config = config();
    config.server.heartbeat_interval = Duration::from_millis(150);
    config.server.heartbeat_max_failures = 1000;
    config.server.ttl = Duration::from_millis(600);
    config.server.prune_interval = Duration::from_millis(50);
    let hub = Hub::builder(config)
        .velo(velo_builder())
        .serve()
        .await
        .unwrap();
    let responsive = new_velo().await;
    install_heartbeat(&responsive);
    wire_mutual(&hub, &responsive).await;
    let silent = new_velo().await;
    install_wedged_heartbeat(&silent, Duration::from_secs(60));
    wire_mutual(&hub, &silent).await;

    tokio::time::sleep(Duration::from_millis(2000)).await;
    let ids = registered_ids(&hub);
    assert!(
        ids.contains(&responsive.instance_id()),
        "probes must refresh the TTL"
    );
    assert!(
        !ids.contains(&silent.instance_id()),
        "the TTL is short enough to evict an untouched entry"
    );
    hub.shutdown().await.unwrap();
}

/// A store touch that never answers cannot stop the heartbeat or hang
/// shutdown: the heartbeat's store calls run under `step_timeout_ms`, and
/// the other peers keep being probed meanwhile.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hung_store_touch_does_not_stop_the_heartbeat_or_hang_shutdown() {
    let store = std::sync::Arc::new(velo_hub::store::testing::FaultStore::new(
        velo_hub::store::InMemoryStore::builder()
            .ttl(Duration::from_millis(800))
            .prune_interval(Duration::from_millis(50))
            .build(),
    ));
    let mut config = config();
    config.server.heartbeat_interval = Duration::from_millis(100);
    config.server.step_timeout = Duration::from_millis(200);
    config.server.shutdown_timeout = Duration::from_millis(500);
    let hub = Hub::builder(config)
        .store(store.clone())
        .velo(velo_builder())
        .serve()
        .await
        .unwrap();
    let peer = new_velo().await;
    install_heartbeat(&peer);
    wire_mutual(&hub, &peer).await;
    let _gate = store.gate_next_touch();
    // Two TTLs: the peer stays only if later probes still touch it.
    tokio::time::sleep(Duration::from_millis(1600)).await;
    assert!(
        registered_ids(&hub).contains(&peer.instance_id()),
        "the heartbeat stopped behind one hung touch"
    );
    let started = std::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(5), hub.shutdown())
        .await
        .expect("shutdown hung behind a hung store touch")
        .unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "shutdown took {:?}",
        started.elapsed()
    );
}

/// The heartbeat's conditional remove, after a peer failed its probes,
/// is bounded too: a hung one does not stop the probes of other peers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hung_store_remove_does_not_stop_the_heartbeat() {
    let store = std::sync::Arc::new(velo_hub::store::testing::FaultStore::new(
        velo_hub::store::InMemoryStore::builder()
            .ttl(Duration::from_millis(800))
            .prune_interval(Duration::from_millis(50))
            .build(),
    ));
    let mut config = config();
    config.server.heartbeat_interval = Duration::from_millis(100);
    config.server.heartbeat_max_failures = 1;
    config.server.step_timeout = Duration::from_millis(200);
    let hub = Hub::builder(config)
        .store(store.clone())
        .velo(velo_builder())
        .serve()
        .await
        .unwrap();
    let healthy = new_velo().await;
    install_heartbeat(&healthy);
    wire_mutual(&hub, &healthy).await;
    let _gate = store.gate_next_remove();
    let wedged = new_velo().await;
    install_wedged_heartbeat(&wedged, Duration::from_secs(60));
    wire_mutual(&hub, &wedged).await;
    tokio::time::sleep(Duration::from_millis(1600)).await;
    assert!(
        registered_ids(&hub).contains(&healthy.instance_id()),
        "the heartbeat stopped behind one hung remove"
    );
    tokio::time::timeout(Duration::from_secs(5), hub.shutdown())
        .await
        .expect("shutdown hung")
        .unwrap();
}
