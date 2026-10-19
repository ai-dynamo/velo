// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Ported from kvbm-hub `src/registry/tests.rs`. `register(peer)` there was
//! unconditional; `put` here is compare-and-swap, so each test passes the
//! incarnation it expects (`None` for a first write).

use std::sync::Mutex;

use velo::WorkerAddress;

use super::*;

fn make_peer() -> PeerInfo {
    PeerInfo::new(
        InstanceId::new_v4(),
        WorkerAddress::from_encoded(b"test".to_vec()),
    )
}

fn peer_with_address(id: InstanceId, address: &[u8]) -> PeerInfo {
    PeerInfo::new(id, WorkerAddress::from_encoded(address.to_vec()))
}

async fn put(store: &InMemoryStore, peer: &PeerInfo) -> Incarnation {
    let expected = store.current_incarnation(peer.instance_id());
    store
        .put(RegistrationRecord::bare(peer.clone()), expected)
        .await
        .unwrap()
}

fn recording_hook() -> (RemovalHook, Arc<Mutex<Vec<Removal>>>) {
    let removals = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&removals);
    let hook: RemovalHook = Arc::new(move |removal| captured.lock().unwrap().push(removal));
    (hook, removals)
}

#[tokio::test]
async fn register_and_list() {
    let store = InMemoryStore::default();
    let peer = make_peer();
    put(&store, &peer).await;
    assert!(store.contains(peer.instance_id()));
    assert_eq!(store.list().len(), 1);
}

#[tokio::test]
async fn reregister_same_instance_replaces_single_entry() {
    let store = InMemoryStore::default();
    let peer = make_peer();
    put(&store, &peer).await;
    put(&store, &peer).await;
    assert_eq!(store.list().len(), 1);
}

#[tokio::test]
async fn reregister_mints_incarnation_and_rejects_stale_mutations() {
    let store = InMemoryStore::default();
    let peer = make_peer();
    let id = peer.instance_id();

    let first = put(&store, &peer).await;
    let second = put(&store, &peer).await;

    assert_ne!(first, second);
    assert_eq!(store.list()[0].incarnation, second);
    assert_eq!(store.current_incarnation(id), Some(second));
    assert!(!store.is_current(id, first));
    assert!(store.is_current(id, second));
    assert!(matches!(
        store.touch(id, first).await,
        Err(StoreError::Stale { .. })
    ));
    assert!(matches!(
        store.remove(id, first).await,
        Err(StoreError::Stale { .. })
    ));
    assert!(store.contains(id));
    store.touch(id, second).await.unwrap();
    store.remove(id, second).await.unwrap();
    assert!(!store.contains(id));
    assert_eq!(store.current_incarnation(id), None);
}

#[tokio::test]
async fn reregister_purges_every_worker_mapping_owned_by_instance() {
    let store = InMemoryStore::default();
    let id = InstanceId::new_v4();
    let peer_a = peer_with_address(id, b"address-a");
    let peer_b = peer_with_address(id, b"address-b");
    let stale_a = WorkerId::from_u64(peer_a.worker_id().as_u64().wrapping_add(1));
    let stale_b = WorkerId::from_u64(peer_a.worker_id().as_u64().wrapping_add(2));

    put(&store, &peer_a).await;
    store.insert_worker_claim(stale_a, id);
    put(&store, &peer_b).await;
    assert!(!store.worker_claims_of(id).contains(&stale_a));

    // A replacement must not keep a claim an earlier incarnation left.
    store.insert_worker_claim(stale_b, id);
    put(&store, &peer_a).await;

    assert_eq!(store.worker_claims_of(id), vec![peer_a.worker_id()]);
    assert_eq!(store.get(id).unwrap().record.peer, peer_a);
}

#[tokio::test]
async fn removal_callback_identifies_removed_incarnation() {
    let store = InMemoryStore::default();
    let (hook, removals) = recording_hook();
    store.install_removal_hook(hook).unwrap();
    let peer = make_peer();
    let id = peer.instance_id();

    let stale = put(&store, &peer).await;
    let current = put(&store, &peer).await;
    assert!(matches!(
        store.remove(id, stale).await,
        Err(StoreError::Stale { .. })
    ));
    assert!(removals.lock().unwrap().is_empty());

    store.remove(id, current).await.unwrap();
    assert_eq!(
        removals.lock().unwrap().as_slice(),
        &[Removal {
            id,
            incarnation: current
        }]
    );
}

#[tokio::test]
async fn hook_installation_returns_existing_registrations() {
    let store = InMemoryStore::default();
    let peer = make_peer();
    let incarnation = put(&store, &peer).await;
    let (hook, removals) = recording_hook();

    let snapshot = store.install_removal_hook(hook).unwrap();

    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].peer(), &peer);
    assert_eq!(snapshot[0].incarnation, incarnation);
    store.remove(peer.instance_id(), incarnation).await.unwrap();
    assert_eq!(
        removals.lock().unwrap().as_slice(),
        &[Removal {
            id: peer.instance_id(),
            incarnation
        }]
    );
}

#[test]
fn removal_hook_cannot_be_replaced() {
    let store = InMemoryStore::default();
    store.install_removal_hook(Arc::new(|_| {})).unwrap();
    assert!(matches!(
        store.install_removal_hook(Arc::new(|_| {})),
        Err(StoreError::HookInstalled)
    ));
}

/// Install and remove race 64 times. Whichever wins, an entry is either in
/// the snapshot and then reported removed, or in neither: never in the
/// snapshot without a report, and never reported without being in it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hook_snapshot_and_removal_share_a_linearization_point() {
    for _ in 0..64 {
        let store = Arc::new(InMemoryStore::default());
        let peer = make_peer();
        let incarnation = put(&store, &peer).await;
        let (hook, removals) = recording_hook();
        let barrier = Arc::new(tokio::sync::Barrier::new(3));

        let install = tokio::spawn({
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            async move {
                barrier.wait().await;
                store.install_removal_hook(hook).unwrap()
            }
        });
        let remove = tokio::spawn({
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            async move {
                barrier.wait().await;
                store.remove(peer.instance_id(), incarnation).await.unwrap();
            }
        });

        barrier.wait().await;
        let snapshot = install.await.unwrap();
        remove.await.unwrap();
        assert_eq!(snapshot.len(), removals.lock().unwrap().len());
    }
}

#[tokio::test]
async fn registrations_snapshot_pairs_peer_with_incarnation() {
    let store = InMemoryStore::default();
    let peer = make_peer();
    let incarnation = put(&store, &peer).await;
    let listed = store.list();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].peer(), &peer);
    assert_eq!(listed[0].incarnation, incarnation);
}

#[tokio::test]
async fn conflict_on_different_instance_same_worker() {
    let store = InMemoryStore::default();
    let a = make_peer();
    put(&store, &a).await;
    // WorkerId derives from InstanceId, so two real peers cannot share one.
    // Plant a claim by another instance to reach the conflict branch.
    store.insert_worker_claim(a.worker_id(), InstanceId::new_v4());
    let error = store
        .put(
            RegistrationRecord::bare(a.clone()),
            store.current_incarnation(a.instance_id()),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, StoreError::Conflict { .. }));
}

#[tokio::test]
async fn unregister_removes_from_all_maps() {
    let store = InMemoryStore::default();
    let peer = make_peer();
    let incarnation = put(&store, &peer).await;
    store.remove(peer.instance_id(), incarnation).await.unwrap();
    assert!(!store.contains(peer.instance_id()));
    assert!(store.discover_by_worker_id(peer.worker_id()).await.is_err());
}

#[tokio::test]
async fn unregister_not_found() {
    let store = InMemoryStore::default();
    let error = store
        .remove(InstanceId::new_v4(), Incarnation::from_u64(1))
        .await
        .unwrap_err();
    assert!(matches!(error, StoreError::NotFound(_)));
}

#[tokio::test]
async fn touch_not_found() {
    let store = InMemoryStore::default();
    let error = store
        .touch(InstanceId::new_v4(), Incarnation::from_u64(1))
        .await
        .unwrap_err();
    assert!(matches!(error, StoreError::NotFound(_)));
}

#[tokio::test(start_paused = true)]
async fn prune_stale_removes_old_entries() {
    let store = InMemoryStore::builder()
        .ttl(Duration::from_millis(100))
        .prune_interval(Duration::from_millis(50))
        .build();
    let peer = make_peer();
    put(&store, &peer).await;
    tokio::time::advance(Duration::from_millis(200)).await;
    store.prune_stale();
    assert!(!store.contains(peer.instance_id()));
}

#[tokio::test(start_paused = true)]
async fn prune_reports_the_expired_incarnation() {
    let store = InMemoryStore::builder()
        .ttl(Duration::from_millis(100))
        .build();
    let (hook, removals) = recording_hook();
    store.install_removal_hook(hook).unwrap();
    let peer = make_peer();
    let incarnation = put(&store, &peer).await;

    tokio::time::advance(Duration::from_millis(200)).await;
    store.prune_stale();

    assert_eq!(
        removals.lock().unwrap().as_slice(),
        &[Removal {
            id: peer.instance_id(),
            incarnation
        }]
    );
}

#[tokio::test(start_paused = true)]
async fn touch_refreshes_ttl() {
    let store = InMemoryStore::builder()
        .ttl(Duration::from_millis(100))
        .build();
    let peer = make_peer();
    let incarnation = put(&store, &peer).await;
    tokio::time::advance(Duration::from_millis(80)).await;
    store.touch(peer.instance_id(), incarnation).await.unwrap();
    tokio::time::advance(Duration::from_millis(80)).await;
    store.prune_stale();
    assert!(store.contains(peer.instance_id()));
}

/// kvbm called this `protect`; here pinning is a field of the record, so
/// any backend honors it through the trait.
#[tokio::test(start_paused = true)]
async fn pinned_entry_is_exempt_from_prune() {
    let store = InMemoryStore::builder()
        .ttl(Duration::from_millis(100))
        .build();
    let peer = make_peer();
    let mut record = RegistrationRecord::bare(peer.clone());
    record.pinned = true;
    store.put(record, None).await.unwrap();
    tokio::time::advance(Duration::from_millis(500)).await;
    store.prune_stale();
    assert!(store.contains(peer.instance_id()));
}

#[tokio::test]
async fn in_memory_store_passes_the_conformance_suite() {
    crate::store::conformance::run_all(|| Arc::new(InMemoryStore::default())).await;
}

/// Cost of a peer lookup on the request path. Not a gate: run it with
/// `cargo test --release -p velo-hub --features server lookup_cost -- --ignored --nocapture`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "measurement, not a check"]
async fn lookup_cost() {
    const PEERS: usize = 10_000;
    const CALLS: usize = 2_000_000;
    let store = Arc::new(InMemoryStore::default());
    let mut ids = Vec::new();
    for _ in 0..PEERS {
        let peer = make_peer();
        let mut record = RegistrationRecord::bare(peer.clone());
        // A payload the size of a small manifest, which `get` would copy.
        record.extensions.insert(
            "probe".to_string(),
            serde_json::json!({ "blob": "x".repeat(2048) }),
        );
        store.put(record, None).await.unwrap();
        ids.push(peer.instance_id());
    }
    for threads in [1usize, 8] {
        let started = std::time::Instant::now();
        std::thread::scope(|scope| {
            for thread in 0..threads {
                let (store, ids) = (&store, &ids);
                scope.spawn(move || {
                    for call in 0..CALLS / threads {
                        let id = ids[(call * 7919 + thread) % ids.len()];
                        std::hint::black_box(
                            futures::executor::block_on(store.discover_by_instance_id(id)).unwrap(),
                        );
                    }
                });
            }
        });
        let elapsed = started.elapsed();
        println!(
            "discover_by_instance_id: {threads} thread(s), {PEERS} peers: {:.1} ns/call per thread, {:.1} M calls/s total",
            elapsed.as_nanos() as f64 * threads as f64 / CALLS as f64,
            CALLS as f64 / elapsed.as_secs_f64() / 1e6
        );
        let started = std::time::Instant::now();
        for call in 0..CALLS / 10 {
            std::hint::black_box(store.get(ids[call % ids.len()]).unwrap());
        }
        println!(
            "get (copies the 2 KiB payload): 1 thread: {:.1} ns/call",
            started.elapsed().as_nanos() as f64 / (CALLS / 10) as f64
        );
        let started = std::time::Instant::now();
        std::hint::black_box(store.list());
        let list = started.elapsed();
        let started = std::time::Instant::now();
        std::hint::black_box(store.peers());
        println!(
            "list(): {list:?}, peers(): {:?} for {PEERS} entries",
            started.elapsed()
        );
    }
}
