// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The etcd store against a real etcd at `ETCD_URL` (default
//! `http://127.0.0.1:2379`, as velo's `discovery_etcd` tests). Every test
//! uses its own random prefix, so tests do not see each other's keys.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use velo_hub::store::etcd::etcd_client::Client;
use velo_hub::store::etcd::{ConnectError, EtcdStore, EtcdStoreBuilder, WatchHold};
use velo_hub::store::{
    Continuity, HubStore, OpId, RegistrationRecord, Removal, RemovalHook, StoreError, conformance,
};
use velo_hub::velo::{InstanceId, PeerInfo, WorkerAddress};

/// Tests that hold the watch or compact etcd run one at a time: a
/// compaction cancels any watch that is behind it, in any test.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn etcd_url() -> String {
    std::env::var("ETCD_URL").unwrap_or_else(|_| "http://127.0.0.1:2379".into())
}

fn prefix() -> String {
    format!("/velo-hub-test/{}", uuid::Uuid::new_v4())
}

fn builder(prefix: &str) -> EtcdStoreBuilder {
    EtcdStore::builder([etcd_url()]).prefix(prefix)
}

async fn connect(prefix: &str) -> EtcdStore {
    builder(prefix).connect().await.expect("connect to etcd")
}

async fn raw() -> Client {
    Client::connect([etcd_url()], None)
        .await
        .expect("raw etcd client")
}

fn instance_key(prefix: &str, id: InstanceId) -> String {
    format!("{prefix}/i/{id}")
}

async fn raw_exists(client: &mut Client, key: &str) -> bool {
    !client.get(key, None).await.unwrap().kvs().is_empty()
}

fn peer() -> PeerInfo {
    PeerInfo::new(
        InstanceId::new_v4(),
        WorkerAddress::from_encoded(b"etcd-test".to_vec()),
    )
}

fn recording_hook() -> (RemovalHook, Arc<Mutex<Vec<Removal>>>) {
    let removals = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&removals);
    let hook: RemovalHook = Arc::new(move |removal| captured.lock().unwrap().push(removal));
    (hook, removals)
}

/// Poll `condition` every 20 ms until it holds or `limit` passes.
async fn eventually(limit: Duration, mut condition: impl FnMut() -> bool) -> bool {
    let started = Instant::now();
    while started.elapsed() < limit {
        if condition() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    condition()
}

async fn wait_for_continuity(store: &EtcdStore, want: Continuity, limit: Duration) -> bool {
    let receiver = store.continuity();
    eventually(limit, || *receiver.borrow() == want).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn etcd_store_passes_the_conformance_suite() {
    conformance::run_all(|| async { Arc::new(connect(&prefix()).await) }).await;
}

/// The store's reaper deletes an entry nobody touched for the registrant
/// TTL, with a conditional delete, and the watch reports it once. There is
/// no lease per registrant (the custody lease outlives every entry), so
/// this is the only way an untouched entry leaves.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reaper_expiry_fires_the_hook_exactly_once() {
    let prefix = prefix();
    let store = builder(&prefix)
        .registrant_ttl(Duration::from_secs(1))
        .prune_interval(Duration::from_millis(200))
        .connect()
        .await
        .unwrap();
    let (hook, removals) = recording_hook();
    store.install_removal_hook(hook).unwrap();
    let peer = peer();
    let id = peer.instance_id();
    let incarnation = store
        .put(RegistrationRecord::bare(peer.clone()), None)
        .await
        .unwrap();

    assert!(
        eventually(Duration::from_secs(10), || !removals
            .lock()
            .unwrap()
            .is_empty())
        .await,
        "the reaper must expire an untouched entry"
    );
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        removals.lock().unwrap().as_slice(),
        &[Removal { id, incarnation }],
        "an expiry is reported exactly once"
    );
    assert!(!store.contains(id));
    assert!(!raw_exists(&mut raw().await, &instance_key(&prefix, id)).await);
}

/// A touched entry outlives the registrant TTL, and a pinned one is never
/// reaped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn touch_keeps_an_entry_and_the_reaper_skips_pinned_entries() {
    let store = builder(&prefix())
        .registrant_ttl(Duration::from_secs(1))
        .prune_interval(Duration::from_millis(200))
        .connect()
        .await
        .unwrap();
    let touched = peer();
    let touched_inc = store
        .put(RegistrationRecord::bare(touched.clone()), None)
        .await
        .unwrap();
    let mut pinned = RegistrationRecord::bare(peer());
    pinned.pinned = true;
    let pinned_id = pinned.peer.instance_id();
    store.put(pinned, None).await.unwrap();
    let started = Instant::now();
    while started.elapsed() < Duration::from_millis(2500) {
        store
            .touch(touched.instance_id(), touched_inc)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        store.contains(touched.instance_id()),
        "a touched entry stays"
    );
    assert!(store.contains(pinned_id), "a pinned entry is never reaped");
}

/// `resolve` settles "not applied" with a tombstone: a put with the
/// resolved op fails its tombstone compare, whenever it arrives.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resolve_tombstone_stops_a_late_put() {
    let prefix = prefix();
    let store = connect(&prefix).await;
    let mut client = raw().await;

    // The put reaches etcd after resolve answered: held behind a gate.
    let peer = peer();
    let id = peer.instance_id();
    let record = RegistrationRecord::bare(peer.clone());
    let op = record.op;
    let gate = store.gate_next_put();
    let store = Arc::new(store);
    let late = tokio::spawn({
        let store = Arc::clone(&store);
        async move { store.put(record, None).await }
    });
    gate.entered().await;
    assert_eq!(store.resolve(id, op).await.unwrap(), None);
    gate.release();
    let late = late.await.unwrap();
    assert!(
        late.is_err(),
        "a put of a resolved op must not apply: {late:?}"
    );
    assert!(!store.contains(id));
    assert!(!raw_exists(&mut client, &instance_key(&prefix, id)).await);

    // The same, with no gate: a resolved op never writes.
    let other = RegistrationRecord::bare(self::peer());
    let other_id = other.peer.instance_id();
    assert_eq!(store.resolve(other_id, other.op).await.unwrap(), None);
    assert!(store.put(other, None).await.is_err());
    assert!(!raw_exists(&mut client, &instance_key(&prefix, other_id)).await);
}

/// A txn whose compare fails still writes a barrier key in the prefix, so
/// the call's barrier completes on a prefix with no other traffic instead
/// of waiting out its deadline (review C1).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_compare_returns_promptly_on_a_quiet_prefix() {
    let store = builder(&prefix())
        .request_deadline(Duration::from_secs(10))
        .connect()
        .await
        .unwrap();
    let peer = peer();
    let id = peer.instance_id();
    let first = store
        .put(RegistrationRecord::bare(peer.clone()), None)
        .await
        .unwrap();
    store
        .put(RegistrationRecord::bare(peer.clone()), Some(first))
        .await
        .unwrap();

    let started = Instant::now();
    assert!(matches!(
        store.remove(id, first).await,
        Err(StoreError::Stale { .. })
    ));
    assert!(matches!(
        store.remove(InstanceId::new_v4(), first).await,
        Err(StoreError::NotFound(_))
    ));
    assert!(matches!(
        store.put(RegistrationRecord::bare(peer), None).await,
        Err(StoreError::Stale { .. })
    ));
    assert_eq!(store.resolve(id, OpId::new()).await.unwrap(), None);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "failed compares must not wait for the deadline: {:?}",
        started.elapsed()
    );
}

/// The "races" half of the barrier rule: an entry deleted behind the
/// store's back (as an operator or a lease would) is reported to the hook
/// before a remove that finds it gone returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_raced_removal_is_reported_before_the_call_returns() {
    let prefix = prefix();
    let store = connect(&prefix).await;
    let (hook, removals) = recording_hook();
    store.install_removal_hook(hook).unwrap();
    let peer = peer();
    let id = peer.instance_id();
    let incarnation = store
        .put(RegistrationRecord::bare(peer), None)
        .await
        .unwrap();
    raw()
        .await
        .delete(instance_key(&prefix, id), None)
        .await
        .unwrap();
    let result = store.remove(id, incarnation).await;
    assert!(
        result.as_ref().is_err_and(StoreError::is_gone),
        "{result:?}"
    );
    assert_eq!(
        removals.lock().unwrap().as_slice(),
        &[Removal { id, incarnation }]
    );
}

/// The hub's last call at shutdown is the remove of its own entry. A call
/// whose txn applied while the store closes returns its outcome: the store
/// says the hook will not run rather than turning a done delete into an
/// error (review C2). A call after close fails without reaching etcd.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_while_a_call_waits_on_the_watch_returns_its_outcome() {
    let _serial = SERIAL.lock().await;
    let prefix = prefix();
    let store = Arc::new(connect(&prefix).await);
    let (hook, removals) = recording_hook();
    store.install_removal_hook(hook).unwrap();
    let mut pinned = RegistrationRecord::bare(peer());
    pinned.pinned = true;
    let id = pinned.peer.instance_id();
    let incarnation = store.put(pinned, None).await.unwrap();

    let hold = store.hold_watch(WatchHold::Pause);
    let remove = tokio::spawn({
        let store = Arc::clone(&store);
        async move { store.remove(id, incarnation).await }
    });
    let mut client = raw().await;
    let key = instance_key(&prefix, id);
    let started = Instant::now();
    while raw_exists(&mut client, &key).await {
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the delete applies"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    store.close().await;
    drop(hold);
    assert!(remove.await.unwrap().is_ok(), "an applied remove is Ok");
    assert!(removals.lock().unwrap().is_empty());

    let other = RegistrationRecord::bare(peer());
    let other_id = other.peer.instance_id();
    assert!(matches!(
        store.put(other, None).await,
        Err(StoreError::Backend(_))
    ));
    assert!(!raw_exists(&mut client, &instance_key(&prefix, other_id)).await);
}

/// The normal stop: remove the pinned entry (the hook runs), then close,
/// which frees the prefix at once for the next hub.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remove_then_close_frees_the_prefix() {
    let prefix = prefix();
    let store = connect(&prefix).await;
    let (hook, removals) = recording_hook();
    store.install_removal_hook(hook).unwrap();
    let mut pinned = RegistrationRecord::bare(peer());
    pinned.pinned = true;
    let id = pinned.peer.instance_id();
    let incarnation = store.put(pinned, None).await.unwrap();
    store.remove(id, incarnation).await.unwrap();
    assert_eq!(removals.lock().unwrap().len(), 1);
    store.close().await;
    assert!(!raw_exists(&mut raw().await, &format!("{prefix}/hub")).await);
    connect(&prefix).await;
}

/// A store dropped without `close` stops its tasks and releases the lock
/// (D-4): the next store on the prefix starts well inside the lock TTL.
/// The release is spawned by drop, so the connect is retried briefly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_store_releases_the_prefix() {
    let prefix = prefix();
    let first = builder(&prefix)
        .lock_ttl(Duration::from_secs(30))
        .custody_ttl(Duration::from_secs(30))
        .connect()
        .await
        .unwrap();
    first
        .put(RegistrationRecord::bare(peer()), None)
        .await
        .unwrap();
    drop(first);
    let started = Instant::now();
    let second = loop {
        match builder(&prefix).connect().await {
            Ok(store) => break store,
            Err(ConnectError::PrefixHeld { .. }) if started.elapsed() < Duration::from_secs(2) => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => panic!("the dropped store kept the prefix: {error}"),
        }
    };
    assert_eq!(second.list().len(), 1, "the record survives the drop");
}

/// The hub reaches `close` through the trait object it holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_through_the_trait_frees_the_prefix() {
    let prefix = prefix();
    let store: Arc<dyn HubStore> = Arc::new(connect(&prefix).await);
    store.close().await;
    assert!(!raw_exists(&mut raw().await, &format!("{prefix}/hub")).await);
    connect(&prefix).await;
}

/// One hub per prefix: the second store cannot start while the first
/// holds the lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_store_cannot_take_a_held_prefix() {
    let prefix = prefix();
    let _first = connect(&prefix).await;
    match builder(&prefix).connect().await {
        Err(ConnectError::PrefixHeld { .. }) => {}
        other => panic!("expected PrefixHeld, got {:?}", other.map(|_| ())),
    }
}

async fn lock_lease(prefix: &str) -> i64 {
    let response = raw()
        .await
        .get(format!("{prefix}/hub"), None)
        .await
        .unwrap();
    response.kvs()[0].lease()
}

/// Losing the lock lease (here revoked out of band, as a partition longer
/// than its TTL would) fences the store for good: it reports `Fenced`, its
/// writes fail, and another store can take the prefix (review C3).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_revoked_lock_lease_fences_the_store() {
    let prefix = prefix();
    let first = connect(&prefix).await;
    raw()
        .await
        .lease_revoke(lock_lease(&prefix).await)
        .await
        .unwrap();
    assert!(
        wait_for_continuity(&first, Continuity::Fenced, Duration::from_secs(10)).await,
        "the store must see it lost the lock"
    );
    assert!(
        first
            .put(RegistrationRecord::bare(peer()), None)
            .await
            .is_err()
    );
    let second = connect(&prefix).await;
    second
        .put(RegistrationRecord::bare(peer()), None)
        .await
        .unwrap();
    assert_eq!(*first.continuity().borrow(), Continuity::Fenced, "terminal");
}

/// Every txn compares the lock lease, so a store that has not yet seen
/// the lock go (its watch is behind) still cannot write, and learns it is
/// fenced from the failed compare.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_lock_compare_fences_the_store() {
    let _serial = SERIAL.lock().await;
    let prefix = prefix();
    let store = builder(&prefix)
        .request_deadline(Duration::from_secs(1))
        .connect()
        .await
        .unwrap();
    let hold = store.hold_watch(WatchHold::Pause);
    raw()
        .await
        .lease_revoke(lock_lease(&prefix).await)
        .await
        .unwrap();
    let peer = peer();
    let result = store
        .put(RegistrationRecord::bare(peer.clone()), None)
        .await;
    assert!(matches!(result, Err(StoreError::Backend(_))), "{result:?}");
    assert_eq!(*store.continuity().borrow(), Continuity::Fenced);
    assert!(!raw_exists(&mut raw().await, &instance_key(&prefix, peer.instance_id())).await);
    drop(hold);
}

/// A compacted watch: the store reports `Lost`, reloads, reports each entry
/// that vanished in the gap once, then reports `Live`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_reports_lost_then_the_gap_removals_once_then_live() {
    let _serial = SERIAL.lock().await;
    let prefix = prefix();
    let store = connect(&prefix).await;
    let (hook, removals) = recording_hook();
    store.install_removal_hook(hook).unwrap();
    let kept = peer();
    let gone = peer();
    store
        .put(RegistrationRecord::bare(kept.clone()), None)
        .await
        .unwrap();
    let gone_inc = store
        .put(RegistrationRecord::bare(gone.clone()), None)
        .await
        .unwrap();

    let hold = store.hold_watch(WatchHold::Disconnect);
    assert!(wait_for_continuity(&store, Continuity::Degraded, Duration::from_secs(5)).await);
    let mut client = raw().await;
    client
        .delete(instance_key(&prefix, gone.instance_id()), None)
        .await
        .unwrap();
    let revision = client
        .put(format!("{prefix}/noise"), "x", None)
        .await
        .unwrap()
        .header()
        .unwrap()
        .revision();
    client.compact(revision, None).await.unwrap();
    drop(hold);

    assert!(wait_for_continuity(&store, Continuity::Live, Duration::from_secs(10)).await);
    assert_eq!(
        removals.lock().unwrap().as_slice(),
        &[Removal {
            id: gone.instance_id(),
            incarnation: gone_inc
        }],
        "the gap's removal is reported once"
    );
    assert!(store.contains(kept.instance_id()));
    let history = store.continuity_history();
    let lost = history
        .iter()
        .position(|c| *c == Continuity::Lost)
        .unwrap_or_else(|| panic!("Lost must be reported: {history:?}"));
    assert_eq!(history.last(), Some(&Continuity::Live), "{history:?}");
    assert!(lost < history.len() - 1);
}

/// A reconnect resumes from the last applied revision + 1: a removal
/// applied before the break is not reported again, and one that happened
/// during it is reported once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconnect_reports_no_removal_twice() {
    let _serial = SERIAL.lock().await;
    let prefix = prefix();
    let store = connect(&prefix).await;
    let (hook, removals) = recording_hook();
    store.install_removal_hook(hook).unwrap();
    let [a, c, d] = [peer(), peer(), peer()];
    let a_inc = store
        .put(RegistrationRecord::bare(a.clone()), None)
        .await
        .unwrap();
    let c_inc = store
        .put(RegistrationRecord::bare(c.clone()), None)
        .await
        .unwrap();
    store.remove(a.instance_id(), a_inc).await.unwrap();

    let hold = store.hold_watch(WatchHold::Disconnect);
    assert!(wait_for_continuity(&store, Continuity::Degraded, Duration::from_secs(5)).await);
    raw()
        .await
        .delete(instance_key(&prefix, c.instance_id()), None)
        .await
        .unwrap();
    drop(hold);
    assert!(wait_for_continuity(&store, Continuity::Live, Duration::from_secs(10)).await);
    assert!(
        eventually(Duration::from_secs(5), || removals.lock().unwrap().len()
            >= 2)
        .await
    );

    let d_inc = store
        .put(RegistrationRecord::bare(d.clone()), None)
        .await
        .unwrap();
    store.remove(d.instance_id(), d_inc).await.unwrap();
    assert_eq!(
        removals.lock().unwrap().as_slice(),
        &[
            Removal {
                id: a.instance_id(),
                incarnation: a_inc
            },
            Removal {
                id: c.instance_id(),
                incarnation: c_inc
            },
            Removal {
                id: d.instance_id(),
                incarnation: d_inc
            },
        ]
    );
    assert!(
        !store.continuity_history().contains(&Continuity::Lost),
        "a reconnect within history is not a loss"
    );
}

/// A call whose barrier times out answers `Indeterminate` and the store
/// reports `Degraded` until its watch catches up, so the hub refuses
/// registrations while it lags instead of acting on a stale view (D-5).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_barrier_timeout_degrades_until_the_watch_catches_up() {
    let _serial = SERIAL.lock().await;
    let store = builder(&prefix())
        .request_deadline(Duration::from_secs(1))
        .connect()
        .await
        .unwrap();
    let hold = store.hold_watch(WatchHold::Pause);
    let record = RegistrationRecord::bare(peer());
    let id = record.peer.instance_id();
    let op = record.op;
    let result = store.put(record, None).await;
    assert!(
        matches!(result, Err(StoreError::Indeterminate(_))),
        "{result:?}"
    );
    assert_eq!(*store.continuity().borrow(), Continuity::Degraded);
    drop(hold);
    assert!(wait_for_continuity(&store, Continuity::Live, Duration::from_secs(5)).await);
    assert!(
        store.contains(id),
        "the write applied; the cache has it now"
    );
    assert_eq!(
        store.resolve(id, op).await.unwrap(),
        store.current_incarnation(id)
    );
}

/// A new store on the prefix of a stopped one keeps the old custody lease
/// alive while a loaded record names it, so the entries survive the old
/// lease's TTL; it lets the lease go once nothing names it; and it deletes
/// the old hub's pinned entry, which no one would ever touch again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_store_adopts_the_custody_lease_and_drops_the_old_pinned_entry() {
    let prefix = prefix();
    let mut client = raw().await;
    let first = builder(&prefix)
        .custody_ttl(Duration::from_secs(3))
        .lock_ttl(Duration::from_secs(2))
        .connect()
        .await
        .unwrap();
    let mut pinned = RegistrationRecord::bare(peer());
    pinned.pinned = true;
    let pinned_id = pinned.peer.instance_id();
    first.put(pinned, None).await.unwrap();
    let kept = peer();
    let kept_record = RegistrationRecord::bare(kept.clone());
    first.put(kept_record.clone(), None).await.unwrap();
    let old_lease = client
        .get(instance_key(&prefix, kept.instance_id()), None)
        .await
        .unwrap()
        .kvs()[0]
        .lease();
    first.close().await;
    drop(first);

    let second = connect(&prefix).await;
    assert!(
        !second.contains(pinned_id),
        "the old hub's pinned entry goes"
    );
    assert!(!raw_exists(&mut client, &instance_key(&prefix, pinned_id)).await);
    let loaded = second.current_incarnation(kept.instance_id()).unwrap();

    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(
        raw_exists(&mut client, &instance_key(&prefix, kept.instance_id())).await,
        "the adopted custody lease is kept alive past its TTL"
    );

    second.put(kept_record, Some(loaded)).await.unwrap();
    let started = Instant::now();
    loop {
        let ttl = client
            .lease_time_to_live(old_lease, None)
            .await
            .unwrap()
            .ttl();
        if ttl < 0 {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "an adopted lease no record names is let go"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(second.contains(kept.instance_id()));
}

/// The watch and keep-alive streams are long-lived: the request deadline
/// must not cut them. Idle for several deadlines, then the store still
/// writes, holds its lock, and never left `Live`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streams_outlive_the_request_deadline() {
    let prefix = prefix();
    let store = builder(&prefix)
        .request_deadline(Duration::from_secs(1))
        .lock_ttl(Duration::from_secs(2))
        .connect()
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(4)).await;
    let peer = peer();
    let incarnation = store
        .put(RegistrationRecord::bare(peer.clone()), None)
        .await
        .unwrap();
    store.remove(peer.instance_id(), incarnation).await.unwrap();
    assert_eq!(store.continuity_history(), vec![Continuity::Live]);
    assert!(raw_exists(&mut raw().await, &format!("{prefix}/hub")).await);
}

/// The builder refuses settings the store cannot keep its promises with.
#[tokio::test]
async fn the_builder_refuses_inconsistent_settings() {
    let refused = |result: Result<EtcdStore, ConnectError>| {
        matches!(result, Err(ConnectError::InvalidConfig(_)))
    };
    assert!(refused(
        builder(&prefix())
            .custody_ttl(Duration::from_secs(5))
            .lock_ttl(Duration::from_secs(10))
            .connect()
            .await
    ));
    assert!(refused(
        builder(&prefix())
            .request_deadline(Duration::ZERO)
            .connect()
            .await
    ));
    assert!(refused(builder("no-leading-slash").connect().await));
}

/// Cost of the store's calls against the local etcd, and of a reaper pass
/// that expires 10,000 entries. Not a gate: run it with
/// `cargo test --release -p velo-hub --features test-support,etcd --test store_etcd etcd_store_cost -- --ignored --nocapture`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement, not a check"]
async fn etcd_store_cost() {
    const PEERS: usize = 10_000;
    const OPS: usize = 1_000;
    let store = Arc::new(
        builder(&prefix())
            // Entries go stale a second after their put, but only the
            // explicit pass below reaps them.
            .registrant_ttl(Duration::from_secs(1))
            .prune_interval(Duration::from_secs(3600))
            .connect()
            .await
            .unwrap(),
    );
    let (hook, removals) = recording_hook();
    store.install_removal_hook(hook).unwrap();
    let per_op = |elapsed: Duration| elapsed.as_secs_f64() * 1e6 / OPS as f64;

    let peers: Vec<PeerInfo> = (0..OPS).map(|_| peer()).collect();
    let started = Instant::now();
    let mut incarnations = Vec::with_capacity(OPS);
    for peer in &peers {
        incarnations.push(
            store
                .put(RegistrationRecord::bare(peer.clone()), None)
                .await
                .unwrap(),
        );
    }
    let put = per_op(started.elapsed());
    let started = Instant::now();
    for (peer, incarnation) in peers.iter().zip(&incarnations) {
        store.touch(peer.instance_id(), *incarnation).await.unwrap();
    }
    let touch = per_op(started.elapsed());
    let started = Instant::now();
    for (peer, incarnation) in peers.iter().zip(&incarnations) {
        store
            .remove(peer.instance_id(), *incarnation)
            .await
            .unwrap();
    }
    let remove = per_op(started.elapsed());
    println!("sequential: put {put:.0} us, touch {touch:.2} us, remove {remove:.0} us");
    // The floor: one plain etcd write, no txn, no wait for the watch.
    let mut client = raw().await;
    let key = format!("{}raw", store.prefix());
    let started = Instant::now();
    for _ in 0..OPS {
        client.put(key.as_str(), "x", None).await.unwrap();
    }
    println!(
        "sequential raw etcd put: {:.0} us",
        per_op(started.elapsed())
    );

    // Fill concurrently; only the reaper pass is measured.
    let started = Instant::now();
    let mut fill = tokio::task::JoinSet::new();
    for chunk in 0..64 {
        let store = Arc::clone(&store);
        fill.spawn(async move {
            for _ in 0..PEERS / 64 + usize::from(chunk < PEERS % 64) {
                store
                    .put(RegistrationRecord::bare(peer()), None)
                    .await
                    .unwrap();
            }
        });
    }
    fill.join_all().await;
    println!("concurrent fill of {PEERS}: {:?}", started.elapsed());
    assert_eq!(store.list().len(), PEERS);
    removals.lock().unwrap().clear();

    tokio::time::sleep(Duration::from_millis(1100)).await;
    let started = Instant::now();
    let reaped = store.reap_stale().await;
    let reaper = started.elapsed();
    assert!(
        eventually(Duration::from_secs(120), || removals.lock().unwrap().len()
            == PEERS)
        .await
    );
    println!(
        "reaper pass expiring {reaped} of {PEERS}: {reaper:?} for the deletes, {:?} until every hook ran",
        started.elapsed()
    );
    assert!(store.list().is_empty());
}

/// A prefix at the hub's target scale loads: 10,000 records of a realistic
/// size (an epoch, two credential digests, a 48-byte address, a services
/// payload) make one load response larger than tonic's default 4 MiB
/// decode limit, which the store lifts. Without that, a hub restarted on
/// such a prefix could not connect, and a store that lost history could
/// never reload.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_prefix_at_target_scale_loads() {
    use velo_hub::protocol::RegistrationEpoch;
    use velo_hub::store::CredentialDigest;
    const ENTRIES: usize = 10_000;
    let prefix = prefix();
    let store = Arc::new(builder(&prefix).connect().await.expect("connect"));
    let digest = CredentialDigest::of(
        &velo_hub::protocol::Credential::from_header_str("00000000-0000-4000-8000-000000000001")
            .unwrap(),
    );
    let mut writers = Vec::new();
    for chunk in 0..32 {
        let store = Arc::clone(&store);
        writers.push(tokio::spawn(async move {
            for _ in (chunk..ENTRIES).step_by(32) {
                let peer = PeerInfo::new(
                    InstanceId::new_v4(),
                    WorkerAddress::from_encoded(vec![7u8; 48]),
                );
                let mut record = RegistrationRecord::bare(peer);
                record.epoch = Some(RegistrationEpoch::new());
                record.accepted = Some(digest);
                record.pending = Some(digest);
                record
                    .extensions
                    .insert("services".into(), serde_json::json!(["svc-a", "svc-b"]));
                store.put(record, None).await.unwrap();
            }
        }));
    }
    for writer in writers {
        writer.await.unwrap();
    }
    store.close().await;
    drop(store);
    let reloaded = builder(&prefix)
        .wait_for_prefix(Duration::from_secs(15))
        .connect()
        .await
        .unwrap_or_else(|error| panic!("a prefix of {ENTRIES} records does not load: {error}"));
    assert_eq!(reloaded.list().len(), ENTRIES);
    reloaded.close().await;
}
