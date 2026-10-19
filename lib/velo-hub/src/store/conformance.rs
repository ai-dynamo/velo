// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The [`HubStore`] contract as executable checks. Every backend runs
//! [`run_all`] in its own tests:
//!
//! ```ignore
//! #[tokio::test]
//! async fn my_store_conforms() {
//!     velo_hub::store::conformance::run_all(|| Arc::new(MyStore::new())).await;
//! }
//! ```
//!
//! Each check takes a fresh store from the factory and panics with the rule
//! it found broken. Liveness (TTL, leases) is backend-specific and is not
//! checked here.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use velo::{InstanceId, PeerInfo, WorkerAddress};

use super::{
    Continuity, HubStore, Incarnation, OpId, RegistrationRecord, Removal, RemovalHook, StoreError,
};
use crate::protocol::RegistrationEpoch;

/// Run every check, each against a fresh store.
pub async fn run_all<S, F>(factory: F)
where
    S: HubStore,
    F: Fn() -> Arc<S>,
{
    incarnations_are_never_reused(factory().as_ref()).await;
    put_is_conditional(factory().as_ref()).await;
    remove_and_touch_are_conditional(factory().as_ref()).await;
    hook_snapshot_and_removals_agree(factory().as_ref()).await;
    hook_runs_outside_store_locks(factory()).await;
    reads_agree_with_writes(factory().as_ref()).await;
    replacement_is_what_discovery_serves(factory().as_ref()).await;
    resolve_names_the_writing_op(factory().as_ref()).await;
    continuity_starts_live(factory().as_ref());
}

fn peer() -> PeerInfo {
    PeerInfo::new(
        InstanceId::new_v4(),
        WorkerAddress::from_encoded(b"conformance".to_vec()),
    )
}

fn record(peer: &PeerInfo) -> RegistrationRecord {
    let mut record = RegistrationRecord::bare(peer.clone());
    record.epoch = Some(RegistrationEpoch::new());
    record
        .extensions
        .insert("probe".to_string(), serde_json::json!({"role": "a"}));
    record
}

fn recording_hook() -> (RemovalHook, Arc<Mutex<Vec<Removal>>>) {
    let removals = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&removals);
    let hook: RemovalHook = Arc::new(move |removal| captured.lock().unwrap().push(removal));
    (hook, removals)
}

/// A put after a replacement or a removal never yields an incarnation the
/// instance had before.
pub async fn incarnations_are_never_reused<S: HubStore>(store: &S) {
    let peer = peer();
    let id = peer.instance_id();
    let first = store.put(record(&peer), None).await.unwrap();
    let second = store.put(record(&peer), Some(first)).await.unwrap();
    store.remove(id, second).await.unwrap();
    let third = store.put(record(&peer), None).await.unwrap();
    let distinct: HashSet<Incarnation> = [first, second, third].into_iter().collect();
    assert_eq!(
        distinct.len(),
        3,
        "incarnation reused: {first} {second} {third}"
    );
}

/// `expected: None` requires no entry; `Some(i)` requires `i` current.
pub async fn put_is_conditional<S: HubStore>(store: &S) {
    let peer = peer();
    let id = peer.instance_id();
    assert!(
        matches!(
            store
                .put(record(&peer), Some(Incarnation::from_u64(u64::MAX)))
                .await,
            Err(StoreError::Stale { current: None, .. })
        ),
        "a put expecting an incarnation must fail when there is no entry"
    );
    assert!(!store.contains(id), "a failed put must write nothing");

    let first = store.put(record(&peer), None).await.unwrap();
    assert!(
        matches!(
            store.put(record(&peer), None).await,
            Err(StoreError::Stale { current: Some(c), .. }) if c == first
        ),
        "a put expecting no entry must fail when there is one"
    );
    let second = store.put(record(&peer), Some(first)).await.unwrap();
    assert!(
        matches!(
            store.put(record(&peer), Some(first)).await,
            Err(StoreError::Stale { .. })
        ),
        "a put expecting a replaced incarnation must fail"
    );
    assert_eq!(store.current_incarnation(id), Some(second));
}

/// `remove` and `touch` act only on the current incarnation.
pub async fn remove_and_touch_are_conditional<S: HubStore>(store: &S) {
    let peer = peer();
    let id = peer.instance_id();
    assert!(matches!(
        store.remove(id, Incarnation::from_u64(1)).await,
        Err(StoreError::NotFound(_))
    ));
    assert!(matches!(
        store.touch(id, Incarnation::from_u64(1)).await,
        Err(StoreError::NotFound(_))
    ));
    let stale = store.put(record(&peer), None).await.unwrap();
    let current = store.put(record(&peer), Some(stale)).await.unwrap();
    assert!(matches!(
        store.touch(id, stale).await,
        Err(StoreError::Stale { .. })
    ));
    assert!(matches!(
        store.remove(id, stale).await,
        Err(StoreError::Stale { .. })
    ));
    assert!(store.contains(id), "a stale remove must not remove");
    store.touch(id, current).await.unwrap();
    store.remove(id, current).await.unwrap();
    assert!(!store.contains(id));
    assert!(matches!(
        store.remove(id, current).await,
        Err(StoreError::NotFound(_))
    ));
}

/// The snapshot holds what was present; each later removal is reported
/// once; replacements and failed removals are not reported; the hook
/// cannot be replaced.
pub async fn hook_snapshot_and_removals_agree<S: HubStore>(store: &S) {
    let a = peer();
    let b = peer();
    let a_inc = store.put(record(&a), None).await.unwrap();
    let b_first = store.put(record(&b), None).await.unwrap();
    let (hook, removals) = recording_hook();

    let snapshot = store.install_removal_hook(hook).unwrap();
    let mut seen: Vec<(InstanceId, Incarnation)> = snapshot
        .iter()
        .map(|stored| (stored.instance_id(), stored.incarnation))
        .collect();
    seen.sort_by_key(|(id, inc)| (id.as_u128(), *inc));
    let mut expected = vec![(a.instance_id(), a_inc), (b.instance_id(), b_first)];
    expected.sort_by_key(|(id, inc)| (id.as_u128(), *inc));
    assert_eq!(seen, expected, "the snapshot must hold every entry present");
    assert!(matches!(
        store.install_removal_hook(Arc::new(|_| {})),
        Err(StoreError::HookInstalled)
    ));

    store.remove(a.instance_id(), a_inc).await.unwrap();
    let b_second = store.put(record(&b), Some(b_first)).await.unwrap();
    let _ = store.remove(b.instance_id(), b_first).await;
    store.remove(b.instance_id(), b_second).await.unwrap();

    assert_eq!(
        removals.lock().unwrap().as_slice(),
        &[
            Removal {
                id: a.instance_id(),
                incarnation: a_inc
            },
            Removal {
                id: b.instance_id(),
                incarnation: b_second
            },
        ],
        "each removal is reported once; a replacement and a stale remove are not"
    );
}

/// The hook may read the store. A backend that calls it under its own lock
/// deadlocks here; the check runs the removal on another thread so the
/// deadlock fails the test instead of hanging it.
pub async fn hook_runs_outside_store_locks<S: HubStore>(store: Arc<S>) {
    let peer = peer();
    let id = peer.instance_id();
    let incarnation = store.put(record(&peer), None).await.unwrap();
    // Weak: the store holds the hook, so a strong clone would be a cycle.
    let reader = Arc::downgrade(&store);
    store
        .install_removal_hook(Arc::new(move |removal| {
            if let Some(reader) = reader.upgrade() {
                let _ = reader.get(removal.id);
                let _ = reader.list();
            }
        }))
        .unwrap();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let remover = Arc::clone(&store);
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = runtime.block_on(remover.remove(id, incarnation));
        let _ = done_tx.send(result.is_ok());
    });
    let finished =
        tokio::task::spawn_blocking(move || done_rx.recv_timeout(Duration::from_secs(5)))
            .await
            .unwrap();
    assert_eq!(
        finished,
        Ok(true),
        "the removal hook must run outside store locks (a hook that reads the store deadlocked or failed)"
    );
}

/// Sync reads and discovery agree with the writes before them.
pub async fn reads_agree_with_writes<S: HubStore>(store: &S) {
    let peer = peer();
    let id = peer.instance_id();
    let mut written = record(&peer);
    written.pinned = true;
    let incarnation = store.put(written.clone(), None).await.unwrap();

    let stored = store.get(id).expect("get after put");
    assert_eq!(stored.record, written, "the record must round-trip");
    assert_eq!(stored.incarnation, incarnation);
    assert!(store.contains(id));
    assert!(store.is_current(id, incarnation));
    assert_eq!(store.current_incarnation(id), Some(incarnation));
    assert_eq!(store.list().len(), 1);
    assert_eq!(
        store.peers(),
        vec![super::StoredPeer {
            peer: peer.clone(),
            incarnation,
            pinned: true
        }],
        "peers() agrees with list()"
    );
    assert_eq!(store.discover_by_instance_id(id).await.unwrap(), peer);
    assert_eq!(
        store.discover_by_worker_id(peer.worker_id()).await.unwrap(),
        peer
    );

    store.remove(id, incarnation).await.unwrap();
    assert!(store.get(id).is_none());
    assert!(store.list().is_empty());
    assert!(store.peers().is_empty());
    assert!(store.discover_by_instance_id(id).await.is_err());
    assert!(store.discover_by_worker_id(peer.worker_id()).await.is_err());
}

/// After a replacement with a new address, every read serves the new one.
/// A backend whose local view keeps the first write fails here.
pub async fn replacement_is_what_discovery_serves<S: HubStore>(store: &S) {
    let id = InstanceId::new_v4();
    let first = PeerInfo::new(id, WorkerAddress::from_encoded(b"first-address".to_vec()));
    let second = PeerInfo::new(id, WorkerAddress::from_encoded(b"second-address".to_vec()));
    let incarnation = store.put(record(&first), None).await.unwrap();
    store.put(record(&second), Some(incarnation)).await.unwrap();
    assert_eq!(store.get(id).unwrap().record.peer, second);
    assert_eq!(
        store.discover_by_instance_id(id).await.unwrap(),
        second,
        "instance lookup serves the replacement"
    );
    assert_eq!(
        store
            .discover_by_worker_id(second.worker_id())
            .await
            .unwrap(),
        second,
        "worker lookup serves the replacement"
    );
    assert_eq!(store.peers()[0].peer, second);
}

/// `resolve` names the incarnation of the op that wrote the current record,
/// and nothing for any other op.
pub async fn resolve_names_the_writing_op<S: HubStore>(store: &S) {
    let peer = peer();
    let id = peer.instance_id();
    let first = record(&peer);
    let first_op = first.op;
    let first_inc = store.put(first, None).await.unwrap();
    assert_eq!(store.resolve(id, first_op).await.unwrap(), Some(first_inc));
    assert_eq!(store.resolve(id, OpId::new()).await.unwrap(), None);

    let second = record(&peer);
    let second_op = second.op;
    let second_inc = store.put(second, Some(first_inc)).await.unwrap();
    assert_eq!(store.resolve(id, first_op).await.unwrap(), None);
    assert_eq!(
        store.resolve(id, second_op).await.unwrap(),
        Some(second_inc)
    );

    store.remove(id, second_inc).await.unwrap();
    assert_eq!(store.resolve(id, second_op).await.unwrap(), None);
}

/// A fresh store is current.
pub fn continuity_starts_live<S: HubStore>(store: &S) {
    assert_eq!(*store.continuity().borrow(), Continuity::Live);
}
