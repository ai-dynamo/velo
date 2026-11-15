// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! A slot is freed when its last reader and its last reference are both gone,
//! whichever goes last, on every path that ends a lease.
//!
//! The case these tests pin: the owner drops its reference while a reader
//! still holds a lease, so the reference count is already 0 when the reader
//! finishes. Six paths end a lease: local and remote detach, local and remote
//! release, a `LeaseGuard` drop, and the reaper. Each once decided whether to
//! free from the counter it changed itself: detach looked at nothing, and
//! release and the reaper asked `ref_decrement`, which returns false when the
//! count is already 0. So every path left a slot nobody could reach, for the
//! life of the process. With RDMA staging the slot also holds its region's
//! in-flight guard, so `RegionGuard::unregister` waited out its whole timeout.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use velo_ext::WorkerId;

use super::RendezvousManager;
use super::handle::DataHandle;
use super::store::DataStore;

const PAYLOAD: &[u8] = b"payload";

/// Drop the owner's reference the way a caller with the store does it,
/// while the reader's lease still holds the slot.
fn drop_owner_reference(store: &DataStore, handle: DataHandle, lease: u64) {
    let (_, slot) = handle.unpack();
    assert_eq!(
        store.lease_slot(lease),
        Some(slot),
        "the reader is attached"
    );
    assert_eq!(store.metadata(slot).unwrap().refcount, 1);
    assert!(
        !store.ref_decrement(slot),
        "the read lock keeps the slot while the reader is attached"
    );
    store.try_free(slot);
    let meta = store
        .metadata(slot)
        .expect("the reader's lease keeps the slot");
    assert_eq!(meta.refcount, 0);
}

fn local() -> RendezvousManager {
    RendezvousManager::new(WorkerId::from_u64(1))
}

#[tokio::test]
async fn local_detach_frees_the_slot_of_a_released_owner() {
    let manager = local();
    let handle = manager.register_data(Bytes::from_static(PAYLOAD));
    let (_, lease) = manager.get(handle).await.unwrap();
    drop_owner_reference(manager.data_store(), handle, lease);

    manager.detach(handle, lease).await.unwrap();

    assert!(manager.data_store().lease_slot(lease).is_none());
    assert!(
        manager.data_store().metadata(handle.unpack().1).is_none(),
        "local detach left a slot with no reader and no reference"
    );
}

#[tokio::test]
async fn local_release_frees_the_slot_of_a_released_owner() {
    let manager = local();
    let handle = manager.register_data(Bytes::from_static(PAYLOAD));
    let (_, lease) = manager.get(handle).await.unwrap();
    drop_owner_reference(manager.data_store(), handle, lease);

    manager.release(handle, lease).await.unwrap();

    assert!(manager.data_store().lease_slot(lease).is_none());
    assert!(
        manager.data_store().metadata(handle.unpack().1).is_none(),
        "local release left a slot with no reader and no reference"
    );
}

#[tokio::test]
async fn a_lease_guard_drop_frees_the_slot_of_a_released_owner() {
    let manager = local();
    let handle = manager.register_data(Bytes::from_static(PAYLOAD));
    let slot = handle.unpack().1;
    let lease = manager.data_store().acquire_read_lock(slot).unwrap();
    let guard = manager.lease_guard(handle, lease, None);
    drop_owner_reference(manager.data_store(), handle, lease);

    drop(guard);

    assert!(manager.data_store().lease_slot(lease).is_none());
    assert!(
        manager.data_store().metadata(slot).is_none(),
        "LeaseGuard::drop left a slot with no reader and no reference"
    );
}

#[test]
fn the_reaper_frees_the_slot_of_a_released_owner() {
    let store = DataStore::new();
    let slot = store.register(Bytes::from_static(PAYLOAD), None);
    let lease = store.acquire_read_lock(slot).unwrap();
    let handle = DataHandle::pack(WorkerId::from_u64(1), slot);
    store.set_lease_deadline(lease, slot, Duration::from_secs(1));
    drop_owner_reference(&store, handle, lease);

    assert_eq!(
        store.sweep_expired(Instant::now() + Duration::from_secs(2)),
        (1, 0),
        "the sweep reaped the lease"
    );

    assert!(store.lease_slot(lease).is_none());
    assert!(
        store.metadata(slot).is_none(),
        "the reaper ended the last lease but left a slot with no reference"
    );
}

/// The control: a second reader is still attached, so the first reader's
/// detach must not free the slot, and the second can still read it.
#[tokio::test]
async fn a_slot_with_a_reader_still_attached_is_not_freed() {
    let manager = local();
    let handle = manager.register_data(Bytes::from_static(PAYLOAD));
    let slot = handle.unpack().1;
    let (_, first) = manager.get(handle).await.unwrap();
    let (_, second) = manager.get(handle).await.unwrap();
    drop_owner_reference(manager.data_store(), handle, first);

    manager.detach(handle, first).await.unwrap();
    assert_eq!(manager.data_store().lease_slot(second), Some(slot));
    assert_eq!(
        manager.data_store().get_data(slot).unwrap(),
        Bytes::from_static(PAYLOAD),
        "the attached reader still reads the slot"
    );

    manager.detach(handle, second).await.unwrap();
    assert!(manager.data_store().metadata(slot).is_none());
}

/// The same control on the reaper: reaping one of two leases keeps the slot.
#[test]
fn reaping_one_of_two_leases_keeps_the_slot() {
    let store = DataStore::new();
    let slot = store.register(Bytes::from_static(PAYLOAD), None);
    let first = store.acquire_read_lock(slot).unwrap();
    let second = store.acquire_read_lock(slot).unwrap();
    let handle = DataHandle::pack(WorkerId::from_u64(1), slot);
    store.set_lease_deadline(first, slot, Duration::from_secs(1));
    drop_owner_reference(&store, handle, first);

    assert_eq!(
        store.sweep_expired(Instant::now() + Duration::from_secs(2)),
        (1, 0)
    );
    assert_eq!(store.lease_slot(second), Some(slot));
    assert_eq!(store.get_data(slot).unwrap(), Bytes::from_static(PAYLOAD));
}

/// A reader that attaches after the last one detached, but before the owner's
/// `try_free`, keeps the slot: `try_free` checks both counts as it removes.
#[test]
fn a_reader_that_attaches_before_try_free_keeps_the_slot() {
    let store = DataStore::new();
    let slot = store.register(Bytes::from_static(PAYLOAD), None);
    let first = store.acquire_read_lock(slot).unwrap();
    assert!(!store.ref_decrement(slot));
    assert_eq!(
        store.consume_lease(first, slot),
        super::store::LeaseOutcome::Consumed
    );
    assert!(store.release_read_lock(slot));

    let second = store.acquire_read_lock(slot).unwrap();
    store.try_free(slot);
    assert_eq!(store.metadata(slot).unwrap().refcount, 0);
    assert_eq!(store.get_data(slot).unwrap(), Bytes::from_static(PAYLOAD));
    assert_eq!(
        store.consume_lease(second, slot),
        super::store::LeaseOutcome::Consumed
    );
}

/// A chunk the reader already holds stays valid after the reaper frees its
/// slot: a heap chunk is a `Bytes` that owns its share of the payload.
#[test]
fn a_reaped_heap_chunk_stays_owned() {
    let store = DataStore::new();
    let slot = store.register(Bytes::from(vec![42u8; 2048]), None);
    let lease = store.acquire_read_lock(slot).unwrap();
    let (transfer, _, _) = store.create_transfer(slot, lease, 1024).unwrap();
    let held = store.get_chunk(transfer, 0).unwrap();
    store.set_lease_deadline(lease, slot, Duration::from_secs(1));

    assert_eq!(
        store.sweep_expired(Instant::now() + Duration::from_secs(2)),
        (1, 0)
    );

    assert!(store.metadata(slot).is_none());
    assert!(store.get_chunk(transfer, 1).is_none());
    assert_eq!(held.as_ref(), &[42u8; 1024]);
}

// --- Remote: the owner's `_rv_detach` and `_rv_release` handlers ---

async fn tcp_node() -> crate::Velo {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    crate::Velo::builder()
        .add_transport(Arc::new(
            crate::transports::tcp::TcpTransportBuilder::new()
                .from_listener(listener)
                .unwrap()
                .build()
                .unwrap(),
        ))
        .build()
        .await
        .unwrap()
}

/// Two connected nodes; the second reads a payload the first staged.
async fn remote_read() -> (crate::Velo, crate::Velo, DataHandle, u64) {
    let owner = tcp_node().await;
    let reader = tcp_node().await;
    reader.register_peer(owner.peer_info()).unwrap();
    owner.register_peer(reader.peer_info()).unwrap();
    let handle = owner.register_data(Bytes::from_static(PAYLOAD));
    let (data, lease) = reader.get(handle).await.unwrap();
    assert_eq!(data, Bytes::from_static(PAYLOAD));
    (owner, reader, handle, lease)
}

/// Wait up to 5 s for the owner to free `slot`. The handler runs after the
/// reader's call returns, so the test polls for the end state.
async fn freed_on_owner(store: &DataStore, slot: u64) -> bool {
    tokio::time::timeout(Duration::from_secs(5), async {
        while store.metadata(slot).is_some() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok()
}

async fn shut_down(nodes: [crate::Velo; 2]) {
    for node in nodes {
        node.shutdown(crate::ShutdownPolicy::Timeout(Duration::from_secs(2)))
            .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_detach_frees_the_slot_of_a_released_owner() {
    let (owner, reader, handle, lease) = remote_read().await;
    let store = Arc::clone(owner.inner.rendezvous_manager.data_store());
    drop_owner_reference(&store, handle, lease);

    reader.detach(handle, lease).await.unwrap();

    let freed = freed_on_owner(&store, handle.unpack().1).await;
    shut_down([reader, owner]).await;
    assert!(
        freed,
        "remote _rv_detach left a slot with no reader and no reference"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_release_frees_the_slot_of_a_released_owner() {
    let (owner, reader, handle, lease) = remote_read().await;
    let store = Arc::clone(owner.inner.rendezvous_manager.data_store());
    drop_owner_reference(&store, handle, lease);

    reader.release(handle, lease).await.unwrap();

    let freed = freed_on_owner(&store, handle.unpack().1).await;
    shut_down([reader, owner]).await;
    assert!(
        freed,
        "remote _rv_release left a slot with no reader and no reference"
    );
}

/// The owner's reference and the last reader's lock can drop at the same time,
/// on two threads. One of the two must free the slot. If each side compared a
/// relaxed load of the other's count, both could read the old value on a
/// weakly ordered CPU and neither would free. The owner here frees the way a
/// caller with the store does: `try_free` only when `ref_decrement` says so.
#[test]
fn an_owner_and_its_last_reader_leaving_together_free_the_slot() {
    use super::store::LeaseEnd;
    use std::sync::Barrier;

    for _ in 0..2_000 {
        let store = Arc::new(DataStore::new());
        let slot = store.register(Bytes::from_static(PAYLOAD), None);
        let lease = store.acquire_read_lock(slot).unwrap();
        let start = Arc::new(Barrier::new(2));
        let owner = {
            let (store, start) = (Arc::clone(&store), Arc::clone(&start));
            std::thread::spawn(move || {
                start.wait();
                if store.ref_decrement(slot) {
                    store.try_free(slot);
                }
            })
        };
        start.wait();
        assert_eq!(
            store.end_lease(lease, slot, LeaseEnd::Detach),
            super::store::LeaseOutcome::Consumed
        );
        owner.join().unwrap();
        assert!(
            store.metadata(slot).is_none(),
            "neither side freed the slot"
        );
    }
}
