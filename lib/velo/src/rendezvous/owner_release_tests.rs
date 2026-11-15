// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The owner's call, [`Velo::unregister_data`](crate::Velo::unregister_data):
//! it drops the reference `register_data` gave the owner, once, from any
//! thread, with no runtime.
//!
//! An owner that held its handle in a struct used to reach into the store
//! (`data_store()`, `ref_decrement`, `try_free`) from that struct's `Drop`.
//! A repeat of that sequence takes a second reference, which may be a
//! reader's; these tests pin that the public call cannot.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use velo_ext::WorkerId;

use super::RendezvousManager;
use super::handle::DataHandle;
use super::store::{DataStore, LeaseEnd, LeaseOutcome};

const PAYLOAD: &[u8] = b"payload";

fn local() -> RendezvousManager {
    RendezvousManager::new(WorkerId::from_u64(1))
}

fn slot_of(handle: DataHandle) -> u64 {
    handle.unpack().1
}

#[test]
fn with_no_reader_the_owner_call_frees_the_slot_at_once() {
    let manager = local();
    let handle = manager.register_data(Bytes::from_static(PAYLOAD));

    assert!(manager.unregister_data(handle));

    assert!(
        manager.data_store().metadata(slot_of(handle)).is_none(),
        "nothing reads the slot, so the owner's call frees it"
    );
}

/// A repeat call must not take a reference that `ref_handle` gave a reader.
/// With a single reference a second call would find the count at 0 and do
/// nothing either way, so the reader's reference is what makes this test fail
/// on a bare `ref_decrement`.
#[tokio::test]
async fn a_repeat_owner_call_does_nothing() {
    let manager = local();
    let handle = manager.register_data(Bytes::from_static(PAYLOAD));
    manager.ref_handle(handle).await.unwrap();
    let store = manager.data_store();
    assert_eq!(store.metadata(slot_of(handle)).unwrap().refcount, 2);

    assert!(manager.unregister_data(handle));
    assert!(
        !manager.unregister_data(handle),
        "a repeat call does nothing"
    );

    let meta = store
        .metadata(slot_of(handle))
        .expect("the reader's reference keeps the slot");
    assert_eq!(
        meta.refcount, 1,
        "the repeat call took the reader's reference"
    );
}

#[test]
fn an_unknown_handle_does_nothing() {
    let manager = local();
    let handle = manager.register_data(Bytes::from_static(PAYLOAD));
    let slot = slot_of(handle);

    // Another worker's handle with the same local id: not this worker's slot.
    let foreign = DataHandle::pack(WorkerId::from_u64(2), slot);
    assert!(!manager.unregister_data(foreign));
    // This worker, a slot it never staged.
    let missing = DataHandle::pack(WorkerId::from_u64(1), slot + 1_000);
    assert!(!manager.unregister_data(missing));

    let meta = manager.data_store().metadata(slot).expect("slot untouched");
    assert_eq!(meta.refcount, 1);
    assert!(
        manager.unregister_data(handle),
        "the owner's reference is intact"
    );
}

#[tokio::test]
async fn a_slot_with_a_reader_survives_the_owner_call_until_the_reader_detaches() {
    let manager = local();
    let handle = manager.register_data(Bytes::from_static(PAYLOAD));
    let (data, lease) = manager.get(handle).await.unwrap();
    assert_eq!(data, Bytes::from_static(PAYLOAD));

    assert!(manager.unregister_data(handle));
    let meta = manager
        .data_store()
        .metadata(slot_of(handle))
        .expect("the reader's lease keeps the slot");
    assert_eq!(meta.refcount, 0);

    manager.detach(handle, lease).await.unwrap();
    assert!(
        manager.data_store().metadata(slot_of(handle)).is_none(),
        "the last reader's detach frees the slot the owner let go"
    );
}

/// The owner's call and the last reader's detach can run at once on two
/// threads; one of them must free the slot. The owner side takes the same
/// rule as `end_lease`: a call that takes its count to 0 always goes to
/// `try_free`, which compares both counts under the shard's write lock. See
/// `docs/src/development/coding-gotchas.md`.
#[test]
fn the_owner_call_and_the_last_reader_leaving_together_free_the_slot() {
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
                assert!(store.release_owner_reference(slot));
            })
        };
        start.wait();
        assert_eq!(
            store.end_lease(lease, slot, LeaseEnd::Detach),
            LeaseOutcome::Consumed
        );
        owner.join().unwrap();
        assert!(
            store.metadata(slot).is_none(),
            "neither side freed the slot"
        );
    }
}

/// An owner keeps its handle in a struct and gives it back in `Drop`. That
/// `Drop` may run on a thread with no tokio runtime, so the call must not
/// need one.
#[test]
fn the_owner_call_works_from_drop_outside_any_runtime() {
    struct Staged {
        velo: crate::Velo,
        handle: DataHandle,
    }
    impl Drop for Staged {
        fn drop(&mut self) {
            assert!(self.velo.unregister_data(self.handle));
        }
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let velo = runtime.block_on(tcp_node());
    let handle = velo.register_data(Bytes::from_static(PAYLOAD));
    let staged = Staged {
        velo: velo.clone(),
        handle,
    };

    std::thread::spawn(move || {
        assert!(
            tokio::runtime::Handle::try_current().is_err(),
            "the drop must run outside any runtime"
        );
        drop(staged);
    })
    .join()
    .unwrap();

    let freed = velo
        .inner
        .rendezvous_manager
        .data_store()
        .metadata(slot_of(handle))
        .is_none();
    runtime.block_on(velo.shutdown(crate::ShutdownPolicy::Timeout(Duration::from_secs(2))));
    assert!(freed, "the owner's call from Drop left the slot staged");
}

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
