// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! `velo_rendezvous_active_slots` reads the number of staged slots after a
//! slot is freed, on every path that frees one.
//!
//! The gauge was set only by the calls that knew they changed the slot count:
//! a registration, a local release, and the reapers. The owner's
//! `Velo::unregister_data`, a local detach, and the owner's `_rv_detach` and
//! `_rv_release` handlers free a slot too, through the same
//! `DataStore::try_free`, but left the gauge at its old value. Each test frees
//! the only slot through one of those paths and reads the gauge.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use prometheus::Registry;

use super::handle::DataHandle;
use super::store::DataStore;
use crate::observability::VeloMetrics;
use crate::observability::test_helpers::MetricSnapshot;

const PAYLOAD: &[u8] = b"payload";
const GAUGE: &str = "velo_rendezvous_active_slots";

fn active_slots(registry: &Registry) -> f64 {
    MetricSnapshot::from_registry(registry).gauge(GAUGE, &[])
}

async fn tcp_node(metrics: Option<Arc<VeloMetrics>>) -> crate::Velo {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut builder = crate::Velo::builder().add_transport(Arc::new(
        crate::transports::tcp::TcpTransportBuilder::new()
            .from_listener(listener)
            .unwrap()
            .build()
            .unwrap(),
    ));
    if let Some(metrics) = metrics {
        builder = builder.metrics(metrics);
    }
    builder.build().await.unwrap()
}

/// A node that emits into a fresh registry.
async fn metered_node() -> (crate::Velo, Registry) {
    let registry = Registry::new();
    let metrics = Arc::new(VeloMetrics::register(&registry).unwrap());
    (tcp_node(Some(metrics)).await, registry)
}

/// Stage one payload and check that the gauge counts it, so a later 0 is a
/// refresh and not a series that was never set.
fn stage_one(node: &crate::Velo, registry: &Registry) -> DataHandle {
    let handle = node.register_data(Bytes::from_static(PAYLOAD));
    assert_eq!(active_slots(registry), 1.0, "control: the gauge counts it");
    handle
}

fn store_of(node: &crate::Velo) -> Arc<DataStore> {
    Arc::clone(node.inner.rendezvous_manager.data_store())
}

/// Wait up to 5 s for the owner to free `slot`. A remote handler runs after
/// the reader's call returns, so the test polls for the end state.
async fn freed(store: &DataStore, slot: u64) -> bool {
    tokio::time::timeout(Duration::from_secs(5), async {
        while store.metadata(slot).is_some() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok()
}

async fn shut_down(nodes: impl IntoIterator<Item = crate::Velo>) {
    for node in nodes {
        node.shutdown(crate::ShutdownPolicy::Timeout(Duration::from_secs(2)))
            .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_owner_call_refreshes_the_gauge() {
    let (owner, registry) = metered_node().await;
    let handle = stage_one(&owner, &registry);

    assert!(owner.unregister_data(handle));

    assert!(store_of(&owner).metadata(handle.unpack().1).is_none());
    let gauge = active_slots(&registry);
    shut_down([owner]).await;
    assert_eq!(gauge, 0.0, "Velo::unregister_data freed the slot");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_local_detach_refreshes_the_gauge() {
    let (owner, registry) = metered_node().await;
    let handle = stage_one(&owner, &registry);
    let (_, lease) = owner.get(handle).await.unwrap();
    assert!(owner.unregister_data(handle));
    assert_eq!(active_slots(&registry), 1.0, "the reader keeps the slot");

    owner.detach(handle, lease).await.unwrap();

    assert!(store_of(&owner).metadata(handle.unpack().1).is_none());
    let gauge = active_slots(&registry);
    shut_down([owner]).await;
    assert_eq!(gauge, 0.0, "the local detach freed the slot");
}

/// Two connected nodes; the second reads a payload the first staged, and
/// the owner then drops its reference, so the reader's lease alone keeps the
/// slot.
async fn remote_read() -> (crate::Velo, crate::Velo, Registry, DataHandle, u64) {
    let (owner, registry) = metered_node().await;
    let reader = tcp_node(None).await;
    reader.register_peer(owner.peer_info()).unwrap();
    owner.register_peer(reader.peer_info()).unwrap();
    let handle = stage_one(&owner, &registry);
    let (data, lease) = reader.get(handle).await.unwrap();
    assert_eq!(data, Bytes::from_static(PAYLOAD));
    assert!(owner.unregister_data(handle));
    assert_eq!(active_slots(&registry), 1.0, "the reader keeps the slot");
    (owner, reader, registry, handle, lease)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_remote_detach_refreshes_the_gauge() {
    let (owner, reader, registry, handle, lease) = remote_read().await;

    reader.detach(handle, lease).await.unwrap();

    let slot_freed = freed(&store_of(&owner), handle.unpack().1).await;
    let gauge = active_slots(&registry);
    shut_down([reader, owner]).await;
    assert!(slot_freed, "the owner's _rv_detach handler freed the slot");
    assert_eq!(gauge, 0.0, "the owner's _rv_detach handler freed the slot");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_remote_release_refreshes_the_gauge() {
    let (owner, reader, registry, handle, lease) = remote_read().await;

    reader.release(handle, lease).await.unwrap();

    let slot_freed = freed(&store_of(&owner), handle.unpack().1).await;
    let gauge = active_slots(&registry);
    shut_down([reader, owner]).await;
    assert!(slot_freed, "the owner's _rv_release handler freed the slot");
    assert_eq!(gauge, 0.0, "the owner's _rv_release handler freed the slot");
}
