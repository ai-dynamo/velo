// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Ported from kvbm-hub `tests/registration_lifecycle.rs`. kvbm drove these
//! with a custom registry and its indexer manager; here `FaultStore` is the
//! custom backend and the `Ledger` toy (common/mod.rs) has the indexer's
//! lifecycle semantics. Every assertion is classified in
//! `agent-docs/velo-hub-m1-ledger.md`; the two that changed say so inline.

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use common::*;
use velo_hub::store::testing::{FaultStore, PutFault};
use velo_hub::store::{HubStore, RegistrationRecord};
use velo_hub::velo::{PeerDiscovery, PeerInfo, WorkerAddress};
use velo_hub::{AttachContext, Committed, Extension, Hub, Rejection, Txn};

async fn start_custom(store: &Arc<FaultStore>, with_velo: bool) -> Hub {
    let mut builder = builder().store(store.clone()).extension(Ledger::default());
    if with_velo {
        builder = builder.velo(velo_builder());
    }
    builder.serve().await.unwrap()
}

#[tokio::test]
async fn registry_write_failure_restores_prior_registration_credential() {
    let store = Arc::new(FaultStore::default());
    let hub = start_custom(&store, false).await;
    let peer = make_peer();
    let credential = register_plain(&hub, &peer, None).await.credential;

    store.fault_next_put(PutFault::Fail);
    let failed = post_register(&hub, &request(&peer), Some(&credential)).await;
    assert_eq!(failed.status(), 500);
    assert!(store.contains(peer.instance_id()));

    assert_eq!(
        unregister_status(&hub, peer.instance_id(), Some(&credential)).await,
        204
    );
}

#[tokio::test]
async fn custom_registry_http_removal_revokes_registration_authority() {
    let store = Arc::new(FaultStore::default());
    let hub = start_custom(&store, false).await;
    let peer = make_peer();
    let credential = register_plain(&hub, &peer, None).await.credential;
    assert_eq!(
        unregister_status(&hub, peer.instance_id(), Some(&credential)).await,
        204
    );
    assert_eq!(
        post_register(&hub, &request(&peer), None).await.status(),
        200
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn authenticated_heartbeat_cannot_touch_a_replacement_incarnation() {
    let store = Arc::new(FaultStore::default());
    let hub = start_custom(&store, false).await;
    let peer = make_peer();
    let stale = register_plain(&hub, &peer, None).await.credential;
    let gate = store.gate_next_touch();

    let heartbeat = tokio::spawn({
        let hub = hub.clone();
        let stale = stale.clone();
        let owner = peer.instance_id();
        async move { heartbeat_status(&hub, owner, Some(&stale)).await }
    });
    gate.entered().await;
    let current = register_plain(&hub, &peer, Some(&stale)).await.credential;
    gate.release();

    assert_eq!(
        heartbeat.await.unwrap(),
        409,
        "the stale authorized heartbeat must fail its conditional touch"
    );
    assert_eq!(
        unregister_status(&hub, peer.instance_id(), Some(&current)).await,
        204
    );
}

#[tokio::test]
async fn preloaded_registry_entry_cannot_be_claimed_without_credential() {
    let store = Arc::new(FaultStore::default());
    let peer = make_peer();
    store
        .put(RegistrationRecord::bare(peer.clone()), None)
        .await
        .unwrap();
    let hub = start_custom(&store, false).await;

    let takeover = post_register(&hub, &request(&peer), None).await;
    assert_eq!(takeover.status(), 401);
    assert_eq!(
        store
            .discover_by_instance_id(peer.instance_id())
            .await
            .unwrap()
            .worker_id(),
        peer.worker_id()
    );
}

#[tokio::test]
async fn hub_self_entry_cannot_be_claimed_as_a_client_registration() {
    let store = Arc::new(FaultStore::default());
    let hub = start_custom(&store, true).await;
    let hub_peer = store.list().into_iter().next().unwrap().record.peer;

    let takeover = post_register(&hub, &request(&hub_peer), None).await;
    assert_eq!(takeover.status(), 401);
    let peers: Vec<PeerInfo> = store
        .list()
        .into_iter()
        .map(|stored| stored.record.peer)
        .collect();
    assert_eq!(peers, vec![hub_peer]);
}

#[tokio::test]
async fn failed_server_build_does_not_consume_custom_registry_removal_hook() {
    let store = Arc::new(FaultStore::default());
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut config = config();
    config.server.public_port = occupied.local_addr().unwrap().port();
    let failed = Hub::builder(config)
        .store(store.clone())
        .velo(velo_builder())
        .serve()
        .await;
    assert!(
        failed.is_err(),
        "an occupied public port must reject startup"
    );
    assert!(
        store.list().is_empty(),
        "a failed startup must leave no hub entry"
    );

    let hub = start_custom(&store, false).await;
    let peer = make_peer();
    register_plain(&hub, &peer, None).await;
    store.expire(peer.instance_id());
    let mut fresh = post_register(&hub, &request(&peer), None).await;
    for _ in 0..50 {
        if fresh.status() != 409 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        fresh = post_register(&hub, &request(&peer), None).await;
    }
    assert_eq!(fresh.status(), 200);
}

/// Spawns work hung off the hub's cancellation, then fails to attach.
#[derive(Default)]
struct FailingAttach {
    stopped: Arc<AtomicBool>,
}

impl Extension for FailingAttach {
    const NAME: &'static str = "failing-attach";
    type Registration = ();
    type Staged = ();

    async fn attach(&self, ctx: &AttachContext<'_>) -> anyhow::Result<()> {
        let stopped = Arc::clone(&self.stopped);
        let cancel = ctx.cancellation().clone();
        tokio::spawn(async move {
            cancel.cancelled().await;
            stopped.store(true, Ordering::Release);
        });
        Err(anyhow::anyhow!("injected attach failure"))
    }

    async fn prepare(&self, _txn: &Txn<'_>, _payload: Option<()>) -> Result<(), Rejection> {
        Ok(())
    }

    fn commit(&self, _staged: (), _registration: &Committed<'_>) {}

    fn on_removed(&self, _removal: velo_hub::store::Removal) {}
}

#[tokio::test]
async fn failed_manager_attach_cancels_spawned_work_and_leaves_no_hub_entry() {
    let store = Arc::new(FaultStore::default());
    let stopped = Arc::new(AtomicBool::new(false));
    let failed = builder()
        .store(store.clone())
        .velo(velo_builder())
        .extension(FailingAttach {
            stopped: Arc::clone(&stopped),
        })
        .serve()
        .await;
    assert!(failed.is_err(), "an attach failure must abort startup");
    assert!(
        eventually(Duration::from_secs(1), || stopped.load(Ordering::Acquire)).await,
        "a failed startup did not cancel extension work"
    );
    assert!(
        store.list().is_empty(),
        "a failed attach must leave no hub entry"
    );
    start_custom(&store, false).await;
}

#[tokio::test]
async fn explicit_shutdown_removes_the_hub_from_a_custom_registry() {
    let store = Arc::new(FaultStore::default());
    let hub = start_custom(&store, true).await;
    assert_eq!(store.list().len(), 1);
    hub.shutdown().await.unwrap();
    assert!(store.list().is_empty());
}

#[tokio::test]
async fn dropping_the_hub_removes_it_from_a_custom_registry() {
    let store = Arc::new(FaultStore::default());
    let hub = start_custom(&store, true).await;
    assert_eq!(store.list().len(), 1);
    drop(hub);
    assert!(
        eventually(Duration::from_secs(1), || store.list().is_empty()).await,
        "dropping the hub did not remove its entry"
    );
}

// ---- the indexer's lifecycle, through the Ledger toy ---------------------

struct Fixture {
    store: Arc<FaultStore>,
    hub: Hub,
    ledger: Arc<Ledger>,
    client: velo_hub::velo::Velo,
}

async fn ledger_fixture() -> Fixture {
    let store = Arc::new(FaultStore::default());
    let hub = start_custom(&store, true).await;
    let ledger = hub.extension::<Ledger>().unwrap();
    let client = new_velo().await;
    Fixture {
        store,
        hub,
        ledger,
        client,
    }
}

fn invalid_address(peer: &PeerInfo) -> PeerInfo {
    PeerInfo::new(
        peer.instance_id(),
        WorkerAddress::from_encoded(b"invalid-velo-address".to_vec()),
    )
}

/// Same instance, a different address: the map gains an entry.
fn replacement_peer(peer: &PeerInfo) -> PeerInfo {
    let mut address: HashMap<String, Vec<u8>> =
        rmp_serde::from_slice(peer.worker_address().as_bytes()).unwrap();
    address.insert(
        "registration-lifecycle-test".to_string(),
        b"replacement".to_vec(),
    );
    PeerInfo::new(
        peer.instance_id(),
        WorkerAddress::from_encoded(rmp_serde::to_vec(&address).unwrap()),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn indexer_omission_revokes_old_authority_before_registry_write() {
    let f = ledger_fixture().await;
    let owner = f.client.instance_id();
    let initial = register(
        &f.hub,
        &request_with(&f.client.peer_info(), &[ledger_opt_in()]),
        None,
    )
    .await;
    let old = initial.credential;
    assert_eq!(publish(&f.hub, owner, &old, "k", 7).await, 204);

    let gate = f.store.gate_next_put();
    let replacement = tokio::spawn({
        let hub = f.hub.clone();
        let body = request(&f.client.peer_info());
        let old = old.clone();
        async move { post_register(&hub, &body, Some(&old)).await.status() }
    });
    gate.entered().await;

    let stale_publish = publish(&f.hub, owner, &old, "k", 8).await;
    let stale_invalidate = invalidate(&f.hub, owner, &old, "k", 7).await;
    let staged_query = query(&f.hub, owner, "k").await;

    gate.release();
    assert_eq!(replacement.await.unwrap(), 200);
    assert!(
        !stale_publish.is_success(),
        "the old credential could publish while the store write was pending"
    );
    assert!(
        stale_invalidate.is_err(),
        "the old credential could invalidate while the store write was pending"
    );
    assert_eq!(
        staged_query, None,
        "a staged omission remained query-visible"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn indexer_omission_stays_revoked_between_global_commit_and_finalization() {
    let f = ledger_fixture().await;
    let owner = f.client.instance_id();
    let old = register(
        &f.hub,
        &request_with(&f.client.peer_info(), &[ledger_opt_in()]),
        None,
    )
    .await
    .credential;
    assert_eq!(publish(&f.hub, owner, &old, "k", 7).await, 204);

    let gate = f.ledger.gate_next_registered();
    let replacement = tokio::spawn({
        let hub = f.hub.clone();
        let body = request(&f.client.peer_info());
        let old = old.clone();
        async move { post_register(&hub, &body, Some(&old)).await.status() }
    });
    gate.entered().await;

    let stale_publish = publish(&f.hub, owner, &old, "k", 8).await;
    let stale_invalidate = invalidate(&f.hub, owner, &old, "k", 7).await;
    let staged_query = query(&f.hub, owner, "k").await;

    gate.release();
    assert_eq!(replacement.await.unwrap(), 200);
    assert!(
        !stale_publish.is_success(),
        "the old credential revived after the credential commit"
    );
    assert!(
        stale_invalidate.is_err(),
        "the old credential invalidated during finalization"
    );
    assert_eq!(staged_query, None);
    assert!(
        !publish(&f.hub, owner, &old, "k", 9).await.is_success(),
        "the omitted authority returned after finalization"
    );
}

/// Changed from kvbm: the invalid replacement address answered 500 there,
/// after a store write that was then rolled back by replay. Here the hub
/// rejects an address its velo cannot decode before the lock (400), so the
/// prior registration is never touched. Everything else is kept.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn custom_registry_expiry_revokes_indexer_bundle_authority() {
    let f = ledger_fixture().await;
    let owner = f.client.instance_id();
    let body = request_with(&f.client.peer_info(), &[ledger_opt_in()]);
    let credential = register(&f.hub, &body, None).await.credential;
    assert_eq!(publish(&f.hub, owner, &credential, "k", 1).await, 204);

    let mut invalid = body.clone();
    invalid.peer = invalid_address(&f.client.peer_info());
    let rejected = post_register(&f.hub, &invalid, Some(&credential)).await;
    assert_eq!(rejected.status(), 400);
    assert_eq!(
        publish(&f.hub, owner, &credential, "k", 2).await,
        204,
        "a failed re-registration must leave the prior credential working"
    );

    f.store.expire(owner);
    assert!(
        eventually(Duration::from_secs(1), || !f.ledger.has_owner(owner)).await,
        "expiry must reach the ledger"
    );
    assert!(
        !publish(&f.hub, owner, &credential, "k", 3)
            .await
            .is_success(),
        "native expiry must revoke the owner's publish authority"
    );
    let mut fresh = post_register(&f.hub, &body, None).await;
    for _ in 0..50 {
        if fresh.status() != 409 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        fresh = post_register(&f.hub, &body, None).await;
    }
    assert_eq!(
        fresh.status(),
        200,
        "native expiry must revoke registration authority"
    );
}

/// Changed from kvbm: there the failed re-registration replayed the prior
/// one as a new store write, so `restored_incarnation != initial`. Here the
/// failure happens before any write, so the incarnation is unchanged.
/// Every other assertion is kept: metadata, credential, epoch, the live
/// entry, and the retirement all survive.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_indexer_omission_restores_live_tombstone_credential_and_epoch() {
    let f = ledger_fixture().await;
    let owner = f.client.instance_id();
    let body = request_with(&f.client.peer_info(), &[ledger_opt_in()]);
    let initial = register(&f.hub, &body, None).await;
    let credential = initial.credential;
    let epoch = initial.epoch;
    let initial_incarnation = f.store.current_incarnation(owner).unwrap();

    assert_eq!(publish(&f.hub, owner, &credential, "retired", 7).await, 204);
    assert_eq!(
        invalidate(&f.hub, owner, &credential, "retired", 7).await,
        Ok(true)
    );
    assert_eq!(publish(&f.hub, owner, &credential, "live", 9).await, 204);

    let mut invalid = request(&f.client.peer_info());
    invalid.peer = invalid_address(&f.client.peer_info());
    let rejected = post_register(&f.hub, &invalid, Some(&credential)).await;
    assert_eq!(rejected.status(), 400);

    assert_eq!(
        f.store.current_incarnation(owner).unwrap(),
        initial_incarnation,
        "no write happened, so the incarnation is the original"
    );
    assert_eq!(
        f.store
            .discover_by_instance_id(owner)
            .await
            .unwrap()
            .address_checksum(),
        body.peer.address_checksum(),
        "the prior peer metadata must remain"
    );
    assert_eq!(
        heartbeat_status(&f.hub, owner, Some(&credential)).await,
        200,
        "prior authority must remain"
    );

    let hit = query(&f.hub, owner, "live")
        .await
        .expect("the prior live entry must remain");
    assert_eq!(hit.generation, 9);
    assert_eq!(hit.epoch, epoch, "the prior epoch must remain");
    assert!(
        !publish(&f.hub, owner, &credential, "retired", 7)
            .await
            .is_success(),
        "the prior retirement must remain"
    );
    assert_eq!(query(&f.hub, owner, "live").await.unwrap().generation, 9);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn successful_reregistration_starts_a_fresh_bundle_generation_domain() {
    let f = ledger_fixture().await;
    let owner = f.client.instance_id();
    let body = request_with(&f.client.peer_info(), &[ledger_opt_in()]);
    let initial = register(&f.hub, &body, None).await;
    let initial_incarnation = f.store.current_incarnation(owner).unwrap();
    assert_eq!(
        publish(&f.hub, owner, &initial.credential, "k", 9).await,
        204
    );
    assert_eq!(
        query(&f.hub, owner, "k").await.unwrap().epoch,
        initial.epoch
    );

    let mut replacement_body = body.clone();
    replacement_body.peer = replacement_peer(&body.peer);
    assert_ne!(
        replacement_body.peer.address_checksum(),
        body.peer.address_checksum(),
        "the test must use a replacement address as well as a new incarnation"
    );
    let replacement = register(&f.hub, &replacement_body, Some(&initial.credential)).await;
    assert_ne!(initial.epoch, replacement.epoch);
    assert_ne!(
        f.store.current_incarnation(owner).unwrap(),
        initial_incarnation
    );
    assert_eq!(
        query(&f.hub, owner, "k").await,
        None,
        "the replacement must not inherit the prior entries"
    );

    assert_eq!(
        publish(&f.hub, owner, &replacement.credential, "k", 1).await,
        204,
        "the replacement may restart at generation one"
    );
    let hit = query(&f.hub, owner, "k").await.unwrap();
    assert_eq!(hit.generation, 1);
    assert_eq!(hit.epoch, replacement.epoch);
}
