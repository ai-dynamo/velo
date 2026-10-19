// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The registration transaction under failure: what each failure leaves
//! behind. These are new in velo-hub; kvbm had no deadline, no
//! indeterminate write, and no hub-owned transaction task.
//!
//! The rule every test here checks from a different side: a failure
//! *before* the store write leaves the prior registration exactly as it
//! was; a failure *after* it removes the registration entirely (the new one
//! and the prior one), and the registrant registers again.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use velo_hub::protocol::{CREDENTIAL_HEADER, ErrorCode, paths, peers_by_instance};
use velo_hub::store::testing::{FaultStore, PutFault};
use velo_hub::store::{Continuity, HubStore};
use velo_hub::velo::{InstanceId, PeerInfo};
use velo_hub::{AuthorityError, Extension, Hub, Rejection};

struct Fixture {
    store: Arc<FaultStore>,
    hub: Hub,
    ledger: Arc<Ledger>,
}

async fn fixture(configure: impl FnOnce(&mut velo_hub::HubConfig), with_velo: bool) -> Fixture {
    let store = Arc::new(FaultStore::default());
    let mut config = config();
    configure(&mut config);
    let mut builder = Hub::builder(config)
        .store(store.clone())
        .extension(Ledger::default())
        .extension(Role::default());
    if with_velo {
        builder = builder.velo(velo_builder());
    }
    let hub = builder.serve().await.unwrap();
    let ledger = hub.extension::<Ledger>().unwrap();
    Fixture { store, hub, ledger }
}

async fn plain() -> Fixture {
    fixture(|_| {}, false).await
}

/// Register `peer` into the ledger and publish one entry.
async fn seeded(f: &Fixture, peer: &PeerInfo) -> velo_hub::protocol::RegisterResponse {
    let response = register(&f.hub, &request_with(peer, &[ledger_opt_in()]), None).await;
    assert_eq!(
        publish(&f.hub, peer.instance_id(), &response.credential, "k", 1).await,
        204
    );
    response
}

/// The prior registration is exactly as it was.
async fn assert_prior_intact(
    f: &Fixture,
    peer: &PeerInfo,
    prior: &velo_hub::protocol::RegisterResponse,
    incarnation: velo_hub::store::Incarnation,
) {
    let owner = peer.instance_id();
    assert_eq!(
        f.store.current_incarnation(owner),
        Some(incarnation),
        "no write"
    );
    assert_eq!(
        heartbeat_status(&f.hub, owner, Some(&prior.credential)).await,
        200,
        "credential still works"
    );
    let hit = query(&f.hub, owner, "k")
        .await
        .expect("entry visible again");
    assert_eq!(hit.epoch, prior.epoch);
}

/// The registration is gone, both the attempt and the prior one.
async fn assert_failed_closed(
    f: &Fixture,
    peer: &PeerInfo,
    prior: &velo_hub::protocol::Credential,
) {
    let owner = peer.instance_id();
    assert!(!f.store.contains(owner), "no entry");
    assert!(!f.ledger.has_owner(owner), "on_removed reached the ledger");
    assert_eq!(
        heartbeat_status(&f.hub, owner, Some(prior)).await,
        404,
        "no registration"
    );
}

// ---- before the lock ----------------------------------------------------

#[tokio::test]
async fn malformed_payload_is_a_bad_request() {
    let f = plain().await;
    let peer = make_peer();
    let response = post_register(
        &f.hub,
        &request_with(&peer, &[("role", serde_json::json!({"role": "neither"}))]),
        None,
    )
    .await;
    assert_eq!(response.status(), 400);
    assert!(!f.store.contains(peer.instance_id()));
}

struct NeedsLedger;

impl Extension for NeedsLedger {
    const NAME: &'static str = "needs-ledger";
    type Registration = ();
    type Staged = ();

    fn registration_requires() -> &'static [&'static str] {
        &["ledger"]
    }

    fn admit(
        &self,
        view: &velo_hub::RegistrationView<'_>,
        _payload: Option<&()>,
    ) -> Result<(), Rejection> {
        if view.peer.worker_address().as_bytes() == b"refuse-me" {
            return Err(Rejection::bad_request("refused at admission"));
        }
        Ok(())
    }

    async fn prepare(
        &self,
        _txn: &velo_hub::Txn<'_>,
        _payload: Option<()>,
    ) -> Result<(), Rejection> {
        Ok(())
    }

    fn commit(&self, _staged: (), _registration: &velo_hub::Committed<'_>) {}

    fn on_removed(&self, _removal: velo_hub::store::Removal) {}
}

#[tokio::test]
async fn registration_requires_and_admit_reject_before_anything_is_written() {
    let hub = builder()
        .extension(Ledger::default())
        .extension(NeedsLedger)
        .serve()
        .await
        .unwrap();
    let peer = make_peer();
    let alone = post_register(
        &hub,
        &request_with(&peer, &[("needs-ledger", serde_json::Value::Null)]),
        None,
    )
    .await;
    assert_eq!(
        alone.status(),
        400,
        "opting in without its registration requirement"
    );
    let both = post_register(
        &hub,
        &request_with(
            &peer,
            &[("needs-ledger", serde_json::Value::Null), ledger_opt_in()],
        ),
        None,
    )
    .await;
    assert_eq!(both.status(), 200);

    let refused = PeerInfo::new(
        InstanceId::new_v4(),
        velo_hub::velo::WorkerAddress::from_encoded(b"refuse-me".to_vec()),
    );
    let response = post_register(&hub, &request(&refused), None).await;
    assert_eq!(
        response.status(),
        400,
        "admit sees every registration, opted in or not"
    );
    assert!(!hub.store().contains(refused.instance_id()));
}

// ---- before the write: the prior registration is untouched --------------

#[tokio::test]
async fn prepare_failure_leaves_prior_untouched() {
    let f = plain().await;
    let peer = make_peer();
    let prior = seeded(&f, &peer).await;
    let incarnation = f.store.current_incarnation(peer.instance_id()).unwrap();

    f.ledger
        .reject_next_prepare(Rejection::conflict("injected"));
    let response = post_register(
        &f.hub,
        &request_with(&peer, &[ledger_opt_in()]),
        Some(&prior.credential),
    )
    .await;
    assert_eq!(response.status(), 409);
    assert_eq!(error_code(response).await, ErrorCode::Conflict);
    assert_prior_intact(&f, &peer, &prior, incarnation).await;
    assert_eq!(f.ledger.revealed(), 1, "the abort undid the hide");
}

/// `prepare` hides, then never returns. The deadline drops its future,
/// which drops the guard the hide created, so the prior entry is visible
/// again; and the lock is released, so the next registration proceeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepare_deadline_aborts_and_releases_the_lock() {
    let f = fixture(
        |config| config.server.step_timeout = Duration::from_millis(200),
        false,
    )
    .await;
    let peer = make_peer();
    let prior = seeded(&f, &peer).await;
    let incarnation = f.store.current_incarnation(peer.instance_id()).unwrap();

    f.ledger.hang_next_prepare();
    let response = post_register(
        &f.hub,
        &request_with(&peer, &[ledger_opt_in()]),
        Some(&prior.credential),
    )
    .await;
    assert_eq!(response.status(), 503);
    assert_eq!(error_code(response).await, ErrorCode::Unavailable);
    assert_prior_intact(&f, &peer, &prior, incarnation).await;

    let started = std::time::Instant::now();
    register_plain(&f.hub, &make_peer(), None).await;
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "the lock was released"
    );
}

#[tokio::test]
async fn indeterminate_put_that_did_not_apply_aborts_and_leaves_prior_untouched() {
    let f = plain().await;
    let peer = make_peer();
    let prior = seeded(&f, &peer).await;
    let incarnation = f.store.current_incarnation(peer.instance_id()).unwrap();

    f.store.fault_next_put(PutFault::IndeterminateNotWritten);
    let response = post_register(
        &f.hub,
        &request_with(&peer, &[ledger_opt_in()]),
        Some(&prior.credential),
    )
    .await;
    assert_eq!(response.status(), 503);
    assert_prior_intact(&f, &peer, &prior, incarnation).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn panicking_prepare_does_not_leave_the_owner_busy() {
    let f = plain().await;
    let peer = make_peer();
    let prior = seeded(&f, &peer).await;
    let incarnation = f.store.current_incarnation(peer.instance_id()).unwrap();

    f.ledger.panic_next_prepare();
    let response = post_register(
        &f.hub,
        &request_with(&peer, &[ledger_opt_in()]),
        Some(&prior.credential),
    )
    .await;
    assert_eq!(response.status(), 500);
    assert_prior_intact(&f, &peer, &prior, incarnation).await;
}

/// The store expires the prior incarnation while `prepare` is gated. The
/// removal is recorded against the transaction, not delivered into the
/// middle of it: the ledger still holds the (hidden) owner until the
/// transaction ends. The conditional put then fails, and the abort
/// delivers the removal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removal_during_prepare_cannot_interleave_with_commit() {
    let f = plain().await;
    let peer = make_peer();
    let owner = peer.instance_id();
    let prior = seeded(&f, &peer).await;

    let gate = f.ledger.gate_next_prepare();
    let attempt = tokio::spawn({
        let hub = f.hub.clone();
        let body = request_with(&peer, &[ledger_opt_in()]);
        let credential = prior.credential.clone();
        async move { post_register(&hub, &body, Some(&credential)).await }
    });
    gate.entered().await;
    f.store.expire(owner);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        f.ledger.has_owner(owner),
        "on_removed ran inside a transaction"
    );
    gate.release();

    let response = attempt.await.unwrap();
    assert_eq!(response.status(), 409);
    assert_eq!(error_code(response).await, ErrorCode::RegisterAgain);
    assert!(
        !f.ledger.has_owner(owner),
        "the abort delivered the removal"
    );
    assert_eq!(
        f.ledger.revealed(),
        0,
        "the removal was delivered before the hide was undone"
    );
    assert_eq!(
        post_register(&f.hub, &request(&peer), None).await.status(),
        200
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn authorize_is_busy_mid_registration() {
    let f = plain().await;
    let peer = make_peer();
    let prior = register_plain(&f.hub, &peer, None).await;
    let gate = f.store.gate_next_put();
    let attempt = tokio::spawn({
        let hub = f.hub.clone();
        let body = request(&peer);
        let credential = prior.credential.clone();
        async move { post_register(&hub, &body, Some(&credential)).await.status() }
    });
    gate.entered().await;
    assert!(matches!(
        f.hub
            .authority()
            .authorize(peer.instance_id(), Some(&prior.credential)),
        Err(AuthorityError::Busy { .. })
    ));
    assert_eq!(
        heartbeat_status(&f.hub, peer.instance_id(), Some(&prior.credential)).await,
        409
    );
    gate.release();
    assert_eq!(attempt.await.unwrap(), 200);
}

/// The client closes its socket while the transaction waits on the store.
/// hyper drops the handler, but the transaction is the hub's own task, so
/// it finishes. (A reqwest timeout is not enough to show this: hyper keeps
/// polling the handler until it writes, so the socket must close.) It ends: the owner ends Active with a credential nobody holds, not
/// Busy. A fresh registration is refused as unauthorized until the entry
/// expires, then accepted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_disconnect_mid_transaction_does_not_leave_the_owner_busy() {
    let store = Arc::new(FaultStore::new(
        velo_hub::store::InMemoryStore::builder()
            .ttl(Duration::from_millis(300))
            .prune_interval(Duration::from_secs(3600))
            .build(),
    ));
    let hub = builder().store(store.clone()).serve().await.unwrap();
    let peer = make_peer();
    let gate = store.gate_next_put();
    let attempt = tokio::spawn({
        let hub = hub.clone();
        let body = serde_json::to_value(request(&peer)).unwrap();
        async move { send_and_abandon(&hub, "POST", paths::INSTANCES, None, Some(&body)).await }
    });
    gate.entered().await;
    drop(attempt.await.unwrap());
    tokio::time::sleep(Duration::from_millis(100)).await;
    gate.release();

    let owner = peer.instance_id();
    assert!(
        eventually(Duration::from_secs(1), || matches!(
            hub.authority().authorize(owner, None),
            Err(AuthorityError::Unauthorized { .. })
        ))
        .await,
        "the abandoned transaction must finish, leaving the owner Active, not Busy"
    );
    assert_eq!(
        post_register(&hub, &request(&peer), None).await.status(),
        401
    );

    tokio::time::sleep(Duration::from_millis(400)).await;
    store.inner().prune_stale();
    assert!(
        eventually(Duration::from_secs(1), || hub
            .authority()
            .authorize(owner, None)
            .is_err_and(|error| matches!(
                error,
                AuthorityError::NotFound { .. }
            )))
        .await
    );
    assert_eq!(
        post_register(&hub, &request(&peer), None).await.status(),
        200
    );
}

// ---- after the write: one path, fail closed ------------------------------

#[tokio::test]
async fn indeterminate_put_that_applied_fails_closed() {
    let f = plain().await;
    let peer = make_peer();
    let prior = seeded(&f, &peer).await;

    f.store.fault_next_put(PutFault::IndeterminateWritten);
    let response = post_register(
        &f.hub,
        &request_with(&peer, &[ledger_opt_in()]),
        Some(&prior.credential),
    )
    .await;
    assert_eq!(response.status(), 409);
    assert_eq!(error_code(response).await, ErrorCode::RegisterAgain);
    assert_failed_closed(&f, &peer, &prior.credential).await;
    assert_eq!(
        post_register(&f.hub, &request(&peer), None).await.status(),
        200
    );
}

/// Neither outcome can be learned. The hub drops the prior registration's
/// extension state and reserves whatever the store holds until it expires.
#[tokio::test]
async fn indeterminate_put_that_cannot_be_resolved_drops_and_reserves() {
    let f = plain().await;
    let peer = make_peer();
    let owner = peer.instance_id();
    let prior = seeded(&f, &peer).await;

    f.store.fault_next_put(PutFault::IndeterminateWritten);
    f.store.fail_next_resolve();
    let response = post_register(
        &f.hub,
        &request_with(&peer, &[ledger_opt_in()]),
        Some(&prior.credential),
    )
    .await;
    assert_eq!(response.status(), 503);
    assert!(!f.ledger.has_owner(owner));
    assert!(f.store.contains(owner), "the written entry is still there");
    assert_eq!(
        heartbeat_status(&f.hub, owner, Some(&prior.credential)).await,
        401,
        "reserved"
    );
    assert_eq!(
        post_register(&f.hub, &request(&peer), None).await.status(),
        401
    );

    f.store.expire(owner);
    assert_eq!(
        post_register(&f.hub, &request(&peer), None).await.status(),
        200
    );
}

/// The address decodes, but no hub transport can use it, so velo's
/// `register_peer` fails after the write. The hub takes the one post-write
/// path and leaves no velo peer behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn post_write_register_peer_failure_fails_closed_and_leaves_no_velo_peer() {
    let f = fixture(|_| {}, true).await;
    let client = new_velo().await;
    let owner = client.instance_id();
    let prior = seeded(&f, &client.peer_info()).await;
    let velo = f.hub.velo().unwrap();
    assert!(
        velo.primary_transport_key(owner).is_some(),
        "the hub's velo knows the peer"
    );

    let unreachable = PeerInfo::new(owner, unreachable_peer().worker_address().clone());
    let response = post_register(
        &f.hub,
        &request_with(&unreachable, &[ledger_opt_in()]),
        Some(&prior.credential),
    )
    .await;
    assert_eq!(response.status(), 409);
    assert_eq!(error_code(response).await, ErrorCode::RegisterAgain);
    assert_failed_closed(&f, &client.peer_info(), &prior.credential).await;
    assert_eq!(
        f.ledger.revealed(),
        0,
        "on_removed ran before the hide was undone"
    );
    assert!(
        velo.primary_transport_key(owner).is_none(),
        "no velo peer is left"
    );

    assert_eq!(
        post_register(&f.hub, &request(&client.peer_info()), None)
            .await
            .status(),
        200
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn panicking_commit_fails_closed() {
    let f = plain().await;
    let peer = make_peer();
    let owner = peer.instance_id();
    let prior = seeded(&f, &peer).await;

    f.ledger.panic_next_commit();
    let response = post_register(
        &f.hub,
        &request_with(&peer, &[ledger_opt_in()]),
        Some(&prior.credential),
    )
    .await;
    assert_eq!(response.status(), 500);
    assert!(
        eventually(Duration::from_secs(1), || !f.store.contains(owner)
            && !f.ledger.has_owner(owner))
        .await,
        "the abandoned write is retired and its removal delivered"
    );
    assert!(
        eventually(Duration::from_secs(1), || matches!(
            f.hub.authority().authorize(owner, None),
            Err(AuthorityError::NotFound { .. })
        ))
        .await
    );
    assert_eq!(
        post_register(&f.hub, &request(&peer), None).await.status(),
        200
    );
}

/// DELETE delivers its own removal; the copy the hook queued arrives after
/// the owner registered again. That late copy must not forget the
/// replacement's velo peer, and the ledger must ignore it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delayed_removal_of_an_old_incarnation_does_not_unregister_the_replacement() {
    let f = fixture(|_| {}, true).await;
    let client = new_velo().await;
    let owner = client.instance_id();
    let body = request_with(&client.peer_info(), &[ledger_opt_in()]);
    let first = register(&f.hub, &body, None).await;

    // DELETE holds the lock while its remove is gated; the re-registration
    // queues on the lock first, so it runs before the lifecycle task's
    // late copy of the removal (the lock is FIFO).
    let gate = f.store.gate_next_remove();
    let delete = tokio::spawn({
        let hub = f.hub.clone();
        let credential = first.credential.clone();
        async move { unregister_status(&hub, owner, Some(&credential)).await }
    });
    gate.entered().await;
    let again = tokio::spawn({
        let hub = f.hub.clone();
        let body = body.clone();
        async move { post_register(&hub, &body, None).await.status() }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    gate.release();
    assert_eq!(delete.await.unwrap(), 204);
    assert_eq!(again.await.unwrap(), 200);

    tokio::time::sleep(Duration::from_millis(200)).await;
    let velo = f.hub.velo().unwrap();
    assert!(
        velo.primary_transport_key(owner).is_some(),
        "the late removal of the first incarnation disconnected the replacement"
    );
    assert!(
        f.ledger.has_owner(owner),
        "the late removal reached the replacement's ledger state"
    );
    assert!(
        f.ledger.late_removals(owner) >= 1,
        "the queued copy of the first removal must arrive after the replacement, or this test proves nothing"
    );
}

// ---- store continuity --------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn continuity_lost_closes_authorization_and_keeps_discovery() {
    let f = plain().await;
    let peer = make_peer();
    let owner = peer.instance_id();
    let prior = seeded(&f, &peer).await;

    f.store.set_continuity(Continuity::Lost);
    assert!(eventually(Duration::from_secs(1), || !f.hub.authority().is_live()).await);
    assert_eq!(
        heartbeat_status(&f.hub, owner, Some(&prior.credential)).await,
        503
    );
    assert_eq!(
        publish(&f.hub, owner, &prior.credential, "k", 2).await,
        503,
        "extension routes fail closed too"
    );
    assert_eq!(
        post_register(&f.hub, &request(&make_peer()), None)
            .await
            .status(),
        503
    );
    assert_eq!(
        unregister_status(&f.hub, owner, Some(&prior.credential)).await,
        503
    );
    let lookup = http()
        .get(public(&f.hub, &peers_by_instance(owner)))
        .send()
        .await
        .unwrap();
    assert_eq!(
        lookup.status(),
        200,
        "discovery reads continue from the cache"
    );

    f.store.set_continuity(Continuity::Live);
    assert!(eventually(Duration::from_secs(1), || f.hub.authority().is_live()).await);
    assert_eq!(
        heartbeat_status(&f.hub, owner, Some(&prior.credential)).await,
        200
    );
}

// ---- the extension fence: a lease's epoch against the committed one ------

/// A publish is authorized with the old lease, then stalls before it takes
/// the ledger lock. A re-registration commits a new epoch meanwhile. The
/// stalled publish must not land: the ledger checks the lease's epoch
/// against its own committed epoch under its lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lease_taken_before_a_reregistration_cannot_write_after_it() {
    let f = plain().await;
    let peer = make_peer();
    let owner = peer.instance_id();
    let prior = seeded(&f, &peer).await;

    let gate = f.ledger.gate_next_publish();
    let stalled = tokio::spawn({
        let hub = f.hub.clone();
        let credential = prior.credential.clone();
        async move { publish(&hub, owner, &credential, "late", 5).await }
    });
    gate.entered().await;
    let replacement = register(
        &f.hub,
        &request_with(&peer, &[ledger_opt_in()]),
        Some(&prior.credential),
    )
    .await;
    gate.release();

    assert_eq!(
        stalled.await.unwrap(),
        409,
        "a stale lease wrote across a re-registration"
    );
    assert_eq!(query(&f.hub, owner, "late").await, None);
    assert_eq!(
        publish(&f.hub, owner, &replacement.credential, "late", 5).await,
        204
    );
}

/// A malformed credential header is a 400, not an anonymous request.
#[tokio::test]
async fn malformed_credential_header_is_a_bad_request() {
    let f = plain().await;
    let response = http()
        .post(control(&f.hub, paths::INSTANCES))
        .header(CREDENTIAL_HEADER, "not-a-credential")
        .json(&request(&make_peer()))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
}

/// Commits before the ledger, and can panic there.
#[derive(Default)]
struct PanicsInCommit {
    armed: std::sync::atomic::AtomicBool,
}

impl Extension for PanicsInCommit {
    const NAME: &'static str = "panics-in-commit";
    type Registration = ();
    type Staged = ();

    async fn prepare(
        &self,
        _txn: &velo_hub::Txn<'_>,
        _payload: Option<()>,
    ) -> Result<(), Rejection> {
        Ok(())
    }

    fn commit(&self, _staged: (), _registration: &velo_hub::Committed<'_>) {
        if self.armed.swap(false, std::sync::atomic::Ordering::AcqRel) {
            panic!("injected commit panic");
        }
    }

    fn on_removed(&self, _removal: velo_hub::store::Removal) {}
}

/// A commit panics while the ledger's staged value (which hid the prior
/// entries) is not yet committed. The hub delivers the removal before it
/// drops that value, so the prior entries never become visible again, and
/// by the time the 500 arrives the ledger has dropped the owner.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_panic_after_the_write_never_shows_the_prior_state_again() {
    let store = Arc::new(FaultStore::default());
    let hub = builder()
        .store(store.clone())
        .extension(PanicsInCommit::default())
        .extension(Ledger::default())
        .serve()
        .await
        .unwrap();
    let ledger = hub.extension::<Ledger>().unwrap();
    let peer = make_peer();
    let owner = peer.instance_id();
    let prior = register(&hub, &request_with(&peer, &[ledger_opt_in()]), None).await;
    assert_eq!(publish(&hub, owner, &prior.credential, "k", 1).await, 204);

    hub.extension::<PanicsInCommit>()
        .unwrap()
        .armed
        .store(true, std::sync::atomic::Ordering::Release);
    // Hold the queued retirement at its store remove, so only the hub's own
    // settling can have removed the owner from the ledger.
    let retire = store.gate_next_remove();
    let response = post_register(
        &hub,
        &request_with(&peer, &[ledger_opt_in()]),
        Some(&prior.credential),
    )
    .await;
    assert_eq!(response.status(), 500);
    assert!(
        !ledger.has_owner(owner),
        "the removal was delivered before the response"
    );
    assert_eq!(query(&hub, owner, "k").await, None);
    assert_eq!(
        ledger.revealed(),
        0,
        "on_removed ran before the hide was undone"
    );
    retire.release();
    assert!(
        eventually(Duration::from_secs(1), || !store.contains(owner)).await,
        "the written entry is retired"
    );
}

/// The client gives up while DELETE waits on the store. Whatever the HTTP
/// stack does with the handler, the unregistration must settle: the owner
/// ends unregistered, not Busy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_disconnect_mid_unregister_does_not_leave_the_owner_busy() {
    let store = Arc::new(FaultStore::default());
    let hub = builder().store(store.clone()).serve().await.unwrap();
    let peer = make_peer();
    let owner = peer.instance_id();
    let credential = register_plain(&hub, &peer, None).await.credential;
    let gate = store.gate_next_remove();
    let attempt = tokio::spawn({
        let hub = hub.clone();
        let credential = credential.clone();
        async move {
            let path = velo_hub::protocol::instance_by_id(owner);
            send_and_abandon(&hub, "DELETE", &path, Some(&credential), None).await
        }
    });
    gate.entered().await;
    drop(attempt.await.unwrap());
    tokio::time::sleep(Duration::from_millis(100)).await;
    gate.release();
    assert!(
        eventually(Duration::from_secs(2), || matches!(
            hub.authority().authorize(owner, Some(&credential)),
            Err(AuthorityError::NotFound { .. })
        ))
        .await,
        "the unregistration must finish, not leave the owner Busy: {:?}",
        hub.authority().authorize(owner, Some(&credential))
    );
    assert!(!store.contains(owner));
}

/// Expires the owner's new entry from inside `commit`, then panics: a
/// removal that lands after the write, then a dead transaction.
struct ExpiresThenPanics {
    store: Arc<FaultStore>,
    armed: std::sync::atomic::AtomicBool,
}

impl Extension for ExpiresThenPanics {
    const NAME: &'static str = "expires-then-panics";
    type Registration = ();
    type Staged = ();

    async fn prepare(
        &self,
        _txn: &velo_hub::Txn<'_>,
        _payload: Option<()>,
    ) -> Result<(), Rejection> {
        Ok(())
    }

    fn commit(&self, _staged: (), registration: &velo_hub::Committed<'_>) {
        if self.armed.swap(false, std::sync::atomic::Ordering::AcqRel) {
            self.store.expire(registration.owner);
            panic!("injected commit panic after a removal");
        }
    }

    fn on_removed(&self, _removal: velo_hub::store::Removal) {}
}

/// The settling of a dead transaction must not reserve an entry that is
/// already gone: nothing would ever clear that reservation, and the owner
/// could not register again until the hub restarts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dead_transaction_whose_entry_was_removed_does_not_lock_the_owner_out() {
    let store = Arc::new(FaultStore::default());
    let hub = builder()
        .store(store.clone())
        .extension(ExpiresThenPanics {
            store: store.clone(),
            armed: std::sync::atomic::AtomicBool::new(true),
        })
        .serve()
        .await
        .unwrap();
    let peer = make_peer();
    assert_eq!(
        post_register(&hub, &request(&peer), None).await.status(),
        500
    );
    assert!(!store.contains(peer.instance_id()));
    let mut fresh = post_register(&hub, &request(&peer), None).await;
    for _ in 0..50 {
        if fresh.status() != 409 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        fresh = post_register(&hub, &request(&peer), None).await;
    }
    assert_eq!(
        fresh.status(),
        200,
        "the owner was left reserved with no entry to clear it"
    );
}

/// A store remove that never returns cannot hold the registration lock
/// forever: removes under the lock run under `step_timeout_ms`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hung_store_remove_does_not_hold_the_lock_forever() {
    let store = Arc::new(FaultStore::default());
    let mut config = config();
    config.server.step_timeout = Duration::from_millis(200);
    config.server.lock_timeout = Duration::from_secs(2);
    let hub = Hub::builder(config)
        .store(store.clone())
        .serve()
        .await
        .unwrap();
    let peer = make_peer();
    let credential = register_plain(&hub, &peer, None).await.credential;
    let _gate = store.gate_next_remove();
    let delete = tokio::time::timeout(
        Duration::from_secs(3),
        unregister_status(&hub, peer.instance_id(), Some(&credential)),
    )
    .await
    .expect("the DELETE hung on the remove");
    assert_eq!(delete, 503, "an unresolved remove is retryable");
    assert_eq!(
        heartbeat_status(&hub, peer.instance_id(), Some(&credential)).await,
        200,
        "the owner is restored, not left Busy"
    );
    assert_eq!(
        post_register(&hub, &request(&make_peer()), None)
            .await
            .status(),
        200,
        "the lock is free"
    );
    tokio::time::timeout(Duration::from_secs(5), hub.shutdown())
        .await
        .expect("shutdown hung")
        .unwrap();
}

// ---- paths the first review found untested -------------------------------

/// An `on_registered` that panics after the credential committed: the
/// entry is retired, its removal revokes the credential nobody received,
/// and the owner registers afresh.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn panicking_on_registered_retires_the_registration() {
    let f = plain().await;
    let peer = make_peer();
    let owner = peer.instance_id();
    f.ledger.panic_next_registered();
    let response = post_register(&f.hub, &request_with(&peer, &[ledger_opt_in()]), None).await;
    assert_eq!(response.status(), 500);
    assert!(
        eventually(Duration::from_secs(1), || !f.store.contains(owner)
            && !f.ledger.has_owner(owner))
        .await,
        "the written entry is retired"
    );
    let mut fresh = post_register(&f.hub, &request(&peer), None).await;
    for _ in 0..50 {
        if fresh.status() != 409 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        fresh = post_register(&f.hub, &request(&peer), None).await;
    }
    assert_eq!(fresh.status(), 200);
}

/// Panics in `on_removed`. Started after the ledger, so removals reach it
/// first.
struct PanicsOnRemoved;

impl Extension for PanicsOnRemoved {
    const NAME: &'static str = "panics-on-removed";
    type Registration = ();
    type Staged = ();

    async fn prepare(
        &self,
        _txn: &velo_hub::Txn<'_>,
        _payload: Option<()>,
    ) -> Result<(), Rejection> {
        Ok(())
    }

    fn commit(&self, _staged: (), _registration: &velo_hub::Committed<'_>) {}

    fn on_removed(&self, _removal: velo_hub::store::Removal) {
        panic!("injected on_removed panic");
    }
}

/// One extension's panic in `on_removed` does not stop delivery to the
/// others, the DELETE, or the lifecycle task.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_panicking_on_removed_does_not_stop_delivery() {
    let hub = builder()
        .extension(Ledger::default())
        .extension(PanicsOnRemoved)
        .serve()
        .await
        .unwrap();
    let ledger = hub.extension::<Ledger>().unwrap();
    let peer = make_peer();
    let owner = peer.instance_id();
    let credential = register(&hub, &request_with(&peer, &[ledger_opt_in()]), None)
        .await
        .credential;
    assert_eq!(unregister_status(&hub, owner, Some(&credential)).await, 204);
    assert!(!ledger.has_owner(owner));
    // The lifecycle task survived its copy of the removal too.
    let second = make_peer();
    let credential = register(&hub, &request_with(&second, &[ledger_opt_in()]), None)
        .await
        .credential;
    assert_eq!(
        unregister_status(&hub, second.instance_id(), Some(&credential)).await,
        204
    );
    assert_eq!(
        post_register(&hub, &request(&peer), None).await.status(),
        200
    );
}

/// A DELETE whose store remove fails leaves the registration as it was.
#[tokio::test]
async fn a_failed_store_remove_restores_the_registration() {
    let f = plain().await;
    let peer = make_peer();
    let credential = register_plain(&f.hub, &peer, None).await.credential;
    f.store.fail_next_remove();
    assert_eq!(
        unregister_status(&f.hub, peer.instance_id(), Some(&credential)).await,
        500
    );
    assert_eq!(
        heartbeat_status(&f.hub, peer.instance_id(), Some(&credential)).await,
        200
    );
    assert_eq!(
        unregister_status(&f.hub, peer.instance_id(), Some(&credential)).await,
        204
    );
}

/// After the write, the failed registration's entry cannot be removed: it
/// stays reserved, so nobody claims it without a credential, until it
/// leaves the store.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_entry_that_could_not_be_removed_after_a_failure_stays_reserved() {
    let f = fixture(|_| {}, true).await;
    let owner = InstanceId::new_v4();
    let unreachable = PeerInfo::new(owner, unreachable_peer().worker_address().clone());
    f.store.fail_next_remove();
    let response = post_register(&f.hub, &request(&unreachable), None).await;
    assert_eq!(response.status(), 409);
    assert!(f.store.contains(owner), "the remove failed");
    assert_eq!(
        post_register(&f.hub, &request(&unreachable), None)
            .await
            .status(),
        401,
        "reserved"
    );
    f.store.expire(owner);
    // The reservation went with the entry: the next attempt reaches velo
    // again (and fails the same way) instead of answering 401.
    assert_eq!(
        post_register(&f.hub, &request(&unreachable), None)
            .await
            .status(),
        409
    );
}

/// A registration that cannot get the lock in `lock_timeout_ms` answers 503.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_registration_waits_at_most_lock_timeout() {
    let f = fixture(
        |config| config.server.lock_timeout = Duration::from_millis(200),
        false,
    )
    .await;
    let gate = f.ledger.gate_next_prepare();
    let held = tokio::spawn({
        let hub = f.hub.clone();
        let body = request(&make_peer());
        async move { post_register(&hub, &body, None).await.status() }
    });
    gate.entered().await;
    let started = std::time::Instant::now();
    let waiting = post_register(&f.hub, &request(&make_peer()), None).await;
    assert_eq!(waiting.status(), 503);
    assert!(started.elapsed() < Duration::from_secs(2));
    gate.release();
    assert_eq!(held.await.unwrap(), 200);
}

// ---- bounds the second review found untested ------------------------------

/// An `on_registered` that never returns is dropped at `step_timeout_ms`;
/// the registration still succeeds and the lock is free.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hung_on_registered_is_dropped_at_the_deadline() {
    let f = fixture(
        |config| config.server.step_timeout = Duration::from_millis(200),
        false,
    )
    .await;
    let _gate = f.ledger.gate_next_registered();
    let registered = tokio::time::timeout(
        Duration::from_secs(3),
        post_register(
            &f.hub,
            &request_with(&make_peer(), &[ledger_opt_in()]),
            None,
        ),
    )
    .await
    .expect("a hung on_registered held the registration");
    assert_eq!(registered.status(), 200);
    let next = tokio::time::timeout(
        Duration::from_secs(3),
        post_register(&f.hub, &request(&make_peer()), None),
    )
    .await
    .expect("the lock stayed held");
    assert_eq!(next.status(), 200);
}

/// The remove after a failed write is bounded: a store that never
/// answers it does not hold the lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hung_remove_after_a_failed_write_is_bounded() {
    let f = fixture(
        |config| config.server.step_timeout = Duration::from_millis(200),
        true,
    )
    .await;
    let unreachable = PeerInfo::new(
        InstanceId::new_v4(),
        unreachable_peer().worker_address().clone(),
    );
    let _gate = f.store.gate_next_remove();
    let failed = tokio::time::timeout(
        Duration::from_secs(3),
        post_register(&f.hub, &request(&unreachable), None),
    )
    .await
    .expect("a hung remove held the registration");
    assert_eq!(failed.status(), 409);
    let next = tokio::time::timeout(
        Duration::from_secs(3),
        post_register(&f.hub, &request(&unreachable_peer()), None),
    )
    .await
    .expect("the lock stayed held");
    assert_eq!(
        next.status(),
        409,
        "the lock is free; this one fails at velo too"
    );
}

/// The lifecycle task's retirement of an abandoned write is bounded: its
/// hung remove does not hold the lock for later registrations.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hung_remove_in_a_retirement_is_bounded() {
    let f = fixture(
        |config| config.server.step_timeout = Duration::from_millis(200),
        false,
    )
    .await;
    f.ledger.panic_next_commit();
    let _gate = f.store.gate_next_remove();
    assert_eq!(
        post_register(
            &f.hub,
            &request_with(&make_peer(), &[ledger_opt_in()]),
            None
        )
        .await
        .status(),
        500
    );
    let next = tokio::time::timeout(
        Duration::from_secs(3),
        post_register(&f.hub, &request(&make_peer()), None),
    )
    .await
    .expect("the retirement held the lock");
    assert_eq!(next.status(), 200);
}

/// A DELETE that cannot get the lock in `lock_timeout_ms` answers 503.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unregistration_waits_at_most_lock_timeout() {
    let f = fixture(
        |config| config.server.lock_timeout = Duration::from_millis(200),
        false,
    )
    .await;
    let victim = make_peer();
    let credential = register_plain(&f.hub, &victim, None).await.credential;
    let gate = f.ledger.gate_next_prepare();
    let held = tokio::spawn({
        let hub = f.hub.clone();
        let body = request(&make_peer());
        async move { post_register(&hub, &body, None).await.status() }
    });
    gate.entered().await;
    let delete = tokio::time::timeout(
        Duration::from_secs(3),
        unregister_status(&f.hub, victim.instance_id(), Some(&credential)),
    )
    .await
    .expect("the DELETE waited past its lock timeout");
    assert_eq!(delete, 503);
    gate.release();
    assert_eq!(held.await.unwrap(), 200);
}

/// Starts after the ledger; its prepare passes a gate, then panics.
#[derive(Default)]
struct PanicsInPrepare {
    gate: parking_lot::Mutex<Option<Arc<velo_hub::store::testing::Gate>>>,
}

impl Extension for PanicsInPrepare {
    const NAME: &'static str = "panics-in-prepare";
    type Registration = ();
    type Staged = ();

    fn requires() -> &'static [&'static str] {
        &["ledger"]
    }

    async fn prepare(
        &self,
        _txn: &velo_hub::Txn<'_>,
        _payload: Option<()>,
    ) -> Result<(), Rejection> {
        let gate = self.gate.lock().take();
        if let Some(gate) = gate {
            gate.pass().await;
            panic!("injected prepare panic after a gate");
        }
        Ok(())
    }

    fn commit(&self, _staged: (), _registration: &velo_hub::Committed<'_>) {}

    fn on_removed(&self, _removal: velo_hub::store::Removal) {}
}

/// A panic before the write, after the store removed the prior
/// registration: settling delivers that removal before it drops the
/// ledger's staged value, so the removed entries never show again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_panic_before_the_write_delivers_a_removal_before_undoing_hides() {
    let store = Arc::new(FaultStore::default());
    let hub = builder()
        .store(store.clone())
        .extension(Ledger::default())
        .extension(PanicsInPrepare::default())
        .serve()
        .await
        .unwrap();
    let ledger = hub.extension::<Ledger>().unwrap();
    let peer = make_peer();
    let owner = peer.instance_id();
    let prior = register(&hub, &request_with(&peer, &[ledger_opt_in()]), None).await;
    assert_eq!(publish(&hub, owner, &prior.credential, "k", 1).await, 204);

    let gate = Arc::new(velo_hub::store::testing::Gate::default());
    *hub.extension::<PanicsInPrepare>().unwrap().gate.lock() = Some(Arc::clone(&gate));
    let attempt = tokio::spawn({
        let hub = hub.clone();
        let body = request_with(&peer, &[ledger_opt_in()]);
        let credential = prior.credential.clone();
        async move { post_register(&hub, &body, Some(&credential)).await.status() }
    });
    gate.entered().await;
    store.expire(owner);
    gate.release();
    assert_eq!(attempt.await.unwrap(), 500);
    assert!(!ledger.has_owner(owner));
    assert_eq!(
        ledger.revealed(),
        0,
        "the removed owner's entries showed again"
    );
}

/// An unresolved write reserves the prior entry; the lost write then lands
/// late and replaces it (a replacement is not a removal), and the
/// replacement expires. The owner must be able to register again.
#[tokio::test]
async fn a_late_landing_write_does_not_strand_a_reservation() {
    let f = plain().await;
    let peer = make_peer();
    let owner = peer.instance_id();
    let credential = register_plain(&f.hub, &peer, None).await.credential;
    let prior = f.store.current_incarnation(owner).unwrap();
    f.store.fault_next_put(PutFault::IndeterminateNotWritten);
    f.store.fail_next_resolve();
    assert_eq!(
        post_register(&f.hub, &request(&peer), Some(&credential))
            .await
            .status(),
        503
    );
    assert_eq!(
        post_register(&f.hub, &request(&peer), None).await.status(),
        401,
        "reserved"
    );
    // The lost write lands late, then expires.
    f.store
        .put(
            velo_hub::store::RegistrationRecord::bare(peer.clone()),
            Some(prior),
        )
        .await
        .unwrap();
    f.store.expire(owner);
    assert_eq!(
        post_register(&f.hub, &request(&peer), None).await.status(),
        200
    );
}
