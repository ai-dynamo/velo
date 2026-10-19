// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Ported from kvbm-hub `tests/integration.rs`: the HTTP behavior of the
//! core routes. kvbm's observed behavior is the spec; where an assertion
//! changed, the test says why. Tests of kvbm's HubClient move to the
//! client milestone; tests of kvbm's own features stay with kvbm. The
//! ledger in `agent-docs/velo-hub-m1-ledger.md` places every one.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use velo_hub::protocol::{
    ErrorCode, HeartbeatResponse, ListInstancesResponse, PeerLookupResponse, ProbeResponse,
    instance_probe, paths, peers_by_instance, peers_by_worker,
};
use velo_hub::store::InMemoryStore;
use velo_hub::velo::{InstanceId, Transport};

// ---- HTTP-direct server tests ------------------------------------------

#[tokio::test]
async fn health_check_discovery_port() {
    let hub = start().await;
    let response = http()
        .get(public(&hub, paths::HEALTH))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
}

#[tokio::test]
async fn health_check_control_port() {
    let hub = start().await;
    let response = http()
        .get(control(&hub, paths::HEALTH))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
}

#[tokio::test]
async fn register_success() {
    let hub = start().await;
    let peer = make_peer();
    let response = post_register(&hub, &request(&peer), None).await;
    assert_eq!(response.status(), 200);
    let body: velo_hub::protocol::RegisterResponse = response.json().await.unwrap();
    assert_eq!(body.instance_id, peer.instance_id());
}

#[tokio::test]
async fn active_reregister_requires_current_credential_and_rotates_it() {
    let hub = start().await;
    let peer = make_peer();
    let first = register_plain(&hub, &peer, None).await.credential;

    let unauthenticated = post_register(&hub, &request(&peer), None).await;
    assert_eq!(unauthenticated.status(), 401);

    let second = register_plain(&hub, &peer, Some(&first)).await.credential;
    assert_ne!(first, second);

    let stale = post_register(&hub, &request(&peer), Some(&first)).await;
    assert_eq!(stale.status(), 401);
}

#[tokio::test]
async fn unregister_success() {
    let hub = start().await;
    let peer = make_peer();
    let credential = register_plain(&hub, &peer, None).await.credential;
    assert_eq!(
        unregister_status(&hub, peer.instance_id(), Some(&credential)).await,
        204
    );
}

#[tokio::test]
async fn one_registration_cannot_unregister_another_owner() {
    let hub = start().await;
    let attacker = make_peer();
    let victim = make_peer();
    let attacker_credential = register_plain(&hub, &attacker, None).await.credential;
    register_plain(&hub, &victim, None).await;

    assert_eq!(
        unregister_status(&hub, victim.instance_id(), Some(&attacker_credential)).await,
        401
    );
    assert!(hub.store().contains(victim.instance_id()));
}

#[tokio::test]
async fn unregister_not_found() {
    let hub = start().await;
    let response = http()
        .delete(control(
            &hub,
            &velo_hub::protocol::instance_by_id(InstanceId::new_v4()),
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 404);
    assert_eq!(error_code(response).await, ErrorCode::NotFound);
}

#[tokio::test]
async fn heartbeat_with_current_credential_is_acknowledged() {
    let hub = start().await;
    let peer = make_peer();
    let credential = register_plain(&hub, &peer, None).await.credential;
    let response = http()
        .post(control(
            &hub,
            &velo_hub::protocol::instance_heartbeat(peer.instance_id()),
        ))
        .header(
            velo_hub::protocol::CREDENTIAL_HEADER,
            credential.to_header_value(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body: HeartbeatResponse = response.json().await.unwrap();
    assert!(body.acknowledged);
}

#[tokio::test]
async fn heartbeat_without_credential_is_unauthorized() {
    let hub = start().await;
    let peer = make_peer();
    register_plain(&hub, &peer, None).await;
    assert_eq!(heartbeat_status(&hub, peer.instance_id(), None).await, 401);
}

#[tokio::test]
async fn stale_and_cross_owner_credentials_cannot_heartbeat() {
    let hub = start().await;
    let victim = make_peer();
    let attacker = make_peer();
    let first = register_plain(&hub, &victim, None).await.credential;
    let current = register_plain(&hub, &victim, Some(&first)).await.credential;
    let attacker = register_plain(&hub, &attacker, None).await.credential;

    for credential in [&first, &attacker] {
        assert_eq!(
            heartbeat_status(&hub, victim.instance_id(), Some(credential)).await,
            401
        );
    }
    assert_eq!(
        heartbeat_status(&hub, victim.instance_id(), Some(&current)).await,
        200
    );
}

#[tokio::test]
async fn unauthorized_heartbeats_do_not_postpone_expiry() {
    let store = Arc::new(
        InMemoryStore::builder()
            .ttl(Duration::from_secs(1))
            .prune_interval(Duration::from_secs(3600))
            .build(),
    );
    let hub = builder().store(store.clone()).serve().await.unwrap();
    let victim = make_peer();
    let attacker = make_peer();
    let stale = register_plain(&hub, &victim, None).await.credential;
    let _current = register_plain(&hub, &victim, Some(&stale)).await.credential;
    let attacker = register_plain(&hub, &attacker, None).await.credential;

    tokio::time::sleep(Duration::from_millis(250)).await;
    for credential in [None, Some(&stale), Some(&attacker)] {
        assert_eq!(
            heartbeat_status(&hub, victim.instance_id(), credential).await,
            401
        );
    }
    tokio::time::sleep(Duration::from_millis(850)).await;
    store.prune_stale();

    assert!(
        !hub.store().contains(victim.instance_id()),
        "rejected heartbeats must not refresh the victim's lease"
    );
}

#[tokio::test]
async fn heartbeat_unregistered_instance() {
    let hub = start().await;
    assert_eq!(
        heartbeat_status(&hub, InstanceId::new_v4(), None).await,
        404
    );
}

#[tokio::test]
async fn get_peer_by_instance_found() {
    let hub = start().await;
    let peer = make_peer();
    register_plain(&hub, &peer, None).await;
    let response = http()
        .get(public(&hub, &peers_by_instance(peer.instance_id())))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body: PeerLookupResponse = response.json().await.unwrap();
    assert_eq!(body.peer.instance_id(), peer.instance_id());
}

#[tokio::test]
async fn get_peer_by_instance_not_found() {
    let hub = start().await;
    let response = http()
        .get(public(&hub, &peers_by_instance(InstanceId::new_v4())))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 404);
    assert_eq!(error_code(response).await, ErrorCode::NotFound);
}

#[tokio::test]
async fn get_peer_by_worker_found() {
    let hub = start().await;
    let peer = make_peer();
    register_plain(&hub, &peer, None).await;
    let response = http()
        .get(public(&hub, &peers_by_worker(peer.worker_id())))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body: PeerLookupResponse = response.json().await.unwrap();
    assert_eq!(body.peer.instance_id(), peer.instance_id());
}

#[tokio::test]
async fn get_peer_by_worker_not_found() {
    let hub = start().await;
    let response = http()
        .get(public(
            &hub,
            &peers_by_worker(InstanceId::new_v4().worker_id()),
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 404);
    assert_eq!(error_code(response).await, ErrorCode::NotFound);
}

#[tokio::test]
async fn control_port_mirrors_discovery_endpoints() {
    let hub = start().await;
    let peer = make_peer();
    register_plain(&hub, &peer, None).await;
    let response = http()
        .get(control(&hub, &peers_by_instance(peer.instance_id())))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
}

/// Registration is a control-plane operation; the public listener does
/// not serve it. (kvbm asserted the same split in `control_routes.rs`.)
#[tokio::test]
async fn public_port_does_not_accept_registrations() {
    let hub = start().await;
    let response = http()
        .post(public(&hub, paths::INSTANCES))
        .json(&request(&make_peer()))
        .send()
        .await
        .unwrap();
    assert!(
        response.status() == 404 || response.status() == 405,
        "{}",
        response.status()
    );
}

#[tokio::test]
async fn peers_snapshot_tracks_registrations() {
    let hub = start().await;
    for _ in 0..3 {
        register_plain(&hub, &make_peer(), None).await;
    }
    assert_eq!(hub.store().list().len(), 3);
}

#[tokio::test]
async fn peers_snapshot_tracks_unregistrations() {
    let hub = start().await;
    let a = make_peer();
    let b = make_peer();
    let a_credential = register_plain(&hub, &a, None).await.credential;
    register_plain(&hub, &b, None).await;
    assert_eq!(
        unregister_status(&hub, a.instance_id(), Some(&a_credential)).await,
        204
    );
    let peers = hub.store().list();
    assert_eq!(peers.len(), 1);
    assert_eq!(peers[0].instance_id(), b.instance_id());
}

/// kvbm removed the registrant by dropping its HubClient (the guard sent
/// the DELETE). The guard is client work; here the DELETE is explicit.
#[tokio::test]
async fn registered_instance_visible_in_list_then_removed() {
    let hub = start().await;
    let list = || async {
        http()
            .get(control(&hub, paths::INSTANCES))
            .send()
            .await
            .unwrap()
            .json::<ListInstancesResponse>()
            .await
            .unwrap()
    };
    assert!(list().await.instances.is_empty());
    let peer = make_peer();
    let credential = register_plain(&hub, &peer, None).await.credential;
    let listed = list().await;
    assert_eq!(listed.instances.len(), 1);
    assert_eq!(listed.instances[0].peer.instance_id(), peer.instance_id());
    assert!(!listed.instances[0].pinned);
    assert_eq!(
        unregister_status(&hub, peer.instance_id(), Some(&credential)).await,
        204
    );
    assert!(list().await.instances.is_empty());
}

/// Ported from kvbm's `register_without_features_field_still_works`.
#[tokio::test]
async fn register_without_extensions_field_still_works() {
    let hub = start().await;
    let peer = make_peer();
    let body = format!(r#"{{"peer":{}}}"#, serde_json::to_string(&peer).unwrap());
    let response = http()
        .post(control(&hub, paths::INSTANCES))
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
}

// ---- velo probe --------------------------------------------------------

async fn start_with_velo() -> velo_hub::Hub {
    builder()
        .velo(velo_builder())
        .serve()
        .await
        .expect("start hub with velo")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn velo_probe_happy_path() {
    let hub = start_with_velo().await;
    let client = new_velo().await;
    install_heartbeat(&client);
    wire_mutual(&hub, &client).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let response = http()
        .post(control(&hub, &instance_probe(client.instance_id())))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body: ProbeResponse = response.json().await.unwrap();
    assert!(body.ok);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn velo_probe_after_client_velo_shutdown_returns_bad_gateway() {
    let hub = start_with_velo().await;
    let transport = tcp_transport();
    let client = velo_hub::velo::Velo::builder()
        .add_transport(Arc::clone(&transport) as Arc<dyn Transport>)
        .build()
        .await
        .unwrap();
    install_heartbeat(&client);
    wire_mutual(&hub, &client).await;
    transport.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let response = http()
        .post(control(&hub, &instance_probe(client.instance_id())))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 502);
    assert_eq!(error_code(response).await, ErrorCode::BadGateway);
}

// ---- hub self-registration and hub_instance_id ------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn register_response_includes_hub_instance_id_when_velo_configured() {
    let hub = start_with_velo().await;
    let client = new_velo().await;
    let response = register_plain(&hub, &client.peer_info(), None).await;
    let hub_id = response
        .hub_instance_id
        .expect("hub_instance_id should be Some");
    let lookup = http()
        .get(public(&hub, &peers_by_instance(hub_id)))
        .send()
        .await
        .unwrap();
    assert_eq!(lookup.status(), 200, "the hub is discoverable at once");
}

#[tokio::test]
async fn register_response_hub_instance_id_none_without_transport() {
    let hub = start().await;
    let response = register_plain(&hub, &make_peer(), None).await;
    assert!(response.hub_instance_id.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hub_self_registered_in_registry() {
    let hub = start_with_velo().await;
    let hub_velo_id = hub.velo().expect("hub velo").instance_id();
    let response = http()
        .get(public(&hub, &peers_by_instance(hub_velo_id)))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert!(hub.store().get(hub_velo_id).unwrap().record.pinned);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hub_self_entry_survives_reaper() {
    let mut config = config();
    config.server.ttl = Duration::from_millis(50);
    config.server.prune_interval = Duration::from_millis(20);
    let hub = velo_hub::Hub::builder(config)
        .velo(velo_builder())
        .serve()
        .await
        .unwrap();
    let hub_velo_id = hub.velo().expect("hub velo").instance_id();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let response = http()
        .get(public(&hub, &peers_by_instance(hub_velo_id)))
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        200,
        "the pinned entry must survive the reaper"
    );
}

// ---- generic extension behavior (kvbm's ConditionalDisagg tests) --------
//
// kvbm exercised these with its CD and P2P managers. The behavior under
// test is the hub's, so the `Role` toy (common/mod.rs) stands in for CD.

async fn start_with_role() -> (velo_hub::Hub, Arc<Role>) {
    let hub = builder().extension(Role::default()).serve().await.unwrap();
    let role = hub.extension::<Role>().unwrap();
    (hub, role)
}

/// kvbm: "feature_register_without_manager_rejects". A payload for an
/// extension the hub does not host is a 400 before anything is written.
#[tokio::test]
async fn payload_for_unknown_extension_is_rejected_and_writes_nothing() {
    let hub = start().await;
    let peer = make_peer();
    let response = post_register(
        &hub,
        &request_with(&peer, &[role_payload(RoleKind::Prefill)]),
        None,
    )
    .await;
    assert_eq!(response.status(), 400);
    assert_eq!(error_code(response).await, ErrorCode::BadRequest);
    let lookup = http()
        .get(public(&hub, &peers_by_instance(peer.instance_id())))
        .send()
        .await
        .unwrap();
    assert_eq!(lookup.status(), 404);
}

#[tokio::test]
async fn failed_authorized_reregister_restores_prior_registration() {
    let (hub, role) = start_with_role().await;
    let peer = make_peer();
    let credential = register(
        &hub,
        &request_with(&peer, &[role_payload(RoleKind::Prefill)]),
        None,
    )
    .await
    .credential;

    let rejected = post_register(
        &hub,
        &request_with(&peer, &[role_payload(RoleKind::Decode)]),
        Some(&credential),
    )
    .await;
    assert_eq!(rejected.status(), 400);
    assert!(hub.store().contains(peer.instance_id()));
    assert_eq!(role.snapshot().prefill, vec![peer.instance_id()]);
    assert!(role.snapshot().decode.is_empty());

    assert_eq!(
        unregister_status(&hub, peer.instance_id(), Some(&credential)).await,
        204
    );
}

#[tokio::test]
async fn extension_list_empty_on_both_ports() {
    let (hub, _role) = start_with_role().await;
    for url in [
        public(&hub, "/v1/ext/role/instances"),
        control(&hub, "/v1/ext/role/instances"),
    ] {
        let body: RoleSnapshot = http().get(url).send().await.unwrap().json().await.unwrap();
        assert!(body.prefill.is_empty());
        assert!(body.decode.is_empty());
    }
}

#[tokio::test]
async fn role_conflict_on_reregister() {
    let (hub, _role) = start_with_role().await;
    let peer = make_peer();
    let credential = register(
        &hub,
        &request_with(&peer, &[role_payload(RoleKind::Prefill)]),
        None,
    )
    .await
    .credential;
    let response = post_register(
        &hub,
        &request_with(&peer, &[role_payload(RoleKind::Decode)]),
        Some(&credential),
    )
    .await;
    assert_eq!(response.status(), 400);
}

/// DELETE answers only after every extension dropped the owner.
#[tokio::test]
async fn unregister_removes_from_extension_lists() {
    let (hub, role) = start_with_role().await;
    let peer = make_peer();
    let credential = register(
        &hub,
        &request_with(&peer, &[role_payload(RoleKind::Prefill)]),
        None,
    )
    .await
    .credential;
    assert_eq!(role.snapshot().prefill.len(), 1);
    assert_eq!(
        unregister_status(&hub, peer.instance_id(), Some(&credential)).await,
        204
    );
    assert!(role.snapshot().prefill.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reaper_eviction_reaches_extensions_and_clears_the_credential() {
    let mut config = config();
    config.server.ttl = Duration::from_millis(60);
    config.server.prune_interval = Duration::from_millis(20);
    let hub = velo_hub::Hub::builder(config)
        .extension(Role::default())
        .serve()
        .await
        .unwrap();
    let role_ext = hub.extension::<Role>().unwrap();
    let peer = make_peer();
    let body = request_with(&peer, &[role_payload(RoleKind::Prefill)]);
    register(&hub, &body, None).await;
    assert_eq!(role_ext.snapshot().prefill.len(), 1);

    assert!(
        eventually(Duration::from_secs(2), || role_ext
            .snapshot()
            .prefill
            .is_empty())
        .await,
        "TTL eviction must reach the extension"
    );
    let mut fresh = post_register(&hub, &body, None).await;
    // Delivery and the end of revocation are one step under the lock, but
    // the extension can be observed empty an instant before the owner is
    // released. Busy here is that instant; it must resolve.
    for _ in 0..50 {
        if fresh.status() != 409 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        fresh = post_register(&hub, &body, None).await;
    }
    assert_eq!(
        fresh.status(),
        200,
        "TTL eviction must also remove the credential"
    );
}
