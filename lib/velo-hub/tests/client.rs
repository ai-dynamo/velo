// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! `HubClient` against a real hub. The first block is ported from kvbm-hub
//! `tests/integration.rs` (its `HubClient` tests); kvbm registered with
//! `register_instance(peer)`, here `register_peer(peer, extensions)`.

mod common;

use std::collections::BTreeMap;
use std::time::Duration;

use common::*;
use serde::{Deserialize, Serialize};
use velo_hub::client::{ClientError, HubClient, HubClientBuilder};
use velo_hub::protocol::{ErrorCode, peers_by_instance};
use velo_hub::velo::discovery::PeerDiscovery;
use velo_hub::velo::{Handler, Velo};
use velo_hub::{Extension, Hub};

fn client_for(hub: &Hub) -> HubClient {
    HubClient::builder()
        .host("127.0.0.1")
        .public_port(hub.public_addr().port())
        .control_port(hub.control_addr().port())
        .build()
        .expect("build client")
}

fn none() -> BTreeMap<String, serde_json::Value> {
    BTreeMap::new()
}

async fn lookup_status(hub: &Hub, peer: &velo_hub::velo::PeerInfo) -> u16 {
    http()
        .get(public(hub, &peers_by_instance(peer.instance_id())))
        .send()
        .await
        .unwrap()
        .status()
        .as_u16()
}

// ---- ported from kvbm-hub ------------------------------------------------

#[tokio::test]
async fn client_builder_requires_host() {
    assert!(HubClientBuilder::default().build().is_err());
}

#[tokio::test]
async fn client_starts_unregistered() {
    let hub = start().await;
    let client = client_for(&hub);
    assert!(!client.is_registered());
}

#[tokio::test]
async fn client_register_sets_is_registered() {
    let hub = start().await;
    let client = client_for(&hub);
    client.register_peer(make_peer(), none()).await.unwrap();
    assert!(client.is_registered());
}

/// kvbm's `client_register_twice_errors`, intentionally changed: the hub
/// supports re-registration with the credential, so a second `register` of
/// the same peer re-registers. It gets a new credential and a new epoch, and
/// the old credential stops working.
#[tokio::test]
async fn a_second_register_re_registers_and_rotates_the_credential() {
    let hub = start().await;
    let client = client_for(&hub);
    let peer = make_peer();
    let first = client.register_peer(peer.clone(), none()).await.unwrap();
    let second = client.register_peer(peer.clone(), none()).await.unwrap();
    assert_ne!(first.credential, second.credential);
    assert_ne!(first.epoch, second.epoch);
    assert_eq!(
        heartbeat_status(&hub, peer.instance_id(), Some(&first.credential)).await,
        401
    );
    client.send_heartbeat().await.unwrap();
}

/// A client holds one registration: registering a different peer while it
/// holds one is refused before any request.
#[tokio::test]
async fn registering_a_second_peer_is_refused() {
    let hub = start().await;
    let client = client_for(&hub);
    client.register_peer(make_peer(), none()).await.unwrap();
    let error = client.register_peer(make_peer(), none()).await.unwrap_err();
    assert!(
        matches!(error, ClientError::AlreadyRegistered(_)),
        "{error}"
    );
}

#[tokio::test]
async fn client_heartbeat_while_registered() {
    let hub = start().await;
    let client = client_for(&hub);
    client.register_peer(make_peer(), none()).await.unwrap();
    client.send_heartbeat().await.unwrap();
}

#[tokio::test]
async fn client_heartbeat_before_register_errors() {
    let hub = start().await;
    let client = client_for(&hub);
    assert!(matches!(
        client.send_heartbeat().await,
        Err(ClientError::NotRegistered)
    ));
}

#[tokio::test]
async fn client_discover_by_instance_id() {
    let hub = start().await;
    let client = client_for(&hub);
    let peer = make_peer();
    client.register_peer(peer.clone(), none()).await.unwrap();
    let found = client
        .discovery()
        .discover_by_instance_id(peer.instance_id())
        .await
        .unwrap();
    assert_eq!(found.instance_id(), peer.instance_id());
}

#[tokio::test]
async fn client_discover_by_worker_id() {
    let hub = start().await;
    let client = client_for(&hub);
    let peer = make_peer();
    client.register_peer(peer.clone(), none()).await.unwrap();
    let found = client
        .discovery()
        .discover_by_worker_id(peer.worker_id())
        .await
        .unwrap();
    assert_eq!(found.instance_id(), peer.instance_id());
}

#[tokio::test]
async fn client_unregister_removes_from_server() {
    let hub = start().await;
    let client = client_for(&hub);
    let peer = make_peer();
    client.register_peer(peer.clone(), none()).await.unwrap();
    client.unregister().await.unwrap();
    assert!(!client.is_registered());
    assert_eq!(lookup_status(&hub, &peer).await, 404);
}

#[tokio::test]
async fn client_unregister_noop_when_not_registered() {
    let hub = start().await;
    let client = client_for(&hub);
    client.unregister().await.unwrap();
}

#[tokio::test]
async fn client_discover_after_unregister_errors() {
    let hub = start().await;
    let client = client_for(&hub);
    let peer = make_peer();
    client.register_peer(peer.clone(), none()).await.unwrap();
    client.unregister().await.unwrap();
    assert!(
        client
            .discovery()
            .discover_by_instance_id(peer.instance_id())
            .await
            .is_err()
    );
}

/// Dropping the last clone of a registered client sends `DELETE`.
#[tokio::test]
async fn registration_guard_drop_fires_delete() {
    let hub = start().await;
    let peer = make_peer();
    {
        let client = client_for(&hub);
        client.register_peer(peer.clone(), none()).await.unwrap();
        let clone = client.clone();
        drop(client);
        // A clone still holds the registration.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(lookup_status(&hub, &peer).await, 200);
        drop(clone);
    }
    let hub_ref = &hub;
    let gone = eventually(Duration::from_secs(2), || {
        hub_ref.store().get(peer.instance_id()).is_none()
    })
    .await;
    assert!(gone, "dropping the client did not unregister");
}

/// The list half of kvbm's `registered_client_visible_in_list_then_removed_on_drop`
/// is in `integration.rs`; this is the drop half.
#[tokio::test]
async fn a_dropped_client_leaves_the_list() {
    let hub = start().await;
    let peer = make_peer();
    {
        let client = client_for(&hub);
        client.register_peer(peer.clone(), none()).await.unwrap();
        assert_eq!(hub.store().list().len(), 1);
    }
    let hub_ref = &hub;
    assert!(eventually(Duration::from_secs(2), || hub_ref.store().list().is_empty()).await);
}

// ---- new in velo-hub -------------------------------------------------------

/// velo keeps its discovery for as long as it lives. The discovery handle
/// that `velo_builder` installs holds no registration, so dropping the
/// user's client still unregisters while that velo lives. (kvbm passed the
/// client itself to velo, and its registration lived as long as velo did.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_the_client_unregisters_while_its_velo_lives() {
    let hub = builder().velo(velo_builder()).serve().await.unwrap();
    let client = client_for(&hub);
    let velo = client
        .velo_builder()
        .add_transport(tcp_transport())
        .build()
        .await
        .unwrap();
    client.register(&velo, none()).await.unwrap();
    let id = velo.instance_id();
    drop(client);
    let hub_ref = &hub;
    assert!(
        eventually(Duration::from_secs(2), || hub_ref.store().get(id).is_none()).await,
        "the velo's discovery handle kept the registration alive"
    );
    drop(velo);
    hub.shutdown().await.unwrap();
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Ping {
    value: u64,
}

/// Two velo instances built from `velo_builder()` find each other through
/// the hub and exchange a message: neither was told the other's address.
/// velo resolves a worker target through its discovery, which is the hub.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_velos_discover_each_other_through_the_hub() {
    let hub = builder().velo(velo_builder()).serve().await.unwrap();
    let (a, b) = (client_for(&hub), client_for(&hub));
    let velo_a = a
        .velo_builder()
        .add_transport(tcp_transport())
        .build()
        .await
        .unwrap();
    let velo_b = b
        .velo_builder()
        .add_transport(tcp_transport())
        .build()
        .await
        .unwrap();
    velo_b
        .register_handler(
            Handler::typed_unary_async::<Ping, Ping, _, _>("echo", |ctx| async move {
                Ok(Ping {
                    value: ctx.input.value + 1,
                })
            })
            .build(),
        )
        .unwrap();
    a.register(&velo_a, none()).await.unwrap();
    b.register(&velo_b, none()).await.unwrap();

    let answer: Ping = tokio::time::timeout(
        Duration::from_secs(5),
        velo_a
            .typed_unary::<Ping>("echo")
            .unwrap()
            .payload(&Ping { value: 41 })
            .unwrap()
            .worker(velo_b.peer_info().worker_id())
            .send(),
    )
    .await
    .expect("the message did not arrive")
    .unwrap();
    assert_eq!(answer, Ping { value: 42 });
    a.unregister().await.unwrap();
    b.unregister().await.unwrap();
    hub.shutdown().await.unwrap();
}

/// `register(&velo, ..)` installs the heartbeat handler, so the hub's velo
/// probes reach the client and keep it registered. Replaces kvbm's
/// `create_heartbeat_handler_builds`, which only built the handler.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_hub_heartbeat_reaches_the_client_and_keeps_it_registered() {
    let mut config = config();
    config.server.heartbeat_interval = Duration::from_millis(100);
    config.server.heartbeat_max_failures = 2;
    let hub = Hub::builder(config)
        .velo(velo_builder())
        .serve()
        .await
        .unwrap();
    let client = client_for(&hub);
    let velo = client
        .velo_builder()
        .add_transport(tcp_transport())
        .build()
        .await
        .unwrap();
    assert!(client.last_hub_heartbeat().is_none());
    client.register(&velo, none()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert!(hub.store().get(velo.instance_id()).is_some());
    let (seq, _) = client
        .last_hub_heartbeat()
        .expect("the hub's probe did not reach the client");
    assert!(seq > 0);
    client.unregister().await.unwrap();
    hub.shutdown().await.unwrap();
}

/// A second client registering through the same velo does not fail on the
/// heartbeat handler that the first one installed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn registering_through_a_velo_that_has_the_handler_works() {
    let hub = builder().velo(velo_builder()).serve().await.unwrap();
    let client = client_for(&hub);
    let velo: Velo = client
        .velo_builder()
        .add_transport(tcp_transport())
        .build()
        .await
        .unwrap();
    client.register(&velo, none()).await.unwrap();
    client.unregister().await.unwrap();
    client.register(&velo, none()).await.unwrap();
    client.unregister().await.unwrap();
    hub.shutdown().await.unwrap();
}

/// An error answer carries the hub's `ErrorCode`, so a caller can act on it.
#[tokio::test]
async fn hub_errors_carry_their_code() {
    let hub = start().await;
    let client = client_for(&hub);
    let mut unknown = none();
    unknown.insert("no-such-extension".to_string(), serde_json::Value::Null);
    let error = client
        .register_peer(make_peer(), unknown)
        .await
        .unwrap_err();
    assert_eq!(error.code(), Some(ErrorCode::BadRequest), "{error}");
    assert!(!client.is_registered());
}

/// When the hub no longer knows the registration (here someone holding the
/// credential deleted it), the client learns it from its next call and
/// drops the dead registration, so the next `register` starts fresh. Both
/// calls that present the credential follow the rule: a heartbeat answered
/// `not_found`, and a re-registration answered `unauthorized`.
#[tokio::test]
async fn a_dead_registration_is_dropped_and_the_next_register_starts_fresh() {
    let hub = start().await;
    let client = client_for(&hub);
    let peer = make_peer();

    let response = client.register_peer(peer.clone(), none()).await.unwrap();
    assert_eq!(
        unregister_status(&hub, peer.instance_id(), Some(&response.credential)).await,
        204
    );
    let error = client.send_heartbeat().await.unwrap_err();
    assert!(error.code().is_some(), "{error}");
    assert!(!client.is_registered());
    let response = client.register_peer(peer.clone(), none()).await.unwrap();

    assert_eq!(
        unregister_status(&hub, peer.instance_id(), Some(&response.credential)).await,
        204
    );
    let error = client
        .register_peer(peer.clone(), none())
        .await
        .unwrap_err();
    assert!(
        matches!(
            error.code(),
            Some(ErrorCode::Unauthorized | ErrorCode::RegisterAgain)
        ),
        "{error}"
    );
    assert!(!client.is_registered());
    client.register_peer(peer, none()).await.unwrap();
}

/// `ext(name)` reaches an extension's routes: public reads without a
/// credential, control mutations with the registration's credential.
#[tokio::test]
async fn ext_reaches_public_and_control_routes() {
    let hub = builder()
        .extension(Ledger::default())
        .serve()
        .await
        .unwrap();
    let client = client_for(&hub);
    let ledger = client.ext(Ledger::NAME);
    let peer = make_peer();
    let owner = peer.instance_id();
    let publish_path = format!("/instances/{owner}/publish");
    assert!(matches!(
        ledger
            .control_post::<_, ()>(
                &publish_path,
                &Publish {
                    key: "k".into(),
                    generation: 1
                }
            )
            .await,
        Err(ClientError::NotRegistered)
    ));

    let (name, payload) = ledger_opt_in();
    let mut extensions = none();
    extensions.insert(name.to_string(), payload);
    client.register_peer(peer, extensions).await.unwrap();
    ledger
        .control_post::<_, ()>(
            &publish_path,
            &Publish {
                key: "k".into(),
                generation: 7,
            },
        )
        .await
        .unwrap();
    let hit: Hit = ledger
        .get(&format!("/instances/{owner}/entries/k"))
        .await
        .unwrap();
    assert_eq!(hit.generation, 7);
    let miss = ledger
        .get::<Hit>(&format!("/instances/{owner}/entries/nope"))
        .await
        .unwrap_err();
    assert_eq!(miss.code(), Some(ErrorCode::NotFound));
}

/// `describe` reads the hub's descriptor from the public listener.
#[tokio::test]
async fn describe_names_the_extensions() {
    let hub = builder()
        .extension(Ledger::default())
        .serve()
        .await
        .unwrap();
    let descriptor = client_for(&hub).describe().await.unwrap();
    assert_eq!(descriptor.hub_id, hub.hub_id());
    assert!(
        descriptor
            .extensions
            .iter()
            .any(|extension| extension.name == Ledger::NAME)
    );
}

/// `from_url` takes the public listener's URL; the control port keeps its
/// default unless set.
#[test]
fn from_url_reads_scheme_host_and_public_port() {
    let client = HubClientBuilder::from_url("http://hub.example:4000")
        .unwrap()
        .build()
        .unwrap();
    assert_eq!(client.public_url().as_str(), "http://hub.example:4000/");
    assert_eq!(
        client.control_url().as_str(),
        format!(
            "http://hub.example:{}/",
            velo_hub::protocol::DEFAULT_CONTROL_PORT
        )
    );
    assert!(HubClientBuilder::from_url("not a url").is_err());
}

/// The hub holds a registration this client does not know about, as after
/// the response to its registration was lost (here the registration is
/// posted around the client). The client's handler answers the hub's probes
/// `ok: false`, so the hub evicts the entry and the owner can register
/// again, instead of being locked out while it answers probes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_without_the_registration_answers_not_ok_and_is_freed() {
    let mut config = config();
    config.server.heartbeat_interval = Duration::from_millis(100);
    config.server.heartbeat_max_failures = 2;
    let hub = Hub::builder(config)
        .velo(velo_builder())
        .serve()
        .await
        .unwrap();
    let client = client_for(&hub);
    let velo = client
        .velo_builder()
        .add_transport(tcp_transport())
        .build()
        .await
        .unwrap();
    client.register(&velo, none()).await.unwrap();
    client.unregister().await.unwrap();
    // A registration whose answer the client never saw.
    let orphan = register(&hub, &request(&velo.peer_info()), None).await;
    assert!(!client.is_registered());
    let hub_ref = &hub;
    let id = velo.instance_id();
    assert!(
        eventually(Duration::from_secs(3), || hub_ref.store().get(id).is_none()).await,
        "the client answered its probes ok without holding the registration"
    );
    assert_eq!(
        heartbeat_status(&hub, id, Some(&orphan.credential)).await,
        404
    );
    client.register(&velo, none()).await.unwrap();
    client.unregister().await.unwrap();
    hub.shutdown().await.unwrap();
}

// ---- cancellation and handle rules ------------------------------------------

/// A `register` future dropped while the hub runs the transaction (here a
/// caller's timeout): the hub commits anyway, in its own task, and the
/// client holds nothing. Its handler must answer the hub's probes not ok,
/// so the hub frees the entry and the next `register` succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dropped_register_future_does_not_answer_probes_ok() {
    let mut config = config();
    config.server.heartbeat_interval = Duration::from_millis(100);
    config.server.heartbeat_max_failures = 2;
    let store = std::sync::Arc::new(velo_hub::store::testing::FaultStore::default());
    let hub = Hub::builder(config)
        .velo(velo_builder())
        .store(store.clone())
        .serve()
        .await
        .unwrap();
    let client = client_for(&hub);
    let velo = client
        .velo_builder()
        .add_transport(tcp_transport())
        .build()
        .await
        .unwrap();
    let id = velo.instance_id();
    let gate = store.gate_next_put();
    let (timed_out, ()) = tokio::join!(
        tokio::time::timeout(Duration::from_millis(200), client.register(&velo, none())),
        gate.entered()
    );
    assert!(timed_out.is_err());
    gate.release();
    let hub_ref = &hub;
    assert!(eventually(Duration::from_secs(2), || hub_ref.store().get(id).is_some()).await);
    assert!(!client.is_registered());
    assert!(
        eventually(Duration::from_secs(3), || hub_ref.store().get(id).is_none()).await,
        "the client holds no registration but answers the hub's probes ok"
    );
    client.register(&velo, none()).await.unwrap();
    client.unregister().await.unwrap();
    hub.shutdown().await.unwrap();
}

/// A re-registration whose future is dropped after the hub committed leaves
/// the client with the old credential. `unregister` then gets 401, which
/// means "this credential owns nothing there", so it answers Ok and drops
/// the registration, while the hub still holds the new one. The client
/// then answers probes not ok, and the hub frees the entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dropped_re_register_heals_through_the_probes() {
    let mut config = config();
    config.server.heartbeat_interval = Duration::from_millis(100);
    config.server.heartbeat_max_failures = 2;
    let store = std::sync::Arc::new(velo_hub::store::testing::FaultStore::default());
    let hub = Hub::builder(config)
        .velo(velo_builder())
        .store(store.clone())
        .serve()
        .await
        .unwrap();
    let client = client_for(&hub);
    let velo = client
        .velo_builder()
        .add_transport(tcp_transport())
        .build()
        .await
        .unwrap();
    let id = velo.instance_id();
    let first = client.register(&velo, none()).await.unwrap();
    let gate = store.gate_next_put();
    let (timed_out, ()) = tokio::join!(
        tokio::time::timeout(Duration::from_millis(200), client.register(&velo, none())),
        gate.entered()
    );
    assert!(timed_out.is_err());
    gate.release();
    let hub_ref = &hub;
    assert!(
        eventually(Duration::from_secs(2), || {
            hub_ref
                .authority()
                .authorize(id, Some(&first.credential))
                .is_err()
        })
        .await
    );
    assert!(client.unregister().await.is_ok());
    assert!(!client.is_registered());
    assert!(
        eventually(Duration::from_secs(3), || hub_ref.store().get(id).is_none()).await,
        "the hub did not free the registration the client lost"
    );
    client.register(&velo, none()).await.unwrap();
    client.unregister().await.unwrap();
    hub.shutdown().await.unwrap();
}

/// An `ExtClient` is not a clone of the client: like `HubDiscovery`, it
/// must not keep the registration alive once every `HubClient` is dropped.
/// Its control calls then answer `NotRegistered`.
#[tokio::test]
async fn an_ext_client_does_not_keep_the_registration_alive() {
    let hub = start().await;
    let peer = make_peer();
    let client = client_for(&hub);
    client.register_peer(peer.clone(), none()).await.unwrap();
    let ext = client.ext("anything");
    drop(client);
    let hub_ref = &hub;
    assert!(
        eventually(Duration::from_secs(2), || hub_ref
            .store()
            .get(peer.instance_id())
            .is_none())
        .await,
        "the ExtClient kept the registration alive"
    );
    assert!(matches!(
        ext.control_post::<_, ()>("/x", &()).await,
        Err(ClientError::NotRegistered)
    ));
}

/// The hub no longer has the registration (an `unregister` whose future
/// was dropped after the hub removed the entry). The next heartbeat's
/// answer says so, and the client drops the registration, so
/// `is_registered()` tells the truth again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_heartbeat_the_hub_does_not_know_drops_the_registration() {
    let store = std::sync::Arc::new(velo_hub::store::testing::FaultStore::default());
    let hub = builder()
        .velo(velo_builder())
        .store(store.clone())
        .serve()
        .await
        .unwrap();
    let client = client_for(&hub);
    let velo = client
        .velo_builder()
        .add_transport(tcp_transport())
        .build()
        .await
        .unwrap();
    let id = velo.instance_id();
    client.register(&velo, none()).await.unwrap();
    let gate = store.gate_next_remove();
    let (timed_out, ()) = tokio::join!(
        tokio::time::timeout(Duration::from_millis(200), client.unregister()),
        gate.entered()
    );
    assert!(timed_out.is_err());
    gate.release();
    let hub_ref = &hub;
    assert!(eventually(Duration::from_secs(2), || hub_ref.store().get(id).is_none()).await);
    assert!(client.send_heartbeat().await.is_err());
    assert!(!client.is_registered());
    client.register(&velo, none()).await.unwrap();
    client.unregister().await.unwrap();
    hub.shutdown().await.unwrap();
}

#[test]
fn an_ipv6_literal_host_builds() {
    let client = HubClientBuilder::default()
        .host("2001:db8::1")
        .build()
        .unwrap();
    assert_eq!(
        client.public_url().as_str(),
        format!(
            "http://[2001:db8::1]:{}/",
            velo_hub::protocol::DEFAULT_PUBLIC_PORT
        )
    );
}
