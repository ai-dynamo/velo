// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! `HubClient` against a real hub. The first block is ported from kvbm-hub
//! `tests/integration.rs` (its `HubClient` tests); kvbm registered with
//! `register_instance(peer)`, here `register_peer(peer, extensions)`.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use common::*;
use serde::{Deserialize, Serialize};
use velo_hub::client::{ClientError, HubClient, HubClientBuilder};
use velo_hub::protocol::{ErrorCode, peers_by_instance};
use velo_hub::store::testing::FaultStore;
use velo_hub::store::{Continuity, HubStore, InMemoryStore};
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

/// A re-registration whose future is dropped: the hub may have committed
/// it (here it did) and rotated the credential, so the client cannot know
/// whether the credential it holds still works. It fails closed: it drops
/// the registration, answers the hub's probes not ok, and the hub frees the
/// entry on its own, with no call from the caller. Then a fresh `register`
/// works.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dropped_re_register_fails_closed_and_the_hub_frees_the_entry() {
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
    assert!(!client.is_registered());
    assert!(
        eventually(Duration::from_secs(3), || hub_ref.store().get(id).is_none()).await,
        "the hub kept a registration that nobody can use"
    );
    client.register(&velo, none()).await.unwrap();
    client.unregister().await.unwrap();
    hub.shutdown().await.unwrap();
}

/// An `ExtClient` control call answered `unauthorized` (someone with the
/// credential re-registered around the client) drops the registration,
/// like every other call that presents the credential.
#[tokio::test]
async fn an_ext_control_call_answered_unauthorized_drops_the_registration() {
    let hub = builder()
        .extension(Ledger::default())
        .serve()
        .await
        .unwrap();
    let client = client_for(&hub);
    let ledger = client.ext(Ledger::NAME);
    let peer = make_peer();
    let owner = peer.instance_id();
    let (name, payload) = ledger_opt_in();
    let mut extensions = none();
    extensions.insert(name.to_string(), payload);
    let first = client
        .register_peer(peer.clone(), extensions)
        .await
        .unwrap();
    let _second = register(
        &hub,
        &request_with(&peer, &[ledger_opt_in()]),
        Some(&first.credential),
    )
    .await;
    let error = ledger
        .control_post::<_, ()>(
            &format!("/instances/{owner}/publish"),
            &Publish {
                key: "k".into(),
                generation: 1,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), Some(ErrorCode::Unauthorized), "{error}");
    assert!(!client.is_registered());
}

/// The hub holds a registration the client never got, and the caller
/// retries `register` in a tight loop (each refused with 401). While a
/// call is in flight the handler would answer probes ok, and the hub would
/// never reach `heartbeat_max_failures`. A credential-free register that the
/// hub refuses tells the client that an orphan holds its instance, so the
/// handler stops answering ok until a registration succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tight_register_retry_loop_still_frees_the_orphan() {
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
    // Installs the handler; the hub refuses the payload, so nothing is held.
    let mut unknown = none();
    unknown.insert("no-such-extension".to_string(), serde_json::Value::Null);
    assert!(client.register(&velo, unknown).await.is_err());
    let _orphan = register(&hub, &request(&velo.peer_info()), None).await;
    assert!(!client.is_registered());

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut got_in = false;
    while tokio::time::Instant::now() < deadline {
        match client.register(&velo, none()).await {
            Ok(_) => {
                got_in = true;
                break;
            }
            // `busy` while the hub evicts the orphan: retry, as the
            // protocol says.
            Err(ClientError::Hub {
                code: ErrorCode::Unauthorized | ErrorCode::Busy,
                ..
            }) => continue,
            Err(error) => panic!("{error}"),
        }
    }
    assert!(got_in, "retries without backoff kept the orphan alive");
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

// ---- M3 review pass 1 -------------------------------------------------------

fn client_with_http(hub: &Hub, http: reqwest::Client) -> HubClient {
    HubClient::builder()
        .host("127.0.0.1")
        .public_port(hub.public_addr().port())
        .control_port(hub.control_addr().port())
        .http_client(http)
        .build()
        .unwrap()
}

/// A `register_service` answered `unauthorized` (the credential was rotated
/// around the client) drops the registration, like every other call that
/// presents the credential.
#[tokio::test]
async fn register_service_answered_unauthorized_drops_the_registration() {
    let hub = start().await;
    let client = client_for(&hub);
    let peer = make_peer();
    let first = client
        .register_peer(peer.clone(), Default::default())
        .await
        .unwrap();
    // Someone with the credential re-registers around the client.
    let _second = register(&hub, &request(&peer), Some(&first.credential)).await;
    let error = client.register_service("svc").await.unwrap_err();
    assert_eq!(error.code(), Some(ErrorCode::Unauthorized), "{error}");
    assert!(
        !client.is_registered(),
        "the client kept a credential the hub rejected"
    );
}

/// The same, checked before any further call: a re-registration that got
/// no answer fails closed (the client holds nothing), so the handler
/// answers not ok and the hub frees the registration nobody can use.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_timed_out_re_registration_leaves_nothing_held() {
    let store = std::sync::Arc::new(velo_hub::store::testing::FaultStore::default());
    let hub = builder().store(store.clone()).serve().await.unwrap();
    let http = reqwest::Client::builder()
        .timeout(Duration::from_millis(300))
        .build()
        .unwrap();
    let client = client_with_http(&hub, http);
    let peer = make_peer();
    client
        .register_peer(peer.clone(), Default::default())
        .await
        .unwrap();
    let gate = store.gate_next_put();
    let again = client.register_peer(peer.clone(), Default::default());
    let (answer, _) = tokio::join!(again, async {
        gate.entered().await;
        tokio::time::sleep(Duration::from_millis(600)).await;
        gate.release();
    });
    assert!(answer.is_err());
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !client.is_registered(),
        "the client holds a credential that may be dead; it must fail closed"
    );
    hub.shutdown().await.unwrap();
}

/// A re-registration whose request times out on the client while the hub
/// commits it is the same "no answer" as a dropped future: the hub rotated
/// the credential, so the one the client held is dead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_timed_out_re_registration_fails_closed() {
    let store = std::sync::Arc::new(velo_hub::store::testing::FaultStore::default());
    let hub = builder().store(store.clone()).serve().await.unwrap();
    let http = reqwest::Client::builder()
        .timeout(Duration::from_millis(300))
        .build()
        .unwrap();
    let client = client_with_http(&hub, http);
    let peer = make_peer();
    let first = client
        .register_peer(peer.clone(), Default::default())
        .await
        .unwrap();
    let gate = store.gate_next_put();
    let again = client.register_peer(peer.clone(), Default::default());
    let (answer, _) = tokio::join!(again, async {
        gate.entered().await;
        tokio::time::sleep(Duration::from_millis(600)).await;
        gate.release();
    });
    assert!(
        matches!(answer, Err(ClientError::Transport(ref e)) if e.is_timeout()),
        "{answer:?}"
    );
    // Give the hub's transaction time to commit.
    tokio::time::sleep(Duration::from_millis(200)).await;
    // The hub rotated the credential, so the one the client held is dead,
    // and the client no longer holds it.
    let id = peer.instance_id();
    assert!(
        hub.authority()
            .authorize(id, Some(&first.credential))
            .is_err()
    );
    assert!(!client.is_registered());
    hub.shutdown().await.unwrap();
}

/// An extension whose `on_registered` takes 700 ms when the registrant opts
/// in: a conforming slow registration, longer than two heartbeat intervals.
#[derive(Default)]
struct Slow;

impl velo_hub::Extension for Slow {
    const NAME: &'static str = "slow";
    type Registration = ();
    type Staged = ();
    async fn prepare(
        &self,
        _: &velo_hub::Txn<'_>,
        _: Option<()>,
    ) -> Result<(), velo_hub::Rejection> {
        Ok(())
    }
    fn commit(&self, _: (), _: &velo_hub::Committed<'_>) {}
    async fn on_registered(&self, reg: &velo_hub::Committed<'_>) {
        if reg.extensions.contains_key("slow") {
            tokio::time::sleep(Duration::from_millis(700)).await;
        }
    }
    fn on_removed(&self, _: velo_hub::store::Removal) {}
}

async fn slow_hub() -> Hub {
    let mut config = config();
    config.server.heartbeat_interval = Duration::from_millis(100);
    config.server.heartbeat_max_failures = 2;
    Hub::builder(config)
        .velo(velo_builder())
        .extension(Slow)
        .serve()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_registration_keeps_its_entry() {
    let hub = slow_hub().await;
    let client = client_for(&hub);
    let velo = client
        .velo_builder()
        .add_transport(tcp_transport())
        .build()
        .await
        .unwrap();
    client.register(&velo, slow_opt_in()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(client.is_registered());
    assert!(
        hub.store().get(velo.instance_id()).is_some(),
        "control: the entry was evicted even without orphaned"
    );
    client.unregister().await.unwrap();
    hub.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
/// After an orphan was freed, the registration that succeeds must not be
/// evicted while its transaction runs: the hub does not probe an owner whose
/// registration is in flight, so a slow `on_registered` cannot let the probes
/// remove the entry before the client has its answer.
async fn a_slow_registration_after_an_orphan_keeps_its_entry() {
    let hub = slow_hub().await;
    let client = client_for(&hub);
    let velo = client
        .velo_builder()
        .add_transport(tcp_transport())
        .build()
        .await
        .unwrap();
    // Install the handler; refuse the payload so nothing is held.
    let mut unknown: std::collections::BTreeMap<String, serde_json::Value> = Default::default();
    unknown.insert("no-such-extension".into(), serde_json::Value::Null);
    assert!(client.register(&velo, unknown).await.is_err());
    // An orphan: a registration the client never got.
    let _orphan = register(&hub, &request(&velo.peer_info()), None).await;
    let refused = client.register(&velo, Default::default()).await;
    assert_eq!(refused.unwrap_err().code(), Some(ErrorCode::Unauthorized));
    // The hub frees the orphan (handler answers not ok).
    assert!(
        eventually(Duration::from_secs(5), || hub
            .store()
            .get(velo.instance_id())
            .is_none())
        .await
    );
    // Now the register that should succeed.
    let answer = client.register(&velo, slow_opt_in()).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let held = hub.store().get(velo.instance_id()).is_some();
    assert!(
        answer.is_ok() && held,
        "the registration was evicted while it ran: answer ok = {}, hub holds = {held}",
        answer.is_ok()
    );
    let _ = client.unregister().await;
    hub.shutdown().await.unwrap();
}

fn slow_opt_in() -> std::collections::BTreeMap<String, serde_json::Value> {
    let mut m = std::collections::BTreeMap::new();
    m.insert("slow".to_string(), serde_json::Value::Null);
    m
}

/// A panic in an extension's `on_registered` after the credential
/// committed: the transaction's `Drop` retires the entry and fails the
/// credential closed, and the hub answers `register_again` (not 500), so
/// the client drops the credential the hub rotated away.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hub_panic_after_the_write_drops_the_registration() {
    let store = std::sync::Arc::new(velo_hub::store::testing::FaultStore::default());
    let hub = builder()
        .store(store.clone())
        .extension(Ledger::default())
        .serve()
        .await
        .unwrap();
    let ledger = hub.extension::<Ledger>().unwrap();
    let client = client_for(&hub);
    let peer = make_peer();
    let first = client
        .register_peer(peer.clone(), Default::default())
        .await
        .unwrap();

    ledger.panic_next_registered();
    let answer = client.register_peer(peer.clone(), Default::default()).await;
    assert!(
        matches!(
            &answer,
            Err(velo_hub::client::ClientError::Hub {
                code: velo_hub::protocol::ErrorCode::RegisterAgain,
                ..
            })
        ),
        "control: {answer:?}"
    );
    assert!(
        eventually(Duration::from_secs(1), || store
            .get(peer.instance_id())
            .is_none())
        .await,
        "control: the hub retired the entry"
    );
    assert!(
        hub.authority()
            .authorize(peer.instance_id(), Some(&first.credential))
            .is_err(),
        "control: the hub rotated the credential away"
    );
    assert!(
        !client.is_registered(),
        "the client holds a credential the hub rotated away after a 500 on a re-registration"
    );
}

/// An indeterminate store write that cannot be resolved: the hub drops
/// both the prior registration and the attempted one and reserves the
/// entry, whose record holds the presented credential. It answers
/// `recovered`, so the client keeps that credential (and raises its
/// `recovered` flag), and the next `register` recovers the entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unresolved_write_answers_recovered_and_the_credential_recovers() {
    use velo_hub::store::testing::PutFault;
    let store = std::sync::Arc::new(velo_hub::store::testing::FaultStore::default());
    let hub = builder().store(store.clone()).serve().await.unwrap();
    let client = client_for(&hub);
    let peer = make_peer();
    let first = client
        .register_peer(peer.clone(), Default::default())
        .await
        .unwrap();

    store.fault_next_put(PutFault::IndeterminateWritten);
    store.fail_next_resolve();
    let answer = client.register_peer(peer.clone(), Default::default()).await;
    assert!(
        matches!(
            &answer,
            Err(velo_hub::client::ClientError::Hub {
                code: velo_hub::protocol::ErrorCode::Recovered,
                ..
            })
        ),
        "{answer:?}"
    );
    assert!(client.is_registered(), "the client keeps the credential");
    assert!(*client.recovered().borrow());
    let again = client
        .register_peer(peer.clone(), Default::default())
        .await
        .expect("the credential recovers the reserved entry");
    assert_ne!(again.epoch, first.epoch);
    assert!(!*client.recovered().borrow());
}

// ---- a list of hosts ------------------------------------------------------

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// A hub at `host` on `ports`, on `store`.
async fn hub_at(host: &str, ports: (u16, u16), store: Arc<dyn HubStore>) -> Hub {
    let mut config = config();
    config.server.bind_addr = host.parse().unwrap();
    config.server.public_port = ports.0;
    config.server.control_port = ports.1;
    Hub::builder(config).store(store).serve().await.unwrap()
}

fn client_on(hosts: &[&str], ports: (u16, u16)) -> HubClient {
    HubClient::builder()
        .hosts(hosts.iter().copied())
        .public_port(ports.0)
        .control_port(ports.1)
        .build()
        .unwrap()
}

/// A host where nothing listens is skipped: the request never left, so
/// the next host gets it, and later calls go to that host first.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dead_first_host_is_skipped() {
    let ports = (free_port(), free_port());
    let hub = hub_at("127.0.0.1", ports, Arc::new(InMemoryStore::default())).await;
    let client = client_on(&["127.0.0.2", "127.0.0.1"], ports);
    let answer = client.register_peer(make_peer(), none()).await.unwrap();
    assert_eq!(answer.hub_id, hub.hub_id());
    assert_eq!(client.control_url().host_str(), Some("127.0.0.1"));
    assert_eq!(client.describe().await.unwrap().hub_id, hub.hub_id());
    client.unregister().await.unwrap();
    hub.shutdown().await.unwrap();
}

/// A hub that is not ready (its store is not live) answers 503 with
/// `not_ready` before it does anything, so the next host gets the request.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hub_that_is_not_ready_is_skipped() {
    let ports = (free_port(), free_port());
    let degraded = Arc::new(FaultStore::default());
    degraded.set_continuity(Continuity::Degraded);
    let first = hub_at(
        "127.0.0.1",
        ports,
        Arc::clone(&degraded) as Arc<dyn HubStore>,
    )
    .await;
    let second = hub_at("127.0.0.2", ports, Arc::new(InMemoryStore::default())).await;
    let client = client_on(&["127.0.0.1", "127.0.0.2"], ports);
    let answer = client.register_peer(make_peer(), none()).await.unwrap();
    assert_eq!(answer.hub_id, second.hub_id());
    assert!(degraded.list().is_empty());
    client.unregister().await.unwrap();
    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
}

/// Every other answer is the hub's own, and the call returns it: a
/// request is never sent to a second hub after one hub answered it. Here
/// a 401 (the first hub holds the instance, and the client has no
/// credential) and an indeterminate 503 (the write's outcome was unknown;
/// it did not apply, but the hub had acted on the request).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hub_answer_is_not_sent_to_the_next_host() {
    use velo_hub::store::testing::PutFault;
    let ports = (free_port(), free_port());
    let store = Arc::new(FaultStore::default());
    let first = hub_at("127.0.0.1", ports, Arc::clone(&store) as Arc<dyn HubStore>).await;
    let second_store = Arc::new(InMemoryStore::default());
    let second = hub_at(
        "127.0.0.2",
        ports,
        Arc::clone(&second_store) as Arc<dyn HubStore>,
    )
    .await;
    let peer = make_peer();
    register_plain(&first, &peer, None).await;
    let client = client_on(&["127.0.0.1", "127.0.0.2"], ports);
    let refused = client.register_peer(peer, none()).await.unwrap_err();
    assert_eq!(refused.code(), Some(ErrorCode::Unauthorized), "{refused}");

    store.fault_next_put(PutFault::IndeterminateNotWritten);
    let unknown = client.register_peer(make_peer(), none()).await.unwrap_err();
    assert_eq!(unknown.code(), Some(ErrorCode::Unavailable), "{unknown}");
    assert!(
        second_store.list().is_empty(),
        "the second hub got a request"
    );
    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
}
