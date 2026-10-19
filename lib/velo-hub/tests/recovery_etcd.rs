// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! A hub restart on an etcd store, at `ETCD_URL` (default
//! `http://127.0.0.1:2379`): the registrations survive, the registrant
//! learns of the restart from the new hub's heartbeat, and gets back in
//! with its credential; nobody else can claim its entry.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use velo_hub::Hub;
use velo_hub::client::HubClient;
use velo_hub::protocol::ErrorCode;
use velo_hub::store::etcd::EtcdStore;
use velo_hub::velo::discovery::ServiceDiscovery;

fn etcd_url() -> String {
    std::env::var("ETCD_URL").unwrap_or_else(|_| "http://127.0.0.1:2379".into())
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn hub_on(prefix: &str, ports: (u16, u16)) -> Hub {
    let store = EtcdStore::builder([etcd_url()])
        .prefix(prefix)
        .wait_for_prefix(Duration::from_secs(15))
        .connect()
        .await
        .expect("connect to etcd");
    let mut config = config();
    config.server.public_port = ports.0;
    config.server.control_port = ports.1;
    config.server.heartbeat_interval = Duration::from_millis(200);
    Hub::builder(config)
        .store(Arc::new(store))
        .velo(velo_builder())
        .serve()
        .await
        .expect("start hub")
}

fn client_for(ports: (u16, u16)) -> HubClient {
    HubClient::builder()
        .host("127.0.0.1")
        .public_port(ports.0)
        .control_port(ports.1)
        .build()
        .unwrap()
}

/// Hub A registers a client that serves a service, then stops. Hub B starts
/// on the same prefix: it reserves the stored registration, so a
/// credential-free claim of the instance is refused, and the old credential
/// answers `recovered`. B's heartbeat tells the client the hub restarted;
/// the client registers again presenting its credential, which B checks
/// against the stored record, and gets a new epoch with its service back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_hub_lets_its_registrant_back_in() {
    let prefix = format!("/velo-hub-test/{}", uuid::Uuid::new_v4());
    let ports = (free_port(), free_port());

    let hub_a = hub_on(&prefix, ports).await;
    let client = client_for(ports);
    let velo = client
        .velo_builder()
        .add_transport(tcp_transport())
        .build()
        .await
        .unwrap();
    let first = client.register(&velo, Default::default()).await.unwrap();
    let _serving = client.register_service("svc").await.unwrap();
    let id = velo.instance_id();
    let hub_a_id = hub_a.hub_id();
    hub_a.shutdown().await.unwrap();

    let hub_b = hub_on(&prefix, ports).await;
    assert_ne!(hub_b.hub_id(), hub_a_id);
    assert!(hub_b.store().get(id).is_some(), "the registration survived");
    assert_eq!(
        post_register(&hub_b, &request(&velo.peer_info()), None)
            .await
            .status(),
        401,
        "a credential-free claim of a recovered entry must be refused"
    );
    let response = http()
        .post(control(&hub_b, &velo_hub::protocol::instance_heartbeat(id)))
        .header(
            velo_hub::protocol::CREDENTIAL_HEADER,
            first.credential.to_header_value(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(error_code(response).await, ErrorCode::Recovered);

    let mut recovered = client.recovered();
    tokio::time::timeout(
        Duration::from_secs(5),
        recovered.wait_for(|restarted| *restarted),
    )
    .await
    .expect("the client did not learn of the restart")
    .unwrap();
    let second = client.register(&velo, Default::default()).await.unwrap();
    assert_ne!(second.epoch, first.epoch);
    assert_eq!(second.hub_id, hub_b.hub_id());
    assert!(!*client.recovered().borrow());
    assert_eq!(
        client.services().get_instances("svc").await.unwrap(),
        [id],
        "the registration carried the service back"
    );
    client.unregister().await.unwrap();
    hub_b.shutdown().await.unwrap();
}

/// A credential from a registration the stored record does not know (a
/// stranger's, or one older than the last recovery) does not get in: the
/// record keeps only the digests of the last registration.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_hub_refuses_a_credential_the_record_does_not_hold() {
    let prefix = format!("/velo-hub-test/{}", uuid::Uuid::new_v4());
    let ports = (free_port(), free_port());
    let hub_a = hub_on(&prefix, ports).await;
    let peer = make_peer_for_velo().await;
    let first = register(&hub_a, &request(&peer), None).await;
    let second = register(&hub_a, &request(&peer), Some(&first.credential)).await;
    hub_a.shutdown().await.unwrap();

    let hub_b = hub_on(&prefix, ports).await;
    // `first` was rotated away by `second`, and the record holds `second`
    // (minted) and `first` (presented): both may recover. A third,
    // unrelated credential may not.
    let stranger = register(&hub_b, &request(&make_peer_for_velo().await), None).await;
    assert_eq!(
        post_register(&hub_b, &request(&peer), Some(&stranger.credential))
            .await
            .status(),
        401
    );
    assert_eq!(
        post_register(&hub_b, &request(&peer), Some(&second.credential))
            .await
            .status(),
        200
    );
    hub_b.shutdown().await.unwrap();
}

/// A peer whose address the hub's velo can decode, for raw registrations.
async fn make_peer_for_velo() -> velo_hub::velo::PeerInfo {
    new_velo().await.peer_info()
}

/// Revoking the hub's lock lease out of band (as if another hub took the
/// prefix after a partition) fences the store, and the hub stops itself:
/// its listeners close without anyone calling `shutdown`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fenced_hub_stops_itself() {
    use velo_hub::store::etcd::etcd_client::Client;
    let prefix = format!("/velo-hub-test/{}", uuid::Uuid::new_v4());
    let ports = (free_port(), free_port());
    let hub = hub_on(&prefix, ports).await;
    let mut etcd = Client::connect([etcd_url()], None).await.unwrap();
    let lock = etcd.get(format!("{prefix}/hub"), None).await.unwrap();
    let lease = lock.kvs().first().expect("the hub holds its lock").lease();
    etcd.lease_revoke(lease).await.unwrap();
    let closed = async {
        for _ in 0..100 {
            if std::net::TcpStream::connect(("127.0.0.1", ports.1)).is_err() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        false
    }
    .await;
    assert!(closed, "a fenced hub kept serving");
    drop(hub);
}

/// The hub tells its velo about each recovered peer, so its probes reach
/// them; when a recovered entry goes (here deleted out of band, as an
/// expiry would), the hub forgets that peer in velo too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recovered_peer_is_forgotten_in_velo_when_its_entry_goes() {
    use velo_hub::store::etcd::etcd_client::Client;
    let prefix = format!("/velo-hub-test/{}", uuid::Uuid::new_v4());
    let ports = (free_port(), free_port());
    let hub_a = hub_on(&prefix, ports).await;
    let peer = make_peer_for_velo().await;
    let owner = peer.instance_id();
    let _ = register(&hub_a, &request(&peer), None).await;
    hub_a.shutdown().await.unwrap();

    let hub_b = hub_on(&prefix, ports).await;
    let velo = hub_b.velo().expect("the hub runs velo");
    assert!(
        velo.primary_transport_key(owner).is_some(),
        "the hub's velo knows the recovered peer"
    );
    let mut etcd = Client::connect([etcd_url()], None).await.unwrap();
    etcd.delete(format!("{prefix}/i/{owner}"), None)
        .await
        .unwrap();
    let mut forgotten = false;
    for _ in 0..100 {
        if velo.primary_transport_key(owner).is_none() {
            forgotten = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        forgotten,
        "velo kept the peer of a recovered entry that went"
    );
    hub_b.shutdown().await.unwrap();
}

/// A fenced hub's stop skips its pinned remove, so
/// the stop reports Ok rather than a store refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fenced_hub_stop_reports_ok() {
    use velo_hub::store::etcd::etcd_client::Client;
    let prefix = format!("/velo-hub-test/{}", uuid::Uuid::new_v4());
    let ports = (free_port(), free_port());
    let hub = hub_on(&prefix, ports).await;
    let mut etcd = Client::connect([etcd_url()], None).await.unwrap();
    let lock = etcd.get(format!("{prefix}/hub"), None).await.unwrap();
    let lease = lock.kvs().first().expect("the hub holds its lock").lease();
    etcd.lease_revoke(lease).await.unwrap();
    let stopped = tokio::time::timeout(Duration::from_secs(20), hub.shutdown())
        .await
        .expect("the fenced hub's stop hung");
    assert!(stopped.is_ok(), "{stopped:?}");
}
