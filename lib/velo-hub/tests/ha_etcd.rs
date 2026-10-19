// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! A standby hub on an etcd store, at `ETCD_URL` (default
//! `http://127.0.0.1:2379`): it waits for the prefix, takes over when the
//! hub that holds it stops or dies, and the registrant gets back in.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::*;
use velo_hub::Hub;
use velo_hub::client::HubClient;
use velo_hub::store::etcd::EtcdStore;

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

async fn hub_on(prefix: &str, ports: (u16, u16), lock_ttl: Duration) -> Hub {
    let store = EtcdStore::builder([etcd_url()])
        .prefix(prefix)
        .lock_ttl(lock_ttl)
        .request_deadline(lock_ttl / 3)
        .wait_for_prefix(Duration::from_secs(60))
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

/// How the active hub goes away.
#[derive(Clone, Copy, Debug)]
enum Exit {
    /// `Hub::shutdown`: the store revokes its lock lease.
    Stop,
    /// The process dies: nothing revokes, the lock lease expires.
    Crash,
}

/// Run hub A, a standby B waiting on the same prefix and ports, and a
/// registrant; make A go away; return how long until B serves and until
/// the registrant is registered with B.
async fn handoff(exit: Exit, lock_ttl: Duration) -> (Duration, Duration) {
    let prefix = format!("/velo-hub-test/{}", uuid::Uuid::new_v4());
    let ports = (free_port(), free_port());

    // A runs on its own runtime, so a crash can stop it without any drop.
    let (a_tx, a_rx) = std::sync::mpsc::channel();
    let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
    let a_prefix = prefix.clone();
    let a_thread = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let hub = runtime.block_on(hub_on(&a_prefix, ports, lock_ttl));
        a_tx.send(hub.hub_id()).unwrap();
        stop_rx.recv().unwrap();
        match exit {
            Exit::Stop => runtime.block_on(hub.shutdown()).unwrap(),
            Exit::Crash => {
                std::mem::forget(hub);
                runtime.shutdown_background();
            }
        }
    });
    let a_id = tokio::task::spawn_blocking(move || a_rx.recv().unwrap())
        .await
        .unwrap();

    let client = client_for(ports);
    let velo = client
        .velo_builder()
        .add_transport(tcp_transport())
        .build()
        .await
        .unwrap();
    let first = client.register(&velo, Default::default()).await.unwrap();
    assert_eq!(first.hub_id, a_id);

    let b_prefix = prefix.clone();
    let standby = tokio::spawn(async move { hub_on(&b_prefix, ports, lock_ttl).await });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!standby.is_finished(), "the standby took a held prefix");

    let gone = Instant::now();
    stop_tx.send(()).unwrap();
    let hub_b = standby.await.unwrap();
    let serving = gone.elapsed();
    tokio::task::spawn_blocking(move || a_thread.join().unwrap())
        .await
        .unwrap();

    let mut recovered = client.recovered();
    tokio::time::timeout(
        Duration::from_secs(10),
        recovered.wait_for(|restarted| *restarted),
    )
    .await
    .expect("the registrant did not learn of the handoff")
    .unwrap();
    let second = client.register(&velo, Default::default()).await.unwrap();
    let registered = gone.elapsed();
    assert_eq!(second.hub_id, hub_b.hub_id());
    assert_ne!(second.epoch, first.epoch);

    client.unregister().await.unwrap();
    hub_b.shutdown().await.unwrap();
    (serving, registered)
}

/// A stopped hub hands the prefix over at once: the standby serves within a
/// second, and the registrant is back within a few heartbeats.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_standby_takes_over_from_a_stopped_hub() {
    let (serving, registered) = handoff(Exit::Stop, Duration::from_secs(3)).await;
    println!("stop: standby serving after {serving:?}, registrant back after {registered:?}");
    assert!(serving < Duration::from_secs(2), "{serving:?}");
}

/// A dead hub's lock lease expires: the standby serves after about one lock
/// TTL, not later.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_standby_takes_over_from_a_dead_hub_after_the_lock_ttl() {
    let lock_ttl = Duration::from_secs(3);
    let (serving, registered) = handoff(Exit::Crash, lock_ttl).await;
    println!("crash: standby serving after {serving:?}, registrant back after {registered:?}");
    assert!(
        serving >= lock_ttl / 2,
        "took the lock before it expired: {serving:?}"
    );
    assert!(serving < lock_ttl * 2, "{serving:?}");
}
