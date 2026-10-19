// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! A standby hub on an etcd store, at `ETCD_URL` (default
//! `http://127.0.0.1:2379`): it waits for the prefix, takes over when the
//! hub that holds it stops, dies, or is cut off from etcd, and the
//! registrant gets back in. A hub that must be cut off reaches etcd
//! through a [`Proxy`].

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::proxy::{Mode, Proxy};
use common::*;
use velo_hub::Hub;
use velo_hub::client::HubClient;
use velo_hub::store::etcd::{EtcdStore, EtcdStoreBuilder};
use velo_hub::store::{Continuity, HubStore};

fn etcd_url() -> String {
    std::env::var("ETCD_URL").unwrap_or_else(|_| "http://127.0.0.1:2379".into())
}

/// etcd's `host:port`, for a proxy in front of it.
fn etcd_addr() -> String {
    etcd_url()
        .trim_start_matches("http://")
        .trim_end_matches('/')
        .to_string()
}

/// A store on `url` with the lock TTL and a request deadline of a third of
/// it, as every hub in this suite.
fn store_on(url: &str, prefix: &str, lock_ttl: Duration) -> EtcdStoreBuilder {
    EtcdStore::builder([url.to_string()])
        .prefix(prefix)
        .lock_ttl(lock_ttl)
        .request_deadline(lock_ttl / 3)
        .wait_for_prefix(Duration::from_secs(60))
}

/// A hub on `store` at `ports`, probing every 200 ms. Also returns the
/// store, for its continuity history.
async fn hub_with(store: EtcdStoreBuilder, ports: (u16, u16)) -> (Hub, Arc<EtcdStore>) {
    hub_at("127.0.0.1", store, ports).await
}

/// As [`hub_with`], listening on `host`.
async fn hub_at(host: &str, store: EtcdStoreBuilder, ports: (u16, u16)) -> (Hub, Arc<EtcdStore>) {
    let store = Arc::new(store.connect().await.expect("connect to etcd"));
    let mut config = config();
    config.server.bind_addr = host.parse().unwrap();
    config.server.public_port = ports.0;
    config.server.control_port = ports.1;
    config.server.heartbeat_interval = Duration::from_millis(200);
    let hub = Hub::builder(config)
        .store(Arc::clone(&store) as Arc<dyn HubStore>)
        .velo(velo_builder())
        .serve()
        .await
        .expect("start hub");
    (hub, store)
}

async fn port_open(port: u16) -> bool {
    tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .is_ok()
}

/// Wait until nothing listens on `port`.
async fn port_closes(port: u16, within: Duration) -> Option<Duration> {
    let started = Instant::now();
    while started.elapsed() < within {
        if !port_open(port).await {
            return Some(started.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    None
}

/// Wait until `store`'s continuity is `want`; how long that took.
async fn continuity_becomes(
    store: &dyn HubStore,
    want: Continuity,
    within: Duration,
) -> Option<Duration> {
    let started = Instant::now();
    let mut continuity = store.continuity();
    tokio::time::timeout(within, continuity.wait_for(|now| *now == want))
        .await
        .ok()
        .map(|_| started.elapsed())
}

/// The ports a hub bound. The first hub of a test binds (0, 0) and the
/// next one reuses its ports: two probe-and-release picks can return the
/// same port, and the hub then fails to bind.
fn ports_of(hub: &Hub) -> (u16, u16) {
    (hub.public_addr().port(), hub.control_addr().port())
}

async fn hub_on(prefix: &str, ports: (u16, u16), lock_ttl: Duration) -> Hub {
    hub_with(store_on(&etcd_url(), prefix, lock_ttl), ports)
        .await
        .0
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
        let hub = runtime.block_on(hub_on(&a_prefix, (0, 0), lock_ttl));
        a_tx.send((hub.hub_id(), ports_of(&hub))).unwrap();
        stop_rx.recv().unwrap();
        match exit {
            Exit::Stop => runtime.block_on(hub.shutdown()).unwrap(),
            Exit::Crash => {
                std::mem::forget(hub);
                runtime.shutdown_background();
            }
        }
    });
    let (a_id, ports) = tokio::task::spawn_blocking(move || a_rx.recv().unwrap())
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

/// A hub cut off from etcd by a partition (packets dropped, nothing reset)
/// is `Degraded` within one lock TTL of the cut, before a standby can take
/// the prefix, and fenced once it reaches etcd again.
///
/// The hub's last answered renewal was sent before the cut, so its lease
/// deadline (send time plus the lock TTL) passes no later than one lock TTL
/// after it, and etcd counts the lease from later still. Without that local
/// deadline, the hub learned of the cut only when the HTTP/2 ping broke its
/// connection, 5 s plus the request deadline later (measured 5.5 s against
/// the standby's 2.5 s). In that window it was `Live` while the standby
/// served: it accepted the old credential and answered 401 to the standby's,
/// and a 401 makes the client drop its registration.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hub_cut_off_from_etcd_is_degraded_before_the_standby_serves() {
    let lock_ttl = Duration::from_secs(3);
    let prefix = format!("/velo-hub-test/{}", uuid::Uuid::new_v4());
    let proxy = Proxy::start(etcd_addr());
    let (hub_a, store_a) = hub_with(store_on(&proxy.url(), &prefix, lock_ttl), (0, 0)).await;
    let ports_a = ports_of(&hub_a);
    let client = client_for(ports_a);
    let velo = client
        .velo_builder()
        .add_transport(tcp_transport())
        .build()
        .await
        .unwrap();
    let owner = velo.instance_id();
    let first = client.register(&velo, Default::default()).await.unwrap();
    let b_prefix = prefix.clone();
    let standby = tokio::spawn(async move {
        hub_with(store_on(&etcd_url(), &b_prefix, lock_ttl), (0, 0))
            .await
            .0
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!standby.is_finished(), "the standby took a held prefix");

    let cut = Instant::now();
    let degraded = tokio::spawn({
        let mut continuity = hub_a.store().continuity();
        async move {
            continuity
                .wait_for(|now| *now != Continuity::Live)
                .await
                .unwrap();
            cut.elapsed()
        }
    });
    proxy.set(Mode::Blackhole);
    let hub_b = standby.await.unwrap();
    let serving = cut.elapsed();
    let at_takeover = *hub_a.store().continuity().borrow();
    let degraded = tokio::time::timeout(Duration::from_secs(10), degraded)
        .await
        .expect("the cut-off hub stayed Live")
        .unwrap();
    println!("black hole: A degraded after {degraded:?}, B serving after {serving:?}");
    assert_ne!(at_takeover, Continuity::Live, "A was Live when B served");
    assert!(
        degraded <= lock_ttl + Duration::from_millis(250),
        "A degraded {degraded:?} after the cut, past the lock TTL"
    );
    assert!(
        !store_a.continuity_history().contains(&Continuity::Fenced),
        "A was fenced without an answer from etcd"
    );

    // B lets the registrant back in with its credential; A, cut off,
    // answers B's credential 503 (not live), not 401.
    let on_b = register(&hub_b, &request(&velo.peer_info()), Some(&first.credential)).await;
    assert_eq!(
        heartbeat_status(&hub_a, owner, Some(&on_b.credential)).await,
        503
    );
    assert_eq!(
        heartbeat_status(&hub_a, owner, Some(&first.credential)).await,
        503
    );

    // A reaches etcd again, learns that B holds the prefix, and stops.
    let restored = Instant::now();
    proxy.set(Mode::Forward);
    let fenced = continuity_becomes(
        hub_a.store().as_ref(),
        Continuity::Fenced,
        Duration::from_secs(10),
    )
    .await
    .expect("A was not fenced after it reached etcd again");
    port_closes(ports_a.1, Duration::from_secs(10))
        .await
        .expect("the fenced hub kept serving");
    println!(
        "after restore: A fenced after {fenced:?}, stopped after {:?}",
        restored.elapsed()
    );
    drop(client);
    hub_b.shutdown().await.unwrap();
    drop(hub_a);
}

/// A cut shorter than the lease's TTL at etcd, but past the hub's local
/// deadline, degrades the hub and then lets it go back to `Live`: etcd
/// answers the next renewal with a TTL above zero, so the lease never
/// lapsed and no other hub can have taken the prefix. Fencing here would
/// turn every etcd outage longer than the deadline (an etcd leader change
/// extends every lease) into a hub restart and a re-registration of every
/// registrant.
///
/// On one local etcd node the window between the two deadlines is near
/// zero, so the store keeps a local deadline (3 s) shorter than the TTL it
/// grants (9 s), and the cut (4.5 s) falls between them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cut_past_the_local_deadline_but_inside_the_ttl_degrades_and_recovers() {
    let prefix = format!("/velo-hub-test/{}", uuid::Uuid::new_v4());
    let proxy = Proxy::start(etcd_addr());
    let local = Duration::from_secs(3);
    let store = store_on(&proxy.url(), &prefix, Duration::from_secs(9))
        .lock_deadline(local)
        .request_deadline(Duration::from_secs(1));
    let (hub, store) = hub_with(store, (0, 0)).await;
    let ports = ports_of(&hub);
    let cut = Instant::now();
    proxy.set(Mode::Blackhole);
    let degraded = continuity_becomes(
        hub.store().as_ref(),
        Continuity::Degraded,
        Duration::from_secs(4),
    )
    .await
    .expect("the hub stayed Live past its local deadline");
    assert!(
        degraded <= local + Duration::from_millis(250),
        "degraded {degraded:?} after the cut, past the local deadline"
    );
    tokio::time::sleep(Duration::from_millis(4500).saturating_sub(cut.elapsed())).await;
    let restored = Instant::now();
    proxy.set(Mode::Forward);
    let live = continuity_becomes(
        hub.store().as_ref(),
        Continuity::Live,
        Duration::from_secs(10),
    )
    .await
    .expect("the hub did not go back to Live");
    println!("cut 4.5 s: degraded after {degraded:?}, Live again {live:?} after the restore");
    assert!(restored.elapsed() < Duration::from_secs(10));
    let history = store.continuity_history();
    assert!(!history.contains(&Continuity::Fenced), "{history:?}");
    assert!(port_open(ports.1).await, "the hub stopped");
    hub.shutdown().await.unwrap();
}

/// A cut longer than the lock TTL: the hub is `Degraded` for the whole
/// cut, never `Fenced` while it cannot reach etcd, and fenced on the first
/// answer from etcd after it (the lease is gone), after which it stops.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cut_past_the_lock_ttl_fences_on_reconnect() {
    let lock_ttl = Duration::from_secs(3);
    let prefix = format!("/velo-hub-test/{}", uuid::Uuid::new_v4());
    let proxy = Proxy::start(etcd_addr());
    let (hub, store) = hub_with(store_on(&proxy.url(), &prefix, lock_ttl), (0, 0)).await;
    let ports = ports_of(&hub);
    let cut = Instant::now();
    proxy.set(Mode::Blackhole);
    tokio::time::sleep(lock_ttl + Duration::from_millis(500)).await;
    assert_eq!(
        *hub.store().continuity().borrow(),
        Continuity::Degraded,
        "{:?} after the cut",
        cut.elapsed()
    );
    tokio::time::sleep(Duration::from_millis(5000).saturating_sub(cut.elapsed())).await;
    assert!(
        !store.continuity_history().contains(&Continuity::Fenced),
        "fenced without an answer from etcd"
    );
    let restored = Instant::now();
    proxy.set(Mode::Forward);
    let fenced = continuity_becomes(
        hub.store().as_ref(),
        Continuity::Fenced,
        Duration::from_secs(10),
    )
    .await
    .expect("the hub was not fenced after it reached etcd again");
    port_closes(ports.1, Duration::from_secs(10))
        .await
        .expect("the fenced hub kept serving");
    println!(
        "cut 5 s: fenced {fenced:?} after the restore, stopped {:?} after it",
        restored.elapsed()
    );
    drop(hub);
}

/// A standby that waits for the prefix without a limit outlives a short
/// etcd outage, and takes the prefix when the holder frees it. A
/// deployment whose standby gave up on any error but "prefix held" (a lease
/// grant that could not connect, for example) survived an outage only as
/// a crash loop.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_waiting_standby_outlives_a_short_etcd_outage() {
    let lock_ttl = Duration::from_secs(3);
    let prefix = format!("/velo-hub-test/{}", uuid::Uuid::new_v4());
    let holder = store_on(&etcd_url(), &prefix, lock_ttl)
        .connect()
        .await
        .unwrap();
    let proxy = Proxy::start(etcd_addr());
    let standby = tokio::spawn(
        store_on(&proxy.url(), &prefix, lock_ttl)
            .wait_for_prefix(Duration::MAX)
            .connect(),
    );
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert!(!standby.is_finished(), "the standby took a held prefix");
    proxy.set(Mode::Refuse);
    tokio::time::sleep(Duration::from_millis(600)).await;
    proxy.set(Mode::Forward);
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert!(
        !standby.is_finished(),
        "the standby stopped waiting during the outage: {:?}",
        standby.await.unwrap().map(|_| ())
    );
    let freed = Instant::now();
    holder.close().await;
    let store = tokio::time::timeout(Duration::from_secs(5), standby)
        .await
        .expect("the standby did not take the freed prefix")
        .unwrap()
        .expect("the standby failed to connect");
    println!(
        "standby took the prefix {:?} after it was freed",
        freed.elapsed()
    );
    store.close().await;
}

/// A registrant settles on the standby while the old hub, cut off from
/// etcd, is still up and still probes it. The client knows both hosts.
///
/// The old hub is `Degraded` for the whole cut and keeps probing its
/// registrants, so that an outage longer than the registrant TTL does not
/// reap them all once it is `Live` again. Its probes carry its own hub id.
/// The client registered there first, so it ignores them: before, each one
/// raised `recovered`, and an application that registers again on
/// `recovered` minted a new epoch per probe (8 in 1.5 s, measured).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_registrant_settles_on_the_standby_while_the_old_hub_probes() {
    let lock_ttl = Duration::from_secs(3);
    let prefix = format!("/velo-hub-test/{}", uuid::Uuid::new_v4());
    let proxy = Proxy::start(etcd_addr());
    // B listens on another address with A's ports, as standbys share ports.
    let (hub_a, _) = hub_at(
        "127.0.0.1",
        store_on(&proxy.url(), &prefix, lock_ttl),
        (0, 0),
    )
    .await;
    let ports = ports_of(&hub_a);
    let b_prefix = prefix.clone();
    let standby = tokio::spawn(async move {
        hub_at(
            "127.0.0.2",
            store_on(&etcd_url(), &b_prefix, lock_ttl),
            ports,
        )
        .await
        .0
    });
    let client = HubClient::builder()
        .hosts(["127.0.0.1", "127.0.0.2"])
        .public_port(ports.0)
        .control_port(ports.1)
        .build()
        .unwrap();
    let velo = client
        .velo_builder()
        .add_transport(tcp_transport())
        .build()
        .await
        .unwrap();
    let first = client.register(&velo, Default::default()).await.unwrap();
    assert_eq!(first.hub_id, hub_a.hub_id());

    let cut = Instant::now();
    proxy.set(Mode::Refuse);
    let hub_b = standby.await.unwrap();
    let mut recovered = client.recovered();
    tokio::time::timeout(Duration::from_secs(10), recovered.wait_for(|now| *now))
        .await
        .expect("no probe from the standby")
        .unwrap();
    // A is not ready, so the call moves on to B, which recovers the entry.
    let second = client.register(&velo, Default::default()).await.unwrap();
    println!("registrant on B {:?} after the cut", cut.elapsed());
    assert_eq!(second.hub_id, hub_b.hub_id());
    assert_ne!(second.epoch, first.epoch);
    assert!(!*client.recovered().borrow());

    // A still runs and probes every 200 ms; none of it moves the client.
    let quiet = tokio::time::timeout(Duration::from_millis(1500), recovered.wait_for(|now| *now))
        .await
        .is_err();
    assert!(quiet, "a probe from the old hub raised `recovered`");
    assert_eq!(client.registration().unwrap().epoch, second.epoch);
    assert_eq!(*hub_a.store().continuity().borrow(), Continuity::Degraded);
    assert!(port_open(ports.1).await, "control: the old hub still runs");

    client.unregister().await.unwrap();
    hub_b.shutdown().await.unwrap();
    drop(hub_a);
}
