// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! A miss in the peer table must not lose a call.
//!
//! A node routes a reply by the sender's `WorkerId`, looked up in its own peer
//! table. That table is filled by `register_peer` and by `_hello`, and emptied
//! by `unregister_peer`, so a node may forget a peer that is still calling it.
//!
//! These tests pin the rule for that case. On a miss, with a discovery
//! configured, the node resolves the peer, registers it, and sends. Without a
//! discovery, the miss behaves as it always has: a logged error and no reply.
//! A send that names a peer by instance id resolves the same way.

use super::*;
use crate::messenger::client::PeerKey;
use crate::transports::{MessageType, TransportErrorHandler, VeloBackend};
use bytes::Bytes;
use futures::future::BoxFuture;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Semaphore;

/// A discovery backend that learns one peer at run time.
///
/// The peer is set after both nodes exist, because a node's address is only
/// known once it is built.
#[derive(Default)]
struct SharedDiscovery {
    peer: parking_lot::Mutex<Option<PeerInfo>>,
    /// Lookups of either kind.
    lookups: AtomicUsize,
    instance_lookups: AtomicUsize,
    /// Lookups whose future is still alive: running, or held at the gate.
    /// Zero after a caller gave up shows its lookup ended with it.
    live: AtomicUsize,
    /// When set, a lookup waits for one permit before it answers.
    gate: Option<Arc<Semaphore>>,
    /// A lookup never answers.
    hang: bool,
    /// A lookup blocks its thread this long, then answers. On a one-thread
    /// runtime every timer that came due meanwhile fires only afterwards.
    block: Option<Duration>,
}

/// Counts a live lookup future down when it is dropped, however it ends.
struct Live<'a>(&'a AtomicUsize);

impl Drop for Live<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl SharedDiscovery {
    fn new() -> Arc<Self> {
        Arc::default()
    }

    fn gated() -> Arc<Self> {
        Arc::new(Self {
            gate: Some(Arc::new(Semaphore::new(0))),
            ..Self::default()
        })
    }

    fn hung() -> Arc<Self> {
        Arc::new(Self {
            hang: true,
            ..Self::default()
        })
    }

    fn blocking(block: Duration) -> Arc<Self> {
        Arc::new(Self {
            block: Some(block),
            ..Self::default()
        })
    }

    fn learn(&self, peer: PeerInfo) {
        *self.peer.lock() = Some(peer);
    }

    fn lookups(&self) -> usize {
        self.lookups.load(Ordering::SeqCst)
    }

    fn live(&self) -> usize {
        self.live.load(Ordering::SeqCst)
    }

    fn release(&self) {
        self.gate
            .as_ref()
            .expect("a gated discovery")
            .add_permits(4096);
    }

    async fn answer(&self) -> Result<PeerInfo> {
        self.lookups.fetch_add(1, Ordering::SeqCst);
        self.live.fetch_add(1, Ordering::SeqCst);
        let _live = Live(&self.live);
        if let Some(gate) = &self.gate {
            let _permit = gate.acquire().await.expect("gate is never closed");
        }
        if self.hang {
            std::future::pending::<()>().await;
        }
        if let Some(block) = self.block {
            std::thread::sleep(block);
        }
        self.peer
            .lock()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("no peer learned"))
    }
}

impl PeerDiscovery for SharedDiscovery {
    fn discover_by_worker_id(&self, _worker: WorkerId) -> BoxFuture<'_, Result<PeerInfo>> {
        Box::pin(self.answer())
    }

    fn discover_by_instance_id(&self, _instance: InstanceId) -> BoxFuture<'_, Result<PeerInfo>> {
        self.instance_lookups.fetch_add(1, Ordering::SeqCst);
        Box::pin(self.answer())
    }
}

struct NoopErrorHandler;

impl TransportErrorHandler for NoopErrorHandler {
    fn on_error(&self, _header: Bytes, _payload: Bytes, _error: String) {}
}

fn tcp_transport() -> Arc<crate::transports::tcp::TcpTransport> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    Arc::new(
        crate::transports::tcp::TcpTransportBuilder::new()
            .from_listener(listener)
            .unwrap()
            .build()
            .unwrap(),
    )
}

async fn node_with(discovery: Option<Arc<SharedDiscovery>>, reply_bound: Option<Duration>) -> Velo {
    let mut builder = Velo::builder().add_transport(tcp_transport());
    if let Some(discovery) = discovery {
        builder = builder.discovery(discovery);
    }
    if let Some(bound) = reply_bound {
        builder = builder.reply_lookup_timeout(bound);
    }
    builder.build().await.unwrap()
}

async fn node(discovery: Option<Arc<SharedDiscovery>>) -> Velo {
    node_with(discovery, None).await
}

fn backend(node: &Velo) -> Arc<VeloBackend> {
    node.messenger().backend().clone()
}

/// Register a handler that forgets `caller` and then answers, so its reply
/// misses the table.
fn forget_then_answer(callee: &Velo, caller: &Velo) {
    let weak_callee = callee.downgrade();
    let caller_id = caller.instance_id();
    callee
        .register_handler(
            Handler::unary_handler("forget_and_answer", move |_| {
                if let Some(callee) = weak_callee.upgrade() {
                    callee.unregister_peer(caller_id);
                }
                Ok(Some(Bytes::from_static(b"answer")))
            })
            .build(),
        )
        .unwrap();
}

/// Poll `done` until it holds, failing after ten seconds.
async fn eventually(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(std::time::Instant::now() < deadline, "{what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn close(nodes: &[&Velo]) {
    for node in nodes {
        node.shutdown(ShutdownPolicy::Timeout(Duration::from_secs(2)))
            .await;
    }
}

#[tokio::test]
async fn reply_reaches_a_caller_the_callee_unregistered() {
    let discovery = SharedDiscovery::new();
    let callee = node(Some(discovery.clone())).await;
    let caller = node(None).await;
    discovery.learn(caller.peer_info());
    forget_then_answer(&callee, &caller);

    caller.register_peer(callee.peer_info()).unwrap();
    let reply = caller
        .unary("forget_and_answer")
        .unwrap()
        .instance(callee.instance_id())
        .timeout(Duration::from_secs(5))
        .send()
        .await;

    assert_eq!(
        reply.expect("the reply was lost: the callee forgot the caller"),
        Bytes::from_static(b"answer")
    );
    assert_eq!(discovery.lookups(), 1, "the reply resolves the caller once");

    close(&[&callee, &caller]).await;
}

/// Without discovery a reply miss is what it always was: the handler counts
/// one failed reply, and discovery counts nothing.
#[tokio::test]
async fn reply_miss_without_discovery_fails_as_today() {
    use crate::observability::test_helpers::MetricSnapshot;

    let registry = prometheus::Registry::new();
    let metrics = Arc::new(crate::observability::VeloMetrics::register(&registry).unwrap());
    let callee = Velo::builder()
        .add_transport(tcp_transport())
        .metrics(metrics)
        .build()
        .await
        .unwrap();
    let caller = node(None).await;
    forget_then_answer(&callee, &caller);

    caller.register_peer(callee.peer_info()).unwrap();
    let result = caller
        .unary("forget_and_answer")
        .unwrap()
        .instance(callee.instance_id())
        .timeout(Duration::from_millis(500))
        .send()
        .await;

    let error = result.expect_err("without discovery the reply cannot be routed");
    assert!(
        error.downcast_ref::<RequestTimeout>().is_some(),
        "expected the caller's own timeout, got {error:#}"
    );
    let failed_replies = || {
        MetricSnapshot::from_registry(&registry).counter_sum(
            "velo_messenger_dispatch_failures_total",
            &[("stage", "response_send")],
        )
    };
    eventually("the handler never counted its failed reply", || {
        failed_replies() == 1.0
    })
    .await;
    let discovery_errors = MetricSnapshot::from_registry(&registry).counter(
        "velo_messenger_client_resolution_total",
        &[("path", "discovery"), ("outcome", "error")],
    );
    assert_eq!(discovery_errors, 0.0, "no discovery, so no discovery miss");

    close(&[&callee, &caller]).await;
}

/// A refused lookup sends nothing back. The callee cannot reach the caller,
/// so it must not pretend to answer it; the caller hears its own timeout.
#[tokio::test]
async fn a_refused_lookup_leaves_the_caller_to_its_timeout() {
    // Discovery that knows no peer refuses every lookup.
    let discovery = SharedDiscovery::new();
    let callee = node(Some(discovery.clone())).await;
    let caller = node(None).await;
    forget_then_answer(&callee, &caller);

    caller.register_peer(callee.peer_info()).unwrap();
    let error = caller
        .unary("forget_and_answer")
        .unwrap()
        .instance(callee.instance_id())
        .timeout(Duration::from_millis(500))
        .send()
        .await
        .expect_err("the callee's discovery refuses the caller");
    assert!(
        error.downcast_ref::<RequestTimeout>().is_some(),
        "expected the caller's own timeout, got {error:#}"
    );
    // The timeout can come first: a slow handshake still sends, and the
    // handler asks discovery only when its reply misses.
    eventually("the reply never asked discovery", || {
        discovery.lookups() == 1
    })
    .await;

    close(&[&callee, &caller]).await;
}

#[tokio::test]
async fn instance_send_to_an_unregistered_peer_resolves_through_discovery() {
    let discovery = SharedDiscovery::new();
    let caller = node(Some(discovery.clone())).await;
    let callee = node(None).await;
    discovery.learn(callee.peer_info());

    callee
        .register_handler(
            Handler::unary_handler("ping", |_| Ok(Some(Bytes::from_static(b"pong")))).build(),
        )
        .unwrap();

    // No `register_peer` for the callee: the caller has never heard of it.
    let reply = caller
        .unary("ping")
        .unwrap()
        .instance(callee.instance_id())
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("a send to an unregistered instance must resolve through discovery");

    assert_eq!(reply, Bytes::from_static(b"pong"));
    assert_eq!(discovery.instance_lookups.load(Ordering::SeqCst), 1);
    assert_eq!(discovery.lookups(), 1);

    close(&[&caller, &callee]).await;
}

/// An `.instance` send whose lookup outlives the call's timeout must not send
/// once discovery answers: the caller gave up and released its slot. The
/// call's deadline bounds the lookup, which ends with its only waiter.
#[tokio::test]
async fn a_timed_out_instance_send_never_sends() {
    let discovery = SharedDiscovery::gated();
    let caller = node(Some(discovery.clone())).await;
    let callee = node(None).await;
    discovery.learn(callee.peer_info());
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    callee
        .register_handler(
            Handler::unary_handler("ping", move |_| {
                seen.fetch_add(1, Ordering::SeqCst);
                Ok(Some(Bytes::from_static(b"pong")))
            })
            .build(),
        )
        .unwrap();

    let error = caller
        .unary("ping")
        .unwrap()
        .instance(callee.instance_id())
        .timeout(Duration::from_millis(100))
        .send()
        .await
        .expect_err("discovery is held past the call's timeout");
    assert!(
        error.downcast_ref::<RequestTimeout>().is_some(),
        "{error:#}"
    );

    let caller_backend = backend(&caller);
    eventually("the lookup outlived its call", || discovery.live() == 0).await;
    discovery.release();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !caller_backend.is_registered(callee.instance_id()),
        "a cancelled lookup registered its answer"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "the send went after its timeout"
    );

    close(&[&caller, &callee]).await;
}

/// `unregister_peer` removes a peer's route before its worker entry. A reply
/// that found the worker and then the route gone has sent nothing, so it
/// resolves the peer again instead of losing the reply.
///
/// It asks by the worker id the reply was addressed to, as any reply miss
/// does.
#[tokio::test]
async fn a_reply_whose_route_went_after_its_worker_hit_resolves_again() {
    let discovery = SharedDiscovery::new();
    let node_a = node(Some(discovery.clone())).await;
    let peer = node(None).await;
    discovery.learn(peer.peer_info());
    node_a.register_peer(peer.peer_info()).unwrap();

    let backend = backend(&node_a);
    backend.drop_route(peer.instance_id());
    backend
        .send_reply(
            peer.instance_id().worker_id(),
            Bytes::new(),
            Bytes::new(),
            MessageType::Response,
            Arc::new(NoopErrorHandler),
        )
        .await
        .expect("the reply resolves the peer whose route went");
    assert_eq!(discovery.lookups(), 1);
    assert!(backend.is_registered(peer.instance_id()));

    close(&[&node_a, &peer]).await;
}

#[tokio::test]
async fn a_table_hit_makes_no_discovery_call() {
    let discovery = SharedDiscovery::new();
    let callee = node(Some(discovery.clone())).await;
    let caller = node(Some(discovery.clone())).await;
    discovery.learn(caller.peer_info());

    callee
        .register_handler(
            Handler::unary_handler("ping", |_| Ok(Some(Bytes::from_static(b"pong")))).build(),
        )
        .unwrap();
    caller.register_peer(callee.peer_info()).unwrap();

    // The handshake registers the caller on the callee, so every reply hits.
    for _ in 0..4 {
        caller
            .unary("ping")
            .unwrap()
            .instance(callee.instance_id())
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .expect("the call must answer");
    }
    assert_eq!(discovery.lookups(), 0, "a table hit must not ask discovery");

    close(&[&callee, &caller]).await;
}

/// A reply whose peer is in the table costs what its send costs: it finishes
/// in its first poll, and it makes the same allocations as a plain send of
/// the same frame.
#[tokio::test]
async fn a_reply_hit_costs_what_its_send_costs() {
    use futures::FutureExt;

    let discovery = SharedDiscovery::new();
    let node_a = node(Some(discovery.clone())).await;
    let peer = node(None).await;
    node_a.register_peer(peer.peer_info()).unwrap();
    let backend = backend(&node_a);
    let handler: Arc<dyn TransportErrorHandler> = Arc::new(NoopErrorHandler);
    let send = || {
        backend
            .send_message_on_lane(
                peer.instance_id(),
                0,
                Bytes::new(),
                Bytes::new(),
                MessageType::Response,
                handler.clone(),
            )
            .expect("the peer is registered")
    };
    // Let the connection open, so neither count pays for it.
    for _ in 0..4 {
        send();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (_, plain) = crate::test_alloc::allocations_in(send);
    let (reply, cost) = crate::test_alloc::allocations_in(|| {
        backend
            .send_reply(
                peer.instance_id().worker_id(),
                Bytes::new(),
                Bytes::new(),
                MessageType::Response,
                handler.clone(),
            )
            .now_or_never()
    });

    assert!(
        matches!(reply, Some(Ok(()))),
        "a hit must finish in its first poll"
    );
    assert_eq!(cost, plain, "a hit allocated more than its send");
    assert_eq!(discovery.lookups(), 0);

    close(&[&node_a, &peer]).await;
}

/// The builder's bound, not the default, ends a reply's lookup, and the lookup
/// task ends with it. On a paused clock the wait is exactly the bound, so a
/// path that ignored the setting and waited the default 5 s would fail here.
#[tokio::test]
async fn a_reply_lookup_gives_up_at_the_builder_bound() {
    let discovery = SharedDiscovery::hung();
    let node_a = node_with(Some(discovery.clone()), Some(Duration::from_millis(200))).await;
    let backend = backend(&node_a);

    tokio::time::pause();
    let started = tokio::time::Instant::now();
    let result = backend
        .send_reply(
            WorkerId::from_u64(7),
            Bytes::new(),
            Bytes::new(),
            MessageType::Response,
            Arc::new(NoopErrorHandler),
        )
        .await;
    let waited = started.elapsed();
    tokio::time::resume();

    let error = result.expect_err("discovery never answers");
    assert!(
        format!("{error:#}").contains("ran out of time"),
        "{error:#}"
    );
    assert!(
        waited >= Duration::from_millis(200) && waited < Duration::from_secs(1),
        "the reply waited {waited:?}, not the 200 ms bound"
    );
    eventually("the lookup outlived its reply", || discovery.live() == 0).await;

    close(&[&node_a]).await;
}

/// Register a `ping` handler on `callee` that counts its calls.
fn counting_ping(callee: &Velo) -> Arc<AtomicUsize> {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    callee
        .register_handler(
            Handler::unary_handler("ping", move |_| {
                seen.fetch_add(1, Ordering::SeqCst);
                Ok(Some(Bytes::from_static(b"pong")))
            })
            .build(),
        )
        .unwrap();
    calls
}

/// Discovery that answers with a peer the miss did not ask for must not
/// deliver the call to that peer. A worker id is a hash of an instance id, so
/// two instances can share one; lookups are shared by the id the miss named,
/// and an answer is checked against it, so neither a hash collision nor a
/// wrong discovery record sends one peer's frame to another.
#[tokio::test]
async fn a_lookup_answer_for_another_peer_is_refused() {
    // This discovery answers every id with the one peer it knows.
    let discovery = SharedDiscovery::new();
    let caller = node(Some(discovery.clone())).await;
    let callee = node(None).await;
    discovery.learn(callee.peer_info());
    let calls = counting_ping(&callee);

    let error = caller
        .unary("ping")
        .unwrap()
        .instance(InstanceId::new_v4())
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect_err("discovery answered with another peer");
    assert!(format!("{error:#}").contains("answered"), "{error:#}");
    assert_eq!(discovery.lookups(), 1);
    assert!(
        !backend(&caller).is_registered(callee.instance_id()),
        "the wrong peer was registered"
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "the wrong peer got the call"
    );

    close(&[&caller, &callee]).await;
}

/// A lookup answer that is read after the call's deadline must not send. The
/// lookup runs on its own task, so its answer can be ready when the waiter is
/// next polled, after the deadline and before the timer is looked at. Here the
/// lookup blocks the one runtime thread past the deadline, which makes that
/// order certain. This is the lookup's boundary; the handshake's is
/// `a_slow_handshake_still_sends_after_the_deadline`.
#[tokio::test]
async fn a_lookup_answer_after_the_deadline_never_sends() {
    let discovery = SharedDiscovery::blocking(Duration::from_millis(120));
    let caller = node(Some(discovery.clone())).await;
    let callee = node(None).await;
    discovery.learn(callee.peer_info());
    let calls = counting_ping(&callee);

    let error = caller
        .unary("ping")
        .unwrap()
        .instance(callee.instance_id())
        .timeout(Duration::from_millis(20))
        .send()
        .await
        .expect_err("discovery answers after the call's timeout");
    assert!(
        error.downcast_ref::<RequestTimeout>().is_some(),
        "{error:#}"
    );
    assert_eq!(discovery.lookups(), 1);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "the send went after its timeout"
    );

    close(&[&caller, &callee]).await;
}

/// The call's deadline does not bound the handshake. A send to a peer in the
/// table whose handshake finishes after the deadline is still made, as it was
/// before discovery resolved misses: the handshake has its own timeout, and a
/// node without discovery must see no change. The callee runs on its own
/// thread so the test can stall it while the handshake waits.
#[tokio::test]
async fn a_slow_handshake_still_sends_after_the_deadline() {
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let remote = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let callee = node(None).await;
            let calls = counting_ping(&callee);
            let handle = tokio::runtime::Handle::current();
            ready_tx.send((callee.peer_info(), calls, handle)).unwrap();
            let _ = stop_rx.await;
            close(&[&callee]).await;
        });
    });
    let (peer, calls, callee_runtime) = ready_rx.recv().unwrap();
    let caller = node(None).await;
    caller.register_peer(peer.clone()).unwrap();

    // Stall the callee before the handshake reaches it, and hold it until
    // the caller has its timeout. A fixed sleep here could end first on a
    // loaded host, and the call would then get its answer.
    let (stalled_tx, stalled_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    callee_runtime.spawn(async move {
        stalled_tx.send(()).unwrap();
        // Opens on the release, or when the test drops the sender.
        let _ = release_rx.recv();
    });
    stalled_rx.recv().unwrap();

    let error = caller
        .unary("ping")
        .unwrap()
        .instance(peer.instance_id())
        .timeout(Duration::from_millis(50))
        .send()
        .await
        .expect_err("the handshake outlasts the call's timeout");
    assert!(
        error.downcast_ref::<RequestTimeout>().is_some(),
        "{error:#}"
    );
    release_tx.send(()).unwrap();
    eventually("the send was dropped after a slow handshake", || {
        calls.load(Ordering::SeqCst) == 1
    })
    .await;

    close(&[&caller]).await;
    stop_tx.send(()).unwrap();
    remote.join().unwrap();
}

/// A call whose lookup ran out at its deadline resolves to `RequestTimeout`,
/// also for a caller that waited in `admitted()` and so read the failed
/// admission first. `admitted()` itself is not bounded and reports the lookup.
#[tokio::test]
async fn a_lookup_that_runs_out_reads_as_a_timeout_after_admitted() {
    let discovery = SharedDiscovery::hung();
    let caller = node(Some(discovery.clone())).await;

    let mut call = caller
        .unary("ping")
        .unwrap()
        .instance(InstanceId::new_v4())
        .timeout(Duration::from_millis(100))
        .send();
    let refused = call
        .admitted()
        .await
        .expect_err("the lookup ran out of time");
    assert!(
        refused.downcast_ref::<RequestTimeout>().is_none(),
        "{refused:#}"
    );
    let error = call.await.expect_err("the call timed out");
    assert!(
        error.downcast_ref::<RequestTimeout>().is_some(),
        "expected the typed timeout, got {error:#}"
    );

    close(&[&caller]).await;
}

/// A reply bound too large to add to the clock does not panic; the lookup
/// keeps its fallback bound. Adding it used to panic in the reply's task,
/// which lost the reply.
#[tokio::test]
async fn a_reply_bound_too_large_to_add_falls_back() {
    let discovery = SharedDiscovery::new();
    let node_a = node_with(Some(discovery.clone()), Some(Duration::MAX)).await;
    let peer = node(None).await;
    discovery.learn(peer.peer_info());

    backend(&node_a)
        .send_reply(
            peer.instance_id().worker_id(),
            Bytes::new(),
            Bytes::new(),
            MessageType::Response,
            Arc::new(NoopErrorHandler),
        )
        .await
        .expect("the reply resolves its peer");
    assert_eq!(discovery.lookups(), 1);

    close(&[&node_a, &peer]).await;
}

/// A reply's bound refuses a lookup answer that is read after it. The lookup
/// runs on its own task, so `timeout_at` can read the answer after the bound
/// and before the timer; the blocked thread makes that order certain.
#[tokio::test]
async fn a_reply_lookup_answer_after_the_bound_sends_nothing() {
    let discovery = SharedDiscovery::blocking(Duration::from_millis(110));
    let node_a = node_with(Some(discovery.clone()), Some(Duration::from_millis(15))).await;
    let peer = node(None).await;
    discovery.learn(peer.peer_info());

    let error = backend(&node_a)
        .send_reply(
            peer.instance_id().worker_id(),
            Bytes::new(),
            Bytes::new(),
            MessageType::Response,
            Arc::new(NoopErrorHandler),
        )
        .await
        .expect_err("discovery answered after the reply's bound");
    assert!(
        format!("{error:#}").contains("ran out of time"),
        "{error:#}"
    );
    assert_eq!(discovery.lookups(), 1);

    close(&[&node_a, &peer]).await;
}

/// A reply names its peer only by worker id. Discovery that answers with a
/// peer of another worker id is refused: the peer is not registered and the
/// reply is not sent to it.
#[tokio::test]
async fn a_reply_lookup_answer_for_another_worker_is_refused() {
    let discovery = SharedDiscovery::new();
    let node_a = node(Some(discovery.clone())).await;
    let peer = node(None).await;
    discovery.learn(peer.peer_info());

    let error = backend(&node_a)
        .send_reply(
            InstanceId::new_v4().worker_id(),
            Bytes::new(),
            Bytes::new(),
            MessageType::Response,
            Arc::new(NoopErrorHandler),
        )
        .await
        .expect_err("discovery answered with another worker");
    assert!(format!("{error:#}").contains("answered"), "{error:#}");
    assert!(
        !backend(&node_a).is_registered(peer.instance_id()),
        "the wrong peer was registered"
    );

    close(&[&node_a, &peer]).await;
}

/// Without discovery, a send to an unknown worker fails with the same
/// configuration error whenever it is read. Only a lookup that the deadline
/// ended reads as `RequestTimeout`; a result first read after its deadline
/// must not turn another failure into one.
#[tokio::test]
async fn a_miss_without_discovery_read_late_keeps_its_error() {
    let caller = node(None).await;

    let call = caller
        .unary("ping")
        .unwrap()
        .worker(InstanceId::new_v4().worker_id())
        .timeout(Duration::from_millis(20))
        .send();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let error = call.await.expect_err("no discovery, so no peer");
    assert!(
        error.downcast_ref::<RequestTimeout>().is_none(),
        "the configuration error read as a timeout"
    );
    assert!(
        format!("{error:#}").contains("No discovery backend"),
        "{error:#}"
    );

    close(&[&caller]).await;
}

/// The trust boundary, as a choice. A reply knows only the worker id of the
/// peer it answers: the request carries no instance id. So a reply that
/// misses asks discovery for that worker id, once, and sends to the instance
/// discovery names. The node checks that the answer has the worker id it
/// asked for, and nothing more. A worker id is a 64-bit hash of an instance
/// id; if discovery holds another instance with the same worker id, that
/// instance gets the reply. Discovery is trusted not to: the filesystem
/// backend and the hub's stores refuse the later instance at registration,
/// when they can already see the earlier one.
/// Do not add a second
/// lookup or an instance check here without a wire change that gives the
/// reply an instance to check against.
#[tokio::test]
async fn a_reply_miss_trusts_discovery_for_its_worker_id() {
    let discovery = SharedDiscovery::new();
    let node_a = node(Some(discovery.clone())).await;
    let peer = node(None).await;
    discovery.learn(peer.peer_info());

    backend(&node_a)
        .send_reply(
            peer.instance_id().worker_id(),
            Bytes::new(),
            Bytes::new(),
            MessageType::Response,
            Arc::new(NoopErrorHandler),
        )
        .await
        .expect("the reply goes to the instance discovery names");
    assert_eq!(discovery.lookups(), 1, "one lookup, by worker id");
    assert_eq!(
        discovery.instance_lookups.load(Ordering::SeqCst),
        0,
        "a reply has no instance id to ask for"
    );
    assert!(backend(&node_a).is_registered(peer.instance_id()));

    close(&[&node_a, &peer]).await;
}

/// A completion sent to a requester the table forgot is looked up within the
/// reply bound. The handler of a trigger request for an event that is already
/// complete awaits that send. With no bound, a discovery that never answers
/// holds the handler, and a shutdown that waits for handlers never ends.
#[cfg(feature = "services")]
#[tokio::test]
async fn an_event_completion_to_a_forgotten_requester_is_bounded() {
    use crate::messenger::events::messages::EventTriggerRequestMessage;

    let discovery = SharedDiscovery::hung();
    let node_a = node_with(Some(discovery.clone()), Some(Duration::from_millis(200))).await;
    let event = node_a.events().new_event().unwrap();
    let handle = event.handle();
    event.trigger().unwrap();

    let requester = InstanceId::new_v4();
    let message = EventTriggerRequestMessage {
        handle: handle.raw(),
        requester_worker: requester.worker_id().as_u64(),
        requester_instance: requester,
        poisoned: None,
        // Slot and generation zero: a response id of the requester's worker.
        response_id: Some(u128::from(requester.worker_id().as_u64())),
    };
    let payload = Bytes::from(serde_json::to_vec(&message).unwrap());

    let handled = tokio::time::timeout(
        Duration::from_secs(5),
        node_a.events().handle_trigger_request(payload),
    )
    .await
    .expect("the completion waited on discovery past the reply bound");
    handled.expect_err("discovery never finds the requester");
    eventually("the lookup outlived the completion", || {
        discovery.live() == 0
    })
    .await;

    close(&[&node_a]).await;
}

/// A miss whose peer came back before its lookup asked is answered from the
/// table. An earlier miss of the same burst, a `_hello` or a `register_peer`
/// can land while the miss waits for its turn, and discovery may hold no
/// record for the peer at all; asking it anyway would fail a reply that has a
/// route.
#[tokio::test]
async fn a_miss_whose_route_came_back_is_answered_from_the_table() {
    // This discovery knows no peer.
    let discovery = SharedDiscovery::new();
    let node_a = node(Some(discovery.clone())).await;
    let peer = node(None).await;
    node_a.register_peer(peer.peer_info()).unwrap();

    let backend = backend(&node_a);
    let found = backend
        .resolve_peer(PeerKey::Worker(peer.instance_id().worker_id()), None)
        .await
        .expect("the table holds the peer");
    assert_eq!(found, peer.instance_id());
    assert_eq!(discovery.lookups(), 0, "the table had the answer");

    close(&[&node_a, &peer]).await;
}

/// A fire send's timeout bounds its discovery lookup and nothing else. A send
/// that has a route must not carry it: a staged payload would take it as the
/// deadline of the receiver's fetch, and a zero bound would then drop a
/// payload that had a route all along.
#[cfg(feature = "services")]
#[tokio::test]
async fn a_fire_lookup_bound_does_not_reach_a_routed_send() {
    let sender = node(None).await;
    let receiver = node(None).await;
    let got = Arc::new(AtomicUsize::new(0));
    let seen = got.clone();
    receiver
        .register_handler(
            Handler::unary_handler("sink", move |ctx| {
                seen.store(ctx.payload.len(), Ordering::SeqCst);
                Ok(None)
            })
            .build(),
        )
        .unwrap();
    sender.register_peer(receiver.peer_info()).unwrap();

    // Large enough to be staged, so the receiver fetches it.
    let payload = Bytes::from(vec![7u8; 300 * 1024]);
    sender
        .messenger()
        .message_builder_unchecked("sink")
        .raw_payload(payload)
        .instance(receiver.instance_id())
        .timeout(Duration::ZERO)
        .fire()
        .await
        .expect("the send has a route");
    eventually("the staged payload never arrived", || {
        got.load(Ordering::SeqCst) == 300 * 1024
    })
    .await;

    close(&[&sender, &receiver]).await;
}

/// Every reply of a burst to one forgotten peer arrives. Each miss runs its
/// own lookup: the ten replies here are all held at discovery at once, and
/// all ten are sent when it answers. Lookups were shared per peer once; the
/// table of waiters that needed had cancellation and teardown rules that cost
/// more than the lookups it saved. The cap bounds a burst instead.
#[tokio::test]
async fn a_burst_of_replies_to_one_forgotten_peer_all_arrive() {
    let discovery = SharedDiscovery::gated();
    let callee = node(Some(discovery.clone())).await;
    let caller = node(None).await;
    discovery.learn(caller.peer_info());
    forget_then_answer(&callee, &caller);
    caller.register_peer(callee.peer_info()).unwrap();

    let mut calls = Vec::new();
    for _ in 0..10 {
        let caller = caller.clone();
        let callee_id = callee.instance_id();
        calls.push(tokio::spawn(async move {
            caller
                .unary("forget_and_answer")
                .unwrap()
                .instance(callee_id)
                .timeout(Duration::from_secs(10))
                .send()
                .await
        }));
    }
    eventually("ten replies never reached discovery", || {
        discovery.live() == 10
    })
    .await;
    discovery.release();

    for call in calls {
        let reply = call.await.expect("call task panicked");
        assert_eq!(
            reply.expect("a held reply must arrive once discovery answers"),
            Bytes::from_static(b"answer")
        );
    }
    assert_eq!(discovery.lookups(), 10, "one lookup for each miss");
    eventually("a lookup outlived its reply", || discovery.live() == 0).await;

    close(&[&callee, &caller]).await;
}

/// A node runs at most `MAX_CONCURRENT_LOOKUPS` lookups at once. A miss past
/// the cap asks nothing: it waits for a permit inside its own bound, and
/// fails with that bound if none comes. Worker ids come off the wire, so
/// without the cap a burst of misses is a burst of discovery requests.
#[tokio::test]
async fn a_burst_of_misses_is_capped() {
    use crate::transports::MAX_CONCURRENT_LOOKUPS;

    let discovery = SharedDiscovery::gated();
    let node_a = node(Some(discovery.clone())).await;
    let backend = backend(&node_a);

    let extra = 8;
    let mut misses = tokio::task::JoinSet::new();
    for worker in 1..=(MAX_CONCURRENT_LOOKUPS + extra) as u64 {
        let backend = backend.clone();
        misses.spawn(async move {
            backend
                .resolve_peer(PeerKey::Worker(WorkerId::from_u64(worker)), None)
                .await
        });
    }
    eventually("the cap was never reached", || {
        discovery.live() == MAX_CONCURRENT_LOOKUPS
    })
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        discovery.lookups(),
        MAX_CONCURRENT_LOOKUPS,
        "a miss past the cap asked discovery"
    );

    // A bounded miss past the cap gives up at its bound, having asked nothing.
    let deadline = tokio::time::Instant::now() + Duration::from_millis(50);
    let error = backend
        .resolve_peer(
            PeerKey::Worker(WorkerId::from_u64(u64::MAX)),
            Some(deadline),
        )
        .await
        .expect_err("no permit came within the bound");
    assert!(
        format!("{error:#}").contains("ran out of time"),
        "{error:#}"
    );
    assert_eq!(discovery.lookups(), MAX_CONCURRENT_LOOKUPS);

    // This discovery knows no peer, so every lookup ends in an error.
    discovery.release();
    while let Some(miss) = misses.join_next().await {
        miss.unwrap().expect_err("discovery knows no peer");
    }
    assert_eq!(discovery.lookups(), MAX_CONCURRENT_LOOKUPS + extra);
    assert_eq!(discovery.live(), 0);

    close(&[&node_a]).await;
}

/// A miss past the pending limit is refused at once. Misses past the cap
/// wait for a permit, and that queue has a bound too: worker ids come off
/// the wire, so a peer must not make this node hold misses without limit.
#[tokio::test]
async fn a_miss_past_the_pending_limit_is_refused() {
    use crate::transports::{MAX_CONCURRENT_LOOKUPS, MAX_PENDING_LOOKUPS};

    let discovery = SharedDiscovery::gated();
    let node_a = node(Some(discovery.clone())).await;
    let backend = backend(&node_a);

    let held = MAX_CONCURRENT_LOOKUPS + MAX_PENDING_LOOKUPS;
    let mut misses = tokio::task::JoinSet::new();
    for worker in 1..=held as u64 {
        let backend = backend.clone();
        misses.spawn(async move {
            backend
                .resolve_peer(PeerKey::Worker(WorkerId::from_u64(worker)), None)
                .await
        });
    }
    eventually("the misses never all took a place", || {
        backend.lookups_held() == held
    })
    .await;

    let error = backend
        .resolve_peer(PeerKey::Worker(WorkerId::from_u64(u64::MAX)), None)
        .await
        .expect_err("the queue is full");
    assert!(format!("{error:#}").contains("pending"), "{error:#}");
    assert_eq!(
        discovery.lookups(),
        MAX_CONCURRENT_LOOKUPS,
        "only the cap asked discovery"
    );

    misses.shutdown().await;
    eventually("a dropped miss kept its place", || {
        backend.lookups_held() == 0 && discovery.live() == 0
    })
    .await;

    close(&[&node_a]).await;
}

/// A lookup ends when its node tears down, also one with no deadline that a
/// detached send holds. Its permit comes back, and discovery is not left
/// running. A lookup that would only now start is refused the same way.
#[tokio::test]
async fn teardown_ends_a_lookup_and_frees_its_permit() {
    let discovery = SharedDiscovery::hung();
    let node_a = node(Some(discovery.clone())).await;
    let backend = backend(&node_a);

    let mut misses = tokio::task::JoinSet::new();
    for worker in 1..=3u64 {
        let backend = backend.clone();
        misses.spawn(async move {
            backend
                .resolve_peer(PeerKey::Worker(WorkerId::from_u64(worker)), None)
                .await
        });
    }
    eventually("the lookups never reached discovery", || {
        discovery.live() == 3
    })
    .await;

    backend.shutdown_now();
    while let Some(miss) = tokio::time::timeout(Duration::from_secs(5), misses.join_next())
        .await
        .expect("a lookup outlived its node")
    {
        let error = miss.unwrap().expect_err("the node is shutting down");
        assert!(format!("{error:#}").contains("shutting down"), "{error:#}");
    }
    assert_eq!(discovery.live(), 0, "discovery was left running");
    assert_eq!(backend.lookups_held(), 0, "a permit was not returned");
    backend
        .resolve_peer(PeerKey::Worker(WorkerId::from_u64(9)), None)
        .await
        .expect_err("a node that tore down starts no lookup");
    assert_eq!(discovery.lookups(), 3);

    close(&[&node_a]).await;
}

/// A discovery answer that comes after teardown began registers no peer. The
/// lookup here is inside a poll, blocked in discovery, while another thread
/// tears the node down; cancelling it cannot stop that poll. When it comes
/// back, it must not put a route into a node that is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_discovery_answer_after_teardown_registers_nothing() {
    let discovery = SharedDiscovery::blocking(Duration::from_millis(200));
    let node_a = node(Some(discovery.clone())).await;
    let peer = node(None).await;
    discovery.learn(peer.peer_info());
    let backend = backend(&node_a);

    let waiting = backend.clone();
    let key = PeerKey::Instance(peer.instance_id());
    let miss = tokio::spawn(async move { waiting.resolve_peer(key, None).await });
    eventually("the lookup never reached discovery", || {
        discovery.live() == 1
    })
    .await;

    backend.shutdown_now();
    miss.await.unwrap().expect_err("the node is shutting down");
    assert!(
        !backend.is_registered(peer.instance_id()),
        "an answer after teardown registered its peer"
    );

    close(&[&node_a, &peer]).await;
}

/// A miss that waited for a permit past its deadline fails, also when the
/// table holds its peer by then. `timeout_at` polls the lookup before its
/// timer; a permit that comes at that late poll would otherwise lead straight
/// to the table and a send for a caller that has given up. Here every permit
/// is held, and the thread is blocked past the deadline while they come free.
#[tokio::test]
async fn a_miss_that_got_its_permit_late_does_not_send() {
    use crate::messenger::client::LookupTimedOut;
    use crate::transports::MAX_CONCURRENT_LOOKUPS;

    let discovery = SharedDiscovery::gated();
    let node_a = node(Some(discovery.clone())).await;
    let peer = node(None).await;
    node_a.register_peer(peer.peer_info()).unwrap();
    let backend = backend(&node_a);

    let mut holders = tokio::task::JoinSet::new();
    for worker in 1..=MAX_CONCURRENT_LOOKUPS as u64 {
        let backend = backend.clone();
        holders.spawn(async move {
            backend
                .resolve_peer(PeerKey::Worker(WorkerId::from_u64(worker)), None)
                .await
        });
    }
    eventually("the permits were never all taken", || {
        discovery.live() == MAX_CONCURRENT_LOOKUPS
    })
    .await;

    let queued = backend.clone();
    let key = PeerKey::Instance(peer.instance_id());
    let deadline = tokio::time::Instant::now() + Duration::from_millis(50);
    let miss = tokio::spawn(async move { queued.resolve_peer(key, Some(deadline)).await });
    eventually("the miss never queued", || {
        backend.lookups_held() == MAX_CONCURRENT_LOOKUPS + 1
    })
    .await;

    // Free the permits, and hold the thread until the deadline has passed.
    discovery.release();
    std::thread::sleep(Duration::from_millis(100));
    let error = miss.await.unwrap().expect_err("the deadline had passed");
    assert!(error.is::<LookupTimedOut>(), "{error:#}");

    holders.shutdown().await;
    close(&[&node_a, &peer]).await;
}

/// A discovery refusal that comes after the deadline is a timeout. The
/// caller gave up before discovery answered; what discovery then said is not
/// why the call failed, and a send must still read as `RequestTimeout`.
#[tokio::test]
async fn a_late_discovery_refusal_is_a_timeout() {
    use crate::messenger::client::LookupTimedOut;

    // Knows no peer: it blocks its thread, then refuses.
    let discovery = SharedDiscovery::blocking(Duration::from_millis(120));
    let node_a = node(Some(discovery.clone())).await;

    let deadline = tokio::time::Instant::now() + Duration::from_millis(20);
    let error = backend(&node_a)
        .resolve_peer(PeerKey::Worker(WorkerId::from_u64(7)), Some(deadline))
        .await
        .expect_err("discovery refused, late");
    assert!(error.is::<LookupTimedOut>(), "{error:#}");
    assert_eq!(discovery.lookups(), 1);

    close(&[&node_a]).await;
}

/// A transport that takes no peer. It can make `register` slow, and it can
/// hold `begin_drain`, which the backend's teardown worker calls first.
struct SlowTransport {
    address: WorkerAddress,
    /// How long `register` blocks, in milliseconds.
    register_ms: std::sync::atomic::AtomicU64,
    /// While armed, `begin_drain` waits for `release`.
    hold_drain: std::sync::atomic::AtomicBool,
    release: flume::Receiver<()>,
}

impl SlowTransport {
    /// The transport and the sender that lets a held `begin_drain` go.
    fn new() -> (Arc<Self>, flume::Sender<()>) {
        let mut address = crate::transports::address::WorkerAddressBuilder::new();
        address.add_entry("slow", b"slow://".to_vec()).unwrap();
        let (release_tx, release) = flume::unbounded();
        let transport = Arc::new(Self {
            address: address.build().unwrap(),
            register_ms: Default::default(),
            hold_drain: Default::default(),
            release,
        });
        (transport, release_tx)
    }
}

impl crate::transports::Transport for SlowTransport {
    fn key(&self) -> velo_ext::TransportKey {
        velo_ext::TransportKey::from("slow")
    }

    fn address(&self) -> WorkerAddress {
        self.address.clone()
    }

    fn register(&self, _peer: PeerInfo) -> Result<(), crate::transports::TransportError> {
        std::thread::sleep(Duration::from_millis(
            self.register_ms.load(Ordering::SeqCst),
        ));
        Err(crate::transports::TransportError::NoEndpoint)
    }

    fn send_message(
        &self,
        _target: InstanceId,
        _header: Bytes,
        _payload: Bytes,
        _kind: MessageType,
        _on_error: Arc<dyn TransportErrorHandler>,
    ) -> crate::transports::SendOutcome {
        unreachable!("this transport registers no peer, so it is no route")
    }

    fn start(
        &self,
        _id: InstanceId,
        _adapter: crate::transports::TransportAdapter,
        _runtime: tokio::runtime::Handle,
    ) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn shutdown(&self) {}

    fn begin_drain(&self) {
        if self.hold_drain.load(Ordering::SeqCst) {
            // Opens on the release, or when the test drops the sender.
            let _ = self.release.recv();
        }
    }

    fn check_health(
        &self,
        _instance_id: InstanceId,
        _timeout: Duration,
    ) -> BoxFuture<'_, Result<(), velo_ext::HealthCheckError>> {
        Box::pin(async { Ok(()) })
    }
}

/// A node with discovery, TCP, and the slow transport.
async fn slow_node(
    discovery: Arc<SharedDiscovery>,
) -> (Velo, Arc<SlowTransport>, flume::Sender<()>) {
    let (slow, release) = SlowTransport::new();
    let node = Velo::builder()
        .add_transport(tcp_transport())
        .add_transport(slow.clone())
        .discovery(discovery)
        .build()
        .await
        .unwrap();
    (node, slow, release)
}

/// A registration that ends after the deadline starts no send. Discovery
/// answered in time here, but a transport took longer than the caller's
/// deadline to register the peer. The caller has read its timeout by then,
/// so the lookup must fail, not hand back an instance to send to.
#[tokio::test]
async fn a_registration_that_ends_late_starts_no_send() {
    use crate::messenger::client::LookupTimedOut;

    let discovery = SharedDiscovery::new();
    let (node_a, slow, _release) = slow_node(discovery.clone()).await;
    let peer = node(None).await;
    discovery.learn(peer.peer_info());

    slow.register_ms.store(120, Ordering::SeqCst);
    let deadline = tokio::time::Instant::now() + Duration::from_millis(20);
    let error = backend(&node_a)
        .resolve_peer(PeerKey::Instance(peer.instance_id()), Some(deadline))
        .await
        .expect_err("the registration ended after the deadline");
    assert!(error.is::<LookupTimedOut>(), "{error:#}");
    assert_eq!(discovery.lookups(), 1);

    slow.register_ms.store(0, Ordering::SeqCst);
    close(&[&node_a, &peer]).await;
}

/// Lookups end when teardown is asked for, not when the teardown worker gets
/// to it. The worker runs the transports' `begin_drain` hooks first, and a
/// hook can take its time; here one is held open. A lookup that waited for
/// the worker could register a peer after the last node drop.
#[tokio::test]
async fn a_lookup_ends_before_the_teardown_hooks_run() {
    let discovery = SharedDiscovery::hung();
    let (node_a, slow, release) = slow_node(discovery.clone()).await;
    let backend = backend(&node_a);

    let waiting = backend.clone();
    let miss = tokio::spawn(async move {
        waiting
            .resolve_peer(PeerKey::Worker(WorkerId::from_u64(7)), None)
            .await
    });
    eventually("the lookup never reached discovery", || {
        discovery.live() == 1
    })
    .await;

    slow.hold_drain.store(true, Ordering::SeqCst);
    backend.shutdown_now();
    let error = tokio::time::timeout(Duration::from_secs(5), miss)
        .await
        .expect("the lookup waited for a held teardown hook")
        .unwrap()
        .expect_err("the node is shutting down");
    assert!(format!("{error:#}").contains("shutting down"), "{error:#}");
    assert_eq!(discovery.live(), 0);

    slow.hold_drain.store(false, Ordering::SeqCst);
    release.send(()).unwrap();
    close(&[&node_a]).await;
}
