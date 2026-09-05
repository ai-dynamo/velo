// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Graceful-shutdown behavior as peers observe it, driven through the public
//! `Velo::begin_drain` / `Velo::graceful_shutdown` API.
//!
//! Drain rejection chain under test: the server's listener rejects a `Message`
//! frame during drain by echoing its header in a `ShuttingDown` frame on the
//! socket the client dialed; the client's dialed-connection reader routes the
//! echo onto its `shutdown_stream`; the messenger's dedicated shutdown
//! handler recovers the response id from the request-format header and fails
//! the awaiter.
//!
//! Phase-2 chain under test: the dispatcher acquires an in-flight guard per
//! handler invocation, so `graceful_shutdown` waits for a running handler
//! instead of tearing down under it.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use prometheus::Registry;
use velo::observability::VeloMetrics;
use velo::transports::tcp::TcpTransportBuilder;
use velo::transports::{AdmitOutcome, MessageType, SendOutcome, Transport};
use velo::*;

/// Departures from the messenger's inbound queue. Paired with
/// `velo_transport_frames_total{direction="inbound",message_type="message",
/// outcome="accepted"}`, the difference is the queue's depth — there is no
/// gauge for it.
const INBOUND_DEQUEUED: &str = "velo_messenger_inbound_dequeued_total";

/// Read an unlabelled counter straight off a registry.
///
/// `observability::test_helpers::MetricSnapshot` would do this, but it lives
/// behind the `test-helpers` feature and this target is built without it.
fn counter_value(registry: &Registry, name: &str) -> f64 {
    registry
        .gather()
        .iter()
        .find(|family| family.name() == name)
        .and_then(|family| family.get_metric().first())
        .map(|metric| metric.get_counter().value())
        .unwrap_or(0.0)
}

/// Poll `registry` until `name` has read the same value three polls running and
/// is at least `at_least`, returning that value.
///
/// A baseline taken the instant a counter first moves can still have work in
/// flight behind it, and an exact-delta assertion against such a baseline fails
/// at random rather than never. Waiting for the value to stop moving is what
/// makes the delta afterwards the test's own traffic and nothing else.
async fn await_counter_settled(registry: &Registry, name: &str, at_least: f64, why: &str) -> f64 {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut last = f64::NAN;
    let mut stable = 0;
    loop {
        let got = counter_value(registry, name);
        if got >= at_least && got == last {
            stable += 1;
            if stable >= 2 {
                return got;
            }
        } else {
            stable = 0;
        }
        last = got;
        assert!(
            std::time::Instant::now() < deadline,
            "{why}: {name} never settled — last read {got}, wanted at least {at_least}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Poll `registry` until `name` reaches `want`, returning what it read, or
/// fail saying where it stalled.
async fn await_counter_at_least(registry: &Registry, name: &str, want: f64, why: &str) -> f64 {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let got = counter_value(registry, name);
        if got >= want {
            return got;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{why}: {name} stalled at {got}, wanted at least {want}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn new_tcp_transport() -> Arc<dyn Transport> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    Arc::new(
        TcpTransportBuilder::new()
            .from_listener(listener)
            .unwrap()
            .build()
            .unwrap(),
    )
}

#[cfg(unix)]
fn new_uds_transport() -> Arc<dyn Transport> {
    let dir = std::env::temp_dir().join(format!("velo-drain-test-{}", InstanceId::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    Arc::new(
        velo::transports::uds::UdsTransportBuilder::new()
            .socket_path(dir.join("velo.sock"))
            .build()
            .unwrap(),
    )
}

/// A unary request to a peer that called `begin_drain` must fail fast with
/// the drain rejection instead of hanging until the response timeout.
async fn unary_to_draining_peer_fails_fast(make: fn() -> Arc<dyn Transport>) {
    let server = Velo::builder().add_transport(make()).build().await.unwrap();
    let ping = Handler::unary_handler("ping", |ctx| Ok(Some(ctx.payload))).build();
    server.register_handler(ping).unwrap();

    let client = Velo::builder().add_transport(make()).build().await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    client.register_peer(server.peer_info()).unwrap();
    server.register_peer(client.peer_info()).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Warm-up while the server is healthy: handshake completes and the
    // client's connection is established.
    let echoed: Bytes = client
        .unary("ping")
        .unwrap()
        .raw_payload(Bytes::from_static(b"warmup"))
        .instance(server.instance_id())
        .send()
        .await
        .unwrap();
    assert_eq!(echoed, Bytes::from_static(b"warmup"));

    // Instance-level gate through the public API; the server's listener now
    // rejects Message frames.
    server.begin_drain();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let result = tokio::time::timeout(
        Duration::from_secs(2),
        client
            .unary("ping")
            .unwrap()
            .raw_payload(Bytes::from_static(b"rejected"))
            .instance(server.instance_id())
            .send(),
    )
    .await;

    let err = result
        .expect("drain rejection must complete the request promptly, not hang until the response timeout")
        .expect_err("request during drain must fail");
    assert!(
        err.to_string().contains("shutting down"),
        "unexpected error: {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_unary_to_draining_peer_fails_fast() {
    unary_to_draining_peer_fails_fast(new_tcp_transport).await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uds_unary_to_draining_peer_fails_fast() {
    unary_to_draining_peer_fails_fast(new_uds_transport).await;
}

/// Phase 2 of `graceful_shutdown` must wait for a handler invocation that is
/// already running, and complete promptly once it finishes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graceful_shutdown_waits_for_in_flight_handlers() {
    let server = Velo::builder()
        .add_transport(new_tcp_transport())
        .build()
        .await
        .unwrap();

    // `entered` signals the handler has started (its in-flight guard is
    // held); `release` lets the test decide when it finishes.
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let (entered_h, release_h) = (entered.clone(), release.clone());
    let slow = Handler::unary_handler_async("slow", move |ctx| {
        let entered = entered_h.clone();
        let release = release_h.clone();
        async move {
            entered.add_permits(1);
            let _permit = release.acquire().await.expect("release semaphore closed");
            Ok(Some(ctx.payload))
        }
    })
    .build();
    server.register_handler(slow).unwrap();

    let client = Velo::builder()
        .add_transport(new_tcp_transport())
        .build()
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    client.register_peer(server.peer_info()).unwrap();
    server.register_peer(client.peer_info()).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let pending = tokio::spawn(
        client
            .unary("slow")
            .unwrap()
            .raw_payload(Bytes::from_static(b"x"))
            .instance(server.instance_id())
            .send(),
    );

    // Handler is now parked inside its invocation, guard held.
    tokio::time::timeout(Duration::from_secs(2), entered.acquire())
        .await
        .expect("handler never started")
        .expect("entered semaphore closed")
        .forget();

    let shutdown = tokio::spawn(async move {
        server.graceful_shutdown(ShutdownPolicy::WaitForever).await;
    });

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !shutdown.is_finished(),
        "graceful_shutdown must wait for the in-flight handler"
    );

    release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), shutdown)
        .await
        .expect("graceful_shutdown must complete once the handler finishes")
        .unwrap();

    // Whether the response beat the teardown is a separate race; just make
    // sure the client task winds down rather than asserting its outcome.
    let _ = tokio::time::timeout(Duration::from_millis(500), pending).await;
}

// ── Teardown-aware consumer loop ─────────────────────────────────────────

/// An in-process loopback transport that can be told to *hold* one frame
/// instead of delivering it.
///
/// Everything the messenger sends is fed straight back into its own inbound
/// streams, which is enough for a single instance registered as its own peer
/// to complete the `_hello` handshake and then talk to itself. The hold hook
/// is what makes the test deterministic: a held frame is a real, decodable
/// active message that the test can drop onto the inbound queue at a moment
/// of its choosing, rather than whenever a listener task happens to run.
#[derive(Default)]
struct LoopbackTransport {
    adapter: std::sync::OnceLock<velo::transports::TransportAdapter>,
    /// Payload of the one frame to intercept rather than deliver.
    ///
    /// Matched by payload equality, so the marker must be distinct from every
    /// payload the runtime sends on its own (`_hello`'s peer-info JSON, and
    /// any future internal frame). The caller asserts that exactly one frame
    /// matched, which is what turns a collision into a clear failure rather
    /// than a mysterious one.
    hold: Mutex<Option<&'static [u8]>>,
    held: Mutex<VecDeque<(Bytes, Bytes)>>,
}

impl LoopbackTransport {
    fn adapter(&self) -> &velo::transports::TransportAdapter {
        self.adapter.get().expect("transport was never started")
    }

    fn hold_payload(&self, payload: &'static [u8]) {
        *self.hold.lock().unwrap() = Some(payload);
    }

    fn take_held(&self) -> Option<(Bytes, Bytes)> {
        self.held.lock().unwrap().pop_front()
    }
}

impl Transport for LoopbackTransport {
    fn key(&self) -> velo_ext::TransportKey {
        velo_ext::TransportKey::from("loopback")
    }

    fn address(&self) -> velo_ext::WorkerAddress {
        let mut entries = std::collections::HashMap::<String, Vec<u8>>::new();
        entries.insert("loopback".to_string(), b"loopback://local".to_vec());
        velo_ext::WorkerAddress::from_encoded(rmp_serde::to_vec(&entries).unwrap())
    }

    fn register(&self, _peer_info: velo_ext::PeerInfo) -> Result<(), velo_ext::TransportError> {
        Ok(())
    }

    fn send_message(
        &self,
        _instance_id: InstanceId,
        header: Bytes,
        payload: Bytes,
        message_type: MessageType,
        _on_error: Arc<dyn velo::transports::TransportErrorHandler>,
    ) -> SendOutcome {
        let adapter = self.adapter();
        match message_type {
            MessageType::Message => {
                let held = self
                    .hold
                    .lock()
                    .unwrap()
                    .is_some_and(|marker| marker == &payload[..]);
                if held {
                    self.held.lock().unwrap().push_back((header, payload));
                } else {
                    // The documented producer contract: `admit_message` owns
                    // both the drain gate and the enqueue.
                    let _ = adapter.admit_message(header, payload);
                }
            }
            MessageType::Response => {
                let _ = adapter.response_stream.send((header, payload));
            }
            MessageType::Ack | MessageType::Event => {
                let _ = adapter.event_stream.send((header, payload));
            }
            MessageType::ShuttingDown => {
                let _ = adapter.shutdown_stream.send((header, payload));
            }
        }
        SendOutcome::Admitted
    }

    fn start(
        &self,
        _instance_id: InstanceId,
        channels: velo::transports::TransportAdapter,
        _rt: tokio::runtime::Handle,
    ) -> futures::future::BoxFuture<'_, anyhow::Result<()>> {
        let _ = self.adapter.set(channels);
        Box::pin(async { Ok(()) })
    }

    fn shutdown(&self) {}

    fn check_health(
        &self,
        _instance_id: InstanceId,
        _timeout: Duration,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), velo_ext::HealthCheckError>> + Send + '_>,
    > {
        Box::pin(async { Ok(()) })
    }
}

/// Under `ShutdownPolicy::Timeout`, work still sitting on the inbound queue
/// when phase 2 gives up must be dropped, not dispatched into an instance that
/// has already torn itself down.
///
/// The queued message is visible to phase 2 — it holds an in-flight guard — so
/// the policy's timeout is what ends the wait. Phase 3 then cancels the
/// teardown token, and the consumer loop has to break on it instead of
/// dispatching the backlog it can still see — while still *releasing* that
/// backlog's guards on the way out, or the count it abandons wedges every
/// later drain wait.
///
/// Starving the consumer is the whole trick: it dispatches within microseconds
/// of a message landing, so the only way to still *have* a backlog at teardown
/// is to stop it running. The instance therefore gets its own single-worker
/// runtime, and a task blocking on a channel freezes that worker. Admission
/// and the shutdown driver both run on the test's runtime, so neither freezes
/// with it — which is exactly the interleaving the fix has to survive.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_shutdown_timeout_drops_queued_work() {
    let server_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    let transport = Arc::new(LoopbackTransport::default());
    let registry = Registry::new();
    let metrics = Arc::new(VeloMetrics::register(&registry).expect("register metrics"));
    let server = {
        let (tx, rx) = std::sync::mpsc::channel();
        let transport = transport.clone();
        server_rt.spawn(async move {
            let built = Velo::builder()
                .add_transport(transport)
                .metrics(metrics)
                .build()
                .await
                .expect("server build");
            tx.send(built).expect("server handoff");
        });
        tokio::task::block_in_place(|| rx.recv_timeout(Duration::from_secs(10)))
            .expect("server never finished building")
    };

    let ran = Arc::new(AtomicUsize::new(0));
    let ran_handler = ran.clone();
    let victim = Handler::unary_handler("victim", move |ctx| {
        ran_handler.fetch_add(1, Ordering::SeqCst);
        Ok(Some(ctx.payload))
    })
    .build();
    server.register_handler(victim).unwrap();
    server.register_peer(server.peer_info()).unwrap();

    // Warm-up over the loopback. This completes the `_hello` handshake and
    // proves the whole inbound path works, so "the handler did not run" later
    // cannot be a never-wired handler masquerading as a pass.
    tokio::time::timeout(
        Duration::from_secs(10),
        server
            .am_send("victim")
            .unwrap()
            .raw_payload(Bytes::from_static(b"warmup"))
            .instance(server.instance_id())
            .send(),
    )
    .await
    .expect("warm-up send timed out")
    .expect("warm-up send failed");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while ran.load(Ordering::SeqCst) == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "warm-up message was never dispatched"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // The dequeue counter is a count of *departures from the queue*, and the
    // warm-up plus `_hello` are the departures so far. Pinning it here is what
    // makes the assertion after teardown mean something: a counter that never
    // moved at all would satisfy "unchanged" for free.
    let dequeued_before = await_counter_settled(
        &registry,
        INBOUND_DEQUEUED,
        1.0,
        "the dispatch loop must count every message it takes off the queue",
    )
    .await;

    // Build the messages the consumer must never dispatch, and hold them.
    //
    // Two, not one, and the difference is the point: the first is what the
    // consumer dequeues and then drops when it sees the cancelled token, so
    // its guard is released on the loop's own path out. Only the *second* one
    // is still sitting in the channel buffer when the loop exits, which is the
    // guard that can be stranded there for good.
    transport.hold_payload(b"queued");
    for attempt in 0..2 {
        tokio::time::timeout(
            Duration::from_secs(10),
            server
                .am_send("victim")
                .unwrap()
                .raw_payload(Bytes::from_static(b"queued"))
                .instance(server.instance_id())
                .send(),
        )
        .await
        .unwrap_or_else(|_| panic!("held send {attempt} timed out"))
        .unwrap_or_else(|e| panic!("held send {attempt} failed: {e}"));
    }
    let first = transport.take_held().expect("frame was not intercepted");
    let second = transport
        .take_held()
        .expect("second frame was not intercepted");
    assert!(
        transport.take_held().is_none(),
        "the hold marker matched more than the two intended frames"
    );

    // Freeze the instance's only worker: its consumer task cannot run again
    // until the test releases it.
    let (frozen_tx, frozen_rx) = std::sync::mpsc::channel::<()>();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    server_rt.spawn(async move {
        frozen_tx.send(()).expect("freeze handshake");
        let _ = release_rx.recv();
    });
    tokio::task::block_in_place(|| frozen_rx.recv_timeout(Duration::from_secs(5)))
        .expect("instance worker never froze");

    for (header, payload) in [first, second] {
        assert!(matches!(
            transport.adapter().admit_message(header, payload),
            AdmitOutcome::Admitted
        ));
    }
    assert_eq!(
        transport.adapter().shutdown_state.in_flight_count(),
        2,
        "both queued messages must be counted work before shutdown starts"
    );

    tokio::time::timeout(
        Duration::from_secs(5),
        server.graceful_shutdown(ShutdownPolicy::Timeout(Duration::from_millis(50))),
    )
    .await
    .expect("graceful_shutdown must give up once its timeout expires");

    // Thaw: the consumer runs again with the teardown token already cancelled
    // and a message still on its queue.
    release_tx.send(()).ok();
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(
        ran.load(Ordering::SeqCst),
        1,
        "messages left on the queue at teardown must be dropped, not dispatched \
         after graceful_shutdown already returned"
    );

    // Three messages reached the queue; one was dispatched. The counter has to
    // agree with the dispatch, not with the admission: the message the loop
    // popped and abandoned on the cancelled token never became work, and the
    // one still buffered behind it never left the queue at all. Counting
    // either would put the derived depth permanently below zero.
    assert_eq!(
        counter_value(&registry, INBOUND_DEQUEUED),
        dequeued_before,
        "an abandoned message is not a dispatched one"
    );

    // Abandoning the message must not abandon the in-flight guard riding with
    // it. flume frees a buffered item only once *both* ends of the channel are
    // gone, and the transport holds a sender clone for the instance's
    // lifetime, so a guard left parked in the buffer would pin the count above
    // zero for good — and every later or concurrent drain wait would hang on a
    // count that can no longer reach zero.
    let state = &transport.adapter().shutdown_state;
    tokio::time::timeout(Duration::from_secs(5), state.wait_for_drain())
        .await
        .expect("a drain wait after a timed-out shutdown must not hang on abandoned work");
    assert_eq!(
        state.in_flight_count(),
        0,
        "the guard riding the abandoned message must have been released at teardown"
    );

    server_rt.shutdown_background();
}

/// A frame the dispatch loop cannot decode still *left* the inbound queue.
///
/// The dequeue counter answers "how much of what was admitted is still
/// waiting", so its increment belongs to the pop, not to the handler. Placed
/// after the decode instead, an undecodable frame would count as admitted
/// forever and the derived depth would drift up by one for every malformed
/// frame a peer ever sent — a leak that reads exactly like a stuck queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_undecodable_frame_still_leaves_the_inbound_queue() {
    let registry = Registry::new();
    let metrics = Arc::new(VeloMetrics::register(&registry).expect("register metrics"));
    let transport = Arc::new(LoopbackTransport::default());
    let server = Velo::builder()
        .add_transport(transport.clone())
        .metrics(metrics)
        .build()
        .await
        .expect("server build");
    let sink = Handler::unary_handler("sink", |ctx| Ok(Some(ctx.payload))).build();
    server.register_handler(sink).unwrap();
    server.register_peer(server.peer_info()).unwrap();

    // Warm up over the loopback. The `_hello` handshake rides the first send
    // rather than `register_peer`, so this is what actually puts a well-formed
    // message on the inbound queue — and it proves the counter is wired before
    // the malformed frame below is asked to move it.
    tokio::time::timeout(
        Duration::from_secs(10),
        server
            .am_send("sink")
            .unwrap()
            .raw_payload(Bytes::from_static(b"warmup"))
            .instance(server.instance_id())
            .send(),
    )
    .await
    .expect("warm-up send timed out")
    .expect("warm-up send failed");

    let before = await_counter_settled(
        &registry,
        INBOUND_DEQUEUED,
        1.0,
        "the warm-up and _hello must be counted off the queue",
    )
    .await;

    assert!(matches!(
        transport
            .adapter()
            .admit_message(Bytes::from_static(b"not-a-velo-header"), Bytes::new()),
        AdmitOutcome::Admitted
    ));

    let after = await_counter_at_least(
        &registry,
        INBOUND_DEQUEUED,
        before + 1.0,
        "a frame that failed to decode must still count as a departure",
    )
    .await;
    assert_eq!(after, before + 1.0, "one frame, one departure");

    // The guard the malformed frame carried is released on the same path, or
    // every later drain wait hangs on a count that can never reach zero.
    tokio::time::timeout(
        Duration::from_secs(5),
        transport.adapter().shutdown_state.wait_for_drain(),
    )
    .await
    .expect("the undecodable frame's in-flight guard must be released");
}

/// The queue's depth is derivable only because the dequeue counter moves when
/// the loop *pops*, and not before.
///
/// This is the measurement the counter exists for, reproduced at its smallest:
/// messages admitted while the dispatch loop cannot run leave the counter flat,
/// so `admitted - dequeued` reads the backlog; releasing the loop closes the
/// gap to zero. A counter incremented at admission instead — anywhere on the
/// producer side of `message_rx` — would read zero backlog here, which is the
/// exact reading the ingest investigation this metric was added for must be
/// able to disbelieve.
///
/// Starving the loop is the whole trick, and it is the same one
/// `graceful_shutdown_timeout_drops_queued_work` uses: the instance gets a
/// single-worker runtime and a task blocking that worker freezes its dispatch
/// loop, while admission runs on the test's own runtime.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_parked_dispatch_loop_leaves_admitted_messages_uncounted() {
    const QUEUED: usize = 3;

    let server_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    let transport = Arc::new(LoopbackTransport::default());
    let registry = Registry::new();
    let metrics = Arc::new(VeloMetrics::register(&registry).expect("register metrics"));
    let server = {
        let (tx, rx) = std::sync::mpsc::channel();
        let transport = transport.clone();
        server_rt.spawn(async move {
            let built = Velo::builder()
                .add_transport(transport)
                .metrics(metrics)
                .build()
                .await
                .expect("server build");
            tx.send(built).expect("server handoff");
        });
        tokio::task::block_in_place(|| rx.recv_timeout(Duration::from_secs(10)))
            .expect("server never finished building")
    };

    let ran = Arc::new(AtomicUsize::new(0));
    let ran_handler = ran.clone();
    let sink = Handler::unary_handler("sink", move |ctx| {
        ran_handler.fetch_add(1, Ordering::SeqCst);
        Ok(Some(ctx.payload))
    })
    .build();
    server.register_handler(sink).unwrap();
    server.register_peer(server.peer_info()).unwrap();

    // Intercept exactly the frames this test intends to queue, so admission
    // happens when the test says so rather than when a task happens to run.
    transport.hold_payload(b"parked");
    for attempt in 0..QUEUED {
        tokio::time::timeout(
            Duration::from_secs(10),
            server
                .am_send("sink")
                .unwrap()
                .raw_payload(Bytes::from_static(b"parked"))
                .instance(server.instance_id())
                .send(),
        )
        .await
        .unwrap_or_else(|_| panic!("held send {attempt} timed out"))
        .unwrap_or_else(|e| panic!("held send {attempt} failed: {e}"));
    }
    let held: Vec<_> = (0..QUEUED)
        .map(|n| {
            transport
                .take_held()
                .unwrap_or_else(|| panic!("frame {n} was not intercepted"))
        })
        .collect();
    assert!(
        transport.take_held().is_none(),
        "the hold marker matched more than the intended frames"
    );

    // The `_hello` handshake rode the first send above (`register_peer` alone
    // sends nothing). Let it finish and the counter stop moving before the
    // freeze, so the baseline is a quiet queue rather than a race.
    let before = await_counter_settled(
        &registry,
        INBOUND_DEQUEUED,
        1.0,
        "the _hello handshake must be counted off the queue",
    )
    .await;

    let (frozen_tx, frozen_rx) = std::sync::mpsc::channel::<()>();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    server_rt.spawn(async move {
        frozen_tx.send(()).expect("freeze handshake");
        let _ = release_rx.recv();
    });
    tokio::task::block_in_place(|| frozen_rx.recv_timeout(Duration::from_secs(5)))
        .expect("instance worker never froze");

    for (header, payload) in held {
        assert!(matches!(
            transport.adapter().admit_message(header, payload),
            AdmitOutcome::Admitted
        ));
    }
    assert_eq!(
        transport.adapter().shutdown_state.in_flight_count(),
        QUEUED,
        "every queued message holds a guard, which is what makes it visible work"
    );

    // The backlog is real and the counter has not moved: `admitted - dequeued`
    // is exactly the depth.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        counter_value(&registry, INBOUND_DEQUEUED),
        before,
        "a parked loop dequeues nothing, so the derived depth must be the whole backlog"
    );
    assert_eq!(
        ran.load(Ordering::SeqCst),
        0,
        "the frozen worker cannot have dispatched anything"
    );

    release_tx.send(()).ok();

    let after = await_counter_at_least(
        &registry,
        INBOUND_DEQUEUED,
        before + QUEUED as f64,
        "releasing the loop must close the derived depth to zero",
    )
    .await;
    assert_eq!(
        after,
        before + QUEUED as f64,
        "every queued message departs exactly once"
    );

    tokio::time::timeout(
        Duration::from_secs(5),
        transport.adapter().shutdown_state.wait_for_drain(),
    )
    .await
    .expect("the drained backlog must release its guards");

    server_rt.shutdown_background();
}
