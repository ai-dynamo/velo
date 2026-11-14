// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The lifecycle of the runtime `TaskRuntime::Dedicated` builds: velo builds
//! it in `build`, stops it at the end of `shutdown`, and stops it without
//! blocking on the last drop. The last two tests count where the parts of a
//! unary call run on each setting.
//!
//! The tests count this process's threads named `velo-rt` in
//! `/proc/self/task`, so each takes [`serial`] first: another test's runtime
//! would change the count. A runtime stopped by a drop exits on a background
//! thread, so each test also waits until no `velo-rt` thread is left before
//! it builds one.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::StreamExt;

use velo::transports::tcp::TcpTransportBuilder;
use velo::{Context, Handler, ShutdownPolicy, StreamFrame, TaskRuntime, Velo};

const PATIENCE: Duration = Duration::from_secs(10);
const THREAD_NAME: &str = "velo-rt";

/// A tokio mutex: each test holds it across its awaits, and each test has a
/// runtime of its own.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Taken by every test in this binary. Waits until no runtime of an earlier
/// test is left.
async fn serial() -> tokio::sync::MutexGuard<'static, ()> {
    let guard = SERIAL.lock().await;
    wait_for_threads(0);
    guard
}

/// Threads of this process named `velo-rt`.
#[cfg(target_os = "linux")]
fn velo_threads() -> usize {
    std::fs::read_dir("/proc/self/task")
        .expect("read /proc/self/task")
        .filter_map(|task| {
            let comm = std::fs::read_to_string(task.ok()?.path().join("comm")).ok()?;
            (comm.trim_end() == THREAD_NAME).then_some(())
        })
        .count()
}

/// Blocks until exactly `n` `velo-rt` threads exist. A thread is listed until
/// it has exited, a little after the runtime lets it go.
#[cfg(target_os = "linux")]
fn wait_for_threads(n: usize) {
    let deadline = Instant::now() + PATIENCE;
    loop {
        let now = velo_threads();
        if now == n {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{now} velo-rt threads, expected {n}"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[cfg(not(target_os = "linux"))]
fn wait_for_threads(_n: usize) {}

/// Counts panics on every thread: a panic on a velo-rt worker or on a
/// teardown thread never reaches a test's `JoinHandle`.
fn panics() -> &'static AtomicUsize {
    static PANICS: std::sync::OnceLock<&'static AtomicUsize> = std::sync::OnceLock::new();
    PANICS.get_or_init(|| {
        let count: &'static AtomicUsize = Box::leak(Box::new(AtomicUsize::new(0)));
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            count.fetch_add(1, Ordering::SeqCst);
            previous(info);
        }));
        count
    })
}

fn tcp() -> Arc<velo::transports::tcp::TcpTransport> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    Arc::new(
        TcpTransportBuilder::new()
            .from_listener(listener)
            .expect("from_listener")
            .build()
            .expect("build transport"),
    )
}

async fn node(runtime: TaskRuntime) -> Velo {
    let velo = Velo::builder()
        .add_transport(tcp())
        .mux_only()
        .task_runtime(runtime)
        .build()
        .await
        .expect("build velo");
    velo.register_handler(
        Handler::unary_handler("echo", |ctx: Context| Ok(Some(ctx.payload))).build(),
    )
    .expect("echo");
    velo
}

async fn introduce(a: &Velo, b: &Velo) {
    a.register_peer(b.peer_info()).expect("register");
    b.register_peer(a.peer_info()).expect("register");
    for (x, y) in [(a, b), (b, a)] {
        tokio::time::timeout(
            PATIENCE,
            x.wait_for_handler(y.instance_id(), "_anchor_attach"),
        )
        .await
        .expect("handshake timed out")
        .expect("handshake");
    }
}

async fn echo(from: &Velo, to: &Velo) -> anyhow::Result<Bytes> {
    tokio::time::timeout(
        PATIENCE,
        from.unary("echo")?
            .raw_payload(Bytes::from_static(b"ping"))
            .instance(to.instance_id())
            .send(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("echo timed out"))?
}

/// One mux stream from `producer` to `consumer`, three items and the end.
async fn stream_once(producer: &Velo, consumer: &Velo) {
    let mut anchor = consumer.create_anchor::<u32>();
    let ticket = consumer.prebind_anchor(anchor.handle()).expect("ticket");
    let sender = producer
        .open_anchor_stream::<u32>(anchor.handle(), ticket)
        .await
        .expect("open stream");
    for i in 0..3 {
        sender.send(i).await.expect("send");
    }
    sender.finalize().expect("finalize");
    let mut items = Vec::new();
    loop {
        match tokio::time::timeout(PATIENCE, anchor.next())
            .await
            .expect("stream")
        {
            Some(Ok(StreamFrame::Item(i))) => items.push(i),
            Some(Ok(StreamFrame::Finalized)) => break,
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(items, [0, 1, 2]);
}

/// `shutdown` returns after the runtime's threads have exited.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_leaves_no_velo_runtime_thread() {
    let _serial = serial().await;
    let a = node(TaskRuntime::Dedicated { threads: 2 }).await;
    let b = node(TaskRuntime::dedicated()).await;
    wait_for_threads(3);
    introduce(&a, &b).await;
    assert_eq!(&echo(&a, &b).await.expect("echo")[..], b"ping");
    stream_once(&a, &b).await;

    a.shutdown(ShutdownPolicy::WaitForever).await;
    // `shutdown` joined a's threads, so only b's thread remains. A thread
    // is listed until it has exited, a moment after it is joined, so this
    // waits rather than reads once.
    wait_for_threads(1);
    b.shutdown(ShutdownPolicy::WaitForever).await;
    wait_for_threads(0);
    // A second call, from a clone, finds the runtime stopped and returns.
    a.clone().shutdown(ShutdownPolicy::WaitForever).await;
}

/// The last drop of a `Velo`, inside a tokio task, with a live connection
/// and an open stream: no panic on any thread, and the threads go.
///
/// A runtime dropped inside an async context panics unless it is shut down
/// without waiting; and a runtime stopped before the transports' teardown
/// would run their shutdown hooks against dead drivers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_the_last_velo_inside_a_task_does_not_panic() {
    let _serial = serial().await;
    let panics = panics();
    let before = panics.load(Ordering::SeqCst);
    let a = node(TaskRuntime::dedicated()).await;
    let b = node(TaskRuntime::dedicated()).await;
    introduce(&a, &b).await;
    echo(&a, &b).await.expect("echo");
    // A stream left open across the drop.
    let anchor = b.create_anchor::<u32>();
    let ticket = b.prebind_anchor(anchor.handle()).expect("ticket");
    let sender = a
        .open_anchor_stream::<u32>(anchor.handle(), ticket)
        .await
        .expect("open stream");
    sender.send(7).await.expect("send");

    tokio::spawn(async move {
        drop(sender);
        drop(a);
        drop(anchor);
        drop(b);
    })
    .await
    .expect("the drop task panicked");
    wait_for_threads(0);
    // The teardown threads may still be finishing their hooks.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(panics.load(Ordering::SeqCst), before, "a thread panicked");
}

/// A transport whose shutdown hook needs its runtime: the hook spawns a task
/// on the runtime it runs in, and `closed` waits for that task. Real hooks
/// that flush on a driver task (QUIC's close) need the same.
struct NeedsRuntime {
    spawned_ran: Arc<std::sync::atomic::AtomicBool>,
    closed: Arc<std::sync::atomic::AtomicBool>,
}

fn needs_runtime_address() -> velo::WorkerAddress {
    let entries = std::collections::HashMap::from([("needs-rt".to_string(), b"x".to_vec())]);
    velo::WorkerAddress::from_encoded(rmp_serde::to_vec(&entries).unwrap())
}

impl velo::Transport for NeedsRuntime {
    fn key(&self) -> velo::ext::TransportKey {
        velo::ext::TransportKey::new("needs-rt")
    }
    fn address(&self) -> velo::WorkerAddress {
        needs_runtime_address()
    }
    fn register(&self, _: velo::PeerInfo) -> Result<(), velo::ext::TransportError> {
        Ok(())
    }
    fn send_message(
        &self,
        _: velo::InstanceId,
        _: Bytes,
        _: Bytes,
        _: velo::ext::MessageType,
        _: Arc<dyn velo::ext::TransportErrorHandler>,
    ) -> velo::ext::SendOutcome {
        velo::ext::SendOutcome::Admitted
    }
    fn start(
        &self,
        _: velo::InstanceId,
        _: velo::ext::TransportAdapter,
        _: tokio::runtime::Handle,
    ) -> futures::future::BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }
    fn shutdown(&self) {
        // The hook runs with the instance's runtime entered.
        let ran = Arc::clone(&self.spawned_ran);
        tokio::runtime::Handle::current().spawn(async move {
            ran.store(true, Ordering::SeqCst);
        });
    }
    fn closed(&self) -> futures::future::BoxFuture<'_, ()> {
        Box::pin(async move {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
            while !self.spawned_ran.load(Ordering::SeqCst) && tokio::time::Instant::now() < deadline
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            self.closed.store(true, Ordering::SeqCst);
        })
    }
    fn check_health(
        &self,
        _: velo::InstanceId,
        _: Duration,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), velo::ext::HealthCheckError>> + Send + '_>,
    > {
        Box::pin(async { Ok(()) })
    }
}

/// On the last drop, the transports' teardown runs while velo's runtime
/// still runs tasks, and their `closed` is waited for before it stops, as
/// `shutdown` does. A runtime stopped first leaves a hook that spawns, sets a
/// timer or does I/O on it with dead drivers, and the mux tasks it drops keep
/// the messenger, whose own drop would start the teardown, alive until then.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_last_drop_tears_down_before_the_runtime_stops() {
    let _serial = serial().await;
    let spawned_ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let velo = Velo::builder()
        .add_transport(Arc::new(NeedsRuntime {
            spawned_ran: Arc::clone(&spawned_ran),
            closed: Arc::clone(&closed),
        }))
        .mux_only()
        .task_runtime(TaskRuntime::dedicated())
        .build()
        .await
        .expect("build velo");
    tokio::spawn(async move { drop(velo) }).await.expect("drop");
    wait_for_threads(0);
    assert!(
        spawned_ran.load(Ordering::SeqCst),
        "the shutdown hook ran after velo's runtime stopped"
    );
    assert!(
        closed.load(Ordering::SeqCst),
        "velo's runtime stopped before the transport closed"
    );
}

/// A build that fails after the transports started (here the mux refuses a
/// zero sweep interval) stops the runtime as the last drop does: after the
/// transports' teardown and close, so no hook runs against a stopped
/// runtime, and without blocking or panicking.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_build_tears_down_before_the_runtime_stops() {
    let _serial = serial().await;
    let spawned_ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let error = Velo::builder()
        .add_transport(Arc::new(NeedsRuntime {
            spawned_ran: Arc::clone(&spawned_ran),
            closed: Arc::clone(&closed),
        }))
        .mux_only()
        .messenger_mux(velo::streaming::MuxConfig {
            credit_sweep_interval: Duration::ZERO,
            ..velo::streaming::MuxConfig::default()
        })
        .expect("mux config")
        .task_runtime(TaskRuntime::dedicated())
        .build()
        .await
        .err()
        .expect("a zero sweep interval is refused");
    assert!(
        error.to_string().contains("credit_sweep_interval"),
        "{error}"
    );
    wait_for_threads(0);
    assert!(
        spawned_ran.load(Ordering::SeqCst),
        "the shutdown hook ran after velo's runtime stopped"
    );
    assert!(
        closed.load(Ordering::SeqCst),
        "velo's runtime stopped before the transport closed"
    );
}

/// A `Messenger` clone kept past the last drop does not keep velo's runtime
/// running: the teardown starts once the drop has waited a bounded time for
/// the `Messenger`'s own drop, and the clone stops working.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_kept_messenger_does_not_keep_the_runtime() {
    let _serial = serial().await;
    let panics = panics();
    let before = panics.load(Ordering::SeqCst);
    let a = node(TaskRuntime::dedicated()).await;
    let b = node(TaskRuntime::Application).await;
    introduce(&a, &b).await;
    echo(&a, &b).await.expect("echo");
    let kept = Arc::clone(a.messenger());
    let started = Instant::now();
    tokio::spawn(async move { drop(a) }).await.expect("drop");
    wait_for_threads(0);
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the runtime stopped only after {:?}",
        started.elapsed()
    );
    let call = tokio::time::timeout(
        PATIENCE,
        kept.unary("echo")
            .unwrap()
            .raw_payload(Bytes::from_static(b"ping"))
            .instance(b.instance_id())
            .send(),
    )
    .await
    .expect("a call on the kept messenger hung");
    assert!(call.is_err(), "the kept messenger still sends");
    drop(kept);
    b.shutdown(ShutdownPolicy::WaitForever).await;
    assert_eq!(panics.load(Ordering::SeqCst), before, "a thread panicked");
}

/// A `Velo` built on a current-thread runtime, as in a plain
/// `#[tokio::test]`, serves calls and streams: handlers run on that runtime,
/// and velo's own thread does the rest.
#[tokio::test]
async fn a_dedicated_runtime_works_under_a_current_thread_test() {
    let _serial = serial().await;
    let a = node(TaskRuntime::dedicated()).await;
    let b = node(TaskRuntime::dedicated()).await;
    introduce(&a, &b).await;
    for _ in 0..10 {
        assert_eq!(&echo(&a, &b).await.expect("echo")[..], b"ping");
        assert_eq!(&echo(&b, &a).await.expect("echo")[..], b"ping");
    }
    stream_once(&a, &b).await;
    stream_once(&b, &a).await;
    a.shutdown(ShutdownPolicy::WaitForever).await;
    b.shutdown(ShutdownPolicy::WaitForever).await;
    wait_for_threads(0);
}

/// Handlers stay on the application's runtime: the plain, the ordered and
/// the async kind. On the dedicated runtime the inbound loop runs on
/// `velo-rt`, and a handler spawned from it with `tokio::spawn` would run
/// there too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handlers_run_on_the_application_runtime() {
    let _serial = serial().await;
    let client = node(TaskRuntime::dedicated()).await;
    let server = node(TaskRuntime::dedicated()).await;
    let thread = || {
        Some(Bytes::from(
            std::thread::current().name().unwrap_or("?").to_owned(),
        ))
    };
    server
        .register_handler(Handler::unary_handler("plain", move |_| Ok(thread())).build())
        .expect("plain");
    server
        .register_handler(
            Handler::unary_handler("ordered", move |_| Ok(thread()))
                .ordered()
                .build(),
        )
        .expect("ordered");
    server
        .register_handler(
            Handler::unary_handler_async("async", move |_| async move { Ok(thread()) }).build(),
        )
        .expect("async");
    introduce(&client, &server).await;
    for name in ["plain", "ordered", "async"] {
        let ran_on = client
            .unary(name)
            .unwrap()
            .instance(server.instance_id())
            .send()
            .await
            .expect("call");
        let ran_on = String::from_utf8_lossy(&ran_on).into_owned();
        assert_ne!(
            ran_on, THREAD_NAME,
            "the {name} handler ran on velo's runtime"
        );
        assert!(
            ran_on.starts_with("tokio-"),
            "the {name} handler ran on {ran_on}"
        );
    }
    client.shutdown(ShutdownPolicy::WaitForever).await;
    server.shutdown(ShutdownPolicy::WaitForever).await;
}

/// After `shutdown` stopped the runtime, a clone's calls and streams fail at
/// once, as they do on the application's runtime. They must not panic (a
/// timer polled against a stopped runtime does) and must not hang.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_clone_fails_cleanly_after_shutdown() {
    let _serial = serial().await;
    let panics = panics();
    let before = panics.load(Ordering::SeqCst);
    let a = node(TaskRuntime::dedicated()).await;
    let b = node(TaskRuntime::dedicated()).await;
    introduce(&a, &b).await;
    echo(&a, &b).await.expect("echo");
    let clone = a.clone();
    a.shutdown(ShutdownPolicy::WaitForever).await;

    let error = echo(&clone, &b).await.expect_err("a call after shutdown");
    assert!(
        !error.to_string().contains("timed out"),
        "the call hung: {error}"
    );
    let anchor = b.create_anchor::<u32>();
    let ticket = b.prebind_anchor(anchor.handle()).expect("ticket");
    let open = tokio::time::timeout(
        PATIENCE,
        clone.open_anchor_stream::<u32>(anchor.handle(), ticket),
    )
    .await
    .expect("opening a stream after shutdown hung");
    assert!(open.is_err(), "a stream opened after shutdown");
    b.shutdown(ShutdownPolicy::WaitForever).await;
    drop((clone, anchor));
    wait_for_threads(0);
    assert_eq!(panics.load(Ordering::SeqCst), before, "a thread panicked");
}

/// `Dedicated` takes 1 or 2 threads; `build` names the limit for any other
/// count, and starts nothing.
#[tokio::test]
async fn build_refuses_a_thread_count_other_than_one_or_two() {
    let _serial = serial().await;
    for threads in [0, 3] {
        let error = Velo::builder()
            .add_transport(tcp())
            .mux_only()
            .task_runtime(TaskRuntime::Dedicated { threads })
            .build()
            .await
            .err()
            .expect("refused");
        assert!(error.to_string().contains("1 to 2 threads"), "{error}");
        wait_for_threads(0);
    }
}

/// A failed build keeps velo's runtime running while a transport's shutdown
/// hook is still running: a task the hook hands to the runtime then is not
/// cancelled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_build_keeps_the_runtime_until_teardown_finishes() {
    struct HeldHook {
        inner: NeedsRuntime,
        runtime: std::sync::OnceLock<tokio::runtime::Handle>,
        entered: flume::Sender<()>,
        release: flume::Receiver<()>,
    }

    impl velo::Transport for HeldHook {
        fn key(&self) -> velo::ext::TransportKey {
            self.inner.key()
        }

        fn address(&self) -> velo::WorkerAddress {
            self.inner.address()
        }

        fn register(&self, peer: velo::PeerInfo) -> Result<(), velo::ext::TransportError> {
            self.inner.register(peer)
        }

        fn send_message(
            &self,
            id: velo::InstanceId,
            header: Bytes,
            payload: Bytes,
            kind: velo::ext::MessageType,
            on_error: Arc<dyn velo::ext::TransportErrorHandler>,
        ) -> velo::ext::SendOutcome {
            self.inner.send_message(id, header, payload, kind, on_error)
        }

        fn start(
            &self,
            id: velo::InstanceId,
            channels: velo::ext::TransportAdapter,
            runtime: tokio::runtime::Handle,
        ) -> futures::future::BoxFuture<'_, anyhow::Result<()>> {
            self.runtime.set(runtime.clone()).unwrap();
            self.inner.start(id, channels, runtime)
        }

        fn shutdown(&self) {
            self.entered.send(()).unwrap();
            let _ = self.release.recv_timeout(PATIENCE);
            self.inner.shutdown();
        }

        fn closed(&self) -> futures::future::BoxFuture<'_, ()> {
            self.inner.closed()
        }

        fn check_health(
            &self,
            id: velo::InstanceId,
            timeout: Duration,
        ) -> futures::future::BoxFuture<'_, Result<(), velo::ext::HealthCheckError>> {
            self.inner.check_health(id, timeout)
        }
    }

    let _serial = serial().await;
    let (entered, in_hook) = flume::bounded(1);
    let (release, held) = flume::bounded(1);
    let transport = Arc::new(HeldHook {
        inner: NeedsRuntime {
            spawned_ran: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            closed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        },
        runtime: std::sync::OnceLock::new(),
        entered,
        release: held,
    });
    let mut build = Box::pin(
        Velo::builder()
            .add_transport(transport.clone())
            .task_runtime(TaskRuntime::dedicated())
            .messenger_mux(velo::streaming::MuxConfig {
                credit_sweep_interval: Duration::ZERO,
                ..Default::default()
            })
            .unwrap()
            .build(),
    );
    let result = futures::poll!(build.as_mut());
    assert!(!matches!(result, std::task::Poll::Ready(Ok(_))));
    tokio::time::timeout(PATIENCE, in_hook.recv_async())
        .await
        .expect("the failed build never began teardown")
        .unwrap();

    // The transport still holds its shutdown hook, so its driver must live.
    let probe = tokio::time::timeout(PATIENCE, transport.runtime.get().unwrap().spawn(async {}))
        .await
        .expect("the runtime probe never completed");
    release.send(()).unwrap();
    if result.is_pending() {
        assert!(build.await.is_err());
    }
    wait_for_threads(0);
    assert!(
        probe.is_ok(),
        "the failed build stopped its runtime before the transport's shutdown hook finished: {probe:?}"
    );
}

/// A build cancelled while a later transport starts lets an earlier one
/// finish closing: velo's runtime stays up until it is idle.
#[tokio::test]
async fn a_cancelled_build_keeps_the_runtime_until_started_transports_close() {
    struct StartingTransport {
        key: &'static str,
        pending: bool,
        runtime: std::sync::OnceLock<tokio::runtime::Handle>,
        release: flume::Receiver<()>,
        finished: Arc<std::sync::atomic::AtomicBool>,
    }

    impl velo::Transport for StartingTransport {
        fn key(&self) -> velo::ext::TransportKey {
            velo::ext::TransportKey::new(self.key)
        }

        fn address(&self) -> velo::WorkerAddress {
            let entries = std::collections::BTreeMap::from([(self.key, b"x".to_vec())]);
            velo::WorkerAddress::from_encoded(rmp_serde::to_vec(&entries).unwrap())
        }

        fn register(&self, _: velo::PeerInfo) -> Result<(), velo::ext::TransportError> {
            Ok(())
        }

        fn send_message(
            &self,
            _: velo::InstanceId,
            _: Bytes,
            _: Bytes,
            _: velo::ext::MessageType,
            _: Arc<dyn velo::ext::TransportErrorHandler>,
        ) -> velo::ext::SendOutcome {
            velo::ext::SendOutcome::Admitted
        }

        fn start(
            &self,
            _: velo::InstanceId,
            _: velo::ext::TransportAdapter,
            runtime: tokio::runtime::Handle,
        ) -> futures::future::BoxFuture<'_, anyhow::Result<()>> {
            self.runtime.set(runtime).unwrap();
            Box::pin(async move {
                if self.pending {
                    std::future::pending::<()>().await;
                }
                Ok(())
            })
        }

        fn shutdown(&self) {
            let release = self.release.clone();
            let finished = Arc::clone(&self.finished);
            self.runtime.get().unwrap().spawn(async move {
                release.recv_async().await.unwrap();
                finished.store(true, Ordering::SeqCst);
            });
        }

        fn closed(&self) -> futures::future::BoxFuture<'_, ()> {
            Box::pin(async move {
                while !self.finished.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
        }

        fn check_health(
            &self,
            _: velo::InstanceId,
            _: Duration,
        ) -> futures::future::BoxFuture<'_, Result<(), velo::ext::HealthCheckError>> {
            Box::pin(async { Ok(()) })
        }
    }

    let _serial = serial().await;
    let (release, held) = flume::bounded(1);
    let transport = |key, pending| {
        Arc::new(StartingTransport {
            key,
            pending,
            runtime: std::sync::OnceLock::new(),
            release: held.clone(),
            finished: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    };
    let ready = transport("ready", false);
    let pending = transport("pending", true);
    let mut build = Box::pin(
        Velo::builder()
            .task_runtime(TaskRuntime::dedicated())
            .add_transport(ready.clone())
            .add_transport(pending.clone())
            .build(),
    );
    assert!(futures::poll!(build.as_mut()).is_pending());
    assert!(
        pending.runtime.get().is_some(),
        "the second start was not reached"
    );
    drop(build);

    // The close is held on a channel, so a live runtime must still run this probe.
    let probe = tokio::time::timeout(PATIENCE, ready.runtime.get().unwrap().spawn(async {}))
        .await
        .expect("runtime probe hung");
    release.send(()).unwrap();
    wait_for_threads(0);
    assert!(
        probe.is_ok(),
        "cancelling startup stopped the runtime before the completed transport closed: {probe:?}"
    );
    assert!(ready.finished.load(Ordering::SeqCst));
}

/// A build cancelled while the rollback of a failed start waits for a started
/// transport's close still lets that close finish before velo's runtime
/// stops. A duplicate transport key fails the start after the first
/// transport has started.
#[tokio::test]
async fn cancelling_a_failed_start_during_close_preserves_the_close() {
    struct CloseOnRelease {
        inner: NeedsRuntime,
        runtime: std::sync::OnceLock<tokio::runtime::Handle>,
        closing: std::sync::atomic::AtomicBool,
        release: flume::Receiver<()>,
        finished: std::sync::atomic::AtomicBool,
    }

    impl velo::Transport for CloseOnRelease {
        fn key(&self) -> velo::ext::TransportKey {
            self.inner.key()
        }

        fn address(&self) -> velo::WorkerAddress {
            self.inner.address()
        }

        fn register(&self, peer: velo::PeerInfo) -> Result<(), velo::ext::TransportError> {
            self.inner.register(peer)
        }

        fn send_message(
            &self,
            id: velo::InstanceId,
            header: Bytes,
            payload: Bytes,
            kind: velo::ext::MessageType,
            on_error: Arc<dyn velo::ext::TransportErrorHandler>,
        ) -> velo::ext::SendOutcome {
            self.inner.send_message(id, header, payload, kind, on_error)
        }

        fn start(
            &self,
            id: velo::InstanceId,
            channels: velo::ext::TransportAdapter,
            runtime: tokio::runtime::Handle,
        ) -> futures::future::BoxFuture<'_, anyhow::Result<()>> {
            self.runtime.set(runtime.clone()).unwrap();
            self.inner.start(id, channels, runtime)
        }

        fn shutdown(&self) {}

        fn closed(&self) -> futures::future::BoxFuture<'_, ()> {
            Box::pin(async move {
                self.closing.store(true, Ordering::SeqCst);
                tokio::time::timeout(Duration::from_secs(2), self.release.recv_async())
                    .await
                    .expect("the test did not release close")
                    .unwrap();
                let probe = self.runtime.get().unwrap().spawn(async {}).await;
                self.finished.store(probe.is_ok(), Ordering::SeqCst);
            })
        }

        fn check_health(
            &self,
            id: velo::InstanceId,
            timeout: Duration,
        ) -> futures::future::BoxFuture<'_, Result<(), velo::ext::HealthCheckError>> {
            self.inner.check_health(id, timeout)
        }
    }

    let _serial = serial().await;
    for cancel in [false, true] {
        let (release, held) = flume::bounded(1);
        let transport = Arc::new(CloseOnRelease {
            inner: NeedsRuntime {
                spawned_ran: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                closed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            },
            runtime: std::sync::OnceLock::new(),
            closing: std::sync::atomic::AtomicBool::new(false),
            release: held,
            finished: std::sync::atomic::AtomicBool::new(false),
        });
        // The duplicate key fails only after the first transport starts.
        let mut build = Box::pin(
            Velo::builder()
                .task_runtime(TaskRuntime::dedicated())
                .add_transport(transport.clone())
                .add_transport(transport.clone())
                .build(),
        );
        assert!(futures::poll!(build.as_mut()).is_pending());
        // The close runs on a task of velo's runtime, not inside this poll.
        let deadline = Instant::now() + PATIENCE;
        while !transport.closing.load(Ordering::SeqCst) {
            assert!(
                Instant::now() < deadline,
                "the rollback never began to close"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        if cancel {
            drop(build);
        } else {
            release.send(()).unwrap();
            let error = build.await.err().expect("duplicate key must fail");
            assert!(error.to_string().contains("Duplicate transport key"));
        }
        if cancel {
            release.send(()).unwrap();
        }
        wait_for_threads(0);
        assert!(
            transport.finished.load(Ordering::SeqCst),
            "cancelling the failed build abandoned closed() and stopped its runtime (cancel={cancel})"
        );
    }
}

/// Demonstration of a limit: a NATS client the application connected keeps
/// its I/O on the runtime that connected it, so under
/// `TaskRuntime::Dedicated` a NATS write still waits for the application's
/// runtime. velo cannot move a client it was handed. Run with `--ignored`.
#[cfg(feature = "nats-transport")]
#[test]
#[ignore = "demonstration: the application's NATS client runs where the application connected it"]
fn a_nats_client_runs_where_the_application_connected_it() {
    use std::io::{BufRead, Read, Write};
    use velo::Transport;
    use velo::ext::{MessageType, TransportErrorHandler};

    struct SendError(flume::Sender<String>);
    impl TransportErrorHandler for SendError {
        fn on_error(&self, _: Bytes, _: Bytes, error: String) {
            let _ = self.0.send(error);
        }
    }

    let _serial = SERIAL.blocking_lock();
    wait_for_threads(0);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (wire_tx, wire) = flume::unbounded();
    let (socket_tx, socket_rx) = flume::bounded(1);
    // A local protocol peer avoids a dependency on a running NATS service.
    let peer = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket.set_read_timeout(Some(PATIENCE)).unwrap();
        socket_tx.send(socket.try_clone().unwrap()).unwrap();
        socket
            .write_all(b"INFO {\"server_id\":\"review\",\"proto\":1,\"headers\":true,\"max_payload\":1048576}\r\n")
            .unwrap();
        let mut reader = std::io::BufReader::new(socket.try_clone().unwrap());
        loop {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
            let fields: Vec<_> = line.split_ascii_whitespace().collect();
            match fields.first().copied() {
                Some("PING") => socket.write_all(b"PONG\r\n").unwrap(),
                Some("HPUB") => {
                    let header_len: usize = fields[fields.len() - 2].parse().unwrap();
                    let total_len: usize = fields.last().unwrap().parse().unwrap();
                    let mut frame = vec![0; total_len + 2];
                    reader.read_exact(&mut frame).unwrap();
                    assert_eq!(&frame[total_len..], b"\r\n");
                    wire_tx.send(frame[header_len..total_len].to_vec()).unwrap();
                }
                Some("CONNECT" | "SUB" | "UNSUB" | "PONG") => {}
                other => panic!("unexpected NATS command: {other:?}"),
            }
        }
    });
    let application = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (errors_tx, errors) = flume::unbounded();
    let on_error: Arc<dyn TransportErrorHandler> = Arc::new(SendError(errors_tx));
    let (velo, transport) = application.block_on(async {
        let client = velo::transports::nats::utils::connect(&format!("nats://{address}"))
            .await
            .unwrap();
        let transport =
            Arc::new(velo::transports::nats::NatsTransportBuilder::new(client, "review").build());
        let velo = Velo::builder()
            .task_runtime(TaskRuntime::dedicated())
            .add_transport(transport.clone())
            .build()
            .await
            .unwrap();
        velo.register_peer(velo.peer_info()).unwrap();
        let outcome = transport.send_message(
            velo.instance_id(),
            Bytes::new(),
            Bytes::from_static(b"warm"),
            MessageType::Message,
            Arc::clone(&on_error),
        );
        assert!(outcome.is_admitted());
        assert_eq!(
            tokio::time::timeout(PATIENCE, wire.recv_async())
                .await
                .unwrap()
                .unwrap(),
            b"warm"
        );
        (velo, transport)
    });

    // A current-thread runtime runs nothing between calls to block_on.
    assert!(
        transport
            .send_message(
                velo.instance_id(),
                Bytes::new(),
                Bytes::from_static(b"held"),
                MessageType::Message,
                on_error,
            )
            .is_admitted()
    );
    let during_hold = wire.recv_timeout(Duration::from_millis(500));
    if during_hold.is_err() {
        let after_hold = application.block_on(async {
            tokio::time::timeout(PATIENCE, wire.recv_async())
                .await
                .unwrap()
                .unwrap()
        });
        assert_eq!(
            after_hold, b"held",
            "resuming the application did not flush the frame"
        );
    }
    application.block_on(velo.shutdown(ShutdownPolicy::WaitForever));
    socket_rx
        .recv_timeout(PATIENCE)
        .unwrap()
        .shutdown(std::net::Shutdown::Both)
        .unwrap();
    peer.join().unwrap();
    wait_for_threads(0);
    assert!(
        errors.is_empty(),
        "NATS send failed: {:?}",
        errors.try_recv()
    );
    assert_eq!(
        during_hold,
        Ok(b"held".to_vec()),
        "the dedicated NATS transport still needs the application's runtime to write"
    );
}

/// The name of the thread this runs on.
fn thread_name() -> String {
    std::thread::current().name().unwrap_or("?").to_owned()
}

/// A TCP transport that records the thread of every request and response
/// it is asked to send, and the thread its writers run on: the TCP transport
/// spawns each connection's writer on the handle `start` gives it.
struct Watched {
    inner: Arc<velo::transports::tcp::TcpTransport>,
    sends: Arc<parking_lot::Mutex<Vec<(velo::ext::MessageType, String)>>>,
    writer_thread: Arc<parking_lot::Mutex<Option<String>>>,
}

impl Watched {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: tcp(),
            sends: Arc::default(),
            writer_thread: Arc::default(),
        })
    }

    fn record(&self, message_type: velo::ext::MessageType) {
        if matches!(
            message_type,
            velo::ext::MessageType::Message | velo::ext::MessageType::Response
        ) {
            self.sends.lock().push((message_type, thread_name()));
        }
    }
}

impl velo::Transport for Watched {
    fn key(&self) -> velo::ext::TransportKey {
        self.inner.key()
    }
    fn address(&self) -> velo::WorkerAddress {
        self.inner.address()
    }
    fn register(&self, peer: velo::PeerInfo) -> Result<(), velo::ext::TransportError> {
        self.inner.register(peer)
    }
    fn unregister(&self, instance_id: velo::InstanceId) {
        self.inner.unregister(instance_id)
    }
    fn send_message(
        &self,
        instance_id: velo::InstanceId,
        header: Bytes,
        payload: Bytes,
        message_type: velo::ext::MessageType,
        on_error: Arc<dyn velo::ext::TransportErrorHandler>,
    ) -> velo::ext::SendOutcome {
        self.record(message_type);
        self.inner
            .send_message(instance_id, header, payload, message_type, on_error)
    }
    fn lanes(&self, target: velo::InstanceId) -> std::num::NonZeroU16 {
        self.inner.lanes(target)
    }
    fn send_message_on_lane(
        &self,
        instance_id: velo::InstanceId,
        lane: u16,
        header: Bytes,
        payload: Bytes,
        message_type: velo::ext::MessageType,
        on_error: Arc<dyn velo::ext::TransportErrorHandler>,
    ) -> velo::ext::SendOutcome {
        self.record(message_type);
        self.inner
            .send_message_on_lane(instance_id, lane, header, payload, message_type, on_error)
    }
    fn max_message_size(&self, target: velo::InstanceId) -> Option<usize> {
        self.inner.max_message_size(target)
    }
    fn start(
        &self,
        instance_id: velo::InstanceId,
        channels: velo::ext::TransportAdapter,
        rt: tokio::runtime::Handle,
    ) -> futures::future::BoxFuture<'_, anyhow::Result<()>> {
        let writer_thread = Arc::clone(&self.writer_thread);
        rt.spawn(async move {
            *writer_thread.lock() = Some(thread_name());
        });
        self.inner.start(instance_id, channels, rt)
    }
    fn shutdown(&self) {
        self.inner.shutdown()
    }
    fn closed(&self) -> futures::future::BoxFuture<'_, ()> {
        self.inner.closed()
    }
    fn set_observability(&self, observability: Arc<dyn velo::ext::TransportObservability>) {
        self.inner.set_observability(observability)
    }
    fn begin_drain(&self) {
        self.inner.begin_drain()
    }
    fn check_health(
        &self,
        instance_id: velo::InstanceId,
        timeout: Duration,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), velo::ext::HealthCheckError>> + Send + '_>,
    > {
        self.inner.check_health(instance_id, timeout)
    }
}

/// One recorded wake of a caller's task.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Wake {
    thread: String,
    /// The wake ran on the caller's own thread, inside its own poll.
    /// `AtomicWaker::register` does that with a wake that races the
    /// registration, whatever thread sent it, so such a wake does not say
    /// where the response came from.
    in_own_poll: bool,
}

impl Wake {
    fn from_velo() -> Self {
        Self {
            thread: THREAD_NAME.to_owned(),
            in_own_poll: false,
        }
    }
}

thread_local! {
    /// Set while `record_wakes` polls its future on this thread.
    static IN_RECORDED_POLL: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Wakes the task it wraps, and records each wake.
struct RecordingWaker {
    inner: std::task::Waker,
    wakes: Arc<parking_lot::Mutex<Vec<Wake>>>,
}

impl futures::task::ArcWake for RecordingWaker {
    fn wake_by_ref(this: &Arc<Self>) {
        this.wakes.lock().push(Wake {
            thread: thread_name(),
            in_own_poll: IN_RECORDED_POLL.get(),
        });
        this.inner.wake_by_ref();
    }
}

/// Polls `inner` with a waker that records each wake. Returns whether
/// `inner` ever returned `Pending`.
async fn record_wakes<F: std::future::Future>(
    inner: F,
    wakes: Arc<parking_lot::Mutex<Vec<Wake>>>,
) -> bool {
    let mut inner = std::pin::pin!(inner);
    let mut waited = false;
    std::future::poll_fn(|cx| {
        let waker = futures::task::waker(Arc::new(RecordingWaker {
            inner: cx.waker().clone(),
            wakes: Arc::clone(&wakes),
        }));
        IN_RECORDED_POLL.set(true);
        let poll = inner
            .as_mut()
            .poll(&mut std::task::Context::from_waker(&waker));
        IN_RECORDED_POLL.set(false);
        waited |= poll.is_pending();
        poll.map(drop)
    })
    .await;
    waited
}

/// How the response of one call reached its caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CallEnd {
    /// One wake, from a velo thread.
    WokenFromVelo,
    /// One wake, from an application thread.
    WokenFromApplication,
    /// No wake: the response was there before the caller's first poll
    /// registered for it.
    NoWake,
    /// One wake that the caller's own poll delivered to itself: a response
    /// raced the registration. It names no thread.
    NoWitness,
    /// More than one wake, or a wait with no wake.
    Other,
}

/// Classifies a call by its wakes, not by whether it returned `Pending`.
/// The response slot registers the caller's waker before it checks for the
/// value, so a response that lands between the two wakes the caller while
/// that same poll returns `Ready`. Call it only once the wakes have settled:
/// the slot publishes the value before it wakes, so another poll can finish
/// the call before its wake is recorded.
fn classify(waited: bool, wakes: &[Wake]) -> CallEnd {
    match wakes {
        [] if !waited => CallEnd::NoWake,
        [wake] if wake.in_own_poll => CallEnd::NoWitness,
        [wake] if wake.thread == THREAD_NAME => CallEnd::WokenFromVelo,
        [_] => CallEnd::WokenFromApplication,
        _ => CallEnd::Other,
    }
}

/// Where the parts of a run of unary calls ran, counted by thread.
#[derive(Debug, Default, PartialEq, Eq)]
struct Crossings {
    /// Requests and responses handed to the transport from an application
    /// thread. Each one wakes a writer on the runtime `start` was given.
    sends_from_application: usize,
    /// Handler runs on an application thread. The inbound loop that spawns
    /// the handler runs where the transport reads, on the runtime `start`
    /// was given.
    handlers_on_application: usize,
    /// Calls whose caller was woken once, from a velo thread: the response
    /// path.
    woken_from_velo: usize,
    /// Calls whose caller was woken once, from an application thread.
    woken_from_application: usize,
    /// Calls that ended with no wake.
    no_wake: usize,
    /// Calls whose one wake came from their own poll.
    no_witness: usize,
    /// Calls that fit none of the above.
    other: usize,
}

/// Makes `calls` unary calls one at a time, after a warm-up, and counts
/// where each part ran.
async fn count_crossings(runtime: TaskRuntime, calls: usize) -> (Crossings, String) {
    let client_transport = Watched::new();
    let server_transport = Watched::new();
    let build = |transport: Arc<Watched>| async move {
        Velo::builder()
            .add_transport(transport)
            .mux_only()
            .task_runtime(runtime)
            .build()
            .await
            .expect("build velo")
    };
    let client = build(Arc::clone(&client_transport)).await;
    let server = build(Arc::clone(&server_transport)).await;
    let handler_threads = Arc::new(parking_lot::Mutex::new(Vec::new()));
    server
        .register_handler(
            Handler::unary_handler("probe", {
                let handler_threads = Arc::clone(&handler_threads);
                move |ctx: Context| {
                    handler_threads.lock().push(thread_name());
                    // The caller is then waiting when the response comes
                    // back, so almost every call has a wake to count.
                    std::thread::sleep(Duration::from_millis(1));
                    Ok(Some(ctx.payload))
                }
            })
            .build(),
        )
        .expect("probe");
    introduce(&client, &server).await;
    let call = || async {
        client
            .unary("probe")
            .expect("unary")
            .raw_payload(Bytes::from_static(b"ping"))
            .instance(server.instance_id())
            .send()
            .await
            .expect("call")
    };
    // The first call dials the connection; it is not the steady state.
    for _ in 0..3 {
        tokio::time::timeout(PATIENCE, call())
            .await
            .expect("warm-up");
    }
    client_transport.sends.lock().clear();
    server_transport.sends.lock().clear();
    handler_threads.lock().clear();

    let mut per_call = Vec::with_capacity(calls);
    for _ in 0..calls {
        let wakes = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let waited = tokio::time::timeout(PATIENCE, record_wakes(call(), Arc::clone(&wakes)))
            .await
            .expect("call timed out");
        per_call.push((waited, wakes));
    }
    // `shutdown` returns once velo's tasks are done and, on the dedicated
    // runtime, its threads have exited, so every wake has been recorded.
    client.shutdown(ShutdownPolicy::WaitForever).await;
    server.shutdown(ShutdownPolicy::WaitForever).await;
    let all_wakes: Vec<_> = per_call
        .iter()
        .map(|(waited, wakes)| (*waited, wakes.lock().clone()))
        .collect();
    let ends: Vec<_> = all_wakes
        .iter()
        .map(|(waited, wakes)| classify(*waited, wakes))
        .collect();

    let is_velo = |thread: &String| thread == THREAD_NAME;
    let sends: Vec<_> = client_transport
        .sends
        .lock()
        .iter()
        .chain(server_transport.sends.lock().iter())
        .cloned()
        .collect();
    let count = |end: CallEnd| ends.iter().filter(|e| **e == end).count();
    let crossings = Crossings {
        sends_from_application: sends.iter().filter(|(_, t)| !is_velo(t)).count(),
        handlers_on_application: handler_threads
            .lock()
            .iter()
            .filter(|t| !is_velo(t))
            .count(),
        woken_from_velo: count(CallEnd::WokenFromVelo),
        woken_from_application: count(CallEnd::WokenFromApplication),
        no_wake: count(CallEnd::NoWake),
        no_witness: count(CallEnd::NoWitness),
        other: count(CallEnd::Other),
    };
    let detail = format!(
        "sends {sends:?}, handler threads {:?}, (waited, caller wakes) per call {all_wakes:?}, \
         writers on {:?} and {:?}",
        handler_threads.lock(),
        client_transport.writer_thread.lock(),
        server_transport.writer_thread.lock()
    );
    assert_eq!(
        sends.len(),
        2 * calls,
        "one request and one response per call: {detail}"
    );
    (crossings, detail)
}

/// On the default runtime, a unary call to a handler crosses between the
/// application's runtime and velo's four times:
///
/// 1. the caller hands the request to the client's writer, on velo's runtime;
/// 2. the server's inbound loop, on velo's runtime, spawns the handler on the
///    application's runtime;
/// 3. the handler hands its response to the server's writer, on velo's
///    runtime;
/// 4. the client's response path, on velo's runtime, wakes the caller.
///
/// The fourth is a wake only if the response comes after the caller's first
/// poll registered for it. A call whose response was already there ends
/// with no wake, so the test allows that and requires a velo wake for every
/// other call (the handler's 1 ms sleep makes those almost all of them).
///
/// On `TaskRuntime::Application` the same call runs on the application's
/// runtime throughout. Each crossing is a wake across threads, and it is the
/// cost of the default for a single caller: the book's "A runtime for velo's
/// own tasks" chapter gives the measured time and CPU. A change that adds a
/// crossing on this path changes that cost and must show here.
///
/// The count is of hand-offs, not of time: a hand-off to a velo thread is a
/// send made on an application thread, because the transport's writer runs on
/// velo's runtime (the probe in `Watched::start` checks that).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_unary_call_crosses_between_runtimes_four_times() {
    let _serial = serial().await;
    const CALLS: usize = 20;
    let (crossings, detail) = count_crossings(TaskRuntime::dedicated(), CALLS).await;
    assert!(
        detail.ends_with(&format!(
            "writers on Some({THREAD_NAME:?}) and Some({THREAD_NAME:?})"
        )),
        "the writers do not run on velo's runtime: {detail}"
    );
    assert!(
        crossings.woken_from_velo > 0,
        "no caller was woken, so the response hand-off was not seen: {detail}"
    );
    assert_eq!(
        crossings,
        Crossings {
            sends_from_application: 2 * CALLS,
            handlers_on_application: CALLS,
            woken_from_velo: CALLS - crossings.no_wake - crossings.no_witness,
            woken_from_application: 0,
            no_wake: crossings.no_wake,
            no_witness: crossings.no_witness,
            other: 0,
        },
        "{detail}"
    );
}

/// The control for the count above: on the application's runtime nothing of
/// the call runs on a velo thread, so nothing crosses.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_unary_call_on_the_application_runtime_stays_there() {
    let _serial = serial().await;
    const CALLS: usize = 20;
    let (crossings, detail) = count_crossings(TaskRuntime::Application, CALLS).await;
    assert!(
        !detail.contains(THREAD_NAME),
        "a velo thread exists on the application's runtime: {detail}"
    );
    assert_eq!(crossings.woken_from_velo, 0, "{detail}");
    assert_eq!(crossings.other, 0, "{detail}");
    assert_eq!(
        crossings.woken_from_application + crossings.no_wake + crossings.no_witness,
        CALLS,
        "{detail}"
    );
}

/// A response that lands after the caller's waker is registered but before
/// the same poll checks for it wakes the caller, and that poll still returns
/// `Ready`. `Slot::poll_wait` registers first, then checks. The count must
/// take the wake as the response hand-off, though the call never waited.
#[tokio::test]
async fn a_wake_inside_the_first_poll_counts_as_the_response_hand_off() {
    // The responder thread is named `velo-rt`, which the other tests count.
    let _serial = serial().await;
    let slot_waker = Arc::new(futures::task::AtomicWaker::new());
    let (registered, may_complete) = std::sync::mpsc::channel();
    let (completed, may_read) = std::sync::mpsc::channel();
    let responder = std::thread::Builder::new()
        .name(THREAD_NAME.to_owned())
        .spawn({
            let slot_waker = Arc::clone(&slot_waker);
            move || {
                may_complete.recv().expect("waker registered");
                slot_waker.wake();
                completed.send(()).expect("response completed");
            }
        })
        .expect("response thread");
    let wakes = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let response = std::future::poll_fn(|cx| {
        slot_waker.register(cx.waker());
        registered.send(()).expect("allow response");
        may_read
            .recv()
            .expect("response available before the check");
        std::task::Poll::Ready(())
    });
    let waited = record_wakes(response, Arc::clone(&wakes)).await;
    responder.join().expect("response thread");
    let wakes = wakes.lock().clone();
    assert!(!waited);
    assert_eq!(wakes, [Wake::from_velo()]);
    assert_eq!(classify(waited, &wakes), CallEnd::WokenFromVelo);
    assert_eq!(classify(false, &[]), CallEnd::NoWake);
    assert_eq!(classify(true, &[]), CallEnd::Other);
    let application = Wake {
        thread: "tokio-runtime-worker".to_owned(),
        in_own_poll: false,
    };
    assert_eq!(
        classify(true, &[application]),
        CallEnd::WokenFromApplication
    );
    assert_eq!(
        classify(true, &[Wake::from_velo(), Wake::from_velo()]),
        CallEnd::Other
    );
}

/// A response slot publishes its value before it wakes the caller, so a
/// spurious poll can finish the call before its wake is recorded. The wake
/// still comes, from velo's runtime, and once the instances have shut down
/// it is there to count: `count_crossings` classifies only then.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_response_read_before_its_wake_counts_as_a_valid_call() {
    use std::future::Future;
    use std::task::{Context as TaskContext, Poll, Waker};

    struct HeldWake {
        inner: Waker,
        entered: flume::Sender<()>,
        release: flume::Receiver<()>,
    }

    impl futures::task::ArcWake for HeldWake {
        fn wake_by_ref(this: &Arc<Self>) {
            this.entered.send(()).unwrap();
            this.release.recv().unwrap();
            this.inner.wake_by_ref();
        }
    }

    let _serial = serial().await;
    let client = node(TaskRuntime::dedicated()).await;
    let server = node(TaskRuntime::dedicated()).await;
    let (answer, may_answer) = flume::bounded::<()>(1);
    server
        .register_handler(
            Handler::unary_handler("probe", move |ctx: Context| {
                may_answer.recv().unwrap();
                Ok(Some(ctx.payload))
            })
            .build(),
        )
        .unwrap();
    introduce(&client, &server).await;
    echo(&client, &server).await.unwrap();

    let call = client
        .unary("probe")
        .unwrap()
        .raw_payload(Bytes::from_static(b"ping"))
        .instance(server.instance_id())
        .send();
    let mut call = std::pin::pin!(call);
    let (entered, waking) = flume::bounded(1);
    let (release, may_wake) = flume::bounded(1);
    let held = std::future::poll_fn(|cx| {
        let waker = futures::task::waker(Arc::new(HeldWake {
            inner: cx.waker().clone(),
            entered: entered.clone(),
            release: may_wake.clone(),
        }));
        call.as_mut()
            .poll(&mut TaskContext::from_waker(&waker))
            .map(|result| assert_eq!(result.unwrap(), Bytes::from_static(b"ping")))
    });
    let wakes = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let mut recorded = std::pin::pin!(record_wakes(held, Arc::clone(&wakes)));
    assert!(futures::poll!(recorded.as_mut()).is_pending());
    answer.send(()).unwrap();
    waking.recv_async().await.unwrap();

    // A spurious poll is legal while the response thread is about to wake us.
    let second_poll = futures::poll!(recorded.as_mut());
    let at_completion = wakes.lock().clone();
    release.send(()).unwrap();
    client.shutdown(ShutdownPolicy::WaitForever).await;
    server.shutdown(ShutdownPolicy::WaitForever).await;
    let settled = wakes.lock().clone();

    assert_eq!(second_poll, Poll::Ready(true));
    assert!(
        at_completion.is_empty(),
        "the call finished before its wake: {at_completion:?}"
    );
    assert_eq!(settled, [Wake::from_velo()]);
    assert_eq!(classify(true, &settled), CallEnd::WokenFromVelo);
}

/// `AtomicWaker::register` delivers a wake that races the registration
/// itself, on the registering thread, inside the caller's poll. The test
/// holds the registration (in the waker's clone) until a later response
/// proves that velo's response loop has already sent this call's wake. The
/// recorded wake then names the caller's thread, not velo's, so the count
/// must take it as no witness rather than as a wake from the application.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_response_wake_during_registration_counts_as_a_valid_call() {
    use std::future::Future;
    use std::mem::ManuallyDrop;
    use std::task::{Context as TaskContext, RawWaker, RawWakerVTable, Waker};

    struct HeldClone {
        inner: Waker,
        entered: flume::Sender<()>,
        completed: flume::Receiver<()>,
    }

    fn raw(held: Arc<HeldClone>) -> RawWaker {
        RawWaker::new(Arc::into_raw(held).cast(), &VTABLE)
    }

    unsafe fn clone_waker(data: *const ()) -> RawWaker {
        // Each raw waker owns one Arc; a clone borrows it without dropping it.
        let held = ManuallyDrop::new(unsafe { Arc::<HeldClone>::from_raw(data.cast()) });
        held.entered.send(()).unwrap();
        held.completed.recv_timeout(PATIENCE).unwrap();
        raw(Arc::clone(&held))
    }

    unsafe fn wake(data: *const ()) {
        // `wake` consumes the Arc owned by this raw waker.
        let held = unsafe { Arc::<HeldClone>::from_raw(data.cast()) };
        held.inner.wake_by_ref();
    }

    unsafe fn wake_by_ref(data: *const ()) {
        // A borrowed wake leaves the raw waker's Arc alive.
        let held = ManuallyDrop::new(unsafe { Arc::<HeldClone>::from_raw(data.cast()) });
        held.inner.wake_by_ref();
    }

    unsafe fn drop_waker(data: *const ()) {
        // `drop` releases the Arc owned by this raw waker.
        drop(unsafe { Arc::<HeldClone>::from_raw(data.cast()) });
    }

    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_waker, wake, wake_by_ref, drop_waker);

    let _serial = serial().await;
    let client = node(TaskRuntime::dedicated()).await;
    let server = node(TaskRuntime::dedicated()).await;
    let (answer, may_answer) = flume::bounded::<()>(1);
    server
        .register_handler(
            Handler::unary_handler("probe", move |ctx: Context| {
                if ctx.payload == Bytes::from_static(b"ping") {
                    may_answer.recv_timeout(PATIENCE).unwrap();
                }
                Ok(Some(ctx.payload))
            })
            .ordered()
            .build(),
        )
        .unwrap();
    introduce(&client, &server).await;
    echo(&client, &server).await.unwrap();

    let call = client
        .unary("probe")
        .unwrap()
        .raw_payload(Bytes::from_static(b"ping"))
        .instance(server.instance_id())
        .send();
    let mut call = std::pin::pin!(call);
    let (entered, cloning) = flume::bounded(1);
    let (completed, may_register) = flume::bounded(1);
    let helper = tokio::spawn({
        let client = client.clone();
        let target = server.instance_id();
        async move {
            cloning.recv_async().await.unwrap();
            answer.send(()).unwrap();
            // The ordered handler and TCP writer put this response second.
            // Receiving it proves the first response's wake has returned.
            let tail = client
                .unary("probe")
                .unwrap()
                .raw_payload(Bytes::from_static(b"tail"))
                .instance(target)
                .send();
            assert_eq!(
                tokio::time::timeout(PATIENCE, tail).await.unwrap().unwrap(),
                Bytes::from_static(b"tail")
            );
            completed.send(()).unwrap();
        }
    });
    let held = std::future::poll_fn(|cx| {
        let held = Arc::new(HeldClone {
            inner: cx.waker().clone(),
            entered: entered.clone(),
            completed: may_register.clone(),
        });
        // The vtable keeps one Arc alive for each raw waker and its clones.
        let waker = unsafe { Waker::from_raw(raw(held)) };
        call.as_mut()
            .poll(&mut TaskContext::from_waker(&waker))
            .map(|result| assert_eq!(result.unwrap(), Bytes::from_static(b"ping")))
    });
    let wakes = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let waited = record_wakes(held, Arc::clone(&wakes)).await;
    helper.await.unwrap();
    client.shutdown(ShutdownPolicy::WaitForever).await;
    server.shutdown(ShutdownPolicy::WaitForever).await;
    let settled = wakes.lock().clone();

    assert!(
        !waited,
        "the response is ready before registration finishes"
    );
    assert_eq!(
        settled,
        [Wake {
            thread: thread_name(),
            in_own_poll: true,
        }]
    );
    assert_eq!(classify(waited, &settled), CallEnd::NoWitness);
}
