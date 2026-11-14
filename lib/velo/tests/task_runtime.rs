// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The lifecycle of the runtime `TaskRuntime::Dedicated` builds: velo builds
//! it in `build`, stops it at the end of `shutdown`, and stops it without
//! blocking on the last drop.
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
