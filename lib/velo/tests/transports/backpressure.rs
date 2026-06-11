// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Backpressure semantics for `Transport::send_message`.
//!
//! These tests use a purpose-built [`SlowSendTransport`] whose writer task
//! sleeps between frames. That makes it trivial to saturate the per-peer
//! bounded flume channel and exercise the `SendBackpressure` return path —
//! real transports (tcp/zmq/etc.) drain so quickly that the Full branch is
//! almost never observable in practice.

use bytes::Bytes;
use futures::future::BoxFuture;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::OnceCell;

use velo::observability::VeloMetrics;
use velo::transports::{
    HealthCheckError, MessageType, SendBackpressure, Transport, TransportAdapter, TransportError,
    TransportErrorHandler,
};
use velo_ext::{InstanceId, PeerInfo, TransportKey, WorkerAddress};

// ── SlowSendTransport ─────────────────────────────────────────────────────

/// Test transport whose writer task sleeps `frame_delay` between consumed
/// frames, letting callers saturate the bounded send channel on demand.
///
/// The transport does not actually move bytes anywhere — it simulates a
/// slow wire by holding each frame for `frame_delay` before discarding it.
/// Use `consumed_count()` to observe drain progress.
struct SlowSendTransport {
    key: TransportKey,
    tx: flume::Sender<SendTask>,
    rx: parking_lot::Mutex<Option<flume::Receiver<SendTask>>>,
    frame_delay: Duration,
    consumed: Arc<AtomicU64>,
    started: Arc<AtomicBool>,
    /// When true, the writer task refuses to pull new frames. Lets tests
    /// hold the channel at its capacity so `SendBackpressure` futures stay
    /// pending deterministically.
    paused: Arc<AtomicBool>,
    /// Lazily populated via `Transport::set_observability`. Lets the bp test
    /// observe the transport send-backpressure counter.
    metrics: OnceCell<std::sync::Arc<dyn velo_ext::TransportObservability>>,
}

struct SendTask {
    _header: Bytes,
    _payload: Bytes,
    _on_error: Arc<dyn TransportErrorHandler>,
}

impl SlowSendTransport {
    fn new(capacity: usize, frame_delay: Duration) -> Arc<Self> {
        let (tx, rx) = flume::bounded(capacity);
        Arc::new(Self {
            key: TransportKey::from("slow"),
            tx,
            rx: parking_lot::Mutex::new(Some(rx)),
            frame_delay,
            consumed: Arc::new(AtomicU64::new(0)),
            started: Arc::new(AtomicBool::new(false)),
            paused: Arc::new(AtomicBool::new(false)),
            metrics: OnceCell::new(),
        })
    }

    fn consumed_count(&self) -> u64 {
        self.consumed.load(Ordering::Acquire)
    }

    fn pause(&self) {
        self.paused.store(true, Ordering::Release);
    }

    fn resume(&self) {
        self.paused.store(false, Ordering::Release);
    }
}

impl Transport for SlowSendTransport {
    fn key(&self) -> TransportKey {
        self.key.clone()
    }

    fn address(&self) -> WorkerAddress {
        let mut entries = std::collections::HashMap::<String, Vec<u8>>::new();
        entries.insert(self.key.to_string(), b"slow://local".to_vec());
        WorkerAddress::from_encoded(rmp_serde::to_vec(&entries).unwrap())
    }

    fn register(&self, _peer_info: PeerInfo) -> Result<(), TransportError> {
        Ok(())
    }

    fn send_message(
        &self,
        _instance_id: InstanceId,
        header: Bytes,
        payload: Bytes,
        _message_type: MessageType,
        on_error: Arc<dyn TransportErrorHandler>,
    ) -> Result<(), SendBackpressure> {
        let task = SendTask {
            _header: header,
            _payload: payload,
            _on_error: on_error,
        };
        match self.tx.try_send(task) {
            Ok(()) => Ok(()),
            Err(flume::TrySendError::Full(task)) => {
                if let Some(m) = self.metrics.get() {
                    m.record_send_backpressure();
                }
                let tx = self.tx.clone();
                Err(SendBackpressure::new(Box::pin(async move {
                    let _ = tx.send_async(task).await;
                })))
            }
            Err(flume::TrySendError::Disconnected(_)) => Ok(()),
        }
    }

    fn start(
        &self,
        _instance_id: InstanceId,
        _channels: TransportAdapter,
        rt: tokio::runtime::Handle,
    ) -> BoxFuture<'_, anyhow::Result<()>> {
        self.started.store(true, Ordering::Release);
        let rx = self.rx.lock().take().expect("start called twice");
        let consumed = self.consumed.clone();
        let paused = self.paused.clone();
        let delay = self.frame_delay;
        rt.spawn(async move {
            loop {
                // Check pause *before* attempting to pull from the channel —
                // this is how tests keep the channel saturated deterministically.
                while paused.load(Ordering::Acquire) {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                match rx.try_recv() {
                    Ok(_task) => {
                        tokio::time::sleep(delay).await;
                        consumed.fetch_add(1, Ordering::Release);
                    }
                    Err(flume::TryRecvError::Empty) => {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                    Err(flume::TryRecvError::Disconnected) => break,
                }
            }
        });
        Box::pin(async { Ok(()) })
    }

    fn shutdown(&self) {}

    fn set_observability(
        &self,
        observability: std::sync::Arc<dyn velo_ext::TransportObservability>,
    ) {
        let _ = self.metrics.set(observability);
    }

    fn check_health(
        &self,
        _instance_id: InstanceId,
        _timeout: Duration,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), HealthCheckError>> + Send + '_>,
    > {
        Box::pin(async { Ok(()) })
    }
}

// ── Test error handler ────────────────────────────────────────────────────

struct CountingHandler {
    count: AtomicUsize,
}

impl CountingHandler {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            count: AtomicUsize::new(0),
        })
    }
    fn count(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }
}

impl TransportErrorHandler for CountingHandler {
    fn on_error(&self, _header: Bytes, _payload: Bytes, _error: String) {
        self.count.fetch_add(1, Ordering::Release);
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────

/// Upper bound on any test-setup await — individual tests supply their own
/// timeouts on the interesting waits.
const TEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Helper: build a started SlowSendTransport on the current runtime.
async fn make_started(capacity: usize, frame_delay: Duration) -> Arc<SlowSendTransport> {
    let t = SlowSendTransport::new(capacity, frame_delay);
    let (adapter, _streams) = velo::transports::make_channels();
    tokio::time::timeout(
        TEST_TIMEOUT,
        t.start(
            InstanceId::new_v4(),
            adapter,
            tokio::runtime::Handle::current(),
        ),
    )
    .await
    .expect("SlowSendTransport::start timed out")
    .expect("SlowSendTransport::start returned Err");
    t
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn saturating_channel_returns_backpressure() {
    // Capacity 2, writer paused so the channel stays full deterministically.
    let t = make_started(2, Duration::from_millis(1)).await;
    t.pause();
    let err = CountingHandler::new();

    // Two fast-path sends fill the channel.
    for _ in 0..2 {
        let r = t.send_message(
            InstanceId::new_v4(),
            Bytes::from_static(b"h"),
            Bytes::from_static(b"p"),
            MessageType::Message,
            err.clone(),
        );
        assert!(r.is_ok(), "first two sends should enqueue synchronously");
    }

    // Third send: channel is full.
    let r = t.send_message(
        InstanceId::new_v4(),
        Bytes::from_static(b"h"),
        Bytes::from_static(b"p"),
        MessageType::Message,
        err.clone(),
    );
    let bp = r.expect_err("third send should return Backpressure");

    // Resume the writer so the bp future can make progress.
    t.resume();
    tokio::time::timeout(Duration::from_secs(2), bp)
        .await
        .expect("bp future should resolve after writer drains");

    assert_eq!(err.count(), 0, "bp resolution should not trigger on_error");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn many_concurrent_sends_all_resolve() {
    // Small capacity + fast drain. Many concurrent callers that all await
    // their bp must all complete.
    let t = make_started(4, Duration::from_millis(5)).await;
    let err = CountingHandler::new();
    let n = 64usize;

    let mut tasks = Vec::with_capacity(n);
    for _ in 0..n {
        let t = t.clone();
        let err = err.clone();
        tasks.push(tokio::spawn(async move {
            match t.send_message(
                InstanceId::new_v4(),
                Bytes::from_static(b"h"),
                Bytes::from_static(b"p"),
                MessageType::Message,
                err,
            ) {
                Ok(()) => {}
                Err(bp) => bp.await,
            }
        }));
    }

    for task in tasks {
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("task should not deadlock")
            .expect("task should not panic");
    }

    // Wait for the writer to drain everything.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while t.consumed_count() < n as u64 {
        if tokio::time::Instant::now() > deadline {
            panic!(
                "writer did not drain: {} / {} consumed",
                t.consumed_count(),
                n
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(err.count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_backpressure_does_not_enqueue() {
    // If a caller drops the SendBackpressure future before awaiting it, the
    // queued flume::send_async future is dropped too — the message should
    // *not* land in the channel.
    let t = make_started(1, Duration::from_millis(5)).await;
    t.pause();
    let err = CountingHandler::new();

    // Fill the channel.
    t.send_message(
        InstanceId::new_v4(),
        Bytes::from_static(b"a"),
        Bytes::from_static(b"a"),
        MessageType::Message,
        err.clone(),
    )
    .expect("first send enqueues");

    // Saturating send returns a bp — drop it immediately.
    let bp = t
        .send_message(
            InstanceId::new_v4(),
            Bytes::from_static(b"b"),
            Bytes::from_static(b"b"),
            MessageType::Message,
            err.clone(),
        )
        .expect_err("second send is backpressured");
    drop(bp);

    // Resume and wait long enough for the first frame to drain. Only the
    // first message should have been consumed; the dropped second bp must
    // not enqueue.
    t.resume();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while t.consumed_count() < 1 {
        if tokio::time::Instant::now() > deadline {
            panic!("writer did not drain the first frame");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // Give any spuriously-enqueued second frame time to drain too.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        t.consumed_count(),
        1,
        "dropped backpressure future must not enqueue"
    );
    assert_eq!(err.count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_wrapping_backpressured_send_cancels_cleanly() {
    // Wrapping the bp future in tokio::time::timeout cancels it cleanly when
    // the timeout fires — no enqueue, no error callback.
    let t = make_started(1, Duration::from_millis(5)).await;
    t.pause();
    let err = CountingHandler::new();

    t.send_message(
        InstanceId::new_v4(),
        Bytes::from_static(b"a"),
        Bytes::from_static(b"a"),
        MessageType::Message,
        err.clone(),
    )
    .expect("first send enqueues");

    let bp = t
        .send_message(
            InstanceId::new_v4(),
            Bytes::from_static(b"b"),
            Bytes::from_static(b"b"),
            MessageType::Message,
            err.clone(),
        )
        .expect_err("second send is backpressured");

    let res = tokio::time::timeout(Duration::from_millis(100), bp).await;
    assert!(res.is_err(), "timeout should fire (writer is paused)");
    assert_eq!(err.count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bp_increments_send_backpressure_counter() {
    use velo::observability::test_helpers::MetricSnapshot;

    let registry = prometheus::Registry::new();
    let metrics = Arc::new(VeloMetrics::register(&registry).expect("register metrics"));

    // Capacity 1, writer paused so every send past the first hits Full.
    let t = make_started(1, Duration::from_millis(1)).await;
    t.set_observability(std::sync::Arc::new(metrics.bind_transport("slow"))
        as std::sync::Arc<dyn velo_ext::TransportObservability>);
    t.pause();
    let err = CountingHandler::new();

    // First send fills the channel synchronously.
    t.send_message(
        InstanceId::new_v4(),
        Bytes::from_static(b"h"),
        Bytes::from_static(b"p"),
        MessageType::Message,
        err.clone(),
    )
    .expect("first send enqueues");

    // Next N sends all hit the Full branch and must bump the counter.
    let bp_sends = 5;
    let mut bps = Vec::with_capacity(bp_sends);
    for _ in 0..bp_sends {
        bps.push(
            t.send_message(
                InstanceId::new_v4(),
                Bytes::from_static(b"h"),
                Bytes::from_static(b"p"),
                MessageType::Message,
                err.clone(),
            )
            .expect_err("should backpressure"),
        );
    }

    let snapshot = MetricSnapshot::from_registry(&registry);
    let value = snapshot.counter(
        "velo_transport_send_backpressure_total",
        &[("transport", "slow")],
    );
    assert_eq!(
        value, bp_sends as f64,
        "counter should fire once per Full-branch send"
    );

    // Cancel the pending bp futures (writer is still paused).
    drop(bps);
    assert_eq!(err.count(), 0);
}

// ── TIPC flood: non-reading receiver fills conn-window → SendBackpressure ─────
//
// Unlike the SlowSendTransport tests above (which use an artificial fake
// transport), this section exercises a *real* TipcTransport against a raw
// TIPC socket that accepts the connection but never reads.  The approach is:
//
// 1. Build a TipcTransport sender (channel_capacity = 4).
// 2. Create a raw AF_TIPC SOCK_STREAM listener, bind it, and listen.
// 3. Build a TipcEndpoint for the raw listener with the sender's own
//    netns_nonce so `register()` yields Gate::Reachable immediately.
// 4. Accept the connect in spawn_blocking without reading from the socket.
// 5. Flood with 32 × 64 KiB sends.  The channel fills to capacity (writer
//    hasn't run yet — no yields in the flood loop), causing subsequent
//    try_send calls to return TrySendError::Full → Err(SendBackpressure).
// 6. Start a drain thread (non-blocking reads) to clear the kernel buffer.
// 7. Await all bp futures (they resolve as the writer drains the channel).
// 8. Assert zero on_error losses.

#[cfg(all(feature = "tipc", target_os = "linux"))]
mod tipc_flood {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;

    use velo::transports::tipc::{TipcEndpoint, TipcTransportBuilder};
    use velo::transports::{MessageType, Transport, make_channels};
    use velo_ext::{InstanceId, PeerInfo, WorkerAddress};

    // ── Minimal TIPC sockaddr ─────────────────────────────────────────────
    //
    // Mirrors `velo::transports::tipc::sys::SockaddrTipc` (16 bytes).
    // Defined here so the test is self-contained with no velo internals.
    #[repr(C)]
    #[derive(Copy, Clone, Default)]
    struct TipcSa {
        family: u16,  // AF_TIPC = 30
        addrtype: u8, // 1 = SERVICE_RANGE, 3 = SOCKET_ADDR
        scope: i8,    // 2 = TIPC_CLUSTER_SCOPE
        f1: u32,      // service_range.type  |  socket_addr.ref
        f2: u32,      // service_range.lower |  socket_addr.node
        f3: u32,      // service_range.upper |  socket_addr.pad
    }

    const AF_TIPC: i32 = 30;
    const TIPC_SERVICE_RANGE: u8 = 1;
    const TIPC_SOCKET_ADDR: u8 = 3;
    const TIPC_CLUSTER_SCOPE: i8 = 2;

    // ── Helpers ───────────────────────────────────────────────────────────

    /// Create a blocking `AF_TIPC / SOCK_STREAM` listener, bind it to
    /// `{service_type, instance..=instance}`, listen, and return
    /// `(listener_fd, socket_ref, node)` from `getsockname`.
    ///
    /// Returns `None` when TIPC is not available or `bind` fails.
    fn bind_raw_tipc_listener(service_type: u32, instance: u32) -> Option<(libc::c_int, u32, u32)> {
        // SAFETY: standard POSIX socket syscalls; all fds and pointers are valid
        // within the call site.  Errors are checked via return-value comparison.
        unsafe {
            let fd = libc::socket(AF_TIPC, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0);
            if fd < 0 {
                return None; // EAFNOSUPPORT: TIPC module not loaded
            }

            let sa = TipcSa {
                family: AF_TIPC as u16,
                addrtype: TIPC_SERVICE_RANGE,
                scope: TIPC_CLUSTER_SCOPE,
                f1: service_type,
                f2: instance,
                f3: instance,
            };
            let sa_len = std::mem::size_of::<TipcSa>() as libc::socklen_t;

            if libc::bind(fd, &sa as *const TipcSa as *const libc::sockaddr, sa_len) < 0 {
                libc::close(fd);
                return None;
            }
            libc::listen(fd, 1);

            // getsockname → SOCKET_ADDR { f1 = socket_ref, f2 = node }
            let mut sa_out = TipcSa::default();
            let mut out_len = sa_len;
            libc::getsockname(
                fd,
                &mut sa_out as *mut TipcSa as *mut libc::sockaddr,
                &mut out_len,
            );
            if sa_out.addrtype != TIPC_SOCKET_ADDR {
                libc::close(fd);
                return None;
            }
            Some((fd, sa_out.f1, sa_out.f2))
        }
    }

    /// Decode a `TipcEndpoint` from a `WorkerAddress`.
    fn decode_tipc_ep(addr: &WorkerAddress) -> Option<TipcEndpoint> {
        let bytes = addr.get_entry("tipc").ok()??;
        rmp_serde::from_slice(&bytes).ok()
    }

    /// Build a `PeerInfo` whose `WorkerAddress` carries only a `"tipc"` entry.
    fn peer_info_for(ep: &TipcEndpoint, peer_id: InstanceId) -> PeerInfo {
        let ep_bytes = rmp_serde::to_vec_named(ep).expect("TipcEndpoint encode");
        let mut map = HashMap::<String, Vec<u8>>::new();
        map.insert("tipc".to_string(), ep_bytes);
        let encoded = rmp_serde::to_vec(&map).expect("WorkerAddress map encode");
        PeerInfo::new(peer_id, WorkerAddress::from_encoded(Bytes::from(encoded)))
    }

    // ── Flood test ────────────────────────────────────────────────────────

    /// Flood a TIPC peer whose connection cannot complete (no `accept()` yet),
    /// proving the per-peer flume channel fills and `send_message` returns
    /// `Err(SendBackpressure)` — deterministically, with no scheduler
    /// assumptions.  Then accept + drain and prove every pending send completes
    /// with zero `on_error` losses.
    ///
    /// Determinism: TIPC completes a connect only when the remote application
    /// calls `accept()` (proposal §2.3, verified on this kernel), and the writer
    /// task connects *before* entering its send loop
    /// (`tipc_connection_writer_inner`).  By deferring `accept()` until after
    /// the flood, the writer provably dequeues nothing, so exactly
    /// `MAX_SENDS - CHANNEL_CAP` sends observe a full channel regardless of how
    /// the tokio workers schedule the writer task.
    ///
    /// Skips silently when `tipc.ko` is not loaded (`EAFNOSUPPORT`).
    ///
    /// Run with: `cargo test --features tipc,test-helpers --test transports_backpressure
    ///               tipc_flood`
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tipc_conn_window_flood_fills_channel() {
        const CHANNEL_CAP: usize = 4;
        const PAYLOAD_SIZE: usize = 64 * 1024; // 64 KiB per message
        const MAX_SENDS: usize = 32;

        // ── Build sender ──────────────────────────────────────────────────
        let sender = match TipcTransportBuilder::new()
            .channel_capacity(CHANNEL_CAP)
            // Generous: the writer's connect is pending (unaccepted) for the
            // duration of the flood loop; a loaded CI box must not time it out.
            .connect_timeout(Duration::from_secs(10))
            .build()
        {
            Ok(t) => Arc::new(t),
            Err(_) => return, // TIPC module not loaded — skip
        };

        let sender_ep = match decode_tipc_ep(&sender.address()) {
            Some(ep) => ep,
            None => return,
        };

        // ── Create raw non-reading listener ───────────────────────────────
        // Use a distinct service instance so we don't collide with other tests.
        let test_instance = sender_ep.service_instance.wrapping_add(0xCAFE_0001);
        let (listener_fd, recv_ref, recv_node) =
            match bind_raw_tipc_listener(sender_ep.service_type, test_instance) {
                Some(r) => r,
                None => return, // TIPC unavailable
            };

        // Build a receiver endpoint.  Using the sender's own netns_nonce
        // (same process → same TIPC stack) ensures register() yields
        // Gate::Reachable without needing a topology watch.
        let recv_ep = TipcEndpoint {
            version: 1,
            service_type: sender_ep.service_type,
            service_instance: test_instance,
            node: recv_node,
            socket_ref: recv_ref,
            netid: sender_ep.netid,
            node_id: [0u8; 16],
            netns_nonce: sender_ep.netns_nonce, // same process = same nonce
            scope: TIPC_CLUSTER_SCOPE as u8,
        };

        let peer_id = InstanceId::new_v4();
        let peer_info = peer_info_for(&recv_ep, peer_id);

        // ── Start sender + register non-reading receiver ──────────────────
        let (adapter, _streams) = make_channels();
        sender
            .start(
                InstanceId::new_v4(),
                adapter,
                tokio::runtime::Handle::current(),
            )
            .await
            .expect("TipcTransport::start must succeed");

        sender
            .register(peer_info)
            .expect("same-nonce peer must register with Gate::Reachable");

        // ── Flood BEFORE accepting ────────────────────────────────────────
        // The first send_message spawns the writer task, whose
        // TipcStream::connect() cannot complete until accept() is called on the
        // listener (TIPC has no kernel-backlog handshake completion — proposal
        // §2.3, verified).  The connect SYN queues in the kernel.  With the
        // writer provably unable to dequeue, sends 1..=CHANNEL_CAP enqueue Ok
        // and every later try_send observes Full → Err(SendBackpressure).
        let err = super::CountingHandler::new();
        let payload = Bytes::from(vec![0u8; PAYLOAD_SIZE]);
        let header = Bytes::from_static(b"flood-hdr");
        let mut bps = Vec::new();

        for _ in 0..MAX_SENDS {
            match sender.send_message(
                peer_id,
                header.clone(),
                payload.clone(),
                MessageType::Message,
                err.clone(),
            ) {
                Ok(()) => {}
                Err(bp) => bps.push(bp),
            }
        }

        // ── Assert backpressure — exact, scheduler-independent ────────────
        assert_eq!(
            bps.len(),
            MAX_SENDS - CHANNEL_CAP,
            "with accept() deferred the writer cannot drain, so exactly \
             MAX_SENDS - CHANNEL_CAP sends must observe a full channel \
             (got {} of {} backpressured, channel_capacity = {})",
            bps.len(),
            MAX_SENDS,
            CHANNEL_CAP,
        );

        // ── Accept the queued connection ──────────────────────────────────
        // spawn_blocking runs the blocking libc::accept on a dedicated OS
        // thread; the writer's queued SYN completes immediately.
        let accept_task = tokio::task::spawn_blocking(move || {
            let afd =
                unsafe { libc::accept(listener_fd, std::ptr::null_mut(), std::ptr::null_mut()) };
            // Close the listener; we accept only one connection.
            unsafe { libc::close(listener_fd) };
            afd
        });
        let accepted_fd = accept_task.await.expect("accept task should not panic");
        if accepted_fd < 0 {
            return; // accept failed unexpectedly; skip
        }

        // ── Drain: read until every flooded byte has arrived ──────────────
        // Frame layout (docs/transports.md): 11-byte preamble + header +
        // payload.  Reading clears the receiver's kernel buffer → the writer
        // unblocks → the flume channel empties → the bp futures' send_async
        // enqueues complete.  TARGET is a floor: any extra protocol bytes only
        // push the total higher.
        const PREAMBLE: usize = 11;
        let target: usize = MAX_SENDS * (PREAMBLE + b"flood-hdr".len() + PAYLOAD_SIZE);
        let deadline = std::time::Instant::now() + Duration::from_secs(30);

        let drain_thread = std::thread::spawn(move || {
            unsafe { libc::fcntl(accepted_fd, libc::F_SETFL, libc::O_NONBLOCK) };
            let mut buf = vec![0u8; 65536];
            let mut total: usize = 0;
            while total < target && std::time::Instant::now() < deadline {
                let n = unsafe {
                    libc::read(
                        accepted_fd,
                        buf.as_mut_ptr() as *mut libc::c_void,
                        buf.len(),
                    )
                };
                if n > 0 {
                    total += n as usize;
                } else if n == 0 {
                    break; // EOF
                } else {
                    let code = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
                    if code == libc::EAGAIN || code == libc::EWOULDBLOCK {
                        std::thread::sleep(Duration::from_millis(1));
                    } else {
                        break; // real error
                    }
                }
            }
            unsafe { libc::close(accepted_fd) };
            total
        });

        // ── Await all backpressure futures ────────────────────────────────
        // Each future resolves once the drain thread has cleared enough of
        // the kernel buffer that the writer drains the channel and the
        // send_async enqueue can proceed.
        for bp in bps {
            tokio::time::timeout(Duration::from_secs(10), bp)
                .await
                .expect("bp future must resolve within 10 s after receiver starts draining");
        }

        // ── Assert complete delivery, zero losses ─────────────────────────
        let total = drain_thread.join().expect("drain thread should not panic");
        assert!(
            total >= target,
            "receiver must observe every flooded byte: read {total} of >= {target}"
        );
        assert_eq!(
            err.count(),
            0,
            "all sends must complete with zero on_error after draining the receiver"
        );
    }
}
