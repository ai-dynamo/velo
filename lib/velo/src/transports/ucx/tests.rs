// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! In-process loopback tests for the UCX transport.
//!
//! Two independent `ucp_context`s in one process, wired over the `tcp` lane:
//! with `UCP_ERR_HANDLING_MODE_PEER` the shm lanes are ineligible (no peer
//! failure handler), so tcp is the deterministic choice — and the exact code
//! path CI runs without RDMA hardware.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;

use super::{UcxConfig, UcxTransport, UcxTransportBuilder};
use crate::transports::transport::{
    DataStreams, HealthCheckError, SendOutcome, Transport, TransportErrorHandler, make_channels,
};
// `super` here is the `transport` module (this file is `#[path]`-included from
// it), so the sibling `rma` module needs its full path.
use crate::transports::ucx::rma::{
    MAX_PACKED_RKEY, MappedRegion, RdmaEndpoint, RmaError, RmaGetRequest, SYS_DEV_UNKNOWN,
    preparse_packed_rkey,
};
use crate::transports::ucx::worker::{
    Cmd, EP_SETUP_BOUND, PARK_MS, SENDER_SLOTS, SenderSightings, ep_scan_period,
};
use velo_ext::{InstanceId, MessageType, PeerInfo};

#[tokio::test]
async fn dropping_transport_releases_peer_drivers() {
    let transport = UcxTransportBuilder::new().build().unwrap();
    transport
        .runtime
        .set(tokio::runtime::Handle::current())
        .ok();
    let peer = InstanceId::new_v4();
    transport.shared.peers.insert(
        peer,
        super::UcxEndpoint {
            v: super::BLOB_VERSION,
            am_id_base: super::AM_ID_BASE,
            eager_max: 1024,
            incarnation: 1,
            worker_addr: Vec::new(),
        },
    );
    // No native worker is needed: this exercises the driver's own lifetime.
    let mut ring = transport.ring_rx.lock().unwrap().take().unwrap();
    let connections = Arc::downgrade(&transport.connections);
    drop(transport.get_or_create_connection(peer).unwrap());
    drop(transport);
    assert!(connections.upgrade().is_none());
    assert!(
        tokio::time::timeout(Duration::from_secs(2), ring.recv_async())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn incomplete_startup_stops_and_joins_its_worker() {
    for cancelled in [true, false] {
        let transport = UcxTransportBuilder::new().build().unwrap();
        let (adapter, _streams) = make_channels();
        let state = adapter.shutdown_state.clone();
        transport.shutdown_state.set(state.clone()).ok();
        let mut ring = transport.ring_rx.lock().unwrap().take().unwrap();
        // Model the native worker waiting for commands, while its startup
        // result is held. This makes both failure paths deterministic.
        *transport.join.lock().unwrap() = Some(std::thread::spawn(move || {
            assert!(matches!(ring.blocking_recv(), Some(Cmd::Shutdown)));
        }));
        let (result, startup) = tokio::sync::oneshot::channel();
        let mut starting = Box::pin(transport.finish_startup(startup));
        assert!(futures::poll!(starting.as_mut()).is_pending());
        if cancelled {
            drop(starting);
        } else {
            assert!(result.send(Err(anyhow::anyhow!("startup failed"))).is_ok());
            assert!(starting.await.is_err());
        }
        assert!(transport.shared.shutdown_requested.load(Ordering::Acquire));
        assert!(transport.join.lock().unwrap().is_none());
        assert!(
            !state.teardown_token().is_cancelled(),
            "partial cleanup must not stop sibling transports"
        );
    }
}

struct CountingErrors {
    count: AtomicUsize,
    notify: tokio::sync::Notify,
}

impl CountingErrors {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            count: AtomicUsize::new(0),
            notify: tokio::sync::Notify::new(),
        })
    }
    fn count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
    async fn wait_for_error(&self, timeout: Duration) -> bool {
        // Register the waiter BEFORE re-checking the count: `notify_waiters`
        // only wakes futures that already exist, so checking first and then
        // creating the future would lose a notification in between.
        let notified = self.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.count() > 0 {
            return true;
        }
        tokio::time::timeout(timeout, notified).await.is_ok()
    }
}

impl TransportErrorHandler for CountingErrors {
    fn on_error(&self, _header: Bytes, _payload: Bytes, _error: String) {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.notify.notify_waiters();
    }
}

struct Node {
    transport: Arc<UcxTransport>,
    streams: DataStreams,
    instance_id: InstanceId,
}

async fn start_node() -> Node {
    start_node_with(|b| b).await
}

/// A node whose builder has been customised — the lifecycle knobs (D9's idle
/// reaper, eager wireup) are off by default, so every test that exercises one
/// has to ask for it.
async fn start_node_with(
    configure: impl FnOnce(UcxTransportBuilder) -> UcxTransportBuilder,
) -> Node {
    start_transport(Arc::new(
        configure(UcxTransportBuilder::new().tls("tcp"))
            .build()
            .expect("build ucx transport"),
    ))
    .await
}

/// The opt-in to UCX's software RMA, for a test whose GET must run over the
/// tcp lane.
///
/// Velo leaves the emulation off: in UCX 1.22 an owner serving a software GET
/// aborts when its puller goes away during the reply (see
/// `an_owner_survives_a_puller_that_vanishes_mid_get`). CI has no RDMA
/// hardware, so every test here that needs a GET to run turns it back on, and
/// that UCX abort is the known reason such a test can fail under load. Only
/// the puller's setting decides whether a GET is emulated.
fn software_rma(builder: UcxTransportBuilder) -> UcxTransportBuilder {
    builder.allow_software_rma(true)
}

/// An explicit disable still needs the warning when UCX ignores the switch.
///
/// On the vendored UCX 1.22 it checks that no warning is logged; on a system
/// UCX older than 1.22 (`UCX_DIR`), that exactly one is, whether velo or the
/// environment turned software RMA off.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_explicit_software_rma_disable_keeps_the_ucx_version_warning() {
    const CHILD: &str = "VELO_UCX_VERSION_WARNING_CHILD";
    if std::env::var_os(CHILD).is_some() {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .without_time()
            .with_writer(std::io::stderr)
            .init();
        for _ in 0..2 {
            let node = start_node().await;
            node.transport.shutdown();
        }
        return;
    }

    let (mut major, mut minor, mut release) = (0, 0, 0);
    // SAFETY: UCX writes its version into these three valid output pointers.
    unsafe { ucx_rs::sys::ucp_get_version(&mut major, &mut minor, &mut release) };
    let expected = usize::from((major, minor) < (1, 22));
    let warning = format!("UCX {major}.{minor}.{release} ignores PROTO_EMULATION_ENABLE=n");
    for explicit in [false, true] {
        let mut child = std::process::Command::new(std::env::current_exe().unwrap());
        child
            .args([
                "--exact",
                "transports::ucx::transport::tests::an_explicit_software_rma_disable_keeps_the_ucx_version_warning",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD, "1")
            .env("UCX_TLS", "tcp")
            .env_remove("UCX_PROTO_EMULATION_ENABLE");
        if explicit {
            child.env("UCX_PROTO_EMULATION_ENABLE", "n");
        }
        let output = child.output().expect("run the warning check in isolation");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "warning check child failed: {}\n{stderr}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert_eq!(
            stderr.matches(&warning).count(),
            expected,
            "UCX {major}.{minor}.{release}: expected one warning across two starts on UCX older than 1.22, whether the disable is explicit or default; explicit={explicit}\n{stderr}"
        );
    }
}

/// A node built from a [`UcxConfig`] directly, bypassing the builder.
///
/// The one thing this can do that [`start_node_with`] cannot is set an idle
/// timeout below `MIN_EP_IDLE_TIMEOUT`. That floor is a builder-level ergonomic
/// guard sized to dominate endpoint wireup, not an invariant of the reaper, and
/// a test isolating the reaper from transfer timing needs to go under it.
///
/// Its one caller GETs over the tcp lane, so the node opts in to software RMA
/// as [`software_rma`] does.
async fn start_node_with_config(config: UcxConfig) -> Node {
    let mut transport = UcxTransport::new(velo_ext::TransportKey::from("ucx"), config);
    transport.software_rma = true;
    start_transport(Arc::new(transport)).await
}

async fn start_transport(transport: Arc<UcxTransport>) -> Node {
    let instance_id = InstanceId::new_v4();
    let (adapter, streams) = make_channels();
    tokio::time::timeout(
        STARTUP,
        transport.start(instance_id, adapter, tokio::runtime::Handle::current()),
    )
    .await
    .expect("ucx transport startup must not hang")
    .expect("start ucx transport");
    Node {
        transport,
        streams,
        instance_id,
    }
}

fn cross_register(a: &Node, b: &Node) {
    a.transport
        .register(PeerInfo::new(b.instance_id, b.transport.address()))
        .expect("register b in a");
    b.transport
        .register(PeerInfo::new(a.instance_id, a.transport.address()))
        .expect("register a in b");
}

async fn recv(rx: &flume::Receiver<(Bytes, Bytes)>, timeout: Duration) -> Option<(Bytes, Bytes)> {
    tokio::time::timeout(timeout, rx.recv_async())
        .await
        .ok()?
        .ok()
}

/// Like `recv`, but for `message_stream`, whose items carry a mandatory
/// in-flight guard (see `InboundMessage`). The guard is dropped here — these
/// tests only assert on header/payload content.
async fn recv_message(
    rx: &flume::Receiver<crate::transports::transport::InboundMessage>,
    timeout: Duration,
) -> Option<(Bytes, Bytes)> {
    let msg = tokio::time::timeout(timeout, rx.recv_async())
        .await
        .ok()?
        .ok()?;
    Some((msg.header, msg.payload))
}

const T: Duration = Duration::from_secs(10);

/// A hang guard for `start`, not a speed check.
///
/// Startup runs `ucp_config_read`, `ucp_init` and `ucp_worker_create` on the
/// new progress thread, and that slows down as more UCX workers share the
/// CPUs. With 24 copies of this module on 8 CPUs it took 1.1 s at the median
/// and 5.2 s at most (4,176 starts). Copies that were not pinned, at a load
/// near 300, passed `T` (10 s) and failed with nothing wrong. A real hang
/// still fails here.
const STARTUP: Duration = Duration::from_secs(60);

/// A hang guard for a frame that [`ping_message`] needs to arrive, not a
/// speed check. With 48 copies of this module on 8 CPUs, 39 such frames
/// took over 5 s and the slowest 9.7 s, so `T` failed tests on frames that
/// were only late. A frame that is lost still fails here.
const ARRIVAL: Duration = Duration::from_secs(60);

#[tokio::test(flavor = "multi_thread")]
async fn message_round_trip_and_stream_routing() {
    let a = start_node().await;
    let b = start_node().await;
    cross_register(&a, &b);
    let errs = CountingErrors::new();

    // Message → message_stream
    let out = a.transport.send_message(
        b.instance_id,
        Bytes::from_static(b"hdr"),
        Bytes::from_static(b"payload"),
        MessageType::Message,
        errs.clone(),
    );
    assert!(matches!(
        out,
        SendOutcome::Admitted | SendOutcome::Pending(_)
    ));
    let (h, p) = recv_message(&b.streams.message_stream, T)
        .await
        .expect("message arrives");
    assert_eq!(&h[..], b"hdr");
    assert_eq!(&p[..], b"payload");

    // Response → response_stream
    b.transport.send_message(
        a.instance_id,
        Bytes::from_static(b"resp-h"),
        Bytes::from_static(b"resp-p"),
        MessageType::Response,
        errs.clone(),
    );
    let (h, _) = recv(&a.streams.response_stream, T)
        .await
        .expect("response arrives");
    assert_eq!(&h[..], b"resp-h");

    // Event → event_stream
    a.transport.send_message(
        b.instance_id,
        Bytes::from_static(b"ev-h"),
        Bytes::new(),
        MessageType::Event,
        errs.clone(),
    );
    let (h, p) = recv(&b.streams.event_stream, T)
        .await
        .expect("event arrives");
    assert_eq!(&h[..], b"ev-h");
    assert!(p.is_empty());

    assert_eq!(errs.count(), 0, "no send errors expected");
    a.transport.shutdown();
    b.transport.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn many_messages_preserve_order() {
    let a = start_node().await;
    let b = start_node().await;
    cross_register(&a, &b);
    let errs = CountingErrors::new();

    const N: u32 = 200;
    for i in 0..N {
        a.transport.send_message(
            b.instance_id,
            Bytes::from(i.to_le_bytes().to_vec()),
            Bytes::from(vec![0u8; 1024]),
            MessageType::Message,
            errs.clone(),
        );
    }
    for i in 0..N {
        let (h, p) = recv_message(&b.streams.message_stream, T)
            .await
            .expect("ordered message");
        assert_eq!(
            u32::from_le_bytes(h[..4].try_into().unwrap()),
            i,
            "order preserved"
        );
        assert_eq!(p.len(), 1024);
    }
    assert_eq!(errs.count(), 0);
    a.transport.shutdown();
    b.transport.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn unregistered_peer_reports_through_on_error() {
    let a = start_node().await;
    let errs = CountingErrors::new();
    let out = a.transport.send_message(
        InstanceId::new_v4(),
        Bytes::from_static(b"h"),
        Bytes::from_static(b"p"),
        MessageType::Message,
        errs.clone(),
    );
    assert!(matches!(out, SendOutcome::Admitted));
    assert!(
        errs.wait_for_error(T).await,
        "pre-wire failure must reach on_error"
    );
    a.transport.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn oversized_frame_fails_pre_wire() {
    let a = start_node().await;
    let b = start_node().await;
    cross_register(&a, &b);
    let errs = CountingErrors::new();

    let limit = a
        .transport
        .max_message_size(b.instance_id)
        .expect("limit known");
    let out = a.transport.send_message(
        b.instance_id,
        Bytes::from_static(b"h"),
        Bytes::from(vec![0u8; limit + 1]),
        MessageType::Message,
        errs.clone(),
    );
    assert!(matches!(out, SendOutcome::Admitted));
    assert!(
        errs.wait_for_error(T).await,
        "oversized frame must reach on_error"
    );
    a.transport.shutdown();
    b.transport.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn draining_receiver_echoes_shutting_down() {
    let a = start_node().await;
    let b = start_node().await;
    cross_register(&a, &b);
    use crate::observability::{VeloMetrics, test_helpers::MetricSnapshot};
    let registry = prometheus::Registry::new();
    let metrics = VeloMetrics::register(&registry).unwrap();
    b.transport
        .set_observability(Arc::new(metrics.bind_transport("ucx")));
    let errs = CountingErrors::new();

    // Warm the path so the ShuttingDown reply exercises an established pair.
    a.transport.send_message(
        b.instance_id,
        Bytes::from_static(b"warm"),
        Bytes::new(),
        MessageType::Message,
        errs.clone(),
    );
    recv_message(&b.streams.message_stream, T)
        .await
        .expect("warmup arrives");

    b.streams.shutdown_state.begin_drain();
    a.transport.send_message(
        b.instance_id,
        Bytes::from_static(b"corr-id"),
        Bytes::from_static(b"ignored"),
        MessageType::Message,
        errs.clone(),
    );

    // The draining receiver must not deliver the message...
    assert!(
        recv_message(&b.streams.message_stream, Duration::from_millis(500))
            .await
            .is_none(),
        "draining receiver must not deliver new messages"
    );
    // ...and the sender sees ShuttingDown with the echoed header.
    let (h, _) = recv(&a.streams.shutdown_stream, T)
        .await
        .expect("ShuttingDown echo");
    assert_eq!(&h[..], b"corr-id");

    let snap = MetricSnapshot::from_registry(&registry);
    assert_eq!(
        snap.counter(
            "velo_transport_rejections_total",
            &[("transport", "ucx"), ("reason", "drain_rejected")]
        ),
        1.0
    );
    assert_eq!(
        snap.counter(
            "velo_transport_frames_total",
            &[
                ("transport", "ucx"),
                ("direction", "inbound"),
                ("message_type", "message"),
                ("outcome", "accepted")
            ]
        ),
        1.0
    );

    drop(b.streams.response_stream);
    a.transport.send_message(
        b.instance_id,
        Bytes::new(),
        Bytes::new(),
        MessageType::Response,
        errs,
    );
    assert!(
        wait_until(T, || {
            MetricSnapshot::from_registry(&registry).counter(
                "velo_transport_rejections_total",
                &[("transport", "ucx"), ("reason", "route_failed")],
            ) == 1.0
        })
        .await,
        "disconnected response receiver must record a rejection"
    );

    a.transport.shutdown();
    b.transport.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn health_check_semantics() {
    let a = start_node().await;
    let b = start_node().await;
    cross_register(&a, &b);
    let errs = CountingErrors::new();

    // Unregistered peer.
    assert!(matches!(
        a.transport.check_health(InstanceId::new_v4(), T).await,
        Err(HealthCheckError::PeerNotRegistered)
    ));

    // Registered, reachable, but never connected: NeverConnected (TCP parity).
    assert!(matches!(
        a.transport.check_health(b.instance_id, T).await,
        Err(HealthCheckError::NeverConnected)
    ));

    // After traffic, healthy.
    a.transport.send_message(
        b.instance_id,
        Bytes::from_static(b"h"),
        Bytes::new(),
        MessageType::Message,
        errs.clone(),
    );
    recv_message(&b.streams.message_stream, T)
        .await
        .expect("message arrives");
    assert!(a.transport.check_health(b.instance_id, T).await.is_ok());

    a.transport.shutdown();
    b.transport.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_fails_queued_sends() {
    let a = start_node().await;
    let b = start_node().await;
    cross_register(&a, &b);
    let errs = CountingErrors::new();

    a.transport.shutdown();
    let out = a.transport.send_message(
        b.instance_id,
        Bytes::from_static(b"h"),
        Bytes::from_static(b"p"),
        MessageType::Message,
        errs.clone(),
    );
    // Post-shutdown sends must not hang: either pre-wire on_error or a failed
    // admission (the channel behind the gate is closed).
    match out {
        SendOutcome::Admitted => {
            assert!(
                errs.wait_for_error(T).await,
                "post-shutdown send must surface an error"
            );
        }
        SendOutcome::Pending(admission) => {
            let resolved = tokio::time::timeout(T, admission)
                .await
                .expect("admission must resolve, not hang");
            assert!(resolved.is_err());
        }
    }
    b.transport.shutdown();
}

// A held command stands in for a UCX send that has not completed. This makes
// pressure and cleanup deterministic without timing a network transfer.
#[tokio::test]
async fn send_budget_bounds_admission_and_releases_on_peer_retirement() {
    use super::super::address::{AM_ID_BASE, BLOB_VERSION, UcxEndpoint};
    let transport = UcxTransport::new(
        "ucx".into(),
        UcxConfig {
            channel_capacity: 1,
            ..Default::default()
        },
    );
    transport
        .runtime
        .set(tokio::runtime::Handle::current())
        .unwrap();
    let peer = InstanceId::new_v4();
    transport.shared.peers.insert(
        peer,
        UcxEndpoint {
            v: BLOB_VERSION,
            am_id_base: AM_ID_BASE,
            eager_max: 1024,
            incarnation: 1,
            worker_addr: vec![],
        },
    );
    let mut rx = transport.ring_rx.lock().unwrap().take().unwrap();
    let errors = CountingErrors::new();
    let send = || {
        transport.send_message(
            peer,
            Bytes::new(),
            Bytes::from_static(b"data"),
            MessageType::Message,
            errors.clone(),
        )
    };
    assert!(send().is_admitted());
    let held = tokio::time::timeout(T, rx.recv_async())
        .await
        .unwrap()
        .unwrap();
    // One command holds the permit, one may wait for it, and one fits in the
    // channel. The following ticket must wait before admission.
    let ticket = loop {
        if let SendOutcome::Pending(ticket) = send() {
            break ticket;
        }
    };
    assert_eq!(
        transport.connections.get(&peer).unwrap().gate.queued_len(),
        1
    );
    send();
    assert_eq!(errors.count(), 1, "excess queue entry must fail explicitly");
    ticket.cancel();
    assert_eq!(
        transport.connections.get(&peer).unwrap().gate.queued_len(),
        0
    );
    let next = send();
    transport.shared.failed_peers.insert(peer, ());
    transport.reap_failed_connection(peer);
    if let SendOutcome::Pending(ticket) = next {
        assert!(tokio::time::timeout(T, ticket).await.unwrap().is_err());
    }
    // Completion releases the outstanding permit and its retained buffers.
    drop(held);
    tokio::time::timeout(T, async {
        while errors.count() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(transport.connections.is_empty());
    assert!(
        rx.is_empty(),
        "retired sends must not reach the progress thread"
    );
}

// ---------------------------------------------------------------------------
// RMA
// ---------------------------------------------------------------------------

/// A page-aligned heap allocation owned by the test frame.
///
/// Registered memory must stay allocated for as long as UCX has it pinned.
/// The arena pool in `rendezvous::rdma` owns that concern for real callers; here every test
/// declares its buffers *before* its [`Node`]s so they drop last, and unmaps or
/// shuts down explicitly before returning.
struct PageBuf {
    ptr: *mut u8,
    len: usize,
}

impl PageBuf {
    const ALIGN: usize = 4096;

    fn new(len: usize) -> Self {
        let layout = std::alloc::Layout::from_size_align(len, Self::ALIGN).expect("valid layout");
        // SAFETY: `len` is non-zero in every caller and the alignment is a
        // power of two, so the layout is valid for the global allocator.
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        assert!(!ptr.is_null(), "allocating {len} bytes failed");
        Self { ptr, len }
    }

    fn addr(&self) -> usize {
        self.ptr as usize
    }

    fn as_slice(&self) -> &[u8] {
        // SAFETY: the allocation is live for `self`'s lifetime and `&self`
        // excludes concurrent mutation through this handle.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    fn fill_pattern(&mut self) {
        // SAFETY: as above, with unique access through `&mut self`.
        let slice = unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) };
        for (i, byte) in slice.iter_mut().enumerate() {
            *byte = (i % 251) as u8;
        }
    }
}

impl Drop for PageBuf {
    fn drop(&mut self) {
        let layout =
            std::alloc::Layout::from_size_align(self.len, Self::ALIGN).expect("valid layout");
        // SAFETY: same pointer and layout the allocation was made with.
        unsafe { std::alloc::dealloc(self.ptr, layout) };
    }
}

/// Two cross-registered nodes plus their RMA handles.
struct RmaPair {
    owner: Node,
    puller: Node,
    owner_rma: RdmaEndpoint,
    puller_rma: RdmaEndpoint,
}

/// Two nodes that GET from each other over the tcp lane, with software RMA on
/// (see [`software_rma`]).
async fn start_rma_pair() -> RmaPair {
    start_rma_pair_with(software_rma).await
}

/// Two cross-registered nodes, both built with `configure`.
async fn start_rma_pair_with(
    configure: impl Fn(UcxTransportBuilder) -> UcxTransportBuilder,
) -> RmaPair {
    let owner = start_node_with(&configure).await;
    let puller = start_node_with(&configure).await;
    cross_register(&owner, &puller);
    let owner_rma = owner.transport.rdma_endpoint();
    let puller_rma = puller.transport.rdma_endpoint();
    RmaPair {
        owner,
        puller,
        owner_rma,
        puller_rma,
    }
}

fn get_request(
    pair: &RmaPair,
    src: &PageBuf,
    remote: &MappedRegion,
    local: &MappedRegion,
) -> RmaGetRequest {
    RmaGetRequest {
        peer: pair.owner.instance_id,
        remote_addr: src.addr() as u64,
        packed_rkey: remote.packed_rkey.clone(),
        local_region: local.region_id,
        local_offset: 0,
        len: src.len as u64,
    }
}

/// Every RMA test ends here: the progress thread must finish owning no
/// registration and no unpacked remote key.
///
/// The counters live on `WorkerShared`, so they are per-transport and stay
/// meaningful with `cargo test`'s parallel harness — a process-global counter
/// would see every other test's traffic.
fn assert_rma_balanced(node: &Node) {
    assert_eq!(
        node.transport.shared.live_regions.load(Ordering::SeqCst),
        0,
        "a registered region outlived the transport"
    );
    assert_eq!(
        node.transport.shared.live_rkeys.load(Ordering::SeqCst),
        0,
        "an unpacked rkey outlived the transport"
    );
    // Every `EpEntry` must pass through exactly one of the three sites that
    // retire an endpoint — `close_ep_raw`, teardown Phase A's inline close, or
    // that phase's completion loop. Asserted here, after `shutdown()` has joined
    // the progress thread, because a counter that drifts by one is otherwise
    // invisible and would make every idle-reaper assertion below meaningless.
    assert_eq!(
        node.transport.shared.eps_open.load(Ordering::SeqCst),
        0,
        "an endpoint outlived the transport, or its close went uncounted"
    );
}

fn assert_pair_balanced(pair: &RmaPair) {
    assert_rma_balanced(&pair.owner);
    assert_rma_balanced(&pair.puller);
}

/// A progress-thread hold, released when dropped, so a failed assertion does
/// not leave the thread held and its node's shutdown hung.
struct HoldGuard<'a>(&'a AtomicBool);

impl Drop for HoldGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Set `hold` and wait until the progress thread reports `held`.
async fn hold_at<'a>(hold: &'a AtomicBool, held: &AtomicBool, what: &str) -> HoldGuard<'a> {
    hold.store(true, Ordering::Release);
    let guard = HoldGuard(hold);
    assert!(
        wait_until(T, || held.load(Ordering::Acquire)).await,
        "the progress thread never stopped {what}"
    );
    guard
}

/// Poll `cond` until it holds or `budget` expires.
async fn wait_until(budget: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        if cond() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_micros(200)).await;
    }
}

/// Push a GET straight onto the progress thread's ring.
///
/// `RdmaEndpoint::get` validates before it submits, which means the progress
/// thread's own defences are shadowed: with the public path alone, deleting
/// `prepare_get`'s checks would break no test. These helpers hand the worker
/// the requests it is supposed to refuse.
async fn ring_get(transport: &UcxTransport, req: RmaGetRequest) -> Result<(), RmaError> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    transport
        .shared
        .ring_tx
        .send_async(Cmd::RmaGet { req, reply: tx })
        .await
        .expect("ring accepts the command");
    transport.shared.doorbell.ring();
    tokio::time::timeout(T, rx)
        .await
        .expect("worker must answer")
        .expect("worker must not drop the reply")
}

/// Enqueue an unmap onto the ring and hand back its reply channel.
///
/// Split from the await so a test can establish "the command is on the ring"
/// before doing something else — waiting a guessed interval for a spawned task
/// to get there is the flake this file keeps removing.
async fn ring_unmap_enqueue(
    transport: &UcxTransport,
    region_id: u64,
) -> tokio::sync::oneshot::Receiver<Result<(), RmaError>> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    transport
        .shared
        .ring_tx
        .send_async(Cmd::UnmapRegion {
            region_id,
            reply: tx,
        })
        .await
        .expect("ring accepts the command");
    transport.shared.doorbell.ring();
    rx
}

#[tokio::test(flavor = "multi_thread")]
async fn map_get_roundtrip() {
    const LEN: usize = 256 * 1024;
    let mut src = PageBuf::new(LEN);
    let dst = PageBuf::new(LEN);
    src.fill_pattern();

    let pair = start_rma_pair().await;
    let remote = pair
        .owner_rma
        .map_region(src.addr(), LEN)
        .await
        .expect("map source region");
    let local = pair
        .puller_rma
        .map_region(dst.addr(), LEN)
        .await
        .expect("map destination region");

    // The effective range always contains what was mapped.
    assert!(remote.effective_addr <= src.addr() as u64);
    assert!(remote.effective_addr + remote.effective_len >= (src.addr() + LEN) as u64);

    tokio::time::timeout(
        T,
        pair.puller_rma
            .get(get_request(&pair, &src, &remote, &local)),
    )
    .await
    .expect("get must not hang")
    .expect("get succeeds");

    assert_eq!(dst.as_slice(), src.as_slice(), "GET must copy the pattern");

    pair.puller_rma
        .unmap_region(local.region_id)
        .await
        .expect("unmap destination");
    pair.owner_rma
        .unmap_region(remote.region_id)
        .await
        .expect("unmap source");
    pair.owner.transport.shutdown();
    pair.puller.transport.shutdown();
    assert_pair_balanced(&pair);
}

/// Over a tcp-only lane UCX has no GET in hardware. It can only emulate one in
/// software, and then the owner's progress thread serves the read. UCX 1.22
/// can abort the owner's process in that code when the puller goes away (see
/// `an_owner_survives_a_puller_that_vanishes_mid_get`), so velo turns the
/// emulation off.
///
/// The GET is then refused before any request reaches the owner, and the
/// endpoint stays usable for frames. The rendezvous layer above takes the
/// refusal as its cue to move the bytes over the chunked path
/// (`a_tcp_only_lane_moves_the_payload_chunked` in `tests/rendezvous_rdma.rs`).
#[tokio::test(flavor = "multi_thread")]
async fn a_get_over_a_tcp_only_lane_is_refused_not_emulated() {
    const LEN: usize = 64 * 1024;
    let mut src = PageBuf::new(LEN);
    let dst = PageBuf::new(LEN);
    src.fill_pattern();

    // Default nodes: nothing here asks for software RMA.
    let pair = start_rma_pair_with(|b| b).await;
    let remote = pair.owner_rma.map_region(src.addr(), LEN).await.unwrap();
    let local = pair.puller_rma.map_region(dst.addr(), LEN).await.unwrap();

    let outcome = tokio::time::timeout(
        T,
        pair.puller_rma
            .get(get_request(&pair, &src, &remote, &local)),
    )
    .await
    .expect("a refused GET must resolve, not hang");
    assert!(
        matches!(outcome, Err(RmaError::NoNativeGet)),
        "a GET over a tcp-only lane must be refused, not emulated in software: {outcome:?}"
    );
    // Remembered for the peer, so the rendezvous layer stops offering RDMA.
    assert!(pair.puller_rma.refuses_get(pair.owner.instance_id));
    assert!(!pair.owner_rma.refuses_get(pair.puller.instance_id));
    assert!(
        dst.as_slice().iter().all(|&b| b == 0),
        "a refused GET must not have written the destination"
    );

    // The refusal does not fail the endpoint: frames still flow on it.
    let errs = CountingErrors::new();
    ping_message(&pair.puller, &pair.owner, &errs).await;
    assert_eq!(errs.count.load(Ordering::SeqCst), 0);

    pair.puller_rma.unmap_region(local.region_id).await.unwrap();
    pair.owner_rma.unmap_region(remote.region_id).await.unwrap();
    pair.owner.transport.shutdown();
    pair.puller.transport.shutdown();
    assert_pair_balanced(&pair);
}

/// An unregistered peer takes its refusal record with it, so peer churn
/// cannot grow the records without bound.
#[tokio::test(flavor = "multi_thread")]
async fn unregister_reclaims_a_peers_get_refusal() {
    const LEN: usize = 64 * 1024;
    const PEERS: usize = 8;
    let src = PageBuf::new(LEN);
    let dst = PageBuf::new(LEN);
    let pair = start_rma_pair_with(|b| b).await;
    let remote = pair.owner_rma.map_region(src.addr(), LEN).await.unwrap();
    let local = pair.puller_rma.map_region(dst.addr(), LEN).await.unwrap();

    for index in 0..PEERS {
        let peer = if index == 0 {
            pair.owner.instance_id
        } else {
            InstanceId::new_v4()
        };
        pair.puller
            .transport
            .register(PeerInfo::new(peer, pair.owner.transport.address()))
            .unwrap();
        let mut req = get_request(&pair, &src, &remote, &local);
        req.peer = peer;
        let outcome = tokio::time::timeout(T, pair.puller_rma.get(req))
            .await
            .expect("the GET must resolve");
        assert!(matches!(outcome, Err(RmaError::NoNativeGet)), "{outcome:?}");
        assert!(pair.puller_rma.refuses_get(peer));

        pair.puller.transport.unregister(peer);
        assert!(
            wait_until(T, || eps_open(&pair.puller) == 0).await,
            "unregister must retire the endpoint"
        );
        assert!(!pair.puller_rma.refuses_get(peer));
        assert!(pair.puller.transport.shared.peers.is_empty());
    }

    let retained = pair.puller.transport.shared.no_native_get.len();
    pair.puller_rma.unmap_region(local.region_id).await.unwrap();
    pair.owner_rma.unmap_region(remote.region_id).await.unwrap();
    pair.owner.transport.shutdown();
    pair.puller.transport.shutdown();
    assert_pair_balanced(&pair);
    assert_eq!(
        retained, 0,
        "{PEERS} unregistered peers left {retained} refusal records in a live transport"
    );
}

/// A GET already preparing during unregister must not recreate its memo.
#[tokio::test(flavor = "multi_thread")]
async fn an_unregister_outlives_a_get_already_preparing_its_endpoint() {
    const LEN: usize = 64 * 1024;
    let src = PageBuf::new(LEN);
    let dst = PageBuf::new(LEN);
    let pair = start_rma_pair_with(|b| b).await;
    let remote = pair.owner_rma.map_region(src.addr(), LEN).await.unwrap();
    let local = pair.puller_rma.map_region(dst.addr(), LEN).await.unwrap();
    let shared = &pair.puller.transport.shared;
    let hold = hold_at(&shared.progress_hold, &shared.progress_held, "the puller").await;
    shared.ep_create_delay_ms.store(1000, Ordering::Relaxed);
    let passes = shared.progress_passes.load(Ordering::Relaxed);
    let mut get = Box::pin(
        pair.puller_rma
            .get(get_request(&pair, &src, &remote, &local)),
    );
    assert!(futures::poll!(get.as_mut()).is_pending());
    drop(hold);
    assert!(
        wait_until(T, || shared.progress_passes.load(Ordering::Relaxed)
            > passes)
        .await
    );
    // Let the cold endpoint enter the injected one-second creation delay.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(futures::poll!(get.as_mut()).is_pending());
    assert_eq!(eps_open(&pair.puller), 0);
    pair.puller.transport.unregister(pair.owner.instance_id);
    let outcome = tokio::time::timeout(T, get)
        .await
        .expect("the GET must resolve");
    assert!(wait_until(T, || eps_open(&pair.puller) == 0).await);
    let retained = shared.no_native_get.len();
    let registered = shared.peers.len();

    pair.puller_rma.unmap_region(local.region_id).await.unwrap();
    pair.owner_rma.unmap_region(remote.region_id).await.unwrap();
    pair.owner.transport.shutdown();
    pair.puller.transport.shutdown();
    assert_pair_balanced(&pair);
    assert!(outcome.is_err(), "the GET must fail: {outcome:?}");
    assert_eq!(registered, 0);
    assert_eq!(
        retained, 0,
        "a late GET refusal recreated an unregistered peer's memo after {outcome:?}"
    );
}

/// A refusal record lapses when the peer's endpoint fails.
///
/// The record must hold only a real refusal. On an RC lane, a queue pair that
/// fails flushes its outstanding work with `IBV_WC_WR_FLUSH_ERR`, which UCX
/// reports as `UCS_ERR_CANCELED` (`uct_ib_wc_to_ucs_status`), and it can
/// purge the GETs before the endpoint's error handler runs. Such a GET looks
/// like a refusal, so a record taken then would keep a peer with GET in
/// hardware on the chunked path. A failed endpoint therefore drops its peer's
/// record. Over tcp the same reap path runs when the peer goes away, which is
/// what this drives.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_endpoint_drops_its_peers_get_refusal() {
    const LEN: usize = 64 * 1024;
    let src = PageBuf::new(LEN);
    let dst = PageBuf::new(LEN);
    let pair = start_rma_pair_with(|b| b).await;
    let remote = pair.owner_rma.map_region(src.addr(), LEN).await.unwrap();
    let local = pair.puller_rma.map_region(dst.addr(), LEN).await.unwrap();
    let outcome = pair
        .puller_rma
        .get(get_request(&pair, &src, &remote, &local))
        .await;
    assert!(matches!(outcome, Err(RmaError::NoNativeGet)), "{outcome:?}");
    assert!(pair.puller_rma.refuses_get(pair.owner.instance_id));
    pair.owner_rma.unmap_region(remote.region_id).await.unwrap();

    // The owner goes away; frames to it fail the puller's endpoint.
    pair.owner.transport.shutdown();
    let errs = CountingErrors::new();
    let failed = wait_until(T, || {
        let _ = pair.puller.transport.send_message(
            pair.owner.instance_id,
            Bytes::from_static(b"h"),
            Bytes::from_static(b"p"),
            MessageType::Message,
            errs.clone(),
        );
        !pair.puller_rma.refuses_get(pair.owner.instance_id)
    })
    .await;
    assert!(
        failed,
        "the refusal record outlived the failed endpoint (failed_peers: {})",
        pair.puller.transport.shared.failed_peers.len()
    );

    pair.puller_rma.unmap_region(local.region_id).await.unwrap();
    pair.puller.transport.shutdown();
    assert_pair_balanced(&pair);
}

#[tokio::test(flavor = "multi_thread")]
async fn get_zero_length() {
    const LEN: usize = 4096;
    let mut src = PageBuf::new(LEN);
    let dst = PageBuf::new(LEN);
    src.fill_pattern();

    let pair = start_rma_pair().await;
    let remote = pair.owner_rma.map_region(src.addr(), LEN).await.unwrap();
    let local = pair.puller_rma.map_region(dst.addr(), LEN).await.unwrap();

    let mut req = get_request(&pair, &src, &remote, &local);
    req.len = 0;
    tokio::time::timeout(T, pair.puller_rma.get(req))
        .await
        .expect("zero-length get must not hang")
        .expect("zero-length get succeeds");
    assert_eq!(dst.as_slice(), &[0u8; LEN][..], "no bytes may be written");

    pair.owner.transport.shutdown();
    pair.puller.transport.shutdown();
    assert_pair_balanced(&pair);
}

#[tokio::test(flavor = "multi_thread")]
async fn get_out_of_range() {
    const LEN: usize = 4096;
    let src = PageBuf::new(LEN);
    let dst = PageBuf::new(LEN);

    let pair = start_rma_pair().await;
    let remote = pair.owner_rma.map_region(src.addr(), LEN).await.unwrap();
    let local = pair.puller_rma.map_region(dst.addr(), LEN).await.unwrap();

    // One byte past the end of the mapped range.
    let mut req = get_request(&pair, &src, &remote, &local);
    req.local_offset = 1;
    assert!(matches!(
        pair.puller_rma.get(req).await,
        Err(RmaError::OutOfRange)
    ));

    // Offset itself outside the region.
    let mut req = get_request(&pair, &src, &remote, &local);
    req.local_offset = LEN as u64 * 2;
    req.len = 1;
    assert!(matches!(
        pair.puller_rma.get(req).await,
        Err(RmaError::OutOfRange)
    ));

    // Length that would overflow the offset arithmetic.
    let mut req = get_request(&pair, &src, &remote, &local);
    req.local_offset = u64::MAX;
    req.len = 2;
    assert!(matches!(
        pair.puller_rma.get(req).await,
        Err(RmaError::OutOfRange)
    ));

    // An unknown region never reaches the progress thread either.
    let mut req = get_request(&pair, &src, &remote, &local);
    req.local_region = local.region_id + 1_000;
    assert!(matches!(
        pair.puller_rma.get(req).await,
        Err(RmaError::RegionNotFound)
    ));

    pair.owner.transport.shutdown();
    pair.puller.transport.shutdown();
    assert_pair_balanced(&pair);
}

#[tokio::test(flavor = "multi_thread")]
async fn unmap_waits_for_inflight() {
    const CHUNK: usize = 8 * 1024 * 1024;
    const CHUNKS: usize = 8;
    const LEN: usize = CHUNK * CHUNKS;
    let mut src = PageBuf::new(LEN);
    let dst = PageBuf::new(LEN);
    src.fill_pattern();

    let pair = start_rma_pair().await;
    let remote = pair.owner_rma.map_region(src.addr(), LEN).await.unwrap();
    let local = pair.puller_rma.map_region(dst.addr(), LEN).await.unwrap();

    let mut gets = Vec::with_capacity(CHUNKS);
    for i in 0..CHUNKS {
        let endpoint = pair.puller_rma.clone();
        let req = RmaGetRequest {
            peer: pair.owner.instance_id,
            remote_addr: (src.addr() + i * CHUNK) as u64,
            packed_rkey: remote.packed_rkey.clone(),
            local_region: local.region_id,
            local_offset: (i * CHUNK) as u64,
            len: CHUNK as u64,
        };
        gets.push(tokio::spawn(async move { endpoint.get(req).await }));
    }
    // Wait until all eight are posted rather than sleeping a guessed interval.
    // `inflight_ops` is incremented on the progress thread at post time, so
    // reaching CHUNKS proves every GET is on the wire and the unmap that follows
    // cannot win the FIFO race — the property a fixed sleep only made likely.
    assert!(
        wait_until(T, || {
            pair.puller
                .transport
                .shared
                .inflight_ops
                .load(Ordering::SeqCst)
                >= CHUNKS
        })
        .await,
        "all {CHUNKS} GETs must be in flight before the unmap is issued"
    );

    let mut unmap = Box::pin(pair.puller_rma.unmap_region(local.region_id));
    // Timing probe: 64 MiB over the tcp lane takes tens of milliseconds, so the
    // unmap should still be parked. If the whole transfer somehow finished
    // first the probe is vacuous rather than wrong — the assertions below
    // (every GET succeeded, every byte landed, and the region really is gone)
    // are what stand for the invariant itself.
    match tokio::time::timeout(Duration::from_millis(5), &mut unmap).await {
        Ok(early) => early.expect("unmap resolves"),
        Err(_) => tokio::time::timeout(T, &mut unmap)
            .await
            .expect("unmap must resolve once the GETs complete")
            .expect("unmap succeeds"),
    }

    for (i, task) in gets.into_iter().enumerate() {
        task.await
            .expect("get task must not panic")
            .unwrap_or_else(|e| panic!("get {i} failed: {e}"));
    }
    assert_eq!(
        dst.as_slice(),
        src.as_slice(),
        "every GET must have completed before the region was unmapped"
    );

    // The region really is gone: a fresh GET no longer finds it.
    let mut req = get_request(&pair, &src, &remote, &local);
    req.len = 1;
    assert!(matches!(
        pair.puller_rma.get(req).await,
        Err(RmaError::RegionNotFound)
    ));

    pair.owner.transport.shutdown();
    pair.puller.transport.shutdown();
    assert_pair_balanced(&pair);
}

#[tokio::test(flavor = "multi_thread")]
async fn get_unknown_peer() {
    const LEN: usize = 4096;
    let src = PageBuf::new(LEN);
    let dst = PageBuf::new(LEN);

    let pair = start_rma_pair().await;
    let remote = pair.owner_rma.map_region(src.addr(), LEN).await.unwrap();
    let local = pair.puller_rma.map_region(dst.addr(), LEN).await.unwrap();

    let mut req = get_request(&pair, &src, &remote, &local);
    req.peer = InstanceId::new_v4();
    assert!(matches!(
        tokio::time::timeout(T, pair.puller_rma.get(req))
            .await
            .expect("must not hang"),
        Err(RmaError::PeerNotRegistered(_))
    ));

    pair.owner.transport.shutdown();
    pair.puller.transport.shutdown();
    assert_pair_balanced(&pair);
}

/// Guards the one UCX API choice that fails loudly nowhere else.
///
/// `ucp_rkey_pack` is deprecated but working; `ucp_memh_pack` without
/// `UCP_MEMH_PACK_FLAG_EXPORT` aborts the process via `ucs_fatal`. If a future
/// UCX bump changes what `ucp_rkey_pack` produces, this fails instead of the
/// GET path silently degrading.
///
/// The printed size settles the `md_map` question for CI: over `UCX_TLS=tcp`
/// the measured packed rkey is exactly **9 bytes** — the header alone, with no
/// per-memory-domain key material, because the tcp MD registers nothing. Real
/// InfiniBand packs a key per MD on top of that: measured **20 B** (see
/// `docs/src/operations/rdma-performance.md`), on a build whose mlx5 memory
/// domain never opened. An independent probe on a DEVX memory domain measured
/// 19 B on the same UCX (see `docs/src/development/rdma-design.md`).
///
/// So the size is memory-domain-dependent, and `>= 9` stays the tightest bound
/// CI can assert. Tightening it to the 20 would pin a value that build produces
/// only because of a link-order defect, and the assertion would go red when
/// that defect is fixed.
#[tokio::test(flavor = "multi_thread")]
async fn rkey_pack_canary() {
    const LEN: usize = 64 * 1024;
    let first = PageBuf::new(LEN);
    let second = PageBuf::new(LEN);

    let node = start_node().await;
    let rma = node.transport.rdma_endpoint();

    let a = rma.map_region(first.addr(), LEN).await.expect("map first");
    println!(
        "ucp_rkey_pack under UCX_TLS=tcp: {} bytes",
        a.packed_rkey.len()
    );
    assert!(
        a.packed_rkey.len() >= 9,
        "packed rkey is implausibly small ({} bytes)",
        a.packed_rkey.len()
    );

    let b = rma
        .map_region(second.addr(), LEN)
        .await
        .expect("map second");
    assert!(!b.packed_rkey.is_empty(), "second pack must also succeed");
    assert_ne!(a.region_id, b.region_id);

    rma.unmap_region(a.region_id).await.expect("unmap first");
    rma.unmap_region(b.region_id).await.expect("unmap second");
    // Unmapping is idempotent: a repeat for an id that is already gone reports
    // the state the caller asked for, not an error a retry cannot distinguish
    // from a use-after-free.
    rma.unmap_region(a.region_id)
        .await
        .expect("repeat unmap is a no-op, not a failure");
    rma.unmap_region(u64::MAX)
        .await
        .expect("unmapping an id that never existed is also a no-op");
    node.transport.shutdown();
    assert_rma_balanced(&node);
}

/// Drives an in-flight GET into the FORCE-close cancellation path.
///
/// Re-registering the owner's instance id against a *different* incarnation
/// makes the puller's progress thread FORCE-close the endpoint the GET is
/// riding (`revalidate_eps`), which purges the operation with
/// `UCS_ERR_CANCELED` and drives the RMA trampoline. That is the one path where
/// the rkey is destroyed while its endpoint is mid-close, so it is the path the
/// module's rkey-lifetime invariant is written for. Either outcome is
/// legitimate (the transfer may beat the close); what must hold is that the
/// caller is answered, the region's in-flight count returns to zero — proven by
/// the unmap resolving — and teardown reports nothing leaked.
#[tokio::test(flavor = "multi_thread")]
async fn get_cancelled_by_endpoint_replacement() {
    const LEN: usize = 64 * 1024 * 1024;
    let mut src = PageBuf::new(LEN);
    let dst = PageBuf::new(LEN);
    src.fill_pattern();

    let pair = start_rma_pair().await;
    // Exists only to supply a worker address with a different incarnation.
    let decoy = start_node().await;

    let remote = pair.owner_rma.map_region(src.addr(), LEN).await.unwrap();
    let local = pair.puller_rma.map_region(dst.addr(), LEN).await.unwrap();

    let endpoint = pair.puller_rma.clone();
    let req = get_request(&pair, &src, &remote, &local);
    let get = tokio::spawn(async move { endpoint.get(req).await });
    tokio::time::sleep(Duration::from_millis(2)).await;

    pair.puller
        .transport
        .register(PeerInfo::new(
            pair.owner.instance_id,
            decoy.transport.address(),
        ))
        .expect("re-register the owner under a new incarnation");

    let outcome = tokio::time::timeout(T, get)
        .await
        .expect("get must resolve, not hang")
        .expect("get task must not panic");
    assert!(
        outcome.is_ok() || matches!(outcome, Err(RmaError::Ucx { .. })),
        "unexpected get outcome: {outcome:?}"
    );

    // The operation released the region whether it completed or was cancelled.
    tokio::time::timeout(T, pair.puller_rma.unmap_region(local.region_id))
        .await
        .expect("unmap must resolve")
        .expect("unmap succeeds once the cancelled op has been accounted for");

    pair.owner_rma
        .unmap_region(remote.region_id)
        .await
        .expect("unmap source");
    decoy.transport.shutdown();
    pair.owner.transport.shutdown();
    pair.puller.transport.shutdown();
    assert_pair_balanced(&pair);
    assert_rma_balanced(&decoy);
}

/// Shutting down under an outstanding GET must answer the caller, not hang.
///
/// Measured over the tcp lane the transfer usually wins this race and the GET
/// resolves `Ok` from teardown's RMA drain. The *cancellation* path is covered
/// by [`get_cancelled_by_endpoint_replacement`] and
/// [`shutdown_cancels_a_get_its_peer_stopped_answering`] instead. Under heavy
/// load the owner shut down below can still abort in UCX, when it reads the
/// GET's request only after the puller is gone; see
/// [`an_owner_survives_a_puller_that_vanishes_mid_get`]. What this test
/// pins down is the resolution guarantee: nothing is left waiting on a oneshot
/// the progress thread took to the grave, including the unmap issued afterwards.
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_with_inflight_get() {
    const LEN: usize = 32 * 1024 * 1024;
    let mut src = PageBuf::new(LEN);
    let dst = PageBuf::new(LEN);
    src.fill_pattern();

    let pair = start_rma_pair().await;
    let remote = pair.owner_rma.map_region(src.addr(), LEN).await.unwrap();
    let local = pair.puller_rma.map_region(dst.addr(), LEN).await.unwrap();

    let endpoint = pair.puller_rma.clone();
    let req = get_request(&pair, &src, &remote, &local);
    let get = tokio::spawn(async move { endpoint.get(req).await });
    tokio::time::sleep(Duration::from_millis(2)).await;

    // Blocking join on the progress thread, from the transport contract.
    pair.puller.transport.shutdown();

    // Whether the GET landed or was cancelled, the caller must be told.
    let outcome = tokio::time::timeout(T, get)
        .await
        .expect("get must resolve, not hang")
        .expect("get task must not panic");
    match outcome {
        Ok(()) | Err(RmaError::ShuttingDown) | Err(RmaError::ChannelClosed) => {}
        Err(RmaError::Ucx { status_name }) => {
            // A cancelled operation completes with an error status; that is a
            // resolution, which is what this test is about.
            println!("get completed with ucx status: {status_name}");
        }
        Err(other) => panic!("unexpected get outcome: {other}"),
    }

    // A post-shutdown command answers rather than hanging.
    assert!(matches!(
        tokio::time::timeout(T, pair.puller_rma.unmap_region(local.region_id))
            .await
            .expect("unmap must resolve"),
        Err(RmaError::ShuttingDown)
    ));

    pair.owner.transport.shutdown();
    assert_pair_balanced(&pair);
}

/// Shutdown must end a GET its peer has stopped answering, so its rkey is
/// destroyed before the worker is.
///
/// Over the tcp lane a GET is software-emulated: the puller sends a request
/// and the owner streams the data back. Once the request is out, the GET waits
/// in the endpoint's `proto_reqs` list. A flush-mode close waits for it, and
/// `ucp_worker_destroy` deletes the endpoint without purging that list. So if
/// teardown flush-closes the endpoint, the GET's callback never runs: the rkey,
/// the request and its `Arc` leak, and UCX warns "1 pending operations were not
/// flushed". Only a FORCE close, which purges `proto_reqs` with
/// `UCS_ERR_CANCELED`, ends it.
///
/// Under load a peer that is merely slow leaves the GET in the same state: 48
/// copies of this module at once on 8 CPUs failed `shutdown_with_inflight_get`
/// and `shutdown_resolves_parked_unmap_and_get` with this message. The child
/// (`rma_teardown_child`) uses holds to make it certain:
///
/// 1. The warm-up GET wires the endpoint, so the big GET's request goes out at
///    once. Otherwise it waits in a pending queue, which `ucp_worker_destroy`
///    does purge.
/// 2. The puller is held, so it reads nothing, and the owner runs one whole
///    pass: it takes the request and sends data until the socket is full.
///    32 MiB is more than the loopback socket buffers hold, so the GET cannot
///    finish while the puller reads nothing.
/// 3. The owner is held for the whole of the puller's teardown.
///
/// The owner is never released. It still holds queued replies to a puller
/// that is gone, and UCX 1.22 can abort the process while it discards that
/// endpoint (`stubs.c:107`, from `uct_tcp_ep_pending_purge` in
/// `ucp_worker_discard_uct_ep_destroy_progress`; 2 of 96 copies under load).
/// That is the owner's UCX defect, not this test's subject (see
/// `an_owner_survives_a_puller_that_vanishes_mid_get`). So the scenario runs in
/// a child process, which exits as soon as the puller has been checked.
#[test]
fn shutdown_cancels_a_get_its_peer_stopped_answering() {
    let (status, output) = run_child_test("rma_teardown_child", &[]);
    assert!(
        status.success() && output.contains(RMA_TEARDOWN_CHECKED),
        "the child test failed ({status}):\n{output}"
    );
}

/// Printed by `rma_teardown_child` once the puller passed its checks.
const RMA_TEARDOWN_CHECKED: &str = "rma teardown: puller balanced";

/// The child half of `shutdown_cancels_a_get_its_peer_stopped_answering`. Does
/// nothing unless that test started it.
#[tokio::test(flavor = "multi_thread")]
async fn rma_teardown_child() {
    if std::env::var_os(CHILD_TEST).is_none() {
        return;
    }
    const LEN: usize = 32 * 1024 * 1024;
    let mut src = PageBuf::new(LEN);
    let dst = PageBuf::new(LEN);
    src.fill_pattern();

    let pair = start_rma_pair().await;
    let remote = pair.owner_rma.map_region(src.addr(), LEN).await.unwrap();
    let local = pair.puller_rma.map_region(dst.addr(), LEN).await.unwrap();
    let mut warm_up = get_request(&pair, &src, &remote, &local);
    warm_up.len = 4096;
    pair.puller_rma.get(warm_up).await.expect("warm-up get");

    let owner = &pair.owner.transport.shared;
    let puller = &pair.puller.transport.shared;
    let owner_hold = hold_at(&owner.progress_hold, &owner.progress_held, "the owner").await;

    let endpoint = pair.puller_rma.clone();
    let req = get_request(&pair, &src, &remote, &local);
    let get = tokio::spawn(async move { endpoint.get(req).await });
    assert!(
        wait_until(T, || puller.inflight_ops.load(Ordering::SeqCst) >= 1).await,
        "the GET must be posted before the puller is held"
    );
    // The pass that posted the GET sent its request before reaching the hold.
    let puller_hold = hold_at(&puller.progress_hold, &puller.progress_held, "the puller").await;

    let passes = owner.progress_passes.load(Ordering::Relaxed);
    drop(owner_hold);
    assert!(
        wait_until(T, || owner.progress_passes.load(Ordering::Relaxed)
            >= passes + 2)
        .await,
        "the owner never ran a whole pass"
    );
    let _owner_hold = hold_at(&owner.progress_hold, &owner.progress_held, "the owner").await;

    let transport = Arc::clone(&pair.puller.transport);
    let teardown = tokio::task::spawn_blocking(move || transport.shutdown());
    drop(puller_hold);
    tokio::time::timeout(T, teardown)
        .await
        .expect("puller shutdown must finish")
        .expect("shutdown task");

    let outcome = tokio::time::timeout(T, get)
        .await
        .expect("the GET must resolve, not hang")
        .expect("get task");
    assert!(
        matches!(
            outcome,
            Err(RmaError::Ucx { .. }) | Err(RmaError::ShuttingDown)
        ),
        "a GET the owner stopped answering cannot succeed: {outcome:?}"
    );
    assert_rma_balanced(&pair.puller);
    println!("{RMA_TEARDOWN_CHECKED}");
    // Exit with the owner still held; see the parent test for why.
    std::process::exit(0);
}

/// Set in a child process started by `run_child_test`.
const CHILD_TEST: &str = "VELO_UCX_CHILD_TEST";

/// Run one test of this module in a child process of this test binary, and
/// return its exit status and output. For scenarios that can abort the
/// process: the abort then fails one test instead of killing every test in
/// the binary.
fn run_child_test(name: &str, envs: &[(&str, &str)]) -> (std::process::ExitStatus, String) {
    let mut child = std::process::Command::new(std::env::current_exe().expect("test binary"))
        .args([
            "--exact",
            &format!("transports::ucx::transport::tests::{name}"),
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD_TEST, "1")
        .env("UCX_TLS", "tcp")
        .envs(envs.iter().copied())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn the child test");
    // Read both pipes while the child runs, so a full pipe cannot stall it.
    let mut stdout = child.stdout.take().expect("stdout");
    let mut stderr = child.stderr.take().expect("stderr");
    let out = std::thread::spawn(move || {
        let mut text = String::new();
        let _ = std::io::Read::read_to_string(&mut stdout, &mut text);
        text
    });
    let err = std::thread::spawn(move || {
        let mut text = String::new();
        let _ = std::io::Read::read_to_string(&mut stderr, &mut text);
        text
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    let status = loop {
        if let Some(status) = child.try_wait().expect("wait for the child") {
            break status;
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("the child test {name} hung");
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let output = format!(
        "{}{}",
        out.join().expect("stdout reader"),
        err.join().expect("stderr reader")
    );
    (status, output)
}

/// An owner must survive a puller that goes away while the owner still has to
/// answer its GET.
///
/// With software RMA on, it does not, in UCX 1.22 (see
/// `software_get_aborts_an_owner_whose_puller_vanishes`). With it off, which
/// is velo's default, a GET over the tcp lane is refused on the puller's side
/// before any request reaches the owner, so the owner has nothing to serve.
/// The child checks that both GETs were refused, and that the owner keeps
/// making progress after the puller is gone.
///
/// The scenario runs in a child process, so an abort fails this test instead
/// of killing the test binary.
#[test]
fn an_owner_survives_a_puller_that_vanishes_mid_get() {
    let (status, output) = run_child_test("owner_abort_child", &[]);
    assert!(
        status.success() && output.contains(OWNER_SURVIVED),
        "the owner did not survive ({status}):\n{output}"
    );
}

/// The control for `an_owner_survives_a_puller_that_vanishes_mid_get`: the
/// same scenario with software RMA turned back on through the environment,
/// as a deployment could, aborts the owner.
///
/// The owner's software-GET handler (`ucp_get_req_handler`) sends the reply on
/// an endpoint that has already failed. `ucp_progress_get_reply` returns the
/// failure status, and `ucp_request_try_send` (`ucp_request.inl:356`) calls
/// `ucs_fatal` on any status but `OK`, `INPROGRESS` and `NO_RESOURCE`:
/// "Fatal: unexpected error: Endpoint timeout". A puller that crashes reaches
/// it as surely as one that shuts down, so no teardown order in velo avoids
/// it. A second site, an assertion in `uct_tcp_ep_pending_purge`, aborts the
/// owner when UCX discards its endpoint with replies still queued.
///
/// This test fails once a UCX release handles both sites. Then software RMA
/// can be reconsidered.
#[test]
fn software_get_aborts_an_owner_whose_puller_vanishes() {
    let (status, output) =
        run_child_test("owner_abort_child", &[("UCX_PROTO_EMULATION_ENABLE", "y")]);
    assert_eq!(
        std::os::unix::process::ExitStatusExt::signal(&status),
        Some(SIGABRT),
        "with software GET on, UCX 1.22 aborts the owner; did UCX change? ({status}):\n{output}"
    );
}

/// `SIGABRT` on Linux, the signal `ucs_fatal` and a failed UCX assertion end
/// the process with.
const SIGABRT: i32 = 6;

/// Printed by `owner_abort_child` once the owner has outlived the puller.
const OWNER_SURVIVED: &str = "owner survived the puller";

/// The child half of `an_owner_survives_a_puller_that_vanishes_mid_get` and of
/// its control. Does nothing unless one of them started it.
#[tokio::test(flavor = "multi_thread")]
async fn owner_abort_child() {
    if std::env::var_os(CHILD_TEST).is_none() {
        return;
    }
    // The control sets this; velo then leaves software RMA on.
    let software_get = std::env::var("UCX_PROTO_EMULATION_ENABLE").is_ok_and(|v| v == "y");
    const LEN: usize = 32 * 1024 * 1024;
    let mut src = PageBuf::new(LEN);
    let dst = PageBuf::new(LEN);
    src.fill_pattern();

    // Velo's defaults: no builder opt-in.
    let pair = start_rma_pair_with(|b| b).await;
    let remote = pair.owner_rma.map_region(src.addr(), LEN).await.unwrap();
    let local = pair.puller_rma.map_region(dst.addr(), LEN).await.unwrap();
    let mut warm_up = get_request(&pair, &src, &remote, &local);
    warm_up.len = 4096;
    let warmed = pair.puller_rma.get(warm_up).await;
    if software_get {
        warmed.expect("warm-up get");
    } else {
        assert!(
            matches!(warmed, Err(RmaError::NoNativeGet)),
            "the warm-up GET was not refused: {warmed:?}"
        );
    }

    // The owner takes nothing until the puller is gone, so a GET request
    // would wait in its socket.
    let owner = &pair.owner.transport.shared;
    let owner_hold = hold_at(&owner.progress_hold, &owner.progress_held, "the owner").await;
    let endpoint = pair.puller_rma.clone();
    let req = get_request(&pair, &src, &remote, &local);
    let get = tokio::spawn(async move { endpoint.get(req).await });
    if software_get {
        assert!(
            wait_until(T, || pair
                .puller
                .transport
                .shared
                .inflight_ops
                .load(Ordering::SeqCst)
                >= 1)
            .await,
            "the GET must be posted before shutdown"
        );
    } else {
        let refused = tokio::time::timeout(T, get)
            .await
            .expect("a refused GET must resolve")
            .expect("get task");
        assert!(
            matches!(refused, Err(RmaError::NoNativeGet)),
            "the GET was not refused: {refused:?}"
        );
    }
    pair.puller.transport.shutdown();

    // Now the owner reads whatever the puller sent, and its departure.
    let passes = owner.progress_passes.load(Ordering::Relaxed);
    drop(owner_hold);
    assert!(
        wait_until(T, || owner.progress_passes.load(Ordering::Relaxed)
            >= passes + 3)
        .await,
        "the owner stopped making progress"
    );
    pair.owner.transport.shutdown();
    println!("{OWNER_SURVIVED}");
}

/// The progress thread's own validation, reached directly.
///
/// `RdmaEndpoint::get` rejects these before they ever occupy a ring slot, so
/// every one of `prepare_get`'s checks is dead code from the public path's point
/// of view — delete them and no other test notices. These push the malformed
/// requests onto the ring by hand.
#[tokio::test(flavor = "multi_thread")]
async fn worker_rejects_bad_get_commands() {
    const LEN: usize = 64 * 1024;
    let src = PageBuf::new(LEN);
    let dst = PageBuf::new(LEN);

    let pair = start_rma_pair().await;
    let remote = pair.owner_rma.map_region(src.addr(), LEN).await.unwrap();
    let local = pair.puller_rma.map_region(dst.addr(), LEN).await.unwrap();
    let puller = &*pair.puller.transport;

    // Past the end of the *requested* range — the check that keeps a caller
    // inside memory the process owns.
    let mut req = get_request(&pair, &src, &remote, &local);
    req.local_offset = 1;
    assert!(matches!(
        ring_get(puller, req).await,
        Err(RmaError::OutOfRange)
    ));

    // Offset arithmetic that would wrap.
    let mut req = get_request(&pair, &src, &remote, &local);
    req.local_offset = u64::MAX;
    req.len = 8;
    assert!(matches!(
        ring_get(puller, req).await,
        Err(RmaError::OutOfRange)
    ));

    // An id the worker never minted.
    let mut req = get_request(&pair, &src, &remote, &local);
    req.local_region = local.region_id + 4_096;
    assert!(matches!(
        ring_get(puller, req).await,
        Err(RmaError::RegionNotFound)
    ));

    // A peer with no entry in the transport's map, checked before any endpoint
    // is created for it.
    let mut req = get_request(&pair, &src, &remote, &local);
    req.peer = InstanceId::new_v4();
    assert!(matches!(
        ring_get(puller, req).await,
        Err(RmaError::PeerNotRegistered(_))
    ));

    // Rkeys the worker must refuse before the pointer reaches
    // `ucp_ep_rkey_unpack`, which parses with no length bound of its own.
    let mut req = get_request(&pair, &src, &remote, &local);
    req.packed_rkey = Bytes::new();
    assert!(matches!(
        ring_get(puller, req).await,
        Err(RmaError::InvalidRkey)
    ));

    let mut req = get_request(&pair, &src, &remote, &local);
    req.packed_rkey = Bytes::from(vec![0xABu8; 2048]);
    assert!(matches!(
        ring_get(puller, req).await,
        Err(RmaError::InvalidRkey)
    ));

    // Zero length is the worker's own no-op path. `RdmaEndpoint::get`
    // short-circuits it, so the ring is the only way to reach this arm.
    let mut req = get_request(&pair, &src, &remote, &local);
    req.len = 0;
    req.packed_rkey = Bytes::new();
    ring_get(puller, req)
        .await
        .expect("a zero-length GET is a no-op, whatever key it carries");

    // Nothing above should have leaked a key or a region.
    assert_eq!(
        pair.puller
            .transport
            .shared
            .live_rkeys
            .load(Ordering::SeqCst),
        0
    );

    pair.puller_rma.unmap_region(local.region_id).await.unwrap();
    pair.owner_rma.unmap_region(remote.region_id).await.unwrap();
    pair.owner.transport.shutdown();
    pair.puller.transport.shutdown();
    assert_pair_balanced(&pair);
}

/// A peer-supplied `mem_type` out of range is refused before it can index a
/// UCX array off its end — on both the submit and the worker paths.
///
/// The framing is otherwise perfect, so nothing but the value check stands
/// between this blob and `worker->mem_type_ep[0xFF]` on the progress thread.
#[tokio::test(flavor = "multi_thread")]
async fn out_of_range_mem_type_is_refused_before_ucx() {
    const LEN: usize = 64 * 1024;
    let src = PageBuf::new(LEN);
    let dst = PageBuf::new(LEN);

    let pair = start_rma_pair().await;
    let remote = pair.owner_rma.map_region(src.addr(), LEN).await.unwrap();
    let local = pair.puller_rma.map_region(dst.addr(), LEN).await.unwrap();

    // md_map = 1, mem_type = 0xFF, one zero-length entry, sys_dev = UNKNOWN.
    let mut blob = 1u64.to_le_bytes().to_vec();
    blob.push(0xFF);
    blob.push(0);
    blob.push(0xFF);

    // Submit side.
    let mut req = get_request(&pair, &src, &remote, &local);
    req.packed_rkey = Bytes::from(blob.clone());
    req.len = 64;
    assert!(matches!(
        pair.puller_rma.get(req).await,
        Err(RmaError::InvalidRkey)
    ));

    // Worker side, reached directly so the submit-side check cannot shadow it.
    let mut req = get_request(&pair, &src, &remote, &local);
    req.packed_rkey = Bytes::from(blob);
    req.len = 64;
    assert!(matches!(
        ring_get(&pair.puller.transport, req).await,
        Err(RmaError::InvalidRkey)
    ));

    assert_eq!(
        pair.puller
            .transport
            .shared
            .live_rkeys
            .load(Ordering::SeqCst),
        0
    );

    pair.puller_rma.unmap_region(local.region_id).await.unwrap();
    pair.owner_rma.unmap_region(remote.region_id).await.unwrap();
    pair.owner.transport.shutdown();
    pair.puller.transport.shutdown();
    assert_pair_balanced(&pair);
}

/// A blob that passes the pre-parse and is still refused by UCX over tcp.
///
/// The complement of [`truncated_rkey_is_refused_before_ucx`]: this one is well
/// formed — `md_map` names one memory domain, the entry is present, `sys_dev` is
/// `UNKNOWN` — so the pre-parse lets it through and `ucp_ep_rkey_unpack` really
/// runs, walking only bytes the blob owns. Over `UCX_TLS=tcp` UCX rejects it
/// because no local memory domain corresponds (it logs `failed to unpack remote
/// key from remote md[0]`). What this pins down is the accounting on that
/// branch, which is lane-independent: `live_rkeys` is incremented only after a
/// successful unpack, so a failed one must leave it untouched rather than
/// counting a key that was never created.
///
/// The rejection is not. On InfiniBand a local mlx5 memory domain *does*
/// correspond, `ucp_ep_rkey_unpack` succeeds, the GET posts with unusable key
/// material, and `uct_rc_verbs` escalates the HCA completion error to
/// `ucs_fatal` — **this same blob aborts the process** (see
/// `docs/src/operations/rdma-performance.md`). The name
/// says `over_tcp` because reading it as a statement about the class would be
/// reading it as "this failure mode is contained", which is the opposite of
/// what the hardware run found.
#[tokio::test(flavor = "multi_thread")]
async fn unusable_rkey_is_refused_by_ucx_over_tcp() {
    const LEN: usize = 64 * 1024;
    let src = PageBuf::new(LEN);
    let dst = PageBuf::new(LEN);

    let pair = start_rma_pair().await;
    let remote = pair.owner_rma.map_region(src.addr(), LEN).await.unwrap();
    let local = pair.puller_rma.map_region(dst.addr(), LEN).await.unwrap();

    let mut well_formed = 1u64.to_le_bytes().to_vec();
    well_formed.push(0); // mem_type
    well_formed.push(0); // md[0]: zero-length key material
    well_formed.push(0xFF); // sys_dev = UNKNOWN, so the distance walk is skipped
    preparse_packed_rkey(&well_formed).expect("this blob is self-terminating");

    let mut req = get_request(&pair, &src, &remote, &local);
    req.packed_rkey = Bytes::from(well_formed);
    req.len = 64;
    let outcome = ring_get(&pair.puller.transport, req).await;
    assert!(
        matches!(outcome, Err(RmaError::Ucx { .. })),
        "UCX should refuse an unreachable memory domain, got {outcome:?}"
    );
    assert_eq!(
        pair.puller
            .transport
            .shared
            .live_rkeys
            .load(Ordering::SeqCst),
        0,
        "a failed unpack must not be counted as a live rkey"
    );

    pair.puller_rma.unmap_region(local.region_id).await.unwrap();
    pair.owner_rma.unmap_region(remote.region_id).await.unwrap();
    pair.owner.transport.shutdown();
    pair.puller.transport.shutdown();
    assert_pair_balanced(&pair);
}

/// `Cmd::refuse_for_shutdown` answers every RMA command it can be handed.
///
/// Teardown's ring drain is the only caller, and reaching it depends on a race
/// no test can pin, so the arms are exercised directly: without this, deleting
/// all three would break nothing.
#[tokio::test(flavor = "multi_thread")]
async fn refused_rma_commands_answer_their_callers() {
    let (map_tx, map_rx) = tokio::sync::oneshot::channel();
    Cmd::MapRegion {
        ptr: 0x1000,
        len: 4096,
        region_id: 1,
        reply: map_tx,
    }
    .refuse_for_shutdown();
    assert!(matches!(
        map_rx.await.expect("MapRegion reply must be sent"),
        Err(RmaError::ShuttingDown)
    ));

    let (unmap_tx, unmap_rx) = tokio::sync::oneshot::channel();
    Cmd::UnmapRegion {
        region_id: 1,
        reply: unmap_tx,
    }
    .refuse_for_shutdown();
    assert!(matches!(
        unmap_rx.await.expect("UnmapRegion reply must be sent"),
        Err(RmaError::ShuttingDown)
    ));

    let (get_tx, get_rx) = tokio::sync::oneshot::channel();
    Cmd::RmaGet {
        req: RmaGetRequest {
            peer: InstanceId::new_v4(),
            remote_addr: 0x2000,
            packed_rkey: Bytes::from_static(&[1, 2, 3]),
            local_region: 1,
            local_offset: 0,
            len: 16,
        },
        reply: get_tx,
    }
    .refuse_for_shutdown();
    assert!(matches!(
        get_rx.await.expect("RmaGet reply must be sent"),
        Err(RmaError::ShuttingDown)
    ));
}

/// A `map_region` whose caller disappears must not leave the region pinned.
///
/// Both halves of the rollback are covered: the progress thread's own, which
/// fires when the reply channel is already closed at send time, and the
/// submit-side `Drop` guard, which fires when the future is dropped after the
/// push. Either way the caller is entitled to free the buffer, so a surviving
/// registration would be a use-after-free waiting to happen.
#[tokio::test(flavor = "multi_thread")]
async fn map_region_cancel_rolls_back() {
    const LEN: usize = 64 * 1024;
    let buf = PageBuf::new(LEN);
    let node = start_node().await;
    let rma = node.transport.rdma_endpoint();
    let live = || node.transport.shared.live_regions.load(Ordering::SeqCst);

    // Half one, deterministically: the receiver is dropped *before* the command
    // exists, so `reply.send` in the MapRegion arm cannot succeed.
    let (tx, rx) = tokio::sync::oneshot::channel();
    drop(rx);
    node.transport
        .shared
        .ring_tx
        .send_async(Cmd::MapRegion {
            ptr: buf.addr(),
            len: LEN,
            region_id: u64::MAX / 2,
            reply: tx,
        })
        .await
        .expect("ring accepts the command");
    node.transport.shared.doorbell.ring();
    assert!(
        wait_until(T, || live() == 0).await,
        "an orphaned registration must be rolled back by the worker"
    );
    // The same buffer maps fine, so the command above really did register and
    // roll back rather than failing on its way in.
    let probe = rma.map_region(buf.addr(), LEN).await.expect("map succeeds");
    assert_eq!(live(), 1);
    rma.unmap_region(probe.region_id).await.expect("unmap");
    assert_eq!(live(), 0);

    // Half two: the public API, cancelled after the push. Polled exactly once —
    // enough to put the command on the ring and start awaiting the reply — then
    // dropped, which is what a `select!` arm losing a race looks like. A timeout
    // would not do: tokio's timer granularity is a millisecond and the round
    // trip is microseconds, so the map would simply win.
    //
    // The progress thread is held first. It is an OS thread of its own, so
    // without the hold it can take the command and reply between the doorbell
    // and the first poll, and the poll returns `Ready`.
    let shared = &node.transport.shared;
    shared.progress_hold.store(true, Ordering::Release);
    assert!(
        wait_until(T, || shared.progress_held.load(Ordering::Acquire)).await,
        "the progress thread never stopped at the hold"
    );
    let delivered = shared.map_replies_delivered.load(Ordering::Acquire);
    let mut pending = Box::pin(rma.map_region(buf.addr(), LEN));
    assert!(
        futures::poll!(pending.as_mut()).is_pending(),
        "the first poll should submit and then await the reply"
    );
    // Then the worker maps the region and delivers its reply into the
    // future's channel, which nobody reads. Dropping the future now is the
    // cancel `MapRollback` exists for: the worker's reply went through, so
    // only the future can give the region back.
    shared.progress_hold.store(false, Ordering::Release);
    assert!(
        wait_until(T, || shared.map_replies_delivered.load(Ordering::Acquire)
            > delivered)
        .await,
        "the worker never delivered the map reply"
    );
    drop(pending);
    assert!(
        wait_until(T, || live() == 0).await,
        "a cancelled map_region must leave no region behind"
    );

    node.transport.shutdown();
    assert_rma_balanced(&node);
}

/// A cancelled `unmap_region` still unmaps, and a retry attaches to it.
///
/// The dangerous shape this pins down: telling a retry "no such region" while
/// the mapping is live and a GET is writing into it would read to the caller as
/// permission to free the buffer.
#[tokio::test(flavor = "multi_thread")]
async fn unmap_cancel_then_retry() {
    const LEN: usize = 32 * 1024 * 1024;
    let mut src = PageBuf::new(LEN);
    let dst = PageBuf::new(LEN);
    src.fill_pattern();

    let pair = start_rma_pair().await;
    let remote = pair.owner_rma.map_region(src.addr(), LEN).await.unwrap();
    let local = pair.puller_rma.map_region(dst.addr(), LEN).await.unwrap();

    let endpoint = pair.puller_rma.clone();
    let req = get_request(&pair, &src, &remote, &local);
    let get = tokio::spawn(async move { endpoint.get(req).await });
    assert!(
        wait_until(T, || {
            pair.puller
                .transport
                .shared
                .inflight_ops
                .load(Ordering::SeqCst)
                >= 1
        })
        .await,
        "the GET must be posted before the unmap is issued"
    );

    // Park an unmap behind the GET, then have the caller walk away: submit it
    // straight onto the ring (guaranteed enqueued) and drop the reply receiver.
    let orphaned = ring_unmap_enqueue(&pair.puller.transport, local.region_id).await;
    drop(orphaned);

    // Fence: an idempotent unmap of an id that never existed round-trips through
    // the same FIFO ring, so its reply proves the worker has already processed
    // the orphaned command above. No sleep, no guessed interval.
    ring_unmap_enqueue(&pair.puller.transport, u64::MAX)
        .await
        .await
        .expect("fence reply is sent")
        .expect("unmapping an unknown id is a no-op");

    // The region must still be mapped: the orphaned unmap parked behind the
    // in-flight GET rather than resolving eagerly. A regression that skipped the
    // in-flight wait would have unmapped it here, dropping the count to 0.
    assert_eq!(
        pair.puller
            .transport
            .shared
            .live_regions
            .load(Ordering::SeqCst),
        1,
        "the region must stay mapped while its GET is in flight"
    );

    // The retry attaches to the unmap already in progress rather than being told
    // the still-mapped region does not exist.
    tokio::time::timeout(T, pair.puller_rma.unmap_region(local.region_id))
        .await
        .expect("retry must resolve")
        .expect("retry must report success, not RegionNotFound");

    get.await
        .expect("get task")
        .expect("the GET completes before the region goes");
    assert_eq!(dst.as_slice(), src.as_slice());
    assert_eq!(
        pair.puller
            .transport
            .shared
            .live_regions
            .load(Ordering::SeqCst),
        0,
        "the puller's region is gone once the retry reports success"
    );

    pair.owner_rma.unmap_region(remote.region_id).await.unwrap();
    pair.owner.transport.shutdown();
    pair.puller.transport.shutdown();
    assert_pair_balanced(&pair);
}

/// Shutdown must resolve an unmap parked behind a GET *and* the GET itself.
///
/// Before the operation registry existed, a survivor of teardown's bounded
/// drain took its caller's `oneshot` sender into UCX's request bookkeeping and
/// the `await` never returned.
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_resolves_parked_unmap_and_get() {
    const LEN: usize = 32 * 1024 * 1024;
    let mut src = PageBuf::new(LEN);
    let dst = PageBuf::new(LEN);
    src.fill_pattern();

    let pair = start_rma_pair().await;
    let remote = pair.owner_rma.map_region(src.addr(), LEN).await.unwrap();
    let local = pair.puller_rma.map_region(dst.addr(), LEN).await.unwrap();

    let endpoint = pair.puller_rma.clone();
    let req = get_request(&pair, &src, &remote, &local);
    let get = tokio::spawn(async move { endpoint.get(req).await });
    assert!(
        wait_until(T, || {
            pair.puller
                .transport
                .shared
                .inflight_ops
                .load(Ordering::SeqCst)
                >= 1
        })
        .await,
        "the GET must be posted before the unmap is issued"
    );

    // Pushed through the ring so `shutdown()` cannot refuse it on the way in,
    // and enqueued inline so the command is provably on the ring before teardown
    // starts — a spawned task plus a sleep would sometimes lose the push into
    // teardown's drain gap and then wait out the test's timeout.
    let unmap = ring_unmap_enqueue(&pair.puller.transport, local.region_id).await;

    pair.puller.transport.shutdown();

    let unmap_outcome = tokio::time::timeout(T, unmap)
        .await
        .expect("the parked unmap must resolve, not hang")
        .expect("the reply must be sent, not dropped");
    let get_outcome = tokio::time::timeout(T, get)
        .await
        .expect("the GET must resolve, not hang")
        .expect("get task");
    println!("shutdown: unmap={unmap_outcome:?} get={get_outcome:?}");

    pair.owner.transport.shutdown();
    assert_pair_balanced(&pair);
}

/// Dropping a `get` future must not strand the region's in-flight count.
///
/// The transfer keeps running — that is deliberate, and it is what stops UCX
/// writing into a range the caller has since unmapped. What must still happen is
/// the decrement, without which the region could never be unmapped at all.
#[tokio::test(flavor = "multi_thread")]
async fn get_cancel_still_releases_the_region() {
    const LEN: usize = 32 * 1024 * 1024;
    let mut src = PageBuf::new(LEN);
    let dst = PageBuf::new(LEN);
    src.fill_pattern();

    let pair = start_rma_pair().await;
    let remote = pair.owner_rma.map_region(src.addr(), LEN).await.unwrap();
    let local = pair.puller_rma.map_region(dst.addr(), LEN).await.unwrap();

    let cancelled = tokio::time::timeout(
        Duration::from_millis(1),
        pair.puller_rma
            .get(get_request(&pair, &src, &remote, &local)),
    )
    .await;
    assert!(cancelled.is_err(), "the GET must be cancelled mid-transfer");

    // Resolves only once the abandoned operation has completed and decremented.
    tokio::time::timeout(T, pair.puller_rma.unmap_region(local.region_id))
        .await
        .expect("unmap must resolve after an abandoned GET")
        .expect("unmap succeeds");
    assert_eq!(
        dst.as_slice(),
        src.as_slice(),
        "the abandoned transfer still ran to completion"
    );

    pair.owner_rma.unmap_region(remote.region_id).await.unwrap();
    pair.owner.transport.shutdown();
    pair.puller.transport.shutdown();
    assert_pair_balanced(&pair);
}

/// The pre-parse is the containment for `ucp_ep_rkey_unpack`'s length-free walk.
///
/// Unit-level, because the shapes that matter are ones a real packer never
/// produces. A length bound cannot stand in for this: nine bytes is exactly what
/// a genuine tcp-lane rkey looks like, so the size check waves the first case
/// straight through to UCX.
#[test]
fn preparse_rejects_blobs_ucx_would_walk_off_the_end_of() {
    // A `md_map` claiming 64 memory domains with no entries behind it. Stage 1
    // walks one length byte per set bit, driven by `md_map` and never by a
    // buffer end.
    let mut truncated = vec![0xFFu8; 8];
    truncated.push(0);
    assert_eq!(truncated.len(), 9);
    assert!(matches!(
        preparse_packed_rkey(&truncated),
        Err(RmaError::InvalidRkey)
    ));

    // A length byte sitting at the very last content byte, declaring 255. This
    // is the shape that makes "just add N bytes of padding" unfixable: the
    // stage-1 walk runs 255 bytes past whatever the blob's own size is.
    let mut tail_declares_255 = 1u64.to_le_bytes().to_vec();
    tail_declares_255.push(0); // mem_type
    tail_declares_255.push(255); // md[0] length, at the last byte
    assert_eq!(tail_declares_255.len(), 10);
    assert!(matches!(
        preparse_packed_rkey(&tail_declares_255),
        Err(RmaError::InvalidRkey)
    ));

    // The same shape at the maximum accepted size, so the overrun starts one
    // byte past `MAX_PACKED_RKEY` — 255 bytes further than any pad this code
    // ever carried. Seven memory domains, six entries of 168 bytes, then a
    // final length byte at index 1023.
    let mut crafted = 0b111_1111u64.to_le_bytes().to_vec();
    crafted.push(0); // mem_type
    for _ in 0..6 {
        crafted.push(168);
        crafted.extend(std::iter::repeat_n(0u8, 168));
    }
    crafted.push(255);
    assert_eq!(crafted.len(), MAX_PACKED_RKEY);
    assert!(matches!(
        preparse_packed_rkey(&crafted),
        Err(RmaError::InvalidRkey)
    ));

    // `md_map != 0` with a `sys_dev` byte that is not `UNKNOWN` and no `0xFF`
    // terminator behind it: stage 2 sets `buffer_end = UINTPTR_MAX` and walks
    // 3-byte records until it finds one.
    let mut unterminated = 1u64.to_le_bytes().to_vec();
    unterminated.push(0); // mem_type
    unterminated.push(0); // md[0] length
    unterminated.push(7); // sys_dev, not UNKNOWN
    unterminated.extend_from_slice(&[1, 2, 3]); // one distance record, no terminator
    assert!(matches!(
        preparse_packed_rkey(&unterminated),
        Err(RmaError::InvalidRkey)
    ));

    // Same, with the terminator UCX's own packer writes.
    let mut terminated = unterminated.clone();
    terminated.push(0xFF);
    preparse_packed_rkey(&terminated).expect("a terminated distance list is parseable");

    // Truncated header.
    assert!(matches!(
        preparse_packed_rkey(&[0u8; 4]),
        Err(RmaError::InvalidRkey)
    ));
    assert!(matches!(
        preparse_packed_rkey(&[0u8; 8]),
        Err(RmaError::InvalidRkey)
    ));

    // The degenerate-but-real shape CI actually produces: empty `md_map`, so no
    // entries, no `sys_dev` byte, nine bytes total.
    preparse_packed_rkey(&[0, 0, 0, 0, 0, 0, 0, 0, 0]).expect("an empty md_map is well formed");

    // Perfect framing, out-of-range `mem_type`. UCX indexes
    // `[UCS_MEMORY_TYPE_LAST]`-sized arrays by this byte with no bounds check, so
    // a value >= 10 is a wild read even though every walk position is in bounds.
    let mut bad_mem_type = 1u64.to_le_bytes().to_vec();
    bad_mem_type.push(0xFF); // mem_type, out of range
    bad_mem_type.push(0); // md[0] length
    bad_mem_type.push(SYS_DEV_UNKNOWN); // skip stage 2
    assert!(matches!(
        preparse_packed_rkey(&bad_mem_type),
        Err(RmaError::InvalidRkey)
    ));

    // The same framing with an in-range `mem_type` passes: HOST..GAUDI are 0..9,
    // and velo's own packer emits 0.
    let mut good_mem_type = bad_mem_type.clone();
    good_mem_type[8] = 9; // GAUDI, the last valid index
    preparse_packed_rkey(&good_mem_type).expect("an in-range mem_type is accepted");
    good_mem_type[8] = 0; // HOST, what velo actually emits
    preparse_packed_rkey(&good_mem_type).expect("host memory is accepted");
}

/// The blobs `ucp_rkey_pack` really produces must pass the pre-parse.
///
/// Guards the other direction from
/// [`preparse_rejects_blobs_ucx_would_walk_off_the_end_of`]: a stricter walk
/// than UCX's own packer would reject every real key and take the RDMA path with
/// it.
#[tokio::test(flavor = "multi_thread")]
async fn preparse_accepts_real_packed_rkeys() {
    const LEN: usize = 64 * 1024;
    let buf = PageBuf::new(LEN);
    let node = start_node().await;
    let rma = node.transport.rdma_endpoint();

    for len in [4096usize, LEN] {
        let region = rma.map_region(buf.addr(), len).await.expect("map");
        preparse_packed_rkey(&region.packed_rkey).unwrap_or_else(|e| {
            panic!(
                "a genuine {}-byte rkey was rejected: {e}",
                region.packed_rkey.len()
            )
        });
        rma.unmap_region(region.region_id).await.expect("unmap");
    }

    node.transport.shutdown();
    assert_rma_balanced(&node);
}

/// A truncated blob is refused before UCX sees it, on both paths.
///
/// The nine-byte shape is the dangerous one: it is exactly the size of a real
/// tcp-lane key, so only the pre-parse distinguishes it. Reaching
/// `ucp_ep_rkey_unpack` with it would walk 64 phantom entries off the end of
/// whatever buffer it sat in.
#[tokio::test(flavor = "multi_thread")]
async fn truncated_rkey_is_refused_before_ucx() {
    const LEN: usize = 64 * 1024;
    let src = PageBuf::new(LEN);
    let dst = PageBuf::new(LEN);

    let pair = start_rma_pair().await;
    let remote = pair.owner_rma.map_region(src.addr(), LEN).await.unwrap();
    let local = pair.puller_rma.map_region(dst.addr(), LEN).await.unwrap();

    let mut hostile = vec![0xFFu8; 8];
    hostile.push(0);

    // Submit side.
    let mut req = get_request(&pair, &src, &remote, &local);
    req.packed_rkey = Bytes::from(hostile.clone());
    req.len = 64;
    assert!(matches!(
        pair.puller_rma.get(req).await,
        Err(RmaError::InvalidRkey)
    ));

    // Worker side, reached directly so the submit-side check cannot shadow it.
    let mut req = get_request(&pair, &src, &remote, &local);
    req.packed_rkey = Bytes::from(hostile);
    req.len = 64;
    assert!(matches!(
        ring_get(&pair.puller.transport, req).await,
        Err(RmaError::InvalidRkey)
    ));

    // Nothing was unpacked, so nothing can have leaked.
    assert_eq!(
        pair.puller
            .transport
            .shared
            .live_rkeys
            .load(Ordering::SeqCst),
        0
    );

    pair.puller_rma.unmap_region(local.region_id).await.unwrap();
    pair.owner_rma.unmap_region(remote.region_id).await.unwrap();
    pair.owner.transport.shutdown();
    pair.puller.transport.shutdown();
    assert_pair_balanced(&pair);
}

/// The peer vanishing mid-transfer must still answer the GET's caller.
///
/// This is the closest an in-process tcp harness gets to teardown's
/// abandoned-operation path: measured, UCX completes the GET with
/// `Endpoint timeout` rather than leaving it outstanding, so the reply comes
/// from the normal completion route. `WorkerState::abandon_rma_ops` remains the
/// backstop for a peer that stops progressing without closing, which this
/// test does not exercise.
#[tokio::test(flavor = "multi_thread")]
async fn peer_shutdown_during_get_answers_caller() {
    const LEN: usize = 64 * 1024 * 1024;
    let mut src = PageBuf::new(LEN);
    let dst = PageBuf::new(LEN);
    src.fill_pattern();

    let pair = start_rma_pair().await;
    let remote = pair.owner_rma.map_region(src.addr(), LEN).await.unwrap();
    let local = pair.puller_rma.map_region(dst.addr(), LEN).await.unwrap();

    let endpoint = pair.puller_rma.clone();
    let req = get_request(&pair, &src, &remote, &local);
    let get = tokio::spawn(async move { endpoint.get(req).await });
    assert!(
        wait_until(T, || {
            pair.puller
                .transport
                .shared
                .inflight_ops
                .load(Ordering::SeqCst)
                >= 1
        })
        .await,
        "the GET must be in flight before the owner goes"
    );

    pair.owner.transport.shutdown();
    pair.puller.transport.shutdown();

    let outcome = tokio::time::timeout(T, get)
        .await
        .expect("the GET must resolve, not hang")
        .expect("get task must not panic");
    println!("peer-shutdown GET resolved as {outcome:?}");
    assert_pair_balanced(&pair);
}

// ---------------------------------------------------------------------------
// Endpoint lifecycle: the idle reaper (D9) and eager wireup
// ---------------------------------------------------------------------------

/// The builder's floor exactly: the shortest idle timeout the public surface
/// admits, so the reaper tests wait as little as the API allows.
///
/// A test that needs the reaper to act *faster* than endpoint wireup takes has
/// to go under the floor with `start_node_with_config`, and only one does.
const IDLE: Duration = Duration::from_millis(500);

fn eps_open(node: &Node) -> usize {
    node.transport.shared.eps_open.load(Ordering::SeqCst)
}

fn eps_closed_idle(node: &Node) -> u64 {
    node.transport.shared.eps_closed_idle.load(Ordering::SeqCst)
}

/// Send one `Message` frame and wait for it to arrive, so the sender's endpoint
/// is provably established and provably not still carrying an in-flight AM.
async fn ping_message(from: &Node, to: &Node, errs: &Arc<CountingErrors>) {
    ping_message_to(from, to.instance_id, to, errs, "frame").await
}

/// As [`ping_message`], but with the addressed instance and the receiving node
/// named separately — a re-registered peer keeps its instance id while its
/// frames arrive at a different worker.
async fn ping_message_to(
    from: &Node,
    target: InstanceId,
    arrives_at: &Node,
    errs: &Arc<CountingErrors>,
    what: &str,
) {
    let out = from.transport.send_message(
        target,
        Bytes::from_static(b"h"),
        Bytes::from_static(b"p"),
        MessageType::Message,
        errs.clone(),
    );
    assert!(matches!(out, SendOutcome::Admitted));
    assert!(
        recv_message(&arrives_at.streams.message_stream, ARRIVAL)
            .await
            .is_some(),
        "{what}: the frame must arrive before the endpoint is called established"
    );
}

/// Wire B's path to A, then give A its endpoint to B, with B's endpoint
/// creation slower than A's idle timeout.
///
/// The order is what makes the reaper tests independent of wireup time. B's
/// first frame to A waits for B's `ucp_ep_create`, and under load that took
/// 630 to 1250 ms, past the 500 ms timeout. With A's endpoint created first,
/// A saw nothing from B for that long, reaped the endpoint correctly, and B's
/// first Message was lost the way `reaping_disrupts_the_peers_path_back`
/// shows. B's slow create is forced here, so the old order fails every time.
///
/// B's warm-up Message reaches A before A has an endpoint to B, so it stamps
/// nothing: the wait for `eps_inbound_unmatched` checks that. A's endpoint is
/// then created by A's own send and stamped by nothing from B.
async fn wire_peer_path_first(a: &Node, b: &Node, errs: &Arc<CountingErrors>) {
    b.transport
        .shared
        .ep_create_delay_ms
        .store((IDLE * 2).as_millis() as u64, Ordering::Relaxed);
    assert_eq!(eps_open(a), 0);
    let unmatched = a
        .transport
        .shared
        .eps_inbound_unmatched
        .load(Ordering::SeqCst);
    ping_message(b, a, errs).await;
    assert!(
        wait_until(T, || a
            .transport
            .shared
            .eps_inbound_unmatched
            .load(Ordering::SeqCst)
            > unmatched)
        .await,
        "B's warm-up frame was never seen by A's stamp pass"
    );
    ping_message(a, b, errs).await;
    assert_eq!(eps_open(a), 1);
}

/// Count `node`'s endpoints as set up as soon as they are created.
///
/// For reaper tests whose subject is not setup: their peers never send to the
/// node, and without this each endpoint would wait `EP_SETUP_BOUND` before
/// idle time counts. That would hide what they test. Setup has its own tests
/// (`an_endpoint_is_not_reaped_while_its_peer_sets_up`).
fn skip_setup(node: &Node) {
    node.transport
        .shared
        .ep_setup_bound_ms
        .store(0, Ordering::Relaxed);
}

fn eps_stamped_inbound(node: &Node) -> u64 {
    node.transport
        .shared
        .eps_stamped_inbound
        .load(Ordering::SeqCst)
}

/// Freeze `node`'s reaper clock, and record when each sender's frames
/// arrive, for [`reaped_while_active`].
///
/// From here on the node's endpoints age only when the test calls
/// [`advance`]. The reaper tests used to wait out real timeouts, and on a
/// loaded host a starved thread aged an endpoint past its timeout with
/// nothing wrong: the test then failed on a correct reap.
fn hold_reaper_clock(node: &Node) {
    let shared = &node.transport.shared;
    shared.track_inbound.store(true, Ordering::Relaxed);
    shared.reaper_clock.manual().freeze();
}

/// Move `node`'s reaper clock forward, then wait until the progress thread
/// has scanned at the new time.
async fn advance(node: &Node, by: Duration) {
    node.transport.shared.reaper_clock.manual().advance(by);
    settle(node).await;
}

/// Wait for two more passes of `node`'s progress thread.
///
/// Two, because one may be under way: it may have read its clock before the
/// clock moved, or be inside the callback the test just saw a sign of.
async fn settle(node: &Node) {
    let shared = &node.transport.shared;
    let passes = shared.passes.load(Ordering::Acquire);
    assert!(
        wait_until(T, || shared.passes.load(Ordering::Acquire) >= passes + 2).await,
        "the progress thread ran no more passes"
    );
}

/// Idle reaps of an endpoint whose peer sent a frame within the timeout.
///
/// Zero at any load for a correct reaper: this is the exact form of "a frame
/// from the peer counts as use of the endpoint" (see the check in
/// `reap_idle_eps`). It needs [`hold_reaper_clock`], which turns the
/// recording on.
fn reaped_while_active(node: &Node) -> u64 {
    node.transport
        .shared
        .eps_reaped_while_active
        .load(Ordering::SeqCst)
}

/// The builder raises a sub-floor idle timeout rather than honouring it.
///
/// Asserted on its own because every other reaper test uses a value at or above
/// the floor, so nothing else would notice the clamp disappearing. What the
/// floor guards against is a timeout shorter than endpoint wireup: endpoints
/// then close between ordinary uses, each next use pays wireup again, and each
/// close costs the peer its next Messages and pings to us, if it sends any
/// before keepalive fails its endpoint. The reaper reports none of that.
#[test]
fn a_sub_floor_ep_idle_timeout_is_clamped() {
    let clamped = UcxTransportBuilder::new()
        .ep_idle_timeout(Some(Duration::from_millis(1)))
        .build()
        .expect("build")
        .config
        .ep_idle_timeout;
    assert_eq!(clamped, Some(super::MIN_EP_IDLE_TIMEOUT));

    let honoured = UcxTransportBuilder::new()
        .ep_idle_timeout(Some(Duration::from_secs(60)))
        .build()
        .expect("build")
        .config
        .ep_idle_timeout;
    assert_eq!(honoured, Some(Duration::from_secs(60)));

    assert_eq!(
        UcxTransportBuilder::new()
            .ep_idle_timeout(None)
            .build()
            .expect("build")
            .config
            .ep_idle_timeout,
        None,
        "explicitly disabling must stay disabled"
    );
}

/// Every kind of frame a peer sends us refreshes our endpoint to that peer, and
/// a frame from a peer we hold no endpoint to refreshes nothing.
///
/// The stamp keys on the sender's incarnation, which starts every frame
/// header, not on UCX's `reply_ep`. UCX sets `reply_ep` only for frames sent
/// with `UCP_AM_SEND_FLAG_REPLY` (Messages and pings), so a stamp keyed on it
/// missed Responses, Events, Acks, Pongs and ShuttingDown echoes. This test
/// sends one frame of each kind from B to A, waits for A's stamp count to move
/// by one each time, and checks that none of them went unmatched. A pong is
/// covered by A probing B. Then a third node C, which A has no endpoint to,
/// sends A a Message: that must count as unmatched and stamp nothing.
#[tokio::test(flavor = "multi_thread")]
async fn an_inbound_frame_refreshes_the_endpoint_to_its_sender() {
    // The reaper gates the stamping work, so it has to be on — at a timeout far
    // longer than this test, since nothing here is about reaping.
    let a = start_node_with(|b| b.ep_idle_timeout(Some(Duration::from_secs(3600)))).await;
    let b = start_node().await;
    let c = start_node().await;
    cross_register(&a, &b);
    cross_register(&a, &c);
    let errs = CountingErrors::new();
    let stamped = || {
        a.transport
            .shared
            .eps_stamped_inbound
            .load(Ordering::SeqCst)
    };
    let unmatched = || {
        a.transport
            .shared
            .eps_inbound_unmatched
            .load(Ordering::SeqCst)
    };

    // A now owns an endpoint to B, and none to C.
    ping_message(&a, &b, &errs).await;
    assert_eq!(eps_open(&a), 1);
    let unmatched_before = unmatched();

    for kind in [
        MessageType::Message,
        MessageType::Response,
        MessageType::Event,
        MessageType::Ack,
        MessageType::ShuttingDown,
    ] {
        let before = stamped();
        let out = b.transport.send_message(
            a.instance_id,
            Bytes::from_static(b"h"),
            Bytes::from_static(b"p"),
            kind,
            errs.clone(),
        );
        assert!(matches!(out, SendOutcome::Admitted));
        assert!(
            wait_until(T, || stamped() > before).await,
            "a {kind:?} frame from B did not refresh A's endpoint to B \
             (unmatched: {})",
            unmatched() - unmatched_before
        );
    }
    // A pong: A probes B, and B's answer arrives at A.
    let before = stamped();
    assert!(a.transport.check_health(b.instance_id, T).await.is_ok());
    assert!(
        wait_until(T, || stamped() > before).await,
        "a pong from B did not refresh A's endpoint to B"
    );
    // A ping: B probes A.
    let before = stamped();
    assert!(b.transport.check_health(a.instance_id, T).await.is_ok());
    assert!(
        wait_until(T, || stamped() > before).await,
        "a ping from B did not refresh A's endpoint to B"
    );
    assert_eq!(
        unmatched(),
        unmatched_before,
        "a frame from B matched no endpoint, though A holds one to B"
    );

    // C's frame arrives, and matches nothing: A has no endpoint to C.
    let before = stamped();
    ping_message(&c, &a, &errs).await;
    assert!(
        wait_until(T, || unmatched() > unmatched_before).await,
        "a frame from a peer A holds no endpoint to was not counted as unmatched"
    );
    assert_eq!(stamped(), before, "a frame from C stamped an endpoint");
    assert_eq!(eps_open(&a), 1);
    assert_eq!(errs.count(), 0);

    a.transport.shutdown();
    b.transport.shutdown();
    c.transport.shutdown();
    assert_rma_balanced(&a);
    assert_rma_balanced(&b);
    assert_rma_balanced(&c);
}

/// A sender seen once in a pass is still seen when many frames from another
/// sender follow it in the same pass.
///
/// The receive callback publishes each frame's sender into a fixed ring that
/// the main loop drains once per pass. When one pass received more frames than
/// the ring has slots, the newest overwrote the oldest. So a peer that sent
/// one ping, followed in the same pass by a burst of Messages from a busy
/// peer, lost its stamp. The pong answering that ping rides our endpoint to
/// the pinger without a send count, so the reaper could close that endpoint
/// in the same pass, under the pong. Eight frames from one peer in one pass is
/// ordinary traffic.
#[test]
fn a_sighting_is_not_overwritten_by_a_burst_from_another_sender() {
    const P: usize = 0x5051;
    const Q: usize = 0x5152;
    let ring = SenderSightings::new();
    ring.record(P);
    for _ in 0..8 {
        ring.record(Q);
    }
    let (mut seen, mut out) = (0, Vec::new());
    ring.drain_into(&mut seen, &mut out);
    assert!(
        out.contains(&P),
        "P's sighting was overwritten by eight sightings of Q: {out:x?}"
    );
    assert!(out.contains(&Q));
}

/// More distinct senders in one pass than the ring holds is reported, not
/// silently dropped: the main loop then stamps every endpoint, so none of
/// the lost senders can be reaped under its own traffic.
#[test]
fn more_senders_in_one_pass_than_the_ring_holds_is_reported() {
    let ring = SenderSightings::new();
    let (mut seen, mut out) = (0, Vec::new());
    for sender in 1..=SENDER_SLOTS {
        ring.record(sender);
    }
    assert!(
        !ring.drain_into(&mut seen, &mut out),
        "a full ring is not an overflow"
    );
    assert_eq!(out.len(), SENDER_SLOTS);

    out.clear();
    for sender in 1..=SENDER_SLOTS + 1 {
        ring.record(sender);
    }
    assert!(
        ring.drain_into(&mut seen, &mut out),
        "{} distinct senders fit in {SENDER_SLOTS} slots only by losing one, \
         and the loss was not reported",
        SENDER_SLOTS + 1
    );

    out.clear();
    assert!(
        !ring.drain_into(&mut seen, &mut out) && out.is_empty(),
        "a drained ring reported sightings again"
    );
}

/// The consequence that matters operationally: a peer that only ever *sends*
/// us Messages keeps its endpoint alive, instead of having it reaped and
/// blackholed under its own traffic. The Response and Event cases are pinned
/// by the two tests after this one.
///
/// This is the mutation target for the inbound stamp. Without it the endpoint
/// is reaped one timeout after A's last send, and `reaped_while_active` counts
/// that reap, because B's last frame is then a quarter timeout old.
///
/// A's clock moves only between B's frames, a quarter timeout at a time, so
/// no endpoint here can age past its timeout while B sends, however loaded
/// the host is.
#[tokio::test(flavor = "multi_thread")]
async fn a_peer_that_keeps_sending_keeps_its_endpoint() {
    let a = start_node_with(|b| b.ep_idle_timeout(Some(IDLE))).await;
    let b = start_node().await;
    cross_register(&a, &b);
    let errs = CountingErrors::new();
    hold_reaper_clock(&a);

    // A owns an endpoint to B, and then never sends again.
    wire_peer_path_first(&a, &b, &errs).await;

    // B sends a Message every quarter timeout of A's clock, for three timeouts.
    let stamped = eps_stamped_inbound(&a);
    for _ in 0..12 {
        ping_message(&b, &a, &errs).await;
        advance(&a, IDLE / 4).await;
    }
    assert_eq!(
        reaped_while_active(&a),
        0,
        "A reaped its endpoint to B within a timeout of a frame from B"
    );
    assert!(
        eps_stamped_inbound(&a) > stamped,
        "B's Messages did not stamp A's endpoint to B"
    );
    assert_eq!(
        eps_closed_idle(&a),
        0,
        "a peer's endpoint was reaped while that peer was actively sending to us"
    );
    assert_eq!(
        eps_open(&a),
        1,
        "the endpoint was retired under live traffic"
    );
    assert_eq!(errs.count(), 0);

    // And once the traffic stops, it *is* reaped — the stamp refreshes idleness,
    // it does not disable it.
    advance(&a, IDLE * 2).await;
    assert_eq!(
        eps_closed_idle(&a),
        1,
        "the endpoint was never reaped after the inbound traffic stopped"
    );
    assert_eq!(reaped_while_active(&a), 0);

    a.transport.shutdown();
    b.transport.shutdown();
    assert_rma_balanced(&a);
    assert_rma_balanced(&b);
}

/// A peer that answers us keeps its endpoint, the same as a peer that sends us
/// Messages.
///
/// The common shape of traffic: A sends B one request, and B streams frames
/// back for longer than the idle timeout while A sends nothing more. A must
/// count B's frames as use of its endpoint to B. If A reaps that endpoint
/// under the stream, the stream itself survives over the tcp lane: Responses
/// and Events sent after the reap were measured to arrive. What the reap
/// costs is B's next Message to A, which is lost without an error
/// (`reaping_disrupts_the_peers_path_back`), or B's next ping, which gets no
/// Pong, and the wireup A's next send pays. So the reap counts are the
/// assertions that fail here, not the arrival count.
///
/// `a_peer_that_keeps_sending_keeps_its_endpoint` is the control: the same
/// schedule with Message frames passes. Each frame arrives before A's clock
/// moves on, so the schedule is the same at any load.
async fn a_peer_that_keeps_answering_keeps_its_endpoint(kind: MessageType) {
    let a = start_node_with(|b| b.ep_idle_timeout(Some(IDLE))).await;
    let b = start_node().await;
    cross_register(&a, &b);
    let errs = CountingErrors::new();
    hold_reaper_clock(&a);

    // A owns an endpoint to B, and B's path back to A rides it. No frame from
    // B stamps A's endpoint before the stream: that stamp would hide the
    // defect for one timeout.
    wire_peer_path_first(&a, &b, &errs).await;
    let stamped = eps_stamped_inbound(&a);

    let stream = match kind {
        MessageType::Response => &a.streams.response_stream,
        MessageType::Event => &a.streams.event_stream,
        other => panic!("no stream check for {other:?}"),
    };
    // One frame per quarter timeout of A's clock, for two timeouts.
    const FRAMES: usize = 8;
    for i in 0..FRAMES {
        let out = b.transport.send_message(
            a.instance_id,
            Bytes::from_static(b"h"),
            Bytes::from(vec![i as u8]),
            kind,
            errs.clone(),
        );
        assert!(matches!(out, SendOutcome::Admitted));
        assert!(
            recv(stream, T).await.is_some(),
            "{kind:?} frame {i} from B never arrived"
        );
        advance(&a, IDLE / 4).await;
    }

    assert_eq!(
        reaped_while_active(&a),
        0,
        "A reaped its endpoint to B within a timeout of a {kind:?} frame from B"
    );
    assert_eq!(
        eps_closed_idle(&a),
        0,
        "A reaped its endpoint to B while B was sending {kind:?} frames to A"
    );
    assert!(
        eps_stamped_inbound(&a) > stamped,
        "B's {kind:?} frames did not stamp A's endpoint to B"
    );
    assert_eq!(errs.count(), 0);

    a.transport.shutdown();
    b.transport.shutdown();
    assert_rma_balanced(&a);
    assert_rma_balanced(&b);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_peer_that_keeps_sending_responses_keeps_its_endpoint() {
    a_peer_that_keeps_answering_keeps_its_endpoint(MessageType::Response).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_peer_that_keeps_sending_events_keeps_its_endpoint() {
    a_peer_that_keeps_answering_keeps_its_endpoint(MessageType::Event).await;
}

/// An endpoint is not idle while its peer is still setting up.
///
/// A creates its endpoint to B by sending to B. B's first frame back waits for
/// B's own `ucp_ep_create`, which took 630-1250 ms under load, past the 500 ms
/// floor. A reaper that counted that wait as idle time closed A's endpoint
/// before B's first frame arrived, and B's next Messages to A were lost
/// (`reaping_disrupts_the_peers_path_back`). Setup ends with B's first frame;
/// only then does idle time count.
///
/// A's clock is held, so the wait for B is two timeouts of A's clock, at any
/// load. B's creation is also made slower than the timeout, as under load.
/// Today's reaper closes A's endpoint at the first scan after the move.
#[tokio::test(flavor = "multi_thread")]
async fn an_endpoint_is_not_reaped_while_its_peer_sets_up() {
    let a = start_node_with(|b| b.ep_idle_timeout(Some(IDLE))).await;
    let b = start_node().await;
    cross_register(&a, &b);
    let errs = CountingErrors::new();
    hold_reaper_clock(&a);

    // A's send creates A's endpoint to B. B has sent nothing to A.
    ping_message(&a, &b, &errs).await;
    assert_eq!(eps_open(&a), 1);
    advance(&a, IDLE * 2).await;
    assert_eq!(
        eps_closed_idle(&a),
        0,
        "A reaped its endpoint to B before B's first frame, while B was setting up"
    );
    assert_eq!(eps_open(&a), 1);

    // B answers. Its first frame waits for its slow `ucp_ep_create`.
    b.transport
        .shared
        .ep_create_delay_ms
        .store((IDLE * 2).as_millis() as u64, Ordering::Relaxed);
    let out = b.transport.send_message(
        a.instance_id,
        Bytes::from_static(b"h"),
        Bytes::from_static(b"p"),
        MessageType::Response,
        errs.clone(),
    );
    assert!(matches!(out, SendOutcome::Admitted));
    assert!(
        recv(&a.streams.response_stream, ARRIVAL).await.is_some(),
        "B's first frame never arrived"
    );
    settle(&a).await;

    // Setup is over, and idle time counts from B's frame.
    advance(&a, IDLE / 2).await;
    assert_eq!(
        eps_closed_idle(&a),
        0,
        "reaped within a timeout of B's frame"
    );
    advance(&a, IDLE * 2).await;
    assert_eq!(
        eps_closed_idle(&a),
        1,
        "the endpoint was never reaped once it was set up and idle"
    );
    assert_eq!(reaped_while_active(&a), 0);
    assert_eq!(errs.count(), 0);

    a.transport.shutdown();
    b.transport.shutdown();
    assert_rma_balanced(&a);
    assert_rma_balanced(&b);
}

/// Setup has a bound: an endpoint whose peer never sends to us is still
/// reaped.
///
/// A peer may never send to us at all (we only send to it, or only read its
/// memory), or may die during setup. Without a bound, its endpoint would stay
/// open forever. After `EP_SETUP_BOUND` the endpoint is judged on its last use
/// like any other, so an endpoint already idle for the timeout is closed at
/// the next scan.
#[tokio::test(flavor = "multi_thread")]
async fn an_endpoint_whose_peer_never_sends_is_reaped_after_the_setup_bound() {
    let a = start_node_with(|b| b.ep_idle_timeout(Some(IDLE))).await;
    let b = start_node().await;
    cross_register(&a, &b);
    let errs = CountingErrors::new();
    hold_reaper_clock(&a);

    ping_message(&a, &b, &errs).await;
    assert_eq!(eps_open(&a), 1);

    // Up to the bound, the endpoint is still being set up.
    advance(&a, EP_SETUP_BOUND).await;
    assert_eq!(
        eps_closed_idle(&a),
        0,
        "reaped before its setup bound, with no frame from its peer"
    );
    // Past it, the endpoint has been idle for far more than the timeout. One
    // timeout is also more than a scan period, so a scan runs.
    advance(&a, IDLE).await;
    assert_eq!(
        eps_closed_idle(&a),
        1,
        "an endpoint whose peer never sent was not reaped after the setup bound"
    );
    assert_eq!(eps_open(&a), 0);
    assert_eq!(reaped_while_active(&a), 0);

    a.transport.shutdown();
    b.transport.shutdown();
    assert_rma_balanced(&a);
    assert_rma_balanced(&b);
}

/// The default is off, and "off" has to mean *never*, not *rarely*.
///
/// D9's sign-off left the reaper disabled because a reconnect costs ~14 ms of
/// UCX wireup against a warm RDMA read of 108–229 µs. A default that quietly
/// closed endpoints would hand that bill to every deployment.
#[tokio::test(flavor = "multi_thread")]
async fn idle_reaper_is_off_by_default() {
    let a = start_node().await;
    let b = start_node().await;
    cross_register(&a, &b);
    let errs = CountingErrors::new();

    ping_message(&a, &b, &errs).await;
    // Both directions, because `reaping_disrupts_the_peers_path_back` asserts
    // the reverse direction is *broken* after a reap and would be vacuous if it
    // were broken here too.
    ping_message(&b, &a, &errs).await;
    assert_eq!(eps_open(&a), 1);

    // Many scan periods' worth of idleness at any timeout a test would pick.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        eps_closed_idle(&a),
        0,
        "the reaper ran without being configured"
    );
    assert_eq!(
        eps_open(&a),
        1,
        "an endpoint was closed with the reaper off"
    );

    a.transport.shutdown();
    b.transport.shutdown();
    assert_rma_balanced(&a);
    assert_rma_balanced(&b);
}

/// Deliverable A's happy path, from the reaping side: an endpoint goes idle, is
/// closed, and the next use wires a new one up with nothing to see from here.
///
/// What the *peer* sees is a different and much less happy story; it has its own
/// test below.
#[tokio::test(flavor = "multi_thread")]
async fn idle_endpoint_closes_and_the_next_send_wires_up_again() {
    let a = start_node_with(|b| b.ep_idle_timeout(Some(IDLE))).await;
    skip_setup(&a);
    let b = start_node().await;
    cross_register(&a, &b);
    let errs = CountingErrors::new();

    ping_message_to(&a, b.instance_id, &b, &errs, "first send").await;
    assert_eq!(eps_open(&a), 1);

    assert!(
        wait_until(T, || eps_closed_idle(&a) >= 1).await,
        "the idle endpoint was never reaped"
    );
    assert_eq!(eps_open(&a), 0, "the reaped endpoint was not retired");

    // The next send from our side wires up again, transparently — no error, no
    // re-registration, and the peer is not marked failed because nothing failed.
    ping_message_to(&a, b.instance_id, &b, &errs, "send after the reap").await;
    assert!(
        eps_open(&a) >= 1,
        "the next send did not re-establish an endpoint"
    );
    assert_eq!(errs.count(), 0, "a send failed across the reap");
    assert!(
        a.transport.shared.failed_peers.is_empty(),
        "an idle close must not mark the peer failed"
    );

    a.transport.shutdown();
    b.transport.shutdown();
    assert_rma_balanced(&a);
    assert_rma_balanced(&b);
}

/// `unregister` forgets one peer, closes its endpoint, and leaves another
/// peer alone.
///
/// The endpoint lives on the progress thread, which may close one only at a
/// safe point of its loop (see `run_loop`), so `unregister` cannot close it in
/// place. It removes the peer's blob and bumps `reg_epoch`, the signal a
/// re-registration sends. The next pass finds the endpoint's peer gone and
/// closes it. A send to the peer then fails before the wire, as one to a peer
/// never registered does, and registering again wires up a new endpoint.
#[tokio::test(flavor = "multi_thread")]
async fn unregister_closes_the_endpoint_and_leaves_others_alone() {
    let a = start_node().await;
    let gone = start_node().await;
    let stays = start_node().await;
    cross_register(&a, &gone);
    cross_register(&a, &stays);
    let errs = CountingErrors::new();
    ping_message(&a, &gone, &errs).await;
    ping_message(&a, &stays, &errs).await;
    assert_eq!(eps_open(&a), 2);

    a.transport.unregister(gone.instance_id);
    assert!(!a.transport.shared.peers.contains_key(&gone.instance_id));
    assert!(!a.transport.connections.contains_key(&gone.instance_id));
    assert!(
        wait_until(T, || eps_open(&a) == 1).await,
        "the endpoint to the unregistered peer stayed open"
    );

    let failed = CountingErrors::new();
    let out = a.transport.send_message(
        gone.instance_id,
        Bytes::from_static(b"h"),
        Bytes::from_static(b"p"),
        MessageType::Message,
        failed.clone(),
    );
    assert!(matches!(out, SendOutcome::Admitted));
    assert!(
        failed.wait_for_error(T).await,
        "a send to an unregistered peer must fail before the wire"
    );
    assert!(!a.transport.connections.contains_key(&gone.instance_id));

    ping_message(&a, &stays, &errs).await;
    assert_eq!(eps_open(&a), 1, "the other peer keeps its endpoint");

    // A second call is a no-op, and registering again starts fresh.
    a.transport.unregister(gone.instance_id);
    a.transport
        .register(PeerInfo::new(gone.instance_id, gone.transport.address()))
        .unwrap();
    ping_message(&a, &gone, &errs).await;
    assert_eq!(eps_open(&a), 2);
    assert_eq!(errs.count(), 0);
    assert!(a.transport.shared.failed_peers.is_empty());

    a.transport.shutdown();
    gone.transport.shutdown();
    stays.transport.shutdown();
}

/// The time `ucp_ep_create` takes is not idle time.
///
/// That call is not always fast. A worker's first `ucp_ep_create` took
/// 110-150 ms on a GB200 node. Across 31 creates in one process during the
/// parallel UCX tests, the median was 630 ms, longer than the 500 ms floor. The
/// new endpoint was stamped from the loop clock read before the call, so it was
/// already past the timeout when it was created, and the scan in the next loop
/// pass (at most one `PARK_MS` later) closed it. That closed an eager endpoint
/// microseconds after it was created, so `eager_wireup_and_the_reaper_compose`
/// never saw it open. The delay seam makes the slow create happen here on
/// demand.
///
/// The endpoint is eager, so no send is posted on it. A send in flight holds
/// its endpoint open by itself, which would hide a stale create stamp.
#[tokio::test(flavor = "multi_thread")]
async fn a_slow_endpoint_create_does_not_age_the_new_endpoint() {
    let a = start_node_with(|b| b.eager_endpoints(true).ep_idle_timeout(Some(IDLE))).await;
    skip_setup(&a);
    let b = start_node().await;
    // Longer than IDLE, the way the measured 630 ms create was. Set before the
    // registration, which is what creates the eager endpoint.
    let delay = IDLE + IDLE / 2;
    a.transport
        .shared
        .ep_create_delay_ms
        .store(delay.as_millis() as u64, Ordering::Relaxed);
    cross_register(&a, &b);

    assert!(
        wait_until(T, || eps_open(&a) == 1 || eps_closed_idle(&a) >= 1).await,
        "eager wireup did not run"
    );
    // With a stale stamp the endpoint is closed by the scan in the same pass,
    // when the create ran in the first drain (the clock is read again after
    // the progress loop), or at the latest by the next pass's scan, at most one
    // `PARK_MS` (100 ms) after the create plus scheduling delay.
    // That broken side is the one that needs slack: a window of IDLE / 2
    // (250 ms) gives it 150 ms. The correct side stays open a full IDLE, so it
    // keeps 250 ms.
    assert!(
        !wait_until(IDLE / 2, || eps_closed_idle(&a) >= 1).await,
        "the endpoint was closed right after it was created: the create time \
         counted as idle time"
    );

    a.transport.shutdown();
    b.transport.shutdown();
    assert_rma_balanced(&a);
    assert_rma_balanced(&b);
}

/// A frame received late in a long pass is stamped when the pass reaches it,
/// not when the pass began.
///
/// The progress thread reads its clock at the top of each pass, then runs
/// `ucp_worker_progress` to quiescence. Under load one progress loop took
/// 526-573 ms. A frame received late in that loop was stamped with the time
/// the pass began, so at the 500 ms floor it was already about a timeout old,
/// and the next pass's scan closed the endpoint it had just used.
///
/// The test makes that pass. `pre_progress_hold` stops A after it reads its
/// pass clock and before it runs `ucp_worker_progress`. B's frame goes into
/// A's socket meanwhile, and A's clock moves three quarters of a timeout. A
/// then receives the frame in the held pass, and the clock moves half a
/// timeout more. With the stale stamp, that scan sees the endpoint a timeout
/// and a quarter old and reaps it, and `reaped_while_active` counts it: the
/// frame is half a timeout old. With a stamp taken after the progress loop,
/// the endpoint is half a timeout old and stays open.
///
/// The step during the hold is less than a timeout on purpose. B's send
/// completing does not prove the frame is in A's socket yet (a corked TCP
/// connection holds it). If the frame arrives only in a later pass, the held
/// pass must not find the endpoint stale: it would reap it, correctly, and
/// fail the test.
#[tokio::test(flavor = "multi_thread")]
async fn a_frame_received_late_in_a_long_pass_is_not_stamped_early() {
    let a = start_node_with(|b| b.ep_idle_timeout(Some(IDLE))).await;
    let b = start_node().await;
    cross_register(&a, &b);
    let errs = CountingErrors::new();
    hold_reaper_clock(&a);

    // A owns an endpoint to B, and B's path back is wired up, so B's next
    // frame goes straight to A's socket and stamps A's endpoint on arrival.
    wire_peer_path_first(&a, &b, &errs).await;

    // Hold A after its pass clock read.
    let shared = &a.transport.shared;
    let held = hold_at(
        &shared.pre_progress_hold,
        &shared.pre_progress_held,
        "after its pass clock read",
    )
    .await;
    // B posts its frame. Once B's thread has handled the send and has nothing
    // in flight, the frame is in A's socket.
    let handled = b.transport.shared.cmds_handled.load(Ordering::Acquire);
    let out = b.transport.send_message(
        a.instance_id,
        Bytes::from_static(b"h"),
        Bytes::from_static(b"p"),
        MessageType::Message,
        errs.clone(),
    );
    assert!(matches!(out, SendOutcome::Admitted));
    assert!(
        wait_until(T, || b
            .transport
            .shared
            .cmds_handled
            .load(Ordering::Acquire)
            > handled
            && b.transport.shared.inflight_ops.load(Ordering::Acquire) == 0)
        .await,
        "B never finished its send"
    );
    // The long pass: the clock moves while A is between its clock read and
    // the progress call. Less than a timeout, so the held pass cannot reap
    // even if the frame reaches A's socket only in a later pass.
    shared.reaper_clock.manual().advance(IDLE * 3 / 4);
    drop(held);
    assert!(
        recv_message(&a.streams.message_stream, T).await.is_some(),
        "B's frame never reached A"
    );
    advance(&a, IDLE / 2).await;

    assert_eq!(
        reaped_while_active(&a),
        0,
        "the endpoint was reaped right after a frame used it: the frame was \
         stamped with the time its pass began"
    );
    assert_eq!(eps_closed_idle(&a), 0);
    assert_eq!(errs.count(), 0);

    a.transport.shutdown();
    b.transport.shutdown();
    assert_rma_balanced(&a);
    assert_rma_balanced(&b);
}

/// A send still in flight keeps its endpoint open, and its completion restarts
/// the idle clock.
///
/// A first send between fresh workers waits while the peer sets up its own
/// endpoint back, inside the peer's `ucp_worker_progress`. In parallel
/// UCX-module runs the peer's progress loop that contained that step took
/// 526-573 ms, so the reaper closed the endpoint with the send in flight: the
/// send failed through `on_error` and the frame was lost.
///
/// B's progress thread is held before A sends, so A's send stays in flight
/// for as long as the test wants. A's clock moves three timeouts meanwhile,
/// and the endpoint must stay open. Then B goes on, the frame arrives, and
/// the send completes. The completion must restart the idle clock: three
/// quarters of a timeout later the endpoint is still open, and a timeout and
/// a quarter later it is reaped. If the admission stamp were the last use,
/// the first scan after the completion would reap it, more than three
/// timeouts after the stamp.
#[tokio::test(flavor = "multi_thread")]
async fn a_send_in_flight_keeps_its_endpoint_open() {
    let a = start_node_with(|b| b.ep_idle_timeout(Some(IDLE))).await;
    skip_setup(&a);
    let b = start_node().await;
    cross_register(&a, &b);
    let errs = CountingErrors::new();
    hold_reaper_clock(&a);
    let held = hold_at(
        &b.transport.shared.progress_hold,
        &b.transport.shared.progress_held,
        "at the top of its pass",
    )
    .await;

    let out = a.transport.send_message(
        b.instance_id,
        Bytes::from_static(b"h"),
        Bytes::from_static(b"p"),
        MessageType::Message,
        errs.clone(),
    );
    assert!(matches!(out, SendOutcome::Admitted));
    assert!(
        wait_until(T, || eps_open(&a) == 1).await,
        "the send did not create an endpoint"
    );
    for _ in 0..6 {
        advance(&a, IDLE / 2).await;
    }
    assert_eq!(
        eps_closed_idle(&a),
        0,
        "the endpoint was closed while its send was in flight"
    );
    assert_eq!(errs.count(), 0);

    drop(held);
    assert!(
        recv_message(&b.streams.message_stream, T).await.is_some(),
        "the frame was lost while its send was in flight"
    );
    assert!(
        wait_until(T, || a
            .transport
            .shared
            .inflight_ops
            .load(Ordering::Acquire)
            == 0)
        .await,
        "A's send never completed"
    );
    // The completion callback lowers `inflight_ops` before it stamps the
    // completion time; let its pass finish before the clock moves.
    settle(&a).await;
    assert_eq!(
        errs.count(),
        0,
        "the send failed: its endpoint was closed while the send was in flight"
    );

    advance(&a, IDLE * 3 / 4).await;
    assert_eq!(
        eps_closed_idle(&a),
        0,
        "the endpoint was closed less than a timeout after its send completed: \
         completion did not restart the idle clock"
    );
    advance(&a, IDLE / 2).await;
    assert_eq!(
        eps_closed_idle(&a),
        1,
        "the endpoint was never closed after its send completed"
    );

    a.transport.shutdown();
    b.transport.shutdown();
    assert_rma_balanced(&a);
    assert_rma_balanced(&b);
}

/// **Measured, and the reason the reaper is off by default.** Closing an idle
/// endpoint disrupts the *peer's* path back to us.
///
/// UCX pairs endpoints by remote worker: our REPLY-flagged Active Messages make
/// UCX create a matching endpoint on the peer, and the peer's own
/// `ucp_ep_create` back to us is then *matched onto that same connection* rather
/// than building a fresh one. After our side closes — FORCE or flush, both were
/// measured — the peer's next REPLY-flagged frame to us is dropped. Why: per
/// the UCX source, a REPLY-flagged frame carries an endpoint id for the reply
/// path, and a frame whose endpoint id no longer resolves is dropped. That is
/// inferred from the source and a measurement, not proven.
///
/// Measured consequences, in order:
///
/// 1. The peer's next Message (a REPLY-flagged frame, as this test sends) to us
///    is admitted and **silently lost**: no `on_error`, no arrival. Re-sending
///    does not help, and neither does our side establishing a fresh endpoint of
///    its own (measured by hand; this test asserts only that the first frame
///    does not arrive). By the same inferred mechanism, its next ping gets no
///    Pong (not measured). Its Responses and Events still arrive
///    (measured separately, not asserted here). Acks carry no REPLY flag
///    either, so the same is expected, but it was not measured.
/// 2. UCX keepalive (default interval ~20 s) eventually declares the peer's
///    endpoint failed, which fires its error handler and populates its
///    `failed_peers`.
/// 3. The frame *after* that goes through velo's existing failed-connection
///    reaping onto a fresh endpoint and arrives normally.
///
/// So it self-heals, but until keepalive fails the peer's endpoint its Messages
/// and pings to us are lost, silently; the first loss is a Message with no
/// error, or (inferred) a ping that times out. That is up to a keepalive
/// interval of disruption per reap — which is a real price to pay for
/// reclaiming an idle connection, and exactly the input D9's "connection-pool
/// policy revisited later" was waiting for.
///
/// This test pins the finding rather than the design intent. The window is short
/// because step 2 cannot happen inside it; if this ever *does* arrive, UCX or
/// velo has fixed the interaction and the caveat in
/// [`UcxTransportBuilder::ep_idle_timeout`] should go with it.
#[tokio::test(flavor = "multi_thread")]
async fn reaping_disrupts_the_peers_path_back() {
    let a = start_node_with(|b| b.ep_idle_timeout(Some(IDLE))).await;
    skip_setup(&a);
    let b = start_node().await;
    cross_register(&a, &b);
    let errs = CountingErrors::new();

    ping_message_to(&a, b.instance_id, &b, &errs, "first send").await;
    assert!(
        wait_until(T, || eps_closed_idle(&a) >= 1).await,
        "the idle endpoint was never reaped"
    );

    let out = b.transport.send_message(
        a.instance_id,
        Bytes::from_static(b"h"),
        Bytes::from_static(b"p"),
        MessageType::Message,
        errs.clone(),
    );
    assert!(
        matches!(out, SendOutcome::Admitted),
        "the peer's send is admitted; the loss is downstream of admission"
    );
    assert!(
        recv_message(&a.streams.message_stream, Duration::from_millis(1500))
            .await
            .is_none(),
        "the peer's first frame after our reap arrived — the UCX endpoint-matching \
         interaction this test pins has been fixed, so update the caveat on \
         UcxTransportBuilder::ep_idle_timeout and delete this test"
    );
    assert_eq!(
        errs.count(),
        0,
        "the loss is silent at this point; keepalive is what eventually reports it"
    );

    a.transport.shutdown();
    b.transport.shutdown();
    assert_rma_balanced(&a);
    assert_rma_balanced(&b);
}

/// Holds a node's progress thread at the top of its pass until dropped.
///
/// Released on drop, so a failed assertion does not leave the thread held and
/// its node's shutdown hung.
struct ProgressHold<'a>(&'a Node);

impl<'a> ProgressHold<'a> {
    async fn new(node: &'a Node) -> Self {
        let shared = &node.transport.shared;
        shared.progress_hold.store(true, Ordering::Release);
        let hold = Self(node);
        assert!(
            wait_until(T, || shared.progress_held.load(Ordering::Acquire)).await,
            "the progress thread never stopped at the hold"
        );
        hold
    }
}

impl Drop for ProgressHold<'_> {
    fn drop(&mut self) {
        self.0
            .transport
            .shared
            .progress_hold
            .store(false, Ordering::Release);
    }
}

/// The in-flight exclusion, isolated: an endpoint old enough to reap is spared
/// purely because [`WorkerState::rma_ops`] still names its peer.
///
/// Four deliberate choices make this neither vacuous nor flaky.
///
/// *Two peers.* The idle one **is** reaped, which is what proves a scan ran past
/// the timeout while the GETs were still outstanding; the busy one is not, and
/// exactly one close between the two is the whole claim. A single-peer version
/// would pass trivially whenever the transfer happened to finish first.
///
/// *The owner's progress thread is held.* Over the tcp lane a GET needs the
/// owner's progress thread, so while `progress_hold` is set no GET can finish.
/// The test holds it from before the GETs are posted until the idle peer is
/// reaped, so the GETs are outstanding at that scan by construction. Timing
/// cannot make this certain: a transfer sized to outlast the idle window
/// finished inside it about 1 run in 125 with 16 copies of the test running at
/// once. The regions are mapped first, because a map is a command the owner's
/// progress thread answers.
///
/// *A sub-floor timeout, set on the config directly.* The builder's floor is
/// sized to dominate endpoint wireup, so that endpoints do not close between
/// ordinary uses and re-pay wireup each time, while the peer loses its Messages
/// and pings to us until keepalive fails its endpoint. Nothing here sends an
/// Active Message, and the idle peer's close is the point of the test, so those
/// costs do not apply, and a short timeout keeps the test short.
///
/// *Eager wireup, no frames.* Both endpoints are established by registration
/// alone, so neither depends on an AM completing.
#[tokio::test(flavor = "multi_thread")]
async fn an_endpoint_with_an_inflight_get_is_not_reaped() {
    const CHUNK: usize = 64 * 1024;
    const CHUNKS: usize = 8;
    const LEN: usize = CHUNK * CHUNKS;
    let mut src = PageBuf::new(LEN);
    let dst = PageBuf::new(LEN);
    src.fill_pattern();

    let puller = start_node_with_config(UcxConfig {
        tls: Some("tcp".into()),
        ep_idle_timeout: Some(Duration::from_millis(10)),
        eager_endpoints: true,
        ..UcxConfig::default()
    })
    .await;
    skip_setup(&puller);
    let owner = start_node().await;
    let idle_peer = start_node().await;
    cross_register(&puller, &owner);

    let owner_rma = owner.transport.rdma_endpoint();
    let puller_rma = puller.transport.rdma_endpoint();

    let remote = owner_rma
        .map_region(src.addr(), LEN)
        .await
        .expect("map src");
    let local = puller_rma
        .map_region(dst.addr(), LEN)
        .await
        .expect("map dst");

    let hold = ProgressHold::new(&owner).await;

    let mut gets = Vec::with_capacity(CHUNKS);
    for i in 0..CHUNKS {
        let endpoint = puller_rma.clone();
        let req = RmaGetRequest {
            peer: owner.instance_id,
            remote_addr: (src.addr() + i * CHUNK) as u64,
            packed_rkey: remote.packed_rkey.clone(),
            local_region: local.region_id,
            local_offset: (i * CHUNK) as u64,
            len: CHUNK as u64,
        };
        gets.push(tokio::spawn(async move { endpoint.get(req).await }));
    }
    let inflight = || puller.transport.shared.inflight_ops.load(Ordering::SeqCst);
    assert!(
        wait_until(T, || inflight() >= CHUNKS).await,
        "all {CHUNKS} GETs must be posted before the idle peer is introduced"
    );

    // A delta, not an absolute: the endpoint eagerly established to the owner
    // may already have been reaped (and recreated by the GETs) while the regions
    // above were being mapped.
    let baseline = eps_closed_idle(&puller);
    // Introduced only now, so its idle window runs entirely inside the hold.
    cross_register(&puller, &idle_peer);

    assert!(
        wait_until(T, || eps_closed_idle(&puller) > baseline).await,
        "no scan closed the idle endpoint"
    );
    let closed = eps_closed_idle(&puller) - baseline;
    assert_eq!(
        inflight(),
        CHUNKS,
        "a GET finished while the owner's progress thread was held, so the hold \
         does not keep a GET in flight and this test proves nothing"
    );
    assert_eq!(
        closed, 1,
        "the endpoint carrying the in-flight GETs was reaped too"
    );
    drop(hold);

    // The backstop for the same property, and the one that would fail loudly if
    // the exclusion were removed: a reaped endpoint cancels its operations.
    for (i, task) in gets.into_iter().enumerate() {
        task.await
            .expect("get task must not panic")
            .unwrap_or_else(|e| panic!("get {i} failed: {e}"));
    }
    assert_eq!(dst.as_slice(), src.as_slice(), "every GET must have landed");

    puller_rma
        .unmap_region(local.region_id)
        .await
        .expect("unmap dst");
    owner_rma
        .unmap_region(remote.region_id)
        .await
        .expect("unmap src");
    puller.transport.shutdown();
    owner.transport.shutdown();
    idle_peer.transport.shutdown();
    assert_rma_balanced(&puller);
    assert_rma_balanced(&owner);
    assert_rma_balanced(&idle_peer);
}

/// The completion of an RDMA GET restarts the idle clock, the same as the
/// completion of a send.
///
/// An outstanding GET keeps its endpoint open (the test above). When the GET
/// completes, its registry entry goes, and if nothing else stamped the
/// endpoint, the last use on record is the moment the GET was posted. A GET
/// slower than the timeout then leaves the endpoint closed at the first scan
/// after it completes. No data is lost, but the next use pays a fresh wireup,
/// and the peer loses its next Messages and pings to us, if it sends any
/// before keepalive fails its endpoint.
///
/// Over the tcp lane a GET needs the owner's progress thread, so stalling that
/// thread holds the GET. The timeout is 2 s, so the scan period is 1 s, and a
/// due scan runs at most one `PARK_MS` (100 ms) late. Without a completion
/// stamp the endpoint closes within 1.1 s of the completion. With it, the
/// endpoint stays open a full 2 s. The bound is 1.5 s, as in
/// `a_send_in_flight_keeps_its_endpoint_open`. The test also checks that the
/// GET was held past the point where the post stamp alone would have let a
/// scan close the endpoint. Without that check, a GET that did not wait on the
/// owner would pass the test for no reason.
#[tokio::test(flavor = "multi_thread")]
async fn an_rma_get_slower_than_the_timeout_restarts_the_idle_clock() {
    const TIMEOUT: Duration = Duration::from_secs(2);
    const STALL: Duration = Duration::from_secs(5);
    const LEN: usize = 64 * 1024;
    let park = Duration::from_millis(PARK_MS as u64);
    let mut src = PageBuf::new(LEN);
    let dst = PageBuf::new(LEN);
    src.fill_pattern();

    let puller = start_node_with(|b| software_rma(b).ep_idle_timeout(Some(TIMEOUT))).await;
    skip_setup(&puller);
    let owner = start_node().await;
    cross_register(&puller, &owner);
    let owner_rma = owner.transport.rdma_endpoint();
    let puller_rma = puller.transport.rdma_endpoint();
    let remote = owner_rma
        .map_region(src.addr(), LEN)
        .await
        .expect("map src");
    let local = puller_rma
        .map_region(dst.addr(), LEN)
        .await
        .expect("map dst");
    let req = RmaGetRequest {
        peer: owner.instance_id,
        remote_addr: src.addr() as u64,
        packed_rkey: remote.packed_rkey.clone(),
        local_region: local.region_id,
        local_offset: 0,
        len: LEN as u64,
    };

    // A warm GET wires the endpoint up, so the timed GET below waits only on
    // the owner.
    tokio::time::timeout(T, puller_rma.get(req.clone()))
        .await
        .expect("warm get must not hang")
        .expect("warm get succeeds");
    assert_eq!(eps_open(&puller), 1);
    assert_eq!(eps_closed_idle(&puller), 0);

    // Stall the owner, and wait until it is asleep in the stall: the seam
    // clears its value just before it sleeps.
    owner
        .transport
        .shared
        .progress_stall_ms
        .store(STALL.as_millis() as u64, Ordering::Relaxed);
    assert!(
        wait_until(T, || owner
            .transport
            .shared
            .progress_stall_ms
            .load(Ordering::Relaxed)
            == 0)
        .await,
        "the owner never entered the stall"
    );

    let posted_at = std::time::Instant::now();
    tokio::time::timeout(STALL + T, puller_rma.get(req))
        .await
        .expect("get must not hang")
        .expect("get succeeds");
    let done_at = std::time::Instant::now();
    let reap_due = TIMEOUT + ep_scan_period(TIMEOUT) + park;
    let held = done_at.duration_since(posted_at);
    assert!(
        held >= reap_due,
        "the GET took only {held:?}; it must outlast {reap_due:?} for this test \
         to prove anything. It did not wait on the owner's progress."
    );
    assert_eq!(
        eps_closed_idle(&puller),
        0,
        "the endpoint was closed while the GET was outstanding"
    );
    assert_eq!(dst.as_slice(), src.as_slice(), "the GET must have landed");

    assert!(
        !wait_until(TIMEOUT * 3 / 4, || eps_closed_idle(&puller) >= 1).await,
        "the endpoint was closed within {:?} of the GET completing: the \
         completion did not restart the idle clock",
        TIMEOUT * 3 / 4
    );
    // And it is closed later: the completion restarts the clock, it does not
    // stop it.
    assert!(
        wait_until(T, || eps_closed_idle(&puller) >= 1).await,
        "the endpoint was never closed after the GET completed"
    );

    puller_rma
        .unmap_region(local.region_id)
        .await
        .expect("unmap dst");
    owner_rma
        .unmap_region(remote.region_id)
        .await
        .expect("unmap src");
    puller.transport.shutdown();
    owner.transport.shutdown();
    assert_rma_balanced(&puller);
    assert_rma_balanced(&owner);
}

/// Teardown racing an idle close: the close may still be parked in
/// `pending_closes` when `shutdown()` arrives, and the endpoint must be
/// accounted for exactly once either way.
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_during_an_idle_close_is_clean() {
    let a = start_node_with(|b| b.ep_idle_timeout(Some(IDLE))).await;
    skip_setup(&a);
    let b = start_node().await;
    cross_register(&a, &b);
    let errs = CountingErrors::new();

    ping_message(&a, &b, &errs).await;
    assert!(
        wait_until(T, || eps_closed_idle(&a) >= 1).await,
        "the idle endpoint was never reaped"
    );

    // Immediately, so teardown lands as close to the close request as this
    // harness can arrange.
    a.transport.shutdown();
    b.transport.shutdown();
    // `assert_rma_balanced` covers the accounting: a double-decrement would
    // have wrapped `eps_open` rather than left it at zero.
    assert_rma_balanced(&a);
    assert_rma_balanced(&b);
}

/// Eager wireup pays the ~14 ms endpoint cost at registration rather than
/// handing it to the first transfer. Off by default, so the peer node proves
/// the default is still lazy.
#[tokio::test(flavor = "multi_thread")]
async fn eager_endpoints_wire_up_at_registration() {
    let eager = start_node_with(|b| b.eager_endpoints(true)).await;
    let lazy = start_node().await;
    cross_register(&eager, &lazy);

    assert!(
        wait_until(T, || eps_open(&eager) == 1).await,
        "registration did not establish an endpoint eagerly"
    );
    // Nothing was sent, so the default side must still have none. Given a moment
    // for the same scheduling the assertion above waited through.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        eps_open(&lazy),
        0,
        "the default wired an endpoint up without being asked to"
    );

    eager.transport.shutdown();
    lazy.transport.shutdown();
    assert_rma_balanced(&eager);
    assert_rma_balanced(&lazy);
}

/// A re-registration under a new incarnation must wire the *replacement* up
/// eagerly too, not leave the peer on an endpoint the incarnation check has
/// already condemned.
#[tokio::test(flavor = "multi_thread")]
async fn eager_wireup_follows_a_re_registration() {
    let eager = start_node_with(|b| b.eager_endpoints(true)).await;
    let first = start_node().await;
    // A second worker address for the same instance: a restarted incarnation.
    let restarted = start_node().await;
    cross_register(&eager, &first);

    assert!(
        wait_until(T, || eps_open(&eager) == 1).await,
        "the first registration did not wire up"
    );

    eager
        .transport
        .register(PeerInfo::new(
            first.instance_id,
            restarted.transport.address(),
        ))
        .expect("re-register under a new incarnation");

    // The superseded endpoint is parked and closed at the next safe point, so
    // the count dips through 2 and settles at 1 — a fresh endpoint for the new
    // incarnation, established without anybody sending anything.
    assert!(
        wait_until(T, || eps_open(&eager) == 1
            && eps_closed_idle(&eager) == 0
            && eager.transport.shared.failed_peers.is_empty())
        .await,
        "the re-registered peer did not end up on exactly one fresh endpoint"
    );

    // And it works: the instance id is unchanged, but the frame arrives at the
    // restarted worker rather than the original one.
    let errs = CountingErrors::new();
    ping_message_to(
        &eager,
        first.instance_id,
        &restarted,
        &errs,
        "re-registered",
    )
    .await;
    assert_eq!(errs.count(), 0);

    eager.transport.shutdown();
    first.transport.shutdown();
    restarted.transport.shutdown();
    assert_rma_balanced(&eager);
    assert_rma_balanced(&first);
    assert_rma_balanced(&restarted);
}

/// Eager wireup and the idle reaper together, which is the combination the
/// builder docs promise composes: an endpoint established at registration and
/// never used is reclaimed about one timeout later (see the close window), and
/// a use after that wires up a new one. Intended behaviour, asserted so it
/// stays intended.
#[tokio::test(flavor = "multi_thread")]
async fn eager_wireup_and_the_reaper_compose() {
    let a = start_node_with(|b| b.eager_endpoints(true).ep_idle_timeout(Some(IDLE))).await;
    skip_setup(&a);
    let b = start_node().await;
    cross_register(&a, &b);

    assert!(
        wait_until(T, || eps_open(&a) == 1).await,
        "eager wireup did not run"
    );
    assert!(
        wait_until(T, || eps_closed_idle(&a) >= 1 && eps_open(&a) == 0).await,
        "an eagerly established but unused endpoint was never reclaimed"
    );

    let errs = CountingErrors::new();
    ping_message(&a, &b, &errs).await;
    assert_eq!(errs.count(), 0, "the send after the reap failed");

    a.transport.shutdown();
    b.transport.shutdown();
    assert_rma_balanced(&a);
    assert_rma_balanced(&b);
}

/// Register / transfer / release, repeatedly, asserting the transport-side
/// lifecycle counters return to where they started every cycle.
///
/// The transport-layer soak: what it can prove that a
/// single round trip cannot is that nothing *accumulates* — not registrations,
/// not unpacked keys, not endpoints. The registration-layer half of the same
/// property (registered bytes returning to baseline) lives in
/// `rendezvous::rdma`'s tests, over a mock backend where the cycle count can be
/// much higher.
#[tokio::test(flavor = "multi_thread")]
async fn rma_lifecycle_soak() {
    const CYCLES: usize = 24;
    const LEN: usize = 256 * 1024;
    let mut src = PageBuf::new(LEN);
    let dst = PageBuf::new(LEN);
    src.fill_pattern();

    let pair = start_rma_pair().await;

    for cycle in 0..CYCLES {
        let remote = pair
            .owner_rma
            .map_region(src.addr(), LEN)
            .await
            .unwrap_or_else(|e| panic!("cycle {cycle}: map src: {e}"));
        let local = pair
            .puller_rma
            .map_region(dst.addr(), LEN)
            .await
            .unwrap_or_else(|e| panic!("cycle {cycle}: map dst: {e}"));

        tokio::time::timeout(
            T,
            pair.puller_rma
                .get(get_request(&pair, &src, &remote, &local)),
        )
        .await
        .unwrap_or_else(|_| panic!("cycle {cycle}: get hung"))
        .unwrap_or_else(|e| panic!("cycle {cycle}: get failed: {e}"));
        assert_eq!(dst.as_slice(), src.as_slice(), "cycle {cycle}: bad data");

        pair.puller_rma
            .unmap_region(local.region_id)
            .await
            .unwrap_or_else(|e| panic!("cycle {cycle}: unmap dst: {e}"));
        pair.owner_rma
            .unmap_region(remote.region_id)
            .await
            .unwrap_or_else(|e| panic!("cycle {cycle}: unmap src: {e}"));

        // The invariants, every cycle rather than only at the end: a leak that
        // grows by one per cycle and a leak that appears once are different
        // bugs, and only a per-cycle assertion tells them apart.
        assert_eq!(
            pair.puller
                .transport
                .shared
                .live_regions
                .load(Ordering::SeqCst),
            0,
            "cycle {cycle}: a destination region survived its unmap"
        );
        assert_eq!(
            pair.owner
                .transport
                .shared
                .live_regions
                .load(Ordering::SeqCst),
            0,
            "cycle {cycle}: a source region survived its unmap"
        );
        assert_eq!(
            pair.puller
                .transport
                .shared
                .live_rkeys
                .load(Ordering::SeqCst),
            0,
            "cycle {cycle}: an unpacked rkey survived its operation"
        );
        // One endpoint per direction, established on the first cycle and reused
        // by every one after it — the reaper is off, so this must not move.
        assert_eq!(
            eps_open(&pair.puller),
            1,
            "cycle {cycle}: the puller's endpoint count drifted"
        );
    }

    pair.owner.transport.shutdown();
    pair.puller.transport.shutdown();
    assert_pair_balanced(&pair);
}

/// Registration and GET cost over the tcp lane. Numbers feed the Phase-3
/// threshold defaults; run with
/// `cargo test --features ucx -p velo bench_rma -- --ignored --nocapture`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark: prints timings, asserts nothing"]
async fn bench_rma() {
    const MAP_SIZES: [usize; 3] = [4 * 1024, 1024 * 1024, 64 * 1024 * 1024];
    const GET_SIZES: [usize; 3] = [64 * 1024, 1024 * 1024, 16 * 1024 * 1024];
    let largest = *MAP_SIZES.iter().max().unwrap();

    let mut src = PageBuf::new(largest);
    let dst = PageBuf::new(largest);
    src.fill_pattern();

    let pair = start_rma_pair().await;

    println!("-- ucp_mem_map latency (UCX_TLS=tcp) --");
    for len in MAP_SIZES {
        let started = std::time::Instant::now();
        let region = pair.owner_rma.map_region(src.addr(), len).await.unwrap();
        let mapped = started.elapsed();
        let started = std::time::Instant::now();
        pair.owner_rma.unmap_region(region.region_id).await.unwrap();
        println!(
            "  {:>9} B: map {:>10.3?}  unmap {:>10.3?}  rkey {} B",
            len,
            mapped,
            started.elapsed(),
            region.packed_rkey.len()
        );
    }

    let remote = pair
        .owner_rma
        .map_region(src.addr(), largest)
        .await
        .unwrap();
    let local = pair
        .puller_rma
        .map_region(dst.addr(), largest)
        .await
        .unwrap();
    println!("-- ucp_get_nbx latency (UCX_TLS=tcp) --");
    for len in GET_SIZES {
        // One warm-up, then three timed passes.
        for round in 0..4 {
            let started = std::time::Instant::now();
            pair.puller_rma
                .get(RmaGetRequest {
                    peer: pair.owner.instance_id,
                    remote_addr: src.addr() as u64,
                    packed_rkey: remote.packed_rkey.clone(),
                    local_region: local.region_id,
                    local_offset: 0,
                    len: len as u64,
                })
                .await
                .expect("get succeeds");
            let elapsed = started.elapsed();
            if round > 0 {
                let mib = len as f64 / (1024.0 * 1024.0);
                println!(
                    "  {:>9} B: {:>10.3?}  ({:.0} MiB/s)",
                    len,
                    elapsed,
                    mib / elapsed.as_secs_f64()
                );
            }
        }
    }

    pair.puller_rma.unmap_region(local.region_id).await.unwrap();
    pair.owner_rma.unmap_region(remote.region_id).await.unwrap();
    pair.owner.transport.shutdown();
    pair.puller.transport.shutdown();
    assert_pair_balanced(&pair);
}

/// Active Message send cost over the tcp lane, with the idle reaper off and on.
///
/// The reaper adds per-send bookkeeping, so the two rows are the price of that
/// bookkeeping. Three measures: a one-way burst of Messages (the progress
/// thread's per-send cost is on its critical path), the same burst of
/// Responses, and a ping-pong round trip. Responses are measured apart because
/// velo sends Messages with UCX's REPLY flag and Responses without it, and a
/// change to what a frame carries shows only in the kind it changes. Run with
/// `timeout 300 cargo test --release --features ucx -p velo --lib bench_am_send -- --ignored --nocapture --test-threads=1`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark: prints timings, asserts nothing"]
async fn bench_am_send() {
    const BURST: usize = 200_000;
    const ROUND_TRIPS: usize = 20_000;
    const WARMUP: usize = 2_000;
    let header = Bytes::from(vec![7u8; 64]);
    let payload = Bytes::from(vec![9u8; 64]);

    for (label, timeout) in [
        ("reaper off", None),
        ("reaper on ", Some(Duration::from_secs(3600))),
    ] {
        let a = start_node_with(|b| b.ep_idle_timeout(timeout)).await;
        let b = start_node_with(|b| b.ep_idle_timeout(timeout)).await;
        cross_register(&a, &b);
        let errs = CountingErrors::new();
        let send_kind = |from: &Node, to: &Node, kind: MessageType| {
            let _ = from.transport.send_message(
                to.instance_id,
                header.clone(),
                payload.clone(),
                kind,
                errs.clone(),
            );
        };
        let send = |from: &Node, to: &Node| send_kind(from, to, MessageType::Message);

        for _ in 0..WARMUP {
            send(&a, &b);
            recv_message(&b.streams.message_stream, T).await.unwrap();
            send(&b, &a);
            recv_message(&a.streams.message_stream, T).await.unwrap();
        }

        let started = std::time::Instant::now();
        for _ in 0..BURST {
            send(&a, &b);
        }
        for _ in 0..BURST {
            recv_message(&b.streams.message_stream, T).await.unwrap();
        }
        let burst = started.elapsed();

        let started = std::time::Instant::now();
        for _ in 0..BURST {
            send_kind(&a, &b, MessageType::Response);
        }
        for _ in 0..BURST {
            recv(&b.streams.response_stream, T).await.unwrap();
        }
        let resp_burst = started.elapsed();

        let mut rtts = Vec::with_capacity(ROUND_TRIPS);
        for _ in 0..ROUND_TRIPS {
            let started = std::time::Instant::now();
            send(&a, &b);
            recv_message(&b.streams.message_stream, T).await.unwrap();
            send(&b, &a);
            recv_message(&a.streams.message_stream, T).await.unwrap();
            rtts.push(started.elapsed());
        }
        rtts.sort();
        println!(
            "bench_am_send {label}: burst {:>7.1} ns/frame  resp burst {:>7.1} ns/frame  rtt p50 {:>9.3?} p99 {:>9.3?}  errors {}",
            burst.as_nanos() as f64 / BURST as f64,
            resp_burst.as_nanos() as f64 / BURST as f64,
            rtts[ROUND_TRIPS / 2],
            rtts[ROUND_TRIPS * 99 / 100],
            errs.count()
        );

        a.transport.shutdown();
        b.transport.shutdown();
        assert_rma_balanced(&a);
        assert_rma_balanced(&b);
    }
}
