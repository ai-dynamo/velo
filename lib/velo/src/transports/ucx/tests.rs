// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! In-process loopback tests for the UCX transport.
//!
//! Two independent `ucp_context`s in one process, wired over the `tcp` lane:
//! with `UCP_ERR_HANDLING_MODE_PEER` the shm lanes are ineligible (no peer
//! failure handler), so tcp is the deterministic choice — and the exact code
//! path CI runs without RDMA hardware.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;

use super::{UcxTransport, UcxTransportBuilder};
use crate::transports::transport::{
    DataStreams, HealthCheckError, SendOutcome, Transport, TransportErrorHandler, make_channels,
};
// `super` here is the `transport` module (this file is `#[path]`-included from
// it), so the sibling `rma` module needs its full path.
use crate::transports::ucx::rma::{
    MAX_PACKED_RKEY, MappedRegion, RdmaEndpoint, RmaError, RmaGetRequest, SYS_DEV_UNKNOWN,
    preparse_packed_rkey,
};
use crate::transports::ucx::worker::Cmd;
use velo_ext::{InstanceId, MessageType, PeerInfo};

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
    let transport = Arc::new(
        UcxTransportBuilder::new()
            .tls("tcp")
            .build()
            .expect("build ucx transport"),
    );
    let instance_id = InstanceId::new_v4();
    let (adapter, streams) = make_channels();
    tokio::time::timeout(
        T,
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

// ---------------------------------------------------------------------------
// RMA
// ---------------------------------------------------------------------------

/// A page-aligned heap allocation owned by the test frame.
///
/// Registered memory must stay allocated for as long as UCX has it pinned.
/// Phase 2's arena pool owns that concern for real callers; here every test
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

async fn start_rma_pair() -> RmaPair {
    let owner = start_node().await;
    let puller = start_node().await;
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
}

fn assert_pair_balanced(pair: &RmaPair) {
    assert_rma_balanced(&pair.owner);
    assert_rma_balanced(&pair.puller);
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
/// InfiniBand packs a key per MD on top of that, which only the hardware
/// checkpoint can observe. The `>= 9` floor is therefore the tightest bound CI
/// can assert.
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
/// Measured over the tcp lane the transfer wins this race and the GET resolves
/// `Ok` from teardown's flush-close progress loop, so the *cancellation* path is
/// covered by [`get_cancelled_by_endpoint_replacement`] instead. What this test
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

/// A blob that passes the pre-parse and is still refused by UCX.
///
/// The complement of [`truncated_rkey_is_refused_before_ucx`]: this one is well
/// formed — `md_map` names one memory domain, the entry is present, `sys_dev` is
/// `UNKNOWN` — so the pre-parse lets it through and `ucp_ep_rkey_unpack` really
/// runs, walking only bytes the blob owns. UCX rejects it because no local
/// memory domain corresponds (it logs `failed to unpack remote key from remote
/// md[0]`). What this pins down is the accounting on that branch: `live_rkeys`
/// is incremented only after a successful unpack, so a failed one must leave it
/// untouched rather than counting a key that was never created.
#[tokio::test(flavor = "multi_thread")]
async fn unusable_rkey_fails_cleanly_inside_ucx() {
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
    let mut pending = Box::pin(rma.map_region(buf.addr(), LEN));
    assert!(
        futures::poll!(pending.as_mut()).is_pending(),
        "the first poll should submit and then await the reply"
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
/// backstop for a peer that stops progressing without closing — a state this
/// harness cannot produce, and a hardware-checkpoint item.
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
