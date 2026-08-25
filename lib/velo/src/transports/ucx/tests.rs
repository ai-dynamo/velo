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
use crate::transports::ucx::rma::{MappedRegion, RdmaEndpoint, RmaError, RmaGetRequest};
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

/// Push an unmap straight onto the ring, bypassing the submit-side range table.
async fn ring_unmap(transport: &UcxTransport, region_id: u64) -> Result<(), RmaError> {
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
    tokio::time::timeout(T, rx)
        .await
        .expect("worker must answer")
        .expect("worker must not drop the reply")
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

/// A truncated but plausibly-sized blob reaches `ucp_ep_rkey_unpack`.
///
/// This is the case `MAX_PACKED_RKEY` cannot catch: nine bytes is exactly what a
/// real tcp-lane rkey looks like, so the length check passes and UCX parses it.
/// With `md_map` bits set and no bytes behind them, only the zero padding
/// `prepare_get` unpacks from keeps the walk inside our own allocation. The
/// assertion is simply that the process survives and the caller is answered.
#[tokio::test(flavor = "multi_thread")]
async fn worker_survives_truncated_rkey() {
    const LEN: usize = 64 * 1024;
    let src = PageBuf::new(LEN);
    let dst = PageBuf::new(LEN);

    let pair = start_rma_pair().await;
    let remote = pair.owner_rma.map_region(src.addr(), LEN).await.unwrap();
    let local = pair.puller_rma.map_region(dst.addr(), LEN).await.unwrap();

    // `md_map` = every bit set, memory type 0, and nothing behind it.
    let mut hostile = vec![0xFFu8; 8];
    hostile.push(0);
    let mut req = get_request(&pair, &src, &remote, &local);
    req.packed_rkey = Bytes::from(hostile);
    req.len = 64;
    let outcome = ring_get(&pair.puller.transport, req).await;
    println!("truncated-rkey GET resolved as {outcome:?}");

    // Whatever UCX made of it, the accounting has to balance.
    assert!(
        wait_until(T, || pair
            .puller
            .transport
            .shared
            .live_rkeys
            .load(Ordering::SeqCst)
            == 0)
        .await,
        "the rkey unpacked for a refused GET must still be destroyed"
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

    // Parks behind the GET, then the caller walks away.
    let cancelled = tokio::time::timeout(
        Duration::from_millis(1),
        pair.puller_rma.unmap_region(local.region_id),
    )
    .await;
    assert!(cancelled.is_err(), "the unmap must park, not resolve");

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

    // Pushed through the ring so `shutdown()` cannot refuse it on the way in:
    // the point is a waiter parked *inside* the progress thread when teardown
    // starts.
    let puller = Arc::clone(&pair.puller.transport);
    let region_id = local.region_id;
    let unmap = tokio::spawn(async move { ring_unmap(&puller, region_id).await });
    tokio::time::sleep(Duration::from_millis(5)).await;

    pair.puller.transport.shutdown();

    let unmap_outcome = tokio::time::timeout(T, unmap)
        .await
        .expect("the parked unmap must resolve, not hang")
        .expect("unmap task");
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
