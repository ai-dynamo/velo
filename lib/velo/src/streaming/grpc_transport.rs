// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! gRPC-based [`FrameTransport`] backed by the VeloStreaming proto service.
//!
//! Hosts a single shared tonic server per instance with per-(anchor, session)
//! routing via [`dashmap::DashMap`]. Each `bind()` call registers a routing
//! slot keyed by `(anchor_id, session_id)`; each `connect()` call opens a
//! bidirectional gRPC stream and routes frames to the bound receiver.
//!
//! Endpoint resolution: there is no endpoint string in the streaming attach
//! handshake. The transport advertises its listener interface(s) via
//! [`Self::address`]. Peers are registered via [`Self::register`], which
//! caches a [`SocketAddr`] keyed by [`WorkerId`]. [`Self::connect`] looks up
//! the cached SocketAddr and dials it.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use dashmap::DashMap;
use futures::StreamExt;
use futures::future::BoxFuture;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, Streaming};
use velo_ext::{PeerInfo, TransportKey, WorkerAddress, WorkerId};

use crate::streaming::transport::FrameTransport;
use crate::transports::address::WorkerAddressBuilder;
use crate::transports::utils::interfaces::{
    InterfaceEndpoint, InterfaceFilter, parse_endpoints, resolve_advertise_endpoints,
    select_best_endpoint,
};

// ---------------------------------------------------------------------------
// Proto-generated code
// ---------------------------------------------------------------------------

pub(crate) mod proto {
    tonic::include_proto!("velo.streaming.v1");
}

use proto::{
    FramedData,
    velo_streaming_client::VeloStreamingClient,
    velo_streaming_server::{VeloStreaming, VeloStreamingServer},
};

/// Default streaming-transport key for gRPC. Matches the `tcp-stream`
/// convention so streaming entries don't collide with messenger entries in
/// the WorkerAddress map.
pub const GRPC_STREAM_KEY: &str = "grpc-stream";

const ANCHOR_ID_META: &str = "x-anchor-id";
const SESSION_ID_META: &str = "x-session-id";

/// How long the client-side frame pump waits, after half-closing the request
/// stream, for the server to end its response — the acknowledgement that every
/// frame (including the terminal sentinel) was handed to the consumer.
///
/// Sized above the consumer's own liveness budget
/// (`DETECTION_MULTIPLIER * heartbeat_interval`, 15s at the protocol defaults)
/// so the heartbeat watchdog is what reports a wedged consumer, not this.
const TERMINAL_ACK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

// ---------------------------------------------------------------------------
// Terminal sentinel check (module-private)
// ---------------------------------------------------------------------------

fn is_terminal_sentinel(bytes: &[u8]) -> bool {
    use crate::streaming::sender::{cached_detached, cached_dropped, cached_finalized};

    if bytes == cached_dropped().as_slice()
        || bytes == cached_detached().as_slice()
        || bytes == cached_finalized().as_slice()
    {
        return true;
    }

    if let Ok(frame) = rmp_serde::from_slice::<crate::streaming::frame::StreamFrame<()>>(bytes) {
        matches!(
            frame,
            crate::streaming::frame::StreamFrame::TransportError(_)
        )
    } else {
        false
    }
}

// ---------------------------------------------------------------------------
// GrpcStreamingService (server-side handler)
// ---------------------------------------------------------------------------

/// Routing key: (anchor_id, session_id) -> per-stream Sender.
type SessionRouting = DashMap<(u64, u64), flume::Sender<Vec<u8>>>;

#[derive(Clone)]
struct GrpcStreamingService {
    routing: Arc<SessionRouting>,
    metrics: Arc<std::sync::OnceLock<Arc<crate::observability::VeloMetrics>>>,
}

#[tonic::async_trait]
impl VeloStreaming for GrpcStreamingService {
    type StreamStream =
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<FramedData, Status>> + Send + 'static>>;

    async fn stream(
        &self,
        request: Request<Streaming<FramedData>>,
    ) -> Result<Response<Self::StreamStream>, Status> {
        let anchor_id = read_u64_meta(&request, ANCHOR_ID_META).map_err(|boxed| *boxed)?;
        let session_id = read_u64_meta(&request, SESSION_ID_META).map_err(|boxed| *boxed)?;

        let frame_tx = match self.routing.remove(&(anchor_id, session_id)) {
            Some((_, tx)) => tx,
            None => {
                return Err(Status::not_found(format!(
                    "no routing slot for (anchor_id={}, session_id={})",
                    anchor_id, session_id
                )));
            }
        };

        let mut stream = request.into_inner();
        let metrics = self.metrics.get().cloned();

        // The pump stays on its own task so that an abrupt client disconnect
        // (RST_STREAM, killed peer) still runs the `Dropped` injection below —
        // if it lived inside the response body it would simply be dropped
        // mid-poll and the consumer would be left waiting on the 15s heartbeat
        // watchdog instead.
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            let mut last_was_terminal = false;
            let mut frames_seen = 0u64;
            let mut end_reason = "end-of-stream";
            while let Some(result) = stream.next().await {
                match result {
                    Ok(framed) => {
                        frames_seen += 1;
                        let payload = framed.payload;
                        last_was_terminal = is_terminal_sentinel(&payload);
                        // Try non-blocking first so we can record server-pump
                        // backpressure on the slow path before falling through
                        // to send_async. The bind-side frame channel is
                        // bounded(4096); it begins to fill once reader_pump
                        // has saturated the per-anchor channel above it.
                        match frame_tx.try_send(payload) {
                            Ok(()) => {}
                            Err(flume::TrySendError::Full(b)) => {
                                if let Some(m) = metrics.as_ref() {
                                    m.record_server_pump_backpressure();
                                }
                                if frame_tx.send_async(b).await.is_err() {
                                    return; // consumer dropped, no Dropped injection
                                }
                            }
                            Err(flume::TrySendError::Disconnected(_)) => {
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        end_reason = "recv-error";
                        tracing::warn!(
                            "gRPC streaming recv error for anchor={} session={}: {}",
                            anchor_id,
                            session_id,
                            e
                        );
                        break;
                    }
                }
            }
            if !last_was_terminal {
                // Mirrors the TCP server pump's warning. Without it the only
                // trace of a lost terminal sentinel is a bare `SenderDropped`
                // surfacing to the consumer with no way to tell it apart from
                // the heartbeat watchdog or a genuine producer crash.
                tracing::warn!(
                    anchor_id,
                    session_id,
                    frames_seen,
                    end_reason,
                    "gRPC server pump: last frame was not a terminal sentinel, injecting Dropped"
                );
                let _ = frame_tx
                    .send_async(crate::streaming::sender::cached_dropped().clone())
                    .await;
            }
            // Release the client's post-terminal drain (see `connect`). This
            // fires only after every request frame has been handed to the
            // consumer's channel, so the client observing end-of-response is a
            // genuine delivery acknowledgement.
            let _ = done_tx.send(());
        });

        // The response carries no items; it exists purely so that its
        // completion (the gRPC trailers) signals "request stream fully
        // consumed". Returning an already-empty stream here would let tonic
        // send the trailers before the request body was read, which is what
        // made the client's teardown race the delivery of its last frames.
        let response_stream = futures::stream::once(async move {
            let _ = done_rx.await;
        })
        .filter_map(|()| std::future::ready(None::<Result<FramedData, Status>>));

        Ok(Response::new(Box::pin(response_stream)))
    }
}

fn read_u64_meta<T>(request: &Request<T>, name: &str) -> Result<u64, Box<Status>> {
    let meta = request.metadata().get(name).ok_or_else(|| {
        Box::new(Status::invalid_argument(format!(
            "missing {} metadata header",
            name
        )))
    })?;
    let s = meta.to_str().map_err(|_| {
        Box::new(Status::invalid_argument(format!(
            "{} metadata is not valid UTF-8",
            name
        )))
    })?;
    s.parse::<u64>().map_err(|_| {
        Box::new(Status::invalid_argument(format!(
            "{} is not a valid u64",
            name
        )))
    })
}

// ---------------------------------------------------------------------------
// GrpcFrameTransport (public API)
// ---------------------------------------------------------------------------

/// gRPC-based [`FrameTransport`] providing per-(anchor, session) routing via
/// a shared tonic server.
pub struct GrpcFrameTransport {
    key: TransportKey,
    bind_addr: SocketAddr,
    local_address: WorkerAddress,
    local_interfaces: std::sync::OnceLock<Vec<InterfaceEndpoint>>,
    interface_filter: InterfaceFilter,
    numa_hint: Option<u32>,
    peers: Arc<DashMap<WorkerId, SocketAddr>>,
    routing: Arc<SessionRouting>,
    cancel: CancellationToken,
    /// Optional metrics handle. Set once by the Velo builder via
    /// [`Self::set_metrics`] before any bind/connect.
    metrics: Arc<std::sync::OnceLock<Arc<crate::observability::VeloMetrics>>>,
}

impl GrpcFrameTransport {
    /// Construct with a custom transport key, interface filter, and NUMA hint.
    pub async fn with_config(
        bind_addr: SocketAddr,
        key: TransportKey,
        interface_filter: InterfaceFilter,
        numa_hint: Option<u32>,
    ) -> Result<Arc<Self>> {
        let routing: Arc<SessionRouting> = Arc::new(DashMap::new());
        let cancel = CancellationToken::new();
        let metrics: Arc<std::sync::OnceLock<Arc<crate::observability::VeloMetrics>>> =
            Arc::new(std::sync::OnceLock::new());

        let listener = tokio::net::TcpListener::bind(bind_addr).await?;
        let bound_addr = listener.local_addr()?;

        let endpoints = resolve_advertise_endpoints(bound_addr, &interface_filter)?;
        let encoded = rmp_serde::to_vec(&endpoints)
            .map_err(|e| anyhow!("Failed to encode interface endpoints: {e}"))?;
        let mut addr_builder = WorkerAddressBuilder::new();
        addr_builder
            .add_entry(key.as_str(), encoded)
            .map_err(|e| anyhow!("Failed to build WorkerAddress entry: {e}"))?;
        let local_address = addr_builder
            .build()
            .map_err(|e| anyhow!("Failed to build WorkerAddress: {e}"))?;

        let service = GrpcStreamingService {
            routing: routing.clone(),
            metrics: metrics.clone(),
        };

        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            let server =
                tonic::transport::Server::builder().add_service(VeloStreamingServer::new(service));
            if let Err(e) = server
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(listener),
                    cancel_clone.cancelled(),
                )
                .await
            {
                tracing::warn!("GrpcFrameTransport server error: {}", e);
            }
        });

        Ok(Arc::new(Self {
            key,
            bind_addr: bound_addr,
            local_address,
            local_interfaces: std::sync::OnceLock::new(),
            interface_filter,
            numa_hint,
            peers: Arc::new(DashMap::new()),
            routing,
            cancel,
            metrics,
        }))
    }

    /// Install a metrics handle. Called by the Velo builder before any
    /// `bind`/`connect`. No-op if already set.
    pub(crate) fn set_metrics(&self, metrics: Arc<crate::observability::VeloMetrics>) {
        let _ = self.metrics.set(metrics);
    }

    /// Convenience constructor: `grpc-stream` key, `InterfaceFilter::All`,
    /// no NUMA hint, bound on the given IP at port 0.
    pub async fn new(bind_addr: SocketAddr) -> Result<Arc<Self>> {
        Self::with_config(
            bind_addr,
            TransportKey::new(GRPC_STREAM_KEY),
            InterfaceFilter::All,
            None,
        )
        .await
    }

    /// Convenience: bind on `0.0.0.0:0`.
    pub async fn default_new() -> Result<Arc<Self>> {
        Self::new("0.0.0.0:0".parse().unwrap()).await
    }

    pub fn bound_addr(&self) -> SocketAddr {
        self.bind_addr
    }
}

impl Drop for GrpcFrameTransport {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

impl FrameTransport for GrpcFrameTransport {
    fn key(&self) -> TransportKey {
        self.key.clone()
    }

    fn address(&self) -> WorkerAddress {
        self.local_address.clone()
    }

    fn register(&self, peer_info: &PeerInfo) -> Result<()> {
        let raw = peer_info
            .worker_address()
            .get_entry(self.key.as_str())
            .map_err(|e| anyhow!("decoding peer WorkerAddress: {e}"))?
            .ok_or_else(|| {
                anyhow!(
                    "peer {} has no '{}' streaming endpoint entry",
                    peer_info.worker_id(),
                    self.key
                )
            })?;

        let remote_endpoints =
            parse_endpoints(&raw).map_err(|e| anyhow!("Failed to parse gRPC endpoints: {e}"))?;

        let local = self.local_interfaces.get_or_init(|| {
            resolve_advertise_endpoints(self.bind_addr, &self.interface_filter).unwrap_or_default()
        });

        let addr =
            select_best_endpoint(&remote_endpoints, local, self.numa_hint).ok_or_else(|| {
                anyhow!(
                    "no suitable endpoint for peer {} from {:?}",
                    peer_info.worker_id(),
                    remote_endpoints
                )
            })?;

        self.peers.insert(peer_info.worker_id(), addr);
        Ok(())
    }

    fn bind(
        &self,
        anchor_id: u64,
        session_id: u64,
    ) -> BoxFuture<'_, Result<flume::Receiver<Vec<u8>>>> {
        let routing = self.routing.clone();
        Box::pin(async move {
            let (frame_tx, frame_rx) = flume::bounded::<Vec<u8>>(4096);
            if routing.insert((anchor_id, session_id), frame_tx).is_some() {
                tracing::warn!(
                    anchor_id,
                    session_id,
                    "GrpcFrameTransport::bind overwrote an existing routing entry; \
                     previous frame_tx dropped (consumer will see channel close)"
                );
            }
            Ok(frame_rx)
        })
    }

    fn connect(
        &self,
        peer: WorkerId,
        anchor_id: u64,
        session_id: u64,
    ) -> BoxFuture<'_, Result<flume::Sender<Vec<u8>>>> {
        let peers = self.peers.clone();
        Box::pin(async move {
            let addr = *peers.get(&peer).ok_or_else(|| {
                anyhow!(
                    "gRPC streaming: peer {} not registered (call register_peer first)",
                    peer
                )
            })?;

            let channel = tonic::transport::Channel::from_shared(format!("http://{}", addr))?
                .connect()
                .await?;

            let mut client = VeloStreamingClient::new(channel);

            let (mpsc_tx, mpsc_rx) = tokio::sync::mpsc::channel::<FramedData>(256);
            let (frame_tx, frame_rx) = flume::bounded::<Vec<u8>>(4096);

            let request_stream = tokio_stream::wrappers::ReceiverStream::new(mpsc_rx);

            let mut request = Request::new(request_stream);
            request.metadata_mut().insert(
                ANCHOR_ID_META,
                anchor_id
                    .to_string()
                    .parse()
                    .map_err(|_| anyhow!("failed to encode anchor_id as metadata"))?,
            );
            request.metadata_mut().insert(
                SESSION_ID_META,
                session_id
                    .to_string()
                    .parse()
                    .map_err(|_| anyhow!("failed to encode session_id as metadata"))?,
            );

            let response = client
                .stream(request)
                .await
                .map_err(|status| anyhow!("gRPC stream rejected: {}", status))?;

            tokio::spawn(async move {
                // Hold the inbound half (and thus the underlying H2 bidi call)
                // for as long as we are pumping frames; dropping it sends
                // RST_STREAM and cancels the call.
                let mut inbound = response.into_inner();
                while let Ok(payload) = frame_rx.recv_async().await {
                    let is_terminal = is_terminal_sentinel(&payload);
                    let framed = FramedData {
                        preamble: vec![],
                        header: vec![],
                        payload,
                    };
                    if mpsc_tx.send(framed).await.is_err() {
                        break;
                    }
                    // Mirror TCP: after pushing a terminal sentinel (Finalized
                    // / Dropped / Detached / TransportError), exit the pump.
                    // Any frame still queued behind the terminal (e.g. a
                    // heartbeat that snuck through between cancel and drop)
                    // would otherwise reach the server and be interpreted as
                    // the new last frame, causing the server to inject Dropped
                    // on close -- which kills a reattached session.
                    if is_terminal {
                        break;
                    }
                }

                // Half-close the request stream: this is the gRPC equivalent of
                // the TCP pump's `shutdown()` and is what makes the server's
                // `stream.next()` return `None`.
                drop(mpsc_tx);

                // Then wait for the server to end its response. `mpsc_tx.send`
                // only queues into a 256-slot channel — it says nothing about
                // whether the bytes reached the wire — and tearing the call
                // down here cancels the H2 stream, discarding whatever is still
                // buffered. On a short stream this routinely threw away *every*
                // frame including the terminal sentinel, leaving the server to
                // inject `Dropped` and the consumer to fail with
                // `StreamError::SenderDropped` even though the producer had
                // finalized cleanly. The server completes its response only
                // after consuming the whole request stream, so end-of-response
                // is the acknowledgement that the terminal sentinel landed.
                let drain = async {
                    while let Some(next) = inbound.next().await {
                        if let Err(status) = next {
                            tracing::debug!(
                                anchor_id,
                                session_id,
                                %status,
                                "gRPC streaming: error draining response after terminal sentinel"
                            );
                            break;
                        }
                    }
                };
                if tokio::time::timeout(TERMINAL_ACK_TIMEOUT, drain)
                    .await
                    .is_err()
                {
                    // Only reachable when the consumer has wedged its own
                    // channel: the server pump is blocked forwarding a frame,
                    // so it never completes the call. Give up rather than leak
                    // the task; the consumer's heartbeat watchdog is the
                    // backstop from here.
                    tracing::warn!(
                        anchor_id,
                        session_id,
                        timeout_ms = TERMINAL_ACK_TIMEOUT.as_millis() as u64,
                        "gRPC streaming: timed out waiting for the server to acknowledge the \
                         terminal sentinel"
                    );
                }
            });

            Ok(frame_tx)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use velo_ext::InstanceId;

    fn fresh_peer(address: WorkerAddress) -> (WorkerId, PeerInfo) {
        let inst = InstanceId::new_v4();
        let wid = inst.worker_id();
        (wid, PeerInfo::new(inst, address))
    }

    /// Regression: the streaming RPC must not complete its response until it
    /// has consumed the whole request stream.
    ///
    /// `connect`'s frame pump uses end-of-response as the acknowledgement that
    /// its terminal sentinel reached the consumer. When the handler answered
    /// with an already-empty stream, tonic sent the trailers before reading the
    /// request body, so the pump had nothing to wait on: it queued its frames
    /// into a 256-slot channel and tore the call down, discarding whatever had
    /// not yet been flushed. On a short stream that routinely threw away *every*
    /// frame including the terminal, and the server — having seen a request
    /// stream that ended without a sentinel — injected `Dropped`. The consumer
    /// then failed with `SenderDropped` despite a clean `finalize()`.
    #[tokio::test(flavor = "multi_thread")]
    async fn response_completes_only_after_request_stream_drains() {
        let server = GrpcFrameTransport::default_new().await.unwrap();
        let rx = server.bind(9, 4).await.unwrap();

        let addr = SocketAddr::new(
            std::net::Ipv4Addr::LOCALHOST.into(),
            server.bound_addr().port(),
        );
        let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = VeloStreamingClient::new(channel);

        let (tx, req_rx) = tokio::sync::mpsc::channel::<FramedData>(8);
        let mut request = Request::new(tokio_stream::wrappers::ReceiverStream::new(req_rx));
        request
            .metadata_mut()
            .insert(ANCHOR_ID_META, "9".parse().unwrap());
        request
            .metadata_mut()
            .insert(SESSION_ID_META, "4".parse().unwrap());
        let mut inbound = client.stream(request).await.unwrap().into_inner();

        let framed = |payload: Vec<u8>| FramedData {
            preamble: vec![],
            header: vec![],
            payload,
        };

        // A frame lands, but the request stream is still open: the call must
        // stay open too, or the client has no delivery acknowledgement to wait
        // on before tearing the stream down.
        tx.send(framed(b"frame".to_vec())).await.unwrap();
        assert_eq!(rx.recv_async().await.unwrap(), b"frame".to_vec());
        assert!(
            tokio::time::timeout(Duration::from_millis(250), inbound.next())
                .await
                .is_err(),
            "response completed while the request stream was still open"
        );

        // Half-close the request stream; now the response must finish.
        tx.send(framed(crate::streaming::sender::cached_finalized().clone()))
            .await
            .unwrap();
        drop(tx);
        let drained = tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(item) = inbound.next().await {
                item.expect("response stream must end cleanly");
            }
        })
        .await;
        assert!(
            drained.is_ok(),
            "response never completed after the request stream ended"
        );
    }

    /// Regression: rapid short-lived sessions must never lose their terminal
    /// sentinel.
    ///
    /// Mirrors soak scenario S4 (rapid create/finalize cycles). A short stream
    /// pushes every frame into the client-side request channel before the H2
    /// body task has flushed any of it; tearing the call down at that point
    /// used to discard the whole buffer, so the server saw a request stream
    /// that ended with zero frames and injected `Dropped` — surfacing to the
    /// consumer as `SenderDropped` even though the producer had finalized
    /// cleanly. The client now waits for the server to end its response before
    /// dropping the call.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn terminal_sentinel_survives_rapid_session_cycles() {
        const CYCLES: u64 = 96;
        const FRAMES: u64 = 8;

        let server = GrpcFrameTransport::default_new().await.unwrap();
        let client = GrpcFrameTransport::default_new().await.unwrap();
        let (server_worker, server_peer) = fresh_peer(server.address());
        client.register(&server_peer).unwrap();

        let finalized = crate::streaming::sender::cached_finalized().clone();
        let dropped = crate::streaming::sender::cached_dropped().clone();

        for cycle in 1..=CYCLES {
            let rx = server.bind(cycle, cycle).await.unwrap();
            let tx = client.connect(server_worker, cycle, cycle).await.unwrap();

            let fin = finalized.clone();
            let producer = tokio::spawn(async move {
                for i in 0..FRAMES {
                    tx.send_async(i.to_be_bytes().to_vec()).await.unwrap();
                }
                tx.send_async(fin).await.unwrap();
                // Sender goes out of scope here, exactly as `StreamSender`
                // does after `finalize()`.
            });

            let mut items = 0u64;
            let mut saw_finalized = false;
            while let Ok(frame) = tokio::time::timeout(Duration::from_secs(10), rx.recv_async())
                .await
                .unwrap_or_else(|_| panic!("cycle {cycle}: timed out after {items} frames"))
            {
                assert_ne!(
                    frame, dropped,
                    "cycle {cycle}: server injected Dropped after {items} frames"
                );
                if frame == finalized {
                    saw_finalized = true;
                    break;
                }
                items += 1;
            }
            producer.await.unwrap();
            assert!(
                saw_finalized,
                "cycle {cycle}: channel closed after {items} frames without Finalized"
            );
            assert_eq!(items, FRAMES, "cycle {cycle}: wrong frame count");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn round_trip_via_register_and_connect() {
        let server = GrpcFrameTransport::default_new().await.unwrap();
        let client = GrpcFrameTransport::default_new().await.unwrap();
        let (server_worker, server_peer) = fresh_peer(server.address());
        client.register(&server_peer).unwrap();

        let rx = server.bind(7, 1).await.unwrap();
        let tx = client.connect(server_worker, 7, 1).await.unwrap();
        let payload = b"hello".to_vec();
        tx.send_async(payload.clone()).await.unwrap();
        let received = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv_async())
            .await
            .expect("recv timeout")
            .expect("channel closed");
        assert_eq!(received, payload);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn bind_overwrite_emits_warn_and_returns_new_rx() {
        let server = GrpcFrameTransport::default_new().await.unwrap();
        // First bind grabs the slot.
        let rx1 = server.bind(42, 7).await.unwrap();
        // Second bind for the same (anchor_id, session_id) overwrites; the
        // previous frame_tx is dropped, so rx1's channel closes.
        let _rx2 = server.bind(42, 7).await.unwrap();
        // rx1 should observe the close (no more frames will arrive on it).
        let res = tokio::time::timeout(std::time::Duration::from_millis(200), rx1.recv_async())
            .await
            .expect("rx1 should have closed promptly");
        assert!(res.is_err(), "rx1 must observe channel closed");
    }

    #[test]
    fn read_u64_meta_missing_header_returns_invalid_argument() {
        let req: Request<()> = Request::new(());
        let err = read_u64_meta(&req, "x-missing").unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message().contains("missing x-missing"),
            "got: {}",
            err.message()
        );
    }

    #[test]
    fn read_u64_meta_non_u64_value_returns_invalid_argument() {
        let mut req: Request<()> = Request::new(());
        req.metadata_mut()
            .insert("x-bad", "not-a-number".parse().unwrap());
        let err = read_u64_meta(&req, "x-bad").unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message().contains("not a valid u64"),
            "got: {}",
            err.message()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn register_rejects_peer_without_grpc_entry() {
        let transport = GrpcFrameTransport::default_new().await.unwrap();
        // PeerInfo with an empty WorkerAddress has no "grpc-stream" entry.
        let (_, peer) = fresh_peer(WorkerAddress::empty());
        let err = transport.register(&peer).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("has no") && msg.contains("streaming endpoint entry"),
            "got: {msg}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn connect_to_unregistered_peer_errors() {
        let transport = GrpcFrameTransport::default_new().await.unwrap();
        let bogus = InstanceId::new_v4().worker_id();
        let err = transport.connect(bogus, 1, 1).await.unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("not registered"), "got: {msg}");
    }

    /// Mirror of `tcp_transport::tests::no_extra_dropped_after_finalized` —
    /// once the client-side forwarder writes a terminal sentinel (Finalized
    /// / Dropped / Detached / TransportError), it must exit immediately so
    /// any frame queued behind the terminal (e.g. a late heartbeat) cannot
    /// reach the server. Without the per-frame `is_terminal_sentinel` check
    /// in the forwarder, the server would see the late heartbeat as the
    /// last frame and inject `Dropped` on stream close, which would kill a
    /// reattached session sharing the routing slot.
    #[tokio::test(flavor = "multi_thread")]
    async fn grpc_client_pump_breaks_after_terminal_sentinel() {
        let server = GrpcFrameTransport::default_new().await.unwrap();
        let client = GrpcFrameTransport::default_new().await.unwrap();
        let (server_worker, server_peer) = fresh_peer(server.address());
        client.register(&server_peer).unwrap();

        let rx = server.bind(11, 3).await.unwrap();
        let tx = client.connect(server_worker, 11, 3).await.unwrap();

        let finalized =
            rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::<()>::Finalized).unwrap();
        tx.send_async(finalized.clone()).await.unwrap();

        // Queue a heartbeat behind the terminal. With the terminal-break in
        // place this should never reach the server.
        let heartbeat =
            rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::<()>::Heartbeat).unwrap();
        let _ = tx.send_async(heartbeat.clone()).await;
        drop(tx);

        let received = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv_async())
            .await
            .expect("recv timeout")
            .expect("channel closed");
        assert_eq!(
            received, finalized,
            "first frame on the server side must be the Finalized sentinel"
        );

        // No further frame should arrive (channel either silent or closed).
        let next =
            tokio::time::timeout(std::time::Duration::from_millis(500), rx.recv_async()).await;
        if let Ok(Ok(extra)) = next {
            assert_ne!(
                extra, heartbeat,
                "heartbeat queued behind Finalized must not reach the server"
            );
            assert_ne!(
                extra.as_slice(),
                crate::streaming::sender::cached_dropped().as_slice(),
                "server must not inject Dropped after Finalized"
            );
        }
        // Ok(Err(_)) (channel closed) and Err(_) (timeout) are both fine.
    }
}
