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
    type StreamStream = futures::stream::Empty<Result<FramedData, Status>>;

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
        tokio::spawn(async move {
            let mut last_was_terminal = false;
            while let Some(result) = stream.next().await {
                match result {
                    Ok(framed) => {
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
                let _ = frame_tx
                    .send_async(crate::streaming::sender::cached_dropped().clone())
                    .await;
            }
        });

        Ok(Response::new(futures::stream::empty()))
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
                // Hold the Response (and thus the underlying H2 bidi call) for as
                // long as we are pumping frames; dropping it sends RST_STREAM and
                // cancels the inbound side of the call.
                let _response = response;
                while let Ok(payload) = frame_rx.recv_async().await {
                    let framed = FramedData {
                        preamble: vec![],
                        header: vec![],
                        payload,
                    };
                    if mpsc_tx.send(framed).await.is_err() {
                        break;
                    }
                }
            });

            Ok(frame_tx)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use velo_ext::InstanceId;

    fn fresh_peer(address: WorkerAddress) -> (WorkerId, PeerInfo) {
        let inst = InstanceId::new_v4();
        let wid = inst.worker_id();
        (wid, PeerInfo::new(inst, address))
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
}
