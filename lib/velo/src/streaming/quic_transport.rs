// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! QUIC [`FrameTransport`]: one QUIC stream for each velo stream (feature
//! `quic`).
//!
//! [`QuicFrameTransport`] has the shape of [`TcpFrameTransport`], with QUIC
//! streams in place of TCP connections:
//!
//! 1. `with_config()` binds the server sockets (a reuse-port group, as in the
//!    QUIC messenger transport) and one dial socket, and advertises the
//!    listener and its certificate fingerprint in the `WorkerAddress`.
//! 2. `bind(anchor, session)` registers a sender keyed by `(anchor, session)`
//!    and returns the matching receiver.
//! 3. `connect(peer, anchor, session)` takes the one QUIC connection to that
//!    peer (and dials it on first use), opens a unidirectional stream, writes
//!    the same 16-byte handshake as TCP, and spawns a writer that sends the
//!    frames with the TCP frame codec.
//! 4. The accept side reads the handshake from each new stream, looks up the
//!    registered sender, and pumps the frames into it.
//!
//! One connection per peer carries every stream to that peer, so a new stream
//! costs no handshake after the first. QUIC does the multiplexing and the flow
//! control for each stream. A lost packet stalls only the streams whose data
//! it carried, where one TCP connection would stall all of them. There is no
//! batching across streams: each stream writes its own frames, and quinn puts
//! the frames that are ready at the same time into one packet.
//!
//! [`TcpFrameTransport`]: crate::streaming::TcpFrameTransport

use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use dashmap::DashMap;
use futures::StreamExt;
use futures::future::BoxFuture;
use tokio::io::AsyncWriteExt;
use tokio_util::codec::FramedRead;
use tokio_util::sync::CancellationToken;
use velo_ext::{PeerInfo, TransportKey, WorkerAddress, WorkerId};

use crate::streaming::sender::is_terminal_sentinel;
use crate::streaming::tcp_transport::EgressFrame;
use crate::streaming::transport::FrameTransport;
use crate::transports::coalesce::{WriterFailure, WriterObserver, run_coalescing_writer};
use crate::transports::quic::endpoint::{
    BufferSizes, QuicEndpointInfo, bind_client_socket, bind_server_sockets,
};
use crate::transports::quic::tls::{self, Identity};
use crate::transports::quic::{
    DEFAULT_SERVER_ENDPOINTS, DEFAULT_UDP_RECV_BUFFER, DEFAULT_UDP_SEND_BUFFER, clamp_to_gso_batch,
};
use crate::transports::tcp::TcpFrameCodec;
use crate::transports::tcp::framing::{DEFAULT_SHRINK_THRESHOLD, maybe_shrink_read_buffer};
use crate::transports::utils::interfaces::{
    InterfaceEndpoint, InterfaceFilter, resolve_advertise_endpoints, select_best_endpoint,
};

/// Default streaming-transport key. The suffix `-stream` keeps it apart from
/// the QUIC messenger transport's `quic` entry in the same `WorkerAddress`.
pub const QUIC_STREAM_KEY: &str = "quic-stream";

/// How long a registered `(anchor, session)` slot waits for its stream before
/// it is freed. The same value as the TCP frame transport.
const ACCEPT_TIMEOUT: Duration = Duration::from_secs(60);

/// How long the accept side waits for the 16-byte handshake on a new stream,
/// and how long `connect` waits to open a stream and write the handshake. The
/// same value as the TCP frame transport.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(20);

/// How long a dial may take before `connect` fails.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Unidirectional streams one peer may have open on a connection at once.
///
/// This is the number of concurrent velo streams from one peer, not a
/// throughput limit. When a peer reaches it, its next `connect` waits for a
/// stream to close. A worker process that holds a thousand requests needs a
/// thousand. Each open stream costs a few hundred bytes of quinn state until
/// data arrives.
const MAX_CONCURRENT_STREAMS: u32 = 16_384;

/// Capacity of the frame channel on each side of a stream. The same value as
/// the TCP frame transport.
const FRAME_CHANNEL_CAPACITY: usize = 4096;

/// Configuration for [`QuicFrameTransport`].
///
/// `None` in an optional field leaves quinn's default in place.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct QuicStreamConfig {
    /// UDP address for the server sockets (default `0.0.0.0:0`).
    pub bind_addr: SocketAddr,
    /// Server sockets in the `SO_REUSEPORT` group (Linux only; default 4).
    /// A node that many peers stream to (a frontend) gains from more.
    pub server_endpoints: usize,
    /// Upper bound for path MTU discovery. Values above 6550 are lowered to
    /// 6550, for the reason the QUIC messenger transport gives.
    pub max_mtu: Option<u16>,
    /// Flow-control window of each stream, in bytes.
    pub stream_receive_window: Option<u32>,
}

impl QuicStreamConfig {
    /// Defaults, bound at `bind_addr`.
    pub fn new(bind_addr: SocketAddr) -> Self {
        Self {
            bind_addr,
            server_endpoints: DEFAULT_SERVER_ENDPOINTS,
            max_mtu: None,
            stream_receive_window: None,
        }
    }

    /// Set the number of server sockets in the reuse-port group.
    pub fn server_endpoints(mut self, count: usize) -> Self {
        self.server_endpoints = count.max(1);
        self
    }

    /// Set the upper bound for path MTU discovery.
    pub fn max_mtu(mut self, bytes: u16) -> Self {
        self.max_mtu = Some(bytes);
        self
    }

    /// Set the flow-control window of each stream.
    pub fn stream_receive_window(mut self, bytes: u32) -> Self {
        self.stream_receive_window = Some(bytes);
        self
    }
}

impl Default for QuicStreamConfig {
    fn default() -> Self {
        Self::new(SocketAddr::from(([0, 0, 0, 0], 0)))
    }
}

/// A registered peer: where to dial, and the TLS config that pins its
/// certificate.
#[derive(Clone)]
struct PeerEntry {
    addr: SocketAddr,
    client_config: quinn::ClientConfig,
}

type SessionRegistry = DashMap<(u64, u64), flume::Sender<Vec<u8>>>;
type Metrics = Arc<OnceLock<Arc<crate::observability::VeloMetrics>>>;

/// QUIC [`FrameTransport`]: one connection per peer, one QUIC stream per velo
/// stream. See the module docs.
pub struct QuicFrameTransport {
    key: TransportKey,
    bind_addr: SocketAddr,
    local_address: WorkerAddress,
    local_interfaces: OnceLock<Vec<InterfaceEndpoint>>,
    transport_config: Arc<quinn::TransportConfig>,
    peers: DashMap<WorkerId, PeerEntry>,
    /// The live connection to each peer. Read on every `connect`.
    connections: DashMap<WorkerId, quinn::Connection>,
    /// Serialises the dial to each peer, so that concurrent first streams to
    /// one peer share one handshake instead of racing several.
    dial_locks: DashMap<WorkerId, Arc<tokio::sync::Mutex<()>>>,
    client_endpoint: quinn::Endpoint,
    server_endpoints: Vec<quinn::Endpoint>,
    registry: Arc<SessionRegistry>,
    cancel: CancellationToken,
    metrics: Metrics,
}

impl QuicFrameTransport {
    /// Bind the sockets, make the certificate, and start the accept loops.
    /// Must run inside a Tokio runtime.
    pub async fn with_config(config: QuicStreamConfig) -> Result<Arc<Self>> {
        Self::with_key(config, TransportKey::new(QUIC_STREAM_KEY)).await
    }

    /// As [`Self::with_config`], with a custom transport key.
    pub async fn with_key(config: QuicStreamConfig, key: TransportKey) -> Result<Arc<Self>> {
        let buffers = BufferSizes {
            recv: DEFAULT_UDP_RECV_BUFFER,
            send: DEFAULT_UDP_SEND_BUFFER,
        };
        let server_sockets =
            bind_server_sockets(config.bind_addr, config.server_endpoints, buffers)?;
        let bind_addr = server_sockets[0].local_addr()?;
        let client_socket = bind_client_socket(bind_addr, buffers)?;

        let mut transport_config = quinn::TransportConfig::default();
        // The dialer opens unidirectional streams only; nothing flows back.
        transport_config.max_concurrent_uni_streams(MAX_CONCURRENT_STREAMS.into());
        transport_config.max_concurrent_bidi_streams(0u32.into());
        transport_config.datagram_receive_buffer_size(None);
        transport_config.keep_alive_interval(Some(Duration::from_secs(5)));
        if let Some(window) = config.stream_receive_window {
            transport_config.stream_receive_window(window.into());
        }
        let mut endpoint_config = quinn::EndpointConfig::default();
        if let Some(max_mtu) = config.max_mtu.map(clamp_to_gso_batch) {
            endpoint_config
                .max_udp_payload_size(max_mtu.max(1200))
                .context("QUIC max_mtu is out of range")?;
            let mut discovery = quinn::MtuDiscoveryConfig::default();
            discovery.upper_bound(max_mtu);
            transport_config.mtu_discovery_config(Some(discovery));
        }
        let transport_config = Arc::new(transport_config);

        let identity = Identity::generate()?;
        let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(tls::server_crypto(&identity)?)
                .context("failed to build the QUIC server TLS config")?,
        ));
        server_config.transport_config(transport_config.clone());

        let runtime = Arc::new(quinn::TokioRuntime);
        let mut server_endpoints = Vec::with_capacity(server_sockets.len());
        for socket in server_sockets {
            server_endpoints.push(
                quinn::Endpoint::new(
                    endpoint_config.clone(),
                    Some(server_config.clone()),
                    socket,
                    runtime.clone(),
                )
                .context("failed to create a QUIC stream server endpoint")?,
            );
        }
        let client_endpoint = quinn::Endpoint::new(endpoint_config, None, client_socket, runtime)
            .context("failed to create the QUIC stream client endpoint")?;

        let info = QuicEndpointInfo {
            endpoints: resolve_advertise_endpoints(bind_addr, &InterfaceFilter::All)?,
            fingerprint: identity.fingerprint,
        };
        let mut addr_builder = crate::transports::address::WorkerAddressBuilder::new();
        addr_builder.add_entry(key.clone(), info.encode()?)?;
        let local_address = addr_builder.build()?;

        let registry: Arc<SessionRegistry> = Arc::new(DashMap::new());
        let cancel = CancellationToken::new();
        let metrics: Metrics = Arc::new(OnceLock::new());
        for endpoint in &server_endpoints {
            tokio::spawn(run_accept_loop(
                endpoint.clone(),
                registry.clone(),
                cancel.clone(),
                metrics.clone(),
            ));
        }

        Ok(Arc::new(Self {
            key,
            bind_addr,
            local_address,
            local_interfaces: OnceLock::new(),
            transport_config,
            peers: DashMap::new(),
            connections: DashMap::new(),
            dial_locks: DashMap::new(),
            client_endpoint,
            server_endpoints,
            registry,
            cancel,
            metrics,
        }))
    }

    /// Install a metrics handle. Called by the Velo builder before any
    /// `bind`/`connect`. No-op if already set.
    pub(crate) fn set_metrics(&self, metrics: Arc<crate::observability::VeloMetrics>) {
        let _ = self.metrics.set(metrics);
    }

    /// The bound UDP address of the server sockets.
    pub fn bound_addr(&self) -> SocketAddr {
        self.bind_addr
    }

    /// The live connection to `peer`, dialed if there is none.
    async fn connection_to(&self, peer: WorkerId) -> Result<quinn::Connection> {
        if let Some(connection) = self.live_connection(peer) {
            return Ok(connection);
        }
        let lock = self.dial_locks.entry(peer).or_default().clone();
        let _dial = lock.lock().await;
        // Another stream may have dialed while this one waited.
        if let Some(connection) = self.live_connection(peer) {
            return Ok(connection);
        }
        let entry = self
            .peers
            .get(&peer)
            .ok_or_else(|| {
                anyhow!("QUIC streaming: peer {peer} not registered (call register_peer first)")
            })?
            .clone();
        let connecting = self
            .client_endpoint
            .connect_with(entry.client_config, entry.addr, tls::SERVER_NAME)
            .context("QUIC streaming: failed to start the handshake")?;
        let connection = tokio::time::timeout(CONNECT_TIMEOUT, connecting)
            .await
            .map_err(|_| {
                anyhow!(
                    "QUIC streaming: connect to peer {peer} ({}) timed out after {CONNECT_TIMEOUT:?}",
                    entry.addr
                )
            })?
            .with_context(|| format!("QUIC streaming: handshake with peer {peer} failed"))?;
        self.connections.insert(peer, connection.clone());
        Ok(connection)
    }

    fn live_connection(&self, peer: WorkerId) -> Option<quinn::Connection> {
        let connection = self.connections.get(&peer)?;
        connection
            .close_reason()
            .is_none()
            .then(|| connection.clone())
    }

    #[cfg(test)]
    fn cached_connection(&self, peer: WorkerId) -> Option<quinn::Connection> {
        self.connections.get(&peer).map(|c| c.clone())
    }
}

impl Drop for QuicFrameTransport {
    fn drop(&mut self) {
        self.cancel.cancel();
        for endpoint in &self.server_endpoints {
            endpoint.close(quinn::VarInt::from_u32(0), b"shutdown");
        }
        self.client_endpoint
            .close(quinn::VarInt::from_u32(0), b"shutdown");
    }
}

/// Accept connections on one server endpoint until the transport drops.
async fn run_accept_loop(
    endpoint: quinn::Endpoint,
    registry: Arc<SessionRegistry>,
    cancel: CancellationToken,
    metrics: Metrics,
) {
    loop {
        let incoming = tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
            incoming = endpoint.accept() => match incoming {
                Some(incoming) => incoming,
                None => return,
            },
        };
        let registry = registry.clone();
        let cancel = cancel.clone();
        let metrics = metrics.clone();
        tokio::spawn(async move {
            match incoming.await {
                Ok(connection) => serve_connection(connection, registry, cancel, metrics).await,
                Err(e) => tracing::debug!("QUIC streaming: inbound handshake failed: {e:#}"),
            }
        });
    }
}

/// Accept the streams that one peer opens, one task for each.
async fn serve_connection(
    connection: quinn::Connection,
    registry: Arc<SessionRegistry>,
    cancel: CancellationToken,
    metrics: Metrics,
) {
    loop {
        let stream = tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
            stream = connection.accept_uni() => stream,
        };
        match stream {
            Ok(recv) => {
                tokio::spawn(serve_stream(recv, registry.clone(), metrics.get().cloned()));
            }
            Err(e) => {
                tracing::debug!(
                    "QUIC streaming: connection from {} ended: {e:#}",
                    connection.remote_address()
                );
                return;
            }
        }
    }
}

/// Read the handshake from one stream, then pump its frames to the bound
/// receiver.
async fn serve_stream(
    mut recv: quinn::RecvStream,
    registry: Arc<SessionRegistry>,
    metrics: Option<Arc<crate::observability::VeloMetrics>>,
) {
    let mut handshake = [0u8; 16];
    match tokio::time::timeout(HANDSHAKE_TIMEOUT, recv.read_exact(&mut handshake)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            tracing::warn!("QUIC streaming handshake read failed: {e}");
            return;
        }
        Err(_) => {
            tracing::warn!("QUIC streaming handshake timed out after {HANDSHAKE_TIMEOUT:?}");
            return;
        }
    }
    let anchor_id = u64::from_be_bytes(handshake[..8].try_into().unwrap());
    let session_id = u64::from_be_bytes(handshake[8..].try_into().unwrap());
    let Some((_, frame_tx)) = registry.remove(&(anchor_id, session_id)) else {
        tracing::warn!(
            "QUIC streaming: rejecting unknown session anchor={anchor_id} session={session_id} \
             (expired or never registered)"
        );
        // Tell the sender at once rather than let it write into a stream
        // nobody reads.
        let _ = recv.stop(quinn::VarInt::from_u32(1));
        return;
    };
    pump_frames(recv, frame_tx, metrics).await;
}

/// Forward the frames of one stream to its bound receiver. The same rules as
/// the TCP frame transport: a stream that ends without a terminal sentinel,
/// while the consumer still listens, gets a `Dropped` sentinel injected.
async fn pump_frames(
    recv: quinn::RecvStream,
    frame_tx: flume::Sender<Vec<u8>>,
    metrics: Option<Arc<crate::observability::VeloMetrics>>,
) {
    let mut framed = FramedRead::new(recv, TcpFrameCodec::new());
    let mut last_was_terminal = false;
    let mut consumer_dropped = false;

    while let Some(result) = framed.next().await {
        match result {
            Ok((_msg_type, header, payload)) => {
                let frame_size = header.len() + payload.len();
                let payload_vec = payload.to_vec();
                last_was_terminal = is_terminal_sentinel(&payload_vec);
                match frame_tx.try_send(payload_vec) {
                    Ok(()) => {}
                    Err(flume::TrySendError::Full(frame)) => {
                        if let Some(m) = metrics.as_ref() {
                            m.record_server_pump_backpressure();
                        }
                        if frame_tx.send_async(frame).await.is_err() {
                            consumer_dropped = true;
                            break;
                        }
                    }
                    Err(flume::TrySendError::Disconnected(_)) => {
                        consumer_dropped = true;
                        break;
                    }
                }
                maybe_shrink_read_buffer(
                    framed.read_buffer_mut(),
                    DEFAULT_SHRINK_THRESHOLD,
                    frame_size,
                );
            }
            Err(e) => {
                tracing::warn!("QUIC streaming read error: {e}");
                break;
            }
        }
    }

    if !last_was_terminal && !consumer_dropped {
        tracing::warn!(
            "QUIC streaming server pump: injecting Dropped (last frame was not terminal, consumer still attached)"
        );
        let _ = frame_tx
            .send_async(crate::streaming::sender::cached_dropped().clone())
            .await;
    }
    if consumer_dropped {
        // Nobody reads the rest; stop the sender instead of letting it fill
        // the stream window.
        let mut recv = framed.into_inner();
        let _ = recv.stop(quinn::VarInt::from_u32(0));
    }
}

/// Write the frames of one stream until its channel closes, a terminal
/// sentinel goes out, or the stream fails, then finish the stream.
async fn egress_pump(
    mut send: quinn::SendStream,
    rx: flume::Receiver<Vec<u8>>,
    metrics: Option<Arc<crate::observability::VeloMetrics>>,
) {
    run_coalescing_writer(
        &mut send,
        &rx,
        EgressFrame,
        None,
        &EgressObserver { metrics },
    )
    .await;
    // `finish` hands quinn the end of the stream; quinn still delivers every
    // byte before it. The connection stays open for the peer's other streams.
    if let Err(e) = send.flush().await {
        tracing::debug!("QUIC streaming flush on close: {e}");
    }
    let _ = send.finish();
}

/// Feeds the egress pump's batching counters and error logs.
struct EgressObserver {
    metrics: Option<Arc<crate::observability::VeloMetrics>>,
}

impl WriterObserver for EgressObserver {
    fn on_flush(&self, frames: usize) {
        if let Some(m) = &self.metrics {
            m.record_streaming_egress_flush(frames);
        }
    }

    fn on_failure(&self, kind: WriterFailure, err: &std::io::Error, _frames: usize) {
        match kind {
            WriterFailure::Write => tracing::error!("QUIC streaming write error: {err}"),
            WriterFailure::Encode => tracing::error!("QUIC streaming encode error: {err}"),
        }
    }
}

impl FrameTransport for QuicFrameTransport {
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
        let info = QuicEndpointInfo::decode(&raw)?;
        let local = self.local_interfaces.get_or_init(|| {
            resolve_advertise_endpoints(self.bind_addr, &InterfaceFilter::All).unwrap_or_default()
        });
        let addr = select_best_endpoint(&info.endpoints, local, None).ok_or_else(|| {
            anyhow!(
                "no suitable endpoint for peer {} from {:?}",
                peer_info.worker_id(),
                info.endpoints
            )
        })?;
        let mut client_config = tls::pinned_client_config(info.fingerprint)?;
        client_config.transport_config(self.transport_config.clone());
        self.peers.insert(
            peer_info.worker_id(),
            PeerEntry {
                addr,
                client_config,
            },
        );
        Ok(())
    }

    fn bind(
        &self,
        anchor_id: u64,
        session_id: u64,
    ) -> BoxFuture<'_, Result<flume::Receiver<Vec<u8>>>> {
        let registry = self.registry.clone();
        Box::pin(async move {
            let (frame_tx, frame_rx) = flume::bounded::<Vec<u8>>(FRAME_CHANNEL_CAPACITY);
            registry.insert((anchor_id, session_id), frame_tx);
            // Free the slot if no stream arrives within ACCEPT_TIMEOUT.
            let expiry = registry.clone();
            tokio::spawn(async move {
                tokio::time::sleep(ACCEPT_TIMEOUT).await;
                if expiry.remove(&(anchor_id, session_id)).is_some() {
                    tracing::warn!(
                        "QUIC streaming: session anchor={anchor_id} session={session_id} expired before peer connected"
                    );
                }
            });
            Ok(frame_rx)
        })
    }

    fn connect(
        &self,
        peer: WorkerId,
        anchor_id: u64,
        session_id: u64,
    ) -> BoxFuture<'_, Result<flume::Sender<Vec<u8>>>> {
        Box::pin(async move {
            let connection = self.connection_to(peer).await?;
            let mut handshake = [0u8; 16];
            handshake[..8].copy_from_slice(&anchor_id.to_be_bytes());
            handshake[8..].copy_from_slice(&session_id.to_be_bytes());
            // `open_uni` waits while the peer's stream limit is reached; the
            // timeout makes that wait fail rather than hang the attach.
            let send = tokio::time::timeout(HANDSHAKE_TIMEOUT, async {
                let mut send = connection.open_uni().await?;
                send.write_all(&handshake).await?;
                anyhow::Ok(send)
            })
            .await
            .map_err(|_| {
                anyhow!(
                    "QUIC streaming: opening a stream to peer {peer} timed out after {HANDSHAKE_TIMEOUT:?}"
                )
            })?
            .with_context(|| format!("QUIC streaming: opening a stream to peer {peer} failed"))?;
            // Nothing comes back on a unidirectional stream, so the handshake
            // needs no reply; the writer takes the stream from here.
            let (tx, rx) = flume::bounded::<Vec<u8>>(FRAME_CHANNEL_CAPACITY);
            tokio::spawn(egress_pump(send, rx, self.metrics.get().cloned()));
            Ok(tx)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::frame::StreamFrame;
    use velo_ext::InstanceId;

    fn fresh_peer(address: WorkerAddress) -> (WorkerId, PeerInfo) {
        let inst = InstanceId::new_v4();
        (inst.worker_id(), PeerInfo::new(inst, address))
    }

    async fn loopback() -> Arc<QuicFrameTransport> {
        QuicFrameTransport::with_config(QuicStreamConfig::new("127.0.0.1:0".parse().unwrap()))
            .await
            .unwrap()
    }

    /// A server and a client that has registered the server.
    async fn pair() -> (Arc<QuicFrameTransport>, Arc<QuicFrameTransport>, WorkerId) {
        let server = loopback().await;
        let client = loopback().await;
        let (server_worker, server_peer) = fresh_peer(server.address());
        client.register(&server_peer).unwrap();
        (server, client, server_worker)
    }

    async fn recv(rx: &flume::Receiver<Vec<u8>>) -> Vec<u8> {
        tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
            .await
            .expect("recv timeout")
            .expect("channel closed")
    }

    fn item<T: serde::Serialize>(value: T) -> Vec<u8> {
        rmp_serde::to_vec(&StreamFrame::<T>::Item(value)).unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn round_trip_via_register_and_connect() {
        let (server, client, server_worker) = pair().await;
        let rx = server.bind(42, 7).await.unwrap();
        let tx = client.connect(server_worker, 42, 7).await.unwrap();
        let frame = item(99u32);
        tx.send_async(frame.clone()).await.unwrap();
        assert_eq!(recv(&rx).await, frame);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn frames_arrive_in_send_order() {
        // The ordered-delivery contract, over enough frames that the writer
        // coalesces and quinn splits the stream across many packets.
        let (server, client, server_worker) = pair().await;
        let rx = server.bind(1, 1).await.unwrap();
        let tx = client.connect(server_worker, 1, 1).await.unwrap();
        let sender = tokio::spawn(async move {
            for i in 0u32..10_000 {
                tx.send_async(item(i)).await.unwrap();
            }
        });
        for i in 0u32..10_000 {
            assert_eq!(recv(&rx).await, item(i), "frame {i} out of order");
        }
        sender.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unknown_session_is_rejected() {
        let (server, client, server_worker) = pair().await;
        let rx = server.bind(1, 1).await.unwrap();
        // A stream whose handshake names a session nobody bound.
        let tx = client.connect(server_worker, 1, 999).await.unwrap();
        let _ = tx.send_async(item(1u32)).await;
        let result = tokio::time::timeout(Duration::from_millis(500), rx.recv_async()).await;
        assert!(
            result.is_err(),
            "a frame from an unknown session reached the bound receiver"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dropped_sentinel_injected_on_abrupt_close() {
        let (server, client, server_worker) = pair().await;
        let rx = server.bind(1, 1).await.unwrap();
        let tx = client.connect(server_worker, 1, 1).await.unwrap();
        let frame = item("data".to_string());
        tx.send_async(frame.clone()).await.unwrap();
        drop(tx);
        assert_eq!(recv(&rx).await, frame);
        assert_eq!(
            recv(&rx).await.as_slice(),
            crate::streaming::sender::cached_dropped().as_slice(),
            "a stream that ends without a terminal must deliver Dropped"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dropped_sentinel_injected_when_the_connection_dies() {
        // Losing the connection ends every stream on it; each consumer must
        // see Dropped rather than wait for a heartbeat timeout.
        let (server, client, server_worker) = pair().await;
        let rx = server.bind(1, 1).await.unwrap();
        let tx = client.connect(server_worker, 1, 1).await.unwrap();
        let frame = item(5u32);
        tx.send_async(frame.clone()).await.unwrap();
        assert_eq!(recv(&rx).await, frame);
        client
            .cached_connection(server_worker)
            .unwrap()
            .close(quinn::VarInt::from_u32(9), b"test");
        assert_eq!(
            recv(&rx).await.as_slice(),
            crate::streaming::sender::cached_dropped().as_slice()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_extra_dropped_after_finalized() {
        let (server, client, server_worker) = pair().await;
        let rx = server.bind(1, 1).await.unwrap();
        let tx = client.connect(server_worker, 1, 1).await.unwrap();
        let finalized = rmp_serde::to_vec(&StreamFrame::<()>::Finalized).unwrap();
        tx.send_async(finalized.clone()).await.unwrap();
        drop(tx);
        assert_eq!(recv(&rx).await, finalized);
        if let Ok(Ok(extra)) = tokio::time::timeout(Duration::from_secs(2), rx.recv_async()).await {
            assert_ne!(
                extra.as_slice(),
                crate::streaming::sender::cached_dropped().as_slice(),
                "should not inject Dropped after Finalized"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_streams_share_one_connection() {
        // The point of this transport: many streams to one peer cost one
        // handshake. 64 concurrent first connects must not dial 64 times.
        let (server, client, server_worker) = pair().await;
        let mut handles = Vec::new();
        for i in 0u64..64 {
            let server = server.clone();
            let client = client.clone();
            handles.push(tokio::spawn(async move {
                let rx = server.bind(i, 0).await.unwrap();
                let tx = client.connect(server_worker, i, 0).await.unwrap();
                tx.send_async(item(i)).await.unwrap();
                assert_eq!(recv(&rx).await, item(i));
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        let connection = client.cached_connection(server_worker).unwrap();
        assert_eq!(client.connections.len(), 1);
        assert_eq!(
            client.client_endpoint.open_connections(),
            1,
            "every stream to one peer must ride one connection"
        );
        assert!(connection.close_reason().is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn redials_after_the_connection_closes() {
        let (server, client, server_worker) = pair().await;
        let rx = server.bind(1, 1).await.unwrap();
        let tx = client.connect(server_worker, 1, 1).await.unwrap();
        tx.send_async(item(1u32)).await.unwrap();
        assert_eq!(recv(&rx).await, item(1u32));
        let first = client.cached_connection(server_worker).unwrap();
        first.close(quinn::VarInt::from_u32(0), b"test");

        let rx = server.bind(2, 2).await.unwrap();
        let tx = client.connect(server_worker, 2, 2).await.unwrap();
        tx.send_async(item(2u32)).await.unwrap();
        assert_eq!(recv(&rx).await, item(2u32));
        let second = client.cached_connection(server_worker).unwrap();
        assert_ne!(first.stable_id(), second.stable_id());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_listener_with_another_certificate_is_refused() {
        // The pinned fingerprint is the only authentication. A peer entry
        // that carries one listener's fingerprint and another's address must
        // fail the handshake.
        let server = loopback().await;
        let impostor = loopback().await;
        let client = loopback().await;
        let mut info = QuicEndpointInfo::decode(
            &server
                .address()
                .get_entry(QUIC_STREAM_KEY)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        let impostor_info = QuicEndpointInfo::decode(
            &impostor
                .address()
                .get_entry(QUIC_STREAM_KEY)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        info.endpoints = impostor_info.endpoints;
        let mut builder = crate::transports::address::WorkerAddressBuilder::new();
        builder
            .add_entry(QUIC_STREAM_KEY, info.encode().unwrap())
            .unwrap();
        let (worker, peer) = fresh_peer(builder.build().unwrap());
        client.register(&peer).unwrap();
        let error = client
            .connect(worker, 1, 1)
            .await
            .expect_err("a dial to a listener with another certificate must fail");
        assert!(
            format!("{error:#}").contains("handshake"),
            "unexpected error: {error:#}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn connect_to_an_unregistered_peer_fails() {
        let client = loopback().await;
        let error = client
            .connect(InstanceId::new_v4().worker_id(), 1, 1)
            .await
            .expect_err("no peer entry, no dial");
        assert!(format!("{error:#}").contains("not registered"));
    }
}
