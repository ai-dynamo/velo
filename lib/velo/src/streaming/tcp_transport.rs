// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! TCP-based [`FrameTransport`] implementation backed by a single shared listener.
//!
//! [`TcpFrameTransport`] opens **one** TCP listener at construction time and
//! demuxes incoming connections to per-stream readers via a 16-byte handshake
//! (`anchor_id` + `session_id`, both u64 big-endian) sent on the wire by the
//! connecting peer.
//!
//! # Endpoint resolution
//!
//! There is no endpoint string in the streaming attach handshake. The
//! transport advertises its listener interface(s) via [`Self::address`] (which
//! the Velo builder merges into the local PeerInfo's WorkerAddress). When a
//! peer is registered (via `Velo::register_peer` or discovery), the transport
//! extracts the peer's endpoint, resolves the best socket address using
//! [`select_best_endpoint`], and caches it keyed by [`WorkerId`].
//!
//! [`Self::connect`] looks up the cached SocketAddr by `WorkerId` — no
//! per-attach DNS or string parsing happens.
//!
//! # Connection lifecycle
//!
//! 1. `new()` binds one [`TcpListener`] and spawns one accept loop.
//! 2. `bind(anchor, session)` registers a Sender keyed by `(anchor, session)`
//!    and returns the matching receiver.
//! 3. `connect(peer, anchor, session)` opens a TCP socket to the peer's
//!    cached SocketAddr, writes a 16-byte handshake, and spawns a pump task
//!    that forwards frames from the returned Sender.
//! 4. The accept loop reads the 16-byte handshake from each new connection,
//!    looks up the registered Sender by `(anchor, session)`, and pumps the
//!    framed stream into it. Unknown handshakes are dropped with a warning.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use crate::transports::MessageType;
use crate::transports::address::WorkerAddressBuilder;
use crate::transports::tcp::TcpFrameCodec;
use crate::transports::utils::interfaces::{
    InterfaceEndpoint, InterfaceFilter, parse_endpoints, resolve_advertise_endpoints,
    select_best_endpoint,
};
use anyhow::{Result, anyhow};
use dashmap::DashMap;
use futures::StreamExt;
use futures::future::BoxFuture;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;
use velo_ext::{PeerInfo, TransportKey, WorkerAddress, WorkerId};

use crate::streaming::transport::FrameTransport;

/// Maximum time a registered (anchor, session) slot may sit unused before its
/// slot is freed. After expiry, an arriving connection with that handshake is
/// treated as unknown.
const ACCEPT_TIMEOUT: Duration = Duration::from_secs(60);

/// Maximum time the accept loop will wait for a connecting peer to send its
/// 16-byte handshake. Bounds attacker-induced fd holding from open-but-silent
/// connections.
const TOKEN_READ_TIMEOUT: Duration = Duration::from_secs(20);

/// Default streaming-transport key used by the convenience constructors. The
/// suffix `-stream` distinguishes the streaming listener from the messenger
/// TCP transport's `tcp` entry in the same WorkerAddress map.
pub const TCP_STREAM_KEY: &str = "tcp-stream";

/// One slot in the per-(anchor, session) registry. The deliverer-side
/// [`flume::Sender`] is removed from the map and given to the per-stream
/// reader task on a successful handshake.
struct PendingStream {
    frame_tx: flume::Sender<Vec<u8>>,
}

type SessionRegistry = DashMap<(u64, u64), PendingStream>;

/// TCP-based [`FrameTransport`] backed by a single shared listener.
///
/// One listener + one accept loop per transport instance, regardless of how
/// many concurrent streams are active. `bind()` is O(1).
pub struct TcpFrameTransport {
    key: TransportKey,
    bind_addr: SocketAddr,
    local_address: WorkerAddress,
    /// Cached local interfaces for endpoint selection (lazy-init on first
    /// register).
    local_interfaces: std::sync::OnceLock<Vec<InterfaceEndpoint>>,
    interface_filter: InterfaceFilter,
    numa_hint: Option<u32>,
    /// Resolved peer endpoints keyed by WorkerId.
    peers: Arc<DashMap<WorkerId, SocketAddr>>,
    registry: Arc<SessionRegistry>,
    /// Cancellation handle for the accept loop. Tripped on `Drop`.
    cancel: CancellationToken,
    /// Optional metrics handle. Set once by the Velo builder via
    /// [`Self::set_metrics`] before any bind/connect; read on the hot path
    /// by the accept loop and the pump tasks.
    metrics: Arc<std::sync::OnceLock<Arc<crate::observability::VeloMetrics>>>,
}

impl TcpFrameTransport {
    /// Construct a TCP streaming transport with a custom transport key,
    /// interface filter, and NUMA hint.
    ///
    /// Mirrors the construction shape of the messenger TCP transport (binds
    /// once on `bind_addr`, advertises one or more interfaces via
    /// `Vec<InterfaceEndpoint>` encoded into [`WorkerAddress`]).
    pub async fn with_config(
        bind_addr: SocketAddr,
        key: TransportKey,
        interface_filter: InterfaceFilter,
        numa_hint: Option<u32>,
    ) -> Result<Arc<Self>> {
        let listener = TcpListener::bind(bind_addr).await?;
        let actual_addr = listener.local_addr()?;
        // Encode the listener's interface(s) for advertisement.
        let endpoints = resolve_advertise_endpoints(actual_addr, &interface_filter)?;
        let encoded = rmp_serde::to_vec(&endpoints)
            .map_err(|e| anyhow!("Failed to encode interface endpoints: {e}"))?;
        let mut addr_builder = WorkerAddressBuilder::new();
        addr_builder
            .add_entry(key.as_str(), encoded)
            .map_err(|e| anyhow!("Failed to build WorkerAddress entry: {e}"))?;
        let local_address = addr_builder
            .build()
            .map_err(|e| anyhow!("Failed to build WorkerAddress: {e}"))?;

        let registry: Arc<SessionRegistry> = Arc::new(DashMap::new());
        let cancel = CancellationToken::new();
        let metrics: Arc<std::sync::OnceLock<Arc<crate::observability::VeloMetrics>>> =
            Arc::new(std::sync::OnceLock::new());

        tokio::spawn(run_accept_loop(
            Arc::new(listener),
            registry.clone(),
            cancel.clone(),
            metrics.clone(),
        ));

        Ok(Arc::new(Self {
            key,
            bind_addr: actual_addr,
            local_address,
            local_interfaces: std::sync::OnceLock::new(),
            interface_filter,
            numa_hint,
            peers: Arc::new(DashMap::new()),
            registry,
            cancel,
            metrics,
        }))
    }

    /// Install a metrics handle. Called by the Velo builder before any
    /// `bind`/`connect`. No-op if already set; safe to call multiple times.
    pub fn set_metrics(&self, metrics: Arc<crate::observability::VeloMetrics>) {
        let _ = self.metrics.set(metrics);
    }

    /// Construct a TCP streaming transport with default key (`"tcp-stream"`),
    /// `InterfaceFilter::All`, no NUMA hint, bound on `(bind_ip, 0)` (ephemeral
    /// port). This is the convenience constructor used by `Velo::builder()`.
    pub async fn new(bind_ip: std::net::IpAddr) -> Result<Arc<Self>> {
        Self::with_config(
            SocketAddr::new(bind_ip, 0),
            TransportKey::new(TCP_STREAM_KEY),
            InterfaceFilter::All,
            None,
        )
        .await
    }

    /// Convenience: bind on `0.0.0.0:0`.
    pub async fn default_bound() -> Result<Arc<Self>> {
        Self::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)).await
    }

    /// Returns the actual bound listener address. Useful for tests that need
    /// to dial the listener directly.
    pub fn bound_addr(&self) -> SocketAddr {
        self.bind_addr
    }
}

impl Drop for TcpFrameTransport {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Single accept loop per transport.
async fn run_accept_loop(
    listener: Arc<TcpListener>,
    registry: Arc<SessionRegistry>,
    cancel: CancellationToken,
    metrics: Arc<std::sync::OnceLock<Arc<crate::observability::VeloMetrics>>>,
) {
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                tracing::debug!("TCP streaming accept loop cancelled, exiting");
                return;
            }
            res = listener.accept() => match res {
                Ok((stream, _peer)) => {
                    let pump_metrics = metrics.get().cloned();
                    tokio::spawn(handle_one_connection(stream, registry.clone(), pump_metrics));
                }
                Err(e) => {
                    tracing::warn!("TCP streaming accept error: {}", e);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            },
        }
    }
}

/// Per-connection task: 16-byte handshake (anchor_id BE + session_id BE) + frame pump.
async fn handle_one_connection(
    mut stream: TcpStream,
    registry: Arc<SessionRegistry>,
    metrics: Option<Arc<crate::observability::VeloMetrics>>,
) {
    let mut handshake = [0u8; 16];
    let read_result = tokio::time::timeout(
        TOKEN_READ_TIMEOUT,
        AsyncReadExt::read_exact(&mut stream, &mut handshake),
    )
    .await;

    let (anchor_id, session_id) = match read_result {
        Ok(Ok(_)) => (
            u64::from_be_bytes(handshake[..8].try_into().unwrap()),
            u64::from_be_bytes(handshake[8..].try_into().unwrap()),
        ),
        Ok(Err(e)) => {
            tracing::warn!("TCP streaming handshake read failed: {}", e);
            return;
        }
        Err(_) => {
            tracing::warn!(
                "TCP streaming handshake timed out after {:?}",
                TOKEN_READ_TIMEOUT
            );
            return;
        }
    };

    let pending = match registry.remove(&(anchor_id, session_id)) {
        Some((_, p)) => p,
        None => {
            tracing::warn!(
                "TCP streaming: rejecting unknown session anchor={} session={} (expired or never registered)",
                anchor_id,
                session_id
            );
            return;
        }
    };

    configure_socket(&stream);
    pump_frames(stream, pending.frame_tx, metrics).await;
}

/// Read frames off the stream and forward them to the per-stream channel.
async fn pump_frames(
    stream: TcpStream,
    frame_tx: flume::Sender<Vec<u8>>,
    metrics: Option<Arc<crate::observability::VeloMetrics>>,
) {
    let mut framed = Framed::new(stream, TcpFrameCodec::new());
    let mut last_was_terminal = false;
    let mut consumer_dropped = false;

    while let Some(result) = framed.next().await {
        match result {
            Ok((_msg_type, _header, payload)) => {
                let payload_vec = payload.to_vec();
                last_was_terminal = is_terminal_sentinel(&payload_vec);
                // Try non-blocking first so we can record server-pump
                // backpressure on the slow path before falling through to
                // the awaited send. The bind-side frame channel is
                // bounded(4096); it begins to fill once reader_pump has
                // already saturated the per-anchor channel above it.
                match frame_tx.try_send(payload_vec) {
                    Ok(()) => {}
                    Err(flume::TrySendError::Full(b)) => {
                        if let Some(m) = metrics.as_ref() {
                            m.record_server_pump_backpressure();
                        }
                        if frame_tx.send_async(b).await.is_err() {
                            consumer_dropped = true;
                            break;
                        }
                    }
                    Err(flume::TrySendError::Disconnected(_)) => {
                        consumer_dropped = true;
                        break;
                    }
                }
            }
            Err(e) => {
                tracing::warn!("TCP streaming read error: {}", e);
                break;
            }
        }
    }

    if !last_was_terminal && !consumer_dropped {
        tracing::warn!(
            "TCP streaming server pump: injecting Dropped (last frame was not terminal, consumer still attached)"
        );
        let _ = frame_tx
            .send_async(crate::streaming::sender::cached_dropped().clone())
            .await;
    }

    let mut stream = framed.into_inner();
    if let Err(e) = stream.shutdown().await {
        tracing::debug!("TCP streaming receiver shutdown: {}", e);
    }
}

/// Configure TCP socket options matching TcpTransport patterns.
fn configure_socket(stream: &TcpStream) {
    if let Err(e) = stream.set_nodelay(true) {
        tracing::warn!("Failed to set TCP_NODELAY: {}", e);
    }
    let sock = socket2::SockRef::from(stream);
    if let Err(e) = sock.set_tcp_keepalive(
        &socket2::TcpKeepalive::new()
            .with_time(Duration::from_secs(60))
            .with_interval(Duration::from_secs(10)),
    ) {
        tracing::warn!("Failed to set TCP keepalive: {}", e);
    }
    if let Err(e) = sock.set_send_buffer_size(1_048_576) {
        tracing::warn!("Failed to set send buffer size: {}", e);
    }
    if let Err(e) = sock.set_recv_buffer_size(1_048_576) {
        tracing::warn!("Failed to set recv buffer size: {}", e);
    }
    if let Err(e) = sock.set_tcp_user_timeout(Some(Duration::from_secs(30))) {
        tracing::warn!("Failed to set TCP_USER_TIMEOUT: {}", e);
    }
}

/// Check if the given raw frame bytes represent a terminal sentinel.
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

impl FrameTransport for TcpFrameTransport {
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
            parse_endpoints(&raw).map_err(|e| anyhow!("Failed to parse TCP endpoints: {e}"))?;

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
        let registry = self.registry.clone();
        Box::pin(async move {
            let (frame_tx, frame_rx) = flume::bounded::<Vec<u8>>(4096);
            registry.insert((anchor_id, session_id), PendingStream { frame_tx });

            // Expire the slot if no peer connects within ACCEPT_TIMEOUT.
            let expiry_registry = registry.clone();
            tokio::spawn(async move {
                tokio::time::sleep(ACCEPT_TIMEOUT).await;
                if expiry_registry.remove(&(anchor_id, session_id)).is_some() {
                    tracing::warn!(
                        "TCP streaming: session anchor={} session={} expired before peer connected",
                        anchor_id,
                        session_id
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
        let peers = self.peers.clone();
        Box::pin(async move {
            let addr = *peers.get(&peer).ok_or_else(|| {
                anyhow!(
                    "TCP streaming: peer {} not registered (call register_peer first)",
                    peer
                )
            })?;

            let mut stream = TcpStream::connect(addr).await?;
            configure_socket(&stream);
            let mut handshake = [0u8; 16];
            handshake[..8].copy_from_slice(&anchor_id.to_be_bytes());
            handshake[8..].copy_from_slice(&session_id.to_be_bytes());
            stream.write_all(&handshake).await?;

            let (tx, rx) = flume::bounded::<Vec<u8>>(4096);

            tokio::spawn(async move {
                while let Ok(frame_bytes) = rx.recv_async().await {
                    let is_terminal = is_terminal_sentinel(&frame_bytes);
                    if let Err(e) = TcpFrameCodec::encode_frame(
                        &mut stream,
                        MessageType::Message,
                        &[],
                        &frame_bytes,
                    )
                    .await
                    {
                        tracing::error!("TCP streaming write error: {}", e);
                        break;
                    }
                    // After writing a terminal sentinel (Finalized / Dropped /
                    // Detached / TransportError) on the wire, exit the pump
                    // immediately. Any frame queued behind the terminal (e.g.
                    // a heartbeat that landed between heartbeat_cancel.cancel()
                    // and the next loop iteration on the producer side) is
                    // discarded — sending it would race the consumer's
                    // post-terminal cleanup and trigger spurious "Connection
                    // reset by peer" RSTs at the wire level.
                    if is_terminal {
                        break;
                    }
                }
                if let Err(e) = stream.flush().await {
                    tracing::debug!("TCP streaming flush on close: {}", e);
                }
                if let Err(e) = stream.shutdown().await {
                    tracing::debug!("TCP streaming shutdown on close: {}", e);
                }
            });

            Ok(tx)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use velo_ext::InstanceId;

    /// Build a PeerInfo whose worker_id is xxh3_64-derived from the random
    /// InstanceId — accept whatever WorkerId comes out and use it as the
    /// connect() target so register/connect cache lookups stay consistent.
    fn fresh_peer(address: WorkerAddress) -> (WorkerId, PeerInfo) {
        let inst = InstanceId::new_v4();
        let wid = inst.worker_id();
        (wid, PeerInfo::new(inst, address))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn round_trip_via_register_and_connect() {
        let server = TcpFrameTransport::default_bound().await.unwrap();
        let client = TcpFrameTransport::default_bound().await.unwrap();

        let (server_worker, server_peer) = fresh_peer(server.address());
        let (client_worker, client_peer) = fresh_peer(client.address());
        client.register(&server_peer).unwrap();
        server.register(&client_peer).unwrap();
        let _ = client_worker; // currently unused on this side

        // Server binds, client connects.
        let rx = server.bind(42, 7).await.unwrap();
        let tx = client.connect(server_worker, 42, 7).await.unwrap();

        let frame_bytes =
            rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::<u32>::Item(99)).unwrap();
        tx.send_async(frame_bytes.clone()).await.unwrap();

        let received = tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
            .await
            .expect("recv timeout")
            .expect("channel closed");
        assert_eq!(received, frame_bytes);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unknown_session_is_rejected() {
        let transport = TcpFrameTransport::default_bound().await.unwrap();
        let rx = transport.bind(1, 1).await.unwrap();

        // Connect manually with a wrong session_id handshake.
        let addr = transport.bound_addr();
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let mut wrong = [0u8; 16];
        wrong[..8].copy_from_slice(&1u64.to_be_bytes());
        wrong[8..].copy_from_slice(&999u64.to_be_bytes());
        stream.write_all(&wrong).await.unwrap();

        let result = tokio::time::timeout(Duration::from_millis(500), rx.recv_async()).await;
        assert!(
            result.is_err(),
            "should not receive frames from invalid handshake"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dropped_sentinel_injected_on_abrupt_close() {
        let server = TcpFrameTransport::default_bound().await.unwrap();
        let client = TcpFrameTransport::default_bound().await.unwrap();
        let (server_worker, server_peer) = fresh_peer(server.address());
        client.register(&server_peer).unwrap();

        let rx = server.bind(1, 1).await.unwrap();
        let tx = client.connect(server_worker, 1, 1).await.unwrap();
        let frame = rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::<String>::Item(
            "data".to_string(),
        ))
        .unwrap();
        tx.send_async(frame.clone()).await.unwrap();
        drop(tx);

        let received = tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
            .await
            .expect("timeout")
            .expect("channel closed");
        assert_eq!(received, frame);

        let sentinel = tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
            .await
            .expect("timeout waiting for Dropped sentinel")
            .expect("channel closed before Dropped sentinel");
        assert_eq!(
            sentinel.as_slice(),
            crate::streaming::sender::cached_dropped().as_slice(),
            "should receive Dropped sentinel after abrupt close"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_extra_dropped_after_finalized() {
        let server = TcpFrameTransport::default_bound().await.unwrap();
        let client = TcpFrameTransport::default_bound().await.unwrap();
        let (server_worker, server_peer) = fresh_peer(server.address());
        client.register(&server_peer).unwrap();

        let rx = server.bind(1, 1).await.unwrap();
        let tx = client.connect(server_worker, 1, 1).await.unwrap();

        let finalized =
            rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::<()>::Finalized).unwrap();
        tx.send_async(finalized.clone()).await.unwrap();
        drop(tx);

        let received = tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
            .await
            .expect("timeout")
            .expect("channel closed");
        assert_eq!(received, finalized);

        let result = tokio::time::timeout(Duration::from_secs(2), rx.recv_async()).await;
        match result {
            Ok(Ok(extra)) => {
                assert_ne!(
                    extra.as_slice(),
                    crate::streaming::sender::cached_dropped().as_slice(),
                    "should not inject Dropped after Finalized"
                );
            }
            Ok(Err(_)) => {} // channel closed — expected
            Err(_) => {}     // timeout — also fine
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shared_listener_serves_many_concurrent_streams() {
        let server = TcpFrameTransport::default_bound().await.unwrap();
        let client = TcpFrameTransport::default_bound().await.unwrap();
        let (server_worker, server_peer) = fresh_peer(server.address());
        client.register(&server_peer).unwrap();

        let mut handles = Vec::new();
        for i in 0u64..32 {
            let server = server.clone();
            let client = client.clone();
            handles.push(tokio::spawn(async move {
                let rx = server.bind(i, 0).await.unwrap();
                let tx = client.connect(server_worker, i, 0).await.unwrap();
                let payload =
                    rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::<u64>::Item(i))
                        .unwrap();
                tx.send_async(payload.clone()).await.unwrap();
                let received = tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
                    .await
                    .expect("recv timeout")
                    .expect("channel closed");
                assert_eq!(received, payload);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    }
}
