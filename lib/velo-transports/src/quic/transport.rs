// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! QUIC transport implementation.
//!
//! Uses `quinn` for QUIC/TLS 1.3 connections. Architecture mirrors the TCP
//! transport: lazy per-peer connections via DashMap, bounded flume channels
//! for per-connection writer tasks, and the same 3-phase shutdown protocol.

use anyhow::{Context, Result};
use bytes::Bytes;
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use velo_observability::VeloMetrics;

use crate::tcp::TcpFrameCodec;
use crate::transport::{HealthCheckError, ShutdownState, TransportError, TransportErrorHandler};
use crate::utils::interfaces::{
    InterfaceEndpoint, InterfaceFilter, parse_endpoints, resolve_advertise_endpoints,
    select_best_endpoint,
};
use crate::{MessageType, PeerInfo, Transport, TransportAdapter, TransportKey, WorkerAddress};

use super::listener::QuicListener;
use super::tls;

/// QUIC transport with lazy per-peer connections and lock-free concurrent access.
///
/// Each peer gets a dedicated QUIC connection with a single bidirectional
/// stream for message framing (reusing `TcpFrameCodec`). Connections are
/// created lazily on first send and cleaned up on error.
pub struct QuicTransport {
    key: TransportKey,
    bind_addr: SocketAddr,
    local_address: WorkerAddress,

    // Peer registry and active connections
    peers: Arc<DashMap<crate::InstanceId, SocketAddr>>,
    connections: Arc<DashMap<crate::InstanceId, ConnectionHandle>>,

    // Runtime and shutdown
    runtime: OnceLock<tokio::runtime::Handle>,
    cancel_token: CancellationToken,
    shutdown_state: OnceLock<ShutdownState>,

    // Configuration
    channel_capacity: usize,
    connect_timeout: Duration,

    // QUIC-specific
    server_config: Mutex<Option<quinn::ServerConfig>>,
    client_config: quinn::ClientConfig,
    endpoint: OnceLock<quinn::Endpoint>,
    socket: Mutex<Option<std::net::UdpSocket>>,

    // Multi-NIC
    local_interfaces: OnceLock<Vec<InterfaceEndpoint>>,
    numa_hint: Option<u32>,

    // Observability
    observability: OnceLock<Arc<VeloMetrics>>,
    metrics: OnceLock<velo_observability::TransportMetricsHandle>,
}

/// Handle to a connection's writer task.
#[derive(Clone)]
struct ConnectionHandle {
    tx: flume::Sender<SendTask>,
}

/// Task sent to writer task containing message data.
struct SendTask {
    msg_type: MessageType,
    header: Bytes,
    payload: Bytes,
    on_error: Arc<dyn TransportErrorHandler>,
}

impl SendTask {
    fn on_error(self, error: impl Into<String>) {
        self.on_error
            .on_error(self.header, self.payload, error.into());
    }
}

/// Configuration for creating a `QuicTransport` (avoids too_many_arguments).
pub(crate) struct QuicTransportConfig {
    pub bind_addr: SocketAddr,
    pub key: TransportKey,
    pub local_address: WorkerAddress,
    pub channel_capacity: usize,
    pub connect_timeout: Duration,
    pub server_config: quinn::ServerConfig,
    pub client_config: quinn::ClientConfig,
    pub socket: Option<std::net::UdpSocket>,
    pub numa_hint: Option<u32>,
}

impl QuicTransport {
    /// Create a new QUIC transport (called by the builder).
    pub(crate) fn new(cfg: QuicTransportConfig) -> Self {
        Self {
            key: cfg.key,
            bind_addr: cfg.bind_addr,
            local_address: cfg.local_address,
            peers: Arc::new(DashMap::new()),
            connections: Arc::new(DashMap::new()),
            runtime: OnceLock::new(),
            cancel_token: CancellationToken::new(),
            shutdown_state: OnceLock::new(),
            channel_capacity: cfg.channel_capacity,
            connect_timeout: cfg.connect_timeout,
            server_config: Mutex::new(Some(cfg.server_config)),
            client_config: cfg.client_config,
            endpoint: OnceLock::new(),
            socket: Mutex::new(cfg.socket),
            local_interfaces: OnceLock::new(),
            numa_hint: cfg.numa_hint,
            observability: OnceLock::new(),
            metrics: OnceLock::new(),
        }
    }

    /// Get or create a connection to a peer (lazy initialization).
    fn get_or_create_connection(&self, instance_id: crate::InstanceId) -> Result<ConnectionHandle> {
        // Fast path: connection already exists and is alive
        if let Some(handle) = self.connections.get(&instance_id) {
            if !handle.tx.is_disconnected() {
                return Ok(handle.clone());
            }
            drop(handle);
            self.connections
                .remove_if(&instance_id, |_, h| h.tx.is_disconnected());
            self.update_connection_gauge();
        }

        let rt = self.runtime.get().ok_or(TransportError::NotStarted)?;

        // Atomic check-and-insert via entry API
        let handle = match self.connections.entry(instance_id) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                if !entry.get().tx.is_disconnected() {
                    entry.get().clone()
                } else {
                    let handle = self.create_connection(instance_id, rt)?;
                    entry.insert(handle.clone());
                    self.update_connection_gauge();
                    handle
                }
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let handle = self.create_connection(instance_id, rt)?;
                entry.insert(handle.clone());
                self.update_connection_gauge();
                handle
            }
        };

        Ok(handle)
    }

    /// Create a new connection handle and spawn the writer task.
    fn create_connection(
        &self,
        instance_id: crate::InstanceId,
        rt: &tokio::runtime::Handle,
    ) -> Result<ConnectionHandle> {
        let addr = *self
            .peers
            .get(&instance_id)
            .ok_or(TransportError::PeerNotRegistered(instance_id))?
            .value();

        let (tx, rx) = flume::bounded(self.channel_capacity);
        let handle = ConnectionHandle { tx };

        let cancel = self.cancel_token.clone();
        let conns = Arc::clone(&self.connections);
        let connect_timeout = self.connect_timeout;
        let metrics = self.metrics.get().cloned();
        let endpoint = self
            .endpoint
            .get()
            .ok_or(TransportError::NotStarted)?
            .clone();
        let client_config = self.client_config.clone();

        rt.spawn(connection_writer_task(WriterTaskParams {
            endpoint,
            client_config,
            addr,
            instance_id,
            rx,
            connections: conns,
            cancel_token: cancel,
            connect_timeout,
            metrics,
        }));

        debug!("Created new QUIC connection to {} ({})", instance_id, addr);
        Ok(handle)
    }

    fn update_peer_gauge(&self) {
        if let Some(metrics) = self.metrics.get() {
            metrics.set_registered_peers(self.peers.len());
        }
    }

    fn update_connection_gauge(&self) {
        if let Some(metrics) = self.metrics.get() {
            metrics.set_active_connections(self.connections.len());
        }
    }
}

impl Transport for QuicTransport {
    fn key(&self) -> TransportKey {
        self.key.clone()
    }

    fn address(&self) -> WorkerAddress {
        self.local_address.clone()
    }

    fn register(&self, peer_info: PeerInfo) -> Result<(), TransportError> {
        let endpoint = peer_info
            .worker_address()
            .get_entry(&self.key)
            .map_err(|_| TransportError::NoEndpoint)?
            .ok_or(TransportError::NoEndpoint)?;

        let remote_endpoints = parse_endpoints(&endpoint).map_err(|e| {
            error!("Failed to parse QUIC endpoint: {}", e);
            TransportError::InvalidEndpoint
        })?;

        let local = self.local_interfaces.get_or_init(|| {
            resolve_advertise_endpoints(self.bind_addr, &InterfaceFilter::All).unwrap_or_default()
        });

        let addr = select_best_endpoint(&remote_endpoints, local, self.numa_hint)
            .ok_or(TransportError::InvalidEndpoint)?;

        self.peers.insert(peer_info.instance_id(), addr);
        self.update_peer_gauge();

        debug!(
            "Registered QUIC peer {} at {}",
            peer_info.instance_id(),
            addr
        );
        Ok(())
    }

    #[inline]
    fn send_message(
        &self,
        instance_id: crate::InstanceId,
        header: Bytes,
        payload: Bytes,
        message_type: MessageType,
        on_error: Arc<dyn TransportErrorHandler>,
    ) {
        let send_msg = SendTask {
            msg_type: message_type,
            header,
            payload,
            on_error,
        };

        // Fast path: try to send on existing connection
        let send_msg = match self.connections.get(&instance_id) {
            Some(handle) => match handle.tx.try_send(send_msg) {
                Ok(()) => return,
                Err(flume::TrySendError::Full(send_msg)) => send_msg,
                Err(flume::TrySendError::Disconnected(send_msg)) => {
                    drop(handle);
                    self.connections
                        .remove_if(&instance_id, |_, h| h.tx.is_disconnected());
                    send_msg
                }
            },
            None => send_msg,
        };

        // Slow path: create new connection
        let rt = match self.runtime.get() {
            Some(rt) => rt,
            None => {
                send_msg.on_error("Transport not started");
                return;
            }
        };

        let handle = match self.get_or_create_connection(instance_id) {
            Ok(h) => h,
            Err(e) => {
                send_msg.on_error(format!("Failed to create connection: {}", e));
                return;
            }
        };

        rt.spawn(async move {
            if let Err(flume::SendError(send_msg)) = handle.tx.send_async(send_msg).await {
                send_msg.on_error("Connection closed");
            }
        });
    }

    fn start(
        &self,
        _instance_id: crate::InstanceId,
        channels: TransportAdapter,
        rt: tokio::runtime::Handle,
    ) -> futures::future::BoxFuture<'_, anyhow::Result<()>> {
        self.runtime.set(rt.clone()).ok();
        self.shutdown_state
            .set(channels.shutdown_state.clone())
            .ok();

        let bind_addr = self.bind_addr;
        let shutdown_state = channels.shutdown_state.clone();

        // Take the server config and socket (can only start once)
        let server_config = self
            .server_config
            .lock()
            .expect("server_config mutex poisoned")
            .take();
        let socket = self.socket.lock().expect("socket mutex poisoned").take();

        Box::pin(async move {
            let server_config =
                server_config.ok_or_else(|| anyhow::anyhow!("QUIC transport already started"))?;

            // Create the QUIC endpoint
            let endpoint = if let Some(socket) = socket {
                socket
                    .set_nonblocking(true)
                    .context("Failed to set UDP socket to non-blocking")?;
                let runtime = quinn::default_runtime()
                    .ok_or_else(|| anyhow::anyhow!("No async runtime found"))?;
                quinn::Endpoint::new(
                    quinn::EndpointConfig::default(),
                    Some(server_config),
                    socket,
                    runtime,
                )
                .context("Failed to create QUIC endpoint from socket")?
            } else {
                quinn::Endpoint::server(server_config, bind_addr)
                    .context(format!("Failed to bind QUIC endpoint to {}", bind_addr))?
            };

            self.endpoint.set(endpoint.clone()).ok();

            struct DefaultErrorHandler;
            impl TransportErrorHandler for DefaultErrorHandler {
                fn on_error(&self, _header: Bytes, _payload: Bytes, error: String) {
                    warn!("QUIC transport error: {}", error);
                }
            }

            let listener = QuicListener::new(
                endpoint,
                channels,
                Arc::new(DefaultErrorHandler),
                shutdown_state,
                self.key.as_str().to_string(),
                self.metrics.get().cloned(),
            );

            rt.spawn(async move {
                if let Err(e) = listener.run().await {
                    error!("QUIC listener error: {}", e);
                }
            });

            info!("QUIC transport started on {}", bind_addr);
            Ok(())
        })
    }

    fn begin_drain(&self) {
        // Per-frame gate in the listener handles drain — no-op here.
    }

    fn shutdown(&self) {
        info!("Shutting down QUIC transport");

        if let Some(state) = self.shutdown_state.get() {
            state.teardown_token().cancel();
        }
        self.cancel_token.cancel();

        // Close the endpoint gracefully
        if let Some(endpoint) = self.endpoint.get() {
            endpoint.close(quinn::VarInt::from_u32(0), b"shutdown");
        }

        self.connections.clear();
        self.update_connection_gauge();
    }

    fn set_observability(&self, observability: Arc<VeloMetrics>) {
        let _ = self
            .metrics
            .set(observability.bind_transport(self.key.as_str()));
        let _ = self.observability.set(observability);
        self.update_peer_gauge();
        self.update_connection_gauge();
    }

    fn check_health(
        &self,
        instance_id: crate::InstanceId,
        timeout: Duration,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), HealthCheckError>> + Send + '_>,
    > {
        Box::pin(async move {
            let connection_exists = self.connections.contains_key(&instance_id);

            if let Some(handle) = self.connections.get(&instance_id) {
                if !handle.tx.is_disconnected() {
                    return Ok(());
                }
                drop(handle);
                self.connections
                    .remove_if(&instance_id, |_, h| h.tx.is_disconnected());
            }

            let addr = *self
                .peers
                .get(&instance_id)
                .ok_or(HealthCheckError::PeerNotRegistered)?
                .value();

            let endpoint = self
                .endpoint
                .get()
                .ok_or(HealthCheckError::TransportNotStarted)?;

            let connecting =
                match endpoint.connect_with(self.client_config.clone(), addr, "localhost") {
                    Ok(c) => c,
                    Err(_) => return Err(HealthCheckError::ConnectionFailed),
                };

            match tokio::time::timeout(timeout, connecting).await {
                Ok(Ok(_conn)) => {
                    if connection_exists {
                        Ok(())
                    } else {
                        Err(HealthCheckError::NeverConnected)
                    }
                }
                Ok(Err(_)) => Err(HealthCheckError::ConnectionFailed),
                Err(_) => Err(HealthCheckError::Timeout),
            }
        })
    }
}

/// Parameters for the QUIC connection writer task.
struct WriterTaskParams {
    endpoint: quinn::Endpoint,
    client_config: quinn::ClientConfig,
    addr: SocketAddr,
    instance_id: crate::InstanceId,
    rx: flume::Receiver<SendTask>,
    connections: Arc<DashMap<crate::InstanceId, ConnectionHandle>>,
    cancel_token: CancellationToken,
    connect_timeout: Duration,
    metrics: Option<velo_observability::TransportMetricsHandle>,
}

/// Connection writer task.
///
/// Connects to the remote peer via QUIC, opens a bidirectional stream,
/// and sends framed messages until the channel closes or an error occurs.
async fn connection_writer_task(p: WriterTaskParams) -> Result<()> {
    let result = connection_writer_inner(
        &p.endpoint,
        p.client_config,
        p.addr,
        p.instance_id,
        &p.rx,
        &p.cancel_token,
        p.connect_timeout,
    )
    .await;

    // Drain queued messages and notify error handlers
    while let Ok(msg) = p.rx.try_recv() {
        msg.on_error("Connection closed");
    }

    drop(p.rx);
    p.connections
        .remove_if(&p.instance_id, |_, h| h.tx.is_disconnected());
    if let Some(metrics) = p.metrics.as_ref() {
        metrics.set_active_connections(p.connections.len());
    }

    debug!("QUIC connection to {} ({}) closed", p.instance_id, p.addr);
    result
}

/// Inner writer loop: connect, open stream, send frames.
async fn connection_writer_inner(
    endpoint: &quinn::Endpoint,
    client_config: quinn::ClientConfig,
    addr: SocketAddr,
    instance_id: crate::InstanceId,
    rx: &flume::Receiver<SendTask>,
    cancel_token: &CancellationToken,
    connect_timeout: Duration,
) -> Result<()> {
    debug!("QUIC connecting to {} ({})", instance_id, addr);

    let connecting = endpoint
        .connect_with(client_config, addr, "localhost")
        .context("QUIC connect failed")?;

    let connection = tokio::select! {
        _ = cancel_token.cancelled() => return Ok(()),
        res = tokio::time::timeout(connect_timeout, connecting) => {
            res.context("QUIC handshake timeout")?.context("QUIC handshake failed")?
        },
    };

    let (mut send_stream, _recv_stream) = tokio::select! {
        _ = cancel_token.cancelled() => return Ok(()),
        res = connection.open_bi() => {
            res.context("Failed to open QUIC bidirectional stream")?
        },
    };

    debug!("QUIC connected to {} ({})", instance_id, addr);

    loop {
        let msg = tokio::select! {
            _ = cancel_token.cancelled() => break,
            res = rx.recv_async() => match res {
                Ok(msg) => msg,
                Err(_) => break,
            },
        };
        if let Err(e) =
            TcpFrameCodec::encode_frame(&mut send_stream, msg.msg_type, &msg.header, &msg.payload)
                .await
        {
            error!("QUIC write error to {} ({}): {}", instance_id, addr, e);
            msg.on_error(format!("Failed to write to QUIC stream: {}", e));
            break;
        }
    }

    Ok(())
}

/// Builder for `QuicTransport`.
pub struct QuicTransportBuilder {
    bind_addr: Option<SocketAddr>,
    key: Option<TransportKey>,
    channel_capacity: usize,
    connect_timeout: Duration,
    socket: Option<std::net::UdpSocket>,
    server_cert: Option<(
        Vec<rustls::pki_types::CertificateDer<'static>>,
        rustls::pki_types::PrivateKeyDer<'static>,
    )>,
    interface_filter: InterfaceFilter,
    numa_hint: Option<u32>,
}

impl QuicTransportBuilder {
    /// Create a new builder with sensible defaults.
    pub fn new() -> Self {
        Self {
            bind_addr: None,
            key: None,
            channel_capacity: 256,
            connect_timeout: Duration::from_secs(5),
            socket: None,
            server_cert: None,
            interface_filter: InterfaceFilter::default(),
            numa_hint: None,
        }
    }

    /// Set the bind address.
    pub fn bind_addr(mut self, addr: SocketAddr) -> Self {
        self.bind_addr = Some(addr);
        self
    }

    /// Set the transport key (default: `"quic"`).
    pub fn key(mut self, key: TransportKey) -> Self {
        self.key = Some(key);
        self
    }

    /// Set the channel capacity for backpressure (default: 256).
    pub fn channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }

    /// Set the connect timeout for outbound connections (default: 5s).
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set the interface selection filter for multi-NIC environments.
    pub fn interface_filter(mut self, filter: InterfaceFilter) -> Self {
        self.interface_filter = filter;
        self
    }

    /// Set the NUMA node hint for topology-aware NIC selection.
    pub fn numa_hint(mut self, node: u32) -> Self {
        self.numa_hint = Some(node);
        self
    }

    /// Provide a TLS certificate and private key for the server.
    ///
    /// If not provided, a self-signed certificate will be generated.
    pub fn server_cert(
        mut self,
        certs: Vec<rustls::pki_types::CertificateDer<'static>>,
        key: rustls::pki_types::PrivateKeyDer<'static>,
    ) -> Self {
        self.server_cert = Some((certs, key));
        self
    }

    /// Use a pre-bound UDP socket instead of binding to a specific address.
    ///
    /// This is useful for tests binding to port 0 to avoid TOCTOU races.
    /// Mutually exclusive with `bind_addr()`.
    pub fn from_socket(mut self, socket: std::net::UdpSocket) -> Result<Self> {
        if self.bind_addr.is_some() {
            anyhow::bail!(
                "Cannot use both bind_addr() and from_socket() - they are mutually exclusive"
            );
        }
        let addr = socket
            .local_addr()
            .context("Failed to get local address from socket")?;
        self.bind_addr = Some(addr);
        self.socket = Some(socket);
        Ok(self)
    }

    /// Build the `QuicTransport`.
    pub fn build(self) -> Result<QuicTransport> {
        // Install the default crypto provider if not already set
        let _ = rustls::crypto::ring::default_provider().install_default();

        let key = self.key.unwrap_or_else(|| TransportKey::from("quic"));

        // Pre-bind UDP socket if none provided
        let (bind_addr, socket) = if let Some(socket) = self.socket {
            let addr = socket.local_addr()?;
            (addr, Some(socket))
        } else {
            let requested = self
                .bind_addr
                .unwrap_or_else(|| "0.0.0.0:0".parse().unwrap());
            let socket =
                std::net::UdpSocket::bind(requested).context("Failed to pre-bind UDP socket")?;
            let actual = socket.local_addr()?;
            (actual, Some(socket))
        };

        // Generate or use provided TLS certs
        let (certs, cert_key) = match self.server_cert {
            Some((certs, key)) => (certs, key),
            None => tls::generate_self_signed_cert()
                .context("Failed to generate self-signed certificate")?,
        };

        let server_config =
            tls::make_server_config(certs, cert_key).context("Failed to create server config")?;
        let client_config =
            tls::make_client_config_insecure().context("Failed to create client config")?;

        // Resolve advertise endpoints
        let endpoints = resolve_advertise_endpoints(bind_addr, &self.interface_filter)?;

        let encoded =
            rmp_serde::to_vec(&endpoints).context("Failed to encode interface endpoints")?;
        let mut addr_builder = crate::address::WorkerAddressBuilder::new();
        addr_builder.add_entry(key.clone(), encoded)?;
        let local_address = addr_builder.build()?;

        Ok(QuicTransport::new(QuicTransportConfig {
            bind_addr,
            key,
            local_address,
            channel_capacity: self.channel_capacity,
            connect_timeout: self.connect_timeout,
            server_config,
            client_config,
            socket,
            numa_hint: self.numa_hint,
        }))
    }
}

impl Default for QuicTransportBuilder {
    fn default() -> Self {
        Self::new()
    }
}
