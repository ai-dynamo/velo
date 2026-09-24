// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The QUIC transport and its builder.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bytes::Bytes;
use dashmap::DashMap;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use velo_ext::{MessageType, PeerInfo, Transport, TransportAdapter, TransportKey, WorkerAddress};

use crate::transports::coalesce::{
    Coalescable, EgressMetrics, WriterFailure, WriterObserver, run_coalescing_writer,
};
use crate::transports::ingress::{DialedReaderContext, run_dialed_reader};
use crate::transports::tcp::framing::{DEFAULT_MAX_FRAME_SIZE, DEFAULT_SHRINK_THRESHOLD};
use crate::transports::transport::{
    AdmissionError, AdmissionGate, HealthCheckError, SendOutcome, ShutdownState, TransportError,
    TransportErrorHandler,
};
use crate::transports::utils::interfaces::{
    InterfaceEndpoint, InterfaceFilter, resolve_advertise_endpoints, select_best_endpoint,
};

use super::endpoint::{BufferSizes, QuicEndpointInfo, bind_client_socket, bind_server_sockets};
use super::listener::{AcceptContext, OrderlyEnd, run_accept_loop};
use super::tls::{self, Identity};

/// How long a writer that stops waits for the peer to acknowledge its last
/// bytes before it closes the connection, on every stop including teardown.
/// Closing at once discards what the peer has not acknowledged, which TCP
/// would still deliver after a close. `shutdown()` closes the dial endpoint
/// only after the writers have had this long.
const FINISH_GRACE: Duration = Duration::from_secs(1);

/// Default QUIC idle timeout: three keep-alives. quinn ignores ICMP
/// unreachable for liveness, so a peer that dies without closing is found only
/// by this timeout; quinn's own default is 30 s.
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(15);

/// QUIC messenger transport.
///
/// One connection per peer, dialed lazily on the first send, with one
/// bidirectional stream. See the module docs for the design.
pub struct QuicTransport {
    key: TransportKey,
    bind_addr: SocketAddr,
    local_address: WorkerAddress,
    fingerprint: tls::Fingerprint,
    shrink_threshold: usize,

    peers: Arc<DashMap<crate::InstanceId, PeerEntry>>,
    connections: Arc<DashMap<crate::InstanceId, ConnectionHandle>>,

    runtime: OnceLock<tokio::runtime::Handle>,
    cancel_token: CancellationToken,
    shutdown_state: OnceLock<ShutdownState>,

    channel_capacity: usize,
    connect_timeout: Duration,

    /// Sockets bound in `build()`, so a port-0 bind resolves before the
    /// address is advertised. `start()` takes them to create the endpoints,
    /// which needs a runtime.
    server_sockets: Mutex<Option<Vec<std::net::UdpSocket>>>,
    client_socket: Mutex<Option<std::net::UdpSocket>>,
    server_config: quinn::ServerConfig,
    transport_config: Arc<quinn::TransportConfig>,
    endpoint_config: quinn::EndpointConfig,
    server_endpoints: OnceLock<Vec<quinn::Endpoint>>,
    client_endpoint: OnceLock<quinn::Endpoint>,

    local_interfaces: OnceLock<Vec<InterfaceEndpoint>>,
    numa_hint: Option<u32>,

    metrics: OnceLock<Arc<dyn velo_ext::TransportObservability>>,
    dialed_ctx: OnceLock<DialedReaderContext>,
}

/// A registered peer: where to dial, and the TLS config that pins its
/// certificate.
#[derive(Clone)]
struct PeerEntry {
    addr: SocketAddr,
    client_config: quinn::ClientConfig,
}

/// Handle to one connection's writer task. One handle is one connection
/// epoch, as in the TCP transport.
#[derive(Clone)]
struct ConnectionHandle {
    tx: flume::Sender<SendTask>,
    gate: AdmissionGate<SendTask>,
}

impl ConnectionHandle {
    fn retire(&self) {
        self.gate.fail_all(AdmissionError::ConnectionReplaced);
    }
}

struct SendTask {
    msg_type: MessageType,
    header: Bytes,
    payload: Bytes,
    on_error: Arc<dyn TransportErrorHandler>,
    /// Stamped before the gate, and only when a writer reports queue wait.
    queued_at: Option<Instant>,
}

impl SendTask {
    fn on_error(self, error: impl Into<String>) {
        self.on_error
            .on_error(self.header, self.payload, error.into());
    }
}

impl QuicTransport {
    /// The SHA-256 fingerprint of this transport's certificate.
    pub fn fingerprint(&self) -> tls::Fingerprint {
        self.fingerprint
    }

    fn reap_stale_connection(&self, instance_id: crate::InstanceId) {
        if let Some((_, stale)) = self
            .connections
            .remove_if(&instance_id, |_, h| h.tx.is_disconnected())
        {
            stale.retire();
            self.update_connection_gauge();
        }
    }

    fn get_or_create_connection(&self, instance_id: crate::InstanceId) -> Result<ConnectionHandle> {
        if let Some(handle) = self.connections.get(&instance_id) {
            if !handle.tx.is_disconnected() {
                return Ok(handle.clone());
            }
            drop(handle);
            self.reap_stale_connection(instance_id);
        }

        let rt = self.runtime.get().ok_or(TransportError::NotStarted)?;
        self.install_connection(instance_id, rt)
    }

    /// Put a live connection in the map for `instance_id`: the one already
    /// there if it is live, else a new one.
    fn install_connection(
        &self,
        instance_id: crate::InstanceId,
        rt: &tokio::runtime::Handle,
    ) -> Result<ConnectionHandle> {
        let handle = match self.connections.entry(instance_id) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                if !entry.get().tx.is_disconnected() {
                    entry.get().clone()
                } else {
                    entry.get().retire();
                    let handle = self.create_connection(instance_id, rt)?;
                    entry.insert(handle.clone());
                    handle
                }
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let handle = self.create_connection(instance_id, rt)?;
                entry.insert(handle.clone());
                handle
            }
        };
        // After the match, not inside it: the gauge reads `len()`, which
        // read-locks every shard, and an occupied entry still holds its
        // shard's write lock, which is not reentrant.
        self.update_connection_gauge();
        Ok(handle)
    }

    fn create_connection(
        &self,
        instance_id: crate::InstanceId,
        rt: &tokio::runtime::Handle,
    ) -> Result<ConnectionHandle> {
        let peer = self
            .peers
            .get(&instance_id)
            .ok_or(TransportError::PeerNotRegistered(instance_id))?
            .value()
            .clone();
        let endpoint = self
            .client_endpoint
            .get()
            .ok_or(TransportError::NotStarted)?
            .clone();

        let (tx, rx) = flume::bounded(self.channel_capacity);
        let handle = ConnectionHandle {
            gate: AdmissionGate::new(tx.clone(), rt.clone()),
            tx,
        };

        rt.spawn(connection_writer_task(
            instance_id,
            rx,
            WriterTaskContext {
                endpoint,
                peer,
                connections: Arc::clone(&self.connections),
                cancel_token: self.cancel_token.clone(),
                connect_timeout: self.connect_timeout,
                reader_ctx: self.dialed_ctx.get().cloned(),
                metrics: self.metrics.get().cloned(),
            },
        ));
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

    fn slow_path_send(&self, instance_id: crate::InstanceId, send_msg: SendTask) -> SendOutcome {
        if self.runtime.get().is_none() {
            send_msg.on_error("Transport not started");
            return SendOutcome::Admitted;
        }
        if self.cancel_token.is_cancelled() {
            send_msg.on_error("Transport shut down");
            return SendOutcome::Admitted;
        }
        match self.get_or_create_connection(instance_id) {
            Ok(handle) => self.admit(&handle, send_msg),
            Err(e) => {
                send_msg.on_error(format!("Failed to create connection: {e:#}"));
                SendOutcome::Admitted
            }
        }
    }

    fn admit(&self, handle: &ConnectionHandle, send_msg: SendTask) -> SendOutcome {
        let outcome = handle.gate.send(send_msg);
        if let Some(m) = self.metrics.get()
            && !outcome.is_admitted()
        {
            m.record_send_backpressure();
        }
        outcome
    }

    /// Create the quinn endpoints from the sockets bound in `build()`.
    /// Must run inside the runtime: quinn spawns each endpoint's driver.
    fn create_endpoints(&self) -> Result<Vec<quinn::Endpoint>> {
        let server_sockets = self
            .server_sockets
            .lock()
            .expect("QUIC socket mutex poisoned")
            .take()
            .context("QUIC transport already started")?;
        let client_socket = self
            .client_socket
            .lock()
            .expect("QUIC socket mutex poisoned")
            .take()
            .context("QUIC transport already started")?;
        let runtime = Arc::new(quinn::TokioRuntime);

        let mut servers = Vec::with_capacity(server_sockets.len());
        for socket in server_sockets {
            servers.push(
                quinn::Endpoint::new(
                    self.endpoint_config.clone(),
                    Some(self.server_config.clone()),
                    socket,
                    runtime.clone(),
                )
                .context("failed to create a QUIC server endpoint")?,
            );
        }
        // No default client config: every dial passes the peer's pinned one,
        // and a dial that does not fails instead of trusting anything.
        let client =
            quinn::Endpoint::new(self.endpoint_config.clone(), None, client_socket, runtime)
                .context("failed to create the QUIC client endpoint")?;
        let _ = self.client_endpoint.set(client);
        Ok(servers)
    }
}

impl Transport for QuicTransport {
    fn key(&self) -> TransportKey {
        self.key.clone()
    }

    fn address(&self) -> WorkerAddress {
        self.local_address.clone()
    }

    /// Frames use the TCP codec, so the ceiling is the codec's.
    fn max_message_size(&self, _target: crate::InstanceId) -> Option<usize> {
        Some(DEFAULT_MAX_FRAME_SIZE as usize)
    }

    fn register(&self, peer_info: PeerInfo) -> Result<(), TransportError> {
        let raw = peer_info
            .worker_address()
            .get_entry(&self.key)
            .map_err(|_| TransportError::NoEndpoint)?
            .ok_or(TransportError::NoEndpoint)?;
        let info = QuicEndpointInfo::decode(&raw).map_err(|e| {
            error!("Failed to parse QUIC endpoint: {e:#}");
            TransportError::InvalidEndpoint
        })?;
        let local = self.local_interfaces.get_or_init(|| {
            resolve_advertise_endpoints(self.bind_addr, &InterfaceFilter::All).unwrap_or_default()
        });
        let addr = select_best_endpoint(&info.endpoints, local, self.numa_hint)
            .ok_or(TransportError::InvalidEndpoint)?;
        let mut client_config = tls::pinned_client_config(info.fingerprint).map_err(|e| {
            error!("Failed to build the QUIC client config: {e:#}");
            TransportError::InvalidEndpoint
        })?;
        client_config.transport_config(self.transport_config.clone());

        self.peers.insert(
            peer_info.instance_id(),
            PeerEntry {
                addr,
                client_config,
            },
        );
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
    ) -> SendOutcome {
        let send_msg = SendTask {
            msg_type: message_type,
            header,
            payload,
            on_error,
            queued_at: self.metrics.get().map(|_| Instant::now()),
        };
        if let Some(handle) = self.connections.get(&instance_id) {
            let live = (!handle.tx.is_disconnected()).then(|| handle.clone());
            drop(handle);
            match live {
                Some(handle) => return self.admit(&handle, send_msg),
                None => self.reap_stale_connection(instance_id),
            }
        }
        self.slow_path_send(instance_id, send_msg)
    }

    fn start(
        &self,
        _instance_id: crate::InstanceId,
        channels: TransportAdapter,
        rt: tokio::runtime::Handle,
    ) -> futures::future::BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            let servers = {
                let _guard = rt.enter();
                self.create_endpoints()?
            };

            // Set before `runtime`: send paths gate on `runtime`, so every
            // connection writer sees the reader context.
            self.dialed_ctx
                .set(DialedReaderContext {
                    adapter: channels.clone(),
                    error_handler: Arc::new(LogErrorHandler),
                    transport_key: self.key.as_str().to_string(),
                    shrink_threshold: self.shrink_threshold,
                })
                .ok();
            self.shutdown_state
                .set(channels.shutdown_state.clone())
                .ok();

            let ctx = AcceptContext {
                teardown: channels.shutdown_state.teardown_token().clone(),
                adapter: channels,
                error_handler: Arc::new(LogErrorHandler),
                transport_key: self.key.as_str().to_string(),
                metrics: self.metrics.get().cloned(),
                shrink_threshold: self.shrink_threshold,
            };
            for endpoint in &servers {
                rt.spawn(run_accept_loop(endpoint.clone(), ctx.clone()));
            }
            let _ = self.server_endpoints.set(servers);
            self.runtime.set(rt).ok();

            info!("QUIC transport started on {}", self.bind_addr);
            Ok(())
        })
    }

    fn begin_drain(&self) {
        // Admission in the listener handles drain.
    }

    fn shutdown(&self) {
        info!("Shutting down QUIC transport");
        if let Some(state) = self.shutdown_state.get() {
            state.teardown_token().cancel();
        }
        self.cancel_token.cancel();
        for endpoint in self.server_endpoints.get().into_iter().flatten() {
            endpoint.close(quinn::VarInt::from_u32(0), b"shutdown");
        }
        // Not the dial endpoint yet: closing it now would discard what the
        // writers wrote but the peers have not acknowledged. Each writer
        // finishes its stream and closes its connection within FINISH_GRACE.
        if let Some(endpoint) = self.client_endpoint.get().cloned() {
            match self.runtime.get() {
                Some(rt) => {
                    rt.spawn(async move {
                        let _ = tokio::time::timeout(FINISH_GRACE * 2, endpoint.wait_idle()).await;
                        endpoint.close(quinn::VarInt::from_u32(0), b"shutdown");
                    });
                }
                None => endpoint.close(quinn::VarInt::from_u32(0), b"shutdown"),
            }
        }
        self.connections.clear();
        self.update_connection_gauge();
    }

    fn set_observability(&self, observability: Arc<dyn velo_ext::TransportObservability>) {
        let _ = self.metrics.set(observability);
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
                self.reap_stale_connection(instance_id);
            }

            let peer = self
                .peers
                .get(&instance_id)
                .ok_or(HealthCheckError::PeerNotRegistered)?
                .value()
                .clone();
            let endpoint = self
                .client_endpoint
                .get()
                .ok_or(HealthCheckError::ConnectionFailed)?;
            let connecting = endpoint
                .connect_with(peer.client_config, peer.addr, tls::SERVER_NAME)
                .map_err(|_| HealthCheckError::ConnectionFailed)?;
            match tokio::time::timeout(timeout, connecting).await {
                Ok(Ok(connection)) => {
                    connection.close(quinn::VarInt::from_u32(0), b"health check");
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

struct WriterTaskContext {
    endpoint: quinn::Endpoint,
    peer: PeerEntry,
    connections: Arc<DashMap<crate::InstanceId, ConnectionHandle>>,
    cancel_token: CancellationToken,
    connect_timeout: Duration,
    reader_ctx: Option<DialedReaderContext>,
    metrics: Option<Arc<dyn velo_ext::TransportObservability>>,
}

/// Dial, then write frames until the channel closes, a write fails, or the
/// peer ends the stream. Cleanup runs even if the dial fails.
async fn connection_writer_task(
    instance_id: crate::InstanceId,
    rx: flume::Receiver<SendTask>,
    ctx: WriterTaskContext,
) {
    let addr = ctx.peer.addr;
    let connections = Arc::clone(&ctx.connections);
    let metrics = ctx.metrics.clone();
    if let Err(e) = connection_writer_inner(instance_id, &rx, ctx).await {
        warn!("QUIC: connection to {instance_id} ({addr}) failed: {e:#}");
    }

    // Drain queued messages and notify their error handlers. The same small
    // race as the TCP writer: a sender can `try_send` between the drain and
    // `drop(rx)`, and that one message is dropped silently (see the TODO in
    // `tcp/transport.rs`).
    while let Ok(msg) = rx.try_recv() {
        msg.on_error("Connection closed");
    }
    drop(rx);
    if let Some((_, stale)) = connections.remove_if(&instance_id, |_, h| h.tx.is_disconnected()) {
        stale.retire();
    }
    if let Some(metrics) = metrics.as_ref() {
        metrics.set_active_connections(connections.len());
    }
    debug!("QUIC connection to {instance_id} ({addr}) closed");
}

async fn connection_writer_inner(
    instance_id: crate::InstanceId,
    rx: &flume::Receiver<SendTask>,
    ctx: WriterTaskContext,
) -> Result<()> {
    let WriterTaskContext {
        endpoint,
        peer,
        cancel_token,
        connect_timeout,
        reader_ctx,
        metrics,
        ..
    } = ctx;

    let connecting = endpoint
        .connect_with(peer.client_config, peer.addr, tls::SERVER_NAME)
        .context("failed to start the QUIC handshake")?;
    let connection = tokio::select! {
        _ = cancel_token.cancelled() => return Ok(()),
        res = tokio::time::timeout(connect_timeout, connecting) => {
            res.context("connect timeout")?.context("QUIC handshake failed")?
        },
    };
    let (mut send, recv) = connection
        .open_bi()
        .await
        .context("failed to open the QUIC stream")?;
    debug!("QUIC connected to {instance_id} ({})", peer.addr);

    // The peer's listener writes `ShuttingDown` echoes back on this stream.
    // The reader routes them, and cancels `conn_cancel` when the peer ends
    // the stream so the writer stops too.
    let conn_cancel = cancel_token.child_token();
    let reader = reader_ctx.map(|reader_ctx| {
        tokio::spawn(run_dialed_reader(
            OrderlyEnd(recv),
            reader_ctx,
            metrics.clone(),
            conn_cancel.clone(),
            format!("{instance_id} ({})", peer.addr),
        ))
    });

    run_coalescing_writer(
        &mut send,
        rx,
        std::convert::identity,
        Some(&conn_cancel),
        &QuicWriterObserver {
            instance_id,
            addr: peer.addr,
            egress: metrics.map(EgressMetrics::new),
        },
    )
    .await;

    // Finish the stream and give the peer a moment to acknowledge it, on every
    // end including teardown, so the close does not discard frames the writer
    // already wrote. See FINISH_GRACE.
    if send.finish().is_ok() {
        let _ = tokio::time::timeout(FINISH_GRACE, send.stopped()).await;
    }
    if let Some(reason) = connection.close_reason() {
        debug!("QUIC connection to {instance_id} closed by peer: {reason}");
    }
    connection.close(quinn::VarInt::from_u32(0), b"done");
    if let Some(reader) = reader {
        reader.abort();
    }
    Ok(())
}

struct LogErrorHandler;

impl TransportErrorHandler for LogErrorHandler {
    fn on_error(&self, _header: Bytes, _payload: Bytes, error: String) {
        warn!("QUIC transport error: {error}");
    }
}

impl Coalescable for SendTask {
    /// The task is its own failure token, as in the TCP and UDS transports.
    type FailureToken = Self;

    fn msg_type(&self) -> MessageType {
        self.msg_type
    }

    fn header(&self) -> &[u8] {
        &self.header
    }

    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn queued_at(&self) -> Option<Instant> {
        self.queued_at
    }

    fn into_failure_token(self) -> Self {
        self
    }

    fn fail(token: Self, reason: &str) {
        token.on_error(format!("Failed to write to QUIC stream: {reason}"));
    }
}

struct QuicWriterObserver {
    instance_id: crate::InstanceId,
    addr: SocketAddr,
    egress: Option<EgressMetrics>,
}

impl WriterObserver for QuicWriterObserver {
    fn on_failure(&self, kind: WriterFailure, err: &std::io::Error, frames: usize) {
        match kind {
            WriterFailure::Write => error!(
                "QUIC write error to {} ({}): {err:#} ({frames} message(s) in batch)",
                self.instance_id, self.addr
            ),
            WriterFailure::Encode => error!(
                "QUIC encode error to {} ({}): {err:#}",
                self.instance_id, self.addr
            ),
        }
    }

    fn egress(&self) -> Option<&EgressMetrics> {
        self.egress.as_ref()
    }
}

/// Builder for [`QuicTransport`].
pub struct QuicTransportBuilder {
    bind_addr: Option<SocketAddr>,
    key: Option<TransportKey>,
    channel_capacity: usize,
    connect_timeout: Duration,
    interface_filter: InterfaceFilter,
    numa_hint: Option<u32>,
    server_endpoints: usize,
    udp_buffers: BufferSizes,
    stream_receive_window: Option<u32>,
    max_mtu: Option<u16>,
    keep_alive_interval: Duration,
    idle_timeout: Duration,
    shrink_threshold: usize,
}

impl QuicTransportBuilder {
    /// A builder with the defaults below.
    pub fn new() -> Self {
        Self {
            bind_addr: None,
            key: None,
            channel_capacity: 256,
            connect_timeout: Duration::from_secs(5),
            interface_filter: InterfaceFilter::default(),
            numa_hint: None,
            server_endpoints: DEFAULT_SERVER_ENDPOINTS,
            udp_buffers: BufferSizes {
                recv: DEFAULT_UDP_RECV_BUFFER,
                send: DEFAULT_UDP_SEND_BUFFER,
            },
            stream_receive_window: None,
            max_mtu: None,
            keep_alive_interval: Duration::from_secs(5),
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            shrink_threshold: DEFAULT_SHRINK_THRESHOLD,
        }
    }

    /// The UDP address to bind (default `0.0.0.0:0`).
    pub fn bind_addr(mut self, addr: SocketAddr) -> Self {
        self.bind_addr = Some(addr);
        self
    }

    /// The transport key (default `quic`).
    pub fn key(mut self, key: TransportKey) -> Self {
        self.key = Some(key);
        self
    }

    /// Capacity of each connection's send channel (default 256).
    pub fn channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }

    /// Handshake timeout for outbound connections (default 5 s).
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Interface selection for multi-NIC hosts.
    pub fn interface_filter(mut self, filter: InterfaceFilter) -> Self {
        self.interface_filter = filter;
        self
    }

    /// NUMA node hint for NIC selection.
    pub fn numa_hint(mut self, node: u32) -> Self {
        self.numa_hint = Some(node);
        self
    }

    /// Number of server sockets in the `SO_REUSEPORT` group (Linux only;
    /// default 4, ignored elsewhere).
    ///
    /// A node that many peers send to at once (a frontend) gains from more:
    /// each socket has its own receive queue and buffer ceiling. Each socket
    /// also costs a quinn endpoint and its receive buffers.
    pub fn server_endpoints(mut self, count: usize) -> Self {
        self.server_endpoints = count.max(1);
        self
    }

    /// Requested `SO_RCVBUF` and `SO_SNDBUF` for every UDP socket (default
    /// 8 MiB and 4 MiB). The kernel clamps them to `net.core.rmem_max` and
    /// `net.core.wmem_max`; a clamp is logged at startup.
    pub fn udp_buffer_sizes(mut self, recv: usize, send: usize) -> Self {
        self.udp_buffers = BufferSizes { recv, send };
        self
    }

    /// Per-stream flow-control window in bytes (default: quinn's).
    pub fn stream_receive_window(mut self, bytes: u32) -> Self {
        self.stream_receive_window = Some(bytes);
        self
    }

    /// Largest UDP payload this transport sends or accepts (default: quinn's
    /// 1452 for probing and 1472 to receive, sized for a 1500-byte Ethernet
    /// MTU).
    ///
    /// Each QUIC packet is encrypted and handled on its own, so larger packets
    /// cost less per byte. Values above 6550 are lowered to 6550 (see
    /// `GSO_SAFE_MAX_MTU`), so on a jumbo-frame network set 6550.
    /// Path MTU discovery still probes up to this bound, so a smaller real
    /// path MTU is found, not assumed.
    pub fn max_mtu(mut self, bytes: u16) -> Self {
        self.max_mtu = Some(bytes);
        self
    }

    /// QUIC keep-alive interval (default 5 s).
    pub fn keep_alive_interval(mut self, interval: Duration) -> Self {
        self.keep_alive_interval = interval;
        self
    }

    /// How long a connection may go without receiving anything before it is
    /// closed (default 15 s, three keep-alives). This is how a peer that died
    /// without closing is found; its epoch fails at this point, and the
    /// frames queued on it go to their error handlers.
    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Read-buffer size above which a reader gives the excess back after a
    /// frame (default: the TCP transport's). The codec reserves the whole
    /// frame length, so without this one large frame pins that much memory
    /// per connection for its life.
    pub fn shrink_threshold(mut self, bytes: usize) -> Self {
        self.shrink_threshold = bytes;
        self
    }

    /// Bind the sockets, make the certificate, and build the transport.
    pub fn build(self) -> Result<QuicTransport> {
        let key = self.key.unwrap_or_else(|| TransportKey::from("quic"));
        let requested = self
            .bind_addr
            .unwrap_or_else(|| "0.0.0.0:0".parse().unwrap());

        let server_sockets =
            bind_server_sockets(requested, self.server_endpoints, self.udp_buffers)?;
        let bind_addr = server_sockets[0].local_addr()?;
        let client_socket = bind_client_socket(bind_addr, self.udp_buffers)?;

        let mut transport_config = quinn::TransportConfig::default();
        // One bidirectional stream per connection. Datagrams and
        // unidirectional streams are unused.
        transport_config.max_concurrent_bidi_streams(1u32.into());
        transport_config.max_concurrent_uni_streams(0u32.into());
        transport_config.datagram_receive_buffer_size(None);
        transport_config.keep_alive_interval(Some(self.keep_alive_interval));
        transport_config.max_idle_timeout(Some(
            self.idle_timeout
                .try_into()
                .context("QUIC idle_timeout is out of range")?,
        ));
        if let Some(window) = self.stream_receive_window {
            transport_config.stream_receive_window(window.into());
        }
        let mut endpoint_config = quinn::EndpointConfig::default();
        let max_mtu = self.max_mtu.map(clamp_to_gso_batch);
        if let Some(max_mtu) = max_mtu {
            endpoint_config
                .max_udp_payload_size(max_mtu.max(1200))
                .context("QUIC max_mtu is out of range")?;
            let mut discovery = quinn::MtuDiscoveryConfig::default();
            discovery.upper_bound(max_mtu);
            transport_config.mtu_discovery_config(Some(discovery));
        }
        let transport_config = Arc::new(transport_config);

        let identity = Identity::generate()?;
        let crypto = tls::server_crypto(&identity)?;
        let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(crypto)
                .context("failed to build the QUIC server TLS config")?,
        ));
        server_config.transport_config(transport_config.clone());

        let endpoints = resolve_advertise_endpoints(bind_addr, &self.interface_filter)?;
        let info = QuicEndpointInfo {
            endpoints,
            fingerprint: identity.fingerprint,
        };
        let mut addr_builder = crate::transports::address::WorkerAddressBuilder::new();
        addr_builder.add_entry(key.clone(), info.encode()?)?;
        let local_address = addr_builder.build()?;

        Ok(QuicTransport {
            key,
            bind_addr,
            local_address,
            fingerprint: identity.fingerprint,
            shrink_threshold: self.shrink_threshold,
            peers: Arc::new(DashMap::new()),
            connections: Arc::new(DashMap::new()),
            runtime: OnceLock::new(),
            cancel_token: CancellationToken::new(),
            shutdown_state: OnceLock::new(),
            channel_capacity: self.channel_capacity,
            connect_timeout: self.connect_timeout,
            server_sockets: Mutex::new(Some(server_sockets)),
            client_socket: Mutex::new(Some(client_socket)),
            server_config,
            transport_config,
            endpoint_config,
            server_endpoints: OnceLock::new(),
            client_endpoint: OnceLock::new(),
            local_interfaces: OnceLock::new(),
            numa_hint: self.numa_hint,
            metrics: OnceLock::new(),
            dialed_ctx: OnceLock::new(),
        })
    }
}

impl Default for QuicTransportBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Server sockets in the reuse-port group by default. Dynamo's QUIC plane
/// measured 8 and 32 on a frontend; 32 cost about 50 MiB of RSS. A frontend
/// sets a higher count with [`QuicTransportBuilder::server_endpoints`].
const DEFAULT_SERVER_ENDPOINTS: usize = 4;
/// Largest packet size at which quinn's send batches still fit in one UDP
/// datagram: 65507 bytes of UDP payload over quinn's 10-packet batch.
///
/// quinn hands up to 10 packets to one GSO send and does not cap the batch
/// size. Above this size a full batch exceeds the UDP limit, the kernel
/// answers `EMSGSIZE`, and quinn-udp 0.5 treats that as success because it
/// expects it only from MTU probes. The whole batch is lost without a trace,
/// and every flight waits out a loss-probe timeout. Measured on loopback with
/// 64 KiB messages: 2,666 msg/s at 6550 bytes, 115 msg/s at 6560.
pub(super) const GSO_SAFE_MAX_MTU: u16 = (65_507 / 10) as u16;

pub(super) fn clamp_to_gso_batch(requested: u16) -> u16 {
    if requested > GSO_SAFE_MAX_MTU {
        warn!(
            requested,
            used = GSO_SAFE_MAX_MTU,
            "QUIC max_mtu lowered: quinn's 10-packet send batch must fit one UDP datagram"
        );
    }
    requested.min(GSO_SAFE_MAX_MTU)
}

const DEFAULT_UDP_RECV_BUFFER: usize = 8 * 1024 * 1024;
const DEFAULT_UDP_SEND_BUFFER: usize = 4 * 1024 * 1024;

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
