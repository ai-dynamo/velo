// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! gRPC transport implementation using tonic bidirectional streaming.
//!
//! This transport wraps the same framing protocol used by TCP/UDS transports
//! inside gRPC `FramedData` messages. Outbound messages are sent via a
//! `VeloStreamingClient` bidi stream; inbound messages arrive through the
//! tonic server (`VeloStreamingService`).

use crate::observability::{Direction, TransportRejection};
use anyhow::{Context, Result};
use bytes::Bytes;
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, info, warn};

use crate::transports::tcp::TcpFrameCodec;
use crate::transports::transport::{
    AdmissionError, AdmissionGate, HealthCheckError, SendOutcome, ShutdownState, TransportError,
    TransportErrorHandler,
};
use crate::transports::utils::interfaces::{
    InterfaceEndpoint, InterfaceFilter, parse_endpoints, resolve_advertise_endpoints,
    select_best_endpoint,
};
use velo_ext::{MessageType, PeerInfo, Transport, TransportAdapter, TransportKey, WorkerAddress};

use super::proto;
use super::proto::velo_streaming_client::VeloStreamingClient;
use super::proto::velo_streaming_server::VeloStreamingServer;
use super::server::VeloStreamingService;

/// gRPC transport using tonic bidirectional streaming.
///
/// Follows the same structural patterns as `TcpTransport`: lazy per-peer
/// connections via `DashMap`, writer tasks fed through flume channels, and
/// 3-phase graceful shutdown.
pub struct GrpcTransport {
    /// Transport identity key (e.g. `"grpc"`).
    key: TransportKey,
    /// Address to bind the tonic server to.
    bind_addr: SocketAddr,
    /// Advertised address included in `WorkerAddress`.
    local_address: WorkerAddress,

    /// Registered peer addresses (instance_id -> socket address).
    peers: Arc<DashMap<crate::InstanceId, SocketAddr>>,
    /// Active outbound connections (instance_id -> writer handle).
    connections: Arc<DashMap<crate::InstanceId, ConnectionHandle>>,

    /// Tokio runtime handle, set during `start()`.
    runtime: OnceLock<tokio::runtime::Handle>,

    /// Per-transport cancellation token for writer tasks.
    cancel_token: tokio_util::sync::CancellationToken,
    /// Shared shutdown coordinator, set during `start()`.
    shutdown_state: OnceLock<ShutdownState>,

    /// Backpressure capacity for per-connection writer channels.
    channel_capacity: usize,
    /// Timeout for outbound gRPC connections.
    connect_timeout: Duration,

    /// Transport adapter for routing inbound frames (set during `start()`).
    adapter: OnceLock<TransportAdapter>,

    /// Optional pre-bound listener (used for tests to avoid port races).
    listener: std::sync::Mutex<Option<std::net::TcpListener>>,

    /// Cached local interfaces for endpoint selection.
    local_interfaces: OnceLock<Vec<InterfaceEndpoint>>,

    /// NUMA hint for topology-aware NIC selection.
    numa_hint: Option<u32>,

    /// Shared observability collectors, installed by the backend.
    metrics: OnceLock<std::sync::Arc<dyn velo_ext::TransportObservability>>,
}

/// Handle to a connection's writer task.
///
/// One handle is one bidi stream. The gate is the only way frames enter `tx`,
/// so per-stream FIFO holds no matter how many tasks are sending; `tx` is
/// retained purely for the liveness probes (`is_disconnected`) that decide
/// when a stream is stale.
#[derive(Clone)]
struct ConnectionHandle {
    tx: flume::Sender<SendTask>,
    gate: AdmissionGate<SendTask>,
}

impl ConnectionHandle {
    /// Kill this epoch: every frame still queued behind the gate belonged to a
    /// stream that no longer exists, so none of them may ride the successor.
    fn retire(&self) {
        self.gate.fail_all(AdmissionError::ConnectionReplaced);
    }
}

/// Task sent to the writer task containing frame data.
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

impl GrpcTransport {
    /// Drop a dead connection's map entry, retiring its epoch first.
    ///
    /// The predicate keeps us from evicting a successor that another task
    /// installed in the meantime; retiring before the entry disappears is what
    /// guarantees the old epoch's queued frames fail rather than linger.
    fn reap_stale_connection(&self, instance_id: crate::InstanceId) {
        if let Some((_, stale)) = self
            .connections
            .remove_if(&instance_id, |_, h| h.tx.is_disconnected())
        {
            stale.retire();
            self.update_connection_gauge();
        }
    }

    /// Get or create a connection to a peer (lazy initialization).
    fn get_or_create_connection(&self, instance_id: crate::InstanceId) -> Result<ConnectionHandle> {
        // Fast path: connection already exists and is alive.
        if let Some(handle) = self.connections.get(&instance_id) {
            if !handle.tx.is_disconnected() {
                return Ok(handle.clone());
            }
            // Stale -- drop guard before mutating the map.
            drop(handle);
            self.reap_stale_connection(instance_id);
        }

        let rt = self.runtime.get().ok_or(TransportError::NotStarted)?;
        // Atomic check-and-insert via entry API.
        let handle = match self.connections.entry(instance_id) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                if !entry.get().tx.is_disconnected() {
                    entry.get().clone()
                } else {
                    // Retire the dead epoch before the successor is installed,
                    // so no frame from the old stream can be observed as
                    // pending on the new one.
                    entry.get().retire();
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
        let handle = ConnectionHandle {
            gate: AdmissionGate::new(tx.clone(), rt.clone()),
            tx,
        };

        let cancel = self.cancel_token.clone();
        let conns = Arc::clone(&self.connections);
        let connect_timeout = self.connect_timeout;
        let channel_capacity = self.channel_capacity;
        let adapter = self.adapter.get().cloned();
        let metrics = self.metrics.get().cloned();
        let transport_name = self.key.to_string();

        rt.spawn(connection_writer_task(
            addr,
            instance_id,
            rx,
            conns,
            cancel,
            connect_timeout,
            channel_capacity,
            adapter,
            metrics,
            transport_name,
        ));

        debug!("Created new gRPC connection to {} ({})", instance_id, addr);
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

    /// Slow path: establish (or reuse) a connection, then offer the frame to
    /// its gate.
    ///
    /// A failure here is terminal for the frame, so it is reported through
    /// `on_error` and the send reports [`SendOutcome::Admitted`] — there is
    /// nothing for the caller to wait on.
    fn slow_path_send(&self, instance_id: crate::InstanceId, send_msg: SendTask) -> SendOutcome {
        if self.runtime.get().is_none() {
            send_msg.on_error("Transport not started");
            return SendOutcome::Admitted;
        }
        let handle = match self.get_or_create_connection(instance_id) {
            Ok(h) => h,
            Err(e) => {
                send_msg.on_error(format!("Failed to create connection: {}", e));
                return SendOutcome::Admitted;
            }
        };
        self.admit(&handle, send_msg)
    }

    /// Offer one frame to a connection's gate, counting the saturated case.
    fn admit(&self, handle: &ConnectionHandle, send_msg: SendTask) -> SendOutcome {
        let outcome = handle.gate.send(send_msg);
        if let Some(m) = self.metrics.get()
            && !outcome.is_admitted()
        {
            m.record_send_backpressure();
        }
        outcome
    }
}

impl Transport for GrpcTransport {
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

        // Parse endpoints (supports both new multi-endpoint and legacy formats)
        let remote_endpoints = parse_endpoints(&endpoint).map_err(|e| {
            error!("Failed to parse gRPC endpoint: {}", e);
            TransportError::InvalidEndpoint
        })?;

        // Lazy-init local interfaces for endpoint selection
        let local = self.local_interfaces.get_or_init(|| {
            resolve_advertise_endpoints(self.bind_addr, &InterfaceFilter::All).unwrap_or_default()
        });

        // Select best endpoint based on NUMA + subnet affinity
        let addr = select_best_endpoint(&remote_endpoints, local, self.numa_hint)
            .ok_or(TransportError::InvalidEndpoint)?;

        self.peers.insert(peer_info.instance_id(), addr);
        self.update_peer_gauge();
        debug!("Registered peer {} at {}", peer_info.instance_id(), addr);

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
        };

        // Fast path: an established stream. The liveness probe comes first
        // because a dead epoch's gate would swallow the frame; the gate itself
        // then decides admitted-vs-queued, so there is no `try_send` here that
        // could overtake a frame already queued behind it.
        if let Some(handle) = self.connections.get(&instance_id) {
            let live = (!handle.tx.is_disconnected()).then(|| handle.clone());
            // Release the shard guard before either admitting (which may spawn
            // a driver) or mutating the map.
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
        self.runtime.set(rt.clone()).ok();
        self.shutdown_state
            .set(channels.shutdown_state.clone())
            .ok();
        self.adapter.set(channels.clone()).ok();

        let bind_addr = self.bind_addr;
        let shutdown_state = channels.shutdown_state.clone();
        let listener = self
            .listener
            .lock()
            .expect("Listener mutex poisoned")
            .take();

        Box::pin(async move {
            // Use pre-bound listener if provided, otherwise bind to the address.
            let std_listener = if let Some(l) = listener {
                l
            } else {
                std::net::TcpListener::bind(bind_addr).context("Failed to bind gRPC listener")?
            };
            let local_addr = std_listener
                .local_addr()
                .context("Failed to get local address")?;
            std_listener
                .set_nonblocking(true)
                .context("Failed to set listener to non-blocking")?;

            let incoming = tokio::net::TcpListener::from_std(std_listener)
                .context("Failed to convert std TcpListener to tokio TcpListener")?;
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(incoming);

            let svc = VeloStreamingService::new(
                channels,
                shutdown_state.clone(),
                self.key.to_string(),
                self.metrics.get().cloned(),
            );
            let teardown_token = shutdown_state.teardown_token().clone();

            rt.spawn(async move {
                if let Err(e) = tonic::transport::Server::builder()
                    .add_service(VeloStreamingServer::new(svc))
                    .serve_with_incoming_shutdown(incoming, teardown_token.cancelled())
                    .await
                {
                    error!("gRPC server error: {}", e);
                }
            });

            info!("gRPC transport started on {}", local_addr);

            Ok(())
        })
    }

    fn begin_drain(&self) {
        // Per-frame gate in the server handler handles drain -- no-op here.
    }

    fn shutdown(&self) {
        info!("Shutting down gRPC transport");

        if let Some(state) = self.shutdown_state.get() {
            state.teardown_token().cancel();
        }
        self.cancel_token.cancel();
        self.connections.clear();
        self.update_connection_gauge();
    }

    fn set_observability(
        &self,
        observability: std::sync::Arc<dyn velo_ext::TransportObservability>,
    ) {
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

            let addr = *self
                .peers
                .get(&instance_id)
                .ok_or(HealthCheckError::PeerNotRegistered)?
                .value();

            let url = format!("http://{}", addr);
            let endpoint = tonic::transport::Endpoint::from_shared(url)
                .map_err(|_| HealthCheckError::ConnectionFailed)?
                .connect_timeout(timeout);

            match tokio::time::timeout(timeout, endpoint.connect()).await {
                Ok(Ok(_channel)) => {
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

/// Connection writer task for gRPC transport.
///
/// Establishes a bidi stream via `VeloStreamingClient`, sends outbound frames,
/// and spawns a reader task for the response stream. Cleanup (draining queued
/// messages and removing the stale map entry) always runs, even on connect failure.
#[allow(clippy::too_many_arguments)]
async fn connection_writer_task(
    addr: SocketAddr,
    instance_id: crate::InstanceId,
    rx: flume::Receiver<SendTask>,
    connections: Arc<DashMap<crate::InstanceId, ConnectionHandle>>,
    cancel_token: tokio_util::sync::CancellationToken,
    connect_timeout: Duration,
    channel_capacity: usize,
    adapter: Option<TransportAdapter>,
    metrics: Option<std::sync::Arc<dyn velo_ext::TransportObservability>>,
    transport_name: String,
) -> Result<()> {
    let result = connection_writer_inner(
        addr,
        instance_id,
        &rx,
        &cancel_token,
        connect_timeout,
        channel_capacity,
        adapter,
        metrics.clone(),
        &transport_name,
    )
    .await;

    // Drain queued messages and notify their error handlers.
    while let Ok(msg) = rx.try_recv() {
        msg.on_error("Connection closed");
    }

    // Drop the receiver so our sender half becomes disconnected, then remove
    // the stale entry. The predicate ensures we only remove our own entry.
    //
    // Retiring the gate is what fails frames still queued behind it. Dropping
    // `rx` would eventually fail them too (the driver's `send_async` sees a
    // closed channel), but `ConnectionReplaced` names the cause and lands
    // without waiting on the driver.
    drop(rx);
    if let Some((_, stale)) = connections.remove_if(&instance_id, |_, h| h.tx.is_disconnected()) {
        stale.retire();
    }
    if let Some(metrics) = metrics.as_ref() {
        metrics.set_active_connections(connections.len());
    }

    debug!("gRPC connection to {} ({}) closed", instance_id, addr);

    result
}

/// Inner loop: connect, open bidi stream, and send frames until the channel
/// closes or a send error occurs.
#[allow(clippy::too_many_arguments)]
async fn connection_writer_inner(
    addr: SocketAddr,
    instance_id: crate::InstanceId,
    rx: &flume::Receiver<SendTask>,
    cancel_token: &tokio_util::sync::CancellationToken,
    connect_timeout: Duration,
    channel_capacity: usize,
    adapter: Option<TransportAdapter>,
    metrics: Option<std::sync::Arc<dyn velo_ext::TransportObservability>>,
    transport_name: &str,
) -> Result<()> {
    debug!("gRPC connecting to {}", addr);

    let url = format!("http://{}", addr);
    let endpoint = tonic::transport::Endpoint::from_shared(url.clone())
        .context("invalid endpoint URL")?
        .connect_timeout(connect_timeout);
    let channel = tokio::select! {
        _ = cancel_token.cancelled() => return Ok(()),
        res = tokio::time::timeout(connect_timeout, endpoint.connect()) => {
            res.context("gRPC connect timeout")?.context("gRPC connect failed")?
        },
    };

    let mut client = VeloStreamingClient::new(channel);

    // Create mpsc channel to feed the outbound side of the bidi stream.
    let (mpsc_tx, mpsc_rx) = tokio::sync::mpsc::channel::<proto::FramedData>(channel_capacity);

    let response = client
        .stream(ReceiverStream::new(mpsc_rx))
        .await
        .context("failed to open gRPC bidi stream")?;
    let mut response_stream = response.into_inner();

    debug!("gRPC connected to {} ({})", instance_id, addr);

    // Spawn a reader task that consumes the response stream from the server.
    if let Some(adapter) = adapter {
        let metrics = metrics.clone();
        let transport_name = transport_name.to_string();
        #[cfg(not(feature = "distributed-tracing"))]
        let _ = &transport_name;
        tokio::spawn(async move {
            loop {
                match response_stream.message().await {
                    Ok(Some(framed_data)) => {
                        if let Ok(msg_type) =
                            TcpFrameCodec::parse_message_type_from_preamble(&framed_data.preamble)
                        {
                            let sender = match msg_type {
                                MessageType::ShuttingDown | MessageType::Response => {
                                    &adapter.response_stream
                                }
                                MessageType::Ack | MessageType::Event => &adapter.event_stream,
                                MessageType::Message => &adapter.message_stream,
                            };
                            if let Some(metrics) = metrics.as_ref() {
                                #[cfg(feature = "distributed-tracing")]
                                let span = tracing::debug_span!(
                                    "velo.transport.receive",
                                    transport = transport_name.as_str(),
                                    message_type = crate::transports::message_type_label(msg_type),
                                    bytes = framed_data.header.len() + framed_data.payload.len()
                                );
                                #[cfg(feature = "distributed-tracing")]
                                let _entered = span.enter();

                                metrics.record_frame(
                                    Direction::Inbound,
                                    crate::transports::message_type_label(msg_type),
                                    framed_data.header.len() + framed_data.payload.len(),
                                );
                            }
                            if sender
                                .send_async((
                                    Bytes::from(framed_data.header),
                                    Bytes::from(framed_data.payload),
                                ))
                                .await
                                .is_err()
                            {
                                if let Some(metrics) = metrics.as_ref() {
                                    metrics.record_rejection(TransportRejection::RouteFailed);
                                }
                                break;
                            }
                        } else if let Some(metrics) = metrics.as_ref() {
                            metrics.record_rejection(TransportRejection::DecodeError);
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        warn!("gRPC response stream error from {}: {}", instance_id, e);
                        break;
                    }
                }
            }
        });
    }

    // Main send loop: receive from flume, wrap in FramedData, send via mpsc.
    loop {
        let msg = tokio::select! {
            // Prioritize cancellation so a hot send queue cannot starve shutdown.
            biased;
            _ = cancel_token.cancelled() => break,
            res = rx.recv_async() => match res {
                Ok(msg) => msg,
                Err(_) => break,
            },
        };

        let preamble = match TcpFrameCodec::build_preamble(
            msg.msg_type,
            msg.header.len() as u32,
            msg.payload.len() as u32,
        ) {
            Ok(p) => p,
            Err(e) => {
                error!(
                    "Failed to build preamble for {} ({}): {}",
                    instance_id, addr, e
                );
                msg.on_error(format!("Failed to build preamble: {}", e));
                continue;
            }
        };

        let framed = proto::FramedData {
            preamble: preamble.to_vec(),
            header: msg.header.to_vec(),
            payload: msg.payload.to_vec(),
        };

        if mpsc_tx.send(framed).await.is_err() {
            error!(
                "gRPC send channel closed for {} ({}), reconnecting",
                instance_id, addr
            );
            msg.on_error("gRPC send channel closed");
            break;
        }
    }

    Ok(())
}

/// Parse a gRPC endpoint string into a `SocketAddr` (legacy format, used in tests).
#[cfg(test)]
fn parse_grpc_endpoint(endpoint: &[u8]) -> Result<SocketAddr> {
    use std::net::ToSocketAddrs;

    let endpoint_str = std::str::from_utf8(endpoint).context("endpoint is not valid UTF-8")?;

    let addr_str = endpoint_str.strip_prefix("grpc://").unwrap_or(endpoint_str);

    let mut addrs = addr_str
        .to_socket_addrs()
        .context("failed to parse socket address")?;

    addrs
        .next()
        .ok_or_else(|| anyhow::anyhow!("no addresses resolved"))
}

/// Builder for `GrpcTransport`.
pub struct GrpcTransportBuilder {
    bind_addr: Option<SocketAddr>,
    key: Option<TransportKey>,
    channel_capacity: usize,
    connect_timeout: Duration,
    listener: Option<std::net::TcpListener>,
    interface_filter: InterfaceFilter,
    numa_hint: Option<u32>,
}

impl GrpcTransportBuilder {
    /// Create a new builder with default settings.
    pub fn new() -> Self {
        Self {
            bind_addr: None,
            key: None,
            channel_capacity: 256,
            connect_timeout: Duration::from_secs(5),
            listener: None,
            interface_filter: InterfaceFilter::default(),
            numa_hint: None,
        }
    }

    /// Set the bind address for the gRPC server.
    pub fn bind_addr(mut self, addr: SocketAddr) -> Self {
        self.bind_addr = Some(addr);
        self
    }

    /// Set the transport key (default: `"grpc"`).
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
    ///
    /// Callers typically resolve this via `dynamo_memory::numa::get_device_numa_node(gpu_id)`.
    pub fn numa_hint(mut self, node: u32) -> Self {
        self.numa_hint = Some(node);
        self
    }

    /// Use a pre-bound TcpListener instead of binding to a specific address.
    ///
    /// This is useful for tests where you want to bind to port 0 and get an OS-assigned
    /// port without creating a race condition between binding and starting the transport.
    pub fn from_listener(mut self, listener: std::net::TcpListener) -> Result<Self> {
        if self.bind_addr.is_some() {
            anyhow::bail!(
                "Cannot use both bind_addr() and from_listener() - they are mutually exclusive"
            );
        }
        let addr = listener
            .local_addr()
            .context("Failed to get local address from listener")?;
        self.bind_addr = Some(addr);
        self.listener = Some(listener);
        Ok(self)
    }

    /// Build the `GrpcTransport`.
    pub fn build(self) -> Result<GrpcTransport> {
        let key = self.key.unwrap_or_else(|| TransportKey::from("grpc"));

        // If we have a listener, use its address; otherwise pre-bind to resolve port 0.
        let (bind_addr, listener) = if let Some(listener) = self.listener {
            let addr = listener.local_addr()?;
            (addr, Some(listener))
        } else {
            let requested = self
                .bind_addr
                .unwrap_or_else(|| "0.0.0.0:0".parse().unwrap());
            let std_listener = std::net::TcpListener::bind(requested)
                .context("Failed to pre-bind gRPC listener")?;
            let actual = std_listener.local_addr()?;
            (actual, Some(std_listener))
        };

        // Resolve advertise endpoints (multi-interface discovery)
        let endpoints = resolve_advertise_endpoints(bind_addr, &self.interface_filter)?;

        // Warn if NUMA hint conflicts with interface filter
        if let (Some(numa), InterfaceFilter::ByName(name)) =
            (self.numa_hint, &self.interface_filter)
        {
            for ep in &endpoints {
                if let Some(ep_numa) = ep.numa_node
                    && ep_numa != numa as i32
                {
                    warn!(
                        "NIC {} is on NUMA node {} but GPU NUMA hint is {}",
                        name, ep_numa, numa
                    );
                }
            }
        }

        let encoded =
            rmp_serde::to_vec(&endpoints).context("Failed to encode interface endpoints")?;
        let mut addr_builder = crate::transports::address::WorkerAddressBuilder::new();
        addr_builder.add_entry(key.clone(), encoded)?;
        let local_address = addr_builder.build()?;

        Ok(GrpcTransport {
            key,
            bind_addr,
            local_address,
            peers: Arc::new(DashMap::new()),
            connections: Arc::new(DashMap::new()),
            runtime: OnceLock::new(),
            cancel_token: tokio_util::sync::CancellationToken::new(),
            shutdown_state: OnceLock::new(),
            channel_capacity: self.channel_capacity,
            connect_timeout: self.connect_timeout,
            adapter: OnceLock::new(),
            listener: std::sync::Mutex::new(listener),
            local_interfaces: OnceLock::new(),
            numa_hint: self.numa_hint,
            metrics: OnceLock::new(),
        })
    }
}

impl Default for GrpcTransportBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// `#[path]` keeps the tests beside their siblings as `grpc/tests.rs`; the
// default resolution would bury them in a one-file `grpc/transport/` directory.
#[cfg(test)]
#[path = "tests.rs"]
mod tests;
