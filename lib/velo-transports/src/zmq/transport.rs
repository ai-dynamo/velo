// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! High-performance ZMQ transport with DEALER/ROUTER socket pattern
//!
//! Uses two dedicated I/O threads (fixed, regardless of peer count):
//! - Listener thread: ROUTER socket for inbound messages
//! - Sender thread: multiplexed DEALER sockets for all outbound messages

use anyhow::{Context, Result};
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};
use velo_observability::VeloMetrics;

use crate::transport::{HealthCheckError, ShutdownState, TransportError, TransportErrorHandler};
use crate::{MessageType, PeerInfo, Transport, TransportAdapter, TransportKey, WorkerAddress};

use super::listener;

/// High-performance ZMQ transport using DEALER/ROUTER sockets.
///
/// Two dedicated I/O threads handle all messaging regardless of peer count:
/// - A ROUTER socket listener thread for inbound messages
/// - A single sender thread multiplexing DEALER sockets for all outbound messages
pub struct ZmqTransport {
    /// Unique transport key (default: `"zmq"`).
    key: TransportKey,
    /// ZMQ endpoint the ROUTER socket is bound to (e.g. `"tcp://127.0.0.1:5555"`).
    bind_endpoint: String,
    /// The local `WorkerAddress` fragment advertised to peers.
    local_address: WorkerAddress,
    /// Per-peer ZMQ endpoint strings.
    peers: Arc<DashMap<crate::InstanceId, String>>,
    /// Shared ZMQ context for all sockets.
    zmq_context: Arc<zmq::Context>,
    /// Single shared sender channel — all peers multiplex through one sender thread.
    sender_tx: OnceLock<flume::Sender<OutboundTask>>,
    /// Tokio runtime handle, set once during `start()`.
    runtime: OnceLock<tokio::runtime::Handle>,
    /// Transport-level cancellation token.
    cancel_token: CancellationToken,
    /// Shared shutdown state, set once during `start()`.
    shutdown_state: OnceLock<ShutdownState>,
    /// Bounded channel capacity for sender backpressure.
    channel_capacity: usize,
    /// ZMQ send high water mark.
    sndhwm: i32,
    /// ZMQ receive high water mark.
    rcvhwm: i32,
    /// ZMQ linger period in milliseconds on socket close.
    linger_ms: i32,
    /// Shared observability collectors installed by the backend.
    observability: OnceLock<Arc<VeloMetrics>>,
    /// Transport-scoped metrics handle.
    metrics: OnceLock<velo_observability::TransportMetricsHandle>,
    /// Handle to the listener thread (for join on shutdown).
    listener_handle: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Handle to the sender thread (for join on shutdown).
    sender_handle: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Control socket endpoint for the listener thread.
    listener_control_endpoint: String,
    /// Control socket endpoint for the sender thread.
    sender_control_endpoint: String,
}

/// Task sent to the sender thread containing a message to send.
pub(crate) struct OutboundTask {
    pub target: crate::InstanceId,
    pub msg_type: MessageType,
    pub header: Bytes,
    pub payload: Bytes,
    pub on_error: Arc<dyn TransportErrorHandler>,
}

impl OutboundTask {
    fn on_error(self, error: impl Into<String>) {
        self.on_error
            .on_error(self.header, self.payload, error.into());
    }
}

impl ZmqTransport {
    fn update_peer_gauge(&self) {
        if let Some(metrics) = self.metrics.get() {
            metrics.set_registered_peers(self.peers.len());
        }
    }
}

impl Transport for ZmqTransport {
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

        let endpoint_str = std::str::from_utf8(&endpoint).map_err(|_| {
            error!("ZMQ endpoint is not valid UTF-8");
            TransportError::InvalidEndpoint
        })?;

        // Validate it looks like a ZMQ endpoint
        if !endpoint_str.starts_with("tcp://")
            && !endpoint_str.starts_with("ipc://")
            && !endpoint_str.starts_with("inproc://")
        {
            error!("Invalid ZMQ endpoint format: {}", endpoint_str);
            return Err(TransportError::InvalidEndpoint);
        }

        self.peers
            .insert(peer_info.instance_id(), endpoint_str.to_string());
        self.update_peer_gauge();

        debug!(
            "Registered ZMQ peer {} at {}",
            peer_info.instance_id(),
            endpoint_str
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
        let task = OutboundTask {
            target: instance_id,
            msg_type: message_type,
            header,
            payload,
            on_error,
        };

        let tx = match self.sender_tx.get() {
            Some(tx) => tx,
            None => {
                task.on_error("Transport not started");
                return;
            }
        };

        // Fast path: non-blocking try_send
        match tx.try_send(task) {
            Ok(()) => {}
            Err(flume::TrySendError::Full(task)) => {
                // Slow path: async backpressure
                let tx = tx.clone();
                if let Some(rt) = self.runtime.get() {
                    rt.spawn(async move {
                        if let Err(flume::SendError(task)) = tx.send_async(task).await {
                            task.on_error("Sender channel closed");
                        }
                    });
                } else {
                    task.on_error("Transport not started");
                }
            }
            Err(flume::TrySendError::Disconnected(task)) => {
                task.on_error("Sender thread exited");
            }
        }
    }

    fn start(
        &self,
        instance_id: crate::InstanceId,
        channels: TransportAdapter,
        rt: tokio::runtime::Handle,
    ) -> futures::future::BoxFuture<'_, Result<()>> {
        self.runtime.set(rt.clone()).ok();
        self.shutdown_state
            .set(channels.shutdown_state.clone())
            .ok();

        let ctx = self.zmq_context.clone();
        let bind_endpoint = self.bind_endpoint.clone();
        let listener_control_ep = self.listener_control_endpoint.clone();
        let sender_control_ep = self.sender_control_endpoint.clone();
        let peers = self.peers.clone();
        let channel_capacity = self.channel_capacity;
        let sndhwm = self.sndhwm;
        let rcvhwm = self.rcvhwm;
        let linger_ms = self.linger_ms;
        let metrics = self.metrics.get().cloned();
        let shutdown_state = channels.shutdown_state.clone();
        let instance_id_bytes = instance_id.as_bytes().to_vec();

        Box::pin(async move {
            // Create the sender channel
            let (sender_tx, sender_rx) = flume::bounded(channel_capacity);
            self.sender_tx.set(sender_tx).ok();

            // Spawn the listener thread (ROUTER socket)
            let listener_cfg = listener::ListenerConfig {
                ctx: ctx.clone(),
                bind_endpoint,
                control_endpoint: listener_control_ep,
                adapter: channels,
                shutdown_state: shutdown_state.clone(),
                rcvhwm,
                linger_ms,
                metrics: metrics.clone(),
            };
            let listener_handle = std::thread::Builder::new()
                .name("zmq-listener".to_string())
                .spawn(move || {
                    listener::run_listener(listener_cfg);
                })
                .context("Failed to spawn ZMQ listener thread")?;

            *self
                .listener_handle
                .lock()
                .expect("listener_handle mutex poisoned") = Some(listener_handle);

            // Spawn the sender thread (multiplexed DEALER sockets)
            let sender_cfg = SenderConfig {
                ctx: ctx.clone(),
                control_endpoint: sender_control_ep,
                rx: sender_rx,
                peers,
                identity: instance_id_bytes,
                sndhwm,
                linger_ms,
                metrics,
            };
            let sender_handle = std::thread::Builder::new()
                .name("zmq-sender".to_string())
                .spawn(move || {
                    run_sender(sender_cfg);
                })
                .context("Failed to spawn ZMQ sender thread")?;

            *self
                .sender_handle
                .lock()
                .expect("sender_handle mutex poisoned") = Some(sender_handle);

            info!("ZMQ transport started on {}", self.bind_endpoint);
            Ok(())
        })
    }

    fn begin_drain(&self) {
        // Drain gating is handled by ShutdownState.is_draining() in the listener
        // thread — no control signal needed. This matches TCP/gRPC behavior.
    }

    fn shutdown(&self) {
        info!("Shutting down ZMQ transport");

        // Signal the listener thread to stop
        if let Ok(ctrl) = self.zmq_context.socket(zmq::PAIR)
            && ctrl.connect(&self.listener_control_endpoint).is_ok()
        {
            let _ = ctrl.send("shutdown", 0);
        }

        // Signal the sender thread to stop
        if let Ok(ctrl) = self.zmq_context.socket(zmq::PAIR)
            && ctrl.connect(&self.sender_control_endpoint).is_ok()
        {
            let _ = ctrl.send("shutdown", 0);
        }

        // Drop the sender channel to unblock the sender thread
        // (OnceLock doesn't support take, but the thread will also see the control signal)

        self.cancel_token.cancel();

        // Join threads (they exit promptly after receiving the shutdown signal)
        if let Some(handle) = self.listener_handle.lock().expect("mutex poisoned").take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.sender_handle.lock().expect("mutex poisoned").take() {
            let _ = handle.join();
        }
    }

    fn set_observability(&self, observability: Arc<VeloMetrics>) {
        let _ = self
            .metrics
            .set(observability.bind_transport(self.key.as_str()));
        let _ = self.observability.set(observability);
        self.update_peer_gauge();
    }

    fn check_health(
        &self,
        instance_id: crate::InstanceId,
        timeout: Duration,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), HealthCheckError>> + Send + '_>,
    > {
        Box::pin(async move {
            let endpoint = self
                .peers
                .get(&instance_id)
                .map(|e| e.value().clone())
                .ok_or(HealthCheckError::PeerNotRegistered)?;

            // ZMQ connect is async internally — create a probe socket, attach a
            // monitor, and wait for a CONNECTED / CONNECT_RETRIED / DISCONNECTED
            // event within the timeout.
            tokio::task::spawn_blocking(move || -> Result<(), HealthCheckError> {
                let timeout_ms = timeout.as_millis() as i32;

                let ctx = zmq::Context::new();
                let sock = ctx
                    .socket(zmq::DEALER)
                    .map_err(|_| HealthCheckError::ConnectionFailed)?;
                sock.set_linger(0).ok();
                sock.set_connect_timeout(timeout_ms).ok();

                // Use a unique inproc endpoint for the monitor.
                let monitor_endpoint = format!("inproc://zmq-healthcheck-monitor-{:p}", &sock);

                // Monitor connection-related events.
                let events = (zmq::SocketEvent::CONNECTED as i32)
                    | (zmq::SocketEvent::CONNECT_RETRIED as i32)
                    | (zmq::SocketEvent::DISCONNECTED as i32);
                sock.monitor(&monitor_endpoint, events)
                    .map_err(|_| HealthCheckError::ConnectionFailed)?;

                let monitor_sock = ctx
                    .socket(zmq::PAIR)
                    .map_err(|_| HealthCheckError::ConnectionFailed)?;
                monitor_sock.set_rcvtimeo(timeout_ms).ok();
                monitor_sock
                    .connect(&monitor_endpoint)
                    .map_err(|_| HealthCheckError::ConnectionFailed)?;

                // Initiate the actual connection to the peer endpoint.
                sock.connect(&endpoint)
                    .map_err(|_| HealthCheckError::ConnectionFailed)?;

                // Wait for a decisive monitor event within the timeout.
                // Monitor events are two-frame messages: [event_data (6 bytes), address].
                // The event_data layout: [u16 event_id, u32 event_value].
                const ZMQ_EVENT_CONNECTED: u16 = 0x0001;
                const ZMQ_EVENT_CONNECT_RETRIED: u16 = 0x0040;
                const ZMQ_EVENT_DISCONNECTED: u16 = 0x0200;

                loop {
                    let data = monitor_sock
                        .recv_bytes(0)
                        .map_err(|_| HealthCheckError::Timeout)?;
                    // Drain the address frame
                    let _ = monitor_sock.recv_bytes(0);

                    if data.len() >= 2 {
                        let event_id = u16::from_le_bytes([data[0], data[1]]);
                        match event_id {
                            ZMQ_EVENT_CONNECTED => return Ok(()),
                            ZMQ_EVENT_CONNECT_RETRIED | ZMQ_EVENT_DISCONNECTED => {
                                return Err(HealthCheckError::ConnectionFailed);
                            }
                            _ => { /* ignore unrelated events */ }
                        }
                    }
                }
            })
            .await
            .map_err(|_| HealthCheckError::Timeout)?
        })
    }
}

/// Sender thread: multiplexes all outbound messages through DEALER sockets.
///
/// Configuration bundle for the sender thread.
struct SenderConfig {
    ctx: Arc<zmq::Context>,
    control_endpoint: String,
    rx: flume::Receiver<OutboundTask>,
    peers: Arc<DashMap<crate::InstanceId, String>>,
    identity: Vec<u8>,
    sndhwm: i32,
    linger_ms: i32,
    metrics: Option<velo_observability::TransportMetricsHandle>,
}

/// Owns a `HashMap<InstanceId, zmq::Socket>` of lazily-created DEALER sockets.
/// Reads `OutboundTask` from a shared flume channel and dispatches to the correct socket.
fn run_sender(cfg: SenderConfig) {
    // Control socket for shutdown signaling
    let control = cfg
        .ctx
        .socket(zmq::PAIR)
        .expect("Failed to create control socket");
    control
        .bind(&cfg.control_endpoint)
        .expect("Failed to bind sender control socket");

    let mut dealer_sockets: HashMap<crate::InstanceId, zmq::Socket> = HashMap::new();
    let mut running = true;

    while running {
        // Use recv_timeout on the flume channel so we can also check the control socket
        match cfg.rx.recv_timeout(Duration::from_millis(10)) {
            Ok(task) => {
                let target = task.target;

                // Get or create DEALER socket for this peer
                let sock = match dealer_sockets.get(&target) {
                    Some(s) => s,
                    None => {
                        let endpoint = match cfg.peers.get(&target) {
                            Some(ep) => ep.value().clone(),
                            None => {
                                task.on_error(format!("Peer not registered: {}", target));
                                continue;
                            }
                        };

                        match create_dealer_socket(
                            &cfg.ctx,
                            &cfg.identity,
                            &endpoint,
                            cfg.sndhwm,
                            cfg.linger_ms,
                        ) {
                            Ok(sock) => {
                                dealer_sockets.insert(target, sock);
                                dealer_sockets.get(&target).unwrap()
                            }
                            Err(e) => {
                                task.on_error(format!("Failed to create DEALER socket: {}", e));
                                continue;
                            }
                        }
                    }
                };

                // Send 3-part multipart: [msg_type, header, payload]
                let type_byte: &[u8] = &[task.msg_type.as_u8()];
                let send_result = sock
                    .send(type_byte, zmq::SNDMORE)
                    .and_then(|_| sock.send(task.header.as_ref(), zmq::SNDMORE))
                    .and_then(|_| sock.send(task.payload.as_ref(), 0));

                match send_result {
                    Ok(()) => {
                        if let Some(ref m) = cfg.metrics {
                            m.record_frame(
                                velo_observability::Direction::Outbound,
                                crate::message_type_label(task.msg_type),
                                task.header.len() + task.payload.len(),
                            );
                        }
                    }
                    Err(e) => {
                        error!("ZMQ send error to {}: {}", target, e);
                        // Remove dead socket so it gets recreated on next attempt
                        dealer_sockets.remove(&target);
                        task.on_error(format!("ZMQ send failed: {}", e));
                    }
                }
            }
            Err(flume::RecvTimeoutError::Timeout) => {
                // Check control socket for shutdown signal
            }
            Err(flume::RecvTimeoutError::Disconnected) => {
                debug!("ZMQ sender channel closed, shutting down");
                break;
            }
        }

        // Non-blocking check for control commands
        if let Ok(msg) = control.recv_bytes(zmq::DONTWAIT)
            && msg.as_slice() == b"shutdown"
        {
            debug!("ZMQ sender received shutdown signal");
            running = false;
        }
    }

    // Drain remaining messages with error callbacks
    while let Ok(task) = cfg.rx.try_recv() {
        task.on_error("Transport shutting down");
    }

    // Close all DEALER sockets
    drop(dealer_sockets);
    debug!("ZMQ sender thread exited");
}

/// Create and configure a DEALER socket connected to a remote ROUTER.
fn create_dealer_socket(
    ctx: &zmq::Context,
    identity: &[u8],
    endpoint: &str,
    sndhwm: i32,
    linger_ms: i32,
) -> Result<zmq::Socket> {
    let sock = ctx
        .socket(zmq::DEALER)
        .context("Failed to create DEALER socket")?;
    sock.set_identity(identity)
        .context("Failed to set DEALER identity")?;
    sock.set_sndhwm(sndhwm)
        .context("Failed to set ZMQ_SNDHWM")?;
    sock.set_linger(linger_ms)
        .context("Failed to set ZMQ_LINGER")?;
    // Set a send timeout to avoid blocking forever on a dead peer
    sock.set_sndtimeo(5000)
        .context("Failed to set ZMQ_SNDTIMEO")?;
    // ZMQ connect is asynchronous — messages sent before the handshake
    // completes will be queued internally by ZMQ and delivered once connected.
    // No post-connect sleep needed; avoids blocking the shared sender thread.
    sock.connect(endpoint)
        .context(format!("Failed to connect DEALER to {}", endpoint))?;
    debug!("Created DEALER socket connected to {}", endpoint);
    Ok(sock)
}

/// Builder for [`ZmqTransport`].
pub struct ZmqTransportBuilder {
    bind_endpoint: Option<String>,
    key: Option<TransportKey>,
    channel_capacity: usize,
    zmq_io_threads: usize,
    sndhwm: i32,
    rcvhwm: i32,
    linger_ms: i32,
}

impl ZmqTransportBuilder {
    /// Create a new builder with sensible defaults.
    pub fn new() -> Self {
        Self {
            bind_endpoint: None,
            key: None,
            channel_capacity: 256,
            zmq_io_threads: 1,
            sndhwm: 1000,
            rcvhwm: 1000,
            linger_ms: 1000,
        }
    }

    /// Set the ZMQ bind endpoint (e.g. `"tcp://0.0.0.0:0"` for OS-assigned port).
    pub fn bind_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.bind_endpoint = Some(endpoint.into());
        self
    }

    /// Set the transport key (default: `"zmq"`).
    pub fn key(mut self, key: TransportKey) -> Self {
        self.key = Some(key);
        self
    }

    /// Set the channel capacity for sender backpressure (default: 256).
    pub fn channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }

    /// Set the number of ZMQ I/O threads (default: 1).
    pub fn zmq_io_threads(mut self, threads: usize) -> Self {
        self.zmq_io_threads = threads;
        self
    }

    /// Set the ZMQ send high water mark (default: 1000).
    pub fn sndhwm(mut self, hwm: i32) -> Self {
        self.sndhwm = hwm;
        self
    }

    /// Set the ZMQ receive high water mark (default: 1000).
    pub fn rcvhwm(mut self, hwm: i32) -> Self {
        self.rcvhwm = hwm;
        self
    }

    /// Set the ZMQ linger period in milliseconds (default: 1000).
    pub fn linger_ms(mut self, ms: i32) -> Self {
        self.linger_ms = ms;
        self
    }

    /// Build the [`ZmqTransport`].
    ///
    /// This pre-binds the ROUTER socket to resolve the actual endpoint
    /// (important when using port 0 for OS-assigned ports).
    pub fn build(self) -> Result<ZmqTransport> {
        let key = self.key.unwrap_or_else(|| TransportKey::from("zmq"));
        let requested_endpoint = self
            .bind_endpoint
            .unwrap_or_else(|| "tcp://127.0.0.1:0".to_string());

        // Create ZMQ context
        let ctx = zmq::Context::new();
        ctx.set_io_threads(self.zmq_io_threads as i32)
            .context("Failed to set ZMQ IO threads")?;

        // Pre-bind a ROUTER socket to resolve the actual endpoint (for port 0)
        let probe = ctx
            .socket(zmq::ROUTER)
            .context("Failed to create probe ROUTER socket")?;
        probe
            .set_linger(0)
            .context("Failed to set linger on probe")?;
        probe.bind(&requested_endpoint).context(format!(
            "Failed to bind ROUTER socket to {}",
            requested_endpoint
        ))?;

        let resolved_endpoint = probe
            .get_last_endpoint()
            .context("Failed to get last endpoint")?
            .map_err(|_| anyhow::anyhow!("Failed to get resolved endpoint"))?;

        // Unbind so the actual listener can bind later
        probe
            .unbind(&resolved_endpoint)
            .context("Failed to unbind probe socket")?;
        drop(probe);

        // Build the WorkerAddress with the resolved endpoint
        let mut addr_builder = crate::address::WorkerAddressBuilder::new();
        addr_builder.add_entry(key.clone(), resolved_endpoint.as_bytes().to_vec())?;
        let local_address = addr_builder.build()?;

        // Generate unique inproc control endpoints using InstanceId for uniqueness
        let unique_id = crate::InstanceId::new_v4();
        let listener_control_endpoint = format!("inproc://zmq-listener-ctrl-{}", unique_id);
        let sender_control_endpoint = format!("inproc://zmq-sender-ctrl-{}", unique_id);

        Ok(ZmqTransport {
            key,
            bind_endpoint: resolved_endpoint,
            local_address,
            peers: Arc::new(DashMap::new()),
            zmq_context: Arc::new(ctx),
            sender_tx: OnceLock::new(),
            runtime: OnceLock::new(),
            cancel_token: CancellationToken::new(),
            shutdown_state: OnceLock::new(),
            channel_capacity: self.channel_capacity,
            sndhwm: self.sndhwm,
            rcvhwm: self.rcvhwm,
            linger_ms: self.linger_ms,
            observability: OnceLock::new(),
            metrics: OnceLock::new(),
            listener_handle: std::sync::Mutex::new(None),
            sender_handle: std::sync::Mutex::new(None),
            listener_control_endpoint,
            sender_control_endpoint,
        })
    }
}

impl Default for ZmqTransportBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::address::WorkerAddressBuilder;
    use velo_common::PeerInfo;

    fn make_zmq_peer(endpoint: &str) -> PeerInfo {
        let instance_id = crate::InstanceId::new_v4();
        let mut builder = WorkerAddressBuilder::new();
        builder
            .add_entry("zmq", endpoint.as_bytes().to_vec())
            .unwrap();
        PeerInfo::new(instance_id, builder.build().unwrap())
    }

    #[test]
    fn test_builder_default() {
        let transport = ZmqTransportBuilder::new().build();
        assert!(transport.is_ok());
    }

    #[test]
    fn test_builder_with_endpoint() {
        let transport = ZmqTransportBuilder::new()
            .bind_endpoint("tcp://127.0.0.1:0")
            .build();
        assert!(transport.is_ok());
        let t = transport.unwrap();
        assert!(t.bind_endpoint.starts_with("tcp://127.0.0.1:"));
    }

    #[test]
    fn test_register_valid_peer() {
        let transport = ZmqTransportBuilder::new().build().unwrap();
        let peer = make_zmq_peer("tcp://127.0.0.1:9999");
        let iid = peer.instance_id();
        assert!(transport.register(peer).is_ok());
        assert!(transport.peers.contains_key(&iid));
    }

    #[test]
    fn test_register_invalid_endpoint() {
        let transport = ZmqTransportBuilder::new().build().unwrap();
        let peer = make_zmq_peer("invalid://foo");
        assert!(transport.register(peer).is_err());
    }

    #[test]
    fn test_address_contains_endpoint() {
        let transport = ZmqTransportBuilder::new().build().unwrap();
        let wa = transport.address();
        let entry = wa.get_entry("zmq").unwrap().unwrap();
        let endpoint = std::str::from_utf8(&entry).unwrap();
        assert!(endpoint.starts_with("tcp://"));
    }
}
