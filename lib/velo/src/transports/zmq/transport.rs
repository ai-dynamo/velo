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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tracing::{debug, error, info};

use crate::transports::transport::{
    AdmissionGate, HealthCheckError, SendOutcome, ShutdownState, TransportError,
    TransportErrorHandler,
};
use velo_ext::{MessageType, PeerInfo, Transport, TransportAdapter, TransportKey, WorkerAddress};

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
    /// Single shared sender channel, set once during `start()`. Lock-free reads
    /// via `OnceLock::get()` on the send hot path. A shutdown command wakes the
    /// sender when idle; `sender_stop` also stops it when this queue is full.
    sender_tx: OnceLock<flume::Sender<SenderCommand>>,
    /// Stops both threads. The listener reads it between polls, so it stops
    /// even when its control message cannot be sent.
    sender_stop: Arc<AtomicBool>,
    /// One admission gate per peer, all feeding `sender_tx`.
    ///
    /// The sender thread multiplexes every peer through one channel, but
    /// ordering is only ever promised per target — so the gate is per target
    /// too. A peer whose DEALER socket is backing up queues behind its own gate
    /// instead of serialising everyone else's sends through it.
    gates: DashMap<crate::InstanceId, AdmissionGate<SenderCommand>>,
    /// Tokio runtime handle, set once during `start()`.
    runtime: OnceLock<tokio::runtime::Handle>,
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
    /// Transport-scoped metrics handle.
    metrics: OnceLock<std::sync::Arc<dyn velo_ext::TransportObservability>>,
    /// Handle to the listener thread (for join on shutdown).
    listener_handle: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Handle to the sender thread (for join on shutdown).
    sender_handle: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Control socket endpoint for the listener thread.
    listener_control_endpoint: String,
    /// Pre-bound ROUTER socket, passed to the listener thread during `start()`.
    router_socket: std::sync::Mutex<Option<zmq::Socket>>,
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

/// Command sent to the sender thread. Unifies message delivery and shutdown
/// signaling through a single channel, eliminating the need for a separate
/// control socket and its associated polling loop.
pub(crate) enum SenderCommand {
    /// Deliver a message to a peer.
    Send(OutboundTask),
    /// Gracefully shut down the sender thread.
    Shutdown,
}

impl ZmqTransport {
    fn update_peer_gauge(&self) {
        if let Some(metrics) = self.metrics.get() {
            metrics.set_registered_peers(self.peers.len());
        }
    }

    /// The gate for one peer, created on first send to it.
    ///
    /// Gates are never retired: a DEALER socket that errors is rebuilt by the
    /// sender thread on the next frame, so there is no epoch boundary at which
    /// queued frames would become invalid.
    fn gate_for(
        &self,
        target: crate::InstanceId,
        tx: &flume::Sender<SenderCommand>,
        rt: &tokio::runtime::Handle,
    ) -> AdmissionGate<SenderCommand> {
        self.gates
            .entry(target)
            .or_insert_with(|| AdmissionGate::new(tx.clone(), rt.clone()))
            .clone()
    }
    fn stop_threads(&self) {
        self.sender_stop.store(true, Ordering::Release);
        // Wake the listener early. Best effort and never blocking: the flag
        // is the real stop request, and the listener may already have stopped
        // on it and closed its socket, so a blocking send could wait forever
        // and the joins below would never run. A lost message costs at most
        // one poll interval.
        if let Ok(ctrl) = self.zmq_context.socket(zmq::PAIR)
            && ctrl.connect(&self.listener_control_endpoint).is_ok()
        {
            let _ = ctrl.send("shutdown", zmq::DONTWAIT);
        }

        // Wake an idle sender. The flag is the stop request: if the queue is
        // full, the sender will see it when it takes the next queued frame.
        if let Some(tx) = self.sender_tx.get()
            && let Err(e) = tx.try_send(SenderCommand::Shutdown)
        {
            debug!("ZMQ shutdown signal not sent (channel full or disconnected): {e}");
        }

        // Join threads (they exit promptly after receiving their shutdown signals).
        if let Some(handle) = self.listener_handle.lock().expect("mutex poisoned").take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.sender_handle.lock().expect("mutex poisoned").take() {
            let _ = handle.join();
        }
    }
}

// `max_message_size` is left at the trait's `None`. The only ZMQ socket option
// this transport sets that bounds anything is `ZMQ_SNDHWM`, which limits queued
// *messages*, not their size; `ZMQ_MAXMSGSIZE` is left at libzmq's unlimited
// default. There is no limit to report.
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

        // Only tcp:// and ipc:// are supported for peer endpoints.
        // inproc:// requires a shared zmq::Context which peers don't share.
        if !endpoint_str.starts_with("tcp://") && !endpoint_str.starts_with("ipc://") {
            error!(
                "Invalid ZMQ peer endpoint (only tcp:// and ipc:// supported): {}",
                endpoint_str
            );
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
    ) -> SendOutcome {
        let task = OutboundTask {
            target: instance_id,
            msg_type: message_type,
            header,
            payload,
            on_error,
        };

        // Lock-free reads via OnceLock — no mutex on the hot path.
        let (Some(tx), Some(rt)) = (self.sender_tx.get(), self.runtime.get()) else {
            task.on_error("Transport not started");
            return SendOutcome::Admitted;
        };

        let outcome = self
            .gate_for(instance_id, tx, rt)
            .send(SenderCommand::Send(task));
        if let Some(m) = self.metrics.get()
            && !outcome.is_admitted()
        {
            m.record_send_backpressure();
        }
        outcome
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
        let peers = self.peers.clone();
        let channel_capacity = self.channel_capacity;
        let sndhwm = self.sndhwm;
        let rcvhwm = self.rcvhwm;
        let linger_ms = self.linger_ms;
        let metrics = self.metrics.get().cloned();
        let instance_id_bytes = instance_id.as_bytes().to_vec();

        // Take the pre-bound ROUTER socket (if available)
        let router_socket = self
            .router_socket
            .lock()
            .expect("router_socket mutex poisoned")
            .take();

        Box::pin(async move {
            // Create the sender channel
            let (sender_tx, sender_rx) = flume::bounded(channel_capacity);
            let _ = self.sender_tx.set(sender_tx);

            // Spawn the listener thread with a ready handshake
            let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
            let listener_cfg = listener::ListenerConfig {
                ctx: ctx.clone(),
                bind_endpoint,
                control_endpoint: listener_control_ep,
                adapter: channels,
                rcvhwm,
                linger_ms,
                metrics,
                router_socket,
                ready_tx,
                stop: self.sender_stop.clone(),
            };
            let listener_handle = std::thread::Builder::new()
                .name("zmq-listener".to_string())
                .spawn(move || {
                    listener::run_listener(listener_cfg);
                })
                .context("Failed to spawn ZMQ listener thread")?;

            // Wait for the listener to signal ready (or fail)
            match ready_rx.recv() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    let _ = listener_handle.join();
                    anyhow::bail!("ZMQ listener failed to start: {}", e);
                }
                Err(_) => {
                    let _ = listener_handle.join();
                    anyhow::bail!("ZMQ listener thread exited before signaling ready");
                }
            }

            *self
                .listener_handle
                .lock()
                .expect("listener_handle mutex poisoned") = Some(listener_handle);

            // The listener is live even if sender startup fails. Its control
            // socket is ready now, so rollback can stop and join both threads.
            struct StartupGuard<'a>(Option<&'a ZmqTransport>);
            impl Drop for StartupGuard<'_> {
                fn drop(&mut self) {
                    if let Some(transport) = self.0 {
                        transport.stop_threads();
                    }
                }
            }
            let mut guard = StartupGuard(Some(self));

            // Spawn the sender thread with a ready handshake
            let (sender_ready_tx, sender_ready_rx) = std::sync::mpsc::sync_channel(1);
            let sender_cfg = SenderConfig {
                ctx,
                rx: sender_rx,
                stop: self.sender_stop.clone(),
                peers,
                identity: instance_id_bytes,
                sndhwm,
                linger_ms,
                ready_tx: sender_ready_tx,
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

            // Wait for the sender to signal ready
            match sender_ready_rx.recv() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => anyhow::bail!("ZMQ sender failed to start: {}", e),
                Err(_) => anyhow::bail!("ZMQ sender thread exited before signaling ready"),
            }

            info!("ZMQ transport started on {}", self.bind_endpoint);
            guard.0 = None;
            Ok(())
        })
    }

    fn begin_drain(&self) {
        // Drain gating happens per frame in the listener thread, inside
        // `TransportAdapter::admit_message` — no control signal needed. This
        // matches TCP/gRPC behavior.
    }

    fn shutdown(&self) {
        info!("Shutting down ZMQ transport");

        self.stop_threads();
    }

    fn set_observability(
        &self,
        observability: std::sync::Arc<dyn velo_ext::TransportObservability>,
    ) {
        let _ = self.metrics.set(observability);
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
            let ctx = self.zmq_context.clone();
            tokio::task::spawn_blocking(move || -> Result<(), HealthCheckError> {
                let timeout_ms = timeout.as_millis() as i32;

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

/// How long a stopping sender may spend on the frames queued before the
/// stop. Each send to a peer that is gone blocks for [`SEND_TIMEOUT`], so with
/// no shared budget a full queue to a dead peer held the sender join, and so
/// teardown, for minutes, past any timeout the caller gave its shutdown.
const DRAIN_BUDGET: Duration = Duration::from_secs(1);

/// How long one send may block on a peer with no free pipe before it fails.
const SEND_TIMEOUT: Duration = Duration::from_secs(5);

/// Configuration bundle for the sender thread.
struct SenderConfig {
    ctx: Arc<zmq::Context>,
    rx: flume::Receiver<SenderCommand>,
    stop: Arc<AtomicBool>,
    peers: Arc<DashMap<crate::InstanceId, String>>,
    identity: Vec<u8>,
    sndhwm: i32,
    linger_ms: i32,
    ready_tx: std::sync::mpsc::SyncSender<Result<(), String>>,
}

/// Sender thread: multiplexes all outbound messages through DEALER sockets.
///
/// Owns a `HashMap<InstanceId, zmq::Socket>` of lazily-created DEALER sockets.
/// Reads `SenderCommand` from a shared flume channel and dispatches to the correct socket.
/// The stop flag is independent of queue capacity. A command wakes an idle
/// sender; a full queue already gives it work on which to observe the flag.
fn run_sender(cfg: SenderConfig) {
    // Signal that the sender is ready
    let _ = cfg.ready_tx.send(Ok(()));

    let mut dealer_sockets: HashMap<crate::InstanceId, zmq::Socket> = HashMap::new();

    // Keep frames already queued ahead of shutdown. Once the flag is seen,
    // drain only that queue prefix, and only within `DRAIN_BUDGET`, so neither
    // later sends nor a dead peer can extend the join. A send already under
    // way when the flag is set can still take up to `SEND_TIMEOUT`.
    let mut remaining = None;
    let mut drain_deadline = None;
    loop {
        if remaining.is_none() && cfg.stop.load(Ordering::Acquire) {
            remaining = Some(cfg.rx.len());
            drain_deadline = Some(std::time::Instant::now() + DRAIN_BUDGET);
        }
        let cmd = match remaining.as_mut() {
            Some(0) => break,
            Some(left) => {
                *left -= 1;
                cfg.rx.try_recv().ok()
            }
            None => cfg.rx.recv().ok(),
        };
        let Some(cmd) = cmd else {
            break;
        };
        let task = match cmd {
            SenderCommand::Send(task) => task,
            SenderCommand::Shutdown => {
                debug!("ZMQ sender received shutdown signal");
                break;
            }
        };

        // While draining, each send gets only what is left of the budget,
        // and a frame that finds none left fails without being sent.
        let drain_left = match drain_deadline {
            Some(deadline) => match deadline.checked_duration_since(std::time::Instant::now()) {
                Some(left) if !left.is_zero() => Some(left),
                _ => {
                    task.on_error("Transport shutting down");
                    continue;
                }
            },
            None => None,
        };

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

        if let Some(left) = drain_left {
            let millis = i32::try_from(left.as_millis()).unwrap_or(i32::MAX).max(1);
            if let Err(e) = sock.set_sndtimeo(millis) {
                task.on_error(format!("ZMQ send failed: {e}"));
                continue;
            }
        }

        // Send 3-part multipart: [msg_type, header, payload]
        let type_byte: &[u8] = &[task.msg_type.as_u8()];
        let send_result = sock
            .send(type_byte, zmq::SNDMORE)
            .and_then(|_| sock.send(task.header.as_ref(), zmq::SNDMORE))
            .and_then(|_| sock.send(task.payload.as_ref(), 0));

        match send_result {
            // The outbound-accepted frame is already recorded by
            // `finalize_send_outcome` (transports.rs) the moment
            // `send_message` returns `Admitted` (or a `Pending` admission
            // resolves `Ok`) — before this thread ever sees the task.
            // Recording it again here double-counted every ZMQ send.
            Ok(()) => {}
            Err(e) => {
                error!("ZMQ send error to {}: {}", target, e);
                // Remove dead socket so it gets recreated on next attempt
                dealer_sockets.remove(&target);
                task.on_error(format!("ZMQ send failed: {}", e));
            }
        }
    }

    // Drain remaining messages with error callbacks
    while let Ok(cmd) = cfg.rx.try_recv() {
        if let SenderCommand::Send(task) = cmd {
            task.on_error("Transport shutting down");
        }
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
    sock.set_sndtimeo(SEND_TIMEOUT.as_millis() as i32)
        .context("Failed to set ZMQ_SNDTIMEO")?;
    // Only queue messages for peers that have completed the TCP handshake.
    // Without this, messages to not-yet-connected peers sit in ZMQ's queue,
    // adding latency to the first message.
    sock.set_immediate(true)
        .context("Failed to set ZMQ_IMMEDIATE")?;
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
    /// Pre-binds the ROUTER socket to resolve the actual endpoint (important
    /// when using port 0). The socket is kept open and handed to the listener
    /// thread during `start()`, avoiding any TOCTOU port race.
    pub fn build(self) -> Result<ZmqTransport> {
        let key = self.key.unwrap_or_else(|| TransportKey::from("zmq"));
        let requested_endpoint = self
            .bind_endpoint
            .unwrap_or_else(|| "tcp://127.0.0.1:0".to_string());

        // Create ZMQ context
        let ctx = zmq::Context::new();
        ctx.set_io_threads(self.zmq_io_threads as i32)
            .context("Failed to set ZMQ IO threads")?;

        // Pre-bind a ROUTER socket to resolve the actual endpoint (for port 0).
        // The socket stays open and is passed to the listener thread in start().
        let router = ctx
            .socket(zmq::ROUTER)
            .context("Failed to create ROUTER socket")?;
        router
            .set_rcvhwm(self.rcvhwm)
            .context("Failed to set ZMQ_RCVHWM")?;
        router
            .set_linger(self.linger_ms)
            .context("Failed to set ZMQ_LINGER")?;
        router
            .set_router_mandatory(true)
            .context("Failed to set ZMQ_ROUTER_MANDATORY")?;
        router
            .set_immediate(true)
            .context("Failed to set ZMQ_IMMEDIATE")?;
        router.bind(&requested_endpoint).context(format!(
            "Failed to bind ROUTER socket to {}",
            requested_endpoint
        ))?;

        let resolved_endpoint = router
            .get_last_endpoint()
            .context("Failed to get last endpoint")?
            .map_err(|_| anyhow::anyhow!("Failed to get resolved endpoint"))?;

        // Build the WorkerAddress with the resolved endpoint
        let mut addr_builder = crate::transports::address::WorkerAddressBuilder::new();
        addr_builder.add_entry(key.clone(), resolved_endpoint.as_bytes().to_vec())?;
        let local_address = addr_builder.build()?;

        // Generate unique inproc control endpoint for the listener thread
        let unique_id = crate::InstanceId::new_v4();
        let listener_control_endpoint = format!("inproc://zmq-listener-ctrl-{}", unique_id);

        Ok(ZmqTransport {
            key,
            bind_endpoint: resolved_endpoint,
            local_address,
            peers: Arc::new(DashMap::new()),
            zmq_context: Arc::new(ctx),
            sender_tx: OnceLock::new(),
            sender_stop: Arc::new(AtomicBool::new(false)),
            gates: DashMap::new(),
            runtime: OnceLock::new(),
            shutdown_state: OnceLock::new(),
            channel_capacity: self.channel_capacity,
            sndhwm: self.sndhwm,
            rcvhwm: self.rcvhwm,
            linger_ms: self.linger_ms,
            metrics: OnceLock::new(),
            listener_handle: std::sync::Mutex::new(None),
            sender_handle: std::sync::Mutex::new(None),
            listener_control_endpoint,
            router_socket: std::sync::Mutex::new(Some(router)),
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
    use crate::transports::address::WorkerAddressBuilder;
    use velo_ext::PeerInfo;

    fn make_zmq_peer(endpoint: &str) -> PeerInfo {
        let instance_id = crate::InstanceId::new_v4();
        let mut builder = WorkerAddressBuilder::new();
        builder
            .add_entry("zmq", endpoint.as_bytes().to_vec())
            .unwrap();
        PeerInfo::new(instance_id, builder.build().unwrap())
    }

    #[test]
    fn full_sender_queue_cannot_lose_shutdown_or_queued_replies() {
        struct BlockSender {
            entered: flume::Sender<()>,
            release: flume::Receiver<()>,
        }
        impl TransportErrorHandler for BlockSender {
            fn on_error(&self, _: Bytes, _: Bytes, _: String) {
                self.entered.send(()).unwrap();
                self.release.recv_timeout(Duration::from_secs(5)).unwrap();
            }
        }
        struct UnexpectedError;
        impl TransportErrorHandler for UnexpectedError {
            fn on_error(&self, _: Bytes, _: Bytes, error: String) {
                panic!("queued reply failed: {error}");
            }
        }
        let ctx = Arc::new(zmq::Context::new());
        let router = ctx.socket(zmq::ROUTER).unwrap();
        router.bind("tcp://127.0.0.1:*").unwrap();
        router.set_rcvtimeo(2000).unwrap();
        let target = crate::InstanceId::new_v4();
        let peers = Arc::new(DashMap::new());
        peers.insert(target, router.get_last_endpoint().unwrap().unwrap());
        let (tx, rx) = flume::bounded(1);
        let (entered_tx, entered) = flume::bounded(1);
        let (release, release_rx) = flume::bounded(1);
        tx.send(SenderCommand::Send(OutboundTask {
            target: crate::InstanceId::new_v4(),
            msg_type: MessageType::Message,
            header: Bytes::new(),
            payload: Bytes::new(),
            on_error: Arc::new(BlockSender {
                entered: entered_tx,
                release: release_rx,
            }),
        }))
        .unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = stop.clone();
        let (ready_tx, _ready) = std::sync::mpsc::sync_channel(1);
        let (done_tx, done) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            run_sender(SenderConfig {
                ctx,
                rx,
                stop: worker_stop,
                peers,
                identity: b"sender".to_vec(),
                sndhwm: 1,
                linger_ms: 1000,
                ready_tx,
            });
            done_tx.send(()).unwrap();
        });
        entered.recv_timeout(Duration::from_secs(2)).unwrap();
        tx.send(SenderCommand::Send(OutboundTask {
            target,
            msg_type: MessageType::Response,
            header: Bytes::new(),
            payload: Bytes::from_static(b"reply"),
            on_error: Arc::new(UnexpectedError),
        }))
        .unwrap();
        stop.store(true, Ordering::Release);
        assert!(matches!(
            tx.try_send(SenderCommand::Shutdown),
            Err(flume::TrySendError::Full(_))
        ));
        release.send(()).unwrap();
        let reply = router.recv_multipart(0);
        let stopped = done.recv_timeout(Duration::from_secs(2));
        // Keep the channel alive while checking that the stop request worked.
        drop(tx);
        worker.join().unwrap();
        assert_eq!(reply.unwrap().last().unwrap(), b"reply");
        stopped.expect("full queue lost the sender stop request");
    }

    /// Frames queued before a stop are still sent, but only within one shared
    /// budget. Each send to a peer that is gone blocks for the full send
    /// timeout, so a drain with no budget held the sender join, and so
    /// teardown, for that timeout once per queued frame.
    #[test]
    fn a_stopping_sender_drains_within_its_budget() {
        struct CountErrors(Arc<std::sync::atomic::AtomicUsize>);
        impl TransportErrorHandler for CountErrors {
            fn on_error(&self, _: Bytes, _: Bytes, _: String) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        // A port nothing listens on: with `ZMQ_IMMEDIATE` the dealer never
        // gets a pipe, so every send waits out its timeout.
        let dead = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("tcp://{}", dead.local_addr().unwrap());
        drop(dead);
        let target = crate::InstanceId::new_v4();
        let peers = Arc::new(DashMap::new());
        peers.insert(target, endpoint);
        let failed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let frames = 3;
        let (tx, rx) = flume::bounded(frames);
        for _ in 0..frames {
            tx.send(SenderCommand::Send(OutboundTask {
                target,
                msg_type: MessageType::Message,
                header: Bytes::new(),
                payload: Bytes::new(),
                on_error: Arc::new(CountErrors(Arc::clone(&failed))),
            }))
            .unwrap();
        }
        // Stopped before it starts, so every queued frame is part of the drain.
        let (ready_tx, _ready) = std::sync::mpsc::sync_channel(1);
        let started = std::time::Instant::now();
        run_sender(SenderConfig {
            ctx: Arc::new(zmq::Context::new()),
            rx,
            stop: Arc::new(AtomicBool::new(true)),
            peers,
            identity: b"sender".to_vec(),
            sndhwm: 1,
            linger_ms: 0,
            ready_tx,
        });
        let took = started.elapsed();
        assert!(
            took < DRAIN_BUDGET + Duration::from_secs(1),
            "draining {frames} frames to a dead peer took {took:?}"
        );
        assert_eq!(failed.load(Ordering::Relaxed), frames);
    }

    /// The listener must stop on the stop flag alone. Its control message is
    /// best effort: with no socket to send it on (file descriptors run out),
    /// a listener that waited only for that message never let the join return.
    #[test]
    fn a_listener_stops_without_its_control_message() {
        let (adapter, _streams) = crate::transports::transport::make_channels();
        let stop = Arc::new(AtomicBool::new(false));
        let (ready_tx, ready) = std::sync::mpsc::sync_channel(1);
        let (done_tx, done) = std::sync::mpsc::channel();
        let cfg = listener::ListenerConfig {
            ctx: Arc::new(zmq::Context::new()),
            bind_endpoint: "tcp://127.0.0.1:*".to_string(),
            control_endpoint: format!("inproc://listener-stop-{}", crate::InstanceId::new_v4()),
            adapter,
            rcvhwm: 1,
            linger_ms: 0,
            metrics: None,
            router_socket: None,
            ready_tx,
            stop: Arc::clone(&stop),
        };
        std::thread::spawn(move || {
            listener::run_listener(cfg);
            let _ = done_tx.send(());
        });
        ready.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
        stop.store(true, Ordering::Release);
        assert!(
            done.recv_timeout(Duration::from_secs(2)).is_ok(),
            "the listener ignored its stop flag"
        );
    }

    /// The control send in `stop_threads` must not block. The listener can
    /// stop on the flag alone and close its control socket between the
    /// connect and the send, and a blocking send to a PAIR whose peer closed
    /// waits forever, so the join after it never runs. This pins the libzmq
    /// behavior that `stop_threads` relies on: with `DONTWAIT` that send
    /// returns at once.
    #[test]
    fn a_control_send_to_a_closed_listener_does_not_block() {
        let ctx = zmq::Context::new();
        let endpoint = format!("inproc://control-{}", crate::InstanceId::new_v4());
        let (blocking_tx, blocking) = std::sync::mpsc::channel();
        let (dontwait_tx, dontwait) = std::sync::mpsc::channel();
        for (flags, done) in [(0, blocking_tx), (zmq::DONTWAIT, dontwait_tx)] {
            let listener = ctx.socket(zmq::PAIR).unwrap();
            let endpoint = format!("{endpoint}-{flags}");
            listener.bind(&endpoint).unwrap();
            let ctrl = ctx.socket(zmq::PAIR).unwrap();
            ctrl.connect(&endpoint).unwrap();
            drop(listener);
            std::thread::sleep(Duration::from_millis(50));
            std::thread::spawn(move || {
                let _ = done.send(ctrl.send("shutdown", flags));
            });
        }
        // The control: the blocking form really does hang here, so the case
        // below is about the flag, not about a send that could not block.
        assert!(
            blocking.recv_timeout(Duration::from_millis(500)).is_err(),
            "a blocking send to a closed PAIR returned; the test no longer shows the hang"
        );
        assert!(matches!(
            dontwait.recv_timeout(Duration::from_secs(2)),
            Ok(Err(zmq::Error::EAGAIN))
        ));
    }

    #[test]
    fn test_builder_default() {
        let transport = ZmqTransportBuilder::new().build();
        assert!(transport.is_ok());
    }

    /// ZMQ reports no capacity, and that has to stay deliberate: `ZMQ_SNDHWM`
    /// bounds queued messages rather than their size, and `ZMQ_MAXMSGSIZE` is
    /// left at libzmq's unlimited default. There is no limit to quote.
    #[test]
    fn max_message_size_is_unknown() {
        let transport = ZmqTransportBuilder::new().build().unwrap();
        assert_eq!(
            transport.max_message_size(crate::InstanceId::new_v4()),
            None
        );
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
    fn test_register_inproc_rejected() {
        let transport = ZmqTransportBuilder::new().build().unwrap();
        let peer = make_zmq_peer("inproc://test");
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
