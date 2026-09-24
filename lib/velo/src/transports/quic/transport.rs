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
use crate::transports::tcp::framing::DEFAULT_MAX_FRAME_SIZE;
use crate::transports::transport::{
    AdmissionError, AdmissionGate, HealthCheckError, SendOutcome, ShutdownState, TransportError,
    TransportErrorHandler,
};
use crate::transports::utils::interfaces::{
    InterfaceEndpoint, InterfaceFilter, resolve_advertise_endpoints, select_best_endpoint,
};

use super::endpoint::QuicEndpointInfo;
use super::listener::{AcceptContext, OrderlyEnd, run_accept_loop};
use super::tls;

/// How long a writer that stops waits for the peer to acknowledge its last
/// bytes before it closes the connection, on every stop including teardown,
/// unless the connection has already ended.
/// Closing at once discards what the peer has not acknowledged, which TCP
/// would still deliver after a close. The dial endpoint closes only after
/// the writers finish, or after CLOSE_WAIT.
const FINISH_GRACE: Duration = Duration::from_secs(1);

/// How long teardown waits for the writers to finish their streams and for
/// the connections to close before it closes the dial endpoint by force.
/// Twice FINISH_GRACE, so a writer that was mid-write when teardown began
/// still gets its full acknowledgement wait.
const CLOSE_WAIT: Duration = FINISH_GRACE.saturating_mul(2);

/// How long `closed()` waits, after it force-closes the dial endpoint, for the
/// writers that were still blocked to report their frames as failed.
const FAIL_REPORT_GRACE: Duration = Duration::from_millis(500);

mod builder;
pub use builder::QuicTransportBuilder;

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
    /// Every connection writer, so `closed()` can wait for them to finish
    /// their streams.
    writers: tokio_util::task::TaskTracker,
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

        self.writers.spawn_on(
            connection_writer_task(
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
            ),
            rt,
        );
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
        self.writers.close();
        for endpoint in self.server_endpoints.get().into_iter().flatten() {
            endpoint.close(quinn::VarInt::from_u32(0), b"shutdown");
        }
        // Not the dial endpoint yet: closing it now would discard what the
        // writers wrote but the peers have not acknowledged. Each writer
        // finishes its stream and closes its connection within FINISH_GRACE
        // of its last write; a writer parked on a peer's flow control is
        // closed by force at CLOSE_WAIT. `closed()` waits for that; this task
        // covers a caller that does not await it, as long as the runtime lives.
        if let Some(endpoint) = self.client_endpoint.get().cloned() {
            match self.runtime.get() {
                Some(rt) => {
                    rt.spawn(async move {
                        let _ = tokio::time::timeout(CLOSE_WAIT, endpoint.wait_idle()).await;
                        endpoint.close(quinn::VarInt::from_u32(0), b"shutdown");
                    });
                }
                None => endpoint.close(quinn::VarInt::from_u32(0), b"shutdown"),
            }
        }
        self.connections.clear();
        self.update_connection_gauge();
    }

    /// Wait for every connection writer to finish its stream (each waits up
    /// to FINISH_GRACE for the peer's acknowledgement) and for every
    /// endpoint's connections to close, then close the dial endpoint.
    /// Bounded by CLOSE_WAIT plus FAIL_REPORT_GRACE. Returns at once if
    /// `shutdown()` has not run, because the writers are still live.
    fn closed(&self) -> futures::future::BoxFuture<'_, ()> {
        Box::pin(async move {
            if !self.cancel_token.is_cancelled() {
                return;
            }
            let _ = tokio::time::timeout(CLOSE_WAIT, async {
                self.writers.wait().await;
                if let Some(endpoint) = self.client_endpoint.get() {
                    endpoint.wait_idle().await;
                }
                // `shutdown()` closed the server endpoints, but a close only
                // queues CONNECTION_CLOSE for each connection's driver. On a
                // current-thread runtime nothing else runs while this future
                // is polled, so without this wait an accept-only node returns
                // at once and its peers learn of the close at their idle
                // timeout.
                for endpoint in self.server_endpoints.get().into_iter().flatten() {
                    endpoint.wait_idle().await;
                }
            })
            .await;
            if let Some(endpoint) = self.client_endpoint.get() {
                endpoint.close(quinn::VarInt::from_u32(0), b"shutdown");
            }
            // A writer still blocked on a peer that stopped reading fails its
            // frames only when the close above ends its write. Wait for those
            // reports, so every frame is delivered or failed when this returns.
            let _ = tokio::time::timeout(FAIL_REPORT_GRACE, self.writers.wait()).await;
        })
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
    // already wrote. See FINISH_GRACE. Only `Ok(None)` means the peer
    // acknowledged every byte.
    //
    // A peer that closed the connection has gone, and what it did not read is
    // gone with it, as with a TCP peer that closes. That is the ordinary end of
    // a peer restart, so it is logged at debug, whether the close came before
    // the finish or during the wait. Every other end (our own force close at
    // CLOSE_WAIT, an idle timeout, a reset, a STOP_SENDING, the wait timing
    // out) can leave written frames unread, so it is a warning.
    let loss = |why: String| {
        warn!(
            "QUIC: {instance_id} ({}) did not acknowledge the stream end ({why}); \
             frames written but not acknowledged can be lost",
            peer.addr
        )
    };
    match connection.close_reason() {
        Some(quinn::ConnectionError::ApplicationClosed(reason)) => {
            debug!("QUIC connection to {instance_id} closed by peer: {reason}");
        }
        Some(quinn::ConnectionError::LocallyClosed) => {
            loss(format!("closed by force at shutdown, after {CLOSE_WAIT:?}"))
        }
        Some(reason) => loss(format!("connection ended: {reason}")),
        None if send.finish().is_ok() => {
            match tokio::time::timeout(FINISH_GRACE, send.stopped()).await {
                Ok(Ok(None)) => {}
                Ok(Err(quinn::StoppedError::ConnectionLost(
                    quinn::ConnectionError::ApplicationClosed(reason),
                ))) => {
                    debug!("QUIC connection to {instance_id} closed by peer: {reason}");
                }
                Err(_) => loss(format!("no acknowledgement within {FINISH_GRACE:?}")),
                Ok(outcome) => loss(format!("{outcome:?}")),
            }
        }
        None => {}
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

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
