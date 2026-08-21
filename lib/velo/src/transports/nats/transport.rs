// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! NATS transport implementation.
//!
//! [`NatsTransport`] implements the [`Transport`] trait using core NATS pub/sub and
//! request-reply. Use [`NatsTransportBuilder`] to construct an instance.
//!
//! All Transport trait methods are fully implemented across plans 01–03.

use crate::observability::{Direction, TransportRejection};
use bytes::{BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use futures::StreamExt;
use futures::future::BoxFuture;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// NATS header name for the Velo message type discriminator (D-05a).
///
/// Value is the [`MessageType`] as a decimal u8 string (e.g. `"0"` for Message).
/// Shared with the inbound path (`route_frame`) so both directions use the same header name.
pub(crate) const HEADER_VELO_TYPE: &str = "Velo-Type";

/// NATS header name for the Velo header-bytes length (D-05a).
///
/// Value is the length of the velo frame header in decimal (e.g. `"42"`).
/// The receiver uses this to split the NATS payload into header and payload portions.
pub(crate) const HEADER_VELO_HLEN: &str = "Velo-HLen";

use super::subjects;
use velo_ext::{
    AdmissionGate, AdmitOutcome, InstanceId, MessageType, PeerInfo, SendOutcome, TransportKey,
    WorkerAddress,
    transport::{
        HealthCheckError, ShutdownState, Transport, TransportAdapter, TransportError,
        TransportErrorHandler,
    },
};

/// Static string representations of [`MessageType`] discriminants.
///
/// Avoids a per-send heap allocation from `(msg_type as u8).to_string()`.
const VELO_TYPE_STRINGS: [&str; 5] = ["0", "1", "2", "3", "4"];

/// Default bounded-channel capacity for the sender task.
const DEFAULT_SENDER_CAPACITY: usize = 1024;

/// Bytes charged against the server's `max_payload` for Velo's own NATS
/// framing — the `Velo-Type` / `Velo-HLen` header block plus subject and
/// command bytes, since `max_payload` bounds the entire HPUB and not just its
/// body. A deliberate over-estimate, inherited from the send-side check.
///
/// [`send_message`](Transport::send_message) rejects against this number and
/// [`max_message_size`](Transport::max_message_size) reports against it, so
/// what the transport advertises and what it will actually accept are the same
/// arithmetic rather than two numbers that agree today.
const NATS_HEADER_OVERHEAD: usize = 64;

/// Task queued from [`send_message`](Transport::send_message) to the dedicated sender task.
struct NatsSendTask {
    subject: String,
    message_type: MessageType,
    header: Bytes,
    payload: Bytes,
    on_error: Arc<dyn TransportErrorHandler>,
}

/// NATS transport that implements the [`Transport`] trait.
///
/// Constructed via [`NatsTransportBuilder`].
pub struct NatsTransport {
    /// Unique transport key identifying this transport instance (default: `"nats"`).
    key: TransportKey,
    /// Shared NATS client. The caller owns the connection lifecycle.
    client: Arc<async_nats::Client>,
    /// Cluster identifier used as the NATS subject prefix.
    cluster_id: String,
    /// The local `WorkerAddress` fragment. Set once during `start()`.
    local_address: OnceLock<WorkerAddress>,
    /// Per-peer NATS subject strings.
    peers: Arc<DashMap<InstanceId, String>>,
    /// Sender channel for the dedicated sender task (set once during `start()`).
    sender_tx: OnceLock<flume::Sender<NatsSendTask>>,
    /// One admission gate per peer, all feeding `sender_tx`.
    ///
    /// The publish channel is shared by every peer, but ordering is only ever
    /// promised per target — so the gate is per target too. A peer whose frames
    /// are backing up queues behind its own gate instead of serialising
    /// everyone else's sends through it, and only the shared channel's capacity
    /// is contended.
    gates: DashMap<InstanceId, AdmissionGate<NatsSendTask>>,
    /// Bounded channel capacity for the sender task.
    sender_capacity: usize,
    /// Tokio runtime handle, set once during `start()`.
    runtime: OnceLock<tokio::runtime::Handle>,
    /// Transport-level cancellation token.
    cancel_token: CancellationToken,
    /// Dedicated token to signal the receive loop to unsubscribe before full cancel.
    /// `shutdown()` cancels this token; the loop unsubscribes both subscribers,
    /// then cancels `cancel_token` itself to propagate full teardown.
    begin_shutdown_token: CancellationToken,
    /// Shared shutdown state, set once during `start()`.
    shutdown_state: OnceLock<ShutdownState>,
    /// Shared observability collectors installed by the backend.
    metrics: OnceLock<std::sync::Arc<dyn velo_ext::TransportObservability>>,
}

impl NatsTransport {
    /// The largest `header + payload` this connection will carry right now.
    ///
    /// Read from the client at every use rather than cached, because the
    /// number is a property of the current connection: `max_payload` is
    /// renegotiated on every (re)connect, and a value snapshotted at `start()`
    /// describes whichever server was answering then. Both the capacity report
    /// and the pre-wire check call this, so what the transport advertises and
    /// what it will accept are one expression rather than two that agree
    /// today.
    ///
    /// [`async_nats::Client::max_payload`] is the same atomic async-nats
    /// itself validates publishes against, which is the reason to prefer it
    /// over `server_info().max_payload`: the two are refreshed together, this
    /// one costs an atomic load instead of cloning a `ServerInfo` full of
    /// `String`s on the send path, and reading it means we cannot reject a
    /// frame the client would have accepted, or advertise one it would not.
    /// Before the first `INFO` — a client built with `retry_on_initial_connect`
    /// while the server is down — that is async-nats' 1 MiB default, which is
    /// still exactly what this client will enforce, so the report stays honest
    /// about what a send will do rather than reporting "unknown".
    fn frame_capacity(&self) -> usize {
        self.client
            .max_payload()
            .saturating_sub(NATS_HEADER_OVERHEAD)
    }

    fn update_peer_gauge(&self) {
        if let Some(metrics) = self.metrics.get() {
            metrics.set_registered_peers(self.peers.len());
        }
    }

    /// The gate for one peer, created on first send to it.
    ///
    /// Gates outlive individual publishes and are never retired: NATS has no
    /// per-peer connection to die, so there is no epoch boundary at which
    /// queued frames would become invalid.
    fn gate_for(
        &self,
        target: InstanceId,
        tx: &flume::Sender<NatsSendTask>,
        rt: &tokio::runtime::Handle,
    ) -> AdmissionGate<NatsSendTask> {
        self.gates
            .entry(target)
            .or_insert_with(|| AdmissionGate::new(tx.clone(), rt.clone()))
            .clone()
    }
}

impl Transport for NatsTransport {
    fn key(&self) -> TransportKey {
        self.key.clone()
    }

    fn address(&self) -> WorkerAddress {
        self.local_address
            .get()
            .cloned()
            .unwrap_or_else(|| WorkerAddress::from_encoded(Bytes::from_static(&[])))
    }

    /// The connection's negotiated `max_payload`, less the
    /// [`NATS_HEADER_OVERHEAD`] this transport charges against it — literally
    /// [`frame_capacity`](Self::frame_capacity), the same call `send_message`
    /// makes before rejecting a frame.
    ///
    /// This is the one transport whose answer is truly negotiated: the value
    /// arrives from whichever server the client is connected to *now*, so it
    /// can differ between deployments and change under a reconnect. Never
    /// `None` — a client that has not been told a limit yet still has one it
    /// will enforce, and that is the number a caller needs.
    fn max_message_size(&self, _target: InstanceId) -> Option<usize> {
        Some(self.frame_capacity())
    }

    fn register(&self, peer_info: PeerInfo) -> Result<(), TransportError> {
        let instance_id = peer_info.instance_id();
        let entry = peer_info
            .worker_address()
            .get_entry("nats")
            .map_err(|_| TransportError::InvalidEndpoint)?
            .ok_or(TransportError::NoEndpoint)?;
        let subject =
            String::from_utf8(entry.to_vec()).map_err(|_| TransportError::InvalidEndpoint)?;
        tracing::debug!(
            instance_id = %instance_id,
            subject = %subject,
            "Registered NATS peer"
        );
        self.peers.insert(instance_id, subject);
        self.update_peer_gauge();
        Ok(())
    }

    #[inline]
    fn send_message(
        &self,
        instance_id: InstanceId,
        header: Bytes,
        payload: Bytes,
        message_type: MessageType,
        on_error: Arc<dyn TransportErrorHandler>,
    ) -> SendOutcome {
        // Look up peer's NATS subject.
        let subject = match self.peers.get(&instance_id) {
            Some(entry) => entry.value().clone(),
            None => {
                on_error.on_error(
                    header,
                    payload,
                    format!("Peer not registered: {}", instance_id),
                );
                return SendOutcome::Admitted;
            }
        };

        // Check the frame against what this connection will carry right now
        // (LIFECYCLE-02 enforcement). NATS `max_payload` covers the total HPUB
        // size, which is what `frame_capacity` has already discounted — and it
        // is the same call `max_message_size` answers with, so a frame sized
        // against the report is never rejected here.
        let frame_size = header.len() + payload.len();
        if frame_size > self.frame_capacity() {
            let max = self.client.max_payload();
            on_error.on_error(
                header,
                payload,
                format!(
                    "Frame size {} exceeds NATS max_payload {} for peer {}",
                    frame_size + NATS_HEADER_OVERHEAD,
                    max,
                    instance_id
                ),
            );
            return SendOutcome::Admitted;
        }

        let task = NatsSendTask {
            subject,
            message_type,
            header,
            payload,
            on_error,
        };

        // Lock-free reads via OnceLock — no mutex on the hot path.
        let (Some(tx), Some(rt)) = (self.sender_tx.get(), self.runtime.get()) else {
            task.on_error.on_error(
                task.header,
                task.payload,
                "NATS transport not started".into(),
            );
            return SendOutcome::Admitted;
        };

        let outcome = self.gate_for(instance_id, tx, rt).send(task);
        if let Some(m) = self.metrics.get()
            && !outcome.is_admitted()
        {
            m.record_send_backpressure();
        }
        outcome
    }

    fn start(
        &self,
        instance_id: InstanceId,
        channels: TransportAdapter,
        rt: tokio::runtime::Handle,
    ) -> BoxFuture<'_, anyhow::Result<()>> {
        let _ = self.runtime.set(rt.clone());
        let _ = self.shutdown_state.set(channels.shutdown_state.clone());

        Box::pin(async move {
            // LIFECYCLE-02: log what this connection will carry. Nothing is
            // cached from it — `frame_capacity` re-reads the client each time,
            // so a reconnect that renegotiates `max_payload` moves both the
            // report and the check without anyone having to refresh anything.
            tracing::info!(
                max_payload = self.client.max_payload(),
                frame_capacity = self.frame_capacity(),
                "NATS max_payload for this connection"
            );

            // TRANSPORT-03: Build WorkerAddress with "nats" entry containing inbound subject
            let subject = subjects::inbound_subject(&self.cluster_id, instance_id);
            let health_subj = subjects::health_subject(&self.cluster_id, instance_id);

            let mut addr_builder = crate::transports::address::WorkerAddressBuilder::new();
            addr_builder.add_entry("nats", Bytes::from(subject.as_bytes().to_vec()))?;
            let _ = self.local_address.set(addr_builder.build()?);

            // LIFECYCLE-01: Subscribe BEFORE returning Ok — subscribe-before-advertise
            let data_sub = self.client.subscribe(subject.clone()).await.map_err(|e| {
                anyhow::anyhow!("Failed to subscribe to inbound subject {}: {}", subject, e)
            })?;
            let health_sub = self
                .client
                .subscribe(health_subj.clone())
                .await
                .map_err(|e| {
                    anyhow::anyhow!(
                        "Failed to subscribe to health subject {}: {}",
                        health_subj,
                        e
                    )
                })?;

            tracing::info!(
                data_subject = %subject,
                health_subject = %health_subj,
                "NATS transport started, subscriptions live"
            );

            // Create bounded sender channel and spawn dedicated sender task.
            let (sender_tx, sender_rx) = flume::bounded(self.sender_capacity);
            let _ = self.sender_tx.set(sender_tx);

            let sender_cancel = self.cancel_token.clone();
            let sender_client = self.client.clone();
            let sender_metrics = self.metrics.get().cloned();
            rt.spawn(run_sender_task(
                sender_rx,
                sender_client,
                sender_cancel,
                sender_metrics,
            ));

            // Spawn receive loop (LIFECYCLE-03)
            let cancel = self.cancel_token.clone();
            let begin_shutdown = self.begin_shutdown_token.clone();
            let client = self.client.clone();
            let transport_key = self.key.to_string();
            let metrics = self.metrics.get().cloned();
            rt.spawn(async move {
                run_receive_loop(
                    data_sub,
                    health_sub,
                    channels,
                    cancel,
                    begin_shutdown,
                    client,
                    transport_key,
                    metrics,
                )
                .await;
            });

            Ok(())
        })
    }

    fn begin_drain(&self) {
        // Per-frame gate reads the shared ShutdownState, which the runtime
        // flips — flipping it here would drain every sibling transport of the
        // instance. No-op, matching TCP; the
        // gate itself lives in run_receive_loop.
    }

    fn shutdown(&self) {
        // Signal the receive loop to unsubscribe before full cancel (D-06, LIFECYCLE-05).
        // The loop's begin_shutdown.cancelled() arm will call unsubscribe() on both
        // subscribers, then cancel cancel_token to propagate full teardown.
        self.begin_shutdown_token.cancel();
        // Also cancel directly in case the loop is not running (transport never started,
        // or loop already exited via stream-end).
        self.cancel_token.cancel();
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
        instance_id: InstanceId,
        timeout: Duration,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), HealthCheckError>> + Send + '_>,
    > {
        Box::pin(async move {
            let _rt = self
                .runtime
                .get()
                .ok_or(HealthCheckError::TransportNotStarted)?;
            let subject = self
                .peers
                .get(&instance_id)
                .ok_or(HealthCheckError::PeerNotRegistered)?
                .clone();

            // Build health subject from the peer's inbound subject by appending ".health"
            let health_subj = format!("{}.health", subject);

            // LIFECYCLE-06 (D-07): Map all RequestError variants (including NoResponders)
            // uniformly to ConnectionFailed. Timeout maps to Timeout.
            // This mapping is correct as-is — no code changes needed.
            let client = self.client.clone();
            match tokio::time::timeout(timeout, client.request(health_subj, Bytes::new())).await {
                Ok(Ok(_response)) => Ok(()),
                Ok(Err(_e)) => Err(HealthCheckError::ConnectionFailed),
                Err(_elapsed) => Err(HealthCheckError::Timeout),
            }
        })
    }
}

/// Build NATS headers and concatenated payload from a [`NatsSendTask`].
///
/// Returns `(headers, payload)` ready for `client.publish_with_headers()`.
fn build_nats_frame(
    message_type: MessageType,
    header: &Bytes,
    payload: &Bytes,
) -> (async_nats::HeaderMap, Bytes) {
    let mut nats_headers = async_nats::HeaderMap::new();
    nats_headers.insert(
        HEADER_VELO_TYPE,
        VELO_TYPE_STRINGS[message_type as u8 as usize],
    );
    let hlen_str = header.len().to_string();
    nats_headers.insert(HEADER_VELO_HLEN, hlen_str);

    let nats_payload: Bytes = if header.is_empty() {
        payload.clone()
    } else if payload.is_empty() {
        header.clone()
    } else {
        let mut buf = BytesMut::with_capacity(header.len() + payload.len());
        buf.put(header.as_ref());
        buf.put(payload.as_ref());
        buf.freeze()
    };

    (nats_headers, nats_payload)
}

/// Dedicated sender task that drains the bounded channel and publishes to NATS.
///
/// Runs until the cancellation token fires or the channel is closed (all senders dropped).
/// All NATS header construction and payload concatenation happen here, keeping the
/// [`send_message`](Transport::send_message) hot path allocation-free.
async fn run_sender_task(
    rx: flume::Receiver<NatsSendTask>,
    client: Arc<async_nats::Client>,
    cancel: CancellationToken,
    metrics: Option<std::sync::Arc<dyn velo_ext::TransportObservability>>,
) {
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                tracing::debug!("NATS sender task cancelled");
                break;
            }
            result = rx.recv_async() => {
                match result {
                    Ok(task) => {
                        let (nats_headers, nats_payload) = build_nats_frame(
                            task.message_type,
                            &task.header,
                            &task.payload,
                        );

                        if let Some(metrics) = metrics.as_ref() {
                            metrics.record_frame(
                                Direction::Outbound,
                                crate::transports::message_type_label(task.message_type),
                                nats_payload.len(),
                            );
                        }

                        if let Err(e) = client
                            .publish_with_headers(task.subject, nats_headers, nats_payload)
                            .await
                        {
                            task.on_error.on_error(
                                task.header,
                                task.payload,
                                format!("NATS publish failed: {}", e),
                            );
                        }
                    }
                    Err(_) => {
                        tracing::debug!("NATS sender channel closed, exiting");
                        break;
                    }
                }
            }
        }
    }
}

/// Receive loop that routes inbound frames to the correct TransportAdapter channels (LIFECYCLE-03).
///
/// Runs until the begin_shutdown token fires (graceful path), the cancel token fires (direct path),
/// or a subscription stream ends. On graceful shutdown, unsubscribes both subscribers before
/// cancelling the main cancel token (LIFECYCLE-05 D-06 unsubscribe-before-cancel ordering).
/// During drain (LIFECYCLE-04), inbound Message frames are rejected with ShuttingDown responses
/// for request-reply sends, or silently discarded for fire-and-forget sends.
#[allow(clippy::too_many_arguments)]
async fn run_receive_loop(
    mut data_sub: async_nats::Subscriber,
    mut health_sub: async_nats::Subscriber,
    adapter: TransportAdapter,
    cancel: CancellationToken,
    begin_shutdown: CancellationToken,
    client: Arc<async_nats::Client>,
    transport_key: String,
    metrics: Option<std::sync::Arc<dyn velo_ext::TransportObservability>>,
) {
    loop {
        tokio::select! {
            biased;

            // LIFECYCLE-05: Graceful shutdown — unsubscribe before cancel.
            // shutdown() cancels begin_shutdown_token; we unsubscribe both
            // subscribers here, then propagate full cancellation.
            _ = begin_shutdown.cancelled() => {
                tracing::debug!("NATS shutdown signaled, unsubscribing before cancel");
                let _ = data_sub.unsubscribe().await;
                let _ = health_sub.unsubscribe().await;
                cancel.cancel(); // propagate full shutdown
                break;
            }

            _ = cancel.cancelled() => {
                tracing::debug!("NATS receive loop cancelled directly");
                break;
            }

            msg = data_sub.next() => {
                match msg {
                    Some(msg) => {
                        // LIFECYCLE-04: the drain gate lives inside route_frame
                        // (it is the only place that has parsed the header out
                        // of the payload). D-04: non-Message frames — Response,
                        // Ack, Event — are routed regardless of drain state.
                        match route_frame(&msg, &adapter, &transport_key, metrics.as_ref()) {
                            NatsRouted::Done => {}
                            NatsRouted::DrainRejected { header } => {
                                if let Some(reply) = &msg.reply {
                                    // D-02: Send ShuttingDown response echoing original header
                                    // for correlation, with empty velo payload.
                                    let mut nats_headers = async_nats::HeaderMap::new();
                                    nats_headers.insert(
                                        HEADER_VELO_TYPE,
                                        (MessageType::ShuttingDown as u8).to_string().as_str(),
                                    );
                                    nats_headers.insert(
                                        HEADER_VELO_HLEN,
                                        header.len().to_string().as_str(),
                                    );

                                    if let Err(e) = client.publish_with_headers(
                                        reply.clone(),
                                        nats_headers,
                                        header,
                                    ).await {
                                        tracing::warn!(error = %e, "Failed to send ShuttingDown response");
                                    }
                                } else {
                                    // D-03: Fire-and-forget during drain — silently discard.
                                    tracing::debug!(
                                        "Discarding fire-and-forget Message during drain (no reply inbox)"
                                    );
                                }
                            }
                        }
                    }
                    None => {
                        tracing::warn!("NATS data subscription stream ended");
                        break;
                    }
                }
            }

            msg = health_sub.next() => {
                match msg {
                    Some(msg) => {
                        if let Some(reply) = msg.reply
                            && let Err(e) = client.publish(reply, Bytes::new()).await
                        {
                            tracing::warn!(error = %e, "Failed to reply to health check");
                        }
                    }
                    None => {
                        tracing::warn!("NATS health subscription stream ended");
                        break;
                    }
                }
            }
        }
    }
    // Post-loop unsubscribe as fallback (in case loop exited via stream-end
    // rather than through the begin_shutdown arm).
    let _ = data_sub.unsubscribe().await;
    let _ = health_sub.unsubscribe().await;
    tracing::debug!("NATS receive loop exited, subscriptions unsubscribed");
}

/// Outcome of routing one inbound NATS message.
enum NatsRouted {
    /// Delivered to its stream, or dropped as malformed. Nothing left to do.
    Done,
    /// LIFECYCLE-04: an inbound `Message` was refused because this instance is
    /// draining. The caller owes the sender a `ShuttingDown` reply echoing this
    /// header (D-02) — or, with no reply inbox, a debug line (D-03).
    DrainRejected { header: Bytes },
}

/// Route an inbound NATS message to the correct [`TransportAdapter`] channel (D-05a).
///
/// Reads frame metadata from NATS headers:
/// - `Velo-Type`: message type as a decimal u8 string
/// - `Velo-HLen`: velo header length in bytes as a decimal string
///
/// The NATS payload is `velo_header_bytes ++ velo_payload_bytes` (no binary preamble).
/// Messages missing required headers or with invalid formats are silently dropped.
///
/// This is also the drain gate: `Message` frames go through
/// [`TransportAdapter::admit_message`], which acquires the in-flight guard
/// before it re-reads the draining flag, so an admitted message is work
/// `wait_for_drain` can see even while it is only queued. The gate lives here
/// rather than in the receive loop because the header it must echo back is a
/// slice of the payload this function has already validated.
fn route_frame(
    msg: &async_nats::Message,
    adapter: &TransportAdapter,
    transport_key: &str,
    metrics: Option<&std::sync::Arc<dyn velo_ext::TransportObservability>>,
) -> NatsRouted {
    #[cfg(not(feature = "distributed-tracing"))]
    let _ = transport_key;
    let headers = match &msg.headers {
        Some(h) => h,
        None => {
            if let Some(metrics) = metrics {
                metrics.record_rejection(TransportRejection::MissingHeaders);
            }
            tracing::trace!("Dropping NATS message with no headers");
            return NatsRouted::Done;
        }
    };

    // Parse message type from Velo-Type header
    let type_str = match headers.get(HEADER_VELO_TYPE) {
        Some(v) => v.as_str(),
        None => {
            if let Some(metrics) = metrics {
                metrics.record_rejection(TransportRejection::MissingType);
            }
            tracing::trace!("Dropping NATS message missing Velo-Type header");
            return NatsRouted::Done;
        }
    };
    let msg_type = match type_str.parse::<u8>() {
        Ok(0) => MessageType::Message,
        Ok(1) => MessageType::Response,
        Ok(2) => MessageType::Ack,
        Ok(3) => MessageType::Event,
        Ok(4) => MessageType::ShuttingDown,
        _ => {
            if let Some(metrics) = metrics {
                metrics.record_rejection(TransportRejection::InvalidType);
            }
            tracing::trace!(
                velo_type = type_str,
                "Dropping NATS message with invalid Velo-Type"
            );
            return NatsRouted::Done;
        }
    };

    // Parse header length from Velo-HLen header
    let hlen: usize = match headers
        .get(HEADER_VELO_HLEN)
        .and_then(|v| v.as_str().parse().ok())
    {
        Some(n) => n,
        None => {
            if let Some(metrics) = metrics {
                metrics.record_rejection(TransportRejection::InvalidHeaderLength);
            }
            tracing::trace!("Dropping NATS message missing or invalid Velo-HLen header");
            return NatsRouted::Done;
        }
    };

    // NATS payload = velo_header ++ velo_payload
    if msg.payload.len() < hlen {
        if let Some(metrics) = metrics {
            metrics.record_rejection(TransportRejection::TruncatedFrame);
        }
        tracing::trace!(
            expected_min = hlen,
            actual = msg.payload.len(),
            "Dropping truncated NATS frame"
        );
        return NatsRouted::Done;
    }
    let header = msg.payload.slice(..hlen);
    let body = msg.payload.slice(hlen..);

    let frame_bytes = header.len() + body.len();

    let result = match msg_type {
        MessageType::Message => match adapter.admit_message(header, body) {
            AdmitOutcome::Admitted => Ok(()),
            AdmitOutcome::Draining { header, .. } => {
                if let Some(metrics) = metrics {
                    metrics.record_rejection(TransportRejection::DrainRejected);
                }
                return NatsRouted::DrainRejected { header };
            }
            AdmitOutcome::Disconnected { .. } => Err(()),
        },
        MessageType::Response => adapter
            .response_stream
            .try_send((header, body))
            .map_err(|_| ()),
        MessageType::Ack | MessageType::Event => adapter
            .event_stream
            .try_send((header, body))
            .map_err(|_| ()),
        MessageType::ShuttingDown => adapter
            .shutdown_stream
            .try_send((header, body))
            .map_err(|_| ()),
    };

    match result {
        // Counted only once the frame is actually on its stream: a
        // drain-rejected Message returns above and counts as a rejection, not
        // as inbound traffic.
        Ok(()) => {
            if let Some(metrics) = metrics {
                #[cfg(feature = "distributed-tracing")]
                let span = tracing::debug_span!(
                    "velo.transport.receive",
                    transport = transport_key,
                    message_type = crate::transports::message_type_label(msg_type),
                    bytes = frame_bytes
                );
                #[cfg(feature = "distributed-tracing")]
                let _entered = span.enter();

                metrics.record_frame(
                    Direction::Inbound,
                    crate::transports::message_type_label(msg_type),
                    frame_bytes,
                );
            }
        }
        Err(()) => {
            if let Some(metrics) = metrics {
                metrics.record_rejection(TransportRejection::RouteFailed);
            }
        }
    }

    NatsRouted::Done
}

/// Builder for [`NatsTransport`].
///
/// # Example
///
/// ```ignore
/// let transport = NatsTransportBuilder::new(client, "my-cluster").build();
/// ```
pub struct NatsTransportBuilder {
    client: Arc<async_nats::Client>,
    cluster_id: String,
    key: TransportKey,
    sender_capacity: usize,
}

impl NatsTransportBuilder {
    /// Create a new builder with the given NATS client and cluster ID.
    ///
    /// The default transport key is `"nats"`. Use [`with_key`](Self::with_key) to override.
    pub fn new(client: Arc<async_nats::Client>, cluster_id: impl Into<String>) -> Self {
        Self {
            client,
            cluster_id: cluster_id.into(),
            key: TransportKey::from("nats"),
            sender_capacity: DEFAULT_SENDER_CAPACITY,
        }
    }

    /// Override the transport key (default: `"nats"`).
    pub fn with_key(mut self, key: impl Into<TransportKey>) -> Self {
        self.key = key.into();
        self
    }

    /// Override the bounded sender channel capacity (default: 1024).
    pub fn with_sender_capacity(mut self, capacity: usize) -> Self {
        self.sender_capacity = capacity;
        self
    }

    /// Consume the builder and produce a [`NatsTransport`].
    pub fn build(self) -> NatsTransport {
        NatsTransport {
            key: self.key,
            client: self.client,
            cluster_id: self.cluster_id,
            local_address: OnceLock::new(),
            peers: Arc::new(DashMap::new()),
            sender_tx: OnceLock::new(),
            gates: DashMap::new(),
            sender_capacity: self.sender_capacity,
            runtime: OnceLock::new(),
            cancel_token: CancellationToken::new(),
            begin_shutdown_token: CancellationToken::new(),
            shutdown_state: OnceLock::new(),
            metrics: OnceLock::new(),
        }
    }
}

// `#[path]` keeps the tests beside their siblings as `nats/tests.rs`; the
// default resolution would bury them in a one-file `nats/transport/` directory.
#[cfg(test)]
#[path = "tests.rs"]
mod tests;
