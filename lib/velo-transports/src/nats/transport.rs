// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! NATS transport implementation.
//!
//! [`NatsTransport`] implements the [`Transport`] trait using core NATS pub/sub and
//! request-reply. Use [`NatsTransportBuilder`] to construct an instance.
//!
//! All Transport trait methods are fully implemented across plans 01–03.

use bytes::{BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use futures::StreamExt;
use futures::future::BoxFuture;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use velo_observability::{Direction, TransportRejection, VeloMetrics};

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
use crate::{
    InstanceId, MessageType, PeerInfo, TransportKey, WorkerAddress,
    transport::{
        HealthCheckError, ShutdownState, Transport, TransportAdapter, TransportError,
        TransportErrorHandler,
    },
};

/// How often to use `.request()` instead of `.publish()` for liveness probing (D-05).
///
/// The first send to any peer always uses `.request()` (send count == 0).
/// After that, every `PROBE_INTERVAL`-th send will use `.request()`.
const PROBE_INTERVAL: u64 = 100;

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
    /// Per-peer send counters for adaptive request/publish strategy.
    send_counts: Arc<DashMap<InstanceId, AtomicU64>>,
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
    /// Maximum NATS payload size in bytes (queried from server on start).
    max_payload: Arc<AtomicUsize>,
    /// Shared observability collectors installed by the backend.
    observability: OnceLock<Arc<VeloMetrics>>,
    metrics: OnceLock<velo_observability::TransportMetricsHandle>,
}

impl NatsTransport {
    fn update_peer_gauge(&self) {
        if let Some(metrics) = self.metrics.get() {
            metrics.set_registered_peers(self.peers.len());
        }
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

    fn send_message(
        &self,
        instance_id: InstanceId,
        header: Bytes,
        payload: Bytes,
        message_type: MessageType,
        on_error: Arc<dyn TransportErrorHandler>,
    ) {
        // Look up peer's NATS subject.
        let subject = match self.peers.get(&instance_id) {
            Some(entry) => entry.value().clone(),
            None => {
                on_error.on_error(
                    header,
                    payload,
                    format!("Peer not registered: {}", instance_id),
                );
                return;
            }
        };

        // Check total frame size against max_payload (LIFECYCLE-02 enforcement).
        // NATS max_payload covers the total HPUB size (headers + payload).
        // Conservative estimate: ~64 bytes for Velo NATS header overhead.
        const NATS_HEADER_OVERHEAD: usize = 64;
        let total_size = header.len() + payload.len() + NATS_HEADER_OVERHEAD;
        let max = self.max_payload.load(Ordering::Relaxed);
        if total_size > max {
            on_error.on_error(
                header,
                payload,
                format!(
                    "Frame size {} exceeds NATS max_payload {} for peer {}",
                    total_size, max, instance_id
                ),
            );
            return;
        }

        // Get runtime handle — transport must be started before sending.
        let rt = match self.runtime.get() {
            Some(rt) => rt.clone(),
            None => {
                on_error.on_error(header, payload, "NATS transport not started".into());
                return;
            }
        };

        // D-05: Determine if this send should use .request() for liveness probing.
        // First send to a peer (count == 0) and every PROBE_INTERVAL-th send use request.
        let count = self
            .send_counts
            .entry(instance_id)
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
        let use_request = count % PROBE_INTERVAL == 0;

        // Clone client for async move block.
        let client = self.client.clone();

        rt.spawn(async move {
            // Build NATS headers with Velo frame metadata (D-05a).
            let mut nats_headers = async_nats::HeaderMap::new();
            nats_headers.insert(HEADER_VELO_TYPE, (message_type as u8).to_string().as_str());
            nats_headers.insert(HEADER_VELO_HLEN, header.len().to_string().as_str());

            // NATS payload = velo_header ++ velo_payload (no binary preamble — D-05a).
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

            if use_request {
                // D-05: Liveness probe — use request_with_headers, expect ack from receiver.
                match tokio::time::timeout(
                    Duration::from_secs(5),
                    client.request_with_headers(subject, nats_headers, nats_payload),
                )
                .await
                {
                    Ok(Ok(_)) => {} // Ack received — peer is alive.
                    Ok(Err(e)) => {
                        on_error.on_error(
                            header,
                            payload,
                            format!("NATS request failed (peer may be unreachable): {}", e),
                        );
                    }
                    Err(_elapsed) => {
                        on_error.on_error(
                            header,
                            payload,
                            format!("NATS request timed out for peer {}", instance_id),
                        );
                    }
                }
            } else {
                // Fire-and-forget publish.
                if let Err(e) = client
                    .publish_with_headers(subject, nats_headers, nats_payload)
                    .await
                {
                    on_error.on_error(header, payload, format!("NATS publish failed: {}", e));
                }
            }
        });
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
            // LIFECYCLE-02: Read max_payload from server_info
            let max = self.client.server_info().max_payload;
            self.max_payload.store(max, Ordering::Relaxed);
            tracing::info!(max_payload = max, "NATS max_payload from server_info");

            // TRANSPORT-03: Build WorkerAddress with "nats" entry containing inbound subject
            let subject = subjects::inbound_subject(&self.cluster_id, instance_id);
            let health_subj = subjects::health_subject(&self.cluster_id, instance_id);

            let mut addr_builder = crate::address::WorkerAddressBuilder::new();
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
        if let Some(state) = self.shutdown_state.get() {
            state.begin_drain();
        }
        // No-op if shutdown_state not yet initialized (transport not started)
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

    fn set_observability(&self, observability: Arc<VeloMetrics>) {
        let _ = self
            .metrics
            .set(observability.bind_transport(self.key.as_str()));
        let _ = self.observability.set(observability);
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

/// Receive loop that routes inbound frames to the correct TransportAdapter channels (LIFECYCLE-03).
///
/// Runs until the begin_shutdown token fires (graceful path), the cancel token fires (direct path),
/// or a subscription stream ends. On graceful shutdown, unsubscribes both subscribers before
/// cancelling the main cancel token (LIFECYCLE-05 D-06 unsubscribe-before-cancel ordering).
/// During drain (LIFECYCLE-04), inbound Message frames are rejected with ShuttingDown responses
/// for request-reply sends, or silently discarded for fire-and-forget sends.
async fn run_receive_loop(
    mut data_sub: async_nats::Subscriber,
    mut health_sub: async_nats::Subscriber,
    adapter: TransportAdapter,
    cancel: CancellationToken,
    begin_shutdown: CancellationToken,
    client: Arc<async_nats::Client>,
    transport_key: String,
    metrics: Option<velo_observability::TransportMetricsHandle>,
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
                        // LIFECYCLE-04: Drain gate — reject Message frames during drain.
                        // Must come BEFORE D-05 ack to avoid sending both ack and ShuttingDown.
                        if adapter.shutdown_state.is_draining() {
                            // Parse Velo-Type to check if this is a Message frame.
                            let is_message = msg.headers.as_ref()
                                .and_then(|h| h.get(HEADER_VELO_TYPE))
                                .and_then(|v| v.as_str().parse::<u8>().ok())
                                .map(|t| t == MessageType::Message as u8)
                                .unwrap_or(false);

                            if is_message {
                                if let Some(metrics) = metrics.as_ref() {
                                    metrics.record_rejection(TransportRejection::DrainRejected);
                                }
                                if let Some(reply) = &msg.reply {
                                    // D-02: Send ShuttingDown response echoing original header
                                    // for correlation, with empty velo payload.
                                    let hlen: usize = msg.headers.as_ref()
                                        .and_then(|h| h.get(HEADER_VELO_HLEN))
                                        .and_then(|v| v.as_str().parse().ok())
                                        .unwrap_or(0);
                                    let original_header = if hlen > 0 && msg.payload.len() >= hlen {
                                        msg.payload.slice(..hlen)
                                    } else {
                                        Bytes::new()
                                    };

                                    let mut nats_headers = async_nats::HeaderMap::new();
                                    nats_headers.insert(
                                        HEADER_VELO_TYPE,
                                        (MessageType::ShuttingDown as u8).to_string().as_str(),
                                    );
                                    nats_headers.insert(
                                        HEADER_VELO_HLEN,
                                        original_header.len().to_string().as_str(),
                                    );

                                    if let Err(e) = client.publish_with_headers(
                                        reply.clone(),
                                        nats_headers,
                                        original_header,
                                    ).await {
                                        tracing::warn!(error = %e, "Failed to send ShuttingDown response");
                                    }
                                } else {
                                    // D-03: Fire-and-forget during drain — silently discard.
                                    tracing::debug!(
                                        "Discarding fire-and-forget Message during drain (no reply inbox)"
                                    );
                            }
                            continue;
                        }
                            // D-04: Non-Message frames (Response, Ack, Event) fall through
                            // to normal ack + routing below.
                        }

                        // D-05: Ack if sender used .request() (adaptive liveness probe)
                        if let Some(reply) = &msg.reply {
                            if let Err(e) = client.publish(reply.clone(), Bytes::new()).await {
                                tracing::warn!(error = %e, "Failed to ack data message");
                            }
                        }
                        route_frame(&msg, &adapter, &transport_key, metrics.as_ref());
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
                        if let Some(reply) = msg.reply {
                            if let Err(e) = client.publish(reply, Bytes::new()).await {
                                tracing::warn!(error = %e, "Failed to reply to health check");
                            }
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

/// Route an inbound NATS message to the correct [`TransportAdapter`] channel (D-05a).
///
/// Reads frame metadata from NATS headers:
/// - `Velo-Type`: message type as a decimal u8 string
/// - `Velo-HLen`: velo header length in bytes as a decimal string
///
/// The NATS payload is `velo_header_bytes ++ velo_payload_bytes` (no binary preamble).
/// Messages missing required headers or with invalid formats are silently dropped.
fn route_frame(
    msg: &async_nats::Message,
    adapter: &TransportAdapter,
    transport_key: &str,
    metrics: Option<&velo_observability::TransportMetricsHandle>,
) {
    #[cfg(not(feature = "distributed-tracing"))]
    let _ = transport_key;
    let headers = match &msg.headers {
        Some(h) => h,
        None => {
            if let Some(metrics) = metrics {
                metrics.record_rejection(TransportRejection::MissingHeaders);
            }
            tracing::trace!("Dropping NATS message with no headers");
            return;
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
            return;
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
            return;
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
            return;
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
        return;
    }
    let header = msg.payload.slice(..hlen);
    let body = msg.payload.slice(hlen..);

    if let Some(metrics) = metrics {
        #[cfg(feature = "distributed-tracing")]
        let span = tracing::debug_span!(
            "velo.transport.receive",
            transport = transport_key,
            message_type = crate::message_type_label(msg_type),
            bytes = header.len() + body.len()
        );
        #[cfg(feature = "distributed-tracing")]
        let _entered = span.enter();

        metrics.record_frame(
            Direction::Inbound,
            crate::message_type_label(msg_type),
            header.len() + body.len(),
        );
    }

    let result = match msg_type {
        MessageType::Message => adapter.message_stream.try_send((header, body)),
        MessageType::Response | MessageType::ShuttingDown => {
            adapter.response_stream.try_send((header, body))
        }
        MessageType::Ack | MessageType::Event => adapter.event_stream.try_send((header, body)),
    };

    if result.is_err() {
        if let Some(metrics) = metrics {
            metrics.record_rejection(TransportRejection::RouteFailed);
        }
    }
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
        }
    }

    /// Override the transport key (default: `"nats"`).
    pub fn with_key(mut self, key: impl Into<TransportKey>) -> Self {
        self.key = key.into();
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
            send_counts: Arc::new(DashMap::new()),
            runtime: OnceLock::new(),
            cancel_token: CancellationToken::new(),
            begin_shutdown_token: CancellationToken::new(),
            shutdown_state: OnceLock::new(),
            max_payload: Arc::new(AtomicUsize::new(usize::MAX)),
            observability: OnceLock::new(),
            metrics: OnceLock::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::make_channels;

    #[test]
    fn test_begin_drain_flips_shutdown_state() {
        // Test the begin_drain logic directly via ShutdownState.
        // Since constructing a full NatsTransport requires a NATS client,
        // we verify the ShutdownState component independently.
        let state = ShutdownState::new();
        assert!(!state.is_draining());
        state.begin_drain();
        assert!(state.is_draining());
    }

    #[test]
    fn test_route_frame_response_routes_during_drain() {
        // Verify that route_frame routes Response frames even when draining.
        // The drain gate is in run_receive_loop, not route_frame — so route_frame
        // should always route regardless. This confirms D-04.
        let (adapter, streams) = make_channels();
        adapter.shutdown_state.begin_drain();
        assert!(adapter.shutdown_state.is_draining());

        // Build a mock NATS message with Velo-Type=1 (Response), Velo-HLen=3
        let mut headers = async_nats::HeaderMap::new();
        headers.insert(HEADER_VELO_TYPE, "1"); // Response
        headers.insert(HEADER_VELO_HLEN, "3");
        let msg = async_nats::Message {
            subject: "test".into(),
            reply: None,
            payload: Bytes::from_static(b"hdrpay"),
            headers: Some(headers),
            status: None,
            description: None,
            length: 0,
        };

        route_frame(&msg, &adapter, "nats", None);

        // Response should be routed to response_stream
        let result = streams.response_stream.try_recv();
        assert!(
            result.is_ok(),
            "Response frame must be routed even during drain"
        );
        let (header, payload) = result.unwrap();
        assert_eq!(&header[..], b"hdr");
        assert_eq!(&payload[..], b"pay");
    }

    #[test]
    fn test_route_frame_event_routes_during_drain() {
        let (adapter, streams) = make_channels();
        adapter.shutdown_state.begin_drain();

        let mut headers = async_nats::HeaderMap::new();
        headers.insert(HEADER_VELO_TYPE, "3"); // Event
        headers.insert(HEADER_VELO_HLEN, "2");
        let msg = async_nats::Message {
            subject: "test".into(),
            reply: None,
            payload: Bytes::from_static(b"evbody"),
            headers: Some(headers),
            status: None,
            description: None,
            length: 0,
        };

        route_frame(&msg, &adapter, "nats", None);

        let result = streams.event_stream.try_recv();
        assert!(
            result.is_ok(),
            "Event frame must be routed even during drain"
        );
    }

    #[test]
    fn test_route_frame_message_routes_when_not_draining() {
        // route_frame always routes — the drain gate is in the loop, not route_frame.
        // This verifies Message frames do reach message_stream when NOT draining.
        let (adapter, streams) = make_channels();
        assert!(!adapter.shutdown_state.is_draining());

        let mut headers = async_nats::HeaderMap::new();
        headers.insert(HEADER_VELO_TYPE, "0"); // Message
        headers.insert(HEADER_VELO_HLEN, "4");
        let msg = async_nats::Message {
            subject: "test".into(),
            reply: None,
            payload: Bytes::from_static(b"hdrrpayload"),
            headers: Some(headers),
            status: None,
            description: None,
            length: 0,
        };

        route_frame(&msg, &adapter, "nats", None);

        let result = streams.message_stream.try_recv();
        assert!(
            result.is_ok(),
            "Message frame must be routed when not draining"
        );
        let (header, payload) = result.unwrap();
        assert_eq!(&header[..], b"hdrr");
        assert_eq!(&payload[..], b"payload");
    }

    #[test]
    fn test_shutting_down_response_type_value() {
        // Verify ShuttingDown is type 4 (used in drain gate Velo-Type header)
        assert_eq!(MessageType::ShuttingDown as u8, 4);
    }
}
