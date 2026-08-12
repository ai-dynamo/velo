// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Active message client.

pub(crate) mod builders;
mod peer_registry;

#[cfg(test)]
mod tests;

use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::messenger::PeerDiscovery;
use crate::messenger::common::messages::{EncodeError, envelope_overhead};
use crate::messenger::common::{ActiveMessage, responses::ResponseManager};

use crate::observability::{ClientResolution, VeloMetrics};
use crate::transports::{SendOutcome, TransportErrorHandler, VeloBackend};
use peer_registry::PeerRegistry;
use velo_ext::InstanceId;

const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

/// Combine what is known about a target into the largest payload the messenger
/// will carry to it *eagerly* — inline, in one message, without a round trip.
///
/// Two ceilings, either of which may be unknown:
///
/// - `transport_capacity` — [`Transport::max_message_size`] for this target.
///   Exceeding it is a hard failure: the frame never reaches the wire and the
///   send's error handler hears about it.
/// - `staging_threshold` — the installed
///   [`LargePayloadStager`](crate::messenger::large_payload::LargePayloadStager)'s
///   threshold, `None` when no stager is installed. Exceeding it is not a
///   failure; the payload is staged through rendezvous instead, which works
///   but costs the receiver a round trip to fetch. Staying under it is what
///   "eager" means.
///
/// With neither known there is still an answer to give, and it is
/// [`DEFAULT_THRESHOLD`](crate::rendezvous::transparent::DEFAULT_THRESHOLD) —
/// the number the transparent stager would use if it were installed, so
/// installing the default stager later never moves the budget.
///
/// With a capacity but no stager the budget is the transport's, not the
/// default threshold. That is deliberate and worth stating plainly: with no
/// stager there is no cheaper path to fall back to, so clamping to 256 KiB
/// would forbid sends that would have succeeded.
///
/// The envelope comes off the *combined* ceiling. Against the transport that
/// is arithmetic — its limit counts header bytes. Against the staging
/// threshold it is deliberate slack, since the stager compares the raw
/// `payload.len()` before any encoding happens; the budget keeps one shape
/// rather than two, and errs by the size of an envelope in the safe direction.
///
/// `saturating_sub` because neither ceiling bounds the envelope: a handler
/// name plus up to 16 KiB of headers can exceed a small `with_threshold`.
/// Wrapping there would hand back a near-`usize::MAX` budget — precisely the
/// oversized eager send this function exists to prevent.
pub(crate) fn eager_payload_budget(
    transport_capacity: Option<usize>,
    staging_threshold: Option<usize>,
    envelope_overhead: usize,
) -> usize {
    let ceiling = match (transport_capacity, staging_threshold) {
        (Some(capacity), Some(threshold)) => capacity.min(threshold),
        (Some(capacity), None) => capacity,
        (None, Some(threshold)) => threshold,
        (None, None) => crate::rendezvous::transparent::DEFAULT_THRESHOLD,
    };
    ceiling.saturating_sub(envelope_overhead)
}

/// Add the headers the messenger puts on an outbound message itself, turning
/// what a caller supplied into what the encoder will actually write.
///
/// Today that is the distributed-tracing context, and it is not a rounding
/// error: a W3C `traceparent` alone is 69 bytes of MessagePack, and injection
/// materialises a header map even when there is no context to put in it.
///
/// Both the encoder ([`encode_outbound`]) and the budget
/// ([`ActiveMessageClient::effective_eager_payload`]) go through here, which is
/// the whole point — a budget sized against the caller's headers while the
/// encoder writes a larger set is a budget that overruns the transport by the
/// difference.
pub(crate) fn finalize_outbound_headers(headers: &mut Option<HashMap<String, String>>) {
    #[cfg(feature = "distributed-tracing")]
    crate::observability::inject_current_context(headers);
    #[cfg(not(feature = "distributed-tracing"))]
    let _ = headers;
}

/// Turn an outbound message into wire bytes: finalize its headers, then encode.
///
/// The single place a client-side send becomes a frame, so there is no second
/// path where the headers and the budget could drift apart.
pub(crate) fn encode_outbound(
    mut message: ActiveMessage,
) -> Result<(bytes::Bytes, bytes::Bytes, crate::transports::MessageType), EncodeError> {
    finalize_outbound_headers(&mut message.metadata.headers);
    message.encode()
}

pub(crate) struct ActiveMessageClient {
    pub(crate) response_manager: ResponseManager,
    pub(crate) backend: Arc<VeloBackend>,
    error_handler: Arc<dyn TransportErrorHandler>,
    peer_registry: Arc<PeerRegistry>,
    discovery: Option<Arc<dyn PeerDiscovery>>,
    handshake_timeout: Duration,
    observability: Option<Arc<VeloMetrics>>,
    /// Late-bound large payload stager for transparent rendezvous.
    pub(crate) large_payload_stager:
        Arc<std::sync::OnceLock<Arc<dyn crate::messenger::large_payload::LargePayloadStager>>>,
}

impl ActiveMessageClient {
    pub(crate) fn new(
        response_manager: ResponseManager,
        backend: Arc<VeloBackend>,
        error_handler: Arc<dyn TransportErrorHandler>,
        discovery: Option<Arc<dyn PeerDiscovery>>,
        observability: Option<Arc<VeloMetrics>>,
    ) -> Self {
        Self {
            response_manager,
            backend,
            error_handler,
            peer_registry: Arc::new(PeerRegistry::new()),
            discovery,
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            observability,
            large_payload_stager: Arc::new(std::sync::OnceLock::new()),
        }
    }

    pub(crate) fn send_message(
        &self,
        target: InstanceId,
        mut message: ActiveMessage,
    ) -> Result<SendOutcome> {
        // Transparent large payload staging: if payload exceeds threshold,
        // stage it via rendezvous and replace with a handle in the headers.
        if let Some(stager) = self.large_payload_stager.get()
            && message.payload.len() > stager.threshold()
        {
            let staged_payload = std::mem::replace(&mut message.payload, bytes::Bytes::new());
            let handle_str = stager.stage(staged_payload);
            message
                .metadata
                .headers
                .get_or_insert_with(std::collections::HashMap::new)
                .insert(
                    crate::messenger::large_payload::RV_HEADER_KEY.to_string(),
                    handle_str,
                );
        }

        let (header, payload, message_type) = encode_outbound(message)?;

        #[cfg(feature = "distributed-tracing")]
        {
            let span = tracing::info_span!(
                "velo.messenger.client_send",
                target = %target,
                message_type = ?message_type,
                bytes = header.len() + payload.len()
            );
            let _entered = span.enter();
            self.backend.send_message(
                target,
                header,
                payload,
                message_type,
                self.error_handler.clone(),
            )
        }

        #[cfg(not(feature = "distributed-tracing"))]
        self.backend.send_message(
            target,
            header,
            payload,
            message_type,
            self.error_handler.clone(),
        )
    }

    /// Largest payload this client will carry to `target` in one eager send
    /// under `handler_name` with `headers`.
    ///
    /// This is the one place both inputs live: the backend knows which
    /// transport serves `target` and what it will carry, and the client holds
    /// the stager whose threshold decides when a payload stops going inline.
    /// See [`eager_payload_budget`] for what the number means.
    ///
    /// The envelope counts what [`finalize_outbound_headers`] will add to
    /// `headers`, not `headers` as passed: [`send_message`](Self::send_message)
    /// encodes the finalized set, so sizing against anything else hands back a
    /// budget the encoder will overrun.
    ///
    /// It is counted, not built. Finalizing runs against an *empty* scratch
    /// map, which yields exactly what the send path would have merged in, and
    /// [`envelope_overhead`] sizes the union across the two maps without
    /// materialising it. So the caller's headers are only ever read here —
    /// budgeting a send cannot duplicate a header set that is arbitrarily
    /// large, or larger than the encoder would accept, since those limits are
    /// checked at encode and not before.
    ///
    /// Finalizing reads ambient state — the current trace context — so the
    /// answer belongs to the context it was asked from. Callers that size a
    /// batch and then send it do both from the same context, which is what
    /// makes the number hold; a budget carried across into an unrelated
    /// context is no longer a promise about that send.
    pub(crate) fn effective_eager_payload(
        &self,
        target: InstanceId,
        handler_name: &str,
        headers: Option<&HashMap<String, String>>,
    ) -> usize {
        let mut injected = None;
        finalize_outbound_headers(&mut injected);
        eager_payload_budget(
            self.backend.max_message_size(target),
            self.large_payload_stager
                .get()
                .map(|stager| stager.threshold()),
            envelope_overhead(handler_name, headers, injected.as_ref()),
        )
    }

    /// Register a peer in the client peer registry (internal use)
    pub(crate) fn register_peer(&self, instance_id: InstanceId) {
        self.peer_registry.register_peer(instance_id);
    }

    /// Check if a peer is registered in the backend
    pub(crate) fn is_peer_registered(&self, instance_id: InstanceId) -> bool {
        self.backend.is_registered(instance_id)
    }

    /// Check if we have handler information for a peer
    pub(crate) fn has_handler_info(&self, instance_id: InstanceId) -> bool {
        self.peer_registry.has_handler_info(instance_id)
    }

    /// Check if we can send a message directly (fast path)
    pub(crate) fn can_send_directly(&self, target: InstanceId, handler: &str) -> bool {
        // 1. Peer must be registered
        if !self.is_peer_registered(target) {
            return false;
        }

        // 2. System handlers (starting with _) always allowed
        if handler.starts_with('_') {
            return true;
        }

        // 3. Must have handler info and handler must exist
        self.peer_registry.handler_exists(target, handler)
    }

    /// Perform handshake with a peer to exchange handler information
    async fn handshake_with_peer(&self, target: InstanceId) -> Result<()> {
        use crate::messenger::server::system_handlers::{HandlersResponse, HelloRequest};

        tracing::debug!(
            target: "crate::messenger::client",
            target_instance = %target,
            "Initiating handshake with peer"
        );
        if let Some(metrics) = self.observability.as_ref() {
            metrics.record_client_resolution(ClientResolution::HandshakeAttempt);
        }

        // Send _hello with our peer info
        let request = HelloRequest {
            peer_info: self.backend.peer_info(),
        };

        // Serialize request
        let payload = serde_json::to_vec(&request)
            .map_err(|e| anyhow::anyhow!("Failed to serialize _hello request: {}", e))?;

        // Register response and send message
        let mut outcome = self.register_outcome()?;
        let response_id = outcome.response_id();

        let message = crate::messenger::common::ActiveMessage {
            metadata: crate::messenger::common::messages::MessageMetadata::new_unary(
                response_id,
                "_hello".to_string(),
                None,
            ),
            payload: bytes::Bytes::from(payload),
        };

        let send_outcome = self.send_message(target, message)?;

        // Share a single handshake_timeout budget across both the admission
        // wait (if the frame was queued) and the response receive. A failed
        // admission is left to surface through `outcome.recv()`, which the
        // backend's completion hook has already resolved with the error.
        let result = tokio::time::timeout(self.handshake_timeout, async {
            if let SendOutcome::Pending(admission) = send_outcome {
                let _ = admission.await;
            }
            outcome.recv().await
        })
        .await;
        let response_bytes = match result {
            Ok(Ok(Some(bytes))) => bytes,
            Ok(Ok(None)) => {
                anyhow::bail!("Expected response from _hello, got empty acknowledgment");
            }
            Ok(Err(err)) => {
                anyhow::bail!("Handshake failed: {}", err);
            }
            Err(_elapsed) => {
                anyhow::bail!(
                    "Handshake with peer {} timed out after {:?}",
                    target,
                    self.handshake_timeout
                );
            }
        };

        // Deserialize response
        let response: HandlersResponse = serde_json::from_slice(&response_bytes)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize _hello response: {}", e))?;

        // Update peer registry with handler list
        self.peer_registry
            .update_handlers(target, response.handlers.clone());

        tracing::debug!(
            target: "crate::messenger::client",
            target_instance = %target,
            handler_count = response.handlers.len(),
            "Handshake completed successfully"
        );
        if let Some(metrics) = self.observability.as_ref() {
            metrics.record_client_resolution(ClientResolution::HandshakeSuccess);
        }

        Ok(())
    }

    /// Ensure a peer is ready for communication (performs handshake if needed)
    pub(crate) async fn ensure_peer_ready(&self, target: InstanceId, handler: &str) -> Result<()> {
        // 1. Check if peer is registered
        if !self.is_peer_registered(target) {
            anyhow::bail!(
                "Peer {} not registered. Call messenger.register_peer() first.",
                target
            );
        }

        // 2. System handlers skip further checks
        if handler.starts_with('_') {
            return Ok(());
        }

        // 3. Ensure we have handler list (perform handshake if needed)
        if !self.has_handler_info(target) {
            self.handshake_with_peer(target).await?;
        }

        // 4. Verify handler exists. If the peer already had cached handler
        // info but this specific handler is missing, refresh once in case the
        // remote instance registered it after our earlier handshake.
        if !self.peer_registry.handler_exists(target, handler) {
            self.handshake_with_peer(target).await?;
        }

        if !self.peer_registry.handler_exists(target, handler) {
            anyhow::bail!(
                "Handler '{}' not found on instance {}. Available handlers: {:?}",
                handler,
                target,
                self.peer_registry.get_handlers(target).unwrap_or_default()
            );
        }

        Ok(())
    }

    /// Get the list of handlers for a peer (may trigger handshake)
    pub(crate) async fn get_peer_handlers(&self, instance_id: InstanceId) -> Result<Vec<String>> {
        if !self.has_handler_info(instance_id) {
            self.handshake_with_peer(instance_id).await?;
        }

        self.peer_registry
            .get_handlers(instance_id)
            .ok_or_else(|| anyhow::anyhow!("Failed to get handlers for instance {}", instance_id))
    }

    /// Refresh the handler list for a peer
    pub(crate) async fn refresh_handler_list(&self, instance_id: InstanceId) -> Result<()> {
        self.handshake_with_peer(instance_id).await
    }

    /// Resolve a peer via discovery and perform registration
    pub(crate) async fn resolve_peer_via_discovery(
        &self,
        worker_id: velo_ext::WorkerId,
    ) -> Result<InstanceId> {
        tracing::debug!(
            target: "crate::messenger::client",
            worker_id = %worker_id,
            "Resolving peer via discovery"
        );

        let discovery = self.discovery.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "No discovery backend configured. Cannot resolve worker {}",
                worker_id
            )
        })?;

        let peer_info = discovery.discover_by_worker_id(worker_id).await?;
        let instance_id = peer_info.instance_id();
        if let Some(metrics) = self.observability.as_ref() {
            metrics.record_client_resolution(ClientResolution::DiscoverySuccess);
        }

        tracing::debug!(
            target: "crate::messenger::client",
            worker_id = %worker_id,
            instance_id = %instance_id,
            "Discovery resolved peer, performing registration"
        );

        // Register with backend (transports)
        self.backend.register_peer(peer_info)?;

        // Register in peer registry (handler discovery)
        self.peer_registry.register_peer(instance_id);

        Ok(instance_id)
    }

    pub(crate) fn register_outcome(
        &self,
    ) -> Result<
        crate::messenger::common::responses::ResponseAwaiter,
        crate::messenger::common::responses::ResponseRegistrationError,
    > {
        self.response_manager.register_outcome()
    }
}
