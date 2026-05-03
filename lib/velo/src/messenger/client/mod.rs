// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Active message client.

pub(crate) mod builders;
mod peer_registry;

use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;

use crate::messenger::PeerDiscovery;
use crate::messenger::common::{ActiveMessage, responses::ResponseManager};

use crate::observability::{ClientResolution, VeloMetrics};
use crate::transports::{SendOutcome, TransportErrorHandler, VeloBackend};
use peer_registry::PeerRegistry;
use velo_ext::InstanceId;

const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

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

    #[allow(unused_mut)]
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

        #[cfg(feature = "distributed-tracing")]
        crate::observability::inject_current_context(&mut message.metadata.headers);

        let (header, payload, message_type) = message.encode()?;

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

        // Share a single handshake_timeout budget across both the send-side
        // backpressure wait (if any) and the response receive.
        let result = tokio::time::timeout(self.handshake_timeout, async {
            if let SendOutcome::Backpressured(bp) = send_outcome {
                bp.await;
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
