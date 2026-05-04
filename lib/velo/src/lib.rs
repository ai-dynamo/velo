// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # Velo
//!
//! Active messaging runtime for Velo distributed systems. Wraps [`Messenger`]
//! with builder sugar for discovery wiring and re-exports the full public API.
//!
//! Out-of-tree implementors of [`Transport`], [`crate::streaming::FrameTransport`],
//! [`PeerDiscovery`], or [`crate::discovery::ServiceDiscovery`] should depend
//! on the smaller [`velo_ext`] crate instead of `velo`. Everything that lives
//! here is the runtime and concrete impls.

use std::sync::Arc;

use anyhow::Result;

// ── Subsystem modules (each was previously a sibling crate) ────────────────
pub mod discovery;
pub mod events;
pub mod messenger;
pub mod observability;
pub mod queue;
pub mod rendezvous;
pub mod streaming;
pub mod transports;

#[cfg(feature = "simulation")]
pub mod simulation;

// ── Convenience re-exports for the most-used public types ──────────────────

// Identity / address types live in velo-ext but are re-exported here so the
// vast majority of consumers depend only on `velo`.
pub use velo_ext::{InstanceId, PeerInfo, Transport, WorkerAddress, WorkerId};

// Public re-exports for the velo-ext crate.
pub use velo_ext as ext;

// Messenger surface
pub use crate::messenger::{
    AmHandlerBuilder, AmSendBuilder, AmSyncBuilder, AsyncExecutor, Context, DispatchMode, Handler,
    HandlerExecutor, Messenger, MessengerBuilder, PeerDiscovery, SyncExecutor, SyncResult,
    TypedContext, TypedUnaryBuilder, TypedUnaryHandlerBuilder, TypedUnaryResult, UnaryBuilder,
    UnaryHandlerBuilder, UnaryResult, UnifiedResponse, VeloEvents,
};

// Events
pub use crate::events::{
    Event, EventAwaiter, EventBackend, EventHandle, EventManager, EventPoison, EventStatus,
};

// Streaming (flat at root for convenience; full surface still under [`streaming`])
pub use crate::streaming::{
    AnchorManager, AttachError, SendError, StreamAnchor, StreamAnchorHandle, StreamController,
    StreamError, StreamFrame, StreamSender,
};

// Rendezvous
pub use crate::rendezvous::{
    DataHandle, DataMetadata, RegisterOptions, RendezvousManager, RendezvousWrite, StageMode,
};

// Observability
pub use crate::observability::VeloMetrics;

/// Configuration for TCP streaming transport.
///
/// Controls the bind address for the TCP streaming listener.
#[derive(Debug, Clone)]
pub struct TcpConfig {
    /// IP address to bind the TCP streaming listener on. Defaults to 0.0.0.0.
    pub bind_addr: std::net::IpAddr,
}

impl Default for TcpConfig {
    fn default() -> Self {
        Self {
            bind_addr: std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
        }
    }
}

impl TcpConfig {
    /// Create a new `TcpConfig` with an explicit bind address.
    pub fn new(bind_addr: std::net::IpAddr) -> Self {
        Self { bind_addr }
    }
}

/// Configuration for gRPC streaming transport.
///
/// Only available when the `grpc` feature is enabled.
#[cfg(feature = "grpc")]
#[derive(Debug, Clone)]
pub struct GrpcConfig {
    /// Socket address to bind the gRPC streaming server. Defaults to 0.0.0.0:0 (OS-assigned port).
    pub bind_addr: std::net::SocketAddr,
}

#[cfg(feature = "grpc")]
impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:0".parse().unwrap(),
        }
    }
}

/// Streaming transport configuration for a [`Velo`] instance.
///
/// Only one `StreamConfig` may be set per [`VeloBuilder`] instance —
/// one streaming server per Velo instance is enforced.
///
/// # Default
///
/// If [`VeloBuilder::stream_config`] is never called and
/// [`VeloBuilder::stream_bind_addr`] was never invoked either, the builder
/// defaults to [`VeloOnly`](StreamConfig::VeloOnly) — streams flow over the
/// messenger transport that's already wired for cross-host reachability. This
/// preserves multi-node correctness without requiring callers to know an
/// externally routable IP.
///
/// To opt into the dedicated TCP transport, either:
/// - call [`VeloBuilder::stream_bind_addr`] with a routable address, or
/// - call [`VeloBuilder::stream_config`] with `Tcp(Some(TcpConfig { bind_addr }))`.
///
/// `Tcp(None)` (the no-bind-addr form) binds on `0.0.0.0:<ephemeral>`. The
/// advertised endpoint is then derived inside `TcpFrameTransport::new()` via
/// `resolve_advertise_ip(bind_addr)`, which maps unspecified addresses
/// (`0.0.0.0` / `::`) to their loopback counterpart. The result is fine for
/// single-host tests but **not reachable across hosts**, which is why TCP
/// isn't the implicit default. Supplying a specific bind IP via
/// `Tcp(Some(TcpConfig { bind_addr }))` or [`VeloBuilder::stream_bind_addr`]
/// passes through `resolve_advertise_ip` unchanged, so the advertised
/// address matches the routable IP you provided.
///
/// # Variants
///
/// - [`StreamConfig::Tcp`]: TCP-based streaming via [`TcpFrameTransport`](crate::streaming::TcpFrameTransport).
///   Pass `None` to bind to `0.0.0.0` (OS-assigned port), or provide a
///   [`TcpConfig`] for an explicit bind address.
///
/// - [`StreamConfig::Grpc`]: gRPC-based streaming via [`GrpcFrameTransport`](crate::streaming::GrpcFrameTransport).
///   Only available when the `grpc` feature is enabled.
///   Pass `None` to bind to `0.0.0.0:0` (OS-assigned port), or provide a
///   [`GrpcConfig`] for an explicit bind address.
///
/// - [`StreamConfig::VeloOnly`]: Skip the dedicated streaming listener entirely
///   and route all streams through `velo-messenger` AMs
///   ([`VeloFrameTransport`](crate::streaming::VeloFrameTransport)). Use this when
///   the host cannot or should not open additional TCP ports (sandboxed
///   environments, restricted-network deployments, single-process tests).
#[derive(Debug, Clone)]
pub enum StreamConfig {
    /// TCP-based streaming transport (TcpFrameTransport).
    ///
    /// Pass `None` to use the default bind address (`0.0.0.0`),
    /// or `Some(TcpConfig)` for an explicit bind address.
    Tcp(Option<TcpConfig>),
    /// gRPC-based streaming transport (GrpcFrameTransport).
    ///
    /// Only available with the `grpc` feature.
    /// Pass `None` to bind to `0.0.0.0:0` (OS-assigned port),
    /// or `Some(GrpcConfig)` for an explicit bind address.
    #[cfg(feature = "grpc")]
    Grpc(Option<GrpcConfig>),
    /// Opt out of the dedicated streaming listener: all streams flow through
    /// the `velo` (messenger-routed) transport. No TCP listener is opened.
    VeloOnly,
}

/// High-level facade for the Velo distributed system.
///
/// Wraps a [`Messenger`], [`AnchorManager`], and [`RendezvousManager`]
/// and provides the same public API with a simpler name.
#[derive(Clone)]
pub struct Velo {
    messenger: Arc<Messenger>,
    anchor_manager: Arc<crate::streaming::AnchorManager>,
    rendezvous_manager: Arc<crate::rendezvous::RendezvousManager>,
}

/// Builder for configuring and creating a [`Velo`] instance.
pub struct VeloBuilder {
    inner: MessengerBuilder,
    stream_config: Option<StreamConfig>,
    metrics: Option<Arc<VeloMetrics>>,
}

impl VeloBuilder {
    /// Create a new empty builder.
    pub fn new() -> Self {
        Self {
            inner: MessengerBuilder::new(),
            stream_config: None,
            metrics: None,
        }
    }

    /// Add a transport to the system.
    pub fn add_transport(mut self, transport: Arc<dyn Transport>) -> Self {
        self.inner = self.inner.add_transport(transport);
        self
    }

    /// Set the streaming transport configuration.
    ///
    /// Only one transport server is allowed per Velo instance. Returns [`Err`]
    /// if called more than once on the same builder.
    ///
    /// Use [`StreamConfig::Tcp(None)`](StreamConfig::Tcp) for TCP with default
    /// bind address (`0.0.0.0`), or `StreamConfig::Tcp(Some(TcpConfig::new(addr)))`
    /// for an explicit bind address.
    ///
    /// Use [`StreamConfig::Grpc(None)`](StreamConfig::Grpc) for gRPC streaming
    /// (requires the `grpc` feature), or `StreamConfig::Grpc(Some(GrpcConfig { bind_addr }))`
    /// for an explicit bind address.
    pub fn stream_config(mut self, config: StreamConfig) -> Result<Self> {
        if self.stream_config.is_some() {
            return Err(anyhow::anyhow!(
                "stream_config called more than once: only one streaming server allowed per Velo instance"
            ));
        }
        self.stream_config = Some(config);
        Ok(self)
    }

    /// Opt into the dedicated TCP streaming transport bound to a specific
    /// interface.
    ///
    /// Convenience wrapper around [`stream_config`](VeloBuilder::stream_config)
    /// with `StreamConfig::Tcp(Some(TcpConfig::new(addr)))`. The address you
    /// pass is used both for binding and as the advertised endpoint, so it
    /// must be reachable from the peers that will connect.
    ///
    /// Without this call (and without [`stream_config`](VeloBuilder::stream_config)),
    /// the builder defaults to [`StreamConfig::VeloOnly`] — streams flow over
    /// the messenger transport. TCP is not chosen implicitly because the
    /// no-bind-address form (`0.0.0.0`) would advertise loopback, breaking
    /// multi-node reachability.
    pub fn stream_bind_addr(self, addr: std::net::IpAddr) -> Self {
        // Convenience: wraps StreamConfig::Tcp with explicit bind address.
        // stream_config() cannot fail here (only one call from this path).
        self.stream_config(StreamConfig::Tcp(Some(TcpConfig::new(addr))))
            .unwrap()
    }

    /// Set the peer discovery backend.
    pub fn discovery(mut self, discovery: Arc<dyn PeerDiscovery>) -> Self {
        self.inner = self.inner.discovery(discovery);
        self
    }

    /// Install Prometheus collectors for this Velo instance.
    pub fn metrics(mut self, metrics: Arc<VeloMetrics>) -> Self {
        self.inner = self.inner.metrics(metrics.clone());
        self.metrics = Some(metrics);
        self
    }

    /// Build the Velo system with the configured transports and discovery.
    ///
    /// Construction order:
    /// 1. Build Messenger (async)
    /// 2. Extract WorkerId
    /// 3. Create VeloFrameTransport (always — used as fallback / opt-out)
    /// 4. Resolve the default streaming transport from `stream_config`. If
    ///    unset, defaults to [`StreamConfig::VeloOnly`] (streams over the
    ///    messenger transport — multi-node reachable by default).
    /// 5. Create AnchorManager via builder
    /// 6. Register streaming control-plane handlers on Messenger
    /// 7. Assemble Velo struct
    pub async fn build(self) -> Result<Arc<Velo>> {
        // Step 1: Build Messenger
        let messenger = self.inner.build().await?;

        // Step 2: Extract worker_id
        let worker_id = messenger.instance_id().worker_id();

        // Step 3: Create VeloFrameTransport. Always constructed; serves as the
        // VeloOnly default and as the `velo://` fallback scheme alongside TCP.
        let velo_transport = Arc::new(crate::streaming::VeloFrameTransport::new(
            Arc::clone(&messenger),
            worker_id,
            self.metrics.clone(),
        )?);

        // Step 4: Resolve transport and registry from stream_config.
        // Implicit default (None) is VeloOnly — TCP without an explicit
        // bind address advertises loopback, which would silently break
        // multi-node streaming. Callers opt into TCP via stream_bind_addr()
        // or stream_config(StreamConfig::Tcp(Some(..))).
        let resolved = self.stream_config.unwrap_or(StreamConfig::VeloOnly);
        let (default_transport, transport_registry) = match resolved {
            StreamConfig::Tcp(tcp_cfg) => {
                let bind_addr = tcp_cfg
                    .map(|c| c.bind_addr)
                    .unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));
                let tcp_transport = crate::streaming::TcpFrameTransport::new(bind_addr).await?;
                let mut registry = std::collections::HashMap::new();
                registry.insert(
                    "tcp".to_string(),
                    Arc::clone(&tcp_transport) as Arc<dyn crate::streaming::FrameTransport>,
                );
                registry.insert(
                    "velo".to_string(),
                    velo_transport.clone() as Arc<dyn crate::streaming::FrameTransport>,
                );
                (
                    tcp_transport as Arc<dyn crate::streaming::FrameTransport>,
                    Arc::new(registry),
                )
            }
            #[cfg(feature = "grpc")]
            StreamConfig::Grpc(grpc_cfg) => {
                let bind_addr = grpc_cfg
                    .map(|c| c.bind_addr)
                    .unwrap_or_else(|| "0.0.0.0:0".parse().unwrap());
                let grpc_transport = Arc::new(
                    crate::streaming::GrpcFrameTransport::new(bind_addr)
                        .await
                        .map_err(|e| anyhow::anyhow!("Failed to start gRPC transport: {}", e))?,
                );
                let mut registry = std::collections::HashMap::new();
                registry.insert(
                    "grpc".to_string(),
                    Arc::clone(&grpc_transport) as Arc<dyn crate::streaming::FrameTransport>,
                );
                registry.insert(
                    "velo".to_string(),
                    velo_transport.clone() as Arc<dyn crate::streaming::FrameTransport>,
                );
                (
                    grpc_transport as Arc<dyn crate::streaming::FrameTransport>,
                    Arc::new(registry),
                )
            }
            StreamConfig::VeloOnly => (
                velo_transport as Arc<dyn crate::streaming::FrameTransport>,
                Arc::new(std::collections::HashMap::new()),
            ),
        };

        // Step 5: Create AnchorManager (pass messenger for cross-worker cancel AMs)
        let anchor_manager = Arc::new(
            crate::streaming::AnchorManagerBuilder::default()
                .worker_id(worker_id)
                .transport(default_transport)
                .transport_registry(transport_registry)
                .messenger(Some(Arc::clone(&messenger)))
                .metrics(self.metrics.clone())
                .build()
                .map_err(|e| anyhow::anyhow!("{}", e))?,
        );

        // Step 6: Register streaming control-plane handlers
        anchor_manager.register_handlers(Arc::clone(&messenger))?;

        // Step 7: Create RendezvousManager and register handlers
        let rendezvous_manager = Arc::new(match self.metrics.as_ref() {
            Some(m) => crate::rendezvous::RendezvousManager::with_metrics(worker_id, Arc::clone(m)),
            None => crate::rendezvous::RendezvousManager::new(worker_id),
        });
        rendezvous_manager.register_handlers(Arc::clone(&messenger))?;

        // Step 8: Enable transparent large payload support
        let stager = Arc::new(crate::rendezvous::RendezvousStager::new(Arc::clone(
            &rendezvous_manager,
        )));
        let resolver = Arc::new(crate::rendezvous::RendezvousResolver::new(Arc::clone(
            &rendezvous_manager,
        )));
        messenger.set_large_payload_support(stager, resolver);

        // Step 9: Assemble Velo
        Ok(Arc::new(Velo {
            messenger,
            anchor_manager,
            rendezvous_manager,
        }))
    }
}

impl Default for VeloBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl Velo {
    /// Create a builder for configuring Velo.
    pub fn builder() -> VeloBuilder {
        VeloBuilder::new()
    }

    /// Get the underlying messenger.
    pub fn messenger(&self) -> &Arc<Messenger> {
        &self.messenger
    }

    /// Get the instance ID of this system.
    pub fn instance_id(&self) -> InstanceId {
        self.messenger.instance_id()
    }

    /// Get the peer information for this instance.
    pub fn peer_info(&self) -> PeerInfo {
        self.messenger.peer_info()
    }

    /// Get the distributed event system.
    pub fn events(&self) -> &Arc<VeloEvents> {
        self.messenger.events()
    }

    /// Create an EventManager wired with the distributed backend.
    pub fn event_manager(&self) -> EventManager {
        self.messenger.event_manager()
    }

    /// Fire-and-forget builder (no response expected).
    pub fn am_send(&self, handler: &str) -> Result<AmSendBuilder> {
        self.messenger.am_send(handler)
    }

    /// Active-message synchronous completion (await handler finish).
    pub fn am_sync(&self, handler: &str) -> Result<AmSyncBuilder> {
        self.messenger.am_sync(handler)
    }

    /// Unary builder returning raw bytes.
    pub fn unary(&self, handler: &str) -> Result<UnaryBuilder> {
        self.messenger.unary(handler)
    }

    /// Typed unary builder returning deserialized response.
    pub fn typed_unary<R: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        handler: &str,
    ) -> Result<TypedUnaryBuilder<R>> {
        self.messenger.typed_unary(handler)
    }

    /// Register a handler on this instance.
    pub fn register_handler(&self, handler: Handler) -> Result<()> {
        self.messenger.register_handler(handler)
    }

    /// Connect to a peer by registering their peer information.
    pub fn register_peer(&self, peer_info: PeerInfo) -> Result<()> {
        self.messenger.register_peer(peer_info)
    }

    /// Discover a peer by instance_id and register it for communication.
    pub async fn discover_and_register_peer(&self, instance_id: InstanceId) -> Result<()> {
        self.messenger.discover_and_register_peer(instance_id).await
    }

    /// Check whether a specific instance has subscribed to a locally-owned event.
    pub fn has_event_subscriber(&self, handle: EventHandle, subscriber: InstanceId) -> bool {
        self.messenger.has_event_subscriber(handle, subscriber)
    }

    /// Get the list of handlers available on a remote instance.
    pub async fn available_handlers(&self, instance_id: InstanceId) -> Result<Vec<String>> {
        self.messenger.available_handlers(instance_id).await
    }

    /// Refresh the handler list for a remote instance.
    pub async fn refresh_handlers(&self, instance_id: InstanceId) -> Result<()> {
        self.messenger.refresh_handlers(instance_id).await
    }

    /// Wait for a specific handler to become available on a remote instance.
    pub async fn wait_for_handler(
        &self,
        instance_id: InstanceId,
        handler_name: &str,
    ) -> Result<()> {
        self.messenger
            .wait_for_handler(instance_id, handler_name)
            .await
    }

    /// Get the list of handlers registered on this local instance.
    pub fn list_local_handlers(&self) -> Vec<String> {
        self.messenger.list_local_handlers()
    }

    /// Get the tokio runtime handle.
    pub fn runtime(&self) -> &tokio::runtime::Handle {
        self.messenger.runtime()
    }

    /// Get the task tracker.
    pub fn tracker(&self) -> &tokio_util::task::TaskTracker {
        self.messenger.tracker()
    }

    /// Create a new streaming anchor.
    ///
    /// Returns a [`StreamAnchor<T>`] that embeds the [`StreamAnchorHandle`];
    /// obtain it via [`.handle()`](StreamAnchor::handle) to pass to a sender
    /// (possibly on another worker) for attachment.
    pub fn create_anchor<T>(&self) -> StreamAnchor<T> {
        self.anchor_manager.create_anchor::<T>()
    }

    /// Attach a sender to an existing anchor (local or remote).
    ///
    /// Delegates to [`AnchorManager::attach_stream_anchor`](crate::streaming::AnchorManager::attach_stream_anchor).
    /// For fine-grained control, use [`anchor_manager()`](Velo::anchor_manager) directly.
    pub async fn attach_anchor<T: serde::Serialize>(
        &self,
        handle: StreamAnchorHandle,
    ) -> Result<StreamSender<T>, AttachError> {
        self.anchor_manager.attach_stream_anchor::<T>(handle).await
    }

    /// Get the underlying anchor manager for direct registry access.
    pub fn anchor_manager(&self) -> &crate::streaming::AnchorManager {
        &self.anchor_manager
    }

    // -----------------------------------------------------------------------
    // MPSC anchor API
    // -----------------------------------------------------------------------

    /// Create a new MPSC streaming anchor with manager defaults.
    ///
    /// Returns an [`streaming::mpsc::MpscStreamAnchor`] that accepts frames
    /// from many senders (each tagged with a unique
    /// [`streaming::mpsc::SenderId`]) and surfaces them to a single consumer.
    /// Sender lifecycle events (`Detached`, `Dropped`) are non-terminal — the
    /// stream only ends when the consumer cancels it or the anchor is dropped.
    pub fn create_mpsc_anchor<T>(&self) -> streaming::mpsc::MpscStreamAnchor<T> {
        self.anchor_manager.create_mpsc_anchor::<T>()
    }

    /// Create a new MPSC streaming anchor with per-anchor config
    /// (`max_senders`, `unattached_timeout`, `heartbeat_interval`,
    /// `channel_capacity`).
    pub fn create_mpsc_anchor_with_config<T>(
        &self,
        config: streaming::mpsc::MpscAnchorConfig,
    ) -> streaming::mpsc::MpscStreamAnchor<T> {
        self.anchor_manager
            .create_mpsc_anchor_with_config::<T>(config)
    }

    /// Attach a sender to an MPSC anchor. Handles both local (same-worker)
    /// and cross-worker targets automatically.
    pub async fn attach_mpsc_anchor<T: serde::Serialize>(
        &self,
        handle: StreamAnchorHandle,
    ) -> Result<streaming::mpsc::MpscStreamSender<T>, AttachError> {
        self.anchor_manager
            .attach_mpsc_stream_anchor::<T>(handle)
            .await
    }

    // -----------------------------------------------------------------------
    // Rendezvous API
    // -----------------------------------------------------------------------

    /// Stage data at this worker and return a [`DataHandle`].
    ///
    /// The handle encodes this worker's ID and a local slot ID. Pass it to
    /// consumers via any channel (AM, event, typed message field).
    /// Default refcount is 1.
    pub fn register_data(&self, data: bytes::Bytes) -> DataHandle {
        self.rendezvous_manager.register_data(data)
    }

    /// Stage data with options (TTL, etc.) and return a [`DataHandle`].
    pub fn register_data_with(&self, data: bytes::Bytes, opts: RegisterOptions) -> DataHandle {
        self.rendezvous_manager.register_data_with(data, opts)
    }

    /// Query metadata about the data behind a handle (no lock acquired).
    pub async fn metadata(&self, handle: DataHandle) -> Result<DataMetadata> {
        self.rendezvous_manager.metadata(handle).await
    }

    /// Pull data from a handle. Acquires a read lock on the owner side.
    ///
    /// Returns `(data, lease_id)`. The `lease_id` must be passed to
    /// [`detach()`](Self::detach) or [`release()`](Self::release) when done.
    pub async fn get(&self, handle: DataHandle) -> Result<(bytes::Bytes, u64)> {
        self.rendezvous_manager.get(handle).await
    }

    /// Pull data from a handle into an explicit destination buffer.
    ///
    /// Returns `lease_id`.
    pub async fn get_into(
        &self,
        handle: DataHandle,
        dest: &mut impl RendezvousWrite,
    ) -> Result<u64> {
        self.rendezvous_manager.get_into(handle, dest).await
    }

    /// Increment the refcount on a handle (for additional consumers).
    pub async fn ref_handle(&self, handle: DataHandle) -> Result<()> {
        self.rendezvous_manager.ref_handle(handle).await
    }

    /// Release the read lock WITHOUT decrementing refcount.
    /// The handle remains alive and can be `get()`-ed again.
    pub async fn detach(&self, handle: DataHandle, lease_id: u64) -> Result<()> {
        self.rendezvous_manager.detach(handle, lease_id).await
    }

    /// Release the read lock AND decrement refcount.
    /// Data is freed when both refcount and read_lock_count reach zero.
    pub async fn release(&self, handle: DataHandle, lease_id: u64) -> Result<()> {
        self.rendezvous_manager.release(handle, lease_id).await
    }

    /// Get the underlying rendezvous manager for direct access.
    pub fn rendezvous_manager(&self) -> &crate::rendezvous::RendezvousManager {
        &self.rendezvous_manager
    }

    /// Enable NIXL/RDMA on the underlying rendezvous manager.
    ///
    /// Required on both the owner (before [`register_data_pinned`](Self::register_data_pinned))
    /// and the consumer (before pulling from a pinned handle). See
    /// `velo-rendezvous`'s `nixl` feature for environment requirements.
    #[cfg(feature = "nixl")]
    pub fn enable_nixl(&self) -> Result<()> {
        self.rendezvous_manager.enable_nixl()
    }

    /// Stage RDMA-pinned data and return a [`DataHandle`].
    ///
    /// Requires [`enable_nixl`](Self::enable_nixl).
    #[cfg(feature = "nixl")]
    pub fn register_data_pinned(&self, data: bytes::Bytes) -> Result<DataHandle> {
        self.rendezvous_manager.register_data_pinned(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test: stream_config double-call returns Err (GRPC-07)
    ///
    /// VeloBuilder enforces one streaming server per instance.
    /// A second call to stream_config() must return Err, not panic.
    #[test]
    fn test_stream_config_double_call_error() {
        let builder = Velo::builder();
        let builder = builder
            .stream_config(StreamConfig::Tcp(None))
            .expect("first stream_config should succeed");
        let result = builder.stream_config(StreamConfig::Tcp(None));
        assert!(
            result.is_err(),
            "second stream_config call should return Err"
        );
        // Extract error without unwrap_err() to avoid T: Debug bound on VeloBuilder
        let err = result.err().unwrap();
        assert!(
            err.to_string().contains("more than once") || err.to_string().contains("one streaming"),
            "error message should indicate double-call: {}",
            err
        );
    }

    /// Test 1: Velo struct has anchor_manager field of type Arc<AnchorManager>
    /// (compile-time check via field accessor)
    #[test]
    fn velo_has_anchor_manager_accessor() {
        // This test verifies the anchor_manager() method exists and returns &AnchorManager.
        // It doesn't construct a Velo (that requires async + transport), so we verify
        // the method signature exists by type-checking a function pointer.
        let _: fn(&Velo) -> &crate::streaming::AnchorManager = Velo::anchor_manager;
    }

    /// Test 2: create_anchor method exists with correct generic signature
    #[test]
    fn velo_create_anchor_signature() {
        // Verify the method exists and has the correct type.
        // We can't call it without a Velo instance, but we can verify the signature.
        let _: fn(&Velo) -> crate::streaming::StreamAnchor<String> = Velo::create_anchor::<String>;
    }

    /// Test 3: attach_anchor method exists with correct async generic signature
    /// (verified via integration test that constructs a real Velo)
    #[tokio::test]
    async fn velo_attach_anchor_type_checks() {
        // Build a real Velo instance to exercise create_anchor + attach_anchor type-checking.
        let transport = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            Arc::new(
                crate::transports::tcp::TcpTransportBuilder::new()
                    .from_listener(listener)
                    .unwrap()
                    .build()
                    .unwrap(),
            )
        };
        let velo = Velo::builder()
            .add_transport(transport)
            .build()
            .await
            .unwrap();

        // Test 1: anchor_manager() returns &AnchorManager
        let _am: &crate::streaming::AnchorManager = velo.anchor_manager();

        // Test 2: create_anchor::<String>() returns StreamAnchor<String>
        let anchor: crate::streaming::StreamAnchor<String> = velo.create_anchor::<String>();
        let handle = anchor.handle();

        // Test 3: attach_anchor::<String>(handle) returns correct Result type
        // The local attach path no longer calls transport.connect(), so it
        // should succeed for local handles.
        let result: Result<crate::streaming::StreamSender<String>, crate::streaming::AttachError> =
            velo.attach_anchor::<String>(handle).await;

        let _sender = result.expect("local attach should succeed");
    }
}
