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

#[cfg(test)]
pub(crate) mod test_alloc;

// ── Convenience re-exports for the most-used public types ──────────────────

// Identity / address types live in velo-ext but are re-exported here so the
// vast majority of consumers depend only on `velo`.
pub use velo_ext::{
    AdmissionState, InstanceId, PeerInfo, ShutdownPolicy, Transport, WorkerAddress, WorkerId,
};

// Public re-exports for the velo-ext crate.
pub use velo_ext as ext;

// Messenger surface
pub use crate::messenger::{
    Admitted, AmHandlerBuilder, AmSendBuilder, AmSyncBuilder, AsyncExecutor, Context, FireResult,
    Handler, HandlerExecutor, Messenger, MessengerBuilder, OrderedConfig, OrderingKey,
    OverflowPolicy, PeerDiscovery, SyncExecutor, SyncResult, TypedContext, TypedUnaryBuilder,
    TypedUnaryHandlerBuilder, TypedUnaryResult, UnaryBuilder, UnaryHandlerBuilder, UnaryResult,
    UnifiedResponse, VeloEvents,
};

// Events
pub use crate::events::{
    Event, EventAwaiter, EventBackend, EventHandle, EventManager, EventPoison, EventStatus,
};

// Streaming (flat at root for convenience; full surface still under [`streaming`])
pub use crate::streaming::control::StreamOpenTicket;
pub use crate::streaming::{
    AnchorManager, AttachError, SendError, StreamAnchor, StreamAnchorHandle, StreamController,
    StreamError, StreamFrame, StreamSender,
};

// Rendezvous
pub use crate::rendezvous::{
    DataHandle, DataMetadata, RegisterOptions, RendezvousManager, RendezvousWrite, StageMode,
};

// RDMA registration. Gated exactly as `transports::ucx` is: these types are the
// public face of a subsystem that only exists when a UCX transport can back it.
#[cfg(all(target_os = "linux", feature = "ucx"))]
pub use crate::rendezvous::rdma::{
    Deregistered, PinnedBuf, RdmaConfig, RdmaError, RdmaPoolConfig, RdmaRendezvousConfig,
    RegionGuard, RegionWatch, RegisterOwnedError,
};

#[cfg(all(target_os = "linux", feature = "ucx"))]
pub use crate::rendezvous::write::PinnedWriter;
/// The registered `get_into` destination, and the capability that describes it.
///
/// `RdmaDestination` is unconditional so the [`RendezvousWrite`] trait has one
/// shape in every build; only velo can construct one, so a build without the
/// RDMA path simply never does.
pub use crate::rendezvous::write::RdmaDestination;

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
/// If neither [`VeloBuilder::stream_config`] nor [`VeloBuilder::stream_bind_addr`]
/// is called, the builder defaults to [`StreamConfig::Tcp(None)`](StreamConfig::Tcp)
/// — bind `0.0.0.0:<ephemeral>` and advertise every UP non-loopback interface
/// via [`Vec<InterfaceEndpoint>`](crate::transports::utils::interfaces::InterfaceEndpoint)
/// in the local [`WorkerAddress`]. The peer-side `register()` walks the
/// advertised list and calls `select_best_endpoint` (NUMA + subnet match)
/// against its own interfaces to choose a routable address — multi-node
/// correctness comes from interface advertisement, not from defaulting away
/// from TCP.
///
/// # Variants
///
/// - [`StreamConfig::Tcp`]: TCP-based streaming via
///   [`TcpFrameTransport`](crate::streaming::TcpFrameTransport). Pass `None`
///   to bind on `0.0.0.0:0`, or provide a [`TcpConfig`] for an explicit
///   single-interface bind.
///
/// - [`StreamConfig::Grpc`]: gRPC-based streaming via
///   [`GrpcFrameTransport`](crate::streaming::GrpcFrameTransport). Only
///   available when the `grpc` feature is enabled. Same advertise-and-select
///   semantics as `Tcp`.
#[derive(Debug, Clone)]
pub enum StreamConfig {
    /// TCP-based streaming transport (TcpFrameTransport).
    Tcp(Option<TcpConfig>),
    /// gRPC-based streaming transport (GrpcFrameTransport).
    #[cfg(feature = "grpc")]
    Grpc(Option<GrpcConfig>),
}

/// High-level facade for the Velo distributed system.
///
/// Wraps a [`Messenger`], [`AnchorManager`], and [`RendezvousManager`]
/// and provides the same public API with a simpler name.
///
/// Clones share ownership of streaming services. Final drop cancels them;
/// a retained [`Arc<Messenger>`] keeps only active messaging available.
/// Use [`Self::shutdown`] to drain work and join owned tasks before dropping.
#[derive(Clone)]
pub struct Velo {
    messenger: Arc<Messenger>,
    anchor_manager: Arc<crate::streaming::AnchorManager>,
    rendezvous_manager: Arc<crate::rendezvous::RendezvousManager>,
    /// The single streaming transport bound for this instance. Held here so
    /// `register_peer` can fan out to it (the messenger does not know about
    /// `FrameTransport`s) and so `peer_info()` can merge the streaming
    /// listener's WorkerAddress entry into the messenger-side WorkerAddress.
    stream_transport: Arc<dyn crate::streaming::FrameTransport>,
    stream_owner: Arc<StreamOwner>,
    /// RDMA registration layer, present only when a UCX transport was added
    /// through [`VeloBuilder::add_ucx_transport`].
    #[cfg(all(target_os = "linux", feature = "ucx"))]
    rdma: Option<Arc<crate::rendezvous::rdma::RdmaRegistry>>,
    /// Serialises [`graceful_shutdown`](Velo::graceful_shutdown).
    ///
    /// [`Velo`] is `Clone`, so two clones can call it at once. Without this the
    /// second caller finds the transport's join handle already taken, skips the
    /// join, and races ahead to declare registrations released — while the
    /// first caller is still inside that join and the progress thread is still
    /// running. It would free arena pages the NIC may still have. Shared, so
    /// every clone contends on the same lock.
    shutdown: Arc<ShutdownOnce>,
}

/// Makes [`Velo::graceful_shutdown`] run exactly once, and makes concurrent
/// callers wait for the run rather than start their own.
struct ShutdownOnce {
    lock: tokio::sync::Mutex<()>,
    done: std::sync::atomic::AtomicBool,
}

/// Concrete handles let shutdown join listeners without changing `FrameTransport`.
enum OwnedStreamTransport {
    Tcp(Arc<crate::streaming::TcpFrameTransport>),
    #[cfg(feature = "grpc")]
    Grpc(Arc<crate::streaming::GrpcFrameTransport>),
}

impl OwnedStreamTransport {
    fn stop(&self) {
        match self {
            Self::Tcp(transport) => transport.stop(),
            #[cfg(feature = "grpc")]
            Self::Grpc(transport) => transport.stop(),
        }
    }

    async fn shutdown(&self) {
        match self {
            Self::Tcp(transport) => transport.shutdown().await,
            #[cfg(feature = "grpc")]
            Self::Grpc(transport) => transport.shutdown().await,
        }
    }
}

/// Shared by Velo clones, but not by Messenger or individual stream handles.
///
/// Drop detaches streams before the Messenger can tear its transports down,
/// because `AnchorManager` holds its Messenger strongly. A weak back-link
/// there would let the Velo's own Messenger reference go first and tear
/// down transports under live streams.
struct StreamOwner {
    manager: Arc<crate::streaming::AnchorManager>,
    transport: OwnedStreamTransport,
}

impl Drop for StreamOwner {
    fn drop(&mut self) {
        self.manager.stop();
        self.transport.stop();
    }
}

/// Builder for configuring and creating a [`Velo`] instance.
pub struct VeloBuilder {
    inner: MessengerBuilder,
    stream_config: Option<StreamConfig>,
    mux_config: Option<crate::streaming::MuxConfig>,
    metrics: Option<Arc<VeloMetrics>>,
    /// The concrete UCX transport, kept beside the type-erased one so the
    /// registration layer can reach its RMA endpoint. `Arc<dyn Transport>`
    /// cannot be downcast, and adding an RDMA accessor to the `velo-ext` trait
    /// would be a coordinated breaking change for every external implementor
    /// (D10) — so the builder simply remembers what it was handed.
    #[cfg(all(target_os = "linux", feature = "ucx"))]
    ucx_transport: Option<Arc<crate::transports::ucx::UcxTransport>>,
    #[cfg(all(target_os = "linux", feature = "ucx"))]
    rdma_config: Option<crate::rendezvous::rdma::RdmaConfig>,
}

impl VeloBuilder {
    /// Create a new empty builder.
    pub fn new() -> Self {
        Self {
            inner: MessengerBuilder::new(),
            stream_config: None,
            mux_config: None,
            metrics: None,
            #[cfg(all(target_os = "linux", feature = "ucx"))]
            ucx_transport: None,
            #[cfg(all(target_os = "linux", feature = "ucx"))]
            rdma_config: None,
        }
    }

    /// Add a transport to the system.
    pub fn add_transport(mut self, transport: Arc<dyn Transport>) -> Self {
        self.inner = self.inner.add_transport(transport);
        self
    }

    /// Add the UCX transport, and with it the RDMA registration layer.
    ///
    /// Registers the transport exactly as [`add_transport`](Self::add_transport)
    /// would, and additionally keeps the concrete handle so
    /// [`build`](Self::build) can construct an
    /// [`RdmaRegistry`](crate::rendezvous::rdma::RdmaRegistry) over its RMA
    /// endpoint. Adding the same transport through `add_transport` instead
    /// leaves messaging fully working and the RDMA registration APIs
    /// unavailable, which is a legitimate configuration.
    ///
    /// Only the last call counts: one registry per instance, over one backend.
    #[cfg(all(target_os = "linux", feature = "ucx"))]
    pub fn add_ucx_transport(
        mut self,
        transport: Arc<crate::transports::ucx::UcxTransport>,
    ) -> Self {
        self.ucx_transport = Some(Arc::clone(&transport));
        self.add_transport(transport)
    }

    /// Tune the RDMA registration layer: arena sizing, the registered-bytes
    /// budget, and the shutdown budgets. Ignored without a UCX transport.
    #[cfg(all(target_os = "linux", feature = "ucx"))]
    pub fn rdma_config(mut self, config: crate::rendezvous::rdma::RdmaConfig) -> Self {
        self.rdma_config = Some(config);
        self
    }

    /// Set the streaming transport configuration.
    ///
    /// Only one transport server is allowed per Velo instance. Returns [`Err`]
    /// if called more than once on the same builder.
    pub fn stream_config(mut self, config: StreamConfig) -> Result<Self> {
        if self.stream_config.is_some() {
            return Err(anyhow::anyhow!(
                "stream_config called more than once: only one streaming server allowed per Velo instance"
            ));
        }
        self.stream_config = Some(config);
        Ok(self)
    }

    /// Convenience: pin the TCP streaming listener to a single interface IP
    /// (instead of the default `0.0.0.0` + multi-interface advertise).
    pub fn stream_bind_addr(self, addr: std::net::IpAddr) -> Self {
        self.stream_config(StreamConfig::Tcp(Some(TcpConfig::new(addr))))
            .unwrap()
    }

    /// Configure the batched, multiplexed streaming transport
    /// (`messenger-mux-v2`), described in `docs/src/concepts/batched-streaming.md`.
    ///
    /// **The mux is on by default.** A builder that never calls this installs
    /// `MuxConfig::default()`, whose
    /// [`enabled`](crate::streaming::MuxConfig::enabled) is `true`. Call this
    /// to tune it, or with `enabled: false` to turn it off, in which case
    /// nothing is registered and nothing is advertised.
    ///
    /// The per-stream transport stays configured either way — a mux-enabled
    /// node registers both, and each attach picks between them from what the
    /// peer advertised, so a peer without the mux is still served. **Rollback
    /// is the same flag**: set it to `false` and the node stops advertising
    /// `messenger-mux-v2`, so the next attach negotiates the per-stream path.
    /// No code change, no wire change, and no coordination with peers, because
    /// a key that is never advertised is never selected. An application that
    /// never calls this can still turn the mux off: set
    /// `VELO_MESSENGER_MUX_DISABLE=1` and restart. The variable is read once,
    /// in [`build`](Self::build), and it wins over `enabled: true` set in
    /// code, as an operator's switch must.
    ///
    /// Only one mux may be installed per instance: its `_stream_batch` handler
    /// is registered on the messenger for its lifetime. The messenger would
    /// replace a second registration without an error, so this check is the
    /// guard. Calling this twice fails here.
    pub fn messenger_mux(mut self, config: crate::streaming::MuxConfig) -> Result<Self> {
        if self.mux_config.is_some() {
            return Err(anyhow::anyhow!(
                "messenger_mux called more than once: only one messenger mux is allowed per Velo instance"
            ));
        }
        self.mux_config = Some(config);
        Ok(self)
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
    /// 3. Resolve the streaming transport from `stream_config` (default: TCP
    ///    on `0.0.0.0:0` with multi-interface advertise via WorkerAddress).
    /// 4. Merge the streaming transport's `address()` into the local
    ///    PeerInfo's WorkerAddress (so peers can discover the streaming
    ///    listener alongside messenger endpoints).
    /// 5. Create AnchorManager via builder, with the streaming transport
    ///    wired in as the default and registered under its TransportKey,
    ///    beside the mux unless the mux is switched off.
    /// 6. Register streaming control-plane handlers on Messenger.
    /// 7. Assemble Velo struct, holding a clone of the streaming transport
    ///    so `register_peer` can fan out to it on every newly-known peer.
    pub async fn build(self) -> Result<Arc<Velo>> {
        // Step 1: Build Messenger.
        let messenger = self.inner.build().await?;

        // Step 2: Extract worker_id (carried on the local PeerInfo).
        let worker_id = messenger.instance_id().worker_id();

        // Step 3: Resolve the streaming transport. Default is Tcp(None) —
        // bind on 0.0.0.0:0 and advertise every UP non-loopback interface
        // via Vec<InterfaceEndpoint> in WorkerAddress. Multi-node correctness
        // comes from the advertise list, not from defaulting away from TCP.
        //
        // Metrics are installed before the transport is type-erased into
        // `Arc<dyn FrameTransport>` because `set_metrics` is a concrete
        // method (the FrameTransport trait stays observability-free so
        // out-of-tree implementors don't take a `prometheus` dep).
        let resolved = self.stream_config.unwrap_or(StreamConfig::Tcp(None));
        let (stream_transport, owned_stream_transport): (
            Arc<dyn crate::streaming::FrameTransport>,
            OwnedStreamTransport,
        ) = match resolved {
            StreamConfig::Tcp(tcp_cfg) => {
                let bind_addr = tcp_cfg
                    .map(|c| c.bind_addr)
                    .unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));
                let tcp = crate::streaming::TcpFrameTransport::new(bind_addr).await?;
                if let Some(m) = self.metrics.as_ref() {
                    tcp.set_metrics(Arc::clone(m));
                }
                (tcp.clone() as _, OwnedStreamTransport::Tcp(tcp))
            }
            #[cfg(feature = "grpc")]
            StreamConfig::Grpc(grpc_cfg) => {
                let bind_addr = grpc_cfg
                    .map(|c| c.bind_addr)
                    .unwrap_or_else(|| "0.0.0.0:0".parse().unwrap());
                let grpc = crate::streaming::GrpcFrameTransport::new(bind_addr)
                    .await
                    .map_err(|e| {
                        anyhow::anyhow!("Failed to start gRPC streaming transport: {}", e)
                    })?;
                if let Some(m) = self.metrics.as_ref() {
                    grpc.set_metrics(Arc::clone(m));
                }
                (grpc.clone() as _, OwnedStreamTransport::Grpc(grpc))
            }
        };

        // Step 4: Build the streaming-transport registry, keyed by
        // TransportKey: the chosen transport here, and the mux in Step 5.
        // The AnchorManager passes the response's `streaming_transport_key`
        // through this map to find the FrameTransport on the client side at
        // attach time.
        let mut registry: std::collections::HashMap<
            String,
            Arc<dyn crate::streaming::FrameTransport>,
        > = std::collections::HashMap::new();
        registry.insert(
            stream_transport.key().as_str().to_string(),
            Arc::clone(&stream_transport),
        );

        // Step 5: Build the mux unless the caller switched it off (it is on by
        // default; see `messenger_mux`). It joins the registry *beside* the
        // per-stream transport rather than replacing it: negotiation answers
        // `messenger-mux-v2` only to peers that advertised it, and every other
        // peer is still answered — and must still be served — on the
        // per-stream key.
        let mut config = self.mux_config.unwrap_or_default();
        // Read once, here, for the reason the RDMA kill switch is: one process
        // must not answer half its attaches one way and half the other.
        if config.enabled && messenger_mux_disabled_by_env() {
            tracing::info!(
                "VELO_MESSENGER_MUX_DISABLE is set: the messenger mux is off. \
                 Streams negotiate the per-stream transport."
            );
            config.enabled = false;
        }
        let mux = if config.enabled {
            let mux = crate::streaming::messenger_mux::MessengerMuxTransport::new(
                Arc::clone(&messenger),
                config,
                self.metrics.clone(),
            )?;
            let mux_key = crate::streaming::FrameTransport::key(mux.as_ref());
            registry.insert(
                mux_key.as_str().to_string(),
                Arc::clone(&mux) as Arc<dyn crate::streaming::FrameTransport>,
            );
            Some(mux)
        } else {
            None
        };

        let anchor_manager = Arc::new(
            crate::streaming::AnchorManagerBuilder::default()
                .worker_id(worker_id)
                .transport(Arc::clone(&stream_transport))
                .transport_registry(Arc::new(registry))
                .messenger(Some(Arc::clone(&messenger)))
                .metrics(self.metrics.clone())
                .build()
                .map_err(|e| anyhow::anyhow!("{}", e))?,
        );

        if let Some(mux) = mux {
            anchor_manager.install_mux(mux)?;
        }

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

        // Step 9: Build the RDMA registration layer, if a UCX transport was
        // added through `add_ucx_transport`.
        //
        // Ordering: `MessengerBuilder::build` above has already called
        // `Transport::start` on every transport (`transports.rs`), so the RMA
        // endpoint this wraps is live. Constructing it earlier would not be
        // unsound — `RdmaEndpoint` is two `Arc`s and answers `NotStarted` until
        // the transport marks itself started — but it would let a registration
        // fail for a reason that reads like a bug.
        #[cfg(all(target_os = "linux", feature = "ucx"))]
        let rdma = match self.ucx_transport.as_ref() {
            Some(transport) => {
                let mut config = self.rdma_config.clone().unwrap_or_default();
                // The kill switch (D6), read once at build. An environment
                // variable rather than only a config field so a rollback is a
                // restart rather than a rebuild, and applied here rather than
                // at each decision point so one process cannot answer half its
                // acquires one way and half the other.
                if rdma_rendezvous_disabled_by_env() {
                    tracing::info!(
                        "VELO_RDMA_RENDEZVOUS_DISABLE is set: the rendezvous RDMA path is off. \
                         Staged data is still readable — every slot answers the chunked path."
                    );
                    config.rendezvous.enabled = false;
                }
                let rendezvous_config = config.rendezvous.clone();
                let registry = Arc::new(crate::rendezvous::rdma::RdmaRegistry::new(
                    crate::rendezvous::rdma::UcxBackend::new(transport.rdma_endpoint()),
                    config,
                    messenger.runtime().clone(),
                    self.metrics.clone(),
                ));
                // Hand the rendezvous manager the registry it was built too
                // early to be given: the registry wraps an RMA endpoint on a
                // transport that has to have started first. This also starts
                // the lease reaper.
                rendezvous_manager.set_rdma_context(
                    Arc::clone(&registry),
                    rendezvous_config,
                    messenger.runtime(),
                )?;
                Some(registry)
            }
            None => None,
        };

        // Step 10: Assemble Velo
        Ok(Arc::new(Velo {
            messenger,
            stream_owner: Arc::new(StreamOwner {
                manager: Arc::clone(&anchor_manager),
                transport: owned_stream_transport,
            }),
            anchor_manager,
            rendezvous_manager,
            stream_transport,
            #[cfg(all(target_os = "linux", feature = "ucx"))]
            rdma,
            shutdown: Arc::new(ShutdownOnce {
                lock: tokio::sync::Mutex::new(()),
                done: std::sync::atomic::AtomicBool::new(false),
            }),
        }))
    }
}

impl Default for VeloBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Whether `VELO_RDMA_RENDEZVOUS_DISABLE` asks for the rendezvous RDMA path to
/// be switched off (D6). Parsed by [`kill_switch_set`].
#[cfg(all(target_os = "linux", feature = "ucx"))]
fn rdma_rendezvous_disabled_by_env() -> bool {
    kill_switch_set(
        std::env::var("VELO_RDMA_RENDEZVOUS_DISABLE")
            .ok()
            .as_deref(),
    )
}

/// Whether `VELO_MESSENGER_MUX_DISABLE` asks for the messenger mux to be
/// switched off. Parsed by [`kill_switch_set`].
///
/// The mux is on by default, so an application that never calls
/// `messenger_mux()` has no configuration of its own to turn it off with.
/// This variable makes that rollback a restart rather than a rebuild.
fn messenger_mux_disabled_by_env() -> bool {
    kill_switch_set(std::env::var("VELO_MESSENGER_MUX_DISABLE").ok().as_deref())
}

/// The parsing half of every `*_DISABLE` kill switch, split out so it can be
/// tested.
///
/// Only `1`, `true`, `yes` and `on` (any case) count. A variable set to
/// anything else — `0`, `false`, an empty string, a typo — leaves the feature
/// enabled, because a kill switch that fires on a typo is worse than one that
/// occasionally does not fire on a misspelling: the first silently costs
/// performance in production, the second is visible the moment somebody checks
/// the metric.
///
/// The environment is process-global and `cargo test` runs in parallel, so a
/// test that *set* a variable would silently switch the feature off for every
/// other test building a `Velo` at that moment. Splitting the decision from the
/// read means the rule can be checked exhaustively without touching the
/// process; each switch's end-to-end effect is covered through the config
/// field it writes, or by a test binary of its own.
fn kill_switch_set(value: Option<&str>) -> bool {
    value.is_some_and(|v| {
        let v = v.trim().to_ascii_lowercase();
        v == "1" || v == "true" || v == "yes" || v == "on"
    })
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

    /// Begin Phase 1 (Gate) of graceful shutdown: reject new inbound requests
    /// while responses, acks, events, and the messages of streams already open
    /// keep flowing. See
    /// [`Messenger::begin_drain`].
    pub fn begin_drain(&self) {
        self.messenger.begin_drain();
    }

    /// Perform a graceful shutdown of the messenger transports: gate inbound
    /// requests, wait for in-flight handler invocations per `policy`, tear
    /// down, then wait for each transport's close. See
    /// [`Messenger::graceful_shutdown`].
    ///
    /// Streams opened before the drain keep flowing through it: the gate lets
    /// their messages through. The drain counts each such message only while
    /// its handler runs, not the stream, so an open stream, busy or quiet, does
    /// not hold this call open. Teardown then ends the streams that ride the
    /// messenger mux. To let streams
    /// finish, call [`begin_drain`](Self::begin_drain), wait for them, then
    /// call this. The per-stream transports have their own teardown, which
    /// this call does not cover.
    ///
    /// # RDMA registrations go first, and are declared released last
    ///
    /// When an RDMA registration layer is installed, shutdown has four
    /// steps:
    ///
    /// 1. [`begin_drain`](Self::begin_drain) — idempotent, and repeated by the
    ///    messenger shutdown below. Closing the inbound gate first means no new
    ///    request can start while registrations are being torn down. The pull
    ///    of a payload staged before the drain still passes the gate, but a
    ///    draining owner answers it chunked, never with an RDMA descriptor.
    /// 2. The registry sweep: registrations refused,
    ///    in-flight transfers drained, every region and arena unmapped.
    /// 3. Messenger gate, drain, teardown and close, unchanged.
    /// 4. Every registration that survived step 2 is declared released.
    ///
    /// Step 1 to 2 is load-bearing, not tidiness. An RDMA GET is issued by the
    /// *peer's* NIC, so it never appears in this instance's in-flight counts;
    /// tearing the transport down first and unmapping afterwards would
    /// deregister memory a peer is still reading.
    ///
    /// Step 4 is what makes [`RegionGuard::deregistered`] a signal worth
    /// waiting on. A region whose unmap could not be confirmed in step 2 — a
    /// wedged backend, a transport already going down — is nonetheless
    /// genuinely unmapped once step 3 returns, because transport teardown
    /// force-unmaps everything the progress thread still holds. Without step 4
    /// those latches would stay pending forever and a caller waiting on one
    /// would hold its memory for the life of the process.
    ///
    /// Step 2 begins by moving anything staged in registered memory onto the
    /// heap. That releases what the sweep's own drains wait on, and it costs
    /// one transient copy of everything staged — the price of letting a
    /// chunked transfer that was already admitted finish rather than fail
    /// halfway through.
    ///
    /// Step 4 is itself conditional: it checks with the backend that nothing is
    /// still registered, and declines to declare anything released if the
    /// answer is not "none". After an abnormal teardown — a panicking progress
    /// thread — the latches therefore stay pending and that memory is leaked on
    /// purpose. Velo will not tell a caller to free pages it cannot establish
    /// were released.
    ///
    /// # Called once, even from clones
    ///
    /// [`Velo`] is `Clone`, so concurrent callers are possible. They are
    /// serialised: the first runs the sequence, the rest wait and return as
    /// soon as it finishes. Only one caller can take the transport's join
    /// handle, and step 4's claim rests on that join having completed — a
    /// second caller running the tail concurrently would be declaring memory
    /// released while the progress thread was still alive.
    ///
    /// # One deadline, not one per phase
    ///
    /// [`ShutdownPolicy::Timeout`] bounds the sweep and messenger drain
    /// together, rather than giving each phase the full duration. Joining mux
    /// tasks comes after the drain and needs the owning runtime to make
    /// progress. The transports' close step also comes on top: it is bounded by each
    /// transport's own [`Transport::closed`] (QUIC: 2.5 s), not by the policy,
    /// because cutting it short would discard frames already written.
    /// Under [`ShutdownPolicy::WaitForever`] the sweep still takes
    /// [`RdmaConfig::shutdown_timeout`], because a peer that crashed
    /// mid-transfer must not wedge shutdown forever even when the caller is
    /// willing to wait on local work.
    ///
    /// # Panics
    ///
    /// Panics if a transport's shutdown hook panicked. The other hooks still
    /// ran, but shutdown cannot report the instance, or its RDMA memory, as
    /// released. A later call panics at once, without draining again.
    pub async fn graceful_shutdown(&self, policy: ShutdownPolicy) {
        // Serialised, and run once. `Velo` is `Clone`, so two clones can arrive
        // here together; the sequence below takes a transport join handle and
        // then declares memory released on the strength of that join having
        // finished, which is only true for whoever took it. A second caller
        // waits here and returns as soon as the first is done.
        let _running = self.shutdown.lock.lock().await;
        if self
            .shutdown
            .done
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return;
        }
        // A failed teardown is final. Running the RDMA sweep again would spend
        // its budget against torn-down transports, then panic anyway. (The
        // backend refuses to drain again on its own.)
        if let Some(error) = self.messenger.backend().teardown_failure() {
            panic!("transport teardown failed: {error}");
        }

        // Shadowed with what is left of the caller budget after the sweep, so
        // the two phases share one deadline instead of taking one each.
        #[cfg(all(target_os = "linux", feature = "ucx"))]
        let policy = {
            let started = std::time::Instant::now();
            if let Some(rdma) = &self.rdma {
                self.begin_drain();
                // Before the sweep, and load-bearing for it. This stops the
                // lease reaper — so it is not taking the same memory apart
                // from the other end while the sweep walks it — and moves
                // every pinned slot to the heap, which is what releases the
                // pool suballocations and region in-flight guards the sweep's
                // drains are about to wait on. A single long-lived anchor left
                // in place would consume the entire budget below on a drain
                // that cannot finish.
                //
                // The demotion copies everything currently staged in
                // registered memory onto the heap, once. That is the cost of
                // letting a chunked pull admitted before the gate finish
                // rather than fail mid-transfer, and it is bounded by what was
                // staged.
                //
                // Cancelling the reaper is not a join: a tick already running
                // may still complete. That overlap is benign — both paths end
                // a lease through the same atomic store operations, and the
                // sweep is trying to make slots disappear anyway.
                self.rendezvous_manager.shutdown();
                let budget = match &policy {
                    ShutdownPolicy::Timeout(deadline) => *deadline,
                    ShutdownPolicy::WaitForever => rdma.shutdown_timeout(),
                };
                rdma.shutdown(budget).await;
            }
            match &policy {
                ShutdownPolicy::Timeout(deadline) => {
                    ShutdownPolicy::Timeout(deadline.saturating_sub(started.elapsed()))
                }
                ShutdownPolicy::WaitForever => ShutdownPolicy::WaitForever,
            }
        };

        let backend = self.messenger.backend();
        backend.drain(policy).await;
        // A cancelled task can still be in a poll that sends a batch. Join it
        // before transport teardown, but leave ingress for reader detachment.
        self.anchor_manager.stop_mux_sending().await;
        backend.finish_shutdown().await;

        // Teardown has returned, so the progress thread has force-unmapped
        // everything it still held: nothing is pinned any more, and every latch
        // the sweep could not resolve honestly can be resolved now.
        #[cfg(all(target_os = "linux", feature = "ucx"))]
        if let Some(rdma) = &self.rdma {
            rdma.latch_all_deregistered();
        }

        self.shutdown
            .done
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Drain messenger work and stop this instance's streaming services.
    ///
    /// This also cancels live streams, and joins the receive loops and the
    /// streaming transport tasks owned by the builder. Stream reader pumps are
    /// cancelled, not joined. Custom frame transports
    /// remain the caller's responsibility. Stream watchdogs and heartbeats are
    /// cancelled; application handlers that exceed `policy` can still be running.
    /// If the sender of a stream is on this instance, shutdown cancels that
    /// sender: its `cancellation_token` fires, and later sends fail. The reader
    /// ends when the application drops or finalizes the sender.
    /// Use this when an instance is removed while its Tokio runtime stays alive.
    /// It closes resources. Drop the instance and its handles to release their memory.
    ///
    /// # Panics
    ///
    /// Panics if a transport's shutdown hook panicked. The other hooks still
    /// ran, but shutdown cannot report the instance, or its RDMA memory, as
    /// released. A later call panics at once, without draining again.
    pub async fn shutdown(&self, policy: ShutdownPolicy) {
        self.graceful_shutdown(policy).await;
        self.anchor_manager.shutdown().await;
        self.stream_owner.transport.shutdown().await;
        self.messenger.closed().await;
    }

    /// Get the instance ID of this system.
    pub fn instance_id(&self) -> InstanceId {
        self.messenger.instance_id()
    }

    /// Write everything the messenger mux has staged, to every peer.
    ///
    /// This is the flush point
    /// [`FlushPolicy::Manual`](crate::streaming::FlushPolicy::Manual) is named
    /// for. A serving loop calls it once per forward pass:
    ///
    /// ```ignore
    /// for request in &mut active {
    ///     request.sender.send(token).await?;   // stage
    /// }
    /// velo.flush_batch();                      // one write per peer
    /// ```
    ///
    /// **Sync and non-blocking.** It kicks each batcher and returns; it does not
    /// wait for the write, and it is not a backpressure point. Whether a
    /// congested peer slows the producer down stays the job of per-slot credit
    /// and of transport admission, exactly as it is when nobody calls this.
    ///
    /// **Every peer, not one.** A producer holds `StreamSender`s and cannot know
    /// which batcher each one feeds — the destination is packed into the anchor
    /// handle and resolved several layers below. So there is nothing to name,
    /// and the flush covers whatever this node has staged for anyone.
    ///
    /// **Valid under either policy, and never an error.** Under
    /// [`FlushPolicy::Auto`](crate::streaming::FlushPolicy::Auto) it forces a
    /// write ahead of the conditions the batcher would otherwise have waited
    /// for; under `Manual` it is the write. It is a cheap no-op when no mux is
    /// installed or when nothing is staged, so a call site does not have to know
    /// how the node was configured.
    ///
    /// A burst between two calls is a *hint*, not a frame boundary: the size
    /// clamps, the records that carry liveness, and credit may each cut a wire
    /// batch in between, so a caller may not assume what it bracketed arrives as
    /// one `_stream_batch`. See `docs/src/concepts/batched-streaming.md` § "Flush policy".
    pub fn flush_batch(&self) {
        self.anchor_manager.flush_mux_batches();
    }

    /// Get the peer information for this instance.
    ///
    /// The returned [`PeerInfo`] carries a [`WorkerAddress`] with both the
    /// messenger transport entries (TCP / gRPC / NATS / etc.) and the
    /// streaming transport entry (e.g., `tcp-stream` / `grpc-stream`). The
    /// streaming entry is required for peers to resolve the streaming
    /// listener via [`crate::streaming::FrameTransport::register`].
    pub fn peer_info(&self) -> PeerInfo {
        let messenger_peer = self.messenger.peer_info();
        let stream_addr = self.stream_transport.address();
        // Empty streaming address (a transport that opens no listener of its
        // own, e.g. the messenger mux) → no merge needed.
        if stream_addr.as_bytes().is_empty()
            || stream_addr
                .available_transports()
                .map(|v| v.is_empty())
                .unwrap_or(true)
        {
            return messenger_peer;
        }
        let mut builder = crate::transports::address::WorkerAddressBuilder::new();
        if let Err(e) = builder.merge(messenger_peer.worker_address()) {
            tracing::warn!(
                instance_id = %messenger_peer.instance_id(),
                error = %e,
                "peer_info: failed to merge messenger WorkerAddress into builder; \
                 falling back to messenger-only PeerInfo (streaming peers will not \
                 see this worker's streaming endpoint)"
            );
            return messenger_peer;
        }
        if let Err(e) = builder.merge(&stream_addr) {
            tracing::warn!(
                instance_id = %messenger_peer.instance_id(),
                streaming_key = %self.stream_transport.key(),
                error = %e,
                "peer_info: failed to merge streaming WorkerAddress into builder; \
                 falling back to messenger-only PeerInfo (likely a key collision \
                 with a messenger transport key)"
            );
            return messenger_peer;
        }
        match builder.build() {
            Ok(merged) => PeerInfo::new(messenger_peer.instance_id(), merged),
            Err(e) => {
                tracing::warn!(
                    instance_id = %messenger_peer.instance_id(),
                    error = %e,
                    "peer_info: WorkerAddressBuilder::build() failed; falling back \
                     to messenger-only PeerInfo"
                );
                messenger_peer
            }
        }
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
    ///
    /// Fans out to every messenger transport (via the messenger) and to the
    /// streaming transport, so each can extract its own entry from the peer's
    /// [`WorkerAddress`] and cache the resolved endpoint.
    pub fn register_peer(&self, peer_info: PeerInfo) -> Result<()> {
        // Streaming-transport register: skip the "no matching entry" case at
        // debug (e.g., a messenger-only peer or a peer using a different
        // streaming transport key). Any other failure -- WorkerAddress decode,
        // endpoint parse, NUMA mismatch -- is a real problem and must propagate
        // so it surfaces at register time, not at first attach.
        let stream_key = self.stream_transport.key();
        match peer_info.worker_address().get_entry(stream_key.as_str()) {
            Ok(Some(_)) => {
                self.stream_transport.register(&peer_info)?;
            }
            Ok(None) => {
                tracing::debug!(
                    peer = %peer_info.worker_id(),
                    streaming_key = %stream_key,
                    "streaming transport register: peer has no matching streaming endpoint"
                );
            }
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "decoding peer WorkerAddress for streaming key '{}': {e}",
                    stream_key
                ));
            }
        }
        self.messenger.register_peer(peer_info)
    }

    /// Discover a peer by instance_id and register it for communication.
    ///
    /// Resolves the [`PeerInfo`] through the configured [`PeerDiscovery`]
    /// backend and routes it through [`Self::register_peer`] so the streaming
    /// transport sees the peer alongside the messenger transports. Calling
    /// `messenger.discover_and_register_peer` directly would skip the
    /// streaming-side `register()` and surface as "peer not registered" on
    /// the next [`Self::attach_anchor`].
    pub async fn discover_and_register_peer(&self, instance_id: InstanceId) -> Result<()> {
        let discovery = self.messenger.discovery().ok_or_else(|| {
            anyhow::anyhow!(
                "No discovery backend configured. Cannot discover instance {}",
                instance_id
            )
        })?;
        let peer_info = discovery.discover_by_instance_id(instance_id).await?;
        self.register_peer(peer_info)
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
    ///
    /// Returns at once, with no network I/O, when the handler list already
    /// learned for `instance_id` names `handler_name`. Otherwise it refreshes
    /// the list with a `_hello` round trip up to 10 times, 100 ms apart, and
    /// returns a timeout error if the handler has not appeared. It is
    /// therefore not a reachability probe: once a handler is known, a later
    /// call does not check that the peer is still there. The cached list is
    /// kept for the process's life, which is safe because an instance id names
    /// one process, and a restarted peer has a new one.
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

    /// Attach a sender to an anchor, placing the stream on the mux lane `key`
    /// hashes to.
    ///
    /// Delegates to [`AnchorManager::attach_stream_anchor_keyed`](crate::streaming::AnchorManager::attach_stream_anchor_keyed).
    /// The key is a placement hint, not an ordering guarantee: streams with
    /// one key to one consumer share a lane, but the mux never orders records
    /// across streams. [`attach_anchor`](Velo::attach_anchor) lets the
    /// consumer pick the least-used lane instead.
    pub async fn attach_anchor_keyed<T: serde::Serialize>(
        &self,
        handle: StreamAnchorHandle,
        key: u64,
    ) -> Result<StreamSender<T>, AttachError> {
        self.anchor_manager
            .attach_stream_anchor_keyed::<T>(handle, key)
            .await
    }

    /// Bind a stream for an anchor now, so its sender never has to ask.
    ///
    /// Delegates to [`AnchorManager::prebind_anchor`](crate::streaming::AnchorManager::prebind_anchor).
    /// Carry the returned [`streaming::control::StreamOpenTicket`] to the worker
    /// in whatever request envelope you already send it, and have the worker
    /// open its sender with [`open_anchor_stream`](Velo::open_anchor_stream).
    /// `None` means no ticket was minted and the worker should
    /// [`attach_anchor`](Velo::attach_anchor) the ordinary way.
    ///
    /// Must be called from a runtime context: it spawns the stream watchdog,
    /// exactly as the attach handler does for a mux bind.
    pub fn prebind_anchor(
        &self,
        handle: StreamAnchorHandle,
    ) -> Option<streaming::control::StreamOpenTicket> {
        self.anchor_manager.prebind_anchor(handle)
    }

    /// As [`prebind_anchor`](Velo::prebind_anchor), placing the stream on the
    /// mux lane `key` hashes to.
    ///
    /// Delegates to [`AnchorManager::prebind_anchor_keyed`](crate::streaming::AnchorManager::prebind_anchor_keyed).
    pub fn prebind_anchor_keyed(
        &self,
        handle: StreamAnchorHandle,
        key: u64,
    ) -> Option<streaming::control::StreamOpenTicket> {
        self.anchor_manager.prebind_anchor_keyed(handle, key)
    }

    /// Open a sender for an anchor whose slot the consumer already bound.
    ///
    /// Delegates to [`AnchorManager::open_anchor_stream`](crate::streaming::AnchorManager::open_anchor_stream).
    /// The zero-RTT counterpart of [`attach_anchor`](Velo::attach_anchor): no
    /// `_anchor_attach` round trip, because `ticket` already carries what one
    /// would have returned.
    ///
    /// The mux carries stop and cancel through the ticket's session and slot
    /// identity. Both producer tokens work even before the first item is sent.
    pub async fn open_anchor_stream<T: serde::Serialize>(
        &self,
        handle: StreamAnchorHandle,
        ticket: streaming::control::StreamOpenTicket,
    ) -> Result<StreamSender<T>, AttachError> {
        self.anchor_manager
            .open_anchor_stream::<T>(handle, ticket)
            .await
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

    /// As [`attach_mpsc_anchor`](Velo::attach_mpsc_anchor), placing the sender
    /// on the mux lane `key` hashes to.
    ///
    /// Delegates to [`AnchorManager::attach_mpsc_stream_anchor_keyed`](crate::streaming::AnchorManager::attach_mpsc_stream_anchor_keyed).
    pub async fn attach_mpsc_anchor_keyed<T: serde::Serialize>(
        &self,
        handle: StreamAnchorHandle,
        key: u64,
    ) -> Result<streaming::mpsc::MpscStreamSender<T>, AttachError> {
        self.anchor_manager
            .attach_mpsc_stream_anchor_keyed::<T>(handle, key)
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

    /// Stage data in RDMA-registered memory, so a capable consumer reads it
    /// with a single RDMA GET instead of a chunk-by-chunk pull.
    ///
    /// Never fails: pool pressure, a spent registered-bytes budget, a
    /// switched-off kill switch and an instance with no UCX transport all stage
    /// the data in plain memory instead. See
    /// [`RendezvousManager::register_data_pinned`] for the full contract.
    pub async fn register_data_pinned(&self, data: &[u8]) -> DataHandle {
        self.rendezvous_manager.register_data_pinned(data).await
    }

    /// Stage a range of memory this instance already registered, zero-copy.
    ///
    /// See [`RendezvousManager::register_data_in_region`]: the slot holds an
    /// in-flight guard on the region, so
    /// [`RegionGuard::unregister`] waits for the anchors staged inside it.
    #[cfg(all(target_os = "linux", feature = "ucx"))]
    pub fn register_data_in_region(
        &self,
        guard: &RegionGuard,
        range: std::ops::Range<u64>,
    ) -> Result<DataHandle, RdmaError> {
        self.rendezvous_manager
            .register_data_in_region(guard, range)
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

    /// Pull data from a handle into registered memory, with no copy out.
    ///
    /// Returns `(buffer, lease_id)`. Dropping the buffer returns its space to
    /// the pool. See [`RendezvousManager::get_pinned`] for what the lease does
    /// and does not cover.
    #[cfg(all(target_os = "linux", feature = "ucx"))]
    pub async fn get_pinned(&self, handle: DataHandle) -> Result<(PinnedBuf, u64)> {
        self.rendezvous_manager.get_pinned(handle).await
    }

    /// Allocate a registered [`get_into`](Self::get_into) destination, so an
    /// RDMA transfer into it costs no copy at all.
    #[cfg(all(target_os = "linux", feature = "ucx"))]
    pub async fn alloc_pinned_writer(&self, len: usize) -> Result<PinnedWriter, RdmaError> {
        self.rendezvous_manager.alloc_pinned_writer(len).await
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

    /// Which transport this instance chose as `instance`'s primary, if it is
    /// registered.
    ///
    /// Exposed for tests only. The RDMA path's eligibility rule deliberately
    /// accepts a peer reachable over UCX *whether or not* UCX is the primary
    /// transport — a TCP control plane with UCX beside it is the expected
    /// deployment — and a test covering that branch has to be able to say which
    /// branch it is on. Without it, a change to transport priority could move
    /// the coverage back onto the primary path with nothing failing.
    #[cfg(feature = "test-helpers")]
    pub fn primary_transport_key(&self, instance: InstanceId) -> Option<String> {
        self.messenger
            .backend()
            .primary_transport_key(instance)
            .map(|key| key.as_str().to_string())
    }

    /// Get the underlying rendezvous manager for direct access.
    pub fn rendezvous_manager(&self) -> &crate::rendezvous::RendezvousManager {
        &self.rendezvous_manager
    }

    // -----------------------------------------------------------------------
    // RDMA registration API (ucx only)
    // -----------------------------------------------------------------------

    /// Register memory this instance does not own for RDMA access.
    ///
    /// Returns a [`RegionGuard`] the caller must keep. See its documentation
    /// for the lifecycle; the short version is that the guard, not the call, is
    /// what holds the registration open.
    ///
    /// # Errors
    ///
    /// [`RdmaError::NotConfigured`] if no UCX transport was installed through
    /// [`VeloBuilder::add_ucx_transport`], [`RdmaError::ShuttingDown`] once
    /// shutdown has begun, [`RdmaError::BudgetExceeded`] over the configured
    /// registered-bytes ceiling, [`RdmaError::OutOfRange`] for a null pointer,
    /// a zero length, or a range that wraps.
    ///
    /// # Safety
    ///
    /// The registration lasts until [`RegionGuard::deregistered`] resolves —
    /// which happens on a confirmed unmap, or at the end of
    /// [`graceful_shutdown`](Self::graceful_shutdown), whichever comes first.
    /// It does **not** end when the guard is dropped, and it does not end when
    /// an `unregister` returns `Err`. For that whole time, all of the following
    /// must hold.
    ///
    /// * `ptr` is valid for **both reads and writes** of `len` bytes. Read
    ///   validity is not enough: registering a range for RMA makes it remotely
    ///   writable by any holder of its key, because UCP carries no enforceable
    ///   protection field and the GET-only shape of the rendezvous protocol is
    ///   a convention rather than an enforcement. Registering a read-only
    ///   mapping is undefined behaviour even though velo never writes to it.
    /// * `ptr + len` does not wrap the address space.
    /// * The allocation is not freed, moved, remapped, or reallocated —
    ///   `realloc` included, whether or not it grows in place.
    /// * **No Rust reference into the range exists**: not `&[u8]`, not
    ///   `&mut [u8]`, not a reference to anything stored inside it. A peer may
    ///   write at any moment, which contradicts what a shared reference
    ///   promises and what a mutable one claims exclusively. Use raw pointers.
    ///
    /// Registration pins whole pages, so bytes adjacent to the allocation share
    /// its pinning *and its remote writability*;
    /// [`RegionGuard::effective_range`] reports what was actually pinned.
    ///
    /// Registering is therefore a trust decision about the peers this instance
    /// talks to, not merely a performance one.
    #[cfg(all(target_os = "linux", feature = "ucx"))]
    pub async unsafe fn register_external_memory(
        &self,
        ptr: std::ptr::NonNull<u8>,
        len: usize,
    ) -> Result<RegionGuard, RdmaError> {
        let rdma = self.rdma_registry()?;
        // SAFETY: forwarded verbatim. This method's contract is the registry's
        // contract, and nothing in between touches the memory.
        unsafe { rdma.register_external(ptr, len) }.await
    }

    /// Register a buffer, handing ownership of it to velo.
    ///
    /// The safe counterpart to
    /// [`register_external_memory`](Self::register_external_memory): velo holds
    /// the allocation until a deregistration is confirmed, so the caller cannot
    /// free it early. Recover it with [`RegionGuard::unregister_owned`], or let
    /// it drop with the region.
    ///
    /// On failure the buffer comes back inside the error.
    /// [`RdmaError::BudgetExceeded`] is a routine refusal that a caller answers
    /// by staging chunked, and an error that consumed the allocation would make
    /// that fallback cost more than the path it falls back from.
    ///
    /// Note that a `Box<[u8]>` is byte-aligned while registration pins whole
    /// pages, so neighbouring heap shares the pinning — and with it the remote
    /// writability. [`RegionGuard::effective_range`] is how to see it.
    #[cfg(all(target_os = "linux", feature = "ucx"))]
    pub async fn register_owned(
        &self,
        buf: Box<[u8]>,
    ) -> Result<RegionGuard, crate::rendezvous::rdma::RegisterOwnedError> {
        let Some(rdma) = self.rdma.as_ref() else {
            return Err(crate::rendezvous::rdma::RegisterOwnedError {
                buffer: Some(buf),
                cause: RdmaError::NotConfigured,
            });
        };
        rdma.register_owned(buf).await
    }

    /// Bytes currently registered for RDMA, pool and external together.
    #[cfg(all(target_os = "linux", feature = "ucx"))]
    pub fn rdma_registered_bytes(&self) -> u64 {
        self.rdma
            .as_ref()
            .map(|r| r.registered_bytes())
            .unwrap_or(0)
    }

    /// Transfers the NIC is still writing into this instance's arenas.
    ///
    /// Exposed for tests only. A test that wants to act *while* a transfer is
    /// in flight has to be able to see that it started; timing the caller's
    /// future instead races the acquire round trip, and a cancelled future
    /// looks identical whether the transfer began or never did.
    #[cfg(all(target_os = "linux", feature = "ucx", feature = "test-helpers"))]
    pub fn rdma_in_flight_transfers(&self) -> usize {
        self.rdma
            .as_ref()
            .map(|r| r.in_flight_transfers())
            .unwrap_or(0)
    }

    /// The registration layer, for tests that need to observe it directly.
    #[cfg(all(target_os = "linux", feature = "ucx", test))]
    pub(crate) fn rdma(&self) -> Option<&Arc<crate::rendezvous::rdma::RdmaRegistry>> {
        self.rdma.as_ref()
    }

    /// The registration layer, for staging and transfers.
    #[cfg(all(target_os = "linux", feature = "ucx"))]
    pub(crate) fn rdma_registry(
        &self,
    ) -> Result<&Arc<crate::rendezvous::rdma::RdmaRegistry>, RdmaError> {
        self.rdma.as_ref().ok_or(RdmaError::NotConfigured)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every kill switch fires on an affirmative and on nothing else.
    ///
    /// The asymmetry is deliberate and worth pinning down: a switch that fired
    /// on a typo would silently cost performance in production, while one that
    /// misses a misspelling shows up the moment anybody reads the feature's
    /// signal: `velo_rendezvous_rdma_path_total` for RDMA, and
    /// `StreamSender::negotiated_transport()` for the mux.
    #[test]
    fn the_kill_switches_read_only_affirmatives() {
        for on in [
            "1", "true", "TRUE", "True", "yes", "YES", "on", "ON", " 1 ", "\ttrue\n",
        ] {
            assert!(
                kill_switch_set(Some(on)),
                "{on:?} should switch the feature off"
            );
        }
        for off in [
            "0", "false", "no", "off", "", "  ", "2", "disable", "ture", "1 1",
        ] {
            assert!(
                !kill_switch_set(Some(off)),
                "{off:?} must not switch the feature off"
            );
        }
        assert!(
            !kill_switch_set(None),
            "an unset variable must leave the feature on"
        );
    }

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

    #[cfg(feature = "grpc")]
    #[tokio::test]
    async fn failed_stream_start_stops_messenger_listener() {
        let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let messenger_addr = listener.local_addr().unwrap();
        let transport = Arc::new(
            crate::transports::tcp::TcpTransportBuilder::new()
                .from_listener(listener)
                .unwrap()
                .build()
                .unwrap(),
        );
        let result = Velo::builder()
            .add_transport(transport)
            .stream_config(StreamConfig::Grpc(Some(GrpcConfig {
                bind_addr: occupied.local_addr().unwrap(),
            })))
            .unwrap()
            .build()
            .await;
        assert!(
            result.is_err(),
            "occupied streaming port must reject the build"
        );
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if let Ok(listener) = std::net::TcpListener::bind(messenger_addr) {
                    break listener;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("failed build retained the messenger listener");
    }

    /// Handlers never own their manager, and a manager never owns its
    /// messenger beyond what it needs for ordering. Registration through the
    /// public API therefore forms no cycle: once the caller drops its
    /// references, the Messenger drops and starts its teardown.
    #[tokio::test]
    async fn standalone_registration_does_not_retain_the_messenger() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let transport = Arc::new(
            crate::transports::tcp::TcpTransportBuilder::new()
                .from_listener(listener)
                .unwrap()
                .build()
                .unwrap(),
        );
        let messenger = Messenger::builder()
            .add_transport(transport)
            .build()
            .await
            .unwrap();
        let worker_id = messenger.instance_id().worker_id();
        let stream = crate::streaming::TcpFrameTransport::new(std::net::Ipv4Addr::LOCALHOST.into())
            .await
            .unwrap();
        let anchors = Arc::new(crate::streaming::AnchorManager::new(
            worker_id,
            Arc::clone(&stream) as Arc<dyn crate::streaming::FrameTransport>,
        ));
        anchors.register_handlers(Arc::clone(&messenger)).unwrap();
        let rendezvous = Arc::new(RendezvousManager::new(worker_id));
        rendezvous
            .register_handlers(Arc::clone(&messenger))
            .unwrap();

        let weak = Arc::downgrade(&messenger);
        let weak_anchors = Arc::downgrade(&anchors);
        drop(messenger);
        drop(anchors);
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while weak.upgrade().is_some() || weak_anchors.upgrade().is_some() {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("public handler registration kept the messenger alive");
        drop(rendezvous);
        stream.shutdown().await;
    }

    /// Shutdown must close resources while Tokio remains alive.
    #[tokio::test]
    async fn shutdown_joins_owned_tasks_and_closes_listener() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let messenger_addr = listener.local_addr().unwrap();
        let transport = {
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

        let stream_addr = match &velo.stream_owner.transport {
            OwnedStreamTransport::Tcp(transport) => transport.bound_addr(),
            #[cfg(feature = "grpc")]
            OwnedStreamTransport::Grpc(_) => unreachable!(),
        };

        let messenger = Arc::downgrade(&velo.messenger);
        let manager = Arc::downgrade(&velo.anchor_manager);
        let rendezvous = Arc::downgrade(&velo.rendezvous_manager);
        let events = Arc::downgrade(velo.messenger.events());
        let pending_event = velo.messenger.events().new_event().unwrap().into_handle();
        #[derive(serde::Serialize)]
        struct Subscription {
            handle: u128,
            subscriber_worker: u64,
            subscriber_instance: InstanceId,
        }
        let subscribe = Subscription {
            handle: pending_event.raw(),
            subscriber_worker: velo.instance_id().worker_id().as_u64(),
            subscriber_instance: velo.instance_id(),
        };
        velo.messenger
            .events()
            .handle_subscribe(bytes::Bytes::from(serde_json::to_vec(&subscribe).unwrap()))
            .await
            .unwrap();

        let anchor: crate::streaming::StreamAnchor<String> = velo.create_anchor::<String>();
        let handle = anchor.handle();

        let result: Result<crate::streaming::StreamSender<String>, crate::streaming::AttachError> =
            velo.attach_anchor::<String>(handle).await;

        let sender = result.expect("local attach should succeed");
        drop(sender);
        drop(anchor);
        velo.shutdown(ShutdownPolicy::Timeout(std::time::Duration::from_secs(1)))
            .await;
        assert!(velo.anchor_manager.registry.is_empty());
        velo.tracker().close();
        tokio::time::timeout(std::time::Duration::from_secs(2), velo.tracker().wait())
            .await
            .expect("shutdown retained a tracked task");
        std::net::TcpListener::bind(messenger_addr)
            .expect("shutdown retained the messenger listener");
        std::net::TcpListener::bind(stream_addr).expect("shutdown retained the streaming listener");
        drop(velo);
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while messenger.upgrade().is_some()
                || manager.upgrade().is_some()
                || rendezvous.upgrade().is_some()
                || events.upgrade().is_some()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("shutdown retained a runtime manager");
    }

    #[tokio::test]
    async fn final_velo_drop_stops_streaming_but_keeps_a_retained_messenger() {
        use futures::StreamExt;

        async fn node() -> (Arc<Velo>, std::net::SocketAddr) {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let node = Velo::builder()
                .add_transport(Arc::new(
                    crate::transports::tcp::TcpTransportBuilder::new()
                        .from_listener(listener)
                        .unwrap()
                        .build()
                        .unwrap(),
                ))
                .build()
                .await
                .unwrap();
            (node, addr)
        }

        async fn wait_for_listener_close(addr: std::net::SocketAddr) {
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                loop {
                    if std::net::TcpListener::bind(addr).is_ok() {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("drop retained a listener");
        }

        let (server, messenger_addr) = node().await;
        let (client, _) = node().await;
        client.register_peer(server.peer_info()).unwrap();
        server.register_peer(client.peer_info()).unwrap();
        server
            .register_handler(Handler::unary_handler("ping", |ctx| Ok(Some(ctx.payload))).build())
            .unwrap();
        let mut anchor = server.create_anchor::<u32>();
        let manager = Arc::downgrade(&server.anchor_manager);
        let retained = Arc::clone(server.messenger());
        let messenger = Arc::downgrade(&retained);
        let backend = Arc::clone(retained.backend());
        let tracker = retained.tracker().clone();
        let frame_transport = match &server.stream_owner.transport {
            OwnedStreamTransport::Tcp(transport) => Arc::clone(transport),
            #[cfg(feature = "grpc")]
            OwnedStreamTransport::Grpc(_) => unreachable!(),
        };
        let stream_addr = frame_transport.bound_addr();
        let last_owner = server.as_ref().clone();
        drop(server);

        let sender = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            client.attach_anchor::<u32>(anchor.handle()),
        )
        .await
        .expect("a Velo clone lost its receive loop")
        .expect("a Velo clone lost streaming control handlers");
        sender.send(7).await.unwrap();
        let frame = tokio::time::timeout(std::time::Duration::from_secs(2), anchor.next())
            .await
            .expect("a Velo clone lost its stream");
        assert!(matches!(
            frame,
            Some(Ok(crate::streaming::StreamFrame::Item(7)))
        ));

        drop(last_owner);
        let ended = tokio::time::timeout(std::time::Duration::from_secs(2), anchor.next())
            .await
            .expect("final Velo drop left its stream open");
        assert!(ended.is_none(), "final Velo drop returned {ended:?}");
        wait_for_listener_close(stream_addr).await;
        assert!(manager.upgrade().is_none());
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            client
                .unary("ping")
                .unwrap()
                .raw_payload(bytes::Bytes::from_static(b"alive"))
                .instance(retained.instance_id())
                .send(),
        )
        .await
        .expect("retained Messenger stopped with Velo")
        .unwrap();
        assert_eq!(response, bytes::Bytes::from_static(b"alive"));
        // Cleanup handlers stay idempotent once the manager is gone: an
        // absent manager holds no anchor, so a peer's cleanup has succeeded.
        let handle = anchor.handle();
        let cleanups = [
            ("_anchor_detach", serde_json::json!({ "handle": handle })),
            ("_anchor_finalize", serde_json::json!({ "handle": handle })),
            ("_anchor_cancel", serde_json::json!({ "handle": handle })),
            (
                "_mpsc_anchor_detach",
                serde_json::json!({ "handle": handle, "sender_id": 1 }),
            ),
            (
                "_mpsc_anchor_cancel",
                serde_json::json!({ "handle": handle }),
            ),
        ];
        for (name, request) in cleanups {
            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                client
                    .messenger()
                    .typed_unary_streaming::<()>(name)
                    .payload(request)
                    .unwrap()
                    .instance(retained.instance_id())
                    .send(),
            )
            .await
            .unwrap_or_else(|_| panic!("{name} did not answer"))
            .unwrap_or_else(|error| panic!("{name} failed after final Velo drop: {error}"));
        }

        drop(anchor);
        drop(sender);
        drop(retained);
        // Teardown runs on its own thread; wait for it before timing the
        // receive loops, so the budget below covers only their exit.
        wait_for_final_messenger_drop(&messenger, &backend).await;
        tracker.close();
        tokio::time::timeout(std::time::Duration::from_secs(2), tracker.wait())
            .await
            .expect("final Messenger drop retained its receive loops");
        wait_for_listener_close(messenger_addr).await;
        frame_transport.shutdown().await;
        client.shutdown(ShutdownPolicy::WaitForever).await;
    }

    /// A node with one TCP transport on a loopback port and a loopback stream listener.
    async fn tcp_stream_node(metrics: Option<Arc<VeloMetrics>>) -> Arc<Velo> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let mut builder = Velo::builder()
            .add_transport(Arc::new(
                crate::transports::tcp::TcpTransportBuilder::new()
                    .from_listener(listener)
                    .unwrap()
                    .build()
                    .unwrap(),
            ))
            .stream_bind_addr(std::net::Ipv4Addr::LOCALHOST.into());
        if let Some(metrics) = metrics {
            builder = builder.metrics(metrics);
        }
        builder.build().await.unwrap()
    }

    /// A (consumer, producer) pair that know each other and both serve `attach_handler`.
    async fn connected_pair(
        consumer_metrics: Option<Arc<VeloMetrics>>,
        attach_handler: &str,
    ) -> (Arc<Velo>, Arc<Velo>) {
        let consumer = tcp_stream_node(consumer_metrics).await;
        let producer = tcp_stream_node(None).await;
        consumer.register_peer(producer.peer_info()).unwrap();
        producer.register_peer(consumer.peer_info()).unwrap();
        for (node, peer) in [
            (&consumer, producer.instance_id()),
            (&producer, consumer.instance_id()),
        ] {
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                node.wait_for_handler(peer, attach_handler),
            )
            .await
            .unwrap()
            .unwrap();
        }
        (consumer, producer)
    }

    /// Shutdown must not write to peers after its transports are gone.
    ///
    /// Credit and slot-close records can race transport teardown even when
    /// the application has stopped sending. Graceful shutdown must stop and
    /// join the mux before closing its transports, then detach readers before
    /// retiring ingress slots.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shutdown_with_live_mux_stream_sends_nothing_after_teardown() {
        use crate::observability::test_helpers::MetricSnapshot;
        use futures::StreamExt;

        let registry = prometheus::Registry::new();
        let metrics = Arc::new(VeloMetrics::register(&registry).unwrap());
        let (consumer, producer) = connected_pair(Some(metrics), "_anchor_attach").await;

        let mut anchor = consumer.create_anchor::<u32>();
        let sender = producer
            .attach_anchor::<u32>(anchor.handle())
            .await
            .unwrap();
        sender.send(1).await.unwrap();
        let first = tokio::time::timeout(std::time::Duration::from_secs(5), anchor.next())
            .await
            .unwrap();
        assert!(matches!(
            first,
            Some(Ok(crate::streaming::StreamFrame::Item(1)))
        ));

        let send_errors = || {
            MetricSnapshot::from_registry(&registry).counter(
                "velo_transport_rejections_total",
                &[("transport", "tcp"), ("reason", "send_error")],
            )
        };
        let before = send_errors();
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            consumer.shutdown(ShutdownPolicy::WaitForever),
        )
        .await
        .expect("consumer shutdown hung");
        // A failed dial reports from the writer task after shutdown returns.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert_eq!(
            send_errors(),
            before,
            "shutdown sent a frame after its transport was torn down"
        );

        drop(sender);
        drop(anchor);
        producer.shutdown(ShutdownPolicy::WaitForever).await;
    }

    /// Final Messenger drop runs transport teardown on its own thread. Wait
    /// until the Messenger is gone and that teardown has finished.
    async fn wait_for_final_messenger_drop(
        messenger: &std::sync::Weak<Messenger>,
        backend: &crate::transports::VeloBackend,
    ) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while messenger.upgrade().is_some() {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("final Velo drop retained its Messenger");
        backend.request_teardown().await.unwrap();
    }

    /// A consumer still reading when its node shuts down must see the stream
    /// end, not `SenderDropped`: the sender did nothing wrong.
    ///
    /// Stopping the mux retires every ingress slot by injecting `Dropped` into
    /// it. A consumer whose direct feed is still installed reads that record
    /// as its sender's. So each anchor's feed must be withdrawn before the mux
    /// stops, which is also what `AnchorEntry::drop` does before it closes a
    /// slot, for the same reason.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shutdown_ends_a_live_mux_stream_without_sender_dropped() {
        use futures::StreamExt;

        // Explicit shutdown; final Velo drop with a retained Messenger; and
        // final Velo drop that is also the final Messenger drop.
        for (drop_owner, retain_messenger) in [(false, true), (true, true), (true, false)] {
            let (consumer, producer) = connected_pair(None, "_anchor_attach").await;

            let mut anchor = consumer.create_anchor::<u32>();
            let sender = producer
                .attach_anchor::<u32>(anchor.handle())
                .await
                .unwrap();
            sender.send(1).await.unwrap();
            let first = tokio::time::timeout(std::time::Duration::from_secs(5), anchor.next())
                .await
                .unwrap();
            assert!(matches!(
                first,
                Some(Ok(crate::streaming::StreamFrame::Item(1)))
            ));

            let reader = tokio::spawn(async move { anchor.next().await });
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let messenger = Arc::downgrade(consumer.messenger());
            let backend = Arc::clone(consumer.messenger().backend());
            let retained = retain_messenger.then(|| Arc::clone(consumer.messenger()));
            if drop_owner {
                drop(consumer);
            } else {
                tokio::time::timeout(
                    std::time::Duration::from_secs(10),
                    consumer.shutdown(ShutdownPolicy::WaitForever),
                )
                .await
                .expect("consumer shutdown hung");
            }
            let ended = tokio::time::timeout(std::time::Duration::from_secs(5), reader)
                .await
                .expect("reader still waiting after shutdown")
                .unwrap();
            assert!(
                ended.is_none(),
                "shutdown must end a live stream cleanly, got {ended:?}"
            );
            if !retain_messenger {
                wait_for_final_messenger_drop(&messenger, &backend).await;
            }

            drop(sender);
            producer.shutdown(ShutdownPolicy::WaitForever).await;
            drop(retained);
        }
    }

    /// The MPSC form of the test above. An MPSC slot's records reach the
    /// consumer through a pump, so stopping the mux injects `Dropped` that a
    /// live pump forwards as the sender's own `Dropped`. The pumps must stop
    /// before the mux does.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shutdown_ends_a_live_mpsc_mux_stream_without_dropped() {
        use futures::StreamExt;

        use crate::observability::test_helpers::MetricSnapshot;

        // Explicit shutdown; final Velo drop with a retained Messenger; and
        // final Velo drop that is also the final Messenger drop.
        for (drop_owner, retain_messenger) in [(false, true), (true, true), (true, false)] {
            let registry = prometheus::Registry::new();
            let metrics = Arc::new(VeloMetrics::register(&registry).unwrap());
            let (consumer, producer) = connected_pair(Some(metrics), "_mpsc_anchor_attach").await;

            let mut anchor = consumer.create_mpsc_anchor::<u32>();
            let sender = producer
                .attach_mpsc_anchor::<u32>(anchor.handle())
                .await
                .unwrap();
            sender.send(1).await.unwrap();
            let first = tokio::time::timeout(std::time::Duration::from_secs(5), anchor.next())
                .await
                .unwrap();
            assert!(matches!(
                first,
                Some(Ok((_, crate::streaming::mpsc::MpscFrame::Item(1))))
            ));

            let send_errors = || {
                MetricSnapshot::from_registry(&registry).counter(
                    "velo_transport_rejections_total",
                    &[("transport", "tcp"), ("reason", "send_error")],
                )
            };
            let before = send_errors();
            let reader = tokio::spawn(async move { anchor.next().await });
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let messenger = Arc::downgrade(consumer.messenger());
            let backend = Arc::clone(consumer.messenger().backend());
            let retained = retain_messenger.then(|| Arc::clone(consumer.messenger()));
            if drop_owner {
                drop(consumer);
            } else {
                tokio::time::timeout(
                    std::time::Duration::from_secs(10),
                    consumer.shutdown(ShutdownPolicy::WaitForever),
                )
                .await
                .expect("consumer shutdown hung");
            }
            let ended = tokio::time::timeout(std::time::Duration::from_secs(5), reader)
                .await
                .expect("reader still waiting after shutdown")
                .unwrap();
            assert!(
                ended.is_none(),
                "shutdown must end a live MPSC stream cleanly, got {ended:?}"
            );
            if !retain_messenger {
                wait_for_final_messenger_drop(&messenger, &backend).await;
            }
            // A stopped pump releases its slot; that must not reach the wire.
            // Only explicit shutdown can check this. A retained Messenger keeps
            // the transports up, so no send can fail; and on a final drop that
            // also drops the Messenger, the notices to remote producers are
            // admitted just before teardown and may fail there, by design
            // (best effort, see the shutdown chapter).
            if !drop_owner {
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                assert_eq!(
                    send_errors(),
                    before,
                    "shutdown sent a frame after its transport was torn down"
                );
            }

            drop(sender);
            producer.shutdown(ShutdownPolicy::WaitForever).await;
            drop(retained);
        }
    }

    /// Final Velo drop with a retained Messenger must tell remote producers
    /// their stream ended, however the stream was opened. The
    /// mux stops on drop, and a retained Messenger's mux handler drops their
    /// batches once the mux is gone, so a producer told nothing fills its
    /// window and then waits forever. An attached stream is told by
    /// `_stream_cancel`. A zero-RTT stream records no cancel handle; it is
    /// told by its slot close, which must go out before the mux stops.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn final_velo_drop_cancels_remote_mux_producers() {
        use futures::StreamExt;

        for mode in ["attach", "mpsc", "ticket"] {
            for retain in [true, false] {
                let handler = if mode == "mpsc" {
                    "_mpsc_anchor_attach"
                } else {
                    "_anchor_attach"
                };
                let (consumer, producer) = connected_pair(None, handler).await;
                // The cancel token, plus the sender that must outlive the wait.
                let (token, _mpsc_sender, _sender) = if mode == "mpsc" {
                    let mut anchor = consumer.create_mpsc_anchor::<u32>();
                    let sender = producer
                        .attach_mpsc_anchor::<u32>(anchor.handle())
                        .await
                        .unwrap();
                    sender.send(1).await.unwrap();
                    tokio::time::timeout(std::time::Duration::from_secs(5), anchor.next())
                        .await
                        .unwrap();
                    let token = sender.cancellation_token();
                    tokio::spawn(async move { while anchor.next().await.is_some() {} });
                    (token, Some(sender), None)
                } else {
                    let mut anchor = consumer.create_anchor::<u32>();
                    let sender = if mode == "ticket" {
                        let ticket = consumer.prebind_anchor(anchor.handle()).unwrap();
                        producer
                            .open_anchor_stream::<u32>(anchor.handle(), ticket)
                            .await
                            .unwrap()
                    } else {
                        producer
                            .attach_anchor::<u32>(anchor.handle())
                            .await
                            .unwrap()
                    };
                    sender.send(1).await.unwrap();
                    tokio::time::timeout(std::time::Duration::from_secs(5), anchor.next())
                        .await
                        .unwrap();
                    let token = sender.cancellation_token();
                    tokio::spawn(async move { while anchor.next().await.is_some() {} });
                    (token, None, Some(sender))
                };
                let retained = retain.then(|| Arc::clone(consumer.messenger()));
                drop(consumer);
                let told =
                    tokio::time::timeout(std::time::Duration::from_secs(5), token.cancelled())
                        .await;
                // Only with a retained Messenger is the notice certain. Without
                // one, the Messenger's own final drop tears the transport down
                // right after the notice is queued, and may fail it there: best
                // effort, as the shutdown chapter says. That arm still has to
                // drop cleanly, without a panic or a hang.
                if retain {
                    told.unwrap_or_else(|_| {
                        panic!("{mode}: the remote producer was never told its stream ended")
                    });
                }
                drop((token, _mpsc_sender, _sender));
                producer.shutdown(ShutdownPolicy::WaitForever).await;
                drop(retained);
            }
        }
    }

    /// What `Velo::shutdown` does to a stream whose sender is on this node
    /// and still held by the application: the sender is cancelled (its
    /// `cancellation_token` fires and later sends fail), and its reader ends
    /// when the application drops it. Shutdown does not end the reader itself:
    /// a consumer poll that watched for it would cost every read a check.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shutdown_cancels_a_held_local_sender_and_its_reader_ends_on_drop() {
        use futures::StreamExt;

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let velo = Velo::builder()
            .add_transport(Arc::new(
                crate::transports::tcp::TcpTransportBuilder::new()
                    .from_listener(listener)
                    .unwrap()
                    .build()
                    .unwrap(),
            ))
            .build()
            .await
            .unwrap();

        let mut anchor = velo.create_anchor::<u32>();
        let sender = velo.attach_anchor::<u32>(anchor.handle()).await.unwrap();
        let mut mpsc_anchor = velo.create_mpsc_anchor::<u32>();
        let mpsc_sender = velo
            .attach_mpsc_anchor::<u32>(mpsc_anchor.handle())
            .await
            .unwrap();
        let reader = tokio::spawn(async move { anchor.next().await });
        let mpsc_reader = tokio::spawn(async move { mpsc_anchor.next().await });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            velo.shutdown(ShutdownPolicy::WaitForever),
        )
        .await
        .expect("shutdown hung");
        assert!(sender.cancellation_token().is_cancelled());
        assert!(mpsc_sender.cancellation_token().is_cancelled());
        assert!(sender.send(1).await.is_err());
        assert!(mpsc_sender.send(1).await.is_err());

        drop(sender);
        drop(mpsc_sender);
        let ended = tokio::time::timeout(std::time::Duration::from_secs(2), reader)
            .await
            .expect("SPSC reader still waiting after its sender was dropped")
            .unwrap();
        assert!(
            matches!(
                ended,
                Some(Err(crate::streaming::StreamError::SenderDropped))
            ),
            "got {ended:?}"
        );
        let ended = tokio::time::timeout(std::time::Duration::from_secs(2), mpsc_reader)
            .await
            .expect("MPSC reader still waiting after its sender was dropped")
            .unwrap();
        assert!(
            matches!(
                ended,
                Some(Ok((_, crate::streaming::mpsc::MpscFrame::Dropped(_))))
            ),
            "got {ended:?}"
        );
    }

    /// Every state change in shutdown happens before its first await.
    ///
    /// A caller can bound `Velo::shutdown` with a timeout and drop it while it
    /// waits for the mux's tasks. Streams must already be off their slots and
    /// their anchors removed by then; a dropped future that had withdrawn the
    /// feeds but not removed the anchors would leave their readers waiting on
    /// nothing. The runtime is single-threaded, so the mux's own tasks have
    /// not run and its join is still pending at the first poll.
    #[tokio::test(flavor = "current_thread")]
    async fn abandoning_anchor_shutdown_still_removes_every_anchor() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let velo = Velo::builder()
            .add_transport(Arc::new(
                crate::transports::tcp::TcpTransportBuilder::new()
                    .from_listener(listener)
                    .unwrap()
                    .build()
                    .unwrap(),
            ))
            .build()
            .await
            .unwrap();
        let _anchor = velo.create_anchor::<u32>();
        let _mpsc_anchor = velo.create_mpsc_anchor::<u32>();
        {
            let shutdown = velo.anchor_manager.shutdown();
            futures::pin_mut!(shutdown);
            assert!(
                futures::poll!(shutdown.as_mut()).is_pending(),
                "the test needs shutdown to wait at its first poll"
            );
        }
        assert!(velo.anchor_manager.registry.is_empty());
        assert!(velo.anchor_manager.mpsc_registry.is_empty());
    }

    /// Final Velo drop can run on a thread with no Tokio context, and after
    /// the runtime that built the instance is gone. The anchor cleanup that
    /// `Velo::shutdown` runs on the runtime now also runs in `Drop`.
    #[test]
    fn final_velo_drop_off_runtime_with_live_mux_streams() {
        for stop_runtime in [false, true] {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            let (consumer, producer, senders, tokens) = runtime.block_on(async {
                let (consumer, producer) = connected_pair(None, "_anchor_attach").await;
                let mut anchor = consumer.create_anchor::<u32>();
                let ticket = consumer.prebind_anchor(anchor.handle()).unwrap();
                let zero_rtt = producer
                    .open_anchor_stream::<u32>(anchor.handle(), ticket)
                    .await
                    .unwrap();
                zero_rtt.send(1).await.unwrap();
                tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    futures::StreamExt::next(&mut anchor),
                )
                .await
                .unwrap();
                tokio::spawn(async move {
                    while futures::StreamExt::next(&mut anchor).await.is_some() {}
                });
                let mut anchor2 = consumer.create_anchor::<u32>();
                let attached = producer
                    .attach_anchor::<u32>(anchor2.handle())
                    .await
                    .unwrap();
                attached.send(1).await.unwrap();
                tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    futures::StreamExt::next(&mut anchor2),
                )
                .await
                .unwrap();
                tokio::spawn(async move {
                    while futures::StreamExt::next(&mut anchor2).await.is_some() {}
                });
                let tokens = (zero_rtt.cancellation_token(), attached.cancellation_token());
                (consumer, producer, (zero_rtt, attached), tokens)
            });
            let messenger = Arc::downgrade(consumer.messenger());
            let backend = Arc::clone(consumer.messenger().backend());
            let runtime = if stop_runtime {
                drop(runtime);
                None
            } else {
                Some(runtime)
            };
            // A plain thread: no Tokio context at all.
            std::thread::spawn(move || drop(consumer))
                .join()
                .unwrap_or_else(|_| {
                    panic!("stop_runtime={stop_runtime}: final Velo drop panicked off the runtime")
                });
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while messenger.upgrade().is_some() && std::time::Instant::now() < deadline {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            assert!(
                messenger.upgrade().is_none(),
                "stop_runtime={stop_runtime}: final Velo drop left the Messenger alive"
            );
            futures::executor::block_on(backend.request_teardown()).unwrap();
            if let Some(runtime) = runtime {
                // With the runtime alive, both remote producers must be told.
                runtime.block_on(async {
                    tokio::time::timeout(std::time::Duration::from_secs(5), tokens.0.cancelled())
                        .await
                        .expect("zero-RTT producer was never told");
                    tokio::time::timeout(std::time::Duration::from_secs(5), tokens.1.cancelled())
                        .await
                        .expect("attached producer was never told");
                    drop(senders);
                    producer.shutdown(ShutdownPolicy::WaitForever).await;
                });
                drop(runtime);
            } else {
                drop(senders);
                drop(producer);
            }
        }
    }

    /// A transport whose peer never reads: its gate holds one frame, and
    /// every later send waits on admission for as long as the test runs.
    struct Stalling {
        gate: velo_ext::AdmissionGate<(bytes::Bytes, bytes::Bytes)>,
        _rx: flume::Receiver<(bytes::Bytes, bytes::Bytes)>,
    }
    fn stalling_address() -> velo_ext::WorkerAddress {
        let entries =
            std::collections::HashMap::from([("stalling".to_string(), b"stalling".to_vec())]);
        velo_ext::WorkerAddress::from_encoded(rmp_serde::to_vec(&entries).unwrap())
    }
    impl velo_ext::Transport for Stalling {
        fn key(&self) -> velo_ext::TransportKey {
            velo_ext::TransportKey::new("stalling")
        }
        fn address(&self) -> velo_ext::WorkerAddress {
            stalling_address()
        }
        fn register(&self, _: velo_ext::PeerInfo) -> Result<(), velo_ext::TransportError> {
            Ok(())
        }
        fn send_message(
            &self,
            _: velo_ext::InstanceId,
            header: bytes::Bytes,
            payload: bytes::Bytes,
            _: velo_ext::MessageType,
            _: Arc<dyn velo_ext::TransportErrorHandler>,
        ) -> velo_ext::SendOutcome {
            self.gate.send((header, payload))
        }
        fn max_message_size(&self, _: velo_ext::InstanceId) -> Option<usize> {
            None
        }
        fn start(
            &self,
            _: velo_ext::InstanceId,
            _: velo_ext::TransportAdapter,
            _: tokio::runtime::Handle,
        ) -> futures::future::BoxFuture<'_, anyhow::Result<()>> {
            Box::pin(async { Ok(()) })
        }
        fn shutdown(&self) {}
        fn check_health(
            &self,
            _: velo_ext::InstanceId,
            _: std::time::Duration,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<(), velo_ext::HealthCheckError>>
                    + Send
                    + '_,
            >,
        > {
            Box::pin(async { Ok(()) })
        }
    }

    /// A Messenger on a [`Stalling`] transport, with one registered peer.
    pub(crate) async fn stalled_messenger() -> (Arc<Messenger>, crate::InstanceId) {
        let (tx, rx) = flume::bounded(1);
        let transport = Arc::new(Stalling {
            gate: velo_ext::AdmissionGate::new(tx, tokio::runtime::Handle::current()),
            _rx: rx,
        });
        let messenger = Messenger::builder()
            .add_transport(transport)
            .build()
            .await
            .unwrap();
        let peer_instance = crate::InstanceId::new_v4();
        messenger
            .register_peer(velo_ext::PeerInfo::new(peer_instance, stalling_address()))
            .unwrap();
        (messenger, peer_instance)
    }

    /// The detach a dropped lease guard sends must not hold the Messenger
    /// either. A guard drops armed when a get fails or is cancelled, which is
    /// most likely when the payload's owner has stopped answering.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stalled_lease_detach_does_not_hold_the_messenger() {
        let (messenger, peer_instance) = stalled_messenger().await;
        let rendezvous = Arc::new(RendezvousManager::new(messenger.instance_id().worker_id()));
        rendezvous
            .register_handlers(Arc::clone(&messenger))
            .unwrap();
        let weak = Arc::downgrade(&messenger);
        let backend = Arc::clone(messenger.backend());
        // The gate holds one frame: the first detach is admitted at once, the
        // second waits on admission for as long as the peer stays stalled.
        for lease in 1..=2 {
            let handle = crate::rendezvous::DataHandle::pack(peer_instance.worker_id(), lease);
            drop(rendezvous.lease_guard(handle, lease));
        }
        drop(messenger);
        let gone = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while weak.upgrade().is_some() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await;
        let still_alive = weak.upgrade().is_some();
        backend.shutdown_now();
        let _ = backend.request_teardown().await;
        assert!(
            gone.is_ok() && !still_alive,
            "a detach parked in admission kept the Messenger alive after its final drop"
        );
    }

    /// No remote rendezvous call may hold the Messenger while it waits on the
    /// owner. The large-payload resolver is an internal task that runs `get`
    /// and then `release`, so an owner that stops answering would otherwise
    /// keep the Messenger, and so its transport teardown, alive forever.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stalled_rendezvous_call_does_not_hold_the_messenger() {
        let (messenger, peer_instance) = stalled_messenger().await;
        let rendezvous = Arc::new(RendezvousManager::new(messenger.instance_id().worker_id()));
        rendezvous
            .register_handlers(Arc::clone(&messenger))
            .unwrap();
        let weak = Arc::downgrade(&messenger);
        let backend = Arc::clone(messenger.backend());
        // The gate holds one frame. Fill it first, so every call below waits
        // on admission, the fire-and-forget ones included.
        messenger
            .am_send_streaming("_fill")
            .unwrap()
            .worker(peer_instance.worker_id())
            .send()
            .await
            .unwrap();
        let handle = crate::rendezvous::DataHandle::pack(peer_instance.worker_id(), 1);
        let calls: Vec<tokio::task::JoinHandle<()>> = vec![
            tokio::spawn({
                let rendezvous = Arc::clone(&rendezvous);
                async move { drop(rendezvous.get(handle).await) }
            }),
            tokio::spawn({
                let rendezvous = Arc::clone(&rendezvous);
                async move { drop(rendezvous.metadata(handle).await) }
            }),
            tokio::spawn({
                let rendezvous = Arc::clone(&rendezvous);
                async move { drop(rendezvous.ref_handle(handle).await) }
            }),
            tokio::spawn({
                let rendezvous = Arc::clone(&rendezvous);
                async move { drop(rendezvous.detach(handle, 1).await) }
            }),
            tokio::spawn({
                let rendezvous = Arc::clone(&rendezvous);
                async move { drop(rendezvous.release(handle, 1).await) }
            }),
        ];
        // Let every call reach its wait before the final drop.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        drop(messenger);
        let gone = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while weak.upgrade().is_some() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await;
        let still_alive = weak.upgrade().is_some();
        for call in calls {
            call.abort();
        }
        backend.shutdown_now();
        let _ = backend.request_teardown().await;
        assert!(
            gone.is_ok() && !still_alive,
            "a rendezvous call waiting on a stalled owner kept the Messenger alive after its final drop"
        );
    }

    /// A typed handler that cannot decode its input still owes the caller an
    /// error reply, and that reply can wait on admission to a peer that stopped
    /// reading. The handler task must not hold the Messenger meanwhile.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stalled_decode_error_reply_does_not_hold_the_messenger() {
        let (messenger, peer_instance) = stalled_messenger().await;
        let weak = Arc::downgrade(&messenger);
        let backend = Arc::clone(messenger.backend());
        let handler =
            crate::messenger::Handler::typed_unary("typed", |_: TypedContext<u32>| Ok(0u32))
                .build();
        // The gate holds one frame: the first error reply is admitted at once,
        // the second waits on admission for as long as the peer stays stalled.
        for slot in 1..=2u128 {
            handler.dispatcher.dispatch(
                crate::messenger::server::InboundCall {
                    message_id: crate::messenger::common::responses::ResponseId::from_u128(
                        u128::from(peer_instance.worker_id().as_u64()) | (slot << 64),
                    ),
                    payload: bytes::Bytes::from_static(b"not json"),
                    response_type: crate::messenger::common::messages::ResponseType::Unary,
                    headers: None,
                    in_flight: None,
                },
                &messenger,
            );
        }
        drop(messenger);
        let gone = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while weak.upgrade().is_some() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await;
        let still_alive = weak.upgrade().is_some();
        backend.shutdown_now();
        let _ = backend.request_teardown().await;
        assert!(
            gone.is_ok() && !still_alive,
            "a decode-error reply parked in admission kept the Messenger alive after its final drop"
        );
    }

    /// An ordered lane runs one message at a time, so while one reply waits
    /// on admission to a peer that stopped reading, the messages behind it
    /// wait in the lane queue. Those queued messages must not hold the
    /// Messenger, or its final drop, and so its teardown, never runs.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stalled_ordered_lane_does_not_hold_the_messenger() {
        let (messenger, peer_instance) = stalled_messenger().await;
        let weak = Arc::downgrade(&messenger);
        let backend = Arc::clone(messenger.backend());
        let handler =
            crate::messenger::Handler::typed_unary("ordered", |_: TypedContext<u32>| Ok(0u32))
                .ordered()
                .build();
        // The gate holds one frame: the first reply is admitted at once, the
        // second waits on admission, and the third waits in the lane queue.
        for slot in 1..=3u128 {
            handler.dispatcher.dispatch(
                crate::messenger::server::InboundCall {
                    message_id: crate::messenger::common::responses::ResponseId::from_u128(
                        u128::from(peer_instance.worker_id().as_u64()) | (slot << 64),
                    ),
                    payload: bytes::Bytes::from_static(b"1"),
                    response_type: crate::messenger::common::messages::ResponseType::Unary,
                    headers: None,
                    in_flight: None,
                },
                &messenger,
            );
        }
        drop(messenger);
        let gone = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while weak.upgrade().is_some() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await;
        let still_alive = weak.upgrade().is_some();
        backend.shutdown_now();
        let _ = backend.request_teardown().await;
        assert!(
            gone.is_ok() && !still_alive,
            "a message queued on an ordered lane kept the Messenger alive after its final drop"
        );
    }

    /// The event handlers send completions to the peer that asked, inline in
    /// the handler. A handler body that keeps its context keeps the Messenger
    /// across that send, which can wait on a peer that stopped reading. The
    /// handler bodies pass only `ctx.payload` into their `async move` block,
    /// and the block captures only that field, so the context, and its
    /// Messenger, drops before the send. Touching `ctx` itself inside the
    /// block would capture all of it; this test fails if that happens.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stalled_event_completion_does_not_hold_the_messenger() {
        let (messenger, peer_instance) = stalled_messenger().await;
        let weak = Arc::downgrade(&messenger);
        let backend = Arc::clone(messenger.backend());
        let handlers = std::sync::Mutex::new(Vec::new());
        crate::messenger::events::handlers::register_event_handlers(
            |handler| {
                handlers.lock().unwrap().push(handler);
                Ok(())
            },
            Arc::clone(messenger.events()),
        )
        .unwrap();
        let handlers = handlers.into_inner().unwrap();
        let subscribe = handlers
            .iter()
            .find(|handler| handler.name() == "_event_subscribe")
            .unwrap();
        // The gate holds one frame: the first completion is admitted at once,
        // the second waits on admission for as long as the peer stays stalled.
        for slot in 1..=2u128 {
            let event = messenger.event_manager().new_event().unwrap();
            let handle = event.handle();
            event.trigger().unwrap();
            let payload =
                serde_json::to_vec(&crate::messenger::events::messages::EventSubscribeMessage {
                    handle: handle.raw(),
                    subscriber_worker: peer_instance.worker_id().as_u64(),
                    subscriber_instance: peer_instance,
                })
                .unwrap();
            subscribe.dispatcher.dispatch(
                crate::messenger::server::InboundCall {
                    message_id: crate::messenger::common::responses::ResponseId::from_u128(
                        u128::from(peer_instance.worker_id().as_u64()) | (slot << 64),
                    ),
                    payload: bytes::Bytes::from(payload),
                    response_type: crate::messenger::common::messages::ResponseType::FireAndForget,
                    headers: None,
                    in_flight: None,
                },
                &messenger,
            );
        }
        drop(handlers);
        drop(messenger);
        let gone = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while weak.upgrade().is_some() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await;
        let still_alive = weak.upgrade().is_some();
        backend.shutdown_now();
        let _ = backend.request_teardown().await;
        assert!(
            gone.is_ok() && !still_alive,
            "an event completion parked in admission kept the Messenger alive after its final drop"
        );
    }

    /// A best-effort `_stream_cancel` to a peer that never admits a frame
    /// must not keep the Messenger alive after its final drop.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stalled_stream_cancel_does_not_hold_the_messenger() {
        let (messenger, peer_instance) = stalled_messenger().await;
        let registry = Arc::new(crate::streaming::control::SenderRegistry::default());
        let weak = Arc::downgrade(&messenger);
        let backend = Arc::clone(messenger.backend());
        // The gate holds one frame: the first notice is admitted at once, the
        // second parks in admission for as long as the peer stays stalled.
        for id in 1..=2 {
            crate::streaming::control::request_sender_cancel(
                crate::streaming::control::StreamCancelHandle::pack(peer_instance.worker_id(), id),
                messenger.instance_id().worker_id(),
                &registry,
                Some(&messenger),
            );
        }
        drop(messenger);
        let gone = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while weak.upgrade().is_some() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await;
        let still_alive = weak.upgrade().is_some();
        // Force the teardown so the test exits cleanly either way.
        backend.shutdown_now();
        let _ = backend.request_teardown().await;
        assert!(
            gone.is_ok() && !still_alive,
            "a notice parked in admission to a stalled peer kept the Messenger alive after its final drop"
        );
    }
}
