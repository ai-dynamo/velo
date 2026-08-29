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
pub use velo_ext::{
    AdmissionState, InstanceId, PeerInfo, ShutdownPolicy, Transport, WorkerAddress, WorkerId,
};

// Public re-exports for the velo-ext crate.
pub use velo_ext as ext;

// Messenger surface
pub use crate::messenger::{
    Admitted, AmHandlerBuilder, AmSendBuilder, AmSyncBuilder, AsyncExecutor, Context, DispatchMode,
    FireResult, Handler, HandlerExecutor, Messenger, MessengerBuilder, OrderedConfig, OrderingKey,
    OverflowPolicy, PeerDiscovery, SyncExecutor, SyncResult, TypedContext, TypedUnaryBuilder,
    TypedUnaryHandlerBuilder, TypedUnaryResult, UnaryBuilder, UnaryHandlerBuilder, UnaryResult,
    UnifiedResponse, VeloEvents,
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

    /// Install the batched, multiplexed streaming transport
    /// (`messenger-mux-v1`), described in `streaming/BATCHING.md`.
    ///
    /// **Opt-in, and the mux is not the default transport.**
    /// [`MuxConfig::enabled`](crate::streaming::MuxConfig::enabled) defaults to
    /// `false`, and calling this with it left `false` is exactly the same node
    /// as not calling it at all: nothing is registered and nothing is
    /// advertised.
    ///
    /// The legacy transport stays configured either way — a mux-enabled node
    /// registers both, and each attach picks between them from what the peer
    /// advertised. So a canary is one node with the flag on, talking the mux to
    /// other canaries and the legacy path to everything else, and **rollback is
    /// the same flag**: set it back to `false` and the node stops advertising
    /// `messenger-mux-v1`, so the next attach negotiates the legacy path. No
    /// code change, no wire change, and no coordination with peers, because a
    /// key that is never advertised is never selected.
    ///
    /// Only one mux may be installed per instance — its `_stream_batch` handler
    /// is registered on the messenger for its lifetime and the messenger
    /// refuses a duplicate handler name. Calling this twice fails here rather
    /// than at the second attach.
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
    ///    wired in as both the default and the only registry entry (keyed by
    ///    its TransportKey).
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
        let stream_transport: Arc<dyn crate::streaming::FrameTransport> = match resolved {
            StreamConfig::Tcp(tcp_cfg) => {
                let bind_addr = tcp_cfg
                    .map(|c| c.bind_addr)
                    .unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));
                let tcp = crate::streaming::TcpFrameTransport::new(bind_addr).await?;
                if let Some(m) = self.metrics.as_ref() {
                    tcp.set_metrics(Arc::clone(m));
                }
                tcp as _
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
                grpc as _
            }
        };

        // Step 4: Build the streaming-transport registry (single entry keyed
        // by the chosen transport's TransportKey). The AnchorManager passes
        // the response's `streaming_transport_key` through this map to find
        // the FrameTransport on the client side at attach time.
        let mut registry: std::collections::HashMap<
            String,
            Arc<dyn crate::streaming::FrameTransport>,
        > = std::collections::HashMap::new();
        registry.insert(
            stream_transport.key().as_str().to_string(),
            Arc::clone(&stream_transport),
        );

        // Step 5: Build the mux, if it was switched on. It joins the registry
        // *beside* the legacy transport rather than replacing it: negotiation
        // answers `messenger-mux-v1` only to peers that advertised it, and
        // every other peer is still answered — and must still be served — on
        // the legacy key.
        let mux = match self.mux_config.filter(|config| config.enabled) {
            Some(config) => {
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
            }
            None => None,
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
/// be switched off (D6).
///
/// Only `1`, `true`, `yes` and `on` (any case) count. A variable set to
/// anything else — `0`, `false`, an empty string, a typo — leaves the path
/// enabled, because a kill switch that fires on a typo is worse than one that
/// occasionally does not fire on a misspelling: the first silently costs
/// performance in production, the second is visible the moment somebody checks
/// the metric.
#[cfg(all(target_os = "linux", feature = "ucx"))]
fn rdma_rendezvous_disabled_by_env() -> bool {
    rdma_rendezvous_disabled(
        std::env::var("VELO_RDMA_RENDEZVOUS_DISABLE")
            .ok()
            .as_deref(),
    )
}

/// The parsing half of the kill switch, split out so it can be tested.
///
/// The environment is process-global and `cargo test` runs in parallel, so a
/// test that *set* the variable would silently switch the path off for every
/// other test building a `Velo` at that moment. Splitting the decision from the
/// read means the rule can be checked exhaustively without touching the
/// process; the end-to-end effect is covered through
/// [`RdmaRendezvousConfig::enabled`], which is the same field this writes.
#[cfg(all(target_os = "linux", feature = "ucx"))]
fn rdma_rendezvous_disabled(value: Option<&str>) -> bool {
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
    /// while responses, acks, and events keep flowing. See
    /// [`Messenger::begin_drain`].
    pub fn begin_drain(&self) {
        self.messenger.begin_drain();
    }

    /// Perform a graceful 3-phase shutdown of the messenger transports: gate
    /// inbound requests, wait for in-flight handler invocations per `policy`,
    /// then tear down. See [`Messenger::graceful_shutdown`].
    ///
    /// Streaming-plane teardown (anchors, stream transports) is separate and
    /// not covered by this call.
    ///
    /// # RDMA registrations go first, and are declared released last
    ///
    /// When an RDMA registration layer is installed, shutdown becomes four
    /// steps rather than three:
    ///
    /// 1. [`begin_drain`](Self::begin_drain) — idempotent, and repeated by the
    ///    messenger shutdown below. Closing the inbound gate first means no new
    ///    request can ask for an RDMA transfer while registrations are being
    ///    torn down.
    /// 2. The registry sweep (D8 steps 1 to 3): registrations refused,
    ///    in-flight transfers drained, every region and arena unmapped.
    /// 3. Messenger gate, drain and teardown, unchanged.
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
    /// [`ShutdownPolicy::Timeout`] names a bound on *this call*, so the sweep
    /// and the messenger phase share it rather than each taking the full
    /// duration — which would make the worst case twice what was asked for.
    /// Under [`ShutdownPolicy::WaitForever`] the sweep still takes
    /// [`RdmaConfig::shutdown_timeout`], because a peer that crashed
    /// mid-transfer must not wedge shutdown forever even when the caller is
    /// willing to wait on local work.
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

        // Shadowed with what is left of the caller budget after the sweep, so
        // the two phases share one deadline instead of taking one each.
        #[cfg(all(target_os = "linux", feature = "ucx"))]
        let policy = {
            let started = std::time::Instant::now();
            if let Some(rdma) = &self.rdma {
                self.begin_drain();
                // Before the sweep, not after: the reaper force-releases leases
                // and drops the pinned staging under them, and doing that while
                // the sweep is walking regions and arenas would have two tasks
                // taking the same memory apart from opposite ends.
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

        self.messenger.graceful_shutdown(policy).await;

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
    /// one `_stream_batch`. See `streaming/BATCHING.md` § "Flush policy".
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
    /// the next [`AnchorManager::attach_anchor`](crate::streaming::AnchorManager::attach_anchor).
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

    /// The registration layer, for tests that need to observe it directly.
    #[cfg(all(target_os = "linux", feature = "ucx", test))]
    pub(crate) fn rdma(&self) -> Option<&Arc<crate::rendezvous::rdma::RdmaRegistry>> {
        self.rdma.as_ref()
    }

    /// The registration layer, for Phase 3 staging and transfers.
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

    /// The kill switch fires on an affirmative and on nothing else.
    ///
    /// The asymmetry is deliberate and worth pinning down: a switch that fired
    /// on a typo would silently cost performance in production, while one that
    /// misses a misspelling shows up the moment anybody reads
    /// `velo_rendezvous_rdma_path_total`.
    #[cfg(all(target_os = "linux", feature = "ucx"))]
    #[test]
    fn the_rdma_kill_switch_reads_only_affirmatives() {
        for on in [
            "1", "true", "TRUE", "True", "yes", "YES", "on", "ON", " 1 ", "\ttrue\n",
        ] {
            assert!(
                rdma_rendezvous_disabled(Some(on)),
                "{on:?} should switch the rendezvous RDMA path off"
            );
        }
        for off in [
            "0", "false", "no", "off", "", "  ", "2", "disable", "ture", "1 1",
        ] {
            assert!(
                !rdma_rendezvous_disabled(Some(off)),
                "{off:?} must not switch the rendezvous RDMA path off"
            );
        }
        assert!(
            !rdma_rendezvous_disabled(None),
            "an unset variable must leave the path enabled"
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
