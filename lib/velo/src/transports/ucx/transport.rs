// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The UCX messenger [`Transport`].
//!
//! Connection model: there is no listener. `address()` publishes the local
//! `ucp_worker`'s packed address (plus negotiation fields) through discovery;
//! `register()` validates and stores the peer's blob; the endpoint itself is
//! created lazily on the progress thread at first send (`ucp_ep_create` from
//! the blob — UCX's address-based wireup needs no in-band handshake from us).
//!
//! Send path: `send_message` performs the pre-wire checks, then admits the
//! frame through a per-peer [`AdmissionGate`] into the progress thread's ring
//! — the gate provides the per-target ordering the trait contract requires,
//! and the single FIFO ring behind all gates cannot reorder what the gates
//! admitted. Frames are posted as UCX Active Messages with
//! `UCP_AM_SEND_FLAG_EAGER` pinned (velo caps AM size and routes bulk through
//! `velo::rendezvous` instead of UCX's rendezvous).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use futures::future::BoxFuture;
use tracing::{debug, info, warn};

use crate::transports::transport::{
    AdmissionError, AdmissionGate, HealthCheckError, SendOutcome, ShutdownState, TransportError,
    TransportErrorHandler,
};
use velo_ext::{
    InstanceId, MessageType, PeerInfo, Transport, TransportAdapter, TransportKey, WorkerAddress,
};

use super::address::{AM_ID_BASE, BLOB_VERSION, UcxEndpoint};
use super::rma::{RdmaEndpoint, RmaState};
use super::worker::{Cmd, Doorbell, SendTask, StartupSlot, WorkerArgs, WorkerShared, worker_main};

/// Hard ceiling on `eager_max`, matching the TCP codec's frame cap.
const MAX_EAGER: u32 = 16 * 1024 * 1024;

/// Tuning for the UCX transport.
#[derive(Debug, Clone)]
pub struct UcxConfig {
    /// Largest `header + payload` this side accepts in one AM send, bytes.
    /// The effective per-peer limit is `min(local, remote)`. Default 1 MiB;
    /// payloads above the messenger's large-payload threshold ride
    /// `velo::rendezvous` rather than the AM path.
    pub eager_max: u32,
    /// How long the progress thread keeps spinning after its last activity
    /// before arming the wakeup fd and parking. Parked wakeups cost ~1-3 µs
    /// (`ucp_worker_signal`) plus platform idle-exit latency; the spin window
    /// makes loaded submission a plain ring push. Default 20 µs.
    pub spin_us: u64,
    /// Capacity of the command ring between senders and the progress thread.
    /// The per-peer admission gates queue (and preserve order) beyond it.
    pub channel_capacity: usize,
    /// Override for `UCX_TLS` (e.g. `"rc_verbs,ud_verbs,self"` or `"tcp"`),
    /// applied only when the environment does not already set it. Note an
    /// RC-only list cannot wire up — UCX needs a `ud`-class transport alongside
    /// RC for wireup/keepalive.
    ///
    /// **Do not reach for `"rc_mlx5,ud_mlx5"` against the vendored UCX today.**
    /// The mlx5 transports register but query zero devices, because
    /// `crates/ucx-rs/build.rs` links `uct_ib_mlx5` ahead of `uct_ib` and the
    /// verbs memory domain wins the open. UCX answers a setting naming only
    /// unavailable transports by falling back — silently, as far as a caller
    /// can tell — so the symptom is a deployment that believes it is on RDMA
    /// and is not (measured 2026-08-29,
    /// `agent-docs/2026-08-29-rdma-phase3-hardware-checkpoint.md` §2).
    /// `UCX_IB_MLX5_DEVX=y` in the environment forces the DEVX domain open and
    /// is a working workaround until the link order is fixed.
    pub tls: Option<String>,
    /// Override for `UCX_NET_DEVICES` (e.g. `"mlx5_0:1"`), applied only when
    /// the environment does not already set it.
    pub net_devices: Option<String>,
    /// Close an endpoint nothing has used for this long. `None` (the default)
    /// never closes one.
    ///
    /// See [`UcxTransportBuilder::ep_idle_timeout`] for what "used" counts as
    /// and what the next use pays.
    pub ep_idle_timeout: Option<Duration>,
    /// Wire an endpoint up at [`Transport::register`] instead of at first use.
    ///
    /// See [`UcxTransportBuilder::eager_endpoints`]. Default `false`.
    pub eager_endpoints: bool,
}

/// Floor on [`UcxConfig::ep_idle_timeout`].
///
/// Sized to **dominate endpoint wireup**, which is the one thing a short timeout
/// actually breaks. `last_used` records when an operation was *admitted*, not
/// when it completed, so a timeout shorter than the time a send takes on the
/// wire lets the reaper close an endpoint out from under a send that is still
/// establishing itself — the frame then fails through `on_error` instead of
/// arriving. Measured wireup is ~14 ms on CX-7 InfiniBand and upwards of 10 ms
/// over the tcp lane in CI, so half a second is roughly thirty-five times the
/// observed cost and leaves the hazard requiring a send an order of magnitude
/// slower than anything measured.
///
/// It is a builder-level ergonomic guard, not an invariant of the reaper: a test
/// constructing a [`UcxConfig`] directly can go below it deliberately.
const MIN_EP_IDLE_TIMEOUT: Duration = Duration::from_millis(500);

impl Default for UcxConfig {
    fn default() -> Self {
        Self {
            eager_max: 1 << 20,
            spin_us: 20,
            channel_capacity: 1024,
            tls: None,
            net_devices: None,
            ep_idle_timeout: None,
            eager_endpoints: false,
        }
    }
}

/// Per-peer connection state: the admission gate in front of the shared ring.
///
/// One gate per peer preserves per-target FIFO admission; all gates feed the
/// same ring, whose single consumer (the progress thread) preserves what the
/// gates admitted. An epoch is retired when the peer's endpoint fails.
#[derive(Clone)]
struct ConnHandle {
    gate: AdmissionGate<Cmd>,
}

/// UCX messenger transport. See the module docs for the model.
pub struct UcxTransport {
    key: TransportKey,
    config: UcxConfig,
    incarnation: u64,

    ring_tx: flume::Sender<Cmd>,
    ring_rx: Mutex<Option<flume::Receiver<Cmd>>>,
    shared: Arc<WorkerShared>,

    /// Populated at `start()`; `address()` before start returns an empty map.
    local_address: OnceLock<WorkerAddress>,
    startup: StartupSlot,

    connections: DashMap<InstanceId, ConnHandle>,
    runtime: OnceLock<tokio::runtime::Handle>,
    shutdown_state: OnceLock<ShutdownState>,
    join: Mutex<Option<std::thread::JoinHandle<()>>>,
    ping_token: AtomicU64,
    metrics: OnceLock<Arc<dyn velo_ext::TransportObservability>>,
    /// Submit-side RMA bookkeeping, shared with every [`RdmaEndpoint`] handed
    /// out by [`UcxTransport::rdma_endpoint`].
    rma: Arc<RmaState>,
}

impl UcxTransport {
    fn new(key: TransportKey, config: UcxConfig) -> Self {
        let (ring_tx, ring_rx) = flume::bounded(config.channel_capacity);
        let shared = Arc::new(WorkerShared {
            ring_tx: ring_tx.clone(),
            doorbell: Arc::new(Doorbell::new()),
            peers: Arc::new(DashMap::new()),
            pending_pings: Arc::new(DashMap::new()),
            failed_peers: Arc::new(DashMap::new()),
            inflight_ops: Arc::new(Default::default()),
            shutdown_requested: Arc::new(Default::default()),
            reg_epoch: Arc::new(Default::default()),
            live_regions: Arc::new(Default::default()),
            live_rkeys: Arc::new(Default::default()),
            eps_open: Arc::new(Default::default()),
            eps_closed_idle: Arc::new(Default::default()),
            eps_stamped_inbound: Arc::new(Default::default()),
            eps_inbound_unmatched: Arc::new(Default::default()),
            reply_eps: Arc::new(super::worker::ReplyEpSightings::new()),
        });
        Self {
            key,
            config,
            incarnation: uuid::Uuid::new_v4().as_u128() as u64,
            ring_tx,
            ring_rx: Mutex::new(Some(ring_rx)),
            shared,
            local_address: OnceLock::new(),
            startup: OnceLock::new(),
            connections: DashMap::new(),
            runtime: OnceLock::new(),
            shutdown_state: OnceLock::new(),
            join: Mutex::new(None),
            ping_token: AtomicU64::new(1),
            metrics: OnceLock::new(),
            rma: Arc::new(RmaState::new()),
        }
    }

    /// Handle for the RMA operations this transport's progress thread can
    /// perform (`velo::rendezvous`'s GET fast path consumes it).
    ///
    /// Valid only after [`Transport::start`] has resolved; before that every
    /// method on it answers `RmaError::NotStarted`.
    // Consumed by `rendezvous::rdma::UcxBackend`, which the builder wires up
    // after `start()` has resolved.
    pub(crate) fn rdma_endpoint(&self) -> RdmaEndpoint {
        RdmaEndpoint::new(Arc::clone(&self.shared), Arc::clone(&self.rma))
    }

    /// Regions the progress thread currently holds mapped.
    ///
    /// The authoritative count, maintained by the progress thread itself, and
    /// the one thing that can prove a registration was really released rather
    /// than merely forgotten by a bookkeeping layer above. Phase 2 asserts on
    /// it from `rendezvous::rdma`, which cannot reach `shared` directly.
    ///
    /// A test and diagnostics accessor: nothing on a production path reads it,
    /// which is why it carries an explicit allow rather than being deleted.
    #[allow(dead_code)]
    pub(crate) fn live_regions(&self) -> usize {
        self.shared.live_regions.load(Ordering::SeqCst)
    }

    /// Unpacked remote keys the progress thread has not destroyed yet. Same
    /// discipline as [`live_regions`](Self::live_regions); signed, because a
    /// negative value would mean a double destroy and is worth seeing.
    #[allow(dead_code)]
    pub(crate) fn live_rkeys(&self) -> i64 {
        self.shared.live_rkeys.load(Ordering::SeqCst)
    }

    /// The effective `header + payload` cap for `peer`:
    /// `min(local eager_max, peer's advertised eager_max)`.
    fn eager_limit(&self, peer: InstanceId) -> Option<usize> {
        self.shared
            .peers
            .get(&peer)
            .map(|e| self.config.eager_max.min(e.value().eager_max) as usize)
    }

    fn get_or_create_connection(&self, peer: InstanceId) -> Result<ConnHandle, TransportError> {
        if let Some(handle) = self.connections.get(&peer) {
            return Ok(handle.clone());
        }
        let rt = self.runtime.get().ok_or(TransportError::NotStarted)?;
        if !self.shared.peers.contains_key(&peer) {
            return Err(TransportError::PeerNotRegistered(peer));
        }
        let handle = self
            .connections
            .entry(peer)
            .or_insert_with(|| ConnHandle {
                gate: AdmissionGate::new(self.ring_tx.clone(), rt.clone()),
            })
            .clone();
        if let Some(m) = self.metrics.get() {
            m.set_active_connections(self.connections.len());
        }
        Ok(handle)
    }

    fn admit(&self, handle: &ConnHandle, task: SendTask) -> SendOutcome {
        match handle.gate.send(Cmd::Send(task)) {
            SendOutcome::Admitted => {
                // The frame is on the ring: wake the progress thread now.
                self.shared.doorbell.ring();
                SendOutcome::Admitted
            }
            SendOutcome::Pending(admission) => {
                if let Some(m) = self.metrics.get() {
                    m.record_send_backpressure();
                }
                // The ring push happens later, from the gate's driver task —
                // ring the doorbell when it actually lands, so a queued frame
                // does not wait out the park timeout. The park's bounded
                // timeout remains the backstop.
                let doorbell = Arc::clone(&self.shared.doorbell);
                SendOutcome::Pending(admission.on_resolved(move |result| {
                    if result.is_ok() {
                        doorbell.ring();
                    }
                }))
            }
        }
    }

    /// Retire a peer's epoch after its endpoint failed: queued frames belong
    /// to a connection that no longer exists.
    ///
    /// Edge-triggered: the `failed_peers` entry is *claimed* (removed) so
    /// exactly one caller retires the epoch. A level-triggered check would
    /// let every subsequent send re-reap the freshly created replacement gate
    /// and fail its queued frames.
    fn reap_failed_connection(&self, peer: InstanceId) {
        if self.shared.failed_peers.remove(&peer).is_some()
            && let Some((_, stale)) = self.connections.remove(&peer)
        {
            stale.gate.fail_all(AdmissionError::ConnectionReplaced);
            if let Some(m) = self.metrics.get() {
                m.set_active_connections(self.connections.len());
            }
        }
    }
}

impl Transport for UcxTransport {
    fn key(&self) -> TransportKey {
        self.key.clone()
    }

    fn address(&self) -> WorkerAddress {
        self.local_address.get().cloned().unwrap_or_else(|| {
            // Before start() there is no worker address yet. An empty map
            // is unregisterable by peers, which is the honest signal.
            crate::transports::address::WorkerAddressBuilder::new()
                .build()
                .expect("empty WorkerAddress")
        })
    }

    fn register(&self, peer_info: PeerInfo) -> Result<(), TransportError> {
        let entry = peer_info
            .worker_address()
            .get_entry(&self.key)
            .map_err(|_| TransportError::NoEndpoint)?
            .ok_or(TransportError::NoEndpoint)?;
        let endpoint = UcxEndpoint::decode(&entry).map_err(|e| {
            debug!("ucx: rejecting peer blob: {e}");
            TransportError::InvalidEndpoint
        })?;
        let peer = peer_info.instance_id();
        // A fresh registration supersedes any failed epoch: the new blob may
        // be a restarted incarnation.
        self.shared.failed_peers.remove(&peer);
        self.shared.peers.insert(peer, endpoint);
        // Tell the progress thread to revalidate cached endpoints: a
        // re-registration may carry a new incarnation of the same instance.
        self.shared.reg_epoch.fetch_add(1, Ordering::AcqRel);
        // Eager wireup (opt-in). Pushed *after* the peers map is populated —
        // `ensure_ep` reads the blob from there, so the reverse order would
        // wire up nothing. `try_send` rather than an await because `register` is
        // synchronous: a full ring drops the hint and the peer is wired up at
        // first use, which is the behaviour with the knob off.
        if self.config.eager_endpoints && self.ring_tx.try_send(Cmd::EnsureEp { peer }).is_err() {
            debug!("ucx: eager wireup for {peer} skipped (ring full or closed)");
        }
        self.shared.doorbell.ring();
        if let Some(m) = self.metrics.get() {
            m.set_registered_peers(self.shared.peers.len());
        }
        debug!("ucx: registered peer {peer}");
        Ok(())
    }

    fn send_message(
        &self,
        instance_id: InstanceId,
        header: Bytes,
        payload: Bytes,
        message_type: MessageType,
        on_error: Arc<dyn TransportErrorHandler>,
    ) -> SendOutcome {
        let task = SendTask {
            peer: instance_id,
            msg_type: message_type,
            header,
            payload,
            on_error,
        };

        // Pre-wire failures report through on_error and return Admitted:
        // there is nothing for the caller to wait on (trait contract).
        if self.runtime.get().is_none() {
            task.on_error.on_error(
                task.header,
                task.payload,
                "ucx transport not started".into(),
            );
            return SendOutcome::Admitted;
        }
        if let Some(out) = self.startup.get()
            && task.header.len() > out.max_am_header
        {
            let why = format!(
                "header {} bytes exceeds ucx max_am_header {}",
                task.header.len(),
                out.max_am_header
            );
            task.on_error.on_error(task.header, task.payload, why);
            return SendOutcome::Admitted;
        }
        match self.eager_limit(instance_id) {
            Some(limit) if task.header.len() + task.payload.len() > limit => {
                let why = format!(
                    "frame {} bytes exceeds negotiated ucx eager limit {limit}",
                    task.header.len() + task.payload.len()
                );
                task.on_error.on_error(task.header, task.payload, why);
                return SendOutcome::Admitted;
            }
            Some(_) => {}
            None => {
                task.on_error.on_error(
                    task.header,
                    task.payload,
                    format!("peer not registered: {instance_id}"),
                );
                return SendOutcome::Admitted;
            }
        }

        self.reap_failed_connection(instance_id);
        match self.get_or_create_connection(instance_id) {
            Ok(handle) => self.admit(&handle, task),
            Err(e) => {
                task.fail(format!("ucx connection unavailable: {e}"));
                SendOutcome::Admitted
            }
        }
    }

    fn max_message_size(&self, target: InstanceId) -> Option<usize> {
        self.eager_limit(target)
    }

    fn start(
        &self,
        _instance_id: InstanceId,
        channels: TransportAdapter,
        rt: tokio::runtime::Handle,
    ) -> BoxFuture<'_, Result<()>> {
        self.runtime.set(rt).ok();
        self.shutdown_state
            .set(channels.shutdown_state.clone())
            .ok();

        let ring_rx = self
            .ring_rx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();

        Box::pin(async move {
            let ring_rx =
                ring_rx.ok_or_else(|| anyhow::anyhow!("ucx transport already started"))?;
            let (startup_tx, startup_rx) = tokio::sync::oneshot::channel();
            let args = WorkerArgs {
                config: self.config.clone(),
                ring_rx,
                shared: Arc::clone(&self.shared),
                adapter: channels,
                startup: startup_tx,
            };
            let join = std::thread::Builder::new()
                .name("velo-ucx-progress".into())
                .spawn(move || worker_main(args))?;
            *self.join.lock().unwrap_or_else(|e| e.into_inner()) = Some(join);

            let out = startup_rx
                .await
                .map_err(|_| anyhow::anyhow!("ucx progress thread died during startup"))??;

            let blob = UcxEndpoint {
                v: BLOB_VERSION,
                am_id_base: AM_ID_BASE,
                eager_max: self.config.eager_max.min(MAX_EAGER),
                incarnation: self.incarnation,
                worker_addr: out.worker_addr.clone(),
            }
            .encode()?;
            let mut builder = crate::transports::address::WorkerAddressBuilder::new();
            builder.add_entry(self.key.clone(), blob)?;
            let address = builder.build()?;

            info!(
                "UCX transport started (worker address {} B, max_am_header {} B)",
                out.worker_addr.len(),
                out.max_am_header
            );
            self.startup.set(out).ok();
            self.local_address.set(address).ok();
            // Only now is the ring being consumed: before this, an RMA command
            // would push successfully and never be answered. The runtime handle
            // rides along so a cancelled `map_region` can retry its rollback push
            // rather than drop it (see `MapRollback`).
            self.rma.mark_started(self.runtime.get().cloned());
            Ok(())
        })
    }

    fn begin_drain(&self) {
        // Per-frame gating happens in the AM recv trampoline via the shared
        // ShutdownState — no-op here, mirroring TCP.
    }

    fn shutdown(&self) {
        info!("Shutting down UCX transport");
        if let Some(state) = self.shutdown_state.get() {
            state.teardown_token().cancel();
        }
        // Ask the progress thread to exit. The flag is the reliable signal
        // (a full ring can drop the command, and `Disconnected` is
        // unreachable while the worker holds its own senders); the command
        // and the forced doorbell just make exit prompt.
        self.shared
            .shutdown_requested
            .store(true, std::sync::atomic::Ordering::Release);
        let _ = self.ring_tx.try_send(Cmd::Shutdown);
        self.shared.doorbell.ring_force();
        if let Some(join) = self.join.lock().unwrap_or_else(|e| e.into_inner()).take()
            && join.join().is_err()
        {
            warn!("ucx progress thread panicked during shutdown");
        }
        for entry in self.connections.iter() {
            entry.value().gate.fail_all(AdmissionError::ChannelClosed);
        }
        self.connections.clear();
        if let Some(m) = self.metrics.get() {
            m.set_active_connections(0);
        }
    }

    fn set_observability(&self, observability: Arc<dyn velo_ext::TransportObservability>) {
        let _ = self.metrics.set(observability);
        if let Some(m) = self.metrics.get() {
            m.set_registered_peers(self.shared.peers.len());
            m.set_active_connections(self.connections.len());
        }
    }

    fn check_health(
        &self,
        instance_id: InstanceId,
        timeout: Duration,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), HealthCheckError>> + Send + '_>,
    > {
        Box::pin(async move {
            if self.runtime.get().is_none() {
                return Err(HealthCheckError::TransportNotStarted);
            }
            if !self.shared.peers.contains_key(&instance_id) {
                return Err(HealthCheckError::PeerNotRegistered);
            }
            let connection_existed = self.connections.contains_key(&instance_id)
                && !self.shared.failed_peers.contains_key(&instance_id);

            let token = self.ping_token.fetch_add(1, Ordering::Relaxed);
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.shared.pending_pings.insert(token, tx);
            // Removes the entry on every exit — including this future being
            // cancelled mid-await, which would otherwise leak it forever.
            struct PingGuard {
                map: Arc<DashMap<u64, tokio::sync::oneshot::Sender<()>>>,
                token: u64,
            }
            impl Drop for PingGuard {
                fn drop(&mut self) {
                    self.map.remove(&self.token);
                }
            }
            let _guard = PingGuard {
                map: Arc::clone(&self.shared.pending_pings),
                token,
            };

            // A momentarily full ring is backpressure, not peer failure: wait
            // for admission inside the caller's deadline. A closed ring means
            // the worker is gone.
            let probe = async {
                self.ring_tx
                    .send_async(Cmd::Ping {
                        peer: instance_id,
                        token,
                    })
                    .await
                    .map_err(|_| HealthCheckError::ConnectionFailed)?;
                self.shared.doorbell.ring();
                rx.await.map_err(|_| HealthCheckError::ConnectionFailed)
            };

            match tokio::time::timeout(timeout, probe).await {
                Ok(Ok(())) => {
                    // Mirrors TCP: a peer that answers but had no established
                    // connection reports NeverConnected on the first probe.
                    if connection_existed {
                        Ok(())
                    } else {
                        Err(HealthCheckError::NeverConnected)
                    }
                }
                Ok(Err(e)) => Err(e),
                Err(_) => {
                    if self.shared.failed_peers.contains_key(&instance_id) {
                        Err(HealthCheckError::ConnectionFailed)
                    } else {
                        Err(HealthCheckError::Timeout)
                    }
                }
            }
        })
    }
}

/// Builder for [`UcxTransport`].
pub struct UcxTransportBuilder {
    key: Option<TransportKey>,
    config: UcxConfig,
}

impl UcxTransportBuilder {
    /// Create a builder with the default [`UcxConfig`].
    pub fn new() -> Self {
        Self {
            key: None,
            config: UcxConfig::default(),
        }
    }

    /// Override the transport key (default `"ucx"`).
    pub fn key(mut self, key: TransportKey) -> Self {
        self.key = Some(key);
        self
    }

    /// Largest `header + payload` accepted in one AM send (capped at 16 MiB).
    pub fn eager_max(mut self, bytes: u32) -> Self {
        self.config.eager_max = bytes.min(MAX_EAGER);
        self
    }

    /// Progress-thread spin window before parking, in microseconds.
    pub fn spin_us(mut self, us: u64) -> Self {
        self.config.spin_us = us;
        self
    }

    /// Command-ring capacity (default 1024).
    pub fn channel_capacity(mut self, capacity: usize) -> Self {
        self.config.channel_capacity = capacity.max(1);
        self
    }

    /// Set `UCX_TLS` for this transport (env wins if already set).
    pub fn tls(mut self, tls: impl Into<String>) -> Self {
        self.config.tls = Some(tls.into());
        self
    }

    /// Set `UCX_NET_DEVICES` for this transport (env wins if already set).
    pub fn net_devices(mut self, devices: impl Into<String>) -> Self {
        self.config.net_devices = Some(devices.into());
        self
    }

    /// Close endpoints idle for longer than `timeout`. `None` — the default —
    /// keeps every endpoint until shutdown.
    ///
    /// # What it costs
    ///
    /// The first operation to a peer pays UCX's lazy endpoint wireup, measured
    /// at roughly **14 ms** on a CX-7 InfiniBand fabric against a warm RDMA read
    /// of 108–229 µs. Closing an idle endpoint hands that bill to whoever uses
    /// the peer next, which is why this is off by default and why the plan's
    /// sign-off (D9) left it that way: an idle endpoint costs NIC resources, and
    /// a reconnect costs two orders of magnitude more than the operation that
    /// triggers it. Turn it on when a process talks to far more peers over its
    /// lifetime than it talks to at any one time.
    ///
    /// # What counts as use — both directions
    ///
    /// Anything this side initiates: a frame send, an RDMA GET, an eager
    /// wireup. **And anything the peer sends us** — an inbound frame refreshes
    /// the endpoint it arrived on, so "idle" means idle in both directions and a
    /// peer that only ever sends to us does not have its endpoint reaped under
    /// its own traffic. `a_peer_that_keeps_sending_keeps_its_endpoint` pins that.
    ///
    /// # Health probes are not free here
    ///
    /// A `check_health` probe both counts as use *and* **creates** an endpoint
    /// if none exists. For a peer this instance otherwise talks to, that is
    /// harmless: the probe simply keeps a live endpoint warm, and a probe
    /// interval shorter than this timeout means nothing is ever reaped.
    ///
    /// For a peer this instance *only* probes, it is a create/reap/disrupt
    /// generator: each probe wires an endpoint up, the reaper closes it one
    /// timeout later, and every close costs that peer a frame (see below).
    /// Probing on an interval longer than this timeout therefore manufactures
    /// exactly the disruption this knob is trying to be worth. There is no
    /// periodic prober in-tree — `check_health` has no in-tree periodic caller —
    /// so this only applies to a caller that has built one; if you have, either
    /// probe faster than the timeout or do not enable this.
    ///
    /// # What it promises
    ///
    /// An endpoint is closed between one and one and a half timeouts after its
    /// last use in either direction (the scan runs at half the timeout, capped
    /// at one a second), and never while an RDMA operation to that peer is
    /// outstanding. The next use re-establishes it transparently — no error
    /// surfaces, nothing has to be re-registered.
    ///
    /// What it does **not** promise is that an Active Message send admitted just
    /// before the timeout expired has landed. `last_used` records admission, not
    /// completion, so a send still on the wire when its endpoint is reaped fails
    /// through its `TransportErrorHandler` with the original buffers — the same
    /// contract a peer-failure reap has always had. The floor below **shrinks**
    /// that window rather than closing it, and two residuals survive: a send
    /// slower than the floor under congestion is still killable, and the
    /// admission stamp is taken from a clock sampled at the top of the progress
    /// loop's pass, so the effective budget is the timeout minus however long
    /// that pass runs.
    ///
    /// Values below half a second are raised to it; see the transport's
    /// `MIN_EP_IDLE_TIMEOUT` for why that is the number.
    ///
    /// # What it costs the peer — read this before enabling
    ///
    /// Closing an endpoint is not a local act. UCX pairs endpoints by remote
    /// worker: velo's REPLY-flagged Active Messages cause UCX to create a
    /// matching endpoint on the peer, and the peer's own `ucp_ep_create` back to
    /// this instance is then *matched onto that same connection* instead of
    /// building a fresh one. Closing this side leaves the peer holding an
    /// endpoint over a connection that no longer exists. Measured over the tcp
    /// lane, with both close modes:
    ///
    /// 1. The peer's next frame to us is admitted and **silently lost** — no
    ///    error at its end, no arrival at ours. Retrying does not help, and
    ///    neither does this side establishing a fresh endpoint of its own.
    /// 2. UCX keepalive (default interval ~20 s) eventually declares the peer's
    ///    endpoint failed and fires its error handler.
    /// 3. The frame after that takes velo's existing failed-connection path onto
    ///    a fresh endpoint and arrives normally.
    ///
    /// So it self-heals, at a cost of one lost frame and up to a keepalive
    /// interval of disruption *per reaped endpoint*. That is what makes the
    /// bidirectional freshness stamp above load-bearing rather than a nicety: a
    /// peer that keeps sending is never reaped, so it never pays this.
    ///
    /// Which leaves the patterns where it is still paid, and they are the ones
    /// to check before enabling:
    ///
    /// * **Genuinely symmetric-idle peers** — neither side has spoken for a
    ///   timeout. Reaping costs whoever speaks first one frame. This is the case
    ///   the knob is for.
    /// * **Send-side-only fan-out** — this instance sends to many peers it never
    ///   hears from. Reaping costs nothing, since the disruption is to the
    ///   *peer's* path back and no peer is using one.
    /// * **Probe-only peers** — see the health-probe section above. Avoid.
    ///
    /// Note what is *not* on that list: "one-directional" is not by itself a
    /// safe answer, because the receiving side of a one-directional flow is the
    /// worst case — it is the side whose path back gets disrupted. The stamp
    /// makes that case correct now, but a deployment reasoning about the knob
    /// should reason about it per-direction rather than per-link.
    ///
    /// This is why the default is off, and why D9 left connection-pool policy to
    /// be revisited with exactly this measurement in hand.
    /// `reaping_disrupts_the_peers_path_back` pins the behaviour.
    ///
    /// # Expected log noise
    ///
    /// Over the tcp lane UCX logs `tcp_ep … recv(-1) failed: Input/output error`
    /// at ERROR level on the peer's side for each reaped endpoint. That is UCX
    /// reporting a close it did not initiate, not a velo fault.
    pub fn ep_idle_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.config.ep_idle_timeout = timeout.map(|t| {
            if t < MIN_EP_IDLE_TIMEOUT {
                debug!("ucx: ep_idle_timeout {t:?} raised to the {MIN_EP_IDLE_TIMEOUT:?} floor");
                MIN_EP_IDLE_TIMEOUT
            } else {
                t
            }
        });
        self
    }

    /// Establish each peer's endpoint at [`Transport::register`] rather than at
    /// its first use. Default `false`.
    ///
    /// The ~14 ms of lazy UCX wireup (measured on CX-7 InfiniBand; a warm RDMA
    /// read is 108–229 µs) is otherwise paid by whichever operation happens to
    /// go first — typically the first rendezvous GET, where it dwarfs the
    /// transfer it is attached to. Registration is the natural place to spend
    /// it: discovery has just produced the peer, and nothing is waiting.
    ///
    /// The wireup is a fire-and-forget hint. `register()` does not wait for it,
    /// a failure is logged and forgotten, and a peer whose eager wireup was
    /// dropped (a full command ring) is simply wired up at first use as before.
    /// Nothing observable changes except when the cost is paid.
    ///
    /// # Composing with [`ep_idle_timeout`](Self::ep_idle_timeout)
    ///
    /// The two knobs deliberately pull in opposite directions and are meant to
    /// be usable together: eager wireup amortises the connection cost away from
    /// the first transfer, and the reaper reclaims it again if the peer turns
    /// out never to be used. An eagerly established endpoint's idle clock starts
    /// at registration, so with both on, a registered-but-never-used peer is
    /// wired up once and closed one timeout later. That is the intended
    /// behaviour, not a conflict.
    pub fn eager_endpoints(mut self, eager: bool) -> Self {
        self.config.eager_endpoints = eager;
        self
    }

    /// Build the [`UcxTransport`]. UCX initialisation is deferred to
    /// [`Transport::start`]; building never touches UCX.
    pub fn build(self) -> Result<UcxTransport> {
        let key = self.key.unwrap_or_else(|| TransportKey::from("ucx"));
        Ok(UcxTransport::new(key, self.config))
    }
}

impl Default for UcxTransportBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// `#[path]` keeps the tests beside their siblings as `ucx/tests.rs`.
#[cfg(test)]
#[path = "tests.rs"]
mod tests;
