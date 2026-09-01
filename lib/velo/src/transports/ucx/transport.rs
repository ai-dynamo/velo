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
    /// Override for `UCX_TLS` (e.g. `"rc_mlx5,ud_mlx5"` or `"tcp"`), applied
    /// only when the environment does not already set it. Note an RC-only
    /// list cannot wire up — UCX needs a `ud`-class transport alongside RC
    /// for wireup/keepalive.
    pub tls: Option<String>,
    /// Override for `UCX_NET_DEVICES` (e.g. `"mlx5_0:1"`), applied only when
    /// the environment does not already set it.
    pub net_devices: Option<String>,
}

impl Default for UcxConfig {
    fn default() -> Self {
        Self {
            eager_max: 1 << 20,
            spin_us: 20,
            channel_capacity: 1024,
            tls: None,
            net_devices: None,
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
    // Phase 2 is the in-crate caller; today only the module's tests exercise it.
    #[allow(dead_code)]
    pub(crate) fn rdma_endpoint(&self) -> RdmaEndpoint {
        RdmaEndpoint::new(Arc::clone(&self.shared), Arc::clone(&self.rma))
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
