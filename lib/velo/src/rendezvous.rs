// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Rendezvous data staging and large payload transfer for velo.
//!
//! The rendezvous primitive allows staging data at one location (the owner),
//! passing a compact [`DataHandle`] to consumers, and pulling data via the handle
//! using a receiver-driven protocol.
//!
//! # Protocol
//!
//! The consumer drives all data transfer:
//! 1. `metadata(handle)` — query size/info without locking
//! 2. `get(handle)` — acquire read lock + pull data (inline or chunked)
//! 3. `detach(handle)` — release read lock, keep handle alive (can get again)
//! 4. `release(handle)` — release read lock + decrement refcount (freed at 0)
//!
//! # Transfer paths
//!
//! - **Chunked pull** (default): Multi-message chunked transfer over active
//!   messages. No NIXL/RDMA dependency.
//! - **NIXL/RDMA** (`nixl` feature, Phase 2): Owner registers the staged
//!   region with libnixl + UCX. The consumer fetches via direct
//!   `NIXL_READ` after a one-shot handshake.

pub mod consumer;
pub mod handle;
pub mod handlers;
#[cfg(feature = "nixl")]
pub(crate) mod nixl_endpoint;
pub mod protocol;
pub mod store;
pub mod transparent;
pub mod write;

pub use handle::DataHandle;
pub use protocol::DataMetadata;
pub use store::{RegisterOptions, StageMode};
pub use transparent::{RendezvousResolver, RendezvousStager};
pub use write::RendezvousWrite;

#[cfg(feature = "nixl")]
pub use nixl_endpoint::NixlEndpoint;

use std::sync::{Arc, OnceLock};
use std::time::Instant;

use crate::observability::{HandlerOutcome, RendezvousOp, VeloMetrics};
use anyhow::Result;
use bytes::Bytes;
use velo_ext::WorkerId;

/// Central manager for rendezvous data staging and retrieval.
///
/// Each Velo worker creates one `RendezvousManager`. It owns the [`DataStore`](store::DataStore)
/// for locally staged data and provides methods for both owner-side (register) and
/// consumer-side (get, release) operations.
pub struct RendezvousManager {
    /// The WorkerId of the worker that owns this manager.
    worker_id: WorkerId,
    /// The data store holding staged slots and active transfers.
    store: Arc<store::DataStore>,
    /// Messenger reference, set once via `register_handlers()`.
    messenger_lock: OnceLock<Arc<crate::messenger::Messenger>>,
    /// Optional Prometheus metrics.
    metrics: Option<Arc<VeloMetrics>>,
    /// Lazily-initialized NIXL endpoint (Phase 2). Set once via [`enable_nixl`].
    ///
    /// Wrapped in `Arc<OnceLock<...>>` so handler closures can hold a clone
    /// and observe `enable_nixl()` calls that happen *after*
    /// [`register_handlers`].
    #[cfg(feature = "nixl")]
    nixl: Arc<OnceLock<Arc<NixlEndpoint>>>,
}

impl RendezvousManager {
    /// Create a new `RendezvousManager` for the given worker.
    pub fn new(worker_id: WorkerId) -> Self {
        Self {
            worker_id,
            store: Arc::new(store::DataStore::new()),
            messenger_lock: OnceLock::new(),
            metrics: None,
            #[cfg(feature = "nixl")]
            nixl: Arc::new(OnceLock::new()),
        }
    }

    /// Create a new `RendezvousManager` with metrics.
    pub fn with_metrics(worker_id: WorkerId, metrics: Arc<VeloMetrics>) -> Self {
        Self {
            worker_id,
            store: Arc::new(store::DataStore::new()),
            messenger_lock: OnceLock::new(),
            metrics: Some(metrics),
            #[cfg(feature = "nixl")]
            nixl: Arc::new(OnceLock::new()),
        }
    }

    /// Register the rendezvous control-plane handlers on the messenger.
    ///
    /// Must be called exactly once. Registers six underscore-prefixed handlers:
    /// `_rv_metadata`, `_rv_acquire`, `_rv_pull`, `_rv_ref`, `_rv_detach`, `_rv_release`.
    pub fn register_handlers(
        self: &Arc<Self>,
        messenger: Arc<crate::messenger::Messenger>,
    ) -> Result<()> {
        use handlers::{
            create_rv_detach_handler, create_rv_metadata_handler, create_rv_pull_handler,
            create_rv_ref_handler, create_rv_release_handler,
        };

        messenger
            .register_streaming_handler(create_rv_metadata_handler(Arc::clone(&self.store)))?;

        // The acquire handler shape depends on the `nixl` feature: with `nixl`
        // we route a shared `Arc<OnceLock<NixlEndpoint>>` so the handler can
        // observe `enable_nixl()` calls that happen *after* this method.
        #[cfg(not(feature = "nixl"))]
        messenger.register_streaming_handler(handlers::create_rv_acquire_handler(Arc::clone(
            &self.store,
        )))?;
        #[cfg(feature = "nixl")]
        messenger.register_streaming_handler(handlers::create_rv_acquire_handler_with_slot(
            Arc::clone(&self.store),
            Arc::clone(&self.nixl),
        ))?;

        messenger.register_streaming_handler(create_rv_pull_handler(Arc::clone(&self.store)))?;
        messenger.register_streaming_handler(create_rv_ref_handler(Arc::clone(&self.store)))?;
        messenger.register_streaming_handler(create_rv_detach_handler(Arc::clone(&self.store)))?;
        messenger.register_streaming_handler(create_rv_release_handler(Arc::clone(&self.store)))?;

        // The handshake handler is always registered when `nixl` is compiled in
        // — it captures the same `Arc<OnceLock<...>>` and reports an error if
        // the endpoint isn't enabled when the AM arrives.
        #[cfg(feature = "nixl")]
        messenger.register_streaming_handler(handlers::create_rv_nixl_handshake_handler(
            Arc::clone(&self.nixl),
        ))?;

        self.messenger_lock
            .set(messenger)
            .map_err(|_| anyhow::anyhow!("register_handlers called twice"))?;

        Ok(())
    }

    /// Get the messenger reference (panics if `register_handlers` not called).
    fn messenger(&self) -> &Arc<crate::messenger::Messenger> {
        self.messenger_lock
            .get()
            .expect("RendezvousManager::register_handlers must be called before use")
    }

    // -----------------------------------------------------------------------
    // Owner-side API
    // -----------------------------------------------------------------------

    /// Stage data at this worker and return a [`DataHandle`].
    ///
    /// The handle encodes this worker's ID and a local slot ID. Pass it to
    /// consumers via any channel (AM, event, typed message field).
    ///
    /// Default refcount is 1.
    pub fn register_data(&self, data: Bytes) -> DataHandle {
        let started = Instant::now();
        let data_len = data.len();
        let local_id = self.store.register(data, None);
        if let Some(m) = &self.metrics {
            m.record_rendezvous_operation(
                RendezvousOp::Register,
                HandlerOutcome::Success,
                started.elapsed(),
            );
            m.record_rendezvous_bytes(RendezvousOp::Register, data_len);
            m.set_rendezvous_active_slots(self.store.slots.len());
        }
        DataHandle::pack(self.worker_id, local_id)
    }

    /// Stage data with options (TTL, etc.) and return a [`DataHandle`].
    pub fn register_data_with(&self, data: Bytes, opts: RegisterOptions) -> DataHandle {
        let started = Instant::now();
        let data_len = data.len();
        let local_id = self.store.register(data, Some(opts));
        if let Some(m) = &self.metrics {
            m.record_rendezvous_operation(
                RendezvousOp::Register,
                HandlerOutcome::Success,
                started.elapsed(),
            );
            m.record_rendezvous_bytes(RendezvousOp::Register, data_len);
            m.set_rendezvous_active_slots(self.store.slots.len());
        }
        DataHandle::pack(self.worker_id, local_id)
    }

    // -----------------------------------------------------------------------
    // Consumer-side API
    // -----------------------------------------------------------------------

    /// Query metadata about the data behind a handle (no lock acquired).
    ///
    /// For local handles, this is a DashMap lookup. For remote handles,
    /// sends a `_rv_metadata` typed unary to the owner.
    pub async fn metadata(&self, handle: DataHandle) -> Result<DataMetadata> {
        let started = Instant::now();
        let (target_worker, local_id) = handle.unpack();
        let result = if target_worker == self.worker_id {
            // Local fast-path
            self.store
                .metadata(local_id)
                .ok_or_else(|| anyhow::anyhow!("rendezvous handle not found: {handle}"))
        } else {
            consumer::Consumer::metadata(self.messenger(), handle).await
        };
        if let Some(m) = &self.metrics {
            let outcome = if result.is_ok() {
                HandlerOutcome::Success
            } else {
                HandlerOutcome::Error
            };
            m.record_rendezvous_operation(RendezvousOp::Metadata, outcome, started.elapsed());
        }
        result
    }

    /// Pull data from a handle. Acquires a read lock on the owner side.
    ///
    /// Returns `(data, lease_id)`. The `lease_id` must be passed to
    /// [`detach()`](Self::detach) or [`release()`](Self::release) when done.
    ///
    /// For local handles: DashMap lookup + `Bytes::clone()` (cheap refcount bump).
    /// For remote handles: receiver-driven pull via `_rv_acquire` + `_rv_pull` AMs.
    /// If the owner returns an `Rdma` response and the `nixl` feature is
    /// enabled with [`enable_nixl`](Self::enable_nixl), the data is fetched
    /// via NIXL_READ instead.
    pub async fn get(&self, handle: DataHandle) -> Result<(Bytes, u64)> {
        let started = Instant::now();
        let (target_worker, local_id) = handle.unpack();
        let result = if target_worker == self.worker_id {
            // Local fast-path: acquire lock and clone bytes
            let lease_id = self
                .store
                .acquire_read_lock(local_id)
                .ok_or_else(|| anyhow::anyhow!("rendezvous handle not found: {handle}"))?;
            let data = self
                .store
                .get_data(local_id)
                .ok_or_else(|| anyhow::anyhow!("slot vanished after lock acquire"))?;
            Ok((data, lease_id))
        } else {
            self.consumer_get(target_worker, handle).await
        };
        if let Some(m) = &self.metrics {
            let outcome = if result.is_ok() {
                HandlerOutcome::Success
            } else {
                HandlerOutcome::Error
            };
            m.record_rendezvous_operation(RendezvousOp::Get, outcome, started.elapsed());
            if let Ok((ref data, _)) = result {
                m.record_rendezvous_bytes(RendezvousOp::Get, data.len());
            }
        }
        result
    }

    /// Pull data from a handle into an explicit destination buffer.
    ///
    /// Returns `lease_id`. The caller must call [`detach()`](Self::detach) or
    /// [`release()`](Self::release) when done.
    pub async fn get_into(
        &self,
        handle: DataHandle,
        dest: &mut impl RendezvousWrite,
    ) -> Result<u64> {
        let started = Instant::now();
        let (target_worker, local_id) = handle.unpack();
        let result = if target_worker == self.worker_id {
            // Local fast-path
            let lease_id = self
                .store
                .acquire_read_lock(local_id)
                .ok_or_else(|| anyhow::anyhow!("rendezvous handle not found: {handle}"))?;
            let data = self
                .store
                .get_data(local_id)
                .ok_or_else(|| anyhow::anyhow!("slot vanished after lock acquire"))?;
            dest.write_chunk(0, &data)?;
            Ok(lease_id)
        } else {
            self.consumer_get_into(target_worker, handle, dest).await
        };
        if let Some(m) = &self.metrics {
            let outcome = if result.is_ok() {
                HandlerOutcome::Success
            } else {
                HandlerOutcome::Error
            };
            m.record_rendezvous_operation(RendezvousOp::Get, outcome, started.elapsed());
        }
        result
    }

    /// Increment the refcount on a handle (for additional consumers).
    pub async fn ref_handle(&self, handle: DataHandle) -> Result<()> {
        let started = Instant::now();
        let (target_worker, local_id) = handle.unpack();
        let result = if target_worker == self.worker_id {
            if !self.store.ref_increment(local_id) {
                anyhow::bail!("rendezvous handle not found: {handle}");
            }
            Ok(())
        } else {
            consumer::Consumer::ref_handle(self.messenger(), handle).await
        };
        if let Some(m) = &self.metrics {
            let outcome = if result.is_ok() {
                HandlerOutcome::Success
            } else {
                HandlerOutcome::Error
            };
            m.record_rendezvous_operation(RendezvousOp::Ref, outcome, started.elapsed());
        }
        result
    }

    /// Release the read lock WITHOUT decrementing refcount. The handle remains
    /// alive and can be `get()`-ed again.
    pub async fn detach(&self, handle: DataHandle, lease_id: u64) -> Result<()> {
        let started = Instant::now();
        let (target_worker, local_id) = handle.unpack();
        let result = if target_worker == self.worker_id {
            match self.store.consume_lease(lease_id) {
                Some(expected) if expected == local_id => {
                    self.store.release_read_lock(local_id);
                    self.store.remove_transfers_by_lease(lease_id);
                    Ok(())
                }
                Some(_) | None => {
                    anyhow::bail!("invalid or already-consumed lease {lease_id} for {handle}")
                }
            }
        } else {
            consumer::Consumer::detach(self.messenger(), handle, lease_id).await
        };
        if let Some(m) = &self.metrics {
            let outcome = if result.is_ok() {
                HandlerOutcome::Success
            } else {
                HandlerOutcome::Error
            };
            m.record_rendezvous_operation(RendezvousOp::Detach, outcome, started.elapsed());
        }
        result
    }

    /// Release the read lock AND decrement refcount. Data is freed when both
    /// refcount and read_lock_count reach zero.
    pub async fn release(&self, handle: DataHandle, lease_id: u64) -> Result<()> {
        let started = Instant::now();
        let (target_worker, local_id) = handle.unpack();
        let result = if target_worker == self.worker_id {
            match self.store.consume_lease(lease_id) {
                Some(expected) if expected == local_id => {
                    self.store.release_read_lock(local_id);
                    self.store.remove_transfers_by_lease(lease_id);
                    let should_free = self.store.ref_decrement(local_id);
                    if should_free {
                        self.store.try_free(local_id);
                    }
                    Ok(())
                }
                Some(_) | None => {
                    anyhow::bail!("invalid or already-consumed lease {lease_id} for {handle}")
                }
            }
        } else {
            consumer::Consumer::release(self.messenger(), handle, lease_id).await
        };
        if let Some(m) = &self.metrics {
            let outcome = if result.is_ok() {
                HandlerOutcome::Success
            } else {
                HandlerOutcome::Error
            };
            m.record_rendezvous_operation(RendezvousOp::Release, outcome, started.elapsed());
            m.set_rendezvous_active_slots(self.store.slots.len());
        }
        result
    }

    /// Get the worker ID of this manager.
    pub fn worker_id(&self) -> WorkerId {
        self.worker_id
    }

    /// Get direct access to the data store (for transparent mode integration).
    pub fn data_store(&self) -> &Arc<store::DataStore> {
        &self.store
    }

    // -----------------------------------------------------------------------
    // Phase 2 — NIXL/RDMA
    // -----------------------------------------------------------------------

    /// Enable the NIXL endpoint for this manager.
    ///
    /// Must be called before [`register_data_pinned`](Self::register_data_pinned)
    /// (owner side) or before consuming a handle that returns
    /// [`AcquireResponse::Rdma`](protocol::AcquireResponse::Rdma).
    ///
    /// May be called either before or after [`register_handlers`](Self::register_handlers);
    /// the registered handlers observe the lazily-set endpoint via a shared
    /// `Arc<OnceLock<...>>`.
    ///
    /// Idempotent: subsequent calls return `Ok(())` without re-initializing.
    #[cfg(feature = "nixl")]
    pub fn enable_nixl(self: &Arc<Self>) -> Result<()> {
        if self.nixl.get().is_some() {
            return Ok(());
        }
        let endpoint = NixlEndpoint::create(self.worker_id)?;
        // OnceLock::set returns Err if a concurrent caller raced and won; in
        // that case the other thread already finished init, which is fine.
        let _ = self.nixl.set(endpoint);
        Ok(())
    }

    /// Borrow the NIXL endpoint, or return an error if NIXL is not enabled.
    #[cfg(feature = "nixl")]
    pub fn nixl(&self) -> Result<&Arc<NixlEndpoint>> {
        self.nixl
            .get()
            .ok_or_else(|| anyhow::anyhow!("RendezvousManager::enable_nixl was not called"))
    }

    /// Stage `data` as RDMA-pinned NIXL-registered memory and return a [`DataHandle`].
    ///
    /// Pinned slots produce [`AcquireResponse::Rdma`](protocol::AcquireResponse::Rdma)
    /// on the owner side, which a NIXL-enabled consumer fulfills via direct
    /// `NIXL_READ` rather than the chunked-pull control path.
    ///
    /// Requires [`enable_nixl`](Self::enable_nixl) on the owner.
    #[cfg(feature = "nixl")]
    pub fn register_data_pinned(self: &Arc<Self>, data: Bytes) -> Result<DataHandle> {
        let started = Instant::now();
        let data_len = data.len();
        let endpoint = self.nixl()?;
        let (registration, data) = endpoint.register_bytes(data)?;
        let local_id = self.store.register_pinned(data, registration, None);
        if let Some(m) = &self.metrics {
            m.record_rendezvous_operation(
                RendezvousOp::Register,
                HandlerOutcome::Success,
                started.elapsed(),
            );
            m.record_rendezvous_bytes(RendezvousOp::Register, data_len);
            m.set_rendezvous_active_slots(self.store.slots.len());
        }
        Ok(DataHandle::pack(self.worker_id, local_id))
    }

    /// Inner consumer dispatch for `get`. With `nixl` enabled, performs the
    /// (one-shot) handshake on first contact with each peer, then routes
    /// `Rdma` responses through `nixl_read`.
    async fn consumer_get(
        &self,
        target_worker: WorkerId,
        handle: DataHandle,
    ) -> Result<(Bytes, u64)> {
        #[cfg(feature = "nixl")]
        {
            if let Some(endpoint) = self.nixl.get() {
                let remote_agent = self.ensure_remote_md(target_worker).await?;
                return consumer::Consumer::get_with_nixl(
                    self.messenger(),
                    handle,
                    Some((endpoint, remote_agent.as_str())),
                )
                .await;
            }
        }
        #[cfg(not(feature = "nixl"))]
        let _ = target_worker;
        consumer::Consumer::get(self.messenger(), handle).await
    }

    /// Inner consumer dispatch for `get_into`. Mirrors [`Self::consumer_get`].
    async fn consumer_get_into(
        &self,
        target_worker: WorkerId,
        handle: DataHandle,
        dest: &mut impl RendezvousWrite,
    ) -> Result<u64> {
        #[cfg(feature = "nixl")]
        {
            if let Some(endpoint) = self.nixl.get() {
                let remote_agent = self.ensure_remote_md(target_worker).await?;
                return consumer::Consumer::get_into_with_nixl(
                    self.messenger(),
                    handle,
                    dest,
                    Some((endpoint, remote_agent.as_str())),
                )
                .await;
            }
        }
        #[cfg(not(feature = "nixl"))]
        let _ = target_worker;
        consumer::Consumer::get_into(self.messenger(), handle, dest).await
    }

    /// Resolve and cache the loaded remote-agent name for a peer worker.
    ///
    /// Performs a one-shot `_rv_nixl_handshake` AM the first time a target is
    /// seen, calls `Agent::load_remote_md` on the returned blob, and caches
    /// the agent name keyed by `target_worker`. Subsequent calls are
    /// `DashMap` lookups.
    #[cfg(feature = "nixl")]
    pub async fn ensure_remote_md(&self, target_worker: WorkerId) -> Result<String> {
        let endpoint = self.nixl()?;
        if let Some(name) = endpoint.remote_md_loaded.get(&target_worker) {
            return Ok(name.clone());
        }
        let response = consumer::Consumer::nixl_handshake(self.messenger(), target_worker).await?;
        let loaded_name = endpoint
            .agent
            .load_remote_md(&response.local_md)
            .map_err(|e| anyhow::anyhow!("velo-nixl: load_remote_md failed: {e}"))?;
        if loaded_name != response.agent {
            tracing::warn!(
                "ensure_remote_md: peer reported agent {:?}, load_remote_md returned {:?}",
                response.agent,
                loaded_name
            );
        }
        endpoint
            .remote_md_loaded
            .entry(target_worker)
            .or_insert(loaded_name.clone());
        Ok(loaded_name)
    }
}
