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
//! # Phases
//!
//! - **Phase 1** (current): Multi-message chunked transfer over active messages
//! - **Phase 2** (future): NIXL RDMA direct memory access via dynamo-memory arena

pub mod consumer;
pub mod handle;
pub mod handlers;
pub mod protocol;
pub mod store;
pub mod transparent;
pub mod write;

pub use handle::DataHandle;
pub use protocol::DataMetadata;
pub use store::{RegisterOptions, StageMode};
pub use transparent::{RendezvousResolver, RendezvousStager};
pub use write::RendezvousWrite;

use std::sync::{Arc, OnceLock};
use std::time::Instant;

use anyhow::Result;
use bytes::Bytes;
use velo_common::WorkerId;
use velo_observability::{HandlerOutcome, RendezvousOp, VeloMetrics};

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
    messenger_lock: OnceLock<Arc<velo_messenger::Messenger>>,
    /// Optional Prometheus metrics.
    metrics: Option<Arc<VeloMetrics>>,
}

impl RendezvousManager {
    /// Create a new `RendezvousManager` for the given worker.
    pub fn new(worker_id: WorkerId) -> Self {
        Self {
            worker_id,
            store: Arc::new(store::DataStore::new()),
            messenger_lock: OnceLock::new(),
            metrics: None,
        }
    }

    /// Create a new `RendezvousManager` with metrics.
    pub fn with_metrics(worker_id: WorkerId, metrics: Arc<VeloMetrics>) -> Self {
        Self {
            worker_id,
            store: Arc::new(store::DataStore::new()),
            messenger_lock: OnceLock::new(),
            metrics: Some(metrics),
        }
    }

    /// Register the rendezvous control-plane handlers on the messenger.
    ///
    /// Must be called exactly once. Registers six underscore-prefixed handlers:
    /// `_rv_metadata`, `_rv_acquire`, `_rv_pull`, `_rv_ref`, `_rv_detach`, `_rv_release`.
    pub fn register_handlers(
        self: &Arc<Self>,
        messenger: Arc<velo_messenger::Messenger>,
    ) -> Result<()> {
        use handlers::{
            create_rv_acquire_handler, create_rv_detach_handler, create_rv_metadata_handler,
            create_rv_pull_handler, create_rv_ref_handler, create_rv_release_handler,
        };

        messenger
            .register_streaming_handler(create_rv_metadata_handler(Arc::clone(&self.store)))?;
        messenger.register_streaming_handler(create_rv_acquire_handler(Arc::clone(&self.store)))?;
        messenger.register_streaming_handler(create_rv_pull_handler(Arc::clone(&self.store)))?;
        messenger.register_streaming_handler(create_rv_ref_handler(Arc::clone(&self.store)))?;
        messenger.register_streaming_handler(create_rv_detach_handler(Arc::clone(&self.store)))?;
        messenger.register_streaming_handler(create_rv_release_handler(Arc::clone(&self.store)))?;

        self.messenger_lock
            .set(messenger)
            .map_err(|_| anyhow::anyhow!("register_handlers called twice"))?;

        Ok(())
    }

    /// Get the messenger reference (panics if `register_handlers` not called).
    fn messenger(&self) -> &Arc<velo_messenger::Messenger> {
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
            consumer::Consumer::get(self.messenger(), handle).await
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
            consumer::Consumer::get_into(self.messenger(), handle, dest).await
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
}
