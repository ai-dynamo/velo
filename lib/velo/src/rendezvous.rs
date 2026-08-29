// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Rendezvous data staging and large payload transfer for velo.
//!
//! Stage data at one worker (the *owner*), pass a compact [`DataHandle`] to
//! consumers by any means, and let each consumer pull it. The consumer drives
//! every transfer; the owner only ever answers.
//!
//! ```text
//! metadata(handle)  →  size and refcount, no lock
//! get(handle)       →  acquire a read lock, move the bytes, return a lease
//! detach(handle, lease)   release the lock, keep the handle usable
//! release(handle, lease)  release the lock and one reference; freed at zero
//! ```
//!
//! # Two ways the bytes move
//!
//! `_rv_acquire` takes the read lock and, in the same round trip, decides how
//! the payload will travel. Both answers end in the same detach-or-release, so
//! a caller writes the same code either way.
//!
//! * **Chunked** ([`AcquireResponse::Ready`](protocol::AcquireResponse::Ready))
//!   — the owner opens a transfer and the consumer pulls `_rv_pull` chunks of
//!   [`DEFAULT_CHUNK_SIZE`](store::DEFAULT_CHUNK_SIZE) until it has them all.
//!   Always available, for every slot and every consumer.
//!
//! * **RDMA** ([`AcquireResponse::Rdma`](protocol::AcquireResponse::Rdma)) —
//!   the owner answers with a [descriptor](descriptor::RdmaDescriptor) naming
//!   an address, a length and a packed remote key, and the consumer's NIC reads
//!   the bytes directly with a single `ucp_get_nbx`. No chunk round trips, no
//!   copy through the owner's handler.
//!
//! The RDMA answer requires four things at once, and any one of them missing
//! means chunked: the slot is staged in registered memory
//! ([`register_data_pinned`](RendezvousManager::register_data_pinned) or
//! [`register_data_in_region`](RendezvousManager::register_data_in_region)),
//! the consumer advertised a backend the owner can serve, the payload is at
//! least [`rdma_min_bytes`](rdma::RdmaRendezvousConfig::rdma_min_bytes), and
//! neither side has the RDMA path switched off. GET-first and decided at
//! acquire time, so the owner revalidates the registration on every transfer
//! and there is no cross-node invalidation protocol to get wrong.
//!
//! **Pinned slots are never RDMA-only.** They answer the chunked path exactly
//! as heap-staged slots do, which is what lets an old consumer, a consumer
//! without a UCX endpoint, and a consumer whose GET failed all read the same
//! slot without the owner knowing in advance which it is talking to.
//!
//! # Leases
//!
//! A `get` returns a lease the caller passes back to `detach` or `release`.
//! Chunked leases live until one of those arrives. **RDMA leases carry a
//! deadline**, because the transfer is issued by the consumer's NIC and the
//! owner cannot see it finish, fail, or never start: an owner-side reaper
//! force-releases a lease whose deadline passed, and a consumer with a slow
//! transfer keeps its lease alive with `_rv_lease_renew` for as long as the
//! transfer is running. Holding an RDMA lease idle past its deadline is not
//! supported in v1 — hold it briefly, or take the data and release.

pub mod consumer;
/// The RDMA transfer descriptor. Runtime-internal: it is a wire format velo
/// owns end to end, and nothing outside the crate constructs or reads one.
///
/// Deliberately *not* feature-gated even though only the RDMA path produces or
/// consumes one. It is pure byte manipulation with no dependency on a backend,
/// and keeping it unconditional keeps its round-trip and strict-decode tests
/// running in every build — including the builds that cannot produce a
/// descriptor, which are exactly the ones that would not notice the format
/// drifting. The `allow` is the price of that, and it is one decision stated
/// here rather than an attribute on each item.
#[allow(dead_code)]
pub(crate) mod descriptor;
pub mod handle;
pub mod handlers;
/// Slot bodies staged in RDMA-registered memory. Gated with the registration
/// layer it depends on, and runtime-internal: a `PinnedSlot` is how the store
/// holds staged memory, not something a caller names.
#[cfg(all(target_os = "linux", feature = "ucx"))]
pub(crate) mod pinned;
pub mod protocol;
// The RDMA registration layer, gated exactly as `transports::ucx` is.
#[cfg(all(target_os = "linux", feature = "ucx"))]
pub mod rdma;
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
    /// Stops the lease reaper. Cancelled by `Velo::graceful_shutdown` before
    /// the registration sweep, so the reaper is not force-releasing leases
    /// while regions are being unmapped underneath them.
    #[cfg(all(target_os = "linux", feature = "ucx"))]
    reaper_shutdown: tokio_util::sync::CancellationToken,
    /// One armed fault for the next RDMA transfer. See
    /// [`arm_rdma_hook`](Self::arm_rdma_hook).
    #[cfg(all(target_os = "linux", feature = "ucx", feature = "test-helpers"))]
    test_hook: parking_lot::Mutex<Option<RdmaTestHook>>,
}

/// A condition to force on the next RDMA transfer, for tests.
///
/// Every variant is something that either cannot happen over `UCX_TLS=tcp` or
/// cannot happen without a peer that misbehaves — and every one of them has a
/// velo-side response this phase is responsible for. Arming it is how those
/// responses stay covered.
///
/// Behind `test-helpers`, so it is not part of a release build's surface.
#[cfg(all(target_os = "linux", feature = "ucx", feature = "test-helpers"))]
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RdmaTestHook {
    /// Overwrite the received descriptor's backend discriminator with one no
    /// build knows.
    UnknownBackend,
    /// Drop the descriptor's last byte, so its key length overstates what
    /// follows.
    TruncateDescriptor,
    /// Append a byte the framing cannot account for.
    TrailingByte,
    /// Overstate the descriptor's declared key length without changing its
    /// bytes.
    LyingKeyLength,
    /// Fail the transfer after the descriptor has decoded, as an RDMA lane
    /// would on a remote access error.
    FailGet,
    /// Delay the transfer.
    ///
    /// **Not a failure.** A transfer that takes longer than half a lease
    /// deadline is the condition the renewal ticker exists for, and over
    /// `UCX_TLS=tcp` on a loopback there is no honest way to produce one.
    SlowGet(std::time::Duration),
}

#[cfg(all(target_os = "linux", feature = "ucx", feature = "test-helpers"))]
impl RdmaTestHook {
    /// Whether this fault applies to the descriptor rather than the transfer.
    fn is_descriptor_fault(&self) -> bool {
        matches!(
            self,
            Self::UnknownBackend
                | Self::TruncateDescriptor
                | Self::TrailingByte
                | Self::LyingKeyLength
        )
    }

    /// Apply a descriptor fault to the bytes the owner sent.
    ///
    /// Deliberately operates on the encoded form rather than on a decoded
    /// struct: what is under test is the decoder's accounting of the bytes on
    /// the wire, and re-encoding a mutated struct would only ever produce blobs
    /// the encoder considers well-formed.
    fn corrupt(&self, mut descriptor: Vec<u8>) -> Vec<u8> {
        match self {
            Self::UnknownBackend => {
                if let Some(byte) = descriptor.first_mut() {
                    *byte = 0xEE;
                }
            }
            Self::TruncateDescriptor => {
                descriptor.pop();
            }
            Self::TrailingByte => descriptor.push(0),
            Self::LyingKeyLength => {
                // The `rkey_len` field sits at the end of the fixed header.
                let at = descriptor::HEADER_LEN - 2;
                if descriptor.len() >= descriptor::HEADER_LEN {
                    descriptor[at..at + 2].copy_from_slice(&u16::MAX.to_le_bytes());
                }
            }
            Self::FailGet | Self::SlowGet(_) => {}
        }
        descriptor
    }
}

/// The RDMA registry and the policy the rendezvous protocol applies to it.
///
/// Bound late — the registry wraps an RMA endpoint on a transport that must
/// have started first — and held by the [`DataStore`](store::DataStore), which
/// is the one thing the `_rv_acquire` handler closure already has an `Arc` to.
/// See the field's own docs for why capturing the manager instead would be a
/// reference cycle.
#[cfg(all(target_os = "linux", feature = "ucx"))]
pub(crate) struct RdmaContext {
    /// The registration layer: the arena pool, the external regions, the GET.
    pub(crate) registry: Arc<rdma::RdmaRegistry>,
    /// Thresholds, the kill switch, and the lease deadline.
    pub(crate) config: rdma::RdmaRendezvousConfig,
    /// The wire discriminator for `registry`'s backend, resolved once at bind
    /// time rather than re-parsed from a string on every acquire.
    pub(crate) backend: descriptor::DescriptorBackend,
}

impl RendezvousManager {
    /// Create a new `RendezvousManager` for the given worker.
    pub fn new(worker_id: WorkerId) -> Self {
        Self::build(worker_id, None)
    }

    /// Create a new `RendezvousManager` with metrics.
    pub fn with_metrics(worker_id: WorkerId, metrics: Arc<VeloMetrics>) -> Self {
        Self::build(worker_id, Some(metrics))
    }

    fn build(worker_id: WorkerId, metrics: Option<Arc<VeloMetrics>>) -> Self {
        Self {
            worker_id,
            store: Arc::new(store::DataStore::with_metrics(metrics.clone())),
            messenger_lock: OnceLock::new(),
            metrics,
            #[cfg(all(target_os = "linux", feature = "ucx"))]
            reaper_shutdown: tokio_util::sync::CancellationToken::new(),
            #[cfg(all(target_os = "linux", feature = "ucx", feature = "test-helpers"))]
            test_hook: parking_lot::Mutex::new(None),
        }
    }

    /// Register the rendezvous control-plane handlers on the messenger.
    ///
    /// Must be called exactly once. Registers seven underscore-prefixed
    /// handlers: `_rv_metadata`, `_rv_acquire`, `_rv_pull`, `_rv_ref`,
    /// `_rv_detach`, `_rv_release`, `_rv_lease_renew`.
    ///
    /// All seven are registered unconditionally, including on a build without
    /// the RDMA path. `_rv_lease_renew` on such an owner is a no-op that logs
    /// at `debug` — no lease it grants ever carries a deadline — and that is
    /// the point: a consumer must not have to know whether the owner can grant
    /// RDMA leases before it is allowed to send a keepalive, and a handler that
    /// existed only in some builds would turn a benign fire-and-forget into an
    /// "unknown handler" error in exactly the mixed deployment the
    /// `#[serde(default)]` discipline exists to survive.
    pub fn register_handlers(
        self: &Arc<Self>,
        messenger: Arc<crate::messenger::Messenger>,
    ) -> Result<()> {
        use handlers::{
            create_rv_acquire_handler, create_rv_detach_handler, create_rv_lease_renew_handler,
            create_rv_metadata_handler, create_rv_pull_handler, create_rv_ref_handler,
            create_rv_release_handler,
        };

        messenger
            .register_streaming_handler(create_rv_metadata_handler(Arc::clone(&self.store)))?;
        messenger.register_streaming_handler(create_rv_acquire_handler(Arc::clone(&self.store)))?;
        messenger.register_streaming_handler(create_rv_pull_handler(Arc::clone(&self.store)))?;
        messenger.register_streaming_handler(create_rv_ref_handler(Arc::clone(&self.store)))?;
        messenger.register_streaming_handler(create_rv_detach_handler(Arc::clone(&self.store)))?;
        messenger.register_streaming_handler(create_rv_release_handler(Arc::clone(&self.store)))?;
        messenger
            .register_streaming_handler(create_rv_lease_renew_handler(Arc::clone(&self.store)))?;

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
        self.stage(store::SlotBody::InMemory(data), None)
    }

    /// Stage data with options (TTL, etc.) and return a [`DataHandle`].
    pub fn register_data_with(&self, data: Bytes, opts: RegisterOptions) -> DataHandle {
        self.stage(store::SlotBody::InMemory(data), Some(opts))
    }

    /// Insert a body and account for it. The one place a slot is created.
    fn stage(&self, body: store::SlotBody, opts: Option<RegisterOptions>) -> DataHandle {
        let started = Instant::now();
        let data_len = body.total_len() as usize;
        let local_id = self.store.register_body(body, opts);
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

    /// Stage data in RDMA-registered pool memory, so consumers that can issue
    /// an RDMA GET read it without a chunk round trip.
    ///
    /// # This never fails
    ///
    /// It returns a [`DataHandle`], not a `Result`, and that is the contract.
    /// Pool exhaustion, a registered-bytes budget that is already spent, a
    /// switched-off kill switch, an instance with no UCX transport at all —
    /// every one of them stages the data in plain memory instead and records
    /// the reason on `velo_rendezvous_rdma_path_total`. Pinning is a transfer
    /// optimisation, and a *staging* call that failed because the pool was busy
    /// would push a fallback onto every caller that most of them would get
    /// wrong (D4).
    ///
    /// The slot is readable either way: a pinned slot still answers the chunked
    /// path, so falling back changes how fast the data moves and never whether
    /// it can be reached.
    ///
    /// # Cost
    ///
    /// One copy, always: the bytes are copied into registered memory here so a
    /// peer's NIC can read them later, and the fallback copies them into a
    /// `Bytes`. Zero-length data is staged in plain memory — there is nothing
    /// for a GET to transfer.
    ///
    /// To stage without a copy, register the memory yourself and use
    /// [`register_data_in_region`](Self::register_data_in_region).
    pub async fn register_data_pinned(&self, data: &[u8]) -> DataHandle {
        #[cfg(all(target_os = "linux", feature = "ucx"))]
        if !data.is_empty()
            && let Some(ctx) = self.store.rdma()
        {
            use crate::observability::RdmaPathReason;
            if !ctx.config.enabled {
                self.store.record_path(RdmaPathReason::KillSwitch);
            } else {
                match ctx.registry.alloc_pinned(data.len()).await {
                    Ok(mut buf) => {
                        // `alloc_pinned` hands back exactly the requested
                        // length, so this cannot be a partial copy.
                        buf.copy_from_slice(data);
                        return self.stage(
                            store::SlotBody::Pinned(pinned::PinnedSlot::from_pool(
                                buf,
                                ctx.backend,
                            )),
                            None,
                        );
                    }
                    Err(e) => {
                        let reason = match e {
                            rdma::RdmaError::BudgetExceeded { .. } => RdmaPathReason::Budget,
                            _ => RdmaPathReason::PoolExhausted,
                        };
                        self.store.record_path(reason);
                        tracing::debug!(
                            bytes = data.len(),
                            error = %e,
                            "rendezvous: pinned staging refused; staging in plain memory"
                        );
                    }
                }
            }
        }
        self.register_data(Bytes::copy_from_slice(data))
    }

    /// Stage data in pool memory *without blocking*, for the transparent
    /// large-payload path.
    ///
    /// Uses only arenas the pool has already mapped
    /// ([`try_alloc_pinned`](rdma::RdmaRegistry::try_alloc_pinned)) and stages
    /// in plain memory otherwise. The caller is the messenger's synchronous
    /// `send_message`, which has no `await` to give and must not grow the pool
    /// on a send: mapping an arena is an `ibv_reg_mr` whose cost is linear in
    /// its size.
    ///
    /// The consequence, stated plainly: a process whose only staging is
    /// transparent never maps an arena and therefore never takes the RDMA path.
    /// The pool is grown by [`register_data_pinned`](Self::register_data_pinned),
    /// which can await. Warming it from a send in the background was considered
    /// and rejected — a message send that side-effects a 64 MiB pin on a
    /// detached task is not something a caller can reason about.
    #[cfg(all(target_os = "linux", feature = "ucx"))]
    pub(crate) fn register_data_pinned_sync(&self, data: Bytes) -> DataHandle {
        use crate::observability::RdmaPathReason;
        if !data.is_empty()
            && let Some(ctx) = self.store.rdma()
        {
            if !ctx.config.enabled {
                self.store.record_path(RdmaPathReason::KillSwitch);
            } else if let Some(mut buf) = ctx.registry.try_alloc_pinned(data.len()) {
                buf.copy_from_slice(&data);
                return self.stage(
                    store::SlotBody::Pinned(pinned::PinnedSlot::from_pool(buf, ctx.backend)),
                    None,
                );
            } else {
                self.store.record_path(RdmaPathReason::PoolExhausted);
            }
        }
        self.register_data(data)
    }

    /// Stage a range of memory the caller registered, without copying it.
    ///
    /// The zero-copy counterpart to
    /// [`register_data_pinned`](Self::register_data_pinned): the bytes stay
    /// where the caller put them and the slot merely describes them. `range` is
    /// measured from the pointer that was registered, not from
    /// [`RegionGuard::effective_range`](rdma::RegionGuard::effective_range), so
    /// a caller can never name a byte inside the registration but outside its
    /// own allocation.
    ///
    /// # The staged slot holds the region open
    ///
    /// The slot takes an in-flight guard on `guard`'s own accounting, so
    /// [`RegionGuard::unregister`](rdma::RegionGuard::unregister) drains the
    /// anchors staged inside the region before it unmaps. Freeing the slot —
    /// the last `release` — is what lets the deregistration through. A caller
    /// that stages anchors and then unregisters without releasing them sees
    /// `unregister` wait and, if it runs out of budget, unmap anyway with a
    /// warning; reads of that slot refuse from that moment rather than touching
    /// freed pages.
    ///
    /// # Errors
    ///
    /// [`NotConfigured`](rdma::RdmaError::NotConfigured) without a UCX
    /// transport, [`OutOfRange`](rdma::RdmaError::OutOfRange) for an empty,
    /// inverted, or out-of-bounds range, and
    /// [`ShuttingDown`](rdma::RdmaError::ShuttingDown) once the region or the
    /// registry has begun to go away.
    ///
    /// Unlike `register_data_pinned` this *does* return a `Result`, because no
    /// fallback could honour what was asked: staging in plain memory would
    /// silently copy bytes the caller asked not to be copied.
    ///
    /// The kill switch does not affect it. The memory is registered either way,
    /// so anchoring in it costs nothing extra; the switch decides whether
    /// `_rv_acquire` answers with a descriptor, and a switched-off owner serves
    /// the same slot chunked.
    #[cfg(all(target_os = "linux", feature = "ucx"))]
    pub fn register_data_in_region(
        &self,
        guard: &rdma::RegionGuard,
        range: std::ops::Range<u64>,
    ) -> Result<DataHandle, rdma::RdmaError> {
        let ctx = self.store.rdma().ok_or(rdma::RdmaError::NotConfigured)?;
        let len = range
            .end
            .checked_sub(range.start)
            .filter(|len| *len != 0)
            .ok_or(rdma::RdmaError::OutOfRange)?;
        if range.end > guard.len() {
            return Err(rdma::RdmaError::OutOfRange);
        }
        let addr = guard
            .addr()
            .checked_add(range.start)
            .ok_or(rdma::RdmaError::OutOfRange)?;

        // Acquire *first*, then check. The guard is what a concurrent
        // `unregister` waits on, so taking it after the check would leave a gap
        // in which the whole gate-drain-unmap sequence could run. The read-time
        // re-check inside the slot is the containment for what remains: a slot
        // that lands after a drain has already timed out refuses its first read
        // rather than touching freed memory.
        let in_flight = guard.in_flight().acquire();
        if guard.in_flight().is_draining() || guard.is_deregistered() || guard.is_shutting_down() {
            drop(in_flight);
            return Err(rdma::RdmaError::ShuttingDown);
        }

        let remote = guard.remote();
        Ok(self.stage(
            store::SlotBody::Pinned(pinned::PinnedSlot::from_region(
                in_flight,
                guard.watch(),
                ctx.backend,
                addr,
                len,
                remote.generation,
                remote.packed_key,
            )),
            None,
        ))
    }

    /// Bind the RDMA registry and start the lease reaper.
    ///
    /// Called once, by `VeloBuilder::build`, after the transports have started
    /// and the registry exists. Mirrors `messenger_lock`: the manager is
    /// constructed before the thing it needs, and the binding is a set-once.
    #[cfg(all(target_os = "linux", feature = "ucx"))]
    pub(crate) fn set_rdma_context(
        &self,
        registry: Arc<rdma::RdmaRegistry>,
        config: rdma::RdmaRendezvousConfig,
        runtime: &tokio::runtime::Handle,
    ) -> Result<()> {
        let key = registry.backend_key().to_string();
        let backend = descriptor::DescriptorBackend::from_key(&key).ok_or_else(|| {
            anyhow::anyhow!("rdma backend {key:?} has no descriptor discriminator")
        })?;
        // Half the deadline, so a lease is force-released between one and one
        // and a half timeouts after its last renewal. The floor keeps a config
        // with a zero or tiny timeout from turning the reaper into a spin.
        let period = (config.lease_timeout / 2).max(std::time::Duration::from_millis(10));

        let ctx = RdmaContext {
            registry: Arc::clone(&registry),
            config,
            backend,
        };
        if self.store.set_rdma(ctx).is_err() {
            anyhow::bail!("set_rdma_context called twice");
        }

        // Weak on both sides: a `Velo` dropped without `graceful_shutdown` must
        // not be kept alive by its own reaper. The token is the orderly exit,
        // the upgrade failure is the backstop, so neither a forgotten shutdown
        // nor an abandoned runtime leaves the task holding a store.
        let store = Arc::downgrade(&self.store);
        let registry = Arc::downgrade(&registry);
        let token = self.reaper_shutdown.clone();
        runtime.spawn(async move {
            reap_expired_leases(store, registry, token, period).await;
        });
        Ok(())
    }

    /// Stop the lease reaper.
    ///
    /// Called by `Velo::graceful_shutdown` before the registration sweep, so
    /// the reaper is not force-releasing leases — and dropping the pinned
    /// staging under them — while regions are being unmapped.
    #[cfg(all(target_os = "linux", feature = "ucx"))]
    pub(crate) fn shutdown(&self) {
        self.reaper_shutdown.cancel();
    }

    /// Arm one fault on the next RDMA transfer this instance performs.
    ///
    /// The fallback paths are the ones that must not rot, and every condition
    /// that reaches them is either impossible to provoke over `UCX_TLS=tcp` (a
    /// GET that fails) or impossible to provoke at all without a peer that
    /// lies (a malformed descriptor). Arming the condition here tests *velo's*
    /// response to it, which is the part this phase owns.
    ///
    /// One-shot: the next transfer takes it and clears it. Sticky would pass
    /// today only because the fallback re-acquire carries no offer and so never
    /// sees a second descriptor — a property of the code under test, which is
    /// not something a test should quietly rely on.
    #[cfg(all(target_os = "linux", feature = "ucx", feature = "test-helpers"))]
    pub fn arm_rdma_hook(&self, hook: RdmaTestHook) {
        *self.test_hook.lock() = Some(hook);
    }

    /// Take an armed descriptor fault, if the armed one is a descriptor fault.
    ///
    /// Split from [`take_get_hook`](Self::take_get_hook) so one slot can hold
    /// either kind without a descriptor test consuming a GET fault or the
    /// reverse.
    #[cfg(all(target_os = "linux", feature = "ucx", feature = "test-helpers"))]
    fn take_descriptor_hook(&self) -> Option<RdmaTestHook> {
        let mut slot = self.test_hook.lock();
        match slot.as_ref() {
            Some(hook) if hook.is_descriptor_fault() => slot.take(),
            _ => None,
        }
    }

    /// Take an armed transfer fault, if the armed one is a transfer fault.
    #[cfg(all(target_os = "linux", feature = "ucx", feature = "test-helpers"))]
    fn take_get_hook(&self) -> Option<RdmaTestHook> {
        let mut slot = self.test_hook.lock();
        match slot.as_ref() {
            Some(hook) if !hook.is_descriptor_fault() => slot.take(),
            _ => None,
        }
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
            consumer::Consumer::get(self, handle).await
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

    /// Pull data from a handle into registered memory, with no copy out.
    ///
    /// The zero-copy counterpart to [`get`](Self::get): where the owner answers
    /// with an RDMA descriptor, the bytes land in the returned buffer written
    /// by this instance's NIC and are never copied. Where it answers chunked —
    /// a heap-staged slot, a payload under the threshold, a kill switch, an
    /// owner without the RDMA path — the chunks are pulled and copied into a
    /// pooled buffer, so the return type does not depend on what the owner
    /// decided.
    ///
    /// # Holding the buffer
    ///
    /// Dropping the [`PinnedBuf`](rdma::PinnedBuf) returns its space to the
    /// pool; nothing else is required and nothing is unregistered. Until then
    /// the space is a live reservation against the registered-bytes budget, so
    /// hold it for as long as the bytes are being used and no longer.
    ///
    /// The lease is a separate matter, and this does **not** hold it open for
    /// you: the renewal ticker is scoped to the transfer, so a caller that
    /// keeps the buffer past the lease deadline will find the owner has
    /// force-released the lease. Release it as soon as the transfer is done —
    /// the returned buffer stays valid, because it is this instance's memory.
    ///
    /// # Errors
    ///
    /// Everything [`get`](Self::get) can fail with, plus
    /// [`RdmaError::NotConfigured`](rdma::RdmaError::NotConfigured) when this
    /// instance has no RDMA registry to allocate a destination from — including
    /// for a purely local handle, which still needs somewhere pinned to copy
    /// into.
    #[cfg(all(target_os = "linux", feature = "ucx"))]
    pub async fn get_pinned(&self, handle: DataHandle) -> Result<(rdma::PinnedBuf, u64)> {
        let started = Instant::now();
        let (target_worker, local_id) = handle.unpack();
        let result = if target_worker == self.worker_id {
            // Local: there is no transfer to make zero-copy, so this is an
            // acquire plus a copy into a pooled buffer. Offered anyway so a
            // caller can use one code path for handles that may be either.
            let lease_id = self
                .store
                .acquire_read_lock(local_id)
                .ok_or_else(|| anyhow::anyhow!("rendezvous handle not found: {handle}"))?;
            let data = self
                .store
                .get_data(local_id)
                .ok_or_else(|| anyhow::anyhow!("slot vanished after lock acquire"))?;
            let ctx = self.store.rdma().ok_or(rdma::RdmaError::NotConfigured)?;
            let mut buf = ctx.registry.alloc_pinned(data.len()).await?;
            buf.copy_from_slice(&data);
            Ok((buf, lease_id))
        } else {
            consumer::Consumer::get_pinned(self, handle).await
        };
        if let Some(m) = &self.metrics {
            let outcome = if result.is_ok() {
                HandlerOutcome::Success
            } else {
                HandlerOutcome::Error
            };
            m.record_rendezvous_operation(RendezvousOp::Get, outcome, started.elapsed());
            if let Ok((ref buf, _)) = result {
                m.record_rendezvous_bytes(RendezvousOp::Get, buf.len());
            }
        }
        result
    }

    /// Allocate a registered destination for [`get_into`](Self::get_into).
    ///
    /// The only [`RendezvousWrite`] a remote NIC can write into directly. Pass
    /// it to `get_into` and, where the owner answers with a descriptor, the
    /// transfer lands in it with no copy at all.
    ///
    /// # Errors
    ///
    /// [`NotConfigured`](rdma::RdmaError::NotConfigured) without a UCX
    /// transport, [`BudgetExceeded`](rdma::RdmaError::BudgetExceeded) over the
    /// registered-bytes ceiling, [`OutOfRange`](rdma::RdmaError::OutOfRange)
    /// for a zero length.
    ///
    /// Unlike the staging APIs there is no fallback here, because there is
    /// nothing to fall back *to*: the caller asked for registered memory
    /// specifically, and an ordinary `Vec` would silently not be one. A caller
    /// that can live with a copy should use a `Vec` directly, which works and
    /// still rides the RDMA path.
    #[cfg(all(target_os = "linux", feature = "ucx"))]
    pub async fn alloc_pinned_writer(
        &self,
        len: usize,
    ) -> Result<write::PinnedWriter, rdma::RdmaError> {
        let ctx = self.store.rdma().ok_or(rdma::RdmaError::NotConfigured)?;
        Ok(write::PinnedWriter::new(
            ctx.registry.alloc_pinned(len).await?,
        ))
    }

    /// Pull data from a handle into an explicit destination buffer.
    ///
    /// Returns `lease_id`. The caller must call [`detach()`](Self::detach) or
    /// [`release()`](Self::release) when done.
    ///
    /// A [`PinnedWriter`](write::PinnedWriter) destination is filled by the
    /// NIC with no copy; every other destination gets one copy out of a pooled
    /// buffer when the owner offers RDMA, and the chunk-by-chunk path when it
    /// does not.
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
            consumer::Consumer::get_into(self, handle, dest).await
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
            match self.store.consume_lease(lease_id, local_id) {
                store::LeaseOutcome::Consumed => {
                    self.store.release_read_lock(local_id);
                    self.store.remove_transfers_by_lease(lease_id);
                    Ok(())
                }
                outcome => {
                    anyhow::bail!(
                        "invalid or already-consumed lease {lease_id} for {handle}: {outcome:?}"
                    )
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
            match self.store.consume_lease(lease_id, local_id) {
                store::LeaseOutcome::Consumed => {
                    self.store.release_read_lock(local_id);
                    self.store.remove_transfers_by_lease(lease_id);
                    let should_free = self.store.ref_decrement(local_id);
                    if should_free {
                        self.store.try_free(local_id);
                    }
                    Ok(())
                }
                outcome => {
                    anyhow::bail!(
                        "invalid or already-consumed lease {lease_id} for {handle}: {outcome:?}"
                    )
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

/// Force-release RDMA leases whose deadline has passed (D8).
///
/// # Why only RDMA leases have deadlines
///
/// A chunked transfer is *visible*: every chunk is an inbound request, and a
/// consumer that died stops sending them, so the owner could in principle
/// notice. An RDMA GET is issued by the consumer's NIC into the owner's memory
/// without the owner's CPU being involved at all — there is no completion the
/// owner sees, no error it is told about, and nothing to time out. Without this
/// task, a consumer that crashes between `_rv_acquire` and `_rv_release` leaves
/// the read lock and its reference held forever, and the slot becomes immortal.
/// That compounding leak is the failure PR #40 shipped.
///
/// # Why the scan collects before it acts
///
/// Force-releasing removes from `lease_deadlines`, `active_leases`, `transfers`
/// and `slots`, all `DashMap`s. Doing any of that while an iterator holds a
/// shard lock deadlocks rather than fails, so
/// [`expired_leases`](store::DataStore::expired_leases) returns an owned `Vec`
/// and nothing here iterates while it mutates.
///
/// # Why the handles are weak
///
/// A `Velo` dropped without `graceful_shutdown` — a panic, a test that lets it
/// fall out of scope — must not be kept alive by its own reaper. The token is
/// the orderly exit; the upgrade failure is the backstop for every other way a
/// runtime ends.
#[cfg(all(target_os = "linux", feature = "ucx"))]
async fn reap_expired_leases(
    store: std::sync::Weak<store::DataStore>,
    registry: std::sync::Weak<rdma::RdmaRegistry>,
    token: tokio_util::sync::CancellationToken,
    period: std::time::Duration,
) {
    loop {
        tokio::select! {
            _ = token.cancelled() => return,
            _ = tokio::time::sleep(period) => {}
        }
        let Some(store) = store.upgrade() else { return };

        let expired = store.expired_leases(Instant::now());
        let mut reaped = 0usize;
        for (lease_id, local_id) in expired {
            // `None` means a detach, release, or a previous sweep got there
            // first, which is the ordinary outcome of racing a consumer that
            // finished just in time. Only an actually-forced release counts.
            if store.force_release_lease(lease_id, local_id) {
                reaped += 1;
                tracing::warn!(
                    lease = lease_id,
                    slot = local_id,
                    "rendezvous: force-releasing an RDMA lease past its deadline; the consumer \
                     did not release it and stopped renewing it"
                );
            }
        }
        if let Some(m) = store.metrics() {
            if reaped != 0 {
                m.record_rendezvous_leases_reaped(reaped);
                m.set_rendezvous_active_slots(store.slots.len());
            }
            // Sampled on the tick rather than pushed from the registration
            // paths, so the gauge reads current at scrape time instead of
            // freezing at whatever the last registration left behind.
            if let Some(registry) = registry.upgrade()
                && let Some(regions) = registry.live_regions()
            {
                m.set_rdma_live_regions(regions);
            }
        }
    }
}
