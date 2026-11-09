// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Transparent large payload support: implements the messenger's
//! [`LargePayloadStager`] and [`LargePayloadResolver`] traits.
//!
//! A payload over [`DEFAULT_THRESHOLD`] is staged by the sender, replaced in
//! the frame by a handle, and resolved by the receiver before the handler is
//! called — so handler code never sees the difference. Where both ends have the
//! RDMA path and the sender's pool is warm, that resolution is a single GET;
//! otherwise it is the chunked pull it has always been.

use std::sync::Arc;
use std::time::Duration;

use crate::messenger::large_payload::{LargePayloadResolver, LargePayloadStager};
use anyhow::Result;
use bytes::Bytes;
use futures::future::BoxFuture;

use crate::RendezvousManager;

/// Default threshold for auto-staging large payloads (256 KiB).
///
/// Sits inside D11's ordering invariant, `rdma_min_bytes <= this <
/// DEFAULT_CHUNK_SIZE` (64 KiB, 256 KiB, 512 KiB at their defaults), so a
/// payload that crosses this threshold is already large enough for the RDMA
/// path. At the default chunk size a payload at the threshold is one pull;
/// both the threshold and the chunk size are configurable, so that is a
/// property of the defaults and not a guarantee — a smaller configured chunk
/// size costs proportionally more pulls, and payloads far above the threshold
/// take as many as their size requires.
pub const DEFAULT_THRESHOLD: usize = 256 * 1024;

/// Sender-side: stages large payloads locally via the [`RendezvousManager`].
pub struct RendezvousStager {
    manager: Arc<RendezvousManager>,
    threshold: usize,
}

impl RendezvousStager {
    pub fn new(manager: Arc<RendezvousManager>) -> Self {
        Self {
            manager,
            threshold: DEFAULT_THRESHOLD,
        }
    }

    pub fn with_threshold(mut self, threshold: usize) -> Self {
        self.threshold = threshold;
        self
    }
}

impl LargePayloadStager for RendezvousStager {
    /// Stage the payload and return its handle.
    ///
    /// # Synchronous, and therefore never grows the pool
    ///
    /// This runs inside the messenger's `send_message`, which is not `async`.
    /// Where an RDMA registry exists, the payload is staged in pool memory that
    /// is *already mapped*, so a consumer can read it with one GET; where no
    /// mapped arena has room it is staged in plain memory instead, and the
    /// consumer pulls chunks exactly as before.
    ///
    /// Mapping a fresh arena is an `ibv_reg_mr` whose cost is linear in its
    /// size, so doing one here would put a multi-millisecond stall in the
    /// middle of a send. A process whose *only* staging is transparent
    /// therefore never maps an arena and never rides the RDMA path; the pool is
    /// grown by [`RendezvousManager::register_data_pinned`], which can await.
    ///
    /// # Ordering caveat (unchanged)
    ///
    /// Staging is still synchronous with respect to the send, so the handle is
    /// in the headers before the frame leaves. The receiver resolves it with a
    /// `get` + `release` before the handler sees the payload, which is what
    /// keeps transparent mode invisible to handler code — and it is why a
    /// transparently staged slot has refcount 1 and a single reader.
    fn stage(&self, payload: Bytes, ttl: Option<Duration>) -> String {
        let opts = ttl.map(|ttl| crate::rendezvous::RegisterOptions::new().ttl(ttl));
        #[cfg(all(target_os = "linux", feature = "ucx"))]
        let handle = self.manager.register_data_pinned_sync(payload, opts);
        #[cfg(not(all(target_os = "linux", feature = "ucx")))]
        let handle = match opts {
            Some(opts) => self.manager.register_data_with(payload, opts),
            None => self.manager.register_data(payload),
        };
        // Encode handle as u128 decimal string for header transport
        handle.as_u128().to_string()
    }

    fn threshold(&self) -> usize {
        self.threshold
    }
}

/// Receiver-side: resolves staged payloads via the [`RendezvousManager`].
///
/// Performs a full get() + release() cycle since transparent staging
/// uses single-use handles (refcount = 1).
pub struct RendezvousResolver {
    manager: Arc<RendezvousManager>,
    limit: u64,
    fetch_timeout: Option<Duration>,
}

impl RendezvousResolver {
    pub fn new(manager: Arc<RendezvousManager>) -> Self {
        Self {
            manager,
            limit: u64::MAX,
            fetch_timeout: None,
        }
    }

    /// Refuse a payload over `limit` bytes before anything of its size is
    /// allocated. The refusal releases the staged payload on its owner.
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = u64::try_from(limit).unwrap_or(u64::MAX);
        self
    }

    /// Give up a fetch after `timeout`, even when the sender asked for more
    /// or set no bound. `None`, the default, sets no bound of its own. See
    /// [`VeloBuilder::staged_fetch_timeout`](crate::VeloBuilder::staged_fetch_timeout).
    pub fn with_fetch_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.fetch_timeout = timeout;
        self
    }
}

impl LargePayloadResolver for RendezvousResolver {
    /// With a bound, the owner is asked to end the lease once no pull renews
    /// it within that bound: the fetch is dropped at the bound with no word
    /// to the owner, which would otherwise keep the payload staged.
    fn resolve(&self, handle_str: &str, within: Option<Duration>) -> BoxFuture<'_, Result<Bytes>> {
        let handle_str = handle_str.to_string();
        Box::pin(async move {
            let raw: u128 = handle_str
                .parse()
                .map_err(|e| anyhow::anyhow!("invalid rendezvous handle: {e}"))?;
            let handle = crate::DataHandle::from_u128(raw);
            let (data, lease_id) = self.manager.get_within(handle, self.limit, within).await?;
            // Auto-release: transparent staging uses refcount=1
            self.manager.release(handle, lease_id).await?;
            Ok(data)
        })
    }

    fn fetch_timeout(&self) -> Option<Duration> {
        self.fetch_timeout
    }
}
