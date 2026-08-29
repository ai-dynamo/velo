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

use crate::messenger::large_payload::{LargePayloadResolver, LargePayloadStager};
use anyhow::Result;
use bytes::Bytes;
use futures::future::BoxFuture;

use crate::RendezvousManager;

/// Default threshold for auto-staging large payloads (256 KiB).
///
/// Sits inside D11's ordering invariant, `rdma_min_bytes <= this <
/// DEFAULT_CHUNK_SIZE` (64 KiB, 256 KiB, 512 KiB): everything staged here is
/// already large enough for the RDMA path, and small enough that the chunked
/// fallback is one or two pulls.
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
    fn stage(&self, payload: Bytes) -> String {
        #[cfg(all(target_os = "linux", feature = "ucx"))]
        let handle = self.manager.register_data_pinned_sync(payload);
        #[cfg(not(all(target_os = "linux", feature = "ucx")))]
        let handle = self.manager.register_data(payload);
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
}

impl RendezvousResolver {
    pub fn new(manager: Arc<RendezvousManager>) -> Self {
        Self { manager }
    }
}

impl LargePayloadResolver for RendezvousResolver {
    fn resolve(&self, handle_str: &str) -> BoxFuture<'_, Result<Bytes>> {
        let handle_str = handle_str.to_string();
        Box::pin(async move {
            let raw: u128 = handle_str
                .parse()
                .map_err(|e| anyhow::anyhow!("invalid rendezvous handle: {e}"))?;
            let handle = crate::DataHandle::from_u128(raw);
            let (data, lease_id) = self.manager.get(handle).await?;
            // Auto-release: transparent staging uses refcount=1
            self.manager.release(handle, lease_id).await?;
            Ok(data)
        })
    }
}
