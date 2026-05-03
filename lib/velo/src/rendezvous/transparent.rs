// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Transparent large payload support: implements the messenger's
//! [`LargePayloadStager`] and [`LargePayloadResolver`] traits.

use std::sync::Arc;

use crate::messenger::large_payload::{LargePayloadResolver, LargePayloadStager};
use anyhow::Result;
use bytes::Bytes;
use futures::future::BoxFuture;

use crate::RendezvousManager;

/// Default threshold for auto-staging large payloads (256 KiB).
///
/// Matches the inline threshold in the `_rv_acquire` handler so that
/// payloads staged transparently always take the chunked path.
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
    fn stage(&self, payload: Bytes) -> String {
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
