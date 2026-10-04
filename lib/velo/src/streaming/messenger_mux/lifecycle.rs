// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! How the mux starts batchers and how it stops: the one stop path every
//! failure and shutdown goes through.

use std::sync::Arc;

use anyhow::{Result, anyhow};

use super::ingress::IngressRegistry;
use super::peer_batcher::{self, BatcherContext, BatcherHandle};
use super::{MessengerMuxTransport, MuxCore, PeerLane};
use crate::observability::MuxMetricsHandle;

impl MessengerMuxTransport {
    /// Stop the mux's tasks, so nothing more is written to any peer, but
    /// leave its slots open: [`Self::shutdown`] retires them. Shutdown calls
    /// this first so streams can be detached from their slots before
    /// retirement injects `Dropped` into them.
    pub(crate) fn stop_sending(&self) {
        self.core.tasks.stop();
    }

    pub(crate) async fn shutdown(&self) {
        self.core.tasks.stop();
        self.core.tasks.wait().await;
        self.core.batchers.clear();
        close_ingress(&self.core.ingress, self.core.metrics.as_ref());
        self.core.drains.clear();
    }
}

impl MuxCore {
    /// The batcher for one (peer, lane), created on first use.
    pub(super) fn batcher(&self, key: PeerLane) -> Result<Arc<BatcherHandle>> {
        if self.tasks.is_stopped() {
            return Err(anyhow!("messenger mux shut down"));
        }
        if let Some(existing) = self.batchers.get(&key) {
            return self.checked_batcher(key, existing.value());
        }
        let entry = self.batchers.entry(key).or_insert_with(|| {
            peer_batcher::spawn(
                key,
                BatcherContext {
                    tasks: self.tasks.clone(),
                    messenger: Arc::clone(&self.messenger),
                    config: self.config.clone(),
                    metrics: self.metrics.clone(),
                    epochs: Arc::clone(&self.epochs),
                    batchers: Arc::clone(&self.batchers),
                    ingress: Arc::clone(&self.ingress),
                    cancel: self.tasks.cancellation_token(),
                    #[cfg(test)]
                    hooks: self.hooks.get().cloned(),
                },
            )
        });
        self.checked_batcher(key, entry.value())
    }

    /// The caller holds the registry guard, so a closed handle here cannot
    /// have been removed by normal retirement between lookup and validation.
    fn checked_batcher(
        &self,
        key: PeerLane,
        handle: &Arc<BatcherHandle>,
    ) -> Result<Arc<BatcherHandle>> {
        // Spawn on a stopped runtime can drop the batcher immediately. A
        // closed registered handle is a failed epoch, not normal retirement.
        if self.tasks.is_stopped() || handle.is_closed() {
            if stop_mux(&self.tasks, &self.ingress, self.metrics.as_ref()) {
                tracing::error!(peer = %key.peer, lane = %key.lane,
                    "messenger mux stopped: registered batcher closed unexpectedly");
            }
            return Err(anyhow!("messenger mux batcher shut down"));
        }
        Ok(Arc::clone(handle))
    }
}

/// Stop the mux's tasks and close every ingress slot. Returns true only for
/// the first stop request.
pub(super) fn stop_mux(
    tasks: &crate::streaming::tasks::StreamTasks,
    ingress: &IngressRegistry,
    metrics: Option<&MuxMetricsHandle>,
) -> bool {
    let first = tasks.stop();
    close_ingress(ingress, metrics);
    first
}

pub(super) fn close_ingress(ingress: &IngressRegistry, metrics: Option<&MuxMetricsHandle>) {
    let closed = ingress.shutdown();
    if let Some(metrics) = metrics {
        for _ in 0..closed {
            metrics.slot_closed();
        }
    }
}

impl Drop for MuxCore {
    fn drop(&mut self) {
        stop_mux(&self.tasks, &self.ingress, self.metrics.as_ref());
    }
}
