// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! How the mux starts batchers and how it stops.
//!
//! A mux that fails stops in one step: `stop_mux` stops its tasks and retires
//! its slots. Shutdown uses two steps, so that streams can come off their
//! slots in between: `stop_sending` stops the tasks, and `shutdown` retires
//! the slots after joining tasks. Final owner Drop uses `stop` to retire slots
//! without waiting. If a shutdown was abandoned, `MuxCore::drop` retires them.

use std::sync::Arc;

use anyhow::{Result, anyhow};

use super::ingress::IngressRegistry;
use super::peer_batcher::{self, BatcherContext, BatcherHandle};
use super::{MessengerMuxTransport, MuxCore, PeerLane};
use crate::observability::MuxMetricsHandle;

impl MessengerMuxTransport {
    /// Request that the mux's tasks stop, but leave its slots open:
    /// [`Self::shutdown`] retires them. Shutdown calls
    /// this first so streams can be detached from their slots before
    /// retirement injects `Dropped` into them.
    pub(crate) fn stop_sending(&self) {
        self.core.tasks.stop();
    }

    /// Join running sends before messenger teardown, without retiring ingress.
    pub(crate) async fn stop_sending_and_wait(&self) {
        self.stop_sending();
        self.core.tasks.wait().await;
    }

    pub(crate) async fn shutdown(&self) {
        self.stop_sending_and_wait().await;
        self.stop();
    }

    /// Retire slots after the owner has detached its streams, without waiting.
    pub(crate) fn stop(&self) {
        self.stop_sending();
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

/// Stop a mux that failed: stop its tasks and close every ingress slot.
/// Returns true only for the first stop request.
///
/// A stop that was not the first leaves the slots alone. Shutdown stops the
/// tasks first and retires the slots only after it has taken every stream off
/// them; a batcher its stop drops mid-wake ends here, and closing the slots
/// now would hand a live reader `Dropped` as its sender's.
pub(super) fn stop_mux(
    tasks: &crate::streaming::tasks::StreamTasks,
    ingress: &IngressRegistry,
    metrics: Option<&MuxMetricsHandle>,
) -> bool {
    let first = tasks.stop();
    if first {
        close_ingress(ingress, metrics);
    }
    first
}

fn close_ingress(ingress: &IngressRegistry, metrics: Option<&MuxMetricsHandle>) {
    let closed = ingress.shutdown();
    if let Some(metrics) = metrics {
        for _ in 0..closed {
            metrics.slot_closed();
        }
    }
}

impl Drop for MuxCore {
    fn drop(&mut self) {
        // Unconditional: a shutdown abandoned after `stop_sending` left the
        // slots open.
        self.tasks.stop();
        close_ingress(&self.ingress, self.metrics.as_ref());
    }
}
