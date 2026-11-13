// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! [`RdmaBackend`] over the NIXL driver.
//!
//! This side holds no NIXL state. It mints region ids, puts commands on the
//! driver's channel, and awaits the answers. See `driver.rs` for what happens
//! to them.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::future::BoxFuture;
use tokio::sync::oneshot;
use velo_ext::InstanceId;

use super::agent::NixlAgent;
use super::driver::{self, Cmd, DriverConfig, Shared};
use crate::rendezvous::rdma::backend::{BackendGet, BackendRegion, RdmaBackend, RdmaError};

/// Commands the channel holds before a caller waits. The bound is the
/// backpressure, as the UCX transport's ring is for its RMA commands.
const RING: usize = 1024;

/// The NIXL backend. One per instance, like every RDMA backend.
pub(crate) struct NixlBackend {
    tx: flume::Sender<Cmd>,
    shared: Arc<Shared>,
    /// Region ids. Each is also the NIXL device id its region is registered
    /// under, so an id is never used twice (see `peers.rs`).
    next_id: AtomicU64,
}

impl NixlBackend {
    /// Start the driver thread, and create the agent on it with `make`.
    ///
    /// Resolves once the agent exists and has given its connection info, so a
    /// build that cannot reach NIXL fails here and not at the first transfer.
    pub(crate) async fn start<A, F>(make: F, cfg: DriverConfig) -> Result<Arc<Self>, RdmaError>
    where
        A: NixlAgent,
        F: FnOnce() -> Result<A, String> + Send + 'static,
    {
        let (tx, rx) = flume::bounded(RING);
        let shared = Arc::new(Shared::default());
        let (ready_tx, ready_rx) = oneshot::channel::<Result<(), String>>();
        let thread_shared = Arc::clone(&shared);
        std::thread::Builder::new()
            .name("velo-nixl".into())
            .spawn(move || {
                // A panic here drops `ready_tx`, and `start` reports that.
                let started = make().and_then(|mut agent| {
                    let conn = agent.conn_blob()?;
                    Ok((agent, conn))
                });
                match started {
                    Ok((agent, conn)) => {
                        let _ = ready_tx.send(Ok(()));
                        driver::run(agent, conn, rx, cfg, thread_shared);
                    }
                    Err(e) => {
                        let _ = ready_tx.send(Err(e));
                    }
                }
            })
            .map_err(|e| {
                RdmaError::Backend(format!("nixl: could not start the driver thread: {e}"))
            })?;

        match ready_rx.await {
            Ok(Ok(())) => Ok(Arc::new(Self {
                tx,
                shared,
                // Zero is the device id of an ordinary host registration in
                // NIXL. Start above it, so no velo region shares an id with
                // memory that something else registered with the same agent.
                next_id: AtomicU64::new(1),
            })),
            Ok(Err(detail)) => Err(RdmaError::Backend(detail)),
            Err(_) => Err(RdmaError::Backend(
                "nixl: the driver thread exited while it started".to_string(),
            )),
        }
    }

    async fn send(&self, cmd: Cmd) -> Result<(), RdmaError> {
        // The receiver lives until this backend is dropped, so a failure here
        // is a driver thread that died.
        self.tx
            .send_async(cmd)
            .await
            .map_err(|_| RdmaError::ShuttingDown)
    }
}

/// Rolls a `map` back if its future is dropped before it returns.
///
/// The driver rolls back on its own when it finds the reply channel closed.
/// That does not cover a caller that goes away after the driver has answered
/// and before the answer is taken, so this guard covers it. The two can both
/// fire: an unmap of a region that is already gone is a no-op.
struct MapRollback<'a> {
    tx: &'a flume::Sender<Cmd>,
    id: u64,
    armed: bool,
}

impl Drop for MapRollback<'_> {
    fn drop(&mut self) {
        if self.armed
            && self
                .tx
                .try_send(Cmd::Unmap {
                    id: self.id,
                    reply: None,
                })
                .is_err()
        {
            tracing::error!(
                region = self.id,
                "nixl: could not roll back a cancelled registration. The region stays \
                 registered until teardown."
            );
        }
    }
}

impl RdmaBackend for NixlBackend {
    fn key(&self) -> &str {
        "nixl"
    }

    fn map(&self, ptr: usize, len: usize) -> BoxFuture<'_, Result<BackendRegion, RdmaError>> {
        Box::pin(async move {
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            let (reply, answer) = oneshot::channel();
            self.send(Cmd::Map {
                id,
                ptr,
                len,
                reply,
            })
            .await?;
            let mut rollback = MapRollback {
                tx: &self.tx,
                id,
                armed: true,
            };
            let result = answer.await.unwrap_or_else(|_| {
                Err(RdmaError::Backend(
                    "nixl: the driver failed during a registration".to_string(),
                ))
            });
            // No await is left: the caller gets this result.
            rollback.armed = false;
            result
        })
    }

    fn unmap(&self, backend_region_id: u64) -> BoxFuture<'_, Result<(), RdmaError>> {
        Box::pin(async move {
            let (reply, answer) = oneshot::channel();
            self.send(Cmd::Unmap {
                id: backend_region_id,
                reply: Some(reply),
            })
            .await?;
            // No answer is "unknown", and unknown is not "unmapped".
            answer.await.unwrap_or_else(|_| {
                Err(RdmaError::Backend(
                    "nixl: the driver failed during a deregistration".to_string(),
                ))
            })
        })
    }

    fn get(&self, req: BackendGet) -> BoxFuture<'_, Result<(), RdmaError>> {
        Box::pin(async move {
            let (reply, answer) = oneshot::channel();
            self.send(Cmd::Get { req, reply }).await?;
            // The driver owns the transfer from here, whether or not this
            // future is still awaited. If the driver dies with the transfer in
            // flight, nobody can say that the transfer stopped.
            answer.await.unwrap_or(Err(RdmaError::Unconfirmed))
        })
    }

    /// Regions the agent holds, plus one for good once the agent holds
    /// something that cannot be released. Deregistration is checked in the
    /// driver before the count goes down.
    fn live_registrations(&self) -> Option<usize> {
        let stuck = usize::from(self.shared.stuck.load(Ordering::Acquire));
        Some(self.shared.live.load(Ordering::Acquire) + stuck)
    }

    fn teardown(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let (reply, done) = oneshot::channel();
            if self.tx.send_async(Cmd::Teardown { reply }).await.is_ok() {
                let _ = done.await;
            }
        })
    }

    fn forget_peer(&self, peer: InstanceId) {
        // Best effort. A peer that is not forgotten here stays loaded until
        // the per-peer cap drops it.
        let _ = self.tx.try_send(Cmd::ForgetPeer { peer });
    }
}
