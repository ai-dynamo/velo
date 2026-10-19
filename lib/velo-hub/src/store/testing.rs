// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! [`FaultStore`]: an [`InMemoryStore`] that fails, stalls, or answers
//! "indeterminate" on request, for testing the hub's failure paths. With no
//! fault armed it is a plain in-memory store, and it passes the conformance
//! suite in that mode, so a test failure under a fault is the hub's, not the
//! fixture's.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::future::BoxFuture;
use parking_lot::Mutex;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use velo::{InstanceId, PeerDiscovery, PeerInfo, WorkerId};

use super::{
    Continuity, HubStore, InMemoryStore, Incarnation, OpId, RegistrationRecord, RemovalHook,
    StoreError, StoredRegistration,
};

/// A point an operation stops at until the test releases it.
#[derive(Debug)]
pub struct Gate {
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}

impl Default for Gate {
    fn default() -> Self {
        Self {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
        }
    }
}

impl Gate {
    /// Wait until the gated operation has reached the gate.
    pub async fn entered(&self) {
        self.entered.notified().await;
    }

    /// Let the gated operation continue.
    pub fn release(&self) {
        self.release.add_permits(1);
    }

    /// Called by the gated operation: announce arrival, then wait for
    /// [`release`](Self::release).
    pub async fn pass(&self) {
        self.entered.notify_one();
        if let Ok(permit) = self.release.acquire().await {
            permit.forget();
        }
    }
}

/// What the next put does instead of a plain write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutFault {
    /// Write nothing; answer [`StoreError::Backend`].
    Fail,
    /// Write nothing; answer [`StoreError::Indeterminate`].
    IndeterminateNotWritten,
    /// Write; answer [`StoreError::Indeterminate`] (a lost response).
    IndeterminateWritten,
}

/// An [`InMemoryStore`] with injectable faults. See the module docs.
pub struct FaultStore {
    inner: Arc<InMemoryStore>,
    next_put: Mutex<Option<PutFault>>,
    put_gate: Mutex<Option<Arc<Gate>>>,
    touch_gate: Mutex<Option<Arc<Gate>>>,
    remove_gate: Mutex<Option<Arc<Gate>>>,
    resolve_gate: Mutex<Option<Arc<Gate>>>,
    stubborn_liveness: AtomicBool,
    fail_next_remove: AtomicBool,
    fail_next_resolve: AtomicBool,
    continuity: watch::Sender<Continuity>,
}

impl Default for FaultStore {
    fn default() -> Self {
        Self::new(InMemoryStore::default())
    }
}

impl FaultStore {
    /// Wrap `inner`.
    pub fn new(inner: InMemoryStore) -> Self {
        Self {
            inner: Arc::new(inner),
            next_put: Mutex::new(None),
            put_gate: Mutex::new(None),
            touch_gate: Mutex::new(None),
            remove_gate: Mutex::new(None),
            resolve_gate: Mutex::new(None),
            stubborn_liveness: AtomicBool::new(false),
            fail_next_remove: AtomicBool::new(false),
            fail_next_resolve: AtomicBool::new(false),
            continuity: watch::Sender::new(Continuity::Live),
        }
    }

    /// The wrapped store, e.g. to call [`InMemoryStore::prune_stale`].
    pub fn inner(&self) -> &InMemoryStore {
        &self.inner
    }

    /// The next put applies `fault`.
    pub fn fault_next_put(&self, fault: PutFault) {
        *self.next_put.lock() = Some(fault);
    }

    /// The next put stops at the returned gate before it writes.
    pub fn gate_next_put(&self) -> Arc<Gate> {
        let gate = Arc::new(Gate::default());
        *self.put_gate.lock() = Some(Arc::clone(&gate));
        gate
    }

    /// The next touch stops at the returned gate before it checks.
    pub fn gate_next_touch(&self) -> Arc<Gate> {
        let gate = Arc::new(Gate::default());
        *self.touch_gate.lock() = Some(Arc::clone(&gate));
        gate
    }

    /// The next remove stops at the returned gate before it checks.
    pub fn gate_next_remove(&self) -> Arc<Gate> {
        let gate = Arc::new(Gate::default());
        *self.remove_gate.lock() = Some(Arc::clone(&gate));
        gate
    }

    /// The next resolve stops at the returned gate.
    pub fn gate_next_resolve(&self) -> Arc<Gate> {
        let gate = Arc::new(Gate::default());
        *self.resolve_gate.lock() = Some(Arc::clone(&gate));
        gate
    }

    /// The liveness task this store spawns ignores its cancellation, as a
    /// broken backend's would. Set before the hub starts.
    pub fn stubborn_liveness(&self) {
        self.stubborn_liveness.store(true, Ordering::Release);
    }

    /// The next remove fails with [`StoreError::Backend`], removing nothing.
    pub fn fail_next_remove(&self) {
        self.fail_next_remove.store(true, Ordering::Release);
    }

    /// The next resolve fails with [`StoreError::Backend`].
    pub fn fail_next_resolve(&self) {
        self.fail_next_resolve.store(true, Ordering::Release);
    }

    /// Expire the instance now, as a backend lease or TTL would: the entry
    /// goes and the removal hook hears it. No-op if it has no entry.
    pub fn expire(&self, id: InstanceId) {
        if let Some(incarnation) = self.inner.current_incarnation(id) {
            let _ = futures::executor::block_on(self.inner.remove(id, incarnation));
        }
    }

    /// Report a continuity change, as a remote backend's watch would.
    pub fn set_continuity(&self, continuity: Continuity) {
        self.continuity.send_replace(continuity);
    }
}

impl PeerDiscovery for FaultStore {
    fn discover_by_worker_id(
        &self,
        worker_id: WorkerId,
    ) -> BoxFuture<'_, anyhow::Result<PeerInfo>> {
        self.inner.discover_by_worker_id(worker_id)
    }

    fn discover_by_instance_id(&self, id: InstanceId) -> BoxFuture<'_, anyhow::Result<PeerInfo>> {
        self.inner.discover_by_instance_id(id)
    }
}

impl HubStore for FaultStore {
    fn put(
        &self,
        record: RegistrationRecord,
        expected: Option<Incarnation>,
    ) -> BoxFuture<'_, Result<Incarnation, StoreError>> {
        let gate = self.put_gate.lock().take();
        let fault = self.next_put.lock().take();
        Box::pin(async move {
            if let Some(gate) = gate {
                gate.pass().await;
            }
            match fault {
                None => self.inner.put(record, expected).await,
                Some(PutFault::Fail) => {
                    Err(StoreError::Backend(anyhow::anyhow!("injected put failure")))
                }
                Some(PutFault::IndeterminateNotWritten) => Err(StoreError::Indeterminate(
                    "injected: request lost before it applied".to_string(),
                )),
                Some(PutFault::IndeterminateWritten) => {
                    self.inner.put(record, expected).await?;
                    Err(StoreError::Indeterminate(
                        "injected: response lost after it applied".to_string(),
                    ))
                }
            }
        })
    }

    fn remove(
        &self,
        id: InstanceId,
        incarnation: Incarnation,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        if self.fail_next_remove.swap(false, Ordering::AcqRel) {
            return Box::pin(async {
                Err(StoreError::Backend(anyhow::anyhow!(
                    "injected remove failure"
                )))
            });
        }
        let gate = self.remove_gate.lock().take();
        Box::pin(async move {
            if let Some(gate) = gate {
                gate.pass().await;
            }
            self.inner.remove(id, incarnation).await
        })
    }

    fn touch(
        &self,
        id: InstanceId,
        incarnation: Incarnation,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        let gate = self.touch_gate.lock().take();
        Box::pin(async move {
            if let Some(gate) = gate {
                gate.pass().await;
            }
            self.inner.touch(id, incarnation).await
        })
    }

    fn resolve(
        &self,
        id: InstanceId,
        op: OpId,
    ) -> BoxFuture<'_, Result<Option<Incarnation>, StoreError>> {
        if self.fail_next_resolve.swap(false, Ordering::AcqRel) {
            return Box::pin(async {
                Err(StoreError::Backend(anyhow::anyhow!(
                    "injected resolve failure"
                )))
            });
        }
        let gate = self.resolve_gate.lock().take();
        Box::pin(async move {
            if let Some(gate) = gate {
                gate.pass().await;
            }
            self.inner.resolve(id, op).await
        })
    }

    fn get(&self, id: InstanceId) -> Option<StoredRegistration> {
        self.inner.get(id)
    }

    fn current_incarnation(&self, id: InstanceId) -> Option<Incarnation> {
        self.inner.current_incarnation(id)
    }

    fn list(&self) -> Vec<StoredRegistration> {
        self.inner.list()
    }

    fn peers(&self) -> Vec<super::StoredPeer> {
        self.inner.peers()
    }

    fn install_removal_hook(
        &self,
        hook: RemovalHook,
    ) -> Result<Vec<StoredRegistration>, StoreError> {
        self.inner.install_removal_hook(hook)
    }

    fn continuity(&self) -> watch::Receiver<Continuity> {
        self.continuity.subscribe()
    }

    fn spawn_liveness_task(self: Arc<Self>, cancel: CancellationToken) -> Option<JoinHandle<()>> {
        if self.stubborn_liveness.load(Ordering::Acquire) {
            return Some(tokio::spawn(std::future::pending()));
        }
        Arc::clone(&self.inner).spawn_liveness_task(cancel)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// With no fault armed the fixture is a faithful backend.
    #[tokio::test]
    async fn fault_store_passes_the_conformance_suite() {
        crate::store::conformance::run_all(|| Arc::new(FaultStore::default())).await;
    }
}
