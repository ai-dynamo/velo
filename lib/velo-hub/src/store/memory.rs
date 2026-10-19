// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The in-memory store: maps behind one `parking_lot::RwLock`, and a TTL
//! reaper task.
//!
//! Writes take the write lock; they are control-plane rare. The removal
//! hook is always called after the lock is released, so a hook may read
//! the store. Discovery lookups, which are on the request path, read two
//! sharded maps of peers that each write updates while it holds the lock:
//! one shard read lock and a clone of the `PeerInfo`. Measured by
//! `store::memory::tests::lookup_cost` (release, 10,000 peers, 20-core
//! host): see `docs/src/concepts/hub.md`. `get` and `list` copy whole
//! records, payloads included, and are not on the request path.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use dashmap::DashMap;
use futures::future::BoxFuture;
use parking_lot::RwLock;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use velo::{InstanceId, PeerDiscovery, PeerInfo, WorkerId};

use super::{
    Continuity, HubStore, Incarnation, OpId, RegistrationRecord, Removal, RemovalHook, StoreError,
    StoredPeer, StoredRegistration,
};

/// Default time after which an entry that was not touched expires.
pub const DEFAULT_TTL: Duration = Duration::from_secs(30);

/// Default interval between reaper passes.
pub const DEFAULT_PRUNE_INTERVAL: Duration = Duration::from_secs(10);

/// The in-memory [`HubStore`]. Starts empty on every process start.
pub struct InMemoryStore {
    inner: RwLock<Inner>,
    /// Read views for discovery, written only under `inner`'s write lock.
    peers: DashMap<InstanceId, PeerInfo>,
    workers: DashMap<WorkerId, PeerInfo>,
    ttl: Duration,
    prune_interval: Duration,
    continuity: watch::Sender<Continuity>,
}

#[derive(Default)]
struct Inner {
    by_instance: HashMap<InstanceId, StoredRegistration>,
    by_worker: HashMap<WorkerId, InstanceId>,
    last_seen: HashMap<InstanceId, Instant>,
    last_incarnation: u64,
    hook: Option<RemovalHook>,
}

impl InMemoryStore {
    /// Remove `id` from every map. Call with the write lock held.
    fn remove_entry(&self, inner: &mut Inner, id: InstanceId) -> Option<StoredRegistration> {
        let stored = inner.by_instance.remove(&id)?;
        inner.by_worker.retain(|_, owner| *owner != id);
        inner.last_seen.remove(&id);
        self.peers.remove(&id);
        self.workers.retain(|_, peer| peer.instance_id() != id);
        Some(stored)
    }
}

impl std::fmt::Debug for InMemoryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryStore")
            .field("entries", &self.inner.read().by_instance.len())
            .field("ttl", &self.ttl)
            .field("prune_interval", &self.prune_interval)
            .finish()
    }
}

impl Default for InMemoryStore {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl InMemoryStore {
    /// Builder with the default TTL and prune interval.
    pub fn builder() -> InMemoryStoreBuilder {
        InMemoryStoreBuilder::default()
    }

    /// Time after which an entry that was not touched expires.
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Interval between reaper passes.
    pub fn prune_interval(&self) -> Duration {
        self.prune_interval
    }

    /// Expire every unpinned entry older than the TTL now. The reaper calls
    /// this on each tick; tests call it directly.
    pub fn prune_stale(&self) {
        let (removals, hook) = {
            let mut inner = self.inner.write();
            let now = Instant::now();
            let stale: Vec<InstanceId> = inner
                .last_seen
                .iter()
                .filter(|(id, seen)| {
                    now.saturating_duration_since(**seen) > self.ttl
                        && !inner
                            .by_instance
                            .get(*id)
                            .is_some_and(|stored| stored.record.pinned)
                })
                .map(|(id, _)| *id)
                .collect();
            let removals: Vec<Removal> = stale
                .into_iter()
                .filter_map(|id| {
                    self.remove_entry(&mut inner, id).map(|stored| Removal {
                        id,
                        incarnation: stored.incarnation,
                    })
                })
                .collect();
            (removals, inner.hook.clone())
        };
        if let Some(hook) = hook {
            for removal in removals {
                hook(removal);
            }
        }
    }

    fn put_now(
        &self,
        record: RegistrationRecord,
        expected: Option<Incarnation>,
    ) -> Result<Incarnation, StoreError> {
        let id = record.peer.instance_id();
        let worker = record.peer.worker_id();
        let mut inner = self.inner.write();
        let current = inner.by_instance.get(&id).map(|stored| stored.incarnation);
        if current != expected {
            return Err(StoreError::Stale {
                id,
                expected,
                current,
            });
        }
        if let Some(existing) = inner.by_worker.get(&worker).copied()
            && existing != id
        {
            return Err(StoreError::Conflict {
                worker_id: worker,
                existing,
            });
        }
        let next = inner
            .last_incarnation
            .checked_add(1)
            .ok_or_else(|| StoreError::Backend(anyhow!("incarnation counter exhausted")))?;
        inner.last_incarnation = next;
        let incarnation = Incarnation::from_u64(next);
        // A replacement drops every worker claim the earlier incarnation
        // held. WorkerId derives from InstanceId today, but clearing by owner
        // keeps the index right if that ever changes.
        inner.by_worker.retain(|_, owner| *owner != id);
        inner.by_worker.insert(worker, id);
        self.workers.retain(|_, peer| peer.instance_id() != id);
        self.workers.insert(worker, record.peer.clone());
        self.peers.insert(id, record.peer.clone());
        inner.by_instance.insert(
            id,
            StoredRegistration {
                record,
                incarnation,
            },
        );
        inner.last_seen.insert(id, Instant::now());
        Ok(incarnation)
    }

    fn remove_now(&self, id: InstanceId, incarnation: Incarnation) -> Result<(), StoreError> {
        let hook = {
            let mut inner = self.inner.write();
            let current = inner
                .by_instance
                .get(&id)
                .ok_or(StoreError::NotFound(id))?
                .incarnation;
            if current != incarnation {
                return Err(StoreError::Stale {
                    id,
                    expected: Some(incarnation),
                    current: Some(current),
                });
            }
            self.remove_entry(&mut inner, id);
            inner.hook.clone()
        };
        if let Some(hook) = hook {
            hook(Removal { id, incarnation });
        }
        Ok(())
    }

    fn touch_now(&self, id: InstanceId, incarnation: Incarnation) -> Result<(), StoreError> {
        let mut inner = self.inner.write();
        let current = inner
            .by_instance
            .get(&id)
            .ok_or(StoreError::NotFound(id))?
            .incarnation;
        if current != incarnation {
            return Err(StoreError::Stale {
                id,
                expected: Some(incarnation),
                current: Some(current),
            });
        }
        inner.last_seen.insert(id, Instant::now());
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn insert_worker_claim(&self, worker: WorkerId, owner: InstanceId) {
        self.inner.write().by_worker.insert(worker, owner);
    }

    #[cfg(test)]
    pub(crate) fn worker_claims_of(&self, owner: InstanceId) -> Vec<WorkerId> {
        self.inner
            .read()
            .by_worker
            .iter()
            .filter_map(|(worker, id)| (*id == owner).then_some(*worker))
            .collect()
    }
}

/// Builder for [`InMemoryStore`].
#[derive(Debug, Clone)]
pub struct InMemoryStoreBuilder {
    ttl: Duration,
    prune_interval: Duration,
}

impl Default for InMemoryStoreBuilder {
    fn default() -> Self {
        Self {
            ttl: DEFAULT_TTL,
            prune_interval: DEFAULT_PRUNE_INTERVAL,
        }
    }
}

impl InMemoryStoreBuilder {
    /// Time after which an entry that was not touched expires (default 30s).
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// Interval between reaper passes (default 10s).
    pub fn prune_interval(mut self, interval: Duration) -> Self {
        self.prune_interval = interval;
        self
    }

    /// The store.
    pub fn build(self) -> InMemoryStore {
        InMemoryStore {
            inner: RwLock::new(Inner::default()),
            peers: DashMap::new(),
            workers: DashMap::new(),
            ttl: self.ttl,
            prune_interval: self.prune_interval,
            continuity: watch::Sender::new(Continuity::Live),
        }
    }
}

impl PeerDiscovery for InMemoryStore {
    fn discover_by_worker_id(
        &self,
        worker_id: WorkerId,
    ) -> BoxFuture<'_, anyhow::Result<PeerInfo>> {
        let result = self
            .workers
            .get(&worker_id)
            .map(|peer| peer.clone())
            .ok_or_else(|| anyhow!("worker {} not registered", worker_id.as_u64()));
        Box::pin(async move { result })
    }

    fn discover_by_instance_id(&self, id: InstanceId) -> BoxFuture<'_, anyhow::Result<PeerInfo>> {
        let result = self
            .peers
            .get(&id)
            .map(|peer| peer.clone())
            .ok_or_else(|| anyhow!("instance {id} not registered"));
        Box::pin(async move { result })
    }
}

impl HubStore for InMemoryStore {
    fn put(
        &self,
        record: RegistrationRecord,
        expected: Option<Incarnation>,
    ) -> BoxFuture<'_, Result<Incarnation, StoreError>> {
        let result = self.put_now(record, expected);
        Box::pin(async move { result })
    }

    fn remove(
        &self,
        id: InstanceId,
        incarnation: Incarnation,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        let result = self.remove_now(id, incarnation);
        Box::pin(async move { result })
    }

    fn touch(
        &self,
        id: InstanceId,
        incarnation: Incarnation,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        let result = self.touch_now(id, incarnation);
        Box::pin(async move { result })
    }

    fn resolve(
        &self,
        id: InstanceId,
        op: OpId,
    ) -> BoxFuture<'_, Result<Option<Incarnation>, StoreError>> {
        let result = self
            .inner
            .read()
            .by_instance
            .get(&id)
            .filter(|stored| stored.record.op == op)
            .map(|stored| stored.incarnation);
        Box::pin(async move { Ok(result) })
    }

    fn get(&self, id: InstanceId) -> Option<StoredRegistration> {
        self.inner.read().by_instance.get(&id).cloned()
    }

    fn current_incarnation(&self, id: InstanceId) -> Option<Incarnation> {
        self.inner
            .read()
            .by_instance
            .get(&id)
            .map(|stored| stored.incarnation)
    }

    fn list(&self) -> Vec<StoredRegistration> {
        self.inner.read().by_instance.values().cloned().collect()
    }

    fn peers(&self) -> Vec<StoredPeer> {
        self.inner
            .read()
            .by_instance
            .values()
            .map(|stored| StoredPeer {
                peer: stored.record.peer.clone(),
                incarnation: stored.incarnation,
                pinned: stored.record.pinned,
            })
            .collect()
    }

    fn install_removal_hook(
        &self,
        hook: RemovalHook,
    ) -> Result<Vec<StoredRegistration>, StoreError> {
        let mut inner = self.inner.write();
        if inner.hook.is_some() {
            return Err(StoreError::HookInstalled);
        }
        inner.hook = Some(hook);
        Ok(inner.by_instance.values().cloned().collect())
    }

    fn continuity(&self) -> watch::Receiver<Continuity> {
        self.continuity.subscribe()
    }

    fn spawn_liveness_task(self: Arc<Self>, cancel: CancellationToken) -> Option<JoinHandle<()>> {
        let interval = self.prune_interval;
        Some(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = ticker.tick() => self.prune_stale(),
                }
            }
        }))
    }
}

#[cfg(test)]
mod tests;
