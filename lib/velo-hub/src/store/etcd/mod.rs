// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The etcd store (feature `etcd`): registrations that outlive the hub
//! process, so the next hub on the prefix can take them over.
//!
//! # Keys
//!
//! Under the prefix `P` (default [`DEFAULT_PREFIX`]):
//!
//! | Key | Holds | Lease |
//! |---|---|---|
//! | `P/i/{instance}` | the [`RegistrationRecord`] as JSON | custody |
//! | `P/hub` | the lock: one hub per prefix | lock |
//! | `P/op/{op}` | a tombstone written by `resolve` | its own, short |
//! | `P/b` | the barrier key, written by every failed txn | none |
//!
//! An entry's [`Incarnation`] is its key's `mod_revision`: etcd revisions
//! only grow, so a later put always has a greater one.
//!
//! # Liveness: one custody lease, and a reaper
//!
//! Every record is written with the store's custody lease (default 5 min),
//! which the store keeps alive. A registrant's liveness is not a lease:
//! `touch` sets a timestamp in the cache after checking the incarnation (a
//! late touch cannot refresh a replacement), and the store's reaper deletes
//! an unpinned entry nobody touched for the registrant TTL with a delete
//! conditional on its incarnation. A lease per registrant would cost a
//! keep-alive stream per touch (etcd-client binds a keep-alive stream to one
//! lease), a grant and a revoke per put, and would expire a healthy
//! registrant after one slow heartbeat.
//!
//! When a hub dies, its records stay for the custody TTL. The next hub on
//! the prefix keeps alive every custody lease its loaded records name,
//! until no record names it, and deletes the dead hub's pinned entry, which
//! nobody would touch again.
//!
//! # Fencing
//!
//! The lock key `P/hub` is on a short lease of its own (default 10 s),
//! revoked by [`EtcdStore::close`]. A second store on the prefix fails to
//! connect ([`ConnectError::PrefixHeld`]). Every txn also compares the lock
//! key's lease with this store's, so a store that lost the lock during a
//! partition, while another hub took the prefix, cannot write: the failed
//! compare, the watch seeing the lock key leave, or a keep-alive finding
//! the lease gone puts the store in [`Continuity::Fenced`] for good.
//! Each of these is an answer from etcd. A store cut off from etcd is
//! `Live` until its watch breaks (up to the 5 s connection keep-alive plus
//! the request deadline when packets are dropped), then `Degraded` until it
//! reaches etcd again, and is fenced then. Its lock can expire before the
//! watch breaks.
//!
//! # The barrier
//!
//! Reads come from a cache the prefix watch feeds. The hub relies on a
//! call's removals reaching the hook before the call returns (see
//! [`HubStore`]), so each `put`, `remove` and `resolve` waits until the
//! watch has applied the call's txn revision and run the hook for it. A
//! txn whose compare fails writes the barrier key, so it too has a revision
//! the watch reports, and the wait does not depend on other traffic. A wait
//! that runs past the request deadline answers
//! [`StoreError::Indeterminate`] and marks the store
//! [`Continuity::Degraded`] until the watch catches up.
//!
//! A broken watch resumes from the last applied revision + 1, so no
//! removal is reported twice. A watch behind compaction reports
//! [`Continuity::Lost`], reloads the prefix, reports each entry that
//! vanished in the gap as a removal, then reports `Live`.
//!
//! # Limits
//!
//! - The request deadline is client-side only: tonic's `Endpoint::timeout`
//!   does not send `grpc-timeout`, and etcd-client exposes no per-request
//!   deadline. A txn that reached etcd can apply after its call answered
//!   `Indeterminate`. `resolve`'s tombstone stops such a put for as long as
//!   the tombstone lives (twice the deadline, at least etcd's minimum TTL);
//!   a put held longer than that, in a client or a proxy, could still
//!   apply.
//! - etcd's auto-compaction on a quiet prefix can make a reconnecting
//!   watch report `Lost` and reload, though nothing in the prefix changed.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use anyhow::anyhow;
use etcd_client::{Compare, CompareOp, PutOptions, Txn, TxnOp};
use futures::future::BoxFuture;
use parking_lot::Mutex;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use velo::{InstanceId, PeerDiscovery, PeerInfo, WorkerId};

use super::{
    Continuity, HubStore, Incarnation, OpId, RegistrationRecord, RemovalHook, StoreError,
    StoredPeer, StoredRegistration,
};
use shared::{Shared, incarnation, revision};

mod builder;
mod hold;
mod shared;
mod tasks;

pub use builder::{
    ConnectError, DEFAULT_CUSTODY_TTL, DEFAULT_LOCK_TTL, DEFAULT_PREFIX, DEFAULT_PRUNE_INTERVAL,
    DEFAULT_REGISTRANT_TTL, DEFAULT_REQUEST_DEADLINE, EtcdStoreBuilder,
};
/// The etcd client this store is built on, for [`ConnectOptions`] (TLS,
/// user) passed to [`EtcdStoreBuilder::connect_options`].
///
/// [`ConnectOptions`]: etcd_client::ConnectOptions
pub use etcd_client;
#[cfg(feature = "test-support")]
pub use hold::{WatchHold, WatchHoldGuard};

/// The etcd [`HubStore`]. Built by [`EtcdStore::builder`]; owns its tasks
/// (watch, keep-alives, reaper); its [`spawn_liveness_task`] only stops
/// the reaper when the hub stops its probes.
///
/// [`spawn_liveness_task`]: HubStore::spawn_liveness_task
pub struct EtcdStore {
    shared: Arc<Shared>,
    cancel: CancellationToken,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl std::fmt::Debug for EtcdStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EtcdStore")
            .field("prefix", &self.shared.keys.root)
            .field("entries", &self.shared.cache.read().entries.len())
            .finish_non_exhaustive()
    }
}

impl EtcdStore {
    /// A builder for a store on `endpoints`.
    pub fn builder<E: Into<String>>(endpoints: impl IntoIterator<Item = E>) -> EtcdStoreBuilder {
        EtcdStoreBuilder::new(endpoints.into_iter().map(Into::into).collect())
    }

    /// The key prefix, with its trailing `/`.
    pub fn prefix(&self) -> &str {
        &self.shared.keys.root
    }

    /// Run one reaper pass now and return how many entries it deleted. The
    /// reaper task calls this every prune interval; tests call it directly.
    pub async fn reap_stale(&self) -> usize {
        self.shared.reap_stale().await
    }

    /// Stop the store: later calls fail without reaching etcd, a call
    /// waiting on the watch returns its txn's outcome (its hook will not
    /// run), the tasks stop, and the lock lease is revoked so the next hub
    /// can start at once. The custody lease is left to the next hub, which
    /// is what keeps the registrations recoverable. Idempotent.
    pub async fn close(&self) {
        self.shared.closing.cancel();
        self.cancel.cancel();
        let deadline = self.shared.settings.request_deadline;
        // The lock first, and marked released only once the revoke
        // returned: if this future is dropped part-way (the hub bounds it),
        // `Drop` revokes again, and the next hub need not wait out the TTL.
        if !self.shared.lock_released.load(Ordering::Acquire) {
            builder::revoke(&self.shared.client, &[self.shared.lock_lease], deadline).await;
            self.shared.lock_released.store(true, Ordering::Release);
        }
        let tasks = std::mem::take(&mut *self.tasks.lock());
        for task in tasks {
            let abort = task.abort_handle();
            if tokio::time::timeout(deadline, task).await.is_err() {
                abort.abort();
            }
        }
    }

    /// Hold the watch until the guard drops (see [`WatchHold`]).
    #[cfg(feature = "test-support")]
    pub fn hold_watch(&self, hold: WatchHold) -> WatchHoldGuard {
        self.shared.holds.watch.send_replace(Some(hold));
        WatchHoldGuard {
            holds: self.shared.holds.watch.clone(),
        }
    }

    /// The next put stops at the returned gate before it sends its txn.
    #[cfg(feature = "test-support")]
    pub fn gate_next_put(&self) -> Arc<crate::store::testing::Gate> {
        let gate = Arc::new(crate::store::testing::Gate::default());
        *self.shared.holds.put_gate.lock() = Some(Arc::clone(&gate));
        gate
    }

    /// Every continuity the store reported, starting with `Live`.
    #[cfg(feature = "test-support")]
    pub fn continuity_history(&self) -> Vec<Continuity> {
        self.shared.holds.history()
    }
}

impl Drop for EtcdStore {
    fn drop(&mut self) {
        self.shared.closing.cancel();
        self.cancel.cancel();
        for task in self.tasks.get_mut().drain(..) {
            task.abort();
        }
        if !self.shared.lock_released.swap(true, Ordering::AcqRel)
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            let client = self.shared.client.clone();
            let lease = self.shared.lock_lease;
            let deadline = self.shared.settings.request_deadline;
            runtime.spawn(async move { builder::revoke(&client, &[lease], deadline).await });
        }
    }
}

impl PeerDiscovery for EtcdStore {
    fn discover_by_worker_id(
        &self,
        worker_id: WorkerId,
    ) -> BoxFuture<'_, anyhow::Result<PeerInfo>> {
        let result = self
            .shared
            .workers
            .get(&worker_id)
            .map(|peer| peer.clone())
            .ok_or_else(|| anyhow!("worker {} not registered", worker_id.as_u64()));
        Box::pin(async move { result })
    }

    fn discover_by_instance_id(&self, id: InstanceId) -> BoxFuture<'_, anyhow::Result<PeerInfo>> {
        let result = self
            .shared
            .peers
            .get(&id)
            .map(|peer| peer.clone())
            .ok_or_else(|| anyhow!("instance {id} not registered"));
        Box::pin(async move { result })
    }
}

impl HubStore for EtcdStore {
    /// The store runs its own tasks; this one only stops the reaper when the
    /// hub stops its probes, so a long stop does not delete registrations
    /// that nobody can refresh any more.
    fn spawn_liveness_task(self: Arc<Self>, cancel: CancellationToken) -> Option<JoinHandle<()>> {
        let shared = Arc::clone(&self.shared);
        Some(tokio::spawn(async move {
            cancel.cancelled().await;
            shared.reaper_stopped.cancel();
        }))
    }

    fn put(
        &self,
        record: RegistrationRecord,
        expected: Option<Incarnation>,
    ) -> BoxFuture<'_, Result<Incarnation, StoreError>> {
        Box::pin(async move {
            let shared = &self.shared;
            shared.ensure_open()?;
            let id = record.peer.instance_id();
            let worker = record.peer.worker_id();
            if let Some(existing) = shared.workers.get(&worker).map(|peer| peer.instance_id())
                && existing != id
            {
                return Err(StoreError::Conflict {
                    worker_id: worker,
                    existing,
                });
            }
            shared.holds.before_put().await;
            let deadline = tokio::time::Instant::now() + shared.settings.request_deadline;
            let key = shared.keys.instance(id);
            let op = record.op;
            let value = serde_json::to_vec(&record)
                .map_err(|error| StoreError::Backend(anyhow::Error::new(error)))?;
            let txn = Txn::new()
                .when([
                    shared.lock_compare(),
                    // An absent key compares as revision 0.
                    Compare::mod_revision(
                        key.as_str(),
                        CompareOp::Equal,
                        expected.map_or(0, revision),
                    ),
                    // `resolve` answered "not applied" for this op.
                    Compare::version(shared.keys.tombstone(op), CompareOp::Equal, 0),
                ])
                .and_then([TxnOp::put(
                    key.as_str(),
                    value,
                    Some(PutOptions::new().with_lease(shared.custody_lease)),
                )])
                .or_else(shared.failure_ops(&key));
            let outcome = match shared.call(txn, None, deadline, "put").await {
                Ok(outcome) => outcome,
                Err(StoreError::Backend(error))
                    if format!("{error:#}").contains("lease not found") =>
                {
                    shared.fence("put: the custody lease is gone");
                    return Err(StoreError::Backend(error));
                }
                Err(error) => return Err(error),
            };
            if outcome.succeeded {
                return Ok(incarnation(outcome.revision));
            }
            let current = outcome.instance.map(|kv| incarnation(kv.mod_revision()));
            if current != expected {
                return Err(StoreError::Stale {
                    id,
                    expected,
                    current,
                });
            }
            Err(StoreError::Backend(anyhow!(
                "op {op} was resolved as not applied; nothing was written"
            )))
        })
    }

    fn remove(
        &self,
        id: InstanceId,
        incarnation: Incarnation,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move {
            let shared = &self.shared;
            let deadline = tokio::time::Instant::now() + shared.settings.request_deadline;
            let key = shared.keys.instance(id);
            let txn = Txn::new()
                .when([
                    shared.lock_compare(),
                    Compare::mod_revision(key.as_str(), CompareOp::Equal, revision(incarnation)),
                ])
                .and_then([TxnOp::delete(key.as_str(), None)])
                .or_else(shared.failure_ops(&key));
            let outcome = shared.call(txn, None, deadline, "remove").await?;
            if outcome.succeeded {
                return Ok(());
            }
            match outcome.instance {
                None => Err(StoreError::NotFound(id)),
                Some(kv) => Err(StoreError::Stale {
                    id,
                    expected: Some(incarnation),
                    current: Some(shared::incarnation(kv.mod_revision())),
                }),
            }
        })
    }

    fn touch(
        &self,
        id: InstanceId,
        incarnation: Incarnation,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        let result = self
            .shared
            .ensure_open()
            .and_then(|()| self.shared.touch(id, incarnation));
        Box::pin(async move { result })
    }

    fn resolve(
        &self,
        id: InstanceId,
        op: OpId,
    ) -> BoxFuture<'_, Result<Option<Incarnation>, StoreError>> {
        Box::pin(async move {
            let shared = &self.shared;
            shared.ensure_open()?;
            let deadline = tokio::time::Instant::now() + shared.settings.request_deadline;
            let mut leases = shared.client.lease_client();
            let grant = leases.grant(shared.settings.tombstone_ttl_secs, None);
            let lease = match tokio::time::timeout_at(deadline, grant).await {
                Ok(Ok(lease)) => lease.id(),
                Ok(Err(error)) => return Err(shared::classify(error, "tombstone lease grant")),
                Err(_) => {
                    return Err(StoreError::Backend(anyhow!(
                        "the tombstone lease grant did not answer in time; nothing was written"
                    )));
                }
            };
            let key = shared.keys.instance(id);
            let tombstone = shared.keys.tombstone(op);
            // The tombstone and the read are one txn: a put of `op` applied
            // before it shows in the read, and one that arrives after it
            // fails its tombstone compare.
            let txn = Txn::new()
                .when([
                    shared.lock_compare(),
                    Compare::version(tombstone.as_str(), CompareOp::Equal, 0),
                ])
                .and_then([
                    TxnOp::put(tombstone, "", Some(PutOptions::new().with_lease(lease))),
                    TxnOp::get(key.as_str(), None),
                ])
                .or_else(shared.failure_ops(&key));
            let outcome = shared.call(txn, Some(1), deadline, "resolve").await?;
            Ok(outcome.instance.and_then(|kv| {
                serde_json::from_slice::<RegistrationRecord>(kv.value())
                    .ok()
                    .filter(|record| record.op == op)
                    .map(|_| shared::incarnation(kv.mod_revision()))
            }))
        })
    }

    fn get(&self, id: InstanceId) -> Option<StoredRegistration> {
        self.shared
            .cache
            .read()
            .entries
            .get(&id)
            .map(|entry| entry.stored.clone())
    }

    fn current_incarnation(&self, id: InstanceId) -> Option<Incarnation> {
        self.shared
            .cache
            .read()
            .entries
            .get(&id)
            .map(|entry| entry.stored.incarnation)
    }

    fn list(&self) -> Vec<StoredRegistration> {
        self.shared
            .cache
            .read()
            .entries
            .values()
            .map(|entry| entry.stored.clone())
            .collect()
    }

    fn peers(&self) -> Vec<StoredPeer> {
        self.shared
            .cache
            .read()
            .entries
            .values()
            .map(|entry| StoredPeer {
                peer: entry.stored.record.peer.clone(),
                incarnation: entry.stored.incarnation,
                pinned: entry.stored.record.pinned,
            })
            .collect()
    }

    fn install_removal_hook(
        &self,
        hook: RemovalHook,
    ) -> Result<Vec<StoredRegistration>, StoreError> {
        // Under the cache's write lock, which the watch holds while it
        // applies events and reads the hook: the snapshot and the first
        // reported removal share one linearization point.
        let mut cache = self.shared.cache.write();
        if cache.hook.is_some() {
            return Err(StoreError::HookInstalled);
        }
        cache.hook = Some(hook);
        Ok(cache
            .entries
            .values()
            .map(|entry| entry.stored.clone())
            .collect())
    }

    fn continuity(&self) -> watch::Receiver<Continuity> {
        self.shared.continuity.subscribe()
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(EtcdStore::close(self))
    }
}
