// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! State the store and its tasks share: the cache the watch feeds, the
//! health that becomes [`Continuity`], the barrier, and the txns.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use anyhow::anyhow;
use dashmap::DashMap;
use etcd_client::{
    Client, Compare, CompareOp, Event, EventType, KeyValue, Txn, TxnOp, TxnOpResponse, TxnResponse,
};
use parking_lot::{Mutex, RwLock};
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use velo::{InstanceId, PeerInfo, WorkerId};

use super::hold::Holds;
use crate::store::{
    Continuity, Incarnation, OpId, RegistrationRecord, Removal, RemovalHook, StoreError,
    StoredRegistration,
};

/// Keys under the prefix. Everything the store writes is under `root`, so
/// one watch on `root` sees every txn's revision.
pub(super) struct Keys {
    /// `{prefix}/`. The trailing slash keeps `/a` from watching `/ab/...`.
    pub(super) root: String,
    /// `{prefix}/i/`: one record per instance.
    pub(super) instances: String,
    /// `{prefix}/hub`: the lock, on the lock lease.
    pub(super) lock: String,
    /// `{prefix}/b`: written by every failed txn, so the failed txn has a
    /// revision in the prefix that the watch reports.
    pub(super) barrier: String,
    /// `{prefix}/op/`: resolve's tombstones.
    pub(super) ops: String,
}

pub(super) enum KeyKind {
    Instance(InstanceId),
    Lock,
    Other,
}

impl Keys {
    pub(super) fn new(prefix: &str) -> Self {
        Self {
            root: format!("{prefix}/"),
            instances: format!("{prefix}/i/"),
            lock: format!("{prefix}/hub"),
            barrier: format!("{prefix}/b"),
            ops: format!("{prefix}/op/"),
        }
    }

    pub(super) fn instance(&self, id: InstanceId) -> String {
        format!("{}{id}", self.instances)
    }

    pub(super) fn tombstone(&self, op: OpId) -> String {
        format!("{}{op}", self.ops)
    }

    pub(super) fn kind(&self, key: &[u8]) -> KeyKind {
        if key == self.lock.as_bytes() {
            return KeyKind::Lock;
        }
        key.strip_prefix(self.instances.as_bytes())
            .and_then(|id| std::str::from_utf8(id).ok())
            .and_then(|id| uuid::Uuid::parse_str(id).ok())
            .map_or(KeyKind::Other, |id| KeyKind::Instance(InstanceId::from(id)))
    }
}

/// The settings the running store reads, in the units etcd takes.
pub(super) struct Settings {
    pub(super) registrant_ttl: Duration,
    pub(super) prune_interval: Duration,
    pub(super) request_deadline: Duration,
    pub(super) lock_ttl_secs: i64,
    pub(super) custody_ttl_secs: i64,
    pub(super) tombstone_ttl_secs: i64,
}

/// An instance's record as the watch last applied it.
pub(super) struct Entry {
    pub(super) stored: StoredRegistration,
    /// The lease the key is attached to: this store's custody lease, or an
    /// adopted one from the hub before it.
    pub(super) lease: i64,
    /// Nanoseconds since [`Shared::epoch`] of the last put or touch. An
    /// atomic so that a touch, the most frequent call, takes the read lock.
    last_seen: AtomicU64,
    /// The reaper sent a delete for this incarnation. A touch now answers
    /// `NotFound`, as the in-memory store's would after its prune.
    reaping: AtomicBool,
}

#[derive(Default)]
pub(super) struct Cache {
    pub(super) entries: HashMap<InstanceId, Entry>,
    pub(super) hook: Option<RemovalHook>,
    /// How many entries name each lease; an adopted lease is kept alive
    /// while its count is above zero.
    pub(super) lease_refs: HashMap<i64, usize>,
}

impl Cache {
    fn add_ref(&mut self, lease: i64) {
        *self.lease_refs.entry(lease).or_default() += 1;
    }

    fn drop_ref(&mut self, lease: i64) {
        if let Some(count) = self.lease_refs.get_mut(&lease) {
            *count -= 1;
            if *count == 0 {
                self.lease_refs.remove(&lease);
            }
        }
    }
}

/// What makes up [`Continuity`]. Fenced is terminal; the rest clear.
#[derive(Default)]
struct Health {
    fenced: bool,
    lost: bool,
    disconnected: bool,
    /// A call gave up waiting for the watch to reach this revision.
    lag_until: Option<i64>,
}

impl Health {
    fn continuity(&self) -> Continuity {
        if self.fenced {
            Continuity::Fenced
        } else if self.lost {
            Continuity::Lost
        } else if self.disconnected || self.lag_until.is_some() {
            Continuity::Degraded
        } else {
            Continuity::Live
        }
    }
}

/// How a call's wait for the watch ended.
pub(super) enum Barrier {
    /// The watch applied the revision and ran the hook for it.
    Reached,
    /// The store is closing; the watch will not apply it.
    Closing,
    /// The deadline passed first.
    TimedOut,
}

/// A txn's answer, read the same way for every call. Every txn's failure
/// branch is [`Shared::failure_ops`], so a failed one says what the
/// instance key held and whether the lock is still this store's.
pub(super) struct Outcome {
    pub(super) succeeded: bool,
    /// The txn's revision: of its writes, or of the barrier key.
    pub(super) revision: i64,
    /// The instance key, where the branch that ran read it.
    pub(super) instance: Option<KeyValue>,
}

pub(super) struct Shared {
    pub(super) client: Client,
    pub(super) keys: Keys,
    pub(super) settings: Settings,
    pub(super) lock_lease: i64,
    pub(super) custody_lease: i64,
    pub(super) cache: RwLock<Cache>,
    /// Read views for discovery, written only under `cache`'s write lock.
    pub(super) peers: DashMap<InstanceId, PeerInfo>,
    /// The worker index (no worker keys in etcd: `WorkerId` is a function
    /// of `InstanceId`, and this hub is the prefix's only writer).
    pub(super) workers: DashMap<WorkerId, PeerInfo>,
    /// The last revision the watch applied, sent after the hook ran for it.
    pub(super) applied: watch::Sender<i64>,
    health: Mutex<Health>,
    pub(super) continuity: watch::Sender<Continuity>,
    /// Cancelled when the store closes: calls fail without reaching etcd,
    /// and a call waiting on the barrier returns its txn's outcome.
    pub(super) closing: CancellationToken,
    /// Cancelled when the hub stops its core tasks (its probes, which keep
    /// entries fresh): the reaper stops then, not at `close`.
    pub(super) reaper_stopped: CancellationToken,
    pub(super) holds: Holds,
    /// Set once the lock lease is revoked (or about to be), so a drop
    /// after `close` does not revoke it again.
    pub(super) lock_released: AtomicBool,
    epoch: std::time::Instant,
}

/// gRPC codes for which etcd refused the request, so nothing applied.
/// Every other failure may have applied. etcd-client does not re-export
/// tonic's `Code`, so the numbers are the gRPC spec's.
const REFUSED_CODES: [i32; 9] = [
    3,  // InvalidArgument
    5,  // NotFound (for example a lease that does not exist)
    6,  // AlreadyExists
    7,  // PermissionDenied
    8,  // ResourceExhausted
    9,  // FailedPrecondition
    11, // OutOfRange
    12, // Unimplemented
    16, // Unauthenticated
];

/// A store error for an etcd failure: refused requests wrote nothing, any
/// other failure may have applied.
pub(super) fn classify(error: etcd_client::Error, what: &str) -> StoreError {
    use etcd_client::Error;
    let refused = match &error {
        Error::GRpcStatus(status) => REFUSED_CODES.contains(&(status.code() as i32)),
        Error::InvalidArgs(_)
        | Error::InvalidUri(_)
        | Error::Utf8Error(_)
        | Error::InvalidHeaderValue(_)
        | Error::EndpointError(_)
        | Error::EndpointsNotManaged => true,
        _ => false,
    };
    if refused {
        StoreError::Backend(anyhow::Error::new(error).context(format!("etcd refused {what}")))
    } else {
        StoreError::Indeterminate(format!("etcd {what}: {error}"))
    }
}

/// The incarnation of an etcd revision. Revisions start at 1 and only grow.
pub(super) fn incarnation(revision: i64) -> Incarnation {
    Incarnation::from_u64(u64::try_from(revision).unwrap_or(0))
}

/// The revision an incarnation compares against. One beyond `i64` cannot
/// be any key's revision, so it compares as -1, which no key has.
pub(super) fn revision(incarnation: Incarnation) -> i64 {
    i64::try_from(incarnation.as_u64()).unwrap_or(-1)
}

impl Shared {
    pub(super) fn new(
        client: Client,
        keys: Keys,
        settings: Settings,
        lock_lease: i64,
        custody_lease: i64,
        applied: i64,
    ) -> Self {
        Self {
            client,
            keys,
            settings,
            lock_lease,
            custody_lease,
            cache: RwLock::new(Cache::default()),
            peers: DashMap::new(),
            workers: DashMap::new(),
            applied: watch::Sender::new(applied),
            health: Mutex::new(Health::default()),
            continuity: watch::Sender::new(Continuity::Live),
            closing: CancellationToken::new(),
            reaper_stopped: CancellationToken::new(),
            holds: Holds::default(),
            lock_released: AtomicBool::new(false),
            epoch: std::time::Instant::now(),
        }
    }

    pub(super) fn now(&self) -> u64 {
        u64::try_from(self.epoch.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    // ---- health -------------------------------------------------------

    fn update_health(&self, change: impl FnOnce(&mut Health)) {
        let mut health = self.health.lock();
        if health.fenced {
            return;
        }
        change(&mut health);
        let next = health.continuity();
        let changed = self.continuity.send_if_modified(|current| {
            let changed = *current != next;
            *current = next;
            changed
        });
        if changed {
            self.holds.record(next);
        }
    }

    pub(super) fn fence(&self, why: &str) {
        if !self.is_fenced() {
            tracing::error!(prefix = %self.keys.root, why, "the etcd store is fenced; the hub must stop");
        }
        self.update_health(|health| health.fenced = true);
    }

    pub(super) fn is_fenced(&self) -> bool {
        self.health.lock().fenced
    }

    pub(super) fn set_disconnected(&self, disconnected: bool) {
        self.update_health(|health| health.disconnected = disconnected);
    }

    pub(super) fn set_lost(&self, lost: bool) {
        self.update_health(|health| health.lost = lost);
    }

    fn lagging(&self, revision: i64) {
        self.update_health(|health| {
            health.lag_until = Some(health.lag_until.map_or(revision, |r| r.max(revision)));
        });
    }

    /// Called by the watch after it published `applied`.
    fn caught_up(&self, applied: i64) {
        if self
            .health
            .lock()
            .lag_until
            .is_some_and(|until| applied >= until)
        {
            self.update_health(|health| {
                if health.lag_until.is_some_and(|until| applied >= until) {
                    health.lag_until = None;
                }
            });
        }
    }

    /// Fail a call that must not reach etcd: the store is closed or fenced.
    pub(super) fn ensure_open(&self) -> Result<(), StoreError> {
        if self.closing.is_cancelled() {
            return Err(StoreError::Backend(anyhow!("the etcd store is closed")));
        }
        if self.is_fenced() {
            return Err(StoreError::Backend(anyhow!(
                "the etcd store is fenced: another hub holds {}",
                self.keys.lock
            )));
        }
        Ok(())
    }

    // ---- txns ---------------------------------------------------------

    pub(super) fn lock_compare(&self) -> Compare {
        Compare::lease(self.keys.lock.as_str(), CompareOp::Equal, self.lock_lease)
    }

    /// The failure branch of every txn: write the barrier key (the txn's
    /// revision), read the instance key (`NotFound` vs `Stale`, and the
    /// current incarnation) and the lock key (fenced or not), in one round
    /// trip.
    pub(super) fn failure_ops(&self, instance_key: &str) -> Vec<TxnOp> {
        vec![
            TxnOp::put(self.keys.barrier.as_str(), "", None),
            TxnOp::get(instance_key, None),
            TxnOp::get(self.keys.lock.as_str(), None),
        ]
    }

    pub(super) async fn send(
        &self,
        txn: Txn,
        deadline: Instant,
        what: &str,
    ) -> Result<TxnResponse, StoreError> {
        let mut kv = self.client.kv_client();
        match tokio::time::timeout_at(deadline, kv.txn(txn)).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(error)) => Err(classify(error, what)),
            Err(_) => Err(StoreError::Indeterminate(format!(
                "etcd {what} did not answer within {:?}",
                self.settings.request_deadline
            ))),
        }
    }

    /// Run a call's txn and wait until the watch has applied its revision,
    /// both by `deadline` (one budget, so the store answers before a caller
    /// bounding the call by the same time cuts it off). `instance_at` is the index of the instance key's read in the
    /// success branch, if it has one; the failure branch reads it at 1.
    pub(super) async fn call(
        &self,
        txn: Txn,
        instance_at: Option<usize>,
        deadline: Instant,
        what: &str,
    ) -> Result<Outcome, StoreError> {
        self.ensure_open()?;
        let response = self.send(txn, deadline, what).await?;
        let revision = response.header().map_or(0, |header| header.revision());
        let succeeded = response.succeeded();
        let responses = response.op_responses();
        let read = |index: usize| match responses.get(index) {
            Some(TxnOpResponse::Get(get)) => get.kvs().first().cloned(),
            _ => None,
        };
        let instance = if succeeded {
            instance_at.and_then(read)
        } else {
            read(1)
        };
        if !succeeded && read(2).is_none_or(|lock| lock.lease() != self.lock_lease) {
            if self.closing.is_cancelled() {
                // Our own close revoked the lock: not another hub.
                return Err(StoreError::Backend(anyhow!("the etcd store is closed")));
            }
            self.fence(&format!("{what}: the lock compare failed"));
            return Err(StoreError::Backend(anyhow!(
                "the etcd store is fenced: {} is no longer this hub's",
                self.keys.lock
            )));
        }
        match self.barrier(revision, deadline).await {
            Barrier::Reached => {}
            Barrier::Closing => tracing::info!(
                what,
                revision,
                "the etcd store is closing: the removal hook will not run for this call"
            ),
            Barrier::TimedOut => {
                return Err(StoreError::Indeterminate(format!(
                    "the etcd store's watch did not reach revision {revision} of {what} within {:?}",
                    self.settings.request_deadline
                )));
            }
        }
        Ok(Outcome {
            succeeded,
            revision,
            instance,
        })
    }

    /// Wait until the watch applied `revision` and ran the hook for it.
    /// A timeout, or a caller that stops waiting, marks the store
    /// `Degraded` until the watch catches up (D-5): the hub refuses
    /// registrations while the store lags.
    pub(super) async fn barrier(&self, revision: i64, deadline: Instant) -> Barrier {
        struct LagGuard<'a> {
            shared: &'a Shared,
            revision: i64,
            armed: bool,
        }
        impl Drop for LagGuard<'_> {
            fn drop(&mut self) {
                if self.armed && *self.shared.applied.borrow() < self.revision {
                    self.shared.lagging(self.revision);
                }
            }
        }
        let mut applied = self.applied.subscribe();
        let mut guard = LagGuard {
            shared: self,
            revision,
            armed: true,
        };
        let outcome = tokio::select! {
            biased;
            reached = applied.wait_for(|applied| *applied >= revision) => {
                if reached.is_ok() { Barrier::Reached } else { Barrier::Closing }
            }
            _ = self.closing.cancelled() => Barrier::Closing,
            _ = tokio::time::sleep_until(deadline) => return Barrier::TimedOut,
        };
        guard.armed = false;
        outcome
    }

    // ---- the cache ----------------------------------------------------

    fn insert(&self, cache: &mut Cache, id: InstanceId, kv: &KeyValue) -> Option<Removal> {
        let record: RegistrationRecord = match serde_json::from_slice(kv.value()) {
            Ok(record) => record,
            Err(error) => {
                // Only this hub writes the prefix; a record it cannot read is
                // gone as far as the hub is concerned.
                tracing::warn!(%id, %error, "unreadable record in the etcd store; treating it as removed");
                return self.remove_entry(cache, id);
            }
        };
        let peer = record.peer.clone();
        let entry = Entry {
            stored: StoredRegistration {
                record,
                incarnation: incarnation(kv.mod_revision()),
            },
            lease: kv.lease(),
            last_seen: AtomicU64::new(self.now()),
            reaping: AtomicBool::new(false),
        };
        cache.add_ref(entry.lease);
        // Overwrite in place, as the in-memory store does: a lock-free
        // reader never misses a peer that is being replaced.
        self.workers.insert(peer.worker_id(), peer.clone());
        self.peers.insert(id, peer);
        if let Some(previous) = cache.entries.insert(id, entry) {
            cache.drop_ref(previous.lease);
        }
        None
    }

    fn remove_entry(&self, cache: &mut Cache, id: InstanceId) -> Option<Removal> {
        let entry = cache.entries.remove(&id)?;
        cache.drop_ref(entry.lease);
        self.peers.remove(&id);
        self.workers
            .remove_if(&id.worker_id(), |_, peer| peer.instance_id() == id);
        // The incarnation comes from the cache, not `prev_kv`, which etcd
        // cannot fill for a watcher behind compaction (D-6).
        Some(Removal {
            id,
            incarnation: entry.stored.incarnation,
        })
    }

    /// Fill the cache from a load at connect. No hook is installed yet.
    pub(super) fn load(&self, kvs: &[KeyValue]) {
        let mut cache = self.cache.write();
        for kv in kvs {
            if let KeyKind::Instance(id) = self.keys.kind(kv.key()) {
                self.insert(&mut cache, id, kv);
            }
        }
    }

    /// Apply one watch response, run the hook for its removals outside the
    /// lock, then publish its revision. Publishing after the hook is what
    /// makes the barrier mean "the hook ran".
    pub(super) fn apply(&self, events: &[Event]) {
        let Some(last) = events
            .iter()
            .filter_map(|event| event.kv().map(KeyValue::mod_revision))
            .max()
        else {
            return;
        };
        let mut fenced = false;
        let (removals, hook) = {
            let mut cache = self.cache.write();
            let mut removals = Vec::new();
            for event in events {
                let Some(kv) = event.kv() else { continue };
                match (self.keys.kind(kv.key()), event.event_type()) {
                    (KeyKind::Instance(id), EventType::Put) => {
                        removals.extend(self.insert(&mut cache, id, kv));
                    }
                    (KeyKind::Instance(id), EventType::Delete) => {
                        removals.extend(self.remove_entry(&mut cache, id));
                    }
                    (KeyKind::Lock, EventType::Delete) => fenced = true,
                    (KeyKind::Lock, EventType::Put) => fenced |= kv.lease() != self.lock_lease,
                    (KeyKind::Other, _) => {}
                }
            }
            (removals, cache.hook.clone())
        };
        if fenced && !self.closing.is_cancelled() {
            self.fence("the watch saw the lock key leave this hub");
        }
        self.report(removals, hook);
        self.publish(last);
    }

    /// Replace the cache with a reload after a compacted watch. An entry
    /// that vanished in the gap is a removal; one whose incarnation changed
    /// is a replacement, as the watch would have reported it.
    pub(super) fn reload(&self, kvs: &[KeyValue], revision: i64) {
        let (removals, hook) = {
            let mut cache = self.cache.write();
            let mut present = std::collections::HashSet::new();
            for kv in kvs {
                if let KeyKind::Instance(id) = self.keys.kind(kv.key()) {
                    present.insert(id);
                    let current = cache.entries.get(&id).map(|entry| entry.stored.incarnation);
                    if current != Some(incarnation(kv.mod_revision())) {
                        self.insert(&mut cache, id, kv);
                    }
                }
            }
            let vanished: Vec<InstanceId> = cache
                .entries
                .keys()
                .filter(|id| !present.contains(id))
                .copied()
                .collect();
            let removals: Vec<Removal> = vanished
                .into_iter()
                .filter_map(|id| self.remove_entry(&mut cache, id))
                .collect();
            (removals, cache.hook.clone())
        };
        self.report(removals, hook);
        self.publish(revision);
    }

    fn report(&self, removals: Vec<Removal>, hook: Option<RemovalHook>) {
        if let Some(hook) = hook {
            for removal in removals {
                hook(removal);
            }
        }
    }

    fn publish(&self, revision: i64) {
        self.applied.send_if_modified(|applied| {
            let advanced = revision > *applied;
            if advanced {
                *applied = revision;
            }
            advanced
        });
        self.caught_up(*self.applied.borrow());
    }

    pub(super) fn touch(&self, id: InstanceId, expected: Incarnation) -> Result<(), StoreError> {
        let cache = self.cache.read();
        let entry = cache
            .entries
            .get(&id)
            .filter(|entry| !entry.reaping.load(Ordering::Acquire))
            .ok_or(StoreError::NotFound(id))?;
        if entry.stored.incarnation != expected {
            return Err(StoreError::Stale {
                id,
                expected: Some(expected),
                current: Some(entry.stored.incarnation),
            });
        }
        // SeqCst with the reaper's claim: either the reaper sees this touch
        // when it re-checks after claiming the entry, or this touch sees the
        // claim and does not report a refresh the delete would undo.
        entry.last_seen.store(self.now(), Ordering::SeqCst);
        if entry.reaping.load(Ordering::SeqCst) {
            return Err(StoreError::NotFound(id));
        }
        Ok(())
    }

    // ---- the reaper ---------------------------------------------------

    /// Delete every unpinned entry untouched for the registrant TTL, each
    /// on its own `mod_revision` compare, batched into nested txns under
    /// one lock compare. The watch reports the deletes like any removal.
    /// Returns how many deletes applied.
    pub(super) async fn reap_stale(&self) -> usize {
        /// Nested txns per request; etcd's default `--max-txn-ops` is 128.
        const BATCH: usize = 64;
        /// Requests in flight at once.
        const CONCURRENCY: usize = 8;
        if self.ensure_open().is_err() || self.reaper_stopped.is_cancelled() {
            return 0;
        }
        let ttl = u64::try_from(self.settings.registrant_ttl.as_nanos()).unwrap_or(u64::MAX);
        let now = self.now();
        let stale_at =
            |entry: &Entry| now.saturating_sub(entry.last_seen.load(Ordering::SeqCst)) > ttl;
        let stale: Vec<(InstanceId, Incarnation)> = {
            let cache = self.cache.read();
            let mut stale = Vec::new();
            for (id, entry) in &cache.entries {
                if entry.stored.record.pinned || !stale_at(entry) {
                    continue;
                }
                if entry.reaping.swap(true, Ordering::SeqCst) {
                    continue;
                }
                // Re-check after the claim: a touch that landed in between
                // keeps the entry (see `touch`).
                if !stale_at(entry) {
                    entry.reaping.store(false, Ordering::SeqCst);
                    continue;
                }
                stale.push((*id, entry.stored.incarnation));
            }
            stale
        };
        if stale.is_empty() {
            return 0;
        }
        use futures::StreamExt;
        let mut batches = stale.chunks(BATCH).map(<[_]>::to_vec);
        let mut in_flight = futures::stream::FuturesUnordered::new();
        let mut reaped = 0;
        loop {
            while in_flight.len() < CONCURRENCY
                && let Some(batch) = batches.next()
            {
                in_flight.push(self.reap_batch(batch));
            }
            match in_flight.next().await {
                Some(count) => reaped += count,
                None => return reaped,
            }
        }
    }

    async fn reap_batch(&self, batch: Vec<(InstanceId, Incarnation)>) -> usize {
        let deletes: Vec<TxnOp> = batch
            .iter()
            .map(|(id, incarnation)| {
                let key = self.keys.instance(*id);
                TxnOp::txn(
                    Txn::new()
                        .when([Compare::mod_revision(
                            key.as_str(),
                            CompareOp::Equal,
                            revision(*incarnation),
                        )])
                        .and_then([TxnOp::delete(key, None)]),
                )
            })
            .collect();
        let txn = Txn::new()
            .when([self.lock_compare()])
            .and_then(deletes)
            .or_else([TxnOp::get(self.keys.lock.as_str(), None)]);
        // The hub stops its probes before it closes the store; reaping after
        // that would delete registrations nobody can touch any more.
        if self.reaper_stopped.is_cancelled() {
            self.release_claims(&batch);
            return 0;
        }
        let deadline = Instant::now() + self.settings.request_deadline;
        let result = self.send(txn, deadline, "reaper delete").await;
        let applied: Vec<bool> = match &result {
            Ok(response) if response.succeeded() => response
                .op_responses()
                .iter()
                .map(|op| matches!(op, TxnOpResponse::Txn(nested) if nested.succeeded()))
                .collect(),
            Ok(_) => {
                if !self.closing.is_cancelled() {
                    self.fence("reaper delete: the lock compare failed");
                }
                vec![false; batch.len()]
            }
            Err(error) => {
                tracing::warn!(%error, "the etcd store's reaper could not delete; retrying next pass");
                vec![false; batch.len()]
            }
        };
        // A delete that did not apply (the entry changed, or the request
        // failed) is tried again on the next pass, if it is still stale.
        let undone: Vec<(InstanceId, Incarnation)> = batch
            .iter()
            .zip(&applied)
            .filter(|(_, done)| !**done)
            .map(|(claim, _)| *claim)
            .collect();
        self.release_claims(&undone);
        applied.iter().filter(|done| **done).count()
    }

    fn release_claims(&self, claims: &[(InstanceId, Incarnation)]) {
        let cache = self.cache.read();
        for (id, incarnation) in claims {
            if let Some(entry) = cache.entries.get(id)
                && entry.stored.incarnation == *incarnation
            {
                entry.reaping.store(false, Ordering::SeqCst);
            }
        }
    }
}
