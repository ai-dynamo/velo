// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! [`EtcdStoreBuilder`] and the connect sequence.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use etcd_client::{
    Client, Compare, CompareOp, ConnectOptions, EventType, GetOptions, KeyValue, PutOptions, Txn,
    TxnOp, WatchOptions,
};
use parking_lot::Mutex;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::EtcdStore;
use super::shared::{KeyKind, Keys, Settings, Shared};
use super::tasks::{self, LeaseRole, Renewals};
use crate::store::RegistrationRecord;

/// Default key prefix.
pub const DEFAULT_PREFIX: &str = "/velo-hub/default";
/// Default time after which an entry nobody touched is deleted.
pub const DEFAULT_REGISTRANT_TTL: Duration = Duration::from_secs(30);
/// Default interval between reaper passes.
pub const DEFAULT_PRUNE_INTERVAL: Duration = Duration::from_secs(10);
/// Default TTL of the custody lease every record is written with. It is
/// how long a dead hub's registrations wait for the next hub.
pub const DEFAULT_CUSTODY_TTL: Duration = Duration::from_secs(300);
/// Default TTL of the lock lease. A crashed hub holds its prefix this long,
/// and a hub that gets no renewal answered for this long is `Degraded`.
pub const DEFAULT_LOCK_TTL: Duration = Duration::from_secs(10);
/// Default budget for one call: its txn and the wait for the watch. Below
/// the hub's default step timeout (5 s), so the store answers, and marks
/// itself `Degraded` on a lagging watch, before the hub cuts the call off.
pub const DEFAULT_REQUEST_DEADLINE: Duration = Duration::from_secs(4);

/// etcd raises a shorter lease TTL to its minimum (1.5 election timeouts,
/// 2 s with default settings), so a tombstone TTL is at least this.
const ETCD_MIN_TTL_SECS: i64 = 2;
/// HTTP/2 ping interval: a dead connection breaks the watch within about
/// this plus the deadline, rather than at the TCP timeout.
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(5);

/// Why [`EtcdStoreBuilder::connect`] failed.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    /// The settings cannot work together.
    #[error("invalid etcd store settings: {0}")]
    InvalidConfig(String),
    /// Another hub holds the prefix. It is freed when that hub stops, or
    /// one lock TTL after it crashed.
    #[error(
        "the etcd prefix {prefix} is held by another hub ({holder}); it is freed when that hub stops, or one lock TTL after it crashed"
    )]
    PrefixHeld {
        /// The contested prefix.
        prefix: String,
        /// What the holder wrote in the lock key.
        holder: String,
    },
    /// etcd could not be reached or refused a request.
    #[error("etcd: {0:#}")]
    Etcd(anyhow::Error),
}

/// Builds an [`EtcdStore`]. See the module docs for what each setting
/// controls.
#[derive(Clone)]
pub struct EtcdStoreBuilder {
    endpoints: Vec<String>,
    options: Option<ConnectOptions>,
    prefix: String,
    registrant_ttl: Duration,
    prune_interval: Duration,
    custody_ttl: Duration,
    lock_ttl: Duration,
    request_deadline: Duration,
    wait_for_prefix: Duration,
    /// The local deadline of the lock lease, when a test sets one shorter
    /// than `lock_ttl`.
    lock_deadline: Option<Duration>,
}

/// Pause before the next connect attempt after a failure, and after a
/// failed watch of a held prefix's lock key.
const PREFIX_RETRY: Duration = Duration::from_millis(250);

impl std::fmt::Debug for EtcdStoreBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EtcdStoreBuilder")
            .field("endpoints", &self.endpoints)
            .field("prefix", &self.prefix)
            .field("registrant_ttl", &self.registrant_ttl)
            .field("prune_interval", &self.prune_interval)
            .field("custody_ttl", &self.custody_ttl)
            .field("lock_ttl", &self.lock_ttl)
            .field("request_deadline", &self.request_deadline)
            .finish_non_exhaustive()
    }
}

fn whole_secs(duration: Duration) -> i64 {
    i64::try_from(duration.as_secs() + u64::from(duration.subsec_nanos() > 0)).unwrap_or(i64::MAX)
}

impl EtcdStoreBuilder {
    pub(super) fn new(endpoints: Vec<String>) -> Self {
        Self {
            endpoints,
            options: None,
            prefix: DEFAULT_PREFIX.to_string(),
            registrant_ttl: DEFAULT_REGISTRANT_TTL,
            prune_interval: DEFAULT_PRUNE_INTERVAL,
            custody_ttl: DEFAULT_CUSTODY_TTL,
            lock_ttl: DEFAULT_LOCK_TTL,
            request_deadline: DEFAULT_REQUEST_DEADLINE,
            wait_for_prefix: Duration::ZERO,
            lock_deadline: None,
        }
    }

    /// Connection options (TLS, user). The store sets the connect timeout,
    /// HTTP/2 keep-alive, and require-leader on them.
    pub fn connect_options(mut self, options: ConnectOptions) -> Self {
        self.options = Some(options);
        self
    }

    /// Key prefix (default [`DEFAULT_PREFIX`]). One hub per prefix. Starts
    /// with `/`; a trailing `/` is ignored.
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Time after which the reaper deletes an entry nobody touched
    /// (default [`DEFAULT_REGISTRANT_TTL`]).
    pub fn registrant_ttl(mut self, ttl: Duration) -> Self {
        self.registrant_ttl = ttl;
        self
    }

    /// Interval between reaper passes (default [`DEFAULT_PRUNE_INTERVAL`]).
    pub fn prune_interval(mut self, interval: Duration) -> Self {
        self.prune_interval = interval;
        self
    }

    /// TTL of the custody lease (default [`DEFAULT_CUSTODY_TTL`], rounded
    /// up to whole seconds). At least the lock TTL.
    pub fn custody_ttl(mut self, ttl: Duration) -> Self {
        self.custody_ttl = ttl;
        self
    }

    /// TTL of the lock lease (default [`DEFAULT_LOCK_TTL`], rounded up to
    /// whole seconds).
    pub fn lock_ttl(mut self, ttl: Duration) -> Self {
        self.lock_ttl = ttl;
        self
    }

    /// Budget for one call (default [`DEFAULT_REQUEST_DEADLINE`]). Keep it
    /// below the hub's `step_timeout`.
    pub fn request_deadline(mut self, deadline: Duration) -> Self {
        self.request_deadline = deadline;
        self
    }

    /// How long `connect` keeps trying (default zero: fail at once, with
    /// [`ConnectError::PrefixHeld`] if another store holds the prefix).
    /// While the prefix is held, `connect` watches its lock key and tries
    /// again when the key goes. After any other failure (etcd cannot be
    /// reached, for example) it tries again after 250 ms. After a hub
    /// crashed, its lock lives one `lock_ttl` more; a hub that restarts in
    /// its place waits that long. A standby waits with `Duration::MAX`,
    /// which never gives up.
    pub fn wait_for_prefix(mut self, wait: Duration) -> Self {
        self.wait_for_prefix = wait;
        self
    }

    /// Tests only: give up on the lock lease this long after the last
    /// renewal etcd answered, though the lease is granted for the longer
    /// `lock_ttl`. The store then sees the deadline pass while etcd still
    /// holds the lease, which on one local etcd node happens almost never.
    /// Renewals go out every third of it, and the request deadline must be
    /// under two thirds of it.
    #[cfg(feature = "test-support")]
    pub fn lock_deadline(mut self, deadline: Duration) -> Self {
        self.lock_deadline = Some(deadline);
        self
    }

    fn settings(&self) -> Result<(String, Settings), ConnectError> {
        let invalid = |why: &str| Err(ConnectError::InvalidConfig(why.to_string()));
        let prefix = self.prefix.trim_end_matches('/');
        if !prefix.starts_with('/') || prefix.len() < 2 {
            return invalid("the prefix must start with '/' and name something");
        }
        if self.endpoints.is_empty() {
            return invalid("no etcd endpoints");
        }
        if self.registrant_ttl.is_zero()
            || self.prune_interval.is_zero()
            || self.request_deadline.is_zero()
            || self.lock_ttl.is_zero()
        {
            return invalid("TTLs, the prune interval and the request deadline must be non-zero");
        }
        let lock_ttl_secs = whole_secs(self.lock_ttl);
        let custody_ttl_secs = whole_secs(self.custody_ttl);
        // The lock must go first: a store partitioned for longer than the
        // lock TTL is fenced before its records' lease can expire.
        if custody_ttl_secs < lock_ttl_secs {
            return invalid("the custody TTL must be at least the lock TTL");
        }
        let lock_deadline = self.lock_deadline.unwrap_or(self.lock_ttl);
        if lock_deadline.is_zero() || lock_deadline > self.lock_ttl {
            return invalid("the lock deadline must be non-zero and at most the lock TTL");
        }
        // The lock is kept alive every third of its deadline, and each
        // answer may take the request deadline: with a request deadline of
        // two thirds of it or more, one slow answer misses the deadline.
        if self.request_deadline * 3 >= lock_deadline * 2 {
            return invalid("the request deadline must be under two thirds of the lock TTL");
        }
        Ok((
            prefix.to_string(),
            Settings {
                registrant_ttl: self.registrant_ttl,
                prune_interval: self.prune_interval,
                request_deadline: self.request_deadline,
                lock_ttl_secs,
                lock_deadline,
                custody_ttl_secs,
                // A put that timed out can still reach etcd; its tombstone
                // must outlive that by a margin (D-10).
                tombstone_ttl_secs: whole_secs(self.request_deadline * 2).max(ETCD_MIN_TTL_SECS),
            },
        ))
    }

    /// Connect, take the prefix's lock, delete the pinned entries of hubs
    /// before this one, load the prefix, and start the store's tasks.
    ///
    /// With [`wait_for_prefix`](Self::wait_for_prefix), a held prefix is
    /// waited for on a watch of its lock key, and any other failure is
    /// tried again after a short pause, until the wait ends.
    pub async fn connect(self) -> Result<EtcdStore, ConnectError> {
        self.settings()?;
        // `None` past the clock's range: wait without a limit.
        let until = Instant::now().checked_add(self.wait_for_prefix);
        let waiting = || until.is_none_or(|until| Instant::now() < until);
        // One client for every attempt: tonic reconnects it, and a new one
        // per attempt is a new connection per attempt.
        let mut client = None;
        let mut last = None;
        loop {
            let error = match self.connect_once(&mut client).await {
                Ok(store) => return Ok(store),
                Err(error) => error,
            };
            if !waiting() {
                return Err(error.into());
            }
            // Logged when it changes, not on every attempt of a long wait.
            let now = error.to_string();
            if last.as_ref() != Some(&now) {
                match &error {
                    Attempt::Held { .. } => tracing::info!(%now, "waiting for the etcd prefix"),
                    Attempt::Failed(_) => {
                        tracing::warn!(%now, "connecting the etcd store failed; retrying")
                    }
                }
                last = Some(now);
            }
            match (&error, &client) {
                (Attempt::Held { revision, .. }, Some(client)) => {
                    if let Err(error) = self.wait_for_release(client, *revision, until).await {
                        tracing::debug!(%error, "watching the etcd prefix lock failed; retrying");
                        tokio::time::sleep(PREFIX_RETRY).await;
                    }
                }
                _ => tokio::time::sleep(PREFIX_RETRY).await,
            }
        }
    }

    /// Wait until the lock key changes after `revision` (the holder let it
    /// go, or its lease expired), `until` passes, or one lock TTL passes,
    /// after which the caller looks again. Watching costs etcd nothing per
    /// poll, where trying to take the lock costs a lease grant, a txn and a
    /// revoke.
    async fn wait_for_release(
        &self,
        client: &Client,
        revision: i64,
        until: Option<Instant>,
    ) -> anyhow::Result<()> {
        let (prefix, settings) = self.settings()?;
        let lock = Keys::new(&prefix).lock;
        let options = WatchOptions::new().with_start_revision(revision + 1);
        let (_watcher, mut events) = bounded(
            settings.request_deadline,
            watch(client).watch(lock.as_str(), Some(options)),
        )
        .await
        .context("watching the prefix lock")?;
        let look_again = Instant::now() + self.lock_ttl;
        let end = until.map_or(look_again, |until| until.min(look_again));
        loop {
            let message = tokio::select! {
                _ = tokio::time::sleep_until(end) => return Ok(()),
                message = events.message() => message,
            };
            match message.context("watching the prefix lock")? {
                Some(response) if response.canceled() || response.compact_revision() != 0 => {
                    return Ok(());
                }
                Some(response) => {
                    if response
                        .events()
                        .iter()
                        .any(|event| event.event_type() == EventType::Delete)
                    {
                        return Ok(());
                    }
                }
                None => anyhow::bail!("the watch of the prefix lock closed"),
            }
        }
    }

    async fn connect_once(&self, client: &mut Option<Client>) -> Result<EtcdStore, Attempt> {
        let (prefix, settings) = self.settings()?;
        let deadline = settings.request_deadline;
        let etcd = |error: anyhow::Error| Attempt::Failed(ConnectError::Etcd(error));
        let client = match client {
            Some(client) => client.clone(),
            None => {
                let options = self
                    .options
                    .clone()
                    .unwrap_or_default()
                    .with_connect_timeout(deadline)
                    .with_keep_alive(KEEP_ALIVE_INTERVAL, deadline)
                    .with_require_leader(true);
                let connected =
                    tokio::time::timeout(deadline, Client::connect(&self.endpoints, Some(options)))
                        .await
                        .context("connecting to etcd timed out")
                        .map_err(etcd)?
                        .context("connecting to etcd")
                        .map_err(etcd)?;
                client.insert(connected).clone()
            }
        };
        let keys = Keys::new(&prefix);

        // Look before taking: a held prefix costs one read, not a lease.
        let found = bounded(deadline, kv(&client).get(keys.lock.as_str(), None))
            .await
            .context("reading the prefix lock")
            .map_err(etcd)?;
        if let Some(lock) = found.kvs().first() {
            return Err(Attempt::Held {
                prefix,
                holder: String::from_utf8_lossy(lock.value()).into_owned(),
                revision: found.header().map_or(0, |header| header.revision()),
            });
        }

        // The lease's first local deadline counts from here.
        let granted_at = Instant::now();
        let lock_lease = grant(&client, settings.lock_ttl_secs, deadline)
            .await
            .map_err(etcd)?;
        let holder = format!("velo-hub store {}", uuid::Uuid::new_v4());
        let taken = bounded(
            deadline,
            kv(&client).txn(
                Txn::new()
                    .when([Compare::version(keys.lock.as_str(), CompareOp::Equal, 0)])
                    .and_then([TxnOp::put(
                        keys.lock.as_str(),
                        holder.as_str(),
                        Some(PutOptions::new().with_lease(lock_lease)),
                    )])
                    .or_else([TxnOp::get(keys.lock.as_str(), None)]),
            ),
        )
        .await;
        let taken = match taken {
            Ok(response) => response,
            Err(error) => {
                revoke(&client, &[lock_lease], deadline).await;
                return Err(etcd(error.context("taking the prefix lock")));
            }
        };
        if !taken.succeeded() {
            revoke(&client, &[lock_lease], deadline).await;
            let holder = taken
                .op_responses()
                .into_iter()
                .find_map(|op| match op {
                    etcd_client::TxnOpResponse::Get(get) => get
                        .kvs()
                        .first()
                        .map(|kv| String::from_utf8_lossy(kv.value()).into_owned()),
                    _ => None,
                })
                .unwrap_or_default();
            return Err(Attempt::Held {
                prefix,
                holder,
                revision: taken.header().map_or(0, |header| header.revision()),
            });
        }

        match start(client.clone(), keys, settings, lock_lease, granted_at).await {
            Ok(store) => Ok(store),
            Err(error) => {
                revoke(&client, &[lock_lease], deadline).await;
                Err(etcd(error))
            }
        }
    }
}

/// How one connect attempt failed.
#[derive(Debug, thiserror::Error)]
enum Attempt {
    /// Another store holds the prefix; its lock key was read at `revision`.
    #[error("the etcd prefix {prefix} is held by another hub ({holder})")]
    Held {
        prefix: String,
        holder: String,
        revision: i64,
    },
    #[error(transparent)]
    Failed(#[from] ConnectError),
}

impl From<Attempt> for ConnectError {
    fn from(attempt: Attempt) -> Self {
        match attempt {
            Attempt::Held { prefix, holder, .. } => Self::PrefixHeld { prefix, holder },
            Attempt::Failed(error) => error,
        }
    }
}

async fn bounded<T>(
    deadline: Duration,
    call: impl Future<Output = Result<T, etcd_client::Error>>,
) -> anyhow::Result<T> {
    Ok(tokio::time::timeout(deadline, call)
        .await
        .context("etcd did not answer in time")??)
}

async fn grant(client: &Client, ttl_secs: i64, deadline: Duration) -> anyhow::Result<i64> {
    let response = bounded(deadline, client.lease_client().grant(ttl_secs, None))
        .await
        .context("granting a lease")?;
    Ok(response.id())
}

/// Best effort: a lease not revoked expires on its own.
pub(super) async fn revoke(client: &Client, leases: &[i64], deadline: Duration) {
    for lease in leases {
        if let Err(error) = bounded(deadline, client.lease_client().revoke(*lease)).await {
            tracing::warn!(lease, %error, "revoking an etcd lease failed; it expires on its own");
        }
    }
}

async fn load(
    client: &Client,
    keys: &Keys,
    deadline: Duration,
) -> anyhow::Result<(Vec<KeyValue>, i64)> {
    let mut response = bounded(
        deadline,
        kv(client).get(
            keys.instances.as_str(),
            Some(GetOptions::new().with_prefix()),
        ),
    )
    .await
    .context("loading the prefix")?;
    let revision = response
        .header()
        .map(|header| header.revision())
        .context("etcd answered without a header")?;
    Ok((response.take_kvs(), revision))
}

/// With the lock held: the rest of the connect sequence.
async fn start(
    client: Client,
    keys: Keys,
    settings: Settings,
    lock_lease: i64,
    granted_at: Instant,
) -> anyhow::Result<EtcdStore> {
    let deadline = settings.request_deadline;

    // D-13. A pinned record is a hub's own entry. At connect none is this
    // store's, and nothing would ever delete the previous hub's: this
    // store keeps its custody lease alive, and the reaper skips pinned
    // entries.
    let (found, _) = load(&client, &keys, deadline).await?;
    let pinned: Vec<TxnOp> = found
        .iter()
        .filter(|kv| {
            serde_json::from_slice::<RegistrationRecord>(kv.value())
                .is_ok_and(|record| record.pinned)
        })
        .map(|kv| {
            let key = kv.key().to_vec();
            TxnOp::txn(
                Txn::new()
                    .when([Compare::mod_revision(
                        key.clone(),
                        CompareOp::Equal,
                        kv.mod_revision(),
                    )])
                    .and_then([TxnOp::delete(key, None)]),
            )
        })
        .collect();
    if !pinned.is_empty() {
        let count = pinned.len();
        let deleted = bounded(
            deadline,
            kv(&client).txn(
                Txn::new()
                    .when([Compare::lease(
                        keys.lock.as_str(),
                        CompareOp::Equal,
                        lock_lease,
                    )])
                    .and_then(pinned),
            ),
        )
        .await
        .context("deleting the previous hubs' pinned entries")?;
        anyhow::ensure!(deleted.succeeded(), "lost the prefix lock while starting");
        tracing::info!(count, prefix = %keys.root, "deleted the pinned entries of previous hubs");
    }

    let custody_lease = grant(&client, settings.custody_ttl_secs, deadline).await?;
    let (records, revision) = match load(&client, &keys, deadline).await {
        Ok(loaded) => loaded,
        Err(error) => {
            revoke(&client, &[custody_lease], deadline).await;
            return Err(error);
        }
    };
    let adopted: BTreeSet<i64> = records
        .iter()
        .filter(|kv| matches!(keys.kind(kv.key()), KeyKind::Instance(_)))
        .map(KeyValue::lease)
        .filter(|lease| *lease != 0 && *lease != custody_lease)
        .collect();
    let shared = Arc::new(Shared::new(
        client.clone(),
        keys,
        settings,
        lock_lease,
        custody_lease,
        revision,
    ));
    shared.load(&records);
    let first = match tasks::open_watch(&shared, revision + 1).await {
        Ok(watch) => watch,
        Err(error) => {
            revoke(&client, &[custody_lease], deadline).await;
            anyhow::bail!("opening the prefix watch: {error}");
        }
    };

    let cancel = CancellationToken::new();
    let (renewals, lock_deadline) = Renewals::new(granted_at, shared.settings.lock_deadline);
    let mut handles = vec![
        tokio::spawn(tasks::watch(Arc::clone(&shared), first, cancel.clone())),
        tokio::spawn(tasks::keep_alive(
            Arc::clone(&shared),
            lock_lease,
            LeaseRole::Lock,
            Some(renewals),
            cancel.clone(),
        )),
        tokio::spawn(tasks::lease_deadline(
            Arc::clone(&shared),
            lock_deadline,
            cancel.clone(),
        )),
        tokio::spawn(tasks::keep_alive(
            Arc::clone(&shared),
            custody_lease,
            LeaseRole::Custody,
            None,
            cancel.clone(),
        )),
        tokio::spawn(tasks::reap(Arc::clone(&shared), cancel.clone())),
    ];
    if !adopted.is_empty() {
        tracing::info!(leases = ?adopted, "keeping alive the custody leases of previous hubs");
    }
    for lease in adopted {
        handles.push(tokio::spawn(tasks::keep_alive(
            Arc::clone(&shared),
            lease,
            LeaseRole::Adopted,
            None,
            cancel.clone(),
        )));
    }
    Ok(EtcdStore {
        shared,
        cancel,
        tasks: Mutex::new(handles),
    })
}

/// A KV client for this store. tonic's default decode limit (4 MiB) is
/// below one load of the prefix at the hub's target scale (about 590 bytes
/// an entry on the wire: the limit falls near 7,000 registrants), and etcd
/// itself puts no cap on a response, so the store lifts it.
pub(super) fn kv(client: &Client) -> etcd_client::KvClient {
    client.kv_client().max_decoding_message_size(usize::MAX)
}

/// A watch client for this store, without tonic's decode limit (see [`kv`]).
pub(super) fn watch(client: &Client) -> etcd_client::WatchClient {
    client.watch_client().max_decoding_message_size(usize::MAX)
}
