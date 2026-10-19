// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! [`EtcdStoreBuilder`] and the connect sequence.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use etcd_client::{
    Client, Compare, CompareOp, ConnectOptions, GetOptions, KeyValue, PutOptions, Txn, TxnOp,
};
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use super::EtcdStore;
use super::shared::{KeyKind, Keys, Settings, Shared};
use super::tasks::{self, LeaseRole};
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
/// Default TTL of the lock lease. A crashed hub holds its prefix this long.
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
}

/// Pause between attempts while another store holds the prefix.
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

    /// How long `connect` keeps trying while another store holds the
    /// prefix (default zero: fail at once with
    /// [`ConnectError::PrefixHeld`]). After a hub crashed, its lock lives
    /// one `lock_ttl` more; a hub that restarts in its place waits that
    /// long.
    pub fn wait_for_prefix(mut self, wait: Duration) -> Self {
        self.wait_for_prefix = wait;
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
        Ok((
            prefix.to_string(),
            Settings {
                registrant_ttl: self.registrant_ttl,
                prune_interval: self.prune_interval,
                request_deadline: self.request_deadline,
                lock_ttl_secs,
                custody_ttl_secs,
                // A put that timed out can still reach etcd; its tombstone
                // must outlive that by a margin (D-10).
                tombstone_ttl_secs: whole_secs(self.request_deadline * 2).max(ETCD_MIN_TTL_SECS),
            },
        ))
    }

    /// Connect, take the prefix's lock, delete the pinned entries of hubs
    /// before this one, load the prefix, and start the store's tasks.
    pub async fn connect(self) -> Result<EtcdStore, ConnectError> {
        let until = tokio::time::Instant::now() + self.wait_for_prefix;
        loop {
            match self.connect_once().await {
                Err(ConnectError::PrefixHeld { .. }) if tokio::time::Instant::now() < until => {
                    tokio::time::sleep(PREFIX_RETRY).await;
                }
                other => return other,
            }
        }
    }

    async fn connect_once(&self) -> Result<EtcdStore, ConnectError> {
        let (prefix, settings) = self.settings()?;
        let deadline = settings.request_deadline;
        let options = self
            .options
            .clone()
            .unwrap_or_default()
            .with_connect_timeout(deadline)
            .with_keep_alive(KEEP_ALIVE_INTERVAL, deadline)
            .with_require_leader(true);
        let etcd = |error: anyhow::Error| ConnectError::Etcd(error);
        let client =
            tokio::time::timeout(deadline, Client::connect(&self.endpoints, Some(options)))
                .await
                .context("connecting to etcd timed out")
                .map_err(etcd)?
                .context("connecting to etcd")
                .map_err(etcd)?;
        let keys = Keys::new(&prefix);

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
            return Err(ConnectError::PrefixHeld { prefix, holder });
        }

        match start(client.clone(), keys, settings, lock_lease).await {
            Ok(store) => Ok(store),
            Err(error) => {
                revoke(&client, &[lock_lease], deadline).await;
                Err(etcd(error))
            }
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
    let mut handles = vec![
        tokio::spawn(tasks::watch(Arc::clone(&shared), first, cancel.clone())),
        tokio::spawn(tasks::keep_alive(
            Arc::clone(&shared),
            lock_lease,
            LeaseRole::Lock,
            cancel.clone(),
        )),
        tokio::spawn(tasks::keep_alive(
            Arc::clone(&shared),
            custody_lease,
            LeaseRole::Custody,
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
