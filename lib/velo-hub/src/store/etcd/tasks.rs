// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The store's own tasks: the watch that feeds the cache, the lease
//! keep-alives, and the reaper. Each holds the shared state, never the
//! store, so dropping the store stops them.

use std::sync::Arc;
use std::time::Duration;

use etcd_client::{GetOptions, Txn, TxnOp, TxnOpResponse, WatchOptions, WatchStream, Watcher};
use tokio_util::sync::CancellationToken;

use super::shared::Shared;

/// Pause between attempts after a failed reconnect, reload, or keep-alive.
const RETRY: Duration = Duration::from_millis(250);

async fn pause(cancel: &CancellationToken, duration: Duration) -> bool {
    tokio::select! {
        _ = cancel.cancelled() => false,
        _ = tokio::time::sleep(duration) => true,
    }
}

/// Open the prefix watch from `start`.
pub(super) async fn open_watch(
    shared: &Shared,
    start: i64,
) -> Result<(Watcher, WatchStream), String> {
    let mut client = shared.client.watch_client();
    let options = WatchOptions::new().with_prefix().with_start_revision(start);
    match tokio::time::timeout(
        shared.settings.request_deadline,
        client.watch(shared.keys.root.as_str(), Some(options)),
    )
    .await
    {
        Ok(Ok(watch)) => Ok(watch),
        Ok(Err(error)) => Err(error.to_string()),
        Err(_) => Err("opening the watch timed out".to_string()),
    }
}

enum Reconnect {
    Watching(Watcher, WatchStream),
    /// History after the last applied revision is gone.
    Compacted,
    Failed(String),
}

/// Reopen the watch from the last applied revision + 1, so nothing applied
/// is applied (and reported) twice (D-6). It first reads at that revision:
/// a compacted one fails the read, and the store reloads before it can
/// briefly report `Live` over a stale cache.
async fn reconnect(shared: &Shared) -> Reconnect {
    let start = *shared.applied.borrow() + 1;
    let mut kv = shared.client.kv_client();
    let probe = kv.get(
        shared.keys.barrier.as_str(),
        Some(GetOptions::new().with_revision(start)),
    );
    match tokio::time::timeout(shared.settings.request_deadline, probe).await {
        Ok(Ok(_)) => {}
        Ok(Err(error)) if error.to_string().contains("compacted") => return Reconnect::Compacted,
        // Nothing was written since: the revision is still in the future.
        Ok(Err(error)) if error.to_string().contains("future revision") => {}
        Ok(Err(error)) => return Reconnect::Failed(error.to_string()),
        Err(_) => return Reconnect::Failed("the compaction probe timed out".to_string()),
    }
    match open_watch(shared, start).await {
        Ok((watcher, stream)) => Reconnect::Watching(watcher, stream),
        Err(error) => Reconnect::Failed(error),
    }
}

/// History is gone: report `Lost`, reload the prefix, report what vanished
/// in the gap, then clear `Lost`. Returns false if cancelled first.
async fn recover(shared: &Shared, cancel: &CancellationToken) -> bool {
    shared.set_lost(true);
    loop {
        // One txn, so the records and the lock are read at one revision.
        let txn = Txn::new().and_then([
            TxnOp::get(
                shared.keys.instances.as_str(),
                Some(GetOptions::new().with_prefix()),
            ),
            TxnOp::get(shared.keys.lock.as_str(), None),
        ]);
        let deadline = tokio::time::Instant::now() + shared.settings.request_deadline;
        match shared.send(txn, deadline, "reload").await {
            Ok(response) => {
                let revision = response.header().map_or(0, |header| header.revision());
                let responses = response.op_responses();
                let (Some(TxnOpResponse::Get(records)), Some(TxnOpResponse::Get(lock))) =
                    (responses.first(), responses.get(1))
                else {
                    tracing::warn!("the etcd reload answered an unexpected shape; retrying");
                    if !pause(cancel, RETRY).await {
                        return false;
                    }
                    continue;
                };
                if lock
                    .kvs()
                    .first()
                    .is_none_or(|kv| kv.lease() != shared.lock_lease)
                    && !shared.closing.is_cancelled()
                {
                    shared.fence("the reload found the lock key is not this hub's");
                }
                shared.reload(records.kvs(), revision);
                shared.set_lost(false);
                return true;
            }
            Err(error) => {
                tracing::warn!(%error, "reloading the etcd store failed; retrying");
                if !pause(cancel, RETRY).await {
                    return false;
                }
            }
        }
    }
}

/// Feed the cache from the prefix watch, starting with `first`.
pub(super) async fn watch(
    shared: Arc<Shared>,
    first: (Watcher, WatchStream),
    cancel: CancellationToken,
) {
    let mut holds = shared.holds.subscribe();
    let mut next = Some(first);
    loop {
        let (watcher, mut events) = match next.take() {
            Some(watch) => watch,
            None => {
                while holds.disconnected() {
                    tokio::select! {
                        _ = cancel.cancelled() => return,
                        _ = holds.changed() => {}
                    }
                }
                match reconnect(&shared).await {
                    Reconnect::Watching(watcher, stream) => (watcher, stream),
                    Reconnect::Compacted => {
                        if !recover(&shared, &cancel).await {
                            return;
                        }
                        continue;
                    }
                    Reconnect::Failed(error) => {
                        tracing::warn!(%error, "reconnecting the etcd watch failed; retrying");
                        if !pause(&cancel, RETRY).await {
                            return;
                        }
                        continue;
                    }
                }
            }
        };
        shared.set_disconnected(false);
        // Dropping the watcher closes the request side; keep it while the
        // stream is read.
        let _watcher = watcher;
        let compacted = loop {
            if holds.disconnected() {
                break false;
            }
            if holds.paused() {
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = holds.changed() => {}
                }
                continue;
            }
            let message = tokio::select! {
                biased;
                _ = cancel.cancelled() => return,
                _ = holds.changed() => continue,
                message = events.message() => message,
            };
            match message {
                Ok(Some(response)) if response.compact_revision() != 0 => {
                    tracing::warn!(
                        compact_revision = response.compact_revision(),
                        "the etcd watch is behind compaction; reloading"
                    );
                    break true;
                }
                Ok(Some(response)) if response.canceled() => {
                    tracing::warn!(
                        reason = response.cancel_reason(),
                        "etcd cancelled the watch"
                    );
                    break false;
                }
                Ok(Some(response)) => shared.apply(response.events()),
                Ok(None) => {
                    tracing::warn!("the etcd watch stream closed");
                    break false;
                }
                Err(error) => {
                    tracing::warn!(%error, "the etcd watch stream failed");
                    break false;
                }
            }
        };
        shared.set_disconnected(true);
        if compacted && !recover(&shared, &cancel).await {
            return;
        }
    }
}

/// Whose lease a keep-alive task holds up.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum LeaseRole {
    /// The lock lease: losing it fences the store.
    Lock,
    /// This store's custody lease: losing it deletes every record it holds,
    /// so it fences the store too.
    Custody,
    /// A custody lease of the hub before this one, kept while a loaded
    /// record names it (D-1).
    Adopted,
}

fn referenced(shared: &Shared, lease: i64) -> bool {
    shared.cache.read().lease_refs.contains_key(&lease)
}

fn lease_gone(shared: &Shared, lease: i64, role: LeaseRole) {
    match role {
        LeaseRole::Adopted => tracing::info!(lease, "an adopted custody lease expired"),
        LeaseRole::Lock | LeaseRole::Custody if !shared.closing.is_cancelled() => {
            shared.fence(&format!("the {role:?} lease {lease} expired"));
        }
        LeaseRole::Lock | LeaseRole::Custody => {}
    }
}

/// Keep `lease` alive at a third of its TTL. A keep-alive answering TTL 0
/// means the lease expired.
pub(super) async fn keep_alive(
    shared: Arc<Shared>,
    lease: i64,
    role: LeaseRole,
    cancel: CancellationToken,
) {
    let mut interval = match role {
        LeaseRole::Lock => third_of(shared.settings.lock_ttl_secs),
        LeaseRole::Custody => third_of(shared.settings.custody_ttl_secs),
        // Learned from the first answer.
        LeaseRole::Adopted => Duration::from_secs(1),
    };
    let deadline = shared.settings.request_deadline;
    loop {
        if role == LeaseRole::Adopted && !referenced(&shared, lease) {
            tracing::debug!(
                lease,
                "no record names the adopted lease; letting it expire"
            );
            return;
        }
        let mut client = shared.client.lease_client();
        let opened = tokio::select! {
            _ = cancel.cancelled() => return,
            opened = tokio::time::timeout(deadline, client.keep_alive(lease)) => opened,
        };
        let (mut keeper, mut answers) = match opened {
            Ok(Ok(pair)) => pair,
            Ok(Err(etcd_client::Error::LeaseKeepAliveError(message)))
                if message.contains("not found") =>
            {
                lease_gone(&shared, lease, role);
                return;
            }
            Ok(Err(error)) => {
                tracing::warn!(lease, %error, "opening a lease keep-alive failed; retrying");
                if !pause(&cancel, RETRY).await {
                    return;
                }
                continue;
            }
            Err(_) => {
                tracing::warn!(lease, "opening a lease keep-alive timed out; retrying");
                continue;
            }
        };
        loop {
            if !pause(&cancel, interval).await {
                return;
            }
            if role == LeaseRole::Adopted && !referenced(&shared, lease) {
                tracing::debug!(
                    lease,
                    "no record names the adopted lease; letting it expire"
                );
                return;
            }
            if keeper.keep_alive().await.is_err() {
                break;
            }
            match tokio::time::timeout(deadline, answers.message()).await {
                Ok(Ok(Some(answer))) if answer.ttl() > 0 => {
                    if role == LeaseRole::Adopted {
                        interval = third_of(answer.ttl());
                    }
                }
                Ok(Ok(Some(_))) => {
                    lease_gone(&shared, lease, role);
                    return;
                }
                _ => break,
            }
        }
    }
}

fn third_of(ttl_secs: i64) -> Duration {
    (Duration::from_secs(u64::try_from(ttl_secs).unwrap_or(1)) / 3).max(Duration::from_millis(100))
}

/// Run a reaper pass every prune interval.
pub(super) async fn reap(shared: Arc<Shared>, cancel: CancellationToken) {
    let mut ticker = tokio::time::interval(shared.settings.prune_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker.tick().await;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = ticker.tick() => {
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = shared.reap_stale() => {}
                }
            }
        }
    }
}
