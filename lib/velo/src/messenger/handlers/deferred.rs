// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! A staged payload that a handler fetches only if it asks.
//!
//! A handler built with `defer_large_payload` runs before the receiver
//! fetches a staged payload. The receive loop builds a [`DeferredPayload`]
//! instead, and the handler's adapter holds it in the messenger's
//! [`DeferredPayloads`] until the handler returns. `Context` cannot gain a
//! field, so the handler's `MessageId` carries the hold's key, and
//! `Context::payload_len` and `Context::take_payload` find the payload with
//! it. The key is this receiver's own: a message id comes off the wire, and
//! two messages can carry one.
//!
//! Ending the hold before the reply is sent is what makes "the fetch must
//! finish before the handler replies" true: it ends a take that is still
//! running, and the sender frees the payload when the reply arrives.

use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::messenger::large_payload::{FetchBound, LargePayloadResolver};

/// The lease deadline that a take without a bound asks the owner for.
///
/// A handler can drop a take at any await, and the handler's return ends one
/// that is still running. The owner is not told. Without a deadline, the
/// lease it grants an acquire that nobody waits for would hold the payload
/// until the owner tears down, even after the reply. Each pull renews the
/// lease, so the deadline does not bound a fetch that makes progress.
const UNBOUNDED_LEASE: Duration = Duration::from_secs(30);

/// A handler's fetch setting, from its builder.
#[derive(Clone, Default)]
pub(crate) struct DeferSetting {
    /// The most fetches the handler runs at once, if capped.
    pub(crate) cap: Option<Arc<FetchCap>>,
}

pub(crate) struct FetchCap {
    pub(crate) limit: usize,
    pub(crate) permits: Semaphore,
}

impl DeferSetting {
    pub(crate) fn capped(limit: usize) -> Self {
        assert!(
            limit > 0,
            "max_concurrent_fetches(0) would refuse every fetch"
        );
        Self {
            cap: Some(Arc::new(FetchCap {
                limit,
                permits: Semaphore::new(limit),
            })),
        }
    }
}

/// `Context::take_payload` found the handler's fetch cap
/// (`max_concurrent_fetches`) full. Nothing was fetched. Find it with
/// `err.is::<FetchLimitReached>()` and refuse the request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("this handler already runs its limit of {limit} staged fetches")]
pub struct FetchLimitReached {
    /// The handler's cap.
    pub limit: usize,
}

/// How to fetch one staged payload, and by when.
pub(crate) struct Fetch {
    pub(crate) resolver: Arc<dyn LargePayloadResolver>,
    pub(crate) handle: String,
    /// The message's fetch bound, counted from `admitted`.
    pub(crate) bound: Option<FetchBound>,
    /// When the receive loop took the message off its queue.
    pub(crate) admitted: tokio::time::Instant,
}

/// One deferred message's staged payload.
pub(crate) struct DeferredPayload {
    pub(crate) len: u64,
    /// Taken by the first `take`, so the payload is fetched at most once.
    fetch: parking_lot::Mutex<Option<Fetch>>,
    cap: Option<Arc<FetchCap>>,
    /// Ends a take. The handler's return cancels it, so no fetch runs on
    /// after the reply. So does the receiver's teardown, as it ends a fetch
    /// before dispatch: a fetch with no bound would otherwise hold the
    /// message's drain guard for as long as the owner stays silent.
    ended: CancellationToken,
}

impl DeferredPayload {
    pub(crate) fn new(
        len: u64,
        fetch: Fetch,
        setting: DeferSetting,
        teardown: &CancellationToken,
    ) -> Self {
        Self {
            len,
            fetch: parking_lot::Mutex::new(Some(fetch)),
            cap: setting.cap,
            ended: teardown.child_token(),
        }
    }

    /// Fetch the payload, once, within what is left of its bound.
    pub(crate) async fn take(&self) -> Result<Bytes> {
        // A full cap refuses before the fetch is taken, so the handler can
        // try again.
        let _permit = match &self.cap {
            Some(cap) => Some(
                cap.permits
                    .try_acquire()
                    .map_err(|_| FetchLimitReached { limit: cap.limit })?,
            ),
            None => None,
        };
        let fetch = self
            .fetch
            .lock()
            .take()
            .ok_or_else(|| anyhow::anyhow!("the staged payload was already taken"))?;
        // Read just before the wait it bounds.
        let bound = fetch.bound.map(|bound| FetchBound {
            within: bound.within.saturating_sub(fetch.admitted.elapsed()),
            ..bound
        });
        // A timeout polls its future once before its timer, and that poll
        // would ask the owner for a payload the caller has given up on.
        if bound.is_some_and(|bound| bound.within.is_zero()) {
            anyhow::bail!("the fetch bound of the staged payload ran out before it was taken");
        }
        let lease = bound.map_or(UNBOUNDED_LEASE, |bound| bound.within);
        let resolve = fetch.resolver.resolve(&fetch.handle, Some(lease));
        tokio::select! {
            // The end first: a take that starts after it must not ask the
            // owner, and the first poll of the fetch does.
            biased;
            () = self.ended.cancelled() => Err(anyhow::anyhow!(
                "the handler returned, or the receiver shut down, before the staged payload was fetched"
            )),
            resolved = crate::messenger::server::resolve_within(resolve, bound) => {
                resolved.unwrap_or_else(|| {
                    Err(anyhow::anyhow!(
                        "the caller's deadline passed before the staged payload was fetched"
                    ))
                })
            }
        }
    }
}

/// The deferred payloads of the handlers running now, each under a key of
/// its own.
#[derive(Default)]
pub(crate) struct DeferredPayloads {
    held: Arc<DashMap<NonZeroU64, Arc<DeferredPayload>>>,
    last_key: AtomicU64,
}

impl DeferredPayloads {
    /// Make `payload` reachable from its handler's `Context`, under the
    /// hold's key, until the hold drops.
    pub(crate) fn hold(&self, payload: Arc<DeferredPayload>) -> DeferredHold {
        let key = NonZeroU64::MIN.saturating_add(self.last_key.fetch_add(1, Ordering::Relaxed));
        self.held.insert(key, payload);
        DeferredHold {
            held: Arc::clone(&self.held),
            key,
        }
    }

    pub(crate) fn get(&self, key: NonZeroU64) -> Option<Arc<DeferredPayload>> {
        self.held.get(&key).map(|entry| Arc::clone(entry.value()))
    }
}

/// Ends a deferred payload's reach, and a take of it that is still running,
/// when dropped. The adapter drops it when the handler returns, before the
/// reply is sent.
pub(crate) struct DeferredHold {
    held: Arc<DashMap<NonZeroU64, Arc<DeferredPayload>>>,
    pub(crate) key: NonZeroU64,
}

impl Drop for DeferredHold {
    fn drop(&mut self) {
        if let Some((_, payload)) = self.held.remove(&self.key) {
            payload.ended.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::BoxFuture;

    /// A resolver whose owner never answers. It records the lease deadline
    /// of each fetch that reaches it.
    #[derive(Default)]
    struct Silent {
        asked: parking_lot::Mutex<Vec<Option<Duration>>>,
    }

    impl LargePayloadResolver for Silent {
        fn resolve(&self, _: &str, lease: Option<Duration>) -> BoxFuture<'_, Result<Bytes>> {
            Box::pin(async move {
                self.asked.lock().push(lease);
                std::future::pending().await
            })
        }

        fn size(&self, _: &str) -> BoxFuture<'_, Result<u64>> {
            Box::pin(std::future::pending())
        }
    }

    fn payload(
        owner: &Arc<Silent>,
        bound: Option<Duration>,
        teardown: &CancellationToken,
    ) -> Arc<DeferredPayload> {
        Arc::new(DeferredPayload::new(
            1,
            Fetch {
                resolver: Arc::clone(owner) as _,
                handle: "1".into(),
                bound: bound.map(|within| FetchBound {
                    within,
                    callers: true,
                }),
                admitted: tokio::time::Instant::now(),
            },
            DeferSetting::default(),
            teardown,
        ))
    }

    /// A deferred fetch ends at the bound the message got when it was taken
    /// off the queue, not a full bound from when the handler asks. A handler
    /// that asks 300 ms into a 400 ms bound has 100 ms, so a slow handler
    /// cannot keep fetching after its caller has given up. The lease
    /// deadline it asks the owner for is the same 100 ms.
    #[tokio::test(start_paused = true)]
    async fn a_deferred_fetch_ends_at_the_admission_deadline() {
        let owner = Arc::new(Silent::default());
        let admitted = tokio::time::Instant::now();
        let payload = payload(
            &owner,
            Some(Duration::from_millis(400)),
            &CancellationToken::new(),
        );
        tokio::time::sleep(Duration::from_millis(300)).await;

        assert!(payload.take().await.is_err());
        assert_eq!(admitted.elapsed(), Duration::from_millis(400));
        assert_eq!(*owner.asked.lock(), [Some(Duration::from_millis(100))]);
    }

    /// A take without a bound still asks the owner for a lease deadline. A
    /// handler can drop a take at any await, for example in a `select!`, and
    /// the receiver tells the owner nothing. An owner that grants the lease
    /// after that would keep the payload until it tears down, even after the
    /// reply, because only a release or a deadline ends a lease. Pulls renew
    /// the deadline (`rendezvous::store`'s
    /// `a_chunked_lease_deadline_is_renewed_by_pulls_and_reaped_without_them`),
    /// so it does not bound a fetch that makes progress.
    #[tokio::test(start_paused = true)]
    async fn an_unbounded_take_asks_for_a_lease_deadline() {
        let owner = Arc::new(Silent::default());
        let payload = payload(&owner, None, &CancellationToken::new());

        let abandoned = tokio::time::timeout(Duration::from_secs(1), payload.take()).await;

        assert!(abandoned.is_err(), "the owner never answers");
        assert_eq!(*owner.asked.lock(), [Some(UNBOUNDED_LEASE)]);
    }

    /// The handler's return ends a take that is still running, and a take
    /// that starts after it never reaches the owner. The reply goes out
    /// next and the sender frees the payload on it, so a context clone in a
    /// spawned task must not go on pulling: it once fetched a whole payload
    /// after its caller had the reply. Teardown ends a take the same way,
    /// as it ends a fetch before dispatch.
    #[tokio::test]
    async fn the_end_of_the_hold_or_teardown_ends_a_take() {
        for by_teardown in [false, true] {
            let owner = Arc::new(Silent::default());
            let teardown = CancellationToken::new();
            let table = DeferredPayloads::default();
            let hold = table.hold(payload(&owner, None, &teardown));
            let running = table.get(hold.key).unwrap();
            let late = Arc::clone(&running);
            let running = tokio::spawn(async move { running.take().await });
            while owner.asked.lock().is_empty() {
                tokio::task::yield_now().await;
            }

            if by_teardown {
                teardown.cancel();
            } else {
                drop(hold);
            }

            assert!(running.await.unwrap().is_err());
            assert!(late.take().await.is_err());
            assert_eq!(owner.asked.lock().len(), 1, "by_teardown: {by_teardown}");
        }
    }

    /// Each hold has a key of its own, so a handler reaches only its own
    /// payload. A message id comes off the wire, and two messages can carry
    /// the same one. Keyed by message id, the second hold replaced the
    /// first, and the first handler took the second message's payload.
    #[test]
    fn each_hold_reaches_only_its_own_payload() {
        let owner = Arc::new(Silent::default());
        let teardown = CancellationToken::new();
        let table = DeferredPayloads::default();
        let payloads = [(); 2].map(|()| payload(&owner, None, &teardown));
        let holds = payloads.clone().map(|payload| table.hold(payload));

        assert_ne!(holds[0].key, holds[1].key);
        for (hold, payload) in holds.iter().zip(&payloads) {
            assert!(Arc::ptr_eq(&table.get(hold.key).unwrap(), payload));
        }
        let [first, second] = holds;
        let key = first.key;
        drop(first);
        assert!(table.get(key).is_none());
        assert!(table.get(second.key).is_some());
    }
}
