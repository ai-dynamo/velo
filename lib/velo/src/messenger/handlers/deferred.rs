// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! A staged payload that a handler fetches only if it asks.
//!
//! A handler built with `defer_large_payload` runs before the receiver
//! fetches a staged payload. The receive loop builds a [`DeferredPayload`]
//! instead, and the handler's adapter holds it in the messenger's
//! [`DeferredPayloads`] under the message id until the handler returns.
//! `Context::payload_len` and `Context::take_payload` find it there, because
//! `Context` cannot gain a field. Ending the hold before the reply is sent
//! is what makes "the fetch must finish before the handler replies" true:
//! the sender frees the payload when the reply arrives.

use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::messenger::common::responses::ResponseId;
use crate::messenger::large_payload::{FetchBound, LargePayloadResolver};

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
    /// The receiver's teardown, which ends a fetch the way it ends one
    /// before dispatch: a fetch with no bound would otherwise hold the
    /// message's drain guard for as long as the owner stays silent.
    teardown: CancellationToken,
}

impl DeferredPayload {
    pub(crate) fn new(
        len: u64,
        fetch: Fetch,
        setting: DeferSetting,
        teardown: CancellationToken,
    ) -> Self {
        Self {
            len,
            fetch: parking_lot::Mutex::new(Some(fetch)),
            cap: setting.cap,
            teardown,
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
        let resolve = fetch
            .resolver
            .resolve(&fetch.handle, bound.map(|bound| bound.within));
        tokio::select! {
            biased;
            resolved = crate::messenger::server::resolve_within(resolve, bound) => {
                resolved.unwrap_or_else(|| {
                    Err(anyhow::anyhow!(
                        "the caller's deadline passed before the staged payload was fetched"
                    ))
                })
            }
            () = self.teardown.cancelled() => {
                Err(anyhow::anyhow!("the receiver shut down before the staged payload was fetched"))
            }
        }
    }
}

/// The deferred payloads of the handlers running now, by message id.
#[derive(Default)]
pub(crate) struct DeferredPayloads(Arc<DashMap<ResponseId, Arc<DeferredPayload>>>);

impl DeferredPayloads {
    /// Make `payload` reachable from its handler's `Context` until the guard
    /// drops.
    pub(crate) fn hold(&self, id: ResponseId, payload: Arc<DeferredPayload>) -> DeferredHold {
        self.0.insert(id, Arc::clone(&payload));
        DeferredHold {
            table: Arc::clone(&self.0),
            id,
            payload,
        }
    }

    pub(crate) fn get(&self, id: ResponseId) -> Option<Arc<DeferredPayload>> {
        self.0.get(&id).map(|entry| Arc::clone(entry.value()))
    }
}

/// Ends a deferred payload's reach when dropped. The adapter drops it when
/// the handler returns, before the reply is sent.
pub(crate) struct DeferredHold {
    table: Arc<DashMap<ResponseId, Arc<DeferredPayload>>>,
    id: ResponseId,
    payload: Arc<DeferredPayload>,
}

impl Drop for DeferredHold {
    fn drop(&mut self) {
        // Only its own: a duplicate message id must not end another hold.
        self.table
            .remove_if(&self.id, |_, held| Arc::ptr_eq(held, &self.payload));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::BoxFuture;
    use std::time::Duration;

    /// A resolver whose owner never answers.
    struct Silent;

    impl LargePayloadResolver for Silent {
        fn resolve(&self, _: &str, _: Option<Duration>) -> BoxFuture<'_, Result<Bytes>> {
            Box::pin(std::future::pending())
        }

        fn size(&self, _: &str) -> BoxFuture<'_, Result<u64>> {
            Box::pin(std::future::pending())
        }
    }

    /// A deferred fetch ends at the bound the message got when it was taken
    /// off the queue, not a full bound from when the handler asks. A handler
    /// that asks 300 ms into a 400 ms bound has 100 ms, so a slow handler
    /// cannot keep fetching after its caller has given up.
    #[tokio::test(start_paused = true)]
    async fn a_deferred_fetch_ends_at_the_admission_deadline() {
        let admitted = tokio::time::Instant::now();
        let payload = DeferredPayload::new(
            1,
            Fetch {
                resolver: Arc::new(Silent),
                handle: "1".into(),
                bound: Some(FetchBound {
                    within: Duration::from_millis(400),
                    callers: true,
                }),
                admitted,
            },
            DeferSetting::default(),
            CancellationToken::new(),
        );
        tokio::time::sleep(Duration::from_millis(300)).await;

        assert!(payload.take().await.is_err());
        assert_eq!(admitted.elapsed(), Duration::from_millis(400));
    }

    /// Teardown ends a fetch whose owner never answers and that has no
    /// bound, as it ends one before dispatch.
    #[tokio::test]
    async fn teardown_ends_an_unbounded_deferred_fetch() {
        let teardown = CancellationToken::new();
        let payload = DeferredPayload::new(
            1,
            Fetch {
                resolver: Arc::new(Silent),
                handle: "1".into(),
                bound: None,
                admitted: tokio::time::Instant::now(),
            },
            DeferSetting::default(),
            teardown.clone(),
        );
        teardown.cancel();
        assert!(payload.take().await.is_err());
    }

    /// A hold ends only its own entry, so a message id that arrives twice
    /// cannot end the other's reach.
    #[test]
    fn a_hold_ends_only_its_own_entry() {
        let table = DeferredPayloads::default();
        let id = ResponseId::from_u128(7);
        let payload = || {
            Arc::new(DeferredPayload::new(
                1,
                Fetch {
                    resolver: Arc::new(Silent),
                    handle: "1".into(),
                    bound: None,
                    admitted: tokio::time::Instant::now(),
                },
                DeferSetting::default(),
                CancellationToken::new(),
            ))
        };
        let first = table.hold(id, payload());
        let second = table.hold(id, payload());
        drop(first);
        assert!(table.get(id).is_some());
        drop(second);
        assert!(table.get(id).is_none());
    }
}
