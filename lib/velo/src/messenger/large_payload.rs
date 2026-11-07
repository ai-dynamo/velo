// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Trait-based extension point for transparent large payload handling.
//!
//! These traits let the rendezvous module (`crate::rendezvous`) plug into the
//! messenger send/receive path, so the messenger does not depend on it.
//!
//! **Sender side** ([`LargePayloadStager`]): When a payload exceeds a threshold,
//! the stager stores it locally and returns a handle string to embed in the message
//! headers. The actual payload is replaced with an empty `Bytes`.
//!
//! **Receiver side** ([`LargePayloadResolver`]): When a message arrives with the
//! `_rv` header, the resolver fetches the full payload from the remote stager
//! before the message is dispatched to the handler.
//!
//! **The fetch has a deadline when either side gives it one.** The sender
//! carries what is left of a call's `.timeout` in the [`RV_BUDGET_HEADER_KEY`]
//! header, and the receiver may set a bound of its own
//! ([`LargePayloadResolver::fetch_timeout`]). The fetch ends at the smaller of
//! the two. Without a deadline, a sender that stops answering keeps the
//! receiver's response slot and the request's drain guard until it answers
//! again, or until the receiver tears down.

use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use futures::future::BoxFuture;

/// Sender-side: stages a large payload locally and returns a handle string.
///
/// Called synchronously in the `send_message()` hot path when the payload
/// exceeds [`threshold()`](LargePayloadStager::threshold).
pub trait LargePayloadStager: Send + Sync {
    /// Stage the payload and return a handle string to embed in the `_rv` header.
    fn stage(&self, payload: Bytes) -> String;

    /// Minimum payload size (in bytes) to trigger staging.
    /// Payloads smaller than this are sent inline as usual.
    fn threshold(&self) -> usize;
}

/// Receiver-side: resolves a handle string back to the full payload.
///
/// Called asynchronously when a received message contains the `_rv` header.
/// The implementation should fetch the data and release the remote handle
/// (since transparent mode uses single-use staging).
pub trait LargePayloadResolver: Send + Sync {
    /// Fetch the full payload from the handle string and release the remote slot.
    fn resolve(&self, handle_str: &str) -> BoxFuture<'_, Result<Bytes>>;

    /// The longest this receiver lets one fetch run, whatever the sender
    /// asked for. `None` sets no bound of its own.
    fn fetch_timeout(&self) -> Option<Duration> {
        None
    }
}

/// Header key used to signal that the payload was staged via rendezvous.
pub const RV_HEADER_KEY: &str = "_rv";

/// Header key for the time the receiver has to fetch a staged payload, in
/// whole milliseconds, as a decimal string.
///
/// The sender sets it beside [`RV_HEADER_KEY`] when the call has a
/// `.timeout`, to what is left of that timeout when the payload is staged. It
/// is a duration, not an instant, because the two clocks are not the same
/// clock. The receiver starts it when it takes the message off its queue, so
/// its bound does not end before the caller's. How much later it ends depends
/// on transit, queueing and scheduling, which nothing bounds.
///
/// The encoder owns this key: it writes the send's own budget and drops any
/// other value it finds in the headers, whoever put it there.
///
/// A separate header, because a receiver that does not know it parses the
/// `_rv` value as a bare number and must keep doing so. Such a receiver
/// ignores this header, and a receiver that gets no header (from a sender
/// that does not set it) uses only its own bound.
pub const RV_BUDGET_HEADER_KEY: &str = "_rv_budget_ms";

/// The budget to send for a call that gives up `left` from now: whole
/// milliseconds, rounded up from nanoseconds, so the receiver's bound cannot
/// end before the caller's. Saturates at `u64::MAX` milliseconds, the most a
/// receiver parses, so a longer `left` (over 584 million years) is sent
/// short.
pub(crate) fn budget_ms(left: Duration) -> String {
    let ms = left.as_nanos().div_ceil(1_000_000);
    u64::try_from(ms).unwrap_or(u64::MAX).to_string()
}

/// How long a receiver may fetch one staged payload, and whose bound it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FetchBound {
    pub(crate) within: Duration,
    /// The caller's own deadline set it. The caller stops waiting no later
    /// than the fetch does, so it needs no reply: one would be discarded,
    /// and one that won the race with the caller's own timer would turn its
    /// timeout into a different error.
    pub(crate) callers: bool,
}

/// The fetch bound for one staged message: the sender's budget from
/// `headers`, or the receiver's `own` bound, whichever is smaller. On a tie
/// the bound is the caller's. A budget that does not parse counts as absent,
/// as it would on a receiver that does not know the header.
pub(crate) fn fetch_bound(
    headers: Option<&std::collections::HashMap<String, String>>,
    own: Option<Duration>,
) -> Option<FetchBound> {
    let sent = headers
        .and_then(|h| h.get(RV_BUDGET_HEADER_KEY))
        .and_then(|ms| ms.parse::<u64>().ok())
        .map(Duration::from_millis);
    let bound = |within, callers| FetchBound { within, callers };
    match (sent, own) {
        (Some(sent), Some(own)) if own < sent => Some(bound(own, false)),
        (Some(sent), _) => Some(bound(sent, true)),
        (None, own) => own.map(|own| bound(own, false)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn budget(ms: &str) -> HashMap<String, String> {
        HashMap::from([(RV_BUDGET_HEADER_KEY.to_string(), ms.to_string())])
    }

    fn callers(ms: u64) -> Option<FetchBound> {
        Some(FetchBound {
            within: Duration::from_millis(ms),
            callers: true,
        })
    }

    fn receivers(ms: u64) -> Option<FetchBound> {
        Some(FetchBound {
            within: Duration::from_millis(ms),
            callers: false,
        })
    }

    #[test]
    fn the_fetch_bound_is_the_smaller_of_the_two() {
        let ms = Duration::from_millis;
        assert_eq!(fetch_bound(None, None), None);
        assert_eq!(fetch_bound(Some(&HashMap::new()), None), None);
        assert_eq!(fetch_bound(Some(&budget("200")), None), callers(200));
        assert_eq!(fetch_bound(None, Some(ms(50))), receivers(50));
        assert_eq!(
            fetch_bound(Some(&budget("200")), Some(ms(50))),
            receivers(50)
        );
        assert_eq!(fetch_bound(Some(&budget("20")), Some(ms(50))), callers(20));
        assert_eq!(fetch_bound(Some(&budget("50")), Some(ms(50))), callers(50));
        assert_eq!(fetch_bound(Some(&budget("0")), None), callers(0));
    }

    /// A budget this receiver cannot read is treated as no budget, so a
    /// malformed header cannot fail a message that would otherwise resolve.
    #[test]
    fn a_budget_that_does_not_parse_counts_as_absent() {
        let ms = Duration::from_millis;
        assert_eq!(fetch_bound(Some(&budget("soon")), None), None);
        assert_eq!(
            fetch_bound(Some(&budget("-1")), Some(ms(50))),
            receivers(50)
        );
    }

    /// The budget rounds up from nanoseconds, never below what is left.
    /// Rounded down, a receiver that took the message off its queue at once
    /// ended the fetch before the caller's own timer. Rounded up from
    /// microseconds, a remainder under one microsecond was lost: 1 ns sent
    /// as 0 ms, and 200,000,001 ns as 200 ms.
    #[test]
    fn the_budget_rounds_up_to_whole_milliseconds() {
        assert_eq!(budget_ms(Duration::ZERO), "0");
        assert_eq!(budget_ms(Duration::from_nanos(1)), "1");
        assert_eq!(budget_ms(Duration::from_nanos(999)), "1");
        assert_eq!(budget_ms(Duration::from_nanos(1_000_001)), "2");
        assert_eq!(budget_ms(Duration::from_micros(199_400)), "200");
        assert_eq!(budget_ms(Duration::from_millis(200)), "200");
        assert_eq!(budget_ms(Duration::from_nanos(200_000_001)), "201");
        for ns in [1, 999, 1_000_001, 200_000_001, 7_123_456_789] {
            let left = Duration::from_nanos(ns);
            let sent = Duration::from_millis(budget_ms(left).parse().unwrap());
            assert!(sent >= left, "{ns} ns left was sent as {sent:?}");
        }
    }

    /// A deadline too far off for a `u64` of milliseconds sends the largest
    /// budget a receiver can parse, not a number it rejects.
    #[test]
    fn the_budget_saturates_at_what_a_receiver_parses() {
        assert_eq!(budget_ms(Duration::MAX), u64::MAX.to_string());
    }
}
