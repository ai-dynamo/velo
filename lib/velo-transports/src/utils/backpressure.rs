// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Send-path backpressure utilities shared across transports.
//!
//! All transports use a fail-fast pattern: `try_send()` on the fast path,
//! and `on_error(CHANNEL_FULL_ERROR)` when the channel is full. The
//! [`send_with_retry`] function provides an ergonomic wrapper for callers
//! that want automatic exponential-backoff retry on channel-full errors.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;

use crate::transport::TransportErrorHandler;

/// Well-known error string returned when a send channel is full.
pub const CHANNEL_FULL_ERROR: &str = "channel full";

/// Default bounded-channel capacity for all transports.
pub const DEFAULT_CHANNEL_CAPACITY: usize = 8192;

/// Configuration for [`send_with_retry`].
pub struct RetryConfig {
    /// Maximum number of retry attempts after the initial send.
    pub max_retries: u32,
    /// Initial backoff duration (doubled each retry, capped at `max_backoff`).
    pub initial_backoff: Duration,
    /// Maximum backoff duration.
    pub max_backoff: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(100),
        }
    }
}

/// Retry a fallible send with exponential backoff on channel-full errors.
///
/// `send_fn` is called with `(header, payload, error_handler)`. If the error
/// handler receives a [`CHANNEL_FULL_ERROR`], the call is retried after a
/// backoff sleep. Other errors (or success) stop iteration immediately.
///
/// Uses `std::thread::sleep` for backoff — acceptable since `send_message`
/// is already a synchronous function.
pub fn send_with_retry(
    send_fn: impl Fn(Bytes, Bytes, Arc<dyn TransportErrorHandler>),
    header: Bytes,
    payload: Bytes,
    on_error: Arc<dyn TransportErrorHandler>,
    config: RetryConfig,
) {
    let detector = Arc::new(FullDetector {
        inner: on_error,
        captured: Mutex::new(None),
    });

    let total_attempts = config.max_retries + 1;
    let mut backoff = config.initial_backoff;

    for attempt in 0..total_attempts {
        let det = Arc::clone(&detector) as Arc<dyn TransportErrorHandler>;
        send_fn(header.clone(), payload.clone(), det);

        // Check if the detector intercepted a channel-full error.
        let captured = detector.captured.lock().take();
        match captured {
            Some((h, p)) => {
                if attempt + 1 < total_attempts {
                    // Retry after backoff.
                    std::thread::sleep(backoff);
                    backoff = (backoff * 2).min(config.max_backoff);
                } else {
                    // Exhausted retries — forward the error to the real handler.
                    detector.inner.on_error(h, p, CHANNEL_FULL_ERROR.into());
                }
            }
            None => {
                // Either success or a non-full error (already forwarded).
                return;
            }
        }
    }
}

/// Private wrapper that intercepts [`CHANNEL_FULL_ERROR`] and captures the
/// message data for retry, forwarding all other errors to the real handler.
struct FullDetector {
    inner: Arc<dyn TransportErrorHandler>,
    captured: Mutex<Option<(Bytes, Bytes)>>,
}

impl TransportErrorHandler for FullDetector {
    fn on_error(&self, header: Bytes, payload: Bytes, error: String) {
        if error == CHANNEL_FULL_ERROR {
            *self.captured.lock() = Some((header, payload));
        } else {
            self.inner.on_error(header, payload, error);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Instant;

    // ── Test error handler ────────────────────────────────────────

    struct TrackingHandler {
        count: AtomicU32,
        last_error: Mutex<Option<String>>,
    }

    impl TrackingHandler {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                count: AtomicU32::new(0),
                last_error: Mutex::new(None),
            })
        }
    }

    impl TransportErrorHandler for TrackingHandler {
        fn on_error(&self, _header: Bytes, _payload: Bytes, error: String) {
            self.count.fetch_add(1, Ordering::SeqCst);
            *self.last_error.lock() = Some(error);
        }
    }

    // ── Constants ─────────────────────────────────────────────────

    #[test]
    fn test_default_channel_capacity() {
        assert_eq!(DEFAULT_CHANNEL_CAPACITY, 8192);
    }

    #[test]
    fn test_channel_full_error_value() {
        assert_eq!(CHANNEL_FULL_ERROR, "channel full");
    }

    // ── RetryConfig ───────────────────────────────────────────────

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.initial_backoff, Duration::from_millis(10));
        assert_eq!(config.max_backoff, Duration::from_millis(100));
    }

    // ── send_with_retry ───────────────────────────────────────────

    #[test]
    fn test_send_with_retry_success_no_retry() {
        let handler = TrackingHandler::new();
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts2 = Arc::clone(&attempts);

        send_with_retry(
            move |_h, _p, _err| {
                attempts2.fetch_add(1, Ordering::SeqCst);
                // Success: don't call on_error at all.
            },
            Bytes::from_static(b"header"),
            Bytes::from_static(b"payload"),
            handler.clone(),
            RetryConfig::default(),
        );

        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert_eq!(handler.count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_send_with_retry_full_then_success() {
        let handler = TrackingHandler::new();
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts2 = Arc::clone(&attempts);

        send_with_retry(
            move |h, p, err| {
                let n = attempts2.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    // First attempt: channel full.
                    err.on_error(h, p, CHANNEL_FULL_ERROR.into());
                }
                // Second attempt: success (no on_error call).
            },
            Bytes::from_static(b"header"),
            Bytes::from_static(b"payload"),
            handler.clone(),
            RetryConfig {
                max_retries: 3,
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(10),
            },
        );

        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        // The real handler should NOT have been called.
        assert_eq!(handler.count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_send_with_retry_exhausts_retries() {
        let handler = TrackingHandler::new();
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts2 = Arc::clone(&attempts);

        send_with_retry(
            move |h, p, err| {
                attempts2.fetch_add(1, Ordering::SeqCst);
                // Always full.
                err.on_error(h, p, CHANNEL_FULL_ERROR.into());
            },
            Bytes::from_static(b"header"),
            Bytes::from_static(b"payload"),
            handler.clone(),
            RetryConfig {
                max_retries: 3,
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(10),
            },
        );

        // 1 initial + 3 retries = 4 attempts.
        assert_eq!(attempts.load(Ordering::SeqCst), 4);
        // Real handler called once with the final error.
        assert_eq!(handler.count.load(Ordering::SeqCst), 1);
        assert_eq!(
            handler.last_error.lock().as_deref(),
            Some(CHANNEL_FULL_ERROR)
        );
    }

    #[test]
    fn test_send_with_retry_non_full_error_no_retry() {
        let handler = TrackingHandler::new();
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts2 = Arc::clone(&attempts);

        send_with_retry(
            move |h, p, err| {
                attempts2.fetch_add(1, Ordering::SeqCst);
                err.on_error(h, p, "connection closed".into());
            },
            Bytes::from_static(b"header"),
            Bytes::from_static(b"payload"),
            handler.clone(),
            RetryConfig::default(),
        );

        // Only 1 attempt — non-full error stops retry.
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        // Error forwarded to the real handler.
        assert_eq!(handler.count.load(Ordering::SeqCst), 1);
        assert_eq!(
            handler.last_error.lock().as_deref(),
            Some("connection closed")
        );
    }

    #[test]
    fn test_send_with_retry_exponential_backoff() {
        let handler = TrackingHandler::new();
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts2 = Arc::clone(&attempts);

        let start = Instant::now();
        send_with_retry(
            move |h, p, err| {
                attempts2.fetch_add(1, Ordering::SeqCst);
                err.on_error(h, p, CHANNEL_FULL_ERROR.into());
            },
            Bytes::from_static(b"header"),
            Bytes::from_static(b"payload"),
            handler.clone(),
            RetryConfig {
                max_retries: 3,
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(10),
            },
        );
        let elapsed = start.elapsed();

        assert_eq!(attempts.load(Ordering::SeqCst), 4);
        // Backoff: 1ms + 2ms + 4ms = 7ms minimum.
        assert!(
            elapsed >= Duration::from_millis(5),
            "Expected >= 5ms of backoff, got {elapsed:?}"
        );
    }

    #[test]
    fn test_send_with_retry_zero_retries() {
        let handler = TrackingHandler::new();
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts2 = Arc::clone(&attempts);

        send_with_retry(
            move |h, p, err| {
                attempts2.fetch_add(1, Ordering::SeqCst);
                err.on_error(h, p, CHANNEL_FULL_ERROR.into());
            },
            Bytes::from_static(b"header"),
            Bytes::from_static(b"payload"),
            handler.clone(),
            RetryConfig {
                max_retries: 0,
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(10),
            },
        );

        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert_eq!(handler.count.load(Ordering::SeqCst), 1);
    }
}
