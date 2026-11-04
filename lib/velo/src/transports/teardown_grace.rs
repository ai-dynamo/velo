// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! What a TCP or UDS connection writer does when its transport tears down:
//! it keeps writing, and keeps dialing, for a grace after the teardown, so
//! that the frames the transport admitted before it closed still go out.
//! One [`TeardownStop`] per transport carries the grace, the one deadline
//! every writer of that transport shares, and the writers themselves, so that
//! `closed()` can wait for them.

use std::time::Duration;

use tokio_util::sync::CancellationToken;

/// A deadline that never comes, for a grace the clock cannot add: thirty
/// years out, as tokio's own far future is.
fn far_future() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(86_400 * 365 * 30)
}

/// The default for how long a TCP or UDS writer keeps writing at teardown:
/// the frames already in its channel, the write in progress, and a dial in
/// progress. Those frames were admitted, and a sender may act on that at once
/// (the messenger mux reports a stream sent once the batch that ends it is
/// admitted). A writer stops at this deadline so that a peer that does not
/// read cannot hold teardown open. The transport builders set it
/// (`teardown_flush_grace`).
pub(crate) const TEARDOWN_FLUSH_GRACE: Duration = Duration::from_secs(1);

/// How much longer than the teardown deadline `closed()` waits for the
/// connection writers. Each one stops by that deadline on its own, so the
/// margin only matters when the runtime that runs them is not being driven.
const WRITERS_CLOSE_MARGIN: Duration = Duration::from_millis(500);

/// The teardown a TCP or UDS connection writer may be stopped by, and the one
/// deadline for everything it still does after: a dial in progress, the write
/// in progress, and the frames already in its channel. The transport's
/// `shutdown` fixes the deadline; a writer that asks first fixes it the same
/// way, so no writer gets a grace of its own. It also counts the writers that
/// can still write, so that `closed()` can wait for what teardown keeps.
pub(crate) struct TeardownStop {
    /// The transport's token: only shutdown cancels it. A connection's own
    /// token is a child of it, so a writer that sees a stop asks here whether
    /// that stop is teardown.
    token: CancellationToken,
    /// How long after the teardown the writers keep writing.
    grace: Duration,
    deadline: std::sync::OnceLock<tokio::time::Instant>,
    /// One token per connection writer while it can still write.
    writers: tokio_util::task::TaskTracker,
}

impl TeardownStop {
    pub(crate) fn new(token: CancellationToken, grace: Duration) -> Self {
        Self {
            token,
            grace,
            deadline: std::sync::OnceLock::new(),
            writers: tokio_util::task::TaskTracker::new(),
        }
    }

    /// `Some(deadline)` once the transport tears down: what the writer still
    /// holds runs until then. `None` while a stop could only be the
    /// connection's own (an unregister, or the peer closed the socket), where
    /// nothing could deliver the frames. A grace too large for the clock
    /// means a deadline that never comes.
    pub(crate) fn deadline(&self) -> Option<tokio::time::Instant> {
        self.token.is_cancelled().then(|| {
            *self.deadline.get_or_init(|| {
                tokio::time::Instant::now()
                    .checked_add(self.grace)
                    .unwrap_or_else(far_future)
            })
        })
    }

    /// Run `pending` (a dial) until the connection's `cancel` fires. Teardown
    /// lets it run on to the deadline, as the writes after it do; a stop that
    /// is only the connection's ends it at once. `None` when it was stopped.
    pub(crate) async fn until_stopped<F: std::future::Future>(
        &self,
        cancel: &CancellationToken,
        pending: F,
    ) -> Option<F::Output> {
        tokio::pin!(pending);
        tokio::select! {
            _ = cancel.cancelled() => match self.deadline() {
                Some(deadline) => tokio::time::timeout_at(deadline, &mut pending).await.ok(),
                None => None,
            },
            output = &mut pending => Some(output),
        }
    }

    /// The token a connection writer holds while it can still write.
    pub(crate) fn writer(&self) -> tokio_util::task::task_tracker::TaskTrackerToken {
        self.writers.token()
    }

    /// For the transport's `shutdown`: cancel the transport's token, then fix
    /// the deadline, so every writer's teardown ends at the same time. Both in
    /// one call: a deadline asked for before the cancel would be `None`, and
    /// the first writer to ask after it would fix it instead.
    pub(crate) fn begin(&self) {
        self.token.cancel();
        self.deadline();
        self.writers.close();
    }

    /// For the transport's `closed`: wait for the writers to write what
    /// teardown kept, until the teardown deadline plus a margin. `what` names
    /// the transport in the warning when they do not finish.
    pub(crate) async fn wait_for_writers(&self, what: &str) {
        let Some(deadline) = self.deadline() else {
            return;
        };
        self.writers.close();
        let bound = deadline
            .checked_add(WRITERS_CLOSE_MARGIN)
            .unwrap_or_else(far_future);
        if tokio::time::timeout_at(bound, self.writers.wait())
            .await
            .is_err()
        {
            tracing::warn!(
                "{what}: connection writers did not finish within {:?} of teardown",
                self.grace.saturating_add(WRITERS_CLOSE_MARGIN)
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stop keeps what is queued only once the transport tears down. Before
    /// that, a stop is the connection's own (an unregister, or the peer closed
    /// the socket), and the writer gives up at once. The deadline is fixed
    /// when it is first asked, so a writer that asks later gets the same one,
    /// not a fresh grace; and the grace is the transport's to set.
    #[tokio::test(start_paused = true)]
    async fn a_stop_is_teardown_only_once_the_transport_tears_down() {
        let token = CancellationToken::new();
        let stop = TeardownStop::new(token.clone(), TEARDOWN_FLUSH_GRACE);
        assert_eq!(
            stop.deadline(),
            None,
            "a connection-only stop keeps nothing"
        );

        token.cancel();
        let deadline = stop.deadline().expect("teardown keeps what is queued");
        assert_eq!(deadline, tokio::time::Instant::now() + TEARDOWN_FLUSH_GRACE);
        tokio::time::advance(Duration::from_millis(300)).await;
        assert_eq!(
            stop.deadline(),
            Some(deadline),
            "one deadline for every writer"
        );

        let token = CancellationToken::new();
        let stop = TeardownStop::new(token.clone(), Duration::from_secs(90));
        token.cancel();
        assert_eq!(
            stop.deadline(),
            Some(tokio::time::Instant::now() + Duration::from_secs(90))
        );
    }

    /// `begin` fixes the deadline at the shutdown, so a writer that first
    /// asks later gets a deadline measured from the shutdown, not a fresh
    /// grace of its own.
    #[tokio::test(start_paused = true)]
    async fn the_deadline_is_fixed_at_the_shutdown() {
        let token = CancellationToken::new();
        let stop = TeardownStop::new(token.clone(), Duration::from_secs(2));
        token.cancel();
        let shutdown = tokio::time::Instant::now();
        stop.begin();
        tokio::time::advance(Duration::from_millis(1500)).await;
        assert_eq!(stop.deadline(), Some(shutdown + Duration::from_secs(2)));
    }

    /// A grace of "forever" is a deadline that never comes, not a panic in
    /// the shutdown hook.
    #[tokio::test(start_paused = true)]
    async fn a_grace_of_forever_does_not_overflow() {
        let token = CancellationToken::new();
        let stop = TeardownStop::new(token.clone(), Duration::MAX);
        token.cancel();
        stop.begin();
        assert!(stop.deadline().is_some());
        // No writer holds a token, so this returns at once.
        tokio::time::timeout(Duration::from_secs(1), stop.wait_for_writers("test"))
            .await
            .expect("nothing to wait for");
    }

    /// A dial stopped by teardown runs on to the deadline; one stopped by its
    /// connection alone ends at once.
    #[tokio::test(start_paused = true)]
    async fn a_dial_runs_to_the_teardown_deadline() {
        let token = CancellationToken::new();
        let stop = TeardownStop::new(token.clone(), Duration::from_secs(2));
        let connection = token.child_token();

        // Connection-only stop: the pending dial is abandoned at once.
        let other = CancellationToken::new();
        other.cancel();
        assert_eq!(
            stop.until_stopped(&other, std::future::pending::<()>())
                .await,
            None
        );

        // Teardown: a dial that completes within the grace is kept.
        token.cancel();
        let started = tokio::time::Instant::now();
        let dial = async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            7
        };
        assert_eq!(stop.until_stopped(&connection, dial).await, Some(7));
        assert_eq!(started.elapsed(), Duration::from_secs(1));

        // One that does not is abandoned at the deadline.
        assert_eq!(
            stop.until_stopped(&connection, std::future::pending::<()>())
                .await,
            None
        );
        assert_eq!(started.elapsed(), Duration::from_secs(2));
    }
}
