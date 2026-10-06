// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

/// Tasks owned by one streaming service. Shutdown refuses new tasks before it waits.
#[derive(Clone)]
pub(crate) struct StreamTasks {
    cancel: CancellationToken,
    /// Mirrors `cancel` for hot-path readers: `CancellationToken::is_cancelled`
    /// takes a mutex that every peer and lane of a mux would share.
    stopped: Arc<AtomicBool>,
    tracker: TaskTracker,
    admission: Arc<parking_lot::Mutex<()>>,
    runtime: tokio::runtime::Handle,
}

impl Default for StreamTasks {
    fn default() -> Self {
        Self::new(tokio::runtime::Handle::current())
    }
}

impl StreamTasks {
    pub(crate) fn new(runtime: tokio::runtime::Handle) -> Self {
        Self {
            cancel: CancellationToken::new(),
            stopped: Arc::default(),
            tracker: TaskTracker::new(),
            admission: Arc::default(),
            runtime,
        }
    }

    pub(crate) fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) -> bool {
        let cancel = self.cancel.clone();
        self.spawn_until_done(async move {
            cancel.run_until_cancelled_owned(future).await;
        })
    }

    pub(crate) fn spawn_until_done(
        &self,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> bool {
        let tracked = {
            let _admission = self.admission.lock();
            if self.is_stopped() {
                // `future` is a parameter, so it drops after `_admission`.
                // Its Drop may stop this service and take the lock again.
                return false;
            }
            self.tracker.track_future(future)
        };
        // A stopped runtime can drop the future inside spawn. Its Drop may
        // stop this service, so the admission lock must already be released.
        self.runtime.spawn(tracked);
        true
    }

    pub(crate) fn cancellation_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    pub(crate) fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    /// Returns true only for the first stop request.
    pub(crate) fn stop(&self) -> bool {
        let _admission = self.admission.lock();
        let first = !self.stopped.swap(true, Ordering::AcqRel);
        self.cancel.cancel();
        self.tracker.close();
        first
    }

    pub(crate) async fn wait(&self) {
        self.tracker.wait().await;
    }
}

/// Enter `runtime` when the calling thread has none, so a drop or callback
/// on a plain thread can still spawn. Not when the thread's runtime context
/// is already destroyed (thread-local teardown): `Handle::enter` panics then,
/// and a panic there aborts the process. The caller's spawns then degrade
/// the way they do with no runtime at all.
pub(crate) fn enter_if_outside_runtime(
    runtime: &tokio::runtime::Handle,
) -> Option<tokio::runtime::EnterGuard<'_>> {
    match tokio::runtime::Handle::try_current() {
        Err(error) if error.is_missing_context() => Some(runtime.enter()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `is_stopped` reads a flag, not the token, so the two must move
    /// together: the first stop wins, refuses later spawns, and cancels
    /// what is running.
    #[tokio::test]
    async fn stop_is_seen_by_flag_token_and_spawn() {
        let tasks = StreamTasks::default();
        let token = tasks.cancellation_token();
        assert!(tasks.spawn(std::future::pending()));
        assert!(!tasks.is_stopped());
        assert!(tasks.stop());
        assert!(!tasks.stop());
        assert!(tasks.is_stopped());
        assert!(token.is_cancelled());
        assert!(!tasks.spawn(async {}));
        tokio::time::timeout(std::time::Duration::from_secs(1), tasks.wait())
            .await
            .expect("a stopped tracker waits only for running tasks");
    }

    /// A refused future is dropped after the admission lock is released. Its
    /// `Drop` can stop the service (a batcher's does), and that takes the
    /// same, non-reentrant lock.
    #[tokio::test]
    async fn a_refused_future_may_stop_the_service_from_its_drop() {
        struct StopOnDrop(StreamTasks);
        impl Drop for StopOnDrop {
            fn drop(&mut self) {
                self.0.stop();
            }
        }

        let tasks = StreamTasks::default();
        tasks.stop();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let refused = tasks.clone();
        std::thread::spawn(move || {
            let guard = StopOnDrop(refused.clone());
            let spawned = refused.spawn_until_done(async move {
                let _guard = guard;
            });
            done_tx.send(spawned).unwrap();
        });
        let spawned = done_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("dropping a refused future deadlocked on the admission lock");
        assert!(!spawned);
    }
}
