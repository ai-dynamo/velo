// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;

use futures::FutureExt;
use futures::future::{BoxFuture, Shared};
use velo_ext::{ShutdownState, Transport, TransportKey};

pub(crate) type Completion = Shared<BoxFuture<'static, Result<(), Arc<str>>>>;

/// The teardown result, written before the completion resolves. A reader
/// that only wants to know whether teardown failed reads this instead of
/// polling the completion: that poll can say "not yet" for a finished
/// teardown, when the task has spent its cooperative budget or another clone
/// is mid-poll.
pub(crate) type Outcome = Arc<std::sync::OnceLock<Result<(), Arc<str>>>>;

/// Records a failure if teardown unwinds before it writes its own result.
///
/// Each hook runs under its own `catch_unwind`, but the code around them can
/// still unwind (a panicking tracing subscriber, say). Without this the cell
/// stays empty, `teardown_failure` reports no failure for a teardown that
/// died, and a later shutdown drains again instead of panicking at once. No
/// disarm is needed: once the result is written, this write is a no-op.
struct RecordUnwind<'a>(&'a Outcome);

impl Drop for RecordUnwind<'_> {
    fn drop(&mut self) {
        let _ = self.0.set(Err(Arc::from("transport teardown panicked")));
    }
}

/// Own the hooks independently of shutdown waiters and the Tokio runtime.
/// Native transports may join threads here, including after final owner drop.
pub(super) fn start(
    state: ShutdownState,
    transports: HashMap<TransportKey, Arc<dyn Transport>>,
    runtime: tokio::runtime::Handle,
    outcome: Outcome,
) -> Completion {
    let (finished, completion) = tokio::sync::oneshot::channel();
    let (worker_state, worker_transports) = (state.clone(), transports.clone());
    let worker_runtime = runtime.clone();
    let worker_outcome = Arc::clone(&outcome);
    let worker = std::thread::Builder::new()
        .name("velo-teardown".into())
        .spawn(move || {
            let _unwind = RecordUnwind(&worker_outcome);
            let result = {
                let _runtime = worker_runtime.enter();
                super::stop_transports(&worker_state, &worker_transports)
            };
            let _ = worker_outcome.set(result.clone());
            let _ = finished.send(result);
        });

    if let Err(error) = worker {
        // Hooks that never run keep their threads and memory for the life of
        // the process. Blocking this caller is the lesser cost.
        tracing::error!(%error, "Could not start transport teardown; running it inline");
        return run_inline(&outcome, || {
            // Not during thread-local teardown: `Handle::enter` panics there,
            // and this may run in a drop, where a panic aborts the process.
            let _runtime = match tokio::runtime::Handle::try_current() {
                Err(error) if error.is_thread_local_destroyed() => None,
                _ => Some(runtime.enter()),
            };
            super::stop_transports(&state, &transports)
        });
    }

    async move {
        completion.await.unwrap_or_else(|_| {
            Err(Arc::from(
                "transport teardown worker exited without a result",
            ))
        })
    }
    .boxed()
    .shared()
}

/// Runs teardown on the caller's thread and returns a ready completion.
///
/// A panic in `body` is caught and becomes the failure. It must not unwind
/// out of the caller's `OnceLock::get_or_init`: that leaves the lock empty
/// and the next caller runs every hook again.
fn run_inline(outcome: &Outcome, body: impl FnOnce() -> Result<(), Arc<str>>) -> Completion {
    let _unwind = RecordUnwind(outcome);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(body))
        .unwrap_or_else(|_| Err(Arc::from("transport teardown panicked")));
    let _ = outcome.set(result.clone());
    futures::future::ready(result).boxed().shared()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The worker's body can unwind outside any hook, and no transport can
    /// make it do so on purpose, so the guard is tested on its own, under a
    /// real unwind. The control case shows it never overwrites a result that
    /// was written.
    #[test]
    fn a_teardown_that_unwinds_still_records_a_failure() {
        let outcome = Outcome::default();
        let unwound = std::panic::catch_unwind(|| {
            let _unwind = RecordUnwind(&outcome);
            panic!("teardown body unwound");
        });
        assert!(unwound.is_err());
        assert!(matches!(outcome.get(), Some(Err(_))));

        let finished = Outcome::default();
        {
            let _unwind = RecordUnwind(&finished);
            let _ = finished.set(Ok(()));
        }
        assert!(matches!(finished.get(), Some(Ok(()))));
    }

    /// The inline path runs inside `OnceLock::get_or_init`. A body that
    /// unwinds there leaves the lock empty, so the next `request_teardown`
    /// would run every `Transport::shutdown` a second time. The body must
    /// not escape: the completion is ready with a failure instead. It
    /// cannot be forced through `start`, which needs a failed thread spawn.
    #[test]
    fn an_inline_teardown_that_unwinds_returns_a_ready_failure() {
        let outcome = Outcome::default();
        let completion = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_inline(&outcome, || panic!("teardown body unwound"))
        }))
        .expect("the panic must not reach the caller");
        assert!(matches!(outcome.get(), Some(Err(_))));
        assert!(matches!(completion.now_or_never(), Some(Err(_))));
    }
}
