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
        let result = {
            // Not during thread-local teardown: `Handle::enter` panics there,
            // and this may run in a drop, where a panic aborts the process.
            let _runtime = match tokio::runtime::Handle::try_current() {
                Err(error) if error.is_thread_local_destroyed() => None,
                _ => Some(runtime.enter()),
            };
            super::stop_transports(&state, &transports)
        };
        let _ = outcome.set(result.clone());
        return futures::future::ready(result).boxed().shared();
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
