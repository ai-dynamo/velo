// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;

use futures::FutureExt;
use futures::future::{BoxFuture, Shared};
use velo_ext::{ShutdownState, Transport, TransportKey};

pub(crate) type Completion = Shared<BoxFuture<'static, Result<(), Arc<str>>>>;

/// Own the hooks independently of shutdown waiters and the Tokio runtime.
/// Native transports may join threads here, including after final owner drop.
pub(super) fn start(
    state: ShutdownState,
    transports: HashMap<TransportKey, Arc<dyn Transport>>,
    runtime: tokio::runtime::Handle,
) -> Completion {
    let (finished, completion) = tokio::sync::oneshot::channel();
    let (worker_state, worker_transports) = (state.clone(), transports.clone());
    let worker = std::thread::Builder::new()
        .name("velo-teardown".into())
        .spawn(move || {
            let result = {
                let _runtime = runtime.enter();
                super::stop_transports(&worker_state, &worker_transports)
            };
            if let Err(error) = &result {
                tracing::error!(%error, "Transport teardown failed");
            }
            let _ = finished.send(result);
        });

    if let Err(error) = worker {
        // Hooks that never run keep their threads and memory for the life of
        // the process. Blocking this caller is the lesser cost.
        tracing::error!(%error, "Could not start transport teardown; running it inline");
        let result = super::stop_transports(&state, &transports);
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
