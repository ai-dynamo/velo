// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;

use futures::FutureExt;
use futures::future::{BoxFuture, Shared};
use velo_ext::{ShutdownState, Transport, TransportKey};

pub(super) type Completion = Shared<BoxFuture<'static, Result<(), Arc<str>>>>;

/// Own the hooks independently of shutdown waiters and the Tokio runtime.
/// Native transports may join threads here, including after final owner drop.
pub(super) fn start(
    state: ShutdownState,
    transports: HashMap<TransportKey, Arc<dyn Transport>>,
    runtime: tokio::runtime::Handle,
) -> Completion {
    let (finished, completion) = tokio::sync::oneshot::channel();
    let worker = std::thread::Builder::new()
        .name("velo-teardown".into())
        .spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _runtime = runtime.enter();
                super::stop_transports(&state, &transports);
            }))
            .map_err(|panic| {
                let message = panic
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| panic.downcast_ref::<&str>().copied())
                    .unwrap_or("transport shutdown hook panicked");
                Arc::<str>::from(message)
            });
            if let Err(error) = &result {
                tracing::error!(%error, "Transport teardown failed");
            }
            let _ = finished.send(result);
        });

    if let Err(error) = worker {
        tracing::error!(%error, "Could not start transport teardown");
        return futures::future::ready(Err(Arc::from(error.to_string())))
            .boxed()
            .shared();
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
