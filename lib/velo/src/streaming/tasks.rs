// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

/// Tasks owned by one streaming service. Shutdown refuses new tasks before it waits.
#[derive(Clone)]
pub(crate) struct StreamTasks {
    cancel: CancellationToken,
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
        self.cancel.is_cancelled()
    }

    /// Returns true only for the first stop request.
    pub(crate) fn stop(&self) -> bool {
        let _admission = self.admission.lock();
        let first = !self.is_stopped();
        self.cancel.cancel();
        self.tracker.close();
        first
    }

    pub(crate) async fn wait(&self) {
        self.tracker.wait().await;
    }
}
