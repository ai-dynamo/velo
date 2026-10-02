// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

/// Tasks owned by one streaming service. Shutdown refuses new tasks before it waits.
#[derive(Clone, Default)]
pub(crate) struct StreamTasks {
    cancel: CancellationToken,
    tracker: TaskTracker,
    stopped: Arc<parking_lot::Mutex<bool>>,
}

impl StreamTasks {
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
        let stopped = self.stopped.lock();
        if *stopped {
            return false;
        }
        self.tracker.spawn(future);
        true
    }

    pub(crate) fn cancellation_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    pub(crate) fn is_stopped(&self) -> bool {
        self.cancel.is_cancelled()
    }

    pub(crate) fn stop(&self) {
        let mut stopped = self.stopped.lock();
        *stopped = true;
        self.cancel.cancel();
        self.tracker.close();
    }

    pub(crate) async fn wait(&self) {
        self.tracker.wait().await;
    }
}
