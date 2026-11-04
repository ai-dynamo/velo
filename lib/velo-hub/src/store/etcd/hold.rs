// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Seams for tests (feature `test-support`): hold the watch, gate a put,
//! and record every continuity change. Without the feature each is a no-op
//! the compiler removes.

use crate::store::Continuity;

#[cfg(not(feature = "test-support"))]
pub(super) use disabled::Holds;
#[cfg(feature = "test-support")]
pub(super) use enabled::Holds;
#[cfg(feature = "test-support")]
pub use enabled::{WatchHold, WatchHoldGuard};

#[cfg(feature = "test-support")]
mod enabled {
    use std::sync::Arc;

    use parking_lot::Mutex;
    use tokio::sync::watch;

    use super::Continuity;
    use crate::store::testing::Gate;

    /// What [`EtcdStore::hold_watch`](super::super::EtcdStore::hold_watch)
    /// does to the watch.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum WatchHold {
        /// Keep the stream open but apply nothing: a watch that lags.
        Pause,
        /// Drop the stream and do not reconnect: a watch that broke.
        Disconnect,
    }

    /// Releases the hold when dropped.
    #[must_use = "the hold ends when the guard is dropped"]
    pub struct WatchHoldGuard {
        pub(in crate::store::etcd) holds: watch::Sender<Option<WatchHold>>,
    }

    impl Drop for WatchHoldGuard {
        fn drop(&mut self) {
            self.holds.send_replace(None);
        }
    }

    pub(in crate::store::etcd) struct Holds {
        pub(in crate::store::etcd) watch: watch::Sender<Option<WatchHold>>,
        pub(in crate::store::etcd) put_gate: Mutex<Option<Arc<Gate>>>,
        history: Mutex<Vec<Continuity>>,
    }

    impl Default for Holds {
        fn default() -> Self {
            Self {
                watch: watch::Sender::new(None),
                put_gate: Mutex::new(None),
                history: Mutex::new(vec![Continuity::Live]),
            }
        }
    }

    impl Holds {
        pub(in crate::store::etcd) fn record(&self, continuity: Continuity) {
            self.history.lock().push(continuity);
        }

        pub(in crate::store::etcd) fn history(&self) -> Vec<Continuity> {
            self.history.lock().clone()
        }

        pub(in crate::store::etcd) fn subscribe(&self) -> Watching {
            Watching(self.watch.subscribe())
        }

        pub(in crate::store::etcd) async fn before_put(&self) {
            let gate = self.put_gate.lock().take();
            if let Some(gate) = gate {
                gate.pass().await;
            }
        }
    }

    pub(in crate::store::etcd) struct Watching(watch::Receiver<Option<WatchHold>>);

    impl Watching {
        pub(in crate::store::etcd) fn paused(&self) -> bool {
            *self.0.borrow() == Some(WatchHold::Pause)
        }

        pub(in crate::store::etcd) fn disconnected(&self) -> bool {
            *self.0.borrow() == Some(WatchHold::Disconnect)
        }

        pub(in crate::store::etcd) async fn changed(&mut self) {
            if self.0.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }
}

#[cfg(not(feature = "test-support"))]
mod disabled {
    use super::Continuity;

    #[derive(Default)]
    pub(in crate::store::etcd) struct Holds;

    impl Holds {
        pub(in crate::store::etcd) fn record(&self, _continuity: Continuity) {}

        pub(in crate::store::etcd) fn subscribe(&self) -> Watching {
            Watching
        }

        pub(in crate::store::etcd) async fn before_put(&self) {}
    }

    pub(in crate::store::etcd) struct Watching;

    impl Watching {
        pub(in crate::store::etcd) fn paused(&self) -> bool {
            false
        }

        pub(in crate::store::etcd) fn disconnected(&self) -> bool {
            false
        }

        pub(in crate::store::etcd) async fn changed(&mut self) {
            std::future::pending::<()>().await;
        }
    }
}
