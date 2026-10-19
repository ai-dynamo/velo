// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Delivery of removals to extensions, serialized with registrations.
//!
//! The store's removal hook revokes authority at once (the owner answers
//! Busy from then on) and only enqueues the delivery. The lifecycle task
//! takes the hub-wide registration lock and delivers it: `on_removed` on
//! every extension, then forgetting the velo peer, then
//! `finish_revocation`, after which the owner may register again.
//!
//! The hook never waits on the lock, so a store may call it from any
//! context, including while a registration holding the lock awaits a store
//! write.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::Shared;
use super::authority::{Authority, RevocationAction};
use crate::store::{Removal, RemovalHook};

/// Work for the lifecycle task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Job {
    /// The store removed this entry; deliver it.
    Removal(Removal),
    /// A registration that wrote this entry stopped without finishing (its
    /// task panicked). Remove the entry, then deliver its removal.
    Retire(Removal),
}

/// The hook the hub installs in its store.
pub(crate) fn removal_hook(authority: Authority, jobs: mpsc::UnboundedSender<Job>) -> RemovalHook {
    Arc::new(move |removal| {
        if authority.begin_revocation(removal) == RevocationAction::Cleanup {
            // A closed queue means the hub is stopping and nothing will
            // read authority again.
            let _ = jobs.send(Job::Removal(removal));
        }
    })
}

impl Shared {
    /// Deliver one removal. The caller holds the registration lock.
    pub(crate) fn deliver_removal(&self, removal: Removal) {
        self.notify_removed(removal);
        // Forget the velo peer only if no later incarnation replaced it, so
        // a delayed delivery cannot disconnect a replacement.
        if let Some(velo) = &self.velo
            && self
                .store
                .current_incarnation(removal.id)
                .is_none_or(|current| current == removal.incarnation)
        {
            velo.unregister_peer(removal.id);
        }
        self.authority.finish_revocation(removal);
    }

    /// `on_removed` on every extension, in reverse start order. The caller
    /// holds the registration lock.
    pub(crate) fn notify_removed(&self, removal: Removal) {
        for extension in self.extensions.iter().rev() {
            // A panicking extension must not stop delivery to the others,
            // nor kill the lifecycle task.
            if std::panic::catch_unwind(AssertUnwindSafe(|| extension.on_removed(removal))).is_err()
            {
                tracing::error!(
                    extension = extension.name(),
                    instance = %removal.id,
                    incarnation = %removal.incarnation,
                    "on_removed panicked"
                );
            }
        }
    }

    /// Run one job. The caller holds the registration lock.
    pub(crate) async fn run_job(&self, job: Job) {
        match job {
            Job::Removal(removal) => self.deliver_removal(removal),
            Job::Retire(removal) => {
                if let Err(error) = self.remove_bounded(removal.id, removal.incarnation).await {
                    tracing::warn!(
                        instance = %removal.id,
                        incarnation = %removal.incarnation,
                        %error,
                        "could not remove the entry of an abandoned registration; it stays reserved until it expires"
                    );
                }
                self.deliver_removal(removal);
            }
        }
    }
}

/// The lifecycle task. Returns the queue and any job it had taken but not
/// run, so shutdown can drain them while it holds the lock.
pub(crate) async fn run(
    shared: Arc<Shared>,
    mut jobs: mpsc::UnboundedReceiver<Job>,
    cancel: CancellationToken,
) -> (mpsc::UnboundedReceiver<Job>, Option<Job>) {
    loop {
        let job = tokio::select! {
            biased;
            _ = cancel.cancelled() => return (jobs, None),
            job = jobs.recv() => match job {
                Some(job) => job,
                None => return (jobs, None),
            },
        };
        let guard = tokio::select! {
            biased;
            _ = cancel.cancelled() => return (jobs, Some(job)),
            guard = shared.lock.lock() => guard,
        };
        shared.run_job(job).await;
        drop(guard);
    }
}
