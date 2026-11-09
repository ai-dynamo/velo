// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Stall the fetch of a staged payload at its owner, then let it finish.
//!
//! A payload over the staging threshold (256 KiB) does not travel in its
//! message. The sender stages it, and the receiver fetches it with an
//! `_rv_acquire` call to the sender, then `_rv_pull` calls. [`stall_fetches`]
//! replaces the owner's `_rv_acquire` with one that waits for a permit, then
//! does what the real handler does. So a gate, not a sleep, decides how long
//! the owner looks unresponsive, and a stalled fetch that is let through
//! completes for real, or, if its receiver gave up, leaves the lease it would
//! leave. A test can then see what each side still holds.
//!
//! Included with `#[path]` by the test binaries that use it, which need the
//! `test-helpers` feature.

#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use tokio::sync::{Notify, Semaphore};
use velo::rendezvous::handlers::acquire_lease;
use velo::rendezvous::protocol::RvAcquireRequest;
use velo::{Handler, TypedContext, Velo};

/// What a stalled owner saw, and the gate that holds it.
#[derive(Clone)]
pub struct Stall {
    /// Signalled each time a fetch reaches the owner.
    pub entered: Arc<Notify>,
    /// Each permit lets one stalled `_rv_acquire` through.
    pub gate: Arc<Semaphore>,
    asked: Arc<Mutex<Vec<u64>>>,
}

impl Stall {
    pub fn new() -> Self {
        Self {
            entered: Arc::new(Notify::new()),
            gate: Arc::new(Semaphore::new(0)),
            asked: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// The owner's local ids of the payloads that fetches asked for, in order.
    pub fn asked(&self) -> Vec<u64> {
        self.asked.lock().unwrap().clone()
    }
}

/// Stall every fetch of a payload that `owner` staged until `stall.gate`
/// lets it through, then answer as the real handler does.
pub fn stall_fetches(owner: &Velo, stall: &Stall) {
    let store = Arc::clone(owner.rendezvous_manager().data_store());
    let stall = stall.clone();
    owner
        .messenger()
        .register_streaming_handler(
            Handler::typed_unary_async(
                "_rv_acquire",
                move |ctx: TypedContext<RvAcquireRequest>| {
                    let store = Arc::clone(&store);
                    let stall = stall.clone();
                    async move {
                        let (_, local_id) = ctx.input.handle.to_handle().unpack();
                        stall.asked.lock().unwrap().push(local_id);
                        stall.entered.notify_one();
                        stall.gate.acquire().await?.forget();
                        acquire_lease(&store, &ctx.input, ctx.headers.as_ref())
                    }
                },
            )
            .build(),
        )
        .unwrap();
}

/// Whether `owner` still holds the staged payload `local_id`.
pub fn still_staged(owner: &Velo, local_id: u64) -> bool {
    owner
        .rendezvous_manager()
        .data_store()
        .get_total_len(local_id)
        .is_some()
}

/// Whether `owner` lets go of the staged payload `local_id` within `within`.
pub async fn released_within(owner: &Velo, local_id: u64, within: std::time::Duration) -> bool {
    tokio::time::timeout(within, async {
        while still_staged(owner, local_id) {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok()
}
