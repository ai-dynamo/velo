// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Session-scoped synchronization primitives — caller-keyed pending-operation
//! correlation ([`PendingMap`]) and reasoned fire-once close signaling
//! ([`CloseSignal`]).
//!
//! # Scope
//!
//! Only small, dependency-light coordination primitives with zero
//! messenger/streaming coupling live here. This module is not a
//! general-purpose concurrency toolbox: if a candidate type pulls in a heavy
//! dep, encodes wire-protocol semantics, or belongs to a specific subsystem
//! (streaming, messenger, rendezvous), it belongs in that subsystem, not here.
//!
//! # Why velo has two correlation systems
//!
//! [`crate::messenger::common::responses::ResponseManager`] (`pub(crate)`) is
//! wire-protocol AM request/response correlation. Its keys are
//! manager-assigned `ResponseId` values that traverse the network with
//! generation-based ABA protection. It uses a fixed `u16::MAX` pre-allocated
//! slot arena with semaphore backpressure, carries `Bytes` payloads, and
//! surfaces one-at-a-time failure through a bounded slot arena.
//!
//! [`PendingMap`] is session-scoped caller-keyed coordination. Keys are
//! generic `K` — the caller assigns them, they never leave the process. The
//! map is unbounded, has first-class closed state, and supports atomic
//! drain-all on close. Values are generic `V`, not `Bytes`.
//!
//! Neither generalizes to the other's shape; do not merge them, and never
//! make `messenger::common::responses` public — doing so would freeze
//! `ResponseId`'s wire layout into public API.
//!
//! # Why velo has multiple close signals
//!
//! [`crate::streaming::StreamSender::cancellation_token`] is wire-level: it
//! fires when the consumer cancels or drops the anchor, and carries no reason.
//! An anchor `closed_token()` is deliberately not exposed yet — the underlying
//! token does not fire on every permanent-close path today.
//!
//! [`CloseSignal`] is application/session-scoped and reason-carrying. The
//! consumer decides which events are terminal and calls
//! [`CloseSignal::close`] with a human-readable reason string. It is
//! appropriate for higher-level session or request-lifecycle management where
//! the reason matters for diagnostics and downstream behavior.
//!
//! # Deliberate convention deviations
//!
//! **`HashMap` under one `parking_lot::Mutex`, not `DashMap`.**  The
//! closed-check and the insert must share a single critical section. A
//! sharded map plus a separate closed flag is exactly the two-lock
//! insert-after-drain race this module exists to eliminate: a caller could
//! pass the closed-check on one shard, see `Open`, then be preempted while
//! `close()` drains a different shard and sets the flag — the insert lands
//! in a closed map with nobody to drain it. One mutex prevents this.
//!
//! **`tokio::sync::oneshot`, not `flume`.**  The receiver is a natural
//! `#[must_use]` `Future` and needs no runtime to create or send. `flume`
//! channels carry allocation overhead and expose a richer API surface that
//! is not needed here.
//!
//! **`parking_lot::Mutex`, not `tokio::sync::Mutex`.**  Every operation is
//! synchronous; no guard ever crosses an `.await`. Additionally, `close()`
//! must be callable from non-tokio threads (e.g., PyO3/vLLM shutdown paths),
//! which rules out `tokio::sync::Mutex` whose `lock()` is `async`.

mod close_signal;
mod pending_map;

pub use close_signal::CloseSignal;
pub use pending_map::{Closed, PendingMap, RegisterError, Waiter};
