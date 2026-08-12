// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Batched, multiplexed streaming over the Messenger — the `messenger-mux-v1`
//! transport described by `BATCHING.md`.
//!
//! Today one stream owns one connection: X concurrent streams to one peer means
//! X sockets, X egress pumps, X heartbeat timers, and one `write` syscall per
//! token. The mux collapses that to **one batcher per peer** and packs every
//! stream's records into `_stream_batch` active messages that ride the
//! Messenger's existing connectivity. There is no dial, no listener, no
//! acceptor and no connection lifecycle, because there is no connection: the
//! sender's identity arrives in the AM envelope, so credit has somewhere to be
//! routed without a handshake invented to learn it.
//!
//! The cost is that streaming no longer owns its wire. It shares queues,
//! framing and backpressure with control traffic, and ordering stops being a
//! TCP guarantee and becomes an explicit protocol obligation. Everything in
//! this module follows from that one trade:
//!
//! - [`protocol`] — the wire. A 16-byte batch header carrying the peer epoch
//!   and a modulo-compared batch sequence, then records tagged with a
//!   `(u24 index, u8 generation)` slot and a per-slot `frame_seq` that is the
//!   authority on stream order. Pure encode/decode; decoding a malformed batch
//!   yields a precise error and never a panic.
//! - [`flow_control`] — the credit. Multiplexing means the shared resource is
//!   the peer's *ordering lane*, and a handler that awaits holds it against
//!   every slot from that peer. So ingress is bounded and nonblocking on
//!   per-slot credit, with one reserved terminal credit, control records that
//!   data exhaustion cannot block, and byte budgets standing in for the
//!   per-stream socket limit the kernel used to enforce for free.
//!
//! > **Stage E1.** These two modules are the pure, dependency-free half. The
//! > transport itself — `MessengerMuxTransport`, the per-peer egress batcher
//! > and the `_stream_batch` ingress handler — lands in the next commit and is
//! > what turns these primitives into a `FrameTransport`.
// Nothing outside `#[cfg(test)]` consumes these primitives yet, so the plain
// lib build sees the whole module as dead. `expect` rather than `allow` so the
// suppression cannot outlive its excuse: once the transport consumes them, the
// expectation goes unfulfilled and fails the build under `-D warnings`. The
// `cfg_attr` is required — under `cfg(test)` the tests already use everything,
// so an unconditional `expect` would be unfulfilled today.
#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "consumed by the mux transport (peer batcher + ingress) landing in the next commit"
    )
)]

pub(crate) mod flow_control;
pub(crate) mod protocol;
