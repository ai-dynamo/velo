// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! UCX messenger transport (RDMA via InfiniBand/RoCE, plus tcp/shm fallback).
//!
//! Feature `ucx`, Linux only. Built on the `ucx-rs` crate (`crates/ucx-rs` in
//! this repository), which vendors a static UCX build — the only runtime system
//! dependency is rdma-core's `libibverbs.so.1`/`libmlx5.so.1` (already
//! required by any RDMA stack), and `UCX_TLS=tcp` runs the identical code
//! path with no RDMA hardware at all (which is how CI exercises this module).
//!
//! Architecture (see `docs/proposals/ibverbs-transport.md` §11 for the full
//! design record and the measurements behind each choice):
//!
//! * **No listener.** The `WorkerAddress` blob carries the packed
//!   `ucp_worker` address; `register()` stores it and the endpoint is created
//!   with `ucp_ep_create` on first send. No in-band bootstrap exists.
//! * **One progress thread** owns all UCX objects ([`worker`] module).
//!   Sends are admitted through per-peer [`AdmissionGate`]s into its command
//!   ring, preserving the per-target ordering contract.
//! * **Eager-only AM.** Every send pins `UCP_AM_SEND_FLAG_EAGER`; the
//!   negotiated `eager_max` (default 1 MiB) bounds AM frames and
//!   `max_message_size` reports it, so large payloads ride the messenger's
//!   rendezvous staging instead.
//! * **Completion-owned operations.** Buffers are owned by the in-flight
//!   operation until UCX completes it — dropped futures cannot free memory
//!   UCX is still reading (the async-ucx issue #1 class, closed by design).
//!
//! [`AdmissionGate`]: crate::transports::transport::AdmissionGate

mod address;
mod transport;
mod worker;

pub use transport::{UcxConfig, UcxTransport, UcxTransportBuilder};
