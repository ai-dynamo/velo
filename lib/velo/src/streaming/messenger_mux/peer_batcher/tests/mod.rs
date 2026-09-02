// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Egress tests.
//!
//! The batcher is driven directly rather than through a `MessengerMuxTransport`,
//! because every property worth pinning here is about what it *packs*, and
//! commands are the only input it has. The peer is a real messenger with a
//! capture handler registered on `_stream_batch`, so the assertions read the
//! actual wire bytes rather than an internal accounting mirror.
//!
//! Nothing here races the batcher for timing. Where a test needs several
//! records in one batch it queues them on parked slots first and then grants
//! credit, which the batcher drains in a single wake — the opportunistic policy
//! taking everything that is *already* queued, exactly as it does under a
//! forward pass.
//!
//! "In a single wake" is a fact these tests establish, not one they assume.
//! Back-to-back grants are separate writes to the control inbox, and a batcher
//! scheduled between two of them takes the first on its own; a test that needs
//! them coalesced holds the loop at [`super::test_hooks::TestHooks`] while it
//! writes them.

mod clamp;
mod control;
mod egress;
mod flush_policy;
mod support;
