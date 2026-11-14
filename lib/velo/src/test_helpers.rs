// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Internals that velo's own integration tests reach, behind the
//! `test-helpers` feature.
//!
//! These are the wire messages of the streaming and rendezvous control
//! planes. A test sends one by hand to reach a handler branch that no
//! well-behaved peer reaches: a request from an older peer, a request that
//! names the wrong kind of anchor, a cancel for an unknown sender. They are
//! not part of the release API, and they change without a version bump.

pub use crate::rendezvous::protocol::{AcquireResponse, RvAcquireRequest};
pub use crate::streaming::control::{
    AnchorAttachRequest, AnchorAttachResponse, StreamCancelHandle, StreamCancelRequest,
};
pub use crate::streaming::mpsc::control::{
    MpscAnchorAttachRequest, MpscAnchorAttachResponse, MpscAnchorCancelRequest,
    MpscAnchorDetachRequest,
};
