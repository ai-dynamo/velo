// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Public value types used by the MPSC streaming API.

use std::time::Duration;

/// Opaque identifier assigned to each sender that attaches to an MPSC anchor.
///
/// Unique within an anchor's lifetime; reattaching via
/// [`crate::mpsc::MpscStreamSender::detach`] yields a *new* [`SenderId`].
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SenderId(pub u64);

impl std::fmt::Display for SenderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SenderId({})", self.0)
    }
}

/// Per-anchor configuration overrides for [`crate::AnchorManager::create_mpsc_anchor_with_config`].
///
/// `None` on any field inherits the manager-level default.
#[derive(Debug, Clone, Default)]
pub struct MpscAnchorConfig {
    /// How long an anchor with zero attached senders may live before being
    /// auto-removed. Armed both at creation and whenever `senders` becomes
    /// empty again after all senders detach.
    pub unattached_timeout: Option<Duration>,

    /// Heartbeat cadence negotiated with each attaching sender. Defaults to
    /// the manager's `default_heartbeat_interval` (5s).
    pub heartbeat_interval: Option<Duration>,

    /// Optional cap on concurrent attached senders. When set, attach attempts
    /// beyond this cap return [`crate::AttachError::MaxSendersReached`].
    pub max_senders: Option<usize>,

    /// Shared frame channel capacity. Defaults to 256. Raise for very high
    /// fan-in where back-pressure against individual producers is desirable.
    pub channel_capacity: Option<usize>,
}
