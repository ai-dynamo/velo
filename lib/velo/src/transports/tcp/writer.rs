// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The coalescing writer's TCP-specific seam: how a queued [`SendTask`] hands
//! its bytes to [`run_coalescing_writer`](crate::transports::coalesce::run_coalescing_writer),
//! and how that loop's log lines and egress instruments attach to one
//! connection.
//!
//! Split out of `transport.rs` because these two impls are the writer's
//! concerns, not the connection lifecycle's — `transport.rs` owns dialing,
//! accepting, and routing, and this module owns what happens to a frame once
//! it is queued for one.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tracing::error;

use velo_ext::MessageType;

use crate::transports::coalesce::{
    Coalescable, EgressMetrics, FrameTally, WriterFailure, WriterObserver,
};

use super::transport::SendTask;

impl Coalescable for SendTask {
    /// A staged task *is* its own failure token: what the transport's error
    /// handler needs — the header, the payload, and the handler itself — is
    /// every field but a one-byte `Copy` enum, and all three are refcounted
    /// handles. Splitting them into a second struct would have the same
    /// footprint, so the writer just keeps the task. Retaining it holds no
    /// payload bytes beyond the ones the sender already owns.
    type FailureToken = Self;

    fn msg_type(&self) -> MessageType {
        self.msg_type
    }

    fn header(&self) -> &[u8] {
        &self.header
    }

    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn queued_at(&self) -> Option<Instant> {
        self.queued_at
    }

    fn into_failure_token(self) -> Self {
        self
    }

    fn fail(token: Self, reason: &str) {
        token.on_error(format!("Failed to write to stream: {}", reason));
    }
}

/// Attaches the connection's identity to the writer loop's log lines, and
/// carries the transport's pre-bound metrics handle so the per-frame egress
/// path does no label lookup.
///
/// The egress-metrics methods delegate to [`EgressMetrics`], which UDS
/// shares byte-for-byte — the only thing that differs per transport is the
/// failure log text below.
///
/// `pub(super)`, along with its fields: `transport.rs` constructs this by
/// struct literal at the connection writer's spawn site, so `tcp` is the
/// visibility this type needs.
pub(super) struct TcpWriterObserver {
    pub(super) instance_id: crate::InstanceId,
    pub(super) addr: SocketAddr,
    pub(super) egress: EgressMetrics,
}

impl WriterObserver for TcpWriterObserver {
    fn on_failure(&self, kind: WriterFailure, err: &std::io::Error, frames: usize) {
        match kind {
            WriterFailure::Write => error!(
                "Write error to {} ({}): {} ({} message(s) in batch)",
                self.instance_id, self.addr, err, frames
            ),
            WriterFailure::Encode => error!(
                "Encode error to {} ({}): {}",
                self.instance_id, self.addr, err
            ),
        }
    }

    fn records_egress(&self) -> bool {
        self.egress.records_egress()
    }

    fn on_dequeue(&self, waited: Duration) {
        self.egress.on_dequeue(waited);
    }

    fn on_write(&self, tally: &FrameTally, elapsed: Duration) {
        self.egress.on_write(tally, elapsed);
    }
}
