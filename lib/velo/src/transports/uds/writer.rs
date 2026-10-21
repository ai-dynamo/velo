// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The coalescing writer's UDS-specific seam: how a queued [`SendTask`] hands
//! its bytes to [`run_coalescing_writer`](crate::transports::coalesce::run_coalescing_writer),
//! and how that loop's log lines and egress instruments attach to one
//! connection.
//!
//! Split out of `transport.rs` for the same reason as the TCP one
//! (`tcp/writer.rs`): `transport.rs` owns dialing, accepting, and routing, and
//! this module owns what happens to a frame once it is queued for a
//! connection.

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use tracing::error;

use velo_ext::MessageType;

use crate::transports::coalesce::{Coalescable, EgressMetrics, WriterFailure, WriterObserver};
use crate::transports::teardown_grace::TeardownStop;

use super::transport::SendTask;

impl Coalescable for SendTask {
    /// A staged task *is* its own failure token: what
    /// [`TransportErrorHandler::on_error`] needs — the header, the payload,
    /// and the handler — is every field but a one-byte `Copy` enum, and all
    /// three are refcounted handles. Splitting them into a second struct would
    /// have the same footprint, so the writer just keeps the task. Retaining
    /// it holds no payload bytes beyond the ones the sender already owns.
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
        token.on_error(format!("Failed to write to UDS stream: {}", reason));
    }
}

/// Attaches the connection's identity to the writer loop's log lines, and
/// carries the transport's pre-bound metrics handle so the per-frame egress
/// path does no label lookup.
///
/// Answers [`WriterObserver::egress`] with that handle, which TCP shares
/// byte-for-byte — the only thing that differs per transport is the failure
/// log text below.
pub(super) struct UdsWriterObserver<'a> {
    pub(super) instance_id: crate::InstanceId,
    pub(super) path: &'a Path,
    pub(super) egress: Option<EgressMetrics>,
    /// Whether a stop is teardown, which keeps what is queued until its
    /// deadline; otherwise only this connection closes.
    pub(super) teardown: Arc<TeardownStop>,
}

impl WriterObserver for UdsWriterObserver<'_> {
    fn keep_queued_on_stop(&self) -> Option<tokio::time::Instant> {
        self.teardown.deadline()
    }

    fn on_failure(&self, kind: WriterFailure, err: &std::io::Error, frames: usize) {
        match kind {
            WriterFailure::Write => error!(
                "Write error to {} ({:?}): {} ({} message(s) in batch)",
                self.instance_id, self.path, err, frames
            ),
            WriterFailure::Encode => error!(
                "Encode error to {} ({:?}): {}",
                self.instance_id, self.path, err
            ),
        }
    }

    fn egress(&self) -> Option<&EgressMetrics> {
        self.egress.as_ref()
    }
}
