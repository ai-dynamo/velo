// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Transport observability extension surface.
//!
//! Out-of-tree transport implementors integrate with the runtime's metrics
//! pipeline through the [`TransportObservability`] trait. The runtime hands
//! each transport a pre-bound observability handle via
//! [`Transport::set_observability`](crate::transport::Transport::set_observability);
//! the transport then calls [`record_frame`](TransportObservability::record_frame),
//! [`record_rejection`](TransportObservability::record_rejection), etc. to
//! emit data into the same `velo_transport_*` metric series as the in-tree
//! transports — without depending on the runtime crate or its concrete
//! metrics implementation.

use std::time::Duration;

/// Direction of a transport frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    /// Inbound frame (received from peer).
    Inbound,
    /// Outbound frame (sent to peer).
    Outbound,
}

impl Direction {
    /// Prometheus label value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inbound => "inbound",
            Self::Outbound => "outbound",
        }
    }
}

/// Reason a transport rejected or dropped a frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportRejection {
    /// Send operation failed.
    SendError,
    /// Message rejected during graceful drain.
    DrainRejected,
    /// Frame decode or preamble parse failed.
    DecodeError,
    /// Failed to route frame to adapter channel.
    RouteFailed,
    /// NATS message missing required headers.
    MissingHeaders,
    /// Velo-Type header missing (NATS).
    MissingType,
    /// Invalid Velo-Type value (NATS).
    InvalidType,
    /// Invalid Velo-HLen header (NATS).
    InvalidHeaderLength,
    /// Frame shorter than declared header length (NATS).
    TruncatedFrame,
    /// Failed to build ShuttingDown response (gRPC).
    DrainReplyBuildFailed,
}

impl TransportRejection {
    /// Prometheus label value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SendError => "send_error",
            Self::DrainRejected => "drain_rejected",
            Self::DecodeError => "decode_error",
            Self::RouteFailed => "route_failed",
            Self::MissingHeaders => "missing_headers",
            Self::MissingType => "missing_type",
            Self::InvalidType => "invalid_type",
            Self::InvalidHeaderLength => "invalid_header_length",
            Self::TruncatedFrame => "truncated_frame",
            Self::DrainReplyBuildFailed => "drain_reply_build_failed",
        }
    }
}

/// Observability hook handed to a [`Transport`](crate::transport::Transport)
/// during [`set_observability`](crate::transport::Transport::set_observability).
///
/// Implementors of `Transport` invoke these methods to publish metrics into
/// the runtime's collectors. The runtime's concrete handle pre-binds the
/// transport's `key` label so the trait surface stays free of label-management
/// concerns.
///
/// All methods take `&self` and have implementations that are typically lock-free
/// — they are safe to call from any hot path.
///
/// The frame, rejection, gauge and backpressure methods carry no default: each
/// names a signal any real implementation would care about, and a transport
/// that reported none of them would be invisible. The three egress methods do
/// carry defaults, for two reasons. A transport whose `send_message` hands the
/// frame straight to the wire has no queue in front of a writer and nothing to
/// report; and adding a bare method to this trait would break every
/// out-of-tree implementation, which is a major bump and a coordinated `velo`
/// release (see `CONTRIBUTING.md`, *velo-ext API stability*).
pub trait TransportObservability: Send + Sync {
    /// Record an accepted frame.
    ///
    /// `message_type` is one of the well-known
    /// [`MessageType`](crate::transport::MessageType) label strings:
    /// `"message"`, `"response"`, `"ack"`, `"event"`, or `"shutting_down"`.
    fn record_frame(&self, direction: Direction, message_type: &str, bytes: usize);

    /// Record a rejected or dropped frame.
    fn record_rejection(&self, reason: TransportRejection);

    /// Set the gauge for the number of registered peers on this transport.
    fn set_registered_peers(&self, count: usize);

    /// Set the gauge for the number of active connections on this transport.
    fn set_active_connections(&self, count: usize);

    /// Record a send that found the bounded per-target channel full and was
    /// queued in the target's [`AdmissionGate`](crate::admission::AdmissionGate)
    /// instead of admitted on the spot.
    fn record_send_backpressure(&self);

    /// Record how long one outbound frame waited between this transport
    /// accepting it and the per-connection writer taking it off the send
    /// queue.
    ///
    /// Observed once per frame, by the writer, at the moment of dequeue. A
    /// transport that stamps the frame before offering it to its
    /// [`AdmissionGate`](crate::admission::AdmissionGate) reports a wait that
    /// spans the gate's pending queue as well as the bounded channel behind
    /// it, which is the reading that matters: the gate is where a saturated
    /// connection actually backs up.
    fn record_egress_queue_wait(&self, _wait: Duration) {}

    /// Record `count` frames of one `message_type` reaching the wire.
    ///
    /// Called once per message type per write, after the write returns — a
    /// writer that coalesces several frames into one write reports them
    /// together, so this counts frames and not writes. `message_type` takes
    /// the same well-known label strings as [`record_frame`](Self::record_frame).
    ///
    /// Paired with [`record_frame`](Self::record_frame)'s outbound count, this
    /// is what makes the egress queue's depth derivable without sampling a
    /// channel length.
    fn record_frames_written(&self, _message_type: &str, _count: u64) {}

    /// Record the wall time of one write, which may carry several coalesced
    /// frames.
    ///
    /// A long write means the socket's send buffer is full and the wire or the
    /// receiver is the constraint. A short write beside a long
    /// [`record_egress_queue_wait`](Self::record_egress_queue_wait) means the
    /// writer is starved of runtime time, or the queue is simply long.
    fn record_egress_write_duration(&self, _duration: Duration) {}
}
