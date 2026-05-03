// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

#![deny(missing_docs)]

//! Shared observability support for Velo.
//!
//! `VeloMetrics` registers Prometheus collectors into a caller-provided
//! [`prometheus::Registry`]. The crate does not expose or serve metrics on its
//! own; hosts remain responsible for gathering and exporting them.

#[cfg(feature = "distributed-tracing")]
use std::collections::HashMap;
use std::time::Duration;

use prometheus::{
    Counter, CounterVec, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, Opts, Registry,
    exponential_buckets,
};

#[cfg(feature = "distributed-tracing")]
use opentelemetry::propagation::{Extractor, Injector};
#[cfg(feature = "distributed-tracing")]
use tracing_opentelemetry::OpenTelemetrySpanExt;

fn register_collector<T>(registry: &Registry, collector: T) -> Result<T, prometheus::Error>
where
    T: prometheus::core::Collector + Clone + 'static,
{
    registry.register(Box::new(collector.clone()))?;
    Ok(collector)
}

const TRANSPORT_DIRECTIONS: [&str; 2] = ["inbound", "outbound"];
const TRANSPORT_MESSAGE_TYPES: [&str; 5] = ["message", "response", "ack", "event", "shutting_down"];
const HANDLER_RESPONSE_TYPES: [&str; 3] = ["fire_and_forget", "ack_nack", "unary"];
const HANDLER_OUTCOMES: [&str; 2] = ["success", "error"];

// ---------------------------------------------------------------------------
// Type-safe label enums
// ---------------------------------------------------------------------------

// `Direction` and `TransportRejection` are defined in `velo-ext` so external
// transport implementors can name them without depending on the runtime.
pub use velo_ext::observability::{Direction, TransportRejection};

#[inline]
fn direction_index(d: Direction) -> usize {
    match d {
        Direction::Inbound => 0,
        Direction::Outbound => 1,
    }
}

/// Messenger handler outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HandlerOutcome {
    /// Handler completed successfully.
    Success,
    /// Handler returned an error.
    Error,
}

impl HandlerOutcome {
    /// Prometheus label value.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Error => "error",
        }
    }

    fn index(self) -> usize {
        match self {
            Self::Success => 0,
            Self::Error => 1,
        }
    }
}

/// Handler response type (mirrors velo-messenger's `ResponseType`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HandlerResponseType {
    /// Fire-and-forget message: no response expected.
    FireAndForget,
    /// Ack/Nack response.
    AckNack,
    /// Unary request-response.
    Unary,
}

impl HandlerResponseType {
    /// Prometheus label value.
    #[allow(dead_code)]
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::FireAndForget => "fire_and_forget",
            Self::AckNack => "ack_nack",
            Self::Unary => "unary",
        }
    }

    fn index(self) -> usize {
        match self {
            Self::FireAndForget => 0,
            Self::AckNack => 1,
            Self::Unary => 2,
        }
    }
}

// (TransportRejection re-exported from velo-ext above; impl comes from there.)

/// Messenger dispatch failure — encodes (stage, reason) pairs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DispatchFailure {
    /// Failed to decode an active message.
    DecodeActiveMessage,
    /// No handler registered for the message type.
    DispatchUnknownHandler,
    /// Failed to send ack/nack response.
    ResponseSendAckNack,
    /// Failed to send unary response.
    ResponseSendUnary,
    /// Failed to send unary error response.
    ResponseSendUnaryError,
    /// Failed to deserialize typed handler input.
    DeserializeTypedInput,
    /// Failed to serialize typed handler output.
    SerializeTypedOutput,
    /// Failed to send response after typed deserialization error.
    ResponseSendTypedDeserialize,
    /// Failed to send typed unary response.
    ResponseSendTypedUnary,
}

impl DispatchFailure {
    /// Returns the `(stage, reason)` Prometheus label values.
    pub(crate) fn as_stage_reason(self) -> (&'static str, &'static str) {
        match self {
            Self::DecodeActiveMessage => ("decode", "active_message"),
            Self::DispatchUnknownHandler => ("dispatch", "unknown_handler"),
            Self::ResponseSendAckNack => ("response_send", "ack_nack"),
            Self::ResponseSendUnary => ("response_send", "unary"),
            Self::ResponseSendUnaryError => ("response_send", "unary_error"),
            Self::DeserializeTypedInput => ("deserialize", "typed_input"),
            Self::SerializeTypedOutput => ("serialize", "typed_output"),
            Self::ResponseSendTypedDeserialize => ("response_send", "typed_deserialize"),
            Self::ResponseSendTypedUnary => ("response_send", "typed_unary"),
        }
    }
}

/// Client resolution path and outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClientResolution {
    /// Direct send succeeded (fast path).
    DirectSuccess,
    /// Handshake initiated.
    HandshakeAttempt,
    /// Handshake completed successfully.
    HandshakeSuccess,
    /// Handshake failed.
    HandshakeError,
    /// Discovery resolved successfully.
    DiscoverySuccess,
    /// Discovery failed.
    DiscoveryError,
    /// Message delivery failed after resolution completed successfully.
    SendError,
}

impl ClientResolution {
    /// Returns the `(path, outcome)` Prometheus label values.
    pub(crate) fn as_path_outcome(self) -> (&'static str, &'static str) {
        match self {
            Self::DirectSuccess => ("direct", "success"),
            Self::HandshakeAttempt => ("handshake", "attempt"),
            Self::HandshakeSuccess => ("handshake", "success"),
            Self::HandshakeError => ("handshake", "error"),
            Self::DiscoverySuccess => ("discovery", "success"),
            Self::DiscoveryError => ("discovery", "error"),
            Self::SendError => ("send", "error"),
        }
    }
}

/// Streaming control-plane operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StreamingOp {
    /// Attach a sender to an anchor.
    Attach,
    /// Detach the current sender from an anchor.
    Detach,
    /// Finalize an anchor (terminal).
    Finalize,
    /// Cancel an anchor (terminal).
    Cancel,
}

impl StreamingOp {
    /// Prometheus label value.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Attach => "attach",
            Self::Detach => "detach",
            Self::Finalize => "finalize",
            Self::Cancel => "cancel",
        }
    }
}

/// Rendezvous operation type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RendezvousOp {
    /// Data registered (staged).
    Register,
    /// Data retrieved (get/get_into).
    Get,
    /// Read lock released without decrementing refcount.
    Detach,
    /// Read lock released and refcount decremented.
    Release,
    /// Metadata queried.
    Metadata,
    /// Refcount incremented.
    Ref,
}

impl RendezvousOp {
    /// Prometheus label value.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Register => "register",
            Self::Get => "get",
            Self::Detach => "detach",
            Self::Release => "release",
            Self::Metadata => "metadata",
            Self::Ref => "ref",
        }
    }
}

/// Well-known message type label constants.
///
/// These match the values returned by `message_type_label(MessageType)` in
/// velo-transports and the pre-bound arrays in [`VeloMetrics::bind_transport`].
pub mod labels {
    /// `MessageType::Message`
    pub const MSG_MESSAGE: &str = "message";
    /// `MessageType::Response`
    pub const MSG_RESPONSE: &str = "response";
    /// `MessageType::Ack`
    pub const MSG_ACK: &str = "ack";
    /// `MessageType::Event`
    pub const MSG_EVENT: &str = "event";
    /// `MessageType::ShuttingDown`
    pub const MSG_SHUTTING_DOWN: &str = "shutting_down";
}

// Keep for the record_frame fallback when message_type is an unknown &str.
fn transport_message_type_index(message_type: &str) -> Option<usize> {
    match message_type {
        "message" => Some(0),
        "response" => Some(1),
        "ack" => Some(2),
        "event" => Some(3),
        "shutting_down" => Some(4),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// GaugeGuard
// ---------------------------------------------------------------------------

/// A gauge guard that decrements the underlying gauge on drop.
#[derive(Clone, Debug)]
pub struct GaugeGuard {
    gauge: Gauge,
}

impl GaugeGuard {
    /// Increment the gauge and return a guard that decrements it on drop.
    pub fn increment(gauge: Gauge) -> Self {
        gauge.inc();
        Self { gauge }
    }
}

impl Drop for GaugeGuard {
    fn drop(&mut self) {
        self.gauge.dec();
    }
}

// ---------------------------------------------------------------------------
// TransportMetricsHandle
// ---------------------------------------------------------------------------

/// A transport-scoped metrics handle with pre-bound child collectors.
#[derive(Clone)]
pub struct TransportMetricsHandle {
    transport_frames_total: CounterVec,
    transport_frame_bytes_total: CounterVec,
    transport: String,
    accepted_frames: [[Counter; TRANSPORT_MESSAGE_TYPES.len()]; TRANSPORT_DIRECTIONS.len()],
    frame_bytes: [[Counter; TRANSPORT_MESSAGE_TYPES.len()]; TRANSPORT_DIRECTIONS.len()],
    registered_peers: Gauge,
    active_connections: Gauge,
    send_error: Counter,
    drain_rejected: Counter,
    decode_error: Counter,
    route_failed: Counter,
    missing_headers: Counter,
    missing_type: Counter,
    invalid_type: Counter,
    invalid_header_length: Counter,
    truncated_frame: Counter,
    drain_reply_build_failed: Counter,
    send_backpressure: Counter,
}

impl TransportMetricsHandle {
    /// Record an accepted frame using pre-bound counters whenever labels are known.
    pub fn record_frame(&self, direction: Direction, message_type: &str, bytes: usize) {
        let direction_idx = direction_index(direction);
        match transport_message_type_index(message_type) {
            Some(message_type_idx) => {
                self.accepted_frames[direction_idx][message_type_idx].inc();
                self.frame_bytes[direction_idx][message_type_idx].inc_by(bytes as f64);
            }
            None => {
                debug_assert!(false, "unknown message_type label: {message_type:?}");
                let transport = self.transport.as_str();
                self.transport_frames_total
                    .with_label_values(&[transport, direction.as_str(), message_type, "accepted"])
                    .inc();
                self.transport_frame_bytes_total
                    .with_label_values(&[transport, direction.as_str(), message_type])
                    .inc_by(bytes as f64);
            }
        }
    }

    /// Record a transport rejection.
    pub fn record_rejection(&self, reason: TransportRejection) {
        match reason {
            TransportRejection::SendError => self.send_error.inc(),
            TransportRejection::DrainRejected => self.drain_rejected.inc(),
            TransportRejection::DecodeError => self.decode_error.inc(),
            TransportRejection::RouteFailed => self.route_failed.inc(),
            TransportRejection::MissingHeaders => self.missing_headers.inc(),
            TransportRejection::MissingType => self.missing_type.inc(),
            TransportRejection::InvalidType => self.invalid_type.inc(),
            TransportRejection::InvalidHeaderLength => self.invalid_header_length.inc(),
            TransportRejection::TruncatedFrame => self.truncated_frame.inc(),
            TransportRejection::DrainReplyBuildFailed => self.drain_reply_build_failed.inc(),
        }
    }

    /// Set the registered-peer gauge.
    pub fn set_registered_peers(&self, count: usize) {
        self.registered_peers.set(count as f64);
    }

    /// Set the active-connection gauge.
    pub fn set_active_connections(&self, count: usize) {
        self.active_connections.set(count as f64);
    }

    /// Record a send that could not enqueue synchronously on the per-peer
    /// bounded channel and fell through to the `SendBackpressure` await path.
    /// Fires once per `try_send_or_backpressure` Full-branch return.
    pub fn record_send_backpressure(&self) {
        self.send_backpressure.inc();
    }

    /// Convenience form: inspect a send result and increment the backpressure
    /// counter iff it's `Err`. Generic over the error type so the helper is
    /// usable without pulling `velo-transports` types into this crate.
    /// Every transport's `send_message` result fits (the `Err` variant is
    /// `SendBackpressure`, but this helper cares only about Ok vs Err).
    pub fn record_send_backpressure_on<T, E>(&self, r: &Result<T, E>) {
        if r.is_err() {
            self.send_backpressure.inc();
        }
    }
}

impl velo_ext::TransportObservability for TransportMetricsHandle {
    fn record_frame(&self, direction: Direction, message_type: &str, bytes: usize) {
        TransportMetricsHandle::record_frame(self, direction, message_type, bytes);
    }

    fn record_rejection(&self, reason: TransportRejection) {
        TransportMetricsHandle::record_rejection(self, reason);
    }

    fn set_registered_peers(&self, count: usize) {
        TransportMetricsHandle::set_registered_peers(self, count);
    }

    fn set_active_connections(&self, count: usize) {
        TransportMetricsHandle::set_active_connections(self, count);
    }

    fn record_send_backpressure(&self) {
        TransportMetricsHandle::record_send_backpressure(self);
    }
}

// ---------------------------------------------------------------------------
// HandlerMetricsHandle
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct HandlerOutcomeMetrics {
    requests: Counter,
    duration: Histogram,
    response_bytes: Counter,
}

#[derive(Clone)]
struct HandlerResponseMetrics {
    request_bytes: Counter,
    outcomes: [HandlerOutcomeMetrics; HANDLER_OUTCOMES.len()],
}

/// A handler-scoped metrics handle with pre-bound child collectors.
#[derive(Clone)]
pub(crate) struct HandlerMetricsHandle {
    in_flight: Gauge,
    response_types: [HandlerResponseMetrics; HANDLER_RESPONSE_TYPES.len()],
}

impl HandlerMetricsHandle {
    /// Increment the in-flight gauge and return a drop guard.
    pub(crate) fn start(&self) -> GaugeGuard {
        GaugeGuard::increment(self.in_flight.clone())
    }

    /// Record handler completion data using cached child collectors.
    pub fn finish(
        &self,
        response_type: HandlerResponseType,
        outcome: HandlerOutcome,
        elapsed: Duration,
        request_bytes: usize,
        response_bytes: usize,
    ) {
        let response_metrics = &self.response_types[response_type.index()];
        let outcome_metrics = &response_metrics.outcomes[outcome.index()];

        response_metrics.request_bytes.inc_by(request_bytes as f64);
        outcome_metrics.requests.inc();
        outcome_metrics.duration.observe(elapsed.as_secs_f64());
        outcome_metrics.response_bytes.inc_by(response_bytes as f64);
    }
}

// ---------------------------------------------------------------------------
// StreamingTransportMetricsHandle
// ---------------------------------------------------------------------------

/// A transport-scheme-scoped streaming metrics handle.
#[derive(Clone)]
pub(crate) struct StreamingTransportMetricsHandle {
    backpressure: Counter,
}

impl StreamingTransportMetricsHandle {
    /// Record a backpressure event.
    pub(crate) fn record_backpressure(&self) {
        self.backpressure.inc();
    }
}

// ---------------------------------------------------------------------------
// VeloMetrics
// ---------------------------------------------------------------------------

/// Shared Prometheus collectors for Velo.
#[derive(Clone)]
pub struct VeloMetrics {
    transport_frames_total: CounterVec,
    transport_frame_bytes_total: CounterVec,
    transport_rejections_total: CounterVec,
    transport_send_backpressure_total: CounterVec,
    transport_registered_peers: GaugeVec,
    transport_active_connections: GaugeVec,
    messenger_handler_requests_total: CounterVec,
    messenger_handler_duration_seconds: HistogramVec,
    messenger_handler_request_bytes_total: CounterVec,
    messenger_handler_response_bytes_total: CounterVec,
    messenger_handler_in_flight: GaugeVec,
    messenger_dispatch_failures_total: CounterVec,
    messenger_client_resolution_total: CounterVec,
    messenger_pending_responses: Gauge,
    messenger_response_slot_exhausted_total: Counter,
    streaming_anchor_operations_total: CounterVec,
    streaming_anchor_operation_duration_seconds: HistogramVec,
    streaming_active_anchors: Gauge,
    streaming_backpressure_total: CounterVec,
    // Rendezvous metrics
    rendezvous_operations_total: CounterVec,
    rendezvous_operation_duration_seconds: HistogramVec,
    rendezvous_bytes_total: CounterVec,
    rendezvous_active_slots: Gauge,
}

impl VeloMetrics {
    /// Register all Velo metrics into the given registry.
    pub fn register(registry: &Registry) -> Result<Self, prometheus::Error> {
        // NOTE: transport_frames_total carries an `outcome` label that is
        // currently always "accepted". Rejections are tracked separately in
        // transport_rejections_total. The label is retained for potential
        // future use (e.g. tracking rejected frames inline).
        let transport_frames_total = register_collector(
            registry,
            CounterVec::new(
                Opts::new(
                    "velo_transport_frames_total",
                    "Transport frames observed by Velo.",
                ),
                &["transport", "direction", "message_type", "outcome"],
            )?,
        )?;
        let transport_frame_bytes_total = register_collector(
            registry,
            CounterVec::new(
                Opts::new(
                    "velo_transport_frame_bytes_total",
                    "Logical frame bytes observed by Velo transports.",
                ),
                &["transport", "direction", "message_type"],
            )?,
        )?;
        let transport_rejections_total = register_collector(
            registry,
            CounterVec::new(
                Opts::new(
                    "velo_transport_rejections_total",
                    "Transport-level rejections and drops observed by Velo.",
                ),
                &["transport", "reason"],
            )?,
        )?;
        let transport_send_backpressure_total = register_collector(
            registry,
            CounterVec::new(
                Opts::new(
                    "velo_transport_send_backpressure_total",
                    "Sends that hit the bounded per-peer channel's Full branch and fell through to the SendBackpressure await path.",
                ),
                &["transport"],
            )?,
        )?;
        let transport_registered_peers = register_collector(
            registry,
            GaugeVec::new(
                Opts::new(
                    "velo_transport_registered_peers",
                    "Registered peer count per transport.",
                ),
                &["transport"],
            )?,
        )?;
        let transport_active_connections = register_collector(
            registry,
            GaugeVec::new(
                Opts::new(
                    "velo_transport_active_connections",
                    "Active connection count per transport.",
                ),
                &["transport"],
            )?,
        )?;
        let messenger_handler_requests_total = register_collector(
            registry,
            CounterVec::new(
                Opts::new(
                    "velo_messenger_handler_requests_total",
                    "Messenger handler requests observed by Velo.",
                ),
                &["handler", "response_type", "outcome"],
            )?,
        )?;
        let messenger_handler_duration_seconds = register_collector(
            registry,
            HistogramVec::new(
                HistogramOpts::new(
                    "velo_messenger_handler_duration_seconds",
                    "Messenger handler execution time.",
                )
                .buckets(exponential_buckets(0.0005, 2.0, 16).map_err(|e| {
                    prometheus::Error::Msg(format!("invalid messenger histogram buckets: {e}"))
                })?),
                &["handler", "response_type", "outcome"],
            )?,
        )?;
        let messenger_handler_request_bytes_total = register_collector(
            registry,
            CounterVec::new(
                Opts::new(
                    "velo_messenger_handler_request_bytes_total",
                    "Messenger handler request payload bytes.",
                ),
                &["handler", "response_type"],
            )?,
        )?;
        let messenger_handler_response_bytes_total = register_collector(
            registry,
            CounterVec::new(
                Opts::new(
                    "velo_messenger_handler_response_bytes_total",
                    "Messenger handler response payload bytes.",
                ),
                &["handler", "response_type", "outcome"],
            )?,
        )?;
        let messenger_handler_in_flight = register_collector(
            registry,
            GaugeVec::new(
                Opts::new(
                    "velo_messenger_handler_in_flight",
                    "Messenger handlers currently executing.",
                ),
                &["handler"],
            )?,
        )?;
        let messenger_dispatch_failures_total = register_collector(
            registry,
            CounterVec::new(
                Opts::new(
                    "velo_messenger_dispatch_failures_total",
                    "Messenger dispatch and response path failures.",
                ),
                &["stage", "reason"],
            )?,
        )?;
        let messenger_client_resolution_total = register_collector(
            registry,
            CounterVec::new(
                Opts::new(
                    "velo_messenger_client_resolution_total",
                    "Messenger client routing path outcomes.",
                ),
                &["path", "outcome"],
            )?,
        )?;
        let messenger_pending_responses = register_collector(
            registry,
            Gauge::with_opts(Opts::new(
                "velo_messenger_pending_responses",
                "Pending messenger response awaiters.",
            ))?,
        )?;
        let messenger_response_slot_exhausted_total = register_collector(
            registry,
            Counter::with_opts(Opts::new(
                "velo_messenger_response_slot_exhausted_total",
                "Response slot acquisitions that hit the per-worker capacity ceiling.",
            ))?,
        )?;
        let streaming_anchor_operations_total = register_collector(
            registry,
            CounterVec::new(
                Opts::new(
                    "velo_streaming_anchor_operations_total",
                    "Streaming control-plane operations observed by Velo.",
                ),
                &["operation", "outcome", "transport_scheme"],
            )?,
        )?;
        let streaming_anchor_operation_duration_seconds = register_collector(
            registry,
            HistogramVec::new(
                HistogramOpts::new(
                    "velo_streaming_anchor_operation_duration_seconds",
                    "Streaming control-plane operation duration.",
                )
                .buckets(exponential_buckets(0.0005, 2.0, 16).map_err(|e| {
                    prometheus::Error::Msg(format!("invalid streaming histogram buckets: {e}"))
                })?),
                &["operation", "outcome", "transport_scheme"],
            )?,
        )?;
        let streaming_active_anchors = register_collector(
            registry,
            Gauge::with_opts(Opts::new(
                "velo_streaming_active_anchors",
                "Anchors currently present in the streaming registry.",
            ))?,
        )?;
        let streaming_backpressure_total = register_collector(
            registry,
            CounterVec::new(
                Opts::new(
                    "velo_streaming_backpressure_total",
                    "Streaming backpressure events observed by Velo.",
                ),
                &["transport_scheme"],
            )?,
        )?;

        // -- Rendezvous metrics --
        let rendezvous_operations_total = register_collector(
            registry,
            CounterVec::new(
                Opts::new(
                    "velo_rendezvous_operations_total",
                    "Rendezvous operations observed by Velo.",
                ),
                &["operation", "outcome"],
            )?,
        )?;
        let rendezvous_operation_duration_seconds = register_collector(
            registry,
            HistogramVec::new(
                HistogramOpts::new(
                    "velo_rendezvous_operation_duration_seconds",
                    "Duration of rendezvous operations.",
                )
                .buckets(exponential_buckets(0.0005, 2.0, 20)?),
                &["operation", "outcome"],
            )?,
        )?;
        let rendezvous_bytes_total = register_collector(
            registry,
            CounterVec::new(
                Opts::new(
                    "velo_rendezvous_bytes_total",
                    "Total bytes transferred via rendezvous.",
                ),
                &["operation"],
            )?,
        )?;
        let rendezvous_active_slots = register_collector(
            registry,
            Gauge::new(
                "velo_rendezvous_active_slots",
                "Number of data slots currently staged in the rendezvous registry.",
            )?,
        )?;

        Ok(Self {
            transport_frames_total,
            transport_frame_bytes_total,
            transport_rejections_total,
            transport_send_backpressure_total,
            transport_registered_peers,
            transport_active_connections,
            messenger_handler_requests_total,
            messenger_handler_duration_seconds,
            messenger_handler_request_bytes_total,
            messenger_handler_response_bytes_total,
            messenger_handler_in_flight,
            messenger_dispatch_failures_total,
            messenger_client_resolution_total,
            messenger_pending_responses,
            messenger_response_slot_exhausted_total,
            streaming_anchor_operations_total,
            streaming_anchor_operation_duration_seconds,
            streaming_active_anchors,
            streaming_backpressure_total,
            rendezvous_operations_total,
            rendezvous_operation_duration_seconds,
            rendezvous_bytes_total,
            rendezvous_active_slots,
        })
    }

    /// Returns `true` when a handler should emit generic per-handler series.
    pub fn should_track_handler(handler: &str) -> bool {
        !handler.starts_with('_')
    }

    /// Bind transport collectors for a specific transport label.
    pub fn bind_transport(&self, transport: &str) -> TransportMetricsHandle {
        let transport = transport.to_string();
        let transport_label = transport.as_str();

        let accepted_frames = std::array::from_fn(|direction_idx| {
            std::array::from_fn(|message_type_idx| {
                self.transport_frames_total.with_label_values(&[
                    transport_label,
                    TRANSPORT_DIRECTIONS[direction_idx],
                    TRANSPORT_MESSAGE_TYPES[message_type_idx],
                    "accepted",
                ])
            })
        });

        let frame_bytes = std::array::from_fn(|direction_idx| {
            std::array::from_fn(|message_type_idx| {
                self.transport_frame_bytes_total.with_label_values(&[
                    transport_label,
                    TRANSPORT_DIRECTIONS[direction_idx],
                    TRANSPORT_MESSAGE_TYPES[message_type_idx],
                ])
            })
        });

        TransportMetricsHandle {
            transport_frames_total: self.transport_frames_total.clone(),
            transport_frame_bytes_total: self.transport_frame_bytes_total.clone(),
            transport: transport.clone(),
            accepted_frames,
            frame_bytes,
            registered_peers: self
                .transport_registered_peers
                .with_label_values(&[transport_label]),
            active_connections: self
                .transport_active_connections
                .with_label_values(&[transport_label]),
            send_error: self
                .transport_rejections_total
                .with_label_values(&[transport_label, "send_error"]),
            drain_rejected: self
                .transport_rejections_total
                .with_label_values(&[transport_label, "drain_rejected"]),
            decode_error: self
                .transport_rejections_total
                .with_label_values(&[transport_label, "decode_error"]),
            route_failed: self
                .transport_rejections_total
                .with_label_values(&[transport_label, "route_failed"]),
            missing_headers: self
                .transport_rejections_total
                .with_label_values(&[transport_label, "missing_headers"]),
            missing_type: self
                .transport_rejections_total
                .with_label_values(&[transport_label, "missing_type"]),
            invalid_type: self
                .transport_rejections_total
                .with_label_values(&[transport_label, "invalid_type"]),
            invalid_header_length: self
                .transport_rejections_total
                .with_label_values(&[transport_label, "invalid_header_length"]),
            truncated_frame: self
                .transport_rejections_total
                .with_label_values(&[transport_label, "truncated_frame"]),
            drain_reply_build_failed: self
                .transport_rejections_total
                .with_label_values(&[transport_label, "drain_reply_build_failed"]),
            send_backpressure: self
                .transport_send_backpressure_total
                .with_label_values(&[transport_label]),
        }
    }

    /// Bind per-handler collectors for a specific handler label.
    pub(crate) fn bind_handler(&self, handler: &str) -> Option<HandlerMetricsHandle> {
        if !Self::should_track_handler(handler) {
            return None;
        }

        let response_types = std::array::from_fn(|response_idx| HandlerResponseMetrics {
            request_bytes: self
                .messenger_handler_request_bytes_total
                .with_label_values(&[handler, HANDLER_RESPONSE_TYPES[response_idx]]),
            outcomes: std::array::from_fn(|outcome_idx| HandlerOutcomeMetrics {
                requests: self.messenger_handler_requests_total.with_label_values(&[
                    handler,
                    HANDLER_RESPONSE_TYPES[response_idx],
                    HANDLER_OUTCOMES[outcome_idx],
                ]),
                duration: self.messenger_handler_duration_seconds.with_label_values(&[
                    handler,
                    HANDLER_RESPONSE_TYPES[response_idx],
                    HANDLER_OUTCOMES[outcome_idx],
                ]),
                response_bytes: self
                    .messenger_handler_response_bytes_total
                    .with_label_values(&[
                        handler,
                        HANDLER_RESPONSE_TYPES[response_idx],
                        HANDLER_OUTCOMES[outcome_idx],
                    ]),
            }),
        });

        Some(HandlerMetricsHandle {
            in_flight: self
                .messenger_handler_in_flight
                .with_label_values(&[handler]),
            response_types,
        })
    }

    /// Bind streaming collectors for a specific transport-scheme label.
    pub(crate) fn bind_streaming_transport(
        &self,
        transport_scheme: &str,
    ) -> StreamingTransportMetricsHandle {
        StreamingTransportMetricsHandle {
            backpressure: self
                .streaming_backpressure_total
                .with_label_values(&[transport_scheme]),
        }
    }

    /// Record a messenger dispatch failure.
    pub(crate) fn record_dispatch_failure(&self, failure: DispatchFailure) {
        let (stage, reason) = failure.as_stage_reason();
        self.messenger_dispatch_failures_total
            .with_label_values(&[stage, reason])
            .inc();
    }

    /// Record a client routing path outcome.
    pub(crate) fn record_client_resolution(&self, resolution: ClientResolution) {
        let (path, outcome) = resolution.as_path_outcome();
        self.messenger_client_resolution_total
            .with_label_values(&[path, outcome])
            .inc();
    }

    /// Set the pending-response gauge.
    pub(crate) fn set_pending_responses(&self, count: usize) {
        self.messenger_pending_responses.set(count as f64);
    }

    /// Record a response-slot acquisition that hit the per-worker capacity
    /// ceiling. Fired once per failed acquisition, whether the caller
    /// fail-fasts via `register_outcome` or falls through to the async
    /// backpressure path.
    pub(crate) fn inc_response_slot_exhausted(&self) {
        self.messenger_response_slot_exhausted_total.inc();
    }

    /// Record a streaming control-plane operation.
    pub(crate) fn record_streaming_operation(
        &self,
        operation: StreamingOp,
        outcome: HandlerOutcome,
        transport_scheme: &str,
        elapsed: Duration,
    ) {
        self.streaming_anchor_operations_total
            .with_label_values(&[operation.as_str(), outcome.as_str(), transport_scheme])
            .inc();
        self.streaming_anchor_operation_duration_seconds
            .with_label_values(&[operation.as_str(), outcome.as_str(), transport_scheme])
            .observe(elapsed.as_secs_f64());
    }

    /// Set the active-anchor gauge.
    pub(crate) fn set_streaming_active_anchors(&self, count: usize) {
        self.streaming_active_anchors.set(count as f64);
    }

    /// Record a streaming backpressure event.
    #[allow(dead_code)]
    pub(crate) fn record_streaming_backpressure(&self, transport_scheme: &str) {
        self.streaming_backpressure_total
            .with_label_values(&[transport_scheme])
            .inc();
    }

    /// Record a rendezvous operation (register, get, release, etc.).
    pub(crate) fn record_rendezvous_operation(
        &self,
        operation: RendezvousOp,
        outcome: HandlerOutcome,
        elapsed: Duration,
    ) {
        self.rendezvous_operations_total
            .with_label_values(&[operation.as_str(), outcome.as_str()])
            .inc();
        self.rendezvous_operation_duration_seconds
            .with_label_values(&[operation.as_str(), outcome.as_str()])
            .observe(elapsed.as_secs_f64());
    }

    /// Record bytes transferred via rendezvous.
    pub(crate) fn record_rendezvous_bytes(&self, operation: RendezvousOp, bytes: usize) {
        self.rendezvous_bytes_total
            .with_label_values(&[operation.as_str()])
            .inc_by(bytes as f64);
    }

    /// Set the number of active rendezvous slots.
    pub(crate) fn set_rendezvous_active_slots(&self, count: usize) {
        self.rendezvous_active_slots.set(count as f64);
    }
}

// ---------------------------------------------------------------------------
// Distributed tracing (optional)
// ---------------------------------------------------------------------------

#[cfg(feature = "distributed-tracing")]
struct HeaderInjector<'a> {
    headers: &'a mut HashMap<String, String>,
}

#[cfg(feature = "distributed-tracing")]
impl Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.headers.insert(key.to_string(), value);
    }
}

#[cfg(feature = "distributed-tracing")]
struct HeaderExtractor<'a> {
    headers: &'a HashMap<String, String>,
}

#[cfg(feature = "distributed-tracing")]
impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.headers.get(key).map(String::as_str)
    }

    fn keys(&self) -> Vec<&str> {
        self.headers.keys().map(String::as_str).collect()
    }
}

/// Inject the current OpenTelemetry context into message headers.
#[cfg(feature = "distributed-tracing")]
pub fn inject_current_context(headers: &mut Option<HashMap<String, String>>) {
    let map = headers.get_or_insert_with(HashMap::new);
    opentelemetry::global::get_text_map_propagator(|propagator| {
        let mut injector = HeaderInjector { headers: map };
        propagator.inject_context(&opentelemetry::Context::current(), &mut injector);
    });
}

/// Apply an extracted OpenTelemetry parent context to a tracing span.
#[cfg(feature = "distributed-tracing")]
pub fn apply_remote_parent(span: &tracing::Span, headers: Option<&HashMap<String, String>>) {
    let Some(headers) = headers else {
        return;
    };

    opentelemetry::global::get_text_map_propagator(|propagator| {
        let extractor = HeaderExtractor { headers };
        let parent = propagator.extract(&extractor);
        let _ = span.set_parent(parent);
    });
}

// ---------------------------------------------------------------------------
// Test helpers (available via `features = ["test-helpers"]`)
// ---------------------------------------------------------------------------

/// Metric extraction utilities for integration tests.
///
/// Enable with `velo-observability = { ..., features = ["test-helpers"] }` in
/// `[dev-dependencies]`.
#[cfg(any(test, feature = "test-helpers"))]
pub mod test_helpers {
    use prometheus::Registry;
    use prometheus::proto::MetricFamily;

    /// A snapshot of all metric families gathered from a [`Registry`].
    ///
    /// Provides typed accessors for counter, gauge, and histogram values
    /// filtered by metric name and label set.
    pub struct MetricSnapshot(Vec<MetricFamily>);

    impl MetricSnapshot {
        /// Gather all metrics from `registry` into a queryable snapshot.
        pub fn from_registry(registry: &Registry) -> Self {
            Self(registry.gather())
        }

        /// Counter value for `name` with the given label pairs, or 0.0 if absent.
        pub fn counter(&self, name: &str, labels: &[(&str, &str)]) -> f64 {
            self.find_metric(name, labels)
                .map(|m| m.get_counter().value())
                .unwrap_or(0.0)
        }

        /// Gauge value for `name` with the given label pairs, or 0.0 if absent.
        pub fn gauge(&self, name: &str, labels: &[(&str, &str)]) -> f64 {
            self.find_metric(name, labels)
                .map(|m| m.get_gauge().value())
                .unwrap_or(0.0)
        }

        /// Histogram sample count for `name` with the given label pairs, or 0 if absent.
        pub fn histogram_count(&self, name: &str, labels: &[(&str, &str)]) -> u64 {
            self.find_metric(name, labels)
                .map(|m| m.get_histogram().sample_count())
                .unwrap_or(0)
        }

        /// Histogram sample sum for `name` with the given label pairs, or 0.0 if absent.
        pub fn histogram_sum(&self, name: &str, labels: &[(&str, &str)]) -> f64 {
            self.find_metric(name, labels)
                .map(|m| m.get_histogram().sample_sum())
                .unwrap_or(0.0)
        }

        fn find_metric(
            &self,
            name: &str,
            labels: &[(&str, &str)],
        ) -> Option<&prometheus::proto::Metric> {
            let family = self.0.iter().find(|f| f.name() == name)?;
            family.get_metric().iter().find(|m| {
                let pairs = m.get_label();
                labels
                    .iter()
                    .all(|(k, v)| pairs.iter().any(|lp| lp.name() == *k && lp.value() == *v))
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_register_into_registry() {
        let registry = Registry::new();
        let metrics = VeloMetrics::register(&registry).expect("register metrics");

        // Exercise transport metrics via pre-bound handle.
        let handle = metrics.bind_transport("tcp");
        handle.record_frame(Direction::Outbound, "message", 42);
        handle.record_rejection(TransportRejection::DecodeError);
        handle.set_registered_peers(3);
        handle.set_active_connections(1);

        // Exercise dispatch failure.
        metrics.record_dispatch_failure(DispatchFailure::DecodeActiveMessage);

        // Exercise client resolution.
        metrics.record_client_resolution(ClientResolution::DirectSuccess);

        // Exercise messenger gauges.
        metrics.set_pending_responses(3);

        // Exercise streaming metrics.
        metrics.record_streaming_operation(
            StreamingOp::Cancel,
            HandlerOutcome::Success,
            "velo",
            Duration::from_millis(1),
        );
        metrics.set_streaming_active_anchors(2);

        let names: Vec<_> = registry
            .gather()
            .into_iter()
            .map(|family| family.name().to_string())
            .collect();

        assert!(names.contains(&"velo_transport_frames_total".to_string()));
        assert!(names.contains(&"velo_transport_rejections_total".to_string()));
        assert!(names.contains(&"velo_messenger_dispatch_failures_total".to_string()));
        assert!(names.contains(&"velo_messenger_client_resolution_total".to_string()));
        assert!(names.contains(&"velo_messenger_pending_responses".to_string()));
        assert!(names.contains(&"velo_streaming_active_anchors".to_string()));
        assert!(names.contains(&"velo_streaming_anchor_operations_total".to_string()));
        assert!(names.contains(&"velo_streaming_anchor_operation_duration_seconds".to_string()));
    }

    #[test]
    fn gauge_guard_increments_and_decrements() {
        let gauge = Gauge::new("test_gauge", "test").unwrap();
        assert_eq!(gauge.get(), 0.0);

        let guard = GaugeGuard::increment(gauge.clone());
        assert_eq!(gauge.get(), 1.0);

        drop(guard);
        assert_eq!(gauge.get(), 0.0);
    }

    #[test]
    fn bind_handler_skips_system_handlers() {
        let registry = Registry::new();
        let metrics = VeloMetrics::register(&registry).expect("register metrics");
        assert!(metrics.bind_handler("_internal").is_none());
        assert!(metrics.bind_handler("user_handler").is_some());
    }

    #[test]
    fn handler_metrics_finish_records_all_dimensions() {
        let registry = Registry::new();
        let metrics = VeloMetrics::register(&registry).expect("register metrics");
        let handle = metrics.bind_handler("test_handler").unwrap();

        let _guard = handle.start();
        handle.finish(
            HandlerResponseType::Unary,
            HandlerOutcome::Success,
            Duration::from_millis(5),
            100,
            200,
        );

        let families = registry.gather();
        let requests = families
            .iter()
            .find(|f| f.name() == "velo_messenger_handler_requests_total")
            .expect("requests metric");
        assert!(!requests.get_metric().is_empty());
    }

    #[test]
    fn enum_label_values_match_const_arrays() {
        assert_eq!(Direction::Inbound.as_str(), TRANSPORT_DIRECTIONS[0]);
        assert_eq!(Direction::Outbound.as_str(), TRANSPORT_DIRECTIONS[1]);

        assert_eq!(
            HandlerResponseType::FireAndForget.as_str(),
            HANDLER_RESPONSE_TYPES[0]
        );
        assert_eq!(
            HandlerResponseType::AckNack.as_str(),
            HANDLER_RESPONSE_TYPES[1]
        );
        assert_eq!(
            HandlerResponseType::Unary.as_str(),
            HANDLER_RESPONSE_TYPES[2]
        );

        assert_eq!(HandlerOutcome::Success.as_str(), HANDLER_OUTCOMES[0]);
        assert_eq!(HandlerOutcome::Error.as_str(), HANDLER_OUTCOMES[1]);
    }
}
