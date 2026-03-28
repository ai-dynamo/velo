// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! ZMQ ROUTER listener thread
//!
//! Accepts inbound multipart messages from remote DEALER sockets and routes
//! them to the appropriate transport adapter stream based on `MessageType`.

use bytes::Bytes;
use std::sync::Arc;
use tracing::{debug, error, warn};

use crate::{MessageType, ShutdownState, TransportAdapter};

/// Configuration bundle for the listener thread (avoids too-many-arguments).
pub(crate) struct ListenerConfig {
    pub ctx: Arc<zmq::Context>,
    pub bind_endpoint: String,
    pub control_endpoint: String,
    pub adapter: TransportAdapter,
    pub shutdown_state: ShutdownState,
    pub rcvhwm: i32,
    pub linger_ms: i32,
    pub metrics: Option<velo_observability::TransportMetricsHandle>,
}

/// Run the ROUTER listener thread.
///
/// Binds a ROUTER socket to `bind_endpoint`, polls for inbound messages, and
/// routes decoded frames to the adapter channels. Uses `ShutdownState` for
/// drain gating and a control PAIR socket for shutdown signaling.
pub(crate) fn run_listener(cfg: ListenerConfig) {
    // Create and bind the ROUTER socket
    let router = match cfg.ctx.socket(zmq::ROUTER) {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to create ROUTER socket: {}", e);
            return;
        }
    };

    if let Err(e) = router.set_rcvhwm(cfg.rcvhwm) {
        warn!("Failed to set ZMQ_RCVHWM: {}", e);
    }
    if let Err(e) = router.set_linger(cfg.linger_ms) {
        warn!("Failed to set ZMQ_LINGER: {}", e);
    }
    // ROUTER_MANDATORY: send to disconnected peer returns error instead of silently dropping
    if let Err(e) = router.set_router_mandatory(true) {
        warn!("Failed to set ZMQ_ROUTER_MANDATORY: {}", e);
    }

    if let Err(e) = router.bind(&cfg.bind_endpoint) {
        error!(
            "Failed to bind ROUTER socket to {}: {}",
            cfg.bind_endpoint, e
        );
        return;
    }
    debug!("ZMQ ROUTER socket bound to {}", cfg.bind_endpoint);

    // Control socket for shutdown signaling
    let control = match cfg.ctx.socket(zmq::PAIR) {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to create control socket: {}", e);
            return;
        }
    };
    if let Err(e) = control.bind(&cfg.control_endpoint) {
        error!(
            "Failed to bind listener control socket to {}: {}",
            cfg.control_endpoint, e
        );
        return;
    }

    loop {
        // Poll both ROUTER and control sockets
        let mut poll_items = [
            router.as_poll_item(zmq::POLLIN),
            control.as_poll_item(zmq::POLLIN),
        ];

        match zmq::poll(&mut poll_items, -1) {
            Ok(_) => {}
            Err(zmq::Error::EINTR) => continue, // interrupted by signal, retry
            Err(e) => {
                error!("ZMQ poll error: {}", e);
                break;
            }
        }

        // Check control socket first (higher priority)
        if poll_items[1].is_readable()
            && let Ok(msg) = control.recv_bytes(0)
            && msg.as_slice() == b"shutdown"
        {
            debug!("ZMQ listener received shutdown signal");
            break;
        }

        // Check ROUTER socket for inbound messages
        if poll_items[0].is_readable() {
            // ROUTER delivers: [identity, msg_type_frame, header_frame, payload_frame]
            let multipart = match router.recv_multipart(0) {
                Ok(parts) => parts,
                Err(e) => {
                    warn!("Error receiving multipart message: {}", e);
                    continue;
                }
            };

            // Expect exactly 4 frames: identity + msg_type + header + payload
            if multipart.len() != 4 {
                if let Some(ref m) = cfg.metrics {
                    m.record_rejection(velo_observability::TransportRejection::DecodeError);
                }
                warn!(
                    "ZMQ: expected 4 frames (identity + type + header + payload), got {}",
                    multipart.len()
                );
                continue;
            }

            let type_frame = &multipart[1];
            let header_frame = &multipart[2];
            let payload_frame = &multipart[3];

            // Parse message type
            if type_frame.len() != 1 {
                if let Some(ref m) = cfg.metrics {
                    m.record_rejection(velo_observability::TransportRejection::DecodeError);
                }
                warn!(
                    "ZMQ: message type frame should be 1 byte, got {}",
                    type_frame.len()
                );
                continue;
            }

            let msg_type = match MessageType::from_u8(type_frame[0]) {
                Some(t) => t,
                None => {
                    if let Some(ref m) = cfg.metrics {
                        m.record_rejection(velo_observability::TransportRejection::DecodeError);
                    }
                    warn!("ZMQ: invalid message type byte: {}", type_frame[0]);
                    continue;
                }
            };

            // During drain: reject new Message frames (do not send reply to DEALER identity)
            if cfg.shutdown_state.is_draining() && msg_type == MessageType::Message {
                if let Some(ref m) = cfg.metrics {
                    m.record_rejection(velo_observability::TransportRejection::DrainRejected);
                }
                debug!("ZMQ: rejecting Message frame during drain (dropping without reply)");
                // NOTE: We intentionally do not send a ShuttingDown reply back to the
                // remote DEALER identity here, because DEALER sockets in this design
                // are write-only and would never receive it. Higher layers are responsible
                // for propagating shutdown notifications via the appropriate outbound path.
                continue;
            }

            // Convert to Bytes (single copy from ZMQ buffer → Bytes)
            let header = Bytes::copy_from_slice(header_frame);
            let payload = Bytes::copy_from_slice(payload_frame);

            // Record metrics
            if let Some(ref m) = cfg.metrics {
                m.record_frame(
                    velo_observability::Direction::Inbound,
                    crate::message_type_label(msg_type),
                    header.len() + payload.len(),
                );
            }

            // Route to appropriate adapter stream
            let sender = match msg_type {
                MessageType::Message => &cfg.adapter.message_stream,
                MessageType::Response => &cfg.adapter.response_stream,
                MessageType::Ack | MessageType::Event => &cfg.adapter.event_stream,
                MessageType::ShuttingDown => &cfg.adapter.response_stream,
            };

            if let Err(e) = sender.try_send((header, payload)) {
                if let Some(ref m) = cfg.metrics {
                    m.record_rejection(velo_observability::TransportRejection::RouteFailed);
                }
                warn!("ZMQ: failed to route {:?} frame: {:?}", msg_type, e);
            }
        }
    }

    debug!("ZMQ listener thread exited");
}
