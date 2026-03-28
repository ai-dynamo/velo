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

/// Run the ROUTER listener thread.
///
/// Binds a ROUTER socket to `bind_endpoint`, polls for inbound messages, and
/// routes decoded frames to the adapter channels. Supports drain/shutdown via
/// a control PAIR socket.
pub(crate) fn run_listener(
    ctx: Arc<zmq::Context>,
    bind_endpoint: &str,
    control_endpoint: &str,
    adapter: TransportAdapter,
    _shutdown_state: ShutdownState,
    rcvhwm: i32,
    linger_ms: i32,
    _transport_key: &str,
    metrics: Option<velo_observability::TransportMetricsHandle>,
) {
    // Create and bind the ROUTER socket
    let router = match ctx.socket(zmq::ROUTER) {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to create ROUTER socket: {}", e);
            return;
        }
    };

    if let Err(e) = router.set_rcvhwm(rcvhwm) {
        warn!("Failed to set ZMQ_RCVHWM: {}", e);
    }
    if let Err(e) = router.set_linger(linger_ms) {
        warn!("Failed to set ZMQ_LINGER: {}", e);
    }
    // ROUTER_MANDATORY: send to disconnected peer returns error instead of silently dropping
    if let Err(e) = router.set_router_mandatory(true) {
        warn!("Failed to set ZMQ_ROUTER_MANDATORY: {}", e);
    }

    if let Err(e) = router.bind(bind_endpoint) {
        error!("Failed to bind ROUTER socket to {}: {}", bind_endpoint, e);
        return;
    }
    debug!("ZMQ ROUTER socket bound to {}", bind_endpoint);

    // Control socket for drain/shutdown signaling
    let control = match ctx.socket(zmq::PAIR) {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to create control socket: {}", e);
            return;
        }
    };
    if let Err(e) = control.bind(control_endpoint) {
        error!(
            "Failed to bind listener control socket to {}: {}",
            control_endpoint, e
        );
        return;
    }

    let mut draining = false;

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
        if poll_items[1].is_readable() {
            match control.recv_bytes(0) {
                Ok(msg) => match msg.as_slice() {
                    b"drain" => {
                        debug!("ZMQ listener entering drain mode");
                        draining = true;
                    }
                    b"shutdown" => {
                        debug!("ZMQ listener received shutdown signal");
                        break;
                    }
                    _ => {}
                },
                Err(e) => {
                    warn!("Error reading control socket: {}", e);
                }
            }
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
                if let Some(ref m) = metrics {
                    m.record_rejection(velo_observability::TransportRejection::DecodeError);
                }
                warn!(
                    "ZMQ: expected 4 frames (identity + type + header + payload), got {}",
                    multipart.len()
                );
                continue;
            }

            let identity = &multipart[0];
            let type_frame = &multipart[1];
            let header_frame = &multipart[2];
            let payload_frame = &multipart[3];

            // Parse message type
            if type_frame.len() != 1 {
                if let Some(ref m) = metrics {
                    m.record_rejection(velo_observability::TransportRejection::DecodeError);
                }
                warn!("ZMQ: message type frame should be 1 byte, got {}", type_frame.len());
                continue;
            }

            let msg_type = match MessageType::from_u8(type_frame[0]) {
                Some(t) => t,
                None => {
                    if let Some(ref m) = metrics {
                        m.record_rejection(velo_observability::TransportRejection::DecodeError);
                    }
                    warn!("ZMQ: invalid message type byte: {}", type_frame[0]);
                    continue;
                }
            };

            // During drain: reject new Message frames with ShuttingDown
            if draining && msg_type == MessageType::Message {
                if let Some(ref m) = metrics {
                    m.record_rejection(velo_observability::TransportRejection::DrainRejected);
                }
                debug!("ZMQ: rejecting Message frame during drain (sending ShuttingDown)");
                // Send ShuttingDown reply: [identity, type, header, empty_payload]
                let shutdown_type: &[u8] = &[MessageType::ShuttingDown.as_u8()];
                let empty: &[u8] = &[];
                let _ = router
                    .send(identity.as_slice(), zmq::SNDMORE)
                    .and_then(|_| router.send(shutdown_type, zmq::SNDMORE))
                    .and_then(|_| router.send(header_frame.as_slice(), zmq::SNDMORE))
                    .and_then(|_| router.send(empty, 0));
                continue;
            }

            // Convert to Bytes (single copy from ZMQ buffer → Bytes)
            let header = Bytes::copy_from_slice(header_frame);
            let payload = Bytes::copy_from_slice(payload_frame);

            // Record metrics
            if let Some(ref m) = metrics {
                m.record_frame(
                    velo_observability::Direction::Inbound,
                    crate::message_type_label(msg_type),
                    header.len() + payload.len(),
                );
            }

            // Route to appropriate adapter stream
            let sender = match msg_type {
                MessageType::Message => &adapter.message_stream,
                MessageType::Response => &adapter.response_stream,
                MessageType::Ack | MessageType::Event => &adapter.event_stream,
                MessageType::ShuttingDown => &adapter.response_stream,
            };

            if let Err(e) = sender.try_send((header, payload)) {
                if let Some(ref m) = metrics {
                    m.record_rejection(velo_observability::TransportRejection::RouteFailed);
                }
                warn!("ZMQ: failed to route {:?} frame: {:?}", msg_type, e);
            }
        }
    }

    debug!("ZMQ listener thread exited");
}
