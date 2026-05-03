// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! gRPC server implementation for the VeloStreaming service.
//!
//! This module implements the tonic server-side handler for bidirectional
//! streaming. Inbound `FramedData` messages are parsed using `TcpFrameCodec`
//! and routed to the appropriate adapter channel based on message type.

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, warn};

use crate::observability::{Direction, TransportRejection};

use crate::transports::tcp::TcpFrameCodec;
use crate::transports::transport::{ShutdownState, TransportAdapter};
use velo_ext::MessageType;

use super::proto;
use super::proto::velo_streaming_server::VeloStreaming;

/// Tonic service implementation for bidirectional gRPC streaming.
///
/// Each inbound `Stream` RPC opens a bidirectional channel. Inbound frames
/// are decoded, routed through the [`TransportAdapter`], and drain-aware
/// rejection is handled by sending `ShuttingDown` frames back on the
/// response stream.
pub struct VeloStreamingService {
    adapter: TransportAdapter,
    shutdown_state: ShutdownState,
    transport_key: String,
    metrics: Option<std::sync::Arc<dyn velo_ext::TransportObservability>>,
}

impl VeloStreamingService {
    /// Create a new service instance with the given adapter and shutdown state.
    pub fn new(
        adapter: TransportAdapter,
        shutdown_state: ShutdownState,
        transport_key: String,
        metrics: Option<std::sync::Arc<dyn velo_ext::TransportObservability>>,
    ) -> Self {
        Self {
            adapter,
            shutdown_state,
            transport_key,
            metrics,
        }
    }
}

#[tonic::async_trait]
impl VeloStreaming for VeloStreamingService {
    type StreamStream = ReceiverStream<Result<proto::FramedData, Status>>;

    async fn stream(
        &self,
        request: Request<Streaming<proto::FramedData>>,
    ) -> Result<Response<Self::StreamStream>, Status> {
        let mut inbound = request.into_inner();
        let adapter = self.adapter.clone();
        let shutdown_state = self.shutdown_state.clone();
        let transport_key = self.transport_key.clone();
        let metrics = self.metrics.clone();
        #[cfg(not(feature = "distributed-tracing"))]
        let _ = &transport_key;

        // Response channel for sending frames back to the client (e.g. ShuttingDown).
        let (response_tx, response_rx) = mpsc::channel::<Result<proto::FramedData, Status>>(256);

        tokio::spawn(async move {
            while let Ok(Some(framed_data)) = inbound.message().await {
                let msg_type =
                    match TcpFrameCodec::parse_message_type_from_preamble(&framed_data.preamble) {
                        Ok(mt) => mt,
                        Err(e) => {
                            if let Some(metrics) = metrics.as_ref() {
                                metrics.record_rejection(TransportRejection::DecodeError);
                            }
                            warn!("gRPC server: invalid preamble: {}", e);
                            continue;
                        }
                    };

                // During drain: reject new Message frames with ShuttingDown,
                // but always pass through Response/Ack/Event frames.
                if shutdown_state.is_draining() && msg_type == MessageType::Message {
                    if let Some(metrics) = metrics.as_ref() {
                        metrics.record_rejection(TransportRejection::DrainRejected);
                    }
                    debug!("gRPC server: rejecting Message during drain (sending ShuttingDown)");
                    let preamble = match TcpFrameCodec::build_preamble(
                        MessageType::ShuttingDown,
                        framed_data.header.len() as u32,
                        0,
                    ) {
                        Ok(p) => p,
                        Err(e) => {
                            if let Some(metrics) = metrics.as_ref() {
                                metrics.record_rejection(TransportRejection::DrainReplyBuildFailed);
                            }
                            warn!("gRPC server: failed to build ShuttingDown preamble: {}", e);
                            continue;
                        }
                    };
                    let reject = proto::FramedData {
                        preamble: preamble.to_vec(),
                        header: framed_data.header,
                        payload: Vec::new(),
                    };
                    if response_tx.send(Ok(reject)).await.is_err() {
                        break;
                    }
                    continue;
                }

                // Route to the appropriate adapter channel.
                let sender = match msg_type {
                    MessageType::Message => &adapter.message_stream,
                    MessageType::Response => &adapter.response_stream,
                    MessageType::Ack | MessageType::Event => &adapter.event_stream,
                    MessageType::ShuttingDown => &adapter.response_stream,
                };

                if let Some(metrics) = metrics.as_ref() {
                    #[cfg(feature = "distributed-tracing")]
                    let span = tracing::debug_span!(
                        "velo.transport.receive",
                        transport = transport_key.as_str(),
                        message_type = crate::transports::message_type_label(msg_type),
                        bytes = framed_data.header.len() + framed_data.payload.len()
                    );
                    #[cfg(feature = "distributed-tracing")]
                    let _entered = span.enter();

                    metrics.record_frame(
                        Direction::Inbound,
                        crate::transports::message_type_label(msg_type),
                        framed_data.header.len() + framed_data.payload.len(),
                    );
                }

                if let Err(e) = sender
                    .send_async((
                        Bytes::from(framed_data.header),
                        Bytes::from(framed_data.payload),
                    ))
                    .await
                {
                    if let Some(metrics) = metrics.as_ref() {
                        metrics.record_rejection(TransportRejection::RouteFailed);
                    }
                    warn!("gRPC server: failed to route {:?} frame: {}", msg_type, e);
                    break;
                }
            }

            debug!("gRPC server: inbound stream ended");
        });

        Ok(Response::new(ReceiverStream::new(response_rx)))
    }
}
