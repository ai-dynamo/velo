// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Inbound frame routing shared by the messenger TCP/UDS listeners and the
//! read half of their dialed connections.
//!
//! Accepted sockets have always been read — that is what the listeners do.
//! Dialed sockets used to be write-only: the connection writer pushed frames
//! at the peer and nothing ever read the reverse direction. But the peer's
//! listener *does* write one thing back on that socket — the ShuttingDown
//! correlation reply it sends when it rejects a `Message` during drain.
//! Unread, those replies rot in the dialing side's kernel receive buffer and
//! the sender never learns its message was dropped.
//!
//! [`run_dialed_reader`] is the missing read half: it decodes frames off a
//! dialed socket and routes them through the same [`route_frame`] path the
//! listeners use, so a drain rejection lands on the sender's shutdown stream
//! where the messenger correlates it. It changes nothing on the wire — it
//! only reads bytes the peer was already sending.

use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use futures::StreamExt;
use tokio::io::AsyncRead;
use tokio_util::codec::FramedRead;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use velo_ext::{AdmitOutcome, MessageType, TransportAdapter, TransportErrorHandler};

use crate::observability::{Direction, TransportRejection};

use super::tcp::framing::{TcpFrameCodec, maybe_shrink_read_buffer};

/// What [`route_frame`] did with a frame.
#[derive(Debug)]
pub(crate) enum Routed {
    /// The frame reached its stream.
    Delivered,
    /// An inbound `Message` arrived while this instance is draining, so it was
    /// not enqueued. The caller owes the peer a `ShuttingDown` frame echoing
    /// this header — it is the only side that knows how to write one (an
    /// accepted socket writes back on itself; a dialed reader has no reply
    /// path at all and drops it).
    DrainRejected { header: Bytes },
}

/// Record an inbound frame against the transport's metrics.
///
/// Only ever called for frames that were actually delivered: a drain-rejected
/// `Message` counts as a `drain_rejected` rejection, not as inbound traffic.
#[inline]
fn record_inbound_frame(
    metrics: Option<&Arc<dyn velo_ext::TransportObservability>>,
    transport_key: &str,
    msg_type: MessageType,
    frame_bytes: usize,
) {
    #[cfg(not(feature = "distributed-tracing"))]
    let _ = transport_key;

    if let Some(metrics) = metrics {
        #[cfg(feature = "distributed-tracing")]
        let span = tracing::debug_span!(
            "velo.transport.receive",
            transport = transport_key,
            message_type = crate::transports::message_type_label(msg_type),
            bytes = frame_bytes
        );
        #[cfg(feature = "distributed-tracing")]
        let _entered = span.enter();

        metrics.record_frame(
            Direction::Inbound,
            crate::transports::message_type_label(msg_type),
            frame_bytes,
        );
    }
}

/// Route a decoded frame to the appropriate stream
///
/// This function performs zero-copy routing by transferring ownership of
/// the Bytes to the flume channel. On error, it invokes the error callback
/// with the original data (requiring a clone).
///
/// Inbound `Message` frames take the admission path
/// ([`TransportAdapter::admit_message`]) rather than a raw send: it is both
/// the drain gate and the point where the in-flight guard is acquired, so a
/// message that is merely *queued* is already work `wait_for_drain` can see.
/// Every other frame type — responses, acks, events, and drain echoes from a
/// peer — must keep flowing while this side drains and is sent directly.
pub(crate) async fn route_frame(
    msg_type: MessageType,
    header: Bytes,
    payload: Bytes,
    adapter: &TransportAdapter,
    error_handler: &Arc<dyn TransportErrorHandler>,
    transport_key: &str,
    metrics: Option<&Arc<dyn velo_ext::TransportObservability>>,
) -> Result<Routed> {
    let frame_bytes = header.len() + payload.len();

    let sender = match msg_type {
        MessageType::Message => {
            return match adapter.admit_message(header, payload) {
                AdmitOutcome::Admitted => {
                    record_inbound_frame(metrics, transport_key, msg_type, frame_bytes);
                    Ok(Routed::Delivered)
                }
                AdmitOutcome::Draining { header, .. } => {
                    if let Some(metrics) = metrics {
                        metrics.record_rejection(TransportRejection::DrainRejected);
                    }
                    Ok(Routed::DrainRejected { header })
                }
                AdmitOutcome::Disconnected { header, payload } => {
                    if let Some(metrics) = metrics {
                        metrics.record_rejection(TransportRejection::RouteFailed);
                    }
                    error_handler.on_error(header, payload, "Failed to route Message".to_string());
                    Err(anyhow::anyhow!("Failed to send to stream"))
                }
            };
        }
        MessageType::Response => &adapter.response_stream,
        MessageType::Ack | MessageType::Event => &adapter.event_stream,
        MessageType::ShuttingDown => {
            // A remote peer rejected our request during its drain. The frame
            // carries our request header echoed back, so higher layers can
            // correlate it; it gets its own lane because that header is
            // request-format, not response-format.
            &adapter.shutdown_stream
        }
    };

    record_inbound_frame(metrics, transport_key, msg_type, frame_bytes);

    // Try to send with ownership transfer (zero-copy)
    match sender.send_async((header, payload)).await {
        Ok(_) => Ok(Routed::Delivered),
        Err(e) => {
            if let Some(metrics) = metrics {
                metrics.record_rejection(TransportRejection::RouteFailed);
            }
            // Send failed - invoke error callback with the data
            error_handler.on_error(
                e.0.0, // header
                e.0.1, // payload
                format!("Failed to route {:?}", msg_type),
            );
            Err(anyhow::anyhow!("Failed to send to stream"))
        }
    }
}

/// Everything a dialed connection's read loop needs besides the socket.
///
/// Built once per transport in `start()` (that is when the adapter exists) and
/// cloned into each connection writer task.
#[derive(Clone)]
pub(crate) struct DialedReaderContext {
    pub adapter: TransportAdapter,
    pub error_handler: Arc<dyn TransportErrorHandler>,
    pub transport_key: String,
    pub shrink_threshold: usize,
}

/// Read frames off the read half of a dialed connection until the peer closes
/// it, a frame fails to decode, or `conn_cancel` fires.
///
/// On exit it cancels `conn_cancel` — a socket whose read side is finished is
/// a socket the peer has abandoned, so the connection's writer (which selects
/// on the same token) should stop instead of pushing frames at it until a
/// write finally errors.
///
/// Frames arriving on a dialed socket are normally the peer's replies to our
/// own sends (ShuttingDown correlations), not new inbound work, and those flow
/// even while this side is draining. A `Message` on this path is anomalous —
/// nothing dials a socket to serve requests on it — but it is not impossible,
/// so it goes through the same admission gate as everything else. There is no
/// per-connection reply plumbing here, so a drain rejection is dropped after
/// recording it, and the peer waits out its own timeout; the alternative,
/// letting it in ungated, would put uncounted work on the inbound queue.
pub(crate) async fn run_dialed_reader<R>(
    read_half: R,
    ctx: DialedReaderContext,
    metrics: Option<Arc<dyn velo_ext::TransportObservability>>,
    conn_cancel: CancellationToken,
    peer: String,
) where
    R: AsyncRead + Unpin,
{
    let mut framed = FramedRead::new(read_half, TcpFrameCodec::new());

    loop {
        tokio::select! {
            // Prioritize cancellation so a saturated connection cannot starve
            // shutdown.
            biased;
            _ = conn_cancel.cancelled() => break,
            frame_result = framed.next() => {
                match frame_result {
                    Some(Ok((msg_type, header, payload))) => {
                        let frame_size = header.len() + payload.len();
                        match route_frame(
                            msg_type,
                            header,
                            payload,
                            &ctx.adapter,
                            &ctx.error_handler,
                            &ctx.transport_key,
                            metrics.as_ref(),
                        )
                        .await
                        {
                            Ok(Routed::Delivered) => {}
                            Ok(Routed::DrainRejected { .. }) => {
                                // No reply path on a dialed socket — the
                                // rejection is already recorded, so drop it.
                                debug!(
                                    "Dropped inbound Message from dialed connection to {} during drain",
                                    peer
                                );
                            }
                            Err(e) => {
                                warn!(
                                    "Failed to route {:?} frame from dialed connection to {}: {}",
                                    msg_type, peer, e
                                );
                            }
                        }
                        maybe_shrink_read_buffer(
                            framed.read_buffer_mut(),
                            ctx.shrink_threshold,
                            frame_size,
                        );
                    }
                    Some(Err(e)) => {
                        if let Some(metrics) = metrics.as_ref() {
                            metrics.record_rejection(TransportRejection::DecodeError);
                        }
                        warn!("Frame decode error on dialed connection to {}: {}", peer, e);
                        break;
                    }
                    None => {
                        debug!("Dialed connection to {} closed by peer", peer);
                        break;
                    }
                }
            }
        }
    }

    conn_cancel.cancel();
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex;

    use velo_ext::make_channels;

    use super::super::tcp::framing::{DEFAULT_SHRINK_THRESHOLD, MIN_HEADER_SIZE};

    /// Records what a transport's error handler was asked to report, so a test
    /// can tell "dropped after recording a rejection" from "reported to the
    /// transport as an undeliverable frame".
    #[derive(Default)]
    struct RecordingErrorHandler {
        calls: Mutex<Vec<(Bytes, Bytes, String)>>,
    }

    impl RecordingErrorHandler {
        fn calls(&self) -> Vec<(Bytes, Bytes, String)> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl TransportErrorHandler for RecordingErrorHandler {
        fn on_error(&self, header: Bytes, payload: Bytes, error: String) {
            self.calls.lock().unwrap().push((header, payload, error));
        }
    }

    /// Append one wire frame in the TCP/UDS framing the dialed reader decodes.
    fn push_frame(wire: &mut Vec<u8>, msg_type: MessageType, header: &[u8], payload: &[u8]) {
        let preamble =
            TcpFrameCodec::build_preamble(msg_type, header.len() as u32, payload.len() as u32)
                .expect("preamble");
        assert_eq!(preamble.len(), MIN_HEADER_SIZE);
        wire.extend_from_slice(&preamble);
        wire.extend_from_slice(header);
        wire.extend_from_slice(payload);
    }

    /// A `Message` refused by the drain gate comes back to the caller with its
    /// header intact — that header is what the peer needs echoed to correlate
    /// the `ShuttingDown` reply — and it is not a transport error.
    #[tokio::test]
    async fn route_frame_hands_back_a_message_rejected_during_drain() {
        let (adapter, streams) = make_channels();
        adapter.shutdown_state.begin_drain();

        let recorder = Arc::new(RecordingErrorHandler::default());
        let error_handler: Arc<dyn TransportErrorHandler> = recorder.clone();

        let routed = route_frame(
            MessageType::Message,
            Bytes::from_static(b"drain-header"),
            Bytes::from_static(b"drain-payload"),
            &adapter,
            &error_handler,
            "test",
            None,
        )
        .await
        .expect("a drain rejection is not a routing failure");

        match routed {
            Routed::DrainRejected { header } => assert_eq!(&header[..], b"drain-header"),
            Routed::Delivered => panic!("a draining instance must not enqueue a Message"),
        }
        assert!(
            streams.message_stream.is_empty(),
            "a rejected message must not reach the inbound queue"
        );
        assert_eq!(
            adapter.shutdown_state.in_flight_count(),
            0,
            "the admission probe guard must not outlive the rejection"
        );
        assert!(
            recorder.calls().is_empty(),
            "a drain rejection is the peer's problem, not a transport error"
        );
    }

    /// A `Message` with no receiver left is reported to the transport's error
    /// handler with both halves of the frame, and releases the guard
    /// `admit_message` acquired to make the decision.
    #[tokio::test]
    async fn route_frame_reports_a_message_with_no_receiver_left() {
        let (adapter, streams) = make_channels();
        drop(streams);

        let recorder = Arc::new(RecordingErrorHandler::default());
        let error_handler: Arc<dyn TransportErrorHandler> = recorder.clone();

        route_frame(
            MessageType::Message,
            Bytes::from_static(b"orphan-header"),
            Bytes::from_static(b"orphan-payload"),
            &adapter,
            &error_handler,
            "test",
            None,
        )
        .await
        .expect_err("an undeliverable Message must surface as a routing failure");

        let calls = recorder.calls();
        assert_eq!(calls.len(), 1, "the frame must be reported exactly once");
        assert_eq!(&calls[0].0[..], b"orphan-header");
        assert_eq!(&calls[0].1[..], b"orphan-payload");
        assert_eq!(
            adapter.shutdown_state.in_flight_count(),
            0,
            "a frame nobody can deliver must not strand the drain"
        );
    }

    /// A `Message` on a dialed socket during drain is anomalous *and*
    /// rejectable: the reader has no reply path, so it drops the frame after
    /// recording it — and, crucially, keeps reading. The drain echoes that are
    /// the whole reason this reader exists arrive on the same socket.
    #[tokio::test]
    async fn dialed_reader_drops_a_drain_rejected_message_and_keeps_reading() {
        let (adapter, streams) = make_channels();
        adapter.shutdown_state.begin_drain();

        let mut wire = Vec::new();
        push_frame(&mut wire, MessageType::Message, b"m-header", b"m-payload");
        push_frame(&mut wire, MessageType::ShuttingDown, b"echo-header", b"");

        let recorder = Arc::new(RecordingErrorHandler::default());
        let conn_cancel = CancellationToken::new();

        run_dialed_reader(
            std::io::Cursor::new(wire),
            DialedReaderContext {
                adapter: adapter.clone(),
                error_handler: recorder.clone(),
                transport_key: "test".to_string(),
                shrink_threshold: DEFAULT_SHRINK_THRESHOLD,
            },
            None,
            conn_cancel.clone(),
            "peer".to_string(),
        )
        .await;

        assert!(
            streams.message_stream.is_empty(),
            "a dialed reader must not put a drain-rejected Message on the inbound queue"
        );
        assert_eq!(
            adapter.shutdown_state.in_flight_count(),
            0,
            "the dropped frame must release the guard it was admitted with"
        );
        assert!(
            recorder.calls().is_empty(),
            "a drain rejection is not an undeliverable frame"
        );

        let (header, _) = streams
            .shutdown_stream
            .try_recv()
            .expect("the reader must keep going past a drain-rejected Message");
        assert_eq!(&header[..], b"echo-header");

        assert!(
            conn_cancel.is_cancelled(),
            "the reader cancels its connection token when the socket ends"
        );
    }
}
