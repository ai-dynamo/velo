// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! [`FrameTransport`] is defined in [`velo_ext::streaming`] and re-exported
//! here so existing imports continue to compile during the workspace
//! migration to the two-crate layout.

pub use velo_ext::streaming::FrameTransport;

/// The largest encoded item the per-stream transports of this crate carry in
/// one frame: the TCP streaming codec's frame limit, 16 MiB, which the gRPC
/// transport adopts so that one limit holds on every per-stream path. A
/// `StreamSender` on such a transport refuses a larger item at `send`
/// (`SendError::ItemTooLarge`) when the `Velo` builder installed the
/// transport; over an `AnchorManager` built by hand, the item reaches the
/// codec, which fails the stream. The gRPC server decodes, and its client
/// encodes, with this value plus the message envelope.
pub const MAX_ITEM_BYTES: usize = crate::transports::tcp::framing::DEFAULT_MAX_FRAME_SIZE as usize;

/// A frame transport of this crate that reports a stream's retirement itself:
/// `Sent` once its pump has written the terminal, `Discarded` when the write
/// fails or the connection ends first. A transport that is only a
/// [`FrameTransport`] cannot say when a frame left, so its sender reports
/// `Sent` when the terminal enters the egress channel.
pub(crate) trait RetiringFrameTransport: FrameTransport {
    /// As [`FrameTransport::connect`], with the stream's retirement for the
    /// pump to report through.
    fn connect_retiring(
        &self,
        peer: velo_ext::WorkerId,
        anchor_id: u64,
        session_id: u64,
        closed: crate::streaming::control::SenderClosed,
    ) -> futures::future::BoxFuture<'_, anyhow::Result<flume::Sender<Vec<u8>>>>;

    /// The largest encoded item this transport carries in one frame, when
    /// the producer should refuse a larger one at `send` rather than let the
    /// transport fail the stream.
    fn item_limit(&self) -> Option<usize> {
        None
    }
}
