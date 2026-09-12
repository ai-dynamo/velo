// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Typed exclusive-attachment streaming abstraction over the velo transport.
//!
//! Core wire types:
//! - [`handle::StreamAnchorHandle`]: compact u128 encoding WorkerId + local anchor ID
//! - [`frame::StreamFrame`]: seven-variant enum representing all frame types on the wire
//!
//! Transport abstraction:
//! - [`transport::FrameTransport`]: pluggable ordered-delivery transport trait
//!   returning [`flume::Receiver<Vec<u8>>`] and [`flume::Sender<Vec<u8>>`]
//!   channel endpoints via [`futures::future::BoxFuture`]
//!
//! Anchor registry:
//! - [`anchor::AnchorManager`]: creates and tracks streaming anchors
//! - [`anchor::StreamAnchor`]: typed receive stream for anchor consumers
//! - [`anchor::AttachError`]: errors for exclusive-attach operations
//! - [`anchor::AnchorManager::prebind_anchor`] /
//!   [`anchor::AnchorManager::open_anchor_stream`]: zero-RTT stream setup — the
//!   receiver mints the terms as a [`control::StreamOpenTicket`] and the sender
//!   opens on them, with no `_anchor_attach` round trip
//!
//! Sender:
//! - [`sender::StreamSender`]: typed sender for pushing frames with heartbeat and drop safety

pub mod anchor;
pub mod control;
pub mod frame;
#[cfg(feature = "grpc")]
pub mod grpc_transport;
pub mod handle;
/// Batched, multiplexed streaming over the Messenger (`messenger-mux-v1`).
///
/// The transport itself is internal — it is opt-in and selected when a
/// stream's terms are decided, at attach or at pre-bind for zero-RTT setup, so
/// nothing outside this crate constructs or names it. What is re-exported
/// below is only what a caller must be able to say: how to configure it, and
/// the key it answers to, which is what
/// [`StreamSender::negotiated_transport`] is compared against.
pub(crate) mod messenger_mux;
pub mod mpsc;
pub(crate) mod negotiation;
pub mod sender;
pub mod tcp_transport;
pub mod transport;

pub use anchor::{
    AnchorConfig, AnchorManager, AnchorManagerBuilder, AttachError, StreamAnchor, StreamController,
};
pub use frame::{SendError, StreamError, StreamFrame};
#[cfg(feature = "grpc")]
pub use grpc_transport::GrpcFrameTransport;
pub use handle::{AnchorKind, StreamAnchorHandle};
pub use messenger_mux::{AutoFlush, FlushPolicy, MESSENGER_MUX_KEY, MuxConfig};
pub use mpsc::{
    MpscAnchorConfig, MpscFrame, MpscStreamAnchor, MpscStreamController, MpscStreamSender, SenderId,
};
pub use sender::StreamSender;
pub use tcp_transport::TcpFrameTransport;
pub use transport::FrameTransport;
