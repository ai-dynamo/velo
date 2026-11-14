// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Typed exclusive-attachment streaming abstraction over the velo transport.
//!
//! Core wire types:
//! - [`handle::StreamAnchorHandle`]: compact u128 encoding WorkerId + local anchor ID
//! - [`frame::StreamFrame`]: seven-variant enum representing all frame types on the wire
//!
//! Transports: a stream rides the messenger mux by default. A node that asks
//! for it also runs a per-stream transport ([`TcpFrameTransport`] or
//! `GrpcFrameTransport`). Both implement a crate-private frame transport
//! trait; a new transport implements the messenger
//! [`Transport`](crate::Transport) instead.
//!
//! Anchors:
//! - [`anchor::StreamAnchor`]: typed receive stream for anchor consumers
//! - [`anchor::AttachError`]: errors for exclusive-attach operations
//! - [`Velo::prebind_anchor`](crate::Velo::prebind_anchor) /
//!   [`Velo::open_anchor_stream`](crate::Velo::open_anchor_stream): zero-RTT
//!   stream setup — the receiver mints the terms as a
//!   [`control::StreamOpenTicket`] and the sender opens on them, with no
//!   `_anchor_attach` round trip
//!
//! Sender:
//! - [`sender::StreamSender`]: typed sender for pushing frames with heartbeat and drop safety

pub mod anchor;
pub mod control;
pub(crate) mod deadline;
pub mod frame;
#[cfg(feature = "grpc")]
pub mod grpc_transport;
pub mod handle;
pub(crate) mod heartbeat;
/// Batched, multiplexed streaming over the Messenger (`messenger-mux-v2`).
///
/// The transport itself is internal — it is on by default and selected when a
/// stream's terms are decided, at attach or at pre-bind for zero-RTT setup, so
/// nothing outside this crate constructs or names it. What is re-exported
/// below is only what a caller must be able to say: how to configure it, and
/// the key it answers to, which is what
/// [`StreamSender::negotiated_transport`] is compared against.
pub(crate) mod messenger_mux;
pub mod mpsc;
pub(crate) mod negotiation;
pub(crate) mod peer_table;
pub mod sender;
pub(crate) mod sweep;
mod tasks;
pub mod tcp_transport;
pub(crate) mod transport;

pub use anchor::{
    AnchorConfig, AnchorReservation, AttachError, ReservationConfig, StreamAnchor, StreamController,
};
pub(crate) use anchor::{AnchorManager, AnchorManagerBuilder};
pub use frame::{ByteItemBuilder, ByteItemFrame, SendError, StreamError, StreamFrame};
#[cfg(feature = "grpc")]
pub use grpc_transport::GrpcFrameTransport;
pub use handle::{AnchorKind, StreamAnchorHandle};
pub use messenger_mux::{AutoFlush, FlushPolicy, MESSENGER_MUX_KEY, MuxConfig};
pub use mpsc::{
    MpscAnchorConfig, MpscFrame, MpscStreamAnchor, MpscStreamController, MpscStreamSender, SenderId,
};
pub use sender::{Retired, Retirement, StreamSender};
pub use tcp_transport::TcpFrameTransport;
pub(crate) use transport::FrameTransport;
pub use transport::MAX_ITEM_BYTES;
