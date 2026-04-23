// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Multi-producer / single-consumer streaming anchors.
//!
//! Unlike the default [`crate::StreamAnchor`], an `MpscStreamAnchor` accepts
//! frames from *many* senders. Each sender is assigned a [`SenderId`] at attach
//! time, and every frame yielded by the anchor is tagged with the originating
//! `SenderId`. Individual sender lifecycle events (`Detached`, `Dropped`) are
//! non-terminal for the anchor: the stream keeps running until the consumer
//! explicitly cancels it via [`MpscStreamController::cancel`] or drops the
//! anchor, or until every sender has gone and the unattached timeout fires.
//!
//! Senders cannot finalize an MPSC anchor; finalization is owned by the
//! consumer side.

pub mod anchor;
pub mod control;
pub mod frame;
pub mod sender;
pub mod types;

pub use anchor::{MpscStreamAnchor, MpscStreamController};
pub use control::{
    MpscAnchorAttachRequest, MpscAnchorAttachResponse, MpscAnchorCancelRequest,
    MpscAnchorDetachRequest, create_mpsc_anchor_attach_handler, create_mpsc_anchor_cancel_handler,
    create_mpsc_anchor_detach_handler,
};
pub use frame::MpscFrame;
pub use sender::MpscStreamSender;
pub use types::{MpscAnchorConfig, SenderId};
