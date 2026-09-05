// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! [`StreamOpenTicket`]: the terms a stream opens on, minted ahead of time.
//!
//! Split out of `control.rs` because it is a pure value type with no
//! dependency on the pump or the attach handlers that live there — the only
//! things this module touches are the wire types it mirrors
//! ([`super::AnchorAttachResponse`]) and the flow-control limits it is minted
//! from.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// The terms a stream opens on, carried to the sender instead of asked for.
///
/// Exactly the five fields of [`super::AnchorAttachResponse::Ok`], because it
/// is the same answer arriving by a different road: the receiver decides them
/// all without needing anything from the sender, so nothing obliges it to
/// wait to be asked. An application that already sends the worker a request
/// envelope puts a ticket in it, the worker opens its sender locally through
/// [`AnchorManager::open_anchor_stream`](crate::streaming::AnchorManager::open_anchor_stream),
/// and the `_anchor_attach` round trip that used to precede the first token
/// does not happen.
///
/// A separate type rather than the response variant reused. The two shapes are
/// identical today and are free to diverge: the response is a reply whose
/// compatibility is owed to every peer that sends an attach, while a ticket is
/// only ever read by a peer new enough to have been sent one. Tying them
/// together would buy nothing and cost that freedom — and, decisively,
/// [`super::AnchorAttachRequest`] and [`super::AnchorAttachResponse`] gain and
/// lose no field for any of this.
///
/// The receiver mints one through [`StreamOpenTicket::from_limits`], which takes
/// the window as a `NegotiatedLimits` rather than as two integers. That keeps
/// the two asymmetric zeros — `initial_credit = 0` means *not offering the mux*,
/// `slot_byte_budget = 0` means *use the default* — decided in the single place
/// that already decides them, so a minted ticket can never quote a window the
/// receiver did not size a buffer for.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamOpenTicket {
    /// The transport the sender must open on, as
    /// [`super::AnchorAttachResponse::Ok::streaming_transport_key`].
    pub streaming_transport_key: velo_ext::TransportKey,
    /// The cadence the sender must beat at, as
    /// [`super::AnchorAttachResponse::Ok::heartbeat_interval_ms`].
    #[serde(default = "super::default_heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u64,
    /// The receiver-allocated routing slot this stream owns, as
    /// [`super::AnchorAttachResponse::Ok::routing_session_id`].
    ///
    /// No `#[serde(default)]`: unlike the response this mirrors, a ticket has
    /// no older sender that predates this field (its own doc says so above),
    /// so one missing it is a corrupt envelope, not a legacy peer, and must
    /// fail to decode rather than silently open against session id 0.
    pub routing_session_id: u64,
    /// Data credit the slot opens holding. Never zero on a minted ticket: zero
    /// is the wire encoding of *not offering the mux*, and a receiver with no
    /// mux mints no ticket at all.
    ///
    /// No `#[serde(default)]`, for the same reason as `routing_session_id`: a
    /// missing value is corruption, and defaulting it to zero would silently
    /// read as "not offering the mux" instead of failing to decode.
    pub initial_credit: u32,
    /// Bytes one slot may hold in flight; zero means the default.
    ///
    /// No `#[serde(default)]`, for the same reason as `routing_session_id`.
    pub slot_byte_budget: u32,
}

impl StreamOpenTicket {
    /// Mint a ticket for a slot this node has already pre-bound.
    pub(crate) fn from_limits(
        streaming_transport_key: velo_ext::TransportKey,
        heartbeat_interval: Duration,
        routing_session_id: u64,
        limits: crate::streaming::messenger_mux::flow_control::NegotiatedLimits,
    ) -> Self {
        Self {
            streaming_transport_key,
            heartbeat_interval_ms: heartbeat_interval.as_millis() as u64,
            routing_session_id,
            initial_credit: limits.initial_credit(),
            slot_byte_budget: limits.slot_byte_budget(),
        }
    }
}
