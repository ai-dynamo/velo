// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Which streaming transport an attach settles on, and on what terms.
//!
//! There is no wire magic and no first-bytes handshake to hide one in: the
//! attach exchange already names a transport, so negotiation is three small
//! decisions layered onto it.
//!
//! 1. The sender lists what it can be asked to `connect()` on
//!    ([`advertised_keys`]).
//! 2. The receiver intersects that with what it has installed and answers with
//!    one key ([`select`]), plus the credit window if that key is the mux.
//! 3. The sender decides what the answer obliges it to do ([`choose`]).
//!
//! Every asymmetry here exists to keep a mixed deployment working. The receiver
//! prefers `messenger-mux-v1` **only** when the sender named it, because
//! `resolve_transport` hard-errors on a key it does not know and a receiver that
//! answered the mux unilaterally would break every older sender. A node with the
//! mux enabled therefore registers both it and the configured legacy transport,
//! and keeps serving legacy peers unchanged.
//!
//! The credit fields carry the rest of the agreement, and their two zeros mean
//! different things: no window means *not offering the mux*, no byte cap means
//! *use the default*. [`NegotiatedLimits::from_wire`] is the one place that
//! split is decided; nothing here re-derives it.

use std::collections::HashMap;
use std::sync::Arc;

use velo_ext::TransportKey;

use super::messenger_mux::flow_control::{NegotiatedLimits, NegotiationError};
use super::messenger_mux::{MESSENGER_MUX_KEY, MessengerMuxTransport};
use super::transport::FrameTransport;

/// The streaming-transport registry an [`AnchorManager`] resolves keys against.
///
/// [`AnchorManager`]: crate::streaming::AnchorManager
pub(crate) type TransportRegistry = HashMap<String, Arc<dyn FrameTransport>>;

/// The transports a sender advertises on its attach request.
///
/// Exactly what this node can be asked to `connect()` on: the registry's keys,
/// or the default transport's key when no registry was populated (the
/// convenience path `resolve_transport` mirrors). The mux is unioned in
/// explicitly so "installed" and "advertised" cannot drift apart — that
/// equivalence is what makes disabling the mux a complete rollback, since a key
/// never advertised is a key never selected.
///
/// Sorted so the advertisement is stable across runs; `HashMap` iteration order
/// is not.
pub(crate) fn advertised_keys(
    registry: &TransportRegistry,
    default_transport: &Arc<dyn FrameTransport>,
    mux: Option<&Arc<MessengerMuxTransport>>,
) -> Vec<TransportKey> {
    let mut keys: Vec<String> = if registry.is_empty() {
        vec![default_transport.key().as_str().to_string()]
    } else {
        registry.keys().cloned().collect()
    };
    if mux.is_some() && !keys.iter().any(|key| key == MESSENGER_MUX_KEY) {
        keys.push(MESSENGER_MUX_KEY.to_string());
    }
    keys.sort_unstable();
    keys.iter()
        .map(|key| TransportKey::new(key.as_str()))
        .collect()
}

/// What the receiver picked for one attach, and what it owes the sender.
pub(crate) struct Selection {
    /// The transport to `bind()` on.
    pub(crate) transport: Arc<dyn FrameTransport>,
    /// The key to answer with, which the sender resolves on its side.
    pub(crate) key: TransportKey,
    /// Data credit granted to each mux slot. Zero unless the mux was selected —
    /// the wire encoding of *not offering the mux*.
    pub(crate) initial_credit: u32,
    /// Bytes one mux slot may hold. Zero unless the mux was selected, and zero
    /// on the wire means *use the default*, so the two never collide.
    pub(crate) slot_byte_budget: u32,
}

/// Intersect the sender's advertisement with what is installed here.
///
/// The mux wins only when both sides named it; everything else falls through to
/// the behaviour that shipped before negotiation — answer with the local default
/// transport's key. An empty `offered` (an older sender, which omits the field
/// entirely) cannot intersect, so such a sender always takes that path.
pub(crate) fn select(
    offered: &[TransportKey],
    mux: Option<&Arc<MessengerMuxTransport>>,
    default_transport: &Arc<dyn FrameTransport>,
) -> Selection {
    if let Some(mux) = mux
        && offered.iter().any(|key| key.as_str() == MESSENGER_MUX_KEY)
    {
        let limits = mux.advertised_limits();
        return Selection {
            transport: Arc::clone(mux) as Arc<dyn FrameTransport>,
            key: TransportKey::new(MESSENGER_MUX_KEY),
            initial_credit: limits.initial_credit(),
            slot_byte_budget: limits.slot_byte_budget(),
        };
    }
    Selection {
        transport: Arc::clone(default_transport),
        key: default_transport.key(),
        initial_credit: 0,
        slot_byte_budget: 0,
    }
}

/// How the sender must honour the key the receiver answered with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Connect {
    /// Open a mux slot already holding the negotiated window.
    Mux(NegotiatedLimits),
    /// Resolve the key in the local registry and `connect()` on it, exactly as
    /// before negotiation existed.
    Legacy,
}

/// Read the receiver's answer.
///
/// A key that is not the mux is the legacy path and the credit fields are not
/// its business — an older receiver names its own transport and sends no credit
/// at all, which is the ordinary mixed-deployment case and lands here.
///
/// The mux key with no window is the case that cannot be honoured. It is
/// unreachable from any shipped peer: no version before this one answers
/// `messenger-mux-v1`, and this one refuses to build a mux at zero credit. So it
/// means a peer that bound a mux receiver and then told us to ignore it, and
/// there is no safe reading of that — connecting over any other transport would
/// reach nothing the peer is listening on and hang until the anchor's watchdog
/// fires. Failing the attach says so immediately.
pub(crate) fn choose(
    key: &TransportKey,
    initial_credit: u32,
    slot_byte_budget: u32,
) -> Result<Connect, NegotiationError> {
    if key.as_str() != MESSENGER_MUX_KEY {
        return Ok(Connect::Legacy);
    }
    NegotiatedLimits::from_wire(initial_credit, slot_byte_budget).map(Connect::Mux)
}

#[cfg(test)]
mod tests;
