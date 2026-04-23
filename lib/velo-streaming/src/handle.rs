// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! [`StreamAnchorHandle`]: compact u128 wire handle encoding WorkerId + local anchor ID.
//!
//! Wire layout (MSB to LSB): `[worker_id: upper 64 bits][local_id: lower 64 bits]`
//!
//! Serializes via rmp-serde as `{hi: u64, lo: u64}` to avoid the u128-as-bytes
//! MessagePack encoding. See WIRE-03.

use serde::{Deserialize, Serialize};
use velo_common::WorkerId;

const WORKER_SHIFT: u32 = 64;

/// High bit of the 64-bit `local_id` slot: when set, the handle refers to an
/// MPSC anchor. See [`StreamAnchorHandle::pack_mpsc`] and
/// [`StreamAnchorHandle::is_mpsc_stream`].
///
/// A 63-bit MPSC counter permits 9.2 × 10^18 MPSC anchors per worker — not a
/// practical limit. SPSC `local_id`s are constrained to the lower 63 bits.
pub const MPSC_STREAM_BIT: u64 = 1 << 63;

/// Discriminator returned by [`StreamAnchorHandle::kind`].
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum AnchorKind {
    /// Single-producer / single-consumer anchor
    /// (see [`crate::StreamAnchor`]).
    Spsc,
    /// Multi-producer / single-consumer anchor
    /// (see [`crate::mpsc::MpscStreamAnchor`]).
    Mpsc,
}

impl std::fmt::Display for AnchorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AnchorKind::Spsc => f.write_str("spsc"),
            AnchorKind::Mpsc => f.write_str("mpsc"),
        }
    }
}

/// Compact wire handle encoding a [`WorkerId`] (upper 64 bits) and a local anchor ID
/// (lower 64 bits) into a single `u128`.
///
/// Serializes via rmp-serde as a two-field struct `{hi: u64, lo: u64}` — not as raw
/// binary bytes — to guarantee correct round-tripping across msgpack boundaries.
///
/// The high bit of the `local_id` slot ([`MPSC_STREAM_BIT`]) is reserved as a
/// kind discriminator: `0` → SPSC anchor, `1` → MPSC anchor. Use
/// [`StreamAnchorHandle::pack`] for SPSC handles and
/// [`StreamAnchorHandle::pack_mpsc`] for MPSC handles;
/// [`StreamAnchorHandle::is_spsc_stream`] / [`StreamAnchorHandle::is_mpsc_stream`]
/// let the attach entry points fail fast on the wrong kind without wasting an
/// AM round-trip.
///
/// `local_id` of `0` is reserved; [`crate::anchor::AnchorManager`] assigns IDs starting at `1`.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct StreamAnchorHandle(u128);

/// Private wire representation for rmp-serde serialization.
///
/// rmp-serde encodes a raw `u128` as a MessagePack binary blob (`bin8`), which
/// cannot be decoded back to a struct. By delegating to this two-field struct we
/// encode as a fixmap with named fields that round-trip correctly.
#[derive(Serialize, Deserialize)]
struct StreamAnchorHandleWire {
    hi: u64,
    lo: u64,
}

impl StreamAnchorHandle {
    /// Encode a [`WorkerId`] and local SPSC anchor ID into a [`StreamAnchorHandle`].
    ///
    /// `local_id` of `0` is reserved. Callers should start local IDs at `1`.
    ///
    /// # Panics (debug only)
    /// Debug-asserts that the high bit of `local_id` is clear.
    /// [`MPSC_STREAM_BIT`] is reserved for MPSC handles; use
    /// [`pack_mpsc`](Self::pack_mpsc) if you need an MPSC anchor handle.
    pub fn pack(worker_id: WorkerId, local_id: u64) -> Self {
        debug_assert!(
            local_id & MPSC_STREAM_BIT == 0,
            "pack() local_id must fit in 63 bits; MPSC_STREAM_BIT is reserved (use pack_mpsc)"
        );
        let raw = ((worker_id.as_u64() as u128) << WORKER_SHIFT) | (local_id as u128);
        Self(raw)
    }

    /// Encode a [`WorkerId`] and local MPSC anchor ID into a
    /// [`StreamAnchorHandle`], setting the [`MPSC_STREAM_BIT`] discriminator.
    ///
    /// `local_id` is the raw 63-bit counter value; this function ORs in the
    /// kind bit. See [`pack`](Self::pack) for SPSC.
    ///
    /// # Panics (debug only)
    /// Debug-asserts that the high bit of `local_id` is clear so callers do
    /// not double-apply the mask.
    pub fn pack_mpsc(worker_id: WorkerId, local_id: u64) -> Self {
        debug_assert!(
            local_id & MPSC_STREAM_BIT == 0,
            "pack_mpsc() local_id must fit in 63 bits; bit is applied internally"
        );
        let tagged = local_id | MPSC_STREAM_BIT;
        let raw = ((worker_id.as_u64() as u128) << WORKER_SHIFT) | (tagged as u128);
        Self(raw)
    }

    /// Decode the [`WorkerId`] and local anchor ID from this handle.
    ///
    /// The returned `local_id` retains the [`MPSC_STREAM_BIT`] if set — it is
    /// the value used as the key in the respective anchor registry
    /// (SPSC/MPSC registries live in disjoint key ranges thanks to the bit).
    pub fn unpack(&self) -> (WorkerId, u64) {
        let worker_id = WorkerId::from_u64((self.0 >> WORKER_SHIFT) as u64);
        let local_id = self.0 as u64;
        (worker_id, local_id)
    }

    /// Return the raw `u128` value (useful for logging and display).
    pub fn as_u128(&self) -> u128 {
        self.0
    }

    /// Reconstruct a handle from a raw `u128` — the inverse of
    /// [`as_u128`](Self::as_u128). Does *not* assert anything about the
    /// kind bit: use this when re-hydrating a handle received over the wire
    /// as opaque bytes. The kind is recovered via [`kind`](Self::kind) etc.
    pub fn from_u128(raw: u128) -> Self {
        Self(raw)
    }

    /// Return the [`AnchorKind`] encoded in the handle.
    pub fn kind(&self) -> AnchorKind {
        if self.is_mpsc_stream() {
            AnchorKind::Mpsc
        } else {
            AnchorKind::Spsc
        }
    }

    /// `true` iff this handle refers to an MPSC anchor
    /// (the [`MPSC_STREAM_BIT`] is set in the `local_id`).
    pub fn is_mpsc_stream(&self) -> bool {
        (self.0 as u64) & MPSC_STREAM_BIT != 0
    }

    /// `true` iff this handle refers to an SPSC anchor.
    pub fn is_spsc_stream(&self) -> bool {
        !self.is_mpsc_stream()
    }
}

impl Serialize for StreamAnchorHandle {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        StreamAnchorHandleWire {
            hi: (self.0 >> 64) as u64,
            lo: self.0 as u64,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for StreamAnchorHandle {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = StreamAnchorHandleWire::deserialize(deserializer)?;
        Ok(Self(((wire.hi as u128) << 64) | (wire.lo as u128)))
    }
}

impl std::fmt::Display for StreamAnchorHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (worker_id, local_id) = self.unpack();
        write!(
            f,
            "StreamAnchorHandle(worker={}, local={}, kind={})",
            worker_id.as_u64(),
            local_id & !MPSC_STREAM_BIT,
            self.kind()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use velo_common::WorkerId;

    #[test]
    fn test_pack_unpack_roundtrip() {
        let worker_id = WorkerId::from_u64(0xDEAD_BEEF_1234_5678);
        let local_id: u64 = 42;
        let handle = StreamAnchorHandle::pack(worker_id, local_id);
        let (recovered_worker, recovered_local) = handle.unpack();
        assert_eq!(recovered_worker, worker_id);
        assert_eq!(recovered_local, local_id);
    }

    #[test]
    fn test_rmp_serde_roundtrip() {
        let worker_id = WorkerId::from_u64(0xDEAD_BEEF_1234_5678);
        let local_id: u64 = 99;
        let handle = StreamAnchorHandle::pack(worker_id, local_id);

        let encoded = rmp_serde::to_vec(&handle).expect("serialize");
        let decoded: StreamAnchorHandle = rmp_serde::from_slice(&encoded).expect("deserialize");

        assert_eq!(handle, decoded);
        let (w, l) = decoded.unpack();
        assert_eq!(w, worker_id);
        assert_eq!(l, local_id);
    }

    #[test]
    fn test_zero_local_id_encodes() {
        // local_id = 0 is reserved but must not panic
        let worker_id = WorkerId::from_u64(1);
        let handle = StreamAnchorHandle::pack(worker_id, 0);
        let (w, l) = handle.unpack();
        assert_eq!(w, worker_id);
        assert_eq!(l, 0);
    }

    #[test]
    fn test_max_spsc_values_roundtrip() {
        // Max SPSC local_id is (1 << 63) - 1 — the high bit is reserved for
        // the MPSC kind discriminator (MPSC_STREAM_BIT).
        let worker_id = WorkerId::from_u64(u64::MAX);
        let local_id = (1u64 << 63) - 1;
        let handle = StreamAnchorHandle::pack(worker_id, local_id);
        assert!(handle.is_spsc_stream());
        assert!(!handle.is_mpsc_stream());
        let encoded = rmp_serde::to_vec(&handle).unwrap();
        let decoded: StreamAnchorHandle = rmp_serde::from_slice(&encoded).unwrap();
        assert_eq!(handle, decoded);
        let (w, l) = decoded.unpack();
        assert_eq!(w.as_u64(), u64::MAX);
        assert_eq!(l, (1u64 << 63) - 1);
    }

    #[test]
    fn test_max_mpsc_values_roundtrip() {
        // Max MPSC raw local_id is (1 << 63) - 1; pack_mpsc ORs in the kind
        // bit, so the packed local_id is u64::MAX.
        let worker_id = WorkerId::from_u64(u64::MAX);
        let raw_local = (1u64 << 63) - 1;
        let handle = StreamAnchorHandle::pack_mpsc(worker_id, raw_local);
        assert!(handle.is_mpsc_stream());
        assert!(!handle.is_spsc_stream());
        let encoded = rmp_serde::to_vec(&handle).unwrap();
        let decoded: StreamAnchorHandle = rmp_serde::from_slice(&encoded).unwrap();
        assert_eq!(handle, decoded);
        let (w, l) = decoded.unpack();
        assert_eq!(w.as_u64(), u64::MAX);
        assert_eq!(l, u64::MAX, "packed local_id is raw | MPSC_STREAM_BIT");
    }

    #[test]
    fn test_kind_discriminator() {
        let wid = WorkerId::from_u64(1);
        let spsc = StreamAnchorHandle::pack(wid, 7);
        let mpsc = StreamAnchorHandle::pack_mpsc(wid, 7);

        assert!(spsc.is_spsc_stream());
        assert!(!spsc.is_mpsc_stream());
        assert_eq!(spsc.kind(), AnchorKind::Spsc);

        assert!(mpsc.is_mpsc_stream());
        assert!(!mpsc.is_spsc_stream());
        assert_eq!(mpsc.kind(), AnchorKind::Mpsc);

        // Same raw counter, different kind → different handles.
        assert_ne!(spsc, mpsc);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "MPSC_STREAM_BIT is reserved")]
    fn test_pack_rejects_mpsc_bit_in_debug() {
        let wid = WorkerId::from_u64(1);
        let _ = StreamAnchorHandle::pack(wid, MPSC_STREAM_BIT | 1);
    }
}
