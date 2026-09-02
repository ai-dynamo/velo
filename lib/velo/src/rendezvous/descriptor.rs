// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The RDMA transfer descriptor (D7 + D12): velo-owned, versioned, and
//! length-framed.
//!
//! # Why this is not serde
//!
//! Everything else in the rendezvous control plane is `serde_json`, and this
//! deliberately is not. The descriptor's payload is a *packed remote key* —
//! opaque backend bytes that end up as an argument to `ucp_ep_rkey_unpack`,
//! which parses with no length bound at all (the public API has no parameter
//! for one). A truncated or corrupt blob is therefore an out-of-bounds read
//! inside UCX that no Rust wrapper can make safe after the fact, so the framing
//! that decides how many bytes are handed over has to be exact, fixed, and
//! ours. A self-describing format that tolerates trailing bytes or re-orders
//! fields would put that decision somewhere we cannot audit.
//!
//! The second reason is D12. The descriptor leads with a **backend
//! discriminator**, so a later NIXL or libfabric provider is a new value of
//! [`DescriptorBackend`] rather than a new wire protocol. Nothing
//! backend-specific appears in the layout: the key material is a byte string
//! whose meaning belongs entirely to the named backend. That is the exact
//! mistake PR #40 made by putting `velo_nixl::MemType` on the wire.
//!
//! # Layout
//!
//! Fixed prefix, little-endian, no padding and no alignment requirement:
//!
//! ```text
//! backend:    u8      1 = ucx
//! version:    u8      1
//! flags:      u8      0
//! generation: u64le   owner's registration generation
//! addr:       u64le   owner-authored absolute address
//! len:        u64le   bytes to read
//! rkey_len:   u16le   length of the packed key that follows
//! rkey:       [u8; rkey_len]
//! ```
//!
//! Twenty-nine header bytes, then exactly `rkey_len` more, then nothing.
//!
//! # Decoding refuses; it never repairs
//!
//! [`RdmaDescriptor::decode`] rejects anything it cannot account for byte for
//! byte: an unknown backend or version, a non-zero `flags` (a sender using a
//! feature this build does not know about is a sender this build must not
//! second-guess), a zero `len`, a `rkey_len` that disagrees with the bytes that
//! actually follow, and any trailing byte at all.
//!
//! A refusal is **not** an error the caller surfaces. The consumer answers it
//! by detaching and re-acquiring on the chunked path, which always works, and
//! records the reason. That is what makes the strictness affordable: the cost
//! of refusing a descriptor is one extra round trip, and the cost of accepting
//! a bad one is an out-of-bounds read on the progress thread.
//!
//! # What this does not validate
//!
//! The rkey bytes themselves. They go to the backend, and for UCX the Phase-1
//! `RdmaEndpoint::get` pre-parses them against the two `ucp_rkey` format stages
//! before any pointer reaches `ucp_ep_rkey_unpack`. There is deliberately no
//! second rkey parser here: one containment argument, in the module that owns
//! the FFI call, is worth more than two that can drift apart.

use bytes::Bytes;

/// Descriptor format version this build writes and accepts.
pub(crate) const DESCRIPTOR_VERSION: u8 = 1;

/// Bytes before the packed key: backend, version, flags, generation, addr, len,
/// rkey_len.
pub(crate) const HEADER_LEN: usize = 1 + 1 + 1 + 8 + 8 + 8 + 2;

/// Backend-agnostic ceiling on a packed key.
///
/// Not the same bound as the UCX transport's `MAX_PACKED_RKEY`, and
/// deliberately looser: this one exists so a `rkey_len` field cannot ask a
/// decoder to reserve an absurd amount before the length has been reconciled
/// with the bytes actually present, while the backend applies whatever tighter
/// bound its own format justifies. A blob that passes here and fails there is
/// refused there, which is the right place for a backend-specific limit to
/// live.
pub(crate) const MAX_KEY_LEN: usize = 4096;

/// Which RDMA provider authored a descriptor (D12's wire discriminator).
///
/// The registry is closed and numeric on the wire so that adding a provider is
/// additive: a peer that does not know a discriminator refuses the descriptor
/// and falls back to chunked, rather than mis-parsing key material meant for
/// somebody else.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DescriptorBackend {
    /// UCX, via `ucp_rkey_pack` / `ucp_get_nbx`.
    Ucx,
}

impl DescriptorBackend {
    /// Wire value.
    pub(crate) fn to_wire(self) -> u8 {
        match self {
            Self::Ucx => 1,
        }
    }

    /// Parse a wire value. `None` for a provider this build does not know.
    pub(crate) fn from_wire(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Ucx),
            _ => None,
        }
    }

    /// The name this backend goes by in an [`RdmaOffer`](super::protocol::RdmaOffer)
    /// and in [`RdmaBackend::key`](super::rdma::RdmaBackend::key).
    pub(crate) fn key(self) -> &'static str {
        match self {
            Self::Ucx => "ucx",
        }
    }

    /// Match a backend name from a consumer's offer, or from the registry.
    pub(crate) fn from_key(key: &str) -> Option<Self> {
        match key {
            "ucx" => Some(Self::Ucx),
            _ => None,
        }
    }
}

/// One remote range a consumer may read with a single RDMA GET.
///
/// Authored by the owner, always. The consumer never computes `addr` — that is
/// the property which makes software-emulated RMA (which validates nothing)
/// safe over `UCX_TLS=tcp`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RdmaDescriptor {
    /// Which provider's key material `packed_key` holds.
    pub backend: DescriptorBackend,
    /// The owner's registration generation, so a descriptor that outlived its
    /// registration is detectable rather than silently wrong. Carried through
    /// for diagnostics in v1; a Phase-5 consumer-side rkey cache keys on it.
    pub generation: u64,
    /// Absolute address in the owner's address space.
    pub addr: u64,
    /// Bytes to read. Never zero — a zero-length transfer has nothing to
    /// describe and is served inline by the chunked path.
    pub len: u64,
    /// The owner's packed key covering `addr`. Opaque here.
    pub packed_key: Bytes,
}

/// Why a descriptor could not be decoded.
///
/// Every variant means the same thing to a caller — *fall back to chunked and
/// count it* — but they are distinguished so a log line says which malformation
/// was seen rather than "bad descriptor".
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum DescriptorError {
    /// Fewer bytes than the fixed header needs.
    #[error("rdma descriptor shorter than its header")]
    Truncated,
    /// A backend discriminator this build does not know.
    #[error("unknown rdma descriptor backend: {0}")]
    UnknownBackend(u8),
    /// A format version this build does not know.
    #[error("unknown rdma descriptor version: {0}")]
    UnknownVersion(u8),
    /// A flag bit this build does not know. Refused rather than ignored: a flag
    /// exists to change how the rest is read.
    #[error("unknown rdma descriptor flags: {0:#x}")]
    UnknownFlags(u8),
    /// A zero-length transfer.
    #[error("rdma descriptor names a zero-length range")]
    EmptyRange,
    /// `rkey_len` is zero, past [`MAX_KEY_LEN`], or disagrees with the bytes
    /// that follow — including any trailing byte after them.
    #[error("rdma descriptor key length {declared} does not match its {actual} remaining bytes")]
    KeyLength {
        /// What the `rkey_len` field claimed.
        declared: usize,
        /// What was actually left after the header.
        actual: usize,
    },
}

impl RdmaDescriptor {
    /// Serialise to the fixed layout in the module docs.
    ///
    /// Returns `None` for a descriptor this decoder would refuse — an empty
    /// range or an over-long key. Encoding something that cannot round-trip
    /// would put a permanent fallback on the wire and blame the consumer for
    /// it.
    pub(crate) fn encode(&self) -> Option<Vec<u8>> {
        if self.len == 0 || self.packed_key.is_empty() || self.packed_key.len() > MAX_KEY_LEN {
            return None;
        }
        let key_len = u16::try_from(self.packed_key.len()).ok()?;
        let mut out = Vec::with_capacity(HEADER_LEN + self.packed_key.len());
        out.push(self.backend.to_wire());
        out.push(DESCRIPTOR_VERSION);
        out.push(0); // flags
        out.extend_from_slice(&self.generation.to_le_bytes());
        out.extend_from_slice(&self.addr.to_le_bytes());
        out.extend_from_slice(&self.len.to_le_bytes());
        out.extend_from_slice(&key_len.to_le_bytes());
        out.extend_from_slice(&self.packed_key);
        Some(out)
    }

    /// Parse the fixed layout, accounting for every byte.
    ///
    /// See the module docs for why this refuses rather than repairs, and why a
    /// refusal is a routing decision rather than an error.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, DescriptorError> {
        let header: &[u8; HEADER_LEN] = bytes
            .get(..HEADER_LEN)
            .and_then(|h| h.try_into().ok())
            .ok_or(DescriptorError::Truncated)?;

        let backend = DescriptorBackend::from_wire(header[0])
            .ok_or(DescriptorError::UnknownBackend(header[0]))?;
        if header[1] != DESCRIPTOR_VERSION {
            return Err(DescriptorError::UnknownVersion(header[1]));
        }
        if header[2] != 0 {
            return Err(DescriptorError::UnknownFlags(header[2]));
        }
        let generation = u64::from_le_bytes(header[3..11].try_into().expect("8 bytes"));
        let addr = u64::from_le_bytes(header[11..19].try_into().expect("8 bytes"));
        let len = u64::from_le_bytes(header[19..27].try_into().expect("8 bytes"));
        if len == 0 {
            return Err(DescriptorError::EmptyRange);
        }
        let declared = u16::from_le_bytes(header[27..29].try_into().expect("2 bytes")) as usize;
        let actual = bytes.len() - HEADER_LEN;
        // Exact, in both directions: a short key is a truncation, and a long
        // remainder is a trailing byte nobody can account for. Either way the
        // sender and this decoder disagree about the framing, and the one thing
        // that must not happen next is handing a length nobody agrees on to a
        // parser with no bound of its own.
        if declared == 0 || declared > MAX_KEY_LEN || declared != actual {
            return Err(DescriptorError::KeyLength { declared, actual });
        }

        Ok(Self {
            backend,
            generation,
            addr,
            len,
            packed_key: Bytes::copy_from_slice(&bytes[HEADER_LEN..]),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> RdmaDescriptor {
        RdmaDescriptor {
            backend: DescriptorBackend::Ucx,
            generation: 0xDEAD_BEEF_1234_5678,
            addr: 0x7F00_0000_1000,
            len: 64 * 1024,
            packed_key: Bytes::from_static(&[1, 2, 3, 4, 5, 6, 7, 8, 9]),
        }
    }

    #[test]
    fn roundtrip_is_exact() {
        let d = sample();
        let bytes = d.encode().expect("encode");
        assert_eq!(bytes.len(), HEADER_LEN + 9);
        assert_eq!(RdmaDescriptor::decode(&bytes).expect("decode"), d);
    }

    #[test]
    fn layout_is_the_documented_one() {
        let bytes = sample().encode().expect("encode");
        assert_eq!(bytes[0], 1, "backend discriminator");
        assert_eq!(bytes[1], DESCRIPTOR_VERSION);
        assert_eq!(bytes[2], 0, "flags");
        assert_eq!(&bytes[3..11], &0xDEAD_BEEF_1234_5678u64.to_le_bytes());
        assert_eq!(&bytes[11..19], &0x7F00_0000_1000u64.to_le_bytes());
        assert_eq!(&bytes[19..27], &(64u64 * 1024).to_le_bytes());
        assert_eq!(&bytes[27..29], &9u16.to_le_bytes());
    }

    #[test]
    fn truncation_anywhere_is_refused() {
        let bytes = sample().encode().expect("encode");
        for cut in 0..bytes.len() {
            let err = RdmaDescriptor::decode(&bytes[..cut])
                .expect_err("a truncated descriptor must not decode");
            // Below the header it is a truncation; above it the key length
            // stops disagreeing only at the full length.
            if cut < HEADER_LEN {
                assert_eq!(err, DescriptorError::Truncated, "cut at {cut}");
            } else {
                assert!(
                    matches!(err, DescriptorError::KeyLength { .. }),
                    "cut at {cut}: {err}"
                );
            }
        }
    }

    #[test]
    fn trailing_bytes_are_refused() {
        let mut bytes = sample().encode().expect("encode");
        bytes.push(0);
        assert_eq!(
            RdmaDescriptor::decode(&bytes),
            Err(DescriptorError::KeyLength {
                declared: 9,
                actual: 10
            })
        );
    }

    #[test]
    fn a_lying_key_length_is_refused() {
        let mut bytes = sample().encode().expect("encode");
        bytes[27..29].copy_from_slice(&300u16.to_le_bytes());
        assert!(matches!(
            RdmaDescriptor::decode(&bytes),
            Err(DescriptorError::KeyLength {
                declared: 300,
                actual: 9
            })
        ));
        // ... and a shorter lie is refused for the same reason, rather than
        // silently handing a prefix to the backend.
        bytes[27..29].copy_from_slice(&4u16.to_le_bytes());
        assert!(matches!(
            RdmaDescriptor::decode(&bytes),
            Err(DescriptorError::KeyLength { declared: 4, .. })
        ));
    }

    #[test]
    fn unknown_backend_version_and_flags_are_refused() {
        let good = sample().encode().expect("encode");

        let mut bytes = good.clone();
        bytes[0] = 7;
        assert_eq!(
            RdmaDescriptor::decode(&bytes),
            Err(DescriptorError::UnknownBackend(7))
        );

        let mut bytes = good.clone();
        bytes[1] = 2;
        assert_eq!(
            RdmaDescriptor::decode(&bytes),
            Err(DescriptorError::UnknownVersion(2))
        );

        let mut bytes = good;
        bytes[2] = 0b10;
        assert_eq!(
            RdmaDescriptor::decode(&bytes),
            Err(DescriptorError::UnknownFlags(0b10))
        );
    }

    #[test]
    fn a_zero_length_range_is_refused_both_ways() {
        let mut d = sample();
        d.len = 0;
        assert!(d.encode().is_none(), "encode must not emit what it refuses");

        let mut bytes = sample().encode().expect("encode");
        bytes[19..27].copy_from_slice(&0u64.to_le_bytes());
        assert_eq!(
            RdmaDescriptor::decode(&bytes),
            Err(DescriptorError::EmptyRange)
        );
    }

    #[test]
    fn an_empty_or_oversized_key_never_encodes() {
        let mut d = sample();
        d.packed_key = Bytes::new();
        assert!(d.encode().is_none());

        let mut d = sample();
        d.packed_key = Bytes::from(vec![0u8; MAX_KEY_LEN + 1]);
        assert!(d.encode().is_none());
    }

    #[test]
    fn backend_names_and_wire_values_agree() {
        assert_eq!(
            DescriptorBackend::from_key("ucx"),
            Some(DescriptorBackend::Ucx)
        );
        assert_eq!(DescriptorBackend::from_key("nixl"), None);
        assert_eq!(
            DescriptorBackend::from_wire(DescriptorBackend::Ucx.to_wire()),
            Some(DescriptorBackend::Ucx)
        );
        assert_eq!(DescriptorBackend::Ucx.key(), "ucx");
    }
}
