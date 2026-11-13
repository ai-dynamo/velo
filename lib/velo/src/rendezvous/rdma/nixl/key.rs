// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The NIXL key: what the NIXL backend puts in a descriptor's key bytes.
//!
//! Everything a consumer needs to read one region:
//!
//! ```text
//! version:     u8      1
//! region_dev:  u64le   the NIXL device id this registration was made under
//! region_addr: u64le   start of the registered region
//! region_len:  u64le   its length; never zero
//! conn_len:    u16le   length of `conn`; never zero
//! md_len:      u16le   length of `md`; never zero
//! conn:        [u8]    NIXL connection info of the owner's agent
//! md:          [u8]    NIXL metadata of this one region
//! ```
//!
//! Twenty-nine header bytes, then exactly `conn_len + md_len` more.
//!
//! NIXL gives connection info and region metadata as two blobs, and a consumer
//! must load the first before the second. Both travel in every key, so a
//! consumer that has never met the owner needs nothing else. The device id is
//! stated beside them because the consumer asks NIXL for the region by it (see
//! `peers.rs`), and cannot read it out of `md`.
//!
//! The blobs are NIXL's format, and velo does not parse them. Decoding makes
//! the frame exact, so NIXL never gets a blob cut at the wrong byte. It does
//! not make a hostile blob safe: some malformed blobs crash inside NIXL.

use bytes::Bytes;
use velo_ext::InstanceId;

/// Key format version this build writes and accepts.
const KEY_VERSION: u8 = 1;

/// Bytes before the two blobs.
pub(crate) const HEADER_LEN: usize = 1 + 8 + 8 + 8 + 2 + 2;

/// One region of an owner, as a consumer needs it to read from that region.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct NixlKey<'a> {
    /// The NIXL device id the region is registered under.
    pub region_dev: u64,
    /// Start of the registered region, in the owner's address space.
    pub region_addr: u64,
    /// Length of the registered region.
    pub region_len: u64,
    /// The owner agent's connection info.
    pub conn: &'a [u8],
    /// The region's metadata.
    pub md: &'a [u8],
}

impl<'a> NixlKey<'a> {
    /// Write the frame. `None` for a key that [`decode`](Self::decode) would
    /// refuse, or one longer than `max_len`: a key that cannot make the round
    /// trip must fail where it is built, not at every consumer.
    pub(crate) fn encode(&self, max_len: usize) -> Option<Bytes> {
        if self.region_len == 0 || self.region_addr.checked_add(self.region_len).is_none() {
            return None;
        }
        if self.conn.is_empty() || self.md.is_empty() {
            return None;
        }
        let conn_len = u16::try_from(self.conn.len()).ok()?;
        let md_len = u16::try_from(self.md.len()).ok()?;
        let total = HEADER_LEN + self.conn.len() + self.md.len();
        if total > max_len {
            return None;
        }
        let mut out = Vec::with_capacity(total);
        out.push(KEY_VERSION);
        out.extend_from_slice(&self.region_dev.to_le_bytes());
        out.extend_from_slice(&self.region_addr.to_le_bytes());
        out.extend_from_slice(&self.region_len.to_le_bytes());
        out.extend_from_slice(&conn_len.to_le_bytes());
        out.extend_from_slice(&md_len.to_le_bytes());
        out.extend_from_slice(self.conn);
        out.extend_from_slice(self.md);
        Some(Bytes::from(out))
    }

    /// Read the frame, with every byte accounted for. The error says which
    /// fault it was, for a log line; a caller refuses the transfer on any.
    pub(crate) fn decode(bytes: &'a [u8]) -> Result<Self, &'static str> {
        let header: &[u8; HEADER_LEN] = bytes
            .get(..HEADER_LEN)
            .and_then(|h| h.try_into().ok())
            .ok_or("shorter than its header")?;
        if header[0] != KEY_VERSION {
            return Err("unknown version");
        }
        let region_dev = u64::from_le_bytes(header[1..9].try_into().expect("8 bytes"));
        let region_addr = u64::from_le_bytes(header[9..17].try_into().expect("8 bytes"));
        let region_len = u64::from_le_bytes(header[17..25].try_into().expect("8 bytes"));
        if region_len == 0 || region_addr.checked_add(region_len).is_none() {
            return Err("empty or wrapping region");
        }
        let conn = u16::from_le_bytes(header[25..27].try_into().expect("2 bytes")) as usize;
        let md = u16::from_le_bytes(header[27..29].try_into().expect("2 bytes")) as usize;
        let rest = &bytes[HEADER_LEN..];
        // Exact in both directions: a short remainder is a truncation, and a
        // long one is a byte nobody can account for. NIXL must not be the one
        // to find out where a blob ends.
        if conn == 0 || md == 0 || conn + md != rest.len() {
            return Err("blob lengths do not match the bytes that follow");
        }
        let (conn, md) = rest.split_at(conn);
        Ok(Self {
            region_dev,
            region_addr,
            region_len,
            conn,
            md,
        })
    }

    /// Whether `[addr, addr + len)` lies inside the region this key names.
    ///
    /// The owner writes both the descriptor's range and this key, so a
    /// mismatch is a broken owner. The consumer checks anyway: it is about to
    /// ask its NIC to read that range.
    pub(crate) fn covers(&self, addr: u64, len: u64) -> bool {
        let Some(end) = addr.checked_add(len) else {
            return false;
        };
        // `decode` and `encode` both refuse a region that wraps.
        len != 0 && addr >= self.region_addr && end <= self.region_addr + self.region_len
    }
}

/// The NIXL agent name of a velo instance.
///
/// NIXL keys what it knows about a peer by the peer's agent name, and refuses
/// new connection info under a name it already holds. An instance id is unique
/// for each run of an instance, so a restarted peer is a new name. A consumer
/// derives the name it expects from the instance it reads from, and refuses a
/// key whose blobs say another.
pub(crate) fn agent_name(instance: InstanceId) -> String {
    format!("velo-{instance}")
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX: usize = 16 * 1024;

    fn sample<'a>() -> NixlKey<'a> {
        NixlKey {
            region_dev: 0x0102_0304_0506_0708,
            region_addr: 0x7f00_0000_1000,
            region_len: 64 << 20,
            conn: b"connection-info",
            md: b"region-md",
        }
    }

    /// The layout is the wire contract between two velo versions. A field
    /// that moves is a consumer that reads a region id as an address.
    #[test]
    fn the_layout_is_fixed_and_round_trips() {
        let bytes = sample().encode(MAX).unwrap();
        assert_eq!(HEADER_LEN, 29);
        assert_eq!(bytes[0], 1);
        assert_eq!(bytes[1..9], 0x0102_0304_0506_0708u64.to_le_bytes());
        assert_eq!(bytes[9..17], 0x7f00_0000_1000u64.to_le_bytes());
        assert_eq!(bytes[17..25], (64u64 << 20).to_le_bytes());
        assert_eq!(bytes[25..27], 15u16.to_le_bytes());
        assert_eq!(bytes[27..29], 9u16.to_le_bytes());
        assert_eq!(&bytes[29..44], b"connection-info");
        assert_eq!(&bytes[44..], b"region-md");
        assert_eq!(NixlKey::decode(&bytes), Ok(sample()));
    }

    /// A key cut anywhere is refused, and so is one with a byte after it.
    /// Without this, a cut inside `md` would hand NIXL a short blob, and NIXL
    /// crashes on some of those.
    #[test]
    fn a_key_cut_at_any_byte_or_with_a_trailing_byte_is_refused() {
        let mut bytes = sample().encode(MAX).unwrap().to_vec();
        for cut in 0..bytes.len() {
            assert!(
                NixlKey::decode(&bytes[..cut]).is_err(),
                "a key cut to {cut} of {} bytes decoded",
                bytes.len()
            );
        }
        bytes.push(0);
        assert!(NixlKey::decode(&bytes).is_err());
    }

    /// A header field that lies is refused: the two lengths must sum to what
    /// follows and neither is zero, the version is known, and the region is
    /// neither empty nor wrapping.
    #[test]
    fn a_header_that_lies_is_refused() {
        let good = sample().encode(MAX).unwrap().to_vec();
        let patched = |at: std::ops::Range<usize>, value: &[u8]| {
            let mut bytes = good.clone();
            bytes[at].copy_from_slice(value);
            bytes
        };
        for (what, bytes) in [
            ("version", patched(0..1, &[2])),
            ("a longer conn", patched(25..27, &16u16.to_le_bytes())),
            ("no conn", patched(25..29, &[0, 0, 24, 0])),
            ("no md", patched(25..29, &[24, 0, 0, 0])),
            ("an empty region", patched(17..25, &0u64.to_le_bytes())),
            (
                "a wrapping region",
                patched(9..17, &(u64::MAX - 1).to_le_bytes()),
            ),
        ] {
            assert!(NixlKey::decode(&bytes).is_err(), "{what} decoded");
        }
    }

    /// A key that no consumer would take must not leave the owner. `None`
    /// makes the owner's `map` fail, which is loud. A key that only fails at
    /// the consumer is a silent chunked fallback on every transfer.
    #[test]
    fn a_key_that_cannot_round_trip_does_not_encode() {
        let refused = |edit: fn(&mut NixlKey<'_>)| {
            let mut key = sample();
            edit(&mut key);
            key.encode(usize::MAX).is_none()
        };
        assert!(refused(|key| key.conn = b""));
        assert!(refused(|key| key.md = b""));
        assert!(refused(|key| key.region_len = 0));
        assert!(refused(|key| key.region_addr = u64::MAX - 1), "wraps");
        // A blob too long for its length field.
        let big = vec![7u8; usize::from(u16::MAX) + 1];
        let mut key = sample();
        key.conn = &big;
        assert!(key.encode(usize::MAX).is_none());

        let exact = HEADER_LEN + 15 + 9;
        assert!(sample().encode(exact).is_some());
        assert!(sample().encode(exact - 1).is_none());
    }

    #[test]
    fn covers_is_exact_at_both_ends() {
        let key = sample();
        let (start, len) = (key.region_addr, key.region_len);
        assert!(key.covers(start, len));
        assert!(key.covers(start, 1));
        assert!(key.covers(start + len - 1, 1));
        assert!(key.covers(start + 7, 4097));

        assert!(!key.covers(start - 1, 1));
        assert!(!key.covers(start + len, 1));
        assert!(!key.covers(start + len - 1, 2));
        assert!(!key.covers(start, len + 1));
        assert!(!key.covers(start, 0));
        // An end that wraps must not compare as "before the region's end".
        assert!(!key.covers(u64::MAX, 2));
        assert!(!key.covers(start + 1, u64::MAX));
    }
}
