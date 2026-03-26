// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! [`DataHandle`]: compact u128 wire handle encoding WorkerId + local slot ID.
//!
//! Wire layout (MSB to LSB): `[worker_id: upper 64 bits][local_id: lower 64 bits]`
//!
//! Serializes via serde as `{hi: u64, lo: u64}` to avoid the u128-as-bytes
//! encoding issue with MessagePack.

use serde::{Deserialize, Serialize};
use velo_common::WorkerId;

const WORKER_SHIFT: u32 = 64;

/// Compact wire handle encoding a [`WorkerId`] (upper 64 bits) and a local slot ID
/// (lower 64 bits) into a single `u128`.
///
/// `local_id` of `0` is reserved; [`crate::RendezvousManager`] assigns IDs starting at `1`.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct DataHandle(u128);

/// Private wire representation for serde serialization.
#[derive(Serialize, Deserialize)]
struct DataHandleWire {
    hi: u64,
    lo: u64,
}

impl DataHandle {
    /// Encode a [`WorkerId`] and local slot ID into a [`DataHandle`].
    pub fn pack(worker_id: WorkerId, local_id: u64) -> Self {
        let raw = ((worker_id.as_u64() as u128) << WORKER_SHIFT) | (local_id as u128);
        Self(raw)
    }

    /// Decode the [`WorkerId`] and local slot ID from this handle.
    pub fn unpack(&self) -> (WorkerId, u64) {
        let worker_id = WorkerId::from_u64((self.0 >> WORKER_SHIFT) as u64);
        let local_id = self.0 as u64;
        (worker_id, local_id)
    }

    /// Get the [`WorkerId`] that owns the data behind this handle.
    pub fn worker_id(&self) -> WorkerId {
        self.unpack().0
    }

    /// Return the raw `u128` value.
    pub fn as_u128(&self) -> u128 {
        self.0
    }

    /// Construct from a raw `u128` value (e.g. received over the wire).
    pub fn from_u128(raw: u128) -> Self {
        Self(raw)
    }
}

impl Serialize for DataHandle {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        DataHandleWire {
            hi: (self.0 >> 64) as u64,
            lo: self.0 as u64,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for DataHandle {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = DataHandleWire::deserialize(deserializer)?;
        Ok(Self(((wire.hi as u128) << 64) | (wire.lo as u128)))
    }
}

impl std::fmt::Display for DataHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (worker_id, local_id) = self.unpack();
        write!(
            f,
            "DataHandle(worker={}, local={})",
            worker_id.as_u64(),
            local_id
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pack_unpack_roundtrip() {
        let worker_id = WorkerId::from_u64(0xDEAD_BEEF_1234_5678);
        let local_id: u64 = 42;
        let handle = DataHandle::pack(worker_id, local_id);
        let (recovered_worker, recovered_local) = handle.unpack();
        assert_eq!(recovered_worker, worker_id);
        assert_eq!(recovered_local, local_id);
    }

    #[test]
    fn test_serde_json_roundtrip() {
        let worker_id = WorkerId::from_u64(0xDEAD_BEEF_1234_5678);
        let local_id: u64 = 99;
        let handle = DataHandle::pack(worker_id, local_id);

        let encoded = serde_json::to_vec(&handle).expect("serialize");
        let decoded: DataHandle = serde_json::from_slice(&encoded).expect("deserialize");

        assert_eq!(handle, decoded);
        let (w, l) = decoded.unpack();
        assert_eq!(w, worker_id);
        assert_eq!(l, local_id);
    }

    #[test]
    fn test_worker_id_accessor() {
        let wid = WorkerId::from_u64(123);
        let handle = DataHandle::pack(wid, 456);
        assert_eq!(handle.worker_id(), wid);
    }

    #[test]
    fn test_from_u128() {
        let wid = WorkerId::from_u64(7);
        let handle = DataHandle::pack(wid, 42);
        let raw = handle.as_u128();
        let handle2 = DataHandle::from_u128(raw);
        assert_eq!(handle, handle2);
    }

    #[test]
    fn test_max_values_roundtrip() {
        let worker_id = WorkerId::from_u64(u64::MAX);
        let local_id = u64::MAX;
        let handle = DataHandle::pack(worker_id, local_id);
        let encoded = serde_json::to_vec(&handle).unwrap();
        let decoded: DataHandle = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(handle, decoded);
        let (w, l) = decoded.unpack();
        assert_eq!(w.as_u64(), u64::MAX);
        assert_eq!(l, u64::MAX);
    }
}
