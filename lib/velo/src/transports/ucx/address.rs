// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The UCX transport's `WorkerAddress` fragment.
//!
//! Unlike every socket transport, UCX needs no listener: `ucp_ep_create` from
//! the peer's packed worker address is the entire connection story. The blob
//! published through discovery is therefore the worker address itself
//! (~200-250 bytes) plus the negotiation fields a peer needs before its first
//! send.

use serde::{Deserialize, Serialize};

/// Wire version of the blob. Bump on incompatible layout changes.
pub(crate) const BLOB_VERSION: u8 = 1;

/// Fixed base for velo's UCX Active Message id space.
///
/// UCX AM ids are worker-global and (since UCX 1.18) must fit in 16 bits.
/// Velo claims the contiguous range `AM_ID_BASE .. AM_ID_BASE + 7`:
/// ids 0-4 mirror [`MessageType`](velo_ext::MessageType), 5/6 carry the
/// health ping/pong. The base is a shared constant rather than a negotiated
/// value so that control replies (`ShuttingDown`, pong) can be addressed to a
/// peer we never registered; the blob still carries it so `register()` can
/// reject a peer built with an incompatible id layout.
pub(crate) const AM_ID_BASE: u16 = 0x5645; // "VE"

/// AM id offsets within velo's claimed range.
pub(crate) const AM_KIND_PING: u8 = 5;
pub(crate) const AM_KIND_PONG: u8 = 6;
pub(crate) const AM_KIND_COUNT: u8 = 7;

/// The decoded contents of a peer's `WorkerAddress` entry for this transport.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct UcxEndpoint {
    /// Blob layout version ([`BLOB_VERSION`]).
    pub v: u8,
    /// The peer's [`AM_ID_BASE`]; must match ours exactly.
    pub am_id_base: u16,
    /// Largest `header + payload` the peer accepts in one AM, in bytes.
    /// The effective per-pair limit is `min(local, remote)`.
    pub eager_max: u32,
    /// Random per-start value. A restarted worker publishes a new incarnation,
    /// so a cached blob from a dead incarnation is distinguishable from the
    /// live one.
    pub incarnation: u64,
    /// The packed `ucp_worker` address (`ucp_worker_query` with
    /// `UCP_WORKER_ATTR_FIELD_ADDRESS`), consumed by `ucp_ep_create`.
    #[serde(with = "serde_bytes")]
    pub worker_addr: Vec<u8>,
}

impl UcxEndpoint {
    pub fn encode(&self) -> anyhow::Result<Vec<u8>> {
        Ok(rmp_serde::to_vec(self)?)
    }

    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        let ep: UcxEndpoint = rmp_serde::from_slice(bytes)?;
        anyhow::ensure!(
            ep.v == BLOB_VERSION,
            "unsupported ucx blob version {} (expected {BLOB_VERSION})",
            ep.v
        );
        anyhow::ensure!(
            ep.am_id_base == AM_ID_BASE,
            "peer uses AM id base {:#x}, this build uses {AM_ID_BASE:#x}",
            ep.am_id_base
        );
        anyhow::ensure!(!ep.worker_addr.is_empty(), "empty ucp worker address");
        Ok(ep)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blob_roundtrip() {
        let ep = UcxEndpoint {
            v: BLOB_VERSION,
            am_id_base: AM_ID_BASE,
            eager_max: 1 << 20,
            incarnation: 0xDEAD_BEEF,
            worker_addr: vec![1, 2, 3, 4, 5],
        };
        let bytes = ep.encode().unwrap();
        let back = UcxEndpoint::decode(&bytes).unwrap();
        assert_eq!(back.eager_max, 1 << 20);
        assert_eq!(back.incarnation, 0xDEAD_BEEF);
        assert_eq!(back.worker_addr, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn blob_rejects_wrong_base() {
        let ep = UcxEndpoint {
            v: BLOB_VERSION,
            am_id_base: AM_ID_BASE + 1,
            eager_max: 0,
            incarnation: 0,
            worker_addr: vec![0],
        };
        let bytes = rmp_serde::to_vec(&ep).unwrap();
        assert!(UcxEndpoint::decode(&bytes).is_err());
    }

    #[test]
    fn blob_rejects_wrong_version() {
        let ep = UcxEndpoint {
            v: BLOB_VERSION + 1,
            am_id_base: AM_ID_BASE,
            eager_max: 0,
            incarnation: 0,
            worker_addr: vec![0],
        };
        let bytes = rmp_serde::to_vec(&ep).unwrap();
        assert!(UcxEndpoint::decode(&bytes).is_err());
    }
}
