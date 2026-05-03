// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Wire types for the rendezvous control-plane protocol.
//!
//! All types are serialized via serde_json for use with velo-messenger's
//! typed-unary and fire-and-forget active message handlers.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Metadata
// ---------------------------------------------------------------------------

/// Request payload for the `_rv_metadata` typed-unary handler.
#[derive(Debug, Serialize, Deserialize)]
pub struct RvMetadataRequest {
    /// The handle to query (as u128 wire format).
    pub handle: RvHandleWire,
}

/// Response from `_rv_metadata`: lightweight info about staged data (no lock acquired).
#[derive(Debug, Serialize, Deserialize)]
pub struct DataMetadata {
    /// Total bytes of the staged payload.
    pub total_len: u64,
    /// Current refcount.
    pub refcount: u32,
    /// Whether the data is RDMA-pinned (Phase 2).
    pub pinned: bool,
}

// ---------------------------------------------------------------------------
// Acquire (read lock + data transfer initiation)
// ---------------------------------------------------------------------------

/// Request payload for the `_rv_acquire` typed-unary handler.
#[derive(Debug, Serialize, Deserialize)]
pub struct RvAcquireRequest {
    /// The handle to acquire a read lock on.
    pub handle: RvHandleWire,
}

/// Response from `_rv_acquire`: transfer metadata with read lock.
///
/// `Ready` payloads are pulled via `_rv_pull` chunk-by-chunk (used for the
/// default in-memory `Bytes` slots). `Rdma` carries a serialized
/// [`NixlAddrDescriptor`] that the consumer fulfills via NIXL_READ when the
/// `nixl` feature is enabled on both sides.
#[derive(Debug, Serialize, Deserialize)]
pub enum AcquireResponse {
    /// Data available via chunked pull (1 or more chunks).
    Ready {
        lease_id: u64,
        transfer_id: u64,
        total_len: u64,
        chunk_size: u32,
        chunk_count: u32,
    },
    /// RDMA descriptor for direct memory access. The `descriptor` is an
    /// `rmp_serde`-encoded [`NixlAddrDescriptor`] (only present when the
    /// owner staged via `register_data_pinned`).
    Rdma { lease_id: u64, descriptor: Vec<u8> },
}

// ---------------------------------------------------------------------------
// Pull (individual chunk retrieval)
// ---------------------------------------------------------------------------

/// Request payload for the `_rv_pull` unary handler.
#[derive(Debug, Serialize, Deserialize)]
pub struct RvPullRequest {
    /// Transfer ID from the `AcquireResponse::Chunked`.
    pub transfer_id: u64,
    /// Zero-based chunk index to retrieve.
    pub chunk_index: u32,
}

// ---------------------------------------------------------------------------
// Ref / Detach / Release
// ---------------------------------------------------------------------------

/// Request payload for the `_rv_ref` fire-and-forget handler.
#[derive(Debug, Serialize, Deserialize)]
pub struct RvRefRequest {
    pub handle: RvHandleWire,
}

/// Request payload for the `_rv_detach` fire-and-forget handler.
#[derive(Debug, Serialize, Deserialize)]
pub struct RvDetachRequest {
    pub handle: RvHandleWire,
    pub lease_id: u64,
}

/// Request payload for the `_rv_release` fire-and-forget handler.
#[derive(Debug, Serialize, Deserialize)]
pub struct RvReleaseRequest {
    pub handle: RvHandleWire,
    pub lease_id: u64,
}

// ---------------------------------------------------------------------------
// Handle wire format
// ---------------------------------------------------------------------------

/// Wire-safe representation of a [`crate::DataHandle`] as two u64 fields.
///
/// Avoids u128 serialization issues across serde backends.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct RvHandleWire {
    pub hi: u64,
    pub lo: u64,
}

impl RvHandleWire {
    pub fn from_handle(handle: crate::DataHandle) -> Self {
        let raw = handle.as_u128();
        Self {
            hi: (raw >> 64) as u64,
            lo: raw as u64,
        }
    }

    pub fn to_handle(self) -> crate::DataHandle {
        crate::DataHandle::from_u128(((self.hi as u128) << 64) | (self.lo as u128))
    }
}

// ---------------------------------------------------------------------------
// Error response
// ---------------------------------------------------------------------------

/// Error returned by rendezvous control-plane handlers.
#[derive(Debug, Serialize, Deserialize)]
pub struct RvError {
    pub message: String,
}

// ---------------------------------------------------------------------------
// Phase 2 — NIXL/RDMA descriptor + handshake
// ---------------------------------------------------------------------------

/// Address descriptor of a remote NIXL-registered region.
///
/// Serialized via `rmp_serde` and packed into [`AcquireResponse::Rdma::descriptor`]
/// when an owner returns RDMA-pinned data.
#[cfg(feature = "nixl")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NixlAddrDescriptor {
    /// The owner-side NIXL agent name (used as the `remote_name` for `create_xfer_req`).
    pub agent: String,
    /// Address of the registered region as a `u64` (host-endian usize).
    pub addr: u64,
    /// Size of the registered region in bytes.
    pub size: u64,
    /// Memory type of the region (Dram for Phase 2; Vram is on the roadmap).
    pub mem_type: velo_nixl::MemType,
    /// NIXL device id. 0 for host DRAM; CUDA device for VRAM; fd for File.
    pub device_id: u64,
}

/// Request payload for the `_rv_nixl_handshake` typed-unary handler.
///
/// Sent once per (consumer, owner) pair to bootstrap NIXL agent metadata.
#[cfg(feature = "nixl")]
#[derive(Debug, Serialize, Deserialize)]
pub struct RvNixlHandshakeRequest;

/// Response from `_rv_nixl_handshake`: the owner's NIXL agent name and the
/// blob produced by `Agent::get_local_md()` for `Agent::load_remote_md()` on
/// the consumer side.
#[cfg(feature = "nixl")]
#[derive(Debug, Serialize, Deserialize)]
pub struct RvNixlHandshakeResponse {
    pub agent: String,
    #[serde(with = "serde_bytes")]
    pub local_md: Vec<u8>,
}
