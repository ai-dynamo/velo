// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! `TipcEndpoint` encode/decode with `rmp_serde::to_vec_named` and cross-version round-trip tests.

use serde::{Deserialize, Serialize};
use velo_ext::{PeerInfo, TransportError, TransportKey, WorkerAddress};

use crate::transports::address::WorkerAddressBuilder;

/// A TIPC endpoint as advertised in a [`WorkerAddress`] under the key `"tipc"`.
///
/// This carries everything the register-time reachability gate (§5.3) and the
/// send-path writer need:
///
/// - `socket_ref` + `node` — the exact TIPC socket address; the primary connect
///   target, immune to anycast. A stale ref yields fast `ECONNREFUSED`; semantics
///   match TCP's `ip:port` (both are ephemeral per process start under a fresh
///   `InstanceId`).
/// - `service_type` + `service_instance` — the bound service name; cross-checked
///   against the live name-table publication in the remote register gate to reject
///   stale or recycled refs.
/// - `netid` — cluster identity equality gate (unauthenticated; default 4711).
/// - `netns_nonce` — `xxh3_64(boot_id_bytes ++ netns_ino)` uniquely identifying the
///   TIPC stack for a given (boot, netns) pair.  Equal nonce ⇒ same stack ⇒
///   exact-ref connect is valid without a bearer.  The inode is **parsed from
///   `readlink("/proc/self/ns/net")` = `"net:[<ino>]"`**, never from `stat()` —
///   syscall-interposing sandboxes (gVisor, seccomp-notify) fabricate procfs stat
///   inodes while passing `readlink` through to the kernel-generated string.
/// - `node_id` — 128-bit node identity (`SIOCGETNODEID`); all-zeros when unset.
/// - `scope` — bind scope: 2 = `TIPC_CLUSTER_SCOPE`, 3 = `TIPC_NODE_SCOPE`.
///
/// # Encoding invariant
///
/// **Encoded with [`rmp_serde::to_vec_named`]; positional arrays (`to_vec`) are
/// forbidden.**  Positional encoding breaks decode in both directions when any field
/// is added or removed — proven empirically (proposal §3).  All post-v1 fields must
/// carry `#[serde(default)]` so that v1 bytes can be decoded by a later struct and
/// later bytes can be decoded by v1.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TipcEndpoint {
    /// Schema version; always 1 for v1 endpoints.
    pub version: u8,
    /// TIPC service type (must be ≥ `TIPC_RESERVED_TYPES` = 64).
    pub service_type: u32,
    /// TIPC service instance — per-process random value namespacing the listener.
    pub service_instance: u32,
    /// 32-bit node hash from `getsockname`; 0 when TIPC identity is unset.
    pub node: u32,
    /// Listener port ref — the PRIMARY connect target (together with `node`).
    pub socket_ref: u32,
    /// Cluster identity (netid); default 4711.  Unequal ⇒ `Gate::Never`.
    pub netid: u32,
    /// 128-bit node identity (`SIOCGETNODEID`); all-zeros when unset.
    pub node_id: [u8; 16],
    /// `xxh3_64(boot_id_bytes ++ netns_ino)` — a pure function of (boot, netns).
    /// Parsed from `readlink("/proc/self/ns/net")` = `"net:[<ino>]"`, not `stat()`.
    pub netns_nonce: u64,
    /// Bind scope: 2 = `TIPC_CLUSTER_SCOPE`, 3 = `TIPC_NODE_SCOPE`.
    pub scope: u8,
}

impl TipcEndpoint {
    /// The [`TransportKey`] this endpoint is stored under in a [`WorkerAddress`].
    pub const KEY: &'static str = "tipc";

    /// Encode this endpoint into a new [`WorkerAddress`] under `key`.
    ///
    /// Uses [`rmp_serde::to_vec_named`] — mandatory for cross-version tolerance
    /// (see type-level doc and proposal §5.2 / §3).  Call from the transport builder
    /// after `getsockname` fills in `socket_ref` and `node`.
    ///
    /// # Errors
    ///
    /// Returns an error if MessagePack serialization fails (infallible for this
    /// struct in practice) or if `key` already exists in the address being built.
    pub fn encode_into_worker_address(
        &self,
        key: &TransportKey,
    ) -> Result<WorkerAddress, anyhow::Error> {
        let encoded = rmp_serde::to_vec_named(self)
            .map_err(|e| anyhow::anyhow!("Failed to msgpack-encode TipcEndpoint: {e}"))?;
        let mut builder = WorkerAddressBuilder::new();
        builder.add_entry(key.clone(), encoded)?;
        Ok(builder.build()?)
    }

    /// Decode a [`TipcEndpoint`] from a peer's [`WorkerAddress`] under `key`.
    ///
    /// Returns:
    /// - `Err(TransportError::NoEndpoint)` — `key` is absent (peer has no TIPC transport).
    /// - `Err(TransportError::InvalidEndpoint)` — bytes present but not a valid endpoint.
    /// - `Ok(ep)` — decoded endpoint; unknown future fields are silently ignored.
    pub fn decode_from_peer(
        peer_info: &PeerInfo,
        key: &TransportKey,
    ) -> Result<Self, TransportError> {
        let bytes = peer_info
            .worker_address()
            .get_entry(key)
            .map_err(|_| TransportError::NoEndpoint)?
            .ok_or(TransportError::NoEndpoint)?;

        rmp_serde::from_slice(&bytes).map_err(|e| {
            tracing::debug!(
                "Failed to decode TipcEndpoint from peer {}: {e}",
                peer_info.instance_id()
            );
            TransportError::InvalidEndpoint
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use velo_ext::InstanceId;

    fn sample_v1() -> TipcEndpoint {
        TipcEndpoint {
            version: 1,
            service_type: 0x5645_4C4F, // "VELO"
            service_instance: 0xDEAD_BEEF,
            node: 0x0102_0304,
            socket_ref: 0xABCD_1234,
            netid: 4711,
            node_id: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            netns_nonce: 0xCAFE_BABE_DEAD_BEEF,
            scope: 2, // TIPC_CLUSTER_SCOPE
        }
    }

    // -------------------------------------------------------------------------
    // Invariant 4 pin: encoding MUST produce a msgpack named map, not an array.
    // -------------------------------------------------------------------------

    #[test]
    fn encoding_is_named_map_not_array() {
        let ep = sample_v1();
        let bytes = rmp_serde::to_vec_named(&ep).unwrap();

        assert!(!bytes.is_empty(), "encoded bytes must not be empty");

        // MessagePack format of the first byte:
        //   fixmap:   0x80..=0x8f  (bits: 1000_xxxx — top nibble 0x8_)
        //   map16:    0xde
        //   map32:    0xdf
        //   fixarray: 0x90..=0x9f  (bits: 1001_xxxx — top nibble 0x9_)  ← must NOT be
        //   array16:  0xdc
        //   array17:  0xdd
        let b0 = bytes[0];
        let is_map = (b0 & 0xf0 == 0x80) || b0 == 0xde || b0 == 0xdf;
        let is_array = (b0 & 0xf0 == 0x90) || b0 == 0xdc || b0 == 0xdd;

        assert!(
            is_map,
            "Expected msgpack map encoding (rmp_serde::to_vec_named), \
             got first byte 0x{b0:02x}; is_array={is_array}. \
             This fires when to_vec (positional arrays) is used instead of to_vec_named."
        );
    }

    // -------------------------------------------------------------------------
    // Cross-version tolerance: V1 ↔ V2 both decode successfully.
    //
    // The load-bearing invariant is #[serde(default)] on future fields plus the
    // named-map encoding — positional arrays would fail in both directions.
    // -------------------------------------------------------------------------

    /// A hypothetical future v2 struct with one extra field.
    /// Only used in tests.  The `#[serde(default)]` on `extra_field` is what makes
    /// V1 bytes decodable into this struct.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct TipcEndpointV2 {
        pub version: u8,
        pub service_type: u32,
        pub service_instance: u32,
        pub node: u32,
        pub socket_ref: u32,
        pub netid: u32,
        pub node_id: [u8; 16],
        pub netns_nonce: u64,
        pub scope: u8,
        /// New field added in a hypothetical v2; absent in v1 bytes → defaults to 0.
        #[serde(default)]
        pub extra_field: u32,
    }

    #[test]
    fn v1_bytes_decode_into_v2_struct() {
        let ep = sample_v1();
        let v1_bytes = rmp_serde::to_vec_named(&ep).unwrap();

        let decoded: TipcEndpointV2 = rmp_serde::from_slice(&v1_bytes)
            .expect("V1 bytes must decode into a V2 struct: extra_field should default to 0");

        assert_eq!(decoded.service_type, ep.service_type);
        assert_eq!(decoded.socket_ref, ep.socket_ref);
        assert_eq!(decoded.netns_nonce, ep.netns_nonce);
        assert_eq!(decoded.node_id, ep.node_id);
        assert_eq!(
            decoded.extra_field, 0,
            "extra_field must default to 0 when absent in v1 bytes"
        );
    }

    #[test]
    fn v2_bytes_decode_into_v1_struct() {
        let v2 = TipcEndpointV2 {
            version: 1,
            service_type: 0x5645_4C4F,
            service_instance: 0xDEAD_BEEF,
            node: 0x0102_0304,
            socket_ref: 0xABCD_1234,
            netid: 4711,
            node_id: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            netns_nonce: 0xCAFE_BABE_DEAD_BEEF,
            scope: 2,
            extra_field: 0x1234_5678, // v2-only; v1 struct must silently ignore it
        };
        let v2_bytes = rmp_serde::to_vec_named(&v2).unwrap();

        let decoded: TipcEndpoint = rmp_serde::from_slice(&v2_bytes)
            .expect("V2 bytes must decode into V1 struct: unknown fields must be silently ignored");

        assert_eq!(decoded.service_type, v2.service_type);
        assert_eq!(decoded.socket_ref, v2.socket_ref);
        assert_eq!(decoded.netns_nonce, v2.netns_nonce);
        assert_eq!(decoded.node_id, v2.node_id);
    }

    // -------------------------------------------------------------------------
    // encode_into_worker_address / decode_from_peer round-trip
    // -------------------------------------------------------------------------

    #[test]
    fn worker_address_roundtrip() {
        let ep = sample_v1();
        let key = TransportKey::from("tipc");

        let addr = ep
            .encode_into_worker_address(&key)
            .expect("encode_into_worker_address must succeed");

        // The "tipc" entry must be present.
        assert!(
            addr.get_entry("tipc").unwrap().is_some(),
            "WorkerAddress must contain the 'tipc' key after encoding"
        );

        // Reconstruct via decode_from_peer.
        let instance_id = InstanceId::new_v4();
        let peer_info = PeerInfo::new(instance_id, addr);
        let decoded = TipcEndpoint::decode_from_peer(&peer_info, &key)
            .expect("decode_from_peer must succeed for a freshly encoded endpoint");

        assert_eq!(decoded, ep);
    }

    #[test]
    fn decode_from_peer_missing_key_returns_no_endpoint() {
        let addr = WorkerAddress::empty();
        let peer_info = PeerInfo::new(InstanceId::new_v4(), addr);
        let key = TransportKey::from("tipc");

        let result = TipcEndpoint::decode_from_peer(&peer_info, &key);
        assert!(
            matches!(result, Err(TransportError::NoEndpoint)),
            "Expected NoEndpoint when the 'tipc' key is absent, got: {result:?}"
        );
    }

    #[test]
    fn decode_from_peer_garbage_bytes_returns_invalid_endpoint() {
        let mut builder = WorkerAddressBuilder::new();
        // Not valid msgpack for TipcEndpoint
        builder.add_entry("tipc", vec![0xcc, 0xdd, 0xee]).unwrap();
        let addr = builder.build().unwrap();
        let peer_info = PeerInfo::new(InstanceId::new_v4(), addr);
        let key = TransportKey::from("tipc");

        let result = TipcEndpoint::decode_from_peer(&peer_info, &key);
        assert!(
            matches!(result, Err(TransportError::InvalidEndpoint)),
            "Expected InvalidEndpoint for malformed bytes, got: {result:?}"
        );
    }

    #[test]
    fn tipc_key_constant_is_tipc() {
        assert_eq!(TipcEndpoint::KEY, "tipc");
    }
}
