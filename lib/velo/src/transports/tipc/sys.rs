// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! TIPC UAPI constants, `#[repr(C)]` structs, socket2 bridge, and compile-time layout assertions.
//!
//! All constants and struct definitions are transcribed verbatim from
//! `/usr/include/linux/tipc.h` (frozen UAPI — ABI-stable since kernel 4.x).
//! Layout sizes and field offsets are asserted at compile time via `const { assert!(...) }`
//! and cross-checked by the `layout_*` unit tests.
//!
//! ## Key types
//!
//! | C type | Rust type | Size |
//! |--------|-----------|------|
//! | `tipc_socket_addr` | [`TipcSocketAddr`] | 8 B |
//! | `tipc_service_addr` | [`TipcServiceAddr`] | 8 B |
//! | `tipc_service_range` | [`TipcServiceRange`] | 12 B |
//! | `sockaddr_tipc` | [`SockaddrTipc`] | 16 B |
//! | `tipc_subscr` | [`TipcSubscr`] | 28 B |
//! | `tipc_event` | [`TipcEvent`] | 48 B |

// ── Address family / socket-level constants ───────────────────────────────────

/// `AF_TIPC = 30` — address family for TIPC sockets.
pub const AF_TIPC: u16 = 30;

/// `AF_TIPC` as `libc::c_int`, for use with `socket(2)` / `setsockopt(2)`.
pub const AF_TIPC_RAW: libc::c_int = 30;

/// `SOL_TIPC = 271` — socket option level for TIPC-specific sockopts.
pub const SOL_TIPC: libc::c_int = 271;

// ── Address type discriminators (sockaddr_tipc.addrtype) ─────────────────────

/// `TIPC_SERVICE_RANGE = 1` — bind a service name range (publisher).
pub const TIPC_SERVICE_RANGE: u8 = 1;
/// `TIPC_SERVICE_ADDR = 2` — connect/send to a logical service name (anycast).
pub const TIPC_SERVICE_ADDR: u8 = 2;
/// `TIPC_SOCKET_ADDR = 3` — connect/send to an exact `{ref, node}` socket address.
pub const TIPC_SOCKET_ADDR: u8 = 3;

// ── Publication scopes (sockaddr_tipc.scope) ──────────────────────────────────

/// `TIPC_CLUSTER_SCOPE = 2` — publication visible to the entire cluster.
pub const TIPC_CLUSTER_SCOPE: i8 = 2;
/// `TIPC_NODE_SCOPE = 3` — publication visible only on the local node.
pub const TIPC_NODE_SCOPE: i8 = 3;

// ── Service type limits ────────────────────────────────────────────────────────

/// `TIPC_RESERVED_TYPES = 64` — service types 0–63 are reserved for kernel use.
pub const TIPC_RESERVED_TYPES: u32 = 64;

// ── Well-known service types ──────────────────────────────────────────────────

/// `TIPC_NODE_STATE = 0` — topology subscription type that yields node up/down events.
pub const TIPC_NODE_STATE: u32 = 0;
/// `TIPC_TOP_SRV = 1` — topology server service type; connect SEQPACKET to `{1,1}`.
pub const TIPC_TOP_SRV: u32 = 1;
/// `TIPC_LINK_STATE = 2` — topology subscription type that yields per-link events.
// Retained for UAPI/ABI completeness; velo uses TIPC_NODE_STATE, not link-state subs.
#[allow(dead_code)]
pub const TIPC_LINK_STATE: u32 = 2;

// ── Datagram size limit ───────────────────────────────────────────────────────

/// `TIPC_MAX_USER_MSG_SIZE = 66000` — maximum user-visible datagram/record size (bytes).
///
/// This cap applies to SOCK_SEQPACKET and SOCK_RDM. SOCK_STREAM is a byte stream;
/// the kernel chunks internally at 66,000 B but the cap is invisible to the application.
// Retained for UAPI/ABI completeness; velo SOCK_STREAM has no per-send size cap.
#[allow(dead_code)]
pub const TIPC_MAX_USER_MSG_SIZE: u32 = 66_000;

// ── Socket options (SOL_TIPC level) ──────────────────────────────────────────

/// `TIPC_IMPORTANCE = 127` — message importance for prioritised delivery.
/// Ignored for connected sockets' receive limits; not set on the hot path.
// Retained for UAPI/ABI completeness; velo accepts the kernel default importance.
#[allow(dead_code)]
pub const TIPC_IMPORTANCE: libc::c_int = 127;
/// `TIPC_CONN_TIMEOUT = 130` — connection timeout in milliseconds (default 8,000).
/// Velo sets 5,000 ms via the builder to ensure the 5 s connect_timeout fires first.
pub const TIPC_CONN_TIMEOUT: libc::c_int = 130;
/// `TIPC_NODELAY = 138` — disable Nagle-equivalent coalescing (kernels ≥ 5.5 only).
/// Best-effort: ENOPROTOOPT/EINVAL on older kernels are silently ignored.
pub const TIPC_NODELAY: libc::c_int = 138;

// ── Topology subscription filter bits ────────────────────────────────────────

/// `TIPC_SUB_PORTS = 0x01` — receive one event per matching binding (fine-grained).
pub const TIPC_SUB_PORTS: u32 = 0x01;
/// `TIPC_SUB_SERVICE = 0x02` — edge-triggered: event at first publication / last withdrawal.
// Retained for UAPI/ABI completeness; velo always uses TIPC_SUB_PORTS for fine-grained events.
#[allow(dead_code)]
pub const TIPC_SUB_SERVICE: u32 = 0x02;
/// `TIPC_SUB_CANCEL = 0x04` — cancel an existing subscription.
// Retained for UAPI/ABI completeness; velo uses TIPC_WAIT_FOREVER, never cancels mid-run.
#[allow(dead_code)]
pub const TIPC_SUB_CANCEL: u32 = 0x04;

/// `TIPC_WAIT_FOREVER = ~0u32` — infinite subscription duration.
pub const TIPC_WAIT_FOREVER: u32 = !0u32;

// ── Topology event types (tipc_event.event) ───────────────────────────────────

/// `TIPC_PUBLISHED = 1` — a matching service binding appeared.
pub const TIPC_PUBLISHED: u32 = 1;
/// `TIPC_WITHDRAWN = 2` — a matching service binding disappeared.
pub const TIPC_WITHDRAWN: u32 = 2;
/// `TIPC_SUBSCR_TIMEOUT = 3` — subscription timer expired.
pub const TIPC_SUBSCR_TIMEOUT: u32 = 3;

// ── Repr(C) structs (verbatim from tipc.h) ────────────────────────────────────

/// `struct tipc_socket_addr { __u32 ref; __u32 node; }` — exact socket address (8 B).
///
/// The primary connect target for velo TIPC connections; immune to anycast routing.
/// Field `ref_` is named with a trailing underscore because `ref` is a Rust keyword.
#[repr(C)]
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
pub struct TipcSocketAddr {
    /// Port reference assigned by the kernel to the bound socket.
    pub ref_: u32,
    /// 32-bit node hash; 0 if no bearer / identity is configured.
    pub node: u32,
}

/// `struct tipc_service_addr { __u32 type; __u32 instance; }` — logical service name (8 B).
///
/// Field `type_` is named with a trailing underscore because `type` is a Rust keyword.
#[repr(C)]
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
pub struct TipcServiceAddr {
    /// Service type (must be ≥ `TIPC_RESERVED_TYPES` = 64 for user applications).
    pub type_: u32,
    /// Service instance within the type.
    pub instance: u32,
}

/// `struct tipc_service_range { __u32 type; __u32 lower; __u32 upper; }` — name range (12 B).
#[repr(C)]
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
pub struct TipcServiceRange {
    /// Service type.
    pub type_: u32,
    /// Lower bound of the instance range (inclusive).
    pub lower: u32,
    /// Upper bound of the instance range (inclusive).
    pub upper: u32,
}

/// `struct { struct tipc_service_addr name; __u32 domain; }` — named-address connect (12 B).
///
/// The `domain` field was historically a scope filter; it is ignored by the kernel
/// in modern TIPC (kept for ABI compatibility).
#[repr(C)]
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
pub struct TipcServiceName {
    /// Logical service name being addressed.
    pub name: TipcServiceAddr,
    /// Legacy domain/scope filter (ignored; must be 0).
    pub domain: u32,
}

/// The `union { ... } addr` embedded in `sockaddr_tipc` (12 B).
///
/// All variants are `Copy` plain-data structs, so this union implements `Copy`.
///
/// Which variant is active depends on `SockaddrTipc::addrtype`:
/// - `TIPC_SOCKET_ADDR` → `socket_addr`
/// - `TIPC_SERVICE_ADDR` → `service_name`
/// - `TIPC_SERVICE_RANGE` → `service_range`
#[repr(C)]
#[derive(Copy, Clone)]
#[allow(dead_code)] // fields accessed via unsafe union reads
pub union TipcAddrUnion {
    /// Active when `addrtype == TIPC_SOCKET_ADDR` (8 B; upper 4 B of the 12-B union are padding).
    pub socket_addr: TipcSocketAddr,
    /// Active when `addrtype == TIPC_SERVICE_RANGE` (12 B).
    pub service_range: TipcServiceRange,
    /// Active when `addrtype == TIPC_SERVICE_ADDR` (12 B).
    pub service_name: TipcServiceName,
}

/// `struct sockaddr_tipc` — 16-byte TIPC socket address passed to `bind`/`connect`/`sendto`.
///
/// Implements `Copy` so it can be written into a `socket2::SockAddrStorage` with `*dst = src`.
///
/// Layout (verified against C `sizeof`/`offsetof` on this host):
/// ```text
/// offset 0: family   u16
/// offset 2: addrtype u8
/// offset 3: scope    i8
/// offset 4: addr     TipcAddrUnion [12 B]
/// ```
/// Total: 16 bytes.
#[repr(C)]
#[derive(Copy, Clone)]
pub struct SockaddrTipc {
    /// Address family; always `AF_TIPC` (= 30).
    pub family: u16,
    /// Which union variant is active: `TIPC_SOCKET_ADDR`, `TIPC_SERVICE_ADDR`, or `TIPC_SERVICE_RANGE`.
    pub addrtype: u8,
    /// Publication scope for `bind` (`TIPC_CLUSTER_SCOPE` or `TIPC_NODE_SCOPE`).
    /// Ignored for `connect`.
    pub scope: i8,
    /// Embedded union holding the actual address payload.
    pub addr: TipcAddrUnion,
}

/// `struct tipc_subscr` — 28-byte topology subscription request written to the topology server.
///
/// Layout verified: seq@0 (12 B), timeout@12 (4 B), filter@16 (4 B), usr_handle@20 (8 B).
#[repr(C)]
#[derive(Debug, Copy, Clone, Default)]
pub struct TipcSubscr {
    /// Service range of interest.
    pub seq: TipcServiceRange,
    /// Subscription duration in milliseconds; use `TIPC_WAIT_FOREVER` for permanent.
    pub timeout: u32,
    /// Bitmask of `TIPC_SUB_*` filter flags.
    pub filter: u32,
    /// Opaque handle returned unchanged in each `TipcEvent` for correlation.
    pub usr_handle: [u8; 8],
}

/// `struct tipc_event` — 48-byte topology event read from the topology server.
///
/// Layout verified: event@0 (4 B), found_lower@4 (4 B), found_upper@8 (4 B),
/// port@12 (8 B), s@20 (28 B).
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct TipcEvent {
    /// Event type: `TIPC_PUBLISHED`, `TIPC_WITHDRAWN`, or `TIPC_SUBSCR_TIMEOUT`.
    pub event: u32,
    /// Lower instance of the matched range.
    pub found_lower: u32,
    /// Upper instance of the matched range.
    pub found_upper: u32,
    /// Socket address (`{ref, node}`) of the publisher/subscriber.
    pub port: TipcSocketAddr,
    /// The subscription that generated this event (echoed back from the original request).
    pub s: TipcSubscr,
}

// ── Compile-time layout assertions ────────────────────────────────────────────
//
// These run on every `cargo check` / `cargo build` — no kernel module needed.
// Values verified against C `sizeof`/`offsetof` on this host (gcc, x86-64).

const _LAYOUT: () = {
    use std::mem::{offset_of, size_of};

    // Struct sizes
    assert!(size_of::<TipcSocketAddr>() == 8);
    assert!(size_of::<TipcServiceAddr>() == 8);
    assert!(size_of::<TipcServiceRange>() == 12);
    assert!(size_of::<TipcServiceName>() == 12);
    assert!(size_of::<TipcAddrUnion>() == 12);
    assert!(size_of::<SockaddrTipc>() == 16);
    assert!(size_of::<TipcSubscr>() == 28);
    assert!(size_of::<TipcEvent>() == 48);

    // SockaddrTipc field offsets
    assert!(offset_of!(SockaddrTipc, family) == 0);
    assert!(offset_of!(SockaddrTipc, addrtype) == 2);
    assert!(offset_of!(SockaddrTipc, scope) == 3);
    assert!(offset_of!(SockaddrTipc, addr) == 4);

    // TipcSubscr field offsets
    assert!(offset_of!(TipcSubscr, seq) == 0);
    assert!(offset_of!(TipcSubscr, timeout) == 12);
    assert!(offset_of!(TipcSubscr, filter) == 16);
    assert!(offset_of!(TipcSubscr, usr_handle) == 20);

    // TipcEvent field offsets
    assert!(offset_of!(TipcEvent, event) == 0);
    assert!(offset_of!(TipcEvent, found_lower) == 4);
    assert!(offset_of!(TipcEvent, found_upper) == 8);
    assert!(offset_of!(TipcEvent, port) == 12);
    assert!(offset_of!(TipcEvent, s) == 20);
};

// ── socket2 bridge helpers ────────────────────────────────────────────────────

/// Convert a `SockaddrTipc` into a `socket2::SockAddr` for use with `socket2::Socket`.
///
/// # Safety
/// The caller must ensure `addr.family == AF_TIPC` and that the `addrtype`/`addr`
/// union variant are consistent with the intended use.
pub fn tipc_to_sockaddr(addr: &SockaddrTipc) -> socket2::SockAddr {
    // socket2 0.6 uses SockAddrStorage (a newtype over sockaddr_storage).
    // We zero it, view it as our SockaddrTipc via view_as (which asserts size),
    // write the 16 bytes, then construct the SockAddr with the correct length.
    // SAFETY: SockaddrTipc is a valid TIPC sockaddr layout (ABI-verified);
    // SockAddrStorage is repr(transparent) over sockaddr_storage which is ≥ 128 B.
    let mut storage = socket2::SockAddrStorage::zeroed();
    unsafe {
        // view_as returns &mut SockaddrTipc at offset 0 of the storage buffer.
        *storage.view_as::<SockaddrTipc>() = *addr;
        socket2::SockAddr::new(
            storage,
            std::mem::size_of::<SockaddrTipc>() as socket2::socklen_t,
        )
    }
}

/// Extract the `TipcSocketAddr` (`{ref, node}`) from a `socket2::SockAddr`.
///
/// Returns `None` if the address is not an AF_TIPC socket address
/// (`addrtype != TIPC_SOCKET_ADDR`).
pub fn sockaddr_to_tipc_socket(addr: &socket2::SockAddr) -> Option<TipcSocketAddr> {
    if (addr.len() as usize) < std::mem::size_of::<SockaddrTipc>() {
        return None;
    }
    // SAFETY: we just checked the length; SockAddr's raw pointer is valid for
    // at least `addr.len()` bytes, which covers all of SockaddrTipc.
    let tipc = unsafe { &*(addr.as_ptr() as *const SockaddrTipc) };
    if tipc.family != AF_TIPC || tipc.addrtype != TIPC_SOCKET_ADDR {
        return None;
    }
    // SAFETY: addrtype == TIPC_SOCKET_ADDR, so socket_addr variant is active.
    Some(unsafe { tipc.addr.socket_addr })
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // Sizes

    #[test]
    fn layout_sockaddr_tipc_is_16_bytes() {
        assert_eq!(std::mem::size_of::<SockaddrTipc>(), 16);
    }

    #[test]
    fn layout_tipc_subscr_is_28_bytes() {
        assert_eq!(std::mem::size_of::<TipcSubscr>(), 28);
    }

    #[test]
    fn layout_tipc_event_is_48_bytes() {
        assert_eq!(std::mem::size_of::<TipcEvent>(), 48);
    }

    #[test]
    fn layout_tipc_socket_addr_is_8_bytes() {
        assert_eq!(std::mem::size_of::<TipcSocketAddr>(), 8);
    }

    #[test]
    fn layout_tipc_service_addr_is_8_bytes() {
        assert_eq!(std::mem::size_of::<TipcServiceAddr>(), 8);
    }

    #[test]
    fn layout_tipc_service_range_is_12_bytes() {
        assert_eq!(std::mem::size_of::<TipcServiceRange>(), 12);
    }

    #[test]
    fn layout_tipc_addr_union_is_12_bytes() {
        assert_eq!(std::mem::size_of::<TipcAddrUnion>(), 12);
    }

    // Field offsets — mirror the compile-time assertions for test reporting clarity

    #[test]
    fn layout_sockaddr_tipc_offsets() {
        assert_eq!(std::mem::offset_of!(SockaddrTipc, family), 0);
        assert_eq!(std::mem::offset_of!(SockaddrTipc, addrtype), 2);
        assert_eq!(std::mem::offset_of!(SockaddrTipc, scope), 3);
        assert_eq!(std::mem::offset_of!(SockaddrTipc, addr), 4);
    }

    #[test]
    fn layout_tipc_subscr_offsets() {
        assert_eq!(std::mem::offset_of!(TipcSubscr, seq), 0);
        assert_eq!(std::mem::offset_of!(TipcSubscr, timeout), 12);
        assert_eq!(std::mem::offset_of!(TipcSubscr, filter), 16);
        assert_eq!(std::mem::offset_of!(TipcSubscr, usr_handle), 20);
    }

    #[test]
    fn layout_tipc_event_offsets() {
        assert_eq!(std::mem::offset_of!(TipcEvent, event), 0);
        assert_eq!(std::mem::offset_of!(TipcEvent, found_lower), 4);
        assert_eq!(std::mem::offset_of!(TipcEvent, found_upper), 8);
        assert_eq!(std::mem::offset_of!(TipcEvent, port), 12);
        assert_eq!(std::mem::offset_of!(TipcEvent, s), 20);
    }

    // Constant values (cross-checked against /usr/include/linux/tipc.h)

    #[test]
    fn constants_af_tipc() {
        assert_eq!(AF_TIPC, 30u16);
        assert_eq!(AF_TIPC_RAW, 30i32);
    }

    #[test]
    fn constants_sol_tipc() {
        assert_eq!(SOL_TIPC, 271i32);
    }

    #[test]
    fn constants_addr_types() {
        assert_eq!(TIPC_SERVICE_RANGE, 1u8);
        assert_eq!(TIPC_SERVICE_ADDR, 2u8);
        assert_eq!(TIPC_SOCKET_ADDR, 3u8);
    }

    #[test]
    fn constants_scopes() {
        assert_eq!(TIPC_CLUSTER_SCOPE, 2i8);
        assert_eq!(TIPC_NODE_SCOPE, 3i8);
    }

    #[test]
    fn constants_reserved_types() {
        assert_eq!(TIPC_RESERVED_TYPES, 64u32);
    }

    #[test]
    fn constants_sockopts() {
        assert_eq!(TIPC_IMPORTANCE, 127i32);
        assert_eq!(TIPC_CONN_TIMEOUT, 130i32);
        assert_eq!(TIPC_NODELAY, 138i32);
    }

    #[test]
    fn constants_max_msg_size() {
        assert_eq!(TIPC_MAX_USER_MSG_SIZE, 66_000u32);
    }

    #[test]
    fn constants_topology_service_types() {
        assert_eq!(TIPC_NODE_STATE, 0u32);
        assert_eq!(TIPC_TOP_SRV, 1u32);
        assert_eq!(TIPC_LINK_STATE, 2u32);
    }

    #[test]
    fn constants_subscription_filters() {
        assert_eq!(TIPC_SUB_PORTS, 0x01u32);
        assert_eq!(TIPC_SUB_SERVICE, 0x02u32);
        assert_eq!(TIPC_SUB_CANCEL, 0x04u32);
        assert_eq!(TIPC_WAIT_FOREVER, 0xffff_ffffu32);
    }

    #[test]
    fn constants_topology_event_types() {
        assert_eq!(TIPC_PUBLISHED, 1u32);
        assert_eq!(TIPC_WITHDRAWN, 2u32);
        assert_eq!(TIPC_SUBSCR_TIMEOUT, 3u32);
    }

    // socket2 bridge round-trip: build a SockaddrTipc for a socket address,
    // convert to socket2::SockAddr, then extract back.

    #[test]
    fn bridge_socket_addr_roundtrip() {
        let tipc = SockaddrTipc {
            family: AF_TIPC,
            addrtype: TIPC_SOCKET_ADDR,
            scope: 0,
            addr: TipcAddrUnion {
                socket_addr: TipcSocketAddr {
                    ref_: 0xdeadbeef,
                    node: 0x12345678,
                },
            },
        };

        let sa = tipc_to_sockaddr(&tipc);
        assert_eq!(sa.len() as usize, std::mem::size_of::<SockaddrTipc>());

        let extracted = sockaddr_to_tipc_socket(&sa).expect("should extract socket addr");
        assert_eq!(extracted.ref_, 0xdeadbeef);
        assert_eq!(extracted.node, 0x12345678);
    }

    #[test]
    fn bridge_rejects_non_socket_addrtype() {
        let tipc = SockaddrTipc {
            family: AF_TIPC,
            addrtype: TIPC_SERVICE_RANGE, // not SOCKET_ADDR
            scope: TIPC_CLUSTER_SCOPE,
            addr: TipcAddrUnion {
                service_range: TipcServiceRange {
                    type_: 0x56454C4F,
                    lower: 0,
                    upper: 0xffff_ffff,
                },
            },
        };
        let sa = tipc_to_sockaddr(&tipc);
        assert!(sockaddr_to_tipc_socket(&sa).is_none());
    }

    // Verify the union is truly at offset 4 (i.e., the SockaddrTipc header bytes
    // are not corrupted by the union's zero bytes when we zero-init the storage).
    #[test]
    fn bridge_service_range_bytes_preserved() {
        let tipc = SockaddrTipc {
            family: AF_TIPC,
            addrtype: TIPC_SERVICE_RANGE,
            scope: TIPC_CLUSTER_SCOPE,
            addr: TipcAddrUnion {
                service_range: TipcServiceRange {
                    type_: 0x1234_5678,
                    lower: 100,
                    upper: 200,
                },
            },
        };
        let sa = tipc_to_sockaddr(&tipc);
        // Cast back to SockaddrTipc and inspect
        let back = unsafe { &*(sa.as_ptr() as *const SockaddrTipc) };
        assert_eq!(back.family, AF_TIPC);
        assert_eq!(back.addrtype, TIPC_SERVICE_RANGE);
        let sr = unsafe { back.addr.service_range };
        assert_eq!(sr.type_, 0x1234_5678);
        assert_eq!(sr.lower, 100);
        assert_eq!(sr.upper, 200);
    }
}
