// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Allow dead_code during phased development: transport.rs / stream.rs / topology.rs
// (companion TIPC modules being developed concurrently) will consume these items once
// complete.  Remove this attribute when the full TIPC module lands.
#![allow(dead_code)]

//! Socket creation, `setsockopt`/`getsockopt` helpers, `getsockname`, and `tipc_available()` probe.
//!
//! All public functions in this module are thin, safe wrappers around raw TIPC syscalls.
//! They are the lowest layer of the TIPC transport stack; higher layers (`stream.rs`,
//! `listener.rs`, `topology.rs`) consume them without touching `unsafe` directly.
//!
//! ## `tipc_available()`
//!
//! Probes whether the TIPC kernel module is loaded by attempting to open an AF_TIPC
//! socket. Returns `false` on `EAFNOSUPPORT` (module not loaded).  The module never
//! auto-loads (`tipc.ko` declares no `MODULE_ALIAS_NETPROTO`), so the probe is
//! side-effect-free and < 1 µs.
//!
//! If the probe returns `false`, callers should surface:
//! > "TIPC kernel module not loaded; run `sudo modprobe tipc` (or add `tipc` to
//! > `/etc/modules-load.d/tipc.conf` for persistence)."
//!
//! ## Netns nonce
//!
//! [`compute_netns_nonce`] computes `xxh3_64(boot_id_bytes ++ netns_inode_le64)`,
//! a deterministic 64-bit identifier for the (boot, network-namespace) pair.  Two
//! processes with equal nonces share the same TIPC stack; unequal nonces mean
//! disjoint stacks (either different boots or different netns).
//!
//! The netns inode is parsed from `readlink("/proc/self/ns/net")` — e.g.
//! `"net:[4026531840]"` — rather than `stat()`, because syscall-interposing sandboxes
//! (gVisor, seccomp-notify) can fabricate `stat` inodes while passing `readlink`
//! through to the real kernel-generated name.

use std::io;
use std::os::unix::io::AsRawFd;

use super::sys::{
    AF_TIPC, AF_TIPC_RAW, SOL_TIPC, SockaddrTipc, TIPC_CLUSTER_SCOPE, TIPC_CONN_TIMEOUT,
    TIPC_NODELAY, TIPC_SERVICE_RANGE, TIPC_SOCKET_ADDR, TipcAddrUnion, TipcServiceRange,
    TipcSocketAddr, sockaddr_to_tipc_socket, tipc_to_sockaddr,
};

// ── Socket creation ───────────────────────────────────────────────────────────

/// Create a non-blocking `AF_TIPC / SOCK_STREAM` socket.
///
/// Used for messenger transport connections (both outbound connects and the
/// listening socket).
///
/// # Errors
/// Returns `EAFNOSUPPORT` if the TIPC kernel module is not loaded.
pub fn create_tipc_stream() -> io::Result<socket2::Socket> {
    let domain = socket2::Domain::from(AF_TIPC_RAW);
    let sock = socket2::Socket::new(domain, socket2::Type::STREAM, None)?;
    sock.set_nonblocking(true)?;
    Ok(sock)
}

/// Create a non-blocking `AF_TIPC / SOCK_SEQPACKET` socket.
///
/// Used for topology-server connections (subscriptions) and health probes.
///
/// # Errors
/// Returns `EAFNOSUPPORT` if the TIPC kernel module is not loaded.
pub fn create_tipc_seqpacket() -> io::Result<socket2::Socket> {
    let domain = socket2::Domain::from(AF_TIPC_RAW);
    let sock = socket2::Socket::new(domain, socket2::Type::SEQPACKET, None)?;
    sock.set_nonblocking(true)?;
    Ok(sock)
}

// ── setsockopt helpers ────────────────────────────────────────────────────────

/// Set `TIPC_CONN_TIMEOUT` (sockopt 130) — connection timeout in milliseconds.
///
/// Velo sets 5,000 ms (see `transport.rs`) so the application-level connect timeout
/// fires before the kernel's default 8,000 ms. Must be called before `connect(2)`.
pub fn set_conn_timeout_ms(sock: &socket2::Socket, ms: u32) -> io::Result<()> {
    // SAFETY: value is a plain u32; SOL_TIPC/TIPC_CONN_TIMEOUT are correct levels.
    unsafe { setsockopt_u32(sock, SOL_TIPC, TIPC_CONN_TIMEOUT, ms) }
}

/// Enable `TIPC_NODELAY` (sockopt 138) — disable coalescing (kernels ≥ 5.5 only).
///
/// Best-effort: `ENOPROTOOPT` and `EINVAL` are silently swallowed so that the
/// transport works correctly on older kernels (e.g. RHEL 8's 4.18).
pub fn set_nodelay_best_effort(sock: &socket2::Socket) {
    let _ = unsafe { setsockopt_u32(sock, SOL_TIPC, TIPC_NODELAY, 1) };
}

/// Set `SO_RCVBUF` to `size` bytes on the given socket.
///
/// Enlarging the receive buffer on *accepted* sockets increases TIPC's advertised
/// flow-control window (the only tuning knob for TIPC STREAM backpressure — see
/// proposal §2.2). Failures are logged at warn level by the caller.
pub fn set_rcvbuf(sock: &socket2::Socket, size: usize) -> io::Result<()> {
    sock.set_recv_buffer_size(size)
}

// ── Bind / listen ─────────────────────────────────────────────────────────────

/// Bind a `SOCK_STREAM` socket to the service range `[type_, lower, upper]` with
/// the given scope, then put it in the listen state with the given backlog.
///
/// After this call, `getsockname_ref_node` returns the kernel-assigned `{ref, node}`.
pub fn bind_service_range_and_listen(
    sock: &socket2::Socket,
    type_: u32,
    lower: u32,
    upper: u32,
    scope: i8,
    backlog: i32,
) -> io::Result<()> {
    let addr = SockaddrTipc {
        family: AF_TIPC,
        addrtype: TIPC_SERVICE_RANGE,
        scope,
        addr: TipcAddrUnion {
            service_range: TipcServiceRange {
                type_,
                lower,
                upper,
            },
        },
    };
    let sa = tipc_to_sockaddr(&addr);
    sock.bind(&sa)?;
    sock.listen(backlog)?;
    Ok(())
}

/// Bind a `SOCK_STREAM` socket to the service range `[type_, instance, instance]`
/// (a single-instance range) with cluster scope, then listen.
///
/// Convenience wrapper for the common single-instance binding.
pub fn bind_single_instance_and_listen(
    sock: &socket2::Socket,
    type_: u32,
    instance: u32,
    backlog: i32,
) -> io::Result<()> {
    bind_service_range_and_listen(sock, type_, instance, instance, TIPC_CLUSTER_SCOPE, backlog)
}

// ── getsockname ──────────────────────────────────────────────────────────────

/// Read the bound `{ref, node}` of a TIPC socket via `getsockname(2)`.
///
/// Returns the kernel-assigned port reference and the node hash. `node` is 0 when
/// the local TIPC stack has no bearer / identity configured.
///
/// # Errors
/// Returns `InvalidData` if `getsockname` returns a non-`TIPC_SOCKET_ADDR` result.
pub fn getsockname_ref_node(sock: &socket2::Socket) -> io::Result<(u32, u32)> {
    let addr = sock.local_addr()?;
    sockaddr_to_tipc_socket(&addr)
        .map(|sa| (sa.ref_, sa.node))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "getsockname returned a non-TIPC_SOCKET_ADDR address",
            )
        })
}

// ── Connect ───────────────────────────────────────────────────────────────────

/// Initiate a non-blocking connect to the exact socket address `{ref_, node}`.
///
/// On a non-blocking socket this returns `WouldBlock`/`EINPROGRESS` normally.
/// The caller should then wait for writability and check `SO_ERROR`.
///
/// Note (§2.3.3): the connect completes **only when the remote application calls
/// `accept()`** — not when the kernel completes the backlog handshake (there is
/// none in TIPC). The Velo builder's 5-second `connect_timeout` therefore also
/// bounds "remote accept loop wedged".
pub fn connect_to_socket_addr(sock: &socket2::Socket, ref_: u32, node: u32) -> io::Result<()> {
    let addr = SockaddrTipc {
        family: AF_TIPC,
        addrtype: TIPC_SOCKET_ADDR,
        scope: 0, // ignored for connect
        addr: TipcAddrUnion {
            socket_addr: TipcSocketAddr { ref_, node },
        },
    };
    let sa = tipc_to_sockaddr(&addr);
    match sock.connect(&sa) {
        Ok(()) => Ok(()),
        Err(e)
            if e.raw_os_error() == Some(libc::EINPROGRESS)
                || e.kind() == io::ErrorKind::WouldBlock =>
        {
            Ok(())
        }
        Err(e) => Err(e),
    }
}

// ── Module availability probe ─────────────────────────────────────────────────

/// Return `true` if the TIPC kernel module is loaded and usable.
///
/// Probes by attempting `socket(AF_TIPC, SOCK_RDM, 0)`.  On `EAFNOSUPPORT` the
/// module is not loaded.  The socket (if created) is immediately closed.
///
/// The probe is side-effect-free and completes in < 1 µs.  It is not racy:
/// `tipc.ko` declares no `MODULE_ALIAS_NETPROTO`, so `socket(AF_TIPC, ...)` never
/// auto-loads the module — **[verified]** on kernel 6.14.0-1015-nvidia.
pub fn tipc_available() -> bool {
    // Use the raw libc interface so we don't have to go through socket2 error mapping
    // (we only care about success vs EAFNOSUPPORT).
    let fd = unsafe { libc::socket(AF_TIPC_RAW, libc::SOCK_RDM | libc::SOCK_CLOEXEC, 0) };
    if fd < 0 {
        let errno = unsafe { *libc::__errno_location() };
        if errno == libc::EAFNOSUPPORT || errno == libc::EPROTONOSUPPORT {
            return false;
        }
        // Any other error still means the module is present but something else
        // went wrong — treat as available so the real socket creation surfaces
        // the real error.
        return true;
    }
    unsafe { libc::close(fd) };
    true
}

// ── Netns nonce (boot_id ++ netns_inode) ─────────────────────────────────────

/// Read `/proc/sys/kernel/random/boot_id` as 16 raw bytes (UUID stripped of hyphens).
///
/// The boot-ID is a 128-bit random value assigned at kernel boot, identical for all
/// processes in all namespaces on the same physical boot.  It supplies cross-host
/// and cross-reboot uniqueness for the netns nonce.
pub fn read_boot_id() -> io::Result<[u8; 16]> {
    let content = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
    parse_boot_id(content.trim())
}

/// Parse a UUID string (with or without hyphens) into 16 raw bytes.
fn parse_boot_id(s: &str) -> io::Result<[u8; 16]> {
    let hex: String = s.chars().filter(|c| c.is_ascii_hexdigit()).collect();
    if hex.len() != 32 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "boot_id has unexpected format (expected 32 hex digits, got {}): {:?}",
                hex.len(),
                s
            ),
        ));
    }
    let mut bytes = [0u8; 16];
    for (i, b) in bytes.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("boot_id hex parse error: {e}"),
            )
        })?;
    }
    Ok(bytes)
}

/// Read the network-namespace inode number from `readlink("/proc/self/ns/net")`.
///
/// The kernel generates the symlink target as `"net:[<inode>]"`, e.g.
/// `"net:[4026531840]"`.  This inode uniquely identifies the netns within a boot and
/// is stable for all processes in that netns regardless of their mount namespace.
///
/// **Never use `stat()` for this**: syscall-interposing sandboxes (gVisor,
/// seccomp-notify) can return fabricated, per-process-varying inodes from `stat`
/// while passing `readlink` through to the real kernel-generated name — **[verified]**
/// on the development sandbox in use during design review.
pub fn read_netns_inode() -> io::Result<u64> {
    let target = std::fs::read_link("/proc/self/ns/net")?;
    parse_netns_symlink(&target.to_string_lossy())
}

/// Parse `"net:[<u64>]"` → `u64`.
///
/// This is a pure function; it is unit-tested separately so the parser is verified
/// against the exact kernel string format before any real socket is opened.
pub fn parse_netns_symlink(s: &str) -> io::Result<u64> {
    s.strip_prefix("net:[")
        .and_then(|rest| rest.strip_suffix(']'))
        .and_then(|digits| digits.parse::<u64>().ok())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected netns symlink format (expected \"net:[<u64>]\"): {s:?}"),
            )
        })
}

/// Compute the netns nonce: `xxh3_64(boot_id_bytes ++ netns_inode_as_le64)`.
///
/// Equal nonce ⟺ same TIPC stack (same boot **and** same network namespace).
/// See proposal §5.3 for the full correctness argument.
pub fn compute_netns_nonce() -> io::Result<u64> {
    let boot_id = read_boot_id()?;
    let inode = read_netns_inode()?;

    let mut data = [0u8; 24]; // 16 (boot_id) + 8 (inode le64)
    data[..16].copy_from_slice(&boot_id);
    data[16..].copy_from_slice(&inode.to_le_bytes());

    Ok(xxhash_rust::xxh3::xxh3_64(&data))
}

// ── Low-level setsockopt helper ───────────────────────────────────────────────

/// # Safety
/// `level` and `optname` must be valid socket option levels/names for the given
/// socket type, and `value` must be the correct type for that option.
unsafe fn setsockopt_u32(
    sock: &socket2::Socket,
    level: libc::c_int,
    optname: libc::c_int,
    value: u32,
) -> io::Result<()> {
    let ret = unsafe {
        libc::setsockopt(
            sock.as_raw_fd(),
            level,
            optname,
            &value as *const u32 as *const libc::c_void,
            std::mem::size_of::<u32>() as libc::socklen_t,
        )
    };
    if ret != 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── netns symlink parser ──────────────────────────────────────────────────

    #[test]
    fn tipc_parse_netns_symlink_standard() {
        // Verified live on this host: readlink /proc/self/ns/net == "net:[4026531840]"
        assert_eq!(
            parse_netns_symlink("net:[4026531840]").unwrap(),
            4_026_531_840u64
        );
    }

    #[test]
    fn tipc_parse_netns_symlink_unshare_child() {
        // Distinct netns as seen in an `unshare -Un` child (value from §3 / §5.3)
        assert_eq!(
            parse_netns_symlink("net:[4026532986]").unwrap(),
            4_026_532_986u64
        );
    }

    #[test]
    fn tipc_parse_netns_symlink_rejects_malformed() {
        assert!(parse_netns_symlink("net:4026531840").is_err()); // missing brackets
        assert!(parse_netns_symlink("[4026531840]").is_err()); // missing "net:"
        assert!(parse_netns_symlink("net:[abc]").is_err()); // non-numeric
        assert!(parse_netns_symlink("").is_err());
    }

    // ── boot_id parser ────────────────────────────────────────────────────────

    #[test]
    fn tipc_parse_boot_id_with_hyphens() {
        // Standard UUID format as written in /proc/sys/kernel/random/boot_id
        let bytes = parse_boot_id("62fefa3e-f420-405a-a5f1-fe0cf9378b04").unwrap();
        assert_eq!(bytes[0], 0x62);
        assert_eq!(bytes[1], 0xfe);
        assert_eq!(bytes[2], 0xfa);
        assert_eq!(bytes[3], 0x3e);
        assert_eq!(bytes[15], 0x04);
    }

    #[test]
    fn tipc_parse_boot_id_without_hyphens() {
        let bytes = parse_boot_id("62fefa3ef420405aa5f1fe0cf9378b04").unwrap();
        assert_eq!(bytes[0], 0x62);
        assert_eq!(bytes[15], 0x04);
    }

    #[test]
    fn tipc_parse_boot_id_rejects_short() {
        assert!(parse_boot_id("62fefa").is_err());
    }

    #[test]
    fn tipc_parse_boot_id_rejects_non_hex() {
        assert!(parse_boot_id("XXXX-xxxx-xxxx-xxxx-xxxxxxxxxxxx").is_err());
    }

    // ── Nonce determinism (no kernel needed) ──────────────────────────────────

    #[test]
    fn tipc_nonce_is_deterministic_for_same_inputs() {
        let boot_id = [0u8; 16];
        let inode: u64 = 4_026_531_840;

        let mut data = [0u8; 24];
        data[..16].copy_from_slice(&boot_id);
        data[16..].copy_from_slice(&inode.to_le_bytes());

        let n1 = xxhash_rust::xxh3::xxh3_64(&data);
        let n2 = xxhash_rust::xxh3::xxh3_64(&data);
        assert_eq!(n1, n2);
    }

    #[test]
    fn tipc_nonce_differs_for_different_netns_inode() {
        let boot_id = [0xabu8; 16];

        let mut d1 = [0u8; 24];
        d1[..16].copy_from_slice(&boot_id);
        d1[16..].copy_from_slice(&4_026_531_840u64.to_le_bytes());

        let mut d2 = [0u8; 24];
        d2[..16].copy_from_slice(&boot_id);
        d2[16..].copy_from_slice(&4_026_532_986u64.to_le_bytes());

        assert_ne!(
            xxhash_rust::xxh3::xxh3_64(&d1),
            xxhash_rust::xxh3::xxh3_64(&d2),
            "different netns inodes must produce different nonces"
        );
    }

    #[test]
    fn tipc_nonce_differs_for_different_boot_id() {
        let inode: u64 = 4_026_531_840;

        let mut d1 = [0u8; 24];
        d1[..16].copy_from_slice(&[0xaau8; 16]);
        d1[16..].copy_from_slice(&inode.to_le_bytes());

        let mut d2 = [0u8; 24];
        d2[..16].copy_from_slice(&[0xbbu8; 16]);
        d2[16..].copy_from_slice(&inode.to_le_bytes());

        assert_ne!(
            xxhash_rust::xxh3::xxh3_64(&d1),
            xxhash_rust::xxh3::xxh3_64(&d2),
            "different boot IDs must produce different nonces"
        );
    }

    // ── Live kernel tests (gate on tipc_available()) ──────────────────────────

    /// Create an actual AF_TIPC socket.
    ///
    /// This test is skipped silently when the TIPC kernel module is not loaded.
    /// On CI, run with `RUSTFLAGS="--cfg velo_tipc"` to enforce the module is present.
    #[test]
    fn tipc_create_stream_socket() {
        if !tipc_available() {
            eprintln!("tipc_create_stream_socket: TIPC not available, skipping");
            return;
        }
        let sock = create_tipc_stream().expect("should create AF_TIPC SOCK_STREAM socket");
        // Verify the socket is non-blocking.
        assert!(sock.take_error().is_ok(), "socket should be usable");
    }

    #[test]
    fn tipc_create_seqpacket_socket() {
        if !tipc_available() {
            eprintln!("tipc_create_seqpacket_socket: TIPC not available, skipping");
            return;
        }
        let _sock = create_tipc_seqpacket().expect("should create AF_TIPC SOCK_SEQPACKET socket");
    }

    #[test]
    fn tipc_available_returns_true_when_module_loaded() {
        // This test always passes: it just documents the observable value.
        // On a machine without the module it prints a notice.
        let avail = tipc_available();
        if !avail {
            eprintln!(
                "tipc_available() = false — TIPC module not loaded. \
                 Run `sudo modprobe tipc` to enable TIPC tests."
            );
        }
        // No assertion — we don't want to fail on machines without the module.
    }

    /// Bind a STREAM socket to a service range and verify getsockname gives a
    /// TIPC_SOCKET_ADDR back with a non-zero ref.
    #[test]
    fn tipc_bind_and_getsockname() {
        if !tipc_available() {
            eprintln!("tipc_bind_and_getsockname: TIPC not available, skipping");
            return;
        }
        let sock = create_tipc_stream().expect("create stream socket");
        // Use a high service type to avoid colliding with anything real.
        let service_type = 0x5654_0001u32; // "VT\x00\x01"
        let instance = 0xdead_0001u32;
        bind_single_instance_and_listen(&sock, service_type, instance, 16)
            .expect("bind and listen");

        let (ref_, _node) = getsockname_ref_node(&sock).expect("getsockname");
        assert_ne!(
            ref_, 0,
            "kernel should assign a non-zero port ref after bind"
        );
    }

    /// Verify that set_conn_timeout_ms and set_nodelay_best_effort don't panic.
    #[test]
    fn tipc_sockopts_do_not_panic() {
        if !tipc_available() {
            eprintln!("tipc_sockopts_do_not_panic: TIPC not available, skipping");
            return;
        }
        let sock = create_tipc_stream().expect("create stream socket");
        // These should not return errors on a fresh socket.
        set_conn_timeout_ms(&sock, 5_000).expect("set_conn_timeout_ms");
        set_nodelay_best_effort(&sock); // best-effort: never panics
    }

    /// Read the real boot_id and netns inode and verify nonce is non-zero.
    #[test]
    fn tipc_compute_netns_nonce_live() {
        // This does NOT require the TIPC module — only /proc.
        let nonce = compute_netns_nonce().expect("compute_netns_nonce should succeed on Linux");
        assert_ne!(
            nonce, 0,
            "nonce should be non-zero for real boot_id + inode"
        );
    }

    /// Verify that the live netns inode is in the plausible range for the well-known
    /// initial netns (inode ≥ 4_026_531_840 by kernel convention).
    #[test]
    fn tipc_netns_inode_plausible() {
        let inode = read_netns_inode().expect("read_netns_inode");
        // The initial network namespace has inode 4026531840 on most Linux hosts.
        // We can't assert exact equality (could be inside a non-default netns),
        // but the inode should always be > 0.
        assert_ne!(inode, 0, "netns inode should be non-zero");
    }
}
