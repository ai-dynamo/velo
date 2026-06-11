// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! `tipc-doctor` — diagnostic tool for the TIPC transport layer.
//!
//! Reports:
//! - TIPC kernel module availability
//! - Local node identity, socket ref, netid, and netns nonce
//! - Bearer list and node list (via the `tipc` iproute2 tool if present)
//! - Current VELO-type service publications from the TIPC name table
//!
//! Usage:
//! ```text
//! cargo run -p velo-examples --features tipc --example tipc_doctor
//! ```
//!
//! Gated: compiles and runs only on Linux with `--features tipc`.

fn main() {
    #[cfg(target_os = "linux")]
    run_doctor();

    #[cfg(not(target_os = "linux"))]
    eprintln!("tipc-doctor: this tool is Linux-only.");
}

// ── Linux implementation ──────────────────────────────────────────────────────

#[cfg(target_os = "linux")]
fn run_doctor() {
    println!("=== TIPC doctor ===\n");

    // 1. Module availability ---------------------------------------------------
    if !tipc_available() {
        println!("TIPC module:  NOT available");
        println!("  To load:    sudo modprobe tipc");
        println!("  To persist: echo tipc | sudo tee /etc/modules-load.d/tipc.conf");
        return;
    }
    println!("TIPC module:  available\n");

    // 2. Local node identity --------------------------------------------------
    show_identity();

    // 3. Bearer and node list (requires iproute2 tipc tool) -------------------
    show_bearers();

    // 4. VELO-type service publications in the name table ---------------------
    show_velo_publications();
}

// ── Module availability ───────────────────────────────────────────────────────

/// Probe whether `tipc.ko` is loaded by attempting to open an AF_TIPC socket.
///
/// Returns `false` on `EAFNOSUPPORT` (module absent).  The probe is < 1 µs
/// and has no side effects — the socket is closed immediately.
#[cfg(target_os = "linux")]
fn tipc_available() -> bool {
    // AF_TIPC = 30, SOCK_RDM | SOCK_CLOEXEC
    let fd = unsafe { libc::socket(30, libc::SOCK_RDM | libc::SOCK_CLOEXEC, 0) };
    if fd < 0 {
        return false;
    }
    unsafe { libc::close(fd) };
    true
}

// ── Identity ──────────────────────────────────────────────────────────────────

/// Print the local node identity derived from a temporary TipcTransport.
///
/// `TipcTransportBuilder::new().build()` binds a VELO-type service range,
/// computes the netns nonce, and exposes the socket address via `address()`.
/// The transport is dropped immediately after we read the endpoint — this
/// withdraws the ephemeral publication from the name table.
#[cfg(target_os = "linux")]
fn show_identity() {
    use velo::ext::WorkerAddress;
    use velo::transports::Transport;
    use velo::transports::tipc::{TipcEndpoint, TipcTransportBuilder};

    let transport = match TipcTransportBuilder::new().build() {
        Ok(t) => t,
        Err(e) => {
            println!("Identity:     could not build transport: {e}");
            return;
        }
    };

    let addr: WorkerAddress = transport.address();
    let ep: TipcEndpoint = match addr.get_entry("tipc") {
        Ok(Some(b)) => match rmp_serde::from_slice(b.as_ref()) {
            Ok(ep) => ep,
            Err(e) => {
                println!("Identity:     failed to decode endpoint: {e}");
                return;
            }
        },
        _ => {
            println!("Identity:     no TIPC entry in WorkerAddress");
            return;
        }
    };

    println!("Identity");
    println!("  socket_ref:       {:#010x}", ep.socket_ref);
    println!("  node:             {:#010x}", ep.node);
    println!("  service_type:     {:#010x}  (VELO = 0x56454C4F)", ep.service_type);
    println!("  service_instance: {:#010x}  (per-process random)", ep.service_instance);
    println!("  netid:            {}  (velo assumes 4711 unless .netid() overridden)", ep.netid);
    println!("  netns_nonce:      {:#018x}", ep.netns_nonce);
    println!(
        "  scope:            {}  (2 = TIPC_CLUSTER_SCOPE, 3 = TIPC_NODE_SCOPE)",
        ep.scope
    );
    println!();

    // Drop the transport here — withdraws the ephemeral service publication.
    drop(transport);
}

// ── Bearers ───────────────────────────────────────────────────────────────────

/// Shell out to the `tipc` iproute2 tool for bearer and node lists.
///
/// Degrades gracefully when the binary is absent — prints a hint instead.
#[cfg(target_os = "linux")]
fn show_bearers() {
    use std::process::Command;

    // Check whether the tipc tool exists.
    let has_tipc = Command::new("which")
        .arg("tipc")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);

    if !has_tipc {
        println!("Bearers:      'tipc' tool not found (install iproute2-tipc or iproute2)");
        println!();
        return;
    }

    println!("Bearers");
    run_tipc_cmd(&["bearer", "list"]);
    println!();

    println!("Nodes");
    run_tipc_cmd(&["node", "list"]);
    println!();
}

/// Run `tipc <args>` and print its output, prefixed with two spaces per line.
#[cfg(target_os = "linux")]
fn run_tipc_cmd(args: &[&str]) {
    use std::process::Command;

    match Command::new("tipc").args(args).output() {
        Ok(out) if out.status.success() => {
            let text = String::from_utf8_lossy(&out.stdout);
            for line in text.lines() {
                println!("  {line}");
            }
            if text.trim().is_empty() {
                println!("  (none)");
            }
        }
        Ok(out) => {
            let stderr = String::from_utf8_lossy(&out.stderr);
            println!("  (command failed: {})", stderr.trim());
        }
        Err(e) => {
            println!("  (could not run tipc: {e})");
        }
    }
}

// ── VELO publications ─────────────────────────────────────────────────────────

/// Query the TIPC topology server for current VELO-type service publications.
///
/// Subscribes to service range `{VELO_TYPE, 0, 0xFFFFFFFF}` with a 200 ms
/// one-shot timeout and prints every `TIPC_PUBLISHED` event.  Uses raw blocking
/// libc calls; no async runtime is needed for this one-shot operation.
#[cfg(target_os = "linux")]
fn show_velo_publications() {
    println!("VELO publications (TIPC_TOP_SRV, timeout 200 ms)");

    // ── Inline TIPC layout types (mirrors velo::transports::tipc::sys) ────

    const AF_TIPC: i32 = 30;
    const TIPC_TOP_SRV: u32 = 1; // topology server service type
    const TIPC_SERVICE_ADDR: u8 = 2;
    const TIPC_CLUSTER_SCOPE: i8 = 2;
    const TIPC_SUB_PORTS: u32 = 0x01;
    const TIPC_PUBLISHED: u32 = 1;
    const TIPC_SUBSCR_TIMEOUT: u32 = 3;
    const VELO_SERVICE_TYPE: u32 = 0x5645_4C4F; // "VELO"

    // Minimal 16-byte TIPC sockaddr for a service address
    #[repr(C)]
    struct TipcSaService {
        family: u16,
        addrtype: u8,
        scope: i8,
        svc_type: u32,
        instance: u32,
        domain: u32,
    }

    // 28-byte subscription message
    #[repr(C)]
    struct TipcSubscr {
        seq_type: u32,       // service range: type
        seq_lower: u32,      // service range: lower
        seq_upper: u32,      // service range: upper
        timeout_ms: u32,
        filter: u32,
        usr_handle: [u8; 8],
    }

    // 48-byte event reply
    #[repr(C)]
    #[derive(Default)]
    struct TipcEvent {
        event: u32,
        found_lower: u32,
        found_upper: u32,
        port_ref: u32,
        port_node: u32,
        subscr_seq_type: u32,
        subscr_seq_lower: u32,
        subscr_seq_upper: u32,
        subscr_timeout: u32,
        subscr_filter: u32,
        subscr_handle: [u8; 8],
    }

    assert_eq!(std::mem::size_of::<TipcSubscr>(), 28);
    assert_eq!(std::mem::size_of::<TipcEvent>(), 48);

    unsafe {
        // ── Connect a SEQPACKET socket to the topology server ─────────────
        let fd = libc::socket(AF_TIPC, libc::SOCK_SEQPACKET | libc::SOCK_CLOEXEC, 0);
        if fd < 0 {
            println!("  (failed to create SEQPACKET socket: {})",
                std::io::Error::last_os_error());
            return;
        }

        let sa = TipcSaService {
            family: AF_TIPC as u16,
            addrtype: TIPC_SERVICE_ADDR,
            scope: TIPC_CLUSTER_SCOPE,
            svc_type: TIPC_TOP_SRV,
            instance: TIPC_TOP_SRV,
            domain: 0,
        };

        if libc::connect(
            fd,
            &sa as *const TipcSaService as *const libc::sockaddr,
            std::mem::size_of::<TipcSaService>() as libc::socklen_t,
        ) < 0
        {
            println!("  (connect to topology server failed: {})",
                std::io::Error::last_os_error());
            libc::close(fd);
            return;
        }

        // Set a 300 ms receive timeout so we don't hang if the server is slow.
        let tv = libc::timeval { tv_sec: 0, tv_usec: 300_000 };
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVTIMEO,
            &tv as *const libc::timeval as *const libc::c_void,
            std::mem::size_of::<libc::timeval>() as libc::socklen_t,
        );

        // ── Send subscription: all VELO services, 200 ms one-shot ─────────
        let subscr = TipcSubscr {
            seq_type: VELO_SERVICE_TYPE,
            seq_lower: 0,
            seq_upper: 0xFFFF_FFFF,
            timeout_ms: 200,
            filter: TIPC_SUB_PORTS,
            usr_handle: [0u8; 8],
        };

        if libc::send(
            fd,
            &subscr as *const TipcSubscr as *const libc::c_void,
            std::mem::size_of::<TipcSubscr>(),
            0,
        ) < 0
        {
            println!("  (send subscription failed: {})",
                std::io::Error::last_os_error());
            libc::close(fd);
            return;
        }

        // ── Read events until TIPC_SUBSCR_TIMEOUT ─────────────────────────
        let mut found = 0usize;
        loop {
            let mut ev = TipcEvent::default();
            let n = libc::recv(
                fd,
                &mut ev as *mut TipcEvent as *mut libc::c_void,
                std::mem::size_of::<TipcEvent>(),
                0,
            );

            if n < 0 {
                let code = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
                if code == libc::EAGAIN || code == libc::EWOULDBLOCK || code == libc::ETIMEDOUT {
                    break; // timeout: no more events
                }
                println!("  (recv error: {})", std::io::Error::last_os_error());
                break;
            }
            if n == 0 {
                break; // server closed connection
            }
            if n as usize != std::mem::size_of::<TipcEvent>() {
                break; // short read — unexpected
            }

            match ev.event {
                e if e == TIPC_PUBLISHED => {
                    found += 1;
                    println!(
                        "  PUBLISHED  service_instance={:#010x}  \
                         port={{ref={:#010x}, node={:#010x}}}",
                        ev.found_lower,
                        ev.port_ref,
                        ev.port_node,
                    );
                }
                e if e == TIPC_SUBSCR_TIMEOUT => break,
                _ => {} // TIPC_WITHDRAWN or unknown; skip
            }
        }

        if found == 0 {
            println!("  (no VELO services currently published)");
        }

        libc::close(fd);
    }
    println!();
}
