// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! UDP sockets and the advertised address for the QUIC transport.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use socket2::{Domain, Protocol, Socket, Type};
use tracing::warn;

use crate::transports::utils::interfaces::InterfaceEndpoint;

use super::tls::Fingerprint;

/// The QUIC entry of a `WorkerAddress`: where to dial, and which certificate
/// to accept there.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuicEndpointInfo {
    /// Candidate addresses, one per advertised interface.
    pub endpoints: Vec<InterfaceEndpoint>,
    /// SHA-256 of the listener's certificate.
    pub fingerprint: Fingerprint,
}

impl QuicEndpointInfo {
    /// Encode as MessagePack for a `WorkerAddress` entry.
    pub fn encode(&self) -> Result<Vec<u8>> {
        // Named fields, so a field can be added later without breaking peers.
        rmp_serde::to_vec_named(self).context("failed to encode the QUIC endpoint")
    }

    /// Decode a `WorkerAddress` entry.
    pub fn decode(raw: &[u8]) -> Result<Self> {
        rmp_serde::from_slice(raw).context("failed to decode the QUIC endpoint")
    }
}

/// Requested and effective UDP buffer sizes of one socket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct BufferSizes {
    pub(super) recv: usize,
    pub(super) send: usize,
}

/// Bind the server sockets: `count` sockets on one port.
///
/// With `count > 1` (Linux only), the sockets form a `SO_REUSEPORT` group.
/// The kernel hashes each client 4-tuple to one socket, so one connection
/// always lands on the same endpoint, and the receive queues and buffer
/// ceilings of all sockets add up. A single socket is capped by
/// `net.core.rmem_max`, which drops datagrams under a burst from many peers.
///
/// The first socket binds without `SO_REUSEPORT` and joins after the bind.
/// With a requested port of 0, this stops the kernel from picking a port
/// that an unrelated reuse-port group already holds.
pub(super) fn bind_server_sockets(
    requested: SocketAddr,
    count: usize,
    buffers: BufferSizes,
) -> Result<Vec<std::net::UdpSocket>> {
    let count = if cfg!(target_os = "linux") {
        count.max(1)
    } else {
        1
    };
    let first = new_udp_socket(requested)?;
    size_buffers(&first, buffers);
    first
        .bind(&requested.into())
        .with_context(|| format!("failed to bind QUIC socket on {requested}"))?;
    // SO_REUSEPORT alone forms the group. SO_REUSEADDR is left off: on Linux
    // two UDP sockets that both set it skip the port conflict check, so any
    // process could bind the group's port.
    #[cfg(target_os = "linux")]
    if count > 1 {
        first.set_reuse_port(true)?;
    }
    let bound: SocketAddr = first
        .local_addr()?
        .as_socket()
        .context("QUIC socket has no IP address")?;

    let mut sockets = Vec::with_capacity(count);
    sockets.push(first.into());
    for index in 1..count {
        let socket = new_udp_socket(bound)?;
        size_buffers(&socket, buffers);
        #[cfg(target_os = "linux")]
        socket.set_reuse_port(true)?;
        socket
            .bind(&bound.into())
            .with_context(|| format!("failed to bind QUIC reuse-port socket {index} on {bound}"))?;
        sockets.push(socket.into());
    }
    Ok(sockets)
}

/// Bind the socket that dials peers.
///
/// It is separate from the server sockets and has its own ephemeral port. A
/// dial from a reuse-port member would get its replies hashed to any member
/// of the group, and a member that does not own the connection drops them.
pub(super) fn bind_client_socket(
    server: SocketAddr,
    buffers: BufferSizes,
) -> Result<std::net::UdpSocket> {
    let unspecified = match server.ip() {
        IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
    };
    let addr = SocketAddr::new(unspecified, 0);
    let socket = new_udp_socket(addr)?;
    size_buffers(&socket, buffers);
    socket
        .bind(&addr.into())
        .context("failed to bind the QUIC client socket")?;
    Ok(socket.into())
}

fn new_udp_socket(addr: SocketAddr) -> Result<Socket> {
    let socket = Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))
        .context("failed to create a UDP socket")?;
    socket.set_nonblocking(true)?;
    Ok(socket)
}

/// Request `buffers`, then read back what the kernel granted.
///
/// Linux silently clamps a request to `net.core.rmem_max` or
/// `net.core.wmem_max`. For UDP the receive buffer is the only queue in front
/// of quinn, so a clamped buffer shows up later as dropped datagrams and
/// retransmits, not as an error. The warning names the sysctl to raise.
pub(super) fn size_buffers(socket: &Socket, requested: BufferSizes) -> BufferSizes {
    if let Err(e) = socket.set_recv_buffer_size(requested.recv) {
        warn!("QUIC: failed to set SO_RCVBUF to {}: {e}", requested.recv);
    }
    if let Err(e) = socket.set_send_buffer_size(requested.send) {
        warn!("QUIC: failed to set SO_SNDBUF to {}: {e}", requested.send);
    }
    let effective = BufferSizes {
        recv: socket.recv_buffer_size().unwrap_or(0),
        send: socket.send_buffer_size().unwrap_or(0),
    };
    if effective.recv < requested.recv {
        warn!(
            requested = requested.recv,
            effective = effective.recv,
            "QUIC: the kernel clamped the UDP receive buffer; raise net.core.rmem_max \
             or add reuse-port endpoints, or expect datagram drops under load"
        );
    }
    if effective.send < requested.send {
        warn!(
            requested = requested.send,
            effective = effective.send,
            "QUIC: the kernel clamped the UDP send buffer; raise net.core.wmem_max"
        );
    }
    effective
}

#[cfg(test)]
mod tests {
    use super::*;

    const SMALL: BufferSizes = BufferSizes {
        recv: 64 * 1024,
        send: 64 * 1024,
    };

    #[test]
    fn server_sockets_share_one_port() {
        let sockets = bind_server_sockets("127.0.0.1:0".parse().unwrap(), 4, SMALL).unwrap();
        let expected = if cfg!(target_os = "linux") { 4 } else { 1 };
        assert_eq!(sockets.len(), expected);
        let port = sockets[0].local_addr().unwrap().port();
        assert_ne!(port, 0);
        for socket in &sockets {
            assert_eq!(socket.local_addr().unwrap().port(), port);
        }
    }

    #[test]
    fn the_client_socket_is_not_in_the_server_group() {
        let server = bind_server_sockets("127.0.0.1:0".parse().unwrap(), 2, SMALL).unwrap();
        let server_addr = server[0].local_addr().unwrap();
        let client = bind_client_socket(server_addr, SMALL).unwrap();
        assert_ne!(client.local_addr().unwrap().port(), server_addr.port());
    }

    #[test]
    fn an_unrelated_bind_cannot_join_the_group() {
        // The first socket joins the group only after its own bind, so a
        // second plain bind to the same port still fails.
        let server = bind_server_sockets("127.0.0.1:0".parse().unwrap(), 2, SMALL).unwrap();
        let addr = server[0].local_addr().unwrap();
        assert!(std::net::UdpSocket::bind(addr).is_err());
        // Nor may a socket with SO_REUSEADDR alone. On Linux two UDP sockets
        // that both set SO_REUSEADDR skip the port conflict check, so if the
        // group set it, any process could bind the group's port.
        let reuse_addr_only = new_udp_socket(addr).unwrap();
        reuse_addr_only.set_reuse_address(true).unwrap();
        assert!(
            reuse_addr_only.bind(&addr.into()).is_err(),
            "a socket with SO_REUSEADDR alone bound the group's port"
        );
    }

    #[test]
    fn size_buffers_reports_what_the_kernel_granted() {
        let socket = new_udp_socket("127.0.0.1:0".parse().unwrap()).unwrap();
        let effective = size_buffers(&socket, SMALL);
        // Linux doubles the request for bookkeeping. A small request is below
        // every default ceiling, so it is granted in full.
        assert!(effective.recv >= SMALL.recv, "{effective:?}");
        assert!(effective.send >= SMALL.send, "{effective:?}");
    }

    #[test]
    fn endpoint_info_round_trips() {
        let info = QuicEndpointInfo {
            endpoints: vec![],
            fingerprint: [7; 32],
        };
        let decoded = QuicEndpointInfo::decode(&info.encode().unwrap()).unwrap();
        assert_eq!(decoded.fingerprint, [7; 32]);
    }
}
