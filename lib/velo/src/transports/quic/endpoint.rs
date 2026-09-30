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
    /// The port of each server socket. The first is the port in
    /// [`endpoints`](Self::endpoints). A dialer spreads its lanes over these.
    /// Empty in an entry from a peer that predates per-socket ports: dial the
    /// port in `endpoints` for every lane.
    #[serde(default)]
    pub ports: Vec<u16>,
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

/// Bind the server sockets: `count` sockets, each on its own port.
///
/// With a fixed requested port `P`, the sockets bind `P` to `P + count - 1`, so
/// an operator who opens the ports in a firewall knows which ones to open.
/// With port 0, each socket takes an ephemeral port. All bind the same IP.
///
/// The peer learns every port from [`QuicEndpointInfo::ports`] and dials lane
/// `k` on socket `(offset + k) % count`, where `offset` is fixed per dialer. So the lanes of one dialer land on different sockets, each with its
/// own quinn endpoint driver and receive buffer.
///
/// An earlier design put every socket on one port in a `SO_REUSEPORT` group.
/// The kernel then hashed each connection to a socket at random, and two lanes
/// of one peer often shared a socket: with 8 lanes, a group of 4 sockets moved
/// 3.3 GB/s where a group of 32 moved 6.5 GB/s.
pub(super) fn bind_server_sockets(
    requested: SocketAddr,
    count: usize,
    buffers: BufferSizes,
) -> Result<Vec<std::net::UdpSocket>> {
    let count = count.max(1);
    // All ports are worked out before any socket binds, so a range past 65535
    // fails without leaving sockets behind.
    let ports: Vec<u16> = if requested.port() == 0 {
        vec![0; count]
    } else {
        let last = u16::try_from(count - 1)
            .ok()
            .and_then(|extra| requested.port().checked_add(extra))
            .with_context(|| {
                format!(
                    "QUIC server_endpoints({count}) needs ports {} to {} + {}, past 65535",
                    requested.port(),
                    requested.port(),
                    count - 1
                )
            })?;
        (requested.port()..=last).collect()
    };
    let mut sockets = Vec::with_capacity(count);
    for (index, port) in ports.into_iter().enumerate() {
        // `set_port` keeps an IPv6 scope id and flow info.
        let mut addr = requested;
        addr.set_port(port);
        let socket = new_udp_socket(addr)?;
        size_buffers(&socket, buffers);
        socket.bind(&addr.into()).with_context(|| {
            format!(
                "failed to bind QUIC server socket {index} of server_endpoints({count}) on {addr}"
            )
        })?;
        sockets.push(socket.into());
    }
    Ok(sockets)
}

/// Bind one socket that dials peers. The transport binds one for each lane.
///
/// It is separate from the server sockets and has its own ephemeral port, so
/// each lane has its own quinn endpoint driver for the replies it receives.
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
             or add server endpoints, or expect datagram drops under load"
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
    fn server_sockets_each_have_their_own_port() {
        let sockets = bind_server_sockets("127.0.0.1:0".parse().unwrap(), 4, SMALL).unwrap();
        assert_eq!(sockets.len(), 4);
        let mut ports: Vec<u16> = sockets
            .iter()
            .map(|socket| socket.local_addr().unwrap().port())
            .collect();
        assert!(ports.iter().all(|&port| port != 0));
        ports.sort_unstable();
        ports.dedup();
        assert_eq!(ports.len(), 4, "every socket is reachable on its own port");
    }

    /// A fixed port opens consecutive ports, so an operator knows what to
    /// allow through a firewall.
    #[test]
    fn a_fixed_port_binds_consecutive_ports() {
        // A free base port can be taken by another process between the probe
        // and the bind, so try a few.
        for _ in 0..10 {
            let base = std::net::UdpSocket::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap()
                .port();
            if base > u16::MAX - 3 {
                continue;
            }
            let requested = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), base);
            let Ok(sockets) = bind_server_sockets(requested, 3, SMALL) else {
                continue;
            };
            let ports: Vec<u16> = sockets
                .iter()
                .map(|socket| socket.local_addr().unwrap().port())
                .collect();
            assert_eq!(ports, vec![base, base + 1, base + 2]);
            return;
        }
        panic!("no free run of three ports in ten tries");
    }

    #[test]
    fn a_fixed_port_past_the_range_is_refused() {
        let requested = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), u16::MAX);
        assert!(bind_server_sockets(requested, 2, SMALL).is_err());
    }

    /// No other socket can bind a server port. A server socket with
    /// `SO_REUSEADDR` or `SO_REUSEPORT` would let any process take one.
    #[test]
    fn an_unrelated_bind_cannot_take_a_server_port() {
        let server = bind_server_sockets("127.0.0.1:0".parse().unwrap(), 2, SMALL).unwrap();
        for socket in &server {
            let addr = socket.local_addr().unwrap();
            assert!(std::net::UdpSocket::bind(addr).is_err());
            let reuse_addr_only = new_udp_socket(addr).unwrap();
            reuse_addr_only.set_reuse_address(true).unwrap();
            assert!(
                reuse_addr_only.bind(&addr.into()).is_err(),
                "a socket with SO_REUSEADDR alone bound a server port"
            );
        }
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
            ports: vec![5000, 5001],
        };
        let decoded = QuicEndpointInfo::decode(&info.encode().unwrap()).unwrap();
        assert_eq!(decoded.fingerprint, [7; 32]);
        assert_eq!(decoded.ports, vec![5000, 5001]);
    }

    /// A peer that predates per-socket ports decodes an entry that has them:
    /// the unknown field is skipped.
    #[test]
    fn an_old_peer_decodes_an_entry_with_ports() {
        #[derive(Deserialize)]
        struct Before {
            fingerprint: Fingerprint,
        }
        let info = QuicEndpointInfo {
            endpoints: vec![],
            fingerprint: [5; 32],
            ports: vec![5000, 5001],
        };
        let old: Before = rmp_serde::from_slice(&info.encode().unwrap()).unwrap();
        assert_eq!(old.fingerprint, [5; 32]);
    }

    /// An entry from a peer that predates per-socket ports has no `ports`, and still
    /// decodes: the dialer then uses the port in `endpoints` for every lane.
    #[test]
    fn an_entry_without_ports_decodes() {
        #[derive(Serialize)]
        struct Before {
            endpoints: Vec<InterfaceEndpoint>,
            fingerprint: Fingerprint,
        }
        let raw = rmp_serde::to_vec_named(&Before {
            endpoints: vec![],
            fingerprint: [3; 32],
        })
        .unwrap();
        let decoded = QuicEndpointInfo::decode(&raw).unwrap();
        assert_eq!(decoded.fingerprint, [3; 32]);
        assert!(decoded.ports.is_empty());
    }
}
