// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! [`TcpTransportBuilder`]: the transport's options and their defaults.

use std::net::SocketAddr;
use std::num::NonZeroU16;
use std::time::Duration;

use anyhow::{Context, Result};
use tracing::warn;
use velo_ext::TransportKey;

use crate::transports::utils::interfaces::{InterfaceFilter, resolve_advertise_endpoints};

use super::TcpTransport;

/// Builder for TcpTransport
pub struct TcpTransportBuilder {
    bind_addr: Option<SocketAddr>,
    key: Option<TransportKey>,
    channel_capacity: usize,
    connect_timeout: Duration,
    listener: Option<std::net::TcpListener>,
    interface_filter: InterfaceFilter,
    numa_hint: Option<u32>,
    shrink_threshold: Option<usize>,
    lanes: NonZeroU16,
}

impl TcpTransportBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            bind_addr: None,
            key: None,
            channel_capacity: 256,
            connect_timeout: Duration::from_secs(5),
            listener: None,
            interface_filter: InterfaceFilter::default(),
            numa_hint: None,
            shrink_threshold: None,
            lanes: NonZeroU16::MIN,
        }
    }

    /// Set the bind address
    pub fn bind_addr(mut self, addr: SocketAddr) -> Self {
        self.bind_addr = Some(addr);
        self
    }

    /// Set the transport key
    pub fn key(mut self, key: TransportKey) -> Self {
        self.key = Some(key);
        self
    }

    /// Set the channel capacity for backpressure (default: 256)
    pub fn channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }

    /// Set the connect timeout for outbound connections (default: 5s)
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set the interface selection filter for multi-NIC environments.
    pub fn interface_filter(mut self, filter: InterfaceFilter) -> Self {
        self.interface_filter = filter;
        self
    }

    /// Set the NUMA node hint for topology-aware NIC selection.
    ///
    /// Callers typically resolve this via `dynamo_memory::numa::get_device_numa_node(gpu_id)`.
    pub fn numa_hint(mut self, node: u32) -> Self {
        self.numa_hint = Some(node);
        self
    }

    /// Override the per-connection read-buffer shrink threshold (bytes).
    ///
    /// If a single oversized inbound frame causes the listener's `BytesMut`
    /// read buffer to grow past this many bytes, the buffer will be reset back
    /// to a small capacity the next time it fully drains. Defaults to 8 MB,
    /// overridable at process start via `VELO_TCP_SHRINK_THRESHOLD`.
    pub fn shrink_threshold(mut self, bytes: usize) -> Self {
        self.shrink_threshold = Some(bytes);
        self
    }

    /// Lanes to each peer: up to this many connections, one for each lane
    /// used, dialed on the first send on that lane (default 1, at least 1).
    ///
    /// One connection is limited by its receiver: one reader task does the
    /// whole receive copy, and fixed socket buffers cap the TCP window. Each
    /// lane is its own connection, read by its own task, so N lanes spread
    /// that work over up to N cores. Order holds
    /// within a lane only, so a caller that uses lanes must keep each ordered
    /// flow on one lane (see `Transport::send_message_on_lane`).
    /// `send_message` uses lane 0.
    ///
    /// Only the dialing side's count matters: the listener accepts however
    /// many connections a peer opens, and reads each on its own task.
    ///
    /// Each lane costs a socket at each end, a writer task and a reader task
    /// on the dialing side, a reader task on the listening side, and the send
    /// and receive buffers of one connection. `channel_capacity` applies to
    /// each lane. A lane stays open until its socket dies or `shutdown()`:
    /// unlike QUIC, TCP has no idle close. `lanes(8)` to 200 peers is up to
    /// 1,600 connections for the life of the transport.
    pub fn lanes(mut self, lanes: u16) -> Self {
        self.lanes = NonZeroU16::new(lanes).unwrap_or(NonZeroU16::MIN);
        self
    }

    /// Use a pre-bound TcpListener instead of binding to a specific address
    ///
    /// This is useful for tests where you want to bind to port 0 and get an OS-assigned
    /// port without creating a race condition between binding and starting the transport.
    ///
    /// Note: This is mutually exclusive with `bind_addr()`. Using both will result in an error.
    pub fn from_listener(mut self, listener: std::net::TcpListener) -> Result<Self> {
        // Validate mutual exclusivity: can't use both bind_addr() and from_listener()
        if self.bind_addr.is_some() {
            anyhow::bail!(
                "Cannot use both bind_addr() and from_listener() - they are mutually exclusive"
            );
        }

        let addr = listener
            .local_addr()
            .context("Failed to get local address from listener")?;
        self.bind_addr = Some(addr);
        self.listener = Some(listener);
        Ok(self)
    }

    /// Build the TcpTransport
    pub fn build(self) -> Result<TcpTransport> {
        let key = self.key.unwrap_or_else(|| TransportKey::from("tcp"));

        // If we have a listener, use its address; otherwise pre-bind to resolve port 0.
        let (bind_addr, listener) = if let Some(listener) = self.listener {
            // Caller-provided listener: it is already live, so this is best
            // effort — connections whose handshake completed before this point
            // keep kernel-default autotuned buffers, which is safe.
            super::super::listener::size_listener_buffers(&listener);
            let addr = listener.local_addr()?;
            (addr, Some(listener))
        } else {
            let requested = self
                .bind_addr
                .unwrap_or_else(|| "0.0.0.0:0".parse().unwrap());
            // Built by hand instead of std::net::TcpListener::bind so the
            // socket buffers are sized before listen() — accepted sockets
            // inherit them at handshake time (see `size_listener_buffers`).
            let domain = if requested.is_ipv4() {
                socket2::Domain::IPV4
            } else {
                socket2::Domain::IPV6
            };
            let socket =
                socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))
                    .context("Failed to create TCP listener socket")?;
            // std::net::TcpListener::bind sets SO_REUSEADDR on Unix; keep that.
            socket
                .set_reuse_address(true)
                .context("Failed to set SO_REUSEADDR")?;
            super::super::listener::size_listener_buffers(&socket);
            socket
                .bind(&requested.into())
                .context("Failed to pre-bind TCP listener")?;
            // 128 matches std::net::TcpListener::bind's backlog.
            socket.listen(128).context("Failed to listen")?;
            let std_listener: std::net::TcpListener = socket.into();
            let actual = std_listener.local_addr()?;
            (actual, Some(std_listener))
        };

        // Resolve advertise endpoints (multi-interface discovery)
        let endpoints = resolve_advertise_endpoints(bind_addr, &self.interface_filter)?;

        // Warn if NUMA hint conflicts with interface filter
        if let (Some(numa), InterfaceFilter::ByName(name)) =
            (self.numa_hint, &self.interface_filter)
        {
            for ep in &endpoints {
                if let Some(ep_numa) = ep.numa_node
                    && ep_numa != numa as i32
                {
                    warn!(
                        "NIC {} is on NUMA node {} but GPU NUMA hint is {}",
                        name, ep_numa, numa
                    );
                }
            }
        }

        let encoded =
            rmp_serde::to_vec(&endpoints).context("Failed to encode interface endpoints")?;
        let mut addr_builder = crate::transports::address::WorkerAddressBuilder::new();
        addr_builder.add_entry(key.clone(), encoded)?;
        let local_address = addr_builder.build()?;

        let mut transport = TcpTransport::new(
            bind_addr,
            key,
            local_address,
            self.channel_capacity,
            self.connect_timeout,
            listener,
            self.numa_hint,
        );
        if let Some(t) = self.shrink_threshold {
            transport.shrink_threshold = t;
        }
        transport.lanes = self.lanes;
        Ok(transport)
    }
}

impl Default for TcpTransportBuilder {
    fn default() -> Self {
        Self::new()
    }
}
