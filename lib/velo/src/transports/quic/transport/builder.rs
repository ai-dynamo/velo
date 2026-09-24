// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! [`QuicTransportBuilder`]: the transport's options and their defaults.

use super::*;

/// Default QUIC idle timeout: three keep-alives. quinn ignores ICMP
/// unreachable for liveness, so a peer that dies without closing is found only
/// by this timeout; quinn's own default is 30 s.
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(15);

/// Builder for [`QuicTransport`].
pub struct QuicTransportBuilder {
    bind_addr: Option<SocketAddr>,
    key: Option<TransportKey>,
    channel_capacity: usize,
    connect_timeout: Duration,
    interface_filter: InterfaceFilter,
    numa_hint: Option<u32>,
    server_endpoints: usize,
    udp_buffers: BufferSizes,
    stream_receive_window: Option<u32>,
    max_mtu: Option<u16>,
    keep_alive_interval: Duration,
    idle_timeout: Duration,
    shrink_threshold: usize,
}

impl QuicTransportBuilder {
    /// A builder with the defaults below.
    pub fn new() -> Self {
        Self {
            bind_addr: None,
            key: None,
            channel_capacity: 256,
            connect_timeout: Duration::from_secs(5),
            interface_filter: InterfaceFilter::default(),
            numa_hint: None,
            server_endpoints: DEFAULT_SERVER_ENDPOINTS,
            udp_buffers: BufferSizes {
                recv: DEFAULT_UDP_RECV_BUFFER,
                send: DEFAULT_UDP_SEND_BUFFER,
            },
            stream_receive_window: None,
            max_mtu: None,
            keep_alive_interval: Duration::from_secs(5),
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            shrink_threshold: DEFAULT_SHRINK_THRESHOLD,
        }
    }

    /// The UDP address to bind (default `0.0.0.0:0`).
    pub fn bind_addr(mut self, addr: SocketAddr) -> Self {
        self.bind_addr = Some(addr);
        self
    }

    /// The transport key (default `quic`).
    pub fn key(mut self, key: TransportKey) -> Self {
        self.key = Some(key);
        self
    }

    /// Capacity of each connection's send channel (default 256).
    pub fn channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }

    /// Handshake timeout for outbound connections (default 5 s).
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Interface selection for multi-NIC hosts.
    pub fn interface_filter(mut self, filter: InterfaceFilter) -> Self {
        self.interface_filter = filter;
        self
    }

    /// NUMA node hint for NIC selection.
    pub fn numa_hint(mut self, node: u32) -> Self {
        self.numa_hint = Some(node);
        self
    }

    /// Number of server sockets in the `SO_REUSEPORT` group (Linux only;
    /// default 4, ignored elsewhere).
    ///
    /// A node that many peers send to at once (a frontend) gains from more:
    /// each socket has its own receive queue and buffer ceiling. Each socket
    /// also costs a quinn endpoint and its receive buffers.
    pub fn server_endpoints(mut self, count: usize) -> Self {
        self.server_endpoints = count.max(1);
        self
    }

    /// Requested `SO_RCVBUF` and `SO_SNDBUF` for every UDP socket (default
    /// 8 MiB and 4 MiB). The kernel clamps them to `net.core.rmem_max` and
    /// `net.core.wmem_max`; a clamp is logged at startup.
    pub fn udp_buffer_sizes(mut self, recv: usize, send: usize) -> Self {
        self.udp_buffers = BufferSizes { recv, send };
        self
    }

    /// Per-stream flow-control window in bytes (default: quinn's).
    pub fn stream_receive_window(mut self, bytes: u32) -> Self {
        self.stream_receive_window = Some(bytes);
        self
    }

    /// Largest UDP payload this transport sends or accepts (default: quinn's
    /// 1452 for probing and 1472 to receive, sized for a 1500-byte Ethernet
    /// MTU).
    ///
    /// Each QUIC packet is encrypted and handled on its own, so larger packets
    /// cost less per byte. Values above 6550 are lowered to 6550 (see
    /// `GSO_SAFE_MAX_MTU`), so on a jumbo-frame network set 6550.
    /// Path MTU discovery still probes up to this bound, so a smaller real
    /// path MTU is found, not assumed.
    pub fn max_mtu(mut self, bytes: u16) -> Self {
        self.max_mtu = Some(bytes);
        self
    }

    /// QUIC keep-alive interval (default 5 s).
    pub fn keep_alive_interval(mut self, interval: Duration) -> Self {
        self.keep_alive_interval = interval;
        self
    }

    /// How long a connection may go without receiving anything before it is
    /// closed (default 15 s, three keep-alives). This is how a peer that died
    /// without closing is found; its epoch fails at this point, and the
    /// frames queued on it go to their error handlers.
    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Read-buffer size above which a reader gives the excess back after a
    /// frame (default: the TCP transport's). The codec reserves the whole
    /// frame length, so without this one large frame pins that much memory
    /// per connection for its life.
    pub fn shrink_threshold(mut self, bytes: usize) -> Self {
        self.shrink_threshold = bytes;
        self
    }

    /// Bind the sockets, make the certificate, and build the transport.
    pub fn build(self) -> Result<QuicTransport> {
        let key = self.key.unwrap_or_else(|| TransportKey::from("quic"));
        let requested = self
            .bind_addr
            .unwrap_or_else(|| "0.0.0.0:0".parse().unwrap());

        let server_sockets =
            bind_server_sockets(requested, self.server_endpoints, self.udp_buffers)?;
        let bind_addr = server_sockets[0].local_addr()?;
        let client_socket = bind_client_socket(bind_addr, self.udp_buffers)?;

        let mut transport_config = quinn::TransportConfig::default();
        // One bidirectional stream per connection. Datagrams and
        // unidirectional streams are unused.
        transport_config.max_concurrent_bidi_streams(1u32.into());
        transport_config.max_concurrent_uni_streams(0u32.into());
        transport_config.datagram_receive_buffer_size(None);
        transport_config.keep_alive_interval(Some(self.keep_alive_interval));
        transport_config.max_idle_timeout(Some(
            self.idle_timeout
                .try_into()
                .context("QUIC idle_timeout is out of range")?,
        ));
        if let Some(window) = self.stream_receive_window {
            transport_config.stream_receive_window(window.into());
        }
        let mut endpoint_config = quinn::EndpointConfig::default();
        let max_mtu = self.max_mtu.map(clamp_to_gso_batch);
        if let Some(max_mtu) = max_mtu {
            endpoint_config
                .max_udp_payload_size(max_mtu.max(1200))
                .context("QUIC max_mtu is out of range")?;
            let mut discovery = quinn::MtuDiscoveryConfig::default();
            discovery.upper_bound(max_mtu);
            transport_config.mtu_discovery_config(Some(discovery));
        }
        let transport_config = Arc::new(transport_config);

        let identity = Identity::generate()?;
        let crypto = tls::server_crypto(&identity)?;
        let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(crypto)
                .context("failed to build the QUIC server TLS config")?,
        ));
        server_config.transport_config(transport_config.clone());

        let endpoints = resolve_advertise_endpoints(bind_addr, &self.interface_filter)?;
        let info = QuicEndpointInfo {
            endpoints,
            fingerprint: identity.fingerprint,
        };
        let mut addr_builder = crate::transports::address::WorkerAddressBuilder::new();
        addr_builder.add_entry(key.clone(), info.encode()?)?;
        let local_address = addr_builder.build()?;

        Ok(QuicTransport {
            key,
            bind_addr,
            local_address,
            fingerprint: identity.fingerprint,
            shrink_threshold: self.shrink_threshold,
            peers: Arc::new(DashMap::new()),
            connections: Arc::new(DashMap::new()),
            runtime: OnceLock::new(),
            cancel_token: CancellationToken::new(),
            writers: tokio_util::task::TaskTracker::new(),
            shutdown_state: OnceLock::new(),
            channel_capacity: self.channel_capacity,
            connect_timeout: self.connect_timeout,
            server_sockets: Mutex::new(Some(server_sockets)),
            client_socket: Mutex::new(Some(client_socket)),
            server_config,
            transport_config,
            endpoint_config,
            server_endpoints: OnceLock::new(),
            client_endpoint: OnceLock::new(),
            local_interfaces: OnceLock::new(),
            numa_hint: self.numa_hint,
            metrics: OnceLock::new(),
            dialed_ctx: OnceLock::new(),
        })
    }
}

impl Default for QuicTransportBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Server sockets in the reuse-port group by default. Dynamo's QUIC plane
/// measured 8 and 32 on a frontend; 32 cost about 50 MiB of RSS. A frontend
/// sets a higher count with [`QuicTransportBuilder::server_endpoints`].
const DEFAULT_SERVER_ENDPOINTS: usize = 4;
/// Largest packet size at which quinn's send batches still fit in one UDP
/// datagram: 65507 bytes of UDP payload over quinn's 10-packet batch.
///
/// quinn hands up to 10 packets to one GSO send and does not cap the batch
/// size. Above this size a full batch exceeds the UDP limit, the kernel
/// answers `EMSGSIZE`, and quinn-udp 0.5 treats that as success because it
/// expects it only from MTU probes. The whole batch is lost without a trace,
/// and every flight waits out a loss-probe timeout. Measured on loopback with
/// 64 KiB messages: 2,666 msg/s at 6550 bytes, 115 msg/s at 6560.
pub(super) const GSO_SAFE_MAX_MTU: u16 = (65_507 / 10) as u16;

pub(super) fn clamp_to_gso_batch(requested: u16) -> u16 {
    if requested > GSO_SAFE_MAX_MTU {
        warn!(
            requested,
            used = GSO_SAFE_MAX_MTU,
            "QUIC max_mtu lowered: quinn's 10-packet send batch must fit one UDP datagram"
        );
    }
    requested.min(GSO_SAFE_MAX_MTU)
}

const DEFAULT_UDP_RECV_BUFFER: usize = 8 * 1024 * 1024;
const DEFAULT_UDP_SEND_BUFFER: usize = 4 * 1024 * 1024;
