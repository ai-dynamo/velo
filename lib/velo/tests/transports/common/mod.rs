// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Common test utilities for transport integration tests
//!
//! This module provides a transport-agnostic test infrastructure that can be reused
//! across different transport implementations (TCP, RDMA, UDP, UDS, etc.).

#![allow(dead_code)]

#[cfg(feature = "grpc")]
use velo::transports::grpc::{GrpcTransport, GrpcTransportBuilder};
#[cfg(feature = "zmq")]
use velo::transports::zmq::{ZmqTransport, ZmqTransportBuilder};
// #[cfg(feature = "http")]
// use velo::transports::http::{HttpTransport, HttpTransportBuilder};
#[cfg(feature = "nats-transport")]
use velo::transports::nats::{NatsTransport, NatsTransportBuilder};

use bytes::Bytes;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::timeout;
use velo::transports::{
    DataStreams, MessageType, SendOutcome, Transport, TransportErrorHandler,
    tcp::{TcpTransport, TcpTransportBuilder},
};
use velo_ext::{InstanceId, PeerInfo};

#[cfg(all(target_os = "linux", feature = "ucx"))]
use velo::transports::ucx::{UcxTransport, UcxTransportBuilder};

/// UCX context/worker creation is a few ms; a stall here is a bug, not load.
#[cfg(all(target_os = "linux", feature = "ucx"))]
const UCX_STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(unix)]
use velo::transports::uds::{UdsTransport, UdsTransportBuilder};

use std::sync::Once;
use tracing_subscriber::FmtSubscriber;

/// Outer bound wrapping every generated scenario in `transport_integration_tests!`,
/// `transport_epoch_tests!`, and `transport_shutdown_tests!` (CodeRabbit F4:
/// "All async test awaits must be wrapped in `tokio::time::timeout`; do not use
/// unbounded waits", `.coderabbit.yaml` path `lib/velo/tests/transports/**`).
///
/// Deliberately generous — 30s, not the 5s of the inner per-assertion
/// `TEST_TIMEOUT` in `scenarios.rs`. This bound exists only to fail a hung test
/// with a message instead of hanging the suite forever; a tight bound risks
/// false failures on `high_throughput` and
/// `admission_ordering_under_capacity_pressure` under `cargo llvm-cov`, where
/// this repo has documented instrumentation-overhead timing flakes.
pub(crate) const OUTER_TEST_TIMEOUT: Duration = Duration::from_secs(30);

#[allow(dead_code)]
static INIT: Once = Once::new();

#[allow(dead_code)]
pub fn init_tracing() {
    INIT.call_once(|| {
        let _ = FmtSubscriber::builder()
            .with_env_filter("trace") // or "info"
            .try_init();
    });
}

pub mod scenarios;
pub mod shutdown_scenarios;

/// Test error handler that tracks errors for verification
#[derive(Clone)]
pub struct TestErrorHandler {
    errors: Arc<Mutex<Vec<(Bytes, Bytes, String)>>>,
}

impl TestErrorHandler {
    pub fn new() -> Self {
        Self {
            errors: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn get_errors(&self) -> Vec<(Bytes, Bytes, String)> {
        self.errors.lock().unwrap().clone()
    }

    pub fn error_count(&self) -> usize {
        self.errors.lock().unwrap().len()
    }

    pub fn clear(&self) {
        self.errors.lock().unwrap().clear();
    }
}

impl TransportErrorHandler for TestErrorHandler {
    fn on_error(&self, header: Bytes, payload: Bytes, error: String) {
        self.errors.lock().unwrap().push((header, payload, error));
    }
}

/// Handle to a transport instance with its streams for testing
///
/// This is a generic test handle that works with any transport implementation.
/// Use `TestTransportHandle::with_factory()` to create instances with custom transports,
/// or use convenience methods like `TestTransportHandle::new()` for TCP transport.
pub struct TestTransportHandle<T: Transport> {
    pub transport: T,
    pub streams: DataStreams,
    pub instance_id: InstanceId,
    pub error_handler: Arc<TestErrorHandler>,
    runtime: tokio::runtime::Handle,
}

impl<T: Transport> TestTransportHandle<T> {
    /// Create a new test transport using a factory function
    ///
    /// This is the generic constructor that works with any transport implementation.
    /// The factory function should create and return a transport instance.
    ///
    /// # Example
    /// ```ignore
    /// let handle = TestTransportHandle::with_factory(|| {
    ///     MyTransportBuilder::new().build()
    /// }).await?;
    /// ```
    pub async fn with_factory<F>(factory: F) -> anyhow::Result<Self>
    where
        F: FnOnce() -> anyhow::Result<T>,
    {
        let transport = factory()?;
        let instance_id = InstanceId::new_v4();
        let error_handler = Arc::new(TestErrorHandler::new());

        // Create channels for this transport
        let (adapter, streams) = velo::transports::make_channels();

        // Get runtime handle
        let runtime = tokio::runtime::Handle::current();

        // Start the transport
        transport
            .start(instance_id, adapter, runtime.clone())
            .await?;

        // Give the listener a moment to bind and start accepting connections
        tokio::time::sleep(Duration::from_millis(50)).await;

        Ok(Self {
            transport,
            streams,
            instance_id,
            error_handler,
            runtime,
        })
    }

    /// Register another transport as a peer
    pub fn register_peer<U: Transport>(
        &self,
        other: &TestTransportHandle<U>,
    ) -> anyhow::Result<()> {
        let peer_info = PeerInfo::new(other.instance_id, other.transport.address());
        self.transport
            .register(peer_info)
            .map_err(|e| anyhow::anyhow!("Failed to register peer: {:?}", e))?;
        Ok(())
    }

    /// Send a message to a peer, fire-and-forget.
    ///
    /// The admission is dropped, which is a legitimate pattern: the frame
    /// belongs to the target's gate from the moment `send_message` returns and
    /// is delivered whether or not anyone waits for it. Tests that need to
    /// observe *when* a frame lands use [`send_admission`](Self::send_admission).
    pub fn send(
        &self,
        target: InstanceId,
        header: Vec<u8>,
        payload: Vec<u8>,
        msg_type: MessageType,
    ) {
        drop(self.send_admission(target, header, payload, msg_type));
    }

    /// Send a message and keep its [`SendOutcome`].
    pub fn send_admission(
        &self,
        target: InstanceId,
        header: Vec<u8>,
        payload: Vec<u8>,
        msg_type: MessageType,
    ) -> SendOutcome {
        self.transport.send_message(
            target,
            Bytes::from(header),
            Bytes::from(payload),
            msg_type,
            self.error_handler.clone(),
        )
    }

    /// Receive a message with timeout
    ///
    /// `message_stream` items carry a mandatory in-flight guard (see
    /// `InboundMessage`); dropping it here is correct, since the caller
    /// receiving the message is standing in for the real consumer that would
    /// otherwise decrement the drain count on dispatch.
    pub async fn recv_message(&self, timeout_duration: Duration) -> anyhow::Result<(Bytes, Bytes)> {
        let msg = timeout(timeout_duration, self.streams.message_stream.recv_async())
            .await
            .map_err(|_| anyhow::anyhow!("Timeout waiting for message"))?
            .map_err(|e| anyhow::anyhow!("Channel error: {}", e))?;
        Ok((msg.header, msg.payload))
    }

    /// Receive a response with timeout
    pub async fn recv_response(
        &self,
        timeout_duration: Duration,
    ) -> anyhow::Result<(Bytes, Bytes)> {
        timeout(timeout_duration, self.streams.response_stream.recv_async())
            .await
            .map_err(|_| anyhow::anyhow!("Timeout waiting for response"))?
            .map_err(|e| anyhow::anyhow!("Channel error: {}", e))
    }

    /// Receive an event with timeout
    pub async fn recv_event(&self, timeout_duration: Duration) -> anyhow::Result<(Bytes, Bytes)> {
        timeout(timeout_duration, self.streams.event_stream.recv_async())
            .await
            .map_err(|_| anyhow::anyhow!("Timeout waiting for event"))?
            .map_err(|e| anyhow::anyhow!("Channel error: {}", e))
    }

    /// Collect multiple messages with timeout
    pub async fn collect_messages(
        &self,
        count: usize,
        timeout_duration: Duration,
    ) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        let mut messages = Vec::new();
        for _ in 0..count {
            messages.push(self.recv_message(timeout_duration).await?);
        }
        Ok(messages)
    }

    /// Collect multiple messages with timeout, sorted by header for order-independent comparison
    ///
    /// This is useful for testing transports that don't guarantee delivery order (e.g., HTTP).
    /// Messages are sorted by header bytes to enable deterministic comparison regardless of
    /// delivery order.
    pub async fn collect_messages_unordered(
        &self,
        count: usize,
        timeout_duration: Duration,
    ) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        let mut messages = self.collect_messages(count, timeout_duration).await?;
        messages.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(messages)
    }

    /// Collect multiple responses with timeout
    pub async fn collect_responses(
        &self,
        count: usize,
        timeout_duration: Duration,
    ) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        let mut responses = Vec::new();
        for _ in 0..count {
            responses.push(self.recv_response(timeout_duration).await?);
        }
        Ok(responses)
    }

    /// Shutdown the transport
    pub fn shutdown(self) {
        self.transport.shutdown();
    }
}

// TCP-specific convenience constructors
impl TestTransportHandle<TcpTransport> {
    /// Create a new TCP transport on a random available port
    ///
    /// This is a convenience method for creating TCP transports.
    /// For other transport types, use `with_factory()`.
    pub async fn new_tcp() -> anyhow::Result<Self> {
        Self::with_factory(|| {
            let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
            TcpTransportBuilder::new().from_listener(listener)?.build()
        })
        .await
    }

    /// Alias for `new_tcp()` to maintain backward compatibility
    pub async fn new() -> anyhow::Result<Self> {
        Self::new_tcp().await
    }
}

// UDS-specific convenience constructors
#[cfg(unix)]
impl TestTransportHandle<UdsTransport> {
    /// Create a new UDS transport using a temp directory socket path
    pub async fn new_uds() -> anyhow::Result<Self> {
        Self::with_factory(|| {
            let dir =
                std::env::temp_dir().join(format!("velo-uds-test-{}", velo::InstanceId::new_v4()));
            std::fs::create_dir_all(&dir)?;
            let socket_path = dir.join("transport.sock");
            UdsTransportBuilder::new().socket_path(&socket_path).build()
        })
        .await
    }
}

// UCX-specific convenience constructors
#[cfg(all(target_os = "linux", feature = "ucx"))]
impl TestTransportHandle<UcxTransport> {
    /// Create a new UCX transport pinned to the tcp lane (deterministic on
    /// hardware-less runners; PEER error mode excludes the shm lanes anyway).
    pub async fn new_ucx() -> anyhow::Result<Self> {
        tokio::time::timeout(
            UCX_STARTUP_TIMEOUT,
            Self::with_factory(|| UcxTransportBuilder::new().tls("tcp").build()),
        )
        .await
        .map_err(|_| anyhow::anyhow!("ucx transport startup timed out"))?
    }
}

// // HTTP-specific convenience constructors
// #[cfg(feature = "http")]
// impl TestTransportHandle<HttpTransport> {
//     /// Create a new HTTP transport with OS-provided port
//     ///
//     /// This is a convenience method for creating HTTP transports.
//     /// For other transport types, use `with_factory()`.
//     pub async fn new_http() -> anyhow::Result<Self> {
//         Self::with_factory(|| {
//             // Use default builder which binds to 0.0.0.0:0 (OS-provided port)
//             HttpTransportBuilder::new().build()
//         })
//         .await
//     }
// }

// NATS-specific convenience constructor
#[cfg(feature = "nats-transport")]
pub fn nats_url() -> String {
    std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string())
}

#[cfg(feature = "nats-transport")]
impl TestTransportHandle<NatsTransport> {
    /// Create a new NATS transport with a unique cluster_id for test isolation (TEST-06).
    pub async fn new_nats(cluster_id: &str) -> anyhow::Result<Self> {
        let client = velo::transports::nats::utils::connect(&nats_url()).await?;
        Self::with_factory(|| Ok(NatsTransportBuilder::new(client.clone(), cluster_id).build()))
            .await
    }
}

// gRPC-specific convenience constructors
#[cfg(feature = "grpc")]
impl TestTransportHandle<GrpcTransport> {
    /// Create a new gRPC transport with OS-provided port
    pub async fn new_grpc() -> anyhow::Result<Self> {
        Self::with_factory(|| {
            let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
            GrpcTransportBuilder::new().from_listener(listener)?.build()
        })
        .await
    }
}

// ZMQ-specific convenience constructors
#[cfg(feature = "zmq")]
impl TestTransportHandle<ZmqTransport> {
    /// Create a new ZMQ transport with OS-assigned port
    pub async fn new_zmq() -> anyhow::Result<Self> {
        Self::with_factory(|| {
            ZmqTransportBuilder::new()
                .bind_endpoint("tcp://127.0.0.1:0")
                .build()
        })
        .await
    }
}

/// Multi-transport test cluster
///
/// A generic cluster that works with any transport implementation.
/// All transports in the cluster are registered with each other in a full mesh topology.
pub struct TestCluster<T: Transport> {
    transports: Vec<TestTransportHandle<T>>,
}

impl<T: Transport> TestCluster<T> {
    /// Create a new test cluster using a factory function
    ///
    /// This is the generic constructor that works with any transport implementation.
    /// The factory function will be called `size` times to create each transport.
    ///
    /// # Example
    /// ```ignore
    /// let cluster = TestCluster::with_factory(3, || {
    ///     MyTransportBuilder::new().build()
    /// }).await?;
    /// ```
    pub async fn with_factory<F>(size: usize, factory: F) -> anyhow::Result<Self>
    where
        F: Fn() -> anyhow::Result<T>,
    {
        let mut transports = Vec::new();

        for _ in 0..size {
            transports.push(TestTransportHandle::with_factory(&factory).await?);
        }

        // Register all peers with each other (full mesh)
        for i in 0..transports.len() {
            for j in 0..transports.len() {
                if i != j {
                    transports[i].register_peer(&transports[j])?;
                }
            }
        }

        Ok(Self { transports })
    }

    /// Get a transport by index
    pub fn get(&self, index: usize) -> &TestTransportHandle<T> {
        &self.transports[index]
    }

    /// Get all transports
    pub fn all(&self) -> &[TestTransportHandle<T>] {
        &self.transports
    }

    /// Shutdown all transports
    pub fn shutdown(self) {
        for transport in self.transports {
            transport.shutdown();
        }
    }
}

// TCP-specific convenience constructor
impl TestCluster<TcpTransport> {
    /// Create a new TCP test cluster with the specified number of transports
    ///
    /// This is a convenience method for creating TCP clusters.
    /// For other transport types, use `with_factory()`.
    pub async fn new(size: usize) -> anyhow::Result<Self> {
        Self::with_factory(size, || {
            let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
            TcpTransportBuilder::new().from_listener(listener)?.build()
        })
        .await
    }
}

// UDS-specific convenience constructor
#[cfg(unix)]
impl TestCluster<UdsTransport> {
    /// Create a new UDS test cluster with the specified number of transports
    pub async fn new_uds(size: usize) -> anyhow::Result<Self> {
        Self::with_factory(size, || {
            let dir =
                std::env::temp_dir().join(format!("velo-uds-test-{}", velo::InstanceId::new_v4()));
            std::fs::create_dir_all(&dir)?;
            let socket_path = dir.join("transport.sock");
            UdsTransportBuilder::new().socket_path(&socket_path).build()
        })
        .await
    }
}

// UCX-specific convenience constructor
#[cfg(all(target_os = "linux", feature = "ucx"))]
impl TestCluster<UcxTransport> {
    /// Create a new UCX test cluster (tcp lane; see `new_ucx`).
    pub async fn new_ucx(size: usize) -> anyhow::Result<Self> {
        tokio::time::timeout(
            UCX_STARTUP_TIMEOUT,
            Self::with_factory(size, || UcxTransportBuilder::new().tls("tcp").build()),
        )
        .await
        .map_err(|_| anyhow::anyhow!("ucx cluster startup timed out"))?
    }
}

// // HTTP-specific convenience constructor
// #[cfg(feature = "http")]
// impl TestCluster<HttpTransport> {
//     /// Create a new HTTP test cluster with the specified number of transports
//     ///
//     /// This is a convenience method for creating HTTP clusters.
//     /// For other transport types, use `with_factory()`.
//     pub async fn new_http(size: usize) -> anyhow::Result<Self> {
//         Self::with_factory(size, || {
//             // Use default builder which binds to OS-provided ports
//             HttpTransportBuilder::new().build()
//         })
//         .await
//     }
// }

// NATS-specific convenience constructor
#[cfg(feature = "nats-transport")]
impl TestCluster<NatsTransport> {
    /// Create a new NATS test cluster sharing a single cluster_id (TEST-06).
    ///
    /// All nodes share the same cluster_id so they can exchange messages.
    /// The client is shared via Arc.
    pub async fn new_nats(size: usize, cluster_id: &str) -> anyhow::Result<Self> {
        let client = velo::transports::nats::utils::connect(&nats_url()).await?;
        Self::with_factory(size, || {
            Ok(NatsTransportBuilder::new(client.clone(), cluster_id).build())
        })
        .await
    }
}

// gRPC-specific convenience constructor
#[cfg(feature = "grpc")]
impl TestCluster<GrpcTransport> {
    /// Create a new gRPC test cluster with the specified number of transports
    pub async fn new_grpc(size: usize) -> anyhow::Result<Self> {
        Self::with_factory(size, || {
            let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
            GrpcTransportBuilder::new().from_listener(listener)?.build()
        })
        .await
    }
}

// ZMQ-specific convenience constructor
#[cfg(feature = "zmq")]
impl TestCluster<ZmqTransport> {
    /// Create a new ZMQ test cluster with the specified number of transports
    pub async fn new_zmq(size: usize) -> anyhow::Result<Self> {
        Self::with_factory(size, || {
            ZmqTransportBuilder::new()
                .bind_endpoint("tcp://127.0.0.1:0")
                .build()
        })
        .await
    }
}

// Helper utilities

/// Get a random available port
pub fn get_random_port() -> u16 {
    use std::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Create test data with the specified size
pub fn test_data(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 256) as u8).collect()
}

/// Create a test message with predictable content
pub fn test_message(id: u32) -> (Vec<u8>, Vec<u8>) {
    let header = format!("header-{}", id).into_bytes();
    let payload = format!("payload-{}", id).into_bytes();
    (header, payload)
}

/// Assert that a received message matches expected values
pub fn assert_message_eq(
    received: (Bytes, Bytes),
    expected_header: &[u8],
    expected_payload: &[u8],
) {
    assert_eq!(received.0.as_ref(), expected_header, "Header mismatch");
    assert_eq!(received.1.as_ref(), expected_payload, "Payload mismatch");
}

// ---------------------------------------------------------------------------
// ShutdownTestClient trait + implementations
// ---------------------------------------------------------------------------

/// Trait abstracting over transport-specific shutdown test operations.
///
/// This allows shutdown tests to be written generically and instantiated
/// for TCP, UDS, etc. via the `transport_shutdown_tests!` macro.
pub trait ShutdownTestClient {
    type Transport: Transport;
    type Stream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send;

    /// Create a new transport handle for testing.
    fn new_handle()
    -> impl std::future::Future<Output = anyhow::Result<TestTransportHandle<Self::Transport>>> + Send;

    /// Connect a raw client to the transport and send one frame. Returns the stream.
    fn connect_and_send_frame(
        handle: &TestTransportHandle<Self::Transport>,
        msg_type: MessageType,
        header: &[u8],
        payload: &[u8],
    ) -> impl std::future::Future<Output = Self::Stream> + Send;

    /// Read one frame from the raw stream.
    fn read_one_frame(
        stream: &mut Self::Stream,
    ) -> impl std::future::Future<Output = (MessageType, Bytes, Bytes)> + Send;
}

/// TCP shutdown test client
pub struct TcpShutdownClient;

impl ShutdownTestClient for TcpShutdownClient {
    type Transport = TcpTransport;
    type Stream = tokio::net::TcpStream;

    async fn new_handle() -> anyhow::Result<TestTransportHandle<Self::Transport>> {
        TestTransportHandle::new_tcp().await
    }

    async fn connect_and_send_frame(
        handle: &TestTransportHandle<Self::Transport>,
        msg_type: MessageType,
        header: &[u8],
        payload: &[u8],
    ) -> Self::Stream {
        use velo::transports::InterfaceEndpoint;
        use velo::transports::tcp::TcpFrameCodec;

        let addr = {
            let wa = handle.transport.address();
            let key = handle.transport.key();
            let endpoint = wa.get_entry(&key).unwrap().unwrap();
            // Try new msgpack format first, fall back to legacy string
            if let Ok(endpoints) = rmp_serde::from_slice::<Vec<InterfaceEndpoint>>(&endpoint) {
                endpoints[0].socket_addr().unwrap()
            } else {
                let s = std::str::from_utf8(&endpoint).unwrap();
                let s = s.strip_prefix("tcp://").unwrap_or(s);
                s.parse::<std::net::SocketAddr>().unwrap()
            }
        };
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        TcpFrameCodec::encode_frame(&mut stream, msg_type, header, payload)
            .await
            .unwrap();
        stream
    }

    async fn read_one_frame(stream: &mut Self::Stream) -> (MessageType, Bytes, Bytes) {
        use futures::StreamExt;
        use tokio_util::codec::Framed;
        use velo::transports::tcp::TcpFrameCodec;

        let mut framed = Framed::new(stream, TcpFrameCodec::new());
        framed.next().await.unwrap().unwrap()
    }
}

/// UDS shutdown test client
#[cfg(unix)]
pub struct UdsShutdownClient;

#[cfg(unix)]
impl ShutdownTestClient for UdsShutdownClient {
    type Transport = UdsTransport;
    type Stream = tokio::net::UnixStream;

    async fn new_handle() -> anyhow::Result<TestTransportHandle<Self::Transport>> {
        TestTransportHandle::new_uds().await
    }

    async fn connect_and_send_frame(
        handle: &TestTransportHandle<Self::Transport>,
        msg_type: MessageType,
        header: &[u8],
        payload: &[u8],
    ) -> Self::Stream {
        use velo::transports::tcp::TcpFrameCodec;

        let socket_path = {
            let wa = handle.transport.address();
            let key = handle.transport.key();
            let endpoint = wa.get_entry(&key).unwrap().unwrap();
            let s = std::str::from_utf8(&endpoint).unwrap();
            let s = s.strip_prefix("uds://").unwrap_or(s);
            std::path::PathBuf::from(s)
        };
        let mut stream = tokio::net::UnixStream::connect(&socket_path).await.unwrap();
        TcpFrameCodec::encode_frame(&mut stream, msg_type, header, payload)
            .await
            .unwrap();
        stream
    }

    async fn read_one_frame(stream: &mut Self::Stream) -> (MessageType, Bytes, Bytes) {
        use futures::StreamExt;
        use tokio_util::codec::Framed;
        use velo::transports::tcp::TcpFrameCodec;

        let mut framed = Framed::new(stream, TcpFrameCodec::new());
        framed.next().await.unwrap().unwrap()
    }
}

// ---------------------------------------------------------------------------
// Test generation macros
// ---------------------------------------------------------------------------

/// Macro to generate integration tests for a transport factory.
///
/// This eliminates the boilerplate of writing individual `#[tokio::test]` functions
/// for each scenario when the only difference is the factory type parameter.
#[allow(unused_macros)]
macro_rules! transport_integration_tests {
    ($factory:ty) => {
        paste::paste! {
            #[tokio::test]
            async fn test_single_message_round_trip() {
                tokio::time::timeout(OUTER_TEST_TIMEOUT, scenarios::single_message_round_trip::<$factory>())
                    .await
                    .expect("single_message_round_trip timed out");
            }
            #[tokio::test]
            async fn test_bidirectional_messaging() {
                tokio::time::timeout(OUTER_TEST_TIMEOUT, scenarios::bidirectional_messaging::<$factory>())
                    .await
                    .expect("bidirectional_messaging timed out");
            }
            #[tokio::test]
            async fn test_multiple_messages_same_connection() {
                tokio::time::timeout(OUTER_TEST_TIMEOUT, scenarios::multiple_messages_same_connection::<$factory>())
                    .await
                    .expect("multiple_messages_same_connection timed out");
            }
            #[tokio::test]
            async fn test_response_message_type() {
                tokio::time::timeout(OUTER_TEST_TIMEOUT, scenarios::response_message_type::<$factory>())
                    .await
                    .expect("response_message_type timed out");
            }
            #[tokio::test]
            async fn test_event_message_type() {
                tokio::time::timeout(OUTER_TEST_TIMEOUT, scenarios::event_message_type::<$factory>())
                    .await
                    .expect("event_message_type timed out");
            }
            #[tokio::test]
            async fn test_ack_message_type() {
                tokio::time::timeout(OUTER_TEST_TIMEOUT, scenarios::ack_message_type::<$factory>())
                    .await
                    .expect("ack_message_type timed out");
            }
            #[tokio::test]
            async fn test_mixed_message_types() {
                tokio::time::timeout(OUTER_TEST_TIMEOUT, scenarios::mixed_message_types::<$factory>())
                    .await
                    .expect("mixed_message_types timed out");
            }
            #[tokio::test]
            async fn test_large_payload() {
                tokio::time::timeout(OUTER_TEST_TIMEOUT, scenarios::large_payload::<$factory>())
                    .await
                    .expect("large_payload timed out");
            }
            #[tokio::test]
            async fn test_empty_header_and_payload() {
                tokio::time::timeout(OUTER_TEST_TIMEOUT, scenarios::empty_header_and_payload::<$factory>())
                    .await
                    .expect("empty_header_and_payload timed out");
            }
            #[tokio::test]
            async fn test_cluster_mesh_communication() {
                tokio::time::timeout(OUTER_TEST_TIMEOUT, scenarios::cluster_mesh_communication::<$factory>())
                    .await
                    .expect("cluster_mesh_communication timed out");
            }
            #[tokio::test]
            async fn test_concurrent_senders() {
                tokio::time::timeout(OUTER_TEST_TIMEOUT, scenarios::concurrent_senders::<$factory>())
                    .await
                    .expect("concurrent_senders timed out");
            }
            #[tokio::test]
            async fn test_send_to_unregistered_peer() {
                tokio::time::timeout(OUTER_TEST_TIMEOUT, scenarios::send_to_unregistered_peer::<$factory>())
                    .await
                    .expect("send_to_unregistered_peer timed out");
            }
            #[tokio::test]
            async fn test_connection_reuse() {
                tokio::time::timeout(OUTER_TEST_TIMEOUT, scenarios::connection_reuse::<$factory>())
                    .await
                    .expect("connection_reuse timed out");
            }
            #[tokio::test]
            async fn test_graceful_shutdown() {
                tokio::time::timeout(OUTER_TEST_TIMEOUT, scenarios::graceful_shutdown::<$factory>())
                    .await
                    .expect("graceful_shutdown timed out");
            }
            #[tokio::test]
            async fn test_high_throughput() {
                tokio::time::timeout(OUTER_TEST_TIMEOUT, scenarios::high_throughput::<$factory>())
                    .await
                    .expect("high_throughput timed out");
            }
            #[tokio::test]
            async fn test_zero_copy_efficiency() {
                tokio::time::timeout(OUTER_TEST_TIMEOUT, scenarios::zero_copy_efficiency::<$factory>())
                    .await
                    .expect("zero_copy_efficiency timed out");
            }
            #[tokio::test]
            async fn test_drain_rejects_messages() {
                tokio::time::timeout(OUTER_TEST_TIMEOUT, scenarios::drain_rejects_messages::<$factory>())
                    .await
                    .expect("drain_rejects_messages timed out");
            }
            #[tokio::test]
            async fn test_drain_accepts_responses() {
                tokio::time::timeout(OUTER_TEST_TIMEOUT, scenarios::drain_accepts_responses::<$factory>())
                    .await
                    .expect("drain_accepts_responses timed out");
            }
            #[tokio::test]
            async fn test_drain_accepts_events() {
                tokio::time::timeout(OUTER_TEST_TIMEOUT, scenarios::drain_accepts_events::<$factory>())
                    .await
                    .expect("drain_accepts_events timed out");
            }
            #[tokio::test]
            async fn test_health_during_drain() {
                tokio::time::timeout(OUTER_TEST_TIMEOUT, scenarios::health_during_drain::<$factory>())
                    .await
                    .expect("health_during_drain timed out");
            }
            #[tokio::test]
            async fn test_admission_ordering_under_capacity_pressure() {
                tokio::time::timeout(OUTER_TEST_TIMEOUT, scenarios::admission_ordering_under_capacity_pressure::<$factory>())
                    .await
                    .expect("admission_ordering_under_capacity_pressure timed out");
            }
        }
    };
}

/// Generate the epoch-death admission test for a transport with per-connection
/// epochs (TCP, UDS). Not part of `transport_integration_tests!` because
/// broker-style transports have no connection to replace.
#[allow(unused_macros)]
macro_rules! transport_epoch_tests {
    ($factory:ty) => {
        #[tokio::test]
        async fn test_admissions_fail_when_the_connection_epoch_dies() {
            tokio::time::timeout(
                OUTER_TEST_TIMEOUT,
                scenarios::admissions_fail_when_the_connection_epoch_dies::<$factory>(),
            )
            .await
            .expect("admissions_fail_when_the_connection_epoch_dies timed out");
        }
    };
}

/// Macro to generate shutdown tests for a transport.
///
/// Generates tests with names like `test_{prefix}_drain_rejects_messages`.
#[allow(unused_macros)]
macro_rules! transport_shutdown_tests {
    ($prefix:ident, $client:ty) => {
        paste::paste! {
            #[tokio::test]
            async fn [<test_ $prefix _drain_rejects_messages>]() {
                tokio::time::timeout(OUTER_TEST_TIMEOUT, shutdown_scenarios::drain_rejects_messages::<$client>())
                    .await
                    .expect("drain_rejects_messages timed out");
            }
            #[tokio::test]
            async fn [<test_ $prefix _drain_accepts_responses>]() {
                tokio::time::timeout(OUTER_TEST_TIMEOUT, shutdown_scenarios::drain_accepts_responses::<$client>())
                    .await
                    .expect("drain_accepts_responses timed out");
            }
            #[tokio::test]
            async fn [<test_ $prefix _drain_accepts_events>]() {
                tokio::time::timeout(OUTER_TEST_TIMEOUT, shutdown_scenarios::drain_accepts_events::<$client>())
                    .await
                    .expect("drain_accepts_events timed out");
            }
            #[tokio::test]
            async fn [<test_ $prefix _new_connection_during_drain>]() {
                tokio::time::timeout(OUTER_TEST_TIMEOUT, shutdown_scenarios::new_connection_during_drain::<$client>())
                    .await
                    .expect("new_connection_during_drain timed out");
            }
            #[tokio::test]
            async fn [<test_ $prefix _graceful_shutdown_lifecycle>]() {
                tokio::time::timeout(OUTER_TEST_TIMEOUT, shutdown_scenarios::graceful_shutdown_lifecycle::<$client>())
                    .await
                    .expect("graceful_shutdown_lifecycle timed out");
            }
            #[tokio::test]
            async fn [<test_ $prefix _queued_message_defers_drain_completion>]() {
                tokio::time::timeout(OUTER_TEST_TIMEOUT, shutdown_scenarios::queued_message_defers_drain_completion::<$client>())
                    .await
                    .expect("queued_message_defers_drain_completion timed out");
            }
            #[tokio::test]
            async fn [<test_ $prefix _shutdown_timeout_forces_teardown>]() {
                tokio::time::timeout(OUTER_TEST_TIMEOUT, shutdown_scenarios::shutdown_timeout_forces_teardown::<$client>())
                    .await
                    .expect("shutdown_timeout_forces_teardown timed out");
            }
            #[tokio::test]
            async fn [<test_ $prefix _outbound_sends_during_drain>]() {
                tokio::time::timeout(OUTER_TEST_TIMEOUT, shutdown_scenarios::outbound_sends_during_drain::<$client>())
                    .await
                    .expect("outbound_sends_during_drain timed out");
            }
            #[tokio::test]
            async fn [<test_ $prefix _connection_writer_exits_on_teardown>]() {
                tokio::time::timeout(OUTER_TEST_TIMEOUT, shutdown_scenarios::connection_writer_exits_on_teardown::<$client>())
                    .await
                    .expect("connection_writer_exits_on_teardown timed out");
            }
            #[tokio::test]
            async fn [<test_ $prefix _drain_rejection_reaches_sender>]() {
                tokio::time::timeout(OUTER_TEST_TIMEOUT, shutdown_scenarios::drain_rejection_reaches_sender::<$client>())
                    .await
                    .expect("drain_rejection_reaches_sender timed out");
            }
        }
    };
}

// Transport factory abstraction for parameterized tests

/// Transport factory trait for creating transports in parameterized tests
pub trait TransportFactory {
    type Transport: Transport;

    async fn create() -> anyhow::Result<TestTransportHandle<Self::Transport>>;
    async fn create_cluster(size: usize) -> anyhow::Result<TestCluster<Self::Transport>>;
}

/// TCP transport factory
pub struct TcpFactory;

impl TransportFactory for TcpFactory {
    type Transport = TcpTransport;

    async fn create() -> anyhow::Result<TestTransportHandle<Self::Transport>> {
        TestTransportHandle::new_tcp().await
    }

    async fn create_cluster(size: usize) -> anyhow::Result<TestCluster<Self::Transport>> {
        TestCluster::new(size).await
    }
}

/// UDS transport factory
#[cfg(unix)]
pub struct UdsFactory;

#[cfg(unix)]
impl TransportFactory for UdsFactory {
    type Transport = UdsTransport;

    async fn create() -> anyhow::Result<TestTransportHandle<Self::Transport>> {
        TestTransportHandle::new_uds().await
    }

    async fn create_cluster(size: usize) -> anyhow::Result<TestCluster<Self::Transport>> {
        TestCluster::new_uds(size).await
    }
}

/// UCX transport factory
#[cfg(all(target_os = "linux", feature = "ucx"))]
pub struct UcxFactory;

#[cfg(all(target_os = "linux", feature = "ucx"))]
impl TransportFactory for UcxFactory {
    type Transport = UcxTransport;

    async fn create() -> anyhow::Result<TestTransportHandle<Self::Transport>> {
        TestTransportHandle::new_ucx().await
    }

    async fn create_cluster(size: usize) -> anyhow::Result<TestCluster<Self::Transport>> {
        TestCluster::new_ucx(size).await
    }
}

// /// HTTP transport factory
// #[cfg(feature = "http")]
// pub struct HttpFactory;

// #[cfg(feature = "http")]
// impl TransportFactory for HttpFactory {
//     type Transport = HttpTransport;

//     async fn create() -> anyhow::Result<TestTransportHandle<Self::Transport>> {
//         TestTransportHandle::new_http().await
//     }

//     async fn create_cluster(size: usize) -> anyhow::Result<TestCluster<Self::Transport>> {
//         TestCluster::new_http(size).await
//     }
// }

/// NATS transport factory
#[cfg(feature = "nats-transport")]
pub struct NatsFactory;

#[cfg(feature = "nats-transport")]
impl TransportFactory for NatsFactory {
    type Transport = NatsTransport;

    async fn create() -> anyhow::Result<TestTransportHandle<Self::Transport>> {
        let cluster_id = format!("test-{}", velo::InstanceId::new_v4());
        TestTransportHandle::new_nats(&cluster_id).await
    }

    async fn create_cluster(size: usize) -> anyhow::Result<TestCluster<Self::Transport>> {
        let cluster_id = format!("test-{}", velo::InstanceId::new_v4());
        TestCluster::new_nats(size, &cluster_id).await
    }
}

/// gRPC transport factory
#[cfg(feature = "grpc")]
pub struct GrpcFactory;

#[cfg(feature = "grpc")]
impl TransportFactory for GrpcFactory {
    type Transport = GrpcTransport;

    async fn create() -> anyhow::Result<TestTransportHandle<Self::Transport>> {
        TestTransportHandle::new_grpc().await
    }

    async fn create_cluster(size: usize) -> anyhow::Result<TestCluster<Self::Transport>> {
        TestCluster::new_grpc(size).await
    }
}

/// ZMQ transport factory
#[cfg(feature = "zmq")]
pub struct ZmqFactory;

#[cfg(feature = "zmq")]
impl TransportFactory for ZmqFactory {
    type Transport = ZmqTransport;

    async fn create() -> anyhow::Result<TestTransportHandle<Self::Transport>> {
        TestTransportHandle::new_zmq().await
    }

    async fn create_cluster(size: usize) -> anyhow::Result<TestCluster<Self::Transport>> {
        TestCluster::new_zmq(size).await
    }
}
