// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Tests for the `VeloBackend` transport orchestrator.

use super::*;
use bytes::Bytes;
use futures::future::BoxFuture;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

/// Mock transport for testing VeloBackend logic without real networking.
struct MockTransport {
    key: TransportKey,
    address: WorkerAddress,
    accept_register: bool,
    started: AtomicBool,
    drained: AtomicBool,
    shut_down: AtomicBool,
    send_count: AtomicUsize,
    /// When true, `start` builds a one-slot channel that nobody drains and
    /// routes sends through a gate over it, so every send past the first
    /// reports `Pending`. Lets tests exercise the backend's queued path.
    saturating: bool,
    /// The saturating mode's gate and its receiver. The receiver is kept alive
    /// so the channel stays open, and draining it releases queued admissions.
    queue: OnceLock<(AdmissionGate<()>, flume::Receiver<()>)>,
}

impl MockTransport {
    fn new(key: &str, accept_register: bool) -> Arc<Self> {
        let mut builder = WorkerAddressBuilder::new();
        builder
            .add_entry(key, format!("mock://{}", key).into_bytes())
            .unwrap();
        let address = builder.build().unwrap();

        Arc::new(Self {
            key: TransportKey::from(key),
            address,
            accept_register,
            started: AtomicBool::new(false),
            drained: AtomicBool::new(false),
            shut_down: AtomicBool::new(false),
            send_count: AtomicUsize::new(0),
            saturating: false,
            queue: OnceLock::new(),
        })
    }

    fn new_saturating(key: &str) -> Arc<Self> {
        let mut builder = WorkerAddressBuilder::new();
        builder
            .add_entry(key, format!("mock://{}", key).into_bytes())
            .unwrap();
        let address = builder.build().unwrap();

        Arc::new(Self {
            key: TransportKey::from(key),
            address,
            accept_register: true,
            started: AtomicBool::new(false),
            drained: AtomicBool::new(false),
            shut_down: AtomicBool::new(false),
            send_count: AtomicUsize::new(0),
            saturating: true,
            queue: OnceLock::new(),
        })
    }

    /// Take one frame off the saturating queue, releasing the next admission.
    fn drain_one(&self) {
        let (_, rx) = self.queue.get().expect("saturating transport not started");
        rx.try_recv().expect("nothing queued to drain");
    }

    /// Fail every queued admission, as a dying connection's epoch would.
    fn fail_all(&self) {
        let (gate, _) = self.queue.get().expect("saturating transport not started");
        gate.fail_all(velo_ext::AdmissionError::ConnectionReplaced);
    }
}

impl Transport for MockTransport {
    fn key(&self) -> TransportKey {
        self.key.clone()
    }
    fn address(&self) -> WorkerAddress {
        self.address.clone()
    }
    fn register(&self, _peer_info: PeerInfo) -> Result<(), TransportError> {
        if self.accept_register {
            Ok(())
        } else {
            Err(TransportError::NoEndpoint)
        }
    }
    fn send_message(
        &self,
        _instance_id: InstanceId,
        _header: Bytes,
        _payload: Bytes,
        _message_type: MessageType,
        _on_error: Arc<dyn TransportErrorHandler>,
    ) -> SendOutcome {
        self.send_count.fetch_add(1, Ordering::Relaxed);
        match self.queue.get() {
            Some((gate, _)) => gate.send(()),
            None => SendOutcome::Admitted,
        }
    }
    fn start(
        &self,
        _instance_id: InstanceId,
        _channels: TransportAdapter,
        rt: tokio::runtime::Handle,
    ) -> BoxFuture<'_, anyhow::Result<()>> {
        self.started.store(true, Ordering::Relaxed);
        if self.saturating {
            let (tx, rx) = flume::bounded(1);
            let _ = self.queue.set((AdmissionGate::new(tx, rt), rx));
        }
        Box::pin(async { Ok(()) })
    }
    fn shutdown(&self) {
        self.shut_down.store(true, Ordering::Relaxed);
    }
    fn begin_drain(&self) {
        self.drained.store(true, Ordering::Relaxed);
    }
    fn check_health(
        &self,
        _instance_id: InstanceId,
        _timeout: Duration,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), transport::HealthCheckError>> + Send + '_>,
    > {
        Box::pin(async { Ok(()) })
    }
}

struct NoopErrorHandler;
impl TransportErrorHandler for NoopErrorHandler {
    fn on_error(&self, _header: Bytes, _payload: Bytes, _error: String) {}
}

/// Counts `on_error` calls so a test can prove backend bookkeeping ran.
#[derive(Default)]
struct CountingErrorHandler {
    calls: AtomicUsize,
}
impl TransportErrorHandler for CountingErrorHandler {
    fn on_error(&self, _header: Bytes, _payload: Bytes, _error: String) {
        self.calls.fetch_add(1, Ordering::Relaxed);
    }
}

/// Helper: build a PeerInfo with entries for specified transport keys.
fn make_peer_info(keys: &[&str]) -> PeerInfo {
    let instance_id = InstanceId::new_v4();
    let mut builder = WorkerAddressBuilder::new();
    for key in keys {
        builder
            .add_entry(*key, format!("mock://{}", key).into_bytes())
            .unwrap();
    }
    let address = builder.build().unwrap();
    PeerInfo::new(instance_id, address)
}

#[tokio::test]
async fn test_new_single_transport() {
    let t = MockTransport::new("tcp", true);
    let (backend, _streams) = VeloBackend::new(vec![t.clone() as Arc<dyn Transport>], None)
        .await
        .unwrap();

    assert!(t.started.load(Ordering::Relaxed));
    // instance_id should be a valid v4 UUID (non-zero)
    assert!(!backend.instance_id().as_bytes().iter().all(|&b| b == 0));
    assert_eq!(backend.available_transports().len(), 1);
}

#[tokio::test]
async fn test_new_multiple_transports() {
    let t1 = MockTransport::new("tcp", true);
    let t2 = MockTransport::new("http", true);
    let (backend, _streams) = VeloBackend::new(
        vec![
            t1.clone() as Arc<dyn Transport>,
            t2.clone() as Arc<dyn Transport>,
        ],
        None,
    )
    .await
    .unwrap();

    assert!(t1.started.load(Ordering::Relaxed));
    assert!(t2.started.load(Ordering::Relaxed));
    assert_eq!(backend.available_transports().len(), 2);
}

#[tokio::test]
async fn test_register_peer_selects_primary_by_priority() {
    let t1 = MockTransport::new("tcp", true);
    let t2 = MockTransport::new("http", true);
    let (backend, _streams) = VeloBackend::new(
        vec![
            t1.clone() as Arc<dyn Transport>,
            t2.clone() as Arc<dyn Transport>,
        ],
        None,
    )
    .await
    .unwrap();

    let peer = make_peer_info(&["tcp", "http"]);
    let peer_id = peer.instance_id();
    backend.register_peer(peer).unwrap();

    assert!(backend.is_registered(peer_id));
    // Primary should be "tcp" (first in priority)
    let primary = backend.primary_transport.get(&peer_id).unwrap();
    assert_eq!(primary.value().key(), TransportKey::from("tcp"));
}

#[tokio::test]
async fn test_register_peer_no_compatible_transports() {
    // Transport rejects all registrations
    let t = MockTransport::new("tcp", false);
    let (backend, _streams) = VeloBackend::new(vec![t as Arc<dyn Transport>], None)
        .await
        .unwrap();

    let peer = make_peer_info(&["tcp"]);
    let result = backend.register_peer(peer);
    assert!(matches!(
        result,
        Err(VeloBackendError::NoCompatibleTransports)
    ));
}

#[tokio::test]
async fn test_register_peer_stores_worker_mapping() {
    let t = MockTransport::new("tcp", true);
    let (backend, _streams) = VeloBackend::new(vec![t as Arc<dyn Transport>], None)
        .await
        .unwrap();

    let peer = make_peer_info(&["tcp"]);
    let peer_id = peer.instance_id();
    let worker_id = peer_id.worker_id();
    backend.register_peer(peer).unwrap();

    let resolved = backend.try_translate_worker_id(worker_id).unwrap();
    assert_eq!(resolved, peer_id);
}

#[tokio::test]
async fn test_send_message_routes_to_primary() {
    let t = MockTransport::new("tcp", true);
    let (backend, _streams) = VeloBackend::new(vec![t.clone() as Arc<dyn Transport>], None)
        .await
        .unwrap();

    let peer = make_peer_info(&["tcp"]);
    let peer_id = peer.instance_id();
    backend.register_peer(peer).unwrap();

    let outcome = backend
        .send_message(
            peer_id,
            Bytes::from_static(&[1]),
            Bytes::from_static(&[2]),
            MessageType::Message,
            Arc::new(NoopErrorHandler),
        )
        .unwrap();

    assert!(
        matches!(outcome, SendOutcome::Admitted),
        "a transport with room reports Admitted"
    );
    assert_eq!(t.send_count.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn test_send_message_pending_admission() {
    // A saturated per-target channel should surface SendOutcome::Pending, and
    // the admission must resolve once the channel has room again.
    let t = MockTransport::new_saturating("tcp");
    let (backend, _streams) = VeloBackend::new(vec![t.clone() as Arc<dyn Transport>], None)
        .await
        .unwrap();

    let peer = make_peer_info(&["tcp"]);
    let peer_id = peer.instance_id();
    backend.register_peer(peer).unwrap();

    let send = || {
        backend
            .send_message(
                peer_id,
                Bytes::from_static(&[1]),
                Bytes::from_static(&[2]),
                MessageType::Message,
                Arc::new(NoopErrorHandler),
            )
            .unwrap()
    };

    assert!(send().is_admitted(), "the first send fills the one slot");

    let admission = match send() {
        SendOutcome::Pending(admission) => admission,
        SendOutcome::Admitted => panic!("a full channel must not report Admitted"),
    };
    assert_eq!(admission.state(), AdmissionState::Pending);

    t.drain_one();
    tokio::time::timeout(Duration::from_secs(5), admission)
        .await
        .expect("admission should resolve once the channel drains")
        .expect("the frame should be admitted, not failed");
}

#[tokio::test]
async fn test_consumer_hook_does_not_suppress_backend_bookkeeping() {
    // The backend installs its metric/error hook on a Pending admission before
    // handing it to the caller. Hooks used to be a last-wins slot, so a caller
    // attaching its own observer silently disabled the backend's error
    // reporting. This pins the additive contract end to end.
    let t = MockTransport::new_saturating("tcp");
    let (backend, _streams) = VeloBackend::new(vec![t.clone() as Arc<dyn Transport>], None)
        .await
        .unwrap();

    let peer = make_peer_info(&["tcp"]);
    let peer_id = peer.instance_id();
    backend.register_peer(peer).unwrap();

    let handler = Arc::new(CountingErrorHandler::default());
    let send = || {
        backend
            .send_message(
                peer_id,
                Bytes::from_static(&[1]),
                Bytes::from_static(&[2]),
                MessageType::Message,
                handler.clone(),
            )
            .unwrap()
    };

    assert!(send().is_admitted(), "the first send fills the one slot");
    let admission = match send() {
        SendOutcome::Pending(admission) => admission,
        SendOutcome::Admitted => panic!("a full channel must not report Admitted"),
    };

    // The caller's observer joins the backend's hook rather than replacing it.
    let observed = Arc::new(AtomicBool::new(false));
    let observer = Arc::clone(&observed);
    let admission = admission.on_resolved(move |result| {
        assert!(result.is_err(), "this admission is about to be failed");
        observer.store(true, Ordering::Relaxed);
    });

    t.fail_all();
    tokio::time::timeout(Duration::from_secs(5), admission)
        .await
        .expect("admission should resolve on epoch failure")
        .expect_err("a failed epoch must fail the admission");

    assert!(
        observed.load(Ordering::Relaxed),
        "the caller's observer must run"
    );
    assert_eq!(
        handler.calls.load(Ordering::Relaxed),
        1,
        "the backend's on_error must still run for the failed frame"
    );
}

#[tokio::test]
async fn test_send_message_unregistered_peer() {
    let t = MockTransport::new("tcp", true);
    let (backend, _streams) = VeloBackend::new(vec![t as Arc<dyn Transport>], None)
        .await
        .unwrap();

    let result = backend.send_message(
        InstanceId::new_v4(),
        Bytes::new(),
        Bytes::new(),
        MessageType::Message,
        Arc::new(NoopErrorHandler),
    );
    assert!(result.is_err());
}

#[tokio::test]
async fn test_send_message_with_transport_primary_match() {
    let t = MockTransport::new("tcp", true);
    let (backend, _streams) = VeloBackend::new(vec![t.clone() as Arc<dyn Transport>], None)
        .await
        .unwrap();

    let peer = make_peer_info(&["tcp"]);
    let peer_id = peer.instance_id();
    backend.register_peer(peer).unwrap();

    backend
        .send_message_with_transport(
            peer_id,
            Bytes::from_static(&[1]),
            Bytes::from_static(&[2]),
            MessageType::Message,
            Arc::new(NoopErrorHandler),
            TransportKey::from("tcp"),
        )
        .unwrap();

    assert_eq!(t.send_count.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn test_send_message_with_transport_alternative() {
    let t1 = MockTransport::new("tcp", true);
    let t2 = MockTransport::new("http", true);
    let (backend, _streams) = VeloBackend::new(
        vec![
            t1.clone() as Arc<dyn Transport>,
            t2.clone() as Arc<dyn Transport>,
        ],
        None,
    )
    .await
    .unwrap();

    let peer = make_peer_info(&["tcp", "http"]);
    let peer_id = peer.instance_id();
    backend.register_peer(peer).unwrap();

    // Send via "http" (the alternative transport)
    backend
        .send_message_with_transport(
            peer_id,
            Bytes::from_static(&[1]),
            Bytes::from_static(&[2]),
            MessageType::Message,
            Arc::new(NoopErrorHandler),
            TransportKey::from("http"),
        )
        .unwrap();

    assert_eq!(t2.send_count.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn test_send_message_with_transport_not_found() {
    let t = MockTransport::new("tcp", true);
    let (backend, _streams) = VeloBackend::new(vec![t as Arc<dyn Transport>], None)
        .await
        .unwrap();

    let peer = make_peer_info(&["tcp"]);
    let peer_id = peer.instance_id();
    backend.register_peer(peer).unwrap();

    let result = backend.send_message_with_transport(
        peer_id,
        Bytes::new(),
        Bytes::new(),
        MessageType::Message,
        Arc::new(NoopErrorHandler),
        TransportKey::from("grpc"),
    );
    assert!(result.is_err());
}

#[tokio::test]
async fn test_try_translate_worker_id_not_found() {
    let t = MockTransport::new("tcp", true);
    let (backend, _streams) = VeloBackend::new(vec![t as Arc<dyn Transport>], None)
        .await
        .unwrap();

    let result = backend.try_translate_worker_id(InstanceId::new_v4().worker_id());
    assert!(matches!(
        result,
        Err(VeloBackendError::WorkerNotRegistered(_))
    ));
}

#[tokio::test]
async fn test_set_transport_priority_valid() {
    let t1 = MockTransport::new("tcp", true);
    let t2 = MockTransport::new("http", true);
    let (backend, _streams) = VeloBackend::new(
        vec![t1 as Arc<dyn Transport>, t2 as Arc<dyn Transport>],
        None,
    )
    .await
    .unwrap();

    // Reverse the priority
    backend
        .set_transport_priority(vec![TransportKey::from("http"), TransportKey::from("tcp")])
        .unwrap();
}

#[tokio::test]
async fn test_set_transport_priority_wrong_length() {
    let t = MockTransport::new("tcp", true);
    let (backend, _streams) = VeloBackend::new(vec![t as Arc<dyn Transport>], None)
        .await
        .unwrap();

    let result =
        backend.set_transport_priority(vec![TransportKey::from("tcp"), TransportKey::from("http")]);
    assert!(matches!(
        result,
        Err(VeloBackendError::InvalidTransportPriority(_))
    ));
}

#[tokio::test]
async fn test_set_transport_priority_unknown_key() {
    let t = MockTransport::new("tcp", true);
    let (backend, _streams) = VeloBackend::new(vec![t as Arc<dyn Transport>], None)
        .await
        .unwrap();

    let result = backend.set_transport_priority(vec![TransportKey::from("unknown")]);
    assert!(matches!(
        result,
        Err(VeloBackendError::InvalidTransportPriority(_))
    ));
}

#[tokio::test]
async fn test_graceful_shutdown_calls_all_transports() {
    let t1 = MockTransport::new("tcp", true);
    let t2 = MockTransport::new("http", true);
    let (backend, _streams) = VeloBackend::new(
        vec![
            t1.clone() as Arc<dyn Transport>,
            t2.clone() as Arc<dyn Transport>,
        ],
        None,
    )
    .await
    .unwrap();

    backend
        .graceful_shutdown(ShutdownPolicy::Timeout(Duration::from_millis(100)))
        .await;

    assert!(t1.drained.load(Ordering::Relaxed));
    assert!(t2.drained.load(Ordering::Relaxed));
    assert!(t1.shut_down.load(Ordering::Relaxed));
    assert!(t2.shut_down.load(Ordering::Relaxed));
    assert!(backend.shutdown_state().is_draining());
    assert!(backend.shutdown_state().teardown_token().is_cancelled());
}

#[tokio::test]
async fn test_peer_info_roundtrip() {
    let t = MockTransport::new("tcp", true);
    let (backend, _streams) = VeloBackend::new(vec![t as Arc<dyn Transport>], None)
        .await
        .unwrap();

    let info = backend.peer_info();
    assert_eq!(info.instance_id(), backend.instance_id());
}
