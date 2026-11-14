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
    fail_start: bool,
    pending_start: bool,
    start_completed: AtomicBool,
    drained: AtomicBool,
    /// How many times `begin_drain` ran: phase 1 of each shutdown attempt.
    drain_calls: AtomicUsize,
    shut_down: AtomicBool,
    /// How many times `shutdown` ran. Counted, not asserted in the hook:
    /// the hook runs inside a `catch_unwind`, so a panic there is only logged.
    shutdown_calls: AtomicUsize,
    shutdown_complete: AtomicBool,
    shutdown_block: Option<(flume::Sender<()>, flume::Receiver<()>)>,
    shutdown_panics: bool,
    /// Set by `closed()`, after a delay, so a test can tell whether graceful
    /// shutdown waited for it.
    closed: Arc<AtomicBool>,
    send_count: AtomicUsize,
    /// The lane of the last send, as the backend handed it over.
    last_lane: AtomicUsize,
    /// When true, `start` builds a one-slot channel that nobody drains and
    /// routes sends through a gate over it, so every send past the first
    /// reports `Pending`. Lets tests exercise the backend's queued path.
    saturating: bool,
    /// The saturating mode's gate and its receiver. The receiver is kept alive
    /// so the channel stays open, and draining it releases queued admissions.
    queue: OnceLock<(AdmissionGate<()>, flume::Receiver<()>)>,
    /// Every peer `unregister` was called with, in order.
    unregistered: std::sync::Mutex<Vec<InstanceId>>,
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
            fail_start: false,
            pending_start: false,
            start_completed: AtomicBool::new(false),
            drained: AtomicBool::new(false),
            drain_calls: AtomicUsize::new(0),
            shut_down: AtomicBool::new(false),
            shutdown_complete: AtomicBool::new(false),
            shutdown_block: None,
            shutdown_panics: false,
            shutdown_calls: AtomicUsize::new(0),
            closed: Arc::new(AtomicBool::new(false)),
            send_count: AtomicUsize::new(0),
            last_lane: AtomicUsize::new(usize::MAX),
            saturating: false,
            queue: OnceLock::new(),
            unregistered: Default::default(),
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
            fail_start: false,
            pending_start: false,
            start_completed: AtomicBool::new(false),
            drained: AtomicBool::new(false),
            drain_calls: AtomicUsize::new(0),
            shut_down: AtomicBool::new(false),
            shutdown_complete: AtomicBool::new(false),
            shutdown_block: None,
            shutdown_panics: false,
            shutdown_calls: AtomicUsize::new(0),
            closed: Arc::new(AtomicBool::new(false)),
            send_count: AtomicUsize::new(0),
            last_lane: AtomicUsize::new(usize::MAX),
            saturating: true,
            queue: OnceLock::new(),
            unregistered: Default::default(),
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
    fn unregister(&self, instance_id: InstanceId) {
        self.unregistered.lock().unwrap().push(instance_id);
    }
    fn lanes(&self, _target: InstanceId) -> std::num::NonZeroU16 {
        std::num::NonZeroU16::new(4).unwrap()
    }
    fn send_message_on_lane(
        &self,
        instance_id: InstanceId,
        lane: u16,
        header: Bytes,
        payload: Bytes,
        message_type: MessageType,
        on_error: Arc<dyn TransportErrorHandler>,
    ) -> SendOutcome {
        self.last_lane.store(usize::from(lane), Ordering::Relaxed);
        self.send_message(instance_id, header, payload, message_type, on_error)
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
        Box::pin(async {
            anyhow::ensure!(!self.fail_start, "mock startup failure");
            if self.pending_start {
                std::future::pending::<()>().await;
            }
            self.start_completed.store(true, Ordering::Relaxed);
            Ok(())
        })
    }
    fn shutdown(&self) {
        self.shutdown_calls.fetch_add(1, Ordering::Relaxed);
        assert!(self.start_completed.load(Ordering::Relaxed));
        assert!(self.drained.load(Ordering::Relaxed));
        assert!(
            !self.shut_down.swap(true, Ordering::Relaxed),
            "transport shutdown called twice"
        );
        if let Some((entered, release)) = &self.shutdown_block {
            entered.send(()).unwrap();
            release.recv_timeout(Duration::from_secs(10)).unwrap();
        }
        assert!(!self.shutdown_panics, "mock teardown failure");
        self.shutdown_complete.store(true, Ordering::Release);
    }
    fn closed(&self) -> futures::future::BoxFuture<'_, ()> {
        assert!(self.start_completed.load(Ordering::Relaxed));
        assert!(self.shutdown_complete.load(Ordering::Acquire));
        let closed = self.closed.clone();
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            closed.store(true, Ordering::Relaxed);
        })
    }
    fn begin_drain(&self) {
        self.drained.store(true, Ordering::Relaxed);
        self.drain_calls.fetch_add(1, Ordering::Relaxed);
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

/// The backend reports the primary transport's lanes, and hands a send's
/// lane to it. `send_message` is lane 0.
#[tokio::test]
async fn a_send_on_a_lane_reaches_the_transport_on_that_lane() {
    let t = MockTransport::new("tcp", true);
    let (backend, _streams) = VeloBackend::new(vec![t.clone() as Arc<dyn Transport>], None)
        .await
        .unwrap();
    let peer = make_peer_info(&["tcp"]);
    let peer_id = peer.instance_id();
    backend.register_peer(peer).unwrap();

    assert_eq!(backend.lanes(peer_id).unwrap().get(), 4);
    backend
        .send_message_on_lane(
            peer_id,
            3,
            Bytes::from_static(&[1]),
            Bytes::new(),
            MessageType::Message,
            Arc::new(NoopErrorHandler),
        )
        .unwrap();
    assert_eq!(t.last_lane.load(Ordering::Relaxed), 3);
    for _ in 0..3 {
        backend
            .send_message(
                peer_id,
                Bytes::from_static(&[1]),
                Bytes::new(),
                MessageType::Message,
                Arc::new(NoopErrorHandler),
            )
            .unwrap();
        assert_eq!(
            t.last_lane.load(Ordering::Relaxed),
            0,
            "send_message is lane 0"
        );
    }
    assert!(backend.lanes(InstanceId::new_v4()).is_err());
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

/// `unregister_peer` drops the peer's routes and worker mapping, and asks
/// every transport to forget it, also one that refused it at registration:
/// that transport is not in the routes, but it can still hold the peer from
/// an earlier registration that it accepted. Other peers keep their routes,
/// and a second call changes nothing.
#[tokio::test]
async fn unregister_peer_drops_routes_and_tells_every_transport() {
    let tcp = MockTransport::new("tcp", true);
    let refuses = MockTransport::new("http", false);
    let (backend, _streams) = VeloBackend::new(
        vec![
            tcp.clone() as Arc<dyn Transport>,
            refuses.clone() as Arc<dyn Transport>,
        ],
        None,
    )
    .await
    .unwrap();

    let gone = make_peer_info(&["tcp", "http"]);
    let stays = make_peer_info(&["tcp", "http"]);
    let (gone_id, stays_id) = (gone.instance_id(), stays.instance_id());
    backend.register_peer(gone).unwrap();
    backend.register_peer(stays).unwrap();

    backend.unregister_peer(gone_id);
    assert!(!backend.is_registered(gone_id));
    assert!(backend.alternative_transport_keys(gone_id).is_none());
    assert!(
        backend
            .try_translate_worker_id(gone_id.worker_id())
            .is_err()
    );
    for transport in [&tcp, &refuses] {
        assert_eq!(*transport.unregistered.lock().unwrap(), vec![gone_id]);
    }
    let send = |target| {
        backend.send_message(
            target,
            Bytes::new(),
            Bytes::new(),
            MessageType::Message,
            Arc::new(NoopErrorHandler),
        )
    };
    let err = send(gone_id).unwrap_err();
    assert!(matches!(
        err.downcast_ref::<VeloBackendError>(),
        Some(VeloBackendError::InstanceNotRegistered(id)) if *id == gone_id
    ));

    assert!(backend.is_registered(stays_id));
    assert_eq!(
        backend
            .try_translate_worker_id(stays_id.worker_id())
            .unwrap(),
        stays_id
    );
    assert!(send(stays_id).is_ok());

    backend.unregister_peer(gone_id);
    assert!(backend.is_registered(stays_id));
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
async fn failed_start_closes_all_transports_already_started() {
    let first = MockTransport::new("first", true);
    let mut failing = MockTransport::new("failing", true);
    Arc::get_mut(&mut failing).unwrap().fail_start = true;
    let result = VeloBackend::new(
        vec![
            first.clone() as Arc<dyn Transport>,
            failing.clone() as Arc<dyn Transport>,
        ],
        None,
    )
    .await;
    assert!(result.is_err());
    assert!(first.shut_down.load(Ordering::Relaxed));
    assert!(first.closed.load(Ordering::Relaxed));
    assert!(failing.started.load(Ordering::Relaxed));
    assert!(!failing.shut_down.load(Ordering::Relaxed));
    assert!(!failing.closed.load(Ordering::Relaxed));
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
async fn test_set_transport_priority_rejects_duplicate_without_changing_priority() {
    let tcp = MockTransport::new("tcp", true);
    let ucx = MockTransport::new("ucx", true);
    let (backend, _streams) = VeloBackend::new(
        vec![tcp as Arc<dyn Transport>, ucx as Arc<dyn Transport>],
        None,
    )
    .await
    .unwrap();
    assert!(matches!(
        backend.set_transport_priority(vec![TransportKey::from("tcp"), TransportKey::from("tcp")]),
        Err(VeloBackendError::InvalidTransportPriority(_))
    ));
    assert_eq!(
        *backend.priorities.lock(),
        vec![TransportKey::from("tcp"), TransportKey::from("ucx")]
    );
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
    // Graceful shutdown returns only after each transport's close finished on
    // the wire; a QUIC transport relies on this to deliver its tail.
    assert!(
        t1.closed.load(Ordering::Relaxed),
        "graceful_shutdown did not await closed()"
    );
    assert!(
        t2.closed.load(Ordering::Relaxed),
        "graceful_shutdown did not await closed()"
    );
    assert!(backend.shutdown_state().is_draining());
    assert!(backend.shutdown_state().teardown_token().is_cancelled());
}

#[tokio::test]
async fn final_messenger_drop_stops_transports_once_after_any_teardown_path() {
    for prior_shutdown in ["none", "graceful", "token"] {
        let transport = MockTransport::new("mock", true);
        let messenger = crate::Messenger::builder()
            .add_transport(transport.clone())
            .build()
            .await
            .unwrap();
        match prior_shutdown {
            "graceful" => {
                messenger
                    .graceful_shutdown(ShutdownPolicy::WaitForever)
                    .await
            }
            "token" => messenger
                .backend()
                .shutdown_state()
                .teardown_token()
                .cancel(),
            _ => {}
        }
        let backend = messenger.backend().clone();
        drop(messenger);
        backend
            .teardown
            .get()
            .expect("drop did not request teardown")
            .clone()
            .await
            .unwrap();
        assert_eq!(
            transport.shutdown_calls.load(Ordering::Relaxed),
            1,
            "{prior_shutdown}"
        );
    }
}

/// A cancelled waiter must not interrupt hooks or let another caller skip them.
#[tokio::test]
async fn concurrent_shutdown_waits_for_hooks_after_first_waiter_is_cancelled() {
    let mut transport = MockTransport::new("mock", true);
    let (entered_tx, entered) = flume::bounded(1);
    let (release, release_rx) = flume::bounded(1);
    Arc::get_mut(&mut transport).unwrap().shutdown_block = Some((entered_tx, release_rx));
    let messenger = crate::Messenger::builder()
        .add_transport(transport.clone())
        .build()
        .await
        .unwrap();
    let first_owner = messenger.clone();
    let first = tokio::spawn(async move {
        first_owner
            .graceful_shutdown(ShutdownPolicy::WaitForever)
            .await;
    });
    tokio::time::timeout(Duration::from_secs(2), entered.recv_async())
        .await
        .unwrap()
        .unwrap();
    let mut second = Box::pin(messenger.graceful_shutdown(ShutdownPolicy::WaitForever));
    assert!(futures::poll!(second.as_mut()).is_pending());
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    assert!(futures::poll!(second.as_mut()).is_pending());
    assert!(!transport.closed.load(Ordering::Relaxed));
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), second)
        .await
        .unwrap();
    assert!(transport.closed.load(Ordering::Relaxed));
}

/// Drop must return while the hook is still blocked, even on one Tokio thread.
#[tokio::test]
async fn final_drop_does_not_join_a_blocked_shutdown_hook() {
    let mut transport = MockTransport::new("mock", true);
    let (entered_tx, entered) = flume::bounded(1);
    let (release, release_rx) = flume::bounded(1);
    Arc::get_mut(&mut transport).unwrap().shutdown_block = Some((entered_tx, release_rx));
    let messenger = crate::Messenger::builder()
        .add_transport(transport.clone())
        .build()
        .await
        .unwrap();
    let backend = messenger.backend().clone();
    drop(messenger);
    tokio::time::timeout(Duration::from_secs(2), entered.recv_async())
        .await
        .unwrap()
        .unwrap();
    assert!(!transport.shutdown_complete.load(Ordering::Acquire));
    release.send(()).unwrap();
    backend.request_teardown().await.unwrap();
}

#[test]
fn final_drop_runs_hooks_outside_tokio_and_after_runtime_shutdown() {
    for stop_runtime in [false, true] {
        let mut transport = MockTransport::new("mock", true);
        let (entered_tx, entered) = flume::bounded(1);
        let (release, release_rx) = flume::bounded(1);
        Arc::get_mut(&mut transport).unwrap().shutdown_block = Some((entered_tx, release_rx));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let messenger = runtime
            .block_on(
                crate::Messenger::builder()
                    .add_transport(transport.clone())
                    .build(),
            )
            .unwrap();
        let backend = messenger.backend().clone();
        let runtime = if stop_runtime {
            drop(runtime);
            None
        } else {
            Some(runtime)
        };
        drop(messenger);
        entered.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(!transport.shutdown_complete.load(Ordering::Acquire));
        release.send(()).unwrap();
        futures::executor::block_on(backend.request_teardown()).unwrap();
        drop(runtime);
    }
}

/// A finished teardown failure must be visible however the task that asks is
/// doing. Reading it by polling the completion asked Tokio's cooperative
/// budget for permission, so a task that had spent its budget saw no failure,
/// and a retry drained again.
#[tokio::test]
async fn a_teardown_failure_is_seen_with_the_coop_budget_spent() {
    use futures::FutureExt;
    // Each ready oneshot poll spends one unit of the task's budget.
    fn spend_coop_budget() {
        for _ in 0..1024 {
            let (tx, rx) = tokio::sync::oneshot::channel::<()>();
            let _ = tx.send(());
            let _ = rx.now_or_never();
        }
    }
    let mut transport = MockTransport::new("mock", true);
    Arc::get_mut(&mut transport).unwrap().shutdown_panics = true;
    let messenger = crate::Messenger::builder()
        .add_transport(transport.clone())
        .build()
        .await
        .unwrap();
    let backend = Arc::clone(messenger.backend());
    // Started with no waiter, so nothing else ever polls the completion.
    backend.shutdown_now();
    let seen = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            spend_coop_budget();
            if backend.teardown_failure().is_some() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    assert!(
        seen.is_ok(),
        "a finished teardown failure was hidden from a task that spent its budget"
    );
}

#[tokio::test]
async fn failed_teardown_never_reports_successful_shutdown() {
    use futures::FutureExt;
    let mut transport = MockTransport::new("mock", true);
    Arc::get_mut(&mut transport).unwrap().shutdown_panics = true;
    let messenger = crate::Messenger::builder()
        .add_transport(transport.clone())
        .build()
        .await
        .unwrap();
    let mut drains = Vec::new();
    for _ in 0..2 {
        let result =
            std::panic::AssertUnwindSafe(messenger.graceful_shutdown(ShutdownPolicy::WaitForever))
                .catch_unwind()
                .await;
        assert!(result.is_err());
        assert!(!transport.closed.load(Ordering::Relaxed));
        drains.push(transport.drain_calls.load(Ordering::Relaxed));
    }
    // A retry fails at once: the failed teardown is final, so draining again
    // would only spend the caller's budget.
    assert_eq!(drains[0], drains[1], "the retry ran the drain again");
}

/// A panicking hook must not skip the hooks after it. Teardown runs once, so a
/// skipped hook would never run: its threads and registered memory would stay
/// for the life of the process. Both hooks panic here, so the result does not
/// depend on the order in which the backend's map visits them.
#[tokio::test]
async fn a_failed_hook_does_not_skip_the_other_hooks() {
    let mut first = MockTransport::new("mock_a", true);
    Arc::get_mut(&mut first).unwrap().shutdown_panics = true;
    let mut second = MockTransport::new("mock_b", true);
    Arc::get_mut(&mut second).unwrap().shutdown_panics = true;
    let messenger = crate::Messenger::builder()
        .add_transport(first.clone())
        .add_transport(second.clone())
        .build()
        .await
        .unwrap();
    let backend = messenger.backend().clone();
    drop(messenger);
    assert!(backend.request_teardown().await.is_err());
    assert!(first.shut_down.load(Ordering::Relaxed));
    assert!(second.shut_down.load(Ordering::Relaxed));
}

/// A failed build stops the transports that started. If one of their hooks
/// panicked, its `closed` may wait on state the hook never set up, so the
/// build must return its error without waiting for it.
#[tokio::test]
async fn failed_build_with_a_panicking_hook_returns_without_closing() {
    let mut started = MockTransport::new("started", true);
    Arc::get_mut(&mut started).unwrap().shutdown_panics = true;
    let mut failing = MockTransport::new("failing", true);
    Arc::get_mut(&mut failing).unwrap().fail_start = true;
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        VeloBackend::new(vec![started.clone(), failing.clone()], None),
    )
    .await
    .expect("a failed build waited on a transport whose hook failed");
    assert!(result.is_err());
    assert!(started.shut_down.load(Ordering::Relaxed));
}

/// A retry after a failed teardown must fail at once. Teardown ran once and
/// failed for good, so running the drain and the RDMA sweep again only spends
/// their budget (30 s by default for the sweep) before the same panic.
#[tokio::test]
async fn velo_shutdown_retry_after_a_failed_hook_skips_the_drain() {
    use futures::FutureExt;
    let mut transport = MockTransport::new("mock", true);
    Arc::get_mut(&mut transport).unwrap().shutdown_panics = true;
    let velo = crate::Velo::builder()
        .add_transport(transport.clone())
        .build()
        .await
        .unwrap();
    let first = std::panic::AssertUnwindSafe(velo.graceful_shutdown(ShutdownPolicy::WaitForever))
        .catch_unwind()
        .await;
    assert!(first.is_err());
    let drains = transport.drain_calls.load(Ordering::Relaxed);
    let retry = std::panic::AssertUnwindSafe(velo.graceful_shutdown(ShutdownPolicy::WaitForever))
        .catch_unwind()
        .await;
    assert!(retry.is_err(), "a retry reported a failed teardown as done");
    assert_eq!(
        transport.drain_calls.load(Ordering::Relaxed),
        drains,
        "the retry ran the drain again"
    );
}

/// The same, when the first attempt was cut off while the hooks ran: no
/// waiter saw the failure, so the retry must look at the finished teardown
/// itself, not at what an earlier waiter observed.
#[tokio::test]
async fn velo_shutdown_retry_sees_a_failure_no_waiter_observed() {
    use futures::FutureExt;
    let mut transport = MockTransport::new("mock", true);
    let (entered_tx, entered) = flume::bounded(1);
    let (release, release_rx) = flume::bounded(1);
    {
        let mock = Arc::get_mut(&mut transport).unwrap();
        mock.shutdown_panics = true;
        mock.shutdown_block = Some((entered_tx, release_rx));
    }
    let velo = crate::Velo::builder()
        .add_transport(transport.clone())
        .build()
        .await
        .unwrap();
    {
        let first = velo.graceful_shutdown(ShutdownPolicy::WaitForever);
        tokio::pin!(first);
        tokio::select! {
            _ = &mut first => panic!("shutdown finished while its hook was blocked"),
            _ = entered.recv_async() => {}
        }
    }
    release.send(()).unwrap();
    // The hook now panics on the teardown thread. Wait for that result without
    // waiting on the completion, which would be a waiter observing it.
    let backend = velo.messenger().backend().clone();
    tokio::time::timeout(Duration::from_secs(5), async {
        while backend.teardown_failure().is_none() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the teardown thread never finished");
    let drains = transport.drain_calls.load(Ordering::Relaxed);
    let retry = std::panic::AssertUnwindSafe(velo.graceful_shutdown(ShutdownPolicy::WaitForever))
        .catch_unwind()
        .await;
    assert!(retry.is_err());
    assert_eq!(
        transport.drain_calls.load(Ordering::Relaxed),
        drains,
        "the retry ran the drain again"
    );
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

#[tokio::test]
async fn cancelled_start_stops_only_completed_transports() {
    let ready = MockTransport::new("ready", true);
    let mut pending = MockTransport::new("pending", true);
    Arc::get_mut(&mut pending).unwrap().pending_start = true;
    let mut build = Box::pin(VeloBackend::new(vec![ready.clone(), pending.clone()], None));
    assert!(futures::poll!(build.as_mut()).is_pending());
    assert!(ready.started.load(Ordering::Relaxed));
    assert!(pending.started.load(Ordering::Relaxed));
    assert!(!ready.shut_down.load(Ordering::Relaxed));
    assert!(!pending.shut_down.load(Ordering::Relaxed));
    drop(build);
    assert!(ready.shut_down.load(Ordering::Relaxed));
    assert!(!pending.shut_down.load(Ordering::Relaxed));
}

/// Records what the backend told it, in order.
#[derive(Default)]
struct RecordingObserver {
    events: parking_lot::Mutex<Vec<(&'static str, InstanceId)>>,
}

impl PeerTableObserver for RecordingObserver {
    fn registered(&self, peer: &PeerInfo) {
        self.events.lock().push(("registered", peer.instance_id()));
    }

    fn unregistered(&self, instance_id: InstanceId) {
        self.events.lock().push(("unregistered", instance_id));
    }
}

/// The backend's peer table is the one every registration path goes
/// through, so it is where the stream transport's table is kept in step.
/// The observer hears of a peer only once a messenger transport accepted it,
/// so a refused peer is not half-registered, and it hears of every
/// unregister, which is idempotent there as it is here. Only one observer
/// can be installed, and it first hears of the peers registered before it.
#[tokio::test]
async fn the_peer_table_observer_follows_registration() {
    let accepting = MockTransport::new("tcp", true);
    let (backend, _streams) = VeloBackend::new(vec![accepting as Arc<dyn Transport>], None)
        .await
        .unwrap();
    let early = make_peer_info(&["tcp"]);
    let early_id = early.instance_id();
    backend.register_peer(early).unwrap();
    let observer = Arc::new(RecordingObserver::default());
    backend
        .set_peer_observer(Arc::clone(&observer) as Arc<dyn PeerTableObserver>)
        .unwrap();
    assert!(
        backend
            .set_peer_observer(Arc::new(RecordingObserver::default()))
            .is_err()
    );

    let peer = make_peer_info(&["tcp"]);
    let id = peer.instance_id();
    backend.register_peer(peer).unwrap();
    backend.unregister_peer(id);
    backend.unregister_peer(id);
    assert_eq!(
        *observer.events.lock(),
        [
            ("registered", early_id),
            ("registered", id),
            ("unregistered", id),
            ("unregistered", id)
        ]
    );

    let refusing = MockTransport::new("tcp", false);
    let (backend, _streams) = VeloBackend::new(vec![refusing as Arc<dyn Transport>], None)
        .await
        .unwrap();
    let observer = Arc::new(RecordingObserver::default());
    backend
        .set_peer_observer(Arc::clone(&observer) as Arc<dyn PeerTableObserver>)
        .unwrap();
    assert!(backend.register_peer(make_peer_info(&["tcp"])).is_err());
    assert!(observer.events.lock().is_empty());
}

/// Register and unregister may finish in either order, but their final
/// messenger and stream tables must agree.
#[tokio::test]
async fn a_delayed_unregister_does_not_erase_a_new_stream_registration() {
    struct PausedUnregister {
        inner: Arc<MockTransport>,
        entered: flume::Sender<()>,
        release: flume::Receiver<()>,
    }

    impl Transport for PausedUnregister {
        fn key(&self) -> TransportKey {
            self.inner.key()
        }

        fn address(&self) -> WorkerAddress {
            self.inner.address()
        }

        fn register(&self, peer: PeerInfo) -> Result<(), TransportError> {
            self.inner.register(peer)
        }

        fn unregister(&self, peer: InstanceId) {
            self.inner.unregister(peer);
            self.entered.send(()).unwrap();
            self.release.recv_timeout(Duration::from_secs(5)).unwrap();
        }

        fn send_message(
            &self,
            target: InstanceId,
            header: Bytes,
            payload: Bytes,
            kind: MessageType,
            on_error: Arc<dyn TransportErrorHandler>,
        ) -> SendOutcome {
            self.inner
                .send_message(target, header, payload, kind, on_error)
        }

        fn start(
            &self,
            id: InstanceId,
            adapter: TransportAdapter,
            runtime: tokio::runtime::Handle,
        ) -> BoxFuture<'_, anyhow::Result<()>> {
            self.inner.start(id, adapter, runtime)
        }

        fn shutdown(&self) {
            self.inner.shutdown();
        }

        fn begin_drain(&self) {
            self.inner.begin_drain();
        }

        fn closed(&self) -> BoxFuture<'_, ()> {
            self.inner.closed()
        }

        fn check_health(
            &self,
            id: InstanceId,
            timeout: Duration,
        ) -> BoxFuture<'_, Result<(), HealthCheckError>> {
            self.inner.check_health(id, timeout)
        }
    }

    let (entered_tx, entered_rx) = flume::bounded(1);
    let (release_tx, release_rx) = flume::bounded(1);
    let transport = Arc::new(PausedUnregister {
        inner: MockTransport::new("tcp", true),
        entered: entered_tx,
        release: release_rx,
    });
    let (backend, _streams) = VeloBackend::new(vec![transport], None).await.unwrap();
    let backend = Arc::new(backend);
    let stream = crate::streaming::TcpFrameTransport::new(std::net::Ipv4Addr::LOCALHOST.into())
        .await
        .unwrap();
    let remote = crate::streaming::TcpFrameTransport::new(std::net::Ipv4Addr::LOCALHOST.into())
        .await
        .unwrap();
    let erased: Arc<dyn crate::streaming::FrameTransport> = stream.clone();
    backend
        .set_peer_observer(Arc::new(
            crate::streaming::peer_table::StreamPeerTable::new(Arc::downgrade(&erased)),
        ))
        .unwrap();
    let peer =
        crate::streaming::peer_table::complete_peer_info(make_peer_info(&["tcp"]), remote.as_ref());
    let id = peer.instance_id();
    backend.register_peer(peer.clone()).unwrap();
    assert!(stream.knows_peer(id.worker_id()));

    let unregistering = Arc::clone(&backend);
    let unregister = std::thread::spawn(move || unregistering.unregister_peer(id));
    tokio::time::timeout(Duration::from_secs(5), entered_rx.recv_async())
        .await
        .unwrap()
        .unwrap();
    let registering = Arc::clone(&backend);
    let (registered_tx, registered_rx) = flume::bounded(1);
    let register = std::thread::spawn(move || {
        registering.register_peer(peer).unwrap();
        registered_tx.send(()).unwrap();
    });
    // A serialized implementation can finish registering after the release.
    // The deadline releases unregister in that case without assuming an order.
    let _ = tokio::time::timeout(Duration::from_secs(1), registered_rx.recv_async()).await;
    release_tx.send(()).unwrap();
    unregister.join().unwrap();
    register.join().unwrap();

    let messenger_knows = backend.is_registered(id);
    let stream_knows = stream.knows_peer(id.worker_id());
    let connect_error = if messenger_knows {
        let bound = crate::streaming::FrameTransport::bind(remote.as_ref(), 1, 1)
            .await
            .unwrap();
        let connected = erased.connect(id.worker_id(), 1, 1).await;
        let error = connected.as_ref().err().map(|error| format!("{error:#}"));
        drop(connected);
        drop(bound);
        error
    } else {
        None
    };
    stream.shutdown().await;
    remote.shutdown().await;
    backend.graceful_shutdown(ShutdownPolicy::WaitForever).await;
    assert_eq!(
        stream_knows, messenger_knows,
        "the delayed unregister erases the re-registered peer's stream route; connect error: {connect_error:?}"
    );
    assert!(connect_error.is_none(), "{connect_error:?}");
}

/// A transport can queue `_hello` as soon as it starts. A successful
/// handshake must leave its stream endpoint registered after the build.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hello_queued_at_startup_reaches_the_stream_table() {
    struct QueuedHello {
        inner: Arc<MockTransport>,
        header: Bytes,
        payload: Bytes,
        answered: flume::Sender<()>,
    }

    impl Transport for QueuedHello {
        fn key(&self) -> TransportKey {
            self.inner.key()
        }

        fn address(&self) -> WorkerAddress {
            self.inner.address()
        }

        fn register(&self, peer: PeerInfo) -> Result<(), TransportError> {
            self.inner.register(peer)
        }

        fn send_message(
            &self,
            target: InstanceId,
            header: Bytes,
            payload: Bytes,
            kind: MessageType,
            on_error: Arc<dyn TransportErrorHandler>,
        ) -> SendOutcome {
            if matches!(kind, MessageType::Response) {
                serde_json::from_slice::<crate::messenger::server::system_handlers::HandlersResponse>(
                    &payload,
                )
                .unwrap();
                self.answered.send(()).unwrap();
            }
            self.inner
                .send_message(target, header, payload, kind, on_error)
        }

        fn start(
            &self,
            id: InstanceId,
            adapter: TransportAdapter,
            runtime: tokio::runtime::Handle,
        ) -> BoxFuture<'_, anyhow::Result<()>> {
            Box::pin(async move {
                self.inner.start(id, adapter.clone(), runtime).await?;
                assert!(matches!(
                    adapter.admit_message(self.header.clone(), self.payload.clone()),
                    AdmitOutcome::Admitted
                ));
                Ok(())
            })
        }

        fn shutdown(&self) {
            self.inner.shutdown();
        }

        fn begin_drain(&self) {
            self.inner.begin_drain();
        }

        fn closed(&self) -> BoxFuture<'_, ()> {
            self.inner.closed()
        }

        fn check_health(
            &self,
            id: InstanceId,
            timeout: Duration,
        ) -> BoxFuture<'_, Result<(), HealthCheckError>> {
            self.inner.check_health(id, timeout)
        }
    }

    let remote = crate::streaming::TcpFrameTransport::new(std::net::Ipv4Addr::LOCALHOST.into())
        .await
        .unwrap();
    let peer =
        crate::streaming::peer_table::complete_peer_info(make_peer_info(&["tcp"]), remote.as_ref());
    let id = peer.instance_id();
    let responses = crate::messenger::common::responses::ResponseManager::with_capacity(
        id.worker_id().as_u64(),
        1,
        None,
    );
    let response = responses.register_outcome().unwrap();
    let message = crate::messenger::common::messages::ActiveMessage {
        metadata: crate::messenger::common::messages::MessageMetadata::new_unary(
            response.response_id(),
            "_hello".to_string(),
            None,
        ),
        payload: Bytes::from(
            serde_json::to_vec(&crate::messenger::server::system_handlers::HelloRequest {
                peer_info: peer,
            })
            .unwrap(),
        ),
    };
    let (header, payload, _) = message.encode().unwrap();
    for attempt in 0..100 {
        let (answered_tx, answered_rx) = flume::bounded(1);
        let transport = Arc::new(QueuedHello {
            inner: MockTransport::new("tcp", true),
            header: header.clone(),
            payload: payload.clone(),
            answered: answered_tx,
        });
        let node = crate::Velo::builder()
            .add_transport(transport)
            .stream_bind_addr(std::net::Ipv4Addr::LOCALHOST.into())
            .build()
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), answered_rx.recv_async())
            .await
            .unwrap()
            .unwrap();
        let messenger_knows = node.messenger().backend().is_registered(id);
        let Some(crate::OwnedStreamTransport::Tcp(stream)) = &node.inner.stream_owner.transport
        else {
            unreachable!();
        };
        let stream_knows = stream.knows_peer(id.worker_id());
        let connect_error = if stream_knows {
            None
        } else {
            Some(
                crate::streaming::FrameTransport::connect(stream.as_ref(), id.worker_id(), 1, 1)
                    .await
                    .unwrap_err()
                    .to_string(),
            )
        };
        node.shutdown(ShutdownPolicy::WaitForever).await;
        if !stream_knows {
            remote.shutdown().await;
        }
        assert!(messenger_knows);
        assert!(
            stream_knows,
            "startup _hello missed the stream table on attempt {attempt}; connect error: {connect_error:?}"
        );
    }
    remote.shutdown().await;
}

/// A failed send's error handler can register another peer without entering the failed gate.
#[tokio::test]
async fn unregister_allows_send_error_handlers_to_register_another_peer() {
    struct RegisterOnError {
        backend: Arc<VeloBackend>,
        peer: PeerInfo,
        completed: AtomicUsize,
        workers: parking_lot::Mutex<Vec<std::thread::JoinHandle<()>>>,
    }

    impl TransportErrorHandler for RegisterOnError {
        fn on_error(&self, _: Bytes, _: Bytes, _: String) {
            let backend = Arc::clone(&self.backend);
            let peer = self.peer.clone();
            let (entered_tx, entered_rx) = flume::bounded(1);
            let (done_tx, done_rx) = flume::bounded(1);
            self.workers.lock().push(std::thread::spawn(move || {
                entered_tx.send(()).unwrap();
                backend.register_peer(peer).unwrap();
                let _ = done_tx.send(());
            }));
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            // Bound a re-entrant registration so the test can release unregister and join the worker.
            if done_rx.recv_timeout(Duration::from_secs(1)).is_ok() {
                self.completed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let tcp = Arc::new(
        crate::transports::tcp::TcpTransportBuilder::new()
            .from_listener(listener)
            .unwrap()
            .channel_capacity(1)
            .build()
            .unwrap(),
    );
    let (backend, _streams) = VeloBackend::new(vec![tcp.clone()], None).await.unwrap();
    let backend = Arc::new(backend);
    let peer = PeerInfo::new(InstanceId::new_v4(), tcp.address());
    let id = peer.instance_id();
    backend.register_peer(peer).unwrap();
    let alternate = PeerInfo::new(InstanceId::new_v4(), tcp.address());
    let alternate_id = alternate.instance_id();
    let errors = Arc::new(RegisterOnError {
        backend: Arc::clone(&backend),
        peer: alternate,
        completed: AtomicUsize::new(0),
        workers: Default::default(),
    });
    errors.on_error(Bytes::new(), Bytes::new(), "control".into());
    assert_eq!(errors.completed.load(Ordering::Relaxed), 1);

    // No yield on this runtime: the writer and gate driver cannot drain the one-slot channel yet.
    assert!(
        backend
            .send_message(
                id,
                Bytes::new(),
                Bytes::new(),
                MessageType::Message,
                Arc::new(NoopErrorHandler)
            )
            .unwrap()
            .is_admitted()
    );
    let pending = backend
        .send_message(
            id,
            Bytes::new(),
            Bytes::new(),
            MessageType::Message,
            errors.clone(),
        )
        .unwrap()
        .into_pending()
        .expect("the second send must wait in the gate");
    assert_eq!(pending.state(), AdmissionState::Pending);
    backend.unregister_peer(id);
    for worker in std::mem::take(&mut *errors.workers.lock()) {
        worker.join().unwrap();
    }
    assert_eq!(pending.state(), AdmissionState::Failed);
    assert!(backend.is_registered(alternate_id));
    backend.graceful_shutdown(ShutdownPolicy::WaitForever).await;
    assert_eq!(
        errors.completed.load(Ordering::Relaxed),
        2,
        "the send error handler's registration blocks until unregister releases the peer-table lock"
    );
}

/// A send error handler can register another peer while unregister waits for that send's route.
#[tokio::test]
async fn unregister_does_not_block_registration_from_an_active_send_error_handler() {
    struct RegisterOnError {
        backend: Arc<VeloBackend>,
        peer: PeerInfo,
        pause: AtomicBool,
        entered: flume::Sender<()>,
        release: flume::Receiver<()>,
        completed: AtomicUsize,
        errors: parking_lot::Mutex<Vec<String>>,
        workers: parking_lot::Mutex<Vec<std::thread::JoinHandle<()>>>,
    }

    impl TransportErrorHandler for RegisterOnError {
        fn on_error(&self, _: Bytes, _: Bytes, error: String) {
            self.errors.lock().push(error);
            if self.pause.load(Ordering::Relaxed) {
                self.entered.send(()).unwrap();
                self.release.recv_timeout(Duration::from_secs(5)).unwrap();
            }
            let backend = Arc::clone(&self.backend);
            let peer = self.peer.clone();
            let (done_tx, done_rx) = flume::bounded(1);
            self.workers.lock().push(std::thread::spawn(move || {
                backend.register_peer(peer).unwrap();
                let _ = done_tx.send(());
            }));
            // Bound the callback so the test releases the send and joins every worker.
            if done_rx.recv_timeout(Duration::from_secs(1)).is_ok() {
                self.completed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let tcp = Arc::new(
        crate::transports::tcp::TcpTransportBuilder::new()
            .from_listener(listener)
            .unwrap()
            .build()
            .unwrap(),
    );
    let (backend, _streams) = VeloBackend::new(vec![tcp.clone()], None).await.unwrap();
    let backend = Arc::new(backend);
    let peer = PeerInfo::new(InstanceId::new_v4(), tcp.address());
    let id = peer.instance_id();
    backend.register_peer(peer).unwrap();

    // A different route shard keeps the control independent of the send's read lock.
    let alternate = {
        let _held = backend.primary_transport.get(&id).unwrap();
        loop {
            let candidate = InstanceId::new_v4();
            if matches!(
                backend.primary_transport.try_get_mut(&candidate),
                dashmap::try_result::TryResult::Absent
            ) {
                break PeerInfo::new(candidate, tcp.address());
            }
        }
    };
    let alternate_id = alternate.instance_id();
    let (entered_tx, entered_rx) = flume::bounded(1);
    let (release_tx, release_rx) = flume::bounded(1);
    let errors = Arc::new(RegisterOnError {
        backend: Arc::clone(&backend),
        peer: alternate,
        pause: AtomicBool::new(false),
        entered: entered_tx,
        release: release_rx,
        completed: AtomicUsize::new(0),
        errors: Default::default(),
        workers: Default::default(),
    });
    errors.on_error(Bytes::new(), Bytes::new(), "control".into());
    assert_eq!(errors.completed.load(Ordering::Relaxed), 1);
    errors.pause.store(true, Ordering::Relaxed);

    // A send during teardown fails inline, while the backend still has the peer's route.
    tcp.shutdown();
    let sending_backend = Arc::clone(&backend);
    let sending_errors = Arc::clone(&errors);
    let sending = std::thread::spawn(move || {
        sending_backend
            .send_message(
                id,
                Bytes::new(),
                Bytes::new(),
                MessageType::Message,
                sending_errors,
            )
            .unwrap();
    });
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let unregistering_backend = Arc::clone(&backend);
    let unregistering = std::thread::spawn(move || unregistering_backend.unregister_peer(id));

    // Give unregister time to block on the route the send holds. Removing a
    // lock the backend adds around its route writes makes this fail.
    std::thread::sleep(Duration::from_millis(200));
    release_tx.send(()).unwrap();
    sending.join().unwrap();
    unregistering.join().unwrap();
    for worker in std::mem::take(&mut *errors.workers.lock()) {
        worker.join().unwrap();
    }
    assert!(errors.errors.lock()[1].contains("transport shut down"));
    assert!(backend.is_registered(alternate_id));
    backend.graceful_shutdown(ShutdownPolicy::WaitForever).await;
    assert_eq!(
        errors.completed.load(Ordering::Relaxed),
        2,
        "unregister holds the peer lock while waiting for the send's route guard, so the send error handler cannot register another peer"
    );
}

/// Two unregister calls can overlap while TCP still holds queued sends.
#[tokio::test]
#[ignore = "F5: a vacant route guard blocks peer registration from TCP's queued send error handler"]
async fn an_absent_route_does_not_block_a_queued_send_error_handler() {
    struct PausedUnregister {
        inner: Arc<crate::transports::tcp::TcpTransport>,
        first: AtomicBool,
        entered: flume::Sender<()>,
        release: flume::Receiver<()>,
    }

    impl Transport for PausedUnregister {
        fn key(&self) -> TransportKey {
            self.inner.key()
        }

        fn address(&self) -> WorkerAddress {
            self.inner.address()
        }

        fn register(&self, peer: PeerInfo) -> Result<(), TransportError> {
            self.inner.register(peer)
        }

        fn unregister(&self, peer: InstanceId) {
            if self.first.swap(false, Ordering::Relaxed) {
                self.entered.send(()).unwrap();
                self.release.recv_timeout(Duration::from_secs(5)).unwrap();
            }
            self.inner.unregister(peer);
        }

        fn send_message(
            &self,
            target: InstanceId,
            header: Bytes,
            payload: Bytes,
            kind: MessageType,
            on_error: Arc<dyn TransportErrorHandler>,
        ) -> SendOutcome {
            self.inner
                .send_message(target, header, payload, kind, on_error)
        }

        fn start(
            &self,
            id: InstanceId,
            adapter: TransportAdapter,
            runtime: tokio::runtime::Handle,
        ) -> BoxFuture<'_, anyhow::Result<()>> {
            self.inner.start(id, adapter, runtime)
        }

        fn shutdown(&self) {
            self.inner.shutdown();
        }

        fn begin_drain(&self) {
            self.inner.begin_drain();
        }

        fn closed(&self) -> BoxFuture<'_, ()> {
            self.inner.closed()
        }

        fn check_health(
            &self,
            id: InstanceId,
            timeout: Duration,
        ) -> BoxFuture<'_, Result<(), HealthCheckError>> {
            self.inner.check_health(id, timeout)
        }
    }

    struct RegisterOnError {
        backend: Arc<VeloBackend>,
        peer: PeerInfo,
        removed: InstanceId,
        completed: AtomicUsize,
        route_locked: AtomicBool,
        workers: parking_lot::Mutex<Vec<std::thread::JoinHandle<()>>>,
    }

    impl TransportErrorHandler for RegisterOnError {
        fn on_error(&self, _: Bytes, _: Bytes, _: String) {
            self.route_locked.store(
                matches!(
                    self.backend.primary_transport.try_get_mut(&self.removed),
                    dashmap::try_result::TryResult::Locked
                ),
                Ordering::Relaxed,
            );
            let backend = Arc::clone(&self.backend);
            let peer = self.peer.clone();
            let (entered_tx, entered_rx) = flume::bounded(1);
            let (done_tx, done_rx) = flume::bounded(1);
            self.workers.lock().push(std::thread::spawn(move || {
                entered_tx.send(()).unwrap();
                backend.register_peer(peer).unwrap();
                let _ = done_tx.send(());
            }));
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            // Bound the callback so unregister returns and every worker can be joined.
            if done_rx.recv_timeout(Duration::from_secs(1)).is_ok() {
                self.completed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let tcp = Arc::new(
        crate::transports::tcp::TcpTransportBuilder::new()
            .from_listener(listener)
            .unwrap()
            .channel_capacity(1)
            .build()
            .unwrap(),
    );
    let (entered_tx, entered_rx) = flume::bounded(1);
    let (release_tx, release_rx) = flume::bounded(1);
    let transport = Arc::new(PausedUnregister {
        inner: tcp,
        first: AtomicBool::new(true),
        entered: entered_tx,
        release: release_rx,
    });
    let (backend, _streams) = VeloBackend::new(vec![transport.clone()], None)
        .await
        .unwrap();
    let backend = Arc::new(backend);
    let peer = PeerInfo::new(InstanceId::new_v4(), transport.address());
    let id = peer.instance_id();
    backend.register_peer(peer).unwrap();

    // No send holds a route during the callbacks below. Pick a peer on the removed route's shard.
    let alternate = {
        let _held = backend.primary_transport.get(&id).unwrap();
        loop {
            let candidate = InstanceId::new_v4();
            if matches!(
                backend.primary_transport.try_get_mut(&candidate),
                dashmap::try_result::TryResult::Locked
            ) {
                break PeerInfo::new(candidate, transport.address());
            }
        }
    };
    let alternate_id = alternate.instance_id();
    let errors = Arc::new(RegisterOnError {
        backend: Arc::clone(&backend),
        peer: alternate,
        removed: id,
        completed: AtomicUsize::new(0),
        route_locked: AtomicBool::new(false),
        workers: Default::default(),
    });
    errors.on_error(Bytes::new(), Bytes::new(), "control".into());
    assert_eq!(errors.completed.load(Ordering::Relaxed), 1);
    assert!(!errors.route_locked.load(Ordering::Relaxed));

    // No runtime yield: the writer cannot drain the one-slot channel before unregister.
    assert!(
        backend
            .send_message(
                id,
                Bytes::new(),
                Bytes::new(),
                MessageType::Message,
                Arc::new(NoopErrorHandler),
            )
            .unwrap()
            .is_admitted()
    );
    let pending = backend
        .send_message(
            id,
            Bytes::new(),
            Bytes::new(),
            MessageType::Message,
            errors.clone(),
        )
        .unwrap()
        .into_pending()
        .expect("the second send waits in the gate");
    assert_eq!(pending.state(), AdmissionState::Pending);

    let unregistering_backend = Arc::clone(&backend);
    let unregistering = std::thread::spawn(move || unregistering_backend.unregister_peer(id));
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(!backend.is_registered(id));
    // The first call removed the route but has not failed the TCP queue yet.
    backend.unregister_peer(id);
    release_tx.send(()).unwrap();
    unregistering.join().unwrap();
    for worker in std::mem::take(&mut *errors.workers.lock()) {
        worker.join().unwrap();
    }
    assert_eq!(pending.state(), AdmissionState::Failed);
    assert!(backend.is_registered(alternate_id));
    backend.graceful_shutdown(ShutdownPolicy::WaitForever).await;
    assert_eq!(
        errors.completed.load(Ordering::Relaxed),
        2,
        "the second unregister keeps its vacant route guard across TCP's error callback; route locked in callback: {}",
        errors.route_locked.load(Ordering::Relaxed),
    );
}
