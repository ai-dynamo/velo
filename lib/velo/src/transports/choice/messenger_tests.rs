// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! A messenger with two transports, `wide` (added first, so the primary) and
//! `narrow` (a small frame capacity). Each records what it was asked to
//! send, so a test reads which transport carried a frame, and whether that
//! transport's own capacity refused it. A request "arrives" on a transport
//! through the adapter the backend numbered for it.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use prometheus::Registry;
use velo_ext::{
    HealthCheckError, InstanceId, MessageType, PeerInfo, SendOutcome, Transport, TransportAdapter,
    TransportError, TransportErrorHandler, TransportKey, WorkerAddress,
};

use super::{PeerProbeError, Route, TransportChoiceError};
use crate::Handler;
use crate::messenger::Messenger;
use crate::messenger::common::ActiveMessage;
use crate::messenger::common::messages::MessageMetadata;
use crate::messenger::common::responses::ResponseId;
use crate::observability::VeloMetrics;
use crate::transports::address::WorkerAddressBuilder;

const WIDE: usize = 1 << 20;
const NARROW: usize = 4 * 1024;

/// A transport that takes every frame up to `capacity` and refuses the rest,
/// as a real transport's frame check does: through the send's error handler.
struct Recording {
    key: TransportKey,
    capacity: usize,
    sent: Mutex<Vec<(MessageType, Bytes, Bytes)>>,
    refused: AtomicUsize,
    probes: AtomicUsize,
    adapter: OnceLock<TransportAdapter>,
}

impl Recording {
    fn new(key: &str, capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            key: TransportKey::from(key),
            capacity,
            sent: Mutex::new(Vec::new()),
            refused: AtomicUsize::new(0),
            probes: AtomicUsize::new(0),
            adapter: OnceLock::new(),
        })
    }

    fn sent_types(&self) -> Vec<MessageType> {
        self.sent
            .lock()
            .unwrap()
            .iter()
            .map(|(t, _, _)| *t)
            .collect()
    }

    fn sent_count(&self) -> usize {
        self.sent.lock().unwrap().len()
    }

    /// Hand `frame` to the runtime as if it came in on this transport.
    fn deliver(&self, (header, payload): (Bytes, Bytes)) {
        let adapter = self.adapter.get().expect("started");
        assert!(matches!(
            adapter.admit_message(header, payload),
            velo_ext::AdmitOutcome::Admitted
        ));
    }
}

impl Transport for Recording {
    fn key(&self) -> TransportKey {
        self.key.clone()
    }

    fn address(&self) -> WorkerAddress {
        address(&[self.key.as_str()])
    }

    fn max_message_size(&self, _target: InstanceId) -> Option<usize> {
        Some(self.capacity)
    }

    /// A peer is served when its address has this transport's key.
    fn register(&self, peer: PeerInfo) -> Result<(), TransportError> {
        match peer.worker_address().get_entry(self.key.as_str()) {
            Ok(Some(_)) => Ok(()),
            _ => Err(TransportError::NoEndpoint),
        }
    }

    fn send_message(
        &self,
        _target: InstanceId,
        header: Bytes,
        payload: Bytes,
        message_type: MessageType,
        on_error: Arc<dyn TransportErrorHandler>,
    ) -> SendOutcome {
        if header.len() + payload.len() > self.capacity {
            self.refused.fetch_add(1, Ordering::SeqCst);
            on_error.on_error(header, payload, format!("over {} capacity", self.key));
            return SendOutcome::Admitted;
        }
        self.sent
            .lock()
            .unwrap()
            .push((message_type, header, payload));
        SendOutcome::Admitted
    }

    fn start(
        &self,
        _instance_id: InstanceId,
        channels: TransportAdapter,
        _rt: tokio::runtime::Handle,
    ) -> futures::future::BoxFuture<'_, anyhow::Result<()>> {
        let _ = self.adapter.set(channels);
        Box::pin(async { Ok(()) })
    }

    fn shutdown(&self) {}

    fn check_health(
        &self,
        _instance_id: InstanceId,
        _timeout: Duration,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), HealthCheckError>> + Send + '_>,
    > {
        self.probes.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

/// An address with an entry for each key.
fn address(keys: &[&str]) -> WorkerAddress {
    let mut builder = WorkerAddressBuilder::new();
    for key in keys {
        builder
            .add_entry(*key, Bytes::from(format!("{key}://peer")))
            .unwrap();
    }
    builder.build().unwrap()
}

struct Node {
    messenger: Arc<Messenger>,
    wide: Arc<Recording>,
    narrow: Arc<Recording>,
    registry: Registry,
    /// A peer registered with an entry for each key in `peer_keys`.
    peer: InstanceId,
}

async fn node(peer_keys: &[&str]) -> Node {
    node_with_narrow(peer_keys, NARROW).await
}

async fn node_with_narrow(peer_keys: &[&str], narrow: usize) -> Node {
    let wide = Recording::new("wide", WIDE);
    let narrow = Recording::new("narrow", narrow);
    let registry = Registry::new();
    let messenger = crate::messenger::MessengerParts::new()
        .add_transport(wide.clone())
        .add_transport(narrow.clone())
        .metrics(Arc::new(VeloMetrics::register(&registry).unwrap()))
        .build()
        .await
        .unwrap();
    let peer = InstanceId::new_v4();
    messenger
        .register_peer(PeerInfo::new(peer, address(peer_keys)))
        .unwrap();
    // A handler that answers with as many bytes as its request names.
    messenger
        .register_handler(
            Handler::unary_handler("sized", |ctx| {
                let n = u32::from_le_bytes(ctx.payload[..4].try_into().unwrap());
                Ok(Some(Bytes::from(vec![7u8; n as usize])))
            })
            .build(),
        )
        .unwrap();
    Node {
        messenger,
        wide,
        narrow,
        registry,
        peer,
    }
}

/// A unary request from `peer` for a reply of `reply_len` bytes.
fn request_from(peer: InstanceId, reply_len: u32) -> (Bytes, Bytes) {
    let id = ResponseId::from_u128(u128::from(peer.worker_id().as_u64()) | (1u128 << 64));
    let message = ActiveMessage {
        metadata: MessageMetadata::new_unary(id, "sized".to_string(), None),
        payload: Bytes::copy_from_slice(&reply_len.to_le_bytes()),
    };
    let (header, payload, _) = crate::messenger::client::encode_outbound(message, None).unwrap();
    (header, payload)
}

/// Wait for a count that only grows to reach `at_least`.
async fn reaches(count: impl Fn() -> usize, at_least: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while count() < at_least {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("the count never got there");
}

fn reroutes(registry: &Registry, wanted: &str, used: &str) -> f64 {
    crate::observability::test_helpers::MetricSnapshot::from_registry(registry).counter(
        "velo_transport_reply_reroutes_total",
        &[("wanted", wanted), ("used", used)],
    )
}

/// The budget a send sizes against is the capacity of the transport it will
/// use. Sized to the narrow budget, a frame fills the narrow transport
/// exactly; one byte more and the narrow transport refuses it, while the same
/// payload unnamed rides the wide primary.
#[tokio::test]
async fn the_named_budget_is_exact_at_the_named_transports_capacity() {
    let node = node(&["wide", "narrow"]).await;
    let backend = node.messenger.backend();
    let route = backend
        .named_route(backend.transport_route(&"narrow".into()).unwrap())
        .unwrap();
    let budget = node
        .messenger
        .effective_eager_payload(node.peer, route, "_sink", None);
    let send = |len: usize, named: bool| {
        let mut builder = node
            .messenger
            .am_send_streaming("_sink")
            .unwrap()
            .raw_payload(Bytes::from(vec![0u8; len]))
            .instance(node.peer);
        if named {
            builder = builder.transport("narrow");
        }
        builder.send()
    };

    let primary = node
        .messenger
        .effective_eager_payload(node.peer, Route::Primary, "_sink", None);
    assert_eq!(
        WIDE - primary,
        NARROW - budget,
        "one envelope comes off each transport's own capacity"
    );

    send(budget, true)
        .await
        .expect("a budget-sized named send fits");
    let (header, payload) = {
        let sent = node.narrow.sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        (sent[0].1.clone(), sent[0].2.clone())
    };
    assert_eq!(header.len() + payload.len(), NARROW);

    let over = send(budget + 1, true).await;
    assert!(over.is_err(), "one byte over the named capacity is refused");
    assert_eq!(node.narrow.refused.load(Ordering::SeqCst), 1);
    assert_eq!(node.wide.sent_count(), 0, "a named send never falls back");

    send(budget + 1, false)
        .await
        .expect("the primary carries it");
    assert_eq!(node.wide.sent_count(), 1);
    assert_eq!(node.narrow.sent_count(), 1);
}

/// A reply goes back over the transport its request came in on, and that
/// transport's capacity bounds it: a frame that fills the narrow capacity is
/// sent on the narrow transport, and one byte more is never sent on the
/// primary. The narrow transport then carries an error reply in its place
/// (`a_reply_over_its_transports_capacity_is_answered_with_an_error`).
#[tokio::test]
async fn a_reply_follows_its_request_and_that_transports_capacity() {
    let node = node(&["wide", "narrow"]).await;
    // An empty reply first, to learn the reply header's size: it carries the
    // request's headers back, which tracing can add to.
    node.narrow.deliver(request_from(node.peer, 0));
    reaches(|| node.narrow.sent_count(), 1).await;
    let header_len = node.narrow.sent.lock().unwrap()[0].1.len();
    let fits = (NARROW - header_len) as u32;

    node.narrow.deliver(request_from(node.peer, fits));
    reaches(|| node.narrow.sent_count(), 2).await;
    assert_eq!(
        node.narrow.sent_types(),
        vec![MessageType::Response, MessageType::Response]
    );
    let (_, header, payload) = node.narrow.sent.lock().unwrap()[1].clone();
    assert_eq!(header.len() + payload.len(), NARROW);

    node.narrow.deliver(request_from(node.peer, fits + 1));
    reaches(|| node.narrow.sent_count(), 3).await;
    let (_, header, payload) = node.narrow.sent.lock().unwrap()[2].clone();
    assert!(header.len() + payload.len() < NARROW, "the error reply");
    assert_eq!(node.narrow.refused.load(Ordering::SeqCst), 0);
    assert_eq!(node.wide.sent_count(), 0, "no reply leaves on the primary");
    assert_eq!(reroutes(&node.registry, "narrow", "wide"), 0.0);

    // The same request on the primary is answered on the primary.
    node.wide.deliver(request_from(node.peer, fits + 1));
    reaches(|| node.wide.sent_count(), 1).await;
    assert_eq!(node.wide.sent_types(), vec![MessageType::Response]);
}

/// A request that came in on a transport the caller is not registered on is
/// answered over the caller's primary, and counted with both transports, so
/// a pinned edge whose replies leave it can be alerted on.
#[tokio::test]
async fn a_reply_the_arrival_transport_cannot_carry_goes_over_the_primary_and_is_counted() {
    let node = node(&["wide"]).await;
    node.narrow.deliver(request_from(node.peer, 8));
    reaches(|| node.wide.sent_count(), 1).await;
    assert_eq!(node.wide.sent_types(), vec![MessageType::Response]);
    assert_eq!(node.narrow.sent_count(), 0);
    assert_eq!(reroutes(&node.registry, "narrow", "wide"), 1.0);
}

/// The choice errors are typed, need no slot, and never send.
#[tokio::test]
async fn choice_errors_are_typed_and_send_nothing() {
    let node = node(&["wide"]).await;
    let backend = node.messenger.backend();

    assert_eq!(
        backend.transport_route(&"quic".into()),
        Err(TransportChoiceError::NotInstalled {
            transport: "quic".into()
        })
    );
    let error = node
        .messenger
        .unary("sized")
        .unwrap()
        .raw_payload(Bytes::from_static(&[0, 0, 0, 0]))
        .instance(node.peer)
        .transport("narrow")
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<TransportChoiceError>(),
        Some(&TransportChoiceError::NotServedByPeer {
            peer: node.peer,
            transport: "narrow".into()
        })
    );
    assert_eq!(
        node.messenger
            .client()
            .response_manager
            .pending_outcome_count(),
        0
    );

    let other = node_with_id().await;
    let foreign = other.backend().transport_route(&"wide".into()).unwrap();
    let error = node
        .messenger
        .am_send("sized")
        .unwrap()
        .raw_payload(Bytes::new())
        .instance(node.peer)
        .route(foreign)
        .send()
        .await
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<TransportChoiceError>(),
        Some(TransportChoiceError::ForeignRoute { .. })
    ));
    assert_eq!(node.wide.sent_count() + node.narrow.sent_count(), 0);
}

async fn node_with_id() -> Arc<Messenger> {
    crate::messenger::MessengerParts::new()
        .add_transport(Recording::new("wide", WIDE))
        .build()
        .await
        .unwrap()
}

/// A call pinned to a transport sends its handshake there too: with the
/// peer's handlers unknown, the `_hello` that precedes the call goes over the
/// named transport, and nothing goes over the primary.
#[tokio::test]
async fn the_handshake_of_a_named_call_uses_the_named_transport() {
    let node = node(&["wide", "narrow"]).await;
    let call = node
        .messenger
        .unary("sized")
        .unwrap()
        .raw_payload(Bytes::from_static(&[0, 0, 0, 0]))
        .instance(node.peer)
        .transport("narrow")
        .send();
    // Nobody answers the `_hello`; the call stays parked on it.
    let pending = tokio::spawn(call);
    reaches(|| node.narrow.sent_count(), 1).await;
    pending.abort();
    let (_, header, _) = node.narrow.sent.lock().unwrap()[0].clone();
    assert_eq!(
        crate::messenger::common::messages::handler_name_from_request_header(&header),
        Some(&b"_hello"[..])
    );
    assert_eq!(node.wide.sent_count(), 0);
}

/// `peer_transports` lists the transports the peer is registered on, primary
/// first; `route_capacity` reads the named transport's capacity; `probe_peer`
/// probes the named transport only. A transport the peer is not on is an
/// error for both, not a fallback.
#[tokio::test]
async fn peer_transports_capacity_and_probe_read_the_named_transport() {
    let node = node(&["wide", "narrow"]).await;
    let backend = node.messenger.backend();
    assert_eq!(
        backend.peer_transports(node.peer),
        Some(vec!["wide".into(), "narrow".into()])
    );
    assert_eq!(
        backend.transport_keys(),
        vec!["wide".into(), "narrow".into()]
    );
    let narrow = backend.transport_route(&"narrow".into()).unwrap();
    assert_eq!(backend.route_capacity(node.peer, narrow), Ok(Some(NARROW)));
    backend
        .probe_peer(node.peer, narrow, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(node.narrow.probes.load(Ordering::SeqCst), 1);
    assert_eq!(node.wide.probes.load(Ordering::SeqCst), 0);

    let stranger = InstanceId::new_v4();
    assert_eq!(backend.peer_transports(stranger), None);
    assert_eq!(
        backend.route_capacity(stranger, narrow),
        Err(TransportChoiceError::PeerNotRegistered { peer: stranger })
    );
    // No probe ran, so this is a route that cannot be used, not a failed
    // probe: the same error as `route_capacity` and a named send give.
    assert!(matches!(
        backend
            .probe_peer(stranger, narrow, Duration::from_secs(1))
            .await,
        Err(PeerProbeError::Choice(TransportChoiceError::PeerNotRegistered { peer }))
            if peer == stranger
    ));
    assert_eq!(node.narrow.probes.load(Ordering::SeqCst), 1);

    let only_wide = node_only_wide().await;
    let backend = only_wide.messenger.backend();
    let narrow = backend.transport_route(&"narrow".into()).unwrap();
    assert_eq!(
        backend.peer_transports(only_wide.peer),
        Some(vec!["wide".into()])
    );
    let not_served = TransportChoiceError::NotServedByPeer {
        peer: only_wide.peer,
        transport: "narrow".into(),
    };
    assert_eq!(
        backend.route_capacity(only_wide.peer, narrow),
        Err(not_served.clone())
    );
    assert!(matches!(
        backend
            .probe_peer(only_wide.peer, narrow, Duration::from_secs(1))
            .await,
        Err(PeerProbeError::Choice(error)) if error == not_served
    ));
    assert_eq!(only_wide.narrow.probes.load(Ordering::SeqCst), 0);
    assert_eq!(only_wide.wide.probes.load(Ordering::SeqCst), 0);
}

async fn node_only_wide() -> Node {
    node(&["wide"]).await
}

/// Registering the peer again with fewer transports clears their bits: a
/// send named onto one it dropped fails, and the route reports it.
#[tokio::test]
async fn a_re_registration_replaces_the_served_transports() {
    let node = node(&["wide", "narrow"]).await;
    let backend = node.messenger.backend();
    let narrow = backend.transport_route(&"narrow".into()).unwrap();
    assert!(
        backend
            .check_served(node.peer, Route::Named(narrow.origin))
            .is_ok()
    );
    node.messenger
        .register_peer(PeerInfo::new(node.peer, address(&["wide"])))
        .unwrap();
    assert_eq!(
        backend.peer_transports(node.peer),
        Some(vec!["wide".into()])
    );
    assert!(
        backend
            .check_served(node.peer, Route::Named(narrow.origin))
            .is_err()
    );
}

/// A named send that the peer cannot take is refused before slot
/// acquisition, so it reports the route even with every slot taken: a
/// caller that refuses edges learns why, not that the arena is full. The
/// unnamed send is the control that the arena really is full.
#[tokio::test]
async fn a_refused_route_is_reported_with_the_slot_arena_exhausted() {
    let node = node(&["wide"]).await;
    let client = node.messenger.client();
    let mut held = Vec::new();
    while let Ok(awaiter) = client.register_outcome() {
        held.push(awaiter);
    }
    assert!(!held.is_empty());

    let unnamed = node
        .messenger
        .am_send("sized")
        .unwrap()
        .raw_payload(Bytes::new())
        .instance(node.peer)
        .send()
        .await
        .unwrap_err();
    assert!(
        unnamed.downcast_ref::<TransportChoiceError>().is_none(),
        "the control send fails on the arena: {unnamed}"
    );

    for named in [
        node.messenger
            .am_send("sized")
            .unwrap()
            .transport("narrow")
            .raw_payload(Bytes::new())
            .instance(node.peer)
            .send()
            .await,
        node.messenger
            .am_send("sized")
            .unwrap()
            .transport("narrow")
            .raw_payload(Bytes::new())
            .worker(node.peer.worker_id())
            .send()
            .await,
    ] {
        assert_eq!(
            named.unwrap_err().downcast_ref::<TransportChoiceError>(),
            Some(&TransportChoiceError::NotServedByPeer {
                peer: node.peer,
                transport: "narrow".into()
            })
        );
    }
    assert_eq!(node.wide.sent_count() + node.narrow.sent_count(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_timed_out_named_send_waiting_for_a_slot_stays_unsent() {
    use futures::future::BoxFuture;
    use tokio::sync::Notify;
    use velo_ext::{PeerDiscovery, WorkerId};

    struct Discovery {
        peer: PeerInfo,
        entered: Notify,
        release: Notify,
    }

    impl PeerDiscovery for Discovery {
        fn discover_by_worker_id(
            &self,
            worker: WorkerId,
        ) -> BoxFuture<'_, anyhow::Result<PeerInfo>> {
            Box::pin(async move {
                assert_eq!(worker, self.peer.instance_id().worker_id());
                self.entered.notify_one();
                self.release.notified().await;
                Ok(self.peer.clone())
            })
        }

        fn discover_by_instance_id(
            &self,
            _: InstanceId,
        ) -> BoxFuture<'_, anyhow::Result<PeerInfo>> {
            unreachable!("the send resolves a worker ID");
        }
    }

    let peer = InstanceId::new_v4();
    let worker = peer.worker_id();
    let discovery = Arc::new(Discovery {
        peer: PeerInfo::new(peer, address(&["narrow"])),
        entered: Notify::new(),
        release: Notify::new(),
    });
    let narrow = Recording::new("narrow", NARROW);
    let messenger = crate::messenger::MessengerParts::new()
        .discovery(discovery.clone())
        .add_transport(narrow.clone())
        .build()
        .await
        .unwrap();
    let client = messenger.client();
    let mut held = Vec::new();
    while let Ok(awaiter) = client.register_outcome() {
        held.push(awaiter);
    }
    assert!(!held.is_empty());

    let deadline = Duration::from_secs(10);
    let mut call = messenger
        .unary("sized")
        .unwrap()
        .transport("narrow")
        .worker(worker)
        .await_capacity()
        .timeout(deadline)
        .send();
    discovery.entered.notified().await;
    discovery.release.notify_one();
    reaches(
        || usize::from(messenger.backend().try_translate_worker_id(worker).is_ok()),
        1,
    )
    .await;
    let error = (&mut call).await.unwrap_err();
    assert!(error.is::<crate::RequestTimeout>());
    assert_eq!(call.admission_state(), velo_ext::AdmissionState::Failed);
    assert_eq!(narrow.sent_count(), 0);

    held.clear();
    let sent = tokio::time::timeout(Duration::from_millis(100), async {
        while narrow.sent_count() == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    assert!(
        sent.is_err(),
        "the timed-out call sent a frame after a slot freed"
    );
}

/// A named send whose payload the stager would take is refused before it
/// takes a slot. Staging would move the payload's bytes in a fetch the call
/// does not route, possibly by RDMA, so a call that keeps to one transport
/// cannot stage. One byte under the threshold goes out on the named
/// transport; the unnamed send of the same large payload stages as before.
#[tokio::test]
async fn a_named_send_that_would_stage_is_refused_before_a_slot() {
    const THRESHOLD: usize = 1024;
    let node = node(&["wide", "narrow"]).await;
    let manager = Arc::new(crate::rendezvous::RendezvousManager::new(
        velo_ext::WorkerId::from_u64(7),
    ));
    node.messenger.set_large_payload_support(
        Arc::new(
            crate::rendezvous::transparent::RendezvousStager::new(manager.clone())
                .with_threshold(THRESHOLD),
        ),
        Arc::new(crate::rendezvous::transparent::RendezvousResolver::new(
            manager,
        )),
    );
    let send = |len: usize, named: bool| {
        let mut builder = node
            .messenger
            .am_send_streaming("_sink")
            .unwrap()
            .raw_payload(Bytes::from(vec![0u8; len]))
            .instance(node.peer);
        if named {
            builder = builder.transport("narrow");
        }
        builder.send()
    };

    let error = send(THRESHOLD + 1, true).await.unwrap_err();
    assert_eq!(
        error.downcast_ref::<TransportChoiceError>(),
        Some(&TransportChoiceError::WouldStage {
            transport: "narrow".into(),
            len: THRESHOLD + 1,
            threshold: THRESHOLD,
        })
    );
    assert_eq!(
        node.messenger
            .client()
            .response_manager
            .pending_outcome_count(),
        0
    );
    assert_eq!(node.wide.sent_count() + node.narrow.sent_count(), 0);

    send(THRESHOLD, true)
        .await
        .expect("at the threshold it is not staged");
    assert_eq!(node.narrow.sent_count(), 1);

    send(THRESHOLD + 1, false)
        .await
        .expect("an unnamed send stages");
    let (_, header, payload) = node.wide.sent.lock().unwrap()[0].clone();
    assert!(payload.is_empty(), "staging leaves no payload to carry");
    assert!(header.len() < THRESHOLD);
}

/// A sync (ack or nack) request from `peer` for handler `handler`.
fn sync_request_from(peer: InstanceId, handler: &str) -> (ResponseId, (Bytes, Bytes)) {
    let id = ResponseId::from_u128(u128::from(peer.worker_id().as_u64()) | (2u128 << 64));
    let message = ActiveMessage {
        metadata: MessageMetadata::new_sync(id, handler.to_string(), None),
        payload: Bytes::new(),
    };
    let (header, payload, _) = crate::messenger::client::encode_outbound(message, None).unwrap();
    (id, (header, payload))
}

/// The id `request_from` puts on its request.
fn unary_id(peer: InstanceId) -> ResponseId {
    ResponseId::from_u128(u128::from(peer.worker_id().as_u64()) | (1u128 << 64))
}

/// A reply that its transport cannot carry must still end the call. The
/// transport refuses an oversized frame at this node, through the send's
/// error handler, and nothing reaches the caller: a caller with no timeout
/// then waits for ever. This was so before replies followed the request, for
/// a reply over the primary's limit. Replies that follow the request made it
/// worse: a reply that fits the responder's primary can be over the limit of
/// the transport the request came in on.
///
/// So the responder checks the reply against the transport it takes, before
/// the send. A reply that is too large is not offered to the transport. The
/// caller gets a short error reply in its place, over the same transport,
/// that names the transport and both sizes. The reply never moves to the
/// primary: a caller that keeps an edge on one transport must be able to
/// rely on that for the reply too.
#[tokio::test]
async fn a_reply_over_its_transports_capacity_is_answered_with_an_error() {
    use crate::messenger::common::events::Outcome;
    use crate::messenger::common::responses::decode_response_header;

    let node = node(&["wide", "narrow"]).await;
    node.narrow
        .deliver(request_from(node.peer, (NARROW + 1) as u32));
    reaches(|| node.narrow.sent_count(), 1).await;

    let (message_type, header, payload) = node.narrow.sent.lock().unwrap()[0].clone();
    assert_eq!(message_type, MessageType::Response);
    let (id, outcome, _) = decode_response_header(header).unwrap();
    assert_eq!(id, unary_id(node.peer), "the error names the caller's call");
    assert!(matches!(outcome, Outcome::Error));
    let text = String::from_utf8(payload.to_vec()).unwrap();
    assert!(
        text.contains("narrow") && text.contains(&NARROW.to_string()),
        "the error names the transport and its limit: {text}"
    );
    assert_eq!(
        node.narrow.refused.load(Ordering::SeqCst),
        0,
        "the oversized reply was offered to the transport"
    );
    assert_eq!(node.wide.sent_count(), 0, "no reply leaves on the primary");
    assert_eq!(reroutes(&node.registry, "narrow", "wide"), 0.0);
    // The handler's own reply is counted as not sent. The count is made
    // after the error reply is handed on, so wait for it.
    reaches(
        || {
            crate::observability::test_helpers::MetricSnapshot::from_registry(&node.registry)
                .counter(
                    "velo_messenger_dispatch_failures_total",
                    &[("stage", "response_send"), ("reason", "unary")],
                ) as usize
        },
        1,
    )
    .await;
}

/// The same for a nack. Its payload is the handler's error text, which has
/// no bound. A nack over the transport's capacity is replaced by a short
/// nack, so an `am_sync` caller is answered too.
#[tokio::test]
async fn a_nack_over_its_transports_capacity_is_answered_with_a_short_nack() {
    use crate::messenger::common::events::{EventType, Outcome, decode_event_header};

    let node = node(&["wide", "narrow"]).await;
    node.messenger
        .register_handler(
            Handler::am_handler("long_error", |_ctx| {
                Err(anyhow::anyhow!("{}", "e".repeat(NARROW + 1)))
            })
            .build(),
        )
        .unwrap();
    let (id, request) = sync_request_from(node.peer, "long_error");
    node.narrow.deliver(request);
    reaches(|| node.narrow.sent_count(), 1).await;

    let (message_type, header, payload) = node.narrow.sent.lock().unwrap()[0].clone();
    assert_eq!(message_type, MessageType::Ack);
    let Some(EventType::Ack(got, Outcome::Error)) = decode_event_header(header) else {
        panic!("the reply is not a nack");
    };
    assert_eq!(got, id);
    assert!(payload.len() < NARROW);
    assert_eq!(node.narrow.refused.load(Ordering::SeqCst), 0);
    assert_eq!(node.wide.sent_count(), 0, "no reply leaves on the primary");
}

/// A named send to an instance that is not registered is refused before slot
/// acquisition, like a named send to a peer that is not on the transport.
/// With every slot taken it reports the peer, not a full arena, and with
/// `await_capacity` it does not wait for a slot to learn that it cannot
/// send. The unnamed send keeps its old behavior: it fails on the arena.
#[tokio::test]
async fn a_named_send_to_an_unregistered_instance_is_refused_before_a_slot() {
    let node = node(&["wide"]).await;
    let stranger = InstanceId::new_v4();
    let client = node.messenger.client();
    let mut held = Vec::new();
    while let Ok(awaiter) = client.register_outcome() {
        held.push(awaiter);
    }
    assert!(!held.is_empty());

    let unnamed = node
        .messenger
        .am_send("sized")
        .unwrap()
        .raw_payload(Bytes::new())
        .instance(stranger)
        .send()
        .await
        .unwrap_err();
    assert!(
        unnamed.downcast_ref::<TransportChoiceError>().is_none(),
        "the control send fails on the arena: {unnamed}"
    );

    let refused = TransportChoiceError::PeerNotRegistered { peer: stranger };
    let named = node
        .messenger
        .am_send("sized")
        .unwrap()
        .transport("wide")
        .raw_payload(Bytes::new())
        .instance(stranger)
        .send()
        .await
        .unwrap_err();
    assert_eq!(named.downcast_ref::<TransportChoiceError>(), Some(&refused));

    // With `await_capacity` the send would wait for a slot that never comes.
    let waiting = tokio::time::timeout(
        Duration::from_secs(5),
        node.messenger
            .unary("sized")
            .unwrap()
            .transport("wide")
            .raw_payload(Bytes::new())
            .instance(stranger)
            .await_capacity()
            .send(),
    )
    .await
    .expect("the named send waited for a response slot")
    .unwrap_err();
    assert_eq!(
        waiting.downcast_ref::<TransportChoiceError>(),
        Some(&refused)
    );
    assert_eq!(node.wide.sent_count() + node.narrow.sent_count(), 0);
}

/// A named send refused for a staging header says so. Its payload can be
/// one byte, so an error that reports a payload over the staging threshold
/// would send the caller to shorten a payload that is not the cause.
#[tokio::test]
async fn a_named_send_refused_for_a_staging_header_names_the_header() {
    let node = node(&["wide", "narrow"]).await;
    let error = node
        .messenger
        .am_send("sized")
        .unwrap()
        .headers(std::collections::HashMap::from([(
            crate::messenger::large_payload::RV_HEADER_KEY.to_string(),
            "1".to_string(),
        )]))
        .raw_payload(Bytes::from_static(b"x"))
        .transport("narrow")
        .instance(node.peer)
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<TransportChoiceError>(),
        Some(&TransportChoiceError::StagingHeader {
            transport: "narrow".into()
        })
    );
    let text = error.to_string();
    assert!(text.contains("_rv"), "{text}");
    assert!(!text.contains("threshold"), "{text}");
    assert_eq!(node.wide.sent_count() + node.narrow.sent_count(), 0);
}

/// A request tagged with a number that is no transport of this node is
/// still answered, over the primary. Only the runtime numbers adapters, but
/// `TransportAdapter::with_origin` is public, so a transport can number its
/// own by mistake. Its replies must not panic the handler task.
///
/// The panic was in the arguments of a `debug!` line, which run only when a
/// subscriber wants that line. So the test runs again in a child process,
/// alone, with a global subscriber at the debug level: a per-thread
/// subscriber in this shared binary may never see the line (see "A test
/// that checks a log line runs in its own process" in the coding gotchas).
#[tokio::test]
async fn a_reply_to_an_origin_that_is_no_transport_goes_over_the_primary() {
    const CHILD: &str = "VELO_TEST_UNKNOWN_ORIGIN_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let test = "transports::choice::messenger_tests::\
                    a_reply_to_an_origin_that_is_no_transport_goes_over_the_primary";
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test, "--nocapture", "--test-threads=1"])
            .env(CHILD, "1")
            .output()
            .expect("run the test in a child process");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "the test failed in its child process:\n{stdout}\n{stderr}"
        );
        assert!(
            stdout.contains("1 passed"),
            "the child process ran no test named {test}:\n{stdout}"
        );
        return;
    }
    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .with_writer(std::io::sink)
            .with_max_level(tracing::Level::DEBUG)
            .finish(),
    )
    .expect("the child process sets the only global subscriber");

    let node = node(&["wide", "narrow"]).await;
    let unknown = node
        .narrow
        .adapter
        .get()
        .unwrap()
        .clone()
        .with_origin(velo_ext::TransportOrigin::new(7));
    let (header, payload) = request_from(node.peer, 8);
    assert!(matches!(
        unknown.admit_message(header, payload),
        velo_ext::AdmitOutcome::Admitted
    ));
    reaches(|| node.wide.sent_count(), 1).await;
    assert_eq!(node.wide.sent_types(), vec![MessageType::Response]);
    assert_eq!(node.narrow.sent_count(), 0);
}

/// The error reply that takes the place of an oversized reply must fit the
/// transport too. Its text names the transport and both sizes, about 100
/// bytes. A transport that carries less would refuse it as well, and the
/// caller would again wait for ever. So the text is cut to the room that
/// the header leaves, and the frame fills the transport at most. The same
/// holds for the short nack.
#[tokio::test]
async fn the_error_for_a_reply_too_large_is_cut_to_fit_the_transport() {
    use crate::messenger::common::events::{EventType, Outcome, decode_event_header};
    use crate::messenger::common::responses::decode_response_header;

    const TINY: usize = 64;
    let node = node_with_narrow(&["wide", "narrow"], TINY).await;
    node.messenger
        .register_handler(
            Handler::am_handler("long_error", |_ctx| {
                Err(anyhow::anyhow!("{}", "e".repeat(TINY + 1)))
            })
            .build(),
        )
        .unwrap();

    node.narrow
        .deliver(request_from(node.peer, (TINY + 1) as u32));
    reaches(|| node.narrow.sent_count(), 1).await;
    let (message_type, header, payload) = node.narrow.sent.lock().unwrap()[0].clone();
    assert_eq!(message_type, MessageType::Response);
    assert_eq!(header.len() + payload.len(), TINY, "cut to fill the frame");
    let (id, outcome, _) = decode_response_header(header).unwrap();
    assert_eq!(id, unary_id(node.peer));
    assert!(matches!(outcome, Outcome::Error));
    assert!(payload.starts_with(b"Reply not sent: "));

    let (id, request) = sync_request_from(node.peer, "long_error");
    node.narrow.deliver(request);
    reaches(|| node.narrow.sent_count(), 2).await;
    let (message_type, header, payload) = node.narrow.sent.lock().unwrap()[1].clone();
    assert_eq!(message_type, MessageType::Ack);
    assert_eq!(header.len() + payload.len(), TINY, "cut to fill the frame");
    let Some(EventType::Ack(got, Outcome::Error)) = decode_event_header(header) else {
        panic!("the reply is not a nack");
    };
    assert_eq!(got, id);

    assert_eq!(node.narrow.refused.load(Ordering::SeqCst), 0);
    assert_eq!(node.wide.sent_count(), 0, "no reply leaves on the primary");
}

/// The dispatcher's own error replies take the same path as a handler's
/// replies. The error for an unknown handler carries the handler's name,
/// which the caller chose. Over the transport's capacity, it is replaced by
/// a short nack, so the caller is still answered. Before, the dispatcher
/// sent these replies itself and only logged the refusal.
#[tokio::test]
async fn an_unknown_handler_error_over_the_capacity_is_answered_with_a_short_nack() {
    use crate::messenger::common::events::{EventType, Outcome, decode_event_header};

    let node = node(&["wide", "narrow"]).await;
    let (id, request) = sync_request_from(node.peer, &"h".repeat(NARROW));
    node.narrow.deliver(request);
    reaches(|| node.narrow.sent_count(), 1).await;

    let (message_type, header, payload) = node.narrow.sent.lock().unwrap()[0].clone();
    assert_eq!(message_type, MessageType::Ack);
    let Some(EventType::Ack(got, Outcome::Error)) = decode_event_header(header) else {
        panic!("the reply is not a nack");
    };
    assert_eq!(got, id);
    assert!(payload.starts_with(b"Reply not sent: "));
    assert!(payload.len() < NARROW);
    assert_eq!(node.narrow.refused.load(Ordering::SeqCst), 0);
    assert_eq!(node.wide.sent_count(), 0, "no reply leaves on the primary");
}

/// A named call does not stage the `_hello` that goes before it. The
/// `_hello` carries this node's address, and `send_message` stages any
/// payload over the threshold: the peer would then fetch the address apart
/// from the call's route. With the real threshold (256 KiB) only an address
/// with a very large entry gets there, so the threshold here is small. The
/// call is refused before the `_hello` takes a slot. The unnamed call
/// stages its `_hello` as before.
#[tokio::test]
async fn a_named_call_whose_hello_would_stage_is_refused_before_a_slot() {
    const THRESHOLD: usize = 16;
    let node = node(&["wide", "narrow"]).await;
    let manager = Arc::new(crate::rendezvous::RendezvousManager::new(
        velo_ext::WorkerId::from_u64(7),
    ));
    node.messenger.set_large_payload_support(
        Arc::new(
            crate::rendezvous::transparent::RendezvousStager::new(manager.clone())
                .with_threshold(THRESHOLD),
        ),
        Arc::new(crate::rendezvous::transparent::RendezvousResolver::new(
            manager,
        )),
    );
    let call = |named: bool| {
        let mut builder = node
            .messenger
            .unary("sized")
            .unwrap()
            .raw_payload(Bytes::from_static(&[0, 0, 0, 0]))
            .instance(node.peer);
        if named {
            builder = builder.transport("narrow");
        }
        builder.send()
    };

    let error = tokio::time::timeout(Duration::from_secs(5), call(true))
        .await
        .expect("the call is refused, not parked on a staged `_hello`")
        .unwrap_err();
    assert!(
        matches!(
            error.downcast_ref::<TransportChoiceError>(),
            Some(TransportChoiceError::WouldStage { transport, len, threshold })
                if transport.as_str() == "narrow" && *len > THRESHOLD && *threshold == THRESHOLD
        ),
        "{error:#}"
    );
    assert_eq!(
        node.messenger
            .client()
            .response_manager
            .pending_outcome_count(),
        0
    );
    assert_eq!(node.wide.sent_count() + node.narrow.sent_count(), 0);

    // Nobody answers the `_hello`; the unnamed call stays parked on it.
    let pending = tokio::spawn(call(false));
    reaches(|| node.wide.sent_count(), 1).await;
    pending.abort();
    let (_, header, payload) = node.wide.sent.lock().unwrap()[0].clone();
    assert_eq!(
        crate::messenger::common::messages::handler_name_from_request_header(&header),
        Some(&b"_hello"[..])
    );
    assert!(payload.is_empty(), "staging leaves no payload to carry");
}

/// The same refusal with every response slot taken. The call reserves its
/// own slot before the handshake runs, so a check made only in the handshake
/// comes too late: the caller sees a full arena, or waits for a slot, and
/// does not learn that the call can never be sent. So the builder makes the
/// check before the slot, as it does for every other refused route.
#[tokio::test]
async fn a_named_handshake_that_would_stage_is_refused_with_a_full_arena() {
    const THRESHOLD: usize = 16;
    let node = node(&["wide", "narrow"]).await;
    let manager = Arc::new(crate::rendezvous::RendezvousManager::new(
        velo_ext::WorkerId::from_u64(7),
    ));
    node.messenger.set_large_payload_support(
        Arc::new(
            crate::rendezvous::transparent::RendezvousStager::new(manager.clone())
                .with_threshold(THRESHOLD),
        ),
        Arc::new(crate::rendezvous::transparent::RendezvousResolver::new(
            manager,
        )),
    );
    let client = node.messenger.client();
    let mut held = Vec::new();
    while let Ok(awaiter) = client.register_outcome() {
        held.push(awaiter);
    }
    assert!(!held.is_empty());

    let error = node
        .messenger
        .unary("sized")
        .unwrap()
        .transport("narrow")
        .raw_payload(Bytes::new())
        .instance(node.peer)
        .send()
        .await
        .unwrap_err();

    assert!(
        matches!(
            error.downcast_ref::<TransportChoiceError>(),
            Some(TransportChoiceError::WouldStage { transport, len, threshold })
                if transport.as_str() == "narrow" && *len > THRESHOLD && *threshold == THRESHOLD
        ),
        "expected WouldStage for the handshake, got {error:#}"
    );
}

#[tokio::test]
async fn a_peer_removed_between_route_and_readiness_checks_keeps_its_error_type() {
    let node = node(&["wide", "narrow"]).await;
    let peer = node.peer;
    let backend = node.messenger.backend();
    let narrow = backend.transport_route(&"narrow".into()).unwrap();
    let route = Route::Named(narrow.origin);
    backend.check_served(peer, route).unwrap();
    node.messenger.unregister_peer(peer);

    let client = node.messenger.client();
    let named = client
        .ensure_peer_ready(peer, "sized", route)
        .await
        .unwrap_err();
    assert_eq!(
        named.downcast_ref::<TransportChoiceError>(),
        Some(&TransportChoiceError::PeerNotRegistered { peer }),
        "{named:#}"
    );
    let unnamed = client
        .ensure_peer_ready(peer, "sized", Route::Primary)
        .await
        .unwrap_err();
    assert_eq!(
        unnamed.to_string(),
        format!("Peer {peer} not registered. Call Velo::register_peer first.")
    );
}

/// A named call whose peer is forgotten while its `_hello` is in flight
/// still fails with the typed error. `check_served` passed before the
/// handshake, so the send after it is the first to see that the peer is
/// gone. Route selection reports that as `PeerNotRegistered` for a named
/// send, not as the backend's untyped error.
#[tokio::test]
async fn a_named_call_reports_an_unregistered_peer_after_its_handshake() {
    use crate::messenger::common::events::Outcome;
    use crate::messenger::common::messages::decode_response_id_from_request_header;
    use crate::messenger::common::responses::encode_response_header;

    let node = node(&["wide", "narrow"]).await;
    let call = node
        .messenger
        .unary("sized")
        .unwrap()
        .transport("narrow")
        .raw_payload(Bytes::new())
        .instance(node.peer)
        .send();
    reaches(|| node.narrow.sent_count(), 1).await;
    let (_, hello, _) = node.narrow.sent.lock().unwrap()[0].clone();
    assert_eq!(
        crate::messenger::common::messages::handler_name_from_request_header(&hello),
        Some(&b"_hello"[..])
    );
    let hello_id = decode_response_id_from_request_header(&hello).unwrap();

    // A handshake reply is already on its way when the application forgets the peer.
    node.messenger.unregister_peer(node.peer);
    node.narrow
        .adapter
        .get()
        .unwrap()
        .response_stream
        .send((
            encode_response_header(hello_id, Outcome::Ok, None).unwrap(),
            Bytes::from_static(br#"{"handlers":["sized"]}"#),
        ))
        .unwrap();

    let error = tokio::time::timeout(Duration::from_secs(5), call)
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(node.narrow.sent_count(), 1);
    assert_eq!(node.wide.sent_count(), 0);
    assert_eq!(
        error.downcast_ref::<TransportChoiceError>(),
        Some(&TransportChoiceError::PeerNotRegistered { peer: node.peer }),
        "{error:#}"
    );
}

/// A peer forgotten between the route check and the `_hello` check is
/// reported as not registered. The `_hello` check asks whether the peer's
/// handler is known, and a peer that is gone has no known handler: read
/// alone, that says the call needs a `_hello`, and the caller would get
/// `WouldStage` for a peer that no longer exists. The stager here forgets
/// the peer inside its `threshold` call, which runs between the two checks.
#[tokio::test]
async fn a_peer_forgotten_during_the_hello_preflight_is_reported_as_unregistered() {
    use crate::messenger::common::events::Outcome;
    use crate::messenger::common::messages::decode_response_id_from_request_header;
    use crate::messenger::common::responses::encode_response_header;
    use crate::messenger::large_payload::LargePayloadStager;

    struct ForgetPeer {
        messenger: std::sync::Weak<Messenger>,
        peer: InstanceId,
        checks: AtomicUsize,
    }

    impl LargePayloadStager for ForgetPeer {
        fn threshold(&self) -> usize {
            // The payload check comes first. Forget the peer after the route check.
            if self.checks.fetch_add(1, Ordering::Relaxed) == 1 {
                self.messenger.upgrade().unwrap().unregister_peer(self.peer);
            }
            16
        }

        fn stage(&self, _: Bytes, _: Option<Duration>) -> String {
            panic!("the request payload is empty");
        }
    }

    let node = node(&["wide", "narrow"]).await;
    let refresh = tokio::spawn({
        let messenger = node.messenger.clone();
        let peer = node.peer;
        async move { messenger.refresh_handlers(peer).await }
    });
    reaches(|| node.wide.sent_count(), 1).await;
    let (_, hello, _) = node.wide.sent.lock().unwrap()[0].clone();
    let hello_id = decode_response_id_from_request_header(&hello).unwrap();
    node.wide
        .adapter
        .get()
        .unwrap()
        .response_stream
        .send((
            encode_response_header(hello_id, Outcome::Ok, None).unwrap(),
            Bytes::from_static(br#"{"handlers":["sized"]}"#),
        ))
        .unwrap();
    refresh.await.unwrap().unwrap();
    assert!(
        node.messenger
            .client()
            .can_send_directly(node.peer, "sized")
    );

    let manager = Arc::new(crate::rendezvous::RendezvousManager::new(
        velo_ext::WorkerId::from_u64(7),
    ));
    node.messenger.set_large_payload_support(
        Arc::new(ForgetPeer {
            messenger: Arc::downgrade(&node.messenger),
            peer: node.peer,
            checks: AtomicUsize::new(0),
        }),
        Arc::new(crate::rendezvous::RendezvousResolver::new(manager)),
    );
    let error = node
        .messenger
        .unary("sized")
        .unwrap()
        .transport("narrow")
        .raw_payload(Bytes::new())
        .instance(node.peer)
        .send()
        .await
        .unwrap_err();

    assert!(!node.messenger.backend().is_registered(node.peer));
    assert_eq!(node.narrow.sent_count(), 0);
    assert_eq!(
        error.downcast_ref::<TransportChoiceError>(),
        Some(&TransportChoiceError::PeerNotRegistered { peer: node.peer }),
        "{error:#}"
    );
}

/// An acquire is refused when the transport its answer takes leaves less
/// room than the chunked answer can need. The refusal comes before the read
/// lock. Without it, an answer over the limit is replaced by an error reply
/// that names no lease: the consumer cannot end a lease it never saw, and a
/// chunked lease has no deadline unless the consumer asked for one, so the
/// lease holds the payload until the owner tears down. At exactly that room,
/// the acquire goes on, with chunks of that size.
#[tokio::test]
async fn an_acquire_needs_room_for_its_answer_on_the_reply_transport() {
    use crate::rendezvous::handlers::{ACQUIRE_ANSWER_ROOM, pull_chunk_limit};

    let header = crate::messenger::common::responses::encode_response_header(
        ResponseId::from_u128(0),
        crate::messenger::common::events::Outcome::Ok,
        None,
    )
    .unwrap()
    .len();
    let narrow = Some(velo_ext::TransportOrigin::new(1));

    let short = node_with_narrow(&["wide", "narrow"], header + ACQUIRE_ANSWER_ROOM - 1).await;
    let error = pull_chunk_limit(
        short.messenger.backend(),
        short.peer.worker_id(),
        narrow,
        None,
    )
    .unwrap_err();
    assert!(error.to_string().contains("narrow"), "{error}");

    let enough = node_with_narrow(&["wide", "narrow"], header + ACQUIRE_ANSWER_ROOM).await;
    assert_eq!(
        pull_chunk_limit(
            enough.messenger.backend(),
            enough.peer.worker_id(),
            narrow,
            None,
        )
        .unwrap(),
        ACQUIRE_ANSWER_ROOM as u32
    );
}
