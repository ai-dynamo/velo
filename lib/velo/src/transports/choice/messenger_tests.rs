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
/// transport's capacity bounds it: one frame under the narrow capacity is
/// sent on the narrow transport, one over is refused there and never sent on
/// the primary.
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
    reaches(|| node.narrow.refused.load(Ordering::SeqCst), 1).await;
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

/// The owner sizes the chunks of a staged fetch so that one `_rv_pull` reply
/// fits the transport the reply takes: that transport's capacity less the
/// reply header. When the fetcher is not registered on the named transport
/// the reply takes the primary, and the primary's capacity sizes them. A
/// transport that cannot carry one byte past the header is refused with the
/// typed error, with no fallback.
#[tokio::test]
async fn staged_chunks_are_sized_to_the_transport_the_pull_reply_takes() {
    use crate::rendezvous::handlers::pull_chunk_limit;
    use crate::rendezvous::store::DEFAULT_CHUNK_SIZE;
    // Response header with no headers: 16-byte id, outcome, 2-byte length.
    const HEADER: usize = 19;

    let node = node(&["wide", "narrow"]).await;
    let backend = node.messenger.backend();
    let worker = node.peer.worker_id();
    assert_eq!(
        pull_chunk_limit(backend, worker, Some("narrow"), None).unwrap() as usize,
        NARROW - HEADER
    );
    assert_eq!(
        pull_chunk_limit(backend, worker, Some("wide"), None).unwrap(),
        DEFAULT_CHUNK_SIZE,
        "a wide transport keeps the default chunk"
    );

    let only_wide = node_with_narrow(&["wide"], NARROW).await;
    assert_eq!(
        pull_chunk_limit(
            only_wide.messenger.backend(),
            only_wide.peer.worker_id(),
            Some("narrow"),
            None
        )
        .unwrap(),
        DEFAULT_CHUNK_SIZE,
        "the reply takes the primary, so the primary sizes the chunk"
    );

    let tiny = node_with_narrow(&["wide", "narrow"], HEADER).await;
    let error = pull_chunk_limit(
        tiny.messenger.backend(),
        tiny.peer.worker_id(),
        Some("narrow"),
        None,
    )
    .unwrap_err();
    assert_eq!(
        error.downcast_ref::<TransportChoiceError>(),
        Some(&TransportChoiceError::CapacityTooSmall {
            transport: "narrow".into(),
            capacity: HEADER,
            header: HEADER,
        })
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
