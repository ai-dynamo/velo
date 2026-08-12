// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the client's eager-payload budget.

use super::*;

use crate::messenger::Messenger;
use crate::rendezvous::transparent::{DEFAULT_THRESHOLD, RendezvousStager};
use crate::transports::Transport;
use crate::transports::tcp::{TcpTransport, TcpTransportBuilder};

/// The `_stream_batch` envelope, spelled out here so a change to
/// [`envelope_overhead`] that these tests depend on shows up as a diff rather
/// than silently rebasing every expected number.
const STREAM_BATCH_ENVELOPE: usize = 22 + "_stream_batch".len();

/// The TCP codec's frame ceiling — what `TcpTransport::max_message_size`
/// reports. Written out rather than imported so the two are pinned against
/// each other instead of being the same expression twice.
const TCP_FRAME_CEILING: usize = 16 * 1024 * 1024;

/// What [`finalize_outbound_headers`] costs a send that passed no headers at
/// all.
///
/// Under `distributed-tracing` the injector materialises a header map whether
/// or not there is a context to put in it, and an empty MessagePack `FixMap`
/// is one byte the encoder would not otherwise have written. With the feature
/// off nothing is added. Applies only to the `None` cases below: a caller that
/// already passed a map pays for the map either way.
#[cfg(feature = "distributed-tracing")]
const INJECTED_HEADER_MAP: usize = 1;
#[cfg(not(feature = "distributed-tracing"))]
const INJECTED_HEADER_MAP: usize = 0;

#[test]
fn budget_falls_back_to_the_staging_threshold_when_capacity_is_unknown() {
    // A transport that reports `None` — gRPC and ZMQ today — must not be read
    // as unlimited. The stager's threshold is the only ceiling left.
    assert_eq!(
        eager_payload_budget(None, Some(64 * 1024), 100),
        64 * 1024 - 100
    );
}

#[test]
fn budget_takes_the_lower_of_the_two_ceilings() {
    // Transport binds below the threshold.
    assert_eq!(eager_payload_budget(Some(4096), Some(64 * 1024), 30), 4066);
    // Threshold binds below the transport.
    assert_eq!(
        eager_payload_budget(Some(TCP_FRAME_CEILING), Some(64 * 1024), 30),
        64 * 1024 - 30
    );
}

#[test]
fn budget_uses_the_transport_alone_when_no_stager_is_installed() {
    // No stager means no cheaper path to fall back to, so clamping to the
    // default threshold would forbid sends that would have worked.
    assert_eq!(
        eager_payload_budget(Some(TCP_FRAME_CEILING), None, STREAM_BATCH_ENVELOPE),
        TCP_FRAME_CEILING - STREAM_BATCH_ENVELOPE
    );
}

#[test]
fn budget_defaults_to_the_transparent_staging_threshold_when_nothing_is_known() {
    assert_eq!(
        eager_payload_budget(None, None, STREAM_BATCH_ENVELOPE),
        DEFAULT_THRESHOLD - STREAM_BATCH_ENVELOPE
    );
}

#[test]
fn budget_saturates_rather_than_wrapping_when_the_envelope_exceeds_the_ceiling() {
    // Reachable for real: headers may carry up to 16 KiB, so a deployment that
    // lowers `with_threshold` far enough can put the envelope above the
    // ceiling. Wrapping here would produce a near-`usize::MAX` budget — the
    // exact oversized send this whole boundary exists to stop.
    assert_eq!(eager_payload_budget(None, Some(1024), 4096), 0);
    assert_eq!(eager_payload_budget(Some(64), None, 4096), 0);
}

/// Install transparent large-payload support with the given threshold.
fn install_stager(messenger: &Messenger, threshold: usize) {
    let manager = std::sync::Arc::new(crate::RendezvousManager::new(velo_ext::WorkerId::from_u64(
        7,
    )));
    messenger.set_large_payload_support(
        std::sync::Arc::new(RendezvousStager::new(manager.clone()).with_threshold(threshold)),
        std::sync::Arc::new(crate::rendezvous::transparent::RendezvousResolver::new(
            manager,
        )),
    );
}

/// Build a messenger over a real TCP transport and register a peer on it, so
/// the budget is read through the full backend → transport path.
async fn tcp_messenger() -> (std::sync::Arc<Messenger>, std::sync::Arc<Messenger>) {
    async fn build() -> std::sync::Arc<Messenger> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let transport: std::sync::Arc<TcpTransport> = std::sync::Arc::new(
            TcpTransportBuilder::new()
                .from_listener(listener)
                .expect("from_listener")
                .build()
                .expect("build"),
        );
        Messenger::builder()
            .add_transport(transport)
            .build()
            .await
            .expect("messenger")
    }
    let (local, remote) = (build().await, build().await);
    local.register_peer(remote.peer_info()).expect("register");
    (local, remote)
}

#[tokio::test]
async fn tcp_budget_is_the_frame_ceiling_less_the_envelope() {
    let (local, remote) = tcp_messenger().await;

    // No stager installed: TCP's framed ceiling is the only bound.
    assert_eq!(
        local.effective_eager_payload(remote.instance_id(), "_stream_batch", None),
        TCP_FRAME_CEILING - (STREAM_BATCH_ENVELOPE + INJECTED_HEADER_MAP),
    );

    // An unregistered peer has no transport to ask, which is the same "cannot
    // say" as a transport that does not know — so the conservative default.
    assert_eq!(
        local.effective_eager_payload(crate::InstanceId::new_v4(), "_stream_batch", None),
        DEFAULT_THRESHOLD - (STREAM_BATCH_ENVELOPE + INJECTED_HEADER_MAP),
    );
}

#[tokio::test]
async fn lowering_the_rendezvous_threshold_lowers_the_budget() {
    let (local, remote) = tcp_messenger().await;
    let target = remote.instance_id();
    let before = local.effective_eager_payload(target, "_stream_batch", None);

    // 64 KiB is far below TCP's 16 MiB ceiling, so the threshold must bind.
    const LOWERED_THRESHOLD: usize = 64 * 1024;
    install_stager(&local, LOWERED_THRESHOLD);

    let after = local.effective_eager_payload(target, "_stream_batch", None);
    assert_eq!(
        before,
        TCP_FRAME_CEILING - (STREAM_BATCH_ENVELOPE + INJECTED_HEADER_MAP)
    );
    assert_eq!(
        after,
        LOWERED_THRESHOLD - (STREAM_BATCH_ENVELOPE + INJECTED_HEADER_MAP)
    );

    // A header set costs exactly what the encoder would spend on it: the `_rv`
    // key and a 39-digit handle push the envelope out by the MessagePack map.
    let mut headers = HashMap::new();
    headers.insert(
        crate::messenger::large_payload::RV_HEADER_KEY.to_string(),
        "9".repeat(39),
    );
    assert_eq!(
        local.effective_eager_payload(target, "_stream_batch", Some(&headers)),
        LOWERED_THRESHOLD - (STREAM_BATCH_ENVELOPE + 1 + 4 + 41),
    );
}

// ---------------------------------------------------------------------------
// A transport small enough to overrun
// ---------------------------------------------------------------------------

/// A transport that carries nothing and remembers everything.
///
/// It exists so a payload sized to the reported budget can actually be built:
/// every real transport that reports a capacity reports megabytes of it. The
/// size check mirrors the NATS transport, including the part that matters most
/// here — an over-capacity frame is reported through `on_error` and the send is
/// still [`SendOutcome::Admitted`], because nothing was queued behind it. So a
/// fire-and-forget caller sees success either way and only the recorded frames
/// say what reached the wire.
struct CappedTransport {
    key: velo_ext::TransportKey,
    address: velo_ext::WorkerAddress,
    capacity: usize,
    accepted: std::sync::Mutex<Vec<(bytes::Bytes, bytes::Bytes)>>,
    rejected: std::sync::Mutex<Vec<String>>,
}

impl CappedTransport {
    fn new(capacity: usize) -> std::sync::Arc<Self> {
        let key = velo_ext::TransportKey::from("capped");
        let mut address = crate::transports::address::WorkerAddressBuilder::new();
        address
            .add_entry(key.as_str(), bytes::Bytes::from_static(b"capped://local"))
            .expect("address entry");
        std::sync::Arc::new(Self {
            key,
            address: address.build().expect("address"),
            capacity,
            accepted: std::sync::Mutex::new(Vec::new()),
            rejected: std::sync::Mutex::new(Vec::new()),
        })
    }

    /// The single frame this transport took, panicking unless there is exactly
    /// one — a second frame would mean the test measured the wrong send.
    fn only_accepted_frame(&self) -> (bytes::Bytes, bytes::Bytes) {
        let accepted = self.accepted.lock().expect("accepted frames poisoned");
        assert_eq!(accepted.len(), 1, "expected exactly one accepted frame");
        accepted[0].clone()
    }

    fn rejections(&self) -> Vec<String> {
        self.rejected
            .lock()
            .expect("rejected frames poisoned")
            .clone()
    }
}

impl crate::transports::Transport for CappedTransport {
    fn key(&self) -> velo_ext::TransportKey {
        self.key.clone()
    }

    fn address(&self) -> velo_ext::WorkerAddress {
        self.address.clone()
    }

    fn max_message_size(&self, _target: InstanceId) -> Option<usize> {
        Some(self.capacity)
    }

    fn register(
        &self,
        _peer_info: velo_ext::PeerInfo,
    ) -> Result<(), crate::transports::TransportError> {
        Ok(())
    }

    fn send_message(
        &self,
        _instance_id: InstanceId,
        header: bytes::Bytes,
        payload: bytes::Bytes,
        _message_type: crate::transports::MessageType,
        on_error: Arc<dyn TransportErrorHandler>,
    ) -> SendOutcome {
        let frame = header.len() + payload.len();
        if frame > self.capacity {
            let reason = format!(
                "Frame size {frame} exceeds capped transport capacity {}",
                self.capacity
            );
            self.rejected
                .lock()
                .expect("rejected frames poisoned")
                .push(reason.clone());
            on_error.on_error(header, payload, reason);
            return SendOutcome::Admitted;
        }
        self.accepted
            .lock()
            .expect("accepted frames poisoned")
            .push((header, payload));
        SendOutcome::Admitted
    }

    fn start(
        &self,
        _instance_id: InstanceId,
        _channels: crate::transports::TransportAdapter,
        _rt: tokio::runtime::Handle,
    ) -> futures::future::BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn shutdown(&self) {}

    fn check_health(
        &self,
        _instance_id: InstanceId,
        _timeout: Duration,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<(), crate::transports::HealthCheckError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async { Ok(()) })
    }
}

/// A messenger whose only transport reports `capacity`, plus a peer registered
/// on it and the transport itself to read frames back from.
async fn capped_messenger(
    capacity: usize,
) -> (
    std::sync::Arc<Messenger>,
    InstanceId,
    std::sync::Arc<CappedTransport>,
) {
    let transport = CappedTransport::new(capacity);
    let messenger = Messenger::builder()
        .add_transport(transport.clone())
        .build()
        .await
        .expect("messenger");
    let target = InstanceId::new_v4();
    messenger
        .register_peer(velo_ext::PeerInfo::new(target, transport.address()))
        .expect("register peer");
    (messenger, target, transport)
}

/// The budget is a promise about the wire, so it has to be tested against the
/// wire: a payload sized to the number the client reported must fill the
/// transport's frame and not exceed it, whatever the send path did to the
/// headers on the way. This is the promise with no trace context in the
/// picture; the `distributed-tracing` test below is the same assertion with
/// one.
#[tokio::test]
async fn a_budget_sized_payload_fills_the_transport_frame_exactly() {
    const CAPACITY: usize = 8 * 1024;
    let (local, target, transport) = capped_messenger(CAPACITY).await;

    let budget = local.effective_eager_payload(target, "_stream_batch", None);
    tokio::time::timeout(
        Duration::from_secs(2),
        local
            .am_send_streaming("_stream_batch")
            .expect("streaming builder")
            .raw_payload(bytes::Bytes::from(vec![0u8; budget]))
            .instance(target)
            .send(),
    )
    .await
    .expect("a registered peer and an underscore handler take the fast path")
    .expect("a budget-sized send is admitted");

    assert_eq!(
        transport.rejections(),
        Vec::<String>::new(),
        "a payload sized to the reported budget must fit the transport it was sized against"
    );
    let (header, payload) = transport.only_accepted_frame();
    assert_eq!(header.len() + payload.len(), CAPACITY);
}

/// What happens to an over-budget send depends on *which* ceiling produced the
/// budget, and with a transport whose capacity sits below the stager's
/// threshold there is a band between them where neither path carries the
/// payload: the stager compares the raw payload against its own threshold and
/// declines to stage, and the frame is then too large for the transport. The
/// send fails — the frame never reaches the wire and the error surfaces on the
/// awaiter.
///
/// The second half is the contrast that makes the first mean something: one
/// byte past the threshold the same send succeeds, because staging replaces the
/// payload with a handle and what goes out is small.
#[tokio::test]
async fn over_budget_sends_fail_below_the_threshold_and_stage_above_it() {
    const CAPACITY: usize = 8 * 1024;
    const THRESHOLD: usize = 64 * 1024;
    let (local, target, transport) = capped_messenger(CAPACITY).await;
    install_stager(&local, THRESHOLD);

    let budget = local.effective_eager_payload(target, "_stream_batch", None);
    assert!(
        budget < CAPACITY && CAPACITY < THRESHOLD,
        "the transport has to be the binding ceiling for this test to mean anything"
    );

    // Between the two ceilings: too large for the transport, too small to stage.
    let error = tokio::time::timeout(
        Duration::from_secs(2),
        local
            .unary_streaming("_stream_batch")
            .raw_payload(bytes::Bytes::from(vec![0u8; (CAPACITY + THRESHOLD) / 2]))
            .instance(target)
            .send(),
    )
    .await
    .expect("the rejection has to surface on the awaiter, not time out there")
    .expect_err("a frame over the transport's capacity cannot be sent");
    assert!(
        error
            .to_string()
            .contains("exceeds capped transport capacity"),
        "the failure must be the capacity rejection, got: {error}"
    );
    assert_eq!(transport.rejections().len(), 1);

    // One byte past the threshold the stager takes over: the payload becomes a
    // handle in the headers and the frame that goes out is a fraction of the
    // capacity. The boundary is exact — the stager triggers on `>`, so this is
    // the smallest payload that stages.
    tokio::time::timeout(
        Duration::from_secs(2),
        local
            .am_send_streaming("_stream_batch")
            .expect("streaming builder")
            .raw_payload(bytes::Bytes::from(vec![0u8; THRESHOLD + 1]))
            .instance(target)
            .send(),
    )
    .await
    .expect("a staged send is not waiting on anything remote")
    .expect("a staged send is admitted");

    let (header, payload) = transport.only_accepted_frame();
    assert!(payload.is_empty(), "staging leaves no payload to carry");
    let decoded = crate::messenger::common::messages::decode_active_message(header, payload)
        .expect("the frame decodes");
    assert!(
        decoded.metadata.headers.is_some_and(
            |headers| headers.contains_key(crate::messenger::large_payload::RV_HEADER_KEY)
        ),
        "a staged send carries its rendezvous handle in the headers"
    );
}

/// Sizing a send reads the caller's headers; it never copies them, and it never
/// runs them past the encoder. So a header set the encoder would refuse — the
/// per-value and total limits are checked at encode, not here — still gets a
/// budget, and that budget accounts for every byte of it.
///
/// The set below is 32 KiB, twice the encoder's ceiling. What is asserted is
/// the difference an empty caller map and this one make to the budget: 32
/// entries of `msgpack_str_len(8) + msgpack_str_len(1024)`, plus the two bytes
/// the map header grows by once the union passes 15 entries. Feature-
/// independent, because whatever the send path injects is in both numbers.
#[tokio::test]
async fn budget_sizes_a_header_set_the_encoder_would_reject() {
    const ENTRIES: usize = 32;
    const KEY_LEN: usize = 8;
    const VALUE_LEN: usize = 1024;
    // Roomy enough that the envelope does not eat the whole budget: what is
    // asserted is a difference, and a saturated budget has no difference left.
    let (local, target, _transport) = capped_messenger(256 * 1024).await;

    let empty = HashMap::new();
    let oversized: HashMap<String, String> = (0..ENTRIES)
        .map(|i| (format!("{i:0KEY_LEN$}"), "v".repeat(VALUE_LEN)))
        .collect();

    let base = local.effective_eager_payload(target, "_stream_batch", Some(&empty));
    let charged = local.effective_eager_payload(target, "_stream_batch", Some(&oversized));

    // FixStr key (1 + 8) + Str16 value (3 + 1024) per entry, and the map header
    // going from a FixMap marker (1) to Map16 (3) as the union passes 15.
    const PER_ENTRY: usize = (1 + KEY_LEN) + (3 + VALUE_LEN);
    assert_eq!(base - charged, ENTRIES * PER_ENTRY + 2);
}

// ---------------------------------------------------------------------------
// The budget and the encoder must agree about the headers
// ---------------------------------------------------------------------------

/// The `traceparent` a W3C propagator writes: a fixed 55 bytes.
#[cfg(feature = "distributed-tracing")]
const TRACEPARENT_KEY: &str = "traceparent";
#[cfg(feature = "distributed-tracing")]
const TRACEPARENT_VALUE: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

/// Marker the propagator below looks for before injecting anything.
///
/// `set_text_map_propagator` is process-global and this test shares a binary
/// with every other budget test, so the injection has to be scoped to the
/// context this test attaches rather than to whatever runs alongside it.
#[cfg(feature = "distributed-tracing")]
#[derive(Debug)]
struct TraceThisSend;

/// Stands in for whichever propagator a deployment installed. A real
/// `TraceContextPropagator` lives in `opentelemetry-sdk`, which this crate does
/// not depend on; what matters to the budget is only that injection puts bytes
/// in the header map between the budget being reported and the frame being
/// encoded.
#[cfg(feature = "distributed-tracing")]
#[derive(Debug)]
struct FixedTraceparentPropagator;

#[cfg(feature = "distributed-tracing")]
impl opentelemetry::propagation::TextMapPropagator for FixedTraceparentPropagator {
    fn inject_context(
        &self,
        cx: &opentelemetry::Context,
        injector: &mut dyn opentelemetry::propagation::Injector,
    ) {
        if cx.get::<TraceThisSend>().is_some() {
            injector.set(TRACEPARENT_KEY, TRACEPARENT_VALUE.to_string());
        }
    }

    fn extract_with_context(
        &self,
        cx: &opentelemetry::Context,
        _extractor: &dyn opentelemetry::propagation::Extractor,
    ) -> opentelemetry::Context {
        cx.clone()
    }

    fn fields(&self) -> opentelemetry::propagation::text_map_propagator::FieldIter<'_> {
        static FIELDS: std::sync::OnceLock<[String; 1]> = std::sync::OnceLock::new();
        opentelemetry::propagation::text_map_propagator::FieldIter::new(
            FIELDS.get_or_init(|| [TRACEPARENT_KEY.to_string()]),
        )
    }
}

/// The budget has to be sized against the headers the *send path* will encode,
/// not the ones the caller handed in. Under `distributed-tracing` the client
/// injects the current trace context immediately before encoding, so a budget
/// that counted only the caller's headers is short by the whole context — and a
/// payload sized to it overruns the transport it was sized against.
///
/// Filling the budget exactly and watching the transport take the frame is the
/// assertion that discriminates: before the fix the same send is rejected for
/// being over capacity.
#[cfg(feature = "distributed-tracing")]
#[tokio::test]
async fn budget_covers_the_trace_context_the_send_path_injects() {
    const CAPACITY: usize = 8 * 1024;
    let (local, target, transport) = capped_messenger(CAPACITY).await;

    opentelemetry::global::set_text_map_propagator(FixedTraceparentPropagator);
    let untraced = local.effective_eager_payload(target, "_stream_batch", None);

    let _attached = opentelemetry::Context::current_with_value(TraceThisSend).attach();
    let budget = local.effective_eager_payload(target, "_stream_batch", None);
    assert!(
        budget < untraced,
        "an injected trace context has to cost envelope: traced {budget}, untraced {untraced}"
    );

    tokio::time::timeout(
        Duration::from_secs(2),
        local
            .am_send_streaming("_stream_batch")
            .expect("streaming builder")
            .raw_payload(bytes::Bytes::from(vec![0u8; budget]))
            .instance(target)
            .send(),
    )
    .await
    .expect("a registered peer and an underscore handler take the fast path")
    .expect("a budget-sized send is admitted");

    assert_eq!(
        transport.rejections(),
        Vec::<String>::new(),
        "a payload sized to the reported budget must fit the transport it was sized against"
    );
    let (header, payload) = transport.only_accepted_frame();
    assert_eq!(
        header.len() + payload.len(),
        CAPACITY,
        "the budget is exactly what the capacity leaves once the real envelope is paid for"
    );

    let decoded = crate::messenger::common::messages::decode_active_message(header, payload)
        .expect("the frame decodes");
    assert!(
        decoded
            .metadata
            .headers
            .is_some_and(|headers| headers.contains_key(TRACEPARENT_KEY)),
        "nothing was injected, so this test would have passed for the wrong reason"
    );
}

/// A caller who already carries the key the injector is about to write has that
/// value overwritten, not duplicated — the merge is a `HashMap::insert` and the
/// injector runs last. The union is sized rather than built, so the key has to
/// be counted once, at the injected value's length and not the caller's, or the
/// budget drifts from the frame in whichever direction the two lengths differ.
///
/// Here the caller's value is deliberately much shorter than the injected one,
/// so counting the wrong one shows up as a frame that overruns the capacity.
#[cfg(feature = "distributed-tracing")]
#[tokio::test]
async fn budget_counts_a_collided_header_once_at_its_injected_size() {
    const CAPACITY: usize = 8 * 1024;
    let (local, target, transport) = capped_messenger(CAPACITY).await;

    opentelemetry::global::set_text_map_propagator(FixedTraceparentPropagator);
    let mut headers = HashMap::new();
    headers.insert(TRACEPARENT_KEY.to_string(), "stale".to_string());

    let _attached = opentelemetry::Context::current_with_value(TraceThisSend).attach();
    let budget = local.effective_eager_payload(target, "_stream_batch", Some(&headers));

    tokio::time::timeout(
        Duration::from_secs(2),
        local
            .am_send_streaming("_stream_batch")
            .expect("streaming builder")
            .headers(headers)
            .raw_payload(bytes::Bytes::from(vec![0u8; budget]))
            .instance(target)
            .send(),
    )
    .await
    .expect("a registered peer and an underscore handler take the fast path")
    .expect("a budget-sized send is admitted");

    assert_eq!(transport.rejections(), Vec::<String>::new());
    let (header, payload) = transport.only_accepted_frame();
    assert_eq!(
        header.len() + payload.len(),
        CAPACITY,
        "the collided key must be counted once, at the length that reaches the wire"
    );

    let decoded = crate::messenger::common::messages::decode_active_message(header, payload)
        .expect("the frame decodes");
    assert_eq!(
        decoded
            .metadata
            .headers
            .expect("the merged headers reach the wire")
            .get(TRACEPARENT_KEY)
            .map(String::as_str),
        Some(TRACEPARENT_VALUE),
        "the injected context wins the collision"
    );
}
