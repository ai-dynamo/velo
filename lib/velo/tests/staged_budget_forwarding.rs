// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The fetch budget on the wire, and the headers a handler can forward.
//!
//! A handler sees every header of its message, the staging headers included,
//! and may pass them on in a send of its own. A budget belongs to the send
//! that staged the payload. The receiver reads it as the caller's own deadline
//! and gives up the fetch at that deadline without a reply, so a budget that a
//! send did not set makes a caller with no timer wait for ever.
//!
//! An inherited budget reached the wire three ways: on a staged send without
//! a timeout, on an eager send, and through a tracing propagator that writes
//! headers after the send path had removed it. So these tests do not check
//! the call's behaviour, which depends on what the receiver does with other
//! headers. They decode the headers the encoder wrote, as they left the
//! sender.
//!
//! The nodes share an in-memory transport, so the tests need no ports.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::future::BoxFuture;
use velo::transports::{
    HealthCheckError, MessageType, SendOutcome, Transport, TransportAdapter, TransportError,
    TransportErrorHandler,
};
use velo::*;

type Bus = Arc<parking_lot::Mutex<HashMap<InstanceId, TransportAdapter>>>;

/// The handler name and decoded headers of each message a node sent.
type Sent = Arc<parking_lot::Mutex<Vec<(String, HashMap<String, String>)>>>;

/// An in-memory transport that delivers on a shared bus and records the
/// headers of every message it sends, as encoded.
#[derive(Default)]
struct Wire {
    bus: Bus,
    sent: Sent,
}

/// Decode the handler name and headers of an encoded active-message header:
/// schema (1), response type (1), response id (16), handler name length
/// (u16 LE), handler name, headers length (u16 LE), MessagePack headers.
fn decode_header(header: &[u8]) -> (String, HashMap<String, String>) {
    let name_len = u16::from_le_bytes([header[18], header[19]]) as usize;
    let name = String::from_utf8(header[20..20 + name_len].to_vec()).unwrap();
    let at = 20 + name_len;
    let headers_len = u16::from_le_bytes([header[at], header[at + 1]]) as usize;
    let headers = if headers_len == 0 {
        HashMap::new()
    } else {
        rmp_serde::from_slice(&header[at + 2..at + 2 + headers_len]).unwrap()
    };
    (name, headers)
}

impl Transport for Wire {
    fn key(&self) -> velo_ext::TransportKey {
        "review".into()
    }

    fn address(&self) -> velo_ext::WorkerAddress {
        let entries = HashMap::from([("review", b"local".to_vec())]);
        velo_ext::WorkerAddress::from_encoded(rmp_serde::to_vec(&entries).unwrap())
    }

    fn register(&self, _: PeerInfo) -> Result<(), TransportError> {
        Ok(())
    }

    fn send_message(
        &self,
        target: InstanceId,
        header: Bytes,
        payload: Bytes,
        kind: MessageType,
        _: Arc<dyn TransportErrorHandler>,
    ) -> SendOutcome {
        if kind == MessageType::Message {
            self.sent.lock().push(decode_header(&header));
        }
        let bus = self.bus.lock();
        let adapter = bus.get(&target).expect("registered target");
        match kind {
            MessageType::Message => {
                let _ = adapter.admit_message(header, payload);
            }
            MessageType::Response => {
                adapter.response_stream.send((header, payload)).unwrap();
            }
            MessageType::Ack | MessageType::Event => {
                adapter.event_stream.send((header, payload)).unwrap();
            }
            MessageType::ShuttingDown => {
                adapter.shutdown_stream.send((header, payload)).unwrap();
            }
        }
        SendOutcome::Admitted
    }

    fn start(
        &self,
        id: InstanceId,
        adapter: TransportAdapter,
        _: tokio::runtime::Handle,
    ) -> BoxFuture<'_, anyhow::Result<()>> {
        self.bus.lock().insert(id, adapter);
        Box::pin(async { Ok(()) })
    }

    fn shutdown(&self) {}

    fn check_health(
        &self,
        _: InstanceId,
        _: Duration,
    ) -> BoxFuture<'_, Result<(), HealthCheckError>> {
        Box::pin(async { Ok(()) })
    }
}

/// Two nodes on one in-memory bus. The server has `capture` (unary) and
/// `capture_am` (sync and fire-and-forget), which report the headers they
/// see, and `echo`. Also returns what the client sent.
async fn capture_pair() -> (
    Velo,
    Velo,
    tokio::sync::mpsc::UnboundedReceiver<HashMap<String, String>>,
    Sent,
) {
    let bus = Bus::default();
    let sent = Sent::default();
    let client = Velo::builder()
        .mux_only()
        .add_transport(Arc::new(Wire {
            bus: bus.clone(),
            sent: sent.clone(),
        }))
        .build()
        .await
        .unwrap();
    let server = Velo::builder()
        .mux_only()
        .add_transport(Arc::new(Wire {
            bus,
            sent: Sent::default(),
        }))
        .build()
        .await
        .unwrap();
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let seen = tx.clone();
    server
        .register_handler(
            Handler::unary_handler("capture", move |ctx| {
                seen.send(ctx.headers.unwrap_or_default()).unwrap();
                Ok(Some(Bytes::new()))
            })
            .build(),
        )
        .unwrap();
    server
        .register_handler(
            Handler::am_handler("capture_am", move |ctx| {
                tx.send(ctx.headers.unwrap_or_default()).unwrap();
                Ok(())
            })
            .build(),
        )
        .unwrap();
    server
        .register_handler(Handler::unary_handler("echo", |ctx| Ok(Some(ctx.payload))).build())
        .unwrap();
    client.register_peer(server.peer_info()).unwrap();
    server.register_peer(client.peer_info()).unwrap();
    client
        .messenger()
        .available_handlers(server.instance_id())
        .await
        .unwrap();
    (client, server, rx, sent)
}

/// The headers of the last message `sent` to `handler`.
fn last_sent(sent: &Sent, handler: &str) -> HashMap<String, String> {
    sent.lock()
        .iter()
        .rev()
        .find(|(name, _)| name == handler)
        .unwrap_or_else(|| panic!("nothing was sent to {handler}"))
        .1
        .clone()
}

/// Check the budget on the wire: none when the send did not stage a payload
/// under a timeout, and otherwise what is left of `timeout`, not the
/// inherited one.
fn assert_own_budget(
    wire: &HashMap<String, String>,
    staged: bool,
    timeout: Option<Duration>,
    row: &str,
) {
    let budget = wire.get("_rv_budget_ms");
    match timeout {
        Some(timeout) if staged => {
            let ms: u64 = budget
                .unwrap_or_else(|| panic!("{row}: no budget of its own on the wire"))
                .parse()
                .unwrap();
            let full = timeout.as_millis() as u64;
            assert!(
                (full - 5_000..=full).contains(&ms),
                "{row}: sent {ms} ms, not what is left of its own timeout"
            );
        }
        _ => assert_eq!(budget, None, "{row}: sent a budget it did not set"),
    }
}

#[derive(Clone, Copy, Debug)]
enum Path {
    Unary,
    Sync,
    Fire,
}

/// On every send path, an inherited budget never reaches the wire. A send
/// writes a budget of its own only when it stages its payload and has a
/// `.timeout`; fire-and-forget sends have no timeout, so they never write one.
/// Other forwarded headers pass through.
#[tokio::test]
async fn a_send_carries_only_a_budget_of_its_own() {
    const TIMEOUT: Duration = Duration::from_secs(60);
    let (client, server, _seen, sent) = capture_pair().await;
    let target = server.instance_id();
    let inherited = HashMap::from([
        ("_rv_budget_ms".to_string(), "7".to_string()),
        ("trace".to_string(), "kept".to_string()),
    ]);
    let eager = Bytes::from_static(b"eager");
    let staged = Bytes::from(vec![0x5a; 512 * 1024]);
    let rows = [
        (Path::Unary, false, None),
        (Path::Unary, false, Some(TIMEOUT)),
        (Path::Unary, true, None),
        (Path::Unary, true, Some(TIMEOUT)),
        (Path::Sync, false, None),
        (Path::Sync, false, Some(TIMEOUT)),
        (Path::Sync, true, None),
        (Path::Sync, true, Some(TIMEOUT)),
        (Path::Fire, false, None),
        (Path::Fire, true, None),
    ];
    for (path, is_staged, timeout) in rows {
        let payload = if is_staged { &staged } else { &eager }.clone();
        let headers = inherited.clone();
        let handler = match path {
            Path::Unary => {
                let call = client.unary("capture").unwrap();
                let call = call.instance(target).headers(headers).raw_payload(payload);
                let call = match timeout {
                    Some(t) => call.timeout(t),
                    None => call,
                };
                call.send().await.unwrap();
                "capture"
            }
            Path::Sync => {
                let call = client.am_sync("capture_am").unwrap();
                let call = call.instance(target).headers(headers).raw_payload(payload);
                let call = match timeout {
                    Some(t) => call.timeout(t),
                    None => call,
                };
                call.send().await.unwrap();
                "capture_am"
            }
            Path::Fire => {
                client
                    .am_send("capture_am")
                    .unwrap()
                    .instance(target)
                    .headers(headers)
                    .raw_payload(payload)
                    .send()
                    .await
                    .unwrap();
                "capture_am"
            }
        };
        let row = format!("{path:?}, staged={is_staged}, timeout={timeout:?}");
        let wire = last_sent(&sent, handler);
        assert_eq!(wire.get("trace").map(String::as_str), Some("kept"), "{row}");
        assert_eq!(wire.contains_key("_rv"), is_staged, "{row}");
        assert_own_budget(&wire, is_staged, timeout, &row);
    }
}

/// An eager send that forwards a staged handle (`_rv`) is resolved as that
/// handle.
///
/// The receiver treats any message with `_rv` as staged: it fetches the handle
/// and replaces the payload with what it fetched. A forwarded handle has been
/// fetched and released already, so the call fails with a resolution error
/// and the handler never sees the payload that was sent. Were the handle still
/// staged, the receiver would hand the handler that payload instead, and
/// release it under its rightful consumer. This was so before the fetch
/// deadline existed.
#[tokio::test]
#[ignore = "inherited _rv: an eager send that forwards a staged handle is resolved as that handle, not delivered"]
async fn an_eager_send_that_forwards_a_staged_handle_delivers_its_own_payload() {
    let (client, server, mut seen, _sent) = capture_pair().await;
    let target = server.instance_id();
    client
        .unary("capture")
        .unwrap()
        .instance(target)
        .raw_payload(Bytes::from(vec![0x5a; 512 * 1024]))
        .send()
        .await
        .unwrap();
    let forwarded = seen.recv().await.unwrap();
    assert!(forwarded.contains_key("_rv"));

    let reply = tokio::time::timeout(
        Duration::from_secs(5),
        client
            .unary("echo")
            .unwrap()
            .instance(target)
            .headers(forwarded)
            .raw_payload(Bytes::from_static(b"mine"))
            .send(),
    )
    .await
    .expect("the forwarded call ended");
    assert_eq!(
        reply.expect("the eager payload was delivered"),
        Bytes::from_static(b"mine")
    );
}

/// A tracing propagator writes headers after the send path has built the
/// message. One that carries `_rv_budget_ms` in its context wrote an
/// inherited budget back after the send path had removed it, so an untimed
/// staged call lost its reply when the owner was slow. The encoder now owns
/// the key: it drops any `_rv_budget_ms` it finds and writes only the send's
/// own.
#[cfg(feature = "distributed-tracing")]
mod tracing_budget {
    use super::*;

    #[derive(Debug)]
    struct ForwardedBudget(String);

    /// Writes the context's budget, and a `probe` header that shows it ran.
    #[derive(Debug)]
    struct BudgetPropagator;

    impl opentelemetry::propagation::TextMapPropagator for BudgetPropagator {
        fn inject_context(
            &self,
            cx: &opentelemetry::Context,
            injector: &mut dyn opentelemetry::propagation::Injector,
        ) {
            if let Some(budget) = cx.get::<ForwardedBudget>() {
                injector.set("_rv_budget_ms", budget.0.clone());
                injector.set("probe", "injected".to_string());
            }
        }

        fn extract_with_context(
            &self,
            cx: &opentelemetry::Context,
            _: &dyn opentelemetry::propagation::Extractor,
        ) -> opentelemetry::Context {
            cx.clone()
        }

        fn fields(&self) -> opentelemetry::propagation::text_map_propagator::FieldIter<'_> {
            static FIELDS: std::sync::OnceLock<[String; 2]> = std::sync::OnceLock::new();
            opentelemetry::propagation::text_map_propagator::FieldIter::new(
                FIELDS.get_or_init(|| ["_rv_budget_ms".to_string(), "probe".to_string()]),
            )
        }
    }

    #[tokio::test]
    async fn a_propagated_budget_never_reaches_the_wire() {
        const TIMEOUT: Duration = Duration::from_secs(60);
        opentelemetry::global::set_text_map_propagator(BudgetPropagator);
        let (client, server, _seen, sent) = capture_pair().await;
        let eager = Bytes::from_static(b"eager");
        let staged = Bytes::from(vec![0x5a; 512 * 1024]);
        for (is_staged, timeout) in [(false, None), (true, None), (true, Some(TIMEOUT))] {
            let call = {
                let context =
                    opentelemetry::Context::current_with_value(ForwardedBudget("7".into()));
                let _attached = context.attach();
                let call = client
                    .unary("echo")
                    .unwrap()
                    .instance(server.instance_id())
                    .raw_payload(if is_staged { &staged } else { &eager }.clone());
                match timeout {
                    Some(t) => call.timeout(t).send(),
                    None => call.send(),
                }
            };
            call.await.unwrap();
            let row = format!("staged={is_staged}, timeout={timeout:?}");
            let wire = last_sent(&sent, "echo");
            assert_eq!(
                wire.get("probe").map(String::as_str),
                Some("injected"),
                "{row}: the propagator did not run"
            );
            assert_own_budget(&wire, is_staged, timeout, &row);
        }
    }
}
