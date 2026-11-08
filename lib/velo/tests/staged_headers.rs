// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The two staging headers, and the headers a handler can forward.
//!
//! A send that stages its payload carries the handle in `_rv` and, when the
//! call has a `.timeout`, what is left of it in `_rv_budget_ms`. Both belong
//! to that send. The receiver fetches the handle and gives up the fetch at
//! the budget without a reply, so a handle or a budget that a send did not
//! set makes the receiver fetch someone else's payload, or makes a caller
//! with no timer wait for ever.
//!
//! A handler may pass its headers on in a send of its own, and a tracing
//! propagator writes headers late in the send path. So the receiver takes
//! both staging headers out of a staged message before its handler runs, and
//! a staged send writes both itself, after the propagator. A budget without a
//! handle means nothing to a receiver, so an eager send needs no such care.
//!
//! The nodes share an in-memory transport, so the tests need no ports. Some
//! tests decode the headers as they left the sender.

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

/// The budget on the wire of a send that staged its payload: what is left of
/// its own `timeout`, or none without one, whatever budget its headers
/// carried.
fn assert_own_budget(wire: &HashMap<String, String>, timeout: Option<Duration>, row: &str) {
    let budget = wire.get("_rv_budget_ms");
    match timeout {
        Some(timeout) => {
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
        None => assert_eq!(budget, None, "{row}: sent a budget it did not set"),
    }
}

#[derive(Clone, Copy, Debug)]
enum Path {
    Unary,
    Sync,
    Fire,
}

const ROWS: [(Path, bool, Option<Duration>); 10] = [
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

const TIMEOUT: Duration = Duration::from_secs(60);

/// Send `payload` with `headers` to `capture` (unary) or `capture_am` (sync
/// and fire) on `path`, and return the handler it went to.
async fn send(
    client: &Velo,
    target: InstanceId,
    path: Path,
    headers: HashMap<String, String>,
    payload: Bytes,
    timeout: Option<Duration>,
) -> &'static str {
    match path {
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
    }
}

fn payload(staged: bool) -> Bytes {
    if staged {
        Bytes::from(vec![0x5a; 512 * 1024])
    } else {
        Bytes::from_static(b"eager")
    }
}

/// On every send path, a handler sees the headers its caller set and neither
/// staging header. Before, a staged message reached its handler with `_rv`,
/// and with `_rv_budget_ms` under a timeout, so a handler that passed its
/// headers on passed them too.
#[tokio::test]
async fn a_handler_never_sees_the_staging_headers() {
    let (client, server, mut seen, _sent) = capture_pair().await;
    let target = server.instance_id();
    let set = HashMap::from([("trace".to_string(), "kept".to_string())]);
    for (path, staged, timeout) in ROWS {
        send(&client, target, path, set.clone(), payload(staged), timeout).await;
        let row = format!("{path:?}, staged={staged}, timeout={timeout:?}");
        let headers = tokio::time::timeout(Duration::from_secs(5), seen.recv())
            .await
            .unwrap_or_else(|_| panic!("{row}: the handler did not run"))
            .unwrap();
        assert_eq!(headers, set, "{row}");
    }
}

/// On every send path, a send that stages its payload carries its own handle
/// and its own budget, or none, whatever its headers carried: a forwarded
/// `_rv` or `_rv_budget_ms` never reaches the wire beside a staged payload.
/// An eager send carries no handle. Other headers pass through.
#[tokio::test]
async fn a_staged_send_carries_only_its_own_staging_headers() {
    let (client, server, _seen, sent) = capture_pair().await;
    let target = server.instance_id();
    let inherited = HashMap::from([
        ("_rv_budget_ms".to_string(), "7".to_string()),
        ("trace".to_string(), "kept".to_string()),
    ]);
    for (path, staged, timeout) in ROWS {
        let mut headers = inherited.clone();
        if staged {
            headers.insert("_rv".to_string(), "1".to_string());
        }
        let handler = send(&client, target, path, headers, payload(staged), timeout).await;
        let row = format!("{path:?}, staged={staged}, timeout={timeout:?}");
        let wire = last_sent(&sent, handler);
        assert_eq!(wire.get("trace").map(String::as_str), Some("kept"), "{row}");
        if staged {
            let handle = wire
                .get("_rv")
                .unwrap_or_else(|| panic!("{row}: no handle"));
            assert_ne!(handle, "1", "{row}: sent a handle it did not stage");
            assert_own_budget(&wire, timeout, &row);
        } else {
            assert!(
                !wire.contains_key("_rv"),
                "{row}: an eager send carried a handle"
            );
        }
    }
}

/// A handler that passes its headers on sends its own payload, eager or
/// staged.
///
/// Before, a handler that got a staged message saw its `_rv` and passed it
/// on. The receiver of an eager send treats any message with `_rv` as
/// staged, so it fetched the forwarded handle, which was already released,
/// and the call failed with "rendezvous handle not found" instead of
/// delivering the payload that was sent.
#[tokio::test]
async fn a_handler_that_forwards_its_headers_sends_its_own_payload() {
    let (client, server, mut seen, _sent) = capture_pair().await;
    let target = server.instance_id();
    client
        .unary("capture")
        .unwrap()
        .instance(target)
        .headers(HashMap::from([("trace".to_string(), "kept".to_string())]))
        .raw_payload(payload(true))
        .timeout(TIMEOUT)
        .send()
        .await
        .unwrap();
    let forwarded = seen.recv().await.unwrap();

    for own in [
        Bytes::from_static(b"mine"),
        Bytes::from(vec![0xa5; 512 * 1024]),
    ] {
        let reply = tokio::time::timeout(
            Duration::from_secs(5),
            client
                .unary("echo")
                .unwrap()
                .instance(target)
                .headers(forwarded.clone())
                .raw_payload(own.clone())
                .send(),
        )
        .await
        .expect("the forwarded call ended")
        .expect("the forwarded call succeeded");
        assert_eq!(reply, own, "a {}-byte payload", own.len());
    }
}

/// A tracing propagator writes headers late in the send path. One that
/// writes `_rv_budget_ms` must not put its budget beside a staged payload:
/// an untimed staged call would lose its reply when the owner was slow. A
/// staged send writes its staging headers after the propagator runs. On an
/// eager send the propagated budget means nothing, and the call is answered.
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
    async fn a_propagated_budget_never_reaches_the_wire_beside_a_staged_payload() {
        opentelemetry::global::set_text_map_propagator(BudgetPropagator);
        let (client, server, _seen, sent) = capture_pair().await;
        for (staged, timeout) in [(false, None), (true, None), (true, Some(TIMEOUT))] {
            let call = {
                let context =
                    opentelemetry::Context::current_with_value(ForwardedBudget("7".into()));
                let _attached = context.attach();
                let call = client
                    .unary("echo")
                    .unwrap()
                    .instance(server.instance_id())
                    .raw_payload(payload(staged));
                match timeout {
                    Some(t) => call.timeout(t).send(),
                    None => call.send(),
                }
            };
            call.await.unwrap();
            let row = format!("staged={staged}, timeout={timeout:?}");
            let wire = last_sent(&sent, "echo");
            assert_eq!(
                wire.get("probe").map(String::as_str),
                Some("injected"),
                "{row}: the propagator did not run"
            );
            if staged {
                assert_own_budget(&wire, timeout, &row);
            }
        }
    }
}
