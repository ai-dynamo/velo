<!--
SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Velo

A high-performance distributed messaging framework for Rust. Velo provides active messaging, typed streaming, and a distributed event system over pluggable transports, with peer discovery and Prometheus metrics built in.

NOTE: Velo is an experimental repository for advanced communication patterns that is still under active design, development and testing, and the APIs and functionality are not yet stable. It should not be leveraged for production usage.

## Table of Contents

- [Overview](#overview)
- [Crates](#crates)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Messaging](#messaging)
  - [Fire-and-Forget](#fire-and-forget)
  - [Synchronous (Ack/Nack)](#synchronous-acknack)
  - [Unary Request-Response](#unary-request-response)
  - [Typed Unary](#typed-unary)
  - [Registering Handlers](#registering-handlers)
- [Events](#events)
  - [Local Events](#local-events)
  - [Distributed Events](#distributed-events)
- [Streaming](#streaming)
- [Transports](#transports)
- [Discovery](#discovery)
- [Observability](#observability)
- [Extending Velo (out-of-tree plugins)](#extending-velo-out-of-tree-plugins)
- [Building and Testing](#building-and-testing)

---

## Overview

A `Velo` instance wraps three managers under a single API:

- **Messenger** — active messaging with four patterns (fire-and-forget, sync, unary, typed unary) and handler registration
- **AnchorManager** — typed exclusive-attachment streaming for moving data between workers
- **RendezvousManager** — large payload staging and retrieval (transparent, used automatically for large message fields)

Transports, discovery backends, and metrics are injected at build time. The `velo` crate is the runtime — depend on it and you have everything.

---

## Crates

Velo ships **two** crates. Application authors depend only on `velo`; only out-of-tree plugin authors reach for `velo-ext`.

| Crate | Audience | Purpose |
|---|---|---|
| `velo` | Application authors | Runtime — active messaging, streaming, rendezvous, discovery backends, all in-tree transports (TCP, NATS, gRPC, ZMQ, UDS), Prometheus metrics, work queues |
| `velo-ext` | Out-of-tree plugin authors | Stable trait surface — `Transport`, `FrameTransport`, `PeerDiscovery`, `ServiceDiscovery`, `TransportObservability`, plus the ID/value/error types those traits reference |

`velo-ext` is `=`-pinned in `velo`'s `[dependencies]` and is the only crate that can be safely depended on alongside `velo` (see [versioning rules in CONTRIBUTING.md](CONTRIBUTING.md#velo-ext-api-stability)). The internal modules (`velo::messenger`, `velo::transports`, `velo::streaming`, `velo::discovery`, `velo::events`, `velo::observability`, `velo::rendezvous`, `velo::queue`) are all reachable through the `velo` umbrella.

---

## Installation

```bash
cargo add velo
```

Default features enable HTTP, NATS messaging, and gRPC transports. Optional / additional feature flags:

| Feature               | Description                                          |
|-----------------------|------------------------------------------------------|
| `http` *(default)*    | HTTP messenger transport                             |
| `nats-transport` *(default)* | NATS messenger transport                      |
| `grpc` *(default)*    | gRPC messenger + streaming transport                 |
| `zmq`                 | ZeroMQ transport                                     |
| `nats-discovery`      | NATS peer discovery backend                          |
| `etcd`                | etcd peer + service discovery backend                |
| `nats-queue`          | NATS JetStream work queue backend                    |
| `queue-messenger`     | Active-message-backed work queue backend             |
| `distributed-tracing` | OpenTelemetry trace context propagation              |
| `simulation`          | Discrete-event simulation transport (loom-rs)        |
| `test-helpers`        | Prometheus snapshot helpers for integration tests    |

UDS is available unconditionally on Unix targets. Filesystem peer/service discovery is unconditional.

---

## Quick Start

Two instances connected over TCP, with a typed handler and a request-response call:

```rust
use std::sync::Arc;
use velo::{Handler, TypedContext, Velo};
use velo::transports::tcp::TcpTransportBuilder;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct AddRequest { a: i64, b: i64 }

#[derive(Serialize, Deserialize)]
struct AddResponse { sum: i64 }

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Build two nodes, each with their own TCP listener
    let node_a = Velo::builder()
        .add_transport(Arc::new(TcpTransportBuilder::new().build()?))
        .build()
        .await?;

    let node_b = Velo::builder()
        .add_transport(Arc::new(TcpTransportBuilder::new().build()?))
        .build()
        .await?;

    // Register a handler on node B
    let handler = Handler::typed_unary_async("add", |ctx: TypedContext<AddRequest>| async move {
        Ok(AddResponse { sum: ctx.input.a + ctx.input.b })
    }).build();
    node_b.register_handler(handler)?;

    // Connect A → B by sharing peer info
    node_a.register_peer(node_b.peer_info())?;

    // Send a typed unary request from A to B
    let resp: AddResponse = node_a
        .typed_unary::<AddResponse>("add")?
        .payload(&AddRequest { a: 3, b: 4 })?
        .instance(node_b.instance_id())
        .send()
        .await?;

    assert_eq!(resp.sum, 7);
    Ok(())
}
```

---

## Messaging

`velo-messenger` is the core of Velo. It provides four messaging patterns, all of which are async. Patterns differ in what the caller waits for:

| Pattern       | Caller waits for…                               | Returns       |
|---------------|-------------------------------------------------|---------------|
| Fire-and-forget | Message queued to transport                   | `()`          |
| Sync          | Remote handler finishes (ack/nack)              | `SyncResult`  |
| Unary         | Remote handler response (raw bytes)             | `Bytes`       |
| Typed unary   | Remote handler response (deserialized)          | `T`           |

All four share the same builder interface: construct a builder from the `Velo` instance, attach a payload, specify the destination instance, then `.send().await`.

### Fire-and-Forget

Send a message with no response expected. Completes once the message is handed to the transport layer. Delivery is best-effort — the sender receives no confirmation that the handler executed.

```rust
node_a
    .am_send("notify")?
    .payload(&event_data)?
    .instance(node_b.instance_id())
    .send()
    .await?;
```

### Synchronous (Ack/Nack)

Wait for the remote handler to finish executing. Returns success or a handler error, but no return value.

```rust
node_a
    .am_sync("process")?
    .payload(&job)?
    .instance(node_b.instance_id())
    .send()
    .await?;
```

### Unary Request-Response

Send a request and receive raw bytes back. Use when you control the serialization format or when the response is already in `Bytes` form.

```rust
use bytes::Bytes;

let response: Bytes = node_a
    .unary("ping")?
    .raw_payload(Bytes::new())
    .instance(node_b.instance_id())
    .send()
    .await?;
```

### Typed Unary

Send a serializable request and receive a deserialized response. Serialization uses `rmp-serde` (MessagePack) by default.

```rust
let resp: MyResponse = node_a
    .typed_unary::<MyResponse>("rpc")?
    .payload(&MyRequest { /* … */ })?
    .instance(node_b.instance_id())
    .send()
    .await?;
```

### Registering Handlers

Handlers are registered by name. Each handler type has sync and async variants. The dispatch mode controls how the handler runs: `.spawn()` (the default) gives each message its own task, `.inline()` spawns without registering the task with the messenger's tracker, and `.ordered()` serialises messages per sending instance (see [Ordered handlers](#ordered-handlers)).

```rust
use velo::{Handler, Context, TypedContext};
use bytes::Bytes;

// Sync unary handler — returns raw bytes
let h = Handler::unary_handler("ping", |_ctx: Context| {
    Ok(Some(Bytes::from("pong")))
}).build();
node.register_handler(h)?;

// Async typed handler — auto deserializes input, serializes output
let h = Handler::typed_unary_async("add", |ctx: TypedContext<AddRequest>| async move {
    Ok(AddResponse { sum: ctx.input.a + ctx.input.b })
}).spawn() // run in separate task
  .build();
node.register_handler(h)?;

// Fire-and-forget async handler
let h = Handler::am_handler_async("notify", |ctx: Context| async move {
    println!("got notification: {} bytes", ctx.payload.len());
    Ok(())
}).build();
node.register_handler(h)?;
```

Handler context objects give you access to the raw payload, message headers, and the `Messenger` itself (via `ctx.msg`) — allowing handlers to send outbound messages, register new handlers, or await events. They also identify the sender: `ctx.sender_worker_id()` is always available, and `ctx.sender_instance_id()` resolves to the peer's `InstanceId` once its handshake has landed.

### Ordered handlers

By default a handler gets one task per message, so two messages from the same peer may run in either order. `.ordered()` gives each *sending instance* its own unbounded queue drained by a single task: messages from one peer are handled in the order that peer sent them, while different peers still run in parallel.

```rust
// Per-sender ordering. Messages from one peer are handled in send order;
// different peers run concurrently.
let h = Handler::am_handler_async("apply_delta", |ctx: Context| async move {
    let from = ctx.sender_instance_id();   // Option<InstanceId>
    apply(from, ctx.payload).await
}).ordered()
  .build();

// Same, but never more than 32 handlers running at once across all lanes.
// The permit is taken per message inside the lane, so per-lane ordering is
// unaffected — a lane that can't get one parks with its queue intact.
let h = Handler::am_handler_async("ingest", ingest)
    .ordered()
    .max_concurrent(32)
    .build();

// One lane for the whole handler: total order across every sender, at the
// cost of all cross-peer parallelism.
let h = Handler::am_handler("append_log", |ctx: Context| log.append(ctx.payload))
    .ordered_global()
    .build();

// Tuned: keep idle lanes alive longer, shed a sender whose own lane
// backs up past 100k queued messages.
let h = Handler::typed_unary_async("bulk", bulk).ordered_with(
    OrderedConfig::by_sender()
        .with_idle_lane_ttl(Some(Duration::from_secs(300)))
        .with_max_queue_depth(Some(100_000))
        .with_overflow(OverflowPolicy::Reject),
).build();
```

Things worth knowing:

- **Ordering is preserved, not created.** Ordered mode hands messages to the handler in the order they reached the messenger. If a peer is reachable over several transports, or a connection drops and reconnects mid-stream, arrival order was already lost upstream.
- **Large payloads are exempt.** Rendezvous-staged messages resolve out-of-band before dispatch, so they are not ordered relative to each other even on an ordered handler. The dispatcher warns once when it sees one.
- **Lane queues are unbounded.** `max_queue_depth` is a soft admission cap driving `OverflowPolicy`, not a bound on the channel. It applies **per lane**, so one backed-up peer cannot shed traffic from peers whose lanes are empty. Watch `velo_messenger_ordered_lane_depth` and `velo_messenger_ordered_lane_wait_seconds` to spot a lane falling behind — they cover the streaming mux's own `_stream_batch` ingress lane as well as your handlers, the one system handler exempted from the `_`-prefix filter; under `Reject`, `velo_messenger_dispatch_failures_total{reason="ordered_lane_shed"}` carries the shed rate (the log line fires once per handler).
- **On a unary handler this serialises request/response per sender** — a client issuing 100 concurrent calls will have them served one at a time.
- Idle lanes reap themselves after `idle_lane_ttl` (30s by default) so churning short-lived peers don't leak tasks.

---

## Events

`velo-events` provides a generational event system for coordinating async tasks. Events carry a compact `u128` handle that can be shared across threads or serialized and sent to remote instances.

### Local Events

```rust
use velo::EventManager;

let manager = EventManager::local();

// Create an event and get its handle
let event = manager.new_event()?;
let handle = event.handle();

// Await the event (can run concurrently with the trigger below)
let awaiter = manager.awaiter(handle)?;

// Trigger it — consumes the event, prevents double-completion
event.trigger()?;
awaiter.await?;
```

**RAII drop safety**: dropping an `Event` without calling `trigger()` or `poison()` automatically poisons it, so waiters are never silently abandoned.

**Merging events** — build AND-gate precondition graphs:

```rust
let load_weights   = manager.new_event()?;
let load_tokenizer = manager.new_event()?;

// Completes only after both inputs complete
let ready = manager.merge_events(vec![
    load_weights.handle(),
    load_tokenizer.handle(),
])?;

load_weights.trigger()?;
load_tokenizer.trigger()?;
manager.awaiter(ready)?.await?;
```

**Poison propagation**: poisoning an event propagates the reason to all awaiters, including merged events that depend on the poisoned input.

### Distributed Events

When using `Velo`, events are automatically backed by a distributed implementation. An `EventHandle` encodes the owning instance's identity — awaiting a remote event transparently subscribes to completion notifications over active messages.

```rust
// On node A: create an event and share its handle
let event = node_a.event_manager().new_event()?;
let handle = event.handle(); // send this handle to node B via any channel

// On node B: await the remote event
let awaiter = node_b.event_manager().awaiter(handle)?;

// On node A: trigger — node B's awaiter wakes up
event.trigger()?;
awaiter.await?;
```

The distributed event system uses a three-tier lookup: a completed-event LRU cache, piggybacking on an existing local subscription, and finally a network subscribe to the owner. A completed event checked after the fact resolves immediately without a network round-trip.

---

## Streaming

`velo-streaming` provides typed exclusive-attachment streaming. One producer (`StreamSender<T>`) pushes data to one consumer (`StreamAnchor<T>`) through the `AnchorManager`. The anchor owns a `StreamAnchorHandle` (a compact `u128`) that can be sent across the network to a producer on a different worker.

```rust
use futures::StreamExt;
use velo::StreamFrame;

// Consumer: create an anchor and share its handle
let mut anchor = node_b.create_anchor::<String>();
let handle = anchor.handle(); // send this to the producer

// Producer: attach to the anchor (can be on a different node)
let sender = node_a.attach_anchor::<String>(handle).await?;

// Produce items
sender.send("hello".into()).await?;
sender.send("world".into()).await?;
sender.finalize()?; // signals normal completion

// Consume the stream
while let Some(frame) = anchor.next().await {
    match frame? {
        StreamFrame::Item(s)    => println!("{s}"),
        StreamFrame::Finalized  => break,
        _                       => {}
    }
}
```

**`StreamFrame<T>` variants:**

| Variant              | Meaning                                         |
|----------------------|-------------------------------------------------|
| `Item(T)`            | A data item from the producer                   |
| `Finalized`          | Producer finished cleanly                       |
| `Detached`           | Producer detached without finalizing            |
| `Dropped`            | Producer task dropped unexpectedly              |
| `SenderError(String)`| Serialization error on the producer side        |
| `TransportError(String)` | Network error during delivery               |

**Cancellation**: the consumer can cancel upstream at any time via `anchor.cancel()` or a cloned `StreamController`. The producer observes this via `sender.cancellation_token()`.

**Streaming transport**: streaming frames travel over a dedicated `FrameTransport`, configured on the builder. TCP is the default; gRPC is available behind the `grpc` feature:

```rust
use velo::{Velo, StreamConfig};

let node = Velo::builder()
    .add_transport(/* messaging transport */)
    .stream_config(StreamConfig::Tcp(None))? // TCP streaming, OS-assigned port
    .build()
    .await?;
```

---

## Transports

Transports are injected at build time. Each peer is routed via the highest-priority transport it supports. Multiple transports can be active simultaneously.

```rust
use std::sync::Arc;
use velo::Velo;
use velo::transports::tcp::TcpTransportBuilder;

let node = Velo::builder()
    .add_transport(Arc::new(TcpTransportBuilder::new().build()?))
    .build()
    .await?;
```

Available transports:

| Transport | Feature Gate           | Protocol           | Notes                                            |
|-----------|------------------------|--------------------|--------------------------------------------------|
| TCP       | _(always)_             | Raw TCP            | Default, lowest latency for direct connections   |
| HTTP      | `http` *(default)*     | axum-based         | HTTP messenger transport                         |
| NATS      | `nats-transport` *(default)* | NATS pub-sub | Subject scheme `velo.{id}.{type}`                |
| gRPC      | `grpc` *(default)*     | HTTP/2 streaming   | Bidirectional, exponential backoff reconnect    |
| ZMQ       | `zmq`                  | ZMQ DEALER/ROUTER  | Automatic reconnection and message queuing       |
| UDS       | Unix only              | Unix Domain Socket | Local-only, lower overhead than TCP              |

---

## Discovery

Peer discovery is abstracted behind the `PeerDiscovery` trait. A backend is injected at build time and used to resolve `InstanceId` or `WorkerId` to a `PeerInfo` (containing the peer's transport addresses).

```rust
use std::sync::Arc;
use velo::Velo;
use velo::discovery::FilesystemPeerDiscovery;

let discovery = Arc::new(FilesystemPeerDiscovery::new("/tmp/peers.json")?);

let node = Velo::builder()
    .add_transport(/* transport */)
    .discovery(discovery)
    .build()
    .await?;

// Resolve and connect to a peer by its InstanceId
node.discover_and_register_peer(peer_instance_id).await?;
```

Available backends (all in `velo::discovery`):

| Backend                     | Feature Gate       | Use case                                        |
|-----------------------------|--------------------|-------------------------------------------------|
| `FilesystemPeerDiscovery`   | _(always)_         | Development, testing, single-host deployments   |
| NATS peer/service discovery | `nats-discovery`   | Multi-host deployments using NATS               |
| etcd peer/service discovery | `etcd`             | Production multi-host deployments               |

Without a discovery backend, peers must be registered manually:

```rust
node_a.register_peer(node_b.peer_info())?;
```

---

## Observability

`velo::observability` provides Prometheus metrics for all Velo subsystems. Create a `Registry`, register `VeloMetrics` into it, and expose or scrape that registry however your application requires. Velo itself does not run an exporter.

```rust
use std::sync::Arc;
use prometheus::Registry;
use velo::{Velo, VeloMetrics};

let registry = Registry::new();
let metrics = Arc::new(VeloMetrics::register(&registry)?);

let node = Velo::builder()
    .add_transport(/* transport */)
    .metrics(metrics)
    .build()
    .await?;

// Expose `registry` via your HTTP server, e.g. with axum or prometheus's text encoder
```

Some of these are meant to be read as a subtraction rather than on their own. Aggregate both sides onto the same label set before subtracting — the inbound frame counter is per transport while the departure counter is per instance, so a bare `a - b` matches on the full label set and returns nothing.

- **Inbound queue depth** = `sum by (job, instance) (velo_transport_frames_total{direction="inbound",message_type="message",outcome="accepted"}) - sum by (job, instance) (velo_messenger_inbound_dequeued_total)`. There is no gauge, because a sampled channel length reads the wrong number under exactly the load that makes the depth worth knowing. Three limits on the identity. The difference reads transiently negative under load, for the same reason the egress identity below does: every transport records the accepted frame only after handing it to `admit_message`, which has already enqueued it, so the dispatch loop draining the other end can count the departure before this side counts the arrival — clamp at zero rather than treating a small negative as a parse error. It holds while the instance is live: messages abandoned when a `Timeout` shutdown tears the dispatch loop down never count as departures, so from teardown onward the difference reads high by the abandoned count and stays there. And it holds only over transports that record what they admit — every in-tree messenger transport does, but the `simulation` transport's `set_observability` is a no-op, and an out-of-tree transport that admits without recording drives the difference negative.
- **Egress queue depth**, per transport, = `sum by (job, instance, transport) (velo_transport_frames_total{direction="outbound",outcome="accepted"}) - sum by (job, instance, transport) (velo_transport_frames_written_total)`. The first counts a frame the send API accepted — not always one that reached the connection's bounded send channel, since a transport that has not started yet or that fails to create the connection reports through the same accepted counter with nothing queued behind it — the second when the writer's write has returned for it, so ordinarily the difference is what is sitting in front of the socket. Six limits. It sees the bounded channel and the batch staged inside the writer, but *not* the admission gate: a frame the gate is still holding has not been accepted yet, so it is in neither term — `velo_transport_send_backpressure_total` counts those, and `velo_transport_egress_queue_wait_seconds` is the one instrument that spans both. It also stops short of the wire: a write returns once the kernel took the bytes, and these sockets carry a 2 MiB send buffer, so frames already counted written can still be queued below the instrument — sample the socket's `tx_queue` for those. The difference reads transiently negative, because a frame reaches the send channel inside the gate's send and is only counted accepted after the transport's send returns, so a fast writer can write it in between; clamp at zero rather than treating a small negative as a parse error. Frames that failed instead of reaching the socket — a replaced connection, a socket error, a failure to create the connection, or the transport not yet having started — were counted accepted and never written, so from then on the difference reads high by that count. Writer cancellation or an ordinary transport teardown does the same: frames still sitting in the channel, never dequeued, were already counted accepted and are reported neither written nor failed once the writer stops. And only TCP and UDS publish the written counter at all: gRPC, NATS, ZMQ and UCX have no series for `velo_transport_frames_written_total`, so the subtraction matches no rows for them rather than reading zero — do not paper over that with `or vector(0)`, since a zero here reads as "queue empty" rather than "not measured".
- **Receiver-side attach queueing** = `velo_streaming_anchor_attach_rtt_seconds` (stamped by the sender, around the round trip to either the `_anchor_attach` or `_mpsc_anchor_attach` handler) − `velo_streaming_anchor_operation_duration_seconds{operation="attach"}` (stamped by the receiver, inside whichever handler it was). The difference bounds that queueing from above rather than equalling it: the sender's own send path, both wire legs, the receiver's handler-spawn delay and the sender task's wake latency all sit inside the bracket too. Both attach kinds share one RTT series, so this is a population average across SPSC and MPSC attaches, not a per-kind reading. The subtraction holds only over successful attaches, with both `transport_scheme` and `instance` summed away on both sides: the sender's `transport_scheme` is its own advertised-vs-answered view, which folds to `"unknown"` whenever the receiver picked a key the sender did not itself advertise — the ordinary mixed-deployment case, not a hostile-peer one — rather than the negotiated key the receiver actually recorded; and the two series are stamped on different nodes into different registries, so they never share an `instance` label to begin with. An RTT recorded with `outcome="error"` has no receiver-side sample at all, so error-outcome attaches have nothing on the other side of the subtraction.

Metric families covered:

| Category     | Metrics                                                              |
|--------------|----------------------------------------------------------------------|
| Transport    | Frame counts, byte counts, rejections, registered peers, active connections, send backpressure; and on TCP/UDS the per-connection egress queue wait, frames written to the socket, and write duration |
| Messenger    | Handler requests, durations, payload bytes, in-flight count, dispatch failures, inbound-queue departures |
| Streaming    | Anchor operations, durations, attach round-trip time, active anchors, backpressure |
| Rendezvous   | Stage/get/release operations, durations, transferred bytes, active slots |

**Distributed tracing**: enable the `distributed-tracing` feature to propagate OpenTelemetry trace context through message headers automatically:

```toml
[dependencies]
velo = { version = "0.10", features = ["distributed-tracing"] }
```

---

## Extending Velo (out-of-tree plugins)

If you want to write a custom transport, frame transport, or discovery backend that lives outside this repository, depend on `velo-ext` instead of `velo`. `velo-ext` is the small, stable trait surface — it has no Prometheus, no Tonic, no NATS, and no transitive `velo` dependency.

```toml
[dependencies]
velo-ext = "0.5"  # exact pin tracked by velo's workspace
```

Implement one of:

| Trait                                  | What you provide                                     |
|----------------------------------------|------------------------------------------------------|
| `velo_ext::Transport`                  | A messenger transport (alternative to TCP/NATS/gRPC) |
| `velo_ext::FrameTransport`             | A streaming-frame transport                          |
| `velo_ext::PeerDiscovery`              | A peer-discovery backend                             |
| `velo_ext::ServiceDiscovery`           | A named-service discovery backend                    |
| `velo_ext::TransportObservability`     | (rarely needed) the runtime hands you one of these — implement to publish metrics into the same `velo_transport_*` Prometheus series as in-tree transports |

```rust
use std::sync::Arc;
use velo_ext::{Transport, TransportObservability, MessageType, Direction};

struct MyTransport { /* ... */ }

impl Transport for MyTransport {
    // ... required methods ...
    fn set_observability(&self, obs: Arc<dyn TransportObservability>) {
        // Store and publish into the shared metrics series
    }
}
```

`velo-ext` is `=`-pinned to a specific exact version inside `velo`. New trait methods always land with default implementations; signature changes require a coordinated release. See [`CONTRIBUTING.md`](CONTRIBUTING.md#velo-ext-api-stability) for the full stability contract.

---

## Building and Testing

```bash
# Build (all features, including zmq which requires cmake)
cargo build --all-features

# Run all tests (NATS tests require a server on localhost:4222, etcd on :2379)
cargo test --all-features --all-targets

# Run a single integration test (test names are <module>_<filename>)
cargo test --features zmq --test transports_zmq

# Lint (zero warnings)
cargo clippy --all-features --no-deps --all-targets -- -D warnings

# Format check
cargo fmt --check

# Unused dependency check
cargo machete

# Semver-check the velo-ext extension surface
bash scripts/check-semver.sh
```

> **CI note**: the `zmq` feature compiles libzmq from source and requires `cmake`. See [`CLAUDE.md`](CLAUDE.md) for full CI requirements including the `mold` linker recommendation and the velo-ext stability rules.
