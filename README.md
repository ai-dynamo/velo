# Velo

A high-performance distributed messaging framework for Rust. Velo provides active messaging, typed streaming, and a distributed event system over pluggable transports, with peer discovery and Prometheus metrics built in.

## Table of Contents

- [Overview](#overview)
- [Crate Map](#crate-map)
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
- [Building and Testing](#building-and-testing)

---

## Overview

A `Velo` instance wraps three managers under a single API:

- **Messenger** — active messaging with four patterns (fire-and-forget, sync, unary, typed unary) and handler registration
- **AnchorManager** — typed exclusive-attachment streaming for moving data between workers
- **RendezvousManager** — large payload staging and retrieval (transparent, used automatically for large message fields)

Transports, discovery backends, and metrics are injected at build time. The `velo` crate re-exports the full public API so a single dependency is sufficient for most applications.

---

## Crate Map

| Crate | Purpose |
|---|---|
| `velo` | Facade — re-exports everything below |
| `velo-messenger` | Active messaging: fire-and-forget, sync, unary, typed-unary |
| `velo-events` | Generational event system (local + distributed) |
| `velo-streaming` | Typed exclusive-attachment streaming (anchors + senders) |
| `velo-transports` | Transport backends: TCP, HTTP, NATS, gRPC, ZMQ, UDS |
| `velo-discovery` | Peer discovery: filesystem, NATS, etcd |
| `velo-observability` | Prometheus metrics + optional OpenTelemetry tracing |
| `velo-rendezvous` | Large payload staging and retrieval |
| `velo-queue` | Named work queues (in-memory, NATS, messenger-backed) |
| `velo-common` | Core types: InstanceId, PeerInfo, WorkerId, WorkerAddress |

---

## Installation

```toml
[dependencies]
velo = { version = "0.1" }
```

Optional feature flags:

| Feature               | Description                                          |
|-----------------------|------------------------------------------------------|
| `grpc`                | gRPC streaming transport                             |
| `zmq`                 | ZeroMQ transport                                     |
| `etcd`                | etcd peer discovery backend                          |
| `queue-nats`          | NATS JetStream work queue backend                    |
| `queue-messenger`     | Active-message-backed work queue backend             |
| `distributed-tracing` | OpenTelemetry trace context propagation              |

TCP, HTTP, and NATS transports are always available (no feature gate required).

---

## Quick Start

Two instances connected over TCP, with a typed handler and a request-response call:

```rust
use std::sync::Arc;
use velo::{Handler, TypedContext, Velo};
use velo::backend::tcp::TcpTransportBuilder;
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

Handlers are registered by name. Each handler type has sync and async variants. The dispatch mode controls whether the handler runs on the dispatcher task (`.inline()`, default, lowest latency) or in a spawned task (`.spawn()`, better isolation).

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

Handler context objects give you access to the raw payload, message headers, and the `Messenger` itself (via `ctx.msg`) — allowing handlers to send outbound messages, register new handlers, or await events.

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

**Streaming transport**: by default, frames travel over active messages (`VeloFrameTransport`). For dedicated throughput, configure a TCP or gRPC streaming server on the builder:

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
use velo::backend::tcp::TcpTransportBuilder;

let node = Velo::builder()
    .add_transport(Arc::new(TcpTransportBuilder::new().build()?))
    .build()
    .await?;
```

Available transports:

| Transport | Feature Gate | Protocol         | Notes                                             |
|-----------|--------------|------------------|---------------------------------------------------|
| TCP       | _(always)_   | Raw TCP          | Default, lowest latency for direct connections    |
| HTTP      | _(always)_   | HTTP/1.1 POST    | Axum server; fire-and-forget via 202 Accepted     |
| NATS      | _(always)_   | NATS pub-sub     | Subject scheme `velo.{id}.{type}`                 |
| gRPC      | `grpc`       | HTTP/2 streaming | Bidirectional, exponential backoff reconnect      |
| ZMQ       | `zmq`        | ZMQ DEALER/ROUTER| Automatic reconnection and message queuing        |
| UDS       | Unix only    | Unix Domain Socket | Local-only, lower overhead than TCP             |

For detailed wire format, shutdown behavior, and priority-based routing, see [`lib/velo-transports/README.md`](lib/velo-transports/README.md).

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

Available backends:

| Backend                     | Crate              | Use case                                        |
|-----------------------------|--------------------|-------------------------------------------------|
| `FilesystemPeerDiscovery`   | `velo-discovery`   | Development, testing, single-host deployments   |
| NATS-backed discovery       | `velo-discovery`   | Multi-host deployments using NATS               |
| etcd-backed discovery       | `velo-discovery` + `etcd` feature | Production multi-host deployments |

Without a discovery backend, peers must be registered manually:

```rust
node_a.register_peer(node_b.peer_info())?;
```

---

## Observability

`velo-observability` provides Prometheus metrics for all Velo subsystems. Create a `Registry`, register `VeloMetrics` into it, and expose or scrape that registry however your application requires. Velo itself does not run an exporter.

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

Metric families covered:

| Category     | Metrics                                                              |
|--------------|----------------------------------------------------------------------|
| Transport    | Frame counts, byte counts, rejections, registered peers, active connections |
| Messenger    | Handler requests, durations, payload bytes, in-flight count, dispatch failures |
| Streaming    | Anchor operations, durations, active anchors, backpressure           |
| Rendezvous   | Stage/get/release operations, durations, transferred bytes, active slots |

**Distributed tracing**: enable the `distributed-tracing` feature to propagate OpenTelemetry trace context through message headers automatically:

```toml
[dependencies]
velo = { version = "0.1", features = ["distributed-tracing"] }
```

---

## Building and Testing

```bash
# Build (all features, including zmq which requires cmake)
cargo build --all-features

# Run all tests (NATS tests require a server on localhost:4222)
cargo test --all-features --all-targets

# Lint (zero warnings)
cargo clippy --all-features --no-deps --all-targets -- -D warnings

# Format check
cargo fmt --check

# Unused dependency check
cargo machete
```

> **CI note**: the `zmq` feature compiles libzmq from source and requires `cmake`. See [`CLAUDE.md`](CLAUDE.md) for full CI requirements including the `mold` linker recommendation.
