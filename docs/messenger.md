# messenger

Active messaging layer for Velo distributed systems. Provides request-response and fire-and-forget messaging patterns over pluggable transports.

## Architecture

```
velo-ext (types + traits)
    ↓
velo::transports (VeloBackend, Transport impls)
    ↓
velo::messenger (Messenger, handlers, client builders)
    ↓
velo::Velo (facade: Messenger + AnchorManager + RendezvousManager)
```

`Messenger` owns a `VeloBackend`, wires up inbound message dispatch, and exposes builder APIs for registering handlers and sending messages.

### Internal Modules

```
messenger/
  messenger.rs       Messenger + MessengerBuilder
  discovery.rs       PeerDiscovery trait (re-exported from velo_ext)
  client/            AmSendBuilder, AmSyncBuilder, UnaryBuilder, TypedUnaryBuilder
  handlers/          Handler definitions, builder API, dispatch adapters
  server/            Inbound message dispatch, system handlers (_hello, _list_handlers)
  events/            VeloEvents: distributed event routing over active messages
  common/            Wire format (MessageId, encoding, ResponseManager)
  large_payload.rs   RendezvousStager/Resolver bridge for transparent large payloads
```

## Messaging Patterns

All four patterns are async.

**Fire-and-forget** — completes once the message is issued to the transport; no application-level acknowledgement:
```rust
messenger.am_send("notify")?.payload(&data)?.instance(peer).send().await?;
```

**Synchronous (ack/nack)** — completes after the remote handler finishes:
```rust
messenger.am_sync("process")?.payload(&data)?.instance(peer).send().await?;
```

**Unary** — send and receive raw bytes:
```rust
let response: Bytes = messenger.unary("ping")?.raw_payload(Bytes::new()).instance(peer).send().await?;
```

**Typed unary** — automatic serde serialization/deserialization:
```rust
let resp: MyResponse = messenger.typed_unary::<MyResponse>("rpc")?.payload(&request)?.instance(peer).send().await?;
```

## Registering Handlers

```rust
use velo::{Handler, Context, TypedContext};

// Sync unary handler
let handler = Handler::unary_handler("ping", |_ctx| Ok(Some(Bytes::new()))).build();
messenger.register_handler(handler)?;

// Async typed handler
let handler = Handler::typed_unary_async("add", |ctx: TypedContext<AddRequest>| async move {
    Ok(AddResponse { sum: ctx.input.a + ctx.input.b })
}).build();
messenger.register_handler(handler)?;
```

Each handler type has sync and async variants, plus a dispatch mode:
- `.spawn()` — task-isolated, independent tokio task per call
- `.inline()` — dispatched on the server pump task, minimal latency

Handler context (`Context`, `TypedContext`) includes a `ctx.msg` reference to the `Messenger`, allowing handlers to send messages or register new handlers during dispatch.

## System Handlers

The messenger automatically registers these internal handlers:
- `_hello` — peer registration exchange (called on `register_peer`)
- `_list_handlers` — returns the list of registered handler names
- `_event_subscribe` / `_event_trigger` / `_event_poison` — distributed event routing

## Builder

```rust
use velo::messenger::{Messenger, MessengerBuilder};

let messenger = MessengerBuilder::new()
    .add_transport(tcp_transport)
    .discovery(my_discovery_impl)
    .metrics(metrics)
    .build()
    .await?;
```

Prefer `Velo::builder()` over `MessengerBuilder` directly — it also wires the `AnchorManager` and `RendezvousManager` and merges the streaming transport's `WorkerAddress` into `peer_info()`.

## Large Payload Transparency

When `large_payload_support` is configured (automatic via `VeloBuilder`), payloads above the staging threshold are transparently replaced with a `_rv` header on send and resolved before handler dispatch on receive. Small payloads continue inline. See [rendezvous.md](rendezvous.md) for protocol details.

## Handler Availability

```rust
// Wait for a handler to appear on a remote instance
messenger.wait_for_handler(instance_id, "my-handler").await?;

// List handlers on a remote instance
let handlers = messenger.available_handlers(instance_id).await?;
```

## Distributed Events

`Messenger::event_manager()` returns an `EventManager` backed by `VeloEvents`, which routes event operations over active messages when the event belongs to a remote instance. See [events.md](events.md).
