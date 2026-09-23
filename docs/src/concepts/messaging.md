# Messaging

The messenger sends active messages. An active message names a handler on a remote instance. That handler runs when the message arrives.

## Four patterns

The patterns differ in what the caller waits for.

| Pattern | Builder | The caller waits for | Returns |
|---|---|---|---|
| Fire-and-forget | `am_send` | The transport to take the message | `()` |
| Sync | `am_sync` | The remote handler to finish (ack or nack) | `()` or the handler error |
| Unary | `unary` | The response bytes from the remote handler | `Bytes` |
| Typed unary | `typed_unary::<T>` | The response from the remote handler, deserialized | `T` |

All four builders work the same way. Get a builder from the `Velo` instance, attach a payload, name the destination instance, then call `.send().await`.

```rust,ignore
// Fire-and-forget. Delivery is best effort.
node_a.am_send("notify")?.payload(&event)?.instance(b).send().await?;

// Sync. Returns when the handler finishes.
node_a.am_sync("process")?.payload(&job)?.instance(b).send().await?;

// Unary with raw bytes.
let reply: Bytes = node_a.unary("ping")?.raw_payload(Bytes::new()).instance(b).send().await?;

// Typed unary. Serialization is MessagePack (rmp-serde).
let reply: MyResponse = node_a.typed_unary::<MyResponse>("rpc")?.payload(&req)?.instance(b).send().await?;
```

A payload that is larger than the large-payload threshold does not travel in the message. The messenger stages it with [rendezvous](rendezvous.md) and sends a handle. The receiver pulls the bytes before the handler runs.

## Handlers

You register a handler by name. Each handler kind has a sync form and an async form.

```rust,ignore
use velo::{Context, Handler, TypedContext};

// Sync unary handler that returns raw bytes.
node.register_handler(Handler::unary_handler("ping", |_ctx: Context| Ok(Some(Bytes::from("pong")))).build())?;

// Async typed handler. The input is deserialized and the output is serialized.
node.register_handler(
    Handler::typed_unary_async("add", |ctx: TypedContext<AddRequest>| async move {
        Ok(AddResponse { sum: ctx.input.a + ctx.input.b })
    })
    .build(),
)?;

// Async fire-and-forget handler.
node.register_handler(Handler::am_handler_async("notify", |ctx: Context| async move { Ok(()) }).build())?;
```

The handler context holds the payload, the message headers, and the messenger (`ctx.msg`). A handler can use the messenger to send messages, register handlers, or wait for events. `ctx.sender_worker_id()` is always available. `ctx.sender_instance_id()` returns the `InstanceId` of the sender after its handshake is complete.

Handler names that start with `_` are reserved for system handlers.

## Dispatch modes

| Mode | Behavior |
|---|---|
| `.spawn()` (default) | One task for each message. Two messages from one peer can run in either order. |
| `.inline()` | One task for each message, not registered with the task tracker of the messenger |
| `.ordered()` | One lane for each sending instance. A lane handles its messages in arrival order. Different senders run in parallel. |
| `.ordered_global()` | One lane for all senders. This gives total order and no parallelism. |
| `.ordered_with(OrderedConfig)` | Ordered dispatch with an explicit configuration |

`.max_concurrent(n)` limits how many lanes run the handler at the same time. A lane takes the permit for each message inside the lane, so the limit does not change the order in a lane. The builder rejects `0` because it would stop every lane.

```rust,ignore
let h = Handler::typed_unary_async("bulk", bulk)
    .ordered_with(
        OrderedConfig::by_sender()
            .with_idle_lane_ttl(Some(Duration::from_secs(300)))
            .with_max_queue_depth(Some(100_000))
            .with_overflow(OverflowPolicy::Reject),
    )
    .build();
```

Ordered dispatch has these limits:

- **It keeps order. It does not create order.** If a peer uses more than one transport, or a connection drops and reconnects, the arrival order was already lost before the messenger saw the messages.
- **Large payloads are not ordered.** A rendezvous-staged message resolves before dispatch. The dispatcher logs a warning once when it sees one.
- **Lane queues have no hard bound.** `max_queue_depth` is a soft limit for each lane. It drives `OverflowPolicy`. It does not bound the channel. One slow peer cannot cause the messages of other peers to be shed.
- **A unary handler serves one request at a time for each sender.** A client that makes 100 concurrent calls gets them one after the other.
- **Idle lanes stop.** A lane with no messages for `idle_lane_ttl` (30 s by default) stops, so short-lived peers do not leak tasks.

Watch `velo_messenger_ordered_lane_depth` and `velo_messenger_ordered_lane_wait_seconds` to find a lane that falls behind. Under `OverflowPolicy::Reject`, `velo_messenger_dispatch_failures_total{reason="ordered_lane_shed"}` counts the shed messages.
