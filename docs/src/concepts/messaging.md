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

// Typed unary. Payloads are JSON (serde_json).
let reply: MyResponse = node_a.typed_unary::<MyResponse>("rpc")?.payload(&req)?.instance(b).send().await?;
```

A payload that is larger than the large-payload threshold does not travel in the message. The messenger stages it with [rendezvous](rendezvous.md) and sends a handle. The receiver pulls the bytes before the handler runs.

## Timeouts

velo puts no deadline on a request. A sync or unary call waits until the remote answers or the send fails. To bound the wait, set `.timeout(d)` on an `am_sync`, `unary`, or `typed_unary` builder.

```rust,ignore
use std::time::Duration;
use velo::RequestTimeout;

match node_a.unary("ping")?.timeout(Duration::from_secs(2)).instance(b).send().await {
    Ok(reply) => { /* the answer */ }
    Err(e) if e.is::<RequestTimeout>() => { /* no answer in time */ }
    Err(e) => return Err(e),
}
```

The clock starts at `send()`. It covers the wait for a response slot, discovery, the handshake, the send, and the answer. When the time runs out, the call fails with `RequestTimeout`. Every other error is a failed send or an error from the remote. Only awaiting the result is bounded, not `admitted()`.

At expiry, the call gives back its response slot, also when the caller keeps the result. A reply that arrives later is discarded. The remote can still run the handler. A call without `.timeout` makes no timer.

When the request's payload is staged, the timeout also bounds the receiver's fetch of it. At expiry the receiver gives up the fetch and its response slot, the handler does not run, and the sender frees the payload. See [Transparent staging](rendezvous.md#the-fetch-has-a-deadline).

A remote that forgot the caller still answers when it has a discovery backend. Replies go through the remote's own peer table, not back over the connection of the request. When the reply's peer is not in that table, the remote resolves it with `PeerDiscovery::discover_by_worker_id`, registers it, and sends. Each miss runs its own lookup. A send that names an instance the table does not hold resolves the same way, through `discover_by_instance_id`.

The lookup is bounded by `VeloBuilder::reply_lookup_timeout`, 5 seconds by default. A bound too large to add to the clock, such as `Duration::MAX`, leaves the lookup the 30 second limit that every lookup has. It applies only on a miss: a reply whose peer is still in the table is a table lookup with no timer. When the bound runs out, or discovery cannot find the peer, the remote logs and counts the miss and sends nothing. It cannot reach the caller, so the caller waits for its own `.timeout`. A lookup lives in the future of the reply or send that needs it, so it ends when that future ends, and it leaves nothing behind.

A node runs at most 64 of these lookups at once. A miss past that waits for a free place inside its own bound, and a miss that gets its place after the peer was registered asks nothing. At most 1024 misses wait; one more is refused at once, so a peer cannot make the node hold misses without limit. So the limits are 64 lookups in flight and 1024 waiting. They bound how many run at once, not how many run in all: each miss that reaches discovery before the peer is registered asks once, so a burst of 256 misses can make 256 asks, 64 at a time. Lookups for one peer were shared at first; the sharing needed a table of waiters, and its cancellation and teardown rules cost more than the lookups it saved. The cap does not cover discovery calls made elsewhere, such as an event subscription that resolves its owner.

A send that misses, by worker or by instance, is bounded by the call's own timeout instead. When the timeout passes before discovery answers, the lookup ends and the send is not made, and an answer that comes after the timeout registers nothing and sends nothing. The same holds for a reply and its bound. The call resolves to `RequestTimeout`, also when the caller waited in `admitted()` first; `admitted()` itself reports the failed lookup. Only a lookup that the timeout ended reads as `RequestTimeout`; any other failed send keeps its own error, whenever it is read. The handshake that follows keeps its own timeout, as before: a send whose handshake ends after the call's timeout is still made.

Discovery is trusted. A request carries only its sender's worker id, a 64-bit hash of the instance id, so a reply that misses can only ask discovery for that worker id. The node checks that the answer has the worker id it asked for, registers that instance, and sends; an answer with another worker id is refused and nothing is sent. It checks nothing else. If discovery holds a different instance with the same worker id, that instance gets the reply and its payload, and the real caller waits for its timeout. Discovery is where this is mostly stopped: the filesystem backend and the hub's stores refuse to register an instance whose worker id another instance holds, when they can already see that earlier registration. Two registrations that race, such as two filesystem handles that register at the same moment, can both succeed. The NATS backend has no such check; there, two instances with one worker id both answer a lookup, and the first answer wins. The node itself does not defend against a collision; to do so, the request would have to carry the sender's instance id.

An event completion sent to a requester the table forgot is looked up within the same reply bound.

A send with no timeout still has a bound on its lookup: 30 seconds, the same as its handshake. And every lookup ends when its node tears down: the send fails then, its place comes free, and a discovery answer that comes afterwards registers no peer.

Without a discovery backend nothing changes: the miss is logged and counted, no reply is sent, and the caller waits until its own timeout. So a call to a remote that called `unregister_peer` on the caller and publishes no discovery record waits until its timeout, and without a timeout it waits for ever.

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
