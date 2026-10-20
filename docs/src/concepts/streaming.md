# Streaming

A stream moves a sequence of typed items from a producer to a consumer. The consumer owns a `StreamAnchor<T>`. A producer attaches to the anchor and gets a `StreamSender<T>`. The anchor has a compact `u128` handle, so the consumer can send the handle to a producer on a different worker.

## Single producer (SPSC)

A default anchor accepts one producer at a time.

```rust,ignore
use futures::StreamExt;
use velo::StreamFrame;

// Consumer: create an anchor and send its handle to the producer.
let mut anchor = node_b.create_anchor::<String>();
let handle = anchor.handle();

// Producer, on the same node or another node.
let sender = node_a.attach_anchor::<String>(handle).await?;
sender.send("hello".into()).await?;
sender.finalize()?;

// Consumer.
while let Some(frame) = anchor.next().await {
    match frame? {
        StreamFrame::Item(s) => println!("{s}"),
        StreamFrame::Finalized => break,
        _ => {}
    }
}
```

| `StreamFrame<T>` variant | Meaning |
|---|---|
| `Item(T)` | A data item |
| `Finalized` | The producer finished normally |
| `Detached` | The producer detached and did not finalize |
| `Dropped` | The producer was dropped |
| `SenderError(String)` | The producer could not serialize an item |
| `TransportError(String)` | The network failed during delivery |

The consumer can request graceful stop with `anchor.controller().request_stop()`. The producer observes `sender.stop_token()` and can send its final output, then call `finalize()`. Stop is idempotent and does not discard buffered output.

The consumer can cancel the stream with `anchor.cancel()` or a cloned `StreamController`. Cancel triggers both producer tokens and ends delivery. Dropping the consumer also cancels. Stop and cancel made before attach or ticket open are retained; the producer observes them when it opens, even if it sends no data. Transport failure remains a stream error.

## Know when the output has left

`finalize` returns before the stream's output leaves the producer's node. Under the messenger mux, records wait on the producer for the consumer's credit, and a slow consumer can hold them there for as long as it does not read. A producer that shuts its node down in that time cuts the stream short.

`sender.retired()` returns a future. The future resolves once velo on the producer's node has handed on all of the stream's output; the table below says what that means on each path. It resolves after `finalize`, `detach`, or drop. Get the future before `finalize` or `detach`, because they consume the sender.

```rust,ignore
let retired = sender.retired();
sender.finalize()?;
match retired.await {
    Retirement::Sent => {}      // Handed on; see the table and the staged-item exception below.
    Retirement::Discarded => {} // Output was thrown away before it left this node.
    _ => {}                     // `Retirement` is not exhaustive.
}
```

`Retirement::Sent` means that velo handed the terminal (`Finalized`, `Detached`, or `Dropped`) on behind every record before it. It is a statement about the producer's node, not about the consumer: it does not say that the consumer received anything, and what "handed on" means depends on the path of the stream:

| Path | `Sent` means |
|---|---|
| Messenger mux | The transport admitted the batch that carries the terminal: it is in the connection writer's queue, or on its way there. At graceful shutdown, TCP and UDS write the frames that they admitted, until their teardown grace has passed (1 second unless the builder set it); QUIC fails the frames that are still queued ([known limitation](https://github.com/ai-dynamo/velo/blob/main/LIMITATIONS.md#quic-discards-queued-frames-at-teardown)); what another messenger transport does with them at teardown is its own (see [Shutdown](shutdown.md)). One exception: an item larger than the eager budget is staged on the producer until the consumer pulls it, so `Sent` can resolve while the producer still holds it ([known limitation](https://github.com/ai-dynamo/velo/blob/main/LIMITATIONS.md#sent-while-a-staged-item-is-still-held)). |
| Same worker | The terminal is in the anchor's channel. |
| Per-stream transport (TCP, gRPC) | The stream's own pump wrote the terminal to the socket (TCP), or the server took the whole request stream (gRPC). `Discarded` if the write failed, or the connection ended first. This holds when the `Velo` builder installed the transport; a frame transport from outside velo, or an `AnchorManager` built by hand, reports as the terminal enters the egress channel ([known limitation](https://github.com/ai-dynamo/velo/blob/main/LIMITATIONS.md#a-hand-built-anchormanager-reports-a-per-stream-sent-at-enqueue)). |

`Retirement::Discarded` means that the output was thrown away before velo handed it on. Over the messenger mux and on the same worker: the consumer cancelled or dropped its anchor, the consumer's process went away, the transport refused the batch, or the producer's node shut down while it held the output. On a per-stream transport: the pump's write failed, the connection ended first, or the gRPC server did not take the stream; a per-stream TCP pump cannot see a consumer that left before it writes ([known limitation](https://github.com/ai-dynamo/velo/blob/main/LIMITATIONS.md#per-stream-tcp-cannot-see-a-consumer-that-dropped-its-anchor)). A task or batch that dies with the terminal also gives `Discarded`, so the future does not wait forever.

The future shares the state that the sender already keeps for the stream. A sender can hand out any number of them, and each one wakes. A stream that nobody asks about pays one compare-exchange when it retires. [Batched streaming](batched-streaming.md#retirement) shows how the mux reports it, and what it costs.

## Byte items

Some streams carry items that the application has already serialized, such as response chunks. For these, use `Bytes` as the item type, send with `send_bytes`, and read with `next_bytes`. Inside a hand-written `Stream`, use `poll_next_bytes`.

```rust,ignore
use bytes::Bytes;
use velo::StreamFrame;

// Producer.
let sender = node_a.open_anchor_stream::<Bytes>(handle, ticket).await?;
sender.send_bytes(&chunk).await?;
sender.finalize()?;

// Consumer of `node_b.create_anchor::<Bytes>()`.
while let Some(frame) = anchor.next_bytes().await {
    match frame? {
        StreamFrame::Item(chunk) => write(chunk),
        StreamFrame::Finalized => break,
        _ => {}
    }
}
```

`send_bytes` writes the frame by hand, and the frame is the same as the one `send` writes for a `Bytes` item. So each end can use either form. `next_bytes` handles every frame other than an item in the same code as `next`, so heartbeats, errors, `Detached` and the end of the stream behave the same.

The difference is the copies of each payload. Over the mux:

| Form | Producer | Consumer |
|---|---|---|
| `send` and `next` | serde writes the frame into a buffer that grows as it writes (1 copy), then the batcher copies it into the batch (1 copy) | the ingress copies the record out of the batch (1 copy), then the `Deserialize` of `Bytes` copies the payload into a new buffer (1 copy) |
| `send_bytes` and `next_bytes` | one allocation of the exact size (1 copy), then the batch (1 copy) | the ingress copy (1 copy); the item is a slice of that buffer, with no allocation |

These counts are from the source, not measured. On the same worker, there is no batch and no ingress, so `send_bytes` and `next_bytes` make one copy in total.

A kept item keeps its frame buffer in memory. The frame is the item plus at most 11 bytes.

Why separate methods: `next` cannot skip serde for `Bytes` alone. Rust has no specialization, and a type check at run time needs `T: 'static` on the `Stream` impl of `StreamAnchor`, which is a breaking change. A marker item type would still go through serde on the producer. Why the mux still copies each record out of its batch is in [Batched streaming design](../development/batched-streaming-design.md#ingress-copies-each-record-out-of-its-batch).

## Many producers (MPSC)

`create_mpsc_anchor` makes an anchor that accepts many producers. Each producer gets a `SenderId` when it attaches, and each frame carries the `SenderId` of its producer. `Detached` and `Dropped` from one producer do not end the stream. The consumer ends the stream: it cancels, drops the anchor, or lets the unattached timeout fire after all producers leave. A producer cannot finalize an MPSC anchor.

## How frames travel

Frames travel on one of two paths:

- **A frame transport.** The builder sets it with `stream_config`. `StreamConfig::Tcp` is the default. `StreamConfig::Grpc` needs the `grpc` feature. Each stream sends its own frames. This path carries a stream when the peer does not offer the mux, or when the mux is off.
- **Batched streaming (the mux).** The builder installs it by default. `messenger_mux(MuxConfig)` changes its settings or turns it off. Records from many streams to the same peer share one messenger frame, with credit-based flow control for each stream. See [Batched streaming](batched-streaming.md).

```rust,ignore
let node = Velo::builder()
    .add_transport(tcp)
    .stream_config(StreamConfig::Tcp(None))? // TCP streams on a port that the OS assigns
    .build()
    .await?;
```

## Zero-RTT setup

`attach_anchor` makes one `_anchor_attach` round trip before the first item can move. Sometimes the consumer knows which worker will produce. For example, the consumer is about to send that worker a request. Then the consumer can make a ticket and put it in its own request, and the worker opens the stream with no round trip.

```rust,ignore
// Consumer: make a ticket.
let mut anchor = node_b.create_anchor::<String>();
let handle = anchor.handle();
let Some(ticket) = node_b.prebind_anchor(handle) else {
    // No ticket. Attach the usual way.
    let sender = node_a.attach_anchor::<String>(handle).await?;
    /* ... */
    return Ok(());
};

// The ticket travels inside the request of the application.

// Worker: open the stream on the ticket.
let sender = node_a.open_anchor_stream::<String>(handle, ticket).await?;
sender.send("hello".into()).await?;
sender.finalize()?;
```

`prebind_anchor` returns `None` when it has nothing to make. The caller then uses `attach_anchor` as usual:

- If no mux is installed, `None` is the normal result, and Velo records nothing.
- For all other causes, Velo logs at debug level and counts `outcome="error"` under `velo_streaming_anchor_operations_total{operation="prebind"}`. These causes are an MPSC anchor, the handle of another node, an anchor that is already attached or pre-bound, and an anchor that was removed. Call `prebind_anchor` only on SPSC anchors that this node created.

Call `prebind_anchor` from a runtime context, because it starts tasks. A ticket that the worker never opens does not stay forever. After the configured accept window (180 seconds by default), the bind is reclaimed and the anchor sees `SenderDropped`. `velo_streaming_unclaimed_bind_reaped_total` counts these tickets.

Ticket senders support `stop_token()` and `cancellation_token()` through messenger mux version 2. The existing slot-open exchange carries the stream session identity; no extra stream registration is needed. A peer epoch and slot generation reject stale lifecycle signals. Applications that require this lifecycle must require `messenger-mux-v2`; legacy attach fallback does not establish that capability.

## Reservation renewal

`VeloBuilder::reservation_config` sets the default unattached lifetime (120 seconds) and prepared-slot lifetime (180 seconds). Per-anchor `AnchorConfig::unattached_timeout` overrides the ordinary lifetime.

Capture `AnchorReservation::new(handle, ticket.as_ref())` when a request takes responsibility for a reply. Keep the reservation while that request waits or executes before attachment. Periodically pass held reservations to `AnchorManager::renew_reservations`. The call groups them by owner and sends at most 256 entries per remote batch. It does not attach a producer. The application owns the collection, renewal cadence, and call deadline. Velo does not schedule renewal. Choose a cadence shorter than the reservation lifetime.

Renewal extends only a live, unattached reservation. Attached, expired, cancelled, and replaced reservations are successful no-ops, including attachment races. Renewal cannot revive an expired stream. A transport failure returns an error. Successful delivery does not prove that a reservation still exists.

Release the held reservation after downstream acceptance, attachment, cancellation, or failure. Renewal does not extend an application request deadline or replace producer heartbeats. A prepared ticket still identifies one slot generation; renewal does not make that ticket reusable after detach.
