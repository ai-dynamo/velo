# streaming

Typed exclusive-attachment streaming over Velo transport. One producer (`StreamSender<T>`) pushes serialized frames to one consumer (`StreamAnchor<T>`) through an `AnchorManager` registry.

## Key Types

| Type | Role |
|------|------|
| `AnchorManager` | Central registry: creates anchors, attaches senders, routes control-plane AMs |
| `StreamAnchor<T>` | SPSC consumer-side `Stream` impl yielding `Result<StreamFrame<T>, StreamError>` |
| `StreamSender<T>` | SPSC producer-side sender with heartbeat and drop safety |
| `StreamAnchorHandle` | Compact `u128` wire handle: upper 64 bits = `WorkerId`, lower 64 bits = local anchor ID |
| `AnchorKind` | Discriminator on the handle: `Spsc` or `Mpsc` |
| `StreamController` | Cloneable cancel handle for consumer-side upstream cancel |
| `StreamFrame<T>` | Seven-variant wire frame enum (see below) |
| `TcpFrameTransport` | Default streaming transport; TCP listener, multi-interface advertise |
| `GrpcFrameTransport` | gRPC-based streaming transport (`grpc` feature) |
| `VeloFrameTransport` | **Deprecated** in 0.4.0; has correctness issues under multi-stream concurrency |
| `FrameTransport` | Trait: pluggable ordered-delivery transport |

## StreamFrame Variants

| Variant | Description |
|---------|-------------|
| `Item(T)` | A data frame |
| `Finalized` | Terminal: sender completed successfully |
| `Detached` | Terminal: sender detached (SPSC can reattach) |
| `Dropped` | Terminal: sender was dropped without finalizing |
| `SenderError(String)` | Terminal: sender encountered an error |
| `TransportError(String)` | Terminal: transport-level failure |
| `Heartbeat` | Internal only; never yielded to consumer |

## SPSC (Exclusive-Attachment) Usage

```rust
use futures::StreamExt;
use velo::{AnchorManager, StreamFrame};

// Consumer creates an anchor
let mut anchor = velo.create_anchor::<String>();
let handle = anchor.handle();

// Producer attaches (can be on a different worker)
let sender = velo.attach_anchor::<String>(handle).await?;

sender.send("hello".into()).await?;
sender.send("world".into()).await?;
sender.finalize()?;

while let Some(frame) = anchor.next().await {
    match frame {
        Ok(StreamFrame::Item(s)) => println!("{s}"),
        Ok(StreamFrame::Finalized) => break,
        Err(e) => eprintln!("{e}"),
        _ => {}
    }
}
```

Only one `StreamSender` may attach to an anchor at a time (enforced atomically). After `Detached`, a new sender can reattach to the same anchor.

## MPSC (Multi-Producer) Usage

`MpscStreamAnchor<T>` accepts frames from multiple senders, each tagged with a `SenderId`. Sender lifecycle events (`Detached`, `Dropped`) are non-terminal — the stream only ends when the consumer cancels or the anchor is dropped.

```rust
use velo::streaming::mpsc::MpscAnchorConfig;

// Default config
let anchor = velo.create_mpsc_anchor::<String>();
let handle = anchor.handle();

// Or with config
let anchor = velo.create_mpsc_anchor_with_config::<String>(MpscAnchorConfig {
    max_senders: 16,
    channel_capacity: 256,
    ..Default::default()
});

// Any sender attaches to the same handle
let sender_a = velo.attach_mpsc_anchor::<String>(handle).await?;
let sender_b = velo.attach_mpsc_anchor::<String>(handle).await?;

sender_a.send("from a".into()).await?;
sender_b.send("from b".into()).await?;
```

## Cancellation

Consumer-initiated upstream cancel:

```rust
let controller = anchor.controller();
controller.cancel();
// Fires a _stream_cancel AM to the sender's worker
// The sender observes via StreamSender::cancellation_token()
```

## Streaming Transport

`VeloBuilder` defaults to `TcpFrameTransport` (bind `0.0.0.0:0`, advertise all UP non-loopback interfaces in `WorkerAddress`). The peer side selects the best endpoint using NUMA + subnet match at `register_peer` time.

```rust
// Default: TCP
let velo = Velo::builder().build().await?;

// Explicit bind address
let velo = Velo::builder()
    .stream_bind_addr("10.0.0.1".parse()?)
    .build().await?;

// gRPC streaming (grpc feature)
let velo = Velo::builder()
    .stream_config(StreamConfig::Grpc(None))?
    .build().await?;
```

Only one streaming transport server is allowed per `Velo` instance.

## Observability

Saturation counters (all increment on backpressure):
- `velo_streaming_reader_pump_backpressure_total` — frame channel full on receive
- `velo_streaming_server_pump_backpressure_total` — server-side dispatch stalled
- `velo_streaming_producer_send_backpressure_total` — `MpscStreamSender` blocked
- `velo_streaming_heartbeat_watchdog_firings_total` — heartbeat missed deadline

See [SATURATION.md](../lib/velo/src/streaming/SATURATION.md) for the full saturation runbook.

## Module Map

```
streaming/
  anchor.rs         AnchorManager, StreamAnchor, AnchorConfig, AttachError, StreamController
  sender.rs         StreamSender (heartbeat task, drop safety)
  frame.rs          StreamFrame<T>, StreamError, SendError
  handle.rs         StreamAnchorHandle, AnchorKind (u128 wire encoding)
  control.rs        Control-plane AM handlers and wire types
  transport.rs      FrameTransport trait
  tcp_transport.rs  TcpFrameTransport (multi-interface advertise, NUMA-aware select)
  grpc_transport.rs GrpcFrameTransport (grpc feature)
  velo_transport.rs VeloFrameTransport (deprecated)
  mpsc/             MpscStreamAnchor, MpscStreamSender, MpscAnchorConfig, SenderId
```
