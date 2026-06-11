# transports

Multi-transport active message routing. Abstracts TCP, NATS, gRPC, ZMQ, and UDS behind a unified `Transport` trait.

The `Transport` trait is defined in `velo_ext` and re-exported as `velo::Transport`. The internal orchestrator (`VeloBackend`) is not part of the public API — consumers interact through `Messenger` or `Velo`.

## Transport Summary

| Transport | Module | Feature | Protocol |
|-----------|--------|---------|----------|
| `TcpTransport` | `transports::tcp` | always | Raw TCP, 11-byte preamble + header + payload |
| `UdsTransport` | `transports::uds` | always (unix) | Unix domain sockets, same framing as TCP |
| `NatsTransport` | `transports::nats` | `nats-transport` | NATS pub-sub, base64 header in NATS HeaderMap |
| `GrpcTransport` | `transports::grpc` | `grpc` | HTTP/2 bidirectional streaming, Protobuf `FramedData` wrapper |
| `ZmqTransport` | `transports::zmq` | `zmq` | ZeroMQ PUSH/PULL, bundled libzmq via `zeromq-src` |

## Wire Format (TCP / UDS)

```text
┌──────────────┬───────────┬──────────────┬───────────────┬────────┬─────────┐
│ version (2B) │ type (1B) │ hdr_len (4B) │ pay_len (4B)  │ header │ payload │
│   u16 BE     │   u8      │   u32 BE     │   u32 BE      │ bytes  │ bytes   │
└──────────────┴───────────┴──────────────┴───────────────┴────────┴─────────┘
```

- `version`: currently 1
- `type`: `Message(0)`, `Response(1)`, `Ack(2)`, `Event(3)`, `ShuttingDown(4)`
- Max frame size: 16 MB

gRPC wraps the same preamble + header + payload in a Protobuf `FramedData` message.

## Transport Trait

```rust
pub trait Transport: Send + Sync {
    fn key(&self) -> TransportKey;
    fn address(&self) -> WorkerAddress;
    fn register(&self, peer_info: PeerInfo) -> Result<(), TransportError>;
    fn send_message(
        &self,
        instance_id: InstanceId,
        header: Bytes,
        payload: Bytes,
        message_type: MessageType,
        on_error: Arc<dyn TransportErrorHandler>,
    ) -> Result<(), SendBackpressure>;
    fn start(
        &self,
        instance_id: InstanceId,
        channels: TransportAdapter,
        rt: tokio::runtime::Handle,
    ) -> BoxFuture<'_, anyhow::Result<()>>;
    fn check_health(
        &self,
        instance_id: InstanceId,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<(), HealthCheckError>> + Send + '_>>;
    fn shutdown(&self);
    fn set_observability(&self, _: Arc<dyn TransportObservability>) {}  // default no-op
}
```

Sends return `Result<(), SendBackpressure>` — callers check for backpressure rather than receiving an error callback for that condition. Hard delivery failures are reported via `on_error`. Three inbound streams (message, response, event) flow via `TransportAdapter` flume channels.

## Shutdown Model

3-phase graceful shutdown:

1. **Gate** — flip the draining flag; transports reject new inbound `Message` frames (`ShuttingDown` response sent)
2. **Drain** — wait for all in-flight `InFlightGuard`s to drop
3. **Teardown** — cancel the teardown token, stop all listener/writer tasks, call `shutdown()` on each transport

`Response`, `Ack`, and `Event` frames continue flowing during drain so in-flight work can complete.

## WorkerAddress and Peer Registration

`WorkerAddress` is a MessagePack-encoded map of `TransportKey → endpoint bytes`. Each transport contributes its endpoint:

```text
tcp  → Vec<InterfaceEndpoint>   (multi-interface advertise list)
grpc → SocketAddr
nats → subject prefix bytes
```

When a peer registers via `WorkerAddress`, each transport extracts its own entry by `TransportKey` and caches the resolved endpoint. TCP uses NUMA + subnet scoring to select the best advertised interface endpoint at `register()` time.

## TCP Transport

```rust
use velo::transports::tcp::TcpTransportBuilder;

let listener = std::net::TcpListener::bind("0.0.0.0:0")?;
let transport = Arc::new(
    TcpTransportBuilder::new()
        .from_listener(listener)?
        .build()?
);
```

Properties:
- Zero-copy codec; DashMap connection pool; keepalive enabled
- Multi-interface advertise via `InterfaceEndpoint` list in `WorkerAddress`
- NUMA-aware endpoint selection: prefers interfaces in the same NUMA node, then subnet match, then first reachable

## NATS Transport

```rust
use velo::transports::nats::NatsTransport;

let client = async_nats::connect("nats://localhost:4222").await?;
let transport = Arc::new(NatsTransport::new(client, instance_id));
```

Subject scheme: `velo.{base58_instance_id}.{message_type}`. Drain unsubscribes from the message subject so new inbound messages stop while responses continue.

## gRPC Transport

Bidirectional streaming over HTTP/2. Tonic channels with exponential backoff reconnect. Subject scheme mirrors the message type lanes.

## Adding an Out-of-Tree Transport

External crates depend only on `velo-ext` and implement `Transport` against it:

```toml
[dependencies]
velo-ext = "0.2"  # caret: any 0.2.x; cargo unifies this with the exact =0.2.x pin in velo's workspace
```

Requirements:
1. Implement all required `Transport` methods
2. Store the `TransportObservability` handle from `set_observability` in a `OnceLock<Arc<dyn TransportObservability>>`
3. Emit `velo_transport_*` metrics via the handle on the hot path

See [CLAUDE.md](../CLAUDE.md) for the full checklist (in-tree transport steps also apply, minus the workspace wiring).

## Observability

Each transport receives a `Arc<dyn TransportObservability>` handle via `set_observability`. The runtime hands it pre-bound to the shared `VeloMetrics` registry. Metrics emitted through this handle appear in the `velo_transport_*` Prometheus series labeled with the transport's `TransportKey`.
