# velo

Top-level facade for the Velo distributed systems stack. Re-exports the public
API from the underlying crates so consumers only need a single dependency.

See the [workspace README](../../README.md) for full documentation covering
messaging patterns, events, streaming, transports, discovery, and observability.

## Crate map

```text
velo                  <- you are here (facade + re-exports)
├── velo-messenger    active messaging, handlers, distributed events
├── velo-streaming    typed exclusive-attachment streaming (anchors, senders)
├── velo-events       generational event system (local + distributed)
├── velo-transports   transport abstraction (TCP, HTTP, NATS, gRPC, ZMQ, UDS)
├── velo-discovery    peer discovery backends (filesystem, NATS, etcd)
├── velo-observability Prometheus metrics + optional OpenTelemetry tracing
├── velo-rendezvous   large payload staging and retrieval
├── velo-queue        named work queues
└── velo-common       shared types (InstanceId, PeerInfo, WorkerId, WorkerAddress)
```

## What's re-exported

| Source crate | Key types |
|---|---|
| `velo-messenger` | `Messenger`, `Handler`, `Context`, `TypedContext`, `AmSendBuilder`, `AmSyncBuilder`, `UnaryBuilder`, `TypedUnaryBuilder`, `VeloEvents`, `PeerDiscovery`, `SyncResult`, `UnaryResult`, `TypedUnaryResult` |
| `velo-streaming` | `AnchorManager`, `StreamAnchor`, `StreamSender`, `StreamAnchorHandle`, `StreamFrame`, `StreamController`, `AttachError`, `SendError`, `StreamError` |
| `velo-events` | `Event`, `EventManager`, `EventHandle`, `EventAwaiter`, `EventStatus`, `EventPoison`, `EventBackend` |
| `velo-transports` | `*` as `velo::backend` |
| `velo-discovery` | `*` as `velo::discovery` |
| `velo-common` | `InstanceId`, `PeerInfo`, `WorkerId`, `WorkerAddress` |
| `velo-rendezvous` | `DataHandle`, `DataMetadata`, `RegisterOptions`, `RendezvousManager`, `RendezvousWrite`, `StageMode` |
| `velo-observability` | `VeloMetrics` |

The `Velo` struct is a thin wrapper around `Arc<Messenger>`, `Arc<AnchorManager>`,
and `Arc<RendezvousManager>` that delegates every method. Use `velo.messenger()`,
`velo.anchor_manager()`, or `velo.rendezvous_manager()` for direct access.

## Tests

Integration tests live in `tests/` and exercise messaging and distributed
events across two `Velo` instances connected over TCP:

```sh
cargo test -p velo
```
