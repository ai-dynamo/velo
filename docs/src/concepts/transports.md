# Transports

A transport moves messenger frames between instances. You add transports when you build the instance. More than one transport can be active at the same time.

```rust,ignore
let node = Velo::builder()
    .add_transport(Arc::new(TcpTransportBuilder::new().build()?))
    .build()
    .await?;
```

## Available transports

| Transport | Feature | Protocol | Notes |
|---|---|---|---|
| TCP | always | Raw TCP | Default. Coalescing writer. |
| UDS | always, Unix only | Unix domain socket | Local only. Coalescing writer. |
| NATS | `nats-transport` (default) | NATS subjects | Subject scheme `velo.{id}.{type}` |
| gRPC | `grpc` (default) | HTTP/2 bidirectional stream | Reconnects with exponential backoff |
| ZMQ | `zmq` | ZMQ DEALER/ROUTER | Reconnects and queues messages |
| UCX | `ucx`, Linux only | UCX active messages over RDMA, TCP, or shared memory | Also carries RDMA rendezvous. See [Rendezvous and RDMA](rendezvous.md). |

The `http` feature exists, but the HTTP messenger transport is not built at this time.

## Peer routing

Velo registers each peer with every transport that the peer and the local instance both support. For each peer, Velo selects a primary transport: the compatible transport with the highest priority. Messages to that peer go on the primary transport.

A `WorkerAddress` is a MessagePack map from `TransportKey` to endpoint bytes. Each transport adds its own entry, and a peer reads the entries for the transports that it has.

## The transport contract

Every transport implements the `Transport` trait from `velo-ext`. The runtime relies on these rules:

- **Sends do not wait for the wire.** `send_message` takes the frame and reports when it reached the send channel for the target. Failures after that point go to a `TransportErrorHandler` callback.
- **Inbound frames go to four lanes**: message, response, event, and shutdown. The `TransportAdapter` routes each frame to its lane.
- **Admission owns the in-flight count.** Each inbound `MessageType::Message` goes through `TransportAdapter::admit_message`. See [Shutdown and drain](shutdown.md) for why a transport must not check `is_draining()` itself.
- **Metrics use one handle.** The runtime gives each transport an observability handle through `set_observability`. In-tree and out-of-tree transports write the same `velo_transport_*` series.

## Coalescing writer

TCP and UDS use a coalescing writer. Small frames that wait in the send channel go out together in one write.

A frame above 64 KB (`COALESCE_THRESHOLD`) goes out directly, as two writes. The first write is the preamble and the header, staged in a 256 B stack buffer. The second write is the payload, with no copy. Three writes cost two extra syscalls on a `TCP_NODELAY` socket, and they put small segments on the wire before each large payload. The writer does not use `write_vectored`, because TCP can return a short write for it past about 128 KB.

The reader reserves the full frame length as soon as it has parsed the preamble. Without this, the read buffer grows by doubling from 8 KB, and each step copies all the bytes received so far. On loopback, the two changes together cut the 8 MB round trip from 6.46 ms to 4.70 ms.

The writer publishes `velo_transport_frames_written_total`, `velo_transport_write_duration_seconds`, and `velo_transport_egress_queue_wait_seconds`. Other transports have no such writer and do not publish these series. See [Observability](../operations/observability.md).

## Socket buffers are set before data flows

TCP sets `SO_RCVBUF` and `SO_SNDBUF` on the listening socket, so each accepted socket inherits the sizes at handshake time. The dial side sets them before its first write.

The old code set the sizes on the accepted socket, one task spawn after `accept`. By then the peer was already sending. On Linux, `SO_RCVBUF` at that point turns off receive autotuning and clamps the buffer to `net.core.rmem_max`. The advertised window then collapses for the life of the connection. Throughput fell about 100 times, to 22.9 MB/s, and the fault was racy, so it showed up in some runs only. The `tx_budget` example measures this path and guards against the fault.

## Frame transports for streams

Streams without the mux use a `FrameTransport`. TCP is the default. gRPC is available with the `grpc` feature. See [Streaming](streaming.md).

## Add a transport

- For an in-tree transport, follow the procedure in the project `CLAUDE.md` under "Adding a new in-tree transport".
- For a transport in another crate, see [Write an out-of-tree transport](../guides/out-of-tree-transport.md).
