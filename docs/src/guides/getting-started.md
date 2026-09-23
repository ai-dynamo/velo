# Getting started

## Add the dependency

```bash
cargo add velo
cargo add serde --features derive
cargo add tokio --features macros,rt-multi-thread
cargo add anyhow
```

The default features are `http`, `nats-transport`, and `grpc`. Add other features as you need them:

| Feature | What it adds |
|---|---|
| `nats-transport` (default) | NATS messenger transport |
| `grpc` (default) | gRPC messenger transport and gRPC frame transport |
| `http` (default) | The `axum` dependency. The HTTP messenger transport is not built at this time. |
| `zmq` | ZeroMQ transport. The build compiles libzmq and needs `cmake`. |
| `ucx` | UCX transport and RDMA rendezvous. Linux only. See [Run rendezvous over RDMA](ucx-rdma.md). |
| `nats-discovery` | NATS peer and service discovery |
| `etcd` | etcd peer and service discovery |
| `nats-queue` | NATS JetStream work-queue backend |
| `queue-messenger` | Work-queue backend on active messages |
| `distributed-tracing` | OpenTelemetry trace context in message headers |
| `simulation` | Discrete-event simulation transport |
| `test-helpers` | Prometheus snapshot helpers for tests |

TCP and filesystem discovery are always available. UDS is always available on Unix.

`nats-transport`, `nats-discovery`, and `nats-queue` are independent. They share the `async-nats` dependency, but one does not enable the others.

## A first program

This program connects two instances over TCP and makes one typed request.

```rust,ignore
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use velo::transports::tcp::TcpTransportBuilder;
use velo::{Handler, TypedContext, Velo};

#[derive(Serialize, Deserialize)]
struct AddRequest { a: i64, b: i64 }

#[derive(Serialize, Deserialize)]
struct AddResponse { sum: i64 }

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let node_a = Velo::builder()
        .add_transport(Arc::new(TcpTransportBuilder::new().build()?))
        .build()
        .await?;
    let node_b = Velo::builder()
        .add_transport(Arc::new(TcpTransportBuilder::new().build()?))
        .build()
        .await?;

    // Register a handler on node B.
    let handler = Handler::typed_unary_async("add", |ctx: TypedContext<AddRequest>| async move {
        Ok(AddResponse { sum: ctx.input.a + ctx.input.b })
    })
    .build();
    node_b.register_handler(handler)?;

    // Without a discovery backend, register the peer by hand.
    node_a.register_peer(node_b.peer_info())?;

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

## Next steps

- Read [Messaging](../concepts/messaging.md) for the four request patterns and the handler options.
- Read [Streaming](../concepts/streaming.md) to move a sequence of items between workers.
- Read [Run the examples](examples.md) to run the examples in the repository.
