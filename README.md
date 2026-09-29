<!--
SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Velo

Velo is a distributed messaging library for Rust. It gives active messages, typed streams, distributed events, large-payload transfer (with RDMA over UCX), and work queues over pluggable transports. It has peer discovery and Prometheus metrics built in.

NOTE: Velo is experimental. Its design, development, and tests are still in progress, and the APIs are not stable. Do not use Velo in production.

**Documentation: [the Velo book](https://ai-dynamo.github.io/velo/)**. The source is in [`docs/src/`](docs/src/SUMMARY.md).

Typed streams support graceful stop, cancellation, and ordered finalization, including prebound tickets. See [Stream lifecycle](docs/src/concepts/streaming.md).

## Crates

| Crate | For | Contents |
|---|---|---|
| `velo` | Application authors | The runtime: messaging, streaming, rendezvous, events, queues, discovery, all in-tree transports (TCP, UDS, NATS, gRPC, ZMQ, UCX), metrics |
| `velo-ext` | Authors of out-of-tree plugins | The stable trait surface: `Transport`, `FrameTransport`, `PeerDiscovery`, `ServiceDiscovery`, `TransportObservability`, and the types they use |

Application authors depend on `velo` only. See [Workspace crates](docs/src/development/architecture.md) and [Versioning](docs/src/development/versioning.md).

## Quick start

```bash
cargo add velo
cargo add serde --features derive
cargo add tokio --features macros,rt-multi-thread
cargo add anyhow
```

```rust
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

    node_b.register_handler(
        Handler::typed_unary_async("add", |ctx: TypedContext<AddRequest>| async move {
            Ok(AddResponse { sum: ctx.input.a + ctx.input.b })
        })
        .build(),
    )?;
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

See [Getting started](docs/src/guides/getting-started.md) for the feature flags.

## Build and test

```bash
cargo build --all-features
cargo test --all-features --all-targets   # NATS on localhost:4222 and etcd on :2379 for those tests
cargo clippy --all-features --no-deps --all-targets -- -D warnings
cargo fmt --check
cargo machete
bash scripts/check-semver.sh
bash scripts/build-book.sh                # needs mdbook, mdbook-mermaid, mdbook-linkcheck
```

The `zmq` feature needs `cmake`. The `ucx` feature needs the `libibverbs-dev` and `librdmacm-dev` headers. See [Testing](docs/src/development/testing.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) and [Documentation](docs/src/development/documentation.md).
