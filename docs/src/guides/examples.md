# Run the examples

The examples are in the `examples/` crate. This crate is not a workspace member. Run the commands from `examples/`, or add `--manifest-path examples/Cargo.toml` from the repository root.

| Example | What it shows | Features |
|---|---|---|
| `ping_pong` | Round-trip time for unary messages between two instances | `zmq` or `grpc` for those transports |
| `throughput` | Messages per second, bytes per second, and p50/p95/p99 for sequential, concurrent, and pipelined sends | `zmq` or `grpc` for those transports |
| `tx_budget` | The latency budget of the send path, one layer at a time | none |
| `mpsc_fanin` | Many producers into one MPSC anchor | none |
| `batched_streaming` | Batched streaming over the mux, in the shape of LLM serving | none |
| `response_plane_bench` | Load harness for the streaming response plane | none |
| `rendezvous_rdma_two_proc` | Two processes that move 8 MiB by RDMA GET | `ucx` |
| `soak` | Stress and correctness driver for messaging, streams, and rendezvous | `grpc` |

## Messaging examples

```bash
cargo run --example ping_pong --all-features -- --rounds 1000
cargo run --example throughput --all-features -- --count 10000
cargo run --example mpsc_fanin --all-features -- --producers 4 --items 40
```

`ping_pong` and `throughput` take `--transport {tcp,uds,zmq,nats,grpc}`. The default is `tcp`. The `nats` transport needs a `nats-server` on `127.0.0.1:4222`.

## The latency budget (`tx_budget`)

`tx_budget` adds one layer on each rung. The difference between two rungs is the cost of the layer that was added.

| Rung | What it adds |
|---|---|
| `l0` | Blocking `write` and `read` on OS threads: the syscall and the kernel round trip |
| `l1` | The same bytes over tokio: the reactor, the task wake, and the scheduler |
| `l2` | `TcpFrameCodec` encode and decode |
| `l3` | The full `Transport::send_message` path, up to the consumer |

Each rung is an echo ping-pong of the same frame size in both directions, so the one-way time is half of the round trip. Use `tx_budget` to find which layer caused a change in latency.

## Soak

```text
soak messenger|stream|rendezvous|all --transport tcp|grpc --tier ci|nightly|long [--faults]
```

CI runs the `ci` tier on each PR. A nightly workflow runs the full set.

## Benchmarks

For `batched_streaming` and `response_plane_bench`, see [Benchmarking](../operations/benchmarking.md). For `rendezvous_rdma_two_proc`, see [Run rendezvous over RDMA](ucx-rdma.md).
