# velo-examples

Runnable examples for the `velo` facade crate. This is a standalone crate
(not a workspace member) — run commands from `examples/` or pass
`--manifest-path examples/Cargo.toml` from the repo root.

## Examples

| Name         | What it shows                                                                |
|--------------|------------------------------------------------------------------------------|
| `ping_pong`  | Round-trip latency for unary messages between two `Velo` instances.          |
| `throughput` | msgs/sec + MB/sec + p50/p95/p99 across sequential, concurrent, and pipeline. |
| `mpsc_fanin` | MPSC streaming: many producers fan into one consumer via `LoopbackTransport`.|

## Run

```bash
# from the examples/ directory:
cargo run --example ping_pong  --all-features -- --rounds 1000
cargo run --example throughput --all-features -- --count 10000
cargo run --example mpsc_fanin --all-features -- --producers 4 --items 40
```

### `throughput` across two hosts

By default, `throughput` runs its server and client in one process, over loopback. To measure a network, run the two halves on two hosts. Give both the same `--peer-file` on a shared filesystem, and set `VELO_BIND_IP` to each host's address on the network under test.

```bash
# host A
VELO_BIND_IP=10.0.0.1 cargo run --release --example throughput --all-features -- \
  --role server --transport quic --peer-file /shared/peer
# host B
VELO_BIND_IP=10.0.0.2 cargo run --release --example throughput --all-features -- \
  --role client --transport quic --peer-file /shared/peer --count 20000
```

The server removes an old file, writes its peer info to the file, and runs until it is stopped. The client reads the file again until the server named in it answers, so a file left by an earlier run does no harm. Then it prints the results table. `VELO_BIND_IP` applies to the `tcp` and `quic` transports of the examples, and the default is `127.0.0.1`.

## Transport selection (`ping_pong`, `throughput`)

`--transport {tcp,uds,zmq,nats,grpc,quic,ucx}` (default: `tcp`).

- `zmq`, `grpc`, `quic` and `ucx` require building with `--features zmq` / `--features grpc` /
  `--features quic` / `--features ucx` (all enabled by `--all-features`; `ucx` is Linux only).
- `nats` requires a local `nats-server` on `127.0.0.1:4222`
  (see repo root `docker-compose.yml` / `scripts/dev-up.sh`).

## Features

- `zmq` — enables the ZMQ transport option.
- `grpc` — enables the gRPC transport option.
- `quic` — enables the QUIC transport option.
