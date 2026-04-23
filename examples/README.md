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

## Transport selection (`ping_pong`, `throughput`)

`--transport {tcp,uds,zmq,nats,grpc}` (default: `tcp`).

- `zmq` and `grpc` require building with `--features zmq` / `--features grpc`
  (both enabled by `--all-features`).
- `nats` requires a local `nats-server` on `127.0.0.1:4222`
  (see repo root `docker-compose.yml` / `scripts/dev-up.sh`).

## Features

- `zmq` — enables the ZMQ transport option.
- `grpc` — enables the gRPC transport option.
