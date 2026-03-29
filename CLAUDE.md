# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test

```bash
# Build (all features, including zmq)
cargo build --all-features

# Run all tests (requires NATS server on localhost:4222 for nats tests)
cargo test --all-features --all-targets

# Run a single crate's tests
cargo test -p velo-transports --features zmq

# Clippy (must pass with zero warnings)
cargo clippy --all-features --no-deps --all-targets -- -D warnings

# Formatting
cargo fmt --check

# Unused dependency check
cargo machete
```

## CI Requirements

- **Always use `--all-features`** for clippy, tests, and coverage. Without it, feature-gated code (zmq, grpc, nats) is silently skipped.
- **`cmake` is required** on CI runners — `zeromq-src` (bundled with the `zmq` crate) compiles libzmq from source and needs cmake.
- **`mold` linker** is used on CI for test/coverage compilation — linking all features (including the bundled libzmq C library) can OOM the default `ld` linker on GitHub runners. Set via `RUSTFLAGS="-C linker=clang -C link-arg=-fuse-ld=mold"`.
- **`rustup component add rustfmt clippy`** — the toolchain pinned in `rust-toolchain.toml` (1.93.1) does not ship these by default on CI runners.
- **`cargo-machete`** may false-positive on feature-gated deps. Use `[package.metadata.cargo-machete] ignored` in Cargo.toml when a dep is only used behind a feature gate (e.g., `prost` in velo-streaming's grpc feature).
- **Codecov** uses `fail_ci_if_error: false` since the token may not be available on external PR runs.

## Architecture

Velo is a Rust workspace (edition 2024) providing a high-performance distributed messaging framework. Key crates:

| Crate | Purpose |
|---|---|
| `velo` | Facade — re-exports all layers |
| `velo-messenger` | Active messaging, handlers, request/response |
| `velo-transports` | Transport abstraction: TCP, UDS, gRPC, NATS, ZMQ |
| `velo-streaming` | Typed exclusive-attachment streaming |
| `velo-rendezvous` | Rendezvous protocol for data staging |
| `velo-events` | Generational event system |
| `velo-discovery` | Peer discovery trait (etcd/NATS backends) |
| `velo-observability` | Prometheus metrics & distributed tracing |
| `velo-common` | Foundational types (InstanceId, PeerInfo, TransportKey) |

### Transport Layer

All transports implement the `Transport` trait (`velo-transports/src/transport.rs`). Key patterns:

- **Fire-and-forget sends** with `TransportErrorHandler` callbacks for failures
- **Three inbound streams**: message, response, event — routed via `TransportAdapter` flume channels
- **3-phase graceful shutdown**: Gate (drain flag) → Drain (wait for in-flight) → Teardown (cancel tokens)
- **`ShutdownState`** is shared between transport and adapter — use `is_draining()` for per-frame gating in listeners
- **`WorkerAddress`** uses MessagePack-encoded maps of TransportKey → endpoint bytes

### Adding a New Transport

1. Create `lib/velo-transports/src/<name>/` with `mod.rs`, `transport.rs`, optionally `listener.rs`
2. Implement the `Transport` trait
3. Feature-gate in `Cargo.toml` and `lib.rs`
4. Add test factory in `tests/common/mod.rs` and create `tests/<name>_integration.rs` using `transport_integration_tests!` macro
5. Update `ping_pong.rs` example with transport selection
6. Propagate feature flag through `velo-messenger` and `velo` Cargo.toml files

### ZMQ Transport Notes

- ZMQ sockets are **not Send and not async-compatible**. All ZMQ I/O runs on dedicated `std::thread` threads, bridged to async via flume channels.
- Uses **DEALER/ROUTER** pattern: ROUTER on listener, multiplexed DEALERs on sender.
- The `zmq` 0.10 crate's `send()` requires `&[u8]` (implements `Sendable`), not `&[u8; N]` — use slice references.
- `zmq` 0.10 does not expose `recv_monitor_event()` — parse raw two-frame monitor messages manually (u16 LE event ID + u32 LE value, then address frame).
- `zmq` 0.10 / zmq-sys 0.12 bundles **libzmq 4.3.1** — `ZMQ_TCP_NODELAY` (constant 167) is NOT available (requires libzmq 4.3.3+). The Rust bindings do not expose it.
- Pre-bind the ROUTER socket in `build()` and pass it to the listener thread to avoid TOCTOU port races with port 0.
- Use `inproc://` PAIR sockets for control signaling between async code and I/O threads (listener thread only — the sender thread uses `SenderCommand::Shutdown` via the flume channel).
- Both I/O threads must signal readiness (via `std::sync::mpsc::SyncSender`) before `start()` returns, preventing shutdown deadlocks.
- **ZMQ has ~2.5x higher latency than TCP** (~85µs vs ~33µs RTT in ping-pong). This is inherent to ZMQ's architecture: each message direction crosses an extra ZMQ I/O thread hop (application thread ↔ libzmq I/O thread), adding ~4 thread wake-ups per round trip. This is the tradeoff for ZMQ's automatic reconnection, message queuing, and identity routing.
- When receiving ZMQ multipart messages, prefer `Bytes::from(Vec<u8>)` (O(1) ownership transfer) over `Bytes::copy_from_slice(&[u8])` (O(n) memcpy). `recv_multipart()` returns `Vec<Vec<u8>>` where each inner Vec already owns a copy from ZMQ's internal buffer — converting to `Bytes::from()` avoids a redundant second copy.

## Code Style

- Rust edition 2024 with let-chains (`if let ... && let ...`) preferred over nested `if let`
- Clippy with `-D warnings` — zero warnings policy
- No `#[allow(clippy::too_many_arguments)]` on new code — use config structs instead
- Prefer `Bytes` / `BytesMut` for message data (zero-copy slicing via `split_to().freeze()`). When converting from owned `Vec<u8>`, use `Bytes::from(vec)` (O(1) ownership transfer) not `Bytes::copy_from_slice(&vec)` (O(n) memcpy).
- Use `DashMap` for lock-free concurrent state, `flume` for bounded channels
- For ZMQ socket options that need non-Send thread isolation, use `OnceLock` for set-once values (lock-free reads) instead of `Mutex<Option<T>>` when the value never needs to be cleared.
