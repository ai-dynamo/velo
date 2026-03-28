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
- Pre-bind the ROUTER socket in `build()` and pass it to the listener thread to avoid TOCTOU port races with port 0.
- Use `inproc://` PAIR sockets for control signaling between async code and I/O threads.
- Both I/O threads must signal readiness (via `std::sync::mpsc::SyncSender`) before `start()` returns, preventing shutdown deadlocks.

## Code Style

- Rust edition 2024 with let-chains (`if let ... && let ...`) preferred over nested `if let`
- Clippy with `-D warnings` — zero warnings policy
- No `#[allow(clippy::too_many_arguments)]` on new code — use config structs instead
- Prefer `Bytes` / `BytesMut` for message data (zero-copy slicing via `split_to().freeze()`)
- Use `DashMap` for lock-free concurrent state, `flume` for bounded channels
