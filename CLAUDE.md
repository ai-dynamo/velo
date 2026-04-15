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

## Code Style

- Rust edition 2024 with let-chains (`if let ... && let ...`) preferred over nested `if let`
- Clippy with `-D warnings` — zero warnings policy
- No `#[allow(clippy::too_many_arguments)]` on new code — use config structs instead
- Prefer `Bytes` / `BytesMut` for message data (zero-copy slicing via `split_to().freeze()`). When converting from owned `Vec<u8>`, use `Bytes::from(vec)` (O(1) ownership transfer) not `Bytes::copy_from_slice(&vec)` (O(n) memcpy).
- Use `DashMap` for lock-free concurrent state, `flume` for bounded channels
- For ZMQ socket options that need non-Send thread isolation, use `OnceLock` for set-once values (lock-free reads) instead of `Mutex<Option<T>>` when the value never needs to be cleared.
