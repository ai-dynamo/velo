# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Workspace layout

Two crates only:

- **`lib/velo`** — the runtime. All concrete transport / discovery / streaming / messenger / queue / rendezvous / observability code lives here as modules (`velo::transports::tcp`, `velo::discovery::etcd`, `velo::streaming`, etc.). This is what application authors depend on.
- **`lib/velo-ext`** — the extension trait surface. Out-of-tree implementors of `Transport`, `FrameTransport`, `PeerDiscovery`, `ServiceDiscovery`, or `TransportObservability` depend only on this crate. It must remain small, free of heavy deps (especially `prometheus`), and stable.

Anything else under `lib/` or as a workspace member is a mistake — there used to be 9 sibling crates (`velo-messenger`, `velo-transports`, `velo-streaming`, …). They were collapsed into `lib/velo/src/` because independent versioning between them silently shipped broken releases. Do not reintroduce them.

## Build & Test

```bash
# Build (all features, including zmq)
cargo build --all-features

# Run all tests (requires NATS server on localhost:4222 for nats tests, etcd on :2379 for etcd tests)
cargo test --all-features --all-targets

# Single integration test (test names are <module>_<filename>; e.g., tests/transports/tcp_integration.rs)
cargo test --features zmq --test transports_zmq

# Clippy (must pass with zero warnings)
cargo clippy --all-features --no-deps --all-targets -- -D warnings

# Formatting
cargo fmt --check

# Unused dependency check
cargo machete
```

## Versioning & publishing — read this before bumping

The bug class that motivated the workspace collapse: a 0.1.1 "patch" of an internal crate silently bumped its dep on another internal crate from 0.1 → 0.2. Cargo treated 0.1.1 as semver-compatible with 0.1.0 and pulled both versions into downstream lockfiles, producing two distinct copies of the messenger types and a flood of E0277/E0308 errors. The protections below are not optional — they are the structural fix.

### Hard rules

1. **Only `velo` and `velo-ext` are publishable.** Any new crate added to the workspace must have `publish = false` in its `Cargo.toml` unless there is a deliberate, documented reason to publish it. Adding a third publishable crate reopens the bug class.
2. **`velo-ext` is `=`-pinned in `[workspace.dependencies]`.** The line is:
   ```toml
   velo-ext = { path = "lib/velo-ext", version = "=0.1.0" }
   ```
   The `=` is load-bearing. Caret (the cargo default) lets a future "compatible" patch silently re-resolve downstream lockfiles. Do not relax this to a caret requirement.
3. **Bumping `velo-ext` requires bumping `velo` in the same PR.** The `=` pin in `[workspace.dependencies]` must be updated to track. CI will fail otherwise.
4. **New methods on `velo-ext` traits MUST have default implementations.** Adding a bare method is a breaking change for every external impl — and a major bump while we are pre-1.0 means a coordinated `velo` release. If a method genuinely cannot have a default, that is a deliberate breaking change requiring a `velo-ext` minor bump.
5. **Removing a public item from `velo-ext` is breaking.** Major bump. Don't do it casually.
6. **The `semver:` CI job is the gate.** `scripts/check-semver.sh` runs `cargo semver-checks check-release` per crate against `origin/main` and fails if a breaking change is not paired with a sufficient version bump. The `semver:skip` PR label exists for true emergencies; reach for it only with explicit reviewer agreement.

### What lives in `velo-ext` (and why it must stay small)

- Trait definitions: `Transport`, `FrameTransport`, `PeerDiscovery`, `ServiceDiscovery`, `TransportObservability`
- Value/error types referenced by those traits: `WorkerId`, `InstanceId`, `PeerInfo`, `WorkerAddress`, `TransportKey`, `MessageType`, `TransportError`, `HealthCheckError`, `SendBackpressure`, `SendOutcome`, `ShutdownState`, `Direction`, `TransportRejection`, etc.
- Channel plumbing types used in trait signatures: `TransportAdapter`, `DataStreams`, `make_channels`, `try_send_or_backpressure`

What does **not** live in `velo-ext`:

- Concrete impls (TCP/NATS/gRPC/etc. transports, etcd/filesystem/NATS discovery)
- `prometheus`, `tonic`, `axum`, or any heavyweight dep
- The `Messenger`, `Velo`, `AnchorManager`, `RendezvousManager` runtime types

The acceptance test for the boundary: `cargo tree -p velo-ext | grep -c prometheus` must be `0`. If it isn't, you smuggled a runtime concrete back into the trait crate.

## CI Requirements

- **Always use `--all-features`** for clippy, tests, and coverage. Without it, feature-gated code (zmq, grpc, nats-transport, nats-discovery, nats-queue, etcd, simulation) is silently skipped.
- **`cmake` is required** on CI runners — `zeromq-src` (bundled with the `zmq` crate) compiles libzmq from source and needs cmake.
- **`mold` linker** is used on CI for test/coverage compilation — linking all features (including the bundled libzmq C library) can OOM the default `ld` linker on GitHub runners. Set via `RUSTFLAGS="-C linker=clang -C link-arg=-fuse-ld=mold"`.
- **`rustup component add rustfmt clippy`** — the toolchain pinned in `rust-toolchain.toml` does not ship these by default on CI runners.
- **`cargo-machete`** may false-positive on feature-gated deps. Use `[package.metadata.cargo-machete] ignored` in `lib/velo/Cargo.toml` when a dep is only used behind a feature gate (e.g., `prost` for the `grpc` feature).
- **Codecov** uses `fail_ci_if_error: false` since the token may not be available on external PR runs.
- **`cargo-semver-checks`** must pass per `scripts/check-semver.sh` — this is the structural protection against the bug class above.

## Feature flags

`velo`'s top-level features (after the workspace collapse):

- Messenger transports: `http`, `nats-transport`, `grpc`, `zmq`
- Discovery backends: `nats-discovery`, `etcd` (filesystem is unconditional)
- Queue backends: `nats-queue`, `queue-messenger`
- Optional subsystems: `distributed-tracing`, `simulation`, `test-helpers`
- Default: `["http", "nats-transport", "grpc"]`

`nats-transport`, `nats-discovery`, and `nats-queue` are independent — different code paths sharing the `async-nats` dep. Enabling one does not enable the others.

## Transport layer

All transports implement the `Transport` trait (`lib/velo-ext/src/transport.rs`, re-exported as `velo::Transport`). Key patterns:

- **Fire-and-forget sends** with `TransportErrorHandler` callbacks for failures
- **Three inbound streams**: message, response, event — routed via `TransportAdapter` flume channels
- **3-phase graceful shutdown**: Gate (drain flag) → Drain (wait for in-flight) → Teardown (cancel tokens)
- **`ShutdownState`** is shared between transport and adapter — use `is_draining()` for per-frame gating in listeners
- **`WorkerAddress`** uses MessagePack-encoded maps of `TransportKey` → endpoint bytes
- **Observability** flows through `Transport::set_observability(Arc<dyn TransportObservability>)` — the runtime hands each transport a pre-bound metrics handle. In-tree transports store it in `OnceLock<Arc<dyn TransportObservability>>` and call trait methods on the hot path. External transport authors get the same handle and emit into the same `velo_transport_*` Prometheus series.

### Adding a new in-tree transport

1. Create `lib/velo/src/transports/<name>/` with `mod.rs`, `transport.rs`, optionally `listener.rs`
2. Implement the `Transport` trait (from `velo_ext`)
3. Feature-gate via a new feature in `lib/velo/Cargo.toml` and a `#[cfg(feature = "<name>")] pub mod <name>;` line in `lib/velo/src/transports.rs`
4. Add a test factory in `lib/velo/tests/transports/common/mod.rs`, create `lib/velo/tests/transports/<name>_integration.rs` using the `transport_integration_tests!` macro, and add a matching `[[test]]` entry in `lib/velo/Cargo.toml`
5. Update `examples/examples/ping_pong.rs` with transport selection
6. If the transport adds public types referenced by the `Transport` trait or its surrounding contract, those types belong in `velo-ext`, not `velo` (and require a coordinated `velo-ext` version bump per the rules above)

### Adding an out-of-tree transport (external authors)

External authors depend only on `velo-ext` and implement `Transport` against it. They never depend on `velo`. Their crate looks like:

```toml
[dependencies]
velo-ext = "0.1"  # exact pin tracked by velo's workspace
```

## Code Style

- Rust edition 2024 with let-chains (`if let ... && let ...`) preferred over nested `if let`
- Clippy with `-D warnings` — zero warnings policy
- No `#[allow(clippy::too_many_arguments)]` on new code — use config structs instead
- Prefer `Bytes` / `BytesMut` for message data (zero-copy slicing via `split_to().freeze()`). When converting from owned `Vec<u8>`, use `Bytes::from(vec)` (O(1) ownership transfer) not `Bytes::copy_from_slice(&vec)` (O(n) memcpy).
- Use `DashMap` for lock-free concurrent state, `flume` for bounded channels
- For ZMQ socket options that need non-Send thread isolation, use `OnceLock` for set-once values (lock-free reads) instead of `Mutex<Option<T>>` when the value never needs to be cleared.
