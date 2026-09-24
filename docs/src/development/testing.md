# Testing

## Commands

```bash
cargo build --all-features
cargo test --all-features --all-targets
cargo clippy --all-features --no-deps --all-targets -- -D warnings
cargo fmt --check
cargo machete
bash scripts/check-semver.sh
```

Always use `--all-features`. Without it, the code behind `zmq`, `grpc`, `quic`, `nats-*`, `etcd`, `ucx`, and `simulation` does not compile, and its tests do not run.

Run one integration test by its name. The name is `<module>_<file>`. For example, `tests/transports/zmq_integration.rs` is `transports_zmq`:

```bash
cargo test --features zmq --test transports_zmq
```

## Put a time limit on each run

Run each suite under `timeout`: `timeout 900` for the full workspace, and `timeout 300` for one target. A test that hangs stops the whole runner, and it does not fail. The tests most likely to hang are those that change timeout and deadline code.

If the run exits with 124, suspect the newest test or the newest change. Run that test binary again with `--test-threads=1 --nocapture` and a short timeout to find the test that hangs.

## Services and system packages

| Need | For |
|---|---|
| NATS server with JetStream on `localhost:4222` | NATS transport, discovery, and queue tests |
| etcd on `localhost:2379` | etcd discovery tests |
| `cmake` | `zmq`. The build compiles libzmq from source. |
| `libibverbs-dev` and `librdmacm-dev` (headers only) | `ucx`. See below. |
| `protoc` | gRPC code generation |

The `ucx` feature builds UCX with InfiniBand support. UCX configure only warns when the rdma-core headers are missing, and then it builds a TCP-only UCX. `ucx-rs` stops the build instead, so a TCP-only build can never ship by mistake. No RDMA hardware is needed for the tests. They run with `UCX_TLS=tcp`.

CI links with `mold` (`RUSTFLAGS="-C linker=clang -C link-arg=-fuse-ld=mold"`). With all features and the bundled libzmq, the default `ld` can run out of memory on CI runners.

## CI jobs

| Job | What it runs |
|---|---|
| Formatting | `cargo fmt --check` |
| Clippy | clippy with `-D warnings`, then `cargo machete` |
| Cargo Deny | license and ban checks |
| Tests | `cargo test --locked --all-features --all-targets`, with NATS and etcd services |
| Coverage | `cargo llvm-cov` over the same set |
| Examples | builds all examples and runs the short ones, including `rendezvous_rdma_two_proc` with `UCX_TLS=tcp` |
| Soak smoke | `scripts/soak-smoke.sh` |
| Semver Check | `scripts/check-semver.sh` |
| Book | builds this book and checks its links |

## Rules for tests

- **Write the test first.** A claim about a fault is proved by a test that fails for that fault. The same test then proves the fix.
- **Prove the precondition. Do not wait for it.** A test that sleeps and hopes that a state is reached fails under load and under coverage instrumentation. Wait on an observable signal, such as a counter or a hook, that shows the state.
- **Put the hard lessons in tests.** A comment does not stop a regression. A test does. The doc comment of the test can be long, because it carries the reason forward.
- **Keep an ignored test honest.** If a failing test must land before its fix, mark it `#[ignore = "<finding>: <why it fails>"]`. An ignored test enforces nothing. Removing the ignore is the first step of the fix.

## Known flaky tests

`peer_batcher::tests::flush_policy::kicks_during_an_admission_park_neither_double_send_nor_reorder` sometimes fails with `left: 3.0, right: 2.0`. The test assumes the timing of a full admission gate. It fails more often under coverage instrumentation. If a PR changes no file under `lib/velo/src/streaming/` and this is the only failure, it is this flake. CI runs only on a new commit, so push a new commit to run it again.
