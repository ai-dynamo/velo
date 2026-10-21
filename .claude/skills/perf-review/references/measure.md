# Measure base against head

Run this only when SKILL.md Stage 4 triggers. A number from a dev box is
"directional". A number that goes into the book must follow
docs/src/operations/benchmarking.md, "Measurement rules". Cite those rules
by number; do not copy them here.

## 1. Choose the cheapest evidence

1. **A count.** A test that counts the cost, run in both worktrees:
   `allocations_in`, a `note_*` seam, `num_alive_tasks`, writes per frame,
   or `MetricSnapshot` ratios (patterns.md, "Pinning idioms"). If the test
   is missing, write it in the scratch worktrees only. It must fail on the
   slow arm and pass on the fast one (rule 11); then it is also the pinning
   test to propose. A count is a fact on any machine.
2. **Instructions.** Run
   `perf stat -x, -e instructions:u,task-clock -- <bin> <flags>` for each
   arm, and divide by the messages or streams moved. This is steadier than
   wall time, but it cannot see wake latency.
3. **Time.** The A/B below.

Also available: `strace -f -c -e trace=write,writev,sendto,sendmsg,futex`
(compare strace runs only with strace runs, because strace changes
batching), and `heaptrack`.

## 2. Pick the harness for the changed code

The examples are their own workspace (`examples/Cargo.toml`) with their
own `Cargo.lock`. The root `[profile.release]` does not apply to them.

| Changed code | Harness and flags | Read |
|---|---|---|
| TCP or UDS framing, the coalescing writer, a listener, ingress, the TCP or UDS transport, `AdmissionGate` | `tx_budget --rungs l1t,l2t,l3t,l1u,l2u,l3u --sizes 1024,262144 --inflight 1,64 --worker-threads 4` | `RESULT,rung,size,ifl,n,p50,p99,p999,MB/s` lines; the values are round-trip ns. L1 is the control and must not move. L2 isolates the codec, L3 the transport. Also read the `send_message() sync cost` line. |
| The `AdmissionGate` slow path | the l3 rungs with `--inflight 64 --channel-capacity 1` | as above, plus "non-fast-path admissions" |
| Messenger client, server, dispatch, lanes, handlers, responses | `throughput --transport uds --modes sequential,concurrent,pipeline --payload-sizes 0,1024 --concurrency 1,100 --count 50000`; `ping_pong --transport uds --rounds 20000` | msg/s, p50 and p99 |
| The mux, streams, credit | `response_plane_bench --anchor-hosts 2 --engines 8 --requests 2000 --credit-sweep-interval-ms 200 --warmup-requests 200 --json`; `batched_streaming` with its defaults; `throughput --modes stream --streams 1,16` | `cpu_total_s` divided by tokens, `ttft_p50_us`, `itl_p50_us`, `tokens_per_write` |
| Stream open, finalize, retirement | `stream_lifecycle --mode plain` (and `--mode retired`) under `perf stat -e instructions:u` | instructions divided by `--streams` |
| The per-stream path | the mux harnesses with `--legacy` (`stream_lifecycle --path per-stream`) | as above |
| QUIC, gRPC, ZMQ | `throughput --transport quic` (or `grpc`, `zmq`), built with `--features quic` (or `grpc`, `zmq`; zmq needs cmake) | msg/s, MiB/s |
| NATS | `throughput --transport nats` | needs `nats-server` on 127.0.0.1:4222; if it is absent, skip and say so |
| UCX | `UCX_TLS=tcp timeout 300 cargo test --release --features ucx -p velo --lib bench_am_send -- --ignored --nocapture --test-threads=1` (and `bench_rma`) | the printed timings |
| Metrics code | only `response_plane_bench`, `batched_streaming` and `stream_lifecycle` install `VeloMetrics` | |

`response_plane_bench` defaults to a 2 ms credit sweep, to reproduce an old
A/B. velo's default is 200 ms. Always pass
`--credit-sweep-interval-ms 200` unless the sweep is what changed.

**Exercise check.** Before you run anything, show that the flags reach the
changed code: the transport, the mux or `--legacy`, metrics installed, a
payload above or below `COALESCE_THRESHOLD`, a depth that fills the
channel. If no harness reaches the code, report "no harness covers this
path" and use a count.

## 3. Build both arms

```shell
S="$SCRATCH/perf-review"
git worktree add --detach "$S/base" "$BASE_SHA"
git worktree add --detach "$S/head" "$HEAD_SHA"
export CARGO_TARGET_DIR="$S/target"      # shared, so dependencies build once
for arm in base head; do
  cargo build --manifest-path "$S/$arm/examples/Cargo.toml" --release --locked \
      --example <harness> [--features <only what it needs>]
  mkdir -p "$S/bin/$arm"
  cp "$CARGO_TARGET_DIR/release/examples/<harness>" "$S/bin/$arm/"
done
sha256sum "$S"/bin/*/<harness>            # the two hashes must differ
```

- Build both arms with the same features (rule 7). Do not use
  `--all-features`: it builds the bundled UCX and libzmq for nothing.
- Copy each binary out before the next build overwrites it. If the hashes
  match, the diff did not reach the binary: say so, and do not time it.
- `--locked` keeps the dependencies equal. If head added a dependency,
  build without `--locked` and say so. A bump in the root `Cargo.lock`
  does not reach the examples; measure it with a lib test or a count.
- Lib tests (the UCX benches, counting tests): run `cargo test` inside
  each worktree, with the same `CARGO_TARGET_DIR`.

## 4. Run the arms interleaved

- Record `nproc`, `uptime`, both SHAs and the exact flags (rule 9). If the
  one-minute load is above about a tenth of the cores, the run is noisy:
  say so in the report.
- Use `taskset -c <the same core list>` for both arms, with enough cores
  for the harness.
- Run at least 5 reps per arm, and alternate which arm goes first (rule
  6). Save each run to `$S/runs/<arm>-<rep>.txt`.

## 5. Read the result

- For each cell, give the median and the min-max range of each arm, and
  the change of the median in percent.
- A difference is real only when the two ranges do not overlap. With 5
  reps a side, chance does that about 1% of the time.
- A real T1 regression of 5% or more is a blocker; 2 to 5% is major;
  under 2% is minor. Overlapping ranges mean "no difference this machine
  can see". That refutes a timing claim only weakly.
- p50 and throughput decide. p99 and p999 decide only with 10 or more reps.
- Report latency beside throughput (rule 10).
- Label every number "directional: dev box, <n> cores, load <x>".

## 6. Clean up

```shell
git worktree remove --force "$S/base"
git worktree remove --force "$S/head"
git worktree prune
git worktree list          # no perf-review worktrees
git status --short         # clean
```

Keep `$S/runs`. Delete `$S/target` if disk space matters.
