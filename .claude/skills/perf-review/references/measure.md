# Measure base against head

Run this when SKILL.md Stage 4 triggers. Stage 0 step 6 uses the "Build"
section too. A number from a dev box is "directional". A number that goes
into the book must follow docs/src/operations/benchmarking.md,
"Measurement rules". Cite those rules by number; do not copy them here.

## 1. Choose the cheapest evidence, and stop when it settles the question

1. **A count.** A test that counts the cost, run in both worktrees:
   `allocations_in`, a `note_*` seam, `num_alive_tasks`, writes per frame,
   or `MetricSnapshot` ratios (patterns.md, "Pinning idioms"). If the test
   is missing, write it in the scratch worktrees only. It must fail on the
   slow arm and pass on the fast one (rule 11); then it is also the pinning
   test to propose. A count is a fact on any machine.
2. **Instructions.** Run
   `perf stat -x, -e instructions:u,task-clock -- <bin> <flags>` for each
   arm, and divide by the work done. This is steadier than wall time, but
   it cannot see wake latency. It is also the count for a cost inside a
   dependency (a lock in tokio-util, for example), which no seam can see.
3. **Time.** The A/B below. Use it when the first two do not settle the
   question, or when the finding is about latency.

When no example reaches the code, a scratch test or microbenchmark in the
worktrees is fine. Write it in `REVIEW_ROOT` only after every agent that
reads it has finished. Report its result as ns or instructions per operation,
and compare that with the budget in hot-paths.md, not as a percentage of
the microbenchmark.

Also available: `strace -f -c -e trace=write,writev,sendto,sendmsg,futex`
(compare strace runs only with strace runs, because strace changes
batching), and `heaptrack`.

## 2. Pick the harness for the changed code

The examples are their own workspace (`examples/Cargo.toml`) with their
own `Cargo.lock`. The root `[profile.release]` does not apply to them.

| Changed code | Harness and flags | Read |
|---|---|---|
| TCP or UDS framing, the coalescing writer, a listener, ingress, the TCP or UDS transport, `AdmissionGate` | `tx_budget --rungs l1t,l2t,l3t,l1u,l2u,l3u --sizes 1024,262144 --inflight 1,64 --worker-threads 4` | `RESULT,rung,size,ifl,n,p50,p99,p999,MB/s` lines; the values are round-trip ns. L1 is the control and must not move. L2 isolates the codec, L3 the transport. Also read the `send_message() sync cost` line. Each cell also runs `max(n/10, 20)` warmup round trips that `n` does not count. |
| The `AdmissionGate` slow path | the l3 rungs with `--inflight 64 --channel-capacity 1` | as above, plus "non-fast-path admissions" |
| Messenger client, server, dispatch, handlers, responses | `throughput --transport uds --modes sequential,concurrent,pipeline --payload-sizes 0,1024 --concurrency 1,100 --count 50000`; `ping_pong --transport uds --rounds 20000` | msg/s, p50 and p99 |
| Ordered handlers and lanes (`LaneRouter`, `LaneTask`) | no example registers an ordered handler | use a count: the lane tests in `messenger/server/lanes.rs` |
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

**Exercise check.** Before you run anything, show that the harness reaches
the mechanism, not only the code:
- the transport, the mux or `--legacy`, and whether metrics are installed;
- a payload above or below `COALESCE_THRESHOLD`, and a depth that fills
  the channel when batching matters;
- the number of tasks or connections that share the structure in
  question. `tx_budget` gives each transport its own `ShutdownState` and
  one connection, so it cannot show contention between connections. No
  harness puts many connections into one listener;
- how the transport was built. `tx_budget` and the other examples use
  `from_listener`, so they never reach the `bind_addr` listener path.

For a socket-option change, also record `net.core.rmem_max`,
`net.core.wmem_max`, `net.ipv4.tcp_rmem` and the kernel version. R11's
collapse needs `rmem_max` to clamp the request, so a host with a large
`rmem_max` cannot show it.

If no harness reaches the mechanism, say "no harness covers this path",
use a count or instructions, and keep the pattern's severity.

## 3. Build

**Each tree gets its own target directory.** cargo names a workspace
member's build the same in every worktree of this repo, and decides
whether it is fresh by file times. So two worktrees that share a target
directory can run each other's code with no warning. This gave false pin
results in this skill's own evaluation.

```shell
set -euo pipefail   # a failed build must stop here, before `cp`
S="$SCRATCH/perf-review"
# REVIEW_ROOT ($S/root) is the head arm. Add the base arm:
git worktree add --detach "$S/base" "$BASE_SHA"
for arm in root base; do
  CARGO_TARGET_DIR="$S/target-$arm" cargo build \
      --manifest-path "$S/$arm/examples/Cargo.toml" --release --locked \
      --example <harness> [--features <only what it needs>] 2>&1 | tee "$S/build-$arm.log"
  mkdir -p "$S/bin/$arm"
  cp "$S/target-$arm/release/examples/<harness>" "$S/bin/$arm/"
done
# Provenance: each binary names the tree it was built from. No pipe and
# no `grep -c`: under `set -e`, a grep that finds nothing exits 1, which
# is the expected result for the second check.
grep -qaF "$S/root/" "$S/bin/root/<harness>"   # must match, or stop here
if grep -qaF "$S/base/" "$S/bin/root/<harness>"; then
  echo "head binary was built from the base tree" >&2; exit 1
else
  status=$?
  if [ "$status" -ne 1 ]; then
    echo "failed to validate head binary provenance" >&2
    exit "$status"
  fi
fi
```

- If a build fails, stop. A seeded target directory still holds an old
  binary, and timing it measures the wrong code.
- A test run in Stage 0 step 6 uses the same layout: `CARGO_TARGET_DIR`
  is `$S/target-root` for `REVIEW_ROOT`, and `$S/target-base` for a base
  rerun. Check that the build log shows `Compiling velo (<that tree>)`.
  A lib-test binary is built from relative source paths, so grep it for
  the tree's root (`$S/root/`), not for `lib/velo/src`.
- Each arm builds its dependencies once. To save that time, you may seed
  an arm's target directory with `cp -a` of a warm one: only `debug/` for
  a lib test, only `release/` for a harness, because a whole target
  directory is several GB. Then run
  `find "$S/<arm>" -name '*.rs' -exec touch {} +` before you build, so
  that cargo cannot treat a copied velo build as fresh.
- Build both arms with the same features (rule 7). Do not use
  `--all-features`: it builds the bundled UCX and libzmq for nothing.
- `--locked` keeps the dependencies equal. If head added a dependency,
  build without `--locked` and say so. A bump in the root `Cargo.lock`
  does not reach the examples; measure it with a lib test or a count.
- The two binaries always differ, because they embed different paths. To
  learn whether the diff reached the binary, rely on the provenance check
  and on the exercise check, not on a hash.

## 4. Run the arms interleaved

- Record `nproc`, `uptime`, both SHAs and the exact flags (rule 9). Start
  timing only after your own builds finish and the load falls. If the
  one-minute load is still above about a tenth of the cores, the run is
  noisy: prefer a count or instructions, and say so in the report.
- Pin both arms to the same cores with `taskset -c <list>`. On a CPU with
  two core types (`ls /sys/devices/ | grep cpu_`), use one type only.
  `perf stat` then prints one line per type (`cpu_core`, `cpu_atom`): sum
  them.
- Run at least 5 reps per arm, and alternate which arm goes first (rule
  6). Save each run to `$S/runs/<arm>-<rep>.txt`.

## 5. Read the result

- For each cell, give the median and the min-max range of each arm, and
  the change of the median in percent.
- A difference is real only when the two ranges do not overlap. With 5
  reps a side, chance does that about 1% of the time.
- A real T1 regression of 5% or more is a blocker; 2 to 5% is major;
  under 2% is minor. This applies only when the harness reaches the
  mechanism (section 2). Overlapping ranges mean "no difference this
  machine can see". That refutes a timing claim only weakly.
- p50 and throughput decide. p99 and p999 decide only with 10 or more reps.
- Report latency beside throughput (rule 10).
- Label every number "directional: dev box, <n> cores, load <x>".

## 6. Check memory kept per operation

Memory that grows with the operations that ended, not with the ones still
live, passes every check above. Check it when the diff touches setup or
teardown (a stream open or close, a connection, a slot), or keeps state
for each operation.

- Run each arm at N, 4N and 16N operations, at the same concurrency.
  Record the peak RSS of each run (`/usr/bin/time -f %M`, in KiB).
  Growth per operation is (RSS at 4N - RSS at N) / 3N. With a constant
  number of live operations, it is about zero in a correct arm. The 16N
  run checks that the growth holds: (RSS at 16N - RSS at 4N) / 12N must
  match it. If it is larger, the growth speeds up with N; report both.
- Growth is not yet a leak. Before you call it one, rule out the two
  other causes, after the run and a settle of a few seconds:
  - **A backlog of tasks.** Read `num_alive_tasks()` before the workload
    and again after it. Tasks above that baseline, of operations that
    already ended, are a backlog: a loop that never
    returns `Pending` (flume and same-worker paths are not under tokio's
    coop budget) cannot let them run. Their peak memory is real, and it
    grows with N for as long as the loop runs. Count the tasks spawned
    per operation; P13 is the cause.
  - **Allocator retention.** Call `libc::malloc_trim(0)` in the harness
    and read RSS again. If it falls back, the memory was freed; glibc
    kept the pages.
  - Only memory that is still allocated after both is a leak (P25).
- Read the harness's own counters too: for `anchor_setup_bench`,
  `active_anchor_count()` and the length of the sender registry.

Example: `anchor_setup_bench` (64 tasks, 8 threads) on a base with two
tasks per stream grew about 1.5 KiB per finished stream, to 24 GB at 16M
lifecycles. It was not a leak: after the run, `malloc_trim(0)` took RSS
from 777 MiB to 6 MiB. The cause was a backlog of the two tasks per
stream, which the non-yielding loop could not let exit. With no task per
stream, 2M lifecycles peaked at 74 MiB instead of about 3 GB.

## 7. Clean up

```shell
git worktree remove --force "$S/base"
git worktree remove --force "$S/root"
git worktree prune
git worktree list          # none of this review's worktrees
git status --short         # as it was at the start
```

Keep `$S/runs`. Delete the `$S/target-*` directories if disk space
matters.
