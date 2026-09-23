# Benchmarking

Use this chapter to measure the streaming response plane. It describes the two in-repo harnesses, the external serving rig, and the measurement rules that earlier mistakes made necessary. [Response plane performance](response-plane-performance.md) has the results.

## Select the harness

| Harness | Use it to | What it reports |
|---|---|---|
| `batched_streaming` | Show that the mux batches, and compare flush policies. | Tokens per wire write, per engine and in total. |
| `response_plane_bench` | Compare two configurations of velo under load, in one process. | TTFT and inter-token latency percentiles, requests per second, tokens per write, process CPU. |
| External serving rig | Compare velo with another response plane inside a real serving stack. | Client TTFT, ITL and end-to-end latency, throughput, frontend CPU per request, errors. |

Both in-repo harnesses model the same serving shape. Anchor hosts stand in for frontends and own one response stream per request. Engines run continuous batching: each forward pass puts exactly one token on each active request's stream. Request dispatch is an in-process channel. The response path is real: anchors live on different workers from the engines, so every attach and every token crosses a loopback TCP socket.

## Run batched_streaming

Run all commands from the `examples/` directory:

```bash
cargo run --release --all-features --example batched_streaming -- --engines 2 --requests 24 --max-batch 8 --tokens 40
```

| Flag | Default | Meaning |
|---|---|---|
| `--engines` | 2 | Token producers. Each keeps its own active batch. |
| `--requests` | 24 | Requests to serve. Each request is one response stream. |
| `--max-batch` | 8 | Requests one engine holds in its active batch at once. |
| `--tokens` | 40 | Longest response, in tokens. Each request's budget comes from its index, so every run has the same composition. |
| `--pass-delay-ms` | 1 | Gap between forward passes, standing in for GPU time. |
| `--legacy` | off | Run the same workload on the per-stream path. |
| `--flush-policy` | `manual` | `manual` calls `flush_batch()` once per pass. `auto` lets the batcher write at every wake. Ignored with `--legacy`. |

The example has three anchor hosts. It fails the run if any request misses a token, sees an unexpected frame, or if the batcher's `velo_streaming_mux_records_per_batch{direction="sent"}` does not account for every token.

Keep `--pass-delay-ms` above zero for a per-stream comparison. With no gap, the engine runs ahead, several tokens queue on each stream, and per-stream coalescing packs them. That flatters the per-stream path and hides the effect under test.

To reproduce the flush-policy comparison in [Batched streaming](../concepts/batched-streaming.md#when-an-explicit-flush-helps), run each configuration five times with `--flush-policy auto` and `--flush-policy manual`:

```bash
cargo run --release --all-features --example batched_streaming -- --engines 2 --requests 96 --max-batch 32 --tokens 40 --flush-policy auto
cargo run --release --all-features --example batched_streaming -- --engines 2 --requests 96 --max-batch 32 --tokens 40 --flush-policy manual
cargo run --release --all-features --example batched_streaming -- --engines 2 --requests 24 --max-batch 8 --tokens 40 --pass-delay-ms 0 --flush-policy auto
```

CI runs `batched_streaming` at the defaults with both flush policies.

## Run response_plane_bench

Run all commands from the `examples/` directory:

```bash
cargo run --release --all-features --example response_plane_bench -- --anchor-hosts 2 --engines 128 --requests 2000 --credit-sweep-interval-ms 200
```

| Flag | Default | Meaning |
|---|---|---|
| `--anchor-hosts` | 3 | Frontends. Sets each engine's ingress peer count. |
| `--engines` | 2 | Token producers. Also each anchor host's ingress peer count. |
| `--requests` | 500 | Requests to serve. |
| `--max-batch` | 32 | Requests one engine holds in its active batch at once. |
| `--tokens` | 64 | Longest response, in tokens. The maximum is 4,096. |
| `--pass-delay-ms` | 1 | Gap between forward passes. |
| `--credit-sweep-interval-ms` | 2 | `MuxConfig::credit_sweep_interval`. |
| `--legacy` | off | Run on the per-stream path. |
| `--flush-policy` | `manual` | `manual` or `auto`, as in `batched_streaming`. |
| `--warmup-requests` | 0 | Leave the first N requests out of the latency histograms. |
| `--json` | off | Print one line of JSON for scripts. |

CAUTION: Set `--credit-sweep-interval-ms 200` to match velo's default. The harness default of 2 ms reproduces an earlier default and exists for the sweep A/B.

The harness reports TTFT and ITL as HDR histograms (p50, p95, p99). It also reports requests per second, tokens per wire write and process CPU. CPU is split into user and system time from `/proc/self/stat`. The consumer asserts the position of every token, so a run fails if sharing a batch ever reorders a stream.

Sweep `--engines`, not `--anchor-hosts`. The per-peer costs on a frontend scale with its ingress peers, and an anchor host's ingress peers are the engines that stream to it. `--anchor-hosts` moves the smaller side of the same product.

The harness agrees with `batched_streaming`. At `--anchor-hosts 3 --engines 2 --requests 96 --max-batch 32 --tokens 40`, it reports 5.41 tokens per write where `batched_streaming` reports 5.38.

### Limits of the in-process harness

- **One process.** The reported CPU belongs to the whole topology, not to one frontend. An A/B that changes one frontend-side setting is valid. A comparison with another system's per-frontend CPU is not.
- **Loopback.** It removes wire time, which exaggerates the syscall term in favor of the mux.
- **Size.** It runs out of memory above about 256 nodes in one process on a 374 GiB host. A 512-worker shape needs the external rig.
- **Queueing.** With `--max-batch` well below `--requests`, most of TTFT is the wait for a batch slot. Read TTFT and requests per second only as comparisons between arms.
- **Raw percentiles.** The harness reports percentiles over every recorded request after `--warmup-requests`. It does not separate steady state from the opening burst. See [Steady-state and raw percentiles](#steady-state-and-raw-percentiles).

## The external serving rig

The external rig runs velo as the response plane inside Dynamo's serving stack. It uses Dynamo's own HTTP frontend, its `mocker` engine and the `aiperf` load generator. The same rig runs Dynamo's own response planes, so only the response plane changes between arms. The rig scripts and the Dynamo adapter live outside this repository.

| Item | Value |
|---|---|
| Nodes | 2 GB200 nodes, aarch64, 144 cores each, exclusive allocation |
| Node A | Frontend on cores 0–71. `aiperf`, etcd and nats-server on cores 72–143. |
| Node B | 8 mocker processes, 64 workers each (512 workers) |
| Load | Concurrency 8,192. 250,000 requests per rep after 8,192 warm-up requests. |
| Shape | Input length 1,024, output length 256 (Qwen3-0.6B tokenizer), mocker speedup 5 |
| Reps | 3 per arm, arms interleaved, fresh processes for each rep |
| CPU per request | 1 Hz samples of `/proc/<frontend_pid>/stat` inside `aiperf`'s measurement window, divided by completed requests |

## Steady-state and raw percentiles

A closed-loop load generator at concurrency N starts its measured phase with N requests at once. `aiperf` 0.10.0 ramps request rate, not concurrency. On the external rig, each rep therefore opens with a burst of 8,192 simultaneous requests. The 8,192 warm-up requests warm caches, but the measured phase still opens with its own burst.

The burst sets the raw p99. In six reps on the external rig, every request at or above the raw TTFT p99 started within the first 0.2 s of the measured phase. That tail was 760 to 930 ms for both velo and the comparison plane. Its excess over the median came mostly from the request plane and the mocker processes absorbing 1,024 simultaneous requests each, not from the response plane.

The rig's summary therefore reports steady-state percentiles beside the raw ones. Steady state is the set of requests that started 10 s or later into the measured phase, about 200,000 of 250,000 per rep. Read verdicts from the steady-state columns. Report the raw columns too, so that a reader sees the burst.

## Measurement rules

Each rule below exists because a measurement without it was wrong.

1. **Run on an exclusive node.** A CPU A/B ran on a shared login node with about 11 other users. Its spread was up to 5.5 µs per token, against an effect of about 2 µs per token. The result was retracted. On an exclusive 144-core node, the total spread over 42 runs was 2.43 µs per token.
2. **Build both arms from one tree on the node, and interleave them.** Run the arms alternately, rep by rep, so that drift hits both. Normalize CPU per token or per request over identical work.
3. **Pin the load generator away from the system under test.** With `aiperf` unpinned on the frontend node, node A ran at 95% utilization. Its load average was 159 on 144 cores. velo's TTFT p50 read 992 ms. With the frontend and `aiperf` on disjoint cores, the same configuration read 98 ms. Pin etcd and nats-server to the load generator's cores too. Unpinned, they took 12% to 38% of the frontend's cores in profiled reps.
4. **Read timestamps in the right zone.** `aiperf` 0.10.0 writes naive local timestamps. Reading them as UTC put the CPU window after the run, on the idle tail. Every frontend CPU figure measured that way was about three times too low.
5. **Compare reps at a matched backlog draw.** On the external rig, one or more mocker processes fall behind during the opening burst and keep that backlog. The number of processes that hold the backlog (the "holders") sets throughput, ITL and end-to-end latency for every arm. Compare two arms only at the same holder count.
6. **Do not fix the arm order.** A matrix that always ran the same arm second put that arm in a degraded band in every rep. Its request-level numbers measured run position, not the setting.
7. **Change one setting per arm.** A pair that differed in two settings attributed a result to the wrong one.
8. **Assert the transport per request.** Fail any request whose negotiated key is not `messenger-mux-v1`. A silent fallback measures the per-stream path under a mux label.
9. **Record the build.** Write the velo commit and its dirty state into each rep's metadata. If the installed build and the checkout differ, fail the rep.
10. **Report latency beside throughput.** A change that adds latency can raise throughput by letting the consumer catch up. Watch TTFT in particular, because any windowed flush policy can make it worse.
11. **Prove a regression test before you trust it.** Revert the fix and make sure that the test fails. A test that passes with the fix reverted proves nothing.
