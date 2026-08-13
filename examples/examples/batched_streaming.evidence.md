<!--
SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# `batched_streaming` — measured results

The numbers quoted in `batched_streaming.rs` and in
`lib/velo/src/streaming/BATCHING.md` § "Flush policy", with the commands that
produced them and their unedited output. Regenerate this file rather than
editing it: every figure here is reproducible in under a minute, and a quoted
number nobody can reproduce is worse than no number.

## Provenance

| | |
|---|---|
| Revision | `20adb354342d344228b0f231e55bffa9a7d868b9` |
| Machine | 20-core arm64 (10x Cortex-X925 + 10x Cortex-A725), 120 GiB RAM |
| Kernel | Linux 6.14.0-1015-nvidia |
| Toolchain | rustc 1.93.1 (01f6ddf75 2026-02-11) |
| Build | `--release --all-features` |

**One developer machine, loopback TCP, five nodes in one process.** Loopback
removes wire time, which exaggerates the syscall term — that cuts *for* the mux,
and `BATCHING.md` § V5 says so. What this file is evidence for is the
*difference between flush policies*, which is measured on one machine against
itself and is not sensitive to that.

Every command is run from `examples/`:

```
cargo run --release --all-features --example batched_streaming -- <args>
```

## Summary

| Configuration | legacy | `--flush-policy auto` | `--flush-policy manual` |
|---|---|---|---|
| 24 requests, max-batch 8 (defaults) | 0.99–1.00 | 2.18–2.19 | **2.19 every run** |
| 96 requests, max-batch 32 | — | 4.67–5.14 | **5.38 every run** |
| 24 requests, max-batch 8, `--pass-delay-ms 0` | — | 6.61–7.44 | 3.47–4.41 |

Tokens per wire write, higher is better. Three things to read out of it:

1. **At the defaults the two policies agree.** With a millisecond between
   passes the batcher always keeps up, so writing at every wake and writing once
   per pass are the same writes. The flush policy is not a throughput knob here.
2. **At serving depth `manual` is higher, and identical every run.** `auto`
   spreads 4.67–5.14 because a batcher writing at every wake sometimes wakes
   mid-pass and writes half of one; `manual` writes exactly the pass, so what a
   batch holds stops depending on how the runtime scheduled the batcher against
   the engine.
3. **With the engine running flat out, `auto` packs far harder.** At
   `--pass-delay-ms 0` the engine outruns the batcher and a batch absorbs the
   pass behind it — 6.61–7.44 against manual's 3.47–4.41. That surplus is
   throughput bought with per-token latency, which is the trade a decode engine
   does not want, and it is the honest cost of choosing `manual`.


## Raw output

### Defaults, legacy — `--engines 2 --requests 24 --max-batch 8 --tokens 40 --legacy`

```text
batched_streaming: 3 anchor hosts, 2 engine(s), 24 requests, max-batch 8, 476 tokens, 1ms between passes
mode: legacy (one TCP connection per stream)
[engine 0] first attach negotiated tcp-stream
[engine 1] first attach negotiated tcp-stream

=== engines (legacy (one TCP connection per stream)) ===
engine      reqs   tokens  passes mean batch  hosts     egress flushes tokens/write
0             12      216      45      4.80      3                217         1.00
1             12      260      48      5.42      3                260         1.00

=== hosts ===
host      completed   tokens  terminals
0                 8      116          8
1                 8      172          8
2                 8      188          8

=== totals ===
  mode                 legacy (one TCP connection per stream)
  tokens streamed      476
  forward passes       93
  wire writes          477  (egress flushes)
  tokens per write     1.00 : 1
  active per host      1.71  (mean batch 5.12 over 3 hosts)
  elapsed              114 ms

477 writes for 476 tokens — 1.00 : 1. BATCHING.md's own ratio,
frames_written / egress_flushes, reads 1.05 : 1 here, higher only because it counts
the heartbeats and terminals that a token count does not. Either way it is the
limitation that document measures rather than a failure: a forward pass puts one
frame on each of many different streams, and per-stream coalescing can only pack
frames queued on the same one. Run without --legacy to bucket them by destination.
```

### Defaults, auto — `--engines 2 --requests 24 --max-batch 8 --tokens 40 --flush-policy auto`

```text
batched_streaming: 3 anchor hosts, 2 engine(s), 24 requests, max-batch 8, 476 tokens, 1ms between passes
mode: mux, flush-policy auto
[engine 1] first attach negotiated messenger-mux-v1
[engine 0] first attach negotiated messenger-mux-v1

=== engines (mux, flush-policy auto) ===
engine      reqs   tokens  passes mean batch  hosts  _stream_batch AMs tokens/write
0             12      216      45      4.80      3                 93         2.32
1             12      260      48      5.42      3                124         2.10

=== hosts ===
host      completed   tokens  terminals
0                 8      116          8
1                 8      172          8
2                 8      188          8

=== totals ===
  mode                 mux, flush-policy auto
  tokens streamed      476
  forward passes       93
  wire writes          217  (_stream_batch AMs)
  tokens per write     2.19 : 1
  active per host      1.71  (mean batch 5.12 over 3 hosts)
  elapsed              101 ms

476 tokens crossed the wire in 217 active messages — 2.19 tokens per write, and
the per-stream TCP path wrote 0 times because no stream ever dialled it. Every token
still arrived in order on its own stream: the batching is on the destination axis, not
the stream axis, so it packs a forward pass that per-stream coalescing cannot touch.

The batcher wrote whenever the last batch was admitted, with nobody telling it to.
That number moves run to run, because what lands in a batch depends on how the
runtime scheduled the batcher against the engine: it can wake mid-pass and write half
of one, or fall behind and pack the pass after it. Try --pass-delay-ms 0 to see the
second effect at its strongest — packing across passes is throughput bought with
per-token latency. Run --flush-policy manual for one write per pass instead.

(active per host, 1.71, averages over all 3 hosts including the ones a
given pass had nothing for, so it reads a little under the tokens each written batch
actually carried.)
Run with --legacy for the same workload at one write per token.
```

### Defaults, manual — `--engines 2 --requests 24 --max-batch 8 --tokens 40 --flush-policy manual`

```text
batched_streaming: 3 anchor hosts, 2 engine(s), 24 requests, max-batch 8, 476 tokens, 1ms between passes
mode: mux, flush-policy manual
[engine 1] first attach negotiated messenger-mux-v1
[engine 0] first attach negotiated messenger-mux-v1

=== engines (mux, flush-policy manual) ===
engine      reqs   tokens  passes mean batch  hosts  _stream_batch AMs tokens/write
0             12      216      45      4.80      3                 93         2.32
1             12      260      48      5.42      3                124         2.10

=== hosts ===
host      completed   tokens  terminals
0                 8      116          8
1                 8      172          8
2                 8      188          8

=== totals ===
  mode                 mux, flush-policy manual
  tokens streamed      476
  forward passes       93
  wire writes          217  (_stream_batch AMs)
  tokens per write     2.19 : 1
  active per host      1.71  (mean batch 5.12 over 3 hosts)
  elapsed              99 ms

476 tokens crossed the wire in 217 active messages — 2.19 tokens per write, and
the per-stream TCP path wrote 0 times because no stream ever dialled it. Every token
still arrived in order on its own stream: the batching is on the destination axis, not
the stream axis, so it packs a forward pass that per-stream coalescing cannot touch.

The engine wrote each pass itself: flush_batch() after the last send, one batch to
each host it touched, carrying that host's whole share of the pass. Run it again and
the ratio comes out the same — what a batch holds is a property of the deployment,
not of how the runtime scheduled the batcher against the engine. No token waits for
the pass behind it, which is the part that matters for time-to-next-token.
Run --flush-policy auto to let the batcher decide instead, and watch the number move.

(active per host, 1.71, averages over all 3 hosts including the ones a
given pass had nothing for, so it reads a little under the tokens each written batch
actually carried.)
Run with --legacy for the same workload at one write per token.
```

### Depth, auto — `--engines 2 --requests 96 --max-batch 32 --tokens 40 --flush-policy auto`

```text
batched_streaming: 3 anchor hosts, 2 engine(s), 96 requests, max-batch 32, 1936 tokens, 1ms between passes
mode: mux, flush-policy auto
[engine 0] first attach negotiated messenger-mux-v1
[engine 1] first attach negotiated messenger-mux-v1

=== engines (mux, flush-policy auto) ===
engine      reqs   tokens  passes mean batch  hosts  _stream_batch AMs tokens/write
0             48      920      45     20.44      3                188         4.89
1             48     1016      52     19.54      3                196         5.18

=== hosts ===
host      completed   tokens  terminals
0                32      608         32
1                32      672         32
2                32      656         32

=== totals ===
  mode                 mux, flush-policy auto
  tokens streamed      1936
  forward passes       97
  wire writes          384  (_stream_batch AMs)
  tokens per write     5.04 : 1
  active per host      6.65  (mean batch 19.96 over 3 hosts)
  elapsed              116 ms

1936 tokens crossed the wire in 384 active messages — 5.04 tokens per write, and
the per-stream TCP path wrote 0 times because no stream ever dialled it. Every token
still arrived in order on its own stream: the batching is on the destination axis, not
the stream axis, so it packs a forward pass that per-stream coalescing cannot touch.

The batcher wrote whenever the last batch was admitted, with nobody telling it to.
That number moves run to run, because what lands in a batch depends on how the
runtime scheduled the batcher against the engine: it can wake mid-pass and write half
of one, or fall behind and pack the pass after it. Try --pass-delay-ms 0 to see the
second effect at its strongest — packing across passes is throughput bought with
per-token latency. Run --flush-policy manual for one write per pass instead.

(active per host, 6.65, averages over all 3 hosts including the ones a
given pass had nothing for, so it reads a little under the tokens each written batch
actually carried.)
Run with --legacy for the same workload at one write per token.
```

### Depth, manual — `--engines 2 --requests 96 --max-batch 32 --tokens 40 --flush-policy manual`

```text
batched_streaming: 3 anchor hosts, 2 engine(s), 96 requests, max-batch 32, 1936 tokens, 1ms between passes
mode: mux, flush-policy manual
[engine 0] first attach negotiated messenger-mux-v1
[engine 1] first attach negotiated messenger-mux-v1

=== engines (mux, flush-policy manual) ===
engine      reqs   tokens  passes mean batch  hosts  _stream_batch AMs tokens/write
0             48      920      45     20.44      3                176         5.23
1             48     1016      52     19.54      3                184         5.52

=== hosts ===
host      completed   tokens  terminals
0                32      608         32
1                32      672         32
2                32      656         32

=== totals ===
  mode                 mux, flush-policy manual
  tokens streamed      1936
  forward passes       97
  wire writes          360  (_stream_batch AMs)
  tokens per write     5.38 : 1
  active per host      6.65  (mean batch 19.96 over 3 hosts)
  elapsed              114 ms

1936 tokens crossed the wire in 360 active messages — 5.38 tokens per write, and
the per-stream TCP path wrote 0 times because no stream ever dialled it. Every token
still arrived in order on its own stream: the batching is on the destination axis, not
the stream axis, so it packs a forward pass that per-stream coalescing cannot touch.

The engine wrote each pass itself: flush_batch() after the last send, one batch to
each host it touched, carrying that host's whole share of the pass. Run it again and
the ratio comes out the same — what a batch holds is a property of the deployment,
not of how the runtime scheduled the batcher against the engine. No token waits for
the pass behind it, which is the part that matters for time-to-next-token.
Run --flush-policy auto to let the batcher decide instead, and watch the number move.

(active per host, 6.65, averages over all 3 hosts including the ones a
given pass had nothing for, so it reads a little under the tokens each written batch
actually carried.)
Run with --legacy for the same workload at one write per token.
```

## Repeat runs

Five runs per mux policy — three for the legacy control, whose 0.99–1.00 has no
variance worth chasing — reporting `tokens per write`. This is where the
determinism claim lives: `manual` repeats exactly, `auto` does not.

**96 requests, max-batch 32, `--flush-policy auto`**

```text
4.88  5.08  5.08  5.14  4.67
```

**96 requests, max-batch 32, `--flush-policy manual`**

```text
5.38  5.38  5.38  5.38  5.38
```

**24 requests, max-batch 8, `--flush-policy auto`**

```text
2.18  2.18  2.19  2.19  2.19
```

**24 requests, max-batch 8, `--flush-policy manual`**

```text
2.19  2.19  2.19  2.19  2.19
```

**24 requests, max-batch 8, `--pass-delay-ms 0 --flush-policy auto`**

```text
7.00  7.44  6.61  7.32  7.32
```

**24 requests, max-batch 8, `--pass-delay-ms 0 --flush-policy manual`**

```text
3.97  3.47  3.75  4.29  4.41
```

**24 requests, max-batch 8, `--legacy`**

```text
0.99  1.00  1.00
```
