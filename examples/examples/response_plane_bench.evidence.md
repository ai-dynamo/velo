<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# `response_plane_bench` — measured results

What this harness was built to settle, and what it actually found. Regenerate rather than edit: every number here comes from a script in `.research/` and is reproducible in a few minutes.

## Provenance

> ## RETRACTED, 2026-09-01 — and the replacement is a negative result
>
> **Every number in the original body below was measured on a shared login node
> with about eleven other users on it.** A paired re-measurement showed
> within-cell spread up to 5.5 us/token against a ~2 us/token effect: the noise
> was as large as the signal. Those numbers are withdrawn, not revised.
>
> The clean replacement is in **"Exclusive-node measurement"** immediately
> below. It does not reproduce the effect. Read that section, not the body.
>
> The failure is worth keeping because nothing about the rig was wrong — the
> harness reproduces `batched_streaming`'s ratio to within noise, the workload
> shape is right, and CPU-per-token over identical token counts is a sound
> normalization. It was still worthless because of *where it ran*.

---

## Exclusive-node measurement, 2026-09-01

`srun --exclusive`, one dedicated 144-core aarch64 node, no other tenants. Both
arms **built from the same tree on that node** — `before` is the same tree with
`git checkout -- lib/velo/src/streaming/` applied — and **interleaved run by
run**, so any residual drift hits both equally. 7 reps per cell, 42 runs, one
warmup per binary discarded. Script `.research/gap1-node.sh`, raw data
`.research/gap1-node.jsonl`.

Total spread across all 42 runs was **2.43 us/token**, against 6.97 on the
shared node — so the rig is now quiet enough to see a 2 us effect if one exists.

### Result 1: the sweep interval costs far less than claimed

Median CPU per token, relative to a 500 ms sweep, unmodified velo:

| 2 ms | 200 ms | 500 ms |
|---|---|---|
| **1.075x** | 1.000x | 1.000x |

The original claim was **1.21x**. The clean number is **1.075x** — about a third
of it. The direction survives; the magnitude does not.

### Result 2: the drain hook costs a little and buys nothing measurable

Two independent 42-run jobs, each on its own exclusive node. The second was run
after fixing a starvation bug in the hook's own `select!` — the ticker arm was
polled first under `biased`, so at short intervals the drain arm rarely ran. The
fix did not change the answer:

| run | median delta (after − before) | after worse in |
|---|---|---|
| A/B #1 (starved drain arm) | +0.30 us/token | 16/21 pairs |
| A/B #2 (fixed) | +0.30 us/token | 18/21 pairs |
| **combined** | **+0.30 us/token** | **34/42 pairs** |

34 of 42 paired runs worse is a consistent signal, and it reproduced across two
jobs and an implementation fix in between. The hook costs roughly 0.3 us/token —
about 2% — and no arrangement of the sweep interval turns that into a win.

### Result 3: the decision comparison is a small improvement, from the interval

Old default (`before` @ 2 ms — the only *safe* setting before this change, since
the sweep was then the only path returning credit to a quiet peer) against new
default (`after` @ 200 ms, safe only because of the hook):

| | old | new | delta |
|---|---|---|---|
| CPU per token | 13.34 us | 12.73 us | **−4.5%** |
| req/s | 4308 | 4332 | +0.6% |

The −4.5% is real but it is **the relaxed interval paying, not the hook**.
Unmodified velo at 200 ms is *cheaper still* (12.13 us) — it simply cannot be
run that way, because credit would never come back to a quiet peer.

So the honest accounting is: relaxing the sweep from 2 ms to 200 ms is worth
about 7.5% of CPU per token; the hook is what makes that relaxation safe; and
the hook itself costs about 2% back. Net, roughly 4.5%.

### What this establishes

- Drain-driven credit return is **worth about 4.5% of CPU per token** at 256
  ingress peers, and every bit of that comes from being able to relax the sweep
  interval. The hook itself is a ~2% cost that buys the safety to do so.
- That is a real but modest win, an order of magnitude below the 21% originally
  claimed, and it is **not** the "2 million wasted slot visits" story. Judge the
  change on correctness first and on this number second.
- The **correctness** case is untouched and rests on tests, not timings: with
  the sweep unreachable a draining consumer's stream dies at frame 4 of a
  4-credit window before the change, and completes after it.
- Whether the 200 ms default is worth keeping is now a **latency-vs-safety**
  question, not a CPU one.
- Gap 1 as originally written — "2 million no-op slot visits per second cost
  21% CPU" — is **not supported**. Something makes 2 ms cost ~7.5% more than
  500 ms, which at 256 peers is real but small.

| | |
|---|---|
| Machine | 128-core x86_64 login node, 374 GiB RAM — **shared, ~11 other users** |
| Kernel | Linux 6.8.0-137-generic |
| Build | `--release`, examples crate default features |
| Scripts | `.research/gap1-sweep.sh` (scan), `.research/gap1-confirm.sh` (confirmation) |
| Raw data | `.research/gap1-results.jsonl`, `.research/gap1-confirm.jsonl` |

## The question

`agent-docs/dynamo-response-plane-competitive-plan.md` claimed, from reading code, that velo's mux credit sweep walks every slot of every ingress peer at 500 Hz and that this costs real CPU at Dynamo's operating point. The claim was never measured. An attempt with `batched_streaming` failed structurally: that example is fixed at three anchor hosts, and at three peers the sweep is free.

This harness scales the axis that matters. An anchor host's ingress peer count is **the number of engines streaming to it**, so `--engines` is the axis, and `--credit-sweep-interval-ms` is the A/B.

## Validation against the known-good example

At `--anchor-hosts 3 --engines 2 --requests 96 --max-batch 32 --tokens 40`, this harness reports **5.41 : 1** tokens per wire write where `batched_streaming` reports **5.38 : 1** at the same configuration. Same measurement, within run-to-run noise. The harness is not measuring something else.

## Result (RETRACTED — see the banner above): the sweep appeared to cost 21% of CPU at 256 ingress peers

Five repetitions per cell, 2000 requests, 32-token responses, max-batch 8, 2 anchor hosts, 256 engines. Every arm streamed **exactly 32,984 tokens**, so CPU per token is a clean normalization and the "shorter run accrues less CPU" confound does not apply.

| sweep interval | CPU (s) | CPU/token (µs) | vs baseline | elapsed (ms) | req/s | TTFT p99 (µs) |
|---|---|---|---|---|---|---|
| **2 ms (velo default)** | 0.400 | 12.13 | **1.212×** | 552 | 3620 | 491,007 |
| 10 ms | 0.360 | 10.91 | 1.091× | 445 | 4491 | 381,695 |
| 50 ms | 0.370 | 11.22 | 1.121× | 474 | 4213 | 398,591 |
| 500 ms (effectively off) | 0.330 | 10.01 | 1.000× | 406 | 4920 | 341,247 |

Reading it out: at 256 ingress peers, velo's default 2 ms sweep costs **+21% CPU per token, +36% wall time, −26% throughput and +44% TTFT p99** against a sweep that is effectively switched off. Nothing else changed between the arms.

## It only appears above ~64 peers

Three repetitions per cell, same workload, CPU per token in µs:

| ingress peers | 2 ms | 500 ms | ratio |
|---|---|---|---|
| 2 | 8.79 | 8.19 | 1.074× |
| 16 | 8.19 | 7.58 | 1.080× |
| 64 | 9.40 | 8.79 | 1.069× |
| 128 | 10.91 | 9.70 | 1.125× |
| 256 | 16.98 | 11.22 | 1.514× |

Flat at roughly 7–8% — the noise floor of this rig — through 64 peers, then it departs: 12.5% at 128 and sharply more at 256. That is the `O(peers × slots)` shape the claim predicted.

**Dynamo's published rig is 512 workers against 2 frontends, which is 256 ingress peers per frontend.** The measurement lands on their operating point rather than near it, which is worth saying plainly and also worth being suspicious of — see the caveats.

## Caveats, including one that cuts against the headline

**The first scan over-read the effect.** Three repetitions put 256 peers at 1.51×; five repetitions put it at 1.21×. The 1.51× was one unlucky run in a three-sample median. **21% is the number to quote**, and the 1.51× row above is retained only to show the scaling shape, not as a magnitude. If a decision rests on this, run more repetitions first.

**One process, loopback.** Reported CPU is the whole topology's, not one frontend's. That is sound for this A/B, where only a frontend-side parameter moves, and it is not sound for comparing against Dynamo's 36.56 ms/req, which is a figure for a frontend process alone. Do not put those two numbers in the same column.

**The harness OOMs above ~256 nodes.** 384 engines in one process is killed on a 374 GiB box. Reaching Dynamo's full 512-per-frontend shape needs the multi-process extension, which is not built.

**Absolute latencies here are queueing-dominated** — max-batch 8 against 2000 requests means most of TTFT is waiting for a batch slot. Treat TTFT and req/s as *relative* comparisons between arms, not as figures comparable to a real serving stack.

**Longer sweeps are not free.** The sweep is the backstop that un-parks a slot which ran out of credit with nothing further arriving, and batcher eviction free-rides on it. Raising the interval to 500 ms is a measurement instrument, not a proposed default. The fix the plan recommends is drain-driven credit return, which removes the need for the sweep to be the primary mechanism at all; the interval can then be relaxed without giving up the backstop.

## What this does and does not establish

Established: gap 1 is real, it is worth roughly a fifth of CPU per token and a quarter of throughput at 256 ingress peers, and it is invisible below about 64. The optimisation work has a measured justification.

Not established: gaps 2 and 3 from the plan (per-stream tokio tasks and the extra runqueue hop). Neither has an A/B knob, so neither was measured here. They need either a code change to A/B against or a profiler, and until then they remain arithmetic.
