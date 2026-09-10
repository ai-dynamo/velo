<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# W7: `reply_linger` batch-count reduction, corrected arm pair

Written 2026-09-05, PR #81 (`w7-reply-linger`), pass-3 review response.

## The mistake this replaces

The `MuxConfig::reply_linger` rustdoc and its `BATCHING.md` mirror originally
cited the tier-3 rig arm pair `velo3n` vs `velo3f` and called it isolated to
this knob. It is not. `.research/rig/t3-frontend.sh` sets:

- `velo3n`: `FLUSH_INTERVAL_US=0`, `REPLY_LINGER_US=0`
- `velo3f`: `FLUSH_INTERVAL_US=500`, `REPLY_LINGER_US` unset (default, 1000)

Two knobs differ, not one, and under this PR's own `FlushGate::deadline`
(`min` of the reply due time and the policy due time) `velo3f`'s replies-only
batches are actually due at the 500 us *policy* window, not the 1 ms reply
window — so `reply_linger` never even binds in that arm. The rig script's own
header comment says as much: "the pair velo3/velo3n attributes the linger."

The correct pair — the one that differs in `REPLY_LINGER_US` alone, everything
else held fixed — is `velo3` (reply_linger default, 1 ms) vs `velo3n`
(reply_linger forced to 0, the pre-knob urgent-flush behaviour).

## Corrected numbers

Source: `.research/results/t3-iso3`, 3 reps, `velo_streaming_mux_batches_total{direction="sent"}`
and `velo_streaming_mux_records_sent_total{record_type="credit_update"}` from
each rep's `prometheus/final_snapshot.txt`. Neither series appears in any
rep's `initial_snapshot.txt`, so the counters start at zero and the final
values are the deltas directly — that absence is load-bearing for the
arithmetic below, not an oversight.

| rep | velo3 sent | velo3n sent | ratio (velo3n/velo3) | velo3 CreditUpdate | velo3n CreditUpdate |
|---|---|---|---|---|---|
| 1 | 275,475 | 1,612,588 | 5.85x | 28,800,083 | 34,077,506 |
| 2 | 368,507 | 1,581,970 | 4.29x | 35,406,747 | 34,607,455 |
| 3 | 300,139 | 1,691,693 | 5.64x | 29,917,693 | 33,644,707 |

`reply_linger` at its 1 ms default (the `velo3` arm) cuts outbound batches
**4.3x-5.9x** versus the pre-knob urgent flush (`velo3n`), on a receiver whose
egress is otherwise idle (zero-RTT attach on both arms, so nothing else in
the path is waiting behind a round trip).

Records per batch (`CreditUpdate` total / batches sent) runs `velo3n` ~20-22,
`velo3` ~96-105 — **4.4x-5.0x** per rep (4.95x, 4.39x, 5.01x), **not the same
ratio as the 5.85x/4.29x/5.64x batch-count reduction above**, because the two arms did not
carry equal total `CreditUpdate` volume (28.8-35.4M vs 33.6-34.6M). That
volume gap is itself a run-order artifact, not a property of the window —
see the addendum below. Both figures are real; they just answer different
questions ("how many batches" vs "how full is each one") and should not be
read as cross-checks of each other.

## Disposition

`mod.rs`'s `MuxConfig::reply_linger` doc and `BATCHING.md`'s mirror now cite
this file instead of `.research/results/t3-iso3` directly — that path is
untracked (`git log --all -- .research` is empty, no `.gitignore` entry
either), so it is unresolvable for any consumer of this repo who is not the
author with the run still on disk. This document is the citable form.

## Addendum, 2026-09-05 (pass-5 fixer, PR #81): the arm pair is confounded by run order

The corrected pair above is still the right pair (`REPLY_LINGER_US` is the
only knob that differs), but the 15-run `t3-iso3` matrix runs every rep in
the fixed order `velo3, velo3n, velo3f, velo34, mux18p`, and `velo3n` — the
`reply_linger`-off arm — sits at position 2 in every rep. The 15 runs split
into two disjoint performance bands (rps < 2500, `ttft_p95` 203-231 ms,
`itl_p99` 99-107 ms, duration 105-111 s vs. everything else), and `velo3n` is
in the degraded band in all three reps: rep1 `velo3n`+`mux18p`, rep2
`velo3`+`velo3n`, rep3 `velo3n`+`velo34`. Every request-level number in the
table above (and in `MuxConfig::reply_linger`'s rustdoc) tracks band
membership, not the knob:

- `frontend_cpu_ms_per_req` looked like a clean separation by arm (`velo3`
  9.516-9.905, `velo3n` 10.008-10.290, non-overlapping) until conditioned on
  band: mux-enabled normal-band runs are 9.052-9.870 and degraded-band runs
  are 9.879-10.290 — the two ranges nearly meet, and the degraded band holds
  two `reply_linger`-on runs (`velo34` at 9.879, `velo3` at 9.905).
- `CreditUpdate`/request is a band quantity, not an arm quantity: normal-band
  runs (n=7, spanning `velo3`, `velo3f`, `velo34`) are 104.7-126.4, degraded
  runs (n=5) are 134.6-141.6, and the two ranges are disjoint. `rep2-velo3`
  (141.6, degraded) sits above every `velo3n` run (134.6-138.4) despite being
  the `reply_linger`-on arm — the opposite of what the batches-per-record
  argument two sections up would predict, because rep2 put `velo3` in the
  degraded band and `velo3n` in the same band right behind it.
- `rep2-velo3` (2253.8 rps) is slower than all three `velo3n` runs
  (2314-2353 rps) with a worse `itl_p99` (106.7 ms vs. 101.5-102.3 ms) —
  `reply_linger` on, throughput worse. That is band, not the window making
  things worse.

**No request-level number in this document — throughput, TTFT, ITL, or
frontend CPU — is established by these three reps.** They are not
independent of run position.

What survives conditioning, because it comes from the receiver's own
transport instrumentation rather than the frontend's request path, is
disjoint against all nine `reply_linger`-on runs (`velo3`, `velo3f`, `velo34`
× 3 reps; `mux18p` runs a different real plane with no `velo_streaming_mux_*`
series and is excluded) versus the three `velo3n` runs, in both bands at
once:

| Series (`_sum`, `transport="tcp"`) | `reply_linger` on (9 runs) | `reply_linger` off, `velo3n` (3 runs) |
|---|---|---|
| `velo_transport_write_duration_seconds` | 2.82-3.88 s | 13.52-14.30 s |
| `velo_transport_egress_queue_wait_seconds` | 3.53-6.56 s | 25.82-27.44 s |

Both series read 0 in every run's `initial_snapshot.txt`, so the final values
above are the deltas directly, the same load-bearing absence-of-registration
argument as the batch-count table. Comparing every pair of runs across the
two groups: roughly 3.5x-5.1x less time in `write_duration` and roughly
3.9x-7.8x less in `egress_queue_wait`, disjoint across every run regardless
of band — this is the evidence `MuxConfig::reply_linger`'s rustdoc now cites
instead of the confounded request-level framing.

**Provenance.** Every run in `t3-iso3` carries `velo_sha =
4afb407e6dedbe139c4b523f71c457f56421a82f`, `velo_dirty = 21`
(`rig_run_meta.json`). That commit is `integration/response-plane-wheel`
("integrate w7-reply-linger @ 221d049"): it contains 221d049 but not 0e50639,
the pass-1 commit that restructured `FlushGate`'s state machine. That gap
does not invalidate the numbers here: the frontend's `records_sent_total` in
these runs carries only `credit_update` (no `data` or `close_slot` record
ever joined a batch in this workload), so the mixed-batch branches 0e50639
touched were never exercised. The measured tree's `MuxConfig` also carries an
`async_open_ack` field this branch does not have, but both arms of the pair
(`velo3` and `velo3n`) set it identically (`zero_rtt_attach=1`,
`async_open_ack=0` for both, per `t3-frontend.sh`'s arm table), so the A/B
comparison itself stays clean.
