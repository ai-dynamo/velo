<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# W0: egress-instrument hot-path cost, accepted un-measured

Written 2026-09-05, PR #77 (`w0-ingest-metrics`), pass 3 review response.

## The finding

`EgressLog::dequeued` / `staged` / `started` / `written`
(`lib/velo/src/transports/coalesce/mod.rs:177-197`) add contended atomic
traffic to the exact outbound path W0 exists to measure. Per outbound frame on
TCP/UDS this PR adds: one `Instant::now()` at send
(`tcp/transport.rs:385`), one `Instant::elapsed()` plus one histogram observe
(4 atomic RMWs in `prometheus` 0.14's `HistogramCore::observe`) at dequeue, and
per write a second observe plus up to five `Counter` compare-exchange loops
(`AtomicF64::inc_by`).

This was raised in pass 1 (finding 5) and pass 2 (finding 2) and deferred both
times pending a measurement neither pass ran. Pass 3 confirmed the mechanism
from the `prometheus` 0.14 source rather than intuition, but still ran no A/B.

## Disposition: accepted, un-measured, for this PR

No A/B has been taken in this PR either. Reasons:

- The A/B this and prior passes specify —
  `examples/examples/response_plane_bench.rs` at `cd8c076^` versus `HEAD`, an
  egress-heavy arm, on the aarch64 compute node — is a multi-rep benchmarking
  task, not a code-review gate fix. It does not belong in a fixer pass that
  also cannot run `cargo` on the login node.
- W0's own goal (`RULINGS.md`) is to split the ~1.1 s TTFT backlog using the
  *inbound*-queue and attach-RTT instruments (rulings 1, 2, 5, 7), neither of
  which is on this egress path. The egress instruments
  (`velo_transport_frames_written_total`,
  `velo_transport_egress_queue_wait_seconds`,
  `velo_transport_write_duration_seconds`) exist for a *different* reading —
  the egress-queue-depth subtraction documented in `README.md` — and are not
  load-bearing for W0's own exit criterion (ruling 13).
- ARM64 LSE makes the plain `fetch_add`s in the counter/bucket path cheap;
  the one genuine contention risk is the `AtomicF64` compare-exchange-weak
  retry loop inside `Counter::inc_by` / `Histogram::observe`'s sum field,
  which is exactly the kind of thing a measurement settles and reading does
  not.

## What to do before this cost is spent for real

If a future pass (or the orchestrator) decides the egress arm needs
measuring before merge, run the A/B named above — one rep, means not
percentiles, recorded in `agent-docs/response-plane-benchmark-results.md`
against the existing baseline — before shipping the egress instruments
enabled by default.

If it regresses: the cheapest lever is observing
`egress_queue_wait`/`write_duration` once per *write* on the batch's oldest
frame instead of once per frame in the batch. That drops the per-frame
`Instant::elapsed()` and histogram observe on every frame coalesced into a
batch, keeping one observation per `write_all` instead of one per frame.

This note closes the loop so a fourth pass does not re-raise the same
un-measured claim as new information; it does not claim the cost is
acceptable, only that it is accepted, unmeasured, for this PR.
