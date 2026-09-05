<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# W7: batcher-instrument hot-path cost, accepted un-measured

Written 2026-09-05, PR #80 (`w7-batcher-instruments`), pass 2 review response.

## The finding

`MuxMetricsHandle::records_sent` and `::batcher_wake`
(`lib/velo/src/observability.rs:708-728`) add unmeasured atomic traffic to the
outbound batch path. Per outbound batch this adds up to `RECORD_TYPE_COUNT`
(5) `CounterVec::with_label_values` lookups (hash + `RwLock` read + `Arc`
clone) and the same number of `Counter::inc_by` calls, each an `AtomicF64`
compare-exchange-weak retry loop in `prometheus` 0.14 (`Cargo.toml:55`) on a
counter child shared by every peer batcher on the node; plus one lookup and
one CAS per wake (`observability.rs:724-728`). On the shape that motivated this
PR (about one record per batch, one batch per wake, 1.77M batches in
rep1-velo0) that roughly doubles the per-batch label lookups and triples the
per-batch counter CAS versus the pre-existing `metrics.batch(...)` call this
PR sits beside.

This is the same mechanism `agent-docs/w0-egress-instrumentation-cost.md`
names on the egress-write path and closes with a written acceptance rather
than a measurement, so a later pass does not re-raise it as new information.
That note is this one's template — it lands in this tree with the pending
rebase onto `w0-ingest-metrics` @ `882e8ca` (the commit that added it); until
then it is reachable only as `git show 882e8ca:agent-docs/w0-egress-instrumentation-cost.md`
from a checkout that has that ref.

## Disposition: accepted, un-measured, for this PR

No A/B has been taken in this PR. Reasons:

- The A/B this would need — `examples/examples/response_plane_bench.rs`
  before/after this PR's HEAD, on the aarch64 compute node, multiple reps
  compared on means — is a benchmarking task, not a code-review fixer pass,
  and this pass cannot run `cargo` on the login node regardless.
- The counting itself is `RULINGS.md` #3-compliant by construction: the
  per-record cost is one bounds-checked `u16` increment inside
  `BatchEncoder::push`'s single funnel, no allocation, no lock, no label
  lookup. The label lookups this note is about are per *batch* (at most 5
  per batch, only for record types actually present — `count > 0` gates the
  lookup), which #3 explicitly permits and which the pre-existing
  `metrics.batch(...)` already does at the same call site.
- ARM64 LSE makes the plain `fetch_add`s in the counter/bucket path cheap;
  the genuine contention risk is the `AtomicF64` CAS retry loop inside
  `Counter::inc_by`, which is exactly the kind of thing a measurement
  settles and reading does not.

One caveat worth recording either way: `batcher_wake`'s lookup is per-wake,
not per-batch, so under `FlushPolicy::Manual` or a starved slot a wake can
fire with no batch behind it — that one series' lookup rate is not bounded
by the batch rate. Under the default `Auto { on_admission: true }`, wakes
are approximately batches.

## What to do before this cost is spent for real

If a future pass (or the orchestrator) decides the batcher-instrument arm
needs measuring before merge, run the A/B named above — one rep, means not
percentiles, recorded in `agent-docs/response-plane-benchmark-results.md`
against the existing baseline — before shipping these instruments enabled
by default.

If it regresses, the cheapest lever is pre-binding `records_sent_total`'s
five children in `bind_mux` via `std::array::from_fn` +
`RecordType::from_u8`, the same pattern `bind_transport` already uses for
`TransportMetricsHandle` (`observability.rs:1637-1661`) — that collapses
`records_sent`'s per-batch lookups to zero, leaving only the `inc_by` CAS.

This note closes the loop so a later pass does not re-raise the same
un-measured claim as new information; it does not claim the cost is
acceptable, only that it is accepted, unmeasured, for this PR.

## Addendum, 2026-09-05: the pre-bind lever above was pulled

Pass 3 review found the caveat two paragraphs up — "`batcher_wake`'s lookup is
per-wake, not per-batch" — was not a caveat on an otherwise-compliant design;
it was the actual shape of the cost. `Work::Slot(_, SlotItem::Frame(_))` is
one `select!` wake per item pulled off `SlotStream::poll_next`
(`slot_stream.rs`), which yields exactly one queued record per poll. So a
`Frame` wake's `CounterVec::with_label_values` lookup is on the per-record
path *by construction*, not only in the low-batch-occupancy regime this note
measured against — the "wakes are approximately batches" framing above
undersold it. That is a `RULINGS.md` #3 violation ("no label lookup on the
per-message path"), not a workload-dependent cost to accept unmeasured.

`records_sent` and `batcher_wake` are both now pre-bound in `bind_mux` —
`records_sent` into a `[Counter; RECORD_TYPE_COUNT]` array indexed by
`RecordType::count_index`, `batcher_wake` into a `[Counter; 5]` array indexed
by `BatcherWake::index` — the same pattern `bind_transport` already used for
`TransportMetricsHandle`. Both hot-path methods are now a plain array index
and an atomic add; no `CounterVec` lookup remains on either path. The A/B
this note called for is accordingly moot for the label-lookup cost; the
`AtomicF64` CAS these counters still pay is unchanged and was never what
this note was about.
