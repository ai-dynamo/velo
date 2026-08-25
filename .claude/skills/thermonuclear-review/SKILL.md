---
name: thermonuclear-review
description: Maximum-intensity multi-agent review of a velo diff/branch before it becomes a PR. Fans out repo-specific finder dimensions (correctness, concurrency, FFI/unsafe, wire-compat, semver boundary, shutdown contract), adversarially verifies every finding, loops until dry, then reports confirmed findings ranked by severity. Use after each implementation phase, or when the user says "thermonuclear review".
---

# Thermonuclear Review

A review harness for velo changes that assumes the diff is guilty until proven
innocent. It is *not* a style pass — it exists to catch the bug classes that
have actually shipped breakage in this repo (dual-crate semver skew, drain
races, FFI lifetime bugs) plus the ones RDMA work newly introduces.

Run it on: the current branch's diff against `origin/main` (default), or an
explicit PR number / commit range given in the arguments.

## Stage 0 — Mechanical gates (run first, fail fast)

Run these directly (not via agents). Any failure is a finding of severity
`blocker` and the review continues so the report is complete:

```
cargo fmt --check
cargo clippy --all-features --no-deps --all-targets -- -D warnings
cargo machete
scripts/check-semver.sh        # if the diff touches lib/velo-ext or lib/velo
cargo test --all-features --all-targets   # note: nats/etcd tests need local servers; skip-and-report if absent
```

Also verify the boundary invariant when `lib/velo-ext` is touched:
`cargo tree -p velo-ext | grep -c prometheus` must print `0`.

## Stage 1 — Fan-out (Workflow tool, sonnet finders)

Launch a Workflow. One finder agent per dimension, each given the diff (have
each agent run `git diff origin/main...HEAD` itself plus read full files it
flags). Dimensions — drop any that cannot apply to the diff, add ad-hoc ones
the diff suggests:

1. **correctness** — logic errors, off-by-one, error paths, races between
   check and use, lost wakeups, TOCTOU on DashMap entries.
2. **concurrency & atomics** — Ordering choices (this repo documents SeqCst
   litmus reasoning in the shutdown module — hold new code to that bar),
   lock-across-await, progress-thread ownership violations (any `ucp_*` worker
   call off the progress thread is a finding), Doorbell arm/disarm races.
3. **unsafe & FFI** — every `unsafe` block gets a SAFETY comment audit: is the
   stated invariant actually upheld at every call site? Completion-owned Arc
   discipline (exactly one reclaim per posted op, all three `*_nbx` exits),
   callback unwind guards, pointer lifetimes into UCX (who holds the buffer
   until completion? who frees requests?), use-after-unmap/dereg windows.
4. **wire & protocol compatibility** — rendezvous protocol structs, transport
   address blobs (BLOB_VERSION bumps), AM id space, serde format changes
   (serde_json vs rmp), old-peer/new-peer matrix: can a new node talk to an
   old node in BOTH directions for every touched message?
5. **semver & workspace boundary** — anything new in velo-ext? Are new trait
   methods default-implemented? Does the `=` pin still track? Publishable
   crate set unchanged? Feature-gating: does the crate build with
   `--no-default-features`, with each new feature alone, and with
   `--all-features`? (Actually run the builds.)
6. **shutdown & drain contract** — new inbound paths route through
   `TransportAdapter::admit_message`; no bare `is_draining()` gates; new
   resources (registrations, EPs, leases) have a teardown order and a bounded
   drain; nothing can wedge `wait_for_drain`.
7. **resource lifecycle & leaks** — every register has a deregister on every
   path (incl. error paths and panics), RAII guards can't double-free or leak
   under racing drop/shutdown, caches have bounded growth and their evictions
   can't free memory still referenced by in-flight ops.
8. **test adequacy** — for each behavior the diff claims, is there a test that
   would fail if the behavior regressed? Deliberately break something small in
   your head and check a test catches it. Flag untested error paths.

Each finder returns findings as `{file, line, claim, why-it-breaks, severity}`.

## Stage 2 — Adversarial verification (opus)

Every finding goes to a verifier agent prompted to **refute** it: read the
real code (not the finder's summary), construct the concrete failure scenario
or prove it impossible. Findings that survive get `CONFIRMED`; refuted ones
are dropped with the refutation recorded. Verify perspective-diverse for
severity ≥ major: one correctness lens, one "does it actually reproduce"
lens. A finding needs a concrete input/state sequence, not vibes.

## Stage 3 — Loop until dry

Feed confirmed findings back: one more finder round (fresh agents, told what
was already found) across the dimensions that produced hits. Stop when a
round produces zero new confirmed findings, or after 3 rounds.

## Stage 4 — Report

Report with ReportFindings if the harness asks for typed findings; otherwise
as markdown: confirmed findings ranked blocker → major → minor, each with
`file:line`, the failure scenario, and a suggested fix direction. Then the
refuted-findings appendix (one line each). State explicitly which Stage-0
gates ran, which were skipped and why. Do not auto-apply fixes — the caller
decides what gets fixed, then re-runs the affected dimensions.

## Calibration

- Finders: sonnet. Verifiers and any judgment call: opus. (User preference:
  Fable only for synthesis/judgment, not bulk work.)
- Silence is a valid outcome: "0 confirmed findings" with the refutation
  appendix is a pass, not a failure to try hard enough.
- Never soften a blocker because the fix is annoying. Never report a style
  nit as a finding — clippy owns style.
