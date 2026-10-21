---
name: perf-review
description: Performance review of a velo diff, or a performance audit of one subsystem. Maps the change onto velo's hot paths (per message, frame and record), hunts the regression patterns that have shipped here (per-frame cancellation futures, per-record timers, copies, label lookups, extra syscalls, task hops, moved defaults), makes each finding prove how often it runs, what it costs and that the diff caused it, verifies adversarially, measures base against head with the in-repo harnesses when needed, and reports regressions and optimization opportunities. Use during PR review, or when the user says "perf review", "check for perf regressions", or "audit <path> for performance".
---

# Perf Review

A functional test cannot see a performance regression: the message still
arrives, only later or at more CPU. This review looks for the costs that
have shipped in velo before, on the paths where they are paid per message,
frame or record. Each finding must prove three things: how often the line
runs, what it costs, and that the diff added it. Perf review drifts into
opinion fast; these three proofs keep it honest.

Invocations:
- `perf-review [<range> | <PR number>] [--measure]` reviews a diff. The
  default is `origin/main...HEAD`. It reports regressions, and
  opportunities on the touched code and its call path.
- `perf-review audit <path>` sweeps one subsystem with no diff. It reports
  opportunities only.

References (agents read them; do not paste them into prompts):
- `references/hot-paths.md`: tiers, entry points, budgets per operation,
  measured defaults, perf-sensitive dependencies.
- `references/patterns.md`: reference costs (R-ids), rules, regression
  patterns (P-ids), pinning tests and idioms, the not-a-lever list, diff
  hints.
- `references/measure.md`: how to build and measure base against head.

## Resolve the target once

**Review mode.** Set `REVIEW_RANGE` from the arguments:
- a commit range, verbatim;
- a PR number: `gh pr view <n> --json baseRefName,headRefOid`, then
  `git fetch origin <baseRefName> pull/<n>/head`, then
  `origin/<baseRefName>...<headRefOid>`;
- no argument: `origin/main...HEAD`.

Set `HEAD_SHA=$(git rev-parse <head>)`. Set `BASE_SHA` to
`git merge-base <base> <head>` for a `...` range, or to `<base>` for a
`..` range.

**Audit mode.** `HEAD_SHA=$(git rev-parse HEAD)`. `AUDIT_PATH` is the
argument. There is no base.

`$SCRATCH` is the session's scratchpad directory, or a fresh `mktemp -d`
when there is none. `$S` is `$SCRATCH/perf-review`.

**Read `HEAD_SHA`, never the live tree.** Someone may edit or commit in the
live tree while the review runs. Stage 0 reads files with
`git show $HEAD_SHA:<path>`. From Stage 0 step 5 on, `REVIEW_ROOT` is a
detached worktree at `HEAD_SHA`, made when first needed:
`git worktree add --detach "$S/root" "$HEAD_SHA"`.

Every agent prompt carries: the mode, `REVIEW_RANGE` or `AUDIT_PATH`,
`BASE_SHA`, `HEAD_SHA`, `REVIEW_ROOT`, and the absolute paths of the
references in this skill's own directory. `REVIEW_ROOT` may hold an older
copy of this skill: agents read the references from the given paths, not
from `REVIEW_ROOT/.claude/`.

## Ground rules (a violation invalidates the review)

- thermonuclear-review's ground rules apply unchanged:
  - Commit the code under review first.
  - Finders and verifiers are read-only, and every prompt says so.
  - Mutations, scratch tests and builds happen only in worktrees under
    `$S`.
- Only Stage 0 step 6 and Stage 4 run code. Finders and verifiers do not
  build or benchmark.
- Each tree that is built gets its own `CARGO_TARGET_DIR` (measure.md,
  "Build"). Two worktrees of this repo that share one target directory can
  run each other's code, with no warning.
- No number without a source: an R-id, a book section, a code comment that
  records a measurement, or a Stage 4 result. Otherwise the finding says
  "unquantified".
- The tier is how often the changed line's cost is paid, not where the
  code sits.
  - Socket setup that collapses the TCP window is a per-byte cost.
  - A per-batch walk over every slot is a per-record cost.
  - An error branch inside a per-frame loop is T3.
- Each stage waits for all of its agents before the next starts. Run them
  in the foreground or in one Workflow. Never remove a worktree while an
  agent may still read it.
- If an agent cannot be started (a concurrency limit, for example), do its
  work yourself, and say in the report that the check was not independent.
- At the end, every worktree this review created is removed, and the live
  tree's `git status` is what it was at the start.

## Stage 0: classify (no agents)

1. Run `git diff --stat $REVIEW_RANGE`. Tier each changed line, not the
   function around it, with hot-paths.md. A file that is not compiled
   (docs, CI, scripts) has no tier; check `include_str!` first.
   `HOT_FILES` is the list of files with a T1 or T2 change.
2. Flag each of these:
   - a changed or new measured default (the table in hot-paths.md);
   - a changed perf-sensitive dependency in any `Cargo.toml` or
     `Cargo.lock`;
   - a changed pinning test (patterns.md, P24), wherever it lives;
   - a changed measured number in the book.
3. If `HOT_FILES` is not empty, run the diff hints from patterns.md over
   it, on added and removed lines. Hints are leads for finders, not
   findings. A reworded log line with the same arguments is not a lead.
4. Claims: run the claim grep from patterns.md over
   `git log --format=%B $BASE_SHA..$HEAD_SHA`, and over the PR body when
   there is a PR.
5. Map check: for each touched file, check that the hot-paths.md symbols
   it names still exist in `REVIEW_ROOT`. When a touched file is a book
   page the references cite, check that the cited headings and rule
   numbers still exist. A missing one goes in the report.
6. Pins: when a touched function has a T1 or T2 change, or a pin changed,
   run the pins that patterns.md names for the matched patterns and the
   touched module's own tests, in `REVIEW_ROOT` (measure.md, "Build", for
   the target directory and the provenance check).
   - Run each test by its full path with `--exact`.
   - If one fails, run it again at `BASE_SHA` in its own worktree. It is a
     blocker only if it passes at base. A failing pin is a confirmed
     blocker: it needs no verifier and no Stage 4.
   - If the diff removes a `note_*()` seam call, or does the same work
     beside it without one, add the call back at the new site in a
     scratch worktree and run the pin. If it fails then, the pin is
     bypassed, and that result is the count for the P24 finding.
   - Put every pin result into the verifier prompts, because verifiers do
     not run code.

If nothing is T1 or T2 and nothing was flagged, report "No hot path
touched" with the files and their tiers, plus the Scope and Housekeeping
sections of Stage 5, and stop. That is a pass.

In audit mode: list the T1 and T2 functions under `AUDIT_PATH` from
hot-paths.md, then plan the shards (Stage 1).

## Stage 1: find (sonnet)

| Finder | Pattern families (patterns.md) |
|---|---|
| F1, data movement | copies, allocation, serialization, buffer growth, label lookups and registry recounts, logging, work that grows with slots or peers |
| F2, sync and scheduling | locks, atomics, cancellation tokens, `Weak` upgrades, spawns, task hops, wakes, timers, a blocked worker, round trips |
| F3, I/O and flow control | syscalls per frame, coalescer bypass, large-frame writes, socket options, batch, credit and linger defaults, QUIC packet size, order |

**Small diffs run inline.** When the T1 and T2 part of the diff is about 80
changed lines or fewer, in at most 3 functions, do the F1 to F3 pass
yourself, and send the candidates to the verifiers. Do not start finder
agents.

**Otherwise, run the finders** (in one Workflow, or as foreground agents),
at most 3. Drop a finder whose families cannot apply. Each finder:
- reads hot-paths.md and patterns.md first;
- runs `git diff $REVIEW_RANGE` itself, and reads the whole of each
  touched function under `REVIEW_ROOT`;
- walks the callers until it reaches an entry point in hot-paths.md;
- greps each touched file for comments that record a measured cost (the
  command is in patterns.md), and engages with any near its finding;
- reports a **regression** when the diff added the cost, and an
  **opportunity** when the cost already exists in a touched function, or
  one call away from it (its direct callers and callees; for a trait impl
  that a library drives, such as `Decoder::decode` under `Framed`, the
  loop that drives the library counts as the caller). Before it
  reports an opportunity, it reads the not-a-lever list. The same pattern
  further away goes under "Related, out of scope" in the report, as one
  line.

**In audit mode**, the same finders ask "what does this code pay now?".
When the T1 and T2 code under `AUDIT_PATH` spans more than 3 subsystems,
shard by subsystem instead: one finder per shard, all families, at most 5
finders. If 5 is not enough, ask the user to narrow the path.

Each finding is thermonuclear's `{file, line, claim, why-it-breaks,
severity}`, plus these fields. `severity` is for regressions only; an
opportunity has `impact` instead.

```
kind:      regression | opportunity
pattern:   a P-id, or "new"
tier:      T1 | T2, with the unit (message, frame, record, batch, connection)
chain:     entry point -> ... -> function   (every hop a function you read)
delta:     BASE_SHA does ..., HEAD_SHA does ...   (regressions; quote both)
mechanism: alloc | copy | lock | atomic | token | upgrade | spawn | hop | wake |
           timer | syscall | label | log | walk | growth | default | dependency | pin
cost:      "about N ns per <unit> (R-id or source)", or "unquantified"
evidence:  inferred < sourced < counted < measured (give the strongest one
           that supports the cost)
pin:       the test that covers it now, or the idiom for a new one
fix:       one line, the direction only; opportunities add effort (S, M or L)
           and what must stay true
impact:    opportunities only: high | medium | low
```

`why-it-breaks` gives the chain, the mechanism and the cost in one
paragraph. No chain, no finding.

For a P24 finding (a pin weakened, removed or bypassed), `chain` is the
chain of the code the pin guards, `delta` quotes the test change, and
`cost` is the cost the pin guards against, with that incident's R-id.

## Stage 2: verify (opus, prompted to refute)

Start one verifier for each finder that returned anything (in inline mode,
one verifier). In parallel, start one more verifier that checks every
candidate the finder rated major or above, from scratch, without the
first verdict. This second verifier runs in inline mode too. If the two disagree, the lower severity stands. That is at
most 4 verifiers per round. When every candidate can only be minor (a
constant T2 cost, or inferred evidence only), skip this stage and report
them as minor and unverified.

Each verifier reads the real code, not the finder's summary, and answers
these questions in order. The first "no" decides.

1. **Frequency.** Walk the chain yourself. Does the line run per
   operation? Or only once per connection, burst, or 128 messages? Or only
   on an error, slow, first-use or non-default-feature path? A fallback
   path (for example the per-stream path, used when the mux is not
   negotiated) stays T1, but say which path it is.
2. **Novelty** (regressions only). Run `git show $BASE_SHA:<file>`. Did
   the base already pay this cost, perhaps in code that moved or was
   renamed? If so, it is an opportunity, not a regression.
3. **Reality.** Does the cost exist? `#[cfg(test)]` seams and
   `debug_assert!` vanish in release. A fast path may skip the line.
   `Bytes::clone` of a shared `Bytes` is a reference count, but the first
   clone of a `Bytes` frozen from a `Vec` allocates. Check the current
   dependency's source, not memory: `Sleep::reset` to a later deadline is
   a lock-free CAS on tokio 1.54 (R21). For P24, judge the cost the pin
   guards against.
4. **Size.** Cite an R-id, and compare with the budget in hot-paths.md.
   For a "new" pattern, under 1% of the budget with no lock, wake, syscall
   or timer is minor at most. A match to a P-pattern or rule keeps the
   severity below, because each one was measured large in its incident;
   state the size anyway, so the reader sees when it is small.
5. **Collision.** Would removing the cost break order, drain, credit, a
   SeqCst argument, or short-write safety? Does a comment at the site
   record a measured trade-off? A finding that ignores that comment is
   refuted.
6. **Not a lever** (opportunities only). Is the fix on the not-a-lever
   list? Then it is refuted, unless the finding gives a new reason.
7. **Pinned?** Would an existing test fail? If yes, and the diff did not
   change or bypass that test, keep the finding and say that CI catches it.

Answer CONFIRMED, DOWNGRADED (with the new severity and why), MERGED (the
same cause and fix as another finding; name it), or REFUTED (with why).
Correct any field that was wrong.

Regression severity. When more than one rule matches, the highest wins.
- **blocker:**
  - a P-pattern or rule from patterns.md on a T1 path, with evidence that
    is sourced or better;
  - a pinning test weakened, removed or bypassed, with no measurement;
  - a pinning test that fails at head and passes at base;
  - a measured T1 regression of 5% or more.

  A measured percentage is a share of the whole T1 path's cost, from a
  harness that runs that path. Turn a microbenchmark result into ns per
  operation and compare it with the budget in hot-paths.md instead.
- **major:**
  - a new lock, contended atomic, token poll, spawn, hop, wake, timer,
    syscall or allocation per operation on a T1 path;
  - a cost of about 100 ns per operation or more, by an R-id;
  - a measured default or perf-sensitive dependency changed with no
    measurement;
  - a measured T1 regression of 2 to 5%.
- **minor:**
  - a smaller T1 cost;
  - a constant T2 cost;
  - anything that rests on inferred evidence only.
- T3 costs are not reported.

Opportunities never block. Rank them by impact first:
- high: T1 and 100 ns or more, a rule, or a lock that many connections
  or streams share;
- medium: T1 and 20 ns or more, or T2 work that grows with load;
- low: the rest.

Then rank by effort (S, M or L; a `velo-ext` trait change is L, and it is a
semver event), and then by evidence.

## Stage 3: loop until dry

Run one more round with fresh finders, only for the families that had new
confirmed regressions, and tell them what is already found. Opportunities
do not start a round. A new finding needs a fix of its own; a sub-point of
an existing finding is not new. Stage 1 is round 1.

Skip this stage in inline mode, unless a verifier named a missed
candidate (then verify that one). Skip it also when the only confirmed
regression is fixed by reverting the diff. Stop when a round confirms
nothing new, or after 3 rounds.

## Stage 4: measure (only when triggered)

Measure when any of these holds:
- `--measure` was given (it still stops at the first evidence that
  settles the question, unless the user asked for timing);
- a major or blocker regression on a T1 path was confirmed by a verifier
  (not by a failing pin, which is already a count);
- the diff claims a speedup.

Never measure in audit mode. Follow references/measure.md:
- Build each arm in its own worktree and target directory.
- Stop at the first evidence that settles the question: a count, then
  instructions, then time.
- A difference is real only when the ranges do not overlap.
- Numbers from a dev box are "directional".

The result updates each finding's evidence and severity, but only when
the harness reaches the mechanism. A harness with one connection cannot
show contention between connections; then keep the pattern's severity and
say why. A claimed speedup that does not show is "not reproduced here"; it
is not a regression.

## Stage 5: report

Use ReportFindings if the harness asks for typed findings. Otherwise, write
markdown with these sections:

1. **Scope:**
   - the mode, the range and the SHAs;
   - the hot files by tier;
   - the Stage 0 flags and the pins that ran;
   - whether Stage 4 ran.
2. **Regressions**, from blocker to minor. Each gives:
   - `file:line`, the P-id and its incident;
   - the chain and the delta;
   - the cost with its source, and the evidence level;
   - the direction of the fix;
   - the pinning test to add (the idiom, and where it goes).

   A measured item also gives the arms, the reps, the medians and the
   ranges.
3. **Opportunities**, ranked, at most 10 in review mode. Each gives the
   chain, the cost with its source, the effort, what must stay true, and
   how to measure it.
4. **Related, out of scope:** at most 3 lines, for the same pattern at a
   site the diff does not touch (a sibling transport, for example).
5. **Claims:** reproduced, not reproduced, or not measured. Say whether
   the book carries the number with its setup.
6. **Refuted:** one line each, with the reason.
7. **Housekeeping:** stale map symbols and references, comments or book
   sections the diff makes false, worktrees removed, `git status`, and
   any check that was not independent.

Do not apply fixes. The caller decides what to fix, and then reruns the
review.

## Calibration

- Finders: sonnet. Verifiers and any judgment call: opus.
- Silence is a valid outcome. Most changes on a hot path add no cost. "0
  confirmed regressions" with the refuted list is a pass.
- Never call a clone, a `Box` or a `match` expensive without a chain and an
  R-id. Never soften a blocker because the fix is annoying.
- A number from a dev box is "directional", always.
