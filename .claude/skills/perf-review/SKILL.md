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
- `references/measure.md`: how to measure base against head.

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

Set `REVIEW_ROOT`, the directory whose files are at `HEAD_SHA`:
- the repo root, when `HEAD_SHA` is checked out and `git status` is clean;
- otherwise `git worktree add --detach "$SCRATCH/perf-review/root" "$HEAD_SHA"`.

An agent that reads the live tree while the review is about another commit
reviews the wrong code.

**Audit mode.** `HEAD_SHA=$(git rev-parse HEAD)`. `REVIEW_ROOT` is the
clean repo root. `AUDIT_PATH` is the argument. There is no base.

Every agent prompt carries: the mode, `REVIEW_RANGE` or `AUDIT_PATH`,
`BASE_SHA`, `HEAD_SHA`, `REVIEW_ROOT`, and the paths of the references.
`$SCRATCH` is the session's scratchpad directory, or a fresh `mktemp -d`
when there is none.

## Ground rules (a violation invalidates the review)

- thermonuclear-review's ground rules apply unchanged:
  - Commit the code under review first.
  - Finders and verifiers are read-only, and every prompt says so.
  - Mutations, scratch tests and builds happen only in worktrees under
    `$SCRATCH`.
  - At the end, `git status` is clean.
- Finders do not build or benchmark. Only Stage 4 runs code.
- No number without a source: an R-id, a book section, a code comment that
  records a measurement, or a Stage 4 result. Otherwise the finding says
  "unquantified".
- The tier is how often the cost is paid, not where the code sits.
  - Socket setup that collapses the TCP window is a per-byte cost.
  - A per-batch walk over every slot is a per-record cost.
- At the end, `git worktree list` shows no perf-review worktrees.

## Stage 0: classify (no agents)

1. Run `git diff --stat $REVIEW_RANGE`. Tier each touched function with
   hot-paths.md. `HOT_FILES` is the list of files with a T1 or T2 change.
2. Flag each of these:
   - a changed measured default (the table in hot-paths.md);
   - a changed perf-sensitive dependency in any `Cargo.toml` or
     `Cargo.lock`;
   - a changed pinning test (patterns.md, P24).
3. Run the diff hints from patterns.md over `HOT_FILES`, on both added and
   removed lines. Hints are leads for finders, not findings.
4. Claims: grep `git log --format=%B $BASE_SHA..$HEAD_SHA` and the PR body
   for perf, faster, speedup, latency, throughput, CPU, alloc, ns and µs.
5. Map check: grep for each hot-paths.md symbol that lives in a touched
   file. If one is missing, the map is stale; put that in the report.
6. If a pinning test lives in a touched file, run just that test in
   `REVIEW_ROOT`, with `CARGO_TARGET_DIR="$SCRATCH/perf-review/target"` so
   that the dependencies build only once (for example
   `cargo test -p velo --lib <test name>`). A failure is a blocker, and no
   agent is needed for it.

If nothing is T1 or T2 and nothing was flagged, report "No hot path
touched" with the files and their tiers, and stop. That is a pass.

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
yourself, and send the candidates to one verifier. Do not start a Workflow.

**Otherwise, run one Workflow** with at most 3 finders. Drop a finder
whose families cannot apply. Each finder:
- reads hot-paths.md and patterns.md first;
- runs `git diff $REVIEW_RANGE` itself, and reads the whole of each
  touched function under `REVIEW_ROOT`;
- walks the callers until it reaches an entry point in hot-paths.md;
- greps each touched file for comments that record a measured cost (the
  command is in patterns.md), and engages with any near its finding;
- reports a **regression** when the diff added the cost, and an
  **opportunity** when the cost already exists in a touched function or on
  its call chain. Before it reports an opportunity, it reads the
  not-a-lever list.

**In audit mode**, the same finders ask "what does this code pay now?".
When the T1 and T2 code under `AUDIT_PATH` spans more than 3 subsystems,
shard by subsystem instead: one finder per shard, all families, at most 5
finders. If 5 is not enough, ask the user to narrow the path.

Each finding is thermonuclear's `{file, line, claim, why-it-breaks,
severity}`, plus these fields:

```
kind:      regression | opportunity
pattern:   a P-id, or "new"
tier:      T1 | T2, with the unit (message, frame, record, batch, connection)
chain:     entry point -> ... -> function   (every hop a function you read)
delta:     BASE_SHA does ..., HEAD_SHA does ...   (regressions; quote both)
mechanism: alloc | copy | lock | atomic | token | upgrade | spawn | hop | wake |
           timer | syscall | label | log | walk | growth | default | dependency | pin
cost:      "about N ns per <unit> (R-id or source)", or "unquantified"
evidence:  inferred | sourced | counted | measured
pin:       the test that covers it now, or the idiom for a new one
fix:       one line, the direction only; opportunities add effort (S, M or L)
           and what must stay true
```

`why-it-breaks` gives the chain, the mechanism and the cost in one
paragraph. No chain, no finding.

## Stage 2: verify (opus, prompted to refute)

Start one verifier for each finder that returned anything (in inline mode,
one verifier). One more verifier checks every candidate at major or above
again from scratch, without the first verdict. If the two disagree, the
lower severity stands. That is at most 4 verifiers.

Each verifier reads the real code, not the finder's summary, and answers
these questions in order. The first "no" decides.

1. **Frequency.** Walk the chain yourself. Does the line run per
   operation? Or only once per connection, burst, or 128 messages? Or only
   on an error, slow, first-use or non-default-feature path?
2. **Novelty** (regressions only). Run `git show $BASE_SHA:<file>`. Did
   the base already pay this cost, perhaps in code that moved or was
   renamed? If so, it is an opportunity, not a regression.
3. **Reality.** Does the cost exist? `Bytes::clone` is a reference count.
   `#[cfg(test)]` seams and `debug_assert!` vanish in release. A fast path
   may skip the line.
4. **Size.** Cite an R-id, and compare with the budget in hot-paths.md.
   Under 1% of the budget, with no lock, wake, syscall or timer, is minor
   at most.
5. **Collision.** Would removing the cost break order, drain, credit, a
   SeqCst argument, or short-write safety? Does a comment at the site
   record a measured trade-off? A finding that ignores that comment is
   refuted.
6. **Not a lever** (opportunities only). Is the fix on the not-a-lever
   list? Then it is refuted, unless the finding gives a new reason.
7. **Pinned?** Would an existing test fail? If yes, and the diff did not
   change or bypass that test, keep the finding and say that CI catches it.

Answer CONFIRMED, DOWNGRADED (with the new severity and why), or REFUTED
(with why). Correct any field that was wrong.

Regression severity:
- **blocker:**
  - a P-pattern or rule from patterns.md on a T1 path, with evidence that
    is sourced or better;
  - a pinning test weakened, removed or bypassed, with no measurement;
  - a measured T1 regression of 5% or more.
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
- high: T1 and 100 ns or more, or a rule;
- medium: a smaller T1 cost, or T2 work that grows with load;
- low: the rest.

Then rank by effort (S, M or L; a `velo-ext` trait change is L, and it is a
semver event), and then by evidence.

## Stage 3: loop until dry

Run one more round with fresh finders, only for the families that had new
confirmed findings. Tell them what is already found. Stop when a round
confirms nothing new, or after 3 rounds.

## Stage 4: measure (only when triggered)

Measure when any of these holds:
- `--measure` was given;
- a major or blocker regression on a T1 path was confirmed;
- the diff claims a speedup.

Never measure in audit mode. Follow references/measure.md:
- Build both arms in worktrees under `$SCRATCH`.
- Prefer a count to a time.
- Interleave the reps.
- A difference is real only when the ranges do not overlap.
- Numbers from a dev box are "directional".

The result updates each finding's evidence and severity. A claimed speedup
that does not show is "not reproduced here"; it is not a regression.

## Stage 5: report

Use ReportFindings if the harness asks for typed findings. Otherwise, write
markdown with these sections:

1. **Scope:**
   - the mode, the range and the SHAs;
   - the hot files by tier;
   - the Stage 0 flags;
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
4. **Claims:** reproduced, not reproduced, or not measured. Say whether
   the book carries the number with its setup.
5. **Refuted:** one line each, with the reason.
6. **Housekeeping:** stale map symbols, worktrees removed, `git status`.

Do not apply fixes. The caller decides what to fix, and then reruns the
review.

## Calibration

- Finders: sonnet. Verifiers and any judgment call: opus.
- Silence is a valid outcome. Most changes on a hot path add no cost. "0
  confirmed regressions" with the refuted list is a pass.
- Never call a clone, a `Box` or a `match` expensive without a chain and an
  R-id. Never soften a blocker because the fix is annoying.
- A number from a dev box is "directional", always.
