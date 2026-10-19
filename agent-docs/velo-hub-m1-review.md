# velo-hub M1: thermonuclear review record

Work branch only; `agent-docs/` never merges to `main`. 2026-10-18.

Harness: repo skill `thermonuclear-review`. Finders ran on Sonnet, one per dimension; verifiers ran on Fable and were told to refute. Neither is the model that wrote the code (Opus), and the verifier is not the model that made the claim. Every surviving claim was then adjudicated by the author with a test: write a test that fails for the stated reason, rule, fix, keep the test. Where a fix landed, the new test was also run against the pre-fix code (a scratch worktree, or a mutation restored from a backup) to show it fails there.

Stage 0 (run directly): `cargo fmt --check`, `cargo clippy --all-features --no-deps --all-targets -D warnings`, `cargo machete`, `scripts/check-semver.sh` (velo 0.21.0 accepted against published 0.20.0; velo-hub never published, skipped), `cargo test --locked --all-features -p velo-hub`, `cargo tree -p velo-ext | grep -c prometheus` = 0, `cargo deny check` (8 advisories, the same 8 as on the base commit c1d66c4; bans, licenses, sources pass). Workspace `cargo test --all-features --all-targets --no-fail-fast`: 1894 passed, 44 failed, all 44 in the five etcd/NATS binaries (connection refused; no servers on this host).

## Round 1 (range c1d66c4..8ad28d2): 21 claims

| # | Claim | Verifier | Ruling | Test | Fix commit |
|---|---|---|---|---|---|
| 1 | DELETE runs in the handler; a client disconnect leaves the owner Busy | split: one verifier reproduced, one refuted (argued hyper's HTTP/1 path never drops the handler) | **valid**. A raw socket close makes hyper drop the handler; a reqwest timeout does not, which is why one verifier could not reproduce it. The existing register-disconnect test used a reqwest timeout and passed with the transaction inline: it was tautological. | `client_disconnect_mid_unregister_*`, register test rewritten to close a socket | 02ba6f1 |
| 2 | A panicked transaction reserves an incarnation that was already removed: permanent lockout | confirmed | valid | `a_dead_transaction_whose_entry_was_removed_does_not_lock_the_owner_out` | c58ef45 |
| 3 | Shutdown holds the lock across the HTTP drain: stalls lock_timeout | confirmed | valid | `shutdown_does_not_wait_out_the_lock_timeout_of_queued_requests` | cd02702 |
| 4 | HTTP drain unbounded; probe route without deadline | confirmed | valid | `shutdown_does_not_wait_forever_for_a_stuck_request`, `a_probe_to_a_wedged_peer_times_out` | cd02702 |
| 5 | `shutdown()` not cancellation-safe: a dropped future leaves a zombie hub | confirmed | valid | `a_cancelled_shutdown_still_finishes` | cd02702 |
| 6 | A dropped `serve()` future skips the unwind | confirmed | valid | `a_cancelled_serve_still_cleans_up` | cd02702 |
| 7 | store removes and `on_registered` unbounded under the lock | confirmed | valid (the `on_registered` part is the extension breaking its contract; bounded anyway) | `a_hung_store_remove_does_not_hold_the_lock_forever` | cd02702 |
| 8 | worker-id discovery misses a peer while it re-registers | confirmed (23% misses) | valid (189,349 misses in our stress test) | `discovery_never_misses_a_peer_while_it_is_re_registered` | 984f3e5 |
| 9 | version-sync check fails open on other TOML spellings | confirmed | valid | script cases (s)-(w) | 2ec0326 |
| 10-14 | docs: three features not two; extension routes per listener; on_removed twice; health not JSON; unrecorded RwLock lookup figure | confirmed | valid (the figure was a real measurement from this session, 994.6 ns and 8.0 M/s, now attributed) | - | 300aa87 |
| 15 | stale discovery after a replacement address untested | confirmed | valid | conformance `replacement_is_what_discovery_serves` | 984f3e5 |
| 16 | probe touch untested | confirmed | valid | `a_successful_probe_refreshes_liveness` | 022eff5 |
| 17 | admission untested | confirmed | partly valid: each of the two checks backs up the other, so neither can be removed alone in a test; the pair is now tested | admission 503 in the shutdown test | 022eff5 |
| 18 | on_registered panic untested | confirmed (mechanism corrected by the verifier) | valid | `panicking_on_registered_retires_the_registration` | 022eff5 |
| 19 | removal-before-undo order untested at three sites | confirmed | valid | `Ledger::revealed()` asserted at all sites | 022eff5 |
| 20 | hook linearization test weak; conformance has no concurrent check | confirmed (mechanism corrected: the race test catches a 5 ms gap 2 times in 20) | valid, documented: the trait doc now says conformance checks it only sequentially | - | 300aa87 |
| 21 | catch_unwind, unregister error branches, post-write reservation, MissingRegistrationRequirement, BundleCycle, lock_timeout untested | confirmed | valid | tests added | 022eff5 |

Refuted: none stood. The one refutation (row 1) was overturned by a test.

## Round 2 (range c1d66c4..300aa87, fixes 8ad28d2..300aa87): 13 claims, 12 confirmed, duplicates merged

| # | Claim | Ruling | Test | Fix commit |
|---|---|---|---|---|
| 1 | Heartbeat awaits store touch/remove unbounded; one hung call stops every later probe and hangs shutdown, which joins the task under the lock | valid (major). Verifier correction: a registration answers 503 at once (admission), only DELETE waits lock_timeout | `a_hung_store_touch_does_not_stop_the_heartbeat_or_hang_shutdown` | 7d4a664 |
| 2 | Aborting the servers after the drain timeout does not end in-flight requests: axum spawns each connection | valid | `shutdown_does_not_wait_forever_for_a_stuck_request` now asserts the request ends | 6816374 |
| 3 | `on_registered` deadline drops the future and still answers 200, undocumented | valid; documented (trait, config, book) | `a_hung_on_registered_is_dropped_at_the_deadline` | fbaf263, df5d5ef |
| 4 | An extension's own panicking commit drops its own `Staged` before `on_removed` | valid; not fixable without changing `commit`'s signature (the value has moved into the extension); documented as a contract limit; `commit` must not panic | - | fbaf263 |
| 5 | Stale rustdoc: second shutdown, `step_timeout`, `shutdown_timeout` | valid | - | fbaf263 |
| 6 | Test gaps: on_registered deadline, the three other bounded-remove sites, DELETE lock timeout, the unwritten Drop arm order, tests that fail by hanging, semver (v) tautological, header whitespace | valid (the probe test fails by assertion after 120 s, not a hang; given a deadline anyway) | six tests and two script cases, each mutation-checked | df5d5ef |

## Round 3 (range c1d66c4..df5d5ef, fixes 300aa87..df5d5ef): 16 claims, 15 confirmed, 1 refuted

| # | Claim | Ruling | Test | Fix commit |
|---|---|---|---|---|
| 1 | A reservation is cleared only by a removal of its own incarnation; after an unresolved write, a late-landing replacement that then expires strands the owner reserved until restart | valid (major). Changes one ported transition: any removal of a reserved owner clears it; safe because a credential-free claim writes with `expected: None` | `any_removal_of_a_reserved_owner_clears_the_reservation`, `a_late_landing_write_does_not_strand_a_reservation` (fails with the old arm) | d7f98e5 |
| 2 | A panicking extension `shutdown` kills the stop task: ports bound, pinned entry kept, velo running, every later `shutdown` errors | valid (found by two finders) | `a_panicking_extension_shutdown_does_not_leave_the_hub_half_stopped` | 546194c |
| 3 | A hung extension `shutdown` hangs the stop under the lock | valid | `a_hung_extension_shutdown_is_bounded` | 546194c |
| 4 | A panicking `attach`/`start` skips the unwind | valid | `a_panic_in_start_unwinds_like_an_error` | 546194c |
| 5 | Routers (extension route code) built after the irreversible hook; a route panic consumes the hook | valid | `a_panic_while_building_routes_unwinds_and_leaves_the_store_reusable` | 546194c |
| 6 | Dropping the hub on a thread outside its runtime skips the ordered stop | valid | `dropping_the_hub_off_the_runtime_still_stops_it_in_order` | 546194c |
| 7 | `ServerConfig` not validated; zero intervals panic the heartbeat or reaper silently | valid; verifier correction: zero `step_timeout` only expires steps that yield, so it is refused for clarity, not because every step fails | `zero_intervals_are_rejected_before_binding` | 546194c |
| 8 | Book: drain "drops the rest" after one budget; it answers 503 and can take two | valid; the config doc's "three times" was also inexact | - | acc3c8e |
| 9 | Book: a timed-out remove is read back only by DELETE | **refuted** by the verifier: every remove under the lock is followed by a store read that decides the next step | - | - |
| 10 | Introduction still says two crates | valid | - | acc3c8e |
| 11 | Code and tests cite `agent-docs/` and plan labels R1/R2/R4, which do not merge | valid (verifier found five more sites) | - | acc3c8e |
| 12 | put and resolve deadlines untested | valid | `a_hung_store_write_is_cut_off_and_aborts`, `a_hung_read_back_is_cut_off_and_fails_closed` | ee189ed |
| 13 | bounded core-task join untested | valid | `a_task_that_ignores_cancellation_does_not_hang_shutdown` (`FaultStore::stubborn_liveness`) | ee189ed |
| 14 | heartbeat remove bound untested | valid | `a_hung_store_remove_does_not_stop_the_heartbeat` | ee189ed |
| 15 | shutdown's lifecycle drain untested | valid; verifier correction: the lock is FIFO, so shutdown must queue before the lifecycle task to reach the drain | `shutdown_delivers_the_removals_still_queued` (two removals: in hand and queued; each mutant fails) | ee189ed |
| 16 | prepare deadline test hangs on regression; semver branches untested | valid in part: of five script mutants, two are real gaps (explicit caret, dotted inheritor); two are behavior-equivalent; one strip was unreachable and is removed | cases (z), (za), (zb) | ee189ed |

The loop did not run dry: round 3 still produced confirmed findings, and the skill stops after 3 rounds. The findings got narrower each round: round 1 lifecycle and protocol defects, round 2 the boundedness of the round-1 fixes, round 3 panics and hangs in extension code plus one state-machine edge under a double store fault. Every confirmed item is fixed with a test, or documented: the hook-linearization race test is probabilistic, the two admission checks back each other up, an extension's own panicking `commit` drops its `Staged` before `on_removed`, and the server-abort fallback after the second drain has no test (no request survives the per-request stop).
