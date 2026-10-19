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

## Round 3

See below (appended when it completes).
