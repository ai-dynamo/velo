# perf-review evals

Run these after any edit to the skill or its references. Each seed is a
known regression planted in a scratch worktree. A good review finds every
seed, and stays quiet on the controls.

The seeds are written as edit instructions, not as patch files, because a
patch stops applying when the code changes. If an instruction no longer
matches the code, update the instruction before you run it.

## Set up one seed

The live tree must be clean. Plant the seed only under the scratch
worktree, never in the repo root.

```shell
SCR="$SCRATCH/perf-eval"; BASE=$(git rev-parse HEAD)
git worktree add --detach "$SCR/S3" "$BASE"
# Apply the seed edit to files under $SCR/S3/ only.
git -C "$SCR/S3" commit -qam "perf-review eval seed S3 (never push)"
SEED=$(git -C "$SCR/S3" rev-parse HEAD)
# From the repo root: perf-review $BASE..$SEED
# (this also tests REVIEW_ROOT, because HEAD is not the seed)
git worktree remove --force "$SCR/S3"; git worktree prune
git status --short; git worktree list     # clean; only the main tree
```

A detached commit makes no branch. Once its worktree is gone, nothing
refers to it, and `git gc` removes it.

## Seeds

| Id | Edit | The review must report |
|---|---|---|
| S1 | `lib/velo/src/transports/tcp/listener.rs`, `handle_connection`: delete `let teardown = connection_teardown(&shutdown_state);` and its `tokio::pin!`. Make the select arm `_ = shutdown_state.teardown_token().cancelled() => {`. | P8, blocker. It says that CI catches it (`a_connection_arms_its_teardown_once` sees 0 arms). |
| S2 | Same loop; keep the seam. At the top of the `Some(Ok((msg_type, header, payload)))` arm, add `if shutdown_state.teardown_token().is_cancelled() { break; }`. | P9 or P11, major or above. No pin covers it. |
| S3 | `lib/velo/src/transports/tcp/framing.rs`, `TcpFrameCodec::decode`: replace each `src.split_to(n).freeze()` with `Bytes::copy_from_slice(&src[..n])` followed by `src.advance(n)`. Run with `--measure`. | P1 and rule 5, major or above. A count shows one more allocation per frame. `tx_budget` `l2t` at 262144 is slower, with ranges that do not overlap; the 1024 cell may show no difference. |
| S4 | `lib/velo/src/streaming/control/pump.rs`, `reader_pump`: replace the half-window re-arm (`if armed_until.saturating_duration_since(last_frame) < rearm_threshold { ... }`) with an unconditional `armed_until = last_frame + heartbeat_deadline; sleep.as_mut().reset(armed_until);`, with no `note_timer_arm()`. | P15 and rule 2, blocker. It names the seam bypass (P24), and the comment at the site that forbids a reset per record. |
| S5 | Delete the `size_socket_buffers` call before `listen` in `lib/velo/src/transports/tcp/transport/builder.rs` and both calls in `TcpListener::run_server`. In the accept arm of `run_server`, add `size_socket_buffers(&stream, self.socket_buffers);`. | P20 and rule 3, blocker. The tier is argued from the cost (per byte), not from the code site (per connection). |
| S6 | `lib/velo/src/observability.rs`, `TransportMetricsHandle::record_frame`: make the body always run the `with_label_values` code of the `None` arm. | P5 and rule 1, blocker. With `--measure`, the exercise check rejects `tx_budget` and `throughput`, which install no metrics. |
| S7 | `lib/velo/src/transports/coalesce/mod.rs`: `DEFAULT_MAX_BATCH_FRAMES = 1`. | P18 plus the moved-default flag, major or above. |
| S8 | `lib/velo/src/messenger/server/lanes.rs`, the test `lane_arms_its_idle_timer_a_handful_of_times_not_once_per_item`: `MAX_ARMS` from 4 to 2_000. | P24, blocker. |
| N1 | Reword one sentence in `docs/src/operations/benchmarking.md`. | "No hot path touched", with no agents. |
| N2 | In `handle_connection`, reword the `warn!` for a failed `set_nodelay`. | 0 confirmed: it is an error path. |
| N3 | No seed: review the real perf win `2d0d3e8^..2d0d3e8`. | 0 confirmed regressions. The claims are listed. |
| N4 | In `handle_connection`, before the loop, add `let label = Arc::new(peer_addr.to_string()); debug!(peer = %label, "loop start");`. | Nothing at major or above; at most one minor. |
| A1 | No seed: `perf-review audit lib/velo/src/transports`. | Must find the per-frame `cancelled()` on the node-wide token in UDS `handle_connection` and QUIC `serve_stream` (P8, high impact, effort S), and the gRPC writer's per-frame `cancelled()` and three `to_vec` copies per frame. Must not propose dropping the `AdmissionGate` mutex without its ordering constraint and R12, or `write_vectored`. At most 8 agents. |
| A2 | Optional: `perf-review audit lib/velo/src/messenger`. | Finds the `with_label_values` lookup in `record_client_resolution` on each direct send (P5). |

## Pass bar

- **Seeds:** each is confirmed at the seeded function, with the right P-id
  (or "new" with the right mechanism) and at least the expected severity.
  There is at most one other confirmed regression.
- **Controls:** N1 to N3 confirm 0 regressions. N4 has nothing at major or
  above.
- **Audits:** A1 has every must-find and breaks no must-not.
- **Every run:** the worktrees are removed, and `git status` is clean.
- Record the agent count, wall time and tokens of each run.

## Hook check

- Run thermonuclear-review on S4: its dimension 9 must fire and confirm
  the blocker.
- On N1, stop after the Stage 1 plan: dimension 9 must be dropped. This
  skips the heavy Stage 0 test suite of thermonuclear-review.
