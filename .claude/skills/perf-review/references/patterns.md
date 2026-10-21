# Patterns

Every cost in a finding cites one of these sources: an R-id below, a book
section, a code comment that records a measurement, or a Stage 4 result.
A finding without one says "unquantified".

## Reference costs

| R | Cost | Source |
|---|---|---|
| R1 | A cross-thread wake of a parked task: about 2.4 µs (216 ns on a pinned `current_thread` runtime) | rdma-design.md, "Where the per-message time goes" |
| R2 | A UCX ring push: about 100 ns. `ucp_worker_signal`: 1 to 3 µs. A design estimate, never measured | rdma-design.md, "The UCX transport"; `UcxConfig::spin_us` doc |
| R3 | `CancellationToken::is_cancelled`: 8.7 ns (it locks the token node). A plain atomic flag: 0.3 ns | `streaming/control/registry.rs` (`SenderClosed`); guides/migrate-dispatch-and-cancellation.md |
| R4 | Polling `cancelled()`: about 20 ns (two mutex round trips). Built per frame on the node-wide token, it adds and removes a waiter under one `Notify` lock that every connection shares: about 5% of frontend CPU | the comment in `create_message_handler`; the `connection_teardown` doc |
| R5 | `Weak::upgrade` under contention: 86 to 94 ns. An `Arc` clone: 30 to 39 ns | commit 442223c message |
| R6 | A record body copied into a `Vec`: 35 to 55 ns per record | response-plane-performance.md, "Where velo's extra CPU goes" |
| R7 | A `tokio::time::timeout` built per record (a new timer entry each time) takes the time driver's lock twice: 7.3% of 72 cores | response-plane-performance.md, "What produced the result", item 4 |
| R8 | A `flume` length read on every slot per batch: 2.1% of 72 cores, about 0.5 ms per request | same chapter, item 3 |
| R9 | A third write per large frame on a `TCP_NODELAY` socket: +10.2 µs one way at 256 KiB | rdma-design.md, "Three writes per large frame" |
| R10 | A read buffer grown by doubling: the 8 MiB codec rung took 3.73 ms, against 2.94 ms with reserve-ahead | same section |
| R11 | `SO_RCVBUF` set after data flows: 22.9 MB/s against 2,648 MB/s | rdma-design.md, "Socket buffers sized after data was in flight" |
| R12 | `TcpFrameCodec`: +16 ns. `AdmissionGate::send`: +16 ns over `try_send` | rdma-design.md, "Where the per-message time goes" |
| R13 | A task per bind: 146,000 live tasks against 25,000, at about 2,000 requests per second | response-plane-performance.md, "Where velo's extra CPU goes" |
| R14 | A mutex-guarded dirty lane per record, replaced by a bitmap: −12.5% and −5.6% CPU per record | response-plane-performance.md, "The reader pump leaves the mux data path" |
| R15 | A relay task per record (a wake, a channel send and receive, a hook allocation). The pump was 28% of CPU; removing it saved 9.2% and 4.3% | same section |
| R16 | A gauge that recounts a registry on each create and retire: 0.49% of 72 cores | response-plane-performance.md, the symbol table |
| R17 | QUIC packets above 6550 B under GSO: 115 msg/s against 2,666 | quic-performance.md, "Packets above 6550 bytes are lost" |
| R18 | A credit window of 256 against 32: TTFT p50 135 to 355 ms against 88 to 102 ms on a saturated frontend | response-plane-performance.md, "The credit window and a saturated frontend" |
| R19 | `CancellationToken::is_cancelled` on the delivery path, per record: 0.43% of 72 cores | response-plane-performance.md, the symbol table in "Where velo's extra CPU goes" |
| R20 | Coalescing, as counts: a burst of 20,001 frames left in 21 writes (952:1); two writes per large frame instead of three took `sendto` from 6,644 to 4,476 over 1,100 frames | concepts/batched-streaming.md, "Per-stream coalescing cannot reach the forward-pass shape"; rdma-design.md, "Three writes per large frame" |
| R21 | Moving one pinned `Sleep` to a later deadline on every record: +48 instructions per record and no lock on tokio 1.54 (`extend_expiration` is a CAS). It is not R7 | measured in this skill's evaluation: `perf stat -e instructions:u`, 16 `reader_pump`s x 200,000 records, 10 interleaved reps per arm, dev box (directional) |
| R22 | One uncontended mutex acquire (`CancellationToken::is_cancelled`): about 8 ns alone, about 6 ns in a loop with real work (aarch64, pinned) | the comment in `create_message_handler` |
| R23 | `memcpy`, memory touched first (B200): 0.05 µs at 4 KiB, 4.6 µs at 256 KiB, 27.5 to 35.8 µs at 1 MiB, 486 to 494 µs at 8 MiB | rdma-design.md, "Measurements behind it" |

The repo has no number for a prometheus `with_label_values` lookup, a
`tokio::spawn`, a `DashMap` shard lock, a small-frame write syscall, or a
contended mutex. For those, use the rules and counts here, or measure.
Check a dependency's cost in the version that `Cargo.lock` pins: R21 shows
that a cost can change with a version.

## Rules

Each rule comes from the book or CLAUDE.md. Breaking one on a T1 path is a
blocker.

1. No label lookup per record or message. Pre-bind the collectors, and
   observe per batch where possible. (response-plane-performance.md,
   "Instrumentation cost")
2. No timer per record. Use one per stream or lane, and re-arm it lazily.
   (response-plane-performance.md, "What produced the result", item 4)
3. Size socket buffers on the listener before `listen`, and on the dial
   side before the first write. (concepts/transports.md, "Socket buffers
   are set before data flows")
4. A large frame is two writes. Do not use `write_vectored`: TCP can
   short-write past about 128 KiB. (concepts/transports.md, "Coalescing
   writer")
5. Use `Bytes::from(vec)`, not `copy_from_slice`. Slice with
   `split_to().freeze()`. (CLAUDE.md)
6. Put set-once values in a `OnceLock`, not behind a lock. (CLAUDE.md;
   442223c)
7. QUIC `max_mtu` is at most 6550. (quic-performance.md)
8. A `#[cfg(test)]` counting seam has an empty `#[cfg(not(test))]
   #[inline(always)]` twin, so it costs the shipped code nothing.
9. Never block a runtime worker on a terminal send. (eeb9093;
   batched-streaming-design.md, "A terminal send wedged the runtime")

## Regression patterns

Each entry gives: what to look for; why it costs; the incident; the test
that pins it now.

### F1: data movement

- **P1. A copy where a move works.** `copy_from_slice`, `to_vec` or
  `to_owned` on header or payload bytes. Cost: an allocation plus a
  `memcpy` of O(size) (R6). Incident: the CLAUDE.md rule; the mux ingress
  copy. Pins: `body_range_indexes_the_payload_for_zero_copy_slicing`,
  `get_pinned_returns_the_bytes_without_a_copy_out`. The TCP codec has no
  pin: `test_zero_copy_bytes_share_buffer` compares contents only, so a
  copying `TcpFrameCodec::decode` passes CI.
- **P2. A full decode to answer yes or no.** A serde decode per item only
  to classify it. Incident: 68f7d12 (`is_terminal_sentinel`). Pin:
  `classifying_an_item_does_not_allocate`.
- **P3. An allocation per operation.** A `Vec`, `String`, `Box`,
  `Arc::new`, `format!`, `to_string` or `collect`; a ticket or handle on a
  fast path. Incidents: 6d41932 (NATS `to_string` per send), 17c2055 (the
  admission fast path takes no ticket). Pins:
  `admitted_fast_path_allocates_no_ticket` (it checks the queue and
  driver, not an allocation count), `an_idle_set_allocates_no_slot_words`.
  A boxed handler future per message is part of the handler API; only new
  boxes count.
- **P4. A buffer that grows without reserve, or is held forever.** Cost:
  R10. Incident: 2d0d3e8. Pins:
  `test_decode_reserves_capacity_for_announced_frame`,
  `an_enormous_declared_body_length_does_not_allocate`.
- **P5. A label lookup or registry recount per operation.**
  `with_label_values` per record or batch; `DashMap::len()` or iteration
  for a gauge, which read-locks every shard. Cost: R16. Incident: 68f7d12.
  Pin: `the_ordered_lane_is_observed_once_per_batch_not_once_per_record`.
  No test pins `TransportMetricsHandle::record_frame` to its pre-bound
  arrays, so a lookup there passes CI. Rule 1.
- **P6. Logging and spans per operation.** `info!` or `debug!` with
  formatted arguments; spans outside the `distributed-tracing` feature. No
  incident yet, so this is "inferred" unless measured.
- **P7. Work that grows with slots, peers or streams.** A walk over every
  slot per batch; `len()` over all entries per operation. Cost: R8.
  Incident: eeb9093.

### F2: synchronization and scheduling

- **P8. A cancellation future built per frame.** `token.cancelled()`
  inside a per-frame `select!`. It is worst on the node-wide teardown
  token. Cost: R4. Incident: 68f7d12. Pin:
  `a_connection_arms_its_teardown_once`. It counts builds through
  `connection_teardown`: removing that call fails it (0 builds), but a
  second future built elsewhere in the loop escapes it.
- **P9. `is_cancelled()` per record where a flag would do, or where no
  check is needed.** Cost: R3, R19; on the node-wide token it also
  contends with every connection. Incident: 442223c (`SenderClosed`). The
  check after dequeue in `create_message_handler` is deliberate, and its
  comment explains why an `AtomicBool` mirror of a token is wrong: read it
  first.
- **P10. `Weak::upgrade` per message.** Cost: R5. Incident: 442223c
  (`BurstRef`).
- **P11. A lock per operation that a lock-free or set-once structure
  avoids.** Cost: R14. Incidents: 442223c, 68f7d12. Collision: the mutex in
  `AdmissionGate::send` makes "queue empty, then `try_send`" one critical
  section. It keeps send order, and it costs 16 ns (R12).
- **P12. A guard held across work that takes the same lock, or across an
  `.await`.** A `DashMap` entry or `Ref` held while calling out. Incident:
  06d2741 (a gauge `len()` inside an entry guard deadlocked itself).
- **P13. A spawn per operation.** Cost: R13. Incidents: 6d41932 (NATS
  spawned per send), 68f7d12 (a task per bind). Pin:
  `a_finished_zero_rtt_stream_leaves_no_task_behind`. `SpawnedDispatcher`
  spawns per message by design.
- **P14. An extra hop or channel relay per operation.** Cost: R1 when the
  receiver is parked; R15. Incident: 68f7d12. Pins:
  `wakes_are_counted_by_source`, `linger_wake_is_counted`.
- **P15. A timer per operation.** A `timeout`, `sleep` or `interval` built
  per record or item costs R7. Resetting the stream's one pinned `Sleep`
  on every record costs R21: much less, but it still breaks rule 2 and
  usually skips the `note_timer_arm` seam. Incident: eeb9093. Pins:
  `a_thousand_records_arm_the_heartbeat_timer_a_handful_of_times`,
  `a_thousand_records_arm_the_mpsc_heartbeat_timer_a_handful_of_times`,
  `lane_arms_its_idle_timer_a_handful_of_times_not_once_per_item`,
  `a_stream_under_traffic_never_fires_its_heartbeat_timer`. A timer that
  does not call `note_timer_arm` escapes them all. Rule 2.
- **P16. A blocked runtime worker.** A blocking send, `thread::sleep`,
  sync I/O, `block_on`, a long std mutex hold, or work that crosses two
  runtimes. Incidents: eeb9093 (a deadlock); response-plane-performance.md
  item 1 (two runtimes cost 2.4% plus 6.8% in lock contention). Pin:
  `a_terminal_from_a_thread_without_a_runtime_does_not_block_it`.
- **P17. A round trip per request.** Incidents: 68f7d12 (`wait_for_handler`
  sent `_hello` per request); eeb9093 (credit only through the doorbell
  halved throughput).

### F3: I/O and flow control

- **P18. A syscall per frame.** A write path around
  `run_coalescing_writer`, a flush per frame, or a batch cap that stops
  frames from sharing a write. Cost: R20 (a count). Incident: ea62058. Pins:
  `burst_coalesces_writes`, `transports/coalesce/tests.rs`.
- **P19. The shape of a large-frame write.** More than two writes, or
  `write_vectored`. Cost: R9. Incident: 2d0d3e8. Pin:
  `test_direct_path_stages_prefix_into_two_writes`. Rule 4.
- **P20. Socket options set after data can flow, or fixed buffers that
  clamp autotuning.** Cost: R11, which depends on the host's
  `net.core.rmem_max`. Incidents: 209209e, 2445cc6. Pins:
  `socket_buffers_sizes_the_listener_or_leaves_it_to_the_kernel`,
  `start_keeps_the_listener_setting`. Rule 3.
- **P21. Batch, credit and linger.** A cap that is ignored; a window,
  linger or sweep default that moves. Cost: R18. Incidents: 67198ce,
  867d25f, eeb9093. Pins: `the_default_credit_window_is_32`,
  `a_larger_configured_cap_is_not_limited_by_tcp_coalescing`,
  `the_configured_cap_can_exceed_the_tcp_coalescing_threshold`. Report
  latency beside throughput.
- **P22. QUIC packet size and connection count.** Cost: R17. One
  connection tops out near 0.8 GB/s, which is why lanes exist. Incidents:
  bee8b8e, ced7fba, 538679c, ef45f6c. Pin:
  `large_frames_round_trip_at_a_jumbo_mtu`. Rule 7.
- **P23. Speed bought with order.** A spawn per message, or a backpressure
  future that owns its frame. Incidents: 1eee2c6, 17c2055. Pins:
  `concurrent_senders_keep_their_own_order`,
  `burst_preserves_order_and_completeness`.

### Any finder

- **P24. A pinning test weakened, removed or bypassed.** A bound raised
  (`MAX_ARMS`), an `#[ignore]` added, an assertion dropped, an
  `allocations_in` closure narrowed, or new code that does the same work
  without calling the `note_*` seam. A blocker unless a measurement comes
  with it.
- **P25. State kept after an operation ends.** Memory that grows with the
  operations that ended, not with the ones that are live: a map entry, a
  queue entry, an `Arc` or a buffer that nothing removes when a stream,
  request or connection ends. Cost: RSS without bound. An allocation
  count per operation does not see it, because the allocation is made
  once. RSS growth alone is not proof: rule out a task backlog (P13) and
  allocator retention first (measure.md, section 6). Pin: run N
  operations, then assert that each registry and queue is empty again,
  and that `num_alive_tasks()` is back to its baseline after a yield.

## Pinning idioms (for the `pin` field)

- **Allocations:** `crate::test_alloc::allocations_in(|| ...)`. It works in
  `lib/velo` unit tests only, because it is that test binary's global
  allocator. It counts on the calling thread only.
- **Builds and arms:** a `#[cfg(test)]` `tokio::task_local!` counter that a
  `note_*()` seam bumps, with an empty `#[cfg(not(test))] #[inline(always)]`
  twin. See `connection_teardown` and `note_timer_arm`.
- **Tasks:** `tokio::runtime::Handle::current().metrics().num_alive_tasks()`
  against a baseline.
- **Label lookups:** after `bind_*`, remove every pre-bound child with
  `remove_label_values`, run the code, and assert that `registry.gather()`
  shows none of them again. Only a `with_label_values` lookup creates a
  removed child again, so this needs no seam.
- **Writes and batches:** the recording writer in
  `transports/coalesce/tests.rs`. End to end, the ratios from
  `observability::test_helpers::MetricSnapshot` (the `test-helpers`
  feature).
- **Prove it:** revert the fix and watch the test fail (benchmarking.md,
  measurement rule 11).

## Not a lever: read these before you propose an optimization

- docs/src/operations/response-plane-performance.md, "What is not a lever"
- docs/src/guides/tune-batched-streaming.md, "Settings that are not levers"
- docs/src/operations/quic-performance.md, "Settings that did not help"
- docs/src/development/rdma-design.md, "Rejected alternatives" and "What
  does not work"
- docs/src/concepts/transports.md, "Coalescing writer" (`write_vectored`)
- docs/src/development/batched-streaming-design.md, "Ingress copies each
  record out of its batch" (why the per-record copy stays)
- Comments at the site. List them with:
  `grep -nE '[0-9.]+ ?(ns|µs)\b|[Mm]easured|not a lever|per (frame|record|message)' <file>`

## Diff hints (Stage 0)

These are leads for the finders, not findings. `$HOT_FILES` is the list of
files with a T1 or T2 change. Skip the first three greps when it is empty:
an empty list makes `git diff` scan the whole diff.

```shell
# Added lines that often carry a cost.
git diff -U0 $REVIEW_RANGE -- $HOT_FILES | grep -nE '^\+.*(copy_from_slice|to_vec\(|to_owned\(|to_string\(|format!|vec!\[|Box::(new|pin)|Arc::new|collect::|serde_json|rmp_serde::|Mutex|RwLock|\.lock\(|\.upgrade\(|is_cancelled\(|\.cancelled\(|spawn|block_on|thread::sleep|time::(timeout|sleep|interval)|\.reset\(|Instant::now|unbounded|\.len\(\)|\.iter\(\)|with_label_values|(info|debug|warn|trace)!|_span!|write_all|write_vectored|flush\(|socket_buffers|buffer_size|SO_(RCV|SND)BUF|listen\(|set_nodelay|SeqCst)'
# Removed lines that often were a protection.
git diff -U0 $REVIEW_RANGE -- $HOT_FILES | grep -nE '^-.*(child_token|cancelled_owned|pin!|reserve\(|with_capacity|Bytes::from\(|split_to|freeze\(|stage_direct_prefix|OnceLock|bind_|\[[a-z_]*idx\]|note_[a-z_]+\(|BurstRef|biased|try_send|socket_buffers|listen\()'
# Constants whose value changed (compare with the measured-defaults table).
git diff -U0 $REVIEW_RANGE -- $HOT_FILES | grep -nE '^[-+].*const [A-Z_]+: [a-z0-9]+ ='
# Pinning tests touched anywhere.
git diff -U0 $REVIEW_RANGE -- '*.rs' | grep -nE '^[-+].*(MAX_ARMS|#\[ignore|allocations_in|num_alive_tasks|note_[a-z_]+arm)'
```

The claim grep (Stage 0 step 4), over commit messages and the PR body:

```shell
grep -niE '\bperf\b|faster|speed-?up|latency|throughput|\bcpu\b|alloc|[0-9.]+ ?(ns|µs|us|ms|%|MB/s|GB/s|msg/s)\b'
```

Over added lines of the book, the same pattern finds a changed measured
number.
