# Review for performance

A functional test cannot see a performance fault. The message still arrives,
only later or at more CPU. This chapter says where velo pays its costs, which
costs have shipped before, how to hold a cost in a test, and how to compare
two commits on one machine.

The `perf-review` skill in `.claude/skills/perf-review/` runs this review with
agents. Run it before a PR that changes code on a hot path. `thermonuclear-review`
runs the same regression check when a change touches a hot path.

## Where the costs are paid

Rank a cost by how often it is paid, not by where the code sits.

| Tier | The cost is paid | Examples |
|---|---|---|
| T1 | per message, frame, record or byte; or per batch when the work grows with streams, slots or peers | the send path, the coalescing writer, frame decode, `route_frame`, dispatch, the mux batcher and ingress |
| T2 | per connection, stream, slot or peer, or per batch of constant work | dialing, opening a slot, the credit sweep |
| T3 | at setup, shutdown, on errors and on first use | builders, discovery, handshakes |

The code site can mislead. Socket buffers are set once per connection, but
when they were set after the first data, the TCP window collapsed for the
life of the connection, and throughput fell 100 to 350 times. That is a
per-byte cost. See
[Socket buffers sized after data was in flight](rdma-design.md#socket-buffers-sized-after-data-was-in-flight).

For scale:

- One 1 KB message over TCP on loopback takes about 11.4 µs one way. Two
  cross-thread wakes are 84% of what the transport adds. See
  [Where the per-message time goes](rdma-design.md#where-the-per-message-time-goes).
- One mux record costs 5 to 9 µs of frontend CPU. See
  [The reader pump leaves the mux data path](../operations/response-plane-performance.md#the-reader-pump-leaves-the-mux-data-path).

So 100 ns per message is about 1% of either budget.

## Costs that shipped

Each of these costs reached velo, and each fix came with a test. A change
that brings one back fails its test, unless the change goes around the place
that the test counts.

| Cost | Fix | Test |
|---|---|---|
| A cancellation future built per frame on the node-wide teardown token. Every build and drop takes a lock that all connections share. | Build one future per connection, on a child token. | `a_connection_arms_its_teardown_once` |
| A `tokio::time::timeout` per record. Each one takes the time driver's lock twice. | One timer per stream, moved forward only inside half a window. | `a_thousand_records_arm_the_heartbeat_timer_a_handful_of_times` |
| A full decode of each stream item, only to see whether it ends the stream. | Decide from the first bytes. | `classifying_an_item_does_not_allocate` |
| A task per stream bind, to time out its accept window. | One deadline queue that the credit sweep drains. | `a_finished_zero_rtt_stream_leaves_no_task_behind` |
| Three writes per large frame. | Stage the preamble and header in a stack buffer, so a frame is two writes. | `test_direct_path_stages_prefix_into_two_writes` |
| A read buffer that doubled for a large frame, and copied at each step. | Reserve the whole frame once the preamble is read. | `test_decode_reserves_capacity_for_announced_frame` |
| Socket buffers set on the accepted socket after data could flow. | Set them on the listener, before `listen`. | `socket_buffers_sizes_the_listener_or_leaves_it_to_the_kernel` |
| One write per frame. | The coalescing writer. | `burst_coalesces_writes` |
| A `tokio::time::timeout` per item on an ordered lane. | One idle timer per lane. | `lane_arms_its_idle_timer_a_handful_of_times_not_once_per_item` |
| QUIC packets above 6550 bytes, which GSO loses. | Clamp `max_mtu` to 6550. | `large_frames_round_trip_at_a_jumbo_mtu` |

Some changes were measured and did not help. Read these lists before you
propose an optimization:

- [What is not a lever](../operations/response-plane-performance.md#what-is-not-a-lever)
- [Settings that are not levers](../guides/tune-batched-streaming.md#settings-that-are-not-levers)
- [Settings that did not help](../operations/quic-performance.md#settings-that-did-not-help)

## Hold a cost in a test

A comment does not stop a regression. Count the cost in a test, and make the
count fail when the cost comes back.

- **Allocations.** `crate::test_alloc::allocations_in(|| ...)` counts the
  allocations of a closure. It works only in `lib/velo` unit tests, because it
  is that test binary's global allocator, and it counts only the calling
  thread.
- **Builds of a future or a timer.** Route every build through one function
  that bumps a counter in tests and compiles to nothing in release:

  ```rust
  #[cfg(test)]
  fn note_teardown_arm() {
      let _ = TEARDOWN_ARMS.try_with(|n| n.fetch_add(1, Ordering::Relaxed));
  }
  #[cfg(not(test))]
  #[inline(always)]
  fn note_teardown_arm() {}
  ```

  The test counts only builds that go through the seam. New code that builds
  the same future somewhere else escapes the test, so a review must check for
  that. To prove a bypass, add the seam call back at the new site in a
  scratch copy and run the test: if it fails there, the change went around
  it.
- **Tasks.** Compare `Handle::current().metrics().num_alive_tasks()` with a
  baseline after the work ends.
- **Writes and batches.** Use the recording writer in
  `transports/coalesce/tests.rs`, or the ratios from
  `observability::test_helpers::MetricSnapshot` end to end.

Prove each test before you trust it: revert the fix, and check that the test
fails. See rule 11 in [Measurement rules](../operations/benchmarking.md#measurement-rules).

## Compare two commits on one machine

1. **Build each arm in its own worktree and target directory.** Use the same
   features for both. Do not share one `CARGO_TARGET_DIR` between two
   worktrees of this repo: cargo gives velo's build the same name in both
   and decides freshness by file times, so one arm can run the other arm's
   code with no warning. Check that each binary names its own worktree's
   source paths (`strings <binary> | grep <worktree>`).
2. **Count before you time.** A count of allocations, timer arms, tasks or
   writes is a fact on any machine. Next best is `perf stat -e instructions:u`
   divided by the work done. Wall time comes last.
3. **Run a harness that reaches the changed code.** `tx_budget` attributes
   the TCP and UDS send path layer by layer. `throughput` and `ping_pong`
   cover the messenger. `response_plane_bench`, `batched_streaming` and
   `stream_lifecycle` cover the mux, and they are the only harnesses that
   install metrics. See [Benchmarking](../operations/benchmarking.md).
4. **Interleave the arms.** Pin both to the same cores, run at least five
   reps of each, and change which arm goes first.
5. **Call a difference real only when the ranges do not overlap.** Report
   latency beside throughput.
6. **Check the memory kept per operation.** Run each arm at N and at 4N
   operations, and divide the RSS difference by 3N. Growth is not yet a
   leak. Tasks of streams that ended can pile up in a loop that never
   yields, and glibc keeps freed pages. Read `num_alive_tasks()` and RSS
   after `malloc_trim(0)` before you call it a leak. In
   `anchor_setup_bench`, two tasks per stream grew RSS to 24 GB at 16M
   lifecycles, and `malloc_trim(0)` gave it all back.
7. **Label the result.** A number from a shared or dev machine is
   directional. A number for this book needs an exclusive node and the
   [Measurement rules](../operations/benchmarking.md#measurement-rules).
