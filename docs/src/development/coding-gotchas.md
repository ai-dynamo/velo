# Coding gotchas

Each entry is a lesson from a race or a flaky test: the rule, the failure it
prevents, how to test for it, and one test that shows it. Read this chapter
before you write, review, or test code here.

## A tokio interval's first tick runs when its task first runs

**Rule.** `tokio::time::interval` ticks at once, but only when the task that
owns it is first polled. Do not assume that tick has run because the code that
spawned the task has returned. When a test needs to know which ticks run, wait
for a counter that the tick itself moves.

On a loaded host a new task can wait a long time for its first poll. So the
"immediate" tick can land in the middle of the test, after state the test set
up on the belief that no tick would run.

**Failure.** The mux tests run the mux's sweep by hand. They set the sweep
interval to an hour, so the only tick that runs by itself is the first. At that
interval one tick retires every idle batcher. In
`a_late_reply_after_the_idle_fixture_is_not_counted_as_idle`, a late control
reply arrives at the producer after the test releases a held batcher, and
creates a new batcher there. The test waits to see that batcher. In 2 of 360
runs (8 copies and 4 busy loops on one CPU), the producer's first tick ran
only after the release. It retired the new batcher before the test saw it, and
the test waited for its full timeout. The counters showed that the reply was
not lost: the batch was sent and received, nothing was dropped, and the new
producer batcher refused the grant it carried, because that stream had ended.

**Test.** Read a counter that the tick moves, and wait for it before you set
up state that a tick would change. The mux keeps such a counter in test builds
(`MuxCore::ticks`). To show that a late tick causes the failure, run one tick
by hand at the point where the late tick landed. The test must then fail
every time.

Test: `streaming::messenger_mux::tests::a_late_reply_after_the_idle_fixture_is_not_counted_as_idle`
(`lib/velo/src/streaming/messenger_mux/tests.rs`).

## A gauge set from a count read in a separate step can go stale

**Rule.** Do not set a gauge to a count that you read in a step apart from
the change to that count. Move the gauge with `inc()` and `dec()` where the
count changes, or set it under the lock that orders the changes.

Two threads can each change the count and then set the gauge. The thread
that read the older count can set it last. The gauge then holds a value
that is not true, and keeps it until the next change, which may not come
for a long time once a burst ends. An `inc()` or `dec()` is one atomic step
on the gauge, so no older value can land last.

**Failure.** The messenger set `velo_messenger_pending_responses` from the
value that its `fetch_add` or `fetch_sub` returned. With sends and drops at
the same time, it read 1 with every awaiter dropped, or 2 with one slot
held. The rendezvous store set `velo_rendezvous_active_slots` from
`DashMap::len()` after a free, and two frees at once could set their
lengths in either order.

**Test.** Line threads up with a `std::sync::Barrier`, so that their changes
overlap. Then read the gauge while every thread waits at the next barrier,
and compare it with the true count. Run many rounds in one test, because
the window is a few instructions wide. Set from a count, both tests below
failed in under 2,000 rounds on every run.

Tests: `messenger::common::pending_gauge_tests::concurrent_drops_leave_the_gauge_at_zero`
(`lib/velo/src/messenger/common/pending_gauge_tests.rs`) and
`rendezvous::active_slots_tests::concurrent_frees_leave_the_gauge_at_zero`
(`lib/velo/src/rendezvous/active_slots_tests.rs`).
