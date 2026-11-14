# Coding gotchas

Each entry is a lesson from a race or a flaky test: the rule, the failure it
prevents, how to test for it, and one test that shows it. Read this chapter
before you write, review, or test code here.

## Two counts dropped on two threads: compare them under a lock

**Rule.** When two threads each drop one of two counters, and the last one to reach 0 must act, do not decrement your counter and then load the other one. Either check both counts under a lock that orders the two checks, or make all four operations `SeqCst`. The shutdown gate uses `SeqCst` for this reason ([Shutdown and drain](../concepts/shutdown.md)).

Decrement-then-load is the store-buffer pattern. On a weakly ordered CPU each thread can read the other's old value, so neither acts.

**Failure.** A rendezvous slot is freed when its reference count and its read-lock count are both 0. The owner drops the reference on one thread, and the last reader drops its read lock on another. With decrement-then-load, neither side freed the slot, and it stayed for the life of the process. The rule in `DataStore::end_lease` now is: whichever call takes a count to 0 calls `try_free`. Each count changes under a read guard on the slot's `DashMap` shard, and `try_free` checks both counts under that shard's write lock. So the later of the two `try_free` calls sees both zeros.

**Test.** Start both sides on a `Barrier` and repeat a few thousand times in one test, then run that test 50 times. On an aarch64 host the test below failed 2 of 21 runs with decrement-then-load, and passed 50 of 50 with the shard-lock rule.

Test: `rendezvous::reclaim_tests::an_owner_and_its_last_reader_leaving_together_free_the_slot`
(`lib/velo/src/rendezvous/reclaim_tests.rs`).

## A test cannot force a tokio scheduling outcome by arranging wakes

**Rule.** Do not assert a tokio scheduling outcome that the test arranges with wakes, such as a stall behind the LIFO slot or a task that runs on a given worker. Assert the workaround or the invariant instead.

Another wake can reach the task first: a heartbeat, a credit reply, or a timer. If the task's poll is still running at that time, tokio only marks the task notified, and the task then runs on another worker.

**Failure.** A test that asserts the stall passes hundreds of times and still fails on a loaded CI runner.

**Test.** Force the other interleaving: pause threads under a debugger, or add a concurrent wake. Then see whether the assertion still holds. Keep a demonstration of the stall as an ignored test.

Test: `block_in_place_keeps_the_stream` is the live test of the workaround, and `a_held_worker_thread_cuts_the_stream` is the ignored demonstration of the stall (`lib/velo/tests/streaming/held_thread.rs`).

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
