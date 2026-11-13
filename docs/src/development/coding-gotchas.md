# Coding gotchas

Each entry is a mistake that shipped or nearly shipped, the failure it caused, and the test that now catches it.

## Two counts, two threads: compare under a lock, not with loads

A rendezvous slot is freed when its reference count and its read-lock count are both 0. The owner drops the reference on one thread, and the last reader drops its read lock on another.

The wrong rule: each side decrements its own count, loads the other count, and frees if both are 0. This is the store-buffer pattern. On a weakly ordered CPU each thread can read the other's old value, so neither frees, and the slot stays for the life of the process. On an aarch64 host, `an_owner_and_its_last_reader_leaving_together_free_the_slot` failed 2 of 21 runs with this rule.

The rule in `DataStore::end_lease`: whichever call takes a count to 0 calls `try_free`. Each count changes under a read guard on the slot's `DashMap` shard, and `try_free` checks both counts under that shard's write lock. So the later of the two `try_free` calls sees both zeros. The test passed 50 of 50 runs with this rule.

The same pattern applies to any pair of counters that two threads drop at once. Either take a lock that orders the two checks, or make all four operations `SeqCst`. The shutdown gate uses `SeqCst` for this reason ([Shutdown and drain](../concepts/shutdown.md)).

## A test cannot force a tokio scheduling outcome by arranging wakes

The wrong test: it arranges wakes so that a task stalls behind another one in the LIFO slot, or runs on a chosen worker thread, and then asserts the stall. Another wake can reach the task first: a heartbeat, a credit reply, or a timer. If the task's poll is still running at that time, tokio only marks the task notified, and the task then runs on another worker. Such a test passes hundreds of times and still fails on a loaded CI runner.

To check a test like this, force the other interleaving: pause threads under a debugger, or add a concurrent wake. Then see whether the assertion still holds. Assert the workaround or the invariant instead of the stall, and keep a demonstration of the stall as an ignored test.

The test that shows this is in `lib/velo/tests/streaming/held_thread.rs`. `block_in_place_keeps_the_stream` is the live test of the workaround. `a_held_worker_thread_cuts_the_stream` is the ignored demonstration of the stall.
