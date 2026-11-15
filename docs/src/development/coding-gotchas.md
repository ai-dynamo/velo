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

## `echo "$out" | grep -q` fails under `pipefail` when grep wins the race

**Rule.** In a bash script with `set -o pipefail`, do not pipe a variable
into `grep -q` or `grep -m`. Use a here-string: `grep -q X <<<"$out"`.

**Failure.** `grep -q` exits at its first match and closes the pipe. If
`echo` still has output to write, it gets SIGPIPE, the pipeline exits 141,
and `pipefail` makes the check fail although the text is there. bash writes
`echo`'s output one line per `write()`, so this can happen at any size.
The semver self-test failed once in CI on a case whose output held the
expected line; here it failed 1 time in 100 runs on two busy CPUs, 1 time
in 2000 for one check on one busy CPU, and every time for output larger
than the pipe buffer. `scripts/check-semver.sh` used the same form to pick
the crates a change touches, so it could skip a changed crate's check
(1 to 2 times in 500, idle, for lists of 5 and 200 files).

**Test.** Run the pipe form on output larger than the pipe buffer: it fails
nearly every time, and the here-string form never does. Then search the
scripts for the pipe form, with any options.

Test: the two `(pipe)` cases in `scripts/check-semver.test.sh`.

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

## A cancel wakes child tokens before the parent's own waiters

**Rule.** A task on a child token wakes before a task on the parent token.
When you move a task to a child token, check what other tasks stop on the same
cancel, and keep the wake order they had. If one task must wake before another,
put the second one on a child of the first one's token.

`CancellationToken::cancel` (tokio-util 0.7) wakes the waiters of each child
token before the waiters of the token itself. It moves a child's own children
up and wakes them after that child. A child token per connection removes a
lock that all connections share from the per-frame poll, so this comes up each
time a loop moves to a child token.

A wake order is not a run order. On a current-thread runtime, tasks run in the
order they woke. On a multi-thread runtime, a task that woke second can run
first. When the order must hold there too, make the second task wait for the
first one, for example on a token that the first one cancels when it is done.

**Failure.** The QUIC listener's stream readers wait on a child token. As
children of the node-wide teardown token, they woke before the connection
task. A reader dropped its stream first, and the peer got a STOP_SENDING
instead of the connection close. The dialer reads that as frames left unread,
and warns on every peer restart. The readers' tokens are now children of the
connection task's token, so the connection task wakes first, as it did when
all of them waited on the node-wide token. On a multi-thread runtime a reader
can still run first, then and now: this keeps the old order, and does not make
it certain.

**Test.** Use a current-thread runtime, where the wake order is the run
order. Hold the cancel at the point between the two wakes: build your own child token before
the code under test starts, so the cancel reaches it last, and poll it with a
waker that blocks until the test releases it. Then check what the peer sees
while the cancel is held.

Test: `transports::quic::transport::tests::teardown_closes_the_connection_before_child_readers_stop_streams`
(`lib/velo/src/transports/quic/tests.rs`).

## A peer's close does not mean its frames were read

**Rule.** When a peer closes a connection, let each reader read to its own
end. Stop readers only when this side ends the connection itself.

A QUIC peer can close as soon as it has an acknowledgement for its last
frames. The acknowledgement says the frames reached this side's buffer. It
does not say that a reader has decoded and routed them. On a closed
connection, a read returns the buffered bytes and then the end, so a reader
left alone delivers them and stops.

**Failure.** The QUIC listener's connection task stopped its stream readers
when `accept_bi` failed, which is what a peer's close looks like. A reader
that was behind lost the frames still in its buffer, and the dialer counted
them as delivered. Two delivery-count tests failed 5 runs in 50 under
parallel load: one or two frames of 256 were neither delivered nor failed.

**Test.** Pause the reader inside the routing of its first frame. Send the
rest, finish the stream, and wait for the acknowledgement. Close the
connection from the peer, and let the connection task see the close. Then
release the reader, and count the frames that arrive.

Test: `transports::quic::listener::tests::a_peer_close_preserves_acknowledged_frames`
(`lib/velo/src/transports/quic/listener.rs`).
