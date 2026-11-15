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

## A cancel wakes child tokens before the parent's own waiters

**Rule.** A task on a child token wakes before a task on the parent token.
When you move a task to a child token, check what other tasks stop on the same
cancel, and keep the wake order they had. If one task must act before another
stops, do not rely on the wake order. Make the second task wait on a token
that the first task cancels after it acts.

`CancellationToken::cancel` (tokio-util 0.7) wakes the waiters of each child
token before the waiters of the token itself. It moves a child's own children
up and wakes them after that child. A child token per connection removes a
lock that all connections share from the per-frame poll, so this comes up each
time a loop moves to a child token.

A wake order is not a run order. On a current-thread runtime, tasks run in the
order they woke. On a multi-thread runtime, a task that woke second can run
first. Child-token nesting orders only the wakes. Keep the second task's token
out of reach of the shared cancel, so that only the first task can cancel it.

**Failure.** At teardown the QUIC listener must close each connection before
its stream readers drop their streams. A stream dropped on an open connection
sends STOP_SENDING, and the dialer reads that as frames left unread and warns
on every peer restart. When the readers stopped on the teardown token, a
reader could run before the connection task on a multi-thread runtime, even
when it woke second. Now the connection task calls `close()` and then cancels
a token of its own, and the readers stop only on that token.

**Test.** Cancel the shared token, and do not poll the task that must act
first. Check that the peer sees nothing from the second task. Then poll the
first task to the end, and check that the peer sees its action. This order is
the worst one a scheduler can choose, so the test fails on every run while the
order is only likely. A loop of the plain test on two workers fails only about
1 run in 100.

Test: `transports::quic::listener::tests::teardown_stops_no_stream_before_the_connection_task_closes`
(`lib/velo/src/transports/quic/listener.rs`).

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
