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

## Two peer tables change in the order of the route

**Rule.** The per-stream transport keeps its own table of peer endpoints, and
it must agree with the backend's routes. Change the stored peer and tell the
observer while you hold that peer's `primary_transport` entry. A register and
an unregister of the same peer then reach the observer in the order they
change the route. An observer installed after the transports start is set
first, and then walks the peers already stored.

**Failure.** An unregister that ran beside a register of the same peer could
tell the stream transport last. The route stayed, the stream entry was gone,
and an attach failed with "peer … not registered" while messages worked. A
`_hello` that arrived after the transports started, but before
`VeloBuilder::build` installed the observer, reached the messenger only.

**Test.** Pause one operation inside a transport hook: a transport whose
`unregister` blocks on a channel. Run the other operation, release the first,
and compare the two tables. For the startup window, a test transport admits a
`_hello` frame from its `start`.

Tests: `transports::tests::a_delayed_unregister_does_not_erase_a_new_stream_registration`
and `transports::tests::a_hello_queued_at_startup_reaches_the_stream_table`
(`lib/velo/src/transports/tests.rs`).

## Hold no lock across a call that can run an error handler

**Rule.** A transport runs a send's `TransportErrorHandler` inline: in the send
when it fails at once, and in `Transport::unregister` when it fails the peer's
queued sends. `VeloBackend` holds the route's `primary_transport` shard while
the transport sends. A handler can call back into the backend. So add no lock
of the backend's own around route writes, and hold no lock or map entry
across `Transport::register`, `Transport::unregister`, or a send.

**Failure.** A peer-table mutex around the route writes deadlocked. An
unregister held the mutex and waited for the shard that a failing send held.
The send's error handler waited for the mutex to register another peer.

**Test.** Give the send an error handler that registers another peer from a
second thread and waits for it with a bound. Make the send fail while an
unregister of its peer runs, then check that the registration finished.

Test: `transports::tests::unregister_does_not_block_registration_from_an_active_send_error_handler`
(`lib/velo/src/transports/tests.rs`).

## A vacant DashMap entry holds its shard

**Rule.** `DashMap::entry` takes the shard's write lock for an occupied and a
vacant entry alike. `if let Entry::Occupied(entry) = entry { entry.remove(); }`
does not move a vacant entry, so its lock lives to the end of the scope. Drop
the entry in every arm before a call that can block or call back.

**Failure.** A second unregister of the same peer found the route gone and
kept the vacant entry across `Transport::unregister`. An error handler there
that registered a peer in the same shard deadlocked.

**Test.** Pick a second peer on the same shard by probing it with
`try_get_mut`. Queue a send whose error handler registers that peer, then
unregister the first peer twice, so that the second call meets a vacant entry.
Check that the handler's registration finished.

Test: `transports::tests::an_absent_route_does_not_block_a_queued_send_error_handler`
(`lib/velo/src/transports/tests.rs`).

## A count made after a hand-off can trail the thing handed off

**Rule.** When a test sees an effect on the far side of a hand-off, such as a
batch at a capture channel or a frame at a peer, it can rely only on what the
code did before the hand-off. A counter, gauge, or flag that the code sets
after the hand-off can still be pending. Wait for that state itself with
`eventually` or a test hook. Do not read it once.

**Failure.** `send_singleton` counts
`velo_streaming_mux_rendezvous_singletons_total` after `fire_singleton`
returns, which is after the batch went to the messenger. The test read the
counter as soon as the batch reached the capture. With 24 copies of the test
running at once, about 1 run in 80 read 0. Run alone, it passed 60 runs of
60.

**Test.** Find what the code does between the hand-off and the state the test
reads. Run the test in a loop with many copies at once, for example 24. One
copy on a quiet host hides the race.

Test: `streaming::messenger_mux::peer_batcher::tests::instruments::singleton_records_are_counted_by_type`
(`lib/velo/src/streaming/messenger_mux/peer_batcher/tests/instruments.rs`).

## Hold a state open with a gate, not with a slow operation

**Rule.** When a test needs a state to last while it checks something, such
as an operation in flight, hold that state with a gate the test controls.
Release the gate when the check is done. Do not size the work so that it
probably outlasts the check. Then assert that the state still held when the
check ran, so a gate that stops holding fails the test.

**Failure.** The UCX reaper test needs GETs in flight while a scan closes an
idle endpoint. It sent 64 MiB over the tcp lane and expected the transfer to
outlast a 10 ms idle window. With 16 copies of the test running at once,
about 1 run in 125 finished the transfer first and failed the precondition.
Over the tcp lane a GET needs the owner's progress thread, so the test now
holds that thread with the `progress_hold` seam until the scan has run.

**Test.** Put the gate in the code under test as a `#[cfg(test)]` seam if it
has none. Hold it, check that the state is in place, run the check, then
release it. Run the test in a loop with many copies at once, for example 16.

Test: `transports::ucx::transport::tests::an_endpoint_with_an_inflight_get_is_not_reaped`
(`lib/velo/src/transports/ucx/tests.rs`).
