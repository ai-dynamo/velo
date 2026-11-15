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

## Count the failure, not the time it costs

**Rule.** A unit test must not check speed. When a defect shows up as lost
time, such as a retry or a loss-probe timeout, count the event that costs the
time: lost packets, retries, timer fires. A speed check belongs in a
benchmark. Keep a timeout in a test only to catch a hang, and make it long.

**Failure.** The QUIC large-frame tests checked that 200 round trips of
64 KiB took less than 2 s, because a send batch over the UDP datagram limit
is lost on every flight. With 12 copies of the lib suite running at once,
they took 2.1 s to 9.7 s and failed in 45 of 72 copies, with no packet lost.
They now read quinn's `lost_packets` through the `record_dialed` test seam.

**Test.** Run the test with many copies of the suite at once. Then break the
code on purpose, for example make `clamp_to_gso_batch` return its input, and
check that the count catches it: the jumbo test then counts about 2,500 lost
packets.

Test: `transports::quic::transport::tests::large_frames_round_trip_at_a_jumbo_mtu`
(`lib/velo/src/transports/quic/tests.rs`).

## A timeout in the code under test is part of the test's clock

**Rule.** When the code under test has its own timeout, such as an idle
timeout, a lease, a shutdown grace, or `TCP_USER_TIMEOUT`, a stalled host can
make it fire before the test reaches its point. Set that timeout long in the
test, through a test seam if needed. Then wait for the event the test checks,
not for a fixed time or a quiet spell, so a passing run stays short.

**Failure.** Several tests failed on a loaded host for this reason:

- A QUIC peer with a 1.5 s idle timeout timed out while the node it watched
  was stalled between its frame and its close.
- `closed()` gave up at its 2 s close wait. An acknowledgement read after a
  stall gives quinn an RTT sample that includes the stall, and the close's
  draining period then outlasts 2 s.
- A rendezvous lease of 2 s left 1 s for a renewal, and the owner reaped it.
- The TCP stream pump test took over 30 s to encode its frames, and the
  socket's `TCP_USER_TIMEOUT` aborted the connection to a peer that never
  reads.

**Test.** Make the stall happen. Block the thread at the point you suspect,
with `std::thread::sleep` in a scratch build, or pause the whole test process
with `SIGSTOP` for 50 ms to 2 s at random points and resume it with
`SIGCONT`. Many copies pinned to one or two CPUs with `taskset` also help.
Check that the old test fails and the new one passes. Then remove the
behavior the test checks, for example skip the close, and check that the new
test still fails.

Test: `transports::quic::transport::tests::the_senders_exit_closes_its_connection`
(`lib/velo/src/transports/quic/tests.rs`).

## Test timer logic on a paused clock

**Rule.** When a test checks what a timer does, such as an idle reap, run it
on tokio's paused clock (`tokio::time::pause()` on a current-thread runtime).
The clock then moves only when every task is idle, so the gaps the test makes
are the gaps the code sees. On the real clock, a host that stops the process
for longer than the timer's period changes the outcome.

**Failure.** `lane_is_not_reaped_while_items_keep_arriving` fed a lane every
5 ms under a 100 ms idle TTL, on the real clock. Pausing the process with
`SIGSTOP` for 50 to 300 ms at random points reaped the lane in 22 runs of 24.

**Test.** Pause the process as above. Then break the timer, for example reap
without comparing with the last item, and check that the test fails.

Test: `messenger::server::lanes::tests::lane_is_not_reaped_while_items_keep_arriving`
(`lib/velo/src/messenger/server/lanes.rs`).

## A port you closed is free for any other test to take

**Rule.** A test that needs a peer which never answers must hold that peer's
port for the whole test. Do not bind a port and close it again to make a
"dead" address: the port is free once it is closed, and in a full test run
another test can bind it at any moment. Keep a `TcpListener` bound and never
accept from it. The kernel completes the TCP connect, but no protocol
handshake ever comes back.

**Failure.** The ZMQ drain test made its dead peer by binding a port and
dropping the listener. In a full `--lib` run another test's ZMQ ROUTER took
the port, the "dead" peer received every frame, and the test failed with no
frame failed. It happened 1 time in 72 full runs (24 at once on 8 CPUs). The
same test passed every time alone.

**Test.** Bind a ZMQ ROUTER to the freed port before the drain: the old test
then fails every time. With the port held, it cannot be taken.

Test: `transports::zmq::transport::tests::a_stopping_sender_drains_within_its_budget`
(`lib/velo/src/transports/zmq/transport.rs`).

## Read what is left of a deadline just before the wait it bounds

**Rule.** When a shared deadline bounds a series of blocking calls, compute
the time left immediately before each call that blocks, not before the work
that leads up to it. Setup between the reading and the wait (creating a
socket, taking a lock, a slow allocation) is spent twice: once from the
deadline, and once more as the wait the stale reading allows.

**Failure.** A stopping ZMQ sender took each send's timeout from its drain
budget, then created the socket for the send. When creating the socket used
up the budget, the send still waited the whole earlier timeout, so the drain
ran a second past its one-second budget.

**Test.** Hold the code at the point between setup and the wait (a per-thread
test hook) until the deadline has passed, then check that the call fails at
once. With the stale reading, it waits the whole earlier timeout.

Test: `transports::zmq::transport::tests::a_stopping_sender_rechecks_its_budget_after_socket_setup`
(`lib/velo/src/transports/zmq/transport.rs`).

## A dead peer must hold its port: a listener or a bound socket, by what the test asserts

**Rule.** A test that needs a peer which never answers must hold that peer's
port for the whole test. Do not bind a port and close it again: the port is
free once it is closed, and in a full test run another test can bind it at
any moment. Pick the holder by what the test asserts:

- The connect must succeed, but no protocol reply may come (a ZMQ handshake,
  for example): hold a `TcpListener` and never accept from it. The kernel
  completes the connect into the listener's backlog.
- The connect must fail: hold a socket that is bound and never listens
  (`tokio::net::TcpSocket::bind`, no `listen`). The kernel refuses the
  connect, and the bind keeps every other socket off the port. Do not set
  `SO_REUSEADDR` or `SO_REUSEPORT` on it, and keep it in a named variable to
  the end of the test.

The same goes for a check that a socket is closed. Do not connect to the
port a transport has just freed: another test can have bound it. Check the
process's own file descriptors instead (`/proc/self/fd`, on Linux; where
it is missing, skip the check rather than fail).

**Failure.** `test_writer_task_drains_on_connect_failure` made its dead peer
by binding a port and dropping it. With another listener on that port, the
connect succeeded and the writer never failed: the test hung, 3 times in 3.
`closed_releases_running_listener` connected to the port its transport had
just closed. With another listener on that port, the connect succeeded and
the test failed, 3 times in 3. It once failed in a full `--lib` run. The old
forms failed 0 times in 76 full `--lib` runs here (4 at once on 4 CPUs), so
another test taking the freed port is the likely cause, not a proven one.
The new check shows that `closed` does release the socket: 100 of 100 runs,
half of them on two busy CPUs.

**Test.** Bind a listener to the freed port before the step that must not
reach a peer: the old tests then fail every time. With the port held, the
bind fails with `EADDRINUSE`, and `test_writer_task_drains_on_connect_failure`
asserts that.

Tests: `transports::tcp::transport::tests::test_writer_task_drains_on_connect_failure` and
`transports::tcp::transport::tests::closed_releases_running_listener`
(`lib/velo/src/transports/tcp/tests.rs`).
