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
