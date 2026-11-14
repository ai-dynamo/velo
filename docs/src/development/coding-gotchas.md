# Coding gotchas

Each entry is a lesson from a race or a flaky test: the rule, the failure it
prevents, how to test for it, and one test that shows it. Read this chapter
before you write, review, or test code here.

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
