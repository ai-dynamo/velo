# Coding gotchas

Each entry is a lesson from a race or a flaky test: the rule, the failure it
prevents, how to test for it, and one test that shows it. Read this chapter
before you write, review, or test code here.

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
