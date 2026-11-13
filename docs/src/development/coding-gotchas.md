# Coding gotchas

Each entry is a lesson from a race or a flaky test: the rule, the failure it
prevents, how to test for it, and one test that shows it. Read this chapter
before you write, review, or test code here.

## A cancel wakes child tokens before the parent's own waiters

**Rule.** When two tasks stop on one cancel, and one must act first, put the
one that goes second on a child token of the first one's token. Do not give
them sibling tokens, and do not put the first one on the parent token itself.

`CancellationToken::cancel` (tokio-util 0.7) wakes the waiters of each child
token before the waiters of the token itself. It moves a child's own children
up and wakes them after that child. So a task on a child token wakes before a
task on the parent token. A child token per connection removes a lock that all
connections share from the per-frame poll, so this ordering comes up each time
a loop moves to a child token.

**Failure.** The QUIC listener's stream readers wait on a child token. As
children of the node-wide teardown token, they woke before the connection
task. A reader dropped its stream first, and the peer got a STOP_SENDING
instead of the connection close. The dialer reads that as frames left unread,
and warns on every peer restart. The readers' tokens are now children of the
connection task's token, so the connection task wakes first.

**Test.** A wake order on a multi-thread runtime is not stable. Hold the
cancel at the point between the two wakes: build your own child token before
the code under test starts, so the cancel reaches it last, and poll it with a
waker that blocks until the test releases it. Then check what the peer sees
while the cancel is held.

Test: `transports::quic::transport::tests::teardown_closes_the_connection_before_child_readers_stop_streams`
(`lib/velo/src/transports/quic/tests.rs`).
