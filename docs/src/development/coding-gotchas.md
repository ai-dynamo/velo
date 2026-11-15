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

## A test of a timeout must control the clock the timeout reads

**Rule.** When code decides something from elapsed time, such as an idle
reaper or a watchdog, a test of that decision must control the clock. Waiting
out a real timeout tests the scheduler too: on a loaded host a starved thread
lets time pass with nothing wrong, and the code then makes the correct
decision at a moment the test did not expect. Route every time read of the
decision through one clock that a test can freeze and move. In a normal build
that clock must be plain `Instant::now()`.

Then check the property itself, not only its outcome. An outcome such as "the
endpoint is still open" also depends on how fast the peer sends. A count of
the wrong decision does not.

**Failure.** The UCX idle reaper closes an endpoint that nothing has used for
its timeout (500 ms in the tests). The tests that check that a peer's frames
count as use failed 20 times in 48 copies of the UCX tests run 24 at once.
A's reaps were correct: in each failure no frame from B had reached A for
more than the timeout. B's first frame waited 630 to 1250 ms for B's
`ucp_ep_create`, and at the highest load even a ready path took 600 to
2250 ms to deliver one frame.

**Test.** `ReaperClock` (`transports/ucx/clock.rs`) is the reaper's clock. The
tests freeze it, move it a quarter timeout at a time between frames, and wait
for a scan after each move. A test-only count, `eps_reaped_while_active`,
records any reap of an endpoint whose peer sent a frame within the timeout.
A correct reaper never adds to it, at any load. To show a test is not empty,
remove the inbound stamp: the count must rise.

Test: `transports::ucx::transport::tests::a_peer_that_keeps_sending_responses_keeps_its_endpoint`
(`lib/velo/src/transports/ucx/tests.rs`).

## Wait for facts that stay true, and keep the test's own steps out of the state it checks

**Rule.** Do not wait to see a state that the code under test ends by itself.
Wait for a fact that stays true once it holds: a counter that only grows, a
`OnceLock` that is set, a channel that has closed. Also check that the test's
own steps do not change what it checks. A read can return credit, and a
send can fail because the event under test has already happened.

**Failure.** Two mux watchdog tests failed under load. The watchdog ends a
stream when its sender holds credit and sends nothing for three heartbeat
windows. `a_watchdog_firing_closes_its_slot` sent three records after the
attach. The watchdog fired before the sends, which is what the test wants,
and the send failed. `the_watchdog_does_not_fire_while_its_sender_is_parked`
read the records of a parked sender. The first reads gave credit back, so the
watchdog correctly fired before the last read.

**Test.** Force the delay that load causes: wait 300 ms before the sends, or
four windows after the first read. The old tests then fail every time. The
fixed tests wait for the anchor to go, the sender's channel to close, and no
live slot. They hold credit back until the test returns it with one sweep.

Tests: `streaming::messenger_mux::tests::a_watchdog_firing_closes_its_slot`
and `streaming::messenger_mux::tests::the_watchdog_does_not_fire_while_its_sender_is_parked`
(`lib/velo/src/streaming/messenger_mux/tests.rs`).

## A connection is not idle until both ends have set it up

**Rule.** A timer that closes a connection for being idle must not count the
time before the peer has answered. Our side's setup ending is not enough: the
peer may still be setting up its side, and nothing reaches us until it has.
End setup on the first message from the peer, and bound it, so a peer that
never answers, or died during setup, still loses its connection.

**Failure.** The UCX idle reaper closed an endpoint 500 ms after its last use.
A created its endpoint to B by sending to B. B's first frame back waited for
B's own `ucp_ep_create`, which took 630 to 1250 ms under load. A saw nothing
from B for longer than the timeout, closed its endpoint, and B's next
Messages to A were lost. The reaper did nothing wrong by its own rule; the
rule counted setup as idle time.

**Test.** Hold A's reaper clock. Let A send to B, move A's clock two
timeouts with nothing from B, and check that A kept the endpoint. Then let B
answer, and check that idle time counts from B's frame. A second test moves
the clock past the setup bound with no frame from B, and checks that the
endpoint is closed.

Tests: `transports::ucx::transport::tests::an_endpoint_is_not_reaped_while_its_peer_sets_up`
and `transports::ucx::transport::tests::an_endpoint_whose_peer_never_sends_is_reaped_after_the_setup_bound`
(`lib/velo/src/transports/ucx/tests.rs`).
