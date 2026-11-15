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
