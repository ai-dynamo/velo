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
