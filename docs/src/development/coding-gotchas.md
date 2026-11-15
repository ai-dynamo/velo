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
