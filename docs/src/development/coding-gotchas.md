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

## A flush close cannot cancel a UCX operation; only a FORCE close can

**Rule.** At teardown, an endpoint that still has a UCX request waiting for
its peer must get a FORCE close, not a flush close. A flush close waits for
the request. Once the endpoint is closed, nothing can cancel the request: a
second close, FORCE or not, is refused. `ucp_worker_destroy` does not complete
it either. It deletes the endpoint without purging the requests that wait on
it, so their callbacks never run, and whatever a callback was meant to free
leaks. Give the operation a bounded time to finish first, then FORCE-close its
endpoint. The FORCE close purges the requests with `UCS_ERR_CANCELED` and runs
the callbacks while the worker still exists.

**Failure.** Over TCP a GET waits for its peer to send the data back. When
the puller shut down while a 32 MiB GET was still in flight, the flush close
waited one second, the FORCE close after it did nothing, and the worker was
destroyed with the GET still waiting. Its rkey and its request leaked, and
`shutdown_with_inflight_get` and `shutdown_resolves_parked_unmap_and_get`
failed with "an unpacked rkey outlived the transport". It happened only under
load, where a peer is slow: 8 failures in 240 runs (48 copies of the UCX test
module at once, on 8 CPUs).

**Test.** Make the peer stop answering, instead of waiting for load to make it
slow. Hold the puller's progress thread so it reads nothing, let the owner run
one whole pass so it starts to reply, then hold the owner and shut the puller
down. Then check that the puller's count of unpacked rkeys is zero. Do not
let the owner run again once the puller is gone. UCX 1.22 can abort an owner
whose puller vanished mid-GET: it calls `ucs_fatal` when it reads a GET
request from a gone puller, and it can fail an assertion while it discards
the endpoint that still holds queued replies. So run the scenario in a child
process that exits once the puller is checked. A crash in UCX then fails one
test, not the whole test binary.

Tests: `transports::ucx::transport::tests::shutdown_cancels_a_get_its_peer_stopped_answering`
and `transports::ucx::transport::tests::an_owner_survives_a_puller_that_vanishes_mid_get`
(`lib/velo/src/transports/ucx/tests.rs`).
