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
