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

## A dead peer must hold its port: a listener or a bound socket, by what the test asserts

**Rule.** A test that needs a peer which never answers must hold that peer's
port for the whole test. Do not bind a port and close it again: the port is
free once it is closed, and in a full test run another test can bind it at
any moment. Pick the holder by what the test asserts:

- The connect must succeed, but no protocol reply may come (a ZMQ handshake,
  for example): hold a `TcpListener` and never accept from it. The kernel
  completes the connect into the listener's backlog.
- The connect must fail: hold a socket that is bound and never listens
  (`tokio::net::TcpSocket::bind`, no `listen`). The kernel refuses the
  connect, and the bind keeps every other socket off the port. Do not set
  `SO_REUSEADDR` or `SO_REUSEPORT` on it, and keep it in a named variable to
  the end of the test.

The same goes for a check that a socket is closed. Do not connect to the
port a transport has just freed: another test can have bound it. Check the
process's own file descriptors instead (`/proc/self/fd`).

**Failure.** `test_writer_task_drains_on_connect_failure` made its dead peer
by binding a port and dropping it. With another listener on that port, the
connect succeeded and the writer never failed: the test hung, 3 times in 3.
`closed_releases_running_listener` connected to the port its transport had
just closed. With another listener on that port, the connect succeeded and
the test failed, 3 times in 3. It once failed in a full `--lib` run. The old
forms failed 0 times in 76 full `--lib` runs here (4 at once on 4 CPUs), so
another test taking the freed port is the likely cause, not a proven one.
The new check shows that `closed` does release the socket: 100 of 100 runs,
half of them on two busy CPUs.

**Test.** Bind a listener to the freed port before the step that must not
reach a peer: the old tests then fail every time. With the port held, the
bind fails with `EADDRINUSE`, and `test_writer_task_drains_on_connect_failure`
asserts that.

Tests: `transports::tcp::transport::tests::test_writer_task_drains_on_connect_failure` and
`transports::tcp::transport::tests::closed_releases_running_listener`
(`lib/velo/src/transports/tcp/tests.rs`).
