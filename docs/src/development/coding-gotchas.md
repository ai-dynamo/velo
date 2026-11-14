# Coding gotchas

Each entry is a lesson from a race or a flaky test: the rule, the failure it
prevents, how to test for it, and one test that shows it. Read this chapter
before you write, review, or test code here.

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
