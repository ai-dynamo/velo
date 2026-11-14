# Coding gotchas

Each entry is a lesson from a race or a flaky test: the rule, the failure it
prevents, how to test for it, and one test that shows it. Read this chapter
before you write, review, or test code here.

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
