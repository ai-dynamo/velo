Status: planned
Updated: 2026-11-20

# Staged transfers on a named route

The messenger change lets a send name its transport. A send that would stage its payload is refused (`TransportChoiceError::WouldStage`), so no named call moves its data off its route. This branch is where staged payloads get a route of their own. It starts from the head of three review passes; its last three commits are red tests.

## The rule (the one the three passes kept circling)

Every frame of a staged transfer keeps to the call's route:

- A request of the transfer (`_rv_acquire`, `_rv_pull`, `_rv_detach`, `_rv_release`, `_rv_lease_renew`, `_rv_metadata`, `_rv_ref`) goes over the transfer's route, fixed once for the whole transfer, and never falls back.
- A reply, data or error, goes over the transport its request came in on.
- A chunk is sized by the live capacity of the transport that carries its reply.
- A frame that cannot keep to the rule fails with a typed error and goes nowhere else.
- **Strict replies end at once.** When the owner cannot send a strict `_rv_*` reply on the arrival transport, it does not send the reply there and does not leave the consumer to wait for its bound. It sends the messenger's final error reply instead, carrying the typed error (`NotServedByPeer`, `CapacityTooSmall`). That final reply may take another transport, as any reply may. The consumer fails at once, and the call ends with that error. Only when no transport at all reaches the requester does the request wait for its bound. Test both outcomes.
- The RDMA read is the one exception, and only where the call allows it: a call that named no transport, or named the transport that owns the RDMA registry.

## Open findings from pass 3 (red tests on this branch)

1. **RDMA gate compares against a literal key.** With `ucx` for messaging and `ucx-rma` via `add_ucx_transport`, a call named `ucx` did a GET on `ucx-rma`, and a call named `ucx-rma` skipped RMA. Compare the named transport with the transport instance whose endpoint the RDMA registry owns, not with the backend kind `"ucx"`. Test: "Test RDMA with two named UCX transports".
2. **An active get reselects its route per frame.** `Route::Primary` picks the current primary for each frame, so a registration change after the acquire moved the pulls to TCP. Fix the transfer's route at its first frame and keep it for every later frame. Test: "Test a route change during an active get".
3. (Fixed on the messenger branch.) A timed-out discovery held a response slot. Test: "Test timeout before discovery publishes its send".
4. **Caller-set staging headers get around the named-send guard** (from the bounded confirmation of the messenger change). A named send with a few inline bytes and a caller-supplied `_rv` header (from `register_data` and `.headers()`) passed the `WouldStage` check, which reads only the inline length, and the receiver fetched the 300 KiB payload over its TCP primary. On a named send, treat caller `_rv*` headers as staging: refuse them, or strip them. Red test on this branch ("Test staging headers on a named send"). In the reviewer's words: explicit staging metadata is a second path into the staged machinery, so every guard that asks "would this be staged?" must look at the header, not only the inline size. (The messenger change refuses `_rv` on any named send.)

## The habit that caught the last class

After any scope cut or redesign, rerun every reproducer from every earlier review pass against the new tree before pushing. The arrival-origin chunk-size regression for an unnamed get (pass 2, finding 2) was found that way after the cut, not by a new review.

## Where the earlier work is

- `97dbde6` and its parents on this branch: strict replies (`ReplyRoute`), the staged-origin task-local, `_rv_pinned`, the RDMA gate, the route-keeping fetch and the eight-cell route matrix (`capped_fetch.rs`, `every_staged_frame_follows_the_call_route`).
- The message table is in `docs/src/concepts/rendezvous.md`, "Every frame keeps to the call's route", on this branch.
