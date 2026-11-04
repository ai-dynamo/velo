# Write an out-of-tree transport

A transport, frame transport, or discovery backend in another crate depends on `velo-ext` only. It does not depend on `velo`. `velo-ext` has no Prometheus, no Tonic, no NATS, and no dependency on `velo`.

```toml
[dependencies]
velo-ext = "0.6"
```

## Pick the trait

| Trait | What you provide |
|---|---|
| `velo_ext::Transport` | A messenger transport |
| `velo_ext::FrameTransport` | A frame transport for streams |
| `velo_ext::PeerDiscovery` | A peer discovery backend |
| `velo_ext::ServiceDiscovery` | A service discovery backend |
| `velo_ext::TransportObservability` | Rarely needed. The runtime gives you one of these. |

## Procedure for a messenger transport

1. Implement `Transport` for your type.
2. Send each inbound `MessageType::Message` frame through admission. If your transport knows which peer is on the connection the frame arrived on, call `TransportAdapter::admit_message_from` with that `SenderIdentity`. If it cannot say (a store-and-forward backend, for example), call `TransportAdapter::admit_message`, which records `SenderIdentity::Unattributed`. Do not check `ShutdownState::is_draining()` first. See [Shutdown and drain](../concepts/shutdown.md).
3. If admission returns `AdmitOutcome::Draining` and your transport has a return path, send a `MessageType::ShuttingDown` frame with the header of the rejected request. If there is no return path, record the rejection and drop the frame.
4. If admission returns `AdmitOutcome::Disconnected`, the runtime has stopped. Record `TransportRejection::RouteFailed` through the observability handle and drop the frame.
5. Route the other inbound types to their streams on the adapter: `Response` to the response stream, `Ack` and `Event` to the event stream, and `ShuttingDown` to the shutdown stream. Each item is a `(header, payload, SenderIdentity)` tuple. Use the same identity that admission would get, or `SenderIdentity::Unattributed`.
6. Never route or send `MessageType::Identify`. It is the record that opens a TCP or UDS connection, and the in-tree reader consumes it before routing starts. `MessageType` is not `#[non_exhaustive]`, so a `match` over it must name `Identify`: drop an inbound one, and refuse an outbound one through the send's error handler.
7. Leave `Transport::identifies_connections` at its default, `false`, unless every frame your transport delivers is attributed. A transport that answers `true` promises that, so the runtime drops an unattributed reply from its peers as forged.
8. Store the handle that `set_observability` gives you. Record each inbound frame as you route it.
9. On the `Admitted` arm, and only there, record the frame as inbound `message`.
10. In `shutdown`, stop your tasks and threads. The runtime calls it at most once, with its Tokio runtime handle entered, on a dedicated thread, so you may block there, for example to join a thread. Two cases run it on the caller instead. Any failure while the transports start (another transport fails to start, a duplicate key, an address that cannot be merged) or a build that is cancelled then runs it on the building task. A thread that cannot be created runs it on whatever dropped or shut down the instance. After the final owner is dropped, the runtime may already be gone, so a task that `shutdown` spawns may never run. If `shutdown` panics, the runtime still calls the other transports' hooks, and then the shutdown call panics.
11. If your transport keeps state for each peer, override `unregister` to drop it and close the peer's connections. It must be cheap and idempotent. See [Peer routing](../concepts/transports.md#peer-routing).

Step 9 matters because the inbound queue depth is a difference between two counters. If your transport admits a frame and does not record it, the depth goes negative. See [Observability](../operations/observability.md).

Step 8 matters because the runtime creates a series for each direction and message type in advance. If only `message` moves, the dashboard reads "no responses arrived", not "responses are not instrumented".

```rust,ignore
use std::sync::{Arc, OnceLock};
use velo_ext::{Transport, TransportObservability};

struct MyTransport {
    obs: OnceLock<Arc<dyn TransportObservability>>,
    /* ... */
}

impl Transport for MyTransport {
    // ... the required methods ...
    fn set_observability(&self, obs: Arc<dyn TransportObservability>) {
        let _ = self.obs.set(obs);
    }
}
```

## Stability of `velo-ext`

New trait methods in `velo-ext` always have a default implementation, so a new release does not break your implementation. A change to an existing signature is a breaking release. See [Versioning](../development/versioning.md).

A transport with one ordered channel to each peer keeps the defaults of `lanes` and `send_message_on_lane`. Override both only if your transport keeps several ordered channels to one peer.

`velo-ext` 0.6.0 is a breaking release for transports. The response, event, and shutdown stream items gained the `SenderIdentity` element, and `MessageType` gained `Identify`. It also adds `TransportAdapter::admit_message_from`, `SenderIdentity`, and `Transport::identifies_connections`. Everything this page names is in `velo-ext = "0.6"`.
