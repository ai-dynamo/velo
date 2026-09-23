# Write an out-of-tree transport

A transport, frame transport, or discovery backend in another crate depends on `velo-ext` only. It does not depend on `velo`. `velo-ext` has no Prometheus, no Tonic, no NATS, and no dependency on `velo`.

```toml
[dependencies]
velo-ext = "0.5"
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
2. Send each inbound `MessageType::Message` frame through `TransportAdapter::admit_message`. Do not check `ShutdownState::is_draining()` first. See [Shutdown and drain](../concepts/shutdown.md).
3. If `admit_message` returns `AdmitOutcome::Draining` and your transport has a return path, send a `MessageType::ShuttingDown` frame with the header of the rejected request. If there is no return path, record the rejection and drop the frame.
4. If `admit_message` returns `AdmitOutcome::Disconnected`, the runtime has stopped. Record `TransportRejection::RouteFailed` through the observability handle and drop the frame.
5. Route the other inbound types (`Response`, `Ack`, `Event`, `ShuttingDown`) to their lanes on the adapter.
6. Store the handle that `set_observability` gives you. Record each inbound frame as you route it.
7. On the `Admitted` arm, and only there, record the frame as inbound `message`.

Step 7 matters because the inbound queue depth is a difference between two counters. If your transport admits a frame and does not record it, the depth goes negative. See [Observability](../operations/observability.md).

Step 6 matters because the runtime creates a series for each direction and message type in advance. If only `message` moves, the dashboard reads "no responses arrived", not "responses are not instrumented".

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
