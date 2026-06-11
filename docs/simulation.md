# simulation

Discrete-event simulation transport for Velo. Runs multiple Velo instances inside one process under virtual time with configurable network latency and congestion.

Feature-gated behind `simulation`.

## Architecture

```
SimFabric ← shared by all instances
    ↑
SimTransport  (implements velo::Transport)
SimDiscovery  (implements velo::discovery::PeerDiscovery)
NetworkModel  (pluggable: how transfers complete over virtual time)
```

All participating instances share the same `Arc<SimFabric>`. The fabric updates transfer progress whenever virtual time advances, reschedules completions when contention changes, and delivers frames into each instance's `TransportAdapter`.

## Core Types

| Type | Role |
|------|------|
| `SimFabric` | Owns all in-flight transfers, delivery scheduling, adapter registration, and discovery state |
| `SimTransport` | Implements `Transport`; routes messages through the fabric's DES queue |
| `SimDiscovery` | Implements `PeerDiscovery` with in-memory lookups |
| `NetworkModel` | Trait: `advance_to`, `next_completion`, `is_complete` |
| `BisectionBandwidth` | Default model: per-link bandwidth sharing, global bisection cap, fixed base latency |

## Usage

```rust
use std::sync::Arc;
use loom_rs::sim::SimulationRuntime;
use velo::simulation::{SimFabric, SimTransport, SimDiscovery, BisectionBandwidth};

let mut sim = SimulationRuntime::new()?;
let fabric = Arc::new(SimFabric::new(
    sim.handle(),
    BisectionBandwidth {
        link_gbps: 200.0,
        bisection_gbps: 12_800.0,
        base_latency: Duration::from_micros(10),
    },
));
let discovery = Arc::new(SimDiscovery::new(fabric.clone()));

let transport_a = Arc::new(SimTransport::new(fabric.clone()));
let transport_b = Arc::new(SimTransport::new(fabric.clone()));

// Build Velo instances with SimTransport + SimDiscovery (instead of real transports)
```

## NetworkModel Contract

Custom models implement:

- `advance_to(&mut [Transfer], now)` — apply progress earned since the last timestamp
- `next_completion(&[Transfer], now)` — pick the next completion event
- `is_complete(&Transfer, now)` — check if a transfer is done at the current tick

`BisectionBandwidth` uses:
- Per `(source, target)` link sharing
- A fabric-wide `bisection_gbps` cap
- A one-time `base_latency` before payload bytes begin transferring

## Shutdown Behavior

Mirrors real transports for drain testing:
- New inbound `Message` frames are rejected during drain
- Rejections surface as `MessageType::ShuttingDown` on the sender side
- `Response`, `Ack`, and `Event` frames continue to flow during drain
- Late delivery failures invoke the original `TransportErrorHandler`

## Lifecycle Helpers

```rust
SimFabric::unregister_adapter(instance_id);
SimFabric::unregister_peer(instance_id);
SimFabric::reset();          // clears adapters, discovery, in-flight transfers
SimDiscovery::unregister(instance_id);
SimDiscovery::reset();
SimTransport::shutdown();    // unregisters adapter from fabric
```

`reset()` is intentionally destructive and invalidates pending fabric callbacks.

## Limitations

- In-process only; not a wall-clock network emulator
- All peers in a scenario must share the same `SimFabric`
- `reset()` invalidates all pending callbacks and in-flight transfers
