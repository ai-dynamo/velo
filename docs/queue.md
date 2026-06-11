# queue

Named work queue abstraction for Velo distributed systems.

A queue is a named entity you create or connect to, then get typed sender/receiver handles for enqueuing and consuming work items.

## Quick Start

```rust
use serde::{Serialize, Deserialize};
use velo::queue::{sender, receiver, backends::memory::InMemoryBackend};

#[derive(Serialize, Deserialize)]
struct Job { id: u64, payload: String }

let backend = InMemoryBackend::new(1024);
let tx = sender::<Job>(&backend, "my-jobs").await?;
let rx = receiver::<Job>(&backend, "my-jobs").await?;

tx.enqueue(&Job { id: 1, payload: "work".into() }).await?;
let job = rx.next().await?.unwrap();
```

## Backends

| Backend | Feature | Description |
|---------|---------|-------------|
| `InMemoryBackend` | always | `DashMap` + `flume` channels; for testing only |
| `MessengerQueueBackend` | `queue-messenger` | Actor on a Velo instance via active messages |
| `NatsQueueBackend` | `nats-queue` | NATS JetStream with WorkQueue retention |

All backends implement `WorkQueueBackend`. The `sender()` and `receiver()` free functions accept any `&dyn WorkQueueBackend`.

## Types

| Type | Role |
|------|------|
| `WorkQueueSender<T>` | Typed sender; `enqueue(&T)` serializes via `rmp_serde` |
| `WorkQueueReceiver<T>` | Typed receiver; `next()` → `Result<Option<T>, WorkQueueRecvError>` |
| `WorkQueueBackend` | Trait: `sender(name)` + `receiver(name)` returning raw channel halves |
| `SenderBackend` | Raw byte-level sender backend |
| `ReceiverBackend` | Raw byte-level receiver backend |
| `WorkQueueError` | Create/connect error |
| `WorkQueueSendError` | Send failure |
| `WorkQueueRecvError` | Receive failure |
| `NextOptions` | Future: per-receive options (timeout, visibility) |

## Acknowledgment

Currently items are auto-acknowledged on receipt. A future iteration will add:
- `WorkItem<T>` wrapper with `ack()`, `nack(delay)`, `in_progress()`, `term()`
- `AckPolicy` config: `None` (auto-ack) vs `Manual` (explicit)
- Redelivery for nack'd or timed-out items
