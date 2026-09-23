# Work queues

A work queue is a named queue of typed work items. You create or connect to a queue by name, and then you get a typed sender and a typed receiver for it.

```rust,ignore
use velo::queue::{backends::memory::InMemoryBackend, receiver, sender};

let backend = InMemoryBackend::new(1024);
let tx = sender::<Job>(&backend, "my-jobs").await?;
let rx = receiver::<Job>(&backend, "my-jobs").await?;

tx.enqueue(&Job { id: 1 }).await?;
let job = rx.next().await?.unwrap();
```

| Backend | Feature | Description |
|---|---|---|
| `InMemoryBackend` | always | `DashMap` and `flume` channels. For tests. |
| `MessengerQueueBackend` | `queue-messenger` | An actor on a Velo instance, reached with active messages |
| `NatsQueueBackend` | `nats-queue` | NATS JetStream with WorkQueue retention |
