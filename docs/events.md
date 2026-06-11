# events

Generational event system for coordinating async tasks, with optional distributed (cross-instance) routing.

## Core Operations

| Operation | API |
|-----------|-----|
| Create | `manager.new_event()` → `Event` (RAII guard) |
| Await | `manager.awaiter(handle)?.await` |
| Merge | `manager.merge_events(vec![a, b, c])` → new event ready when all complete |
| Trigger | `event.trigger()?` (consumes `Event`) |
| Poison | `event.poison(reason)?` (consumes `Event`) |

`Event` is an RAII guard: dropping without calling `trigger()` or `poison()` auto-poisons so waiters are never silently abandoned. Both `trigger` and `poison` consume `self`, preventing double-completion at compile time.

## Basic Usage

```rust
use velo::EventManager;

let manager = EventManager::local();
let event = manager.new_event()?;
let handle = event.handle();

let mgr = manager.clone();
let waiter = tokio::spawn(async move { mgr.awaiter(handle)?.await });

event.trigger()?;
waiter.await??;
```

## Merging Events

`merge_events` creates a new event that completes only after all inputs complete. Merged events are themselves events, so you can build arbitrary DAGs.

```rust
let a = manager.new_event()?;
let b = manager.new_event()?;
let ready = manager.merge_events(vec![a.handle(), b.handle()])?;

a.trigger()?;
b.trigger()?;
manager.awaiter(ready)?.await?;
```

## Poison Propagation

Merged events accumulate poison reasons from their inputs:

```rust
manager.poison(a.handle(), "a failed")?;
manager.poison(b.handle(), "b failed")?;
let err = manager.awaiter(merged)?.await.unwrap_err();
// err contains both "a failed" and "b failed"
```

## Pattern: Don't Use trigger/poison as if/else

Poison reasons persist in a `BTreeMap` per entry. Use separate events per outcome arm and `tokio::select!` instead:

```rust
let success = manager.new_event()?;
let failure = manager.new_event()?;
// producer picks one: success.trigger()? OR failure.trigger()?

tokio::select! {
    ok = manager.awaiter(success.handle())? => { ok?; }
    err = manager.awaiter(failure.handle())? => { err?; }
}
```

## Distributed Events

When wired through `Messenger`, events are automatically routed cross-instance. Each event carries a `system_id` identifying its owner. When an operation targets a remote event the messenger layer handles it transparently.

**Subscribe flow** (instance B awaiting an event owned by A):
1. B sends `_event_subscribe` AM to A; A records the subscription.
2. When A triggers, it sends `_event_trigger` AMs to all subscribers.
3. B receives completion and wakes its local awaiters.

Three-tier lookup keeps the fast path fast:

| Tier | When |
|------|------|
| LRU cache | Event completed recently — instant response |
| Active DashMap | Another local task already subscribed — piggyback |
| Network | First subscriber — send `_event_subscribe` to owner |

**TOCTOU safety**: The subscribe handler checks if the event is already completed before recording the subscriber. If so, it sends completion immediately.

## Custom Distributed Backend

Implement `EventBackend` to route local vs. remote handles:

```rust
use velo::{EventSystemBase, EventBackend, EventManager, EventHandle, EventAwaiter};

struct MyBackend {
    local: Arc<EventSystemBase>,
}

impl EventBackend for MyBackend {
    fn trigger(&self, handle: EventHandle) -> Result<()> {
        if handle.system_id() == self.local.system_id() {
            self.local.trigger_inner(handle)
        } else {
            todo!("route over network")
        }
    }
    // ... poison, awaiter
}

let base = EventSystemBase::distributed(0x42);
let backend = Arc::new(MyBackend { local: base.clone() });
let manager = EventManager::new(base, backend);
```

For cases that only need handles stamped with a `system_id`:

```rust
use velo::events::DistributedEventFactory;

let factory = DistributedEventFactory::new(0x42.try_into().unwrap());
let manager = factory.event_manager();
```
