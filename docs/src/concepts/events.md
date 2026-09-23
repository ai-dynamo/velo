# Events

Velo has a generational event system to coordinate async tasks. An event has a compact `u128` handle. You can share the handle between threads, or send it to another instance.

## Local events

```rust,ignore
use velo::EventManager;

let manager = EventManager::local();
let event = manager.new_event()?;
let awaiter = manager.awaiter(event.handle())?;

// trigger() consumes the event, so an event cannot complete twice.
event.trigger()?;
awaiter.await?;
```

If you drop an `Event` without `trigger()` or `poison()`, the drop poisons it. A waiter never waits for an event that nobody can complete.

`merge_events` makes an AND gate. The merged event completes after all its inputs complete.

```rust,ignore
let weights = manager.new_event()?;
let tokenizer = manager.new_event()?;
let ready = manager.merge_events(vec![weights.handle(), tokenizer.handle()])?;
weights.trigger()?;
tokenizer.trigger()?;
manager.awaiter(ready)?.await?;
```

When an input is poisoned, the poison reason goes to all its waiters, and to every merged event that uses the input.

## Distributed events

In a `Velo` instance, the event manager is distributed. The handle encodes the instance that owns the event. When you wait on a remote handle, Velo subscribes to the owner with active messages.

```rust,ignore
// Node A creates the event and sends the handle to node B.
let event = node_a.event_manager().new_event()?;
let handle = event.handle();

// Node B waits on the remote event.
let awaiter = node_b.event_manager().awaiter(handle)?;

// Node A triggers the event, and the waiter on node B wakes.
event.trigger()?;
awaiter.await?;
```

A remote wait looks in three places, in this order:

1. A cache of completed events. A wait on an event that is already complete returns immediately, with no network round trip.
2. An existing local subscription to the same event.
3. A new subscription to the owner, over the network.
