# Getting Started

> **Key takeaway**: Get disruptor-rs running in 5 minutes

- Add the dependency, define an Event, build the Disruptor, publish events
- All you need is `build_single_producer` + `handle_events_with` + `publish`
- A complete runnable example is at the end of this document

---

## 1. Add the Dependency

Add the `disruptor` crate to your `Cargo.toml`:

```toml
[dependencies]
disruptor = "4.0"
```

> You need Rust **1.85+** (edition 2024). Upgrade with `rustup update stable`.

---

## 2. Define the Event Struct

Each slot in the ring buffer holds one Event. You need to provide a factory closure to initialize them.

```rust
/// Data entity on the ring buffer.
struct Event {
    price: f64,
}

/// Factory closure -- used by the Disruptor to pre-populate the ring buffer.
let factory = || Event { price: 0.0 };
```

The Event must satisfy `Send + Sync + 'static`. No trait implementation is required.

---

## 3. Create a Single-Producer Disruptor

Call `build_single_producer` to start building:

```rust
use disruptor::*;

let mut producer = build_single_producer(64, factory, BusySpin)
    .handle_events_with(|event: &Event, sequence: Sequence, end_of_batch: bool| {
        println!("seq={} price={}", sequence, event.price);
    })
    .build();
```

Three parameters:

| Parameter | Description | Constraint |
|-----------|-------------|------------|
| `size` | Ring buffer capacity | Must be a power of 2 |
| `factory` | Pre-population closure `FnMut() -> E` | -- |
| `wait_strategy` | Wait strategy | `BusySpin` or `BusySpinWithSpinLoopHint` |

---

## 4. Add an Event Handler

`handle_events_with` accepts a closure with the following signature:

```rust
FnMut(&E, Sequence, bool)
//     ^   ^         ^
//     |   |         end_of_batch: is this the last event in the current batch?
//     |   sequence number (i64)
//     immutable reference to the event
```

The handler runs on a dedicated thread managed by the Disruptor.

---

## 5. Publish Events

Use `publish` to publish a single event. It accepts an `FnOnce(&mut E)` closure:

```rust
producer.publish(|e| {
    e.price = 42.0;
});
```

`publish` spins when the ring buffer is full. If you want non-blocking publishing, use `try_publish`:

```rust
match producer.try_publish(|e| { e.price = 42.0; }) {
    Ok(seq)              => println!("published at {}", seq),
    Err(RingBufferFull)  => eprintln!("ring buffer full!"),
}
```

---

## 6. Shutdown

No explicit shutdown is needed. When the Producer is dropped:

1. The Disruptor notifies all consumer threads to shut down
2. Waits for all published events to be processed
3. Joins all consumer threads
4. Releases the ring buffer

```rust
{
    let mut producer = build_single_producer(8, factory, BusySpin)
        .handle_events_with(handler)
        .build();

    producer.publish(|e| e.price = 1.0);
    // producer is dropped here -- automatic shutdown
}
```

---

## 7. Complete Example

```rust
use disruptor::*;

fn main() {
    // 1. Define Event
    struct Event {
        price: f64,
    }

    // 2. Define factory
    let factory = || Event { price: 0.0 };

    // 3. Define handler
    let handler = |e: &Event, seq: Sequence, end_of_batch: bool| {
        print!("[seq={}] price={:.2}", seq, e.price);
        if end_of_batch {
            println!(" (batch end)");
        } else {
            println!();
        }
    };

    // 4. Build Disruptor
    let mut producer = build_single_producer(64, factory, BusySpin)
        .handle_events_with(handler)
        .build();

    // 5. Publish events
    for i in 0..10 {
        producer.publish(|e| {
            e.price = i as f64 * 1.5;
        });
    }

    // 6. Automatic shutdown when producer is dropped
}
```

Run:

```bash
cargo run
```

---

## 8. Next Steps

| Goal | Documentation |
|------|---------------|
| Batch publishing / multi-threaded publishing | [Producer Guide](producer-guide.md) |
| Stateful handler / EventPoller | [Consumer Guide](consumer-guide.md) |
| Pipeline / Diamond topologies | [Topology Patterns](topology-patterns.md) |
| Latency optimization / CPU affinity | [Performance Tuning](performance-tuning.md) |
| Upgrading from v4 to v5 | [Migration Guide](migration-v4-to-v5.md) |

---

## FAQ

### How do I choose the ring buffer size?

Start with 64 or 128. Too small causes backpressure; too large wastes cache. Must be a power of 2.

### How do I choose between `BusySpin` and `BusySpinWithSpinLoopHint`?

| Strategy | Latency | CPU Usage |
|----------|---------|-----------|
| `BusySpin` | Lowest | 100% |
| `BusySpinWithSpinLoopHint` | Slightly higher | ~95% |

Use `BusySpin` for ultra-low latency scenarios. Use `BusySpinWithSpinLoopHint` for everything else.

### Must I use a closure as a handler?

Yes. The current API uses `FnMut` closures and does not require implementing a trait. If you need to share state across handlers, use `handle_events_and_state_with`.
