# Consumer Guide

> **Key takeaway**: Three consumption modes, choose as needed

- Managed consumer: `handle_events_with` -- Disruptor manages the thread, simplest approach
- Stateful consumer: `handle_events_and_state_with` -- with custom state
- EventPoller: `new_event_poller` -- you control the thread, pull mode
- Use `pin_at_core` + `thread_name` for core pinning and thread naming

---

## 1. Managed Consumer

The most common consumption method. The Disruptor creates and manages a dedicated thread for each handler.

### Basic Usage

```rust
use disruptor::*;

struct Event { price: f64 }

let handler = |e: &Event, seq: Sequence, end_of_batch: bool| {
    println!("seq={}, price={}", seq, e.price);
};

let mut producer = build_single_producer(64, || Event { price: 0.0 }, BusySpin)
    .handle_events_with(handler)
    .build();
```

Handler signature:

```rust
FnMut(&E, Sequence, bool)
//     |   |         |
//     |   |         end_of_batch
//     |   sequence number (i64)
//     immutable reference to the event (&E)
```

**Key points**:

- The handler receives `&E` (immutable reference) -- you cannot modify the event
- `sequence` starts at 0 and increases monotonically
- The handler runs in a thread managed by the Disruptor
- The handler must satisfy `Send + 'static`

---

## 2. Stateful Consumer

Use this when the handler needs to maintain state (e.g., accumulators, connection pools, non-`Send` types).

### Basic Usage

```rust
use disruptor::*;

struct Event { amount: f64 }

struct State {
    total: f64,
    count: u64,
}

let mut producer = build_single_producer(64, || Event { amount: 0.0 }, BusySpin)
    .handle_events_and_state_with(
        // handler: FnMut(&mut S, &E, Sequence, bool)
        |state: &mut State, event: &Event, _seq, _eob| {
            state.total += event.amount;
            state.count += 1;
        },
        // state initializer: FnOnce() -> S
        || State { total: 0.0, count: 0 },
    )
    .build();
```

### Using non-`Send` / non-`Sync` Types

The state initializer executes on the consumer thread. So you can use `Rc`, `RefCell`, and other non-`Send` types:

```rust
use std::cell::RefCell;
use std::rc::Rc;
use disruptor::*;

struct Event { data: i32 }

#[derive(Default)]
struct State {
    buffer: Rc<RefCell<Vec<i32>>>,
}

let mut producer = build_single_producer(64, || Event { data: 0 }, BusySpin)
    .handle_events_and_state_with(
        |state: &mut State, event: &Event, _seq, _eob| {
            state.buffer.borrow_mut().push(event.data);
        },
        || State::default(), // Executed on the consumer thread
    )
    .build();
```

---

## 3. EventPoller (Pull Mode)

Use EventPoller when you want to control the consumption thread yourself.

```mermaid
graph LR
    P[Producer] -->|publish| RB[Ring Buffer]
    RB -.->|poll| EP[EventPoller]
    EP -->|your thread| Logic[Processing logic]
```

### Creating an EventPoller

```rust
use disruptor::*;

#[derive(Debug)]
struct Event { price: f64 }

let builder = build_single_producer(8, || Event { price: 0.0 }, BusySpin);
let (mut poller, builder) = builder.new_event_poller();
let mut producer = builder.build();
```

> `new_event_poller` returns a tuple: `(EventPoller, Builder)`. You need to continue building after creating the poller.

### Polling Events: `poll()`

`poll()` returns all available events:

```rust
use disruptor::Polling;

loop {
    match poller.poll() {
        Ok(mut guard) => {
            // guard is an Iterator that yields &E
            for event in &mut guard {
                println!("price={}", event.price);
            }
            // guard drop notifies the Disruptor that processing is complete
        }
        Err(Polling::NoEvents) => {
            // No new events -- do other work or retry
        }
        Err(Polling::Shutdown) => {
            // Producer has been dropped -- exit the loop
            break;
        }
    }
}
```

### Other EventPoller Methods

- `take(limit)`: Yields at most `limit` events. Usage is the same as `poll()`, but with an upper bound.
- `has_available()`: Checks whether readable events are available without consuming them. Useful for double-checking.

### EventGuard

The `EventGuard` returned by `poll()` / `take()` implements `Iterator` (yields `&E`) and `ExactSizeIterator`. When dropped, it notifies the Disruptor that consumption is complete.

> Even if not all events are iterated, dropping the guard advances the cursor. Skipped events will not be read again.

---

## 4. Batch Processing (end_of_batch)

When the producer publishes faster than the consumer processes, the consumer sees multiple events at once. The `end_of_batch` flag helps you optimize batch processing:

```rust
struct Event { order_id: u64 }

struct State {
    batch: Vec<u64>,
}

let mut producer = build_single_producer(64, || Event { order_id: 0 }, BusySpin)
    .handle_events_and_state_with(
        |state: &mut State, event: &Event, _seq, end_of_batch| {
            // Accumulate into batch
            state.batch.push(event.order_id);

            if end_of_batch {
                // Batch write to database / network flush
                // db.batch_insert(&state.batch);
                state.batch.clear();
            }
        },
        || State { batch: Vec::with_capacity(64) },
    )
    .build();
```

When the consumer sees 3 available events at once: `event(0, eob=false)` -> `event(1, eob=false)` -> `event(2, eob=true)`. Execute the flush at `eob=true`.

---

## 5. CPU Affinity (pin_at_core)

Pin consumer threads to specific CPU cores to reduce context switches:

```rust
let mut producer = build_single_producer(64, || Event { price: 0.0 }, BusySpin)
    .pin_at_core(1)
    .handle_events_with(handler1)
    .pin_at_core(2)
    .handle_events_with(handler2)
    .build();
```

**Notes**:

- `pin_at_core(id)` applies to the **next** handler added
- If the specified core does not exist, the build will panic
- If core pinning fails (e.g., permission issues), a warning is printed to stderr but it will not panic
- Core IDs start from 0

---

## 6. Thread Naming (thread_name)

Give consumer threads a recognizable name:

```rust
let mut producer = build_single_producer(64, || Event { price: 0.0 }, BusySpin)
    .pin_at_core(1)
    .thread_name("price-handler")
    .handle_events_with(handler1)
    .thread_name("risk-checker")
    .handle_events_with(handler2)
    .build();
```

**Default name**: If not set, thread names are `processor-1`, `processor-2`, ...

**Debugging tip**: Use `htop` or `top -H` to view thread names.

---

## 7. Shutdown Detection

### Managed consumer

Managed consumers handle shutdown automatically. When the Producer is dropped:

1. The Disruptor sets the shutdown sequence
2. The consumer finishes processing all published events
3. The consumer thread exits automatically

You do not need to do any special handling in the handler.

### EventPoller

EventPoller notifies you via `Polling::Shutdown`:

```rust
loop {
    match poller.poll() {
        Ok(mut guard) => {
            for event in &mut guard { /* process */ }
        }
        Err(Polling::NoEvents) => {
            // Can do other work
            std::hint::spin_loop();
        }
        Err(Polling::Shutdown) => {
            // Clean up resources
            println!("shutting down");
            break;
        }
    }
}
```

---

## 8. Multi-Consumer Patterns

For topology combinations of multiple consumers (parallel, pipeline, hybrid), see [Topology Patterns](topology-patterns.md).

Core rules:

- Consecutive calls to `handle_events_with` = parallel consumption
- Inserting `and_then()` = sequential dependency
- `new_event_poller` can be mixed with managed consumers

```rust
let mut producer = build_single_producer(64, factory, BusySpin)
    .handle_events_with(handler_a)  // A and B read each event in parallel
    .handle_events_with(handler_b)
    .build();
```
