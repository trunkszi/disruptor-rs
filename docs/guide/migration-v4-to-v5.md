# Migrating from v4 to v5

> **Key takeaway**: New workspace structure + new traits + error type changes + `#[non_exhaustive]` enums

- New build functions: `Disruptor::single` / `Disruptor::multi` (old functions retained)
- Handler changed to trait: closures still work (blanket impl), new `EventHandler<E>` trait added
- Error types refactored: `RingBufferFull` -> `PublishError::Full`
- New WaitStrategy options: Yielding / Sleeping / PhasedBackoff, etc.
- Migrate incrementally; the compiler will point out everything that needs changing

---

## 1. Breaking Changes Overview

| Category | v4 (current) | v5 (target) |
|----------|-------------|-------------|
| Build function | `build_single_producer(size, factory, ws)` | `Disruptor::single(size, factory, ws)` |
| Build function | `build_multi_producer(size, factory, ws)` | `Disruptor::multi(size, factory, ws)` |
| Handler | `FnMut(&E, Sequence, bool)` closure | `impl EventHandler<E>` trait (closures still work) |
| Stateful handler | `handle_events_and_state_with(handler, init)` | `handle_events_with(MyHandler::new())` |
| Error: single publish | `RingBufferFull` | `PublishError::Full` |
| Error: batch publish | `MissingFreeSlots(u64)` | `PublishError::InsufficientCapacity { missing: u64 }` |
| WaitStrategy | `BusySpin`, `BusySpinWithSpinLoopHint` | Added `YieldingWaitStrategy`, `SleepingWaitStrategy`, `PhasedBackoffWaitStrategy` |
| Sequence type | `pub type Sequence = i64` | `pub type Sequence = i64` (unchanged) |
| Crate structure | Single crate | Workspace (core / wait / seq / consumer / dsl) |
| Enums | Exhaustive matching | `#[non_exhaustive]`, requires `_ =>` wildcard arm |

---

## 2. Build Function Migration

### Before (v4)

```rust
use disruptor::*;

let mut producer = build_single_producer(64, || Event::default(), BusySpin)
    .handle_events_with(handler)
    .build();
```

### After (v5)

```rust
use disruptor_dsl::prelude::*;

let mut producer = Disruptor::single(64, || Event::default(), BusySpin)
    .handle_events_with(handler)
    .build();
```

### Migration Steps

1. Add the `disruptor-dsl` dependency (or use the `disruptor` facade)
2. Replace `build_single_producer` with `Disruptor::single`
3. Replace `build_multi_producer` with `Disruptor::multi`

---

## 3. EventHandler Trait Migration

v5 replaces closures with a trait. Closures still work (blanket impl), but struct + trait is recommended.

```rust
// v4: closure (still works)
let handler = |e: &Event, seq: Sequence, eob: bool| { println!("{}", e.data); };

// v5: trait (recommended)
struct MyHandler;
impl EventHandler<Event> for MyHandler {
    fn on_event(&mut self, event: &Event, _: Sequence, _: bool) {
        println!("{}", event.data);
    }
    fn on_start(&mut self) { /* optional lifecycle hook */ }
    fn on_shutdown(&mut self) { /* optional lifecycle hook */ }
}
```

---

## 4. Error Type Migration

v5 uses `#[non_exhaustive]` enums; match statements must include a wildcard arm.

```rust
// v4
match producer.try_publish(|e| { e.data = 1; }) {
    Ok(seq) => { /* ... */ }
    Err(RingBufferFull) => { /* ... */ }
}

// v5
match producer.try_publish(|e| { e.data = 1; }) {
    Ok(seq) => { /* ... */ }
    Err(PublishError::Full) => { /* ... */ }
    Err(PublishError::InsufficientCapacity { missing }) => { /* ... */ }
    Err(_) => unreachable!(),  // required by #[non_exhaustive]
}
```

EventPoller errors:

```rust
// v4
match poller.poll() {
    Ok(guard) => { /* ... */ }
    Err(Polling::NoEvents) => { /* ... */ }
    Err(Polling::Shutdown) => { break; }
}

// v5: requires wildcard arm
match poller.poll() {
    Ok(guard) => { /* ... */ }
    Err(Polling::NoEvents) => { /* ... */ }
    Err(Polling::Shutdown) => { break; }
    Err(_) => unreachable!(),  // required by #[non_exhaustive]
}
```

---

## 5. WaitStrategy Migration

v5 adds several new WaitStrategy options:

| Strategy | Latency | CPU | v4 | v5 |
|----------|---------|-----|----|----|
| `BusySpin` | Lowest | 100% | Yes | Yes |
| `BusySpinWithSpinLoopHint` | Low | ~95% | Yes | Yes (alias: `SpinLoopHint`) |
| `YieldingWaitStrategy` | Medium-low | ~50% | No | New |
| `SleepingWaitStrategy` | Medium | Low | No | New |
| `PhasedBackoffWaitStrategy` | Adaptive | Adaptive | No | New |

`BusySpin` and `BusySpinWithSpinLoopHint` remain unchanged. New strategies are optional.

---

## 6. Workspace Structure

v5 splits into 5 crates:

| Crate | Purpose |
|-------|---------|
| `disruptor-core` | Core primitives (Sequence, RingBuffer, Cursor, Barrier) |
| `disruptor-wait` | WaitStrategy trait and implementations |
| `disruptor-seq` | Single/multi producer sequencers |
| `disruptor-consumer` | Consumer, EventHandler, EventPoller, WorkerPool |
| `disruptor-dsl` | Builder DSL, Producer trait and implementations |

Most users only need to depend on `disruptor-dsl` (or use the `disruptor` facade).

---

## 7. Migration Checklist

Execute in order. Compile after each step and fix errors.

| Step | Action |
|------|--------|
| 1 | Update Cargo.toml dependency version |
| 2 | Build function: `build_single_producer` -> `Disruptor::single` |
| 3 | Build function: `build_multi_producer` -> `Disruptor::multi` |
| 4 | Handler: closure -> struct + `impl EventHandler<E>` (optional) |
| 5 | Error type: `RingBufferFull` -> `PublishError::Full` |
| 6 | Error type: `MissingFreeSlots(n)` -> `PublishError::InsufficientCapacity { missing: n }` |
| 7 | Add `_ => unreachable!()` arm to all `PublishError` / `Polling` match statements |
| 8 | Compile: `cargo build` -- the compiler will point out anything missed |
| 9 | Test: `cargo test` |

---

## 8. FAQ

**Q: Can I still use closure handlers?** Yes. v5 provides a blanket impl for `FnMut(&E, Sequence, bool)`, but struct + trait is recommended for lifecycle hooks.

**Q: Will performance change?** No. Trait methods are monomorphized + inlined, resulting in equal or better performance.

**Q: Can v4 and v5 coexist?** No. Types are incompatible; a full migration is required.

**Q: Why do match statements need a `_ =>` arm?** v5 enums are annotated with `#[non_exhaustive]`, allowing new variants to be added in the future without breaking backward compatibility.
