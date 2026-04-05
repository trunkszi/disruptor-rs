# ADR-0005: Unify PublishError Enum to Replace Scattered Error Types

> **Key Takeaway**: Unify `RingBufferFull` and `MissingFreeSlots(u64)` into a `PublishError` enum, and rename `Polling` to `PollStatus`, providing a consistent error handling experience and better `match` ergonomics.

## Status

Proposed

## Date

2026-04-03

## Context

Currently disruptor-rs v4 has three separate error types:

```rust
// producer.rs
#[derive(Debug, Error, PartialEq)]
#[error("Ring Buffer is full.")]
pub struct RingBufferFull;

#[derive(Debug, Error, PartialEq)]
#[error("Missing free slots in Ring Buffer: {0}")]
pub struct MissingFreeSlots(pub u64);

// consumer/event_poller.rs
#[derive(Debug, Error, PartialEq)]
pub enum Polling {
    #[error("No available events.")]
    NoEvents,
    #[error("Disruptor is shut down.")]
    Shutdown,
}
```

Problem analysis:

1. **Semantic overlap**: `RingBufferFull` is a special case of `MissingFreeSlots(1)`, but uses a different type
2. **API inconsistency**: `try_publish` returns `Result<_, RingBufferFull>`, `try_batch_publish` returns `Result<_, MissingFreeSlots>`, requiring callers to handle two different error types
3. **Extension difficulty**: If a `Timeout` error variant is added in the future, a fourth error type would be needed
4. **Naming issue**: `Polling` reads like an action ("currently polling"), not like a status

$$
\text{Current: } 3 \text{ error types} \times 2 \text{ return paths} = 6 \text{ match arms}
$$
$$
\text{Goal: } 2 \text{ error enums} \times 1 \text{ return path each} = \text{fewer arms, unified handling}
$$

## Decision

### 1. Define `PublishError` Enum

```rust
/// Errors that can occur when publishing events to the Disruptor.
#[derive(Debug, Error, PartialEq)]
pub enum PublishError {
    /// Ring buffer is full, cannot publish a single event.
    #[error("Ring buffer is full.")]
    Full,

    /// Ring buffer does not have enough free slots for batch publishing.
    /// Contains the number of missing slots.
    #[error("Insufficient capacity: {0} more slots needed.")]
    InsufficientCapacity(u64),
}
```

### 2. Define `PollStatus` Enum

```rust
/// Non-success statuses when polling with EventPoller.
#[derive(Debug, Error, PartialEq)]
pub enum PollStatus {
    /// No available events.
    #[error("No available events.")]
    NoEvents,

    /// Disruptor has shut down.
    #[error("Disruptor is shut down.")]
    Shutdown,

    /// Wait timed out (used with TimeoutBlocking strategy).
    #[error("Poll timed out after {0:?}.")]
    Timeout(std::time::Duration),
}
```

### 3. Unify Producer Trait

```rust
pub trait Producer<E> {
    /// Publish a single event.
    fn try_publish<F>(&mut self, update: F) -> Result<Sequence, PublishError>
    where F: FnOnce(&mut E);

    /// Batch publish events.
    fn try_batch_publish<'a, F>(&'a mut self, n: usize, update: F)
        -> Result<Sequence, PublishError>
    where E: 'a, F: FnOnce(MutBatchIter<'a, E>);

    // publish() and batch_publish() remain unchanged (do not return errors)
    fn publish<F>(&mut self, update: F) where F: FnOnce(&mut E);
    fn batch_publish<'a, F>(&'a mut self, n: usize, update: F)
        where E: 'a, F: FnOnce(MutBatchIter<'a, E>);
}
```

### 4. Error Conversion Relationships

```rust
impl From<PublishError> for PublishError {
    // Identity -- no additional conversion needed
}

impl PublishError {
    /// If Full, returns the equivalent of InsufficientCapacity(1).
    pub fn missing_slots(&self) -> u64 {
        match self {
            PublishError::Full => 1,
            PublishError::InsufficientCapacity(n) => *n,
        }
    }

    /// Whether this is a capacity error (including Full).
    pub fn is_capacity_error(&self) -> bool {
        true // Currently all variants are capacity errors
    }
}
```

### 5. User Code Migration

**Before (v4)**:
```rust
match producer.try_publish(|e| e.price = 42.0) {
    Ok(seq)              => println!("Published at {seq}"),
    Err(RingBufferFull)  => eprintln!("Full!"),
}

match producer.try_batch_publish(10, |iter| { /* ... */ }) {
    Ok(seq)                   => println!("Published at {seq}"),
    Err(MissingFreeSlots(n))  => eprintln!("Need {n} more slots"),
}
```

**After (v5)**:
```rust
match producer.try_publish(|e| e.price = 42.0) {
    Ok(seq) => println!("Published at {seq}"),
    Err(PublishError::Full) => eprintln!("Full!"),
    Err(PublishError::InsufficientCapacity(n)) => {
        // try_publish will not return this variant, but match exhaustiveness requires handling it
        unreachable!()
    }
}

// Or handle uniformly:
match producer.try_batch_publish(10, |iter| { /* ... */ }) {
    Ok(seq) => println!("Published at {seq}"),
    Err(e)  => eprintln!("Publish failed: {e}"),
}
```

## Consequences

### Positive Consequences

- **Unified error handling**: A single `match` handles all publish errors
- **Extensible**: Adding `PublishError::Shutdown` or `PublishError::Timeout` in the future only requires a new variant
- **Clear semantics**: `PublishError::Full` and `PublishError::InsufficientCapacity(n)` clearly express their meaning
- **Helper methods**: `missing_slots()` lets callers obtain the missing slot count without matching

### Negative Consequences

- **Breaking change**: All code using `RingBufferFull` and `MissingFreeSlots` needs to be updated
- **Match exhaustiveness**: `try_publish` returns `PublishError` but never produces `InsufficientCapacity`; users need `unreachable!()` or `_` branches
- **Reduced type information**: The signature of `try_publish` no longer expresses at the type level that "only Full can be returned"

### Neutral Consequences

- The `thiserror` derive macro usage is slightly different on enums
- `PartialEq` is automatically derived for `InsufficientCapacity(u64)`

## Alternatives Considered

| Alternative | Description | Reason for Rejection |
|---|---|---|
| Keep separate | Do not unify, keep three error types | API inconsistency, hard to extend |
| Generic error | `Result<_, Box<dyn Error>>` | Loses type safety, cannot match |
| Rename only | `RingBufferFull` -> `PublishError`, without merging | Still has two different error types |
| Associated type | `Producer::Error` associated type | Over-generalization, increases trait complexity |

## References

- Rust Error Handling Guidelines: https://rust-lang.github.io/api-guidelines/interoperability.html#c-good-err
- `thiserror` crate: https://docs.rs/thiserror/
