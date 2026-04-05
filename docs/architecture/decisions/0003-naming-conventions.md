# ADR-0003: Align Naming with LMAX Canonical Terminology

> **Key Takeaway**: Rename names in disruptor-rs v4 that deviate from the LMAX standard (`Cursor`, `SingleProducerBarrier`, `BusySpinWithSpinLoopHint`) to canonical terms (`Sequence`, `SingleProducerSequencer`, `SpinLoopHint`), reducing cognitive overhead for Java Disruptor users, at the cost of a breaking change from v4 to v5.

## Status

Proposed

## Date

2026-04-03

## Context

The naming in disruptor-rs v4 deviates from the Java LMAX Disruptor in several places:

| Concept | Java LMAX Name | disruptor-rs v4 Name | Problem |
|---|---|---|---|
| Sequence counter | `Sequence` | `Cursor` | "Cursor" implies iterator semantics, inconsistent with LMAX terminology |
| Producer sequencer | `SingleProducerSequencer` | `SingleProducerBarrier` | "Barrier" in LMAX refers to the consumer-side wait barrier, not the producer |
| Producer sequencer | `MultiProducerSequencer` | `MultiProducerBarrier` | Same as above |
| Consumer wait barrier | `SequenceBarrier` | `Barrier` trait | Too generic; Producer and Consumer barriers share the same trait |
| Wait strategy | `BusySpinWaitStrategy` | `BusySpin` | Reasonable simplification, kept |
| Wait strategy | No direct equivalent | `BusySpinWithSpinLoopHint` | Name too long, not concise enough |
| Event processor | `BatchEventProcessor` | `start_processor()` function | Function replacing class is an idiomatic Rust pattern |

These deviations cause:

1. **Confusion for Java users**: Searching for "Sequence" yields no matching type; "Barrier" semantics are mixed up
2. **Difficult documentation mapping**: Cannot directly reference LMAX documentation to explain disruptor-rs behavior
3. **API review cost**: New contributors need to learn both LMAX terminology and disruptor-rs terminology

Meanwhile, Rust's module paths provide disambiguation context that Java packages do not:

```rust
// Rust module paths carry context
disruptor::producer::SingleProducerSequencer  // No need to repeat "Producer" prefix
disruptor::consumer::SequenceBarrier          // Module name eliminates ambiguity
```

## Decision

### Complete Rename Table

| v4 Name | v5 Name | Module Path | Reason for Change |
|---|---|---|---|
| `Cursor` | `Sequence` | `disruptor::core::Sequence` | Align with LMAX core terminology |
| `SingleProducerBarrier` | `SingleProducerSequencer` | `disruptor::seq::SingleProducerSequencer` | Distinguish Sequencer (producer) from Barrier (consumer) |
| `MultiProducerBarrier` | `MultiProducerSequencer` | `disruptor::seq::MultiProducerSequencer` | Same as above |
| `Barrier` trait | `SequenceBarrier` trait | `disruptor::core::SequenceBarrier` | Precisely matches LMAX terminology |
| `SingleConsumerBarrier` | `DependentSequence` | `disruptor::consumer::DependentSequence` | Expresses "depends on a single upstream sequence" |
| `MultiConsumerBarrier` | `DependentSequenceGroup` | `disruptor::consumer::DependentSequenceGroup` | Expresses "depends on a group of upstream sequences" |
| `BusySpin` | `BusySpin` | `disruptor::wait::BusySpin` | **Unchanged** -- already concise enough |
| `BusySpinWithSpinLoopHint` | `SpinLoopHint` | `disruptor::wait::SpinLoopHint` | Simplified; `SpinLoop` already implies busy spin |
| `Producer` trait | `Producer` trait | `disruptor::Producer` | **Unchanged** -- `Producer` is more intuitive in Rust than `Sequencer` |
| `SingleProducer` | `SingleProducer` | `disruptor::SingleProducer` | **Unchanged** |
| `MultiProducer` | `MultiProducer` | `disruptor::MultiProducer` | **Unchanged** |
| `Consumer` | `EventProcessor` | `disruptor::consumer::EventProcessor` | Aligns with LMAX terminology. Java's `Consumer` is not a public type |
| `RingBufferFull` | `PublishError::Full` | `disruptor::PublishError` | Unified error types (see ADR-0005) |
| `MissingFreeSlots(u64)` | `PublishError::InsufficientCapacity(u64)` | `disruptor::PublishError` | Same as above |
| `Polling` enum | `PollStatus` enum | `disruptor::PollStatus` | Clearer: this is a status, not an action |
| `build_single_producer()` | `build_single_producer()` | `disruptor::build_single_producer` | **Unchanged** -- function name is already clear |
| `build_multi_producer()` | `build_multi_producer()` | `disruptor::build_multi_producer` | **Unchanged** |
| `SPBuilder` | `SingleProducerBuilder` | `disruptor::dsl::SingleProducerBuilder` | Expand abbreviation for improved readability |
| `MPBuilder` | `MultiProducerBuilder` | `disruptor::dsl::MultiProducerBuilder` | Same as above |

### Naming Principles

1. **LMAX first**: When LMAX has a clear term, use the LMAX term
2. **Module disambiguation**: Leverage Rust module paths for disambiguation, avoiding redundant module information in names
3. **Conciseness**: Choose shorter names when they do not cause ambiguity
4. **Rust idioms**: Maintain Rust community naming conventions (e.g., `Producer` instead of `Sequencer` in the public API)

### Internal vs. External API

| Layer | Naming Strategy | Example |
|---|---|---|
| Public API | Rust idioms first | `Producer`, `build_single_producer()` |
| Internal implementation | LMAX terminology first | `SingleProducerSequencer`, `SequenceBarrier` |
| Type aliases | Bridge between both naming schemes | `pub type Sequence = i64;` |

## Consequences

### Positive Consequences

- **LMAX familiarity**: Java Disruptor users can directly search LMAX terms to find corresponding types
- **Documentation alignment**: Can directly reference LMAX papers and JavaDoc to explain behavior
- **Reduced confusion**: `Sequencer` and `Barrier` are no longer conflated
- **Name conciseness**: `SpinLoopHint` is 50% shorter than `BusySpinWithSpinLoopHint`

### Negative Consequences

- **v4 to v5 breaking change**: All code depending on `disruptor` v4 needs import updates
- **Migration cost**: Need to provide a migration guide and deprecation period
- **Dual terminology**: Internal use of `Sequencer` vs. external use of `Producer` may confuse contributors

### Neutral Consequences

- `pub type Sequence = i64;` already exists in `lib.rs` (currently `type Sequence = i64`)
- The `Cursor` struct still makes sense internally, but the public interface uses `Sequence`

## Alternatives Considered

| Alternative | Description | Reason for Rejection |
|---|---|---|
| Keep v4 naming | Do not rename anything | Poor experience for Java users, hard to align documentation |
| Full Java-ification | Exactly replicate all Java names | `BatchEventProcessor`, `EventTranslator` are not idiomatic in Rust |
| Gradual rename | Transition incrementally via `type alias` + `#[deprecated]` | High maintenance cost during transition, dual names are more confusing |

## References

- LMAX Disruptor Java glossary: https://lmax-exchange.github.io/disruptor/
- Rust API Guidelines (naming): https://rust-lang.github.io/api-guidelines/naming.html
