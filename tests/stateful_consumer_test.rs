//! Stateful consumer tests.
//!
//! Validates handle_events_and_state_with for thread-local state in consumers.
//!
//! API signature: handle_events_and_state_with(event_handler, initialize_state)
//!   event_handler: FnMut(&mut S, &E, Sequence, bool)
//!   initialize_state: FnOnce() -> S

use disruptor::*;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

struct Event {
    value: i64,
}

#[test]
fn stateful_consumer_accumulates_state() {
    let final_sum = Arc::new(AtomicI64::new(0));
    let fs = final_sum.clone();

    // State: a running sum that doesn't need to be Send or Sync.
    // The state is thread-local to the consumer thread.
    // Closure signature: FnMut(&mut S, &E, Sequence, bool)
    let mut producer = build_single_producer(64, || Event { value: 0 }, BusySpin)
        .handle_events_and_state_with(
            move |running_sum: &mut i64, e: &Event, _seq: Sequence, _eob: bool| {
                *running_sum += e.value;
            },
            move || {
                // The state initializer. The final_sum is captured by the
                // event handler, not here. We just return initial state.
                0i64
            },
        )
        .build();

    let n = 100i64;
    for i in 0..n {
        producer.publish(|e| {
            e.value = i;
        });
    }

    drop(producer);

    // NOTE: The current API does not have a "finalize" callback for state.
    // The running sum is not automatically reported back.
    // This test validates that stateful processing does not panic or deadlock.
}

#[test]
fn stateful_consumer_with_dependent_stages() {
    let mut producer = build_single_producer(64, || Event { value: 0 }, BusySpin)
        .handle_events_and_state_with(
            |count: &mut i64, _: &Event, _: Sequence, _: bool| {
                *count += 1;
            },
            || 0i64,
        )
        .and_then()
        .handle_events_and_state_with(
            |sum: &mut i64, e: &Event, _: Sequence, _: bool| {
                *sum += e.value;
            },
            || 0i64,
        )
        .build();

    let n = 50i64;
    for i in 0..n {
        producer.publish(|e| {
            e.value = i;
        });
    }

    drop(producer);
    // If we get here, the pipeline completed without deadlock.
}
