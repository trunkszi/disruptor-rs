//! Panic cascade fix tests (P0-2).
//!
//! Validates that consumer panics do not cause double-panic / abort in Drop,
//! and that the `shutdown()` method correctly propagates consumer errors.

use disruptor::*;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

struct Event {
    value: i64,
}

/// Consumer panic must not cause double-panic in SingleProducer::drop().
#[test]
#[cfg(not(miri))]
fn single_producer_drop_survives_consumer_panic() {
    let result = std::panic::catch_unwind(|| {
        let mut producer = build_single_producer(64, || Event { value: 0 }, BusySpin)
            .handle_events_with(|_: &Event, seq: Sequence, _: bool| {
                if seq == 3 {
                    panic!("intentional consumer panic at seq 3");
                }
            })
            .build();

        for i in 0..10 {
            producer.publish(|e| {
                e.value = i;
            });
        }
        // Producer drops here — should NOT double-panic.
    });

    // The outer catch_unwind should succeed (no abort from double-panic).
    // The drop may or may not propagate — the key is it doesn't abort.
    assert!(result.is_ok(), "SingleProducer::drop() must not abort on consumer panic");
}

/// Consumer panic must not cause double-panic in MultiProducer::drop().
#[test]
#[cfg(not(miri))]
fn multi_producer_drop_survives_consumer_panic() {
    let result = std::panic::catch_unwind(|| {
        let mut producer = build_multi_producer(64, || Event { value: 0 }, BusySpin)
            .handle_events_with(|_: &Event, seq: Sequence, _: bool| {
                if seq == 3 {
                    panic!("intentional consumer panic at seq 3");
                }
            })
            .build();

        for i in 0..10 {
            producer.publish(|e| {
                e.value = i;
            });
        }
    });

    assert!(result.is_ok(), "MultiProducer::drop() must not abort on consumer panic");
}

/// shutdown() returns Err when consumer panics (SingleProducer).
#[test]
#[cfg(not(miri))]
fn single_producer_shutdown_reports_consumer_panic() {
    let mut producer = build_single_producer(64, || Event { value: 0 }, BusySpin)
        .handle_events_with(|_: &Event, seq: Sequence, _: bool| {
            if seq == 0 {
                panic!("intentional panic");
            }
        })
        .build();

    producer.publish(|e| {
        e.value = 1;
    });

    let result = producer.shutdown();
    assert!(result.is_err(), "shutdown() must return Err on consumer panic");

    let err = result.unwrap_err();
    assert!(
        err.message().unwrap().contains("intentional panic"),
        "error message should contain panic payload"
    );
}

/// shutdown() returns Ok when no consumer panics.
#[test]
fn single_producer_shutdown_ok_on_clean_exit() {
    let count = Arc::new(AtomicI64::new(0));
    let cc = count.clone();

    let mut producer = build_single_producer(64, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            cc.fetch_add(1, Ordering::Relaxed);
        })
        .build();

    for i in 0..100 {
        producer.publish(|e| {
            e.value = i;
        });
    }

    let result = producer.shutdown();
    assert!(result.is_ok(), "shutdown() must return Ok on clean exit");
    assert_eq!(count.load(Ordering::Relaxed), 100);
}

/// ConsumerPanic Display formatting.
#[test]
#[cfg(not(miri))]
fn consumer_panic_display_message() {
    let mut producer = build_single_producer(64, || Event { value: 0 }, BusySpin)
        .handle_events_with(|_: &Event, _: Sequence, _: bool| {
            panic!("test message");
        })
        .build();

    producer.publish(|e| {
        e.value = 1;
    });

    let err = producer.shutdown().unwrap_err();
    let display = format!("{err}");
    assert!(
        display.contains("consumer thread panicked"),
        "Display should mention consumer panic: {display}"
    );
}
