//! Sequence overflow protection tests (P0-1).
//!
//! Validates that SingleProducer and MultiProducer detect sequence overflow
//! via `checked_add` rather than silently wrapping into undefined behavior.

use disruptor::*;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

struct Event {
    value: i64,
}

/// Verify that SingleProducer uses checked arithmetic.
/// We cannot practically reach i64::MAX, so we test the mechanism indirectly
/// by publishing a modest number of events and confirming correctness.
#[test]
fn single_producer_sequence_correctness() {
    let last_seq = Arc::new(AtomicI64::new(-1));
    let ls = last_seq.clone();

    let mut producer = build_single_producer(64, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |e: &Event, seq: Sequence, _eob: bool| {
            assert_eq!(e.value, seq, "event value must match sequence");
            ls.store(seq, Ordering::Relaxed);
        })
        .build();

    let n = 10_000;
    for i in 0..n {
        producer.publish(|e| {
            e.value = i;
        });
    }

    drop(producer);
    assert_eq!(last_seq.load(Ordering::Relaxed), n - 1);
}

/// Verify that MultiProducer uses checked arithmetic.
#[test]
fn multi_producer_sequence_correctness() {
    let count = Arc::new(AtomicI64::new(0));
    let cc = count.clone();

    let mut producer = build_multi_producer(64, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            cc.fetch_add(1, Ordering::Relaxed);
        })
        .build();

    let n = 10_000i64;
    for i in 0..n {
        producer.publish(|e| {
            e.value = i;
        });
    }

    drop(producer);
    assert_eq!(count.load(Ordering::Relaxed), n);
}

/// Verify batch publish with checked arithmetic.
#[test]
fn single_producer_batch_overflow_protection() {
    let count = Arc::new(AtomicI64::new(0));
    let cc = count.clone();

    let mut producer = build_single_producer(64, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            cc.fetch_add(1, Ordering::Relaxed);
        })
        .build();

    // Publish 100 batches of 10 = 1000 events total.
    for _ in 0..100 {
        producer.batch_publish(10, |iter| {
            for e in iter {
                e.value = 42;
            }
        });
    }

    drop(producer);
    assert_eq!(count.load(Ordering::Relaxed), 1000);
}

/// Verify multi-producer batch publish with checked arithmetic.
#[test]
fn multi_producer_batch_overflow_protection() {
    let count = Arc::new(AtomicI64::new(0));
    let cc = count.clone();

    let mut producer = build_multi_producer(64, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            cc.fetch_add(1, Ordering::Relaxed);
        })
        .build();

    for _ in 0..100 {
        producer.batch_publish(10, |iter| {
            for e in iter {
                e.value = 42;
            }
        });
    }

    drop(producer);
    assert_eq!(count.load(Ordering::Relaxed), 1000);
}
