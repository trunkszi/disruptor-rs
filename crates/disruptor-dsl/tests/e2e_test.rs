//! End-to-end tests for disruptor-dsl crate.
//!
//! Tests the full pipeline: Disruptor::single/multi → publish → consume → verify.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use disruptor_dsl::prelude::*;

struct Event {
    value: i64,
}

// ─── Disruptor::single e2e ─────────────────────────────────────────────────

#[test]
fn disruptor_single_publishes_and_consumes() {
    let count = Arc::new(AtomicI64::new(0));
    let sum = Arc::new(AtomicI64::new(0));
    let cc = count.clone();
    let sc = sum.clone();

    let mut producer = Disruptor::single(64, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |e: &Event, _: Sequence, _: bool| {
            cc.fetch_add(1, Ordering::Relaxed);
            sc.fetch_add(e.value, Ordering::Relaxed);
        })
        .build();

    let n = 1000i64;
    for i in 0..n {
        producer.publish(|e| {
            e.value = i;
        });
    }

    drop(producer);

    assert_eq!(count.load(Ordering::Relaxed), n);
    assert_eq!(sum.load(Ordering::Relaxed), n * (n - 1) / 2);
}

// ─── Disruptor::multi e2e ──────────────────────────────────────────────────

#[test]
fn disruptor_multi_publishes_and_consumes() {
    let count = Arc::new(AtomicI64::new(0));
    let cc = count.clone();

    let mut producer = Disruptor::multi(64, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            cc.fetch_add(1, Ordering::Relaxed);
        })
        .build();

    for i in 0..500i64 {
        producer.publish(|e| {
            e.value = i;
        });
    }

    drop(producer);
    assert_eq!(count.load(Ordering::Relaxed), 500);
}

// ─── Batch publish ─────────────────────────────────────────────────────────

#[test]
fn disruptor_single_batch_publish() {
    let count = Arc::new(AtomicI64::new(0));
    let cc = count.clone();

    let mut producer = Disruptor::single(64, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            cc.fetch_add(1, Ordering::Relaxed);
        })
        .build();

    // 50 batches of 10 = 500 events.
    for _ in 0..50 {
        producer.batch_publish(10, |iter| {
            for e in iter {
                e.value = 42;
            }
        });
    }

    drop(producer);
    assert_eq!(count.load(Ordering::Relaxed), 500);
}

// ─── try_publish error ─────────────────────────────────────────────────────

#[test]
fn try_publish_returns_error_when_full() {
    // Atomic gate: consumer busy-spins until gate is opened (idiomatic for BusySpin).
    let gate = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let g = gate.clone();

    let mut producer = Disruptor::single(2, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            while !g.load(std::sync::atomic::Ordering::Acquire) {
                std::hint::spin_loop();
            }
        })
        .build();

    // Fill the buffer. Consumer is stalled on gate.
    let _ = producer.try_publish(|e| { e.value = 1; });
    let _ = producer.try_publish(|e| { e.value = 2; });

    // Third should fail (buffer size = 2).
    let result = producer.try_publish(|e| { e.value = 3; });
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), PublishError::Full);

    // Open gate so consumer finishes and producer can drop cleanly.
    gate.store(true, std::sync::atomic::Ordering::Release);
    drop(producer);
}

// ─── shutdown() returns Ok ─────────────────────────────────────────────────

#[test]
fn shutdown_returns_ok_on_clean_exit() {
    let count = Arc::new(AtomicI64::new(0));
    let cc = count.clone();

    let mut producer = Disruptor::single(64, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            cc.fetch_add(1, Ordering::Relaxed);
        })
        .build();

    for i in 0..100i64 {
        producer.publish(|e| { e.value = i; });
    }

    let result = producer.shutdown();
    assert!(result.is_ok());
    assert_eq!(count.load(Ordering::Relaxed), 100);
}

// ─── YieldingWaitStrategy works ────────────────────────────────────────────

#[test]
fn disruptor_with_yielding_wait_strategy() {
    let count = Arc::new(AtomicI64::new(0));
    let cc = count.clone();

    let mut producer = Disruptor::single(64, || Event { value: 0 }, YieldingWaitStrategy::default())
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            cc.fetch_add(1, Ordering::Relaxed);
        })
        .build();

    for i in 0..100i64 {
        producer.publish(|e| { e.value = i; });
    }

    drop(producer);
    assert_eq!(count.load(Ordering::Relaxed), 100);
}
