//! Boundary condition and stress tests.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use disruptor_dsl::prelude::*;

struct Event { value: i64 }

// ─── Boundary: minimum buffer size ────────────────────────────────────────

#[test]
fn minimum_buffer_size_2() {
    let last = Arc::new(AtomicI64::new(-1));
    let lc = last.clone();

    let mut producer = Disruptor::single(2, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |e: &Event, _: Sequence, _: bool| {
            lc.store(e.value, Ordering::Relaxed);
        })
        .build();

    for i in 0..100i64 {
        producer.publish(|e| { e.value = i; });
    }
    drop(producer);
    assert_eq!(last.load(Ordering::Relaxed), 99);
}

// ─── Boundary: batch exactly fills buffer ─────────────────────────────────

#[test]
fn batch_exactly_fills_buffer() {
    let count = Arc::new(AtomicI64::new(0));
    let cc = count.clone();

    let mut producer = Disruptor::single(8, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            cc.fetch_add(1, Ordering::Relaxed);
        })
        .build();

    // Batch of exactly buffer size.
    producer.batch_publish(8, |iter| {
        for (i, e) in iter.enumerate() {
            e.value = i as i64;
        }
    });

    drop(producer);
    assert_eq!(count.load(Ordering::Relaxed), 8);
}

// ─── Boundary: try_batch_publish larger than buffer ───────────────────────

#[test]
fn try_batch_larger_than_buffer_fails() {
    let mut producer = Disruptor::single(8, || Event { value: 0 }, BusySpin)
        .handle_events_with(|_: &Event, _: Sequence, _: bool| {})
        .build();

    let result = producer.try_batch_publish(16, |iter| {
        for e in iter { e.value = 1; }
    });
    assert!(result.is_err());
    drop(producer);
}

// ─── Stress: 1M events SPSC correctness ───────────────────────────────────

#[test]
#[cfg(not(miri))]
fn stress_1m_events_spsc_correctness() {
    let n = 1_000_000i64;
    let sum = Arc::new(AtomicI64::new(0));
    let count = Arc::new(AtomicI64::new(0));
    let sc = sum.clone();
    let cc = count.clone();

    let mut producer = Disruptor::single(65536, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |e: &Event, _: Sequence, _: bool| {
            sc.fetch_add(e.value, Ordering::Relaxed);
            cc.fetch_add(1, Ordering::Relaxed);
        })
        .build();

    for i in 0..n {
        producer.publish(|e| { e.value = i; });
    }
    drop(producer);

    assert_eq!(count.load(Ordering::Relaxed), n);
    assert_eq!(sum.load(Ordering::Relaxed), n * (n - 1) / 2);
}

// ─── Stress: 1M events MPSC correctness ───────────────────────────────────

#[test]
#[cfg(not(miri))]
fn stress_1m_events_mpsc_correctness() {
    let n_per_producer = 500_000i64;
    let count = Arc::new(AtomicI64::new(0));
    let cc = count.clone();

    let mut p1 = Disruptor::multi(65536, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            cc.fetch_add(1, Ordering::Relaxed);
        })
        .build();
    let mut p2 = p1.clone();

    std::thread::scope(|s| {
        s.spawn(move || {
            for i in 0..n_per_producer {
                p1.publish(|e| { e.value = i; });
            }
        });
        s.spawn(move || {
            for i in 0..n_per_producer {
                p2.publish(|e| { e.value = i; });
            }
        });
    });

    assert_eq!(count.load(Ordering::Relaxed), n_per_producer * 2);
}

// ─── Stress: multiple rounds wrapping ring buffer ─────────────────────────

#[test]
fn many_rounds_wrapping() {
    let count = Arc::new(AtomicI64::new(0));
    let cc = count.clone();

    // Small buffer, many events → lots of wrapping.
    let mut producer = Disruptor::single(4, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            cc.fetch_add(1, Ordering::Relaxed);
        })
        .build();

    for i in 0..10_000i64 {
        producer.publish(|e| { e.value = i; });
    }
    drop(producer);
    assert_eq!(count.load(Ordering::Relaxed), 10_000);
}
