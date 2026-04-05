//! Concurrency correctness tests.
//!
//! Verifies that parallel consumers actually run concurrently,
//! and sequential stages respect ordering.

use std::sync::atomic::{AtomicI64, AtomicBool, Ordering};
use std::sync::Arc;
use disruptor_dsl::prelude::*;

struct Event { value: i64 }

/// Verify parallel consumers see all events independently.
#[test]
#[cfg(not(miri))]
fn parallel_consumers_are_independent() {
    let n = 10_000i64;
    let sum_a = Arc::new(AtomicI64::new(0));
    let sum_b = Arc::new(AtomicI64::new(0));
    let sa = sum_a.clone();
    let sb = sum_b.clone();

    let mut producer = Disruptor::single(1024, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |e: &Event, _: Sequence, _: bool| {
            sa.fetch_add(e.value, Ordering::Relaxed);
        })
        .handle_events_with(move |e: &Event, _: Sequence, _: bool| {
            sb.fetch_add(e.value, Ordering::Relaxed);
        })
        .build();

    for i in 0..n {
        producer.publish(|e| { e.value = i; });
    }
    drop(producer);

    let expected = n * (n - 1) / 2;
    assert_eq!(sum_a.load(Ordering::Relaxed), expected, "consumer A sum mismatch");
    assert_eq!(sum_b.load(Ordering::Relaxed), expected, "consumer B sum mismatch");
}

/// Verify and_then ordering: stage 2 only sees events after stage 1 processed them.
#[test]
#[cfg(not(miri))]
fn and_then_ordering_is_correct() {
    let n = 5_000i64;
    let stage1_seen = Arc::new(AtomicBool::new(false));
    let ordering_violated = Arc::new(AtomicBool::new(false));
    let s1 = stage1_seen.clone();
    let ov = ordering_violated.clone();

    let mut producer = Disruptor::single(256, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |e: &Event, seq: Sequence, _: bool| {
            // Stage 1: mark as seen when we hit the last event.
            if seq == n - 1 {
                s1.store(true, Ordering::Release);
            }
        })
        .and_then()
        .handle_events_with(move |_: &Event, seq: Sequence, _: bool| {
            // Stage 2: by the time we process the last event,
            // stage 1 should have already seen it.
            if seq == n - 1 && !stage1_seen.load(Ordering::Acquire) {
                ov.store(true, Ordering::Relaxed);
            }
        })
        .build();

    for i in 0..n {
        producer.publish(|e| { e.value = i; });
    }
    drop(producer);

    assert!(!ordering_violated.load(Ordering::Relaxed), "Stage 2 processed before stage 1");
}

/// MPSC: 4 producers correctness.
#[test]
#[cfg(not(miri))]
fn four_producer_correctness() {
    let n_per = 10_000i64;
    let count = Arc::new(AtomicI64::new(0));
    let cc = count.clone();

    let mut p1 = Disruptor::multi(8192, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            cc.fetch_add(1, Ordering::Relaxed);
        })
        .build();

    let mut p2 = p1.clone();
    let mut p3 = p1.clone();
    let mut p4 = p1.clone();

    std::thread::scope(|s| {
        s.spawn(move || { for i in 0..n_per { p1.publish(|e| { e.value = i; }); } });
        s.spawn(move || { for i in 0..n_per { p2.publish(|e| { e.value = i; }); } });
        s.spawn(move || { for i in 0..n_per { p3.publish(|e| { e.value = i; }); } });
        s.spawn(move || { for i in 0..n_per { p4.publish(|e| { e.value = i; }); } });
    });

    assert_eq!(count.load(Ordering::Relaxed), n_per * 4);
}
