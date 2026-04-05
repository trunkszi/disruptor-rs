//! Consumer topology tests.
//!
//! Validates Pipeline, Diamond, Multicast, and DAG topologies
//! described in the LMAX Disruptor paper.

use disruptor::*;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

struct Event {
    value: i64,
}

// ─── Pipeline (1P → A → B → C) ─────────────────────────────────────────────

#[test]
fn pipeline_three_stage() {
    let stage_a_count = Arc::new(AtomicI64::new(0));
    let stage_b_count = Arc::new(AtomicI64::new(0));
    let stage_c_count = Arc::new(AtomicI64::new(0));
    let ac = stage_a_count.clone();
    let bc = stage_b_count.clone();
    let cc = stage_c_count.clone();

    let mut producer = build_single_producer(64, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            ac.fetch_add(1, Ordering::Relaxed);
        })
        .and_then()
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            bc.fetch_add(1, Ordering::Relaxed);
        })
        .and_then()
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            cc.fetch_add(1, Ordering::Relaxed);
        })
        .build();

    let n = 500;
    for i in 0..n {
        producer.publish(|e| {
            e.value = i;
        });
    }

    drop(producer);

    assert_eq!(stage_a_count.load(Ordering::Relaxed), n);
    assert_eq!(stage_b_count.load(Ordering::Relaxed), n);
    assert_eq!(stage_c_count.load(Ordering::Relaxed), n);
}

// ─── Multicast (1P → A, B, C parallel) ─────────────────────────────────────

#[test]
fn multicast_parallel_consumers() {
    let count_a = Arc::new(AtomicI64::new(0));
    let count_b = Arc::new(AtomicI64::new(0));
    let ca = count_a.clone();
    let cb = count_b.clone();

    let mut producer = build_single_producer(64, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            ca.fetch_add(1, Ordering::Relaxed);
        })
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            cb.fetch_add(1, Ordering::Relaxed);
        })
        .build();

    let n = 200;
    for i in 0..n {
        producer.publish(|e| {
            e.value = i;
        });
    }

    drop(producer);

    // Both consumers see all events.
    assert_eq!(count_a.load(Ordering::Relaxed), n);
    assert_eq!(count_b.load(Ordering::Relaxed), n);
}

// ─── Diamond (1P → A|B → C) ────────────────────────────────────────────────

#[test]
fn diamond_topology() {
    let count_a = Arc::new(AtomicI64::new(0));
    let count_b = Arc::new(AtomicI64::new(0));
    let count_c = Arc::new(AtomicI64::new(0));
    let ca = count_a.clone();
    let cb = count_b.clone();
    let cc = count_c.clone();

    let mut producer = build_single_producer(64, || Event { value: 0 }, BusySpin)
        // Stage 1: A and B in parallel
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            ca.fetch_add(1, Ordering::Relaxed);
        })
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            cb.fetch_add(1, Ordering::Relaxed);
        })
        // Stage 2: C depends on both A and B
        .and_then()
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            cc.fetch_add(1, Ordering::Relaxed);
        })
        .build();

    let n = 300;
    for i in 0..n {
        producer.publish(|e| {
            e.value = i;
        });
    }

    drop(producer);

    assert_eq!(count_a.load(Ordering::Relaxed), n);
    assert_eq!(count_b.load(Ordering::Relaxed), n);
    assert_eq!(count_c.load(Ordering::Relaxed), n);
}

// ─── Chained Disruptors (Pipeline of two separate Disruptors) ───────────────

#[test]
fn chained_disruptors() {
    let final_count = Arc::new(AtomicI64::new(0));
    let fc = final_count.clone();

    // Second disruptor
    let mut producer2 = build_single_producer(64, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            fc.fetch_add(1, Ordering::Relaxed);
        })
        .build();

    // First disruptor feeds into second
    let mut producer1 = build_single_producer(64, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |e: &Event, _: Sequence, _: bool| {
            producer2.publish(|e2| {
                e2.value = e.value;
            });
        })
        .build();

    let n = 100;
    for i in 0..n {
        producer1.publish(|e| {
            e.value = i;
        });
    }

    drop(producer1);

    assert_eq!(final_count.load(Ordering::Relaxed), n);
}
