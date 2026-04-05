//! Pipeline topology tests: Producer → Stage A → Stage B → Stage C.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use disruptor_dsl::prelude::*;

struct Event {
    value: i64,
}

#[test]
fn pipeline_two_stages() {
    let stage1_count = Arc::new(AtomicI64::new(0));
    let stage2_count = Arc::new(AtomicI64::new(0));
    let s1 = stage1_count.clone();
    let s2 = stage2_count.clone();

    let mut producer = Disruptor::single(64, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            s1.fetch_add(1, Ordering::Relaxed);
        })
        .and_then()
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            s2.fetch_add(1, Ordering::Relaxed);
        })
        .build();

    for i in 0..100i64 {
        producer.publish(|e| { e.value = i; });
    }

    drop(producer);

    assert_eq!(stage1_count.load(Ordering::Relaxed), 100);
    assert_eq!(stage2_count.load(Ordering::Relaxed), 100);
}

#[test]
fn parallel_consumers_then_join() {
    let c1 = Arc::new(AtomicI64::new(0));
    let c2 = Arc::new(AtomicI64::new(0));
    let cc1 = c1.clone();
    let cc2 = c2.clone();

    // Two consumers in parallel, both see all events.
    let mut producer = Disruptor::single(64, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            cc1.fetch_add(1, Ordering::Relaxed);
        })
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            cc2.fetch_add(1, Ordering::Relaxed);
        })
        .build();

    for i in 0..100i64 {
        producer.publish(|e| { e.value = i; });
    }

    drop(producer);

    assert_eq!(c1.load(Ordering::Relaxed), 100);
    assert_eq!(c2.load(Ordering::Relaxed), 100);
}

#[test]
fn multi_producer_pipeline() {
    let count = Arc::new(AtomicI64::new(0));
    let cc = count.clone();

    let mut p1 = Disruptor::multi(64, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            cc.fetch_add(1, Ordering::Relaxed);
        })
        .build();

    let mut p2 = p1.clone();

    std::thread::scope(|s| {
        s.spawn(move || {
            for i in 0..50i64 {
                p1.publish(|e| { e.value = i; });
            }
        });
        s.spawn(move || {
            for i in 50..100i64 {
                p2.publish(|e| { e.value = i; });
            }
        });
    });

    assert_eq!(count.load(Ordering::Relaxed), 100);
}
