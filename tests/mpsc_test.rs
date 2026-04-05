//! Multi Producer Single Consumer integration tests.
//!
//! Validates the Sequencer (3P-1C) topology from the LMAX Disruptor paper.

use disruptor::*;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::thread;

struct Event {
    value: i64,
}

#[cfg(not(miri))]
#[test]
fn mpsc_multiple_producers_all_events_consumed() {
    let count = Arc::new(AtomicI64::new(0));
    let sum = Arc::new(AtomicI64::new(0));
    let cc = count.clone();
    let sc = sum.clone();

    let producer = build_multi_producer(1024, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |e: &Event, _: Sequence, _: bool| {
            cc.fetch_add(1, Ordering::Relaxed);
            sc.fetch_add(e.value, Ordering::Relaxed);
        })
        .build();

    let num_producers = 4;
    let events_per_producer = 10_000;

    let handles: Vec<_> = (0..num_producers)
        .map(|id| {
            let mut p = producer.clone();
            thread::spawn(move || {
                for i in 0..events_per_producer {
                    p.publish(|e| {
                        e.value = (id * events_per_producer + i) as i64;
                    });
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    drop(producer);

    let total = num_producers * events_per_producer;
    assert_eq!(count.load(Ordering::Relaxed), total as i64);
}

#[cfg(not(miri))]
#[test]
fn mpsc_batch_publish_from_multiple_producers() {
    let count = Arc::new(AtomicI64::new(0));
    let cc = count.clone();

    let producer = build_multi_producer(1024, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            cc.fetch_add(1, Ordering::Relaxed);
        })
        .build();

    let handles: Vec<_> = (0..3)
        .map(|_| {
            let mut p = producer.clone();
            thread::spawn(move || {
                for _ in 0..100 {
                    p.batch_publish(10, |iter| {
                        for e in iter {
                            e.value = 1;
                        }
                    });
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    drop(producer);
    assert_eq!(count.load(Ordering::Relaxed), 3000);
}
