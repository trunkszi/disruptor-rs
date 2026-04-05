//! Core MPSC (Multi Producer Single Consumer) tests extracted from lib.rs.
//!
//! Validates multi-producer ring buffer full behavior, batch publication,
//! and concurrent multi-publisher correctness.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::mpsc;
use std::thread;
use disruptor::*;

#[derive(Debug)]
struct Event {
    num: i64,
}

fn factory() -> impl Fn() -> Event {
    || Event { num: -1 }
}

#[test]
fn mpsc_full_ringbuffer() {
    let (s, r)    = mpsc::channel();
    let barrier   = Arc::new(AtomicBool::new(true));
    let processor = {
        let barrier = Arc::clone(&barrier);
        move |e: &Event, _, _| {
            while barrier.load(Relaxed) { /* Wait. */ }
            s.send(e.num).expect("Should be able to send.");
        }
    };
    let mut producer1 = build_multi_producer(64, factory(), BusySpinWithSpinLoopHint)
        .handle_events_with(processor)
        .build();

    let mut producer2 = producer1.clone();

    for i in 0..64 {
        producer1.try_publish(|e| e.num = i).expect("Should publish");
    }

    // Now ring buffer is full.
    assert_eq!(RingBufferFull, producer1.try_publish(|e| e.num = 4).err().unwrap());
    // And it is full also as seen from second producer.
    assert_eq!(RingBufferFull, producer2.try_publish(|e| e.num = 4).err().unwrap());
    // Until the processor continues reading events.
    barrier.store(false, Relaxed);
    producer1.publish(|e| e.num = 64);
    producer2.publish(|e| e.num = 65);

    drop(producer1);
    drop(producer2);
    let result: Vec<_> = r.iter().collect();
    assert_eq!(result, (0..=65).into_iter().collect::<Vec<i64>>());
}

#[test]
fn mpsc_insufficient_space_for_batch_publication() {
    let (s, r)    = mpsc::channel();
    let barrier   = Arc::new(AtomicBool::new(true));
    let processor = {
        let barrier = Arc::clone(&barrier);
        move |e: &Event, _, _| {
            while barrier.load(Relaxed) { /* Wait. */ }
            s.send(e.num).expect("Should be able to send.");
        }
    };
    let mut producer1 = build_multi_producer(64, factory(), BusySpin)
        .handle_events_with(processor)
        .build();
    let mut producer2 = producer1.clone();

    for i in 0..58 {
        producer1.publish(|e| e.num = i);
    }
    assert_eq!(MissingFreeSlots(2),   producer1.try_batch_publish(  8, |_iter| {} ).err().unwrap());
    assert_eq!(MissingFreeSlots(100), producer1.try_batch_publish(106, |_iter| {} ).err().unwrap());
    assert_eq!(MissingFreeSlots(2),   producer2.try_batch_publish(  8, |_iter| {} ).err().unwrap());
    assert_eq!(MissingFreeSlots(100), producer2.try_batch_publish(106, |_iter| {} ).err().unwrap());

    barrier.store(false, Relaxed);
    producer1.try_batch_publish(2, |iter| {
        for e in iter {
            e.num = 2;
        }
    }).expect("Batch publication should now succeed.");
    producer2.try_batch_publish(2, |iter| {
        for e in iter {
            e.num = 3;
        }
    }).expect("Batch publication should now succeed.");

    drop(producer1);
    drop(producer2);
    let mut result: Vec<_> = r.iter().collect();
    result.sort();
    // Initial events published.
    let mut expected = (0..58).into_iter().collect::<Vec<i64>>();
    // Now add the two successfull batch publications.
    expected.push(2);
    expected.push(2);
    expected.push(3);
    expected.push(3);
    expected.sort();
    assert_eq!(result, expected);
}

#[test]
fn multi_publisher_disruptor() {
    let (s, r)    = mpsc::channel();
    let processor = move |e: &Event, _, _| {
        s.send(e.num).expect("Should be able to send.");
    };

    let mut producer1 = build_multi_producer(64, factory(), BusySpinWithSpinLoopHint)
        .handle_events_with(processor)
        .build();
    let mut producer2 = producer1.clone();

    let num_items = 100;

    thread::scope(|s| {
        s.spawn(move || {
            for i in 0..num_items/2 {
                producer1.publish(|e| e.num = i);
            }
        });

        s.spawn(move || {
            for i in (num_items/2)..num_items {
                producer2.publish(|e| e.num = i);
            }
        });
    });

    let mut result: Vec<_> = r.iter().collect();
    result.sort();

    let expected: Vec<i64> = (0..num_items).collect();
    assert_eq!(result, expected);
}

#[test]
fn multi_publisher_disruptor_with_batch_publication() {
    let (s, r)    = mpsc::channel();
    let processor = move |e: &Event, _, _| {
        s.send(e.num).expect("Should be able to send.");
    };

    let mut producer1 = build_multi_producer(64, factory(), BusySpin)
        .handle_events_with(processor)
        .build();
    let mut producer2 = producer1.clone();

    let num_items  = 100_i64;
    let batch_size = 5;

    thread::scope(|s| {
        s.spawn(move || {
            for b in 0..(num_items/2)/batch_size {
                producer1.batch_publish(batch_size as usize, |iter| {
                    for (i, e) in iter.enumerate() {
                        e.num = batch_size*b + i as i64;
                    }
                });
            }
        });

        s.spawn(move || {
            for i in (num_items/2)..num_items {
                producer2.publish(|e| e.num = i as i64);
            }
        });
    });

    let mut result: Vec<_> = r.iter().collect();
    result.sort();

    let expected: Vec<i64> = (0..num_items).collect();
    assert_eq!(result, expected);
}
