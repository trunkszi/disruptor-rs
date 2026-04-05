//! Core SPSC (Single Producer Single Consumer) tests extracted from lib.rs.
//!
//! Validates fundamental SPSC behaviors: ring buffer full, batch publication,
//! stateful consumers, pinned threads, and pipeline composition.

use std::rc::Rc;
use std::cell::RefCell;
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
#[should_panic(expected = "Size must be power of 2.")]
fn size_not_a_factor_of_2() {
    build_single_producer(3, || { 0 }, BusySpin);
}

#[test]
fn spsc_full_ringbuffer() {
    let (s, r)    = mpsc::channel();
    let barrier   = Arc::new(AtomicBool::new(true));
    let processor = {
        let barrier = Arc::clone(&barrier);
        move |e: &Event, _, _| {
            while barrier.load(Relaxed) { /* Wait. */ }
            s.send(e.num).expect("Should be able to send.");
        }
    };
    let mut producer = build_single_producer(4, factory(), BusySpinWithSpinLoopHint)
        .handle_events_with(processor)
        .build();

    for i in 0..4 {
        producer.try_publish(|e| e.num = i).expect("Should publish");
    }
    // Now ring buffer is full.
    assert_eq!(RingBufferFull, producer.try_publish(|e| e.num = 4).err().unwrap());
    // And it stays full.
    assert_eq!(RingBufferFull, producer.try_publish(|e| e.num = 4).err().unwrap());
    // Until the processor continues reading events.
    barrier.store(false, Relaxed);
    producer.publish(|e| e.num = 4);

    drop(producer);
    let result: Vec<_> = r.iter().collect();
    assert_eq!(result, [0, 1, 2, 3, 4]);
}

#[test]
fn spsc_insufficient_space_for_batch_publication() {
    let (s, r)    = mpsc::channel();
    let barrier   = Arc::new(AtomicBool::new(true));
    let processor = {
        let barrier = Arc::clone(&barrier);
        move |e: &Event, _, _| {
            while barrier.load(Relaxed) { /* Wait. */ }
            s.send(e.num).expect("Should be able to send.");
        }
    };
    let mut producer = build_single_producer(4, factory(), BusySpin)
        .handle_events_with(processor)
        .build();

    for i in 0..2 {
        producer.publish(|e| e.num = i);
    }
    assert_eq!(MissingFreeSlots(2),   producer.try_batch_publish(  4, |_iter| {} ).err().unwrap());
    assert_eq!(MissingFreeSlots(100), producer.try_batch_publish(102, |_iter| {} ).err().unwrap());

    barrier.store(false, Relaxed);
    producer.try_batch_publish(2, |iter| {
        for e in iter {
            e.num = 2;
        }
    }).expect("Batch publication should now succeed.");

    drop(producer);
    let result: Vec<_> = r.iter().collect();
    assert_eq!(result, [0, 1, 2, 2]);
}

#[test]
fn spsc_disruptor() {
    let (s, r)    = mpsc::channel();
    let processor = move |e: &Event, _, _| {
        s.send(e.num).expect("Should be able to send.");
    };
    let mut producer = build_single_producer(8, factory(), BusySpin)
        .handle_events_with(processor)
        .build();

    thread::scope(|s| {
        s.spawn(move || {
            for i in 0..10 {
                producer.publish(|e| e.num = i*i );
            }
        });
    });

    let result: Vec<_> = r.iter().collect();
    assert_eq!(result, [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]);
}

#[cfg_attr(miri, ignore)]
#[test]
fn spsc_disruptor_with_pinned_and_named_thread() {
    let (s, r)    = mpsc::channel();
    let processor = move |e: &Event, _, _| {
        s.send(e.num).expect("Should be able to send.");
    };
    let mut producer = build_single_producer(8, factory(), BusySpin)
        .pin_at_core(0).thread_name("my-processor").handle_events_with(processor)
        .build();

    thread::scope(|s| {
        s.spawn(move || {
            for i in 0..10 {
                producer.publish(|e| e.num = i*i );
            }
        });
    });

    let result: Vec<_> = r.iter().collect();
    assert_eq!(result, [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]);
}

#[test]
fn spsc_disruptor_with_batch_publication() {
    let (s, r)    = mpsc::channel();
    let processor = move |e: &Event, _, _| {
        s.send(e.num).expect("Should be able to send.");
    };
    let mut producer = build_single_producer(8, factory(), BusySpin)
        .handle_events_with(processor)
        .build();

    let mut i = 0;
    for _ in 0..3 {
        producer.batch_publish(3, |iter| {
            // We are guaranteed that the iterator will yield three elements:
            assert_eq!((3, Some(3)), iter.size_hint());

            // Publish.
            for e in iter {
                e.num = i*i;
                i    += 1;
            }
        });
    }
    drop(producer);

    let result: Vec<_> = r.iter().collect();
    assert_eq!(result, [0, 1, 4, 9, 16, 25, 36, 49, 64]);
}

#[test]
fn spsc_disruptor_with_zero_batch_publication() {
    let processor = |_e: &Event, _, _| {
        panic!("No events should be published.");
    };
    let mut producer = build_single_producer(8, factory(), BusySpin)
        .handle_events_with(processor)
        .build();

    producer.batch_publish(0, |iter| {
        assert_eq!(0, iter.count());
    });
    drop(producer);
}

#[test]
fn spsc_disruptor_with_state() {
    let (s, r)           = mpsc::channel();
    let initialize_state = || { Rc::new(RefCell::new(0)) };
    let processor        = move |state: &mut Rc<RefCell<i64>>, e: &Event, _, _| {
        let mut ref_cell = state.borrow_mut();
        *ref_cell       += e.num;
        s.send(*ref_cell).expect("Should be able to send.");
    };
    let mut producer     = build_single_producer(8, factory(), BusySpin)
        .handle_events_and_state_with(processor, initialize_state)
        .build();

    for i in 0..10 {
        producer.publish(|e| e.num = i );
    }

    drop(producer);
    let result: Vec<_> = r.iter().collect();
    assert_eq!(result, [0, 1, 3, 6, 10, 15, 21, 28, 36, 45]);
}

#[test]
fn pipeline_of_two_spsc_disruptors() {
    let (s, r)    = mpsc::channel();
    let processor = move |e: &Event, _, _| {
        s.send(e.num).expect("Should be able to send.");
    };

    // Last Disruptor.
    let mut producer = build_single_producer(8, factory(), BusySpin)
        .handle_events_with(processor)
        .build();
    let processor = move |e: &Event, _, _| {
        producer.publish(|e2| e2.num = e.num );
    };

    // First Disruptor.
    let mut producer = build_single_producer(8, factory(), BusySpin)
        .handle_events_with(processor)
        .build();
    for i in 0..10 {
        producer.publish(|e| e.num = i*i );
    }

    drop(producer);
    let result: Vec<_> = r.iter().collect();
    assert_eq!(result, [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]);
}
