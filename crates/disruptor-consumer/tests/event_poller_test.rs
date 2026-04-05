//! EventPoller tests for pull-based consumption in the new crate.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use crossbeam_utils::CachePadded;
use disruptor_core::*;
use disruptor_consumer::*;

struct Event {
    value: i64,
}

fn setup(size: usize) -> (Arc<RingBuffer<Event>>, Arc<Cursor>, Arc<CachePadded<AtomicI64>>) {
    let rb = Arc::new(RingBuffer::new(size, || Event { value: 0 }));
    let cursor = Arc::new(Cursor::new());
    let shutdown = Arc::new(CachePadded::new(AtomicI64::new(NONE)));
    (rb, cursor, shutdown)
}

#[test]
fn poller_returns_no_events_when_empty() {
    let (rb, cursor, shutdown) = setup(64);
    let barrier = SingleConsumerBarrier::new(Arc::clone(&cursor));
    let poller_cursor = Arc::new(Cursor::new());
    let mut poller = EventPoller::new(rb, Arc::new(barrier), shutdown, poller_cursor);

    match poller.poll() {
        Err(Polling::NoEvents) => {} // expected
        Err(_) => unreachable!(),
        other => panic!("Expected NoEvents, got {:?}", other.map(|_| "Ok")),
    }
}

#[test]
fn poller_reads_published_events() {
    let (rb, cursor, shutdown) = setup(64);

    // Publish 5 events.
    for i in 0..5i64 {
        let ptr = rb.get(i);
        unsafe { (*ptr).value = i * 10; }
    }
    cursor.store(4); // publish 0..=4

    let barrier = SingleConsumerBarrier::new(Arc::clone(&cursor));
    let poller_cursor = Arc::new(Cursor::new());
    let mut poller = EventPoller::new(Arc::clone(&rb), Arc::new(barrier), Arc::clone(&shutdown), poller_cursor);

    match poller.poll() {
        Ok(mut guard) => {
            let events: Vec<i64> = (&mut guard).map(|e| e.value).collect();
            assert_eq!(events, vec![0, 10, 20, 30, 40]);
        }
        Err(e) => panic!("Expected Ok, got {:?}", e),
    }
}

#[test]
fn poller_take_limits_batch_size() {
    let (rb, cursor, shutdown) = setup(64);

    for i in 0..10i64 {
        let ptr = rb.get(i);
        unsafe { (*ptr).value = i; }
    }
    cursor.store(9);

    let barrier = SingleConsumerBarrier::new(Arc::clone(&cursor));
    let poller_cursor = Arc::new(Cursor::new());
    let mut poller = EventPoller::new(Arc::clone(&rb), Arc::new(barrier), Arc::clone(&shutdown), poller_cursor);

    match poller.take(3) {
        Ok(mut guard) => {
            let count = (&mut guard).count();
            assert_eq!(count, 3);
        }
        Err(e) => panic!("Expected Ok, got {:?}", e),
    }
}

#[test]
fn poller_returns_shutdown() {
    let (rb, cursor, shutdown) = setup(64);
    // Signal shutdown at sequence 0.
    shutdown.store(0, Ordering::Relaxed);

    let barrier = SingleConsumerBarrier::new(Arc::clone(&cursor));
    let poller_cursor = Arc::new(Cursor::new());
    let mut poller = EventPoller::new(rb, Arc::new(barrier), shutdown, poller_cursor);

    match poller.poll() {
        Err(Polling::Shutdown) => {} // expected
        Err(_) => unreachable!(),
        other => panic!("Expected Shutdown, got {:?}", other.map(|_| "Ok")),
    }
}

#[test]
fn poller_has_available() {
    let (rb, cursor, shutdown) = setup(64);
    let barrier = SingleConsumerBarrier::new(Arc::clone(&cursor));
    let poller_cursor = Arc::new(Cursor::new());
    let poller = EventPoller::new(Arc::clone(&rb), Arc::new(barrier), shutdown, poller_cursor);

    assert!(!poller.has_available());

    cursor.store(0);
    assert!(poller.has_available());
}

#[test]
fn poller_exact_size_iterator() {
    let (rb, cursor, shutdown) = setup(64);

    for i in 0..8i64 {
        let ptr = rb.get(i);
        unsafe { (*ptr).value = i; }
    }
    cursor.store(7);

    let barrier = SingleConsumerBarrier::new(Arc::clone(&cursor));
    let poller_cursor = Arc::new(Cursor::new());
    let mut poller = EventPoller::new(Arc::clone(&rb), Arc::new(barrier), shutdown, poller_cursor);

    match poller.poll() {
        Ok(mut guard) => {
            assert_eq!((&mut guard).len(), 8);
        }
        Err(e) => panic!("Expected Ok, got {:?}", e),
    }
}

#[test]
fn poller_take_zero_returns_empty_guard() {
    let (rb, cursor, shutdown) = setup(64);
    let ptr = rb.get(0);
    unsafe { (*ptr).value = 99; }
    cursor.store(0);

    let barrier = SingleConsumerBarrier::new(Arc::clone(&cursor));
    let poller_cursor = Arc::new(Cursor::new());
    let mut poller = EventPoller::new(rb, Arc::new(barrier), shutdown, poller_cursor);

    // take(0) should return Ok with an empty guard (no events yielded).
    match poller.take(0) {
        Ok(mut guard) => {
            let count = (&mut guard).count();
            assert_eq!(count, 0, "take(0) should yield 0 events");
        }
        Err(e) => panic!("Expected Ok(empty), got {:?}", e),
    }
}

#[test]
fn poller_sequential_polls_advance_cursor() {
    let (rb, cursor, shutdown) = setup(64);

    for i in 0..10i64 {
        let ptr = rb.get(i);
        unsafe { (*ptr).value = i; }
    }
    cursor.store(9);

    let barrier = SingleConsumerBarrier::new(Arc::clone(&cursor));
    let poller_cursor = Arc::new(Cursor::new());
    let mut poller = EventPoller::new(Arc::clone(&rb), Arc::new(barrier), Arc::clone(&shutdown), poller_cursor);

    // First poll: take 3.
    let guard = poller.take(3).unwrap();
    let vals: Vec<i64> = (&mut { guard }).map(|e| e.value).collect();
    assert_eq!(vals, vec![0, 1, 2]);

    // Second poll: should continue from 3.
    let guard2 = poller.take(3).unwrap();
    let vals2: Vec<i64> = (&mut { guard2 }).map(|e| e.value).collect();
    assert_eq!(vals2, vec![3, 4, 5]);
}
