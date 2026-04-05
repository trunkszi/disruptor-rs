//! Tests for ProcessingSequenceBarrier.

use std::sync::Arc;
use std::thread;
use disruptor_core::*;
use disruptor_consumer::*;
use disruptor_wait::BusySpin;

#[test]
fn processing_barrier_returns_available_sequence() {
    let cursor = Arc::new(Cursor::new());
    let dep = SingleConsumerBarrier::new(Arc::clone(&cursor));
    let barrier = ProcessingSequenceBarrier::new(dep, BusySpin);

    // Nothing published yet.
    assert!(!barrier.is_alerted());

    // Publish seq 5.
    cursor.store(5);
    let result = barrier.wait_for(0);
    assert!(result.is_ok());
    assert!(result.unwrap() >= 0);
}

#[test]
fn processing_barrier_alert_interrupts_wait() {
    let cursor = Arc::new(Cursor::new());
    let dep = SingleConsumerBarrier::new(Arc::clone(&cursor));
    let barrier = Arc::new(ProcessingSequenceBarrier::new(dep, BusySpin));

    let b = Arc::clone(&barrier);
    let handle = thread::spawn(move || {
        // This will block because nothing is published.
        b.wait_for(0)
    });

    // Give thread time to start waiting.
    std::thread::sleep(std::time::Duration::from_millis(10));

    // Alert should interrupt.
    barrier.alert();

    let result = handle.join().unwrap();
    assert!(result.is_err(), "wait_for should return AlertError");
}

#[test]
fn processing_barrier_clear_alert() {
    let cursor = Arc::new(Cursor::new());
    let dep = SingleConsumerBarrier::new(Arc::clone(&cursor));
    let barrier = ProcessingSequenceBarrier::new(dep, BusySpin);

    barrier.alert();
    assert!(barrier.is_alerted());

    barrier.clear_alert();
    assert!(!barrier.is_alerted());
}

#[test]
fn processing_barrier_get_cursor() {
    let cursor = Arc::new(Cursor::new());
    let dep = SingleConsumerBarrier::new(Arc::clone(&cursor));
    let barrier = ProcessingSequenceBarrier::new(dep, BusySpin);

    assert_eq!(barrier.get_cursor(), NONE);
    cursor.store(42);
    assert_eq!(barrier.get_cursor(), 42);
}
