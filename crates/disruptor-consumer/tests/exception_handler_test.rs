//! ExceptionHandler tests: Fatal halts, Ignore continues, Logging continues.

use std::sync::atomic::{AtomicI64, AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use disruptor_core::*;
use disruptor_consumer::*;
use disruptor_wait::BusySpin;

struct Event {
    value: i64,
}

#[test]
fn fatal_exception_handler_halts_on_panic() {
    let ring_buffer = Arc::new(RingBuffer::new(64, || Event { value: 0 }));
    let cursor = Arc::new(Cursor::new());
    let barrier = SingleConsumerBarrier::new(Arc::clone(&cursor));
    let seq_barrier = ProcessingSequenceBarrier::new(barrier, BusySpin);

    let processed = Arc::new(AtomicI64::new(0));
    let pc = processed.clone();

    let handler = move |_: &Event, seq: Sequence, _: bool| {
        if seq == 2 {
            panic!("fatal at seq 2");
        }
        pc.fetch_add(1, Ordering::Relaxed);
    };

    let mut processor = BatchEventProcessor::new(handler, seq_barrier);
    processor.set_exception_handler(Box::new(FatalExceptionHandler));

    for i in 0..5 {
        let ptr = ring_buffer.get(i);
        unsafe { (*ptr).value = i; }
    }
    cursor.store(4);

    let proc_seq = Arc::clone(processor.sequence());
    let rb = Arc::clone(&ring_buffer);
    let _handle = thread::spawn(move || {
        processor.run(&rb);
    });

    // Wait for processor to halt (should stop at seq 2).
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while proc_seq.relaxed_value() < 2 {
        if std::time::Instant::now() > deadline {
            break;
        }
        std::hint::spin_loop();
    }

    // With FatalExceptionHandler: only events 0, 1 processed (halted at 2).
    let count = processed.load(Ordering::Relaxed);
    assert_eq!(count, 2, "Should halt after processing seq 0 and 1, got {count}");
}

#[test]
fn logging_exception_handler_continues() {
    let ring_buffer = Arc::new(RingBuffer::new(64, || Event { value: 0 }));
    let cursor = Arc::new(Cursor::new());
    let barrier = SingleConsumerBarrier::new(Arc::clone(&cursor));
    let seq_barrier = ProcessingSequenceBarrier::new(barrier, BusySpin);

    let processed = Arc::new(AtomicI64::new(0));
    let pc = processed.clone();

    let handler = move |_: &Event, seq: Sequence, _: bool| {
        if seq == 2 {
            panic!("log this panic");
        }
        pc.fetch_add(1, Ordering::Relaxed);
    };

    let mut processor = BatchEventProcessor::new(handler, seq_barrier);
    processor.set_exception_handler(Box::new(LoggingExceptionHandler));

    for i in 0..5 {
        let ptr = ring_buffer.get(i);
        unsafe { (*ptr).value = i; }
    }
    cursor.store(4);

    let proc_seq = Arc::clone(processor.sequence());
    let rb = Arc::clone(&ring_buffer);
    let _handle = thread::spawn(move || {
        processor.run(&rb);
    });

    while proc_seq.relaxed_value() < 4 {
        std::hint::spin_loop();
    }

    // Events 0, 1, 3, 4 processed (seq 2 panicked but logging handler continues).
    assert_eq!(processed.load(Ordering::Relaxed), 4);
}

#[test]
fn exception_action_variants() {
    assert_ne!(ExceptionAction::Continue, ExceptionAction::Halt);
    assert_eq!(ExceptionAction::Continue, ExceptionAction::Continue);
    assert_eq!(format!("{:?}", ExceptionAction::Halt), "Halt");
}
