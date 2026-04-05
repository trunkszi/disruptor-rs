//! Integration tests for BatchEventProcessor.

use std::sync::atomic::{AtomicI64, AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use disruptor_core::*;
use disruptor_consumer::*;
use disruptor_wait::BusySpin;

struct TestEvent {
    value: i64,
}

// ─── Basic processing ──────────────────────────────────────────────────────

#[test]
fn batch_event_processor_processes_all_events() {
    let ring_buffer = Arc::new(RingBuffer::new(64, || TestEvent { value: 0 }));
    let cursor = Arc::new(Cursor::new());
    let barrier = SingleConsumerBarrier::new(Arc::clone(&cursor));
    let seq_barrier = ProcessingSequenceBarrier::new(barrier, BusySpin);

    let count = Arc::new(AtomicI64::new(0));
    let sum = Arc::new(AtomicI64::new(0));
    let cc = count.clone();
    let sc = sum.clone();

    let handler = move |_event: &TestEvent, _seq: Sequence, _eob: bool| {
        cc.fetch_add(1, Ordering::Relaxed);
        sc.fetch_add(1, Ordering::Relaxed);
    };

    let mut processor = BatchEventProcessor::new(handler, seq_barrier);
    let proc_seq = Arc::clone(processor.sequence());
    let rb = Arc::clone(&ring_buffer);

    // Publish 10 events.
    for i in 0..10 {
        let ptr = ring_buffer.get(i);
        unsafe { (*ptr).value = i; }
    }
    cursor.store(9); // publish sequences 0..=9

    // Run processor in a thread, halt after it catches up.
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    let handle = thread::spawn(move || {
        // We need to run the processor until it processes all events.
        // Use a simple polling approach since we can't easily halt mid-run.
        processor.run(&rb);
    });

    // Wait for processor to catch up.
    while proc_seq.relaxed_value() < 9 {
        std::hint::spin_loop();
    }

    // Processor should have processed 10 events (seq 0..=9).
    assert_eq!(count.load(Ordering::Relaxed), 10);
}

// ─── Lifecycle callbacks ───────────────────────────────────────────────────

#[test]
fn lifecycle_callbacks_called_in_order() {
    let ring_buffer = Arc::new(RingBuffer::new(64, || TestEvent { value: 0 }));
    let cursor = Arc::new(Cursor::new());
    let barrier = SingleConsumerBarrier::new(Arc::clone(&cursor));
    let alert = Arc::new(Alert::new());
    let seq_barrier = ProcessingSequenceBarrier::new(barrier, BusySpin);

    let log = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

    struct LifecycleHandler {
        log: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl EventHandler<TestEvent> for LifecycleHandler {
        fn on_event(&mut self, _event: &TestEvent, seq: Sequence, _eob: bool) {
            self.log.lock().unwrap().push(format!("event:{seq}"));
        }
        fn on_start(&mut self) {
            self.log.lock().unwrap().push("start".to_string());
        }
        fn on_shutdown(&mut self) {
            self.log.lock().unwrap().push("shutdown".to_string());
        }
    }

    let handler = LifecycleHandler { log: log.clone() };
    let mut processor = BatchEventProcessor::new(handler, seq_barrier);

    // Publish 2 events.
    let ptr0 = ring_buffer.get(0);
    unsafe { (*ptr0).value = 1; }
    let ptr1 = ring_buffer.get(1);
    unsafe { (*ptr1).value = 2; }
    cursor.store(1);

    let proc_seq = Arc::clone(processor.sequence());
    let rb = Arc::clone(&ring_buffer);

    let handle = thread::spawn(move || {
        processor.run(&rb);
    });

    // Wait for events to be processed.
    while proc_seq.relaxed_value() < 1 {
        std::hint::spin_loop();
    }

    // Give a small window for the processor to settle.
    std::thread::sleep(std::time::Duration::from_millis(10));

    let entries = log.lock().unwrap();
    // Verify order: start, events, (shutdown happens when halted).
    assert!(entries.len() >= 3, "expected at least start + 2 events, got {:?}", *entries);
    assert_eq!(entries[0], "start");
    assert_eq!(entries[1], "event:0");
    assert_eq!(entries[2], "event:1");
}

// ─── ExceptionHandler ──────────────────────────────────────────────────────

#[test]
fn exception_handler_continue_skips_panicking_event() {
    let ring_buffer = Arc::new(RingBuffer::new(64, || TestEvent { value: 0 }));
    let cursor = Arc::new(Cursor::new());
    let barrier = SingleConsumerBarrier::new(Arc::clone(&cursor));
    let seq_barrier = ProcessingSequenceBarrier::new(barrier, BusySpin);

    let processed = Arc::new(AtomicI64::new(0));
    let pc = processed.clone();

    let handler = move |_event: &TestEvent, seq: Sequence, _eob: bool| {
        if seq == 2 {
            panic!("intentional panic at seq 2");
        }
        pc.fetch_add(1, Ordering::Relaxed);
    };

    let mut processor = BatchEventProcessor::new(handler, seq_barrier);
    processor.set_exception_handler(Box::new(IgnoreExceptionHandler));

    // Publish 5 events.
    for i in 0..5 {
        let ptr = ring_buffer.get(i);
        unsafe { (*ptr).value = i; }
    }
    cursor.store(4);

    let proc_seq = Arc::clone(processor.sequence());
    let rb = Arc::clone(&ring_buffer);

    let handle = thread::spawn(move || {
        processor.run(&rb);
    });

    while proc_seq.relaxed_value() < 4 {
        std::hint::spin_loop();
    }

    // Events 0, 1, 3, 4 should be processed (seq 2 panicked but was skipped).
    assert_eq!(processed.load(Ordering::Relaxed), 4);
}
