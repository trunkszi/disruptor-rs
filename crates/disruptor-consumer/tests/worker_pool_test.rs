//! WorkerPool integration tests.
//!
//! Validates exactly-once semantics: each event processed by exactly one worker.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use disruptor_core::*;
use disruptor_consumer::*;

struct Event {
    value: i64,
}

struct CountingHandler {
    id: usize,
    processed: Arc<AtomicI64>,
    bitset: Arc<Vec<AtomicU64>>,
}

impl WorkHandler<Event> for CountingHandler {
    fn on_event(&mut self, _event: &Event, sequence: Sequence) {
        self.processed.fetch_add(1, Ordering::Relaxed);
        // Mark this sequence as processed by setting its bit.
        let word = sequence as usize / 64;
        let bit = sequence as usize % 64;
        if word < self.bitset.len() {
            self.bitset[word].fetch_or(1 << bit, Ordering::Relaxed);
        }
    }
}

#[test]
#[cfg(not(miri))] // multi-threaded + spin loop
fn worker_pool_exactly_once_3_workers() {
    let n_events: i64 = 10_000;
    let buffer_size = 16384;

    let ring_buffer = Arc::new(RingBuffer::new(buffer_size, || Event { value: 0 }));
    let cursor = Arc::new(Cursor::new());

    // Track total processed and per-sequence bitset.
    let total = Arc::new(AtomicI64::new(0));
    let n_words = (n_events as usize + 63) / 64;
    let bitset: Arc<Vec<AtomicU64>> = Arc::new((0..n_words).map(|_| AtomicU64::new(0)).collect());

    let handlers: Vec<CountingHandler> = (0..3)
        .map(|id| CountingHandler {
            id,
            processed: Arc::clone(&total),
            bitset: Arc::clone(&bitset),
        })
        .collect();

    let barrier = SingleConsumerBarrier::new(Arc::clone(&cursor));
    let mut pool = WorkerPool::new(Arc::clone(&ring_buffer), handlers);
    let handles = pool.start(Arc::new(barrier));

    // Publish events.
    for i in 0..n_events {
        let ptr = ring_buffer.get(i);
        unsafe { (*ptr).value = i; }
        cursor.store(i);
    }

    // Wait for all events to be processed.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while total.load(Ordering::Relaxed) < n_events {
        if std::time::Instant::now() > deadline {
            panic!(
                "Timeout: only {} of {} events processed",
                total.load(Ordering::Relaxed),
                n_events
            );
        }
        std::hint::spin_loop();
    }

    pool.drain_and_halt(handles).expect("worker threads should not panic");

    // Verify exactly-once: each sequence bit should be set exactly once.
    let processed = total.load(Ordering::Relaxed);
    assert_eq!(processed, n_events, "Total processed must equal published");

    // Verify every sequence was processed.
    for seq in 0..n_events {
        let word = seq as usize / 64;
        let bit = seq as usize % 64;
        let val = bitset[word].load(Ordering::Relaxed);
        assert!(
            val & (1 << bit) != 0,
            "Sequence {seq} was not processed"
        );
    }
}

#[test]
#[cfg(not(miri))]
fn worker_pool_halt_stops_workers() {
    let ring_buffer = Arc::new(RingBuffer::new(64, || Event { value: 0 }));
    let cursor = Arc::new(Cursor::new());

    struct NoopHandler;
    impl WorkHandler<Event> for NoopHandler {
        fn on_event(&mut self, _: &Event, _: Sequence) {}
    }

    let barrier = SingleConsumerBarrier::new(Arc::clone(&cursor));
    let mut pool = WorkerPool::new(Arc::clone(&ring_buffer), vec![NoopHandler, NoopHandler]);
    let handles = pool.start(Arc::new(barrier));

    // Immediately halt — workers should stop.
    pool.drain_and_halt(handles).expect("worker threads should not panic");
    // If we get here, halt works.
}
