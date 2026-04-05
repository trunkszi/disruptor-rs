//! Single Producer Single Consumer integration tests.
//!
//! Validates the core 1P-1C (Unicast) topology described in the LMAX Disruptor paper.

use disruptor::*;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

/// Basic event type for testing.
struct Event {
    value: i64,
}

// ─── Correctness ────────────────────────────────────────────────────────────

#[test]
fn spsc_publishes_and_consumes_all_events() {
    let count = Arc::new(AtomicI64::new(0));
    let sum = Arc::new(AtomicI64::new(0));
    let cc = count.clone();
    let sc = sum.clone();

    let mut producer = build_single_producer(64, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |e: &Event, _seq: Sequence, _eob: bool| {
            cc.fetch_add(1, Ordering::Relaxed);
            sc.fetch_add(e.value, Ordering::Relaxed);
        })
        .build();

    let n = 1_000;
    for i in 0..n {
        producer.publish(|e| {
            e.value = i;
        });
    }

    drop(producer);

    assert_eq!(count.load(Ordering::Relaxed), n);
    assert_eq!(sum.load(Ordering::Relaxed), n * (n - 1) / 2);
}

#[test]
fn spsc_full_ringbuffer_does_not_lose_events() {
    let count = Arc::new(AtomicI64::new(0));
    let cc = count.clone();

    // Ring buffer size = 2 (minimum), publish much more than buffer size.
    let mut producer = build_single_producer(2, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |_e: &Event, _: Sequence, _: bool| {
            cc.fetch_add(1, Ordering::Relaxed);
        })
        .build();

    for i in 0..128 {
        producer.publish(|e| {
            e.value = i;
        });
    }

    drop(producer);
    assert_eq!(count.load(Ordering::Relaxed), 128);
}

// ─── Batch publication ──────────────────────────────────────────────────────

#[test]
fn spsc_batch_publish_delivers_all_events() {
    let count = Arc::new(AtomicI64::new(0));
    let cc = count.clone();

    let mut producer = build_single_producer(64, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            cc.fetch_add(1, Ordering::Relaxed);
        })
        .build();

    for _ in 0..20 {
        producer.batch_publish(5, |iter| {
            for e in iter {
                e.value = 42;
            }
        });
    }

    drop(producer);
    assert_eq!(count.load(Ordering::Relaxed), 100);
}

#[test]
fn spsc_try_publish_reports_full() {
    // Use an atomic gate to stall the consumer (idiomatic for a busy-spin framework).
    // The consumer busy-spins on the gate, keeping the buffer full without any sleep.
    let gate = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let g = gate.clone();

    let mut producer =
        build_single_producer(4, || Event { value: 0 }, BusySpinWithSpinLoopHint)
            .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
                // Busy-wait until the gate is opened.
                while !g.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
            })
            .build();

    // Fill the ring buffer: 4 slots. The consumer is stalled on the gate,
    // so eventually try_publish must return RingBufferFull.
    let mut published = 0;
    for _ in 0..100 {
        match producer.try_publish(|e| {
            e.value = 1;
        }) {
            Ok(_) => published += 1,
            Err(RingBufferFull) => break,
        }
    }

    // We should have published at least 1 and fewer than 100.
    assert!(published >= 1);
    assert!(published < 100);

    // Open the gate so the consumer can finish and the producer can be dropped.
    gate.store(true, Ordering::Release);
}

// ─── End-of-batch flag ──────────────────────────────────────────────────────

#[test]
fn spsc_end_of_batch_flag_is_set_correctly() {
    let eob_sequences = Arc::new(std::sync::Mutex::new(Vec::new()));
    let eob = eob_sequences.clone();

    let mut producer = build_single_producer(64, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |_: &Event, seq: Sequence, end_of_batch: bool| {
            if end_of_batch {
                eob.lock().unwrap().push(seq);
            }
        })
        .build();

    // Publish 10 events; the consumer should see at least one end_of_batch.
    for i in 0..10 {
        producer.publish(|e| {
            e.value = i;
        });
    }

    drop(producer);

    let seqs = eob_sequences.lock().unwrap();
    // The last event (sequence 9) must be marked end_of_batch.
    assert!(seqs.contains(&9), "sequence 9 should be end_of_batch");
}
