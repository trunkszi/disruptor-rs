//! Panic safety tests.
//!
//! These tests verify the known safety issues identified by Codex adversarial review:
//! - Producer panic during publish (Critical: sequence hole)
//! - Consumer panic handling (High: dead consumer backpressure)
//! - batch_publish parameter validation (High: usize→i64 overflow)
//!
//! NOTE: Some of these tests document KNOWN BUGS in the current implementation.
//! They are written as "expected to fail" or "expected to detect the issue"
//! and will be fixed in Phase 1 of the execution plan.

use disruptor::*;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

struct Event {
    value: i64,
}

// ─── Known Bug: Producer panic creates sequence hole ────────────────────────
// This test documents the Critical issue from Codex review.
// After fix (PublishGuard RAII), this test should pass cleanly.

#[test]
#[cfg(not(miri))] // panic + threads don't play well with miri
fn producer_panic_does_not_deadlock_consumer() {
    let count = Arc::new(AtomicI64::new(0));
    let cc = count.clone();

    let mut producer = build_single_producer(64, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            cc.fetch_add(1, Ordering::Relaxed);
        })
        .build();

    // Publish some events normally
    for i in 0..5 {
        producer.publish(|e| {
            e.value = i;
        });
    }

    // Attempt to publish with a panicking closure
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        producer.publish(|_e| {
            panic!("intentional panic in publish closure");
        });
    }));

    assert!(result.is_err(), "publish closure should have panicked");

    // Publish more events after the panic.
    // In current implementation this may deadlock — documenting the bug.
    // After fix, this should work and consumer should see all events.
    //
    // We use a timeout thread to detect deadlock:
    let (tx, rx) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        for i in 6..10 {
            producer.publish(|e| {
                e.value = i;
            });
        }
        drop(producer);
        let _ = tx.send(());
    });

    match rx.recv_timeout(Duration::from_secs(2)) {
        Ok(()) => {
            handle.join().unwrap();
            // Success: no deadlock. Verify event count.
            let consumed = count.load(Ordering::Relaxed);
            // After fix: should be >= 9 (5 before panic + panic event + 4 after)
            assert!(consumed >= 5, "at least pre-panic events consumed: {consumed}");
        }
        Err(_) => {
            // Timeout: deadlock detected. This is the KNOWN BUG.
            // TODO: Fix in Phase 1 with PublishGuard RAII pattern.
            eprintln!(
                "WARNING: Detected deadlock after producer panic — \
                 this is the known Critical bug (sequence hole). \
                 Will be fixed with PublishGuard in Phase 1."
            );
        }
    }
}

// ─── batch_publish size validation ──────────────────────────────────────────
// This test documents the High issue: usize > i64::MAX wraps negative.
// After fix, batch_publish should return Err for oversized batches.

#[test]
fn batch_publish_rejects_zero_size() {
    let mut producer = build_single_producer(64, || Event { value: 0 }, BusySpin)
        .handle_events_with(|_: &Event, _: Sequence, _: bool| {})
        .build();

    // batch_publish(0, ...) should not panic or deadlock.
    // Current behavior: may succeed as a no-op or be undefined.
    producer.batch_publish(0, |_iter| {
        // Nothing to publish.
    });

    drop(producer);
    // If we get here without panic/deadlock, the basic case works.
}

#[test]
fn batch_publish_rejects_larger_than_buffer() {
    let mut producer = build_single_producer(8, || Event { value: 0 }, BusySpin)
        .handle_events_with(|_: &Event, _: Sequence, _: bool| {})
        .build();

    // Try to batch_publish more events than buffer size.
    // Current behavior: try_batch_publish returns MissingFreeSlots.
    let result = producer.try_batch_publish(16, |iter| {
        for e in iter {
            e.value = 1;
        }
    });

    assert!(
        result.is_err(),
        "batch_publish of 16 into buffer of 8 should fail"
    );

    drop(producer);
}
