//! Integration tests for Sequencer trait implementations.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::thread;
use disruptor_core::*;
use disruptor_seq::*;

/// Barrier that always reports all consumed (infinite capacity).
struct UnboundedBarrier;
impl Barrier for UnboundedBarrier {
    fn get_after(&self, _prev: Sequence) -> Sequence { i64::MAX }
}

/// Barrier tracking a cursor position.
struct CursorBarrier { cursor: Arc<Cursor> }
impl Barrier for CursorBarrier {
    fn get_after(&self, _prev: Sequence) -> Sequence { self.cursor.relaxed_value() }
}

// ─── SingleSequencer ───────────────────────────────────────────────────────

#[test]
fn single_sequencer_sequential_claim_publish() {
    let seq = SingleSequencer::new(16, UnboundedBarrier);
    for i in 0..100 {
        let s = seq.claim(1);
        assert_eq!(s, i);
        seq.publish(s);
        assert_eq!(seq.cursor(), i);
    }
}

#[test]
fn single_sequencer_batch_claim() {
    let seq = SingleSequencer::new(64, UnboundedBarrier);
    let s = seq.claim(10);
    assert_eq!(s, 9); // claimed 0..=9
    seq.publish_range(0, 9);
    assert_eq!(seq.cursor(), 9);
}

#[test]
fn single_sequencer_capacity_gating() {
    let consumer = Arc::new(Cursor::new()); // at -1
    let seq = SingleSequencer::new(4, CursorBarrier { cursor: consumer.clone() });

    // Claim 4 (fills buffer: 0,1,2,3).
    let s = seq.claim(4);
    assert_eq!(s, 3);
    seq.publish_range(0, 3);

    // Buffer full — try_claim should fail.
    assert!(seq.try_claim(1).is_err());

    // Consumer advances to 0 → 1 slot freed.
    consumer.store(0);
    let s = seq.try_claim(1);
    assert!(s.is_ok());
    assert_eq!(s.unwrap(), 4);
}

#[test]
fn single_sequencer_is_available() {
    let seq = SingleSequencer::new(16, UnboundedBarrier);
    assert!(!seq.is_available(0)); // nothing published yet

    let s = seq.claim(1);
    seq.publish(s);
    assert!(seq.is_available(0));
}

// ─── MultiSequencer ────────────────────────────────────────────────────────

#[test]
fn multi_sequencer_sequential_claim_publish() {
    let seq = MultiSequencer::new(64, UnboundedBarrier);
    for i in 0..50 {
        let s = seq.claim(1);
        assert_eq!(s, i);
        seq.publish(s);
    }
    assert_eq!(seq.cursor(), 49);
}

#[test]
fn multi_sequencer_batch_claim() {
    let seq = MultiSequencer::new(64, UnboundedBarrier);
    let s = seq.claim(10);
    assert_eq!(s, 9);
    seq.publish_range(0, 9);
}

#[test]
fn multi_sequencer_capacity_gating() {
    let consumer = Arc::new(Cursor::new());
    let seq = MultiSequencer::new(64, CursorBarrier { cursor: consumer.clone() });

    // Claim 64 (fills buffer).
    let s = seq.claim(64);
    assert_eq!(s, 63);

    // Buffer full.
    assert!(seq.try_claim(1).is_err());

    // Consumer advances.
    consumer.store(0);
    assert!(seq.try_claim(1).is_ok());
}

#[test]
#[cfg(not(miri))] // threads + high iteration count
fn multi_sequencer_concurrent_claims() {
    let consumer = Arc::new(Cursor::new());
    // Use a large buffer to avoid gating in this test.
    let seq = Arc::new(MultiSequencer::new(65536, CursorBarrier { cursor: consumer.clone() }));
    // Simulate consumer always caught up.
    consumer.store(i64::MAX / 2);

    let n_threads = 4;
    let claims_per_thread = 1000;
    let total = Arc::new(AtomicI64::new(0));

    let handles: Vec<_> = (0..n_threads)
        .map(|_| {
            let seq = Arc::clone(&seq);
            let total = Arc::clone(&total);
            thread::spawn(move || {
                for _ in 0..claims_per_thread {
                    let s = seq.claim(1);
                    seq.publish(s);
                    total.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(total.load(Ordering::Relaxed), n_threads * claims_per_thread);
    // Cursor should be at total - 1.
    assert_eq!(seq.cursor(), n_threads * claims_per_thread - 1);
}
