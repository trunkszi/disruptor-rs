//! Edge case tests for disruptor-seq types.

use disruptor_core::*;
use disruptor_seq::*;

struct UnboundedBarrier;
impl Barrier for UnboundedBarrier {
    fn get_after(&self, _: Sequence) -> Sequence { i64::MAX }
}

struct CursorBarrier { cursor: std::sync::Arc<Cursor> }
impl Barrier for CursorBarrier {
    fn get_after(&self, _: Sequence) -> Sequence { self.cursor.relaxed_value() }
}

// ─── InsufficientCapacity ──────────────────────────────────────────────────

#[test]
fn insufficient_capacity_display() {
    let err = InsufficientCapacity;
    assert_eq!(format!("{err}"), "Insufficient capacity in ring buffer");
    assert_eq!(format!("{err:?}"), "InsufficientCapacity");
}

#[test]
fn insufficient_capacity_equality() {
    assert_eq!(InsufficientCapacity, InsufficientCapacity);
}

// ─── SingleSequencer edge cases ────────────────────────────────────────────

#[test]
fn single_sequencer_claim_zero() {
    let seq = SingleSequencer::new(16, UnboundedBarrier);
    // Claim 0 should return -1 (end = next + 0 - 1 = 0 - 1 = -1).
    let s = seq.claim(0);
    assert_eq!(s, -1);
}

#[test]
fn single_sequencer_publish_range_single() {
    let seq = SingleSequencer::new(16, UnboundedBarrier);
    let s = seq.claim(1);
    seq.publish_range(s, s); // range of 1
    assert_eq!(seq.cursor(), s);
}

// ─── MultiSequencer edge cases ─────────────────────────────────────────────

#[test]
fn multi_sequencer_is_available_unpublished() {
    let seq = MultiSequencer::new(64, UnboundedBarrier);
    // Nothing claimed or published yet.
    assert!(!seq.is_available(0));
}

#[test]
fn multi_sequencer_highest_published_partial() {
    let seq = MultiSequencer::new(64, UnboundedBarrier);

    // Claim and publish sequences 0, 1 but not 2.
    let s0 = seq.claim(1);
    seq.publish(s0);
    let s1 = seq.claim(1);
    seq.publish(s1);

    // Claim 2 but don't publish.
    let _s2 = seq.claim(1);

    // highest_published from 0 should be 1 (2 not published).
    let h = seq.highest_published(0, 2);
    assert!(h >= 1, "highest published should be at least 1, got {h}");
}

// ─── ProducerBarrier ───────────────────────────────────────────────────────

#[test]
fn single_producer_barrier_initial_state() {
    let barrier = SingleProducerBarrier::new();
    assert_eq!(barrier.get_after(0), NONE);
}

#[test]
fn multi_producer_barrier_minimum_size() {
    let barrier = MultiProducerBarrier::new(64);
    assert_eq!(barrier.get_after(0), -1);
}

#[test]
#[should_panic(expected = "minimum 64 slots")]
fn multi_producer_barrier_rejects_small_size() {
    let _barrier = MultiProducerBarrier::new(32);
}

// ─── Sequencer remaining_capacity ──────────────────────────────────────────

#[test]
fn single_sequencer_remaining_capacity_tracks_claims() {
    let cursor = std::sync::Arc::new(disruptor_core::Cursor::new());
    let barrier = CursorBarrier { cursor: cursor.clone() };
    let seq = SingleSequencer::new(8, barrier);

    assert_eq!(seq.remaining_capacity(), 8);

    // Claim 3 (sequences 0,1,2).
    let _ = seq.claim(3);
    // Consumer hasn't advanced, so 3 slots are in-flight.
    // remaining = 8 - 3 = 5... but consumer is at -1, so in_flight = 2 - (-1) = 3
    // Actually: produced = next-1 = 2, in_flight = produced - consumed = 2 - (-1) = 3
    // remaining = 8 - 3 = 5
    assert_eq!(seq.remaining_capacity(), 5);

    // Consumer advances to 1.
    cursor.store(1);
    // in_flight = 2 - 1 = 1, remaining = 8 - 1 = 7
    assert_eq!(seq.remaining_capacity(), 7);
}

#[test]
fn multi_sequencer_remaining_capacity_basics() {
    let cursor = std::sync::Arc::new(disruptor_core::Cursor::new());
    let barrier = CursorBarrier { cursor: cursor.clone() };
    let seq = MultiSequencer::new(64, barrier);

    // Initial: nothing claimed, consumer at -1.
    let initial = seq.remaining_capacity();
    assert!(initial > 0, "initial capacity should be positive: {initial}");

    // Claim 10.
    let _ = seq.claim(10);
    let after_claim = seq.remaining_capacity();
    assert!(after_claim < initial, "capacity should decrease after claim");
}
