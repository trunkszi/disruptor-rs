//! Unit tests for disruptor-core types.

use disruptor_core::*;
use std::sync::Arc;

// ─── EventFactory ──────────────────────────────────────────────────────────

#[test]
fn event_factory_closure() {
    let mut factory = || 42i64;
    assert_eq!(factory.create(), 42);
}

#[test]
fn event_factory_struct() {
    struct Counter { n: i32 }
    impl EventFactory<i32> for Counter {
        fn create(&mut self) -> i32 {
            self.n += 1;
            self.n
        }
    }
    let mut f = Counter { n: 0 };
    assert_eq!(f.create(), 1);
    assert_eq!(f.create(), 2);
}

// ─── RingBuffer ────────────────────────────────────────────────────────────

#[test]
fn ringbuffer_power_of_two_sizes() {
    let rb = RingBuffer::new(4, || 0i32);
    assert_eq!(rb.size(), 4);
}

#[test]
#[should_panic(expected = "Size must be power of 2")]
fn ringbuffer_rejects_non_power_of_two() {
    let _rb = RingBuffer::new(3, || 0i32);
}

#[test]
fn ringbuffer_get_wraps_around() {
    let rb = RingBuffer::new(4, || 0i64);

    // Write to slot 0.
    let ptr = rb.get(0);
    unsafe { *ptr = 100; }

    // Sequence 4 wraps to slot 0.
    let ptr2 = rb.get(4);
    let val = unsafe { *ptr2 };
    assert_eq!(val, 100);
}

#[test]
fn ringbuffer_free_slots() {
    let rb = RingBuffer::new(8, || 0);
    assert_eq!(rb.free_slots(0, 0), 8);
    assert_eq!(rb.free_slots(7, 0), 1);
    assert_eq!(rb.free_slots(7, -1), 0);
}

// ─── Cursor ────────────────────────────────────────────────────────────────

#[test]
fn cursor_starts_at_none() {
    let cursor = Cursor::new();
    assert_eq!(cursor.relaxed_value(), NONE);
    assert_eq!(cursor.relaxed_value(), INITIAL_SEQUENCE);
}

#[test]
fn cursor_store_and_load() {
    let cursor = Cursor::new();
    cursor.store(42);
    assert_eq!(cursor.relaxed_value(), 42);
}

#[test]
fn cursor_compare_exchange() {
    let cursor = Cursor::new();
    assert!(cursor.compare_exchange(-1, 0).is_ok());
    assert!(cursor.compare_exchange(0, 5).is_ok());
    assert!(cursor.compare_exchange(0, 6).is_err()); // current is 5, not 0
}

// ─── Barrier ───────────────────────────────────────────────────────────────

#[test]
fn single_consumer_barrier_tracks_cursor() {
    let cursor = Arc::new(Cursor::new());
    let barrier = SingleConsumerBarrier::new(Arc::clone(&cursor));

    assert_eq!(barrier.get_after(0), NONE);
    cursor.store(5);
    assert_eq!(barrier.get_after(0), 5);
}

#[test]
fn multi_consumer_barrier_tracks_slowest() {
    let c1 = Arc::new(Cursor::new());
    let c2 = Arc::new(Cursor::new());
    let barrier = MultiConsumerBarrier::new(vec![Arc::clone(&c1), Arc::clone(&c2)]);

    c1.store(10);
    c2.store(5);
    assert_eq!(barrier.get_after(0), 5); // min of 10 and 5

    c2.store(15);
    assert_eq!(barrier.get_after(0), 10); // min of 10 and 15
}

// ─── Alert ─────────────────────────────────────────────────────────────────

#[test]
fn alert_initially_unset() {
    let alert = Alert::new();
    assert!(!alert.is_set());
}

#[test]
fn alert_set_and_clear() {
    let alert = Alert::new();
    alert.set();
    assert!(alert.is_set());
    alert.clear();
    assert!(!alert.is_set());
}

// ─── SequenceBarrier trait ─────────────────────────────────────────────────

#[test]
fn alert_error_display() {
    let err = AlertError;
    assert_eq!(format!("{err}"), "SequenceBarrier has been alerted");
}
