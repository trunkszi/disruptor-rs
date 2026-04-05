//! loom-based concurrency tests for disruptor-rs (P0-4).
//!
//! These tests validate the core lock-free protocols using loom's deterministic
//! scheduler to explore all possible interleavings.
//!
//! Run with: RUSTFLAGS="--cfg loom" cargo test --test loom_tests --release
//!
//! Since the library does not yet have `cfg(loom)` conditional compilation,
//! these tests replicate the critical concurrent logic using loom primitives
//! directly, validating the algorithmic correctness of the protocols.

#![cfg(loom)]

use loom::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use loom::sync::Arc;
use loom::thread;

// ─── Test 1: SPSC visibility ───────────────────────────────────────────────
// Validates that a single producer publishing with Release ordering is visible
// to a consumer reading with Acquire ordering.

#[test]
fn loom_spsc_visibility() {
    loom::model(|| {
        let cursor = Arc::new(AtomicI64::new(-1));
        let data = Arc::new(AtomicI64::new(0));

        let cursor_w = Arc::clone(&cursor);
        let data_w = Arc::clone(&data);

        // Producer: write data, then publish cursor with Release.
        let producer = thread::spawn(move || {
            data_w.store(42, Ordering::Relaxed);
            cursor_w.store(0, Ordering::Release);
        });

        // Consumer: spin on cursor (Acquire), then read data.
        loop {
            let seq = cursor.load(Ordering::Acquire);
            if seq >= 0 {
                let val = data.load(Ordering::Relaxed);
                assert_eq!(val, 42, "data must be visible after Acquire load of cursor");
                break;
            }
            loom::thread::yield_now();
        }

        producer.join().unwrap();
    });
}

// ─── Test 2: MPSC visibility ───────────────────────────────────────────────
// Validates that two producers using CAS to claim sequences, followed by
// Release publish, are both visible to the consumer.

#[test]
fn loom_mpsc_visibility() {
    loom::model(|| {
        let claim_cursor = Arc::new(AtomicI64::new(-1));
        let published = Arc::new([AtomicI64::new(0), AtomicI64::new(0)]);
        let data = Arc::new([AtomicI64::new(0), AtomicI64::new(0)]);

        // Producer 1: CAS claim seq 0.
        let cc1 = Arc::clone(&claim_cursor);
        let p1 = Arc::clone(&published);
        let d1 = Arc::clone(&data);
        let t1 = thread::spawn(move || {
            // CAS to claim sequence 0.
            if cc1.compare_exchange(-1, 0, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                d1[0].store(100, Ordering::Relaxed);
                p1[0].store(1, Ordering::Release);
            } else {
                // Lost the race, claim sequence 1.
                let _ = cc1.compare_exchange(0, 1, Ordering::AcqRel, Ordering::Relaxed);
                d1[1].store(200, Ordering::Relaxed);
                p1[1].store(1, Ordering::Release);
            }
        });

        // Producer 2: CAS claim next available seq.
        let cc2 = Arc::clone(&claim_cursor);
        let p2 = Arc::clone(&published);
        let d2 = Arc::clone(&data);
        let t2 = thread::spawn(move || {
            if cc2.compare_exchange(-1, 0, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                d2[0].store(100, Ordering::Relaxed);
                p2[0].store(1, Ordering::Release);
            } else {
                let _ = cc2.compare_exchange(0, 1, Ordering::AcqRel, Ordering::Relaxed);
                d2[1].store(200, Ordering::Relaxed);
                p2[1].store(1, Ordering::Release);
            }
        });

        // Consumer: wait for both slots to be published.
        let mut seen = [false; 2];
        while !seen[0] || !seen[1] {
            for i in 0..2 {
                if !seen[i] && published[i].load(Ordering::Acquire) == 1 {
                    let val = data[i].load(Ordering::Relaxed);
                    assert!(val == 100 || val == 200, "data must be visible: got {val}");
                    seen[i] = true;
                }
            }
            loom::thread::yield_now();
        }

        t1.join().unwrap();
        t2.join().unwrap();
    });
}

// ─── Test 3: Multi-producer reference counting ─────────────────────────────
// Validates that clone/drop of the shared counter is correct under concurrency.

#[test]
fn loom_multi_producer_refcount() {
    loom::model(|| {
        let counter = Arc::new(AtomicI64::new(1)); // starts at 1 (initial producer)

        // Clone: increment counter.
        let c2 = Arc::clone(&counter);
        let clone_count = c2.fetch_add(1, Ordering::AcqRel);
        assert!(clone_count >= 1);

        // Spawn a thread that drops (decrements) the cloned producer.
        let c3 = Arc::clone(&counter);
        let t1 = thread::spawn(move || {
            let old = c3.fetch_sub(1, Ordering::AcqRel);
            // Should not be the last one (original still alive).
            assert!(old >= 2, "clone drop should not be last: old={old}");
        });

        // Original drops.
        t1.join().unwrap();
        let old = counter.fetch_sub(1, Ordering::AcqRel);
        assert_eq!(old, 1, "original is the last one: old={old}");
    });
}

// ─── Test 4: Shutdown signal propagation ───────────────────────────────────
// Validates that the shutdown_at_sequence store (Relaxed) is eventually visible
// to the consumer thread, causing it to exit.

#[test]
fn loom_shutdown_signal() {
    loom::model(|| {
        let shutdown_at = Arc::new(AtomicI64::new(i64::MAX));
        let cursor = Arc::new(AtomicI64::new(-1));

        let sa = Arc::clone(&shutdown_at);
        let c = Arc::clone(&cursor);

        // Consumer thread: waits for events or shutdown.
        let consumer = thread::spawn(move || {
            let mut sequence = 0i64;
            loop {
                let available = c.load(Ordering::Acquire);
                if available >= sequence {
                    sequence = available + 1;
                }
                // Check shutdown.
                if sa.load(Ordering::Relaxed) <= sequence {
                    return sequence;
                }
                loom::thread::yield_now();
            }
        });

        // Producer: publish one event then signal shutdown.
        cursor.store(0, Ordering::Release);
        // Signal shutdown at sequence 1 (consumer should stop after processing seq 0).
        shutdown_at.store(1, Ordering::Relaxed);

        let final_seq = consumer.join().unwrap();
        assert!(final_seq >= 1, "consumer should have seen at least seq 0");
    });
}

// ─── Test 5: Availability bitmap (multi-producer publish protocol) ─────────
// Validates that XOR-based availability tracking correctly encodes even/odd
// round publications under concurrent producers.

#[test]
fn loom_availability_bitmap() {
    loom::model(|| {
        // Simplified: 2 slots, 1 AtomicU64 tracking 2 bits.
        // Initial state: all 1s (nothing published in round 0 = even round).
        let availability = Arc::new(AtomicU64::new(0b11));

        let a1 = Arc::clone(&availability);
        let a2 = Arc::clone(&availability);

        // Producer 1 publishes slot 0 (XOR bit 0).
        let t1 = thread::spawn(move || {
            a1.fetch_xor(1 << 0, Ordering::Release);
        });

        // Producer 2 publishes slot 1 (XOR bit 1).
        let t2 = thread::spawn(move || {
            a2.fetch_xor(1 << 1, Ordering::Release);
        });

        t1.join().unwrap();
        t2.join().unwrap();

        // After both publish: bits should be flipped from 0b11 to 0b00.
        let val = availability.load(Ordering::Acquire);
        assert_eq!(val, 0b00, "both bits should be flipped: got {val:#b}");
    });
}
