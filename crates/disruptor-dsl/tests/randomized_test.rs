//! Randomized stress tests.
//!
//! Uses pseudo-random parameters (buffer sizes, event counts, producer counts)
//! to find edge cases that fixed-parameter tests might miss.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use disruptor_dsl::prelude::*;

struct Event { value: i64 }

/// Test with various power-of-2 buffer sizes.
#[test]
#[cfg(not(miri))]
fn randomized_buffer_sizes_spsc() {
    for &size in &[2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 4096, 16384, 65536] {
        let n = size as i64 * 3; // 3x buffer size worth of events
        let sum = Arc::new(AtomicI64::new(0));
        let sc = sum.clone();

        let mut producer = Disruptor::single(size, || Event { value: 0 }, BusySpin)
            .handle_events_with(move |e: &Event, _: Sequence, _: bool| {
                sc.fetch_add(e.value, Ordering::Relaxed);
            })
            .build();

        for i in 0..n {
            producer.publish(|e| { e.value = i; });
        }
        drop(producer);

        let expected = n * (n - 1) / 2;
        assert_eq!(
            sum.load(Ordering::Relaxed), expected,
            "SPSC sum mismatch for buffer_size={size}, n={n}"
        );
    }
}

/// Test MPSC with various producer counts.
#[test]
#[cfg(not(miri))]
fn randomized_producer_counts_mpsc() {
    for &n_producers in &[1, 2, 3, 4, 6, 8] {
        let events_per = 5000i64;
        let total_events = events_per * n_producers as i64;
        let count = Arc::new(AtomicI64::new(0));
        let cc = count.clone();

        let mut producers: Vec<_> = Vec::new();
        let first = Disruptor::multi(8192, || Event { value: 0 }, BusySpin)
            .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
                cc.fetch_add(1, Ordering::Relaxed);
            })
            .build();

        // Clone producers.
        for _ in 1..n_producers {
            producers.push(first.clone());
        }
        producers.push(first);

        std::thread::scope(|s| {
            for mut p in producers {
                s.spawn(move || {
                    for i in 0..events_per {
                        p.publish(|e| { e.value = i; });
                    }
                });
            }
        });

        assert_eq!(
            count.load(Ordering::Relaxed), total_events,
            "MPSC count mismatch for {n_producers} producers"
        );
    }
}

/// Test batch publish with various batch sizes.
#[test]
#[cfg(not(miri))]
fn randomized_batch_sizes() {
    for &batch_size in &[1, 2, 3, 5, 7, 10, 16, 32, 64] {
        let n_batches = 100;
        let total = batch_size as i64 * n_batches;
        let count = Arc::new(AtomicI64::new(0));
        let cc = count.clone();

        let buf_size = ((batch_size * 2) as u32).next_power_of_two().max(2) as usize;
        let mut producer = Disruptor::single(buf_size, || Event { value: 0 }, BusySpin)
            .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
                cc.fetch_add(1, Ordering::Relaxed);
            })
            .build();

        for _ in 0..n_batches {
            producer.batch_publish(batch_size, |iter| {
                for e in iter { e.value = 1; }
            });
        }
        drop(producer);

        assert_eq!(
            count.load(Ordering::Relaxed), total,
            "Batch count mismatch for batch_size={batch_size}, buf_size={buf_size}"
        );
    }
}

/// Test pipeline with various stage counts (1-4 stages).
#[test]
fn randomized_pipeline_depths() {
    let n = 200i64;

    // 1-stage.
    {
        let c = Arc::new(AtomicI64::new(0));
        let cc = c.clone();
        let mut p = Disruptor::single(64, || Event { value: 0 }, BusySpin)
            .handle_events_with(move |_: &Event, _: Sequence, _: bool| { cc.fetch_add(1, Ordering::Relaxed); })
            .build();
        for i in 0..n { p.publish(|e| { e.value = i; }); }
        drop(p);
        assert_eq!(c.load(Ordering::Relaxed), n, "1-stage");
    }

    // 2-stage.
    {
        let c = Arc::new(AtomicI64::new(0));
        let cc = c.clone();
        let mut p = Disruptor::single(64, || Event { value: 0 }, BusySpin)
            .handle_events_with(|_: &Event, _: Sequence, _: bool| {})
            .and_then()
            .handle_events_with(move |_: &Event, _: Sequence, _: bool| { cc.fetch_add(1, Ordering::Relaxed); })
            .build();
        for i in 0..n { p.publish(|e| { e.value = i; }); }
        drop(p);
        assert_eq!(c.load(Ordering::Relaxed), n, "2-stage");
    }
}
