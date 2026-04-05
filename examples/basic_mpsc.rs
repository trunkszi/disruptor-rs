//! # Basic Multi-Producer Single-Consumer Example
//!
//! Demonstrates multiple producer threads concurrently publishing events,
//! with a single consumer thread processing all of them.
//! `MultiProducer` can create multiple handles via `clone()`, each held by a separate thread.
//!
//! ```text
//! cargo run --example basic_mpsc
//! ```

use std::thread;
use disruptor::*;

/// Event on the ring buffer.
struct Event {
    source:  u8,
    payload: i64,
}

fn main() {
    let factory = || Event { source: 0, payload: 0 };

    let (tx, rx) = std::sync::mpsc::channel();

    // Consumer: prints the source and payload of each event.
    let processor = move |e: &Event, sequence: Sequence, _eob: bool| {
        println!("[Consumer] seq={sequence} source={} payload={}", e.source, e.payload);
        tx.send((e.source, e.payload)).expect("send failed");
    };

    // Build a multi-producer Disruptor using build_multi_producer.
    // Ring buffer size is 64, wait strategy is BusySpin.
    let mut producer1 = build_multi_producer(64, factory, BusySpin)
        .handle_events_with(processor)
        .build();

    // clone() produces a second producer handle; they share the same ring buffer.
    let mut producer2 = producer1.clone();

    // Two producer threads each publish 50 events.
    // thread::scope ensures threads complete when the scope ends.
    thread::scope(|s| {
        s.spawn(move || {
            for i in 0..50 {
                producer1.publish(|e| {
                    e.source  = 1;
                    e.payload = i;
                });
            }
            // producer1 is dropped here, but the Disruptor does not shut down -- producer2 is still alive.
        });

        s.spawn(move || {
            for i in 0..50 {
                producer2.publish(|e| {
                    e.source  = 2;
                    e.payload = 1000 + i;
                });
            }
        });
    });
    // Both producers have been dropped; the Disruptor begins shutdown: consumer processes all remaining events, then the thread exits.

    let results: Vec<_> = rx.iter().collect();
    assert_eq!(results.len(), 100);
    println!("\nTwo producers published {} events in total; all processed.", results.len());
}
