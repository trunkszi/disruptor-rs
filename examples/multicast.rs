//! # Multicast topology example: 3 consumers each independently process all events
//!
//! All consumers receive every event in parallel (multicast/fan-out pattern).
//! The Disruptor does not copy data between consumers -- each consumer reads the same event directly from the ring buffer.
//!
//! ```text
//!            /-- [C1]
//! Producer --+-- [C2]
//!            \-- [C3]
//! ```
//!
//! ```text
//! cargo run --example multicast
//! ```

use std::sync::atomic::{AtomicUsize, Ordering};
use disruptor::*;

struct Event {
    message_id: i64,
}

fn main() {
    let factory = || Event { message_id: 0 };

    // Use atomic counters to verify that each consumer processed all events.
    static COUNT_1: AtomicUsize = AtomicUsize::new(0);
    static COUNT_2: AtomicUsize = AtomicUsize::new(0);
    static COUNT_3: AtomicUsize = AtomicUsize::new(0);

    let consumer_1 = |e: &Event, seq: Sequence, _eob: bool| {
        println!("[C1] seq={seq} message_id={}", e.message_id);
        COUNT_1.fetch_add(1, Ordering::Relaxed);
    };

    let consumer_2 = |e: &Event, seq: Sequence, _eob: bool| {
        println!("[C2] seq={seq} message_id={}", e.message_id);
        COUNT_2.fetch_add(1, Ordering::Relaxed);
    };

    let consumer_3 = |e: &Event, seq: Sequence, _eob: bool| {
        println!("[C3] seq={seq} message_id={}", e.message_id);
        COUNT_3.fetch_add(1, Ordering::Relaxed);
    };

    // Calling handle_events_with consecutively without calling and_then
    // places all three consumers in the same stage, each processing all events in parallel.
    let mut producer = build_single_producer(64, factory, BusySpin)
        .handle_events_with(consumer_1)
        .handle_events_with(consumer_2)
        .handle_events_with(consumer_3)
        .build();

    let total = 20;
    for i in 0..total {
        producer.publish(|e| {
            e.message_id = i;
        });
    }

    // Shut down the Disruptor and wait for all consumers to finish.
    drop(producer);

    // Verify: each consumer should have processed all events.
    assert_eq!(COUNT_1.load(Ordering::Relaxed), total as usize);
    assert_eq!(COUNT_2.load(Ordering::Relaxed), total as usize);
    assert_eq!(COUNT_3.load(Ordering::Relaxed), total as usize);

    println!(
        "\nMulticast complete: 3 consumers each processed {} events.",
        total
    );
}
