//! # Basic Single-Producer Single-Consumer Example
//!
//! Demonstrates the simplest Disruptor usage: one producer thread publishes events,
//! one consumer thread processes them.
//!
//! ```text
//! cargo run --example basic_spsc
//! ```

use disruptor::*;

/// Event struct on the ring buffer.
/// The Disruptor pre-allocates these objects and updates them in-place when publishing,
/// avoiding heap allocation.
struct Event {
    value: i64,
}

fn main() {
    // Event factory: the Disruptor uses this to fill every slot in the ring buffer at startup.
    let factory = || Event { value: 0 };

    // Received event count (shared state captured via move).
    let (tx, rx) = std::sync::mpsc::channel();

    // Consumer closure. Parameter descriptions:
    //   e            - immutable reference to the event
    //   sequence     - the event's sequence number in the ring buffer (incrementing from 0)
    //   end_of_batch - whether this is the last event in the current batch
    let processor = move |e: &Event, sequence: Sequence, _end_of_batch: bool| {
        println!("[Consumer] sequence={sequence}, value={}", e.value);
        tx.send(e.value).expect("channel send failed");
    };

    // Build the Disruptor:
    //   Ring buffer size must be a power of 2 (64 here).
    //   BusySpin is the lowest-latency wait strategy (consumer thread busy-waits).
    let mut producer = build_single_producer(64, factory, BusySpin)
        .handle_events_with(processor)
        .build();

    // Publish 100 events.
    // publish() spin-waits until a free slot is available -- suitable for scenarios where events cannot be dropped.
    for i in 0..100 {
        producer.publish(|e| {
            e.value = i;
        });
    }

    // Dropping the producer triggers the shutdown sequence: waits for the consumer to process
    // all published events, then stops the consumer thread.
    drop(producer);

    // Verify that all events were received by the consumer.
    let received: Vec<i64> = rx.iter().collect();
    assert_eq!(received.len(), 100);
    println!("\nAll {} events have been processed.", received.len());
}
