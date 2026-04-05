//! # Batch Publish Example
//!
//! Demonstrates the `batch_publish` and `try_batch_publish` batch publishing APIs.
//! Batch publishing is more efficient than publishing one at a time: it reserves multiple
//! slots at once, reducing sequence number contention.
//!
//! ```text
//! cargo run --example batch_publish
//! ```

use disruptor::*;

struct Event {
    value: i64,
}

fn main() {
    let factory = || Event { value: 0 };

    let (tx, rx) = std::sync::mpsc::channel();

    let processor = move |e: &Event, seq: Sequence, _eob: bool| {
        println!("[Consumer] seq={seq} value={}", e.value);
        tx.send(e.value).expect("send failed");
    };

    let mut producer = build_single_producer(64, factory, BusySpin)
        .handle_events_with(processor)
        .build();

    // -- 1. batch_publish: blocking batch publish --
    // Publish 5 events at once. The closure receives a MutBatchIter guaranteed to yield exactly 5 mutable references.
    println!("--- batch_publish: publishing 5 events ---");
    producer.batch_publish(5, |iter| {
        for (i, event) in iter.enumerate() {
            event.value = (i as i64) * 10;
        }
    });

    // Publish another batch of 3.
    println!("--- batch_publish: publishing 3 more events ---");
    producer.batch_publish(3, |iter| {
        for (i, event) in iter.enumerate() {
            event.value = 100 + i as i64;
        }
    });

    // -- 2. try_batch_publish: non-blocking batch publish --
    // If the ring buffer does not have enough space, returns Err(MissingFreeSlots(n)) where n is the number of missing slots.
    println!("--- try_batch_publish: attempting to publish 4 events ---");
    match producer.try_batch_publish(4, |iter| {
        for (i, event) in iter.enumerate() {
            event.value = 200 + i as i64;
        }
    }) {
        Ok(seq) => println!("  Success, highest sequence: {seq}"),
        Err(MissingFreeSlots(n)) => println!("  Insufficient space, missing {n} slots"),
    }

    // -- 3. try_publish: non-blocking single publish (for comparison) --
    println!("--- try_publish: attempting to publish 1 event ---");
    match producer.try_publish(|e| { e.value = 999; }) {
        Ok(seq)              => println!("  Success, sequence: {seq}"),
        Err(RingBufferFull)  => println!("  Ring buffer is full"),
    }

    // Shut down and collect results.
    drop(producer);

    let results: Vec<i64> = rx.iter().collect();
    println!("\nProcessed {} events in total: {:?}", results.len(), results);
}
