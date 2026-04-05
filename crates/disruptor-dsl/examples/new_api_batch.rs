//! Batch publishing example using the new Disruptor API.

use disruptor_dsl::prelude::*;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

struct Event {
    data: i64,
}

fn main() {
    let count = Arc::new(AtomicI64::new(0));
    let cc = count.clone();

    let mut producer = Disruptor::single(64, || Event { data: 0 }, BusySpin)
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            cc.fetch_add(1, Ordering::Relaxed);
        })
        .build();

    // Publish 10 batches of 5 events each.
    for batch_id in 0..10 {
        producer.batch_publish(5, |iter| {
            for (i, e) in iter.enumerate() {
                e.data = batch_id * 5 + i as i64;
            }
        });
    }

    drop(producer);
    println!("Processed {} events in batches", count.load(Ordering::Relaxed));
}
