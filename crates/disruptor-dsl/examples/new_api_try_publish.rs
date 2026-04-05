//! Error handling with try_publish — non-blocking publish that returns Result.

use disruptor_dsl::prelude::*;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

struct Event {
    data: i64,
}

fn main() {
    let count = Arc::new(AtomicI64::new(0));
    let cc = count.clone();

    let mut producer = Disruptor::single(8, || Event { data: 0 }, BusySpin)
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            // Simulate slow consumer.
            std::thread::sleep(std::time::Duration::from_millis(10));
            cc.fetch_add(1, Ordering::Relaxed);
        })
        .build();

    let mut published = 0;
    let mut full_errors = 0;

    for i in 0..100i64 {
        match producer.try_publish(|e| { e.data = i; }) {
            Ok(seq) => {
                published += 1;
                if published <= 3 {
                    println!("Published event {i} at sequence {seq}");
                }
            }
            Err(PublishError::Full) => {
                full_errors += 1;
                // In production: back-pressure, discard, or retry.
                if full_errors <= 3 {
                    println!("Buffer full at event {i}, applying back-pressure...");
                }
                std::thread::sleep(std::time::Duration::from_millis(5));
                // Retry.
                producer.publish(|e| { e.data = i; });
                published += 1;
            }
            Err(PublishError::InsufficientCapacity { missing }) => {
                println!("Missing {missing} slots (batch)");
            }
            Err(_) => unreachable!(),
        }
    }

    drop(producer);
    println!("Published: {published}, Full errors: {full_errors}");
    println!("Consumed: {}", count.load(Ordering::Relaxed));
}
