//! Multi Producer Single Consumer using the new Disruptor API.

use disruptor_dsl::prelude::*;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::thread;

struct Event {
    value: i64,
}

fn main() {
    let total = Arc::new(AtomicI64::new(0));
    let tc = total.clone();

    let mut producer1 = Disruptor::multi(64, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |e: &Event, _: Sequence, _: bool| {
            tc.fetch_add(e.value, Ordering::Relaxed);
        })
        .build();

    let mut producer2 = producer1.clone();

    thread::scope(|s| {
        s.spawn(move || {
            for i in 0..100 {
                producer1.publish(|e| { e.value = i; });
            }
        });
        s.spawn(move || {
            for i in 100..200 {
                producer2.publish(|e| { e.value = i; });
            }
        });
    });

    println!("Total sum: {}", total.load(Ordering::Relaxed));
    // Expected: 0+1+...+199 = 19900
}
