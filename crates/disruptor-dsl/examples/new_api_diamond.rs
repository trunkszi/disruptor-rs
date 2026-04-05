//! Diamond/DAG topology using new_branch + EventPoller.
//!
//! ```text
//!              /── Consumer A (managed) ──\
//! Producer ──>                              >── Poller (pull)
//!              \── EventPoller (branch) ──/
//! ```

use disruptor_dsl::prelude::*;
use disruptor_consumer::Polling;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

struct Event {
    data: i64,
}

fn main() {
    let managed_count = Arc::new(AtomicI64::new(0));
    let mc = managed_count.clone();

    // Create builder with an event poller branch.
    let builder = disruptor_dsl::build_single_producer(64, || Event { data: 0 }, BusySpin);
    let (mut poller, builder) = builder.new_event_poller();

    // Managed consumer (runs in its own thread).
    let mut producer = builder
        .handle_events_with(move |e: &Event, _: Sequence, _: bool| {
            mc.fetch_add(1, Ordering::Relaxed);
        })
        .build();

    // Publish events.
    for i in 0..20 {
        producer.publish(|e| { e.data = i; });
    }
    drop(producer);

    // Poll events from the branch (pull-mode).
    let mut polled = 0;
    loop {
        match poller.poll() {
            Ok(mut guard) => {
                for event in &mut guard {
                    println!("Polled: data={}", event.data);
                    polled += 1;
                }
            }
            Err(Polling::NoEvents) => continue,
            Err(Polling::Shutdown) => break,
            Err(_) => unreachable!(),
        }
    }

    println!("Managed consumer: {} events", managed_count.load(Ordering::Relaxed));
    println!("Poller branch: {} events", polled);
}
