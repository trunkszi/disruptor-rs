//! EventPoller pull-based consumption using the new Disruptor API.
//!
//! EventPoller gives you control over when events are consumed,
//! useful for integrating with existing event loops.

use disruptor_dsl::prelude::*;
use disruptor_consumer::Polling;

#[derive(Debug)]
struct Event {
    price: f64,
}

fn main() {
    let factory = || Event { price: 0.0 };
    let builder = disruptor_dsl::build_single_producer(8, factory, BusySpin);
    let (mut event_poller, builder) = builder.new_event_poller();
    let mut producer = builder.build();

    // Publish some events.
    for i in 0..5 {
        producer.publish(|e| {
            e.price = i as f64 * 1.5;
        });
    }
    drop(producer); // Signal shutdown.

    // Poll events in a loop.
    loop {
        match event_poller.poll() {
            Ok(mut guard) => {
                let batch_size = (&mut guard).len();
                println!("Got batch of {batch_size} events:");
                for event in &mut guard {
                    println!("  price = {:.1}", event.price);
                }
            }
            Err(Polling::NoEvents) => {
                println!("No events, doing other work...");
                continue;
            }
            Err(Polling::Shutdown) => {
                println!("Disruptor shut down.");
                break;
            }
            Err(_) => unreachable!(),
        }
    }
}
