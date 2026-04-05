//! # Event polling example: manual consumer thread control
//!
//! EventPoller provides pull-based event consumption: the user controls the consumer thread
//! rather than letting the Disruptor manage it. Suitable for integration with existing threading models.
//!
//! ```text
//! cargo run --example event_poller
//! ```

use std::thread;
use disruptor::*;

#[derive(Debug)]
struct Event {
    price: f64,
}

fn main() {
    let factory = || Event { price: 0.0 };

    // Obtain a poller via new_event_poller() when building the Disruptor.
    // Note: new_event_poller consumes the builder and returns (EventPoller, new builder).
    let builder = build_single_producer(64, factory, BusySpin);
    let (mut poller, builder) = builder.new_event_poller();
    let mut producer = builder.build();

    // Run the polling loop in a dedicated thread.
    let consumer_thread = thread::spawn(move || {
        let mut total = 0u64;
        loop {
            // poll() returns all currently available events.
            match poller.poll() {
                Ok(mut guard) => {
                    // EventGuard implements ExactSizeIterator, so we can get the batch size.
                    let batch_size = (&mut guard).len();
                    // Iterate over events (via &mut guard).
                    for event in &mut guard {
                        println!("[poller] price={:.1}", event.price);
                        total += 1;
                    }
                    println!("  (batch of {batch_size} events)");
                    // When guard is dropped, it automatically notifies the Disruptor that processing is complete.
                }
                Err(Polling::NoEvents) => {
                    // No new events -- can do other work or briefly yield the CPU.
                    thread::yield_now();
                }
                Err(Polling::Shutdown) => {
                    // All producers have shut down and all events have been consumed.
                    println!("[poller] received shutdown signal, processed {total} events total.");
                    break;
                }
                Err(_) => unreachable!(),
            }
        }
        total
    });

    // Producer publishes 30 events.
    for i in 0..30 {
        producer.publish(|e| {
            e.price = i as f64 * 0.5;
        });
    }

    // Dropping the producer triggers the shutdown process.
    drop(producer);

    let total = consumer_thread.join().expect("consumer thread panicked");
    assert_eq!(total, 30);
    println!("\nEvent polling example complete.");
}
