//! # Diamond topology example: A and B in parallel -> C joins
//!
//! Consumers A and B process the same batch of events in parallel. C only begins processing after both A and B have finished.
//!
//! ```text
//!            /-- [A] --\
//! Producer --          --> [C]
//!            \-- [B] --/
//! ```
//!
//! ```text
//! cargo run --example diamond
//! ```

use disruptor::*;

struct Event {
    price: f64,
}

fn main() {
    let factory = || Event { price: 0.0 };

    // Consumer A: e.g., compute a moving average.
    let handler_a = |e: &Event, seq: Sequence, _eob: bool| {
        println!("[A] seq={seq} price={:.2}", e.price);
    };

    // Consumer B: e.g., risk check -- runs in parallel with A.
    let handler_b = |e: &Event, seq: Sequence, _eob: bool| {
        println!("[B] seq={seq} price={:.2}", e.price);
    };

    // Consumer C: join stage -- waits for both A and B to finish before executing.
    let handler_c = |e: &Event, seq: Sequence, _eob: bool| {
        println!("[C] seq={seq} joined processing price={:.2}", e.price);
    };

    // Build the diamond topology:
    //   First handle_events_with adds A (builder state transitions from NC -> SC).
    //   Second handle_events_with adds B (state transitions from SC -> MC, A and B run in parallel).
    //   .and_then() marks A and B as upstream dependencies for downstream consumers.
    //   Third handle_events_with adds C (depends on A and B).
    let mut producer = build_single_producer(64, factory, BusySpin)
        .handle_events_with(handler_a)
        .handle_events_with(handler_b)
        .and_then()
            .handle_events_with(handler_c)
        .build();

    for i in 0..8 {
        producer.publish(|e| {
            e.price = (i as f64) * 1.5;
        });
    }

    drop(producer);
    println!("\nDiamond topology processing complete.");
}
