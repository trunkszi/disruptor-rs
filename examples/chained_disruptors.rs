//! Chained Disruptors example
//!
//! Chains two Disruptors together: the first Disruptor's consumer publishes processed results to the second Disruptor.
//! Uses Arc<Mutex<Producer>> to hold the second Disruptor's Producer handle inside the first consumer's closure.
//! This pattern is suitable for multi-stage pipeline processing.

use disruptor::*;
use std::sync::{Arc, Mutex};

struct RawEvent {
    value: i64,
}

struct EnrichedEvent {
    value:   i64,
    doubled: i64,
}

fn main() {
    // -- Stage 2: final consumer --
    let stage2_producer =
        build_single_producer(64, || EnrichedEvent { value: 0, doubled: 0 }, BusySpin)
            .handle_events_with(|e: &EnrichedEvent, seq, _| {
                println!("[stage-2] seq={seq} value={} doubled={}", e.value, e.doubled);
            })
            .build();

    // Wrap with Arc<Mutex<_>> so it can be used inside the first stage's consumer closure
    let stage2 = Arc::new(Mutex::new(stage2_producer));
    let stage2_clone = Arc::clone(&stage2);

    // -- Stage 1: preprocessing, forwarding results to stage 2 --
    let mut stage1_producer =
        build_single_producer(64, || RawEvent { value: 0 }, BusySpin)
            .handle_events_with(move |e: &RawEvent, seq, _| {
                let doubled = e.value * 2;
                println!("[stage-1] seq={seq} value={} -> doubled={doubled}", e.value);
                stage2_clone.lock().unwrap().publish(|out| {
                    out.value   = e.value;
                    out.doubled = doubled;
                });
            })
            .build();

    // -- Publish raw events --
    for i in 1..=5 {
        stage1_producer.publish(|e| e.value = i);
    }

    // Shut down stage 1 first, ensuring all events have been forwarded
    drop(stage1_producer);
    // Then shut down stage 2 (at this point the closure has been dropped, so the Arc has only one reference left)
    match Arc::try_unwrap(stage2) {
        Ok(mutex) => drop(mutex.into_inner().unwrap()),
        Err(_)    => panic!("stage2 Arc should have only one reference remaining"),
    }

    println!("Two-stage pipeline processing complete.");
}
