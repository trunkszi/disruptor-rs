//! Wait Strategy Comparison Example
//!
//! BusySpin: pure busy-wait, lowest latency, highest CPU usage.
//! BusySpinWithSpinLoopHint: calls core::hint::spin_loop(), allowing the CPU to save power
//! or switch hyper-threads; slightly higher latency but more energy-efficient.

use disruptor::*;

struct Event {
    value: i64,
}

fn main() {
    // -- Using BusySpin (lowest latency) --
    let mut producer_bs = build_single_producer(64, || Event { value: 0 }, BusySpin)
        .handle_events_with(|e: &Event, seq, end_of_batch| {
            if end_of_batch {
                println!("[BusySpin]        seq={seq} value={}", e.value);
            }
        })
        .build();

    for i in 0..5 {
        producer_bs.publish(|e| e.value = i);
    }
    drop(producer_bs); // Wait for consumer to finish processing, then shut down

    // -- Using BusySpinWithSpinLoopHint (slightly higher latency, more energy-efficient) --
    let mut producer_hint =
        build_single_producer(64, || Event { value: 0 }, BusySpinWithSpinLoopHint)
            .handle_events_with(|e: &Event, seq, end_of_batch| {
                if end_of_batch {
                    println!("[SpinLoopHint]    seq={seq} value={}", e.value);
                }
            })
            .build();

    for i in 100..105 {
        producer_hint.publish(|e| e.value = i);
    }
    drop(producer_hint);

    println!("Both wait strategies completed.");
}
