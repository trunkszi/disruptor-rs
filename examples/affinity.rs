//! CPU affinity and thread naming example
//!
//! Uses pin_at_core(0) to pin the consumer thread to CPU core 0,
//! and thread_name("worker-0") to assign a name to that thread.
//! Core pinning reduces context switches and cache invalidation, which is critical in low-latency scenarios.

use disruptor::*;

struct Event {
    value: i64,
}

fn main() {
    // Build the Disruptor, pinning the consumer thread to core 0 and naming it "worker-0"
    let mut producer = build_single_producer(64, || Event { value: 0 }, BusySpin)
        .pin_at_core(0)
        .thread_name("worker-0")
        .handle_events_with(|e: &Event, seq, _end_of_batch| {
            let name = std::thread::current().name().unwrap_or("unknown").to_owned();
            println!("[{name}] seq={seq} value={}", e.value);
        })
        .build();

    for i in 0..5 {
        producer.publish(|e| e.value = i);
    }

    // Dropping the producer waits for the consumer thread to exit
    drop(producer);
    println!("Consumer thread has finished all processing on core 0.");
}
