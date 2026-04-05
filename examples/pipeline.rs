//! # Pipeline Topology Example: Stage A -> Stage B -> Stage C
//!
//! Uses `.and_then()` to build a sequential pipeline: each stage only begins processing
//! an event after the previous stage has finished processing it.
//!
//! ```text
//! Producer --> [Stage A] --> [Stage B] --> [Stage C]
//! ```
//!
//! ```text
//! cargo run --example pipeline
//! ```

use disruptor::*;

/// Events flow through multiple stages in the pipeline; each stage can read but not modify the event.
/// (To pass computed results across stages, use handle_events_and_state_with.)
struct Event {
    raw_value: i64,
}

fn main() {
    let factory = || Event { raw_value: 0 };

    // Stage A: validation/filtering stage.
    let stage_a = |e: &Event, seq: Sequence, _eob: bool| {
        println!("[Stage A] seq={seq} received raw value: {}", e.raw_value);
    };

    // Stage B: transformation stage -- executes only after A completes.
    let stage_b = |e: &Event, seq: Sequence, _eob: bool| {
        let transformed = e.raw_value * 2;
        println!("[Stage B] seq={seq} transformed: {transformed}");
    };

    // Stage C: output stage -- executes only after B completes.
    let stage_c = |e: &Event, seq: Sequence, _eob: bool| {
        println!("[Stage C] seq={seq} final processing: raw_value={}", e.raw_value);
    };

    // Build the pipeline topology:
    //   handle_events_with(A)  -- add A
    //   .and_then()            -- subsequent consumers can only read after A completes
    //   handle_events_with(B)  -- add B (depends on A)
    //   .and_then()            -- subsequent consumers can only read after B completes
    //   handle_events_with(C)  -- add C (depends on B)
    let mut producer = build_single_producer(64, factory, BusySpin)
        .handle_events_with(stage_a)
        .and_then()
            .handle_events_with(stage_b)
        .and_then()
            .handle_events_with(stage_c)
        .build();

    // Publish 10 events.
    for i in 0..10 {
        producer.publish(|e| {
            e.raw_value = i;
        });
    }

    // drop(producer) triggers an orderly shutdown: A finishes first, then B, then C.
    drop(producer);
    println!("\nPipeline processing complete.");
}
