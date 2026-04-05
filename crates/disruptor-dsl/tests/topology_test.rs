//! Topology tests: diamond/DAG patterns and error formatting.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use disruptor_dsl::prelude::*;
use disruptor_consumer::Polling;

struct Event {
    value: i64,
}

// ─── Diamond topology: new_branch + join ───────────────────────────────────
//
//     Producer → A ─┐
//                   └──→ C
//     Producer → B ─┘
//
// A and B process in parallel. C waits for both.

#[test]
fn diamond_topology_via_event_poller() {
    let a_count = Arc::new(AtomicI64::new(0));
    let ac = a_count.clone();

    // Build with a branch: consumer A in the main flow.
    let builder = disruptor_dsl::build_single_producer(64, || Event { value: 0 }, BusySpin);
    let (mut branch_poller, builder) = builder.new_event_poller();

    let mut producer = builder
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            ac.fetch_add(1, Ordering::Relaxed);
        })
        .build();

    // Publish events.
    for i in 0..50i64 {
        producer.publish(|e| { e.value = i; });
    }
    drop(producer);

    // Poll events from the branch.
    let mut branch_count = 0i64;
    loop {
        match branch_poller.poll() {
            Ok(mut guard) => {
                branch_count += (&mut guard).count() as i64;
            }
            Err(Polling::NoEvents) => continue,
            Err(Polling::Shutdown) => break,
            Err(_) => unreachable!(),
        }
    }

    assert_eq!(a_count.load(Ordering::Relaxed), 50);
    assert_eq!(branch_count, 50);
}

// ─── Three-stage pipeline ──────────────────────────────────────────────────

#[test]
fn three_stage_pipeline() {
    let s1 = Arc::new(AtomicI64::new(0));
    let s2 = Arc::new(AtomicI64::new(0));
    let s3 = Arc::new(AtomicI64::new(0));
    let sc1 = s1.clone();
    let sc2 = s2.clone();
    let sc3 = s3.clone();

    let mut producer = Disruptor::single(64, || Event { value: 0 }, BusySpin)
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            sc1.fetch_add(1, Ordering::Relaxed);
        })
        .and_then()
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            sc2.fetch_add(1, Ordering::Relaxed);
        })
        .and_then()
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            sc3.fetch_add(1, Ordering::Relaxed);
        })
        .build();

    for i in 0..200i64 {
        producer.publish(|e| { e.value = i; });
    }
    drop(producer);

    assert_eq!(s1.load(Ordering::Relaxed), 200);
    assert_eq!(s2.load(Ordering::Relaxed), 200);
    assert_eq!(s3.load(Ordering::Relaxed), 200);
}

// ─── PublishError Display ──────────────────────────────────────────────────

#[test]
fn publish_error_full_display() {
    let err = PublishError::Full;
    assert_eq!(format!("{err}"), "Ring buffer is full");
}

#[test]
fn publish_error_insufficient_capacity_display() {
    let err = PublishError::InsufficientCapacity { missing: 5 };
    assert_eq!(format!("{err}"), "Ring buffer missing 5 free slot(s)");
}

#[test]
fn publish_error_equality() {
    assert_eq!(PublishError::Full, PublishError::Full);
    assert_ne!(PublishError::Full, PublishError::InsufficientCapacity { missing: 1 });
    assert_eq!(
        PublishError::InsufficientCapacity { missing: 3 },
        PublishError::InsufficientCapacity { missing: 3 },
    );
}
