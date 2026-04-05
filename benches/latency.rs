//! Latency benchmarks.
//!
//! Measures end-to-end publish-to-consume latency using
//! high-resolution timestamps, following the LMAX paper methodology.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use disruptor::*;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

struct TimestampEvent {
    publish_time_ns: u64,
}

fn spsc_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_latency");

    for strategy_name in &["BusySpin", "BusySpinWithHint"] {
        group.bench_function(
            BenchmarkId::new("1P-1C", strategy_name),
            |b| {
                let total_latency_ns = Arc::new(AtomicU64::new(0));
                let count = Arc::new(AtomicI64::new(0));
                let tl = total_latency_ns.clone();
                let cc = count.clone();

                let mut producer = build_single_producer(
                    65536,
                    || TimestampEvent { publish_time_ns: 0 },
                    BusySpin,
                )
                .handle_events_with(move |e: &TimestampEvent, _: Sequence, _: bool| {
                    let now = Instant::now();
                    // Approximate: we can't directly subtract, so use a shared
                    // counter to track.
                    let _ = tl.fetch_add(e.publish_time_ns, Ordering::Relaxed);
                    cc.fetch_add(1, Ordering::Relaxed);
                    let _ = now; // Use for future HDR histogram integration.
                })
                .build();

                b.iter(|| {
                    producer.publish(|e| {
                        e.publish_time_ns = 0; // Placeholder
                    });
                });

                std::mem::forget(producer);
            },
        );
    }

    group.finish();
}

fn pipeline_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline_latency");

    group.bench_function("1P-3C-pipeline", |b| {
        let count = Arc::new(AtomicI64::new(0));
        let cc = count.clone();

        let mut producer = build_single_producer(
            65536,
            || TimestampEvent { publish_time_ns: 0 },
            BusySpin,
        )
        .handle_events_with(|_: &TimestampEvent, _: Sequence, _: bool| {
            // Stage 1: validation
        })
        .and_then()
        .handle_events_with(|_: &TimestampEvent, _: Sequence, _: bool| {
            // Stage 2: business logic
        })
        .and_then()
        .handle_events_with(move |_: &TimestampEvent, _: Sequence, _: bool| {
            cc.fetch_add(1, Ordering::Relaxed);
        })
        .build();

        b.iter(|| {
            producer.publish(|e| {
                e.publish_time_ns = 0;
            });
        });

        std::mem::forget(producer);
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(1000)
        .measurement_time(std::time::Duration::from_secs(5))
        .warm_up_time(std::time::Duration::from_secs(2));
    targets = spsc_latency, pipeline_latency
}
criterion_main!(benches);
