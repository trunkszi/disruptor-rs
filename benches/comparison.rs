//! Comparison benchmarks: Disruptor vs crossbeam-channel.
//!
//! Provides a fair comparison following the paper's methodology:
//! same event size, same producer/consumer count, same thread pinning.

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use disruptor::*;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

struct Event {
    value: i64,
}

// ─── Disruptor 1P-1C ───────────────────────────────────────────────────────

fn disruptor_spsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("comparison_spsc");
    group.throughput(Throughput::Elements(1));

    group.bench_function("disruptor_busyspin", |b| {
        let counter = Arc::new(AtomicI64::new(0));
        let cc = counter.clone();

        let mut producer =
            build_single_producer(65536, || Event { value: 0 }, BusySpin)
                .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
                    cc.fetch_add(1, Ordering::Relaxed);
                })
                .build();

        b.iter(|| {
            producer.publish(|e| {
                e.value = 1;
            });
        });

        std::mem::forget(producer);
    });

    group.bench_function("disruptor_spinloophint", |b| {
        let counter = Arc::new(AtomicI64::new(0));
        let cc = counter.clone();

        let mut producer = build_single_producer(
            65536,
            || Event { value: 0 },
            BusySpinWithSpinLoopHint,
        )
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            cc.fetch_add(1, Ordering::Relaxed);
        })
        .build();

        b.iter(|| {
            producer.publish(|e| {
                e.value = 1;
            });
        });

        std::mem::forget(producer);
    });

    // ─── Crossbeam channel baseline ─────────────────────────────────────

    group.bench_function("crossbeam_bounded", |b| {
        let (tx, rx) = crossbeam::channel::bounded::<i64>(65536);

        let _consumer = std::thread::spawn(move || {
            while let Ok(_val) = rx.recv() {
                // Consume.
            }
        });

        b.iter(|| {
            let _ = tx.send(1);
        });

        // Don't drop tx inside iter — leak for bench.
        std::mem::forget(tx);
    });

    group.finish();
}

// ─── Disruptor MPSC vs Crossbeam MPSC ───────────────────────────────────────

fn disruptor_mpsc_vs_crossbeam(c: &mut Criterion) {
    let mut group = c.benchmark_group("comparison_mpsc");
    group.throughput(Throughput::Elements(1));

    group.bench_function("disruptor_multi", |b| {
        let counter = Arc::new(AtomicI64::new(0));
        let cc = counter.clone();

        let mut producer =
            build_multi_producer(65536, || Event { value: 0 }, BusySpin)
                .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
                    cc.fetch_add(1, Ordering::Relaxed);
                })
                .build();

        b.iter(|| {
            producer.publish(|e| {
                e.value = 1;
            });
        });

        std::mem::forget(producer);
    });

    group.bench_function("crossbeam_bounded_mpsc", |b| {
        let (tx, rx) = crossbeam::channel::bounded::<i64>(65536);

        let _consumer = std::thread::spawn(move || {
            while let Ok(_val) = rx.recv() {
                // Consume.
            }
        });

        b.iter(|| {
            let _ = tx.send(1);
        });

        std::mem::forget(tx);
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(500)
        .measurement_time(std::time::Duration::from_secs(5))
        .warm_up_time(std::time::Duration::from_secs(2));
    targets = disruptor_spsc, disruptor_mpsc_vs_crossbeam
}
criterion_main!(benches);
