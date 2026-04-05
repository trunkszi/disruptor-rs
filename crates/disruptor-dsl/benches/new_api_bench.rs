//! Benchmarks for the new disruptor-dsl API (Disruptor::single/multi).

use criterion::{criterion_group, criterion_main, Criterion, black_box, Throughput};
use disruptor_dsl::prelude::*;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

struct Event {
    value: i64,
}

const EVENTS: i64 = 1_000_000;

fn bench_spsc_new_api(c: &mut Criterion) {
    let mut group = c.benchmark_group("new_api_spsc");
    group.throughput(Throughput::Elements(EVENTS as u64));

    group.bench_function("BusySpin", |b| {
        b.iter(|| {
            let count = Arc::new(AtomicI64::new(0));
            let cc = count.clone();
            let mut producer = Disruptor::single(65536, || Event { value: 0 }, BusySpin)
                .handle_events_with(move |e: &Event, _: Sequence, _: bool| {
                    black_box(e.value);
                    cc.fetch_add(1, Ordering::Relaxed);
                })
                .build();
            for i in 0..EVENTS {
                producer.publish(|e| { e.value = i; });
            }
            drop(producer);
            assert_eq!(count.load(Ordering::Relaxed), EVENTS);
        });
    });

    group.bench_function("Yielding", |b| {
        b.iter(|| {
            let count = Arc::new(AtomicI64::new(0));
            let cc = count.clone();
            let mut producer = Disruptor::single(65536, || Event { value: 0 }, YieldingWaitStrategy::default())
                .handle_events_with(move |e: &Event, _: Sequence, _: bool| {
                    black_box(e.value);
                    cc.fetch_add(1, Ordering::Relaxed);
                })
                .build();
            for i in 0..EVENTS {
                producer.publish(|e| { e.value = i; });
            }
            drop(producer);
            assert_eq!(count.load(Ordering::Relaxed), EVENTS);
        });
    });

    group.finish();
}

fn bench_mpsc_new_api(c: &mut Criterion) {
    let mut group = c.benchmark_group("new_api_mpsc");
    let events_per_producer = EVENTS / 2;
    group.throughput(Throughput::Elements(EVENTS as u64));

    group.bench_function("2_producers_BusySpin", |b| {
        b.iter(|| {
            let count = Arc::new(AtomicI64::new(0));
            let cc = count.clone();
            let mut p1 = Disruptor::multi(65536, || Event { value: 0 }, BusySpin)
                .handle_events_with(move |e: &Event, _: Sequence, _: bool| {
                    black_box(e.value);
                    cc.fetch_add(1, Ordering::Relaxed);
                })
                .build();
            let mut p2 = p1.clone();
            std::thread::scope(|s| {
                s.spawn(move || {
                    for i in 0..events_per_producer {
                        p1.publish(|e| { e.value = i; });
                    }
                });
                s.spawn(move || {
                    for i in 0..events_per_producer {
                        p2.publish(|e| { e.value = i; });
                    }
                });
            });
            assert_eq!(count.load(Ordering::Relaxed), EVENTS);
        });
    });

    group.finish();
}

fn bench_batch_publish(c: &mut Criterion) {
    let mut group = c.benchmark_group("new_api_batch");
    group.throughput(Throughput::Elements(EVENTS as u64));

    group.bench_function("batch_10", |b| {
        b.iter(|| {
            let count = Arc::new(AtomicI64::new(0));
            let cc = count.clone();
            let mut producer = Disruptor::single(65536, || Event { value: 0 }, BusySpin)
                .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
                    cc.fetch_add(1, Ordering::Relaxed);
                })
                .build();
            for _ in 0..(EVENTS / 10) {
                producer.batch_publish(10, |iter| {
                    for e in iter {
                        e.value = black_box(42);
                    }
                });
            }
            drop(producer);
            assert_eq!(count.load(Ordering::Relaxed), EVENTS);
        });
    });

    group.finish();
}

fn bench_mpsc_4_producers(c: &mut Criterion) {
    let mut group = c.benchmark_group("new_api_mpsc_4p");
    let events_per_producer = EVENTS / 4;
    group.throughput(Throughput::Elements(EVENTS as u64));

    group.bench_function("4_producers_BusySpin", |b| {
        b.iter(|| {
            let count = Arc::new(AtomicI64::new(0));
            let cc = count.clone();
            let mut p1 = Disruptor::multi(65536, || Event { value: 0 }, BusySpin)
                .handle_events_with(move |e: &Event, _: Sequence, _: bool| {
                    black_box(e.value);
                    cc.fetch_add(1, Ordering::Relaxed);
                })
                .build();
            let mut p2 = p1.clone();
            let mut p3 = p1.clone();
            let mut p4 = p1.clone();
            std::thread::scope(|s| {
                s.spawn(move || { for i in 0..events_per_producer { p1.publish(|e| { e.value = i; }); } });
                s.spawn(move || { for i in 0..events_per_producer { p2.publish(|e| { e.value = i; }); } });
                s.spawn(move || { for i in 0..events_per_producer { p3.publish(|e| { e.value = i; }); } });
                s.spawn(move || { for i in 0..events_per_producer { p4.publish(|e| { e.value = i; }); } });
            });
            assert_eq!(count.load(Ordering::Relaxed), EVENTS);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_spsc_new_api, bench_mpsc_new_api, bench_batch_publish, bench_mpsc_4_producers);
criterion_main!(benches);
