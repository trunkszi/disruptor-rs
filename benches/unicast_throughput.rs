//! Unicast (1P-1C) throughput benchmarks.
//!
//! Measures throughput across different burst sizes and ring buffer sizes,
//! following the LMAX Disruptor paper's benchmark methodology.

use criterion::{
    BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
};
use disruptor::*;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

struct Event {
    value: i64,
}

fn spsc_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_throughput");

    for &ring_size in &[64, 256, 1024, 4096, 65536] {
        for &burst in &[1u64, 10, 100] {
            group.throughput(Throughput::Elements(burst));

            group.bench_with_input(
                BenchmarkId::new(
                    format!("ring{ring_size}/burst{burst}"),
                    ring_size,
                ),
                &(ring_size, burst),
                |b, &(ring_size, burst)| {
                    let counter = Arc::new(AtomicI64::new(0));
                    let cc = counter.clone();

                    let mut producer =
                        build_single_producer(ring_size, || Event { value: 0 }, BusySpin)
                            .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
                                cc.fetch_add(1, Ordering::Relaxed);
                            })
                            .build();

                    b.iter(|| {
                        for _ in 0..burst {
                            producer.publish(|e| {
                                e.value = 1;
                            });
                        }
                    });

                    // Don't drop producer inside iter — just leak for bench.
                    std::mem::forget(producer);
                },
            );
        }
    }

    group.finish();
}

fn spsc_batch_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_batch_throughput");

    for &batch_size in &[1usize, 10, 50, 100] {
        group.throughput(Throughput::Elements(batch_size as u64));

        group.bench_with_input(
            BenchmarkId::new("batch", batch_size),
            &batch_size,
            |b, &batch_size| {
                let counter = Arc::new(AtomicI64::new(0));
                let cc = counter.clone();

                let mut producer =
                    build_single_producer(65536, || Event { value: 0 }, BusySpin)
                        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
                            cc.fetch_add(1, Ordering::Relaxed);
                        })
                        .build();

                b.iter(|| {
                    producer.batch_publish(batch_size, |iter| {
                        for e in iter {
                            e.value = 1;
                        }
                    });
                });

                std::mem::forget(producer);
            },
        );
    }

    group.finish();
}

fn mpsc_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("mpsc_throughput");
    group.throughput(Throughput::Elements(1));

    for &num_producers in &[2u32, 4, 8] {
        group.bench_with_input(
            BenchmarkId::new("producers", num_producers),
            &num_producers,
            |b, &_num_producers| {
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
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(500)
        .measurement_time(std::time::Duration::from_secs(5))
        .warm_up_time(std::time::Duration::from_secs(2));
    targets = spsc_throughput, spsc_batch_throughput, mpsc_throughput
}
criterion_main!(benches);
