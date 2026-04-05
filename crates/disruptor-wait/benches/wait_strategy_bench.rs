//! Benchmark comparing WaitStrategy implementations.
//!
//! Measures the overhead of a single wait_for() call for each strategy.

use criterion::{criterion_group, criterion_main, Criterion, black_box};
use disruptor_wait::*;
use std::time::Duration;

fn bench_wait_strategies(c: &mut Criterion) {
    let mut group = c.benchmark_group("wait_strategy_overhead");

    group.bench_function("BusySpin", |b| {
        let ws = BusySpin;
        b.iter(|| {
            ws.wait_for(black_box(0));
        });
    });

    group.bench_function("BusySpinWithSpinLoopHint", |b| {
        let ws = BusySpinWithSpinLoopHint;
        b.iter(|| {
            ws.wait_for(black_box(0));
        });
    });

    group.bench_function("YieldingWaitStrategy", |b| {
        let ws = YieldingWaitStrategy::default();
        b.iter(|| {
            ws.wait_for(black_box(0));
        });
    });

    group.bench_function("SleepingWaitStrategy_1us", |b| {
        let ws = SleepingWaitStrategy::new(Duration::from_micros(1));
        b.iter(|| {
            ws.wait_for(black_box(0));
        });
    });

    group.finish();
}

criterion_group!(benches, bench_wait_strategies);
criterion_main!(benches);
