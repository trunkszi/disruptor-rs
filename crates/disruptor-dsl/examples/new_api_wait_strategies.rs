//! Comparing different WaitStrategies.

use disruptor_dsl::prelude::*;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Instant;

struct Event {
    value: i64,
}

fn bench_strategy<W: WaitStrategy + 'static>(name: &str, ws: W, n: i64) {
    let count = Arc::new(AtomicI64::new(0));
    let cc = count.clone();

    let mut producer = Disruptor::single(1024, || Event { value: 0 }, ws)
        .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
            cc.fetch_add(1, Ordering::Relaxed);
        })
        .build();

    let start = Instant::now();
    for i in 0..n {
        producer.publish(|e| { e.value = i; });
    }
    drop(producer);
    let elapsed = start.elapsed();

    println!(
        "{:30} | {:>8} events | {:>10.2?} | {:.1}M ops/sec",
        name,
        count.load(Ordering::Relaxed),
        elapsed,
        n as f64 / elapsed.as_secs_f64() / 1_000_000.0,
    );
}

fn main() {
    let n = 1_000_000;
    println!("{:30} | {:>14} | {:>10} | {}", "Strategy", "Events", "Time", "Throughput");
    println!("{:-<80}", "");

    bench_strategy("BusySpin", BusySpin, n);
    bench_strategy("BusySpinWithSpinLoopHint", BusySpinWithSpinLoopHint, n);
    bench_strategy("YieldingWaitStrategy", YieldingWaitStrategy::default(), n);
    bench_strategy("SleepingWaitStrategy(1us)", SleepingWaitStrategy::new(std::time::Duration::from_micros(1)), n / 10);
}
