//! Micro-benchmarks for Sequencer claim/publish throughput.

use criterion::{criterion_group, criterion_main, Criterion, black_box, Throughput};
use disruptor_core::*;
use disruptor_seq::*;

struct UnboundedBarrier;
impl Barrier for UnboundedBarrier {
    fn get_after(&self, _: Sequence) -> Sequence { i64::MAX }
}

fn bench_single_sequencer(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequencer_single");
    let n = 1_000_000u64;
    group.throughput(Throughput::Elements(n));

    group.bench_function("claim_publish_1M", |b| {
        b.iter(|| {
            let seq = SingleSequencer::new(65536, UnboundedBarrier);
            for _ in 0..n {
                let s = seq.claim(1);
                seq.publish(s);
                black_box(s);
            }
        });
    });

    group.bench_function("batch_claim_10", |b| {
        b.iter(|| {
            let seq = SingleSequencer::new(65536, UnboundedBarrier);
            for _ in 0..(n / 10) {
                let s = seq.claim(10);
                seq.publish_range(s - 9, s);
                black_box(s);
            }
        });
    });

    group.finish();
}

fn bench_multi_sequencer(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequencer_multi");
    let n = 1_000_000u64;
    group.throughput(Throughput::Elements(n));

    group.bench_function("claim_publish_1M_single_thread", |b| {
        b.iter(|| {
            let seq = MultiSequencer::new(65536, UnboundedBarrier);
            for _ in 0..n {
                let s = seq.claim(1);
                seq.publish(s);
                black_box(s);
            }
        });
    });

    group.finish();
}

criterion_group!(benches, bench_single_sequencer, bench_multi_sequencer);
criterion_main!(benches);
