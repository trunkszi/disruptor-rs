# Benchmark Methodology

> **Key takeaway**: disruptor-rs benchmarks are based on the Criterion framework, using `iter_custom` for precise control of measurement boundaries, burst mode to simulate real-world load patterns, and equal-condition comparison with Crossbeam bounded channels.

**Abstract**: This document describes the design philosophy, measurement methods, environment controls, and statistical analysis process for disruptor-rs benchmarks, ensuring reproducibility and scientific validity of performance data.

---

## 1. Framework & Toolchain

### 1.1 Criterion.rs

disruptor-rs uses [Criterion.rs](https://github.com/bheisler/criterion.rs) 0.5 as its benchmark framework:

```toml
[dev-dependencies]
criterion = { version = "0.5", features = ["html_reports"] }
```

Criterion provides:
- Automatic warm-up and iteration count selection
- Statistical significance testing (t-test)
- HTML reports and regression detection
- `iter_custom` support for custom timing boundaries

### 1.2 Toolchain Requirements

```toml
# rust-toolchain.toml
[toolchain]
channel = "nightly"
components = ["clippy", "rustfmt", "rust-src", "rust-analyzer", "miri"]
```

Benchmarks run on nightly to ensure the same compiler optimizations as production builds.

---

## 2. Measurement Methods

### 2.1 Timing Boundaries

Benchmarks use `iter_custom` for precise control of the measurement range, excluding setup/teardown overhead:

```rust
group.bench_with_input(id, &inputs, |b, (size, pause_ms)| {
    b.iter_custom(|iters| {
        pause(*pause_ms);               // Not included in timing
        let start = Instant::now();      // <-- Measurement start
        for _ in 0..iters {
            // Publish + wait for consumption to complete
            producer.batch_publish(*size as usize, |iter| { /* ... */ });
            while sink.load(Ordering::Acquire) != last_data {}
        }
        start.elapsed()                  // <-- Measurement end
    })
});
```

Key principles:
- **End-to-end latency**: From publish start to consumer processing completion (sink confirmation)
- **Exclude setup**: `Instant::now()` is placed after the pause
- **Include synchronization**: The consumer's store + producer's load are included in the measurement

### 2.2 Warm-up Strategy

Criterion performs a default 3-second warm-up:
- Allows CPU caches to warm up
- Allows the OS scheduler to stabilize thread scheduling
- Allows the branch predictor to build prediction models

### 2.3 Burst Mode

Benchmarks use burst mode to simulate real-world scenarios:

| Parameter | Values | Meaning |
|------|-----|------|
| `BURST_SIZES` | `[1, 10, 100]` | Number of events published per burst |
| `PAUSES_MS` | `[0, 1, 10]` | Pause duration between bursts |
| `DATA_STRUCTURE_SIZE` | `128` (SPSC), `256` (MPSC) | Ring buffer size |

Advantages of burst mode:
- `burst=1, pause=0`: Tests sustained high-frequency single-event publishing
- `burst=100, pause=0`: Tests the throughput limit of batch publishing
- `burst=10, pause=10ms`: Simulates real intermittent load

---

## 3. Throughput Measurement

### 3.1 Throughput Declaration

```rust
group.throughput(Throughput::Elements(burst_size));
```

Criterion automatically calculates ops/sec based on the number of elements processed per iteration.

### 3.2 SPSC Benchmark

Single producer, single consumer scenario. In `benches/spsc.rs`:

```
Producer Thread ──publish──> RingBuffer ──consume──> Consumer Thread
                                                         │
                                                    sink.store()
                                                         │
Producer Thread <──sink.load()──────────────── confirmation
```

### 3.3 MPSC Benchmark

Multiple producer, single consumer scenario. In `benches/mpsc.rs`:

```
Producer Thread 1 ─┐
                    ├──publish──> RingBuffer ──consume──> Consumer Thread
Producer Thread 2 ─┘                                          │
                                                         sink.fetch_add()
                                                              │
Benchmark Thread <──sink.load()──────────────────── confirmation
```

The MPSC benchmark uses a `BurstProducer` structure to manage producer threads, avoiding the overhead of recreating threads for each sample:

```rust
struct BurstProducer {
    start_barrier: Arc<CachePadded<AtomicBool>>,  // Controls burst triggering
    stop:          Arc<CachePadded<AtomicBool>>,  // Controls thread exit
    join_handle:   Option<JoinHandle<()>>,
}
```

---

## 4. Latency Measurement

### 4.1 Percentile Metrics

Latency benchmarks should report the following percentiles:

| Percentile | Meaning | Importance |
|--------|------|--------|
| p50 | Median latency | Typical user experience |
| p99 | 99th percentile | SLA-level metric |
| p99.9 | 99.9th percentile | Tail latency |
| p99.99 | 99.99th percentile | Extreme cases |

$$
\text{p99 latency} = \text{value at rank} \lceil 0.99 \times N \rceil \text{ in sorted samples}
$$

### 4.2 Measurement Considerations

- **Use `rdtsc`/`Instant`**: Avoid NTP jumps with `SystemTime`
- **Measure publish and consume separately**: Distinguish latency contributions from producer and consumer
- **Large sample size**: At least 100,000 measurements for reliable tail distribution

---

## 5. Environment Controls

### 5.1 CPU Isolation

For reproducible results, the following Linux configuration is recommended:

```bash
# Isolate CPU cores 2 and 3 for benchmark use
# /etc/default/grub: GRUB_CMDLINE_LINUX="isolcpus=2,3 nohz_full=2,3"

# Or use taskset to run
taskset -c 2,3 cargo bench
```

### 5.2 CPU Frequency Locking

```bash
# Disable frequency scaling
echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor

# Disable turbo boost (Intel)
echo 1 | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo
```

### 5.3 NUMA Control

```bash
# Ensure the benchmark runs on the same NUMA node
numactl --cpunodebind=0 --membind=0 cargo bench
```

### 5.4 OS Noise Minimization

```bash
# Check IRQ affinity
cat /proc/interrupts

# Disable unnecessary services
sudo systemctl stop cron
sudo systemctl stop snapd
```

---

## 6. Comparison Methodology

### 6.1 Comparison Targets

| Data Structure | Crate | Type | Notes |
|---|---|---|---|
| Crossbeam bounded channel | `crossbeam` 0.8 | MPMC bounded queue | Most widely used Rust concurrent channel |
| flume | `flume` | MPMC channel | Another popular channel |
| kanal | `kanal` | MPMC channel | High-performance channel |

### 6.2 Fair Comparison Principles

1. **Same data structure size**: Disruptor and Crossbeam use the same buffer capacity
2. **Same thread topology**: Same number of producer and consumer threads
3. **Same workload**: Same burst size and pause pattern
4. **Same confirmation mechanism**: Both use `AtomicI64` sink for consumer completion confirmation
5. **Same compilation optimization**: Both compiled in `--release` mode

### 6.3 Known Bias Disclosure

- Disruptor uses pre-allocated events; Crossbeam `try_send` requires move ownership
- Disruptor consumers have framework-managed threads; Crossbeam requires manual spawning
- Disruptor `batch_publish` writes multiple slots at once; Crossbeam sends one at a time via `try_send`
- These differences reflect different **design philosophies**, not benchmark defects

---

## 7. Regression Detection

### 7.1 Criterion Baseline Comparison

```bash
# Establish a baseline
cargo bench -- --save-baseline main

# Compare against the baseline
cargo bench -- --baseline main
```

### 7.2 Threshold Settings

Recommended regression alert thresholds:

| Metric | Alert Threshold | Blocking Threshold |
|------|----------|----------|
| Throughput decrease | > 5% | > 10% |
| p50 latency increase | > 10% | > 20% |
| p99 latency increase | > 15% | > 30% |

### 7.3 CI Integration

```yaml
# Can be added in .github/workflows/build_and_test.yml
- name: Run benchmarks
  run: cargo bench -- --output-format bencher | tee bench_output.txt
```

---

## 8. Reproduction Guide

```bash
# 1. Lock CPU frequency
echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor

# 2. Run benchmarks (release mode is Criterion's default)
cargo bench

# 3. View HTML report
open target/criterion/report/index.html

# 4. Compare two runs
cargo bench -- --save-baseline before
# (modify code)
cargo bench -- --baseline before
```
