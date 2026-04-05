# ADR-0002: Lock Nightly Toolchain and Use Feature-Gated Unstable Features

> **Key Takeaway**: disruptor-rs locks the nightly Rust toolchain and places unstable features such as `portable_simd`, `allocator_api`, and `hint::prefetch` behind Cargo feature flags, enabling them only when benchmarks demonstrate significant gains.

## Status

Accepted

## Date

2026-04-03

## Context

disruptor-rs is positioned as an **ultra-low-latency** inter-thread communication library. The current `rust-toolchain.toml` already specifies nightly:

```toml
[toolchain]
channel = "nightly"
components = ["clippy", "rustfmt", "rust-src", "rust-analyzer", "miri"]
targets = ["x86_64-unknown-linux-gnu"]
```

The following nightly features have potential gains on performance-critical paths:

| Feature | Stabilization Status | Expected Gain | Application Location |
|---|---|---|---|
| `portable_simd` | Nightly (tracking #86656) | Batch availability bit checking | `MultiProducerBarrier::get_after()` |
| `allocator_api` | Nightly (tracking #32838) | Custom allocator for page alignment | `RingBuffer` slot allocation |
| `core::hint::prefetch_*` | Nightly (tracking #117020) | Prefetch next slot to L1 cache | `consumer.rs` main loop |
| `asm!` (inline assembly) | **Stable since 1.59** | `rdtsc` precise timing | Benchmark |

There are also competing forces:

- **User base**: Many Rust users use the stable toolchain; a nightly requirement reduces potential users
- **Correctness tooling**: Miri is a nightly-only component and is critical for unsafe code verification
- **Competing libraries**: Crossbeam works on stable and is the default choice when users evaluate options

## Decision

### 1. Nightly is Required, but Unstable Features are Optional

```toml
# Cargo.toml
[features]
default = []
simd     = []  # Enable portable_simd optimizations
prefetch = []  # Enable cache prefetch hints
alloc    = []  # Enable custom allocator
```

Gated in code via conditional compilation:

```rust
// producer/multi.rs
#[cfg(feature = "simd")]
fn check_availability_simd(&self, from: Sequence, count: usize) -> Sequence {
    use std::simd::*;
    // SIMD-accelerated availability check
    // ...
}

#[cfg(not(feature = "simd"))]
fn check_availability_scalar(&self, from: Sequence, count: usize) -> Sequence {
    // Current scalar implementation
    // ...
}
```

### 2. Feature Enablement Threshold

Each feature must meet the following criteria before being added:

1. **Benchmark evidence**: A/B comparison data exists in `benches/`
2. **Minimal impact scope**: No behavior changes when the feature is disabled
3. **Miri compatible**: Must still pass Miri detection with the feature enabled
4. **Documentation**: Expected gains and applicable scenarios are documented

### 3. Stabilization Tracking

Maintain a tracking table; remove the feature gate once a feature is stabilized:

| Feature | Tracking Issue | Target Stable Version | Action After Stabilization |
|---|---|---|---|
| `portable_simd` | rust#86656 | TBD | Remove `simd` feature, enable by default |
| `allocator_api` | rust#32838 | TBD | Remove `alloc` feature, enable by default |
| `hint::prefetch` | rust#117020 | TBD | Remove `prefetch` feature, enable by default |

## Consequences

### Positive Consequences

- **Performance**: Access to the compiler's latest optimizations and hardware instructions
- **Miri available**: Nightly ensures Miri is always available for unsafe verification
- **Gradual adoption**: Feature flags let users selectively enable optimizations
- **Forward-looking**: When features are stabilized, the library is already prepared to leverage them

### Negative Consequences

- **Excludes stable users**: Projects using the stable toolchain cannot depend on disruptor-rs
- **Nightly breakage**: Nightly occasionally introduces breaking changes that need prompt fixes
- **CI complexity**: Need to pin nightly dates or accept occasional CI failures
- **Ecosystem friction**: Nightly-only crates on `crates.io` are viewed as immature by some users

### Neutral Consequences

- `rust-toolchain.toml` ensures all contributors use the same nightly version
- Feature flags increase the configuration matrix (CI needs to test default and various feature combinations)

## Alternatives Considered

| Alternative | Description | Reason for Rejection |
|---|---|---|
| Stable only | Do not use any nightly features | Gives up Miri (a critical tool for unsafe verification) and potential performance optimizations |
| Nightly without feature gates | Always enable all unstable features | Users cannot selectively disable; Miri may not support certain features |
| Dual version support | Support both stable and nightly simultaneously | High maintenance cost; conditional compilation logic is complex |
| MSRV policy | Pin a minimum supported version (e.g., 1.75) | Conflicts with Miri requirements; cannot use any unstable features |

## References

- Rust feature flags: https://doc.rust-lang.org/cargo/reference/features.html
- `portable_simd` tracking: https://github.com/rust-lang/rust/issues/86656
- `allocator_api` tracking: https://github.com/rust-lang/rust/issues/32838
