# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Workspace split into 5 focused crates: `disruptor-core`, `disruptor-wait`,
  `disruptor-seq`, `disruptor-consumer`, `disruptor-dsl`
- `EventHandler` trait with lifecycle hooks (`on_start`, `on_shutdown`)
- `ExceptionHandler` trait for consumer panic recovery
- `BatchEventProcessor` for managed event processing with exception handling
- `WorkerPool` for distributing events across parallel worker threads
- `WorkHandler` trait for pool-based event processing
- 5 wait strategies: `BusySpin`, `BusySpinWithSpinLoopHint`, `YieldingWaitStrategy`,
  `SleepingWaitStrategy`, `PhasedBackoffWaitStrategy`
- `LifecycleAware` trait for thread lifecycle notifications
- `try_publish` / `try_batch_publish` for non-blocking publication with backpressure
- Out-of-band `Branch` API for diamond/DAG topologies
- Cross-platform CI (x86_64, aarch64, riscv64, WASM)
- TLA+ formal verification specs
- `no_std` support via `disruptor-core` `alloc` feature
- `LICENSE-MIT` file
- `CODE_OF_CONDUCT.md`, `SECURITY.md`
- GitHub issue/PR templates and dependabot configuration
- Release automation workflow
- MSRV CI verification (Rust 1.85)
- Multi-toolchain CI matrix (stable, beta, nightly)

### Fixed
- **[Critical]** `RingBuffer<E>` `Sync` impl now requires `E: Send + Sync`
  to prevent unsound usage with non-thread-safe types like `Rc<T>`
- **[Critical]** `SingleProducer` advances sequence before invoking user
  update closure, preventing data race on panic recovery
- **[Critical]** `MultiProducer::shutdown` no longer double-decrements
  the producer counter (which caused premature consumer shutdown)
- **[High]** `WorkerPool` uses RAII cursor guard to prevent pipeline
  deadlock when a worker handler panics; cursor advances to `sequence - 1`
  to preserve DAG dependency semantics
- **[High]** `BatchEventProcessor` now resumes panics via
  `resume_unwind` instead of silently swallowing them; cursor stores
  `next_sequence - 1` so downstream sees only fully-processed events

### Removed
- **[High]** Removed `RingBufferFull` and `MissingFreeSlots` type aliases
  from `disruptor-dsl` -- in Rust, type aliases to enums cannot be used as
  match patterns, causing silent logic bugs. Use `PublishError::Full` and
  `PublishError::InsufficientCapacity` directly.

### Changed
- Minimum supported Rust version (MSRV) is now 1.85 (Rust 2024 edition)
- Public enums (`PublishError`, `Polling`, `ExceptionAction`) are now
  `#[non_exhaustive]` for future extensibility
- `.cargo/config.toml` no longer requires nightly-only `-Z` flags;
  library users can compile with stable Rust 1.85+
- `WorkerPool::drain_and_halt` now returns `Result<(), ConsumerPanic>`
  to propagate worker panics, consistent with producer shutdown behavior
- All 6 crates enforce `#![deny(missing_docs)]`
- All crates include `[package.metadata.docs.rs]` configuration
- Workspace dependencies include version fields for crates.io publishing
