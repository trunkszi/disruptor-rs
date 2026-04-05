# Contributing to Disruptor-RS

Thank you for your interest in Disruptor-RS! Below are the contribution guidelines.

## Setting Up the Development Environment

```bash
# 1. Clone the repository
git clone https://github.com/trunkszi/disruptor-rs.git
cd disruptor-rs

# 2. Automatic toolchain installation (managed by rust-toolchain.toml)
rustup show  # Confirm nightly + components are ready

# 3. Verify the build
RUSTC_WRAPPER="" cargo build
RUSTC_WRAPPER="" cargo test
```

### Prerequisites

- Rust nightly (pinned by `rust-toolchain.toml`)
- Components: `clippy`, `rustfmt`, `rust-src`, `rust-analyzer`, `miri`

## Code Style

### Rust Code

- **Formatting**: `cargo fmt` (follows `rustfmt.toml`)
- **Lint**: `cargo clippy -- -D warnings`
- **unsafe rules**: every `unsafe` block must have a `// SAFETY:` comment
  ```rust
  // SAFETY: index is computed by sequence & index_mask,
  // index_mask = size - 1, size is a power of 2,
  // so index is always in the range [0, size).
  unsafe { self.slots.get_unchecked(index) }
  ```

### Documentation

- Follow the [Google Developer Documentation Style Guide](https://developers.google.com/style)
- Use the pyramid principle: lead with the conclusion
- Use Mermaid for diagrams, LaTeX for formulas

## Commit Conventions

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add Yielding wait strategy
fix: prevent sequence overflow UB
refactor: extract Sequencer trait from Producer
docs: add ring buffer design document
test: add loom tests for MultiSequencer
perf: optimize availability bitmap with SIMD
ci: add Miri to CI pipeline
```

## Pull Request Process

1. **Create a branch**: `feat/phase-N-task-description`
2. **Write code**: follow the code style guidelines above
3. **Add tests**: new features must have corresponding tests
4. **Run checks**:
   ```bash
   RUSTC_WRAPPER="" cargo fmt --check
   RUSTC_WRAPPER="" cargo clippy -- -D warnings
   RUSTC_WRAPPER="" cargo test
   RUSTC_WRAPPER="" cargo +nightly miri test
   ```
5. **Submit PR**: describe the reason for the change and its scope of impact
6. **unsafe review**: changes involving unsafe code require review by two people

## Performance Testing

When modifying performance-critical paths, please run benchmarks and include the results:

```bash
RUSTC_WRAPPER="" cargo bench --bench spsc
RUSTC_WRAPPER="" cargo bench --bench mpsc
```

Use `criterion-compare` to compare before and after the PR:

```bash
# Baseline
git stash && RUSTC_WRAPPER="" cargo bench --bench spsc -- --save-baseline before
# PR changes
git stash pop && RUSTC_WRAPPER="" cargo bench --bench spsc -- --save-baseline after --baseline before
```

Regression threshold: **> 5% degradation requires an explanation**.

## Project Structure

```
disruptor-rs/
├── src/            # Source code (currently a single crate)
├── tests/          # Integration tests
├── benches/        # Criterion benchmarks
├── benchs/         # Extended benchmarks
├── examples/       # 14 usage examples
├── docs/           # Documentation (45 files)
│   ├── architecture/   # C4 architecture + ADR
│   ├── design/         # Technical design
│   ├── guide/          # User guide
│   ├── reference/      # API reference
│   ├── appendix/       # Appendix
│   └── verification/   # Verification
├── verification/   # TLA+ formal models
└── fuzz/           # Fuzz testing (planned)
```

## Reporting Issues

- Bugs: create an Issue with a minimal reproduction
- Performance: include benchmark results and environment details
- Security: for unsafe/soundness issues, apply the `security` label

## Code of Conduct

Please maintain friendly, professional, and constructive communication.
