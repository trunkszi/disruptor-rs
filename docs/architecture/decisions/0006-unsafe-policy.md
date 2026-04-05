# ADR-0006: Unsafe Code Policy

> **Key Takeaway**: Every `unsafe` block must have a `// SAFETY:` comment, and Miri is enforced as a mandatory CI gate.

- **Status**: Accepted
- **Date**: 2026-04-03
- **Decision Makers**: Architecture team

---

## Summary

- All `unsafe` blocks must include a `// SAFETY:` comment explaining why the operation is safe
- Miri is a mandatory CI gate with zero UB tolerance
- Periodic unsafe audits, with results recorded in `docs/appendix/unsafe-audit.md`
- New unsafe code requires two-person review

## Context

The current repository contains 8 `unsafe` code locations:

| Location | Operation | Risk Level |
|------|------|----------|
| `ringbuffer.rs:5` | `unsafe impl<E: Send> Sync for RingBuffer<E>` | High |
| `ringbuffer.rs:49` | `get_unchecked(index)` | Medium |
| `consumer.rs:96-97` | `& *event_ptr` dereference | Medium |
| `consumer.rs:142-143` | `& *event_ptr` dereference (stateful variant) | Medium |
| `producer.rs:61` | `&mut *event_ptr` dereference | Medium |
| `producer/single.rs:159` | `&mut *event_ptr` dereference | Medium |
| `producer/multi.rs:202-203` | `&mut *event_ptr` dereference | Medium |
| `producer/multi.rs:355` | `get_unchecked(index)` | Low |

Among these, `RingBuffer`'s `unsafe impl Sync` is the highest risk point -- it relies on the Sequencer protocol to guarantee mutual exclusion, but this is not enforced at compile time. Some existing `unsafe` blocks lack `// SAFETY:` comments.

### Problems

1. **Missing safety documentation**: Some `unsafe` blocks lack `// SAFETY:` comments, making it difficult for maintainers to quickly assess safety preconditions
2. **Implicit dependencies**: `RingBuffer`'s Sync safety depends on an external protocol, but this protocol is not formally expressed
3. **Audit difficulty**: No centralized list of unsafe code, making systematic review impossible

## Decision

### 1. SAFETY Comment Convention

Every `unsafe` block **must** have a `// SAFETY:` comment immediately above it:

```rust
// SAFETY: `index` is computed by `sequence & index_mask`,
// where index_mask = size - 1 and size is a power of 2,
// so index is always in the range [0, size).
unsafe { self.slots.get_unchecked(index) }
```

`unsafe impl` also requires this:

```rust
// SAFETY: Mutual exclusion for RingBuffer is guaranteed by the Sequencer protocol:
// - Producers obtain exclusive write access through claim()
// - Consumers wait via SequenceBarrier until after publication before reading
// - Invariants INV-1 (exclusive write) and INV-2 (publish-before-read) guarantee safety
unsafe impl<E: Send> Sync for RingBuffer<E> {}
```

### 2. Miri CI Gate

```yaml
# .github/workflows/test_coverage.yml
- name: Miri
  run: cargo +nightly miri test --workspace
  # Zero tolerance: any UB detection causes immediate failure
```

### 3. Audit Process

| Frequency | Activity | Output |
|------|------|------|
| Every PR | New/modified unsafe requires two-person review | PR approval |
| Every Phase | Full unsafe audit | Update `unsafe-audit.md` |
| Every release | Miri + audit checklist review | Release checklist |

### 4. Clippy Lint Enforcement

```toml
# clippy.toml or lib.rs
#![deny(unsafe_op_in_unsafe_fn)]
#![warn(clippy::undocumented_unsafe_blocks)]
```

### 5. Minimization Principle

- Prefer safe abstractions (e.g., `AtomicI64` instead of raw pointers)
- `unsafe` is only for operations that cannot be expressed in safe Rust
- Encapsulate unsafe in the smallest possible `unsafe {}` blocks
- Exposing unsafe interfaces in public APIs is prohibited

## Alternatives Considered

| Alternative | Pros | Cons | Decision |
|------|------|------|------|
| All safe Rust (no unsafe) | Zero UB risk | Unacceptable performance, cannot implement core functionality | Rejected |
| Unsafe without required comments | Faster coding | Hard to maintain, hard to audit | Rejected |
| Miri only, no required comments | Runtime detection | Miri does not cover all paths | Rejected |
| **SAFETY comments + Miri + periodic audit** | Defense in depth | Slightly higher maintenance cost | **Adopted** |

## Consequences

### Positive

- Every `unsafe` block has explicit documentation of safety preconditions
- Miri automatically detects undefined behavior
- Audit checklist provides a centralized view
- Clippy lints prevent omissions

### Negative

- Additional effort to write SAFETY comments during coding
- New contributors need to learn the unsafe review process
- Miri adds CI time (approximately 2-5 minutes)

### Risks

- Miri does not cover all platform-specific behavior (e.g., ARM weak memory model)
- SAFETY comments may become outdated -- mitigated through the audit process

## References

- [Rust Unsafe Code Guidelines](https://rust-lang.github.io/unsafe-code-guidelines/)
- [Clippy: undocumented_unsafe_blocks](https://rust-lang.github.io/rust-clippy/master/index.html#undocumented_unsafe_blocks)
- [Miri](https://github.com/rust-lang/miri)
- [docs/appendix/unsafe-audit.md](../../appendix/unsafe-audit.md)
- [docs/verification/safety-invariants.md](../../verification/safety-invariants.md)
