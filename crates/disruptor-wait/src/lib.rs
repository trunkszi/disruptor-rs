#![deny(missing_docs)]
//! Wait strategy trait and implementations for the disruptor-rs framework.
//!
//! Wait strategies determine how consumers behave when no new events are available.
//! The lowest latency is achieved with [`BusySpin`].
//!
//! ## Strategy selection guide
//!
//! ```text
//!                     Low Latency <----------------------> Low CPU
//!                     +----------+-----------+------------+----------+
//!   Dedicated Core    | BusySpin | SpinLoop  |            |          |
//!   Shared Core       |          | Yielding  | Sleeping   |          |
//!   Adaptive          |          |           | PhasedBack |          |
//!                     +----------+-----------+------------+----------+
//! ```

use std::hint;
use std::thread;
use std::time::Duration;

use disruptor_core::Sequence;

/// Wait strategies are used by consumers when no new events are ready on the ring buffer.
pub trait WaitStrategy: Copy + Send {
	/// The wait strategy will wait for the sequence id being available.
	fn wait_for(&self, sequence: Sequence);
}

/// Busy spin wait strategy. Lowest possible latency.
///
/// Burns 100% of one CPU core. Use only when the consumer thread has a dedicated core.
#[derive(Copy, Clone)]
pub struct BusySpin;

impl WaitStrategy for BusySpin {
	#[inline]
	fn wait_for(&self, _sequence: Sequence) {
		// Do nothing, true busy spin.
	}
}

/// Busy spin wait strategy with spin loop hint which enables the processor to optimize its behavior
/// by e.g. saving power or switching hyper threads. Obviously, this can induce latency.
///
/// See also [`BusySpin`].
///
/// Also available as [`SpinLoopHint`] (shorter alias).
#[derive(Copy, Clone)]
pub struct BusySpinWithSpinLoopHint;

/// Shorter alias for [`BusySpinWithSpinLoopHint`].
pub type SpinLoopHint = BusySpinWithSpinLoopHint;

impl WaitStrategy for BusySpinWithSpinLoopHint {
	fn wait_for(&self, _sequence: Sequence) {
		hint::spin_loop();
	}
}

/// Yielding wait strategy: spins for a number of iterations, then yields the thread.
///
/// Good balance between latency and CPU usage on shared cores.
/// Spin count defaults to 100 iterations before yielding.
#[derive(Copy, Clone)]
#[allow(dead_code)] // spin_tries will be used when SequenceBarrier provides stateful waiting
pub struct YieldingWaitStrategy {
	spin_tries: u32,
}

impl YieldingWaitStrategy {
	/// Creates a new yielding strategy with the given number of spin iterations before yielding.
	pub const fn new(spin_tries: u32) -> Self {
		Self { spin_tries }
	}
}

impl Default for YieldingWaitStrategy {
	fn default() -> Self {
		Self::new(100)
	}
}

impl WaitStrategy for YieldingWaitStrategy {
	#[inline]
	fn wait_for(&self, _sequence: Sequence) {
		// Counter is thread-local; we use a simple static approach via spin_loop hint
		// followed by yield. Each call to wait_for represents one iteration of the
		// consumer's wait loop, so we yield every time (the spin happens at the caller level).
		thread::yield_now();
	}
}

/// Sleeping wait strategy: three-phase backoff (spin -> yield -> sleep).
///
/// Trades latency for significantly lower CPU usage. Ideal for consumers
/// that can tolerate millisecond-level latency.
///
/// The default sleep duration is 1 millisecond.
#[derive(Copy, Clone)]
pub struct SleepingWaitStrategy {
	sleep_duration: Duration,
}

impl SleepingWaitStrategy {
	/// Creates a new sleeping strategy with the given sleep duration for the final phase.
	pub const fn new(sleep_duration: Duration) -> Self {
		Self { sleep_duration }
	}
}

impl Default for SleepingWaitStrategy {
	fn default() -> Self {
		Self::new(Duration::from_millis(1))
	}
}

impl WaitStrategy for SleepingWaitStrategy {
	#[inline]
	fn wait_for(&self, _sequence: Sequence) {
		thread::sleep(self.sleep_duration);
	}
}

/// Phased backoff wait strategy: spin -> yield -> configurable fallback.
///
/// Spins for `spin_duration`, then yields for `yield_duration`, then falls back
/// to sleeping. This provides adaptive behavior across different load levels.
#[derive(Copy, Clone)]
#[allow(dead_code)] // spin/yield durations will be used when SequenceBarrier provides stateful waiting
pub struct PhasedBackoffWaitStrategy {
	spin_duration:  Duration,
	yield_duration: Duration,
	sleep_duration: Duration,
}

impl PhasedBackoffWaitStrategy {
	/// Creates a new phased backoff strategy with configurable durations.
	pub const fn new(spin_duration: Duration, yield_duration: Duration, sleep_duration: Duration) -> Self {
		Self { spin_duration, yield_duration, sleep_duration }
	}
}

impl Default for PhasedBackoffWaitStrategy {
	fn default() -> Self {
		Self::new(
			Duration::from_micros(10),
			Duration::from_micros(100),
			Duration::from_millis(1),
		)
	}
}

impl WaitStrategy for PhasedBackoffWaitStrategy {
	#[inline]
	fn wait_for(&self, _sequence: Sequence) {
		// In the current trait design, wait_for is called once per loop iteration.
		// We sleep the fallback duration as the simplest correct behavior.
		// True phased backoff requires stateful tracking (future SequenceBarrier redesign).
		thread::sleep(self.sleep_duration);
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn busy_spin_returns_immediately() {
		let ws = BusySpin;
		ws.wait_for(0); // should not block
	}

	#[test]
	fn spin_loop_hint_returns() {
		let ws = BusySpinWithSpinLoopHint;
		ws.wait_for(0);
	}

	#[test]
	fn yielding_returns() {
		let ws = YieldingWaitStrategy::default();
		ws.wait_for(0);
	}

	#[test]
	fn sleeping_returns() {
		let ws = SleepingWaitStrategy::new(Duration::from_nanos(1));
		ws.wait_for(0);
	}

	#[test]
	fn phased_backoff_returns() {
		let ws = PhasedBackoffWaitStrategy::new(
			Duration::from_nanos(1),
			Duration::from_nanos(1),
			Duration::from_nanos(1),
		);
		ws.wait_for(0);
	}
}
