use std::sync::atomic::{fence, Ordering};
use disruptor_core::{Barrier, Sequence};
use crate::{ProducerBarrier, SingleProducerBarrier, MultiProducerBarrier};

/// Error indicating that the ring buffer has insufficient capacity for the requested claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsufficientCapacity;

impl std::fmt::Display for InsufficientCapacity {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "Insufficient capacity in ring buffer")
	}
}

impl std::error::Error for InsufficientCapacity {}

/// Sequencer coordinates claiming and publishing sequences to the RingBuffer.
///
/// This is the core abstraction that separates single-producer and multi-producer
/// strategies. A Sequencer owns the cursor (highest published sequence) and
/// controls how sequences are claimed by producers and made visible to consumers.
pub trait Sequencer: Send + Sync {
	/// Claim the next `n` sequences for publishing. Spins until space is available.
	/// Returns the highest claimed sequence number.
	fn claim(&self, n: usize) -> Sequence;

	/// Try to claim the next `n` sequences without blocking.
	/// Returns `Err(InsufficientCapacity)` if the RingBuffer does not have enough free slots.
	fn try_claim(&self, n: usize) -> Result<Sequence, InsufficientCapacity>;

	/// Publish the given sequence, making it visible to consumers.
	fn publish(&self, sequence: Sequence);

	/// Publish a range of sequences `[lo, hi]` inclusive.
	fn publish_range(&self, lo: Sequence, hi: Sequence);

	/// Check if a sequence has been published and is available for reading.
	fn is_available(&self, sequence: Sequence) -> bool;

	/// Get the highest contiguously published sequence in the range starting from `lo`.
	/// For SingleSequencer this is always the cursor.
	/// For MultiSequencer, sequences may be published out of order.
	fn highest_published(&self, lo: Sequence, available: Sequence) -> Sequence;

	/// Get the current cursor value (highest claimed sequence for single, highest claimed for multi).
	fn cursor(&self) -> Sequence;

	/// Get the remaining capacity of the RingBuffer.
	fn remaining_capacity(&self) -> usize;
}

/// Single-threaded sequencer. No CAS contention, lowest overhead.
///
/// Only one thread may call `claim`/`try_claim` at a time.
pub struct SingleSequencer<C> {
	barrier:          SingleProducerBarrier,
	consumer_barrier: C,
	buffer_size:      i64,
	/// Next sequence to be claimed (not thread-safe — single producer only).
	/// Uses interior mutability via UnsafeCell since only one thread claims.
	next_sequence:    std::cell::UnsafeCell<i64>,
	/// Cache of the last known clear-of-consumers position.
	cached_clear:     std::cell::UnsafeCell<i64>,
}

// SAFETY: SingleSequencer is designed for single-producer use. The UnsafeCell fields
// (next_sequence, cached_clear) are only accessed by the single producer thread.
// The barrier and consumer_barrier fields are inherently Send + Sync.
unsafe impl<C: Send + Sync> Send for SingleSequencer<C> {}
unsafe impl<C: Send + Sync> Sync for SingleSequencer<C> {}

impl<C> SingleSequencer<C>
where
	C: Barrier,
{
	/// Creates a new single sequencer for a ring buffer of the given `size`.
	pub fn new(size: i64, consumer_barrier: C) -> Self {
		Self {
			barrier: SingleProducerBarrier::new(),
			consumer_barrier,
			buffer_size: size,
			next_sequence: std::cell::UnsafeCell::new(0),
			cached_clear: std::cell::UnsafeCell::new(size - 1),
		}
	}
}

impl<C> Sequencer for SingleSequencer<C>
where
	C: Barrier,
{
	fn claim(&self, n: usize) -> Sequence {
		loop {
			match self.try_claim(n) {
				Ok(seq) => return seq,
				Err(_) => std::hint::spin_loop(),
			}
		}
	}

	fn try_claim(&self, n: usize) -> Result<Sequence, InsufficientCapacity> {
		let n = n as i64;
		// SAFETY: Only one producer thread calls this.
		let next = unsafe { *self.next_sequence.get() };
		let end = next + n - 1;

		let cached = unsafe { *self.cached_clear.get() };
		if cached < end {
			let last_published = next - 1;
			let rear = self.consumer_barrier.get_after(last_published);
			let wrap_point = end - self.buffer_size;
			if rear < wrap_point {
				return Err(InsufficientCapacity);
			}
			fence(Ordering::Acquire);
			unsafe { *self.cached_clear.get() = rear.saturating_add(self.buffer_size) };
		}

		unsafe { *self.next_sequence.get() = next + n };
		Ok(end)
	}

	fn publish(&self, sequence: Sequence) {
		self.barrier.publish(sequence);
	}

	fn publish_range(&self, lo: Sequence, hi: Sequence) {
		// For single producer, publishing hi is sufficient since sequences are contiguous.
		self.barrier.publish(hi);
		let _ = lo;
	}

	fn is_available(&self, sequence: Sequence) -> bool {
		sequence <= self.barrier.get_after(sequence)
	}

	fn highest_published(&self, _lo: Sequence, _available: Sequence) -> Sequence {
		self.barrier.get_after(0)
	}

	fn cursor(&self) -> Sequence {
		self.barrier.get_after(0)
	}

	fn remaining_capacity(&self) -> usize {
		let next = unsafe { *self.next_sequence.get() };
		let consumed = self.consumer_barrier.get_after(next.saturating_sub(1));
		// produced = next - 1, in_flight = produced - consumed = next - 1 - consumed
		let produced = next.saturating_sub(1);
		let in_flight = produced.saturating_sub(consumed).max(0);
		(self.buffer_size - in_flight).max(0) as usize
	}
}

/// Multi-threaded sequencer. Uses CAS for contention-free claiming across threads.
pub struct MultiSequencer<C> {
	barrier:          MultiProducerBarrier,
	consumer_barrier: C,
	buffer_size:      i64,
}

impl<C> MultiSequencer<C>
where
	C: Barrier,
{
	/// Creates a new multi sequencer for a ring buffer of the given `size`.
	///
	/// # Panics
	///
	/// Panics if `size < 64`.
	pub fn new(size: usize, consumer_barrier: C) -> Self {
		Self {
			barrier: MultiProducerBarrier::new(size),
			consumer_barrier,
			buffer_size: size as i64,
		}
	}
}

impl<C> Sequencer for MultiSequencer<C>
where
	C: Barrier,
{
	fn claim(&self, n: usize) -> Sequence {
		loop {
			match self.try_claim(n) {
				Ok(seq) => return seq,
				Err(_) => std::hint::spin_loop(),
			}
		}
	}

	fn try_claim(&self, n: usize) -> Result<Sequence, InsufficientCapacity> {
		let n_i64 = n as i64;
		let mut current = self.barrier.current();
		let mut next = current.checked_add(n_i64)
			.expect("Sequence overflow: claimed > i64::MAX sequences");

		loop {
			// Check if consumers are far enough behind.
			let rear = self.consumer_barrier.get_after(current);
			let wrap_point = next - self.buffer_size;
			if rear < wrap_point {
				return Err(InsufficientCapacity);
			}
			fence(Ordering::Acquire);

			match self.barrier.compare_exchange(current, next) {
				Ok(_) => return Ok(next),
				Err(new_current) => {
					current = new_current;
					next = current.checked_add(n_i64)
						.expect("Sequence overflow: claimed > i64::MAX sequences");
				}
			}
		}
	}

	fn publish(&self, sequence: Sequence) {
		ProducerBarrier::publish(&self.barrier, sequence);
	}

	fn publish_range(&self, lo: Sequence, hi: Sequence) {
		let n = hi - lo + 1;
		fence(Ordering::Release);
		self.barrier.publish_range_relaxed(lo, n);
	}

	fn is_available(&self, sequence: Sequence) -> bool {
		// Use get_after to check if the sequence is published.
		self.barrier.get_after(sequence) >= sequence
	}

	fn highest_published(&self, lo: Sequence, _available: Sequence) -> Sequence {
		// For multi-producer, get_after scans the availability bitmap from `lo`.
		self.barrier.get_after(lo)
	}

	fn cursor(&self) -> Sequence {
		self.barrier.current()
	}

	fn remaining_capacity(&self) -> usize {
		let current = self.barrier.current();
		let consumed = self.consumer_barrier.get_after(current);
		let remaining = self.buffer_size - (current - consumed);
		remaining.max(0) as usize
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use disruptor_core::Cursor;
	use std::sync::Arc;

	/// Simple barrier that always reports all sequences as consumed.
	struct AlwaysAvailableBarrier;
	impl Barrier for AlwaysAvailableBarrier {
		fn get_after(&self, _prev: Sequence) -> Sequence {
			i64::MAX
		}
	}

	/// Barrier that tracks a fixed consumed position.
	struct FixedBarrier {
		cursor: Arc<Cursor>,
	}
	impl Barrier for FixedBarrier {
		fn get_after(&self, _prev: Sequence) -> Sequence {
			self.cursor.relaxed_value()
		}
	}

	#[test]
	fn single_sequencer_claim_and_publish() {
		let seq = SingleSequencer::new(8, AlwaysAvailableBarrier);

		// Claim 1 sequence.
		let s = seq.claim(1);
		assert_eq!(s, 0);
		seq.publish(s);
		assert_eq!(seq.cursor(), 0);

		// Claim 3 more.
		let s = seq.claim(3);
		assert_eq!(s, 3); // sequences 1, 2, 3 claimed, highest is 3
		seq.publish_range(1, s);
		assert_eq!(seq.cursor(), 3);
	}

	#[test]
	fn single_sequencer_try_claim_full() {
		let cursor = Arc::new(Cursor::new()); // starts at -1
		let barrier = FixedBarrier { cursor: Arc::clone(&cursor) };
		let seq = SingleSequencer::new(4, barrier);

		// Claim 4 sequences (fills buffer).
		let s = seq.try_claim(4);
		assert!(s.is_ok());
		assert_eq!(s.unwrap(), 3);
		seq.publish_range(0, 3);

		// Buffer is full, try_claim should fail.
		let result = seq.try_claim(1);
		assert_eq!(result, Err(InsufficientCapacity));

		// Consumer advances — now space is available.
		cursor.store(1);
		let s = seq.try_claim(1);
		assert!(s.is_ok());
	}

	#[test]
	fn multi_sequencer_claim_and_publish() {
		let seq = MultiSequencer::new(64, AlwaysAvailableBarrier);

		let s = seq.claim(1);
		assert_eq!(s, 0);
		seq.publish(s);

		let s = seq.claim(5);
		assert_eq!(s, 5); // claimed 1..=5, highest is 5
		seq.publish_range(1, s);
	}

	#[test]
	fn multi_sequencer_try_claim_full() {
		let cursor = Arc::new(Cursor::new());
		let barrier = FixedBarrier { cursor: Arc::clone(&cursor) };
		let seq = MultiSequencer::new(64, barrier);

		// Claim 64 sequences.
		let s = seq.try_claim(64);
		assert!(s.is_ok());
		assert_eq!(s.unwrap(), 63);

		// Buffer full.
		let result = seq.try_claim(1);
		assert_eq!(result, Err(InsufficientCapacity));

		// Consumer advances.
		cursor.store(0);
		let s = seq.try_claim(1);
		assert!(s.is_ok());
	}

	#[test]
	fn single_sequencer_remaining_capacity() {
		let seq = SingleSequencer::new(8, AlwaysAvailableBarrier);
		assert_eq!(seq.remaining_capacity(), 8);

		seq.claim(3);
		// With AlwaysAvailable consumer, remaining is still full effective capacity.
		assert!(seq.remaining_capacity() > 0);
	}
}
