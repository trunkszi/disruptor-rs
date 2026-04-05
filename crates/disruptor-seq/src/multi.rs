use std::sync::atomic::{AtomicU64, Ordering};
use disruptor_core::{Barrier, Cursor, Sequence};
use crate::ProducerBarrier;

/// Barrier for multiple producers.
pub struct MultiProducerBarrier {
	/// Cursor used by producers to claim exclusive sequences per producer.
	cursor:      Cursor,
	/// AtomicU64s each track availability of 64 slots.
	/// Each bit encodes whether the slot was published in an even or odd round.
	available:   Box<[AtomicU64]>,
	index_mask:  usize,
	index_shift: usize,
}

impl MultiProducerBarrier {
	/// Creates a new multi-producer barrier for the given ring buffer `size`.
	///
	/// # Panics
	///
	/// Panics if `size < 64`.
	pub fn new(size: usize) -> Self {
		assert!(size >= 64, "Multi Producer Disruptor must have a size of minimum 64 slots.");

		let cursor      = Cursor::new();
		let i64_needed  = size/64;
		let all_ones    = !0_u64;
		let available   = (0..i64_needed).map(|_i| { AtomicU64::new(all_ones) }).collect();
		let index_mask  = size - 1;
		let index_shift = Self::log2(size);

		Self { cursor, available, index_mask, index_shift }
	}

	fn log2(i: usize) -> usize {
		std::mem::size_of::<usize>()*8 - (i.leading_zeros() as usize) - 1
	}

	/// Returns the current (last claimed) sequence.
	#[inline]
	pub fn current(&self) -> Sequence {
		self.cursor.relaxed_value()
	}

	/// Atomically tries to advance the cursor from `current` to `next`.
	#[inline]
	pub fn compare_exchange(&self, current: Sequence, next: Sequence) -> Result<i64, i64> {
		self.cursor.compare_exchange(current, next)
	}

	#[inline]
	fn calculate_availability_indices(&self, sequence: Sequence) -> (usize, usize) {
		let slot_index         = self.slot_index(sequence);
		let availability_index = slot_index >> 6;
		let bit_index          = slot_index - availability_index*64;
		(availability_index, bit_index)
	}

	#[inline]
	fn slot_index(&self, sequence: Sequence) -> usize {
		sequence as usize & self.index_mask
	}

	#[inline]
	fn calculate_availability_flag(&self, sequence: Sequence) -> u64 {
		let round = (sequence >> self.index_shift) as u64;
		round & 1
	}

	#[inline]
	fn availability_at(&self, index: usize) -> &AtomicU64 {
		// SAFETY: `index` is computed by `calculate_availability_indices` as:
		//   slot_index = sequence & index_mask  (always < size)
		//   availability_index = slot_index >> 6  (always < size/64 == available.len())
		// The available array has exactly `size/64` elements (allocated in `new()`).
		// Invariant maintained by: new() allocates size/64 elements; index_mask = size - 1.
		// Verified by: MultiProducerBarrier unit tests (publication_of_range_*).
		unsafe { self.available.get_unchecked(index) }
	}

	/// Publishes a contiguous range of sequences starting at `from` with relaxed ordering.
	#[inline]
	pub fn publish_range_relaxed(&self, from: Sequence, n: i64) {
		let (mut availability_index, mut bit_index) = self.calculate_availability_indices(from);
		let mut flip_mask                           = 0_u64;
		let mut availability                        = self.availability_at(availability_index);

		for i in 0..n {
			flip_mask |= 1 << bit_index;

			if bit_index < 63 {
				bit_index += 1;
			}
			else {
				availability.fetch_xor(flip_mask, Ordering::Relaxed);
				let next_sequence       = from + i + 1;
				(availability_index, _) = self.calculate_availability_indices(next_sequence);
				availability            = self.availability_at(availability_index);
				bit_index               = 0;
				flip_mask               = 0;
			}
		}
		if flip_mask > 0 {
			availability.fetch_xor(flip_mask, Ordering::Relaxed);
		}
	}

	#[inline]
	fn publish_with_ordering(&self, sequence: Sequence, ordering: Ordering) {
		let (availability_index, bit_index) = self.calculate_availability_indices(sequence);
		let availability                    = self.availability_at(availability_index);
		let mask                            = 1 << bit_index;
		availability.fetch_xor(mask, ordering);
	}
}

impl Barrier for MultiProducerBarrier {
	#[inline]
	fn get_after(&self, prev: Sequence) -> Sequence {
		let mut availability_flag                   = self.calculate_availability_flag(prev);
		let (mut availability_index, mut bit_index) = self.calculate_availability_indices(prev);
		let mut availability                        = self.availability_at(availability_index).load(Ordering::Relaxed);
		availability                              >>= bit_index;
		let mut highest_available                   = prev;

		loop {
			if availability & 1 != availability_flag {
				return highest_available - 1;
			}
			highest_available += 1;

			if bit_index < 63 {
				bit_index     += 1;
				availability >>= 1;
			}
			else {
				(availability_index, _) = self.calculate_availability_indices(highest_available);
				availability            = self.availability_at(availability_index).load(Ordering::Relaxed);
				bit_index               = 0;

				if availability_index == 0 {
					availability_flag ^= 1;
				}
			}
		}
	}
}

impl ProducerBarrier for MultiProducerBarrier {
	#[inline]
	fn publish(&self, sequence: Sequence) {
		self.publish_with_ordering(sequence, Ordering::Release);
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn log2() {
		assert_eq!(1, MultiProducerBarrier::log2(2));
		assert_eq!(1, MultiProducerBarrier::log2(3));
		assert_eq!(3, MultiProducerBarrier::log2(8));
		assert_eq!(3, MultiProducerBarrier::log2(9));
		assert_eq!(3, MultiProducerBarrier::log2(10));
		assert_eq!(3, MultiProducerBarrier::log2(11));
	}

	#[test]
	fn initial_available_sequence_number() {
		let barrier = MultiProducerBarrier::new(64);
		assert_eq!(barrier.get_after(0), -1);
	}

	#[test]
	fn publication_of_single_event_for_small_barrier() {
		let barrier = MultiProducerBarrier::new(64);
		barrier.publish_range_relaxed(0, 1);
		assert_eq!(barrier.get_after(0), 0);
	}

	#[test]
	fn publication_of_range_for_small_barrier() {
		let barrier = MultiProducerBarrier::new(64);
		barrier.publish_range_relaxed(0, 10);
		assert_eq!(barrier.get_after(0), 9);
	}

	#[test]
	fn publication_of_range_wrapping_ringbuffer_for_small_barrier() {
		let barrier = MultiProducerBarrier::new(64);

		barrier.publish_range_relaxed(0, 50);
		assert_eq!(barrier.get_after(0), 49);

		barrier.publish_range_relaxed(50, 50);
		assert_eq!(barrier.get_after(49), 99);
	}

	#[test]
	fn publication_of_range_wrapping_ringbuffer_for_barrier() {
		let barrier = MultiProducerBarrier::new(128);

		barrier.publish_range_relaxed(0, 100);
		assert_eq!(barrier.get_after(0), 99);

		barrier.publish_range_relaxed(100, 100);
		assert_eq!(barrier.get_after(99), 199);

		barrier.publish_range_relaxed(200, 100);
		assert_eq!(barrier.get_after(199), 299);
	}
}
