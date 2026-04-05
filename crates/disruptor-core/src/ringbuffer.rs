use std::cell::UnsafeCell;

use crate::Sequence;

// SAFETY: RingBuffer uses UnsafeCell for interior mutability, but concurrent access is
// coordinated by the Disruptor's sequence-based protocol:
//   - Producers only write to slots they have exclusively claimed via sequence allocation.
//   - Consumers only read slots that have been published (Release) and synchronize via Acquire fence.
//   - No two writers or a writer and reader access the same slot simultaneously.
// Invariant maintained by: Sequencer protocol (SingleProducer/MultiProducer claim + publish).
// Verified by: loom tests, Miri, and integration tests.
//
// The `E: Send` bound ensures the element type can be safely transferred across threads
// (producers write on one thread, consumers read on another).
// The `E: Sync` bound ensures shared references to elements are safe across threads
// (multiple consumers may hold `&E` simultaneously in multicast topologies).
unsafe impl<E: Send + Sync> Sync for RingBuffer<E> {}

// SAFETY: A RingBuffer can be sent between threads as long as its elements can be.
// The UnsafeCell is behind the Disruptor's sequence protocol which prevents data races.
unsafe impl<E: Send> Send for RingBuffer<E> {}

/// Pre-allocated ring buffer with power-of-two sizing.
///
/// Uses `UnsafeCell` for interior mutability; safe concurrent access is
/// guaranteed by the Disruptor's sequence coordination protocol.
#[must_use]
pub struct RingBuffer<E> {
	slots:      Box<[UnsafeCell<E>]>,
	index_mask: i64,
}

impl <E> RingBuffer<E> {
	/// Creates a new ring buffer with the given `size` (must be a power of two).
	///
	/// Each slot is initialized by calling `event_factory`.
	///
	/// # Panics
	///
	/// Panics if `size` is not a power of two.
	pub fn new<F>(size: usize, mut event_factory: F)
	-> Self
	where
		F: crate::EventFactory<E>
	{
		if !size.is_power_of_two() { panic!("Size must be power of 2.") }

		let slots: Box<[UnsafeCell<E>]> = (0..size)
			.map(|_i| UnsafeCell::new(event_factory.create()) )
			.collect();
		let index_mask = (size - 1) as i64;

		RingBuffer {
			slots,
			index_mask,
		}
	}

	#[inline]
	fn wrap_point(&self, sequence: Sequence) -> Sequence {
		sequence - self.size()
	}

	/// Returns the number of free slots given the producer and consumer positions.
	#[inline]
	pub fn free_slots(&self, producer: Sequence, highest_read_by_consumers: Sequence) -> i64 {
		let wrap_point = self.wrap_point(producer);
		highest_read_by_consumers - wrap_point
	}

	/// Returns a raw mutable pointer to the event at the given `sequence`.
	///
	/// Callers must ensure that only a single mutable reference or multiple immutable references
	/// exist at any point in time.
	#[inline]
	pub fn get(&self, sequence: Sequence) -> *mut E {
		let index = (sequence & self.index_mask) as usize;
		// SAFETY: `index = sequence & (size - 1)` where size is a power of two,
		// so index is always in [0, size). The mask guarantees this arithmetically.
		// Invariant maintained by: RingBuffer::new() asserts size is power of two.
		// Verified by: ringbuffer::tests::free_slots and all integration tests.
		let slot  = unsafe { self.slots.get_unchecked(index) };
		slot.get()
	}

	/// Returns the size (capacity) of the ring buffer.
	#[inline]
	pub fn size(&self) -> i64 {
		self.slots.len() as i64
	}
}


#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn free_slots() {
		let ring_buffer = RingBuffer::new(8, || { 0 });

		// Round 1:
		assert_eq!(1, ring_buffer.free_slots(7, 0));
		assert_eq!(0, ring_buffer.free_slots(8, 0));
		assert_eq!(8, ring_buffer.free_slots(0, 0));
		assert_eq!(4, ring_buffer.free_slots(3, -1));
		assert_eq!(0, ring_buffer.free_slots(7, -1));
		assert_eq!(1, ring_buffer.free_slots(7, 0));
		assert_eq!(5, ring_buffer.free_slots(5, 2));
		assert_eq!(6, ring_buffer.free_slots(5, 3));
		assert_eq!(7, ring_buffer.free_slots(5, 4));
		assert_eq!(8, ring_buffer.free_slots(5, 5));

		// Round 2:
		assert_eq!(6, ring_buffer.free_slots(11, 9));
		assert_eq!(8, ring_buffer.free_slots(12, 12));
		assert_eq!(0, ring_buffer.free_slots(15, 7));
	}
}
