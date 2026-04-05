use std::{process, sync::{atomic::{fence, AtomicI64, AtomicU64, Ordering}, Arc, Mutex}};
use crossbeam_utils::CachePadded;
use crate::{barrier::{Barrier, NONE}, consumer::Consumer, producer::ProducerBarrier, ringbuffer::RingBuffer, producer::{Producer, RingBufferFull}, Sequence};
use crate::cursor::Cursor;

use super::{MissingFreeSlots, MutBatchIter};

struct SharedProducer {
	consumers: Vec<Consumer>,
	counter:   AtomicI64,
}

/// Producer for publishing to the Disruptor from multiple threads.
///
/// See also [SingleProducer](crate::SingleProducer) for single-threaded publication and
/// [`Producer`] for how to use a Producer.
pub struct MultiProducer<E, C> {
	shutdown_at_sequence:        Arc<CachePadded<AtomicI64>>,
	ring_buffer:                 Arc<RingBuffer<E>>,
	shared_producer:             Arc<Mutex<SharedProducer>>,
	producer_barrier:            Arc<MultiProducerBarrier>,
	consumer_barrier:            Arc<C>,
	/// Next sequence number for this MultiProducer to publish.
	claimed_sequence:            Sequence,
	/// Highest sequence available for publication because the Consumers are "enough" behind
	/// to not interfere.
	sequence_clear_of_consumers: Sequence,
}

impl<E, C> Producer<E> for MultiProducer<E, C>
where
	C: Barrier
{
	#[inline]
	fn try_publish<F>(&mut self, update: F) -> Result<Sequence, RingBufferFull>
	where
		F: FnOnce(&mut E)
	{
		self.next_sequences(1).map_err(|_| RingBufferFull)?;
		let sequence = self.apply_update(update);
		Ok(sequence)
	}

	#[inline]
	fn publish<F>(&mut self, update: F)
	where
		F: FnOnce(&mut E)
	{
		while self.next_sequences(1).is_err() { /* Empty. */ }
		self.apply_update(update);
	}

	#[inline]
	fn try_batch_publish<'a, F>(&'a mut self, n: usize, update: F) -> Result<Sequence, MissingFreeSlots>
	where
		E: 'a,
		F: FnOnce(MutBatchIter<'a, E>),
	{
		self.next_sequences(n)?;
		let sequence = self.apply_updates(n, update);
		Ok(sequence)
	}

	#[inline]
	fn batch_publish<'a, F>(&'a mut self, n: usize, update: F)
	where
		E: 'a,
		F: FnOnce(MutBatchIter<'a, E>)
	{
		while self.next_sequences(n).is_err() { /* Empty. */ }
		self.apply_updates(n, update);
	}
}

impl<E, C> Clone for MultiProducer<E, C> {
	fn clone(&self) -> Self {
		let shared = self.shared_producer.lock().unwrap();
		let count  = shared.counter.fetch_add(1, Ordering::AcqRel);

		// Cloning publishers and calling `mem::forget` on the clones could potentially overflow the
		// counter. It's very difficult to recover sensibly from such degenerate scenarios so we
		// just abort when the count becomes very large.
		if count > i64::MAX/2 {
			process::abort();
		}

		let shutdown_at_sequence = Arc::clone(&self.shutdown_at_sequence);
		let producer_barrier     = Arc::clone(&self.producer_barrier);
		let shared_producer      = Arc::clone(&self.shared_producer);
		let consumer_barrier     = Arc::clone(&self.consumer_barrier);
		let ring_buffer          = Arc::clone(&self.ring_buffer);

		MultiProducer {
			shutdown_at_sequence,
			ring_buffer,
			shared_producer,
			producer_barrier,
			consumer_barrier,
			claimed_sequence: NONE,
			sequence_clear_of_consumers: 0
		}
	}
}

impl<E, C> MultiProducer<E, C> {
	/// Explicitly shuts down the Disruptor and returns an error if any consumer thread panicked.
	///
	/// This consumes the producer. If there are still other cloned producers alive, this
	/// only decrements the reference count without shutting down. Shutdown only happens
	/// when the last producer calls this method or is dropped.
	///
	/// # Errors
	///
	/// Returns `ConsumerPanic` if any consumer thread panicked during shutdown.
	pub fn shutdown(self) -> Result<(), crate::consumer::ConsumerPanic> {
		let mut shared = self.shared_producer.lock().unwrap();
		let old_count  = shared.counter.fetch_sub(1, Ordering::AcqRel);
		let result = if old_count == 1 {
			let sequence = self.producer_barrier.current() + 1;
			self.shutdown_at_sequence.store(sequence, Ordering::Relaxed);
			let mut first_err = None;
			for c in shared.consumers.iter_mut() {
				if let Err(e) = c.join() {
					if first_err.is_none() {
						first_err = Some(e);
					} else {
						eprintln!("disruptor: additional consumer thread panicked during shutdown");
					}
				}
			}
			first_err.map_or(Ok(()), Err)
		} else {
			Ok(())
		};
		// Release the lock before self drops (which triggers Drop, re-acquiring the lock).
		drop(shared);
		// When self drops: counter goes from 0 to -1 (old_count=0, skip shutdown block).
		// Consumer join handles are already None — no double-join.
		// All Arc fields are properly released by normal drop glue.
		result
	}
}

impl<E, C> Drop for MultiProducer<E, C> {
	fn drop(&mut self) {
		let mut shared = self.shared_producer.lock().unwrap();
		let old_count  = shared.counter.fetch_sub(1, Ordering::AcqRel);
		if old_count == 1 {
			let sequence = self.producer_barrier.current() + 1;
			self.shutdown_at_sequence.store(sequence, Ordering::Relaxed);
			for c in shared.consumers.iter_mut() {
				if let Err(e) = c.join() {
					eprintln!("disruptor: consumer thread panicked during shutdown: {e}");
				}
			}
		}
	}
}

impl<E, C> MultiProducer<E, C>
where
	C: Barrier
{
	pub(crate) fn new(
		shutdown_at_sequence: Arc<CachePadded<AtomicI64>>,
		ring_buffer:          Arc<RingBuffer<E>>,
		producer_barrier:     Arc<MultiProducerBarrier>,
		consumers:            Vec<Consumer>,
		consumer_barrier:     C
	) -> Self
	{
		let shared_producer = Arc::new(
			Mutex::new(
				SharedProducer {
					consumers,
					counter: AtomicI64::new(1)
				}
			)
		);
		let consumer_barrier            = Arc::new(consumer_barrier);
		// Known to be available initially as consumers start at index 0.
		let sequence_clear_of_consumers = ring_buffer.size() - 1;
		MultiProducer {
			shutdown_at_sequence,
			ring_buffer,
			shared_producer,
			producer_barrier,
			consumer_barrier,
			claimed_sequence: NONE,
			sequence_clear_of_consumers
		}
	}

	#[inline]
	fn next_sequences(&mut self, n: usize) -> Result<Sequence, MissingFreeSlots> {
		let n = i64::try_from(n).expect("batch size exceeds i64::MAX");
		// We get the last produced sequence number and try and increment it.
		let mut current = self.producer_barrier.current();
		let mut n_next  = current.checked_add(n)
			.expect("Sequence overflow: published > i64::MAX events");

		loop {
			if self.sequence_clear_of_consumers < n_next {
				// We have to check where the rear consumer is in case we're about to
				// publish into the slot currently being read by the slowest consumer.
				// (Consumer is too far behind the producer to publish next n events).
				let rear_sequence_read = self.consumer_barrier.get_after(current);
				let free_slots         = self.ring_buffer.free_slots(current, rear_sequence_read);
				if free_slots < n {
					return Err(MissingFreeSlots((n - free_slots) as u64));
				}
				fence(Ordering::Acquire);

				// We now know how far we can continue until we get right behind the slowest consumers'
				// current position without checking where they actually are.
				self.sequence_clear_of_consumers = current + free_slots;
			}

			match self.producer_barrier.compare_exchange(current, n_next) {
				Ok(_) => {
					// The sequence interval `]current; n_next] is now exclusive for this producer.
					self.claimed_sequence = n_next;
					break;
				}
				Err(new_current) => {
					current = new_current;
					n_next  = current.checked_add(n)
						.expect("Sequence overflow: published > i64::MAX events");
				}
			}
		}

		Ok(n_next)
	}

	/// Precondition: `sequence` is available for publication.
	#[inline]
	fn apply_update<F>(&mut self, update: F) -> Sequence
	where
		F: FnOnce(&mut E)
	{
		let sequence  = self.claimed_sequence;
		// SAFETY: PublishGuard ensures the sequence is published even if `update` panics,
		// preventing permanent sequence holes that would deadlock consumers.
		let guard = PublishGuard::single(&self.producer_barrier, sequence);
		let event_ptr = self.ring_buffer.get(sequence);
		// SAFETY: This MultiProducer won exclusive ownership of `claimed_sequence` via
		// a successful CAS in next_sequences(). No other producer can claim this slot
		// until we publish it, and consumers cannot read unpublished slots.
		// Invariant maintained by: CAS in next_sequences() + availability bitmap.
		// Verified by: MPSC integration tests and loom tests (P0-4).
		let event     = unsafe { &mut *event_ptr };
		update(event);
		// Normal path: explicit commit (prevents the Drop from publishing again).
		guard.commit();
		sequence
	}

	/// Precondition: `sequence` and previous `n - 1` sequences are available for publication.
	#[inline]
	fn apply_updates<'a, F>(&'a mut self, n: usize, updates: F) -> Sequence
	where
		E: 'a,
		F: FnOnce(MutBatchIter<'a, E>)
	{
		let n_i64 = i64::try_from(n).expect("batch size exceeds i64::MAX");
		let upper  = self.claimed_sequence;
		let lower  = upper - n_i64 + 1;
		// SAFETY: PublishGuard ensures the range is published even if `updates` panics.
		let guard = PublishGuard::range(&self.producer_barrier, lower, n_i64);
		let iter   = MutBatchIter::new(lower, upper, &self.ring_buffer);
		updates(iter);
		// Normal path: explicit commit.
		guard.commit();
		upper
	}
}

/// RAII guard that ensures claimed sequences are always published, even if the
/// user-provided closure panics. This prevents the Critical-severity sequence
/// hole bug where a panic after claim but before publish permanently deadlocks
/// all consumers and subsequent producers.
struct PublishGuard<'a> {
	barrier:   &'a MultiProducerBarrier,
	lower:     Sequence,
	count:     i64,
	committed: bool,
}

impl<'a> PublishGuard<'a> {
	/// Guard for a single sequence publication.
	#[inline]
	fn single(barrier: &'a MultiProducerBarrier, sequence: Sequence) -> Self {
		Self { barrier, lower: sequence, count: 1, committed: false }
	}

	/// Guard for a range publication [lower, lower + count).
	#[inline]
	fn range(barrier: &'a MultiProducerBarrier, lower: Sequence, count: i64) -> Self {
		Self { barrier, lower, count, committed: false }
	}

	/// Mark the publication as successfully committed (normal path).
	/// After this, Drop will not re-publish.
	#[inline]
	fn commit(mut self) {
		if self.count == 1 {
			self.barrier.publish(self.lower);
		} else {
			fence(Ordering::Release);
			self.barrier.publish_range_relaxed(self.lower, self.count);
		}
		self.committed = true;
	}
}

impl Drop for PublishGuard<'_> {
	#[inline]
	fn drop(&mut self) {
		if !self.committed {
			// Panic path: publish anyway to prevent sequence holes.
			// The event data may be incomplete, but the topology will not deadlock.
			if self.count == 1 {
				self.barrier.publish(self.lower);
			} else {
				fence(Ordering::Release);
				self.barrier.publish_range_relaxed(self.lower, self.count);
			}
		}
	}
}

/// Barrier for multiple producers.
pub struct MultiProducerBarrier {
	/// Cursor used by producers to claim exclusive sequences per producer.
	cursor:      Cursor,
	/// AtomicU64s each track availability of 64 slots.
	/// Each bit in the AtomicU64 encodes whether the slot was published in an even or odd round.
	/// This way producers avoid coordinating directly (with the added overhead that would have).
	/// Note, producers can never "overtake" each other and overwrite the availability of an event
	/// in the "previous" round as all producers must wait for the consumer furtherst behind (which
	/// is again blocked by the slowest producer).
	available:   Box<[AtomicU64]>,
	index_mask:  usize,
	index_shift: usize,
}

impl MultiProducerBarrier {
	pub(crate) fn new(size: usize) -> Self {
		assert!(size >= 64, "Multi Producer Disruptor must have a size of minimum 64 slots.");

		let cursor      = Cursor::new();
		let i64_needed  = size/64;
		// available encodes 1 bit for each slot.
		// If the bit is 1 it means that the slot was published in the latest odd round
		// and 0 means latest even round. A ring buffer starts with round 0 (an even round)
		// so that is why we initialize with all 1's as nothing is published initially.
		let all_ones    = !0_u64;
		let available   = (0..i64_needed).map(|_i| { AtomicU64::new(all_ones) }).collect();
		let index_mask  = size - 1;
		let index_shift = Self::log2(size);

		Self { cursor, available, index_mask, index_shift }
	}

	fn log2(i: usize) -> usize {
		std::mem::size_of::<usize>()*8 - (i.leading_zeros() as usize) - 1
	}

	#[inline]
	fn current(&self) -> Sequence {
		self.cursor.relaxed_value()
	}

	#[inline]
	fn compare_exchange(&self, current: Sequence, next: Sequence) -> Result<i64, i64> {
		self.cursor.compare_exchange(current, next)
	}

	/// Returns the availability index and the bit index of the given sequence number.
	#[inline]
	fn calculate_availability_indices(&self, sequence: Sequence) -> (usize, usize) {
		let slot_index         = self.slot_index(sequence);
		let availability_index = slot_index >> 6; // == divide by 64.
		let bit_index          = slot_index - availability_index*64;
		(availability_index, bit_index)
	}

	#[inline]
	fn slot_index(&self, sequence: Sequence) -> usize {
		sequence as usize & self.index_mask
	}

	/// Calculates if we're in an even (0) or odd (1) round.
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

	#[inline]
	fn publish_range_relaxed(&self, from: Sequence, n: i64) {
		let (mut availability_index, mut bit_index) = self.calculate_availability_indices(from);
		let mut flip_mask                           = 0_u64;
		let mut availability                        = self.availability_at(availability_index);

		for i in 0..n {
			// Encode which bits need to be flipped in the next bit field, counting bit_index upwards while < 63.
			flip_mask |= 1 << bit_index;

			if bit_index < 63 { // We shift max 63 places.
				bit_index += 1;
			}
			else {
				// Commit flip mask so far to current bit field.
				availability.fetch_xor(flip_mask, Ordering::Relaxed);
				// Load next bit field and reset flip_mask.
				let next_sequence       = from + i + 1;
				(availability_index, _) = self.calculate_availability_indices(next_sequence);
				availability            = self.availability_at(availability_index);
				bit_index               = 0;
				flip_mask               = 0;
			}
		}
		// If there's any remaining - commit them to the last bit field.
		if flip_mask > 0 {
			availability.fetch_xor(flip_mask, Ordering::Relaxed);
		}
	}

	#[inline]
	fn publish_with_ordering(&self, sequence: Sequence, ordering: Ordering) {
		let (availability_index, bit_index) = self.calculate_availability_indices(sequence);
		let availability                    = self.availability_at(availability_index);
		let mask                            = 1 << bit_index;
		// XOR operation will flip the bit on exactly the bit_index position - encoding that we have
		// published an event in an even or odd round.
		availability.fetch_xor(mask, ordering);
	}
}

impl Barrier for MultiProducerBarrier {
	#[inline]
	fn get_after(&self, prev: Sequence) -> Sequence {
		let mut availability_flag                   = self.calculate_availability_flag(prev);
		let (mut availability_index, mut bit_index) = self.calculate_availability_indices(prev);
		let mut availability                        = self.availability_at(availability_index).load(Ordering::Relaxed);
		// Shift bits to first relevant bit.
		availability                              >>= bit_index;
		let mut highest_available                   = prev;

		loop {
			if availability & 1 != availability_flag {
				return highest_available - 1;
			}
			highest_available += 1;

			// Prepare for checking the next bit.
			if bit_index < 63 { // We shift max 63 places.
				bit_index     += 1;
				availability >>= 1;
			}
			else {
				// Load next bit field.
				(availability_index, _) = self.calculate_availability_indices(highest_available);
				availability            = self.availability_at(availability_index).load(Ordering::Relaxed);
				bit_index               = 0;

				if availability_index == 0 {
					// If we wrapped then we're now looking for the flipped bit.
					// (I.e. from odd to even or from even to odd.)
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
		// Verify published:
		assert_eq!(barrier.get_after(0), 0);
	}

	#[test]
	fn publication_of_range_for_small_barrier() {
		let barrier = MultiProducerBarrier::new(64);

		barrier.publish_range_relaxed(0, 10);
		// Verify published:
		assert_eq!(barrier.get_after(0), 9);
	}

	#[test]
	fn publication_of_range_wrapping_ringbuffer_for_small_barrier() {
		let barrier = MultiProducerBarrier::new(64);

		barrier.publish_range_relaxed(0, 50);
		// Verify published:
		assert_eq!(barrier.get_after(0), 49);

		barrier.publish_range_relaxed(50, 50);
		// Verify published:
		assert_eq!(barrier.get_after(49), 99);
	}

	#[test]
	fn publication_of_range_wrapping_ringbuffer_for_barrier() {
		let barrier = MultiProducerBarrier::new(128);

		barrier.publish_range_relaxed(0, 100);
		// Verify published:
		assert_eq!(barrier.get_after(0), 99);
		// Verify not published:

		barrier.publish_range_relaxed(100, 100);
		// Verify published:
		assert_eq!(barrier.get_after(99), 199);

		barrier.publish_range_relaxed(200, 100);
		// Verify published:
		assert_eq!(barrier.get_after(199), 299);
	}
}
