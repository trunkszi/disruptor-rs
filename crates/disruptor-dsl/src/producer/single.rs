use std::sync::atomic::{fence, Ordering};
use crossbeam_utils::CachePadded;
use std::sync::{Arc, atomic::AtomicI64};
use disruptor_core::{Barrier, RingBuffer, Sequence};
use disruptor_consumer::{Consumer, ConsumerPanic};
use disruptor_seq::{ProducerBarrier, SingleProducerBarrier};
use super::*;

/// RAII guard ensuring single-producer sequences are published even on panic.
struct SinglePublishGuard<'a> {
	barrier:   &'a SingleProducerBarrier,
	sequence:  Sequence,
	committed: bool,
}

impl SinglePublishGuard<'_> {
	#[inline]
	fn commit(mut self) {
		self.barrier.publish(self.sequence);
		self.committed = true;
	}
}

impl Drop for SinglePublishGuard<'_> {
	#[inline]
	fn drop(&mut self) {
		if !self.committed {
			self.barrier.publish(self.sequence);
		}
	}
}

/// Producer for publishing to the Disruptor from a single thread.
pub struct SingleProducer<E, C> {
	shutdown_at_sequence:        Arc<CachePadded<AtomicI64>>,
	ring_buffer:                 Arc<RingBuffer<E>>,
	producer_barrier:            Arc<SingleProducerBarrier>,
	consumers:                   Vec<Consumer>,
	consumer_barrier:            C,
	sequence:                    Sequence,
	sequence_clear_of_consumers: Sequence,
}

impl<E, C> Producer<E> for SingleProducer<E, C>
where
	C: Barrier
{
	#[inline]
	fn try_publish<F>(&mut self, update: F) -> Result<Sequence, PublishError>
	where
		F: FnOnce(&mut E)
	{
		self.next_sequences(1).map_err(|_| PublishError::Full)?;
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
	fn try_batch_publish<'a, F>(&'a mut self, n: usize, update: F) -> Result<Sequence, PublishError>
	where
		E: 'a,
		F: FnOnce(MutBatchIter<'a, E>)
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

impl<E, C> SingleProducer<E, C>
where
	C: Barrier
{
	pub(crate) fn new(
		shutdown_at_sequence: Arc<CachePadded<AtomicI64>>,
		ring_buffer:          Arc<RingBuffer<E>>,
		producer_barrier:     Arc<SingleProducerBarrier>,
		consumers:            Vec<Consumer>,
		consumer_barrier:     C,
	) -> Self
	{
		let sequence_clear_of_consumers = ring_buffer.size() - 1;
		Self {
			shutdown_at_sequence,
			ring_buffer,
			producer_barrier,
			consumers,
			consumer_barrier,
			sequence: 0,
			sequence_clear_of_consumers,
		}
	}

	#[inline]
	fn next_sequences(&mut self, n: usize) -> Result<Sequence, PublishError> {
		let n = i64::try_from(n).expect("batch size exceeds i64::MAX");
		let n_next = (self.sequence - 1).checked_add(n)
			.expect("Sequence overflow: published > i64::MAX events");

		if self.sequence_clear_of_consumers < n_next {
			let last_published     = self.sequence - 1;
			let rear_sequence_read = self.consumer_barrier.get_after(last_published);
			let free_slots         = self.ring_buffer.free_slots(last_published, rear_sequence_read);
			if free_slots < n {
				return Err(PublishError::InsufficientCapacity { missing: (n - free_slots) as u64 });
			}
			fence(Ordering::Acquire);
			self.sequence_clear_of_consumers = last_published + free_slots;
		}

		Ok(n_next)
	}

	#[inline]
	fn apply_update<F>(&mut self, update: F) -> Sequence
	where
		F: FnOnce(&mut E)
	{
		let sequence  = self.sequence;
		// Advance sequence BEFORE calling update() so that if update() panics,
		// the guard will publish the sequence to consumers and self.sequence will
		// already point to the next slot, preventing a subsequent publish from
		// reclaiming the same slot (which would be a data race with consumers).
		self.sequence = self.sequence.checked_add(1)
			.expect("Sequence overflow: published > i64::MAX events");
		let guard = SinglePublishGuard {
			barrier: &self.producer_barrier,
			sequence,
			committed: false,
		};
		let event_ptr = self.ring_buffer.get(sequence);
		// SAFETY: SingleProducer has exclusive ownership of this sequence slot.
		// Invariant maintained by: next_sequences() free_slots check.
		let event     = unsafe { &mut *event_ptr };
		update(event);
		guard.commit();
		sequence
	}

	#[inline]
	fn apply_updates<'a, F>(&'a mut self, n: usize, updates: F) -> Sequence
	where
		E: 'a,
		F: FnOnce(MutBatchIter<'a, E>)
	{
		let n     = i64::try_from(n).expect("batch size exceeds i64::MAX");
		let lower = self.sequence;
		let upper = (lower + n).checked_sub(1)
			.expect("Sequence overflow: published > i64::MAX events");
		// Advance sequence BEFORE calling updates() — same rationale as apply_update.
		self.sequence = self.sequence.checked_add(n)
			.expect("Sequence overflow: published > i64::MAX events");
		let guard = SinglePublishGuard {
			barrier: &self.producer_barrier,
			sequence: upper,
			committed: false,
		};
		let iter  = MutBatchIter::new(lower, upper, &self.ring_buffer);
		updates(iter);
		guard.commit();
		upper
	}
}

impl<E, C> SingleProducer<E, C> {
	/// Explicitly shuts down the Disruptor and returns an error if any consumer thread panicked.
	pub fn shutdown(mut self) -> Result<(), ConsumerPanic> {
		self.shutdown_at_sequence.store(self.sequence, Ordering::Relaxed);
		let mut first_err = None;
		for c in self.consumers.iter_mut() {
			if let Err(e) = c.join() {
				if first_err.is_none() {
					first_err = Some(e);
				} else {
					eprintln!("disruptor: additional consumer thread panicked during shutdown");
				}
			}
		}
		match first_err {
			Some(e) => Err(e),
			None    => Ok(()),
		}
	}
}

impl<E, C> Drop for SingleProducer<E, C> {
	fn drop(&mut self) {
		self.shutdown_at_sequence.store(self.sequence, Ordering::Relaxed);
		for c in self.consumers.iter_mut() {
			if let Err(e) = c.join() {
				eprintln!("disruptor: consumer thread panicked during shutdown: {e}");
			}
		}
	}
}
