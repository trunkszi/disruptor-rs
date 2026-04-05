use std::{process, sync::{atomic::{fence, AtomicI64, Ordering}, Arc, Mutex}};
use crossbeam_utils::CachePadded;
use disruptor_core::{Barrier, RingBuffer, Sequence, NONE};
use disruptor_consumer::{Consumer, ConsumerPanic};
use disruptor_seq::{MultiProducerBarrier, ProducerBarrier};
use super::*;

struct SharedProducer {
	consumers: Vec<Consumer>,
	counter:   AtomicI64,
}

/// Producer for publishing to the Disruptor from multiple threads.
pub struct MultiProducer<E, C> {
	shutdown_at_sequence:        Arc<CachePadded<AtomicI64>>,
	ring_buffer:                 Arc<RingBuffer<E>>,
	shared_producer:             Arc<Mutex<SharedProducer>>,
	producer_barrier:            Arc<MultiProducerBarrier>,
	consumer_barrier:            Arc<C>,
	claimed_sequence:            Sequence,
	sequence_clear_of_consumers: Sequence,
	/// Set to true when `shutdown()` has already decremented the counter,
	/// so `Drop` does not decrement it a second time.
	shutdown_done:               bool,
}

impl<E, C> Producer<E> for MultiProducer<E, C>
where
	C: Barrier
{
	#[inline]
	fn try_publish<F>(&mut self, update: F) -> Result<Sequence, PublishError>
	where F: FnOnce(&mut E)
	{
		self.next_sequences(1).map_err(|_| PublishError::Full)?;
		let sequence = self.apply_update(update);
		Ok(sequence)
	}

	#[inline]
	fn publish<F>(&mut self, update: F)
	where F: FnOnce(&mut E)
	{
		while self.next_sequences(1).is_err() { /* Empty. */ }
		self.apply_update(update);
	}

	#[inline]
	fn try_batch_publish<'a, F>(&'a mut self, n: usize, update: F) -> Result<Sequence, PublishError>
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

		if count > i64::MAX/2 {
			process::abort();
		}

		MultiProducer {
			shutdown_at_sequence:        Arc::clone(&self.shutdown_at_sequence),
			ring_buffer:                 Arc::clone(&self.ring_buffer),
			shared_producer:             Arc::clone(&self.shared_producer),
			producer_barrier:            Arc::clone(&self.producer_barrier),
			consumer_barrier:            Arc::clone(&self.consumer_barrier),
			claimed_sequence:            NONE,
			sequence_clear_of_consumers: 0,
			shutdown_done:               false,
		}
	}
}

impl<E, C> MultiProducer<E, C> {
	/// Explicitly shuts down the Disruptor and returns an error if any consumer thread panicked.
	pub fn shutdown(mut self) -> Result<(), ConsumerPanic> {
		self.shutdown_done = true;
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
		drop(shared);
		result
	}
}

impl<E, C> Drop for MultiProducer<E, C> {
	fn drop(&mut self) {
		if self.shutdown_done { return; }
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
		let sequence_clear_of_consumers = ring_buffer.size() - 1;
		MultiProducer {
			shutdown_at_sequence,
			ring_buffer,
			shared_producer,
			producer_barrier,
			consumer_barrier,
			claimed_sequence: NONE,
			sequence_clear_of_consumers,
			shutdown_done: false,
		}
	}

	#[inline]
	fn next_sequences(&mut self, n: usize) -> Result<Sequence, PublishError> {
		let n = i64::try_from(n).expect("batch size exceeds i64::MAX");
		let mut current = self.producer_barrier.current();
		let mut n_next  = current.checked_add(n)
			.expect("Sequence overflow: published > i64::MAX events");

		loop {
			if self.sequence_clear_of_consumers < n_next {
				let rear_sequence_read = self.consumer_barrier.get_after(current);
				let free_slots         = self.ring_buffer.free_slots(current, rear_sequence_read);
				if free_slots < n {
					return Err(PublishError::InsufficientCapacity { missing: (n - free_slots) as u64 });
				}
				fence(Ordering::Acquire);
				self.sequence_clear_of_consumers = current + free_slots;
			}

			match self.producer_barrier.compare_exchange(current, n_next) {
				Ok(_) => {
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

	#[inline]
	fn apply_update<F>(&mut self, update: F) -> Sequence
	where F: FnOnce(&mut E)
	{
		let sequence  = self.claimed_sequence;
		let guard = PublishGuard::single(&self.producer_barrier, sequence);
		let event_ptr = self.ring_buffer.get(sequence);
		// SAFETY: This MultiProducer won exclusive ownership via a successful CAS.
		// Invariant maintained by: CAS in next_sequences() + availability bitmap.
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
		let n_i64 = i64::try_from(n).expect("batch size exceeds i64::MAX");
		let upper  = self.claimed_sequence;
		let lower  = upper - n_i64 + 1;
		let guard = PublishGuard::range(&self.producer_barrier, lower, n_i64);
		let iter   = MutBatchIter::new(lower, upper, &self.ring_buffer);
		updates(iter);
		guard.commit();
		upper
	}
}

/// RAII guard that ensures claimed sequences are always published.
struct PublishGuard<'a> {
	barrier:   &'a MultiProducerBarrier,
	lower:     Sequence,
	count:     i64,
	committed: bool,
}

impl<'a> PublishGuard<'a> {
	#[inline]
	fn single(barrier: &'a MultiProducerBarrier, sequence: Sequence) -> Self {
		Self { barrier, lower: sequence, count: 1, committed: false }
	}

	#[inline]
	fn range(barrier: &'a MultiProducerBarrier, lower: Sequence, count: i64) -> Self {
		Self { barrier, lower, count, committed: false }
	}

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
			if self.count == 1 {
				self.barrier.publish(self.lower);
			} else {
				fence(Ordering::Release);
				self.barrier.publish_range_relaxed(self.lower, self.count);
			}
		}
	}
}
