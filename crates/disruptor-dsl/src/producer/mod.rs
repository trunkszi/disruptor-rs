//! Producer handles for publishing into the Disruptor.
use thiserror::Error;

/// Single-producer implementation.
pub mod single;
/// Multi-producer implementation.
pub mod multi;

use disruptor_core::{RingBuffer, Sequence};

/// Errors that can occur when publishing events.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[must_use]
#[non_exhaustive]
pub enum PublishError {
	/// The ring buffer has no free slots for a single event.
	#[error("Ring buffer is full")]
	Full,

	/// The ring buffer is missing free slots for a batch publish.
	#[error("Ring buffer missing {missing} free slot(s)")]
	InsufficientCapacity {
		/// Number of additional slots needed.
		missing: u64,
	},
}

// NOTE: Backward-compatible type aliases `RingBufferFull` and `MissingFreeSlots`
// were intentionally removed. In Rust, type aliases to enums cannot be used as
// match patterns — `Err(RingBufferFull)` silently becomes a catch-all variable
// binding rather than a variant match, causing logic bugs.
// Use `PublishError::Full` and `PublishError::InsufficientCapacity` directly.

/// Iterator for events that can be batch updated.
pub struct MutBatchIter<'a, E> {
	ring_buffer: &'a RingBuffer<E>,
	current:     Sequence,
	last:        Sequence,
}

impl<'a, E> MutBatchIter<'a, E> {
	pub(crate) fn new(start: Sequence, end: Sequence, ring_buffer: &'a RingBuffer<E>) -> Self {
		Self {
			ring_buffer,
			current: start,
			last:    end,
		}
	}

	fn remaining(&self) -> usize {
		(self.last - self.current + 1) as usize
	}
}

impl<'a, E> Iterator for MutBatchIter<'a, E> {
	type Item = &'a mut E;

	fn next(&mut self) -> Option<Self::Item> {
		if self.current > self.last {
			None
		}
		else {
			let event_ptr = self.ring_buffer.get(self.current);
			// SAFETY: MutBatchIter is created with a [lower, upper] range that the producer
			// has exclusively claimed. Each call to next() yields a unique sequence in that
			// range (current increments monotonically), so no two iterations alias the same slot.
			// Invariant maintained by: producer's next_sequences() + CAS for MultiProducer.
			// Verified by: batch publish integration tests.
			let event     = unsafe { &mut *event_ptr };
			self.current += 1;
			Some(event)
		}
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		let remaining = self.remaining();
		(remaining, Some(remaining))
	}
}

/// # Panics
///
/// Publishing panics if the total number of published events exceeds `i64::MAX` (2^63 - 1).
/// At 1 billion events per second this would take ~292 years, so in practice the limit is
/// unreachable.
pub trait Producer<E> {
	/// Publish an Event into the Disruptor.
	/// Returns a `Result` with the published sequence number or a [`PublishError`].
	fn try_publish<F>(&mut self, update: F) -> Result<Sequence, PublishError>
	where F: FnOnce(&mut E);

	/// Publish a batch of Events into the Disruptor.
	fn try_batch_publish<'a, F>(&'a mut self, n: usize, update: F) -> Result<Sequence, PublishError>
	where
		E: 'a,
		F: FnOnce(MutBatchIter<'a, E>);

	/// Publish an Event into the Disruptor. Spins until there is an available slot.
	fn publish<F>(&mut self, update: F)
	where F: FnOnce(&mut E);

	/// Publish a batch of Events into the Disruptor. Spins until there are enough available slots.
	fn batch_publish<'a, F>(&'a mut self, n: usize, update: F)
	where
		E: 'a,
		F: FnOnce(MutBatchIter<'a, E>);
}
