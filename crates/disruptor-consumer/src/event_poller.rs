//! Pull-based event polling API.

use std::sync::{atomic::{fence, AtomicI64, Ordering}, Arc};
use crossbeam_utils::CachePadded;
use disruptor_core::{Cursor, Barrier, RingBuffer, Sequence};

/// Represents an EventPoller for pulling events from the ring buffer.
pub struct EventPoller<E, B> {
	ring_buffer:          Arc<RingBuffer<E>>,
	dependent_barrier:    Arc<B>,
	shutdown_at_sequence: Arc<CachePadded<AtomicI64>>,
	cursor:               Arc<Cursor>,
}

/// An opaque branch to be joined back into the main flow.
pub struct Branch<E, B> {
	poller: EventPoller<E, B>,
}

impl<E, B> Branch<E, B> {
	/// Creates a new branch wrapping an EventPoller.
	pub fn new(poller: EventPoller<E, B>) -> Self {
		Self { poller }
	}

	/// Returns a clone of the poller's cursor for dependency tracking.
	pub fn cursor_for_poller(&self) -> Arc<Cursor> {
		Arc::clone(&self.poller.cursor)
	}

	/// Consumes the branch and returns the inner EventPoller.
	pub fn into_poller(self) -> EventPoller<E, B> {
		self.poller
	}
}

/// Error types for unsuccessful polling.
#[derive(Debug, PartialEq)]
#[non_exhaustive]
pub enum Polling {
	/// No events available to process.
	NoEvents,
	/// The Disruptor has been shut down.
	Shutdown,
}

impl std::fmt::Display for Polling {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Polling::NoEvents => write!(f, "No available events."),
			Polling::Shutdown => write!(f, "Disruptor is shut down."),
		}
	}
}

impl std::error::Error for Polling {}

/// Guards available events from the EventPoller.
///
/// Dropping the guard signals the Disruptor that reading is complete.
pub struct EventGuard<'p, E, B> {
	parent:    &'p mut EventPoller<E, B>,
	sequence:  Sequence,
	available: Sequence,
}

impl<'g, E, B> Iterator for &'g mut EventGuard<'_, E, B> {
	type Item = &'g E;

	fn next(&mut self) -> Option<Self::Item> {
		if self.sequence > self.available {
			return None;
		}

		let event_ptr = self.parent.ring_buffer.get(self.sequence);
		// SAFETY: EventGuard is created only after the barrier confirmed `available >= sequence`,
		// meaning the producer has published this slot (Release). The Acquire fence in
		// EventPoller::poll/take establishes happens-before. The guard holds a mutable borrow
		// of the EventPoller, preventing concurrent access. Producers cannot overwrite this
		// slot until the guard is dropped and the cursor advances.
		// Invariant maintained by: poll()/take() barrier check + Acquire fence.
		// Verified by: event_poller integration tests.
		let event     = unsafe { &*event_ptr };
		self.sequence += 1;
		Some(event)
	}
}

impl<E, B> ExactSizeIterator for &mut EventGuard<'_, E, B> {
	fn len(&self) -> usize {
		(self.available - self.sequence + 1) as usize
	}
}

impl<E, B> Drop for EventGuard<'_, E, B> {
	fn drop(&mut self) {
		self.parent.cursor.store(self.available);
	}
}

impl<E, B> EventPoller<E, B>
where
	B: Barrier,
{
	/// Creates a new EventPoller.
	pub fn new(
		ring_buffer:          Arc<RingBuffer<E>>,
		dependent_barrier:    Arc<B>,
		shutdown_at_sequence: Arc<CachePadded<AtomicI64>>,
		cursor:               Arc<Cursor>,
	) -> Self {
		Self {
			ring_buffer,
			dependent_barrier,
			shutdown_at_sequence,
			cursor,
		}
	}

	/// Polls for all available events.
	pub fn poll(&mut self) -> Result<EventGuard<'_, E, B>, Polling> {
		self.take(u64::MAX)
	}

	/// Returns `true` if at least one event is available.
	#[inline]
	pub fn has_available(&self) -> bool {
		let sequence = self.cursor.relaxed_value() + 1;
		self.dependent_barrier.get_after(sequence) >= sequence
	}

	/// Polls for available events, yielding at most `limit` events.
	pub fn take(&mut self, limit: u64) -> Result<EventGuard<'_, E, B>, Polling> {
		let cursor_at = self.cursor.relaxed_value();
		let sequence  = cursor_at + 1;

		if sequence == self.shutdown_at_sequence.load(Ordering::Relaxed) {
			return Err(Polling::Shutdown);
		}

		let available = self.dependent_barrier.get_after(sequence);
		if available < sequence {
			return Err(Polling::NoEvents);
		}
		fence(Ordering::Acquire);

		let max_sequence = (cursor_at).saturating_add_unsigned(limit);
		let available    = std::cmp::min(available, max_sequence);

		Ok(EventGuard {
			parent: self,
			sequence,
			available,
		})
	}
}
