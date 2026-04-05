use crossbeam_utils::CachePadded;
use std::sync::atomic::{AtomicI64, Ordering};

use crate::{Sequence, barrier::NONE};

/// A cache-line padded atomic cursor for tracking sequence positions.
///
/// Used by both producers and consumers to publish their current position.
pub struct Cursor {
	counter: CachePadded<AtomicI64>
}

impl Cursor {
	/// Creates a new cursor initialized to [`NONE`] (-1).
	pub fn new() -> Self {
		Self {
			counter: CachePadded::new(AtomicI64::new(NONE))
		}
	}

	/// Atomically compares and exchanges the cursor value.
	#[inline]
	pub fn compare_exchange(&self, current: Sequence, next: Sequence) -> Result<i64, i64> {
		self.counter.compare_exchange(current, next, Ordering::AcqRel, Ordering::Relaxed)
	}

	/// Stores `sequence` to the cursor with `Ordering::Release` semantics.
	#[inline]
	pub fn store(&self, sequence: Sequence) {
		self.counter.store(sequence, Ordering::Release);
	}

	/// Retrieves the cursor value with `Ordering::Relaxed` semantics.
	#[inline]
	pub fn relaxed_value(&self) -> Sequence {
		self.counter.load(Ordering::Relaxed)
	}
}

impl Default for Cursor {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn cursor_operations() {
		let cursor = Cursor::new();

		assert_eq!(cursor.compare_exchange(-1, 0).ok().unwrap(), -1);
		assert_eq!(cursor.compare_exchange( 0, 1).ok().unwrap(),  0);
		// Simulate other thread having updated the cursor.
		assert_eq!(cursor.compare_exchange( 0, 1).err().unwrap(), 1);

		cursor.store(100);
		assert_eq!(cursor.relaxed_value(), 100);
	}
}
