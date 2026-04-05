use disruptor_core::{Barrier, Cursor, Sequence};
use crate::ProducerBarrier;

/// Barrier for a single producer.
pub struct SingleProducerBarrier {
	cursor: Cursor
}

impl SingleProducerBarrier {
	/// Creates a new single-producer barrier.
	pub fn new() -> Self {
		Self {
			cursor: Cursor::new()
		}
	}
}

impl Default for SingleProducerBarrier {
	fn default() -> Self {
		Self::new()
	}
}

impl Barrier for SingleProducerBarrier {
	/// Gets the `Sequence` of the last published event.
	#[inline]
	fn get_after(&self, _prev: Sequence) -> Sequence {
		self.cursor.relaxed_value()
	}
}

impl ProducerBarrier for SingleProducerBarrier {
	#[inline]
	fn publish(&self, sequence: Sequence) {
		self.cursor.store(sequence);
	}
}
