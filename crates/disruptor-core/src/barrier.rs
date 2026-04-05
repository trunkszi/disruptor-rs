use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use crate::{Cursor, Sequence};

/// Indicates no sequence number has been claimed (yet).
pub const NONE: Sequence = -1;

/// More descriptive alias for [`NONE`]: the initial sequence value before any events are published.
pub const INITIAL_SEQUENCE: Sequence = NONE;

/// Trait for sequence barriers that track progress of producers or consumers.
///
/// Barriers are used to coordinate access to the ring buffer:
/// - Producer barriers track the latest published sequence.
/// - Consumer barriers track the slowest consumer's position.
pub trait Barrier: Send + Sync {
	/// Gets the sequence number of the barrier with relaxed memory ordering.
	/// `prev` must be the last sequence returned from this barrier.
	///
	/// Note, to establish proper happens-before relationships (and thus proper synchronization),
	/// the caller must issue a [`std::sync::atomic::fence`] with [`Ordering::Acquire`].
	fn get_after(&self, prev: Sequence) -> Sequence;
}

/// Error raised when a [`SequenceBarrier`] is alerted (typically during shutdown).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlertError;

impl std::fmt::Display for AlertError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "SequenceBarrier has been alerted")
	}
}

impl std::error::Error for AlertError {}

/// SequenceBarrier coordinates consumers waiting for sequences to become available.
///
/// It combines a dependent sequence (from the Sequencer's cursor or from
/// upstream consumers) with a wait mechanism. The alert mechanism supports
/// graceful shutdown.
pub trait SequenceBarrier: Send + Sync {
	/// Wait for the given sequence to be available.
	/// Returns the highest available sequence (may be > requested sequence).
	///
	/// # Errors
	///
	/// Returns [`AlertError`] if the barrier has been alerted (e.g., during shutdown).
	fn wait_for(&self, sequence: Sequence) -> Result<Sequence, AlertError>;

	/// Get the current value of the dependent sequence without waiting.
	fn get_cursor(&self) -> Sequence;

	/// Signal an alert to interrupt waiting consumers (used during shutdown).
	fn alert(&self);

	/// Clear the alert status.
	fn clear_alert(&self);

	/// Check if an alert has been raised.
	fn is_alerted(&self) -> bool;
}

/// Shared alert flag for SequenceBarrier implementations.
#[derive(Default)]
pub struct Alert {
	flag: AtomicBool,
}

impl Alert {
	/// Create a new unset alert.
	pub fn new() -> Self {
		Self { flag: AtomicBool::new(false) }
	}

	/// Raise the alert.
	pub fn set(&self) {
		self.flag.store(true, Ordering::Release);
	}

	/// Clear the alert.
	pub fn clear(&self) {
		self.flag.store(false, Ordering::Release);
	}

	/// Check if the alert is raised.
	pub fn is_set(&self) -> bool {
		self.flag.load(Ordering::Acquire)
	}
}

/// Barrier tracking a single consumer.
pub struct SingleConsumerBarrier {
	cursor: Arc<Cursor>
}

/// Barrier tracking the minimum sequence of a group of consumers.
pub struct MultiConsumerBarrier {
	cursors: Vec<Arc<Cursor>>
}

impl SingleConsumerBarrier {
	/// Creates a new barrier tracking the given cursor.
	pub fn new(cursor: Arc<Cursor>) -> Self {
		Self {
			cursor
		}
	}
}

impl Barrier for SingleConsumerBarrier {
	#[inline]
	fn get_after(&self, _lower_bound: Sequence) -> Sequence {
		self.cursor.relaxed_value()
	}
}

impl MultiConsumerBarrier {
	/// Creates a new barrier tracking the minimum of all given cursors.
	pub fn new(cursors: Vec<Arc<Cursor>>) -> Self {
		Self {
			cursors
		}
	}
}

impl Barrier for MultiConsumerBarrier {
	/// Gets the available `Sequence` of the slowest consumer.
	#[inline]
	fn get_after(&self, _lower_bound: Sequence) -> Sequence {
		self.cursors.iter().fold(i64::MAX, |min_sequence, cursor| {
			let sequence = cursor.relaxed_value();
			std::cmp::min(sequence, min_sequence)
		})
	}
}
