#![deny(missing_docs)]
//! Sequencer barriers and traits for the disruptor-rs framework.
//!
//! Provides:
//! - [`ProducerBarrier`] — low-level barrier for tracking published sequences
//! - [`SingleProducerBarrier`] / [`MultiProducerBarrier`] — barrier implementations
//! - [`Sequencer`] — high-level trait for claiming and publishing sequences
//! - [`SingleSequencer`] / [`MultiSequencer`] — sequencer implementations

mod single;
mod multi;
mod sequencer;

pub use single::SingleProducerBarrier;
pub use multi::MultiProducerBarrier;
pub use sequencer::{Sequencer, InsufficientCapacity, SingleSequencer, MultiSequencer};

use disruptor_core::{Barrier, Sequence};

/// Barrier for producers.
pub trait ProducerBarrier : Barrier {
	/// Publishes the sequence number that is now available for being read by consumers.
	/// (The sequence number is stored with [`std::sync::atomic::Ordering::Release`] semantics.)
	fn publish(&self, sequence: Sequence);
}
