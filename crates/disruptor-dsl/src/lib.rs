#![deny(missing_docs)]
//! Builder DSL and Producer implementations for the disruptor-rs framework.
//!
//! Use [`Disruptor::single()`] or [`Disruptor::multi()`] to get started.
//!
//! # Examples
//!
//! ```ignore
//! use disruptor_dsl::prelude::*;
//!
//! struct Event { price: f64 }
//!
//! let mut producer = Disruptor::single(8, || Event { price: 0.0 }, BusySpin)
//!     .handle_events_with(|e: &Event, _seq, _eob| {
//!         // process event
//!     })
//!     .build();
//!
//! producer.publish(|e| { e.price = 42.0; });
//! ```

pub mod producer;
pub mod builder;

pub use producer::{Producer, PublishError};
pub use producer::single::SingleProducer;
pub use producer::multi::MultiProducer;
pub use builder::{build_single_producer, build_multi_producer, ProcessorSettings};

use disruptor_seq::{SingleProducerBarrier, MultiProducerBarrier};
use disruptor_wait::WaitStrategy;
use builder::{single::SPBuilder, multi::MPBuilder, NC};

/// The main entry point for creating a Disruptor.
///
/// Use `Disruptor::single()` for single-threaded publishing or
/// `Disruptor::multi()` for multi-threaded publishing.
pub struct Disruptor;

impl Disruptor {
	/// Create a single-producer Disruptor.
	///
	/// Use this when only one thread publishes events.
	/// For multi-threaded publishing, use [`Disruptor::multi()`].
	pub fn single<E, W, F>(size: usize, factory: F, wait_strategy: W)
	-> SPBuilder<NC, E, W, SingleProducerBarrier>
	where
		F: FnMut() -> E,
		E: 'static + Send + Sync,
		W: 'static + WaitStrategy,
	{
		build_single_producer(size, factory, wait_strategy)
	}

	/// Create a multi-producer Disruptor.
	///
	/// Use this when multiple threads need to publish events.
	/// The returned Producer can be cloned for each publishing thread.
	pub fn multi<E, W, F>(size: usize, factory: F, wait_strategy: W)
	-> MPBuilder<NC, E, W, MultiProducerBarrier>
	where
		F: FnMut() -> E,
		E: 'static + Send + Sync,
		W: 'static + WaitStrategy,
	{
		build_multi_producer(size, factory, wait_strategy)
	}
}

/// Convenient re-exports for common usage.
///
/// ```ignore
/// use disruptor_dsl::prelude::*;
/// ```
pub mod prelude {
	pub use crate::Disruptor;
	pub use crate::producer::Producer;
	pub use crate::producer::PublishError;
	pub use disruptor_core::Sequence;
	pub use disruptor_consumer::EventHandler;
	pub use disruptor_wait::{
		BusySpin,
		BusySpinWithSpinLoopHint,
		SpinLoopHint,
		WaitStrategy,
		YieldingWaitStrategy,
		SleepingWaitStrategy,
		PhasedBackoffWaitStrategy,
	};
}
