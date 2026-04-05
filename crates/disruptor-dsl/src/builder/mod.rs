//! Module for building the Disruptor and adding event handlers.

use std::sync::{atomic::AtomicI64, Arc};
use std::thread;
use core_affinity::CoreId;
use crossbeam_utils::CachePadded;
use disruptor_core::{Barrier, Cursor, RingBuffer, Sequence, NONE};
use disruptor_consumer::{Consumer, EventPoller, Branch, set_affinity_if_defined, cpu_has_core_else_panic, wait_for_events};
use disruptor_seq::{SingleProducerBarrier, MultiProducerBarrier};
use disruptor_wait::WaitStrategy;

use self::{multi::MPBuilder, single::SPBuilder};

pub mod single;
pub mod multi;

mod sealed {
	#[allow(dead_code)] // used for type-level state machine sealing
	pub trait BuilderState {}
}

/// State: No consumers (yet).
#[doc(hidden)]
pub struct NC;
impl sealed::BuilderState for NC {}

/// State: Single consumer.
#[doc(hidden)]
pub struct SC;
impl sealed::BuilderState for SC {}

/// State: Multiple consumers.
#[doc(hidden)]
pub struct MC;
impl sealed::BuilderState for MC {}

/// Build a single producer Disruptor.
pub fn build_single_producer<E, W, F>(size: usize, event_factory: F, wait_strategy: W)
-> SPBuilder<NC, E, W, SingleProducerBarrier>
where
	F: FnMut() -> E,
	E: 'static + Send + Sync,
	W: 'static + WaitStrategy,
{
	let producer_barrier  = Arc::new(SingleProducerBarrier::new());
	let dependent_barrier = Arc::clone(&producer_barrier);
	SPBuilder::new(size, event_factory, wait_strategy, producer_barrier, dependent_barrier)
}

/// Build a multi producer Disruptor.
pub fn build_multi_producer<E, W, F>(size: usize, event_factory: F, wait_strategy: W)
-> MPBuilder<NC, E, W, MultiProducerBarrier>
where
	F: FnMut() -> E,
	E: 'static + Send + Sync,
	W: 'static + WaitStrategy,
{
	let producer_barrier  = Arc::new(MultiProducerBarrier::new(size));
	let dependent_barrier = Arc::clone(&producer_barrier);
	MPBuilder::new(size, event_factory, wait_strategy, producer_barrier, dependent_barrier)
}

/// The processor's thread name and CPU affinity can be set via builders that implement this trait.
pub trait ProcessorSettings<E, W>: Sized {
	#[doc(hidden)]
	fn shared(&mut self) -> &mut Shared<E, W>;

	/// Pin processor thread on the core with `id` for the next added event handler.
	fn pin_at_core(mut self, id: usize) -> Self {
		self.shared().pin_at_core(id);
		self
	}

	/// Set a name for the processor thread for the next added event handler.
	fn thread_name(mut self, name: &'static str) -> Self {
		self.shared().thread_named(name);
		self
	}
}

pub(crate) trait Builder<E, W, B>: ProcessorSettings<E, W>
where
	E: 'static + Send + Sync,
	B: 'static + Barrier,
	W: 'static + WaitStrategy,
{
	fn add_event_handler<EH>(&mut self, event_handler: EH)
	where
		EH: 'static + Send + FnMut(&E, Sequence, bool)
	{
		let barrier            = self.dependent_barrier();
		let (cursor, consumer) = start_processor(event_handler, self.shared(), barrier);
		self.shared().add_consumer_and_cursor(consumer, cursor);
	}

	fn build_event_poller(&mut self) -> EventPoller<E, B> {
		let cursor = Arc::new(Cursor::new());
		self.shared().add_cursor(Arc::clone(&cursor));

		EventPoller::new(
			Arc::clone(&self.shared().ring_buffer),
			Arc::clone(&self.dependent_barrier()),
			Arc::clone(&self.shared().shutdown_at_sequence),
			cursor,
		)
	}

	fn build_branch(&mut self) -> Branch<E, B> {
		let poller = EventPoller::new(
			Arc::clone(&self.shared().ring_buffer),
			Arc::clone(&self.dependent_barrier()),
			Arc::clone(&self.shared().shutdown_at_sequence),
			Arc::new(Cursor::new()));
		Branch::new(poller)
	}

	fn add_cursor_from_branch<B2>(&mut self, branch: &Branch<E, B2>) {
		self.shared().add_cursor(branch.cursor_for_poller());
	}

	fn add_event_handler_with_state<EH, S, IS>(&mut self, event_handler: EH, initialize_state: IS)
	where
		EH: 'static + Send + FnMut(&mut S, &E, Sequence, bool),
		IS: 'static + Send + FnOnce() -> S,
	{
		let barrier            = self.dependent_barrier();
		let (cursor, consumer) = start_processor_with_state(event_handler, self.shared(), barrier, initialize_state);
		self.shared().add_consumer_and_cursor(consumer, cursor);
	}

	fn dependent_barrier(&self) -> Arc<B>;
}

#[doc(hidden)]
pub struct Shared<E, W> {
	pub(crate) shutdown_at_sequence: Arc<CachePadded<AtomicI64>>,
	pub(crate) ring_buffer:          Arc<RingBuffer<E>>,
	pub(crate) consumers:            Vec<Consumer>,
	pub(crate) current_consumer_cursors: Option<Vec<Arc<Cursor>>>,
	pub(crate) wait_strategy:        W,
	pub(crate) thread_context:       ThreadContext,
}

impl <E, W> Shared<E, W> {
	pub(crate) fn new<F>(size: usize, event_factory: F, wait_strategy: W) -> Self
	where
		F: FnMut() -> E
	{
		let ring_buffer              = Arc::new(RingBuffer::new(size, event_factory));
		let shutdown_at_sequence     = Arc::new(CachePadded::new(AtomicI64::new(NONE)));
		let current_consumer_cursors = Some(vec![]);

		Self {
			ring_buffer,
			wait_strategy,
			shutdown_at_sequence,
			current_consumer_cursors,
			consumers: vec![],
			thread_context: ThreadContext::default(),
		}
	}

	pub(crate) fn add_consumer_and_cursor(&mut self, consumer: Consumer, cursor: Arc<Cursor>) {
		self.consumers.push(consumer);
		self.add_cursor(cursor);
	}

	pub(crate) fn add_cursor(&mut self, cursor: Arc<Cursor>) {
		self.current_consumer_cursors.as_mut().unwrap().push(cursor);
	}

	fn pin_at_core(&mut self, id: usize) {
		cpu_has_core_else_panic(id);
		self.thread_context.affinity = Some(CoreId { id } );
	}

	fn thread_named(&mut self, name: &'static str) {
		self.thread_context.name = Some(name.to_owned());
	}
}

#[derive(Default)]
pub(crate) struct ThreadContext {
	pub(crate) affinity: Option<CoreId>,
	pub(crate) name:     Option<String>,
	pub(crate) id:       usize,
}

impl ThreadContext {
	pub(crate) fn name(&mut self) -> String {
		let name = match &self.name {
			Some(name) => name.clone(),
			None       => format!("processor-{}", self.id),
		};
		self.id += 1;
		self.name = None;
		name
	}

	pub(crate) fn affinity(&mut self) -> Option<CoreId> {
		self.affinity.take()
	}
}

fn start_processor<E, EP, W, B>(
	mut event_handler: EP,
	builder:           &mut Shared<E, W>,
	barrier:           Arc<B>)
-> (Arc<Cursor>, Consumer)
where
	E:  'static + Send + Sync,
	EP: 'static + Send + FnMut(&E, Sequence, bool),
	W:  'static + WaitStrategy,
	B:  'static + Barrier + Send + Sync,
{
	let consumer_cursor      = Arc::new(Cursor::new());
	let wait_strategy        = builder.wait_strategy;
	let ring_buffer          = Arc::clone(&builder.ring_buffer);
	let shutdown_at_sequence = Arc::clone(&builder.shutdown_at_sequence);
	let thread_name          = builder.thread_context.name();
	let affinity             = builder.thread_context.affinity();
	let thread_builder       = thread::Builder::new().name(thread_name.clone());
	let join_handle          = {
		let consumer_cursor = Arc::clone(&consumer_cursor);
		thread_builder.spawn(move || {
			set_affinity_if_defined(affinity, thread_name.as_str());
			let mut sequence = 0;
			while let Some(available) = wait_for_events(sequence, &shutdown_at_sequence, barrier.as_ref(), &wait_strategy) {
				while available >= sequence {
					let end_of_batch = available == sequence;
					let event_ptr    = ring_buffer.get(sequence);
					// SAFETY: Shared read access — wait_for_events guarantees the producer
					// published this slot (Release), Acquire fence establishes happens-before.
					let event        = unsafe { & *event_ptr };
					event_handler(event, sequence, end_of_batch);
					sequence += 1;
				}
				consumer_cursor.store(sequence - 1);
			}
		}).expect("Should spawn thread.")
	};

	let consumer = Consumer::new(join_handle);
	(consumer_cursor, consumer)
}

fn start_processor_with_state<E, EP, W, B, S, IS>(
	mut event_handler: EP,
	builder:           &mut Shared<E, W>,
	barrier:           Arc<B>,
	initialize_state:  IS)
-> (Arc<Cursor>, Consumer)
where
	E:  'static + Send + Sync,
	IS: 'static + Send + FnOnce() -> S,
	EP: 'static + Send + FnMut(&mut S, &E, Sequence, bool),
	W:  'static + WaitStrategy,
	B:  'static + Barrier + Send + Sync,
{
	let consumer_cursor      = Arc::new(Cursor::new());
	let wait_strategy        = builder.wait_strategy;
	let ring_buffer          = Arc::clone(&builder.ring_buffer);
	let shutdown_at_sequence = Arc::clone(&builder.shutdown_at_sequence);
	let thread_name          = builder.thread_context.name();
	let affinity             = builder.thread_context.affinity();
	let thread_builder       = thread::Builder::new().name(thread_name.clone());
	let join_handle          = {
		let consumer_cursor = Arc::clone(&consumer_cursor);
		thread_builder.spawn(move || {
			set_affinity_if_defined(affinity, thread_name.as_str());
			let mut sequence = 0;
			let mut state    = initialize_state();
			while let Some(available_sequence) = wait_for_events(sequence, &shutdown_at_sequence, barrier.as_ref(), &wait_strategy) {
				while available_sequence >= sequence {
					let end_of_batch = available_sequence == sequence;
					let event_ptr    = ring_buffer.get(sequence);
					// SAFETY: Shared read access — same reasoning as start_processor.
					let event        = unsafe { & *event_ptr };
					event_handler(&mut state, event, sequence, end_of_batch);
					sequence += 1;
				}
				consumer_cursor.store(sequence - 1);
			}
		}).expect("Should spawn thread.")
	};

	let consumer = Consumer::new(join_handle);
	(consumer_cursor, consumer)
}
