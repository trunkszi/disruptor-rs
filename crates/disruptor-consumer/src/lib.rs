#![deny(missing_docs)]
//! Consumer types, event polling, and thread affinity for the disruptor-rs framework.
//!
//! This crate provides:
//! - [`Consumer`] — managed consumer thread handle
//! - [`ConsumerPanic`] — error type for panicked consumer threads
//! - [`EventHandler`] — trait for processing events (with closure blanket impl)
//! - [`EventPoller`] / [`EventGuard`] — pull-based event consumption API
//! - [`wait_for_events`] — core wait loop used by managed consumers

pub mod event_poller;
mod affinity;

#[cfg(feature = "async")]
pub mod async_adapter;

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{fence, AtomicI64, Ordering};
use std::thread::JoinHandle;
use crossbeam_utils::CachePadded;
use disruptor_core::{Alert, AlertError, Barrier, Sequence, SequenceBarrier};
use disruptor_wait::WaitStrategy;

pub use affinity::{set_affinity_if_defined, cpu_has_core_else_panic};
pub use event_poller::{EventPoller, EventGuard, Polling, Branch};

/// A SequenceBarrier implementation that combines a dependent barrier with a WaitStrategy.
///
/// This is the standard implementation used by managed consumers in the Disruptor.
pub struct ProcessingSequenceBarrier<B, W> {
	dependent_barrier: B,
	wait_strategy:     W,
	alert:             Alert,
}

impl<B, W> ProcessingSequenceBarrier<B, W>
where
	B: Barrier,
	W: WaitStrategy,
{
	/// Creates a new processing sequence barrier.
	pub fn new(dependent_barrier: B, wait_strategy: W) -> Self {
		Self {
			dependent_barrier,
			wait_strategy,
			alert: Alert::new(),
		}
	}
}

impl<B, W> SequenceBarrier for ProcessingSequenceBarrier<B, W>
where
	B: Barrier,
	W: WaitStrategy + Sync,
{
	fn wait_for(&self, sequence: Sequence) -> Result<Sequence, AlertError> {
		let mut available = self.dependent_barrier.get_after(sequence);
		while available < sequence {
			if self.alert.is_set() {
				return Err(AlertError);
			}
			self.wait_strategy.wait_for(sequence);
			available = self.dependent_barrier.get_after(sequence);
		}
		std::sync::atomic::fence(std::sync::atomic::Ordering::Acquire);
		Ok(available)
	}

	fn get_cursor(&self) -> Sequence {
		self.dependent_barrier.get_after(0)
	}

	fn alert(&self) {
		self.alert.set();
	}

	fn clear_alert(&self) {
		self.alert.clear();
	}

	fn is_alerted(&self) -> bool {
		self.alert.is_set()
	}
}

/// Error type representing a panic that occurred in a consumer thread.
#[derive(Debug)]
pub struct ConsumerPanic {
	payload: Box<dyn std::any::Any + Send>,
}

impl ConsumerPanic {
	/// Returns the original panic payload.
	pub fn payload(&self) -> &(dyn std::any::Any + Send) {
		&*self.payload
	}

	/// Attempts to extract the panic message as a `&str`.
	pub fn message(&self) -> Option<&str> {
		self.payload.downcast_ref::<&str>().copied()
			.or_else(|| self.payload.downcast_ref::<String>().map(|s| s.as_str()))
	}
}

impl fmt::Display for ConsumerPanic {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self.message() {
			Some(msg) => write!(f, "consumer thread panicked: {msg}"),
			None      => write!(f, "consumer thread panicked"),
		}
	}
}

impl std::error::Error for ConsumerPanic {}

/// A managed consumer thread handle.
pub struct Consumer {
	join_handle: Option<JoinHandle<()>>,
}

impl Consumer {
	/// Creates a new consumer wrapping the given thread join handle.
	pub fn new(join_handle: JoinHandle<()>) -> Self {
		Self {
			join_handle: Some(join_handle),
		}
	}

	/// Joins the consumer thread, returning an error if it panicked.
	pub fn join(&mut self) -> Result<(), ConsumerPanic> {
		match self.join_handle.take() {
			Some(h) => h.join().map_err(|payload| ConsumerPanic { payload }),
			None    => Ok(()),
		}
	}
}

/// Waits for events to become available on the barrier.
///
/// Returns `Some(available_sequence)` when events are ready, or `None`
/// when the shutdown signal is received.
#[inline]
pub fn wait_for_events<B, W>(
	sequence:             Sequence,
	shutdown_at_sequence: &CachePadded<AtomicI64>,
	barrier:              &B,
	wait_strategy:        &W
)
-> Option<Sequence>
where
	B: Barrier + Send + Sync,
	W: WaitStrategy,
{
	let mut available = barrier.get_after(sequence);
	while available < sequence {
		if shutdown_at_sequence.load(Ordering::Relaxed) == sequence {
			return None;
		}
		wait_strategy.wait_for(sequence);
		available = barrier.get_after(sequence);
	}
	fence(Ordering::Acquire);
	Some(available)
}

/// Handles events as they become available from the RingBuffer.
///
/// Implement this trait for your event processing logic. For simple cases,
/// a closure blanket implementation is provided so closures work unchanged.
///
/// # Examples
///
/// Using a closure (blanket impl):
/// ```
/// use disruptor_consumer::EventHandler;
///
/// let mut handler = |_event: &i32, _seq: i64, _eob: bool| {
///     // process event
/// };
/// handler.on_event(&42, 0, true);
/// ```
///
/// Using a struct:
/// ```
/// use disruptor_consumer::EventHandler;
///
/// struct Counter { count: u64 }
/// impl EventHandler<i32> for Counter {
///     fn on_event(&mut self, _event: &i32, _sequence: i64, _end_of_batch: bool) {
///         self.count += 1;
///     }
///     fn on_start(&mut self) { self.count = 0; }
///     fn on_shutdown(&mut self) { println!("Processed {} events", self.count); }
/// }
/// ```
pub trait EventHandler<E>: Send {
	/// Process an event published to the RingBuffer.
	fn on_event(&mut self, event: &E, sequence: Sequence, end_of_batch: bool);

	/// Called once when the processing thread starts, before any events are processed.
	fn on_start(&mut self) {}

	/// Called once after the last event has been processed and before the thread exits.
	fn on_shutdown(&mut self) {}
}

/// Blanket implementation for closures — maintains backward compatibility.
impl<E, F> EventHandler<E> for F
where
	F: FnMut(&E, Sequence, bool) + Send,
{
	fn on_event(&mut self, event: &E, sequence: Sequence, end_of_batch: bool) {
		self(event, sequence, end_of_batch);
	}
}

// ─── P3-4: LifecycleAware ──────────────────────────────────────────────────

/// Trait for components that need to be notified of lifecycle events.
///
/// Implement this trait when your handler needs to perform initialization
/// or cleanup on the processing thread (e.g., thread-local storage, pinning).
pub trait LifecycleAware {
	/// Called when the processing thread starts, before any events are processed.
	fn on_start(&mut self);

	/// Called after the last event has been processed and before the thread exits.
	fn on_shutdown(&mut self);
}

// ─── P3-5: ExceptionHandler ────────────────────────────────────────────────

/// Action to take after an exception in event processing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ExceptionAction {
	/// Skip the failed event and continue processing the next one.
	Continue,
	/// Halt the processor.
	Halt,
}

/// Handles exceptions thrown by EventHandlers during event processing.
///
/// When an EventHandler panics, the ExceptionHandler decides whether
/// to skip the event and continue, or to halt the processor.
pub trait ExceptionHandler<E>: Send {
	/// Called when an EventHandler throws an exception during `on_event`.
	fn handle_event_exception(
		&mut self,
		error:    &(dyn std::any::Any + Send),
		sequence: Sequence,
		event:    &E,
	) -> ExceptionAction;

	/// Called when an exception occurs during `on_start`.
	fn handle_on_start_exception(&mut self, _error: &(dyn std::any::Any + Send)) -> ExceptionAction {
		ExceptionAction::Halt
	}

	/// Called when an exception occurs during `on_shutdown`.
	fn handle_on_shutdown_exception(&mut self, _error: &(dyn std::any::Any + Send)) {
		// Default: ignore
	}
}

/// Exception handler that halts the processor on any exception (default behavior).
pub struct FatalExceptionHandler;

impl<E> ExceptionHandler<E> for FatalExceptionHandler {
	fn handle_event_exception(
		&mut self,
		_error:   &(dyn std::any::Any + Send),
		_sequence: Sequence,
		_event:   &E,
	) -> ExceptionAction {
		ExceptionAction::Halt
	}
}

/// Exception handler that skips failed events and continues processing.
pub struct IgnoreExceptionHandler;

impl<E> ExceptionHandler<E> for IgnoreExceptionHandler {
	fn handle_event_exception(
		&mut self,
		_error:   &(dyn std::any::Any + Send),
		_sequence: Sequence,
		_event:   &E,
	) -> ExceptionAction {
		ExceptionAction::Continue
	}

	fn handle_on_start_exception(&mut self, _error: &(dyn std::any::Any + Send)) -> ExceptionAction {
		ExceptionAction::Continue
	}
}

/// Exception handler that logs errors to stderr and continues processing.
pub struct LoggingExceptionHandler;

impl<E> ExceptionHandler<E> for LoggingExceptionHandler {
	fn handle_event_exception(
		&mut self,
		_error:   &(dyn std::any::Any + Send),
		sequence: Sequence,
		_event:   &E,
	) -> ExceptionAction {
		eprintln!("disruptor: exception in event handler at sequence {sequence}, continuing");
		ExceptionAction::Continue
	}

	fn handle_on_start_exception(&mut self, _error: &(dyn std::any::Any + Send)) -> ExceptionAction {
		eprintln!("disruptor: exception in on_start, continuing");
		ExceptionAction::Continue
	}

	fn handle_on_shutdown_exception(&mut self, _error: &(dyn std::any::Any + Send)) {
		eprintln!("disruptor: exception in on_shutdown");
	}
}

// ─── P3-2: WorkHandler ─────────────────────────────────────────────────────

/// Handler for work items distributed across a WorkerPool.
///
/// Unlike [`EventHandler`] which processes ALL events, a WorkHandler
/// only processes events assigned to it by the WorkerPool.
/// Each event is processed by exactly one WorkHandler in the pool.
pub trait WorkHandler<E>: Send {
	/// Process a single event.
	fn on_event(&mut self, event: &E, sequence: Sequence);

	/// Called when the WorkerPool encounters an unrecoverable error.
	fn on_timeout(&mut self) {}
}

// ─── P3-1: BatchEventProcessor ─────────────────────────────────────────────

/// The standard event processor that reads events from the RingBuffer
/// and dispatches them to an EventHandler.
///
/// BatchEventProcessor is the primary consumer implementation. It:
/// 1. Waits for sequences to become available via the SequenceBarrier
/// 2. Reads events from the RingBuffer in batches
/// 3. Dispatches events to the EventHandler
/// 4. Updates its own sequence cursor
pub struct BatchEventProcessor<E, H, B>
where
	H: EventHandler<E>,
	B: SequenceBarrier,
{
	sequence:          Arc<disruptor_core::Cursor>,
	event_handler:     H,
	sequence_barrier:  B,
	exception_handler: Option<Box<dyn ExceptionHandler<E>>>,
	running:           std::sync::atomic::AtomicBool,
	_marker:           std::marker::PhantomData<E>,
}

impl<E, H, B> BatchEventProcessor<E, H, B>
where
	H: EventHandler<E>,
	B: SequenceBarrier,
{
	/// Create a new BatchEventProcessor.
	pub fn new(event_handler: H, sequence_barrier: B) -> Self {
		Self {
			sequence: Arc::new(disruptor_core::Cursor::new()),
			event_handler,
			sequence_barrier,
			exception_handler: None,
			running: std::sync::atomic::AtomicBool::new(false),
			_marker: std::marker::PhantomData,
		}
	}

	/// Get the sequence cursor for this processor.
	pub fn sequence(&self) -> &Arc<disruptor_core::Cursor> {
		&self.sequence
	}

	/// Halt the processor (sets running to false and alerts the barrier).
	pub fn halt(&self) {
		self.running.store(false, Ordering::Release);
		self.sequence_barrier.alert();
	}

	/// Set the exception handler for this processor.
	pub fn set_exception_handler(&mut self, handler: Box<dyn ExceptionHandler<E>>) {
		self.exception_handler = Some(handler);
	}

	/// Run the event processing loop. Call this from the consumer thread.
	///
	/// This method blocks until `halt()` is called or the barrier is alerted.
	pub fn run(&mut self, ring_buffer: &disruptor_core::RingBuffer<E>) {
		self.running.store(true, Ordering::Release);
		self.event_handler.on_start();

		let mut next_sequence = self.sequence.relaxed_value() + 1;

		loop {
			match self.sequence_barrier.wait_for(next_sequence) {
				Ok(available) => {
					while next_sequence <= available {
						let end_of_batch = next_sequence == available;
						let event_ptr = ring_buffer.get(next_sequence);
						// SAFETY: SequenceBarrier::wait_for guarantees the producer
						// has published this slot. Acquire fence was issued by the barrier.
						let event = unsafe { &*event_ptr };

						let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
							self.event_handler.on_event(event, next_sequence, end_of_batch);
						}));

						if let Err(panic_payload) = result {
							let action = match &mut self.exception_handler {
								Some(handler) => handler.handle_event_exception(&*panic_payload, next_sequence, event),
								None => ExceptionAction::Halt,
							};
							if action == ExceptionAction::Halt {
								// Store the sequence BEFORE the failed one so downstream
								// consumers don't see the incompletely-processed event.
								self.sequence.store(next_sequence - 1);
								self.event_handler.on_shutdown();
								// Resume the panic so JoinHandle properly reports the failure,
								// rather than silently returning Ok(()).
								std::panic::resume_unwind(panic_payload);
							}
						}

						next_sequence += 1;
					}
					self.sequence.store(available);
				}
				Err(_alert) => {
					if !self.running.load(Ordering::Acquire) {
						break;
					}
					self.sequence_barrier.clear_alert();
				}
			}
		}

		self.event_handler.on_shutdown();
	}
}

// ─── WorkerCursorGuard ─────────────────────────────────────────────────────

/// RAII guard that ensures a worker cursor is advanced even if the handler panics.
/// On panic, the cursor is set to `sequence - 1` (the last successfully processed
/// event) so downstream consumers don't observe the incompletely-processed event.
struct WorkerCursorGuard<'a> {
	cursor:   &'a disruptor_core::Cursor,
	sequence: Sequence,
}

impl Drop for WorkerCursorGuard<'_> {
	fn drop(&mut self) {
		// Advance to the sequence before the failed one to preserve DAG semantics.
		self.cursor.store(self.sequence - 1);
	}
}

// ─── P3-3: WorkerPool ──────────────────────────────────────────────────────

/// A pool of WorkHandlers that process events from the same RingBuffer in parallel.
///
/// Each event is processed by exactly one WorkHandler. The pool uses an
/// atomic sequence counter (CAS) to distribute work items without contention.
///
/// Use WorkerPool when event processing is CPU-bound and you want to
/// parallelize across multiple cores while maintaining exactly-once semantics.
pub struct WorkerPool<E, H>
where
	H: WorkHandler<E>,
{
	ring_buffer:    Arc<disruptor_core::RingBuffer<E>>,
	work_sequence:  Arc<disruptor_core::Cursor>,
	handlers:       Vec<H>,
	worker_cursors: Vec<Arc<disruptor_core::Cursor>>,
	running:        Arc<std::sync::atomic::AtomicBool>,
	_marker:        std::marker::PhantomData<E>,
}

impl<E, H> WorkerPool<E, H>
where
	E: 'static + Send + Sync,
	H: 'static + WorkHandler<E>,
{
	/// Create a new WorkerPool with the given handlers.
	///
	/// The number of workers equals the number of handlers provided.
	pub fn new(ring_buffer: Arc<disruptor_core::RingBuffer<E>>, handlers: Vec<H>) -> Self {
		let n = handlers.len();
		let worker_cursors: Vec<_> = (0..n).map(|_| Arc::new(disruptor_core::Cursor::new())).collect();
		Self {
			ring_buffer,
			work_sequence: Arc::new(disruptor_core::Cursor::new()),
			handlers,
			worker_cursors,
			running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
			_marker: std::marker::PhantomData,
		}
	}

	/// Get the cursors of all workers (for tracking the slowest worker).
	pub fn worker_cursors(&self) -> &[Arc<disruptor_core::Cursor>] {
		&self.worker_cursors
	}

	/// Get the shared work sequence cursor.
	pub fn work_sequence(&self) -> &Arc<disruptor_core::Cursor> {
		&self.work_sequence
	}

	/// Start all worker threads. Each thread runs a work-claiming loop.
	///
	/// `barrier` is the dependent barrier (typically the producer's sequence).
	/// Returns join handles for all worker threads.
	pub fn start<B>(
		&mut self,
		barrier: Arc<B>,
	) -> Vec<std::thread::JoinHandle<()>>
	where
		B: 'static + Barrier + Send + Sync,
	{
		self.running.store(true, Ordering::Release);

		let mut handles = Vec::with_capacity(self.handlers.len());

		// Take handlers out so we can move them into threads.
		let handlers = std::mem::take(&mut self.handlers);

		for (i, mut handler) in handlers.into_iter().enumerate() {
			let ring_buffer    = Arc::clone(&self.ring_buffer);
			let work_sequence  = Arc::clone(&self.work_sequence);
			let worker_cursor  = Arc::clone(&self.worker_cursors[i]);
			let running        = Arc::clone(&self.running);
			let barrier        = Arc::clone(&barrier);

			let handle = std::thread::Builder::new()
				.name(format!("worker-{i}"))
				.spawn(move || {
					let mut processed_sequence: Sequence = -1;

					while running.load(Ordering::Acquire) {
						// CAS to claim the next work item.
						let mut current = work_sequence.relaxed_value();
						loop {
							// Check if any work is available.
							let available = barrier.get_after(current + 1);
							if available <= current {
								// No work yet — yield and retry.
								std::hint::spin_loop();
								break;
							}
							fence(Ordering::Acquire);

							// Try to claim the next sequence.
							match work_sequence.compare_exchange(current, current + 1) {
								Ok(_) => {
									let sequence = current + 1;
									// RAII guard ensures worker_cursor is updated even if
									// handler.on_event() panics, preventing downstream
									// barriers from deadlocking on a stuck cursor.
									let _cursor_guard = WorkerCursorGuard {
										cursor: &worker_cursor,
										sequence,
									};
									let event_ptr = ring_buffer.get(sequence);
									// SAFETY: The barrier confirmed this slot is published.
									// CAS ensures exactly one worker claims each sequence.
									let event = unsafe { &*event_ptr };
									handler.on_event(event, sequence);
									processed_sequence = sequence;
									// Explicit store on success path (guard also stores on drop).
									worker_cursor.store(processed_sequence);
									std::mem::forget(_cursor_guard);
									break;
								}
								Err(new_current) => {
									current = new_current;
								}
							}
						}
					}

					// Store final position.
					if processed_sequence >= 0 {
						worker_cursor.store(processed_sequence);
					}
				})
				.expect("Should spawn worker thread");

			handles.push(handle);
		}

		handles
	}

	/// Signal all workers to stop after they finish their current work item.
	pub fn halt(&self) {
		self.running.store(false, Ordering::Release);
	}

	/// Halt and wait for all workers to finish.
	///
	/// Returns an error if any worker thread panicked. If multiple workers
	/// panicked, the first panic is returned and subsequent ones are logged.
	pub fn drain_and_halt(&self, handles: Vec<std::thread::JoinHandle<()>>) -> Result<(), ConsumerPanic> {
		self.halt();
		let mut first_err = None;
		for h in handles {
			if let Err(payload) = h.join() {
				if first_err.is_none() {
					first_err = Some(ConsumerPanic { payload });
				} else {
					eprintln!("disruptor: additional worker thread panicked during drain_and_halt");
				}
			}
		}
		first_err.map_or(Ok(()), Err)
	}
}
