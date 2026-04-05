//! Async adapters for the disruptor-rs framework.
//!
//! These traits and types enable disruptor-rs to be used in async contexts
//! (tokio, async-std, etc.) without depending on any specific runtime.
//!
//! Enable with the `async` feature flag:
//! ```toml
//! disruptor-consumer = { version = "0.1", features = ["async"] }
//! ```

use core::future::Future;
use disruptor_core::Sequence;

/// Async wait strategy that yields to the async runtime instead of blocking.
///
/// This is the async equivalent of [`WaitStrategy`](disruptor_wait::WaitStrategy).
/// Implementations should yield to the runtime when no events are available,
/// allowing other tasks to make progress.
pub trait AsyncWaitStrategy: Send + Sync {
	/// Asynchronously wait for the given sequence to become available.
	fn wait_for(&self, sequence: Sequence) -> impl Future<Output = ()> + Send;
}

/// Async event handler for processing events in an async context.
///
/// This is the async equivalent of [`EventHandler`](crate::EventHandler).
pub trait AsyncEventHandler<E>: Send {
	/// Asynchronously process an event.
	fn on_event(
		&mut self,
		event: &E,
		sequence: Sequence,
		end_of_batch: bool,
	) -> impl Future<Output = ()> + Send;

	/// Called once when the processing task starts.
	fn on_start(&mut self) -> impl Future<Output = ()> + Send {
		async {}
	}

	/// Called once before the processing task exits.
	fn on_shutdown(&mut self) -> impl Future<Output = ()> + Send {
		async {}
	}
}

/// A yield-based async wait strategy that calls `std::future::pending` style yielding.
///
/// This is a simple default strategy that yields once per wait cycle.
#[derive(Clone, Copy)]
pub struct YieldAsyncWaitStrategy;

impl AsyncWaitStrategy for YieldAsyncWaitStrategy {
	async fn wait_for(&self, _sequence: Sequence) {
		// Yield to the runtime once, allowing other tasks to progress.
		core::future::poll_fn::<(), _>(|cx| {
			cx.waker().wake_by_ref();
			core::task::Poll::Pending
		}).await;
	}
}
