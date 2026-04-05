//! Warning: this example targets the v5 API and cannot compile with the current v4.
//!
//! Demonstrates WorkerPool: 4 WorkHandlers consuming events in parallel.
//! In v4, multiple handle_events_with calls cause each consumer to read all events (broadcast mode).
//! v5 plans to introduce WorkerPool, which distributes events across multiple workers (each event is processed by only one worker).

#![allow(dead_code)]

/// v5 target trait: work handler, each event is processed by only one worker
///
/// ```ignore
/// pub trait WorkHandler<E>: Send {
///     fn on_event(&mut self, event: &mut E);
/// }
/// ```
trait WorkHandler<E>: Send {
    fn on_event(&mut self, event: &mut E);
}

// -- Event definition --
struct OrderEvent {
    order_id: u64,
    filled:   bool,
}

// -- Worker implementation --
struct OrderWorker {
    worker_id: usize,
}

impl WorkHandler<OrderEvent> for OrderWorker {
    fn on_event(&mut self, event: &mut OrderEvent) {
        event.filled = true;
        println!(
            "[Worker-{}] processed order_id={}, filled={}",
            self.worker_id, event.order_id, event.filled
        );
    }
}

/// v5 target WorkerPool pseudo-struct
///
/// ```ignore
/// pub struct WorkerPool<E, W> { /* ... */ }
///
/// impl<E, W> WorkerPool<E, W> {
///     pub fn new(size: usize, factory: impl FnMut() -> E, wait_strategy: W) -> Self;
///     pub fn with_handlers(self, handlers: Vec<Box<dyn WorkHandler<E>>>) -> Self;
///     pub fn build(self) -> Producer<E>;
/// }
/// ```

/// v5 target usage (pseudocode):
///
/// ```ignore
/// let workers: Vec<Box<dyn WorkHandler<OrderEvent>>> = (0..4)
///     .map(|id| Box::new(OrderWorker { worker_id: id }) as _)
///     .collect();
///
/// let mut producer = WorkerPool::new(1024, || OrderEvent { order_id: 0, filled: false }, BusySpin)
///     .with_handlers(workers)
///     .build();
/// ```
fn main() {
    println!("This example requires the v5 API (WorkerPool + WorkHandler trait) and cannot run with the current v4.");
}
