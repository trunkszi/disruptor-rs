//! ⚠️ This example targets the v5 API and cannot be compiled with the current v4.
//!
//! Demonstrates ExceptionHandler: recovery handling when an event handler panics.
//! In v4, a consumer thread panic renders the entire Disruptor unusable.
//! v5 plans to introduce ExceptionHandler, allowing users to define custom exception recovery strategies.

#![allow(dead_code)]

use std::any::Any;

/// v5 target trait: exception handler
///
/// ```ignore
/// pub trait ExceptionHandler<E>: Send {
///     /// Called when an event handler panics.
///     /// Returns true to continue processing subsequent events, false to stop the consumer.
///     fn handle_event_exception(
///         &mut self,
///         error:    Box<dyn Any + Send>,
///         sequence: i64,
///         event:    &E,
///     ) -> bool;
/// }
/// ```
trait ExceptionHandler<E>: Send {
    fn handle_event_exception(
        &mut self,
        error:    Box<dyn Any + Send>,
        sequence: i64,
        event:    &E,
    ) -> bool;
}

// -- Event definition --
struct PriceEvent {
    price: f64,
}

// -- Custom exception handler: log error and continue --
struct LogAndContinue {
    error_count: u64,
}

impl ExceptionHandler<PriceEvent> for LogAndContinue {
    fn handle_event_exception(
        &mut self,
        _error:   Box<dyn Any + Send>,
        sequence: i64,
        event:    &PriceEvent,
    ) -> bool {
        self.error_count += 1;
        eprintln!(
            "[ExceptionHandler] error #{}, seq={sequence} price={:.2}, continuing...",
            self.error_count, event.price
        );
        true // continue consuming
    }
}

/// v5 target usage (pseudocode):
///
/// ```ignore
/// let mut producer = build_single_producer(64, || PriceEvent { price: 0.0 }, BusySpin)
///     .handle_events_with(|e: &PriceEvent, seq, _| {
///         if e.price < 0.0 { panic!("negative price!"); }
///     })
///     .with_exception_handler(LogAndContinue { error_count: 0 })
///     .build();
/// ```
fn main() {
    println!("This example requires the v5 API (ExceptionHandler trait) and cannot run on the current v4.");
}
