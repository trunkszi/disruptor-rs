//! Warning: this example targets the v5 API and cannot compile with the current v4.
//!
//! Demonstrates implementing the EventHandler trait with a custom struct.
//! v4 uses closures as event handlers; v5 plans to support trait objects, allowing handlers to encapsulate more complex state and behavior.

#![allow(dead_code)]

/// v5 target trait: event handler
///
/// ```ignore
/// pub trait EventHandler<E>: Send {
///     /// Process a single event.
///     /// sequence: the event's sequence number in the RingBuffer
///     /// end_of_batch: whether this is the last event in the current batch
///     fn on_event(&mut self, event: &E, sequence: i64, end_of_batch: bool);
/// }
/// ```
trait EventHandler<E>: Send {
    fn on_event(&mut self, event: &E, sequence: i64, end_of_batch: bool);
}

// -- Custom event --
struct TradeEvent {
    symbol: &'static str,
    price:  f64,
}

// -- Custom handler with internal statistics state --
struct TradeLogger {
    count: u64,
    total: f64,
}

impl TradeLogger {
    fn new() -> Self {
        Self { count: 0, total: 0.0 }
    }
}

impl EventHandler<TradeEvent> for TradeLogger {
    fn on_event(&mut self, event: &TradeEvent, sequence: i64, end_of_batch: bool) {
        self.count += 1;
        self.total += event.price;
        if end_of_batch {
            let avg = self.total / self.count as f64;
            println!(
                "[TradeLogger] seq={sequence} symbol={} price={:.2} avg={avg:.2}",
                event.symbol, event.price
            );
        }
    }
}

/// v5 target usage (pseudocode):
///
/// ```ignore
/// let mut producer = build_single_producer(64, || TradeEvent { symbol: "", price: 0.0 }, BusySpin)
///     .handle_events_with(TradeLogger::new())   // pass a trait object instead of a closure
///     .build();
/// ```
fn main() {
    println!("This example requires the v5 API (EventHandler trait) and cannot run with the current v4.");
}
