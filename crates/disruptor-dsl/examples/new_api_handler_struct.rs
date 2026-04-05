//! Using a struct-based EventHandler (instead of closures) with lifecycle callbacks.

use disruptor_dsl::prelude::*;
use disruptor_consumer::EventHandler;

struct Event {
    price: f64,
}

/// A structured event handler with initialization and cleanup.
struct PriceAggregator {
    sum: f64,
    count: u64,
}

impl EventHandler<Event> for PriceAggregator {
    fn on_event(&mut self, event: &Event, _sequence: Sequence, end_of_batch: bool) {
        self.sum += event.price;
        self.count += 1;
        if end_of_batch {
            println!("Batch avg: {:.2} ({} events)", self.sum / self.count as f64, self.count);
        }
    }

    fn on_start(&mut self) {
        println!("PriceAggregator started");
        self.sum = 0.0;
        self.count = 0;
    }

    fn on_shutdown(&mut self) {
        println!("PriceAggregator shutdown — total: {:.2} over {} events", self.sum, self.count);
    }
}

fn main() {
    let mut handler = PriceAggregator { sum: 0.0, count: 0 };
    handler.on_start();

    let mut producer = Disruptor::single(64, || Event { price: 0.0 }, BusySpin)
        .handle_events_with(move |e: &Event, seq: Sequence, eob: bool| {
            handler.on_event(e, seq, eob);
        })
        .build();

    for i in 0..50 {
        producer.publish(|e| {
            e.price = i as f64 * 0.5;
        });
    }

    let _ = producer.shutdown();
}
