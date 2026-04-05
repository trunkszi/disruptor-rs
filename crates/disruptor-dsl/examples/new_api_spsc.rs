//! Single Producer Single Consumer using the new Disruptor API.

use disruptor_dsl::prelude::*;

struct Event {
    price: f64,
}

fn main() {
    let mut producer = Disruptor::single(64, || Event { price: 0.0 }, BusySpin)
        .handle_events_with(|e: &Event, seq: Sequence, eob: bool| {
            if eob {
                println!("[seq={seq}] price={:.2} (end of batch)", e.price);
            }
        })
        .build();

    for i in 0..20 {
        producer.publish(|e| {
            e.price = i as f64 * 1.5;
        });
    }

    let result = producer.shutdown();
    println!("Shutdown: {:?}", result);
}
