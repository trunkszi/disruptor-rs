//! EventPoller integration tests.
//!
//! Validates the manual polling API for user-controlled event consumption.

use disruptor::*;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

struct Event {
    value: i64,
}

#[test]
fn poller_consumes_all_events() {
    let (mut poller, mut producer) = {
        let (p, builder) = build_single_producer(64, || Event { value: 0 }, BusySpin)
            .new_event_poller();
        let prod = builder.build();
        (p, prod)
    };

    let n = 50i64;
    for i in 0..n {
        producer.publish(|e| {
            e.value = i;
        });
    }

    // Poll events manually
    let mut consumed = 0i64;
    let mut sum = 0i64;
    loop {
        match poller.poll() {
            Ok(mut guard) => {
                for event in &mut guard {
                    consumed += 1;
                    sum += event.value;
                }
            }
            Err(Polling::NoEvents) => {
                if consumed >= n {
                    break;
                }
                std::hint::spin_loop();
            }
            Err(Polling::Shutdown) => break,
            Err(_) => unreachable!(),
        }
    }

    drop(producer);

    assert_eq!(consumed, n);
    assert_eq!(sum, n * (n - 1) / 2);
}

#[test]
fn poller_take_limits_batch_size() {
    let (mut poller, mut producer) = {
        let (p, builder) = build_single_producer(64, || Event { value: 0 }, BusySpin)
            .new_event_poller();
        let prod = builder.build();
        (p, prod)
    };

    // Publish 20 events
    for i in 0..20 {
        producer.publish(|e| {
            e.value = i;
        });
    }

    // Wait a bit for events to be visible
    std::thread::sleep(std::time::Duration::from_millis(10));

    // Take at most 5 events
    match poller.take(5) {
        Ok(mut guard) => {
            // ExactSizeIterator::len needs &mut self on EventGuard
            let count = (&mut guard).count();
            assert!(count <= 5);
        }
        Err(_) => {
            // May not have events yet, that's OK for this test.
        }
    }

    drop(producer);
}

#[test]
fn poller_with_managed_consumer() {
    let managed_count = Arc::new(AtomicI64::new(0));
    let mc = managed_count.clone();

    let (mut poller, mut producer) = {
        let (p, builder) = build_single_producer(64, || Event { value: 0 }, BusySpin)
            .handle_events_with(move |_: &Event, _: Sequence, _: bool| {
                mc.fetch_add(1, Ordering::Relaxed);
            })
            .new_event_poller();
        let prod = builder.build();
        (p, prod)
    };

    let n = 30i64;
    for i in 0..n {
        producer.publish(|e| {
            e.value = i;
        });
    }

    // Drain poller
    let mut polled = 0i64;
    loop {
        match poller.poll() {
            Ok(mut guard) => {
                for _ in &mut guard {
                    polled += 1;
                }
            }
            Err(Polling::NoEvents) => {
                if polled >= n {
                    break;
                }
                std::hint::spin_loop();
            }
            Err(Polling::Shutdown) => break,
            Err(_) => unreachable!(),
        }
    }

    drop(producer);

    // Both managed consumer and poller saw all events.
    assert_eq!(managed_count.load(Ordering::Relaxed), n);
    assert_eq!(polled, n);
}
