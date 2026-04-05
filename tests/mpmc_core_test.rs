//! MPMC (Multi Producer Multi Consumer) tests extracted from lib.rs.
//!
//! Validates multi-producer multi-consumer topologies: dependent consumers
//! with state, event pollers, and mixed poller/processor configurations.

use std::cell::RefCell;
use std::collections::HashSet;
use std::sync::mpsc;
use std::thread;
use disruptor::*;

#[derive(Debug)]
struct Event {
    num: i64,
}

fn factory() -> impl Fn() -> Event {
    || Event { num: -1 }
}

#[test]
fn mpmc_with_dependent_consumers_and_some_with_state() {
    let (s, r) = mpsc::channel();

    let processor1 = {
        let s = s.clone();
        move |e: &Event, _, _| { s.send(e.num + 0).unwrap(); }
    };
    let processor2 = {
        let s = s.clone();
        move |state: &mut RefCell<i64>, e: &Event, _, _| {
            *state.borrow_mut() += e.num;
            s.send(*state.borrow() + 10).unwrap();
        }
    };
    let processor3 = {
        let s = s.clone();
        move |state: &mut RefCell<i64>, e: &Event, _, _| {
            *state.borrow_mut() += e.num;
            s.send(*state.borrow() + 20).unwrap();
        }
    };
    let processor4 = {
        let s = s.clone();
        move |e: &Event, _, _| { s.send(e.num + 2).unwrap(); }
    };
    let processor5 = {
        let s = s.clone();
        move |e: &Event, _, _| { s.send(e.num + 4).unwrap(); }
    };
    let processor6 = {
        let s = s.clone();
        move |state: &mut RefCell<i64>, e: &Event, _, _| {
            *state.borrow_mut() += e.num;
            s.send(*state.borrow() + 30).unwrap();
        }
    };
    let processor7 = {
        move |state: &mut RefCell<i64>, e: &Event, _, _| {
            *state.borrow_mut() += e.num;
            s.send(*state.borrow() + 40).unwrap();
        }
    };

    let builder      = build_multi_producer(64, factory(), BusySpin);
    let mut producer1 = builder
        .handle_events_with(processor1)
        .handle_events_and_state_with(processor2, || { RefCell::new(0) })
        .and_then()
            .handle_events_and_state_with(processor3, || { RefCell::new(0) })
            .handle_events_with(processor4)
            .and_then()
                .handle_events_with(processor5)
                .handle_events_and_state_with(processor6, || { RefCell::new(0) })
                .handle_events_and_state_with(processor7, || { RefCell::new(0) })
        .build();

    let mut producer2 = producer1.clone();

    thread::scope(|s| {
        s.spawn(move || {
            producer1.publish(|e| e.num = 1);
        });

        s.spawn(move || {
            producer2.publish(|e| e.num = 1);
        });
    });

    let mut result: Vec<i64> = r.iter().collect();
    result.sort();
    assert_eq!(vec![1, 1, 3, 3, 5, 5, 11, 12, 21, 22, 31, 32, 41, 42], result);
}

#[test]
fn mpmc_with_event_pollers() {
    let builder       = build_multi_producer(64, factory(), BusySpin);
    let (mut ep1, b)  = builder.new_event_poller();
    let (mut ep2, b)  = b.and_then().new_event_poller();
    let (mut ep3, b)  = b.and_then().new_event_poller();
    let mut producer1 = b.build();
    let mut producer2 = producer1.clone();

    // Polling before publication should yield no events.
    assert_eq!(ep1.poll().err(), Some(Polling::NoEvents));
    assert_eq!(ep2.poll().err(), Some(Polling::NoEvents));
    assert_eq!(ep3.poll().err(), Some(Polling::NoEvents));

    // Publish two events.
    producer1.publish(|e| { e.num = 1; });
    producer2.publish(|e| { e.num = 2; });

    let expected = HashSet::from([1, 2]);

    {// Only first poller sees events.
        let mut guard_1 = ep1.poll().unwrap();
        assert_eq!(ep2.poll().err(), Some(Polling::NoEvents));
        assert_eq!(ep3.poll().err(), Some(Polling::NoEvents));
        assert_eq!(expected, guard_1.map(|e| e.num).collect::<HashSet<_>>());
    }

    {// Now second poller sees events - here one at a time.
        let mut guard_2 = ep2.take(1).unwrap();
        assert_eq!(ep3.poll().err(), Some(Polling::NoEvents));
        assert_eq!(vec![1], guard_2.map(|e| e.num).collect::<Vec<_>>());
        drop(guard_2);
        // Read next event.
        let mut guard_2 = ep2.take(1).unwrap();
        assert_eq!(vec![2], guard_2.map(|e| e.num).collect::<Vec<_>>());
    }

    {// Now third poller sees both events.
        let mut guard_3 = ep3.poll().unwrap();
        assert_eq!(expected, guard_3.map(|e| e.num).collect::<HashSet<_>>());
    }

    // Dropping the producers should indicate shutdown to all pollers.
    drop(producer1);
    drop(producer2);
    assert_eq!(ep1.poll().err(), Some(Polling::Shutdown));
    assert_eq!(ep2.poll().err(), Some(Polling::Shutdown));
    assert_eq!(ep3.poll().err(), Some(Polling::Shutdown));
}

#[test]
fn mpmc_with_mixed_event_pollers_and_processors() {
    let (s, r)        = mpsc::channel();
    let processor1    = move |e: &Event, _, _| { s.send(e.num).unwrap(); };

    let builder       = build_multi_producer(64, factory(), BusySpin)
        .handle_events_with(processor1);
    let (mut ep1, b)  = builder.new_event_poller();
    let (mut ep2, b)  = b.and_then().new_event_poller();
    let mut producer1 = b.build();
    let mut producer2 = producer1.clone();

    // Polling before publication should yield no events.
    assert_eq!(ep1.poll().err(), Some(Polling::NoEvents));
    assert_eq!(ep2.poll().err(), Some(Polling::NoEvents));

    // Publish two events.
    producer1.publish(|e| { e.num = 1; });
    producer2.publish(|e| { e.num = 2; });
    drop(producer1);
    drop(producer2);

    let expected = vec![1, 2];

    {// Only first poller sees events.
        let mut guard_1 = ep1.poll().unwrap();
        assert_eq!(ep2.poll().err(), Some(Polling::NoEvents));
        assert_eq!(expected, guard_1.map(|e| e.num).collect::<Vec<_>>());
    }
    // Next poll should indicate shutdown to the poller.
    assert_eq!(ep1.poll().err(), Some(Polling::Shutdown));

    {// Now second poller sees events.
        let mut guard_2 = ep2.poll().unwrap();
        assert_eq!(expected, guard_2.map(|e| e.num).collect::<Vec<_>>());
    }

    let mut result: Vec<i64> = r.iter().collect();
    result.sort();
    assert_eq!(expected, result);
}
