//! Branch / DAG topology tests extracted from lib.rs.
//!
//! Validates out-of-band branch creation, join semantics, back-pressure
//! through branch pollers, and multi-branch fan-out topologies.

use std::collections::HashSet;
use disruptor::*;

#[derive(Debug)]
struct Event {
    num: i64,
}

fn factory() -> impl Fn() -> Event {
    || Event { num: -1 }
}

#[test]
fn spsc_branch_from_producer_joined_after_multiple_stages() {
    let mut builder = build_single_producer(8, factory(), BusySpin);

    // Create out-of-band branch, depending on the producer.
    let b = builder.new_branch();

    let _unused = builder.new_branch(); // Unused branch should not cause any issues.

    // R1 depends on producer (main flow).
    let (mut ep_r1, builder) = builder.new_event_poller();

    // ME depends on R1.
    let (mut ep_me, builder) = builder.and_then().new_event_poller();

    // R2 depends on ME.
    let (mut ep_r2, builder) = builder.and_then().new_event_poller();

    // Join J back into the main flow (J also depends on ME).
    let (mut ep_j, builder) = builder.join(b);

    let (mut ep_report, builder) = builder.and_then().new_event_poller();

    let mut producer = builder.build();

    // Publish three events.
    producer.publish(|e| { e.num = 10; });
    producer.publish(|e| { e.num = 20; });
    producer.publish(|e| { e.num = 30; });
    drop(producer);

    let expected = vec![10, 20, 30];

    // Downstream stages are blocked until upstream stages advance.
    assert_eq!(ep_me.poll().err(),     Some(Polling::NoEvents));
    assert_eq!(ep_r2.poll().err(),     Some(Polling::NoEvents));
    assert_eq!(ep_report.poll().err(), Some(Polling::NoEvents));

    // J is independent of R1/ME/R2.
    {
        let mut guard = ep_j.poll().unwrap();
        assert_eq!(expected, guard.map(|e| e.num).collect::<Vec<_>>());
    }
    // Still blocked because R2 hasn't advanced.
    assert_eq!(ep_report.poll().err(), Some(Polling::NoEvents));

    // Progress the main pipeline.
    {
        let mut guard = ep_r1.poll().unwrap();
        assert_eq!(expected, guard.map(|e| e.num).collect::<Vec<_>>());
    }
    {
        let mut guard = ep_me.poll().unwrap();
        assert_eq!(expected, guard.map(|e| e.num).collect::<Vec<_>>());
    }
    {
        let mut guard = ep_r2.poll().unwrap();
        assert_eq!(expected, guard.map(|e| e.num).collect::<Vec<_>>());
    }

    // Now report can proceed after both R2 and J.
    {
        let mut guard = ep_report.poll().unwrap();
        assert_eq!(expected, guard.map(|e| e.num).collect::<Vec<_>>());
    }

    assert_eq!(ep_j.poll().err(),      Some(Polling::Shutdown));
    assert_eq!(ep_r1.poll().err(),     Some(Polling::Shutdown));
    assert_eq!(ep_me.poll().err(),     Some(Polling::Shutdown));
    assert_eq!(ep_r2.poll().err(),     Some(Polling::Shutdown));
    assert_eq!(ep_report.poll().err(), Some(Polling::Shutdown));
}

#[test]
fn mpmc_branch_from_producer_joined_after_multiple_stages() {
    let mut builder = build_multi_producer(64, factory(), BusySpin);

    // Create out-of-band branch B, depending on the producer.
    let b = builder.new_branch();

    // R1 depends on producer.
    let (mut ep_r1, builder) = builder.new_event_poller();

    // ME depends on R1.
    let (mut ep_me, builder) = builder.and_then().new_event_poller();

    // R2 depends on ME.
    let (mut ep_r2, builder) = builder.and_then().new_event_poller();

    // Join branch back into the main flow (B depends on ME).
    let (mut ep_b, builder) = builder.join(b);

    let (mut ep_report, builder) = builder.and_then().new_event_poller();

    let mut producer1 = builder.build();
    let mut producer2 = producer1.clone();

    // Publish two events.
    producer1.publish(|e| { e.num = 1; });
    producer2.publish(|e| { e.num = 2; });
    drop(producer1);
    drop(producer2);

    let expected = HashSet::from([1, 2]);

    // Downstream stages are blocked until upstream stages advance.
    assert_eq!(ep_me.poll().err(),     Some(Polling::NoEvents));
    assert_eq!(ep_r2.poll().err(),     Some(Polling::NoEvents));
    assert_eq!(ep_report.poll().err(), Some(Polling::NoEvents));

    // B is independent of R1/ME/R2.
    {
        let mut guard = ep_b.poll().unwrap();
        assert_eq!(expected, guard.map(|e| e.num).collect::<HashSet<_>>());
    }
    // Still blocked because R2 hasn't advanced.
    assert_eq!(ep_report.poll().err(), Some(Polling::NoEvents));

    // Progress the main pipeline.
    {
        let mut guard = ep_r1.poll().unwrap();
        assert_eq!(expected, guard.map(|e| e.num).collect::<HashSet<_>>());
    }
    {
        let mut guard = ep_me.poll().unwrap();
        assert_eq!(expected, guard.map(|e| e.num).collect::<HashSet<_>>());
    }
    {
        let mut guard = ep_r2.poll().unwrap();
        assert_eq!(expected, guard.map(|e| e.num).collect::<HashSet<_>>());
    }

    // Now report can proceed after both R2 and B.
    {
        let mut guard = ep_report.poll().unwrap();
        assert_eq!(expected, guard.map(|e| e.num).collect::<HashSet<_>>());
    }

    assert_eq!(ep_b.poll().err(),      Some(Polling::Shutdown));
    assert_eq!(ep_r1.poll().err(),     Some(Polling::Shutdown));
    assert_eq!(ep_me.poll().err(),     Some(Polling::Shutdown));
    assert_eq!(ep_r2.poll().err(),     Some(Polling::Shutdown));
    assert_eq!(ep_report.poll().err(), Some(Polling::Shutdown));
}

#[test]
fn spsc_back_pressure_through_out_of_band_branch_poller() {
    let mut builder = build_single_producer(4, factory(), BusySpin);

    // Create out-of-band branch B, depending on the producer.
    let b = builder.new_branch();

    // Join B back into the main flow and get Event Poller.
    let (mut ep_b, builder) = builder.join(b);
    // A depends on B.
    let (mut ep_a, builder) = builder.and_then().new_event_poller();

    let mut producer = builder.build();

    // Fill the ring buffer (size 4).
    for i in 0..4 {
        producer.try_publish(|e| e.num = i).expect("Should publish.");
    }

    // Buffer full: no consumers have advanced yet.
    assert_eq!(RingBufferFull, producer.try_publish(|e| e.num = 99).err().unwrap());

    // Poll B → advances to 3, but A is still at -1.
    {
        let mut g = ep_b.poll().unwrap();
        assert_eq!(vec![0, 1, 2, 3], g.map(|e| e.num).collect::<Vec<_>>());
    }

    // Still full because A causes producer back-pressure.
    assert_eq!(RingBufferFull, producer.try_publish(|e| e.num = 99).err().unwrap());

    // Poll A → advances to sequence 3, Producer can now publish.
    {
        let mut g = ep_a.poll().unwrap();
        assert_eq!(vec![0, 1, 2, 3], g.map(|e| e.num).collect::<Vec<_>>());
    }

    // Now the 5th publish succeeds.
    producer.try_publish(|e| e.num = 100).expect("Should publish after all consumers advanced.");

    // Shutdown.
    drop(producer);

    // Drain the remaining event(s) before asserting shutdown.
    {
        let mut g = ep_b.poll().unwrap();
        assert_eq!(vec![100], g.map(|e| e.num).collect::<Vec<_>>());
    }
    {
        let mut g = ep_a.poll().unwrap();
        assert_eq!(vec![100], g.map(|e| e.num).collect::<Vec<_>>());
    }

    assert_eq!(ep_a.poll().err(), Some(Polling::Shutdown));
    assert_eq!(ep_b.poll().err(), Some(Polling::Shutdown));
}

#[test]
fn spsc_triple_branch_fan_out_and_join() {
    let mut builder = build_single_producer(8, factory(), BusySpin);

    // Branch out from the producer.
    let b1 = builder.new_branch();
    let b2 = builder.new_branch();
    let b3 = builder.new_branch();

    // A depends on producer.
    let (mut ep_a, builder) = builder.new_event_poller();

    // Join branches B1/B2/B3 into main flow.
    let (mut ep_b1, builder) = builder.join(b1);
    let (mut ep_b2, builder) = builder.join(b2);
    let (mut ep_b3, builder) = builder.join(b3);

    let builder = builder.and_then();
    let (mut ep_report, builder) = builder.new_event_poller();

    let mut producer = builder.build();

    // Publish 3 events.
    for i in 1..=3 {
        producer.publish(|e| { e.num = i; });
    }
    drop(producer);

    let expected = vec![1, 2, 3];

    // Report is blocked until A and all branches advance.
    assert_eq!(ep_report.poll().err(), Some(Polling::NoEvents));

    {
        let mut g = ep_a.poll().unwrap();
        assert_eq!(expected, g.map(|e| e.num).collect::<Vec<_>>());
    }
    {
        let mut g = ep_b1.poll().unwrap();
        assert_eq!(expected, g.map(|e| e.num).collect::<Vec<_>>());
    }
    {
        let mut g = ep_b2.poll().unwrap();
        assert_eq!(expected, g.map(|e| e.num).collect::<Vec<_>>());
    }
    // Still blocked on B3.
    assert_eq!(ep_report.poll().err(), Some(Polling::NoEvents));
    {
        let mut g = ep_b3.poll().unwrap();
        assert_eq!(expected, g.map(|e| e.num).collect::<Vec<_>>());
    }
    {
        let mut g = ep_report.poll().unwrap();
        assert_eq!(expected, g.map(|e| e.num).collect::<Vec<_>>());
    }

    assert_eq!(ep_a.poll().err(),      Some(Polling::Shutdown));
    assert_eq!(ep_b1.poll().err(),     Some(Polling::Shutdown));
    assert_eq!(ep_b2.poll().err(),     Some(Polling::Shutdown));
    assert_eq!(ep_b3.poll().err(),     Some(Polling::Shutdown));
    assert_eq!(ep_report.poll().err(), Some(Polling::Shutdown));
}
