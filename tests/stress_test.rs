//! Stress test extracted from lib.rs.
//!
//! Validates correctness under high contention with multiple producers
//! and multiple concurrent consumers.

use std::sync::mpsc;
use std::thread;
use disruptor::*;

#[cfg_attr(miri, ignore)] // Miri disabled due to excessive runtime.
#[test]
fn stress_test() {
    #[derive(Debug)]
    struct StressEvent {
        i: Sequence,
        a: i64,
        b: i64,
        s: String,
    }

    let (s_seq, r_seq)         = mpsc::channel();
    let num_events             = 250_000;
    let producers              = 4;
    let consumers              = 3;

    let mut processors: Vec<_> = (0..consumers).into_iter().map(|pid| {
        let s_seq        = s_seq.clone();
        let mut prev_seq = -1;
        Some(move |e: &StressEvent, sequence, _| {
            assert_eq!(e.a, e.i - 5);
            assert_eq!(e.b, e.i + 7);
            assert_eq!(e.s, format!("Blackbriar {}", e.i).to_string());
            assert_eq!(sequence, prev_seq + 1);

            prev_seq                 = sequence;
            let sequence_seen_by_pid = sequence*consumers + pid;
            s_seq.send(sequence_seen_by_pid).expect("Should send.");
        })
    }).collect();

    // Drop unused Sender.
    drop(s_seq);

    let factory = || StressEvent {
            i: -1,
            a: -1,
            b: -1,
            s: "".to_string()
        };

    let producer = build_multi_producer(1 << 16, factory, BusySpin)
        .handle_events_with(processors[0].take().unwrap())
        .handle_events_with(processors[1].take().unwrap())
        .handle_events_with(processors[2].take().unwrap())
        .build();

    thread::scope(|s| {
        for _ in 0..producers {
            let mut producer = producer.clone();
            s.spawn(move || {
                for i in 0..num_events {
                    producer.publish(|e| {
                        e.i = i;
                        e.a = i - 5;
                        e.b = i + 7;
                        e.s = format!("Blackbriar {}", i).to_string();
                    });
                }
            });
        }
        drop(producer); // Drop excess producer not used.
    });

    let expected_sequence_reads    = consumers*num_events*producers;
    let mut seen_sequences: Vec<_> = r_seq.iter().collect();

    assert_eq!(expected_sequence_reads as usize, seen_sequences.len());
    // Assert that each consumer saw each sequence number.
    seen_sequences.sort();
    for seq_seen_by_pid in 0..expected_sequence_reads {
        assert_eq!(seq_seen_by_pid, seen_sequences[seq_seen_by_pid as usize]);
    }
}
