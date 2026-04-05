//! Low latency library for inter-thread communication.
//!
//! Use it when a single thread is not enough and you need multiple threads to communicate
//! with the lowest latency possible.
//!
//! # General Usage
//!
//! The usage can be divided into three stages:
//! 1. **Setup:** Build the Disruptor and setup consumers including any interdependencies.
//! 2. **Publish:** Publish into the Disruptor.
//! 3. **Shutdown:** Stop all consumer threads and drop the Disruptor and the consumer(s).
//!
//! The Disruptor in this library can only be used once. I.e. it cannot be rewound and restarted.
//!
//! There is two ways to process events:
//! 1. Managed threads where the Disruptor owns and manages the processing thread(s) for the convenience of the library users.
//! 2. Event polling API where the library user controls the processing thread(s).
//!
//! ## Setup
//!
//! When the Disruptor is created, you choose whether publication to the Disruptor will happen from
//! one or multiple threads via [`Producer`] handles. The size of the
//! RingBuffer is also specified and you have to provide a "factory" closure for initializing the
//! events inside the RingBufffer. Then all consumers are added.
//!
//! Use either [`build_single_producer`] or [`build_multi_producer`] to get started.
//!
//! ## Publish
//!
//! Once the Disruptor is built, you have a [`Producer`] "handle" which can be used to publish into
//! the Disruptor. In case of a multi producer Disruptor, the [`Producer`] can be cloned so that
//! each publishing thread has its own handle.
//!
//! ## Shutdown
//!
//! Finally, when there's no more events to publish and the last Producer goes out of scope, all events
//! published are processed and then the processing thread(s) will be stopped and the entire Disruptor
//! will be dropped including consumers.
//!
//! # Examples
//!
//! ### Basic Usage
//!
//! ```
//! use disruptor::*;
//!
//! // *** Phase SETUP ***
//!
//! // The data entity on the ring buffer.
//! struct Event {
//!     price: f64
//! }
//!
//! // Define a factory for populating the ring buffer with events.
//! let factory = || Event { price: 0.0 };
//!
//! // Define a closure for processing events. A thread, controlled by the disruptor, will run this
//! // processor closure each time an event is published.
//! let processor = |e: &Event, sequence: Sequence, end_of_batch: bool| {
//!     // Process the Event `e` published at `sequence`.
//!     // If `end_of_batch` is false, you can batch up events until it's invoked with
//!     // `end_of_batch` being true.
//! };
//!
//! // Create a Disruptor with a ring buffer of size 8 and use the `BusySpin` wait strategy.
//! let mut producer = build_single_producer(8, factory, BusySpin)
//!     .handle_events_with(processor)
//!     .build();
//!
//! // *** Phase PUBLISH ***
//!
//! // Publish into the Disruptor.
//! for i in 0..10 {
//!     producer.publish(|e| {
//!         e.price = i as f64;
//!     });
//! }
//! // *** Phase SHUTDOWN ***
//!
//! // The Producer instance goes out of scope and the Disruptor, the processor (consumer) and
//! // the Producer are dropped.
//! ```
//!
//! ### Batch Publication:
//!
//! ```
//! use disruptor::*;
//!
//! // The data entity on the ring buffer.
//! struct Event {
//!     price: f64
//! }
//!
//! let factory = || Event { price: 0.0 };
//!
//! let processor = |e: &Event, sequence: Sequence, end_of_batch: bool| {
//!     // Processing logic.
//! };
//!
//! let mut producer = build_single_producer(8, factory, BusySpin)
//!     .handle_events_with(processor)
//!     .build();
//!
//! // Batch publish into the Disruptor - 5 events at a time.
//! for i in 0..10 {
//!     producer.batch_publish(5, |iter| {
//!         // `iter` is guaranteed to yield 5 events.
//!         for e in iter {
//!             e.price = i as f64;
//!         }
//!     });
//! }
//! ```
//!
//! ###  Multiple Producers and Multiple, Pinned Consumers
//! ```
//! use disruptor::*;
//! use std::thread;
//!
//! // The event on the ring buffer.
//! struct Event {
//!     price: f64
//! }
//!
//! # #[cfg(miri)] fn main() {}
//! # #[cfg(not(miri))]
//! fn main() {
//!     # // Skip on machines with fewer than 4 cores (e.g. CI runners).
//!     # if core_affinity::get_core_ids().map_or(true, |c| c.len() < 4) { return; }
//!     // Factory closure for initializing events in the Ring Buffer.
//!     let factory = || Event { price: 0.0 };
//!
//!     // Closure for processing events.
//!     let h1 = |e: &Event, sequence: Sequence, end_of_batch: bool| {
//!         // Processing logic here.
//!     };
//!     let h2 = |e: &Event, sequence: Sequence, end_of_batch: bool| {
//!         // Some processing logic here.
//!     };
//!     let h3 = |e: &Event, sequence: Sequence, end_of_batch: bool| {
//!         // More processing logic here.
//!     };
//!
//!     let mut producer1 = disruptor::build_multi_producer(64, factory, BusySpin)
//!         // `h2` handles events concurrently with `h1`.
//!         .pin_at_core(1).handle_events_with(h1)
//!         .pin_at_core(2).handle_events_with(h2)
//!             .and_then()
//!             // `h3` handles events after `h1` and `h2`.
//!             .pin_at_core(3).handle_events_with(h3)
//!         .build();
//!
//!     // Create another producer.
//!     let mut producer2 = producer1.clone();
//!
//!     // Publish into the Disruptor.
//!     thread::scope(|s| {
//!         s.spawn(move || {
//!             for i in 0..10 {
//!                 producer1.publish(|e| {
//!                     e.price = i as f64;
//!                 });
//!             }
//!         });
//!         s.spawn(move || {
//!             for i in 10..20 {
//!                 producer2.publish(|e| {
//!                     e.price = i as f64;
//!                 });
//!             }
//!         });
//!     });
//!     // At this point, the Producers instances go out of scope and when the
//!     // processors are done handling all events, the Disruptor is dropped
//!     // as well.
//! }
//! ```
//!
//! ### Adding Custom State That is Neither `Send` Nor `Sync`
//!
//! ```
//! use std::{cell::RefCell, rc::Rc};
//! use disruptor::*;
//!
//! // The event on the ring buffer.
//! struct Event {
//!     price: f64
//! }
//!
//! // Your custom state.
//! #[derive(Default)]
//! struct State {
//!     data: Rc<RefCell<i32>>
//! }
//!
//! let factory = || Event { price: 0.0 };
//! let initial_state = || State::default();
//!
//! // Closure for processing events *with* state.
//! let processor = |s: &mut State, e: &Event, _: Sequence, _: bool| {
//!     // Mutate your custom state:
//!     *s.data.borrow_mut() += 1;
//! };
//!
//! let size = 64;
//! let mut producer = disruptor::build_single_producer(size, factory, BusySpin)
//!     .handle_events_and_state_with(processor, initial_state)
//!     .build();
//!
//! // Publish into the Disruptor via the `Producer` handle.
//! for i in 0..10 {
//!     producer.publish(|e| {
//!         e.price = i as f64;
//!     });
//! }
//! ```
//!
//! ### Event Polling API
//!
//! ```
//! use disruptor::*;
//!
//! // The data entity on the ring buffer.
//! #[derive(Debug)]
//! struct Event {
//!     price: f64
//! }
//!
//! let factory = || Event { price: 0.0 };
//! let builder = build_single_producer(8, factory, BusySpin);
//! let (mut event_poller, builder) = builder.new_event_poller();
//! let mut producer = builder.build();
//!
//! // Publish into the Disruptor.
//! for i in 0..5 {
//!     producer.publish(|e| {
//!         e.price = i as f64;
//!     });
//! }
//! drop(producer); // Drop the producer to signal that no more events will be published.
//!
//! // Process events.
//! loop {
//!     // 1. Either poll for all available events.
//!     match event_poller.poll() {
//!         Ok(mut guard) => {
//!             // Batch process events if efficient in your use case.
//!             let batch_size = (&mut guard).len();
//!             // Guard is an `Iterator` so read events by iterating.
//!             for event in &mut guard {
//!                 println!("Processing event: {:?}", event);
//!             }
//!             // At this point the EventGuard is dropped,
//!             // signaling the Disruptor that the events have been processed.
//!         },
//!         Err(Polling::NoEvents) => { /* Do other work or try again. */ },
//!         Err(Polling::Shutdown) => { break; }, // Exit the loop if the Disruptor is shut down.
//!         Err(_) => unreachable!(),
//!     }
//!     // 2. Or limit the maximum number of events yielded per poll.
//!     match event_poller.take(64) {
//!         Ok(mut guard) => {
//!             // Max 64 events (or fewer if less available) are yielded.
//!             for event in &mut guard {
//!                 println!("Processing event: {:?}", event);
//!             }
//!         },
//!         Err(Polling::NoEvents) => { },
//!         Err(Polling::Shutdown) => { break; },
//!         Err(_) => unreachable!(),
//!     }
//! }
//! ```
//!
//! ### Diamond / DAG Topology
//!
//! You can build DAG topologies where a branch runs in parallel with multiple stages, and is only
//! joined back at a later point.
//!
//! For example, a journaling poller `J` can run in parallel with a multi-stage pipeline, and the
//! final stage can wait for both the pipeline and `J`:
//!
//! ```text
//!            /-- A -> B -> C --\
//! Producer -> ------- J --------> D
//! ```
//!
//! Use [`SPBuilder::new_branch`](builder::single::SPBuilder::new_branch) /
//! [`MPBuilder::new_branch`](builder::multi::MPBuilder::new_branch) to create a branch,
//! take the returned [`Branch`] and then use
//! [`SPBuilder::join`](builder::single::SPBuilder::join) /
//! [`MPBuilder::join`](builder::multi::MPBuilder::join) to join it into the
//! downstream dependency chain. Joining returns an [`EventPoller`] for the branch
//! which can be used to poll events in the branch. (This ensures UB cannot happen.)

#![deny(rustdoc::broken_intra_doc_links)]
#![deny(missing_docs)]

/// The type for Sequence numbers in the Ring Buffer ([`i64`]).
pub type Sequence = i64;

pub mod wait_strategies;
pub mod builder;
mod affinity;
mod barrier;
mod consumer;
mod cursor;
mod ringbuffer;
mod producer;

pub use crate::builder::{build_single_producer, build_multi_producer, ProcessorSettings};
pub use crate::producer::{Producer, RingBufferFull, MissingFreeSlots};
pub use crate::wait_strategies::{BusySpin, BusySpinWithSpinLoopHint};
pub use crate::producer::{single::{SingleProducer, SingleProducerBarrier}, multi::{MultiProducer,  MultiProducerBarrier}};
pub use crate::consumer::{SingleConsumerBarrier, MultiConsumerBarrier, ConsumerPanic};
pub use crate::consumer::event_poller::{EventPoller, Polling, EventGuard, Branch};
