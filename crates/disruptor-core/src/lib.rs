#![deny(missing_docs)]
//! Core primitives for the disruptor-rs framework.
//!
//! This crate provides the fundamental building blocks:
//! - [`Sequence`] — the sequence number type
//! - [`Cursor`] — cache-padded atomic sequence cursor
//! - [`RingBuffer`] — pre-allocated ring buffer
//! - [`Barrier`] — sequence coordination trait
//! - [`SingleConsumerBarrier`] / [`MultiConsumerBarrier`] — consumer tracking barriers
//! - [`EventFactory`] — trait for pre-populating ring buffer slots
//!
//! ## `no_std` support
//!
//! This crate supports `#![no_std]` with the `alloc` feature (for `Box` / `Vec`).
//! Disable default features and enable `alloc`:
//! ```toml
//! disruptor-core = { version = "0.1", default-features = false, features = ["alloc"] }
//! ```

#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
extern crate alloc;

mod cursor;
mod ringbuffer;
mod barrier;

pub use cursor::Cursor;
pub use ringbuffer::RingBuffer;
pub use barrier::{Barrier, NONE, INITIAL_SEQUENCE, SingleConsumerBarrier, MultiConsumerBarrier, SequenceBarrier, AlertError, Alert};

/// The type for Sequence numbers in the Ring Buffer ([`i64`]).
pub type Sequence = i64;

/// Factory for pre-populating the RingBuffer with events.
///
/// The RingBuffer pre-allocates all event slots at construction time.
/// This trait defines how each slot is initialized.
///
/// A blanket implementation is provided for closures (`FnMut() -> E`),
/// so existing code using closure factories continues to work unchanged.
///
/// # Examples
///
/// ```
/// use disruptor_core::EventFactory;
///
/// // Using a closure (blanket impl):
/// let mut factory = || String::from("default");
/// assert_eq!(factory.create(), "default");
///
/// // Using a struct:
/// struct MyFactory;
/// impl EventFactory<i32> for MyFactory {
///     fn create(&mut self) -> i32 { 0 }
/// }
/// let mut f = MyFactory;
/// assert_eq!(f.create(), 0);
/// ```
pub trait EventFactory<E> {
	/// Create a new event instance for a RingBuffer slot.
	fn create(&mut self) -> E;
}

/// Blanket implementation for closures.
impl<E, F: FnMut() -> E> EventFactory<E> for F {
	fn create(&mut self) -> E {
		self()
	}
}
