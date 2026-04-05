//! Fine-grained unit tests for consumer types.

use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use disruptor_core::*;
use disruptor_consumer::*;

struct Event { value: i64 }

// ─── Consumer join-twice is safe ───────────────────────────────────────────

#[test]
fn consumer_join_twice_returns_ok() {
    let handle = std::thread::spawn(|| {});
    let mut consumer = Consumer::new(handle);

    let r1 = consumer.join();
    assert!(r1.is_ok());

    // Second join should be Ok (handle already taken).
    let r2 = consumer.join();
    assert!(r2.is_ok());
}

// ─── ConsumerPanic message extraction ──────────────────────────────────────

#[test]
#[cfg(not(miri))]
fn consumer_panic_message_extraction() {
    let handle = std::thread::spawn(|| {
        panic!("test panic message");
    });
    let mut consumer = Consumer::new(handle);
    let result = consumer.join();
    assert!(result.is_err());

    let err = result.unwrap_err();
    assert_eq!(err.message(), Some("test panic message"));
    assert!(format!("{err}").contains("consumer thread panicked"));
    assert!(format!("{err}").contains("test panic message"));
}

#[test]
#[cfg(not(miri))]
fn consumer_panic_without_string_message() {
    let handle = std::thread::spawn(|| {
        std::panic::panic_any(42i32); // non-string panic
    });
    let mut consumer = Consumer::new(handle);
    let err = consumer.join().unwrap_err();
    assert_eq!(err.message(), None);
    assert_eq!(format!("{err}"), "consumer thread panicked");
}

// ─── EventGuard drops cursor correctly ─────────────────────────────────────

#[test]
fn event_guard_advances_cursor_on_drop() {
    let rb = Arc::new(RingBuffer::new(64, || Event { value: 0 }));
    let cursor = Arc::new(Cursor::new());
    let shutdown = Arc::new(crossbeam_utils::CachePadded::new(AtomicI64::new(NONE)));

    // Publish 3 events.
    for i in 0..3i64 {
        let ptr = rb.get(i);
        unsafe { (*ptr).value = i; }
    }
    cursor.store(2);

    let barrier = SingleConsumerBarrier::new(Arc::clone(&cursor));
    let poller_cursor = Arc::new(Cursor::new());
    let pc = Arc::clone(&poller_cursor);
    let mut poller = EventPoller::new(rb, Arc::new(barrier), shutdown, poller_cursor);

    assert_eq!(pc.relaxed_value(), NONE);

    // Poll and consume.
    {
        let mut guard = poller.poll().unwrap();
        let _count = (&mut guard).count();
        // Guard drops here.
    }

    // After guard drop, cursor should advance to 2 (last available).
    assert_eq!(pc.relaxed_value(), 2);
}

// ─── LifecycleAware trait ──────────────────────────────────────────────────

#[test]
fn lifecycle_aware_trait_exists() {
    struct MyComponent { started: bool, shutdown: bool }
    impl LifecycleAware for MyComponent {
        fn on_start(&mut self) { self.started = true; }
        fn on_shutdown(&mut self) { self.shutdown = true; }
    }

    let mut c = MyComponent { started: false, shutdown: false };
    c.on_start();
    assert!(c.started);
    c.on_shutdown();
    assert!(c.shutdown);
}

// ─── WorkHandler trait ─────────────────────────────────────────────────────

#[test]
fn work_handler_default_on_timeout() {
    struct MyWorker;
    impl WorkHandler<i32> for MyWorker {
        fn on_event(&mut self, _: &i32, _: Sequence) {}
    }
    let mut w = MyWorker;
    w.on_timeout(); // should not panic (default impl)
}

// ─── ExceptionAction ───────────────────────────────────────────────────────

#[test]
fn exception_action_debug_display() {
    let c = ExceptionAction::Continue;
    let h = ExceptionAction::Halt;
    assert_eq!(format!("{c:?}"), "Continue");
    assert_eq!(format!("{h:?}"), "Halt");
    assert_ne!(c, h);
    assert_eq!(c, c.clone());
}

// ─── Polling error ─────────────────────────────────────────────────────────

#[test]
fn polling_error_display() {
    assert_eq!(format!("{}", Polling::NoEvents), "No available events.");
    assert_eq!(format!("{}", Polling::Shutdown), "Disruptor is shut down.");
}
