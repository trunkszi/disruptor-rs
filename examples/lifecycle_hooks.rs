//! ⚠️ This example targets the v5 API and cannot be compiled with the current v4.
//!
//! Demonstrates LifecycleAware: lifecycle callbacks when a consumer thread starts and shuts down.
//! v4 does not have lifecycle hooks; v5 plans to let EventHandler optionally implement
//! LifecycleAware for initializing resources (e.g., opening files, establishing network
//! connections) and cleaning up resources.

#![allow(dead_code)]

/// v5 target trait: lifecycle awareness
///
/// ```ignore
/// pub trait LifecycleAware: Send {
///     /// Called after the consumer thread starts and before it begins processing events.
///     fn on_start(&mut self);
///     /// Called after all events have been processed and before the consumer thread exits.
///     fn on_shutdown(&mut self);
/// }
/// ```
trait LifecycleAware: Send {
    fn on_start(&mut self);
    fn on_shutdown(&mut self);
}

/// v5 target trait: event handler
trait EventHandler<E>: Send {
    fn on_event(&mut self, event: &E, sequence: i64, end_of_batch: bool);
}

// -- Event definition --
struct LogEntry {
    message: &'static str,
}

// -- Handler with lifecycle hooks --
struct FileLogger {
    path:    &'static str,
    started: bool,
}

impl FileLogger {
    fn new(path: &'static str) -> Self {
        Self { path, started: false }
    }
}

impl LifecycleAware for FileLogger {
    fn on_start(&mut self) {
        // Real-world scenario: open the log file
        println!("[FileLogger] on_start: opening file {}", self.path);
        self.started = true;
    }

    fn on_shutdown(&mut self) {
        // Real-world scenario: flush and close the file handle
        println!("[FileLogger] on_shutdown: closing file {}", self.path);
        self.started = false;
    }
}

impl EventHandler<LogEntry> for FileLogger {
    fn on_event(&mut self, event: &LogEntry, sequence: i64, _end_of_batch: bool) {
        assert!(self.started, "on_start should be called before on_event");
        println!("[FileLogger] seq={sequence} -> {}", event.message);
    }
}

/// v5 target usage (pseudocode):
///
/// ```ignore
/// let mut producer = build_single_producer(64, || LogEntry { message: "" }, BusySpin)
///     .handle_events_with(FileLogger::new("/var/log/app.log"))
///     .build();
/// // on_start() is called automatically when the consumer thread starts
/// // on_shutdown() is called automatically after the Producer is dropped and all events are processed
/// ```
fn main() {
    println!("This example requires the v5 API (LifecycleAware trait) and cannot run on the current v4.");
}
