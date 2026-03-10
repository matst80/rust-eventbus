use async_trait::async_trait;
use crate::event::{Event, EventPayload};
use crate::store::{EventStore, StoreError};

/// Trait for Optimistic Concurrency Control (OCC).
/// Extends the base EventStore to enforce handling of concurrent appends to the same aggregate.
#[async_trait]
pub trait OptimisticEventStore<E: EventPayload>: EventStore<E> {
    /// Append events but expect the stream (aggregate) to be at exactly `expected_version`.
    /// If the version mismatches (e.g., another node wrote an event in the meantime),
    /// it returns `StoreError::Conflict`, signaling the caller to retry.
    async fn append_optimistic(
        &self,
        stream_id: &str,
        expected_version: u64,
        events: Vec<Event<E>>,
    ) -> Result<Vec<Event<E>>, StoreError>;
}

/// Pattern for handling Concurrency Conflicts gracefully.
/// Takes a closure that reloads state and attempts the command again.
pub fn with_retries<F, T>(mut max_retries: usize, mut operation: F) -> Result<T, StoreError>
where
    F: FnMut() -> Result<T, StoreError>,
{
    loop {
        match operation() {
            Ok(val) => return Ok(val),
            Err(StoreError::Conflict(_)) if max_retries > 0 => {
                max_retries -= 1;
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}
