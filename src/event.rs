use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use uuid::Uuid;

/// The base trait that all application events or enums must implement.
///
/// In idiomatic Rust event sourcing, it's highly recommended to define a single `Enum`
/// for all your domain events and implement this trait for that enum. This completely avoids
/// downcasting or global mutable type registries used in Go, maintaining strict type safety
/// and allowing `serde` to handle dynamic unmarshaling via `#[serde(tag)]` automatically.
pub trait EventPayload: Serialize + DeserializeOwned + Send + Sync + Clone + 'static {
    /// Provide a string representation of the event type, useful for storage routing or debugging.
    fn event_type(&self) -> &'static str;
}

/// A strongly-typed wrapper around the actual payload, containing metadata.
/// The `sequence_num` provides monotonic ordering to fix the replay brittleness of the old Go implementation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(bound = "T: Serialize + DeserializeOwned")]
pub struct Event<T> {
    /// Unique identifier for this specific occurrence.
    pub id: Uuid,
    /// Identifier for the aggregate (entity) this event belongs to.
    pub aggregate_id: String,
    /// Monotonically increasing sequence number for this aggregate's stream.
    pub sequence_num: u64,
    /// Monotonically increasing global sequence number across all aggregates.
    pub global_sequence_num: u64,
    /// When the event was created.
    pub timestamp: DateTime<Utc>,
    /// The actual domain event payload.
    pub payload: T,
}

impl<T: EventPayload> Event<T> {
    /// Creates a new `Event` wrapping the provided payload, generating a new UUID and Utc timestamp.
    pub fn new(aggregate_id: impl Into<String>, sequence_num: u64, payload: T) -> Self {
        Self {
            id: Uuid::new_v4(),
            aggregate_id: aggregate_id.into(),
            sequence_num,
            global_sequence_num: 0, // Will be set by EventStore on append
            timestamp: Utc::now(),
            payload,
        }
    }
}
