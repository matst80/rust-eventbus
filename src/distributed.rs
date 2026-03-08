use async_trait::async_trait;
use futures::stream::BoxStream;
use thiserror::Error;
use uuid::Uuid;

use crate::event::{Event, EventPayload};
use crate::store::{EventStore, StoreError};

#[derive(Debug, Error)]
pub enum DistributedError {
    #[error("Network error: {0}")]
    Network(String),
    #[error("Discovery error: {0}")]
    Discovery(String),
    #[error("Quorum not reached")]
    NoQuorum,
}

#[derive(Debug, Error)]
pub enum LockError {
    #[error("Lock already held by another node")]
    AlreadyHeld,
    #[error("Storage error: {0}")]
    Storage(String),
    #[error("Network/Quorum error: {0}")]
    Network(String),
}

/// Node representation in the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Node {
    pub id: Uuid,
    pub address: String, // e.g., "192.168.1.10:8080"
}

/// Abstraction for discovering other nodes in the cluster.
#[async_trait]
pub trait NodeDiscovery: Send + Sync + 'static {
    /// Returns a list of known nodes.
    async fn discover_nodes(&self) -> Result<Vec<Node>, DistributedError>;
    /// Register the current node with the discovery service.
    async fn register(&self, node: Node) -> Result<(), DistributedError>;
    /// Unregister the current node.
    async fn unregister(&self, node: &Node) -> Result<(), DistributedError>;
}

/// A zero-dependency direct socket-based peer-to-peer clustering PubSub interface.
/// Replaces `tokio::sync::broadcast` for cross-node communication.
#[async_trait]
pub trait DistributedPubSub<E: EventPayload>: Send + Sync + 'static {
    /// Broadcast an event to the clustered mesh.
    /// To prevent split-brain partition scenarios, ensuring a quorum is established
    /// before accepting/publishing might be enforced here depending on the implementation.
    async fn publish(&self, event: &Event<E>) -> Result<(), DistributedError>;

    /// Subscribes to events arriving from other nodes over the network.
    async fn subscribe(&self) -> BoxStream<'static, Result<Event<E>, DistributedError>>;
}

/// Trait to handle Projection Singleton/Sharding in a distributed environment.
/// Ensures that durable projections are only processed by one node/shard at a time.
#[async_trait]
pub trait ProjectionLockManager: Send + Sync + 'static {
    /// Attempt to acquire a distributed lock for a specific projection name.
    /// Returns true if the lock was acquired, false if it is held by someone else,
    /// or an error if there were issues (e.g., lost quorum).
    async fn acquire_lock(&self, projection_name: &str, node_id: &Uuid) -> Result<bool, LockError>;

    /// Extend the TTL of the lock. Must be called periodically by the lock holder.
    async fn keep_alive(&self, projection_name: &str, node_id: &Uuid) -> Result<(), LockError>;

    /// Release the lock gracefully.
    async fn release_lock(&self, projection_name: &str, node_id: &Uuid) -> Result<(), LockError>;
}

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
pub async fn with_retries<T, E, F, Fut>(mut max_retries: usize, mut operation: F) -> Result<T, StoreError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, StoreError>>,
{
    loop {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(StoreError::Conflict(_msg)) if max_retries > 0 => {
                // Normally you'd want to log a warning here.
                max_retries -= 1;
                // Backoff could be implemented here
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}
