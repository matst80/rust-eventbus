use async_trait::async_trait;
use uuid::Uuid;
use super::types::LockError;

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
