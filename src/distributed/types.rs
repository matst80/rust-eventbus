use thiserror::Error;
use uuid::Uuid;

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
