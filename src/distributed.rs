use async_trait::async_trait;
use futures::stream::BoxStream;
use std::sync::Arc;
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

/// Environment-based node discovery.
/// Reads peers from `PEERS` env var (e.g., "127.0.0.1:3001,127.0.0.1:3002").
pub struct EnvironmentNodeDiscovery {
    pub peers: Vec<String>,
}

impl EnvironmentNodeDiscovery {
    pub fn new() -> Self {
        let peers = std::env::var("PEERS")
            .unwrap_or_default()
            .split(',')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        Self { peers }
    }
}

#[async_trait]
impl NodeDiscovery for EnvironmentNodeDiscovery {
    async fn discover_nodes(&self) -> Result<Vec<Node>, DistributedError> {
        Ok(self.peers.iter().map(|addr| Node {
            id: Uuid::nil(), // ID is often not known upfront in static config
            address: addr.clone(),
        }).collect())
    }

    async fn register(&self, _node: Node) -> Result<(), DistributedError> {
        Ok(()) // Static discovery doesn't support dynamic registration
    }

    async fn unregister(&self, _node: &Node) -> Result<(), DistributedError> {
        Ok(())
    }
}

/// DNS-based node discovery (ideal for Kubernetes Headless Services).
/// Performs an A/AAAA record lookup to find peer IP addresses.
pub struct DnsNodeDiscovery {
    pub service_name: String,
    pub port: u16,
}

impl DnsNodeDiscovery {
    pub fn new(service_name: String, port: u16) -> Self {
        Self { service_name, port }
    }
}

#[async_trait]
impl NodeDiscovery for DnsNodeDiscovery {
    async fn discover_nodes(&self) -> Result<Vec<Node>, DistributedError> {
        use tokio::net::lookup_host;
        
        let query = format!("{}:{}", self.service_name, self.port);
        let Ok(addrs) = lookup_host(&query).await else {
            return Ok(vec![]);
        };

        Ok(addrs.map(|addr| Node {
            id: Uuid::nil(),
            address: addr.to_string(),
        }).collect())
    }

    async fn register(&self, _node: Node) -> Result<(), DistributedError> {
        Ok(())
    }

    async fn unregister(&self, _node: &Node) -> Result<(), DistributedError> {
        Ok(())
    }
}

/// TCP-based PubSub implementation for peer-to-peer event mesh.
pub struct TcpPubSub<E: EventPayload> {
    _node_id: Uuid,
    listen_addr: String,
    discovery: Arc<dyn NodeDiscovery>,
    _phantom: std::marker::PhantomData<E>,
}

impl<E: EventPayload + serde::Serialize + for<'de> serde::Deserialize<'de>> TcpPubSub<E> {
    pub fn new(node_id: Uuid, listen_addr: String, discovery: Arc<dyn NodeDiscovery>) -> Self {
        Self {
            _node_id: node_id,
            listen_addr,
            discovery,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<E: EventPayload + serde::Serialize + for<'de> serde::Deserialize<'de>> DistributedPubSub<E> for TcpPubSub<E> {
    async fn publish(&self, event: &Event<E>) -> Result<(), DistributedError> {
        let nodes = self.discovery.discover_nodes().await?;
        let payload = bincode::serialize(event).map_err(|e| DistributedError::Network(e.to_string()))?;

        for node in nodes {
            if node.address == self.listen_addr {
                continue;
            }

            let addr = node.address.clone();
            let payload_clone = payload.clone();
            
            // Fire and forget or handle error? Usually we want some retry logic.
            tokio::spawn(async move {
                if let Ok(mut stream) = tokio::net::TcpStream::connect(&addr).await {
                    use tokio::io::AsyncWriteExt;
                    let _ = stream.write_all(&payload_clone).await;
                }
            });
        }
        Ok(())
    }

    async fn subscribe(&self) -> BoxStream<'static, Result<Event<E>, DistributedError>> {
        let addr = self.listen_addr.clone();
        
        let stream = futures::stream::unfold(None, move |state| {
            let addr = addr.clone();
            async move {
                let listener = match state {
                    Some(l) => l,
                    None => tokio::net::TcpListener::bind(&addr).await.ok()?,
                };

                loop {
                    if let Ok((mut socket, _)) = listener.accept().await {
                        let mut buf = Vec::new();
                        use tokio::io::AsyncReadExt;
                        if socket.read_to_end(&mut buf).await.is_ok() {
                            if let Ok(event) = bincode::deserialize::<Event<E>>(&buf) {
                                return Some((Ok(event), Some(listener)));
                            }
                        }
                    }
                }
            }
        });

        Box::pin(stream)
    }
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
                max_retries -= 1;
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}
