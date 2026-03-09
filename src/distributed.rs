use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::event::{Event, EventPayload};
use crate::store::{EventStore, StoreError};

// Bytes-oriented backend trait and codec helpers allow pluggable transports
// (libp2p, nats, tcp, etc.) to be implemented once and wired into the
// higher-level `DistributedPubSub<E>` adapter that works with typed `Event<E>`.
/// A minimal backend trait that operates on raw message bytes.
/// Implementations can map topics/subjects as-needed for the transport.
#[async_trait]
pub trait PubSubBackend: Send + Sync + 'static {
    /// Publish a raw payload to the given topic/subject.
    async fn publish_bytes(&self, topic: &str, payload: Vec<u8>) -> Result<(), DistributedError>;

    /// Subscribe to a topic/subject and receive raw payloads as a stream.
    /// Implementations should return a 'static boxed stream of payloads.
    fn subscribe_bytes(&self, topic: &str)
        -> BoxStream<'static, Result<Vec<u8>, DistributedError>>;

    /// Verify if quorum is met for the backend.
    async fn check_quorum(&self) -> Result<(), DistributedError>;
}

/// Helper for encoding/decoding `Event<E>` to/from bytes for wire transport.
pub struct EventCodec;

impl EventCodec {
    pub fn encode<E: serde::Serialize + for<'de> serde::Deserialize<'de>>(
        event: &Event<E>,
    ) -> Result<Vec<u8>, DistributedError> {
        bincode::serialize(event).map_err(|e| DistributedError::Network(e.to_string()))
    }

    pub fn decode<E: for<'de> serde::Deserialize<'de> + serde::Serialize>(
        bytes: &[u8],
    ) -> Result<Event<E>, DistributedError> {
        bincode::deserialize(bytes).map_err(|e| DistributedError::Network(e.to_string()))
    }
}

/// Adapter that implements the typed `DistributedPubSub<E>` by delegating to
/// a bytes-oriented `PubSubBackend` and using `EventCodec` for (de)serialization.
pub struct BackendPubSub {
    backend: std::sync::Arc<dyn PubSubBackend>,
    topic: String,
}

impl BackendPubSub {
    pub fn new(backend: std::sync::Arc<dyn PubSubBackend>, topic: String) -> Self {
        Self { backend, topic }
    }
}

/// Factory that returns the default mesh implementation as a trait object.
/// By default this returns the `TcpPubSub` implementation. Enabling the
/// `libp2p-backend` feature will switch to the libp2p adapter if available.
pub async fn default_mesh<E>(
    node_id: Uuid,
    listen_addr: String,
    advertised_addr: String,
    discovery: std::sync::Arc<dyn NodeDiscovery>,
) -> std::sync::Arc<dyn DistributedPubSub<E>>
where
    E: EventPayload + serde::Serialize + for<'de> serde::Deserialize<'de> + 'static,
{
    #[cfg(feature = "libp2p-backend")]
    {
        // When the feature is enabled we prefer the libp2p adapter. The adapter
        // is expected to implement `DistributedPubSub<E>` and currently provides
        // an async constructor.
        if let Ok(adapter) = crate::libp2p_adapter::Libp2pAdapter::new_with_addrs(
            node_id,
            listen_addr.clone(),
            advertised_addr.clone(),
            discovery.clone(),
        )
        .await
        {
            let backend_arc: std::sync::Arc<dyn PubSubBackend> = adapter;
            return std::sync::Arc::new(BackendPubSub::new(backend_arc, "eventbus".to_string()));
        }
    }

    // Default: TcpPubSub
    std::sync::Arc::new(TcpPubSub::new_with_advertised_addr(
        node_id,
        listen_addr,
        advertised_addr,
        discovery,
    ))
}

#[async_trait]
impl<E: EventPayload + serde::Serialize + for<'de> serde::Deserialize<'de>> DistributedPubSub<E>
    for BackendPubSub
{
    async fn publish(&self, event: &Event<E>) -> Result<(), DistributedError> {
        let bytes = EventCodec::encode(event)?;
        self.backend.publish_bytes(&self.topic, bytes).await
    }

    async fn subscribe(&self) -> BoxStream<'static, Result<Event<E>, DistributedError>> {
        let stream = self.backend.subscribe_bytes(&self.topic);
        let mapped = stream.map(|res| match res {
            Ok(bytes) => EventCodec::decode::<E>(&bytes),
            Err(e) => Err(e),
        });
        Box::pin(mapped)
    }

    async fn check_quorum(&self) -> Result<(), DistributedError> {
        self.backend.check_quorum().await
    }
}

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

    /// Verify that a majority of nodes in the cluster are reachable.
    /// Returns Ok(()) if the node count satisfies (TotalNodes / 2) + 1.
    async fn check_quorum(&self) -> Result<(), DistributedError>;
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
        Ok(self
            .peers
            .iter()
            .map(|addr| Node {
                id: Uuid::nil(), // ID is often not known upfront in static config
                address: addr.clone(),
            })
            .collect())
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
        match lookup_host(query.clone()).await {
            Ok(addrs) => {
                let mut seen = std::collections::HashSet::new();
                let mut nodes = Vec::new();
                for addr in addrs {
                    let addr = addr.to_string();
                    if seen.insert(addr.clone()) {
                        nodes.push(Node {
                            id: Uuid::nil(),
                            address: addr,
                        });
                    }
                }

                if nodes.is_empty() {
                    tracing::warn!("DNS discovery for {} returned no addresses", query);
                } else {
                    tracing::info!("DNS discovery for {}: found {} nodes", query, nodes.len());
                }
                Ok(nodes)
            }
            Err(e) => {
                // Return empty list instead of Err to avoid mesh collapse on DNS flakes, but log the error
                tracing::error!("DNS discovery failed for {}: {}", query, e);
                Ok(vec![])
            }
        }
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
    node_id: Uuid,
    listen_addr: String,
    advertised_addr: String,
    discovery: Arc<dyn NodeDiscovery>,
    discovery_cache: Arc<parking_lot::Mutex<(std::time::Instant, Vec<Node>)>>,
    connections: Arc<Mutex<HashMap<String, tokio::net::TcpStream>>>,
    _phantom: std::marker::PhantomData<E>,
}

impl<E: EventPayload + serde::Serialize + for<'de> serde::Deserialize<'de>> TcpPubSub<E> {
    pub fn new(node_id: Uuid, listen_addr: String, discovery: Arc<dyn NodeDiscovery>) -> Self {
        Self::new_with_advertised_addr(node_id, listen_addr.clone(), listen_addr, discovery)
    }

    pub fn new_with_advertised_addr(
        node_id: Uuid,
        listen_addr: String,
        advertised_addr: String,
        discovery: Arc<dyn NodeDiscovery>,
    ) -> Self {
        let connections = Arc::new(Mutex::new(HashMap::new()));
        let mesh = Self {
            node_id,
            listen_addr,
            advertised_addr: advertised_addr.clone(),
            discovery: discovery.clone(),
            discovery_cache: Arc::new(parking_lot::Mutex::new((
                std::time::Instant::now() - std::time::Duration::from_secs(60),
                vec![],
            ))),
            connections: connections.clone(),
            _phantom: std::marker::PhantomData,
        };

        // Proactive Connection Manager: Fixes quorum-on-startup chicken-and-egg problem.
        // Periodically ensures we have connections to all discovered peers.
        let discovery_cache = mesh.discovery_cache.clone();
        let discovery_inner = discovery.clone();
        let advertised_addr_inner = advertised_addr.clone();
        tokio::spawn(async move {
            loop {
                // Refresh nodes
                if let Ok(nodes) = discovery_inner.discover_nodes().await {
                    {
                        let mut cache = discovery_cache.lock();
                        *cache = (std::time::Instant::now(), nodes.clone());
                    }

                    for node in nodes {
                        if same_endpoint(&node.address, &advertised_addr_inner) {
                            continue;
                        }

                        let addr = node.address.clone();
                        let conns = connections.clone();

                        // Only connect if we don't already have an active stream
                        let has_conn = {
                            let c = conns.lock().await;
                            c.contains_key(&addr)
                        };

                        if !has_conn {
                            tokio::spawn(async move {
                                if let Ok(Ok(mut s)) = tokio::time::timeout(
                                    std::time::Duration::from_secs(2),
                                    tokio::net::TcpStream::connect(&addr),
                                )
                                .await
                                {
                                    let _ = s.set_nodelay(true);
                                    let mut c = conns.lock().await;
                                    c.insert(addr, s);
                                }
                            });
                        }
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        });

        mesh
    }
}

fn same_endpoint(a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }

    match (
        a.parse::<std::net::SocketAddr>(),
        b.parse::<std::net::SocketAddr>(),
    ) {
        (Ok(lhs), Ok(rhs)) => lhs.ip() == rhs.ip() && lhs.port() == rhs.port(),
        _ => false,
    }
}

/// TCP-based bytes-oriented PubSub backend. Mirrors the existing `TcpPubSub`
/// framing (4-byte length prefix, 16-byte sender UUID, then payload bytes).
pub struct TcpPubSubBackend {
    node_id: Uuid,
    listen_addr: String,
    advertised_addr: String,
    discovery: Arc<dyn NodeDiscovery>,
    discovery_cache: Arc<parking_lot::Mutex<(std::time::Instant, Vec<Node>)>>,
    connections: Arc<Mutex<HashMap<String, tokio::net::TcpStream>>>,
}

impl TcpPubSubBackend {
    pub fn new(node_id: Uuid, listen_addr: String, discovery: Arc<dyn NodeDiscovery>) -> Self {
        Self::new_with_advertised_addr(node_id, listen_addr.clone(), listen_addr, discovery)
    }

    pub fn new_with_advertised_addr(
        node_id: Uuid,
        listen_addr: String,
        advertised_addr: String,
        discovery: Arc<dyn NodeDiscovery>,
    ) -> Self {
        Self {
            node_id,
            listen_addr,
            advertised_addr,
            discovery,
            discovery_cache: Arc::new(parking_lot::Mutex::new((
                std::time::Instant::now() - std::time::Duration::from_secs(60),
                vec![],
            ))),
            connections: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn get_nodes(&self) -> Result<Vec<Node>, DistributedError> {
        let cached = {
            let cache = self.discovery_cache.lock();
            if cache.0.elapsed() < std::time::Duration::from_secs(5) {
                Some(cache.1.clone())
            } else {
                None
            }
        };

        if let Some(n) = cached {
            return Ok(n);
        }

        let n = self.discovery.discover_nodes().await?;
        {
            let mut cache = self.discovery_cache.lock();
            *cache = (std::time::Instant::now(), n.clone());
        }
        Ok(n)
    }
}

#[async_trait]
impl PubSubBackend for TcpPubSubBackend {
    async fn publish_bytes(&self, _topic: &str, payload: Vec<u8>) -> Result<(), DistributedError> {
        self.check_quorum().await?;
        let nodes = self.get_nodes().await?;
        let my_addr = self.advertised_addr.clone();

        let event_payload = payload;
        let my_node_id = self.node_id;

        let mut full_payload = Vec::with_capacity(16 + event_payload.len() + 4);
        let total_len = (16 + event_payload.len()) as u32;
        full_payload.extend_from_slice(&total_len.to_be_bytes());
        full_payload.extend_from_slice(my_node_id.as_bytes());
        full_payload.extend_from_slice(&event_payload);

        let payload_arc = Arc::new(full_payload);
        let conns_ref = self.connections.clone();

        for node in nodes {
            if same_endpoint(&node.address, &my_addr) {
                continue;
            }

            let addr = node.address.clone();
            let payload = payload_arc.clone();
            let conns = conns_ref.clone();

            tokio::spawn(async move {
                let stream = {
                    let mut c = conns.lock().await;
                    c.remove(&addr)
                };

                let mut send_success = false;

                if let Some(mut s) = stream {
                    if s.write_all(&payload).await.is_ok() {
                        let mut c = conns.lock().await;
                        c.insert(addr.clone(), s);
                        send_success = true;
                    }
                }

                if !send_success {
                    match tokio::time::timeout(
                        std::time::Duration::from_secs(2),
                        tokio::net::TcpStream::connect(&addr),
                    )
                    .await
                    {
                        Ok(Ok(mut s)) => {
                            let _ = s.set_nodelay(true);
                            if s.write_all(&payload).await.is_ok() {
                                let mut c = conns.lock().await;
                                c.insert(addr, s);
                            }
                        }
                        _ => {}
                    }
                }
            });
            tokio::task::yield_now().await;
        }

        Ok(())
    }

    async fn check_quorum(&self) -> Result<(), DistributedError> {
        let nodes = self.get_nodes().await?;
        // If we only see 1 node total, quorum is reached by definition (single node cluster)
        if nodes.len() <= 1 {
            return Ok(());
        }

        let min_required = (nodes.len() / 2) + 1;
        
        // Count active connections + self
        let active_nodes = {
            let conns = self.connections.lock().await;
            conns.len() + 1
        };

        if active_nodes >= min_required {
            Ok(())
        } else {
            tracing::warn!("Quorum check failed: have {} nodes, need {}", active_nodes, min_required);
            Err(DistributedError::NoQuorum)
        }
    }

    fn subscribe_bytes(
        &self,
        _topic: &str,
    ) -> BoxStream<'static, Result<Vec<u8>, DistributedError>> {
        let addr = self.listen_addr.clone();
        let my_node_id = self.node_id;
        let (tx, rx) = tokio::sync::mpsc::channel(128);

        tokio::spawn(async move {
            let listener = match tokio::net::TcpListener::bind(&addr).await {
                Ok(l) => l,
                Err(e) => {
                    tracing::error!("Failed to bind tcp backend listener {}: {}", addr, e);
                    return;
                }
            };

            loop {
                match listener.accept().await {
                    Ok((mut socket, _peer_addr)) => {
                        let tx = tx.clone();
                        let my_node_id = my_node_id;
                        tokio::spawn(async move {
                            let mut len_buf = [0u8; 4];
                            use tokio::io::AsyncReadExt;

                            loop {
                                match socket.read_exact(&mut len_buf).await {
                                    Ok(_) => {
                                        let body_len = u32::from_be_bytes(len_buf) as usize;
                                        if body_len > 10 * 1024 * 1024 {
                                            break;
                                        }

                                        let mut body = vec![0u8; body_len];
                                        if let Err(_) = socket.read_exact(&mut body).await {
                                            break;
                                        }

                                        if body.len() < 16 {
                                            continue;
                                        }

                                        let mut id_bytes = [0u8; 16];
                                        id_bytes.copy_from_slice(&body[..16]);
                                        let sender_id = Uuid::from_bytes(id_bytes);

                                        if sender_id == my_node_id {
                                            continue;
                                        }

                                        let payload = body[16..].to_vec();
                                        let _ = tx.send(Ok(payload)).await;
                                    }
                                    Err(_) => break,
                                }
                                tokio::task::yield_now().await;
                            }
                        });
                    }
                    Err(e) => {
                        tracing::error!("accept error: {}", e);
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                }
            }
        });

        Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx))
    }
}

#[async_trait]
impl<E: EventPayload + serde::Serialize + for<'de> serde::Deserialize<'de>> DistributedPubSub<E>
    for TcpPubSub<E>
{
    async fn publish(&self, event: &Event<E>) -> Result<(), DistributedError> {
        self.check_quorum().await?;
        let nodes = self.get_nodes().await?;
        let my_addr = self.advertised_addr.clone();
        let active_peer_addrs: std::collections::HashSet<String> = nodes
            .iter()
            .filter(|n| !same_endpoint(&n.address, &my_addr))
            .map(|n| n.address.clone())
            .collect();

        {
            let mut c = self.connections.lock().await;
            c.retain(|addr, _| active_peer_addrs.contains(addr));
        }

        let event_payload =
            bincode::serialize(event).map_err(|e| DistributedError::Network(e.to_string()))?;
        let event_id = event.id;
        let my_node_id = self.node_id;

        let mut full_payload = Vec::with_capacity(20 + event_payload.len());
        let total_len = (16 + event_payload.len()) as u32;
        full_payload.extend_from_slice(&total_len.to_be_bytes());
        full_payload.extend_from_slice(my_node_id.as_bytes());
        full_payload.extend_from_slice(&event_payload);

        let payload_arc = Arc::new(full_payload);
        let conns_ref = self.connections.clone();

        for node in nodes {
            // Self-filter check (ip-based as fallback for nil-id discovery)
            if same_endpoint(&node.address, &my_addr) {
                continue;
            }

            let addr = node.address;
            let payload = payload_arc.clone();
            let conns = conns_ref.clone();

            tokio::spawn(async move {
                let stream = {
                    let mut c = conns.lock().await;
                    c.remove(&addr)
                };

                let mut send_success = false;

                if let Some(mut s) = stream {
                    if s.write_all(&payload).await.is_ok() {
                        tracing::info!("Mesh OUT: Event {} (to Peer {})", event_id, addr);
                        let mut c = conns.lock().await;
                        c.insert(addr.clone(), s);
                        send_success = true;
                    } else {
                        tracing::debug!("Mesh send failed to {}, retrying connection", addr);
                    }
                }

                if !send_success {
                    match tokio::time::timeout(
                        std::time::Duration::from_secs(2),
                        tokio::net::TcpStream::connect(&addr),
                    )
                    .await
                    {
                        Ok(Ok(mut s)) => {
                            let _ = s.set_nodelay(true);
                            if let Err(e) = s.write_all(&payload).await {
                                tracing::debug!("Mesh send failed to {}, closing: {}", addr, e);
                            } else {
                                tracing::info!("Mesh OUT: Event {} (to Peer {})", event_id, addr);
                                let mut c = conns.lock().await;
                                c.insert(addr, s);
                            }
                        }
                        _ => {
                            tracing::debug!("Mesh connect failed to {}", addr);
                        }
                    }
                }
            });
            tokio::task::yield_now().await;
        }
        Ok(())
    }

    async fn check_quorum(&self) -> Result<(), DistributedError> {
        let nodes = self.get_nodes().await?;
        if nodes.len() <= 1 {
            return Ok(());
        }

        let min_required = (nodes.len() / 2) + 1;
        
        let active_nodes = {
            let conns = self.connections.lock().await;
            conns.len() + 1
        };

        if active_nodes >= min_required {
            Ok(())
        } else {
            tracing::warn!("Quorum check failed: have {} nodes, need {}", active_nodes, min_required);
            Err(DistributedError::NoQuorum)
        }
    }

    async fn subscribe(&self) -> BoxStream<'static, Result<Event<E>, DistributedError>> {
        let addr = self.listen_addr.clone();
        let my_node_id = self.node_id;
        let (tx, rx) = tokio::sync::mpsc::channel(128);

        tokio::spawn(async move {
            let listener = match tokio::net::TcpListener::bind(&addr).await {
                Ok(l) => {
                    tracing::info!("Mesh listening on {} (Node: {})", addr, my_node_id);
                    l
                }
                Err(e) => {
                    tracing::error!("FATAL: Failed to bind mesh listener: {}", e);
                    return;
                }
            };

            loop {
                match listener.accept().await {
                    Ok((mut socket, peer_addr)) => {
                        let tx = tx.clone();
                        let my_node_id = my_node_id;
                        tokio::spawn(async move {
                            tracing::trace!("Mesh connection from {}", peer_addr);
                            let mut len_buf = [0u8; 4];
                            use tokio::io::AsyncReadExt;

                            loop {
                                match socket.read_exact(&mut len_buf).await {
                                    Ok(_) => {
                                        let body_len = u32::from_be_bytes(len_buf) as usize;
                                        if body_len > 10 * 1024 * 1024 {
                                            tracing::error!(
                                                "Oversized packet: {} from {}",
                                                body_len,
                                                peer_addr
                                            );
                                            break;
                                        }

                                        let mut body = vec![0u8; body_len];
                                        if let Err(e) = socket.read_exact(&mut body).await {
                                            tracing::error!(
                                                "Body read error from {}: {}",
                                                peer_addr,
                                                e
                                            );
                                            break;
                                        }

                                        if body.len() < 16 {
                                            continue;
                                        }
                                        let mut id_bytes = [0u8; 16];
                                        id_bytes.copy_from_slice(&body[..16]);
                                        let sender_id = Uuid::from_bytes(id_bytes);

                                        if sender_id == my_node_id {
                                            continue;
                                        }

                                        match bincode::deserialize::<Event<E>>(&body[16..]) {
                                            Ok(event) => {
                                                tracing::info!(
                                                    "Mesh IN: Event {} (from Peer {})",
                                                    event.id,
                                                    sender_id
                                                );
                                                let _ = tx.send(Ok(event)).await;
                                            }
                                            Err(e) => tracing::error!(
                                                "Bincode error from {}: {}",
                                                peer_addr,
                                                e
                                            ),
                                        }
                                    }
                                    Err(e) => {
                                        if e.kind() != std::io::ErrorKind::UnexpectedEof {
                                            tracing::debug!(
                                                "Mesh connection closed by {}: {}",
                                                peer_addr,
                                                e
                                            );
                                        }
                                        break;
                                    }
                                }
                                tokio::task::yield_now().await;
                            }
                        });
                    }
                    Err(e) => {
                        tracing::error!("Mesh accept error: {}", e);
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                }
                tokio::task::yield_now().await;
            }
        });

        Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx))
    }
}

impl<E: EventPayload> TcpPubSub<E> {
    async fn get_nodes(&self) -> Result<Vec<Node>, DistributedError> {
        let cached = {
            let cache = self.discovery_cache.lock();
            if cache.0.elapsed() < std::time::Duration::from_secs(5) {
                Some(cache.1.clone())
            } else {
                None
            }
        };

        if let Some(n) = cached {
            return Ok(n);
        }

        let n = self.discovery.discover_nodes().await?;
        {
            let mut cache = self.discovery_cache.lock();
            *cache = (std::time::Instant::now(), n.clone());
        }
        Ok(n)
    }
}

/// Pattern for handling Concurrency Conflicts gracefully.
/// Takes a closure that reloads state and attempts the command again.
pub async fn with_retries<T, E, F, Fut>(
    mut max_retries: usize,
    mut operation: F,
) -> Result<T, StoreError>
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
