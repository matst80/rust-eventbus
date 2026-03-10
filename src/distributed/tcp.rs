use async_trait::async_trait;
use futures::stream::BoxStream;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::event::{Event, EventPayload};
use super::types::{Node, DistributedError};
use super::discovery::{NodeDiscovery, DiscoveryHandler};
use super::pubsub::{DistributedPubSub, PubSubBackend};

/// TCP-based PubSub implementation for peer-to-peer event mesh.
pub struct TcpPubSub<E: EventPayload> {
    pub(crate) node_id: Uuid,
    pub(crate) listen_addr: String,
    pub(crate) advertised_addr: String,
    pub(crate) discovery: Arc<dyn NodeDiscovery>,
    pub(crate) connections: Arc<Mutex<HashMap<String, tokio::net::TcpStream>>>,
    pub(crate) desired_nodes: Arc<Mutex<std::collections::HashSet<String>>>,
    pub(crate) task_limiter: Option<crate::task_limiter::TaskLimiter>,
    pub(crate) task_registry: Arc<Mutex<TaskRegistry>>,
    pub(crate) _phantom: std::marker::PhantomData<E>,
}

pub(crate) struct TaskRegistry {
    handles: Vec<tokio::task::AbortHandle>,
}

impl TaskRegistry {
    pub(crate) fn new() -> Self {
        Self { handles: Vec::new() }
    }
    pub(crate) fn add(&mut self, handle: tokio::task::AbortHandle) {
        self.handles.push(handle);
    }
}

impl Drop for TaskRegistry {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
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
            connections,
            desired_nodes: Arc::new(Mutex::new(std::collections::HashSet::new())),
            task_limiter: None,
            task_registry: Arc::new(Mutex::new(TaskRegistry::new())),
            _phantom: std::marker::PhantomData,
        };

        mesh
    }

    pub fn with_task_limiter(mut self, limiter: crate::task_limiter::TaskLimiter) -> Self {
        self.task_limiter = Some(limiter);
        self
    }
    
    pub async fn start_background_manager(self: Arc<Self>) {
        let discovery = self.discovery.clone();
        let task_registry = self.task_registry.clone();
        let self_clone = self.clone();
        
        let handler = Arc::new(TcpDiscoveryHandler { inner: self.clone() });
        let watch_task = async move {
            if let Err(e) = discovery.watch(handler).await {
                tracing::error!("Discovery watch failed: {}", e);
            }
        };

        // Reconciler task ensures we are connected to all desired nodes
        let reconciler_task = async move {
            loop {
                let desired = {
                    let d = self_clone.desired_nodes.lock().await;
                    d.clone()
                };
                
                for addr in desired {
                    let has_conn = {
                        let c = self_clone.connections.lock().await;
                        c.contains_key(&addr)
                    };

                    if !has_conn {
                        let inner = self_clone.clone();
                        let addr_clone = addr.clone();
                        let task_inner = inner.clone();
                        let task = async move {
                            if let Ok(Ok(s)) = tokio::time::timeout(
                                std::time::Duration::from_secs(2),
                                tokio::net::TcpStream::connect(&addr_clone),
                            )
                            .await
                            {
                                tracing::info!("Mesh connected to {}", addr_clone);
                                let mut c = task_inner.connections.lock().await;
                                c.insert(addr_clone, s);
                            }
                        };
                        if let Some(l) = inner.task_limiter.as_ref() {
                            l.spawn(task).await;
                        } else {
                            tokio::spawn(task);
                        }
                    }
                }
                
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        };

        let h1 = tokio::spawn(watch_task);
        let h2 = tokio::spawn(reconciler_task);
        let mut reg = task_registry.lock().await;
        reg.add(h1.abort_handle());
        reg.add(h2.abort_handle());
    }
}

struct TcpDiscoveryHandler<E: EventPayload> {
    inner: Arc<TcpPubSub<E>>,
}

#[async_trait]
impl<E: EventPayload + serde::Serialize + for<'de> serde::Deserialize<'de>> DiscoveryHandler for TcpDiscoveryHandler<E> {
    async fn on_node_added(&self, node: Node) {
        if same_endpoint(&node.address, &self.inner.advertised_addr) {
            return;
        }
        let mut desired = self.inner.desired_nodes.lock().await;
        desired.insert(node.address);
    }

    async fn on_node_removed(&self, node: Node) {
        {
            let mut desired = self.inner.desired_nodes.lock().await;
            desired.remove(&node.address);
        }
        let mut conns = self.inner.connections.lock().await;
        conns.remove(&node.address);
    }
}

impl<E: EventPayload + serde::Serialize + for<'de> serde::Deserialize<'de>> TcpPubSub<E> {

    pub(crate) async fn get_nodes(&self) -> Result<Vec<Node>, DistributedError> {
        let desired = self.desired_nodes.lock().await;
        Ok(desired.iter().map(|addr| Node { id: Uuid::nil(), address: addr.clone() }).collect())
    }
}

pub fn same_endpoint(a: &str, b: &str) -> bool {
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
    pub(crate) node_id: Uuid,
    pub(crate) listen_addr: String,
    pub(crate) advertised_addr: String,
    pub(crate) discovery: Arc<dyn NodeDiscovery>,
    pub(crate) connections: Arc<Mutex<HashMap<String, tokio::net::TcpStream>>>,
    pub(crate) desired_nodes: Arc<Mutex<std::collections::HashSet<String>>>,
    pub(crate) task_limiter: Option<crate::task_limiter::TaskLimiter>,
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
            connections: Arc::new(Mutex::new(HashMap::new())),
            desired_nodes: Arc::new(Mutex::new(std::collections::HashSet::new())),
            task_limiter: None,
        }
    }

    pub fn with_task_limiter(mut self, limiter: crate::task_limiter::TaskLimiter) -> Self {
        self.task_limiter = Some(limiter);
        self
    }

    pub(crate) async fn get_nodes(&self) -> Result<Vec<Node>, DistributedError> {
        let desired = self.desired_nodes.lock().await;
        Ok(desired.iter().map(|addr| Node { id: Uuid::nil(), address: addr.clone() }).collect())
    }

    pub async fn start_background_manager(self: Arc<Self>) {
        let discovery = self.discovery.clone();
        let self_clone = self.clone();
        
        let handler = Arc::new(TcpBackendDiscoveryHandler { inner: self.clone() });
        let watch_task = async move {
            if let Err(e) = discovery.watch(handler).await {
                tracing::error!("Backend discovery watch failed: {}", e);
            }
        };

        let reconciler_task = async move {
            loop {
                let desired = {
                    let d = self_clone.desired_nodes.lock().await;
                    d.clone()
                };
                
                for addr in desired {
                    let has_conn = {
                        let c = self_clone.connections.lock().await;
                        c.contains_key(&addr)
                    };

                    if !has_conn {
                        let inner = self_clone.clone();
                        let addr_clone = addr.clone();
                        let task_inner = inner.clone();
                        let task = async move {
                            if let Ok(Ok(s)) = tokio::time::timeout(
                                std::time::Duration::from_secs(2),
                                tokio::net::TcpStream::connect(&addr_clone),
                            )
                            .await
                            {
                                let mut c = task_inner.connections.lock().await;
                                c.insert(addr_clone, s);
                            }
                        };
                        if let Some(l) = inner.task_limiter.as_ref() {
                            l.spawn(task).await;
                        } else {
                            tokio::spawn(task);
                        }
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        };

        tokio::spawn(watch_task);
        tokio::spawn(reconciler_task);
    }
}

struct TcpBackendDiscoveryHandler {
    inner: Arc<TcpPubSubBackend>,
}

#[async_trait]
impl DiscoveryHandler for TcpBackendDiscoveryHandler {
    async fn on_node_added(&self, node: Node) {
        if same_endpoint(&node.address, &self.inner.advertised_addr) {
            return;
        }
        let mut desired = self.inner.desired_nodes.lock().await;
        desired.insert(node.address);
    }

    async fn on_node_removed(&self, node: Node) {
        {
            let mut desired = self.inner.desired_nodes.lock().await;
            desired.remove(&node.address);
        }
        let mut conns = self.inner.connections.lock().await;
        conns.remove(&node.address);
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

            let payload = payload_arc.clone();
            let conns = conns_ref.clone();
            let limiter = self.task_limiter.clone();
            let addr = node.address.clone();

            let task = async move {
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
            };

            if let Some(l) = limiter {
                l.spawn(task).await;
            } else {
                tokio::spawn(task);
            }
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

    fn subscribe_bytes(
        &self,
        _topic: &str,
    ) -> BoxStream<'static, Result<Vec<u8>, DistributedError>> {
        let addr = self.listen_addr.clone();
        let my_node_id = self.node_id;
        let (tx, rx) = tokio::sync::mpsc::channel(128);

        let limiter = self.task_limiter.clone();
        let listener_task = async move {
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
                        let limiter_inner = limiter.clone();
                        let peer_task = async move {
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
                        };
                        if let Some(l) = limiter_inner {
                            l.spawn(peer_task).await;
                        } else {
                            tokio::spawn(peer_task);
                        }
                    }
                    Err(e) => {
                        tracing::error!("accept error from {}: {}", addr, e);
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                }
            }
        };

        if let Some(l) = self.task_limiter.clone() {
            let _ = tokio::spawn(async move { l.spawn(listener_task).await });
        } else {
            tokio::spawn(listener_task);
        }

        Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx))
    }
}

#[async_trait]
impl<E: EventPayload + serde::Serialize + for<'de> serde::Deserialize<'de>> DistributedPubSub<E>
    for TcpPubSub<E>
{
    async fn publish(&self, event: &Event<E>) -> Result<(), DistributedError> {
        // In event-driven discovery, connections are managed by the DiscoveryHandler.
        // We only need to check quorum here.
        self.check_quorum().await?;
        let nodes = self.get_nodes().await?;
        let my_addr = self.advertised_addr.clone();

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
            if same_endpoint(&node.address, &my_addr) {
                continue;
            }

            let addr = node.address.clone();
            let payload = payload_arc.clone();
            let conns = conns_ref.clone();
            let limiter = self.task_limiter.clone();

            let task = async move {
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
                            // Suppress timeout/connect errors to avoid spamming logs unless severe
                        }
                    }
                }
            };

            if let Some(l) = limiter {
                l.spawn(task).await;
            } else {
                tokio::spawn(task);
            }
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

        let limiter = self.task_limiter.clone();
        let listener_task = async move {
            let listener = match tokio::net::TcpListener::bind(&addr).await {
                Ok(l) => l,
                Err(e) => {
                    tracing::error!("Failed to bind tcp mesh listener {}: {}", addr, e);
                    return;
                }
            };

            loop {
                match listener.accept().await {
                    Ok((mut socket, _peer_addr)) => {
                        let tx = tx.clone();
                        let my_node_id = my_node_id;
                        let limiter_inner = limiter.clone();
                        let peer_task = async move {
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

                                        let event_payload = &body[16..];
                                        match bincode::deserialize::<Event<E>>(event_payload) {
                                            Ok(event) => {
                                                tracing::info!("Mesh IN: Event {} (from Peer {})", event.id, sender_id);
                                                let _ = tx.send(Ok(event)).await;
                                            }
                                            Err(_) => {
                                                // Log or handle deserialization error
                                            }
                                        }
                                    }
                                    Err(_) => break,
                                }
                                tokio::task::yield_now().await;
                            }
                        };
                        if let Some(l) = limiter_inner {
                            l.spawn(peer_task).await;
                        } else {
                            tokio::spawn(peer_task);
                        }
                    }
                    Err(e) => {
                        tracing::error!("accept error from {}: {}", addr, e);
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                }
            }
        };

        if let Some(l) = self.task_limiter.clone() {
            let _ = tokio::spawn(async move { l.spawn(listener_task).await });
        } else {
            tokio::spawn(listener_task);
        }

        Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx))
    }
}
