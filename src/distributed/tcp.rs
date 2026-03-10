use async_trait::async_trait;
use futures::stream::BoxStream;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use uuid::Uuid;

use super::discovery::{DiscoveryHandler, NodeDiscovery};
use super::pubsub::PubSubBackend;
use super::types::{DistributedError, Node};

#[macro_export]
macro_rules! debug_println {
    ($($arg:tt)*) => {
        if std::env::var("EVENTBUS_DEBUG").is_ok() {
            println!($($arg)*);
        }
    };
}

/// TCP-based PubSub implementation for peer-to-peer event mesh.
pub struct TcpPubSub {
    pub(crate) node_id: Uuid,
    pub(crate) listen_addr: String,
    pub(crate) advertised_addr: String,
    pub(crate) discovery: Arc<dyn NodeDiscovery>,
    /// Per-peer connections. Inner mutex serialises writes to each peer.
    pub(crate) connections: Arc<Mutex<HashMap<String, Arc<Mutex<tokio::net::TcpStream>>>>>,
    pub(crate) desired_nodes: Arc<Mutex<HashSet<String>>>,
    pub(crate) task_limiter: Option<crate::task_limiter::TaskLimiter>,
    pub(crate) task_registry: Arc<Mutex<TaskRegistry>>,
    pub(crate) inbound_tx: tokio::sync::broadcast::Sender<Vec<u8>>,
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

impl TcpPubSub {
    pub fn new(node_id: Uuid, listen_addr: String, discovery: Arc<dyn NodeDiscovery>) -> Self {
        Self::new_with_advertised_addr(node_id, listen_addr.clone(), listen_addr, discovery)
    }

    pub fn new_with_advertised_addr(
        node_id: Uuid,
        listen_addr: String,
        advertised_addr: String,
        discovery: Arc<dyn NodeDiscovery>,
    ) -> Self {
        let (inbound_tx, _) = tokio::sync::broadcast::channel(2048);
        Self {
            node_id,
            listen_addr,
            advertised_addr,
            discovery,
            connections: Arc::new(Mutex::new(HashMap::new())),
            desired_nodes: Arc::new(Mutex::new(HashSet::new())),
            task_limiter: None,
            task_registry: Arc::new(Mutex::new(TaskRegistry::new())),
            inbound_tx,
        }
    }

    pub fn with_task_limiter(mut self, limiter: crate::task_limiter::TaskLimiter) -> Self {
        self.task_limiter = Some(limiter);
        self
    }

    pub(crate) async fn get_nodes(&self) -> Result<Vec<Node>, DistributedError> {
        let desired = self.desired_nodes.lock().await;
        Ok(desired
            .iter()
            .map(|addr| Node {
                id: Uuid::nil(),
                address: addr.clone(),
            })
            .collect())
    }

    pub async fn start_background_manager(self: Arc<Self>) {
        let discovery = self.discovery.clone();
        let task_registry = self.task_registry.clone();

        let handler = Arc::new(TcpDiscoveryHandler {
            connections: self.connections.clone(),
            desired_nodes: self.desired_nodes.clone(),
            advertised_addr: self.advertised_addr.clone(),
        });

        // Discovery Watching
        let h_watch = tokio::spawn(async move {
            let _ = discovery.watch(handler).await;
        });

        // Peer Reconnection
        let self_recon = self.clone();
        let h_recon = tokio::spawn(async move {
            loop {
                let desired = {
                    let d = self_recon.desired_nodes.lock().await;
                    d.clone()
                };

                for addr in desired {
                    if self_recon.advertised_addr == addr {
                        continue;
                    }

                    {
                        let conns = self_recon.connections.lock().await;
                        if conns.contains_key(&addr) {
                            continue;
                        }
                    }

                    match tokio::time::timeout(
                        Duration::from_millis(1500),
                        tokio::net::TcpStream::connect(&addr),
                    )
                    .await
                    {
                        Ok(Ok(socket)) => {
                            let _ = socket.set_nodelay(true);
                            let mut conns = self_recon.connections.lock().await;
                            conns.insert(addr, Arc::new(Mutex::new(socket)));
                        }
                        _ => {}
                    }
                }

                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });

        // Inbound Listener
        let self_listen = self.clone();
        let h_listen = tokio::spawn(async move {
            let addr = self_listen.listen_addr.clone();
            let listener = match tokio::net::TcpListener::bind(&addr).await {
                Ok(l) => l,
                Err(e) => {
                    tracing::error!("Failed to bind TCP listener {}: {}", addr, e);
                    return;
                }
            };

            loop {
                match listener.accept().await {
                    Ok((mut socket, _peer_addr)) => {
                        let self_peer = self_listen.clone();
                        let peer_task = async move {
                            let mut len_buf = [0u8; 4];
                            use tokio::io::AsyncReadExt;
                            let my_node_id = self_peer.node_id;

                            loop {
                                match socket.read_exact(&mut len_buf).await {
                                    Ok(_) => {
                                        let body_len = u32::from_be_bytes(len_buf) as usize;
                                        if body_len > 10 * 1024 * 1024 { break; }

                                        let mut body = vec![0u8; body_len];
                                        if socket.read_exact(&mut body).await.is_err() { break; }
                                        if body.len() < 16 { continue; }

                                        let mut id_bytes = [0u8; 16];
                                        id_bytes.copy_from_slice(&body[..16]);
                                        let sender_id = Uuid::from_bytes(id_bytes);

                                        if sender_id == my_node_id { continue; }

                                        let payload = body[16..].to_vec();
                                        let _ = self_peer.inbound_tx.send(payload);
                                    }
                                    Err(_) => break,
                                }
                                tokio::task::yield_now().await;
                            }
                        };
                        if let Some(ref l) = self_listen.task_limiter {
                            l.spawn(peer_task).await;
                        } else {
                            tokio::spawn(peer_task);
                        }
                    }
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        });

        let mut reg = task_registry.lock().await;
        reg.add(h_watch.abort_handle());
        reg.add(h_recon.abort_handle());
        reg.add(h_listen.abort_handle());
    }

    fn frame_payload(&self, payload: &[u8]) -> Vec<u8> {
        let body_len = 16 + payload.len();
        let mut frame = Vec::with_capacity(4 + body_len);
        frame.extend_from_slice(&(body_len as u32).to_be_bytes());
        frame.extend_from_slice(self.node_id.as_bytes());
        frame.extend_from_slice(payload);
        frame
    }
}

// ── Discovery handler ───────────────────────────────────────────────────

struct TcpDiscoveryHandler {
    connections: Arc<Mutex<HashMap<String, Arc<Mutex<tokio::net::TcpStream>>>>>,
    desired_nodes: Arc<Mutex<HashSet<String>>>,
    advertised_addr: String,
}

#[async_trait]
impl DiscoveryHandler for TcpDiscoveryHandler {
    async fn on_node_added(&self, node: Node) {
        if same_endpoint(&node.address, &self.advertised_addr) {
            return;
        }
        let mut desired = self.desired_nodes.lock().await;
        desired.insert(node.address);
    }

    async fn on_node_removed(&self, node: Node) {
        let mut desired = self.desired_nodes.lock().await;
        desired.remove(&node.address);
        let mut conns = self.connections.lock().await;
        conns.remove(&node.address);
    }
}

// ── PubSubBackend implementation ────────────────────────────────────────

#[async_trait]
impl PubSubBackend for TcpPubSub {
    async fn publish_bytes(&self, _topic: &str, payload: Vec<u8>) -> Result<(), DistributedError> {
        self.check_quorum().await?;

        let frame = Arc::new(self.frame_payload(&payload));

        let peers: Vec<(String, Arc<Mutex<tokio::net::TcpStream>>)> = {
            let conns = self.connections.lock().await;
            debug_println!("DEBUG: Node {} publishing to {} peers: {:?}", self.node_id, conns.len(), conns.keys().collect::<Vec<_>>());
            conns
                .iter()
                .filter(|(addr, _)| !same_endpoint(addr, &self.advertised_addr))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        };

        if peers.is_empty() {
             debug_println!("DEBUG: Node {} has NO peers to publish to", self.node_id);
        }

        let futs: Vec<_> = peers
            .into_iter()
            .map(|(addr, peer)| {
                let frame = frame.clone();
                async move {
                    let mut guard = peer.lock().await;
                    if guard.write_all(&frame).await.is_ok() {
                        return Ok(());
                    }
                    match tokio::time::timeout(
                        Duration::from_secs(2),
                        tokio::net::TcpStream::connect(&addr),
                    )
                    .await
                    {
                        Ok(Ok(mut new_s)) => {
                            let _ = new_s.set_nodelay(true);
                            if new_s.write_all(&frame).await.is_ok() {
                                *guard = new_s;
                                return Ok(());
                            }
                        }
                        _ => {}
                    }
                    Err(addr)
                }
            })
            .collect();

        let results = futures::future::join_all(futs).await;
        let failed: Vec<String> = results.into_iter().filter_map(Result::err).collect();
        if !failed.is_empty() {
            let mut conns = self.connections.lock().await;
            for addr in &failed {
                conns.remove(addr);
            }
            tracing::warn!("Publish failed to {} peer(s): {:?}", failed.len(), failed);
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
            Err(DistributedError::NoQuorum)
        }
    }

    fn subscribe_bytes(
        &self,
        _topic: &str,
    ) -> BoxStream<'static, Result<Vec<u8>, DistributedError>> {
        use tokio_stream::StreamExt;
        let rx = self.inbound_tx.subscribe();
        let stream = tokio_stream::wrappers::BroadcastStream::new(rx).map(|res| match res {
            Ok(v) => Ok(v),
            Err(e) => Err(DistributedError::Network(e.to_string())),
        });
        Box::pin(stream)
    }
}

fn same_endpoint(a: &str, b: &str) -> bool {
    if a == b { return true; }
    let a_parts: Vec<&str> = a.split(':').collect();
    let b_parts: Vec<&str> = b.split(':').collect();
    if a_parts.len() == 2 && b_parts.len() == 2 && a_parts[1] == b_parts[1] {
        if (a_parts[0] == "127.0.0.1" || a_parts[0] == "localhost" || a_parts[0] == "0.0.0.0") &&
           (b_parts[0] == "127.0.0.1" || b_parts[0] == "localhost" || b_parts[0] == "0.0.0.0") {
            return true;
        }
    }
    false
}
