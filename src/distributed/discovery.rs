use async_trait::async_trait;
use uuid::Uuid;
use std::sync::Arc;
use super::types::{Node, DistributedError};

#[async_trait]
pub trait DiscoveryHandler: Send + Sync + 'static {
    /// Called when a new node is discovered.
    async fn on_node_added(&self, node: Node);
    /// Called when a node is no longer discovered.
    async fn on_node_removed(&self, node: Node);
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

    /// Watch for node changes. This may start a background task or loop.
    async fn watch(&self, handler: Arc<dyn DiscoveryHandler>) -> Result<(), DistributedError> {
        // Default implementation for backwards compatibility
        let nodes = self.discover_nodes().await?;
        for node in nodes {
            handler.on_node_added(node).await;
        }
        Ok(())
    }
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

    async fn watch(&self, handler: Arc<dyn DiscoveryHandler>) -> Result<(), DistributedError> {
        for addr in &self.peers {
            handler.on_node_added(Node {
                id: Uuid::nil(),
                address: addr.clone(),
            }).await;
        }
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

    async fn watch(&self, handler: Arc<dyn DiscoveryHandler>) -> Result<(), DistributedError> {
        let mut last_nodes = std::collections::HashSet::<String>::new();
        
        loop {
            if let Ok(nodes) = self.discover_nodes().await {
                let current_nodes: std::collections::HashSet<String> = nodes.into_iter().map(|n| n.address).collect();
                
                // Added
                for addr in current_nodes.difference(&last_nodes) {
                    handler.on_node_added(Node {
                        id: Uuid::nil(),
                        address: addr.clone(),
                    }).await;
                }
                
                // Removed
                for addr in last_nodes.difference(&current_nodes) {
                    handler.on_node_removed(Node {
                        id: Uuid::nil(),
                        address: addr.clone(),
                    }).await;
                }
                
                last_nodes = current_nodes;
            }
            
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    }
}
