use crate::distributed::{DnsNodeDiscovery, EnvironmentNodeDiscovery, NodeDiscovery, ProjectionLockManager, DistributedPubSub};
use crate::store::{FileEventStore, FileSnapshotStore};
use crate::event::EventPayload;
use std::sync::Arc;
use uuid::Uuid;
use std::error::Error;
use std::sync::atomic::AtomicU64;
use tokio::io::AsyncWriteExt;

/// File-based lease lock manager used by examples/apps to coordinate durable projections.
pub struct FileLeaseLockManager {
    lock_dir: String,
    lease_ttl: std::time::Duration,
}

impl FileLeaseLockManager {
    pub fn new(lock_dir: String, lease_ttl: std::time::Duration) -> Self {
        Self { lock_dir, lease_ttl }
    }

    fn lock_path(&self, projection_name: &str) -> String {
        format!("{}/{}.lock", self.lock_dir, projection_name)
    }

    async fn read_owner(&self, lock_path: &str) -> Result<Option<Uuid>, crate::distributed::LockError> {
        match tokio::fs::read_to_string(lock_path).await {
            Ok(content) => Ok(Uuid::parse_str(content.trim()).ok()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(crate::distributed::LockError::Storage(e.to_string())),
        }
    }

    async fn is_stale(&self, lock_path: &str) -> Result<bool, crate::distributed::LockError> {
        match tokio::fs::metadata(lock_path).await {
            Ok(meta) => {
                let modified = meta.modified().map_err(|e| crate::distributed::LockError::Storage(e.to_string()))?;
                let age = std::time::SystemTime::now()
                    .duration_since(modified)
                    .unwrap_or_default();
                Ok(age > self.lease_ttl)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(crate::distributed::LockError::Storage(e.to_string())),
        }
    }

    async fn write_owner(&self, lock_path: &str, node_id: &Uuid) -> Result<(), crate::distributed::LockError> {
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(lock_path)
            .await
            .map_err(|e| crate::distributed::LockError::Storage(e.to_string()))?;

        file.write_all(node_id.to_string().as_bytes())
            .await
            .map_err(|e| crate::distributed::LockError::Storage(e.to_string()))
    }
}

#[async_trait::async_trait]
impl ProjectionLockManager for FileLeaseLockManager {
    async fn acquire_lock(&self, projection_name: &str, node_id: &Uuid) -> Result<bool, crate::distributed::LockError> {
        tokio::fs::create_dir_all(&self.lock_dir)
            .await
            .map_err(|e| crate::distributed::LockError::Storage(e.to_string()))?;

        let lock_path = self.lock_path(projection_name);
        match tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&lock_path)
            .await
        {
            Ok(mut file) => {
                file.write_all(node_id.to_string().as_bytes())
                    .await
                    .map_err(|e| crate::distributed::LockError::Storage(e.to_string()))?;
                return Ok(true);
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(crate::distributed::LockError::Storage(e.to_string())),
        }

        if let Some(owner) = self.read_owner(&lock_path).await? {
            if owner == *node_id {
                self.write_owner(&lock_path, node_id).await?;
                return Ok(true);
            }
        }

        if self.is_stale(&lock_path).await? {
            if let Err(e) = tokio::fs::remove_file(&lock_path).await {
                if e.kind() != std::io::ErrorKind::NotFound {
                    return Err(crate::distributed::LockError::Storage(e.to_string()));
                }
            }

            match tokio::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&lock_path)
                .await
            {
                Ok(mut file) => {
                    file.write_all(node_id.to_string().as_bytes())
                        .await
                        .map_err(|e| crate::distributed::LockError::Storage(e.to_string()))?;
                    return Ok(true);
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => return Ok(false),
                Err(e) => return Err(crate::distributed::LockError::Storage(e.to_string())),
            }
        }

        Ok(false)
    }

    async fn keep_alive(&self, projection_name: &str, node_id: &Uuid) -> Result<(), crate::distributed::LockError> {
        let lock_path = self.lock_path(projection_name);
        match self.read_owner(&lock_path).await? {
            Some(owner) if owner == *node_id => self.write_owner(&lock_path, node_id).await,
            _ => Err(crate::distributed::LockError::AlreadyHeld),
        }
    }

    async fn release_lock(&self, projection_name: &str, node_id: &Uuid) -> Result<(), crate::distributed::LockError> {
        let lock_path = self.lock_path(projection_name);
        if let Some(owner) = self.read_owner(&lock_path).await? {
            if owner != *node_id {
                return Ok(());
            }
        } else {
            return Ok(());
        }

        if let Err(e) = tokio::fs::remove_file(lock_path).await {
            if e.kind() != std::io::ErrorKind::NotFound {
                return Err(crate::distributed::LockError::Storage(e.to_string()));
            }
        }
        Ok(())
    }
}

/// Aggregate of components needed to wire an application node.
pub struct ClusterComponents<E: EventPayload + serde::Serialize + for<'de> serde::Deserialize<'de>> {
    pub node_id: Uuid,
    pub mesh: Arc<dyn DistributedPubSub<E>>,
    pub event_store: Arc<FileEventStore>,
    pub snapshot_store: Arc<FileSnapshotStore>,
    pub lock_manager: Arc<dyn ProjectionLockManager>,
    pub mesh_bind_addr: String,
    pub mesh_advertised_addr: String,
    pub data_dir: String,
    pub projection_version: Arc<AtomicU64>,
    pub discovery: Arc<dyn NodeDiscovery>,
}

/// Initialize cluster-related components from environment variables (DATA_DIR, PORT, MESH_* etc.).
pub async fn init_cluster<
    E: EventPayload + serde::Serialize + for<'de> serde::Deserialize<'de>,
>() -> Result<ClusterComponents<E>, Box<dyn Error>> {
    let _port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "3000".to_string())
        .parse()
        .expect("PORT must be a number");

    let mesh_port: u16 = std::env::var("MESH_PORT")
        .unwrap_or_else(|_| "3001".to_string())
        .parse()
        .expect("MESH_PORT must be a number");

    let host = std::env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let mesh_bind_host = std::env::var("MESH_BIND_HOST").unwrap_or_else(|_| host.clone());
    let mesh_advertise_host = std::env::var("MESH_ADVERTISE_HOST")
        .ok()
        .or_else(|| std::env::var("POD_IP").ok())
        .unwrap_or_else(|| mesh_bind_host.clone());

    let data_dir = std::env::var("DATA_DIR").unwrap_or_else(|_| "./data".to_string());

    let node_id_str = std::env::var("NODE_ID").ok();
    let node_id = node_id_str
        .and_then(|s| Uuid::parse_str(&s).ok())
        .unwrap_or_else(Uuid::new_v4);

    // Node Discovery
    let peer_discovery: Arc<dyn NodeDiscovery> = if let Ok(dns_query) = std::env::var("DNS_QUERY") {
        Arc::new(DnsNodeDiscovery::new(dns_query, mesh_port))
    } else {
        Arc::new(EnvironmentNodeDiscovery::new())
    };

    tokio::fs::create_dir_all(&data_dir).await?;

    let event_store_path = format!("{}/events.bin", data_dir);
    let snapshot_store_path = format!("{}/snapshots", data_dir);

    let event_store = Arc::new(FileEventStore::new(&event_store_path).await?);
    let snapshot_store = Arc::new(FileSnapshotStore::new(&snapshot_store_path).await?);

    let mesh_bind_addr = format!("{}:{}", mesh_bind_host, mesh_port);
    let mesh_advertised_addr = format!("{}:{}", mesh_advertise_host, mesh_port);

    let mesh: Arc<dyn DistributedPubSub<E>> = crate::distributed::default_mesh(
        node_id,
        mesh_bind_addr.clone(),
        mesh_advertised_addr.clone(),
        peer_discovery.clone(),
    )
    .await;

    let lock_dir = format!("{}/locks", data_dir);
    let lock_manager = Arc::new(FileLeaseLockManager::new(
        lock_dir,
        std::time::Duration::from_secs(15),
    ));

    let projection_version = Arc::new(AtomicU64::new(0));

    Ok(ClusterComponents {
        node_id,
        mesh,
        event_store,
        snapshot_store,
        lock_manager,
        mesh_bind_addr,
        mesh_advertised_addr,
        data_dir,
        projection_version,
        discovery: peer_discovery,
    })
}
