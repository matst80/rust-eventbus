use crate::distributed::{
    DistributedPubSub, DnsNodeDiscovery, EnvironmentNodeDiscovery, NodeDiscovery,
    ProjectionLockManager, PubSubBackend, QuorumLockManager, TcpPubSub,
};
use crate::event::EventPayload;
use crate::store::{FileEventStore, FileSnapshotStore};
use std::error::Error;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

/// File-based lease lock manager used by examples/apps to coordinate durable projections.
pub struct FileLeaseLockManager {
    lock_dir: String,
    lease_ttl: std::time::Duration,
}

impl FileLeaseLockManager {
    pub fn new(lock_dir: String, lease_ttl: std::time::Duration) -> Self {
        Self {
            lock_dir,
            lease_ttl,
        }
    }

    fn lock_path(&self, projection_name: &str) -> String {
        format!("{}/{}.lock", self.lock_dir, projection_name)
    }

    async fn read_owner(
        &self,
        lock_path: &str,
    ) -> Result<Option<Uuid>, crate::distributed::LockError> {
        match tokio::fs::read_to_string(lock_path).await {
            Ok(content) => Ok(Uuid::parse_str(content.trim()).ok()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(crate::distributed::LockError::Storage(e.to_string())),
        }
    }

    async fn is_stale(&self, lock_path: &str) -> Result<bool, crate::distributed::LockError> {
        match tokio::fs::metadata(lock_path).await {
            Ok(meta) => {
                let modified = meta
                    .modified()
                    .map_err(|e| crate::distributed::LockError::Storage(e.to_string()))?;
                let age = std::time::SystemTime::now()
                    .duration_since(modified)
                    .unwrap_or_default();
                Ok(age > self.lease_ttl)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(crate::distributed::LockError::Storage(e.to_string())),
        }
    }

    async fn write_owner(
        &self,
        lock_path: &str,
        node_id: &Uuid,
    ) -> Result<(), crate::distributed::LockError> {
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
    async fn acquire_lock(
        &self,
        projection_name: &str,
        node_id: &Uuid,
    ) -> Result<bool, crate::distributed::LockError> {
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

    async fn keep_alive(
        &self,
        projection_name: &str,
        node_id: &Uuid,
    ) -> Result<(), crate::distributed::LockError> {
        let lock_path = self.lock_path(projection_name);
        match self.read_owner(&lock_path).await? {
            Some(owner) if owner == *node_id => self.write_owner(&lock_path, node_id).await,
            _ => Err(crate::distributed::LockError::AlreadyHeld),
        }
    }

    async fn release_lock(
        &self,
        projection_name: &str,
        node_id: &Uuid,
    ) -> Result<(), crate::distributed::LockError> {
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
pub struct ClusterComponents<E: EventPayload + serde::Serialize + for<'de> serde::Deserialize<'de>>
{
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
    pub task_limiter: crate::task_limiter::TaskLimiter,
}
/// Initialize cluster-related components from environment variables (DATA_DIR, PORT, MESH_* etc.).
pub use crate::cluster_config::ClusterConfig;
pub async fn init_cluster<E: EventPayload + serde::Serialize + for<'de> serde::Deserialize<'de>>(
    config: ClusterConfig,
) -> Result<ClusterComponents<E>, Box<dyn Error>> {
    let _port: u16 = config.port;

    let mesh_port: u16 = config.mesh_port;

    let mesh_bind_host = config.mesh_bind_host.clone();
    let mesh_advertise_host = config.mesh_advertise_host.clone();

    let data_dir = config.data_dir.clone();

    let node_id = config.node_id.unwrap_or_else(Uuid::new_v4);

    let task_limiter = crate::task_limiter::TaskLimiter::new(config.max_concurrent_tasks);

    // Node Discovery
    let peer_discovery: Arc<dyn NodeDiscovery> = if let Some(dns_query) = config.dns_query {
        Arc::new(DnsNodeDiscovery::new(dns_query, mesh_port))
    } else {
        Arc::new(EnvironmentNodeDiscovery::new())
    };

    let node_data_dir = format!("{}/node_{}", data_dir, node_id);
    tokio::fs::create_dir_all(&node_data_dir).await?;

    let event_store_path = format!("{}/events.bin", node_data_dir);
    let snapshot_store_path = format!("{}/snapshots", node_data_dir);

    let event_store = Arc::new(FileEventStore::new(&event_store_path).await?);
    let snapshot_store = Arc::new(FileSnapshotStore::new(&snapshot_store_path).await?);

    let mesh_bind_addr = format!("{}:{}", mesh_bind_host, mesh_port);
    let mesh_advertised_addr = format!("{}:{}", mesh_advertise_host, mesh_port);

    let mesh: Arc<dyn DistributedPubSub<E>> = crate::distributed::default_mesh(
        node_id,
        mesh_bind_addr.clone(),
        mesh_advertised_addr.clone(),
        peer_discovery.clone(),
        Some(task_limiter.clone()),
        Some(event_store.clone()),
    )
    .await;

    // Select lock manager implementation based on env var.
    // If USE_QUORUM_LOCK=true we create a small quorum-based manager that
    // negotiates ownership over the mesh. Otherwise fall back to the
    // legacy FileLeaseLockManager used by examples.
    let use_quorum = std::env::var("USE_QUORUM_LOCK")
        .ok()
        .map(|s| s == "true")
        .unwrap_or(false);

    let lock_manager: Arc<dyn ProjectionLockManager> = if use_quorum {
        // Create a bytes-oriented backend (TcpPubSubBackend) for lock negotiation and start its background manager.
        let backend_concrete = Arc::new(TcpPubSub::new_with_advertised_addr(
            node_id,
            mesh_bind_addr.clone(),
            mesh_advertised_addr.clone(),
            peer_discovery.clone(),
        ));

        // Start the backend's background manager so it begins discovery/connect loops.
        {
            let bc = backend_concrete.clone();
            tokio::spawn(async move {
                bc.start_background_manager().await;
            });
        }

        // Coerce concrete backend to trait object for the QuorumLockManager API.
        let backend_trait: Arc<dyn PubSubBackend> = backend_concrete.clone();

        // Create quorum manager and start its listener to process lock-topic messages.
        let quorum_manager_concrete = Arc::new(QuorumLockManager::new(
            node_id,
            backend_trait,
            peer_discovery.clone(),
            std::time::Duration::from_secs(15),
        ));

        {
            let qm = quorum_manager_concrete.clone();
            tokio::spawn(async move {
                let _ = qm.start_listener().await;
            });
        }

        // Use quorum manager as the ProjectionLockManager trait object.
        quorum_manager_concrete as Arc<dyn ProjectionLockManager>
    } else {
        let lock_dir = format!("{}/locks", data_dir);
        Arc::new(FileLeaseLockManager::new(
            lock_dir,
            std::time::Duration::from_secs(15),
        ))
    };

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
        task_limiter,
    })
}
