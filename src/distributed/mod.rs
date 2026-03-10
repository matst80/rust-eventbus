use uuid::Uuid;

pub mod discovery;
pub mod locks;
pub mod pubsub;
pub mod quorum_lock;
pub mod store;
pub mod tcp;
pub mod types;

pub use discovery::*;
pub use locks::*;
pub use pubsub::*;
pub use quorum_lock::*;
pub use store::*;
pub use tcp::*;
pub use types::*;

use crate::event::EventPayload;

/// Factory that returns the default mesh implementation as a trait object.
/// Creates a `TcpPubSub` (bytes-oriented) wrapped in `BackendPubSub` (typed).
pub async fn default_mesh<E>(
    node_id: Uuid,
    listen_addr: String,
    advertised_addr: String,
    discovery: std::sync::Arc<dyn NodeDiscovery>,
    task_limiter: Option<crate::task_limiter::TaskLimiter>,
    event_store: Option<std::sync::Arc<dyn crate::store::EventStore<E>>>,
) -> std::sync::Arc<dyn DistributedPubSub<E>>
where
    E: EventPayload + serde::Serialize + for<'de> serde::Deserialize<'de> + 'static,
{
    let mut tcp =
        TcpPubSub::new_with_advertised_addr(node_id, listen_addr, advertised_addr, discovery);
    if let Some(l) = task_limiter {
        tcp = tcp.with_task_limiter(l);
    }
    let tcp = std::sync::Arc::new(tcp);
    tcp.clone().start_background_manager().await;

    let backend: std::sync::Arc<dyn PubSubBackend> = tcp.clone();
    let mesh = std::sync::Arc::new(BackendPubSub::new(backend, "eventbus".to_string()));

    // Background Sync/Anti-Entropy Manager
    if let Some(store) = event_store {
        let mesh_clone = mesh.clone();
        let tcp_clone = tcp.clone();
        tokio::spawn(async move {
            use futures::StreamExt;
            let mut raw_incoming = tcp_clone.subscribe_bytes("eventbus");
            while let Some(res) = raw_incoming.next().await {
                if let Ok(bytes) = res {
                    if let Ok(envelope) = EventCodec::decode::<E>(&bytes) {
                        match envelope {
                            MeshEnvelope::SyncRequest { from_seq } => {
                                tracing::debug!("Received SyncRequest from seq {}", from_seq);
                                let mut events_stream = store.read_all_from(from_seq + 1);
                                while let Some(Ok(event)) = events_stream.next().await {
                                    let _ = mesh_clone.publish(&event).await;
                                }
                            }
                            MeshEnvelope::Event(_) => {}
                        }
                    }
                }
            }
        });

        // Request a sync on startup to catch up with existing nodes
        let mesh_clone = mesh.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let _ = DistributedPubSub::<E>::request_sync(mesh_clone.as_ref(), 0).await;
        });
    }

    mesh
}
