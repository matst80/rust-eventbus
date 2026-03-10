use uuid::Uuid;

pub mod types;
pub mod discovery;
pub mod pubsub;
pub mod tcp;
pub mod locks;
pub mod store;

pub use types::*;
pub use discovery::*;
pub use pubsub::*;
pub use tcp::*;
pub use locks::*;
pub use store::*;

use crate::event::EventPayload;

/// Factory that returns the default mesh implementation as a trait object.
/// By default this returns the `TcpPubSub` implementation. Enabling the
/// `libp2p-backend` feature will switch to the libp2p adapter if available.
pub async fn default_mesh<E>(
    node_id: Uuid,
    listen_addr: String,
    advertised_addr: String,
    discovery: std::sync::Arc<dyn NodeDiscovery>,
    task_limiter: Option<crate::task_limiter::TaskLimiter>,
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
    let mut mesh = TcpPubSub::new_with_advertised_addr(
        node_id,
        listen_addr,
        advertised_addr,
        discovery,
    );
    if let Some(l) = task_limiter {
        mesh = mesh.with_task_limiter(l);
    }
    let mesh = std::sync::Arc::new(mesh);
    mesh.clone().start_background_manager().await;
    mesh
}
