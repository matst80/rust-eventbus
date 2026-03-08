// Minimal libp2p adapter placeholder used when the feature is enabled.
// For now this provides a compileable shim that behaves like a simple
// broadcast-based backend so we can validate wiring. Replace with a real
// libp2p implementation later.
#[cfg(feature = "libp2p-backend")]
mod libp2p_adapter {
    use futures::stream::BoxStream;
    use futures::StreamExt;
    use std::error::Error;
    use std::sync::Arc;
    use tokio::sync::{broadcast, mpsc};

    use crate::distributed::{DistributedError, PubSubBackend};
    use crate::libp2p_swarm;

    pub struct Libp2pAdapter {
        topic: String,
        publish_tx: mpsc::Sender<Vec<u8>>,
        inbound_tx: broadcast::Sender<Vec<u8>>,
    }

    impl Libp2pAdapter {
        pub async fn new(topic: &str) -> Result<Arc<Self>, Box<dyn Error + Send + Sync>> {
            // spawn the real swarm (no explicit listen addr)
            let handle = libp2p_swarm::spawn_swarm(None).await?;
            Ok(Arc::new(Self { topic: topic.to_string(), publish_tx: handle.publish_tx, inbound_tx: handle.inbound_tx }))
        }

        pub async fn new_with_addrs(
            _node_id: uuid::Uuid,
            listen_addr: String,
            _advertised_addr: String,
            _discovery: std::sync::Arc<dyn crate::distributed::NodeDiscovery>,
        ) -> Result<Arc<Self>, Box<dyn Error + Send + Sync>> {
            let handle = libp2p_swarm::spawn_swarm(Some(listen_addr)).await?;
            Ok(Arc::new(Self { topic: "eventbus".to_string(), publish_tx: handle.publish_tx, inbound_tx: handle.inbound_tx }))
        }
    }

    #[async_trait::async_trait]
    impl PubSubBackend for Libp2pAdapter {
        async fn publish_bytes(&self, _topic: &str, payload: Vec<u8>) -> Result<(), DistributedError> {
            self.publish_tx
                .send(payload)
                .await
                .map_err(|e| DistributedError::Network(format!("publish channel closed: {}", e)))
        }

        fn subscribe_bytes(&self, _topic: &str) -> BoxStream<'static, Result<Vec<u8>, DistributedError>> {
            let rx = self.inbound_tx.subscribe();
            let stream = tokio_stream::wrappers::BroadcastStream::new(rx)
                .map(|res| match res {
                    Ok(v) => Ok(v),
                    Err(e) => Err(DistributedError::Network(e.to_string())),
                });
            Box::pin(stream)
        }
    }
}

#[cfg(feature = "libp2p-backend")]
pub use libp2p_adapter::Libp2pAdapter;
