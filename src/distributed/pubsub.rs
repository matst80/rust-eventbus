use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::sync::Arc;

use crate::event::{Event, EventPayload};
use super::types::DistributedError;

/// A minimal backend trait that operates on raw message bytes.
#[async_trait]
pub trait PubSubBackend: Send + Sync + 'static {
    async fn publish_bytes(&self, topic: &str, payload: Vec<u8>) -> Result<(), DistributedError>;
    fn subscribe_bytes(&self, topic: &str)
        -> BoxStream<'static, Result<Vec<u8>, DistributedError>>;
    async fn check_quorum(&self) -> Result<(), DistributedError>;
}

/// Internal wrapper for messages sent over the mesh.
/// Allows mixing application events with control messages (like sync requests).
#[derive(Serialize, Deserialize)]
#[serde(bound = "E: Serialize + DeserializeOwned")]
pub enum MeshEnvelope<E> {
    Event(Event<E>),
    SyncRequest { from_seq: u64 },
}

/// Helper for encoding/decoding `MeshEnvelope<E>` to/from bytes for wire transport.
pub struct EventCodec;

impl EventCodec {
    pub fn encode<E: EventPayload + Serialize + DeserializeOwned>(
        env: &MeshEnvelope<E>,
    ) -> Result<Vec<u8>, DistributedError> {
        bincode::serialize(env).map_err(|e| DistributedError::Network(e.to_string()))
    }

    pub fn decode<E: EventPayload + Serialize + DeserializeOwned>(
        bytes: &[u8],
    ) -> Result<MeshEnvelope<E>, DistributedError> {
        bincode::deserialize(bytes).map_err(|e| DistributedError::Network(e.to_string()))
    }
}

/// A zero-dependency direct socket-based peer-to-peer clustering PubSub interface.
/// Replaces `tokio::sync::broadcast` for cross-node communication.
#[async_trait]
pub trait DistributedPubSub<E: EventPayload + Serialize + DeserializeOwned>:
    Send + Sync + 'static
{
    /// Broadcast an event to the clustered mesh.
    async fn publish(&self, event: &Event<E>) -> Result<(), DistributedError>;

    /// Subscribes to events arriving from other nodes over the network.
    async fn subscribe(&self) -> BoxStream<'static, Result<Event<E>, DistributedError>>;

    /// Request a sync from peers starting from a sequence number.
    async fn request_sync(&self, from_seq: u64) -> Result<(), DistributedError>;

    /// Verify that a majority of nodes in the cluster are reachable.
    async fn check_quorum(&self) -> Result<(), DistributedError>;
}

/// Adapter that implements the typed `DistributedPubSub<E>` by delegating to
/// a bytes-oriented `PubSubBackend` and using `EventCodec` for (de)serialization.
pub struct BackendPubSub {
    backend: Arc<dyn PubSubBackend>,
    topic: String,
}

impl BackendPubSub {
    pub fn new(backend: Arc<dyn PubSubBackend>, topic: String) -> Self {
        Self { backend, topic }
    }
}

#[async_trait]
impl<E: EventPayload + Serialize + DeserializeOwned> DistributedPubSub<E> for BackendPubSub {
    async fn publish(&self, event: &Event<E>) -> Result<(), DistributedError> {
        let env = MeshEnvelope::Event(event.clone());
        let bytes = EventCodec::encode(&env)?;
        self.backend.publish_bytes(&self.topic, bytes).await
    }

    async fn subscribe(&self) -> BoxStream<'static, Result<Event<E>, DistributedError>> {
        let stream = self.backend.subscribe_bytes(&self.topic);
        let mapped = stream.filter_map(|res| async move {
            match res {
                Ok(bytes) => match EventCodec::decode::<E>(&bytes) {
                    Ok(MeshEnvelope::Event(ev)) => Some(Ok(ev)),
                    Ok(MeshEnvelope::SyncRequest { .. }) => None, // Handled by separate sync listener
                    Err(e) => Some(Err(e)),
                },
                Err(e) => Some(Err(e)),
            }
        });
        Box::pin(mapped)
    }

    async fn request_sync(&self, from_seq: u64) -> Result<(), DistributedError> {
        let env = MeshEnvelope::<E>::SyncRequest { from_seq };
        let bytes = EventCodec::encode(&env)?;
        self.backend.publish_bytes(&self.topic, bytes).await
    }

    async fn check_quorum(&self) -> Result<(), DistributedError> {
        self.backend.check_quorum().await
    }
}
