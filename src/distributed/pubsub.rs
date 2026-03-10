use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use std::sync::Arc;
use crate::event::{Event, EventPayload};
use super::types::DistributedError;

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
