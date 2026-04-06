use std::sync::Arc;
use serde::{Deserialize, Serialize};
use crate::{
    bus::EventBus,
    distributed::DistributedPubSub,
    event::{Event, EventPayload},
    projection::{Projection, ProjectionError},
    store::EventStore,
};
use super::OnnxEmbeddingService;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum EmbeddingEvent {
    TextChunkCreated { id: String, text: String },
    EmbeddingExtracted { id: String, embedding: Vec<f32> },
}

impl EventPayload for EmbeddingEvent {
    fn event_type(&self) -> &'static str {
        match self {
            EmbeddingEvent::TextChunkCreated { .. } => "TextChunkCreated",
            EmbeddingEvent::EmbeddingExtracted { .. } => "EmbeddingExtracted",
        }
    }
}

pub struct EmbeddingProjection;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct EmbeddingState {
    pub embeddings: HashMap<String, Vec<f32>>,
}

impl Projection<EmbeddingEvent, EmbeddingState> for EmbeddingProjection {
    fn name(&self) -> &'static str {
        "embedding_projection"
    }

    fn handle(
        &self,
        state: &mut EmbeddingState,
        event: &Event<EmbeddingEvent>,
    ) -> Result<(), ProjectionError> {
        if let EmbeddingEvent::EmbeddingExtracted { id, embedding } = &event.payload {
            state.embeddings.insert(id.clone(), embedding.clone());
        }
        Ok(())
    }
}

/// An actor that listens for TextChunkCreated events and publishes EmbeddingExtracted events.
pub struct EmbeddingServiceActor<ES: EventStore<EmbeddingEvent>> {
    service: Arc<OnnxEmbeddingService>,
    bus: EventBus<EmbeddingEvent>,
    mesh: Arc<dyn DistributedPubSub<EmbeddingEvent>>,
    event_store: Arc<ES>,
}

impl<ES: EventStore<EmbeddingEvent>> EmbeddingServiceActor<ES> {
    pub fn new(
        service: Arc<OnnxEmbeddingService>,
        bus: EventBus<EmbeddingEvent>,
        mesh: Arc<dyn DistributedPubSub<EmbeddingEvent>>,
        event_store: Arc<ES>,
    ) -> Self {
        Self {
            service,
            bus,
            mesh,
            event_store,
        }
    }

    pub async fn spawn(self) {
        let mut rx = self.bus.subscribe();
        let service = self.service.clone();
        let bus = self.bus.clone();
        let mesh = self.mesh.clone();
        let event_store = self.event_store.clone();

        tokio::spawn(async move {
            let mut batch = Vec::new();
            let mut timer = tokio::time::interval(std::time::Duration::from_millis(50));
            // First tick passes immediately
            timer.tick().await;

            loop {
                tokio::select! {
                    res = rx.recv() => {
                        match res {
                            Ok(event) => {
                                if let EmbeddingEvent::TextChunkCreated { id, text } = &event.payload {
                                    batch.push((id.clone(), text.clone(), event.sequence_num));
                                    if batch.len() >= 16 {
                                        Self::process_batch(&mut batch, &service, &bus, &mesh, &event_store).await;
                                    }
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        }
                    }
                    _ = timer.tick() => {
                        if !batch.is_empty() {
                            Self::process_batch(&mut batch, &service, &bus, &mesh, &event_store).await;
                        }
                    }
                }
            }
        });
    }

    async fn process_batch(
        batch: &mut Vec<(String, String, u64)>,
        service: &Arc<OnnxEmbeddingService>,
        bus: &EventBus<EmbeddingEvent>,
        mesh: &Arc<dyn DistributedPubSub<EmbeddingEvent>>,
        event_store: &Arc<ES>,
    ) {
        let texts: Vec<String> = batch.iter().map(|(_, text, _)| text.clone()).collect();
        
        match service.embed_batch(&texts) {
            Ok(embeddings) => {
                let mut events_to_store = Vec::new();
                for ((id, _, seq), embedding) in batch.drain(..).zip(embeddings) {
                    let new_payload = EmbeddingEvent::EmbeddingExtracted { id: id.clone(), embedding };
                    let new_event = Event::new(id, seq + 1, new_payload);
                    events_to_store.push(new_event);
                }

                if let Ok(stored) = event_store.append(events_to_store).await {
                    for event in stored {
                        let _ = bus.publish(event.clone());
                        let _ = mesh.publish(&event).await;
                    }
                }
            }
            Err(e) => {
                eprintln!("Batch embedding failed: {}", e);
                batch.clear();
            }
        }
    }
}
