use std::sync::Arc;
use tracing::info;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use crate::{
    bus::EventBus,
    distributed::DistributedPubSub,
    event::{Event, EventPayload},
    projection::{Projection, ProjectionError},
    store::EventStore,
    app_event::{AppEvent, GraphEvent},
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
 
impl Projection<AppEvent, EmbeddingState> for EmbeddingProjection {
    fn name(&self) -> &'static str {
        "embedding_projection"
    }
 
    fn handle(
        &self,
        state: &mut EmbeddingState,
        event: &Event<AppEvent>,
    ) -> Result<(), ProjectionError> {
        if let AppEvent::Embedding(EmbeddingEvent::EmbeddingExtracted { id, embedding }) = &event.payload {
            state.embeddings.insert(id.clone(), embedding.clone());
        }
        Ok(())
    }
}

/// An actor that listens for TextChunkCreated or RequestEmbedding events and publishes EmbeddingExtracted events.
pub struct EmbeddingServiceActor<ES: EventStore<AppEvent>> {
    service: Arc<OnnxEmbeddingService>,
    bus: EventBus<AppEvent>,
    mesh: Arc<dyn DistributedPubSub<AppEvent>>,
    event_store: Arc<ES>,
    /// Cache to avoid re-embedding the same ID in the same run/replay
    processed_ids: Arc<parking_lot::Mutex<std::collections::HashSet<String>>>,
    /// IDs currently queued or in-flight so large content strings are not retained multiple times.
    pending_ids: Arc<parking_lot::Mutex<std::collections::HashSet<String>>>,
}

impl<ES: EventStore<AppEvent>> EmbeddingServiceActor<ES> {
    pub async fn new(
        service: Arc<OnnxEmbeddingService>,
        bus: EventBus<AppEvent>,
        mesh: Arc<dyn DistributedPubSub<AppEvent>>,
        event_store: Arc<ES>,
    ) -> Result<Self, crate::store::StoreError> {
        let processed_ids = Arc::new(parking_lot::Mutex::new(std::collections::HashSet::new()));
        let pending_ids = Arc::new(parking_lot::Mutex::new(std::collections::HashSet::new()));
        
        // Pre-populate cache from store to avoid re-embedding on restart
        let mut stream = event_store.read_all_from(0);
        {
            let mut lock = processed_ids.lock();
            while let Some(Ok(event)) = stream.next().await {
                if let AppEvent::Embedding(EmbeddingEvent::EmbeddingExtracted { id, .. }) = &event.payload {
                    lock.insert(id.clone());
                }
            }
        }
        drop(stream);

        Ok(Self {
            service,
            bus,
            mesh,
            event_store,
            processed_ids,
            pending_ids,
        })
    }

    pub async fn spawn(self) {
        let (tx, mut worker_rx) = tokio::sync::mpsc::channel(4096);
        let bus = self.bus.clone();
        // Explicit single Arc clone — the only reference to the ONNX model kept alive after `self` is dropped.
        let service = Arc::clone(&self.service);
        let mesh = self.mesh.clone();
        let event_store = self.event_store.clone();
        // Two explicit clones so `self` can be dropped before spawning tasks.
        let processed_ids_rx = Arc::clone(&self.processed_ids);
        let processed_ids_worker = Arc::clone(&self.processed_ids);
        let pending_ids_rx = Arc::clone(&self.pending_ids);
        let pending_ids_worker = Arc::clone(&self.pending_ids);
        let mut bus_rx = self.bus.subscribe();
        // Drop self here so OnnxEmbeddingService has exactly one Arc reference (via `service`).
        drop(self);

        // 1. Receiver Task: Always drain the bus to internal queue (prevent lagging)
        tokio::spawn(async move {
            loop {
                match bus_rx.recv().await {
                    Ok(event) => {
                        match &event.payload {
                            AppEvent::Embedding(EmbeddingEvent::TextChunkCreated { id, text }) => {
                                if Self::mark_pending(&processed_ids_rx, &pending_ids_rx, id) {
                                    if let Err(_) = tx.try_send((id.clone(), text.clone(), event.sequence_num)) {
                                        // If internal channel is full, we must block to preserve ordering/backpressure
                                        // but only for a bit to avoid overall bus lag if possible.
                                        let _ = tx.send((id.clone(), text.clone(), event.sequence_num)).await;
                                    }
                                }
                            },
                            AppEvent::Graph(GraphEvent::RequestEmbedding { id, content }) => {
                                if Self::mark_pending(&processed_ids_rx, &pending_ids_rx, id) {
                                    if let Err(_) = tx.try_send((id.clone(), content.clone(), event.sequence_num)) {
                                        let _ = tx.send((id.clone(), content.clone(), event.sequence_num)).await;
                                    }
                                }
                            },
                            _ => {}
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        eprintln!("EmbeddingServiceActor: Lagged by {} events. Consider increasing bus capacity.", n);
                        tokio::task::yield_now().await;
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });

        // 2. Worker Task: Batch and process
        tokio::spawn(async move {
            let mut batch = Vec::new();
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));

            loop {
                tokio::select! {
                    Some(item) = worker_rx.recv() => {
                        batch.push(item);
                        if batch.len() >= 64 {
                            info!("EmbeddingActor: Batch full (64), processing...");
                            Self::process_batch(&mut batch, &service, &bus, &mesh, &event_store, &processed_ids_worker, &pending_ids_worker).await;
                        }
                    }
                    _ = interval.tick() => {
                        if !batch.is_empty() {
                            info!("EmbeddingActor: Interval flush ({} items), processing...", batch.len());
                            Self::process_batch(&mut batch, &service, &bus, &mesh, &event_store, &processed_ids_worker, &pending_ids_worker).await;
                        }
                    }
                }
            }
        });
    }

    fn mark_pending(
        processed_ids: &Arc<parking_lot::Mutex<std::collections::HashSet<String>>>,
        pending_ids: &Arc<parking_lot::Mutex<std::collections::HashSet<String>>>,
        id: &str,
    ) -> bool {
        if processed_ids.lock().contains(id) {
            return false;
        }

        let mut pending = pending_ids.lock();
        if pending.contains(id) {
            return false;
        }

        pending.insert(id.to_string());
        true
    }

    async fn process_batch(
        batch: &mut Vec<(String, String, u64)>,
        service: &Arc<OnnxEmbeddingService>,
        bus: &EventBus<AppEvent>,
        mesh: &Arc<dyn DistributedPubSub<AppEvent>>,
        event_store: &Arc<ES>,
        processed_ids: &Arc<parking_lot::Mutex<std::collections::HashSet<String>>>,
        pending_ids: &Arc<parking_lot::Mutex<std::collections::HashSet<String>>>,
    ) {
        if batch.is_empty() { return; }

        let work_items = std::mem::take(batch);
        let mut item_meta = Vec::with_capacity(work_items.len());
        let mut texts = Vec::with_capacity(work_items.len());
        for (id, text, seq) in work_items {
            item_meta.push((id, seq));
            texts.push(text);
        }
        let service_clone = service.clone();
        
        let result = tokio::task::spawn_blocking(move || {
            service_clone.embed_batch(texts)
        }).await;

        match result {
            Ok(Ok(embeddings)) => {
                let mut events_to_store = Vec::new();
                for ((id, seq), embedding) in item_meta.iter().cloned().zip(embeddings) {
                    let new_payload = AppEvent::Embedding(EmbeddingEvent::EmbeddingExtracted {
                        id: id.clone(),
                        embedding,
                    });
                    let new_event = Event::new(id, seq + 1, new_payload);
                    events_to_store.push(new_event);
                }

                match event_store.append(events_to_store).await {
                    Ok(stored) => {
                        {
                            let mut processed = processed_ids.lock();
                            let mut pending = pending_ids.lock();
                            for (id, _) in &item_meta {
                                processed.insert(id.clone());
                                pending.remove(id);
                            }
                        }

                        for event in stored {
                            let _ = bus.publish(event.clone());
                            let _ = mesh.publish(&event).await;
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to store embeddings: {}", e);
                        let mut pending = pending_ids.lock();
                        for (id, _) in item_meta {
                            pending.remove(&id);
                        }
                    }
                }
            }
            Ok(Err(e)) => {
                eprintln!("Batch embedding failed: {}", e);
                let mut pending = pending_ids.lock();
                for (id, _) in item_meta {
                    pending.remove(&id);
                }
            }
            Err(e) => {
                eprintln!("Blocking task panicked: {}", e);
                let mut pending = pending_ids.lock();
                for (id, _) in item_meta {
                    pending.remove(&id);
                }
            }
        }
    }
}
