use super::{event::EmbeddingEvent, OnnxEmbeddingService};
use crate::{
    app_event::{AppEvent, GraphEvent},
    bus::EventBus,
    event::Event,
    store::EventStore,
};
use futures::StreamExt;
use std::sync::Arc;
use tracing::error;

/// Reactive processor that listens for embedding requests and emits
/// `EmbeddingExtracted` events after batching ONNX inference work.
pub struct EmbeddingProcessor<ES: EventStore<AppEvent>> {
    service: Arc<OnnxEmbeddingService>,
    bus: EventBus<AppEvent>,
    event_store: Arc<ES>,
    /// Cache to avoid re-embedding the same ID in the same run/replay.
    processed_ids: Arc<parking_lot::Mutex<std::collections::HashSet<String>>>,
    /// IDs currently queued or in-flight so large content strings are not retained multiple times.
    pending_ids: Arc<parking_lot::Mutex<std::collections::HashSet<String>>>,
}

impl<ES: EventStore<AppEvent>> EmbeddingProcessor<ES> {
    pub async fn new(
        service: Arc<OnnxEmbeddingService>,
        bus: EventBus<AppEvent>,
        event_store: Arc<ES>,
    ) -> Result<Self, crate::store::StoreError> {
        let processed_ids = Arc::new(parking_lot::Mutex::new(std::collections::HashSet::new()));
        let pending_ids = Arc::new(parking_lot::Mutex::new(std::collections::HashSet::new()));

        // Pre-populate cache from store to avoid re-embedding on restart.
        let mut stream = event_store.read_all_from(0);
        {
            let mut lock = processed_ids.lock();
            while let Some(Ok(event)) = stream.next().await {
                if let AppEvent::Embedding(EmbeddingEvent::EmbeddingExtracted { id, .. }) =
                    &event.payload
                {
                    lock.insert(id.clone());
                }
            }
        }
        drop(stream);

        Ok(Self {
            service,
            bus,
            event_store,
            processed_ids,
            pending_ids,
        })
    }

    pub async fn spawn(self) {
        let (tx, mut worker_rx) = tokio::sync::mpsc::channel(4096);
        let bus = self.bus.clone();
        let service = Arc::clone(&self.service);
        let event_store = Arc::clone(&self.event_store);
        let processed_ids_rx = Arc::clone(&self.processed_ids);
        let processed_ids_worker = Arc::clone(&self.processed_ids);
        let pending_ids_rx = Arc::clone(&self.pending_ids);
        let pending_ids_worker = Arc::clone(&self.pending_ids);
        let mut bus_rx = self.bus.subscribe();

        drop(self);

        // Receiver task: drain the bus into an internal queue.
        tokio::spawn(async move {
            loop {
                match bus_rx.recv().await {
                    Ok(event) => match &event.payload {
                        AppEvent::Embedding(EmbeddingEvent::TextChunkCreated { id, text }) => {
                            if Self::mark_pending(&processed_ids_rx, &pending_ids_rx, id) {
                                if tx
                                    .try_send((id.clone(), text.clone(), event.sequence_num))
                                    .is_err()
                                {
                                    let _ = tx
                                        .send((id.clone(), text.clone(), event.sequence_num))
                                        .await;
                                }
                            }
                        }
                        AppEvent::Graph(GraphEvent::RequestEmbedding { id, content }) => {
                            if Self::mark_pending(&processed_ids_rx, &pending_ids_rx, id) {
                                if tx
                                    .try_send((id.clone(), content.clone(), event.sequence_num))
                                    .is_err()
                                {
                                    let _ = tx
                                        .send((id.clone(), content.clone(), event.sequence_num))
                                        .await;
                                }
                            }
                        }
                        AppEvent::Graph(GraphEvent::ResetEmbeddingCache) => {
                            tracing::info!("Resetting embedding processor cache");
                            processed_ids_rx.lock().clear();
                            pending_ids_rx.lock().clear();
                        }
                        _ => {}
                    },
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        eprintln!(
                            "EmbeddingProcessor: Lagged by {} events. Consider increasing bus capacity.",
                            n
                        );
                        tokio::task::yield_now().await;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });

        // Worker task: process one queued item at a time to avoid ONNX batch stalls.
        tokio::spawn(async move {
            loop {
                let Some(item) = worker_rx.recv().await else {
                    break;
                };

                let mut batch = vec![item];

                Self::process_batch(
                    &mut batch,
                    &service,
                    &bus,
                    &event_store,
                    &processed_ids_worker,
                    &pending_ids_worker,
                )
                .await;
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
        event_store: &Arc<ES>,
        processed_ids: &Arc<parking_lot::Mutex<std::collections::HashSet<String>>>,
        pending_ids: &Arc<parking_lot::Mutex<std::collections::HashSet<String>>>,
    ) {
        if batch.is_empty() {
            return;
        }

        let work_items = std::mem::take(batch);
        let batch_size = work_items.len();
        let mut item_meta = Vec::with_capacity(batch_size);
        let mut texts = Vec::with_capacity(batch_size);

        for (id, text, seq) in work_items {
            item_meta.push((id, seq));
            texts.push(text);
        }

        let batch_ids: Vec<&str> = item_meta.iter().map(|(id, _)| id.as_str()).collect();

        let service_clone = Arc::clone(service);
        let result = tokio::task::spawn_blocking(move || service_clone.embed_batch(&texts)).await;

        match result {
            Ok(Ok(embeddings)) => {
                if embeddings.len() != item_meta.len() {
                    error!(
                        "EmbeddingProcessor: embedding size mismatch for ids {:?}, expected {} embeddings but received {}. Releasing batch for retry.",
                        batch_ids,
                        item_meta.len(),
                        embeddings.len()
                    );
                    let mut pending = pending_ids.lock();
                    for (id, _) in item_meta {
                        pending.remove(&id);
                    }
                    return;
                }

                let mut events_to_store = Vec::with_capacity(item_meta.len());
                for ((id, seq), embedding) in item_meta.iter().cloned().zip(embeddings) {
                    let aggregate_id = id.clone();
                    let new_event = Event::new(
                        aggregate_id,
                        seq + 1,
                        AppEvent::Embedding(EmbeddingEvent::EmbeddingExtracted { id, embedding }),
                    );
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
                            match bus.publish(event) {
                                Ok(_) => {}
                                Err(e) => {
                                    error!("EmbeddingProcessor: unable to publish event {}", e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            "EmbeddingProcessor: failed to store embeddings for ids {:?}: {}",
                            batch_ids, e
                        );
                        let mut pending = pending_ids.lock();
                        for (id, _) in item_meta {
                            pending.remove(&id);
                        }
                    }
                }
            }
            Ok(Err(e)) => {
                error!(
                    "EmbeddingProcessor: embedding failed for ids {:?}: {}",
                    batch_ids, e
                );
                let mut pending = pending_ids.lock();
                for (id, _) in item_meta {
                    pending.remove(&id);
                }
            }
            Err(e) => {
                error!(
                    "EmbeddingProcessor: embedding task panicked for ids {:?}: {}",
                    batch_ids, e
                );
                let mut pending = pending_ids.lock();
                for (id, _) in item_meta {
                    pending.remove(&id);
                }
            }
        }
    }
}
