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
    /// Content retained only until a chunk is embedded.
    content_cache: Arc<parking_lot::Mutex<std::collections::HashMap<String, String>>>,
}

impl<ES: EventStore<AppEvent>> EmbeddingProcessor<ES> {
    pub async fn new(
        service: Arc<OnnxEmbeddingService>,
        bus: EventBus<AppEvent>,
        event_store: Arc<ES>,
    ) -> Result<Self, crate::store::StoreError> {
        let processed_ids = Arc::new(parking_lot::Mutex::new(std::collections::HashSet::new()));
        let pending_ids = Arc::new(parking_lot::Mutex::new(std::collections::HashSet::new()));
        let content_cache = Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new()));

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
            content_cache,
        })
    }

    pub async fn spawn(self) -> tokio::sync::mpsc::Sender<(String, String, u64)> {
        let queue_capacity = std::env::var("EMBEDDING_QUEUE_CAPACITY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(64);
        let embedding_batch_size = std::env::var("EMBEDDING_BATCH_SIZE")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(2);

        let (tx, mut worker_rx) = tokio::sync::mpsc::channel::<(String, u64)>(queue_capacity);
        let (direct_tx, mut direct_rx) =
            tokio::sync::mpsc::channel::<(String, String, u64)>(queue_capacity);
        let bus = self.bus.clone();
        let service = Arc::clone(&self.service);
        let event_store = Arc::clone(&self.event_store);
        let processed_ids_rx = Arc::clone(&self.processed_ids);
        let processed_ids_worker = Arc::clone(&self.processed_ids);
        let pending_ids_rx = Arc::clone(&self.pending_ids);
        let pending_ids_worker = Arc::clone(&self.pending_ids);
        let content_cache_rx = Arc::clone(&self.content_cache);
        let content_cache_worker = Arc::clone(&self.content_cache);
        let processed_ids_direct = Arc::clone(&self.processed_ids);
        let pending_ids_direct = Arc::clone(&self.pending_ids);
        let content_cache_direct = Arc::clone(&self.content_cache);
        let tx_direct = tx.clone();
        let mut bus_rx = self.bus.subscribe();

        drop(self);

        // Receiver task: drain the bus into an internal queue.
        tokio::spawn(async move {
            loop {
                match bus_rx.recv().await {
                    Ok(event) => match &event.payload {
                        AppEvent::Graph(GraphEvent::NodeCreated { id, metadata }) => {
                            if let Some(content) = Self::extract_embedding_content(metadata) {
                                Self::cache_content(&content_cache_rx, id, content);
                            }
                        }
                        AppEvent::Embedding(EmbeddingEvent::TextChunkCreated { id, text }) => {
                            if Self::mark_pending(&processed_ids_rx, &pending_ids_rx, id) {
                                Self::cache_content(&content_cache_rx, id, text.clone());
                                if tx
                                    .try_send((id.clone(), event.sequence_num))
                                    .is_err()
                                {
                                    let _ = tx.send((id.clone(), event.sequence_num)).await;
                                }
                            }
                        }
                        AppEvent::Graph(GraphEvent::RequestEmbedding { id, content }) => {
                            if Self::mark_pending(&processed_ids_rx, &pending_ids_rx, id) {
                                if let Some(content) = content.clone() {
                                    Self::cache_content(&content_cache_rx, id, content);
                                }
                                if tx
                                    .try_send((id.clone(), event.sequence_num))
                                    .is_err()
                                {
                                    let _ = tx.send((id.clone(), event.sequence_num)).await;
                                }
                            }
                        }
                        AppEvent::Graph(GraphEvent::ResetEmbeddingCache) => {
                            tracing::info!("Resetting embedding processor cache");
                            processed_ids_rx.lock().clear();
                            pending_ids_rx.lock().clear();
                            content_cache_rx.lock().clear();
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

        tokio::spawn(async move {
            while let Some((id, content, sequence_num)) = direct_rx.recv().await {
                if Self::mark_pending(&processed_ids_direct, &pending_ids_direct, &id) {
                    Self::cache_content(&content_cache_direct, &id, content);
                    if tx_direct.try_send((id.clone(), sequence_num)).is_err() {
                        let _ = tx_direct.send((id, sequence_num)).await;
                    }
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
                while batch.len() < embedding_batch_size {
                    match worker_rx.try_recv() {
                        Ok(item) => batch.push(item),
                        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
                        | Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
                    }
                }

                Self::process_batch(
                    &mut batch,
                    &service,
                    &bus,
                    &event_store,
                    &processed_ids_worker,
                    &pending_ids_worker,
                    &content_cache_worker,
                )
                .await;
            }
        });

        direct_tx
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

    fn cache_content(
        content_cache: &Arc<parking_lot::Mutex<std::collections::HashMap<String, String>>>,
        id: &str,
        content: String,
    ) {
        content_cache.lock().insert(id.to_string(), content);
    }

    fn extract_embedding_content(
        metadata: &std::collections::HashMap<String, String>,
    ) -> Option<String> {
        metadata
            .get("embedding_content")
            .cloned()
            .or_else(|| metadata.get("content").cloned())
    }

    async fn process_batch(
        batch: &mut Vec<(String, u64)>,
        service: &Arc<OnnxEmbeddingService>,
        bus: &EventBus<AppEvent>,
        event_store: &Arc<ES>,
        processed_ids: &Arc<parking_lot::Mutex<std::collections::HashSet<String>>>,
        pending_ids: &Arc<parking_lot::Mutex<std::collections::HashSet<String>>>,
        content_cache: &Arc<parking_lot::Mutex<std::collections::HashMap<String, String>>>,
    ) {
        if batch.is_empty() {
            return;
        }

        let work_items = std::mem::take(batch);
        let batch_size = work_items.len();
        let mut item_meta = Vec::with_capacity(batch_size);
        let mut texts = Vec::with_capacity(batch_size);

        let mut missing_ids = Vec::new();
        {
            let mut cache = content_cache.lock();
            for (id, seq) in work_items {
                match cache.remove(&id) {
                    Some(text) => {
                        item_meta.push((id, seq));
                        texts.push(text);
                    }
                    None => missing_ids.push(id),
                }
            }
        }

        if !missing_ids.is_empty() {
            error!(
                "EmbeddingProcessor: missing content for ids {:?}; dropping them from pending queue",
                missing_ids
            );
            let mut pending = pending_ids.lock();
            for id in missing_ids {
                pending.remove(&id);
            }
        }

        if item_meta.is_empty() {
            return;
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
                         let mut cache = content_cache.lock();
                         let mut pending = pending_ids.lock();
                         for (id, _) in item_meta {
                             let _ = cache.remove(&id);
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
                let mut cache = content_cache.lock();
                let mut pending = pending_ids.lock();
                for (id, _) in item_meta {
                    let _ = cache.remove(&id);
                    pending.remove(&id);
                }
            }
            Err(e) => {
                error!(
                    "EmbeddingProcessor: embedding task panicked for ids {:?}: {}",
                    batch_ids, e
                );
                let mut cache = content_cache.lock();
                let mut pending = pending_ids.lock();
                for (id, _) in item_meta {
                    let _ = cache.remove(&id);
                    pending.remove(&id);
                }
            }
        }
    }
}
