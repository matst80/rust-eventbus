use std::sync::Arc;
use tokio::time::Duration;
use futures::StreamExt;
use rust_eventbus::{
    bus::EventBus,
    event::Event,
    embedding::{
        downloader::ModelDownloader,
        onnx::OnnxEmbeddingService,
        projection::{EmbeddingEvent, EmbeddingProjection, EmbeddingServiceActor, EmbeddingState},
    },
    store::{FileEventStore, EventStore},
    distributed::{DistributedPubSub, DistributedError},
    projection::Projection,
};
use async_trait::async_trait;

// Simple MockPubSub for testing
pub struct MockPubSub<E> {
    _marker: std::marker::PhantomData<E>,
}

impl<E> MockPubSub<E> {
    pub fn new() -> Self {
        Self { _marker: std::marker::PhantomData }
    }
}

#[async_trait]
impl<E: rust_eventbus::event::EventPayload + serde::Serialize + for<'de> serde::Deserialize<'de>> DistributedPubSub<E> for MockPubSub<E> {
    async fn publish(&self, _event: &Event<E>) -> Result<(), DistributedError> {
        Ok(())
    }
    async fn subscribe(&self) -> futures::stream::BoxStream<'static, Result<Event<E>, DistributedError>> {
        futures::stream::empty().boxed()
    }
    async fn request_sync(&self, _from_seq: u64) -> Result<(), DistributedError> {
        Ok(())
    }
    async fn check_quorum(&self) -> Result<(), DistributedError> {
        Ok(())
    }
}

#[tokio::test]
async fn test_onnx_embedding_flow() -> anyhow::Result<()> {
    // 1. Setup temporary directory for model and store
    let temp_dir = tempfile::tempdir()?;
    let model_cache = temp_dir.path().join("models");
    let store_path = temp_dir.path().join("events.bin");
    
    let downloader = ModelDownloader::new(&model_cache);
    
    // 2. Download model
    let (model_path, tokenizer_path) = downloader.get_bge_small().await?;
    
    // 3. Initialize Service
    let service = Arc::new(OnnxEmbeddingService::new(model_path, tokenizer_path)?);
    
    // 4. Setup Event Bus and Mesh
    let bus = EventBus::<EmbeddingEvent>::new(100);
    let mesh = Arc::new(MockPubSub::<EmbeddingEvent>::new());
    let event_store = Arc::new(FileEventStore::new(&store_path).await?);

    // 5. Setup Actor
    let actor = EmbeddingServiceActor::new(
        service.clone(),
        bus.clone(),
        mesh.clone(),
        event_store.clone(),
    );
    actor.spawn().await;
    
    // 6. Setup Projection
    let projection = Arc::new(EmbeddingProjection);
    let state = Arc::new(parking_lot::RwLock::new(EmbeddingState::default()));
    
    // Manual event handling for the test
    let mut rx = bus.subscribe();
    let state_clone = state.clone();
    tokio::spawn(async move {
        while let Ok(event) = rx.recv().await {
            let mut s = state_clone.write();
            let _ = projection.handle(&mut s, &event);
        }
    });

    // 7. Publish a TextChunkCreated event
    let id = "test-doc-1".to_string();
    let text = "This is a test chunk of text for embedding extraction.".to_string();
    let event = Event::new(&id, 1, EmbeddingEvent::TextChunkCreated { id: id.clone(), text });
    
    let stored = event_store.append(vec![event]).await?;
    for e in stored {
        let _ = bus.publish(e);
    }

    // 8. Wait for EmbeddingExtracted event
    let mut success = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let s = state.read();
        if s.embeddings.contains_key(&id) {
            success = true;
            let embedding = s.embeddings.get(&id).unwrap();
            assert_eq!(embedding.len(), 384); // BGE small dimension
            println!("Embedding extracted successfully! Dim: {}", embedding.len());
            break;
        }
    }
    
    assert!(success, "Timed out waiting for embedding extraction");

    Ok(())
}
