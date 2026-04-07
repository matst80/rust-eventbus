use anyhow::Result;
use futures::StreamExt;
use rust_eventbus::{
    Event, EventBus,
    app_event::{AppEvent, GraphEvent},
    crawler::{CrawlerConfig, CrawlerEvent, CrawlerService},
    embedding::{OnnxEmbeddingService, projection::{EmbeddingServiceActor, EmbeddingEvent}},
    projection::EphemeralProjectionActor,
    store::{FileEventStore, FileSnapshotStore},
    graph::{GraphProjection, GraphState},
    distributed::{DistributedPubSub, DistributedError},
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::{HashMap, HashSet};
use tracing::info;
use tracing_subscriber::EnvFilter;
use async_trait::async_trait;

// Simple MockPubSub for the demo
pub struct MockPubSub;
#[async_trait]
impl DistributedPubSub<AppEvent> for MockPubSub {
    async fn publish(&self, _event: &Event<AppEvent>) -> Result<(), DistributedError> { Ok(()) }
    async fn subscribe(&self) -> futures::stream::BoxStream<'static, Result<Event<AppEvent>, DistributedError>> {
        futures::stream::empty().boxed()
    }
    async fn request_sync(&self, _from_seq: u64) -> Result<(), DistributedError> { Ok(()) }
    async fn check_quorum(&self) -> Result<(), DistributedError> { Ok(()) }
}

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Setup Tracing
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .init();

    let temp_dir = tempfile::tempdir()?;
    let store_path = temp_dir.path().join("events.log");
    let snapshot_dir = temp_dir.path().join("snapshots");

    info!("Using temp directory: {:?}", temp_dir.path());

    // 2. Initialize Core Components
    let bus = Arc::new(EventBus::<AppEvent>::new(2048));
    let event_store = Arc::new(FileEventStore::new(&store_path).await?);
    let snapshot_store = Arc::new(FileSnapshotStore::new(&snapshot_dir).await?);

    // 3. Initialize Crawler Service
    let crawler_config = CrawlerConfig { concurrency: 1, headless: true, ..Default::default() };
    let crawler = CrawlerService::new(Arc::clone(&bus), crawler_config);
    tokio::spawn(async move { crawler.run().await });

    // 4. Initialize Embedding Service (Using Onnx)
    let model_path = "data/models/bge-small-en-v1.5.onnx";
    let tokenizer_path = "data/models/tokenizer.json";
    
    if std::path::Path::new(model_path).exists() {
        let onnx_service = Arc::new(OnnxEmbeddingService::new(model_path, tokenizer_path)?);
        let embedding_actor = EmbeddingServiceActor::new(
            onnx_service,
            (*bus).clone(),
            Arc::new(MockPubSub), // Using local mock
            Arc::clone(&event_store),
        ).await?;
        embedding_actor.spawn().await;
    } else {
        info!("Skipping Real EmbeddingService. Will mock EmbeddingExtracted for demo.");
        let mut rx_mock = bus.subscribe();
        let bus_mock = Arc::clone(&bus);
        tokio::spawn(async move {
            use std::collections::HashMap;
            while let Ok(event) = rx_mock.recv().await {
                if let AppEvent::Graph(GraphEvent::RequestEmbedding { id, .. }) = &event.payload {
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    let _ = bus_mock.publish(Event::new(
                        id,
                        event.sequence_num + 1,
                        AppEvent::Embedding(EmbeddingEvent::EmbeddingExtracted { 
                            id: id.clone(), 
                            embedding: vec![0.1; 384] 
                        })
                    ));
                    
                    let related_id = "https://www.rust-lang.org/learn";
                    let _ = bus_mock.publish(Event::new(related_id, 1, 
                        AppEvent::Graph(GraphEvent::NodeCreated { id: related_id.into(), metadata: HashMap::new() })
                    ));
                    let _ = bus_mock.publish(Event::new(related_id, 2,
                        AppEvent::Embedding(EmbeddingEvent::EmbeddingExtracted { id: related_id.into(), embedding: vec![0.11; 384] })
                    ));
                }
            }
        });
    }

    // 5. Initialize Graph Projection
    let graph_projection = Arc::new(GraphProjection);
    let graph_actor = EphemeralProjectionActor::<AppEvent, GraphState, _, _, _>::new(
        (*bus).clone(),
        Arc::clone(&event_store),
        graph_projection,
        Arc::clone(&snapshot_store),
    );
    let graph_state = graph_actor.get_state();
    graph_actor.spawn().await;

    // 6. Subscription for Extraction Logic (Multi-page Discovery & Chunking)
    let start_url = "https://www.rust-lang.org";
    let start_domain = url::Url::parse(start_url).ok().and_then(|u| u.domain().map(|d| d.to_string()));
    
    let mut rx = bus.subscribe();
    let bus_for_extraction = Arc::clone(&bus);
    let visited = Arc::new(parking_lot::Mutex::new(HashSet::new()));
    let crawl_count = Arc::new(AtomicUsize::new(0));
    let max_crawls = 50;

    fn normalize_url(u: &str) -> Option<String> {
        let mut parsed = url::Url::parse(u).ok()?;
        parsed.set_query(None);
        parsed.set_fragment(None);
        // Ensure consistent trailing slash for directories if needed, 
        // but url::Url::to_string() is usually good enough for normalization.
        Some(parsed.to_string())
    }

    if let Some(norm_start) = normalize_url(start_url) {
        visited.lock().insert(norm_start);
    }
    
    tokio::spawn(async move {
        while let Ok(event) = rx.recv().await {
            if let AppEvent::Crawler(CrawlerEvent::PageIngested { url, title, links, chunks }) = &event.payload {
                info!("Pipeline: Page ingested {} ({} links, {} chunks)", url, links.len(), chunks.len());

                // --- Discovery Logic (Same domain only) ---
                // links and chunks were already extracted inside the crawler before publishing,
                // so no HTML parsing happens here — just routing logic.
                for target in links {
                    let Some(normalized) = normalize_url(target) else { continue };

                    let mut visited_lock = visited.lock();
                    if !visited_lock.contains(&normalized) {
                        if let Some(target_url) = url::Url::parse(&normalized).ok() {
                            let target_domain = target_url.domain();
                            let is_same_domain = target_domain.is_some() && target_domain == start_domain.as_deref();

                            if is_same_domain && crawl_count.load(Ordering::Relaxed) < max_crawls {
                                visited_lock.insert(normalized.clone());
                                crawl_count.fetch_add(1, Ordering::Relaxed);

                                info!("Pipeline: Queuing same-domain link: {}", normalized);
                                let _ = bus_for_extraction.publish(Event::new(
                                    "discovery",
                                    event.sequence_num + 1,
                                    AppEvent::Crawler(CrawlerEvent::CrawlRequested {
                                        url: normalized.clone(),
                                        wait_selector: None,
                                    })
                                ));
                                let _ = bus_for_extraction.publish(Event::new(
                                    url,
                                    event.sequence_num + 1,
                                    AppEvent::Graph(GraphEvent::EdgeAdded {
                                        from: url.clone(),
                                        to: normalized,
                                        relation: "links_to".into(),
                                        weight: 1.0,
                                    })
                                ));
                            }
                        }
                    }
                }

                // --- Chunk → Node + Embedding events ---
                let mut prev_chunk_id: Option<String> = None;
                for (i, chunk) in chunks.iter().enumerate() {
                    let chunk_id = if i == 0 { url.clone() } else { format!("{}#chunk-{}", url, i) };

                    let mut metadata = HashMap::new();
                    metadata.insert("type".into(), "chunk".into());
                    metadata.insert("page_title".into(), title.clone());
                    metadata.insert("content".into(), chunk.content.clone());
                    if !chunk.headers.is_empty() {
                        metadata.insert("section".into(), chunk.headers.join(" > "));
                    }
                    let _ = bus_for_extraction.publish(Event::new(
                        &chunk_id,
                        event.sequence_num + 10 + (i * 20) as u64,
                        AppEvent::Graph(GraphEvent::NodeCreated { id: chunk_id.clone(), metadata })
                    ));

                    if i == 0 {
                        let mut page_meta = HashMap::new();
                        page_meta.insert("type".into(), "page".into());
                        page_meta.insert("title".into(), title.clone());
                        let _ = bus_for_extraction.publish(Event::new(
                            url,
                            event.sequence_num + 11,
                            AppEvent::Graph(GraphEvent::NodeCreated { id: url.clone(), metadata: page_meta })
                        ));
                    }

                    let mut contextual_content = format!("Source: {}\nPage: {}\n", url, title);
                    if !chunk.headers.is_empty() {
                        contextual_content.push_str(&format!("Section: {}\n\n", chunk.headers.join(" > ")));
                    }
                    contextual_content.push_str(&chunk.content);

                    let _ = bus_for_extraction.publish(Event::new(
                        &chunk_id,
                        event.sequence_num + 12 + (i * 20) as u64,
                        AppEvent::Graph(GraphEvent::RequestEmbedding { id: chunk_id.clone(), content: contextual_content })
                    ));

                    if i > 0 {
                        let _ = bus_for_extraction.publish(Event::new(
                            &chunk_id,
                            event.sequence_num + 15 + (i * 20) as u64,
                            AppEvent::Graph(GraphEvent::EdgeAdded { from: chunk_id.clone(), to: url.clone(), relation: "part_of".into(), weight: 1.0 })
                        ));
                    }

                    if let Some(ref prev_id) = prev_chunk_id {
                        let _ = bus_for_extraction.publish(Event::new(
                            prev_id,
                            event.sequence_num + 16 + (i * 20) as u64,
                            AppEvent::Graph(GraphEvent::EdgeAdded { from: prev_id.clone(), to: chunk_id.clone(), relation: "next".into(), weight: 1.0 })
                        ));
                        let _ = bus_for_extraction.publish(Event::new(
                            &chunk_id,
                            event.sequence_num + 17 + (i * 20) as u64,
                            AppEvent::Graph(GraphEvent::EdgeAdded { from: chunk_id.clone(), to: prev_id.clone(), relation: "prev".into(), weight: 1.0 })
                        ));
                    }
                    prev_chunk_id = Some(chunk_id.clone());
                }
            }
        }
    });

    // 7. Trigger the Pipeline
    info!("Waiting for components to be ready...");
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Optional: Progress reporter
    let graph_state_report = Arc::clone(&graph_state);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(3));
        loop {
            interval.tick().await;
            let state = graph_state_report.read();
            let nodes_with_embeddings = state.nodes.values().filter(|n| n.embedding.is_some()).count();
            if !state.nodes.is_empty() {
                info!("Progress: {} nodes, {} with embeddings, {} edges", 
                    state.nodes.len(), nodes_with_embeddings, state.edges.len());
            }
        }
    });

    info!("Triggering pipeline for: {}", start_url);
    bus.publish(Event::new(
        "pipeline-trigger",
        1,
        AppEvent::Crawler(CrawlerEvent::CrawlRequested { 
            url: start_url.to_string(), 
            wait_selector: None 
        })
    ))?;

    // Wait for the user to decide when to finish (allowing embeddings to complete)
    info!("Pipeline running. The graph is being populated asynchronously.");
    info!("Press [ENTER] to stop crawling and save the final graph state...");
    
    let mut input = String::new();
    let _ = std::io::stdin().read_line(&mut input);

    // 8. Inspect the Result
    {
        let state = graph_state.read();
        let nodes_with_embeddings = state.nodes.values().filter(|n| n.embedding.is_some()).count();
        
        info!("--- GRAPH STATUS ---");
        info!("Nodes collected:      {}", state.nodes.len());
        info!("Nodes with embeddings: {}", nodes_with_embeddings);
        info!("Edges created:        {}", state.edges.len());
        
        for (id, node) in state.nodes.iter().take(3) {
            info!("Node: {} (Title: {:?}, Embedded: {})", 
                id, 
                node.metadata.get("title"),
                node.embedding.is_some()
            );
        }

        for edge in state.edges.values().take(5) {
            info!("Edge: {} --[{}]--> {} (w={:.2})", edge.from, edge.relation, edge.to, edge.weight);
        }

        // --- Save Result for Search Demo ---
        let output_path = "examples/outputs/graph_state.json";
        std::fs::create_dir_all("examples/outputs")?;
        let json = serde_json::to_string_pretty(&*state)?;
        std::fs::write(output_path, json)?;
        info!("Graph state persisted to: {}", output_path);
    }

    Ok(())
}
