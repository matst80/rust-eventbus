use anyhow::Result;
use axum::{
    extract::{Query, State},
    response::{sse::{Event as SseEvent, Sse}, IntoResponse},
    routing::{get, post},
    Json, Router,
};
use futures::stream::{self, Stream};
use rust_eventbus::{
    app_event::{AppEvent, GraphEvent},
    crawler::{CrawlerConfig, CrawlerEvent, CrawlerService},
    embedding::{downloader::ModelDownloader, OnnxEmbeddingService, EmbeddingProcessor},
    graph::{GraphProjection, GraphState},
    projection::EphemeralProjectionActor,
    store::{FileEventStore, FileSnapshotStore},
    Event, EventBus,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::info;
use tracing_subscriber::EnvFilter;
use url::Url;

#[derive(Debug, Deserialize)]
struct CrawlRequest {
    url: String,
}

#[derive(Debug, Deserialize)]
struct EmbeddingRequest {
    text: String,
}

#[derive(Debug, Serialize)]
struct EmbeddingResponse {
    embedding: Vec<f32>,
}

struct AppState {
    bus: Arc<EventBus<AppEvent>>,
    embedding_service: Arc<OnnxEmbeddingService>,
    graph_state: Arc<parking_lot::RwLock<GraphState>>,
    // We keep these alive
    _crawler: Arc<CrawlerService>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Setup Tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into())
                .add_directive("chromiumoxide=error".parse().unwrap()),
        )
        .init();

    let temp_dir = tempfile::tempdir()?;
    let store_path = temp_dir.path().join("events_web.log");
    let snapshot_dir = temp_dir.path().join("snapshots_web");

    info!("Web server using temp directory: {:?}", temp_dir.path());

    // 2. Initialize Core Components
    let bus = Arc::new(EventBus::<AppEvent>::new(4096));
    let event_store = Arc::new(FileEventStore::new(&store_path).await?);
    let snapshot_store = Arc::new(FileSnapshotStore::new(&snapshot_dir).await?);

    // 3. Initialize Crawler Service
    let crawler_config = CrawlerConfig {
        concurrency: 2,
        headless: true,
        max_chunks: 50,
        ..Default::default()
    };
    let crawler = Arc::new(CrawlerService::new(Arc::clone(&bus), crawler_config));
    let crawler_clone = Arc::clone(&crawler);
    tokio::spawn(async move { 
        if let Err(e) = crawler_clone.run().await {
            tracing::error!("Crawler service failed: {}", e);
        }
    });

    // 4. Initialize Embedding Service
    let model_cache = "data/models";
    let downloader = ModelDownloader::new(model_cache);
    let (model_path, tokenizer_path) = downloader.get_bge_small().await?;
    let embedding_service = Arc::new(OnnxEmbeddingService::new(&model_path, &tokenizer_path)?);
    
    let embedding_processor = 
        EmbeddingProcessor::new(Arc::clone(&embedding_service), (*bus).clone(), Arc::clone(&event_store)).await?;
    
    embedding_processor.spawn().await;

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

    // 6. Global Pipeline Discovery Logic (simplified from graph_pipeline.rs)
    let bus_for_pipeline = Arc::clone(&bus);
    let mut rx = bus.subscribe();
    
    tokio::spawn(async move {
        let visited = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let max_crawls = 500;
        let requested_count = Arc::new(AtomicUsize::new(0));

        while let Ok(event) = rx.recv().await {
            if let AppEvent::Crawler(CrawlerEvent::PageIngested {
                url,
                title,
                links,
                chunks,
            }) = &event.payload
            {
                info!("Web Pipeline: Processing page {}", url);

                // Discovery
                let start_domain = Url::parse(url).ok().and_then(|u| u.domain().map(|d| d.to_string()));
                
                for target in links {
                    let mut target_url = match Url::parse(target) {
                        Ok(u) => u,
                        Err(_) => continue,
                    };
                    target_url.set_query(None);
                    target_url.set_fragment(None);
                    let normalized = target_url.to_string();

                    {
                        let mut visited_lock = visited.lock();
                        if !visited_lock.contains(&normalized) {
                            let target_domain = target_url.domain();
                            let is_same_domain = target_domain.is_some() && target_domain == start_domain.as_deref();

                            if is_same_domain && requested_count.load(Ordering::Relaxed) < max_crawls {
                                visited_lock.insert(normalized.clone());
                                requested_count.fetch_add(1, Ordering::Relaxed);

                                let _ = bus_for_pipeline.publish(Event::new(
                                    "discovery",
                                    event.sequence_num + 1,
                                    AppEvent::Crawler(CrawlerEvent::CrawlRequested {
                                        url: normalized.clone(),
                                        wait_selector: None,
                                    }),
                                ));
                                let _ = bus_for_pipeline.publish(Event::new(
                                    url,
                                    event.sequence_num + 1,
                                    AppEvent::Graph(GraphEvent::EdgeAdded {
                                        from: url.clone(),
                                        to: normalized,
                                        relation: "links_to".into(),
                                        weight: 1.0,
                                    }),
                                ));
                            }
                        }
                    }
                }

                // Chunking & Graph creation
                let mut prev_chunk_id: Option<String> = None;
                for (i, chunk) in chunks.iter().enumerate() {
                    let chunk_id = format!("{}#chunk-{}", url, i);
                    
                    let mut metadata = HashMap::new();
                    metadata.insert("type".into(), "chunk".into());
                    metadata.insert("page_title".into(), title.clone());
                    metadata.insert("content".into(), chunk.content.clone());
                    if !chunk.headers.is_empty() {
                        metadata.insert("section".into(), chunk.headers.join(" > "));
                    }

                    let _ = bus_for_pipeline.publish(Event::new(
                        &chunk_id,
                        event.sequence_num + 2,
                        AppEvent::Graph(GraphEvent::NodeCreated {
                            id: chunk_id.clone(),
                            metadata,
                        }),
                    ));

                    let _ = bus_for_pipeline.publish(Event::new(
                        &chunk_id,
                        event.sequence_num + 3,
                        AppEvent::Graph(GraphEvent::EdgeAdded {
                            from: chunk_id.clone(),
                            to: url.clone(),
                            relation: "part_of".into(),
                            weight: 1.0,
                        }),
                    ));

                    // Request embedding
                    let _ = bus_for_pipeline.publish(Event::new(
                        &chunk_id,
                        event.sequence_num + 4,
                        AppEvent::Graph(GraphEvent::RequestEmbedding {
                            id: chunk_id.clone(),
                            content: format!("Page: {}\nChunk: {}", title, chunk.content),
                        }),
                    ));

                    if let Some(prev) = prev_chunk_id {
                         let _ = bus_for_pipeline.publish(Event::new(
                            &chunk_id,
                            event.sequence_num + 5,
                            AppEvent::Graph(GraphEvent::EdgeAdded {
                                from: prev,
                                to: chunk_id.clone(),
                                relation: "next".into(),
                                weight: 1.0,
                            }),
                        ));
                    }
                    prev_chunk_id = Some(chunk_id);
                }
            }
        }
    });

    // 7. Setup Axum State and Router
    let state = Arc::new(AppState {
        bus,
        embedding_service,
        graph_state,
        _crawler: crawler,
    });

    let app = Router::new()
        .route("/crawl", get(crawl_handler))
        .route("/embeddings", post(embeddings_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    info!("Listening on http://0.0.0.0:3000");
    axum::serve(listener, app).await?;

    Ok(())
}

async fn embeddings_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<EmbeddingRequest>,
) -> impl IntoResponse {
    match state.embedding_service.embed(&payload.text) {
        Ok(embedding) => Json(EmbeddingResponse { embedding }).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn crawl_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<CrawlRequest>,
) -> Sse<impl Stream<Item = Result<SseEvent, Infallible>>> {
    let url = Arc::new(params.url);
    let start_domain = Url::parse(&url).ok().and_then(|u| u.domain().map(|d| d.to_string()));
    
    info!("Crawl request (with embeddings & auto-close) for: {} (domain: {:?})", url, start_domain);

    // Initial trigger
    let _ = state.bus.publish(Event::new(
        "web-trigger",
        0,
        AppEvent::Crawler(CrawlerEvent::CrawlRequested {
            url: (*url).clone(),
            wait_selector: None,
        }),
    ));

    let bus_rx = state.bus.subscribe();
    
    // Track session-specific progress to know when to close
    let mut requested_urls = HashSet::new();
    requested_urls.insert((*url).clone());
    let mut ingested_urls = HashSet::new();
    let mut created_nodes = HashSet::new();
    let mut embedded_nodes = HashSet::new();
    let mut last_activity = std::time::Instant::now();
    let mut settle_count = 0;

    let stream = stream::unfold((bus_rx, url, start_domain, requested_urls, ingested_urls, created_nodes, embedded_nodes, last_activity, settle_count), 
        move |(mut rx, url_arc, domain, mut req, mut ing, mut created, mut embedded, mut last, mut settle)| async move {
        loop {
            // Check for completion every loop or on timeout
            let is_done = !ing.is_empty() && ing.len() >= req.len() && !created.is_empty() && embedded.len() >= created.len();
            
            if is_done {
                settle += 1;
                if settle > 3 {
                    info!("Crawl session completed for: {}", url_arc);
                    return None;
                }
            } else if last.elapsed().as_secs() > 30 && !ing.is_empty() {
                 info!("Crawl session timed out (quiescence) for: {}", url_arc);
                 return None;
            }

            match tokio::time::timeout(std::time::Duration::from_millis(500), rx.recv()).await {
                Ok(Ok(event)) => {
                    match event.payload {
                        AppEvent::Crawler(CrawlerEvent::CrawlRequested { url, ..}) => {
                            if let Some(ref d) = domain {
                                if Url::parse(&url).ok().and_then(|u| u.domain().map(|d| d.to_string())).as_ref() == Some(d) {
                                    if req.insert(url.clone()) {
                                         last = std::time::Instant::now();
                                         settle = 0;
                                    }
                                }
                            }
                        }
                        AppEvent::Crawler(CrawlerEvent::PageIngested { url, .. }) => {
                            if req.contains(&url) {
                                if ing.insert(url) {
                                    last = std::time::Instant::now();
                                    settle = 0;
                                }
                            }
                        }
                        AppEvent::Graph(graph_event) => {
                            let (id, is_rel) = match &graph_event {
                                GraphEvent::NodeCreated { id, .. } => (id.clone(), true),
                                GraphEvent::EdgeAdded { from, to, .. } => (from.clone(), from.contains(&*url_arc) || to.contains(&*url_arc)),
                                _ => (String::new(), false),
                            };

                            if is_rel {
                                if let GraphEvent::NodeCreated { id: ref node_id, .. } = graph_event {
                                    created.insert(node_id.clone());
                                }
                                last = std::time::Instant::now();
                                settle = 0;
                                let json = serde_json::to_string(&AppEvent::Graph(graph_event)).unwrap_or_default();
                                return Some((Ok(SseEvent::default().data(json)), (rx, url_arc, domain, req, ing, created, embedded, last, settle)));
                            }
                        }
                        AppEvent::Embedding(rust_eventbus::embedding::event::EmbeddingEvent::EmbeddingExtracted { id, embedding }) => {
                            if created.contains(&id) {
                                if embedded.insert(id.clone()) {
                                    last = std::time::Instant::now();
                                    settle = 0;
                                    let json = serde_json::to_string(&AppEvent::Embedding(rust_eventbus::embedding::event::EmbeddingEvent::EmbeddingExtracted { id, embedding })).unwrap_or_default();
                                    return Some((Ok(SseEvent::default().data(json)), (rx, url_arc, domain, req, ing, created, embedded, last, settle)));
                                }
                            }
                        }
                        _ => {}
                    }
                }
                Ok(Err(_)) => return None,
                Err(_) => {
                    // Timeout - just continue to check completion logic
                    continue;
                }
            }
        }
    });

    Sse::new(stream)
}

