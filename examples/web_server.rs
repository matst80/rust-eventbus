use anyhow::Result;
use axum::{
    extract::{Query, State},
    response::{
        sse::{Event as SseEvent, Sse},
        IntoResponse, Response,
    },
    routing::{get, post},
    Json, Router,
};
use futures::stream::{self};
use http::header::HeaderValue;
use rust_eventbus::{
    app_event::{AppEvent, GraphEvent},
    crawler::{CrawlerConfig, CrawlerEvent, CrawlerService},
    embedding::{downloader::ModelDownloader, EmbeddingProcessor, OnnxEmbeddingService},
    store::FileEventStore,
    Event, EventBus,
};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use url::Url;
use uuid::Uuid;

mod persistence;
use persistence::PersistenceService;

#[derive(Debug, Deserialize)]
struct CrawlRequest {
    url: String,
    project_id: String,
    #[serde(default = "default_max_crawls")]
    max_crawls: usize,
    #[serde(default = "default_max_chunks")]
    max_chunks: usize,
}

fn default_max_crawls() -> usize {
    500
}
fn default_max_chunks() -> usize {
    50
}

#[derive(Debug, Deserialize)]
struct EmbeddingRequest {
    text: String,
}

#[derive(Debug, Serialize)]
struct EmbeddingResponse {
    embedding: Vec<f32>,
}

#[derive(Debug, Deserialize)]
struct SearchRequest {
    query: String,
    project_id: String,
}

#[derive(Debug, Clone)]
struct AppConfig {
    max_crawls: usize,
    max_chunks: usize,
    session_timeout_secs: u64,
    crawl_timeout_secs: u64,
    db_max_connections: usize,
    bus_capacity: usize,
    crawler_concurrency: usize,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            max_crawls: std::env::var("MAX_CRAWLS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(500),
            max_chunks: std::env::var("MAX_CHUNKS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(50),
            session_timeout_secs: std::env::var("SESSION_TIMEOUT_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(30),
            crawl_timeout_secs: std::env::var("CRAWL_TIMEOUT_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(120),
            db_max_connections: std::env::var("DB_MAX_CONNECTIONS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10),
            bus_capacity: std::env::var("BUS_CAPACITY")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(4096),
            crawler_concurrency: std::env::var("CRAWLER_CONCURRENCY")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(2),
        }
    }
}

struct SessionState {
    session_id: String,
    project_id: String,
    start_domain: Option<String>,
    max_crawls: usize,
    requested: HashSet<String>,
    visited: HashSet<String>,
    counts: Arc<AtomicUsize>,
    last_crawl: Instant,
}

struct AppState {
    bus: Arc<EventBus<AppEvent>>,
    embedding_service: Arc<OnnxEmbeddingService>,
    persistence: Arc<PersistenceService>,
    _crawler: Arc<CrawlerService>,
    url_projects: Arc<parking_lot::RwLock<HashMap<String, (String, String, usize, Instant)>>>,
    sessions: Arc<parking_lot::RwLock<HashMap<String, SessionState>>>,
    shutdown_flag: Arc<AtomicBool>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = AppConfig::default();
    info!("Starting with config: {:?}", config);

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into())
                .add_directive("chromiumoxide=error".parse().unwrap()),
        )
        .init();

    // Graceful shutdown flag
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let shutdown_flag_clone = Arc::clone(&shutdown_flag);

    // 1. Database
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://localhost/eventbus".to_string());
    let db = PgPoolOptions::new()
        .max_connections(config.db_max_connections as u32)
        .connect(&database_url)
        .await?;
    info!("Connected to PostgreSQL");

    // 2. Event store
    let temp_dir = tempfile::tempdir()?;
    let store_path = temp_dir.path().join("events_web.log");
    let event_store = Arc::new(FileEventStore::new(&store_path).await?);

    // 3. Event bus
    let bus = Arc::new(EventBus::<AppEvent>::new(config.bus_capacity));

    // 4. Crawler
    let crawler_config = CrawlerConfig {
        concurrency: config.crawler_concurrency,
        headless: true,
        max_chunks: config.max_chunks,
        shutdown_flag: Some(Arc::clone(&shutdown_flag_clone)),
        ..Default::default()
    };
    let crawler = Arc::new(CrawlerService::new(Arc::clone(&bus), crawler_config));
    let crawler_clone = Arc::clone(&crawler);
    let shutdown_flag_worker = Arc::clone(&shutdown_flag_clone);
    tokio::spawn(async move {
        tokio::select! {
            result = crawler_clone.run() => {
                if let Err(e) = result {
                    tracing::error!("Crawler service failed: {}", e);
                }
            }
            _ = tokio::signal::ctrl_c() => {
                info!("Crawler shutdown signal received");
            }
        }
        shutdown_flag_worker.store(true, Ordering::SeqCst);
    });

    // 5. Embedding service
    let model_cache = "data/models";
    let downloader = ModelDownloader::new(model_cache);
    let (model_path, tokenizer_path) = downloader.get_bge_small().await?;
    let embedding_service = Arc::new(OnnxEmbeddingService::new(&model_path, &tokenizer_path)?);

    let embedding_service_for_persistence = Arc::clone(&embedding_service);

    let embedding_processor = EmbeddingProcessor::new(
        Arc::clone(&embedding_service),
        (*bus).clone(),
        Arc::clone(&event_store),
    )
    .await?;
    embedding_processor.spawn().await;

    // 2. Persistence service
    let persistence = Arc::new(PersistenceService::new(
        Arc::new(db.clone()),
        Arc::new(
            move |text: &str| match embedding_service_for_persistence.embed(text) {
                Ok(emb) => {
                    tracing::debug!("Generated embedding of length {}", emb.len());
                    emb
                }
                Err(e) => {
                    tracing::error!("Failed to embed query: {}", e);
                    vec![]
                }
            },
        ),
    ));
    persistence.init_schema().await?;

    // 6. Shared URL → (project_id, session_id, max_crawls, timestamp) mapping with TTL-based cleanup
    let url_projects: Arc<parking_lot::RwLock<HashMap<String, (String, String, usize, Instant)>>> =
        Arc::new(parking_lot::RwLock::new(HashMap::new()));
    let url_projects_cleanup = Arc::clone(&url_projects);

    // Sessions map for per-session state isolation
    let sessions: Arc<parking_lot::RwLock<HashMap<String, SessionState>>> =
        Arc::new(parking_lot::RwLock::new(HashMap::new()));
    let sessions_cleanup = Arc::clone(&sessions);

    let session_ttl = std::env::var("SESSION_TTL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3600);
    let cleanup_interval = Duration::from_secs(60);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(cleanup_interval);
        loop {
            interval.tick().await;
            let now = Instant::now();
            let mut url_to_remove = Vec::new();
            {
                let map = url_projects_cleanup.read();
                for (url, (_, _, _, timestamp)) in map.iter() {
                    if now.duration_since(*timestamp) > Duration::from_secs(300) {
                        url_to_remove.push(url.clone());
                    }
                }
            }
            if !url_to_remove.is_empty() {
                let count = url_to_remove.len();
                let mut map = url_projects_cleanup.write();
                for url in url_to_remove {
                    map.remove(&url);
                }
                info!("Cleaned up {} stale URL mappings", count);
            }
            let mut session_to_remove = Vec::new();
            {
                let map = sessions_cleanup.read();
                for (session_id, state) in map.iter() {
                    if now.duration_since(state.last_crawl) > Duration::from_secs(session_ttl) {
                        session_to_remove.push(session_id.clone());
                    }
                }
            }
            if !session_to_remove.is_empty() {
                let count = session_to_remove.len();
                let mut map = sessions_cleanup.write();
                for sid in session_to_remove {
                    map.remove(&sid);
                }
                info!("Cleaned up {} stale sessions", count);
            }
        }
    });

    // 7. Pipeline: events → Postgres storage + graph events for SSE
    let pipeline_bus = Arc::clone(&bus);
    let pipeline_persistence = Arc::clone(&persistence);
    let pipeline_projects = Arc::clone(&url_projects);
    let mut pipeline_rx = bus.subscribe();
    let config_clone = config.clone();

    // Semaphore for bounded concurrent batch operations
    let batch_semaphore = Arc::new(tokio::sync::Semaphore::new(20));
    let batch_semaphore_clone = Arc::clone(&batch_semaphore);

    tokio::spawn(async move {
        let requested_counts: Arc<parking_lot::RwLock<HashMap<String, Arc<AtomicUsize>>>> =
            Arc::new(parking_lot::RwLock::new(HashMap::new()));
        let visited: Arc<parking_lot::RwLock<HashMap<String, HashSet<String>>>> =
            Arc::new(parking_lot::RwLock::new(HashMap::new()));
        let last_crawl: Arc<parking_lot::RwLock<HashMap<String, std::time::Instant>>> =
            Arc::new(parking_lot::RwLock::new(HashMap::new()));
        let max_crawls_map: Arc<parking_lot::RwLock<HashMap<String, usize>>> =
            Arc::new(parking_lot::RwLock::new(HashMap::new()));

        while let Ok(event) = pipeline_rx.recv().await {
            match &event.payload {
                AppEvent::Crawler(CrawlerEvent::CrawlRequested { url, .. }) => {
                    let (project_id, session_id, max_crawls, _) =
                        match pipeline_projects.read().get(url).cloned() {
                            Some((pid, sid, max, ts)) => (pid, sid, max, ts),
                            None => continue,
                        };

                    let _should_reset = {
                        let mut last_write = last_crawl.write();
                        let mut counts_write = requested_counts.write();
                        let mut visited_write = visited.write();
                        let mut max_write = max_crawls_map.write();

                        let count = counts_write
                            .get(&session_id)
                            .map(|c| c.load(Ordering::Relaxed))
                            .unwrap_or(0);
                        let visited_count =
                            visited_write.get(&session_id).map(|v| v.len()).unwrap_or(0);

                        let max_crawls_for_session = max_crawls;

                        // Store max_crawls for this session
                        max_write.insert(session_id.clone(), max_crawls);

                        let last_time = last_write.get(&session_id).cloned();
                        let should_reset = match last_time {
                            Some(instant) => {
                                instant.elapsed().as_secs() > config_clone.session_timeout_secs
                                    || count >= max_crawls_for_session
                                    || visited_count == 0
                            }
                            None => true,
                        };

                        if should_reset {
                            info!(
                                "Starting new crawl session for project: {} session: {} (max: {})",
                                project_id, session_id, max_crawls_for_session
                            );
                            counts_write.remove(&session_id);
                            visited_write.remove(&session_id);
                        }
                        last_write.insert(session_id.clone(), std::time::Instant::now());

                        should_reset
                    };
                }
                AppEvent::Crawler(CrawlerEvent::PageIngested {
                    url,
                    title,
                    links,
                    chunks,
                }) => {
                    let (project_id, session_id, max_crawls, _) =
                        match pipeline_projects.read().get(url).cloned() {
                            Some((pid, sid, max, ts)) => (pid, sid, max, ts),
                            None => continue,
                        };

                    info!(
                        "Pipeline: storing page {} (project {}, session {}, max: {})",
                        url, project_id, session_id, max_crawls
                    );

                    // Upsert page
                    if let Err(e) = pipeline_persistence
                        .upsert_page(url, &project_id, title)
                        .await
                    {
                        tracing::error!("Failed to upsert page {}: {}", url, e);
                    }

                    // Link discovery - collect links for batch insert
                    let start_domain = Url::parse(url)
                        .ok()
                        .and_then(|u| u.domain().map(|d| d.to_string()));

                    let mut links_to_insert = Vec::new();
                    let mut urls_to_crawl = Vec::new();

                    // Get the max_crawls limit for this session
                    let max_crawls_limit = max_crawls_map
                        .read()
                        .get(&session_id)
                        .copied()
                        .unwrap_or(max_crawls);

                    for target in links {
                        let mut target_url = match Url::parse(target) {
                            Ok(u) => u,
                            Err(_) => continue,
                        };
                        target_url.set_query(None);
                        target_url.set_fragment(None);
                        let normalized = target_url.to_string();

                        // Check per-session visited set
                        let is_visited = {
                            let visited = visited.read();
                            visited
                                .get(&session_id)
                                .map(|v| v.contains(&normalized))
                                .unwrap_or(false)
                        };
                        if is_visited {
                            continue;
                        }

                        let target_domain = target_url.domain();
                        let is_same_domain =
                            target_domain.is_some() && target_domain == start_domain.as_deref();

                        // Get or create per-session counter
                        let count = {
                            let counts = requested_counts.read();
                            counts.get(&session_id).cloned()
                        };
                        let count = match count {
                            Some(c) => c,
                            None => {
                                let c = Arc::new(AtomicUsize::new(0));
                                requested_counts
                                    .write()
                                    .insert(session_id.clone(), Arc::clone(&c));
                                c
                            }
                        };
                        target_url.set_query(None);
                        target_url.set_fragment(None);
                        let normalized = target_url.to_string();

                        // Check per-session visited set
                        let is_visited = {
                            let visited = visited.read();
                            visited
                                .get(&session_id)
                                .map(|v| v.contains(&normalized))
                                .unwrap_or(false)
                        };
                        if is_visited {
                            continue;
                        }

                        let target_domain = target_url.domain();
                        let is_same_domain =
                            target_domain.is_some() && target_domain == start_domain.as_deref();

                        // Get or create per-session counter
                        let count = {
                            let counts = requested_counts.read();
                            counts.get(&session_id).cloned()
                        };
                        let count = match count {
                            Some(c) => c,
                            None => {
                                let c = Arc::new(AtomicUsize::new(0));
                                requested_counts
                                    .write()
                                    .insert(session_id.clone(), Arc::clone(&c));
                                c
                            }
                        };

                        if !is_same_domain || count.load(Ordering::Relaxed) >= max_crawls_limit {
                            continue;
                        }

                        // Mark as visited for this session
                        {
                            let mut visited = visited.write();
                            visited
                                .entry(session_id.clone())
                                .or_insert_with(HashSet::new)
                                .insert(normalized.clone());
                        }
                        count.fetch_add(1, Ordering::Relaxed);

                        // Track project for discovered URL with timestamp and max_crawls
                        pipeline_projects.write().insert(
                            normalized.clone(),
                            (
                                project_id.clone(),
                                session_id.clone(),
                                max_crawls,
                                Instant::now(),
                            ),
                        );

                        // Queue link for batch insert
                        links_to_insert.push((url.clone(), normalized.clone(), project_id.clone()));

                        // Queue URL for crawling
                        urls_to_crawl.push(normalized.clone());

                        // Publish edge for SSE
                        if let Err(e) = pipeline_bus.publish(Event::new(
                            url,
                            event.sequence_num + 1,
                            AppEvent::Graph(GraphEvent::EdgeAdded {
                                from: url.clone(),
                                to: normalized.clone(),
                                relation: "links_to".into(),
                                weight: 1.0,
                            }),
                        )) {
                            warn!("Failed to publish edge event: {}", e);
                        }
                    }

                    // Batch insert links
                    if !links_to_insert.is_empty() {
                        let persist = Arc::clone(&pipeline_persistence);
                        let sem = Arc::clone(&batch_semaphore_clone);
                        tokio::spawn(async move {
                            let _permit = sem.acquire().await;
                            if let Err(e) = persist.insert_links_batch(links_to_insert).await {
                                tracing::error!("Failed to insert links batch: {}", e);
                            }
                        });
                    }

                    // Batch insert chunks
                    let chunks_to_insert: Vec<(
                        String,
                        String,
                        String,
                        i32,
                        Option<String>,
                        String,
                    )> = chunks
                        .iter()
                        .enumerate()
                        .map(|(i, chunk)| {
                            let chunk_id = format!("{}#chunk-{}", url, i);
                            let section = if chunk.headers.is_empty() {
                                None
                            } else {
                                Some(chunk.headers.join(" > "))
                            };
                            (
                                chunk_id,
                                project_id.clone(),
                                url.clone(),
                                i as i32,
                                section,
                                chunk.content.clone(),
                            )
                        })
                        .collect();

                    if !chunks_to_insert.is_empty() {
                        let persist = Arc::clone(&pipeline_persistence);
                        let sem = Arc::clone(&batch_semaphore_clone);
                        tokio::spawn(async move {
                            let _permit = sem.acquire().await;
                            if let Err(e) = persist.insert_chunks_batch(chunks_to_insert).await {
                                tracing::error!("Failed to insert chunks batch: {}", e);
                            }
                        });

                        // Publish NodeCreated events for SSE
                        for (i, chunk) in chunks.iter().enumerate() {
                            let chunk_id = format!("{}#chunk-{}", url, i);
                            let section = if chunk.headers.is_empty() {
                                None
                            } else {
                                Some(chunk.headers.join(" > "))
                            };

                            let mut metadata = HashMap::new();
                            metadata.insert("type".into(), "chunk".into());
                            metadata.insert("page_title".into(), title.clone());
                            metadata.insert("content".into(), chunk.content.clone());
                            if let Some(ref s) = section {
                                metadata.insert("section".into(), s.clone());
                            }

                            if let Err(e) = pipeline_bus.publish(Event::new(
                                &chunk_id,
                                event.sequence_num + 2,
                                AppEvent::Graph(GraphEvent::NodeCreated {
                                    id: chunk_id.clone(),
                                    metadata,
                                }),
                            )) {
                                warn!("Failed to publish NodeCreated event: {}", e);
                            }

                            // Publish part_of edge for SSE
                            if let Err(e) = pipeline_bus.publish(Event::new(
                                &chunk_id,
                                event.sequence_num + 3,
                                AppEvent::Graph(GraphEvent::EdgeAdded {
                                    from: chunk_id.clone(),
                                    to: url.clone(),
                                    relation: "part_of".into(),
                                    weight: 1.0,
                                }),
                            )) {
                                warn!("Failed to publish part_of edge: {}", e);
                            }

                            // Request embedding (consumed by EmbeddingProcessor)
                            if let Err(e) = pipeline_bus.publish(Event::new(
                                &chunk_id,
                                event.sequence_num + 4,
                                AppEvent::Graph(GraphEvent::RequestEmbedding {
                                    id: chunk_id.clone(),
                                    content: format!("Page: {}\nChunk: {}", title, chunk.content),
                                }),
                            )) {
                                warn!("Failed to publish RequestEmbedding: {}", e);
                            }
                        }
                    }

                    // Trigger new crawls
                    for normalized in urls_to_crawl {
                        if let Err(e) = pipeline_bus.publish(Event::new(
                            "discovery",
                            event.sequence_num + 1,
                            AppEvent::Crawler(CrawlerEvent::CrawlRequested {
                                url: normalized,
                                wait_selector: None,
                            }),
                        )) {
                            warn!("Failed to publish CrawlRequested: {}", e);
                        }
                    }
                }

                // Store computed embeddings in Postgres
                AppEvent::Embedding(
                    rust_eventbus::embedding::event::EmbeddingEvent::EmbeddingExtracted {
                        id,
                        embedding,
                    },
                ) => {
                    tracing::debug!("Received embedding extracted for id: {}", id);
                    let page_url = match id.rsplit_once("#chunk-") {
                        Some((url, _)) => url,
                        None => {
                            tracing::warn!("Could not parse page_url from id: {}", id);
                            continue;
                        }
                    };
                    let (project_id, _, _, _) =
                        match pipeline_projects.read().get(page_url).cloned() {
                            Some(data) => data,
                            None => {
                                tracing::warn!("No project_id found for page_url: {}", page_url);
                                continue;
                            }
                        };
                    tracing::debug!("Updating embedding for id: {} project: {}", id, project_id);

                    // Batch update embedding
                    let persist = Arc::clone(&pipeline_persistence);
                    let sem = Arc::clone(&batch_semaphore_clone);
                    let id_clone = id.clone();
                    let embed_clone = embedding.to_vec();
                    tokio::spawn(async move {
                        let _permit = sem.acquire().await;
                        if let Err(e) = persist
                            .update_embeddings_batch(vec![(id_clone, project_id, embed_clone)])
                            .await
                        {
                            tracing::error!("Failed to update embeddings batch: {}", e);
                        }
                    });
                }

                _ => {}
            }
        }
    });

    // 8. Axum router
    let state = Arc::new(AppState {
        bus,
        embedding_service,
        persistence,
        _crawler: crawler,
        url_projects,
        sessions,
        shutdown_flag,
    });

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/crawl", get(crawl_handler))
        .route("/embeddings", post(embeddings_handler))
        .route("/search", post(search_handler))
        .with_state(state);

    // Graceful shutdown handler
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for Ctrl+C");
        info!("Shutdown signal received");
        shutdown_flag_clone.store(true, Ordering::SeqCst);
    });

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3010").await?;
    info!("Listening on http://0.0.0.0:3010");
    axum::serve(listener, app).await?;

    Ok(())
}

// --- Handlers ---

fn make_progress(
    session_id: &str,
    pending: &HashSet<String>,
    embedded: &HashSet<String>,
    ing: &HashSet<String>,
    req: &HashSet<String>,
    failed: &HashSet<String>,
) -> serde_json::Value {
    serde_json::json!({
        "type": "progress",
        "session_id": session_id,
        "total_nodes": pending.len() + embedded.len(),
        "embedded_nodes": embedded.len(),
        "ingested_urls": ing.len(),
        "requested_urls": req.len(),
        "failed_urls": failed.len()
    })
}

async fn health_handler() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok" }))
}

async fn search_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<SearchRequest>,
) -> impl IntoResponse {
    match state
        .persistence
        .search(&payload.query, &payload.project_id)
        .await
    {
        Ok(results) => Json(results).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
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
) -> impl IntoResponse {
    let url = Arc::new(params.url);
    let project_id = params.project_id.clone();
    let start_domain = Url::parse(&url)
        .ok()
        .and_then(|u| u.domain().map(|d| d.to_string()));

    let session_id = Uuid::new_v4().to_string();

    info!(
        "Crawl request for: {} project: {} (domain: {:?}) session: {} max_crawls: {} max_chunks: {}",
        url, project_id, start_domain, session_id, params.max_crawls, params.max_chunks
    );

    let session_state = SessionState {
        session_id: session_id.clone(),
        project_id: project_id.clone(),
        start_domain: start_domain.clone(),
        max_crawls: params.max_crawls,
        requested: HashSet::new(),
        visited: HashSet::new(),
        counts: Arc::new(AtomicUsize::new(0)),
        last_crawl: Instant::now(),
    };

    state
        .sessions
        .write()
        .insert(session_id.clone(), session_state);

    state.url_projects.write().insert(
        (*url).clone(),
        (
            project_id,
            session_id.clone(),
            params.max_crawls,
            Instant::now(),
        ),
    );

    // Kick off the crawl AFTER registering the mapping
    if let Err(e) = state.bus.publish(Event::new(
        "web-trigger",
        1,
        AppEvent::Crawler(CrawlerEvent::CrawlRequested {
            url: (*url).clone(),
            wait_selector: None,
        }),
    )) {
        warn!("Failed to publish initial crawl request: {}", e);
    }

    // Subscribe to events AFTER publishing to ensure we receive all events
    let bus_rx = state.bus.subscribe();

    // Track session progress for SSE completion
    let session_id_clone = session_id.clone();
    let mut requested_urls = HashSet::new();
    requested_urls.insert((*url).clone());
    let ingested_urls = HashSet::new();
    let pending_chunks = HashSet::new();
    let embedded_chunks = HashSet::new();
    let failed_urls = HashSet::new();
    let last_activity = std::time::Instant::now();
    let idle_count = 0;
    let state_clone = Arc::clone(&state);
    let config_for_sse = AppConfig::default();
    let max_crawls_limit = params.max_crawls;

    let stream = stream::unfold(
        (
            bus_rx,
            url,
            session_id_clone,
            start_domain,
            requested_urls,
            ingested_urls,
            pending_chunks,
            embedded_chunks,
            failed_urls,
            last_activity,
            idle_count,
            state_clone,
            max_crawls_limit,
        ),
        move |(
            mut rx,
            url_arc,
            session_id,
            domain,
            mut req,
            mut ing,
            mut pending_chunks,
            mut embedded_chunks,
            mut failed,
            mut last,
            mut idle_count,
            state_clone,
            max_crawls,
        )| async move {
            loop {
                // Track progress
                let total_chunks = pending_chunks.len() + embedded_chunks.len();
                let embedded_count = embedded_chunks.len();
                let requested_count = req.len();

                // Only check for completion after we've had at least one activity
                // and no pending chunks remain
                // Complete if: reached max_crawls OR (has progress, no pending chunks, and idle)
                let has_started = !ing.is_empty() || embedded_count > 0;
                let total_processed = requested_count + failed.len();
                let reached_max = total_processed >= max_crawls;
                let is_complete =
                    reached_max || (pending_chunks.is_empty() && has_started && idle_count >= 2);

                // After completion is sent, end the stream
                if is_complete && idle_count >= 3 {
                    info!("Crawl session ended for: {}", url_arc);
                    // Reset embedding cache for next crawl session
                    let _ = state_clone.bus.publish(Event::new(
                        "reset",
                        1,
                        AppEvent::Graph(GraphEvent::ResetEmbeddingCache),
                    ));
                    // Clean up session state
                    state_clone.sessions.write().remove(&session_id);
                    let mut urls_to_remove = Vec::new();
                    {
                        let map = state_clone.url_projects.read();
                        for (url, (_, sid, _, _)) in map.iter() {
                            if sid == &session_id {
                                urls_to_remove.push(url.clone());
                            }
                        }
                    }
                    if !urls_to_remove.is_empty() {
                        let mut map = state_clone.url_projects.write();
                        for url in urls_to_remove {
                            map.remove(&url);
                        }
                    }
                    info!("Cleaned up session: {}", session_id);
                    return None;
                }

                if is_complete {
                    info!(
                        "Crawl session completed for: {} ({} chunks, requested: {})",
                        url_arc, embedded_count, requested_count
                    );
                    // Send final progress and increment idle to signal we're done
                    idle_count += 1;
                    let progress_json = make_progress(
                        &session_id,
                        &pending_chunks,
                        &embedded_chunks,
                        &ing,
                        &req,
                        &failed,
                    );
                    return Some((
                        Ok(SseEvent::default().data(progress_json.to_string())),
                        (
                            rx,
                            url_arc,
                            session_id,
                            domain,
                            req,
                            ing,
                            pending_chunks,
                            embedded_chunks,
                            failed,
                            last,
                            idle_count,
                            state_clone,
                            max_crawls,
                        ),
                    ));
                }

                // Timeout check using configurable timeout
                // Only timeout if we've actually had some activity and been waiting too long
                let has_progress = !ing.is_empty() || embedded_count > 0;
                if has_progress && last.elapsed().as_secs() > config_for_sse.crawl_timeout_secs {
                    info!(
                        "Crawl session timed out for: {} (ing: {}, embedded: {})",
                        url_arc,
                        ing.len(),
                        embedded_count
                    );
                    let progress_json = make_progress(
                        &session_id,
                        &pending_chunks,
                        &embedded_chunks,
                        &ing,
                        &req,
                        &failed,
                    );
                    return Some((
                        Ok(SseEvent::default().data(progress_json.to_string())),
                        (
                            rx,
                            url_arc,
                            session_id,
                            domain,
                            req,
                            ing,
                            pending_chunks,
                            embedded_chunks,
                            failed,
                            last,
                            idle_count,
                            state_clone,
                            max_crawls,
                        ),
                    ));
                }

                // Wait for next event
                match tokio::time::timeout(std::time::Duration::from_millis(500), rx.recv()).await {
                    Ok(Ok(event)) => match event.payload {
                        AppEvent::Crawler(CrawlerEvent::CrawlRequested { url, .. }) => {
                            if let Some(ref d) = domain {
                                if Url::parse(&url)
                                    .ok()
                                    .and_then(|u| u.domain().map(|d| d.to_string()))
                                    .as_ref()
                                    == Some(d)
                                {
                                    if req.insert(url.clone()) {
                                        last = std::time::Instant::now();
                                        idle_count = 0;
                                    }
                                }
                            }
                        }
                        AppEvent::Crawler(CrawlerEvent::PageIngested { url, chunks, .. }) => {
                            if req.contains(&url) {
                                if ing.insert(url.clone()) {
                                    last = std::time::Instant::now();
                                    idle_count = 0;

                                    // Add chunks to pending
                                    let chunk_count = chunks.len().max(1);
                                    for i in 0..chunk_count {
                                        let chunk_id = format!("{}#chunk-{}", url, i);
                                        pending_chunks.insert(chunk_id);
                                    }

                                    // Send progress update
                                    let progress_json = make_progress(
                                        &session_id,
                                        &pending_chunks,
                                        &embedded_chunks,
                                        &ing,
                                        &req,
                                        &failed,
                                    );
                                    return Some((
                                        Ok(SseEvent::default().data(progress_json.to_string())),
                                        (
                                            rx,
                                            url_arc,
                                            session_id,
                                            domain,
                                            req,
                                            ing,
                                            pending_chunks,
                                            embedded_chunks,
                                            failed,
                                            last,
                                            idle_count,
                                            state_clone,
                                            max_crawls,
                                        ),
                                    ));
                                }
                            }
                        }
                        AppEvent::Graph(GraphEvent::NodeCreated { id: _, .. }) => {
                            last = std::time::Instant::now();
                            idle_count = 0;
                        }
                        AppEvent::Graph(GraphEvent::EdgeAdded { from, to, .. }) => {
                            if from.contains(&*url_arc) || to.contains(&*url_arc) {
                                last = std::time::Instant::now();
                                idle_count = 0;

                                // Send progress update
                                let progress_json = make_progress(
                                    &session_id,
                                    &pending_chunks,
                                    &embedded_chunks,
                                    &ing,
                                    &req,
                                    &failed,
                                );
                                return Some((
                                    Ok(SseEvent::default().data(progress_json.to_string())),
                                    (
                                        rx,
                                        url_arc,
                                        session_id,
                                        domain,
                                        req,
                                        ing,
                                        pending_chunks,
                                        embedded_chunks,
                                        failed,
                                        last,
                                        idle_count,
                                        state_clone,
                                        max_crawls,
                                    ),
                                ));
                            }
                        }
                        AppEvent::Embedding(
                            rust_eventbus::embedding::event::EmbeddingEvent::EmbeddingExtracted {
                                id,
                                ..
                            },
                        ) => {
                            if pending_chunks.remove(&id) {
                                embedded_chunks.insert(id);
                                last = std::time::Instant::now();
                                idle_count = 0;

                                // Send progress update
                                let progress_json = make_progress(
                                    &session_id,
                                    &pending_chunks,
                                    &embedded_chunks,
                                    &ing,
                                    &req,
                                    &failed,
                                );
                                return Some((
                                    Ok(SseEvent::default().data(progress_json.to_string())),
                                    (
                                        rx,
                                        url_arc,
                                        session_id,
                                        domain,
                                        req,
                                        ing,
                                        pending_chunks,
                                        embedded_chunks,
                                        failed,
                                        last,
                                        idle_count,
                                        state_clone,
                                        max_crawls,
                                    ),
                                ));
                            }
                        }
                        AppEvent::Crawler(CrawlerEvent::CrawlFailed { url, .. }) => {
                            if req.contains(&url) {
                                if failed.insert(url.clone()) {
                                    last = std::time::Instant::now();
                                    idle_count = 0;
                                }
                            }
                        }
                        _ => {}
                    },
                    Ok(Err(_)) => return None,
                    Err(_) => {
                        // Timeout - increment idle counter
                        idle_count += 1;
                    }
                }
            }
        },
    );

    use axum::response::sse::KeepAlive;
    let sse = Sse::new(stream).keep_alive(KeepAlive::new());

    // Custom response with nginx X-Accel-Buffering header to disable buffering
    struct SseWithHeaders<S>(Sse<S>);

    impl<S> IntoResponse for SseWithHeaders<S>
    where
        S: futures::Stream<Item = Result<SseEvent, Infallible>> + Send + 'static,
    {
        fn into_response(self) -> Response {
            let mut res = self.0.into_response();
            res.headers_mut()
                .insert("X-Accel-Buffering", HeaderValue::from_static("no"));
            res
        }
    }

    SseWithHeaders(sse)
}
