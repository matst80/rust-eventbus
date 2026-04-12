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
    store::NoopEventStore,
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
    max_chunks: usize,
    session_timeout_secs: u64,
    crawl_timeout_secs: u64,
    db_max_connections: usize,
    bus_capacity: usize,
    crawler_concurrency: usize,
    persistence_concurrency: usize,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
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
            persistence_concurrency: std::env::var("PERSISTENCE_CONCURRENCY")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(4),
        }
    }
}

struct SessionState {
    last_crawl: Instant,
}

#[derive(Debug, Clone, Default)]
struct CrawlProgressState {
    requested_urls: HashSet<String>,
    ingested_urls: HashSet<String>,
    pending_chunks: HashSet<String>,
    embedded_chunks: HashSet<String>,
    failed_urls: HashSet<String>,
    preview_chunks: Vec<PreviewChunk>,
    last_activity: Option<Instant>,
}

struct AppState {
    bus: Arc<EventBus<AppEvent>>,
    embedding_service: Arc<OnnxEmbeddingService>,
    persistence: Arc<PersistenceService>,
    embedding_queue: tokio::sync::mpsc::Sender<(String, String, u64)>,
    url_projects:
        Arc<parking_lot::RwLock<HashMap<String, (String, String, usize, usize, Instant)>>>,
    chunk_sessions: Arc<parking_lot::RwLock<HashMap<String, String>>>,
    progress: Arc<parking_lot::RwLock<HashMap<String, CrawlProgressState>>>,
    sessions: Arc<parking_lot::RwLock<HashMap<String, SessionState>>>,
    shutdown_flag: Arc<AtomicBool>,
}

#[derive(Debug, Clone, Serialize)]
struct PreviewChunk {
    id: String,
    page_url: String,
    page_title: String,
    section: Option<String>,
    preview: String,
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
    let event_store = Arc::new(NoopEventStore::new());

    // 3. Event bus
    let bus = Arc::new(EventBus::<AppEvent>::new(config.bus_capacity));

    // 4. Crawler
    let crawler_config = CrawlerConfig {
        concurrency: config.crawler_concurrency,
        headless: true,
        max_chunks: config.max_chunks,
        shutdown_flag: Some(Arc::clone(&shutdown_flag_clone)),
        user_data_dir: std::env::var("CHROME_USER_DATA_DIR").ok().map(Into::into),
        ..Default::default()
    };
    let crawler = Arc::new(CrawlerService::new(Arc::clone(&bus), crawler_config));
    let crawler_clone = Arc::clone(&crawler);
    let _shutdown_flag_worker = Arc::clone(&shutdown_flag_clone);
    tokio::spawn(async move {
        if let Err(e) = crawler_clone.run().await {
            tracing::error!("Crawler service failed: {}", e);
        }
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
    let embedding_queue = embedding_processor.spawn().await;

    // 2. Persistence service
    let persistence = Arc::new(PersistenceService::new(
        Arc::new(db.clone()),
        Arc::new(move |text: &str| {
            let emb = embedding_service_for_persistence.embed(text)?;
            tracing::debug!("Generated embedding of length {}", emb.len());
            Ok(emb)
        }),
    ));
    persistence.init_schema().await?;

    // 6. Shared URL → (project_id, session_id, max_crawls, max_chunks, timestamp) mapping
    let url_projects: Arc<
        parking_lot::RwLock<HashMap<String, (String, String, usize, usize, Instant)>>,
    > = Arc::new(parking_lot::RwLock::new(HashMap::new()));
    let url_projects_cleanup = Arc::clone(&url_projects);
    let chunk_sessions: Arc<parking_lot::RwLock<HashMap<String, String>>> =
        Arc::new(parking_lot::RwLock::new(HashMap::new()));
    let chunk_sessions_cleanup = Arc::clone(&chunk_sessions);
    let progress: Arc<parking_lot::RwLock<HashMap<String, CrawlProgressState>>> =
        Arc::new(parking_lot::RwLock::new(HashMap::new()));
    let progress_cleanup = Arc::clone(&progress);

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
                for (url, (_, _, _, _, timestamp)) in map.iter() {
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
                for sid in &session_to_remove {
                    map.remove(sid);
                    progress_cleanup.write().remove(sid);
                }
                info!("Cleaned up {} stale sessions", count);
            }
            if !session_to_remove.is_empty() {
                let session_ids: HashSet<_> = session_to_remove.into_iter().collect();
                chunk_sessions_cleanup
                    .write()
                    .retain(|_, sid| !session_ids.contains(sid));
            }
        }
    });

    // 7. Pipeline: events → Postgres storage + graph events for SSE
    let pipeline_bus = Arc::clone(&bus);
    let pipeline_persistence = Arc::clone(&persistence);
    let pipeline_projects = Arc::clone(&url_projects);
    let pipeline_chunk_sessions = Arc::clone(&chunk_sessions);
    let pipeline_progress = Arc::clone(&progress);
    let pipeline_embedding_queue = embedding_queue.clone();
    let mut pipeline_rx = bus.subscribe();
    let config_clone = config.clone();

    // Semaphore for bounded concurrent batch operations
    let batch_semaphore = Arc::new(tokio::sync::Semaphore::new(config.persistence_concurrency));
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
                    let (project_id, session_id, max_crawls, _, _) =
                        match pipeline_projects.read().get(url).cloned() {
                            Some((pid, sid, max, max_chunks, ts)) => {
                                (pid, sid, max, max_chunks, ts)
                            }
                            None => continue,
                        };

                    {
                        let mut progress = pipeline_progress.write();
                        let entry = progress.entry(session_id.clone()).or_default();
                        entry.requested_urls.insert(url.clone());
                        entry.last_activity = Some(Instant::now());
                    }

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
                    let (project_id, session_id, max_crawls, max_chunks, _) =
                        match pipeline_projects.read().get(url).cloned() {
                            Some((pid, sid, max, max_chunks, ts)) => {
                                (pid, sid, max, max_chunks, ts)
                            }
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
                                max_chunks,
                                Instant::now(),
                            ),
                        );

                        // Queue link for batch insert
                        links_to_insert.push((url.clone(), normalized.clone(), project_id.clone()));

                        // Queue URL for crawling
                        urls_to_crawl.push(normalized.clone());

                    }

                    // Batch insert links
                    if !links_to_insert.is_empty() {
                        let persist = Arc::clone(&pipeline_persistence);
                        let permit = match Arc::clone(&batch_semaphore_clone).acquire_owned().await {
                            Ok(permit) => permit,
                            Err(e) => {
                                tracing::error!("Failed to acquire link batch permit: {}", e);
                                continue;
                            }
                        };
                        tokio::spawn(async move {
                            let _permit = permit;
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
                        {
                            let mut progress = pipeline_progress.write();
                            let entry = progress.entry(session_id.clone()).or_default();
                            entry.ingested_urls.insert(url.clone());
                            entry.last_activity = Some(Instant::now());
                            for (i, chunk) in chunks.iter().enumerate() {
                                let chunk_id = format!("{}#chunk-{}", url, i);
                                entry.pending_chunks.insert(chunk_id.clone());
                                pipeline_chunk_sessions
                                    .write()
                                    .insert(chunk_id.clone(), session_id.clone());
                                entry.preview_chunks.push(PreviewChunk {
                                    id: chunk_id,
                                    page_url: url.clone(),
                                    page_title: title.clone(),
                                    section: (!chunk.headers.is_empty())
                                        .then(|| chunk.headers.join(" > ")),
                                    preview: preview_text(&chunk.content, 220),
                                });
                            }
                            if entry.preview_chunks.len() > 3 {
                                let drain_len = entry.preview_chunks.len() - 3;
                                entry.preview_chunks.drain(0..drain_len);
                            }
                        }

                        let persist = Arc::clone(&pipeline_persistence);
                        let permit = match Arc::clone(&batch_semaphore_clone).acquire_owned().await {
                            Ok(permit) => permit,
                            Err(e) => {
                                tracing::error!("Failed to acquire chunk batch permit: {}", e);
                                continue;
                            }
                        };
                        tokio::spawn(async move {
                            let _permit = permit;
                            if let Err(e) = persist.insert_chunks_batch(chunks_to_insert).await {
                                tracing::error!("Failed to insert chunks batch: {}", e);
                            }
                        });

                        for (i, chunk) in chunks.iter().enumerate() {
                            let chunk_id = format!("{}#chunk-{}", url, i);
                            // Request embedding (consumed by EmbeddingProcessor)
                            if let Err(e) = pipeline_embedding_queue
                                .send((
                                    chunk_id.clone(),
                                    format!("Page: {}\nChunk: {}", title, chunk.content),
                                    event.sequence_num + 2,
                                ))
                                .await
                            {
                                warn!("Failed to queue embedding request: {}", e);
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
                                max_chunks: Some(max_chunks),
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

                    if let Some(session_id) = pipeline_chunk_sessions.read().get(id).cloned() {
                        let mut progress = pipeline_progress.write();
                        if let Some(entry) = progress.get_mut(&session_id) {
                            entry.pending_chunks.remove(id);
                            entry.embedded_chunks.insert(id.clone());
                            entry.last_activity = Some(Instant::now());
                        }
                    }

                    // Batch update embedding
                    let persist = Arc::clone(&pipeline_persistence);
                    let id_clone = id.clone();
                    let embed_clone = embedding.clone();
                    let permit = match Arc::clone(&batch_semaphore_clone).acquire_owned().await {
                        Ok(permit) => permit,
                        Err(e) => {
                            tracing::error!("Failed to acquire embedding update permit: {}", e);
                            continue;
                        }
                    };
                    tokio::spawn(async move {
                        let _permit = permit;
                        match persist.update_embedding(&id_clone, embed_clone).await {
                            Ok(0) => {
                                tracing::warn!("No chunk row found to update embedding for id: {}", id_clone);
                            }
                            Ok(_) => {}
                            Err(e) => {
                                tracing::error!("Failed to update embedding: {}", e);
                            }
                        }
                    });
                }
                AppEvent::Crawler(CrawlerEvent::CrawlFailed { url, .. }) => {
                    if let Some((_, session_id, _, _, _)) = pipeline_projects.read().get(url).cloned()
                    {
                        let mut progress = pipeline_progress.write();
                        let entry = progress.entry(session_id).or_default();
                        entry.failed_urls.insert(url.clone());
                        entry.last_activity = Some(Instant::now());
                    }
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
        embedding_queue,
        url_projects,
        chunk_sessions,
        progress,
        sessions,
        shutdown_flag: Arc::clone(&shutdown_flag),
    });

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/crawl", get(crawl_handler))
        .route("/embeddings", post(embeddings_handler))
        .route("/search", post(search_handler))
        .with_state(state);


    let host = std::env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".to_string());
    let addr = format!("{}:{}", host, port);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("Listening on http://{}", addr);
    
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            tokio::signal::ctrl_c()
                .await
                .expect("Failed to listen for Ctrl+C");
            info!("Shutdown signal received, stopping server...");
            shutdown_flag_clone.store(true, Ordering::SeqCst);
        })
        .await?;

    info!("Web server stopped cleanly");
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
    previews: &[PreviewChunk],
) -> serde_json::Value {
    serde_json::json!({
        "type": "progress",
        "session_id": session_id,
        "total_nodes": pending.len() + embedded.len(),
        "embedded_nodes": embedded.len(),
        "ingested_urls": ing.len(),
        "requested_urls": req.len(),
        "failed_urls": failed.len(),
        "preview_chunks": previews
    })
}

fn preview_text(text: &str, max_chars: usize) -> String {
    let mut out = String::with_capacity(text.len().min(max_chars + 1));
    let mut count = 0usize;
    for ch in text.chars() {
        if count == max_chars {
            out.push_str("...");
            return out;
        }
        out.push(ch);
        count += 1;
    }
    out
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
    let embedding_service = Arc::clone(&state.embedding_service);
    let text = payload.text;

    match tokio::task::spawn_blocking(move || embedding_service.embed(&text)).await {
        Ok(Ok(embedding)) => Json(EmbeddingResponse { embedding }).into_response(),
        Ok(Err(e)) => {
            (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
        Err(e) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Embedding task failed: {}", e),
        )
            .into_response(),
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
        last_crawl: Instant::now(),
    };

    state
        .sessions
        .write()
        .insert(session_id.clone(), session_state);
    state
        .progress
        .write()
        .insert(
            session_id.clone(),
            CrawlProgressState {
                requested_urls: HashSet::from([(*url).clone()]),
                last_activity: Some(Instant::now()),
                ..Default::default()
            },
        );

    state.url_projects.write().insert(
        (*url).clone(),
        (
            project_id,
            session_id.clone(),
            params.max_crawls,
            params.max_chunks,
            Instant::now(),
        ),
    );

    // Subscribe before publishing so the SSE stream can see the first crawl events.
    let bus_rx = state.bus.subscribe();

    if let Err(e) = state.bus.publish(Event::new(
        "web-trigger",
        1,
        AppEvent::Crawler(CrawlerEvent::CrawlRequested {
            url: (*url).clone(),
            wait_selector: None,
            max_chunks: Some(params.max_chunks),
        }),
    )) {
        warn!("Failed to publish initial crawl request: {}", e);
    }

    // Track session progress for SSE completion
    let session_id_clone = session_id.clone();
    let mut requested_urls = HashSet::new();
    requested_urls.insert((*url).clone());
    let ingested_urls = HashSet::new();
    let pending_chunks = HashSet::new();
    let embedded_chunks = HashSet::new();
    let failed_urls = HashSet::new();
    let preview_chunks = Vec::new();
    let last_activity = std::time::Instant::now();
    let idle_count = 0;
    let state_clone = Arc::clone(&state);
    let config_for_sse = AppConfig::default();
    let max_crawls_limit = params.max_crawls;

    let shutdown_flag_sse = Arc::clone(&state_clone.shutdown_flag);
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
            preview_chunks,
            last_activity,
            idle_count,
            state_clone,
            max_crawls_limit,
            shutdown_flag_sse,
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
            mut previews,
            mut last,
            mut idle_count,
            state_clone,
            max_crawls,
            shutdown_flag,
        )| async move {
            loop {
                // Check if server is shutting down
                if shutdown_flag.load(Ordering::SeqCst) {
                    info!("SSE stream: server shutting down, ending session {}", session_id);
                    return None;
                }

                if let Some(current) = state_clone.progress.read().get(&session_id).cloned() {
                    req = current.requested_urls;
                    ing = current.ingested_urls;
                    pending_chunks = current.pending_chunks;
                    embedded_chunks = current.embedded_chunks;
                    failed = current.failed_urls;
                    previews = current.preview_chunks;
                    if let Some(activity) = current.last_activity {
                        if activity > last {
                            last = activity;
                            idle_count = 0;
                        }
                    }
                }

                // Track progress
                let embedded_count = embedded_chunks.len();
                let requested_count = req.len();

                // Only check for completion after we've had at least one activity
                // and no pending chunks remain.
                // Complete only when all requested URLs have been processed (ingested or failed),
                // or when we've processed up to the max crawl limit.
                let has_started = !ing.is_empty() || embedded_count > 0;
                let processed_urls = ing.len() + failed.len();
                let reached_max = processed_urls >= max_crawls;
                let all_requested_processed = processed_urls >= requested_count;
                let is_complete = has_started
                    && pending_chunks.is_empty()
                    && idle_count >= 2
                    && (reached_max || all_requested_processed);

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
                        for (url, (_, sid, _, _, _)) in map.iter() {
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
                    state_clone.progress.write().remove(&session_id);
                    state_clone
                        .chunk_sessions
                        .write()
                        .retain(|_, sid| sid != &session_id);
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
                        &previews,
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
                            previews,
                            last,
                            idle_count,
                            state_clone,
                            max_crawls,
                            shutdown_flag,
                        ),
                    ));
                }

                // Timeout check using configurable timeout
                // Only timeout if we've actually had some activity and been waiting too long
                let has_progress = !ing.is_empty() || embedded_count > 0;
                let timeout_secs = if pending_chunks.is_empty() {
                    config_for_sse.crawl_timeout_secs
                } else {
                    config_for_sse.crawl_timeout_secs.saturating_mul(10)
                };
                if has_progress && last.elapsed().as_secs() > timeout_secs {
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
                        &previews,
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
                            previews,
                            last,
                            idle_count,
                            state_clone,
                            max_crawls,
                            shutdown_flag,
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
                        AppEvent::Crawler(CrawlerEvent::PageIngested { url, title, chunks, .. }) => {
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

                                    for (i, chunk) in chunks.iter().take(3).enumerate() {
                                        previews.push(PreviewChunk {
                                            id: format!("{}#chunk-{}", url, i),
                                            page_url: url.clone(),
                                            page_title: title.clone(),
                                            section: (!chunk.headers.is_empty())
                                                .then(|| chunk.headers.join(" > ")),
                                            preview: preview_text(&chunk.content, 220),
                                        });
                                    }
                                    if previews.len() > 3 {
                                        let drain_len = previews.len() - 3;
                                        previews.drain(0..drain_len);
                                    }

                                    // Send progress update
                                    let progress_json = make_progress(
                                        &session_id,
                                        &pending_chunks,
                                        &embedded_chunks,
                                        &ing,
                                        &req,
                                        &failed,
                                        &previews,
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
                                            previews,
                                            last,
                                            idle_count,
                                            state_clone,
                                            max_crawls,
                                            shutdown_flag,
                                        ),
                                    ));
                                }
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
                                    &previews,
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
                                        previews,
                                        last,
                                        idle_count,
                                        state_clone,
                                        max_crawls,
                                        shutdown_flag,
                                    ),
                                ));
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
