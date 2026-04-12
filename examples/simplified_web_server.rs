use anyhow::{Context, Result};
use ax_sse::{Event as SseEvent, Sse};
use axum::{
    extract::{Query, State},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use chromiumoxide::browser::{Browser, BrowserConfig};
use chromiumoxide::handler::viewport::Viewport;
use futures::StreamExt;
use rust_eventbus::{
    crawler::CrawlerService,
    embedding::{downloader::ModelDownloader, OnnxEmbeddingService},
    parser::{chunker::{MarkdownChunker, ChunkerOptions}, Chunk},
};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::collections::{HashSet, VecDeque};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info, warn};
use url::Url;

// We need a small alias for SSE since axum uses different SSE types
mod ax_sse {
    pub use axum::response::sse::{Event, KeepAlive};
    pub use axum::response::Sse;
}

#[path = "persistence.rs"]
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
    50
}
fn default_max_chunks() -> usize {
    50
}

#[derive(Debug, Deserialize)]
struct SearchRequest {
    query: String,
    project_id: String,
}

#[derive(Debug, Deserialize)]
struct EmbeddingRequest {
    text: String,
}

#[derive(Debug, Serialize)]
struct CrawlProgress {
    url: String,
    title: String,
    chunk_count: usize,
    links_found: usize,
    total_processed: usize,
    is_done: bool,
}

struct AppState {
    browser: Arc<Browser>,
    embedding_service: Arc<OnnxEmbeddingService>,
    persistence: Option<Arc<PersistenceService>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    info!("Starting Simplified Web Server (PDFs disabled for memory stability)...");

    // 1. Initialize Browser (shared across requests)
    let (browser, mut handler) = Browser::launch(
        BrowserConfig::builder()
            .window_size(1920, 1080)
            .viewport(Viewport {
                width: 1920,
                height: 1080,
                ..Default::default()
            })
            .arg("--disable-crashpad")
            .arg("--disable-dev-shm-usage")
            .arg("--no-first-run")
            .arg("--no-default-browser-check")
            .arg("--incognito")
            .arg("--disable-gpu")
            .arg("--disable-software-rasterizer")
            .arg("--disable-extensions")
            .arg("--disable-component-extensions-with-background-pages")
            .arg("--memory-pressure-thresholds=1")
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build browser config: {}", e))?,
    )
    .await?;

    let browser = Arc::new(browser);

    // Spawn browser handler
    tokio::spawn(async move {
        while let Some(h) = handler.next().await {
            if let Err(e) = h {
                error!("Browser handler error: {:?}", e);
                break;
            }
        }
    });

    // 2. Initialize Embedding Service
    let model_cache = "data/models";
    let downloader = ModelDownloader::new(model_cache);
    let (model_path, tokenizer_path) = downloader.get_bge_small().await?;
    let embedding_service = Arc::new(OnnxEmbeddingService::new(&model_path, &tokenizer_path)?);

    // 3. Initialize Persistence (Optional)
    let persistence = if let Ok(db_url) = std::env::var("DATABASE_URL") {
        info!("Connecting to database: {}", db_url);
        let db = PgPoolOptions::new()
            .max_connections(10)
            .connect(&db_url)
            .await?;
        
        let emb_service_clone = Arc::clone(&embedding_service);
        let service = Arc::new(PersistenceService::new(
            Arc::new(db),
            Arc::new(move |text: &str| emb_service_clone.embed(text)),
        ));
        service.init_schema().await?;
        Some(service)
    } else {
        warn!("DATABASE_URL not set, persistence disabled");
        None
    };

    let state = Arc::new(AppState {
        browser,
        embedding_service,
        persistence,
    });

    // 4. Setup Axum Router
    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/crawl", get(crawl_handler))
        .route("/search", post(search_handler))
        .route("/embeddings", post(embeddings_handler))
        .with_state(state);

    let host = std::env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".to_string());
    let addr = format!("{}:{}", host, port);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("Listening on http://{}", addr);

    axum::serve(listener, app).await?;

    Ok(())
}

async fn health_handler() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok" }))
}

async fn embeddings_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<EmbeddingRequest>,
) -> impl IntoResponse {
    let embedding_service = Arc::clone(&state.embedding_service);
    match tokio::task::spawn_blocking(move || embedding_service.embed(&payload.text)).await {
        Ok(Ok(embedding)) => Json(serde_json::json!({ "embedding": embedding })).into_response(),
        Ok(Err(e)) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn search_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<SearchRequest>,
) -> impl IntoResponse {
    let Some(persistence) = &state.persistence else {
        return (axum::http::StatusCode::SERVICE_UNAVAILABLE, "Persistence disabled").into_response();
    };

    match persistence.search(&payload.query, &payload.project_id).await {
        Ok(results) => Json(results).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn crawl_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<CrawlRequest>,
) -> impl IntoResponse {
    let (tx, rx) = mpsc::channel::<Result<SseEvent, Infallible>>(100);

    tokio::spawn(async move {
        if let Err(e) = perform_crawl(state, params, tx.clone()).await {
            error!("Crawl failed: {:?}", e);
            let _ = tx.send(Ok(SseEvent::default().data(format!("Error: {:?}", e)))).await;
        }
    });

    Sse::new(tokio_stream::wrappers::ReceiverStream::new(rx))
        .keep_alive(ax_sse::KeepAlive::new())
}

async fn perform_crawl(
    state: Arc<AppState>,
    params: CrawlRequest,
    tx: mpsc::Sender<Result<SseEvent, Infallible>>,
) -> Result<()> {
    let start_url = params.url;
    let project_id = params.project_id;
    let max_crawls = params.max_crawls;
    let max_chunks = params.max_chunks;

    let start_domain = Url::parse(&start_url)?
        .domain()
        .map(|d| d.to_string())
        .context("Invalid start URL domain")?;

    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    queue.push_back(start_url.clone());
    visited.insert(start_url.clone());

    let mut processed_count = 0;

    // Use a SINGLE page for the entire crawl to avoid the overhead of opening/closing tabs
    let page = state.browser.new_page("about:blank").await
        .context("Failed to create crawl page")?;

    while let Some(current_url) = queue.pop_front() {
        if processed_count >= max_crawls {
            break;
        }

        info!("Crawling [{}]: {}", processed_count, current_url);
        
        let is_pdf_url = current_url.to_lowercase().ends_with(".pdf");
        if is_pdf_url {
            warn!("Skipping PDF for memory stability: {}", current_url);
            continue;
        }

        match CrawlerService::process_crawl(&page, &current_url, None, max_chunks).await {
            Ok((title, links, chunks)) => {
                processed_count += 1;

                if let Some(ref p) = state.persistence {
                    let _ = p.upsert_page(&current_url, &project_id, &title).await;
                    
                    let mut chunk_batch = Vec::new();
                    let mut embedding_texts = Vec::new();
                    let mut chunk_ids = Vec::new();

                    for (i, chunk) in chunks.iter().enumerate() {
                        let chunk_id = format!("{}#chunk-{}", current_url, i);
                        let text = format!("Page: {}\nChunk: {}", title, chunk.content);
                        
                        let section = if chunk.headers.is_empty() {
                            None
                        } else {
                            Some(chunk.headers.join(" > "))
                        };
                        
                        chunk_batch.push((
                            chunk_id.clone(),
                            project_id.clone(),
                            current_url.clone(),
                            i as i32,
                            section,
                            chunk.content.clone(),
                        ));
                        embedding_texts.push(text);
                        chunk_ids.push(chunk_id);
                    }

                    if !chunk_batch.is_empty() {
                        let _ = p.insert_chunks_batch(chunk_batch).await;
                        
                        // Batch embed all chunks for this page
                        let emb_service = Arc::clone(&state.embedding_service);
                        match tokio::task::spawn_blocking(move || {
                            emb_service.embed_batch(&embedding_texts)
                        }).await {
                            Ok(Ok(embeddings)) => {
                                let mut embedding_batch = Vec::new();
                                for (id, emb) in chunk_ids.into_iter().zip(embeddings.into_iter()) {
                                    embedding_batch.push((id, emb));
                                }
                                let _ = p.update_embeddings_batch(embedding_batch).await;
                            }
                            Ok(Err(e)) => error!("Batch embedding failed: {:?}", e),
                            Err(e) => error!("Blocking task for batch embedding panicked: {:?}", e),
                        }
                    }
                }

                let progress = CrawlProgress {
                    url: current_url.clone(),
                    title,
                    chunk_count: chunks.len(),
                    links_found: links.len(),
                    total_processed: processed_count,
                    is_done: false,
                };

                let event = SseEvent::default().data(serde_json::to_string(&progress)?);
                if tx.send(Ok(event)).await.is_err() {
                    break;
                }

                for link in links {
                    let Ok(url) = Url::parse(&link) else { continue };
                    let normalized = {
                        let mut u = url.clone();
                        u.set_query(None);
                        u.set_fragment(None);
                        u.to_string()
                    };

                    if url.domain().as_ref().map(|d| d.to_string()) == Some(start_domain.clone()) {
                        if !visited.contains(&normalized) {
                            visited.insert(normalized.clone());
                            queue.push_back(normalized);
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Failed to crawl {}: {:?}", current_url, e);
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let _ = page.close().await;

    let final_progress = CrawlProgress {
        url: "".to_string(),
        title: "".to_string(),
        chunk_count: 0,
        links_found: 0,
        total_processed: processed_count,
        is_done: true,
    };
    let _ = tx.send(Ok(SseEvent::default().data(serde_json::to_string(&final_progress)?))).await;
    
    visited.clear();
    queue.clear();
    
    Ok(())
}
