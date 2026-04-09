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
use pgvector::Vector;
use rust_eventbus::{
    app_event::{AppEvent, GraphEvent},
    crawler::{CrawlerConfig, CrawlerEvent, CrawlerService},
    embedding::{downloader::ModelDownloader, EmbeddingProcessor, OnnxEmbeddingService},
    store::FileEventStore,
    Event, EventBus,
};
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgPoolOptions, Row};
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::EnvFilter;
use url::Url;

#[derive(Debug, Deserialize)]
struct CrawlRequest {
    url: String,
    project_id: String,
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

#[derive(Debug, Serialize)]
struct SearchResultItem {
    vector_score: f32,
    text_score: f32,
    score: f32,
    id: String,
    page_url: String,
    title: String,
    section: String,
    content: String,
    chunk_index: i32,
}

struct AppState {
    bus: Arc<EventBus<AppEvent>>,
    embedding_service: Arc<OnnxEmbeddingService>,
    db: sqlx::PgPool,
    _crawler: Arc<CrawlerService>,
    /// Maps crawled URL → project_id so async handlers can associate chunks
    url_projects: Arc<parking_lot::RwLock<HashMap<String, String>>>,
}

async fn init_db(pool: &sqlx::PgPool) -> Result<()> {
    sqlx::query("CREATE EXTENSION IF NOT EXISTS vector")
        .execute(pool)
        .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS bge_pages (
            url         TEXT NOT NULL,
            project_id  TEXT NOT NULL,
            title       TEXT,
            crawled_at  TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (url, project_id)
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS bge_chunks (
            id          TEXT NOT NULL,
            project_id  TEXT NOT NULL,
            page_url    TEXT NOT NULL,
            chunk_index INT NOT NULL,
            section     TEXT,
            content     TEXT NOT NULL,
            embedding   vector(384),
            created_at  TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (id, project_id),
            UNIQUE (page_url, project_id, chunk_index)
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS bge_page_links (
            from_url    TEXT NOT NULL,
            to_url      TEXT NOT NULL,
            project_id  TEXT NOT NULL,
            PRIMARY KEY (from_url, to_url, project_id)
        )",
    )
    .execute(pool)
    .await?;

    // HNSW works on empty tables (unlike IVFFlat)
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_bge_chunks_embedding
         ON bge_chunks USING hnsw (embedding vector_cosine_ops)",
    )
    .execute(pool)
    .await
    .ok();

    sqlx::query("CREATE INDEX IF NOT EXISTS idx_bge_chunks_project ON bge_chunks (project_id)")
        .execute(pool)
        .await?;

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_bge_chunks_page
         ON bge_chunks (page_url, project_id, chunk_index)",
    )
    .execute(pool)
    .await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into())
                .add_directive("chromiumoxide=error".parse().unwrap()),
        )
        .init();

    // 1. Database
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://localhost/eventbus".to_string());
    let db = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;
    init_db(&db).await?;
    info!("Connected to PostgreSQL");

    // 2. Event store (still needed for EmbeddingProcessor replay)
    let temp_dir = tempfile::tempdir()?;
    let store_path = temp_dir.path().join("events_web.log");
    let event_store = Arc::new(FileEventStore::new(&store_path).await?);

    // 3. Event bus
    let bus = Arc::new(EventBus::<AppEvent>::new(4096));

    // 4. Crawler
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

    // 5. Embedding service
    let model_cache = "data/models";
    let downloader = ModelDownloader::new(model_cache);
    let (model_path, tokenizer_path) = downloader.get_bge_small().await?;
    let embedding_service = Arc::new(OnnxEmbeddingService::new(&model_path, &tokenizer_path)?);

    let embedding_processor = EmbeddingProcessor::new(
        Arc::clone(&embedding_service),
        (*bus).clone(),
        Arc::clone(&event_store),
    )
    .await?;
    embedding_processor.spawn().await;

    // 6. Shared URL → project_id mapping
    let url_projects: Arc<parking_lot::RwLock<HashMap<String, String>>> =
        Arc::new(parking_lot::RwLock::new(HashMap::new()));

    // 7. Pipeline: events → Postgres storage + graph events for SSE
    let pipeline_bus = Arc::clone(&bus);
    let pipeline_db = db.clone();
    let pipeline_projects = Arc::clone(&url_projects);
    let mut pipeline_rx = bus.subscribe();

    tokio::spawn(async move {
        let max_crawls = 500;
        let requested_counts: Arc<parking_lot::RwLock<HashMap<String, Arc<AtomicUsize>>>> =
            Arc::new(parking_lot::RwLock::new(HashMap::new()));
        let visited: Arc<parking_lot::RwLock<HashMap<String, HashSet<String>>>> =
            Arc::new(parking_lot::RwLock::new(HashMap::new()));

        while let Ok(event) = pipeline_rx.recv().await {
            match &event.payload {
                AppEvent::Crawler(CrawlerEvent::PageIngested {
                    url,
                    title,
                    links,
                    chunks,
                }) => {
                    let project_id = match pipeline_projects.read().get(url).cloned() {
                        Some(pid) => pid,
                        None => continue,
                    };

                    info!("Pipeline: storing page {} (project {})", url, project_id);

                    // Upsert page
                    if let Err(e) = sqlx::query(
                        "INSERT INTO bge_pages (url, project_id, title) VALUES ($1, $2, $3)
                         ON CONFLICT (url, project_id) DO UPDATE SET title = $3",
                    )
                    .bind(url)
                    .bind(&project_id)
                    .bind(title)
                    .execute(&pipeline_db)
                    .await
                    {
                        tracing::error!("Failed to insert page: {}", e);
                    }

                    // Link discovery
                    let start_domain = Url::parse(url)
                        .ok()
                        .and_then(|u| u.domain().map(|d| d.to_string()));

                    for target in links {
                        let mut target_url = match Url::parse(target) {
                            Ok(u) => u,
                            Err(_) => continue,
                        };
                        target_url.set_query(None);
                        target_url.set_fragment(None);
                        let normalized = target_url.to_string();

                        // Check per-project visited set
                        let is_visited = {
                            let visited = visited.read();
                            visited
                                .get(&project_id)
                                .map(|v| v.contains(&normalized))
                                .unwrap_or(false)
                        };
                        if is_visited {
                            continue;
                        }

                        let target_domain = target_url.domain();
                        let is_same_domain =
                            target_domain.is_some() && target_domain == start_domain.as_deref();

                        // Get or create per-project counter
                        let count = {
                            let counts = requested_counts.read();
                            counts.get(&project_id).cloned()
                        };
                        let count = match count {
                            Some(c) => c,
                            None => {
                                let c = Arc::new(AtomicUsize::new(0));
                                requested_counts
                                    .write()
                                    .insert(project_id.clone(), Arc::clone(&c));
                                c
                            }
                        };

                        if !is_same_domain || count.load(Ordering::Relaxed) >= max_crawls {
                            continue;
                        }

                        // Mark as visited for this project
                        {
                            let mut visited = visited.write();
                            visited
                                .entry(project_id.clone())
                                .or_insert_with(HashSet::new)
                                .insert(normalized.clone());
                        }
                        count.fetch_add(1, Ordering::Relaxed);

                        // Track project for discovered URL
                        pipeline_projects
                            .write()
                            .insert(normalized.clone(), project_id.clone());

                        // Store link in Postgres
                        let link_db = pipeline_db.clone();
                        let link_from = url.clone();
                        let link_to = normalized.clone();
                        let link_pid = project_id.clone();
                        tokio::spawn(async move {
                            let _ = sqlx::query(
                                "INSERT INTO bge_page_links (from_url, to_url, project_id)
                                 VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                            )
                            .bind(&link_from)
                            .bind(&link_to)
                            .bind(&link_pid)
                            .execute(&link_db)
                            .await;
                        });

                        // Trigger crawl
                        let _ = pipeline_bus.publish(Event::new(
                            "discovery",
                            event.sequence_num + 1,
                            AppEvent::Crawler(CrawlerEvent::CrawlRequested {
                                url: normalized.clone(),
                                wait_selector: None,
                            }),
                        ));

                        // Publish edge for SSE
                        let _ = pipeline_bus.publish(Event::new(
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

                    // Store chunks
                    for (i, chunk) in chunks.iter().enumerate() {
                        let chunk_id = format!("{}#chunk-{}", url, i);
                        let section = if chunk.headers.is_empty() {
                            None
                        } else {
                            Some(chunk.headers.join(" > "))
                        };

                        if let Err(e) = sqlx::query(
                            "INSERT INTO bge_chunks (id, project_id, page_url, chunk_index, section, content)
                             VALUES ($1, $2, $3, $4, $5, $6)
                             ON CONFLICT (id, project_id) DO NOTHING",
                        )
                        .bind(&chunk_id)
                        .bind(&project_id)
                        .bind(url)
                        .bind(i as i32)
                        .bind(&section)
                        .bind(&chunk.content)
                        .execute(&pipeline_db)
                        .await
                        {
                            tracing::error!("Failed to insert chunk: {}", e);
                        }

                        // Publish NodeCreated for SSE
                        let mut metadata = HashMap::new();
                        metadata.insert("type".into(), "chunk".into());
                        metadata.insert("page_title".into(), title.clone());
                        metadata.insert("content".into(), chunk.content.clone());
                        if let Some(ref s) = section {
                            metadata.insert("section".into(), s.clone());
                        }

                        let _ = pipeline_bus.publish(Event::new(
                            &chunk_id,
                            event.sequence_num + 2,
                            AppEvent::Graph(GraphEvent::NodeCreated {
                                id: chunk_id.clone(),
                                metadata,
                            }),
                        ));

                        // Publish part_of edge for SSE
                        let _ = pipeline_bus.publish(Event::new(
                            &chunk_id,
                            event.sequence_num + 3,
                            AppEvent::Graph(GraphEvent::EdgeAdded {
                                from: chunk_id.clone(),
                                to: url.clone(),
                                relation: "part_of".into(),
                                weight: 1.0,
                            }),
                        ));

                        // Request embedding (consumed by EmbeddingProcessor)
                        let _ = pipeline_bus.publish(Event::new(
                            &chunk_id,
                            event.sequence_num + 4,
                            AppEvent::Graph(GraphEvent::RequestEmbedding {
                                id: chunk_id.clone(),
                                content: format!("Page: {}\nChunk: {}", title, chunk.content),
                            }),
                        ));
                    }
                }

                // Store computed embeddings in Postgres
                AppEvent::Embedding(
                    rust_eventbus::embedding::event::EmbeddingEvent::EmbeddingExtracted {
                        id,
                        embedding,
                    },
                ) => {
                    let page_url = match id.rsplit_once("#chunk-") {
                        Some((url, _)) => url,
                        None => continue,
                    };
                    let project_id = match pipeline_projects.read().get(page_url).cloned() {
                        Some(pid) => pid,
                        None => continue,
                    };

                    let vec = Vector::from(embedding.clone());
                    if let Err(e) = sqlx::query(
                        "UPDATE bge_chunks SET embedding = $1 WHERE id = $2 AND project_id = $3",
                    )
                    .bind(vec)
                    .bind(id)
                    .bind(&project_id)
                    .execute(&pipeline_db)
                    .await
                    {
                        tracing::error!("Failed to update embedding: {}", e);
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
        db,
        _crawler: crawler,
        url_projects,
    });

    let app = Router::new()
        .route("/crawl", get(crawl_handler))
        .route("/embeddings", post(embeddings_handler))
        .route("/search", post(search_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3010").await?;
    info!("Listening on http://0.0.0.0:3010");
    axum::serve(listener, app).await?;

    Ok(())
}

// --- Handlers ---

async fn search_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<SearchRequest>,
) -> impl IntoResponse {
    let query_emb = match state.embedding_service.embed(&payload.query) {
        Ok(emb) => emb,
        Err(e) => {
            return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    };

    let query_vec = Vector::from(query_emb);

    let rows = sqlx::query(
        "SELECT
            c.id,
            c.page_url,
            COALESCE(p.title, c.page_url) AS title,
            COALESCE(c.section, '') AS section,
            c.content,
            c.chunk_index,
            (1 - (c.embedding <=> $1))::real AS vector_score,
            ts_rank(to_tsvector('english', c.content), plainto_tsquery('english', $2)) AS text_score
        FROM bge_chunks c
        JOIN bge_pages p ON c.page_url = p.url AND c.project_id = p.project_id
        WHERE c.project_id = $3
          AND c.embedding IS NOT NULL
        ORDER BY
            (1 - (c.embedding <=> $1))::real * 0.7
            + ts_rank(to_tsvector('english', c.content), plainto_tsquery('english', $2)) * 0.3
            DESC
        LIMIT 20",
    )
    .bind(&query_vec)
    .bind(&payload.query)
    .bind(&payload.project_id)
    .fetch_all(&state.db)
    .await;

    match rows {
        Ok(rows) => {
            let results: Vec<SearchResultItem> = rows
                .iter()
                .map(|row| {
                    let vector_score: f32 = row.get("vector_score");
                    let text_score: f32 = row.get("text_score");
                    SearchResultItem {
                        vector_score,
                        text_score,
                        score: vector_score * 0.7 + text_score * 0.3,
                        id: row.get("id"),
                        page_url: row.get("page_url"),
                        title: row.get("title"),
                        section: row.get("section"),
                        content: row.get("content"),
                        chunk_index: row.get("chunk_index"),
                    }
                })
                .collect();
            Json(results).into_response()
        }
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
    let project_id = params.project_id;
    let start_domain = Url::parse(&url)
        .ok()
        .and_then(|u| u.domain().map(|d| d.to_string()));

    info!(
        "Crawl request for: {} project: {} (domain: {:?})",
        url, project_id, start_domain
    );

    // Register URL → project_id so the pipeline handler can find it
    state
        .url_projects
        .write()
        .insert((*url).clone(), project_id);

    let bus_rx = state.bus.subscribe();

    // Kick off the crawl
    let _ = state.bus.publish(Event::new(
        "web-trigger",
        1,
        AppEvent::Crawler(CrawlerEvent::CrawlRequested {
            url: (*url).clone(),
            wait_selector: None,
        }),
    ));

    // Track session progress for SSE completion
    let mut requested_urls = HashSet::new();
    requested_urls.insert((*url).clone());
    let ingested_urls = HashSet::new();
    let created_nodes = HashSet::new();
    let embedded_nodes = HashSet::new();
    let last_activity = std::time::Instant::now();
    let settle_count = 0;

    let stream = stream::unfold(
        (
            bus_rx,
            url,
            start_domain,
            requested_urls,
            ingested_urls,
            created_nodes,
            embedded_nodes,
            last_activity,
            settle_count,
        ),
        move |(
            mut rx,
            url_arc,
            domain,
            mut req,
            mut ing,
            mut created,
            mut embedded,
            mut last,
            mut settle,
        )| async move {
            loop {
                let is_done = !ing.is_empty()
                    && ing.len() >= req.len()
                    && !created.is_empty()
                    && embedded.len() >= created.len();

                if is_done {
                    settle += 1;
                    if settle > 3 {
                        info!("Crawl session completed for: {}", url_arc);
                        let progress_json = serde_json::json!({
                            "type": "progress",
                            "total_nodes": created.len(),
                            "embedded_nodes": embedded.len(),
                            "ingested_urls": ing.len(),
                            "requested_urls": req.len()
                        });
                        return Some((
                            Ok(SseEvent::default().data(progress_json.to_string())),
                            (
                                rx, url_arc, domain, req, ing, created, embedded, last, settle,
                            ),
                        ));
                    }
                } else if last.elapsed().as_secs() > 30 && !ing.is_empty() {
                    info!("Crawl session timed out for: {}", url_arc);
                    return None;
                }

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
                            let (_id, is_rel) = match &graph_event {
                                GraphEvent::NodeCreated { id, .. } => (id.clone(), true),
                                GraphEvent::EdgeAdded { from, to, .. } => (
                                    from.clone(),
                                    from.contains(&*url_arc) || to.contains(&*url_arc),
                                ),
                                _ => (String::new(), false),
                            };

                            if is_rel {
                                if let GraphEvent::NodeCreated {
                                    id: ref node_id, ..
                                } = graph_event
                                {
                                    created.insert(node_id.clone());
                                }
                                last = std::time::Instant::now();
                                settle = 0;

                                let progress_json = serde_json::json!({
                                    "type": "progress",
                                    "total_nodes": created.len(),
                                    "embedded_nodes": embedded.len(),
                                    "ingested_urls": ing.len(),
                                    "requested_urls": req.len()
                                });
                                return Some((
                                    Ok(SseEvent::default().data(progress_json.to_string())),
                                    (
                                        rx, url_arc, domain, req, ing, created, embedded, last,
                                        settle,
                                    ),
                                ));
                            }
                        }
                        AppEvent::Embedding(
                            rust_eventbus::embedding::event::EmbeddingEvent::EmbeddingExtracted {
                                id,
                                embedding: _,
                            },
                        ) => {
                            if created.contains(&id) {
                                if embedded.insert(id.clone()) {
                                    last = std::time::Instant::now();
                                    settle = 0;

                                    let progress_json = serde_json::json!({
                                        "type": "progress",
                                        "total_nodes": created.len(),
                                        "embedded_nodes": embedded.len(),
                                        "ingested_urls": ing.len(),
                                        "requested_urls": req.len()
                                    });
                                    return Some((
                                        Ok(SseEvent::default().data(progress_json.to_string())),
                                        (
                                            rx, url_arc, domain, req, ing, created, embedded, last,
                                            settle,
                                        ),
                                    ));
                                }
                            }
                        }
                        _ => {}
                    },
                    Ok(Err(_)) => return None,
                    Err(_) => continue,
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
