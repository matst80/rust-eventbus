use async_trait::async_trait;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, put},
    Json, Router,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use uuid::Uuid;

use rust_eventbus::{
    bus::EventBus,
    cluster,
    distributed::{DistributedPubSub, LockError, ProjectionLockManager, TcpPubSub},
    event::{Event, EventPayload},
    projection::{DurableProjectionActor, EphemeralProjectionActor, Projection, ProjectionError},
    store::{CompactionRule, EventStore, FileEventStore, FileSnapshotStore},
};

fn same_endpoint(a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }

    match (
        a.parse::<std::net::SocketAddr>(),
        b.parse::<std::net::SocketAddr>(),
    ) {
        (Ok(lhs), Ok(rhs)) => lhs.ip() == rhs.ip() && lhs.port() == rhs.port(),
        _ => false,
    }
}

// 1. Define Domain Events
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TodoEvent {
    TodoCreated { title: String },
    TodoCompleted,
    TodoDeleted,
}

impl EventPayload for TodoEvent {
    fn event_type(&self) -> &'static str {
        match self {
            TodoEvent::TodoCreated { .. } => "TodoCreated",
            TodoEvent::TodoCompleted => "TodoCompleted",
            TodoEvent::TodoDeleted => "TodoDeleted",
        }
    }
}

// 2. Define Projection State (Read Model)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TodoItem {
    pub id: String,
    pub title: String,
    pub completed: bool,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct TodoState {
    pub todos: HashMap<String, TodoItem>,
}

// 3. Define Projections

/// Ephemeral Projection: Runs on ALL nodes independently.
/// Used to maintain the API's in-memory Read Model.
pub struct TodoProjection;

#[async_trait]
impl Projection<TodoEvent, TodoState> for TodoProjection {
    fn name(&self) -> &'static str {
        "todo_projection"
    }

    async fn handle(
        &self,
        state: &mut TodoState,
        event: &Event<TodoEvent>,
    ) -> Result<(), ProjectionError> {
        let aggregate_id = event.aggregate_id.clone();

        match &event.payload {
            TodoEvent::TodoCreated { title } => {
                state.todos.insert(
                    aggregate_id.clone(),
                    TodoItem {
                        id: aggregate_id,
                        title: title.clone(),
                        completed: false,
                    },
                );
            }
            TodoEvent::TodoCompleted => {
                if let Some(todo) = state.todos.get_mut(&aggregate_id) {
                    todo.completed = true;
                }
            }
            TodoEvent::TodoDeleted => {
                state.todos.remove(&aggregate_id);
            }
        }
        Ok(())
    }
}

/// Durable Projection: Runs on exactly ONE node across the cluster.
pub struct EmailNotificationProjection;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct EmailState {
    pub count: u32,
}

#[async_trait]
impl Projection<TodoEvent, EmailState> for EmailNotificationProjection {
    fn name(&self) -> &'static str {
        "email_projection_durable"
    }

    async fn handle(
        &self,
        state: &mut EmailState,
        event: &Event<TodoEvent>,
    ) -> Result<(), ProjectionError> {
        if let TodoEvent::TodoCreated { title } = &event.payload {
            state.count += 1;
            println!(
                "📧 [DURABLE PROJECTION] Sending welcome email for Todo: '{}' (Total emails: {})",
                title, state.count
            );
        }
        Ok(())
    }
}

// 4. Distributed Implementations
// Cluster helpers (discovery, mesh, lock manager, stores) are provided by `crate::cluster`.

// 5. Shared Web State
#[derive(Clone)]
struct AppState {
    node_id: Uuid,
    bus: EventBus<TodoEvent>,
    mesh: Arc<TcpPubSub<TodoEvent>>,
    event_store: Arc<FileEventStore>,
    projection_state: Arc<tokio::sync::Mutex<TodoState>>,
    projection_version: Arc<AtomicU64>,
}

// 6. Define API Handlers

async fn get_todos(
    State(state): State<AppState>,
    request: axum::http::Request<axum::body::Body>,
) -> Result<(axum::http::HeaderMap, Json<Vec<TodoItem>>), StatusCode> {
    let version = state.projection_version.load(Ordering::Relaxed);
    let etag = format!("\"{}\"", version);

    let proj = state.projection_state.lock().await;
    let todos: Vec<TodoItem> = proj.todos.values().cloned().collect();

    let mut headers = axum::http::HeaderMap::new();
    headers.insert(axum::http::header::ETAG, etag.parse().unwrap());

    if let Some(if_none_match) = request.headers().get(axum::http::header::IF_NONE_MATCH) {
        if if_none_match.to_str().map_or(false, |v| v == etag) {
            return Ok((headers, Json(todos)));
        }
    }

    Ok((headers, Json(todos)))
}

#[derive(Deserialize)]
struct CreateTodoRequest {
    title: String,
}

async fn create_todo(
    State(state): State<AppState>,
    Json(req): Json<CreateTodoRequest>,
) -> Result<Json<TodoItem>, StatusCode> {
    println!(
        "Node {} handling create_todo for: {}",
        state.node_id, req.title
    );
    let aggregate_id = Uuid::new_v4().to_string();
    let event = Event::new(
        &aggregate_id,
        1,
        TodoEvent::TodoCreated {
            title: req.title.clone(),
        },
    );

    let stored = state.event_store.append(vec![event]).await.map_err(|e| {
        eprintln!("Store error: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    for e in stored {
        let _ = state.bus.publish(e.clone());
        let _ = state.mesh.publish(&e).await;
    }

    let item = TodoItem {
        id: aggregate_id,
        title: req.title,
        completed: false,
    };

    Ok(Json(item))
}

async fn complete_todo(
    Path(id): Path<String>,
    State(state): State<AppState>,
) -> Result<StatusCode, StatusCode> {
    let event = Event::new(&id, 2, TodoEvent::TodoCompleted);

    let stored = state
        .event_store
        .append(vec![event])
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    for e in stored {
        let _ = state.bus.publish(e.clone());
        let _ = state.mesh.publish(&e).await;
    }

    Ok(StatusCode::OK)
}

async fn delete_todo(
    Path(id): Path<String>,
    State(state): State<AppState>,
) -> Result<StatusCode, StatusCode> {
    let event = Event::new(&id, 3, TodoEvent::TodoDeleted);

    let stored = state
        .event_store
        .append(vec![event])
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let es = state.event_store.clone();
    tokio::spawn(async move {
        let rule = CompactionRule::PruneIf(|payload| matches!(payload, TodoEvent::TodoDeleted));
        match EventStore::<TodoEvent>::compact(es.as_ref(), rule).await {
            Ok(removed) if removed > 0 => println!(
                "🧹 [LOG COMPACTION] Removed {} stale events from log",
                removed
            ),
            Err(e) => eprintln!("❌ Log compaction failed: {}", e),
            _ => (),
        }
    });

    for e in stored {
        let _ = state.bus.publish(e.clone());
        let _ = state.mesh.publish(&e).await;
    }

    Ok(StatusCode::OK)
}

// 7. Wiring
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    tracing::info!("Starting Distributed Todo App Node (TCP Mesh)...");

    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "3000".to_string())
        .parse()
        .expect("PORT must be a number");

    let mesh_port: u16 = std::env::var("MESH_PORT")
        .unwrap_or_else(|_| "3001".to_string())
        .parse()
        .expect("MESH_PORT must be a number");

    let host = std::env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let mesh_bind_host = std::env::var("MESH_BIND_HOST").unwrap_or_else(|_| host.clone());
    let mesh_advertise_host = std::env::var("MESH_ADVERTISE_HOST")
        .ok()
        .or_else(|| std::env::var("POD_IP").ok())
        .unwrap_or_else(|| mesh_bind_host.clone());

    let data_dir = std::env::var("DATA_DIR").unwrap_or_else(|_| "./data".to_string());

    let cluster = rust_eventbus::cluster::init_cluster::<TodoEvent>().await?;
    let node_id = cluster.node_id;
    let event_store = cluster.event_store.clone();
    let snapshot_store = cluster.snapshot_store.clone();
    let mesh = cluster.mesh.clone();
    let lock_manager = cluster.lock_manager.clone();
    let mesh_bind_addr = cluster.mesh_bind_addr.clone();
    let mesh_advertised_addr = cluster.mesh_advertised_addr.clone();
    let discovery_for_probe = cluster.discovery.clone();
    let data_dir = cluster.data_dir.clone();

    let listen_addr = format!("{}:{}", host, port);

    tracing::info!("***************************************************");
    tracing::info!("Starting Node ID: {}", node_id);
    tracing::info!("API Listen:      {}", listen_addr);
    tracing::info!("Mesh Listen:     {}", mesh_bind_addr);
    tracing::info!("Mesh Advertise:  {}", mesh_advertised_addr);
    tracing::info!("Storage:         {}", data_dir);
    tracing::info!("***************************************************");

    let bus = EventBus::<TodoEvent>::new(1024);
    let projection_version = cluster.projection_version.clone();

    let ephemeral_actor = EphemeralProjectionActor::new(
        bus.clone(),
        event_store.clone(),
        Arc::new(TodoProjection),
        snapshot_store.clone(),
    )
    .with_version(projection_version.clone());
    let projection_state = ephemeral_actor.get_state();
    tokio::spawn(ephemeral_actor.spawn());

    let durable_actor = DurableProjectionActor::new(
        bus.clone(),
        event_store.clone(),
        Arc::new(EmailNotificationProjection),
        snapshot_store.clone(),
        lock_manager,
        node_id,
    );
    tokio::spawn(durable_actor.spawn());

    let self_mesh_addr = mesh_advertised_addr.clone();
    tokio::spawn(async move {
        loop {
            match discovery_for_probe.discover_nodes().await {
                Ok(nodes) => {
                    let peer_count = nodes
                        .iter()
                        .filter(|n| !same_endpoint(&n.address, &self_mesh_addr))
                        .count();
                    tracing::info!("Discovery probe: {} peer(s) visible", peer_count);
                }
                Err(e) => tracing::warn!("Discovery probe failed: {}", e),
            }

            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    });

    let app_state = AppState {
        node_id,
        bus,
        mesh,
        event_store,
        projection_state,
        projection_version,
    };

    let mesh_for_subscribe = app_state.mesh.clone();
    let bus_for_mesh = app_state.bus.clone();
    let event_store_for_mesh = app_state.event_store.clone();
    tokio::spawn(async move {
        let mut incoming = mesh_for_subscribe.subscribe().await;
        while let Some(result) = incoming.next().await {
            match result {
                Ok(event) => {
                    // Persist the remote event into the local EventStore so the local
                    // projections can rely on a monotonic, local `global_sequence_num`.
                    // Do NOT re-publish to the mesh here to avoid loops.
                    match event_store_for_mesh.append(vec![event.clone()]).await {
                        Ok(stored) => {
                            for e in stored {
                                let _ = bus_for_mesh.publish(e);
                            }
                        }
                        Err(e) => tracing::warn!("Failed to persist mesh event: {}", e),
                    }
                }
                Err(e) => tracing::warn!("Mesh subscribe error: {}", e),
            }
        }
    });

    let app = Router::new()
        .route("/todos", axum::routing::get(get_todos).post(create_todo))
        .route("/todos/{id}/complete", put(complete_todo))
        .route("/todos/{id}", delete(delete_todo))
        .with_state(app_state);

    let addr: SocketAddr = listen_addr.parse()?;
    let listener = TcpListener::bind(addr).await?;

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("Failed to install SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("Received SIGINT, shutting down...");
                }
                _ = sigterm.recv() => {
                    tracing::info!("Received SIGTERM, shutting down...");
                }
            }
        })
        .await?;

    Ok(())
}
