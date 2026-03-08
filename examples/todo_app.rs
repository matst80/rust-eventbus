use async_trait::async_trait;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, put},
    Json, Router,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;
use uuid::Uuid;

use rust_eventbus::{
    bus::EventBus,
    distributed::{DistributedPubSub, DnsNodeDiscovery, EnvironmentNodeDiscovery, LockError, ProjectionLockManager, TcpPubSub},
    event::{Event, EventPayload},
    projection::{DurableProjectionActor, EphemeralProjectionActor, Projection, ProjectionError},
    store::{CompactionRule, EventStore, FileEventStore, FileSnapshotStore},
};

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
            println!("📧 [DURABLE PROJECTION] Sending welcome email for Todo: '{}' (Total emails: {})", title, state.count);
        }
        Ok(())
    }
}

// 4. Distributed Implementations

pub struct MockLockManager;

#[async_trait]
impl ProjectionLockManager for MockLockManager {
    async fn acquire_lock(&self, _projection_name: &str, _node_id: &Uuid) -> Result<bool, LockError> {
        Ok(true)
    }

    async fn keep_alive(&self, _projection_name: &str, _node_id: &Uuid) -> Result<(), LockError> {
        Ok(())
    }

    async fn release_lock(&self, _projection_name: &str, _node_id: &Uuid) -> Result<(), LockError> {
        Ok(())
    }
}

// 5. Shared Web State
#[derive(Clone)]
struct AppState {
    node_id: Uuid,
    bus: EventBus<TodoEvent>,
    mesh: Arc<TcpPubSub<TodoEvent>>,
    event_store: Arc<FileEventStore>,
    projection_state: Arc<tokio::sync::Mutex<TodoState>>,
}

// 6. Define API Handlers

async fn get_todos(State(state): State<AppState>) -> Json<Vec<TodoItem>> {
    let proj = state.projection_state.lock().await;
    let mut todos: Vec<TodoItem> = proj.todos.values().cloned().collect();
    todos.sort_by(|a, b| a.id.cmp(&b.id)); 
    Json(todos)
}

#[derive(Deserialize)]
struct CreateTodoRequest {
    title: String,
}

async fn create_todo(
    State(state): State<AppState>,
    Json(req): Json<CreateTodoRequest>,
) -> Result<Json<TodoItem>, StatusCode> {
    println!("Node {} handling create_todo for: {}", state.node_id, req.title);
    let aggregate_id = Uuid::new_v4().to_string();
    let event = Event::new(&aggregate_id, 1, TodoEvent::TodoCreated { title: req.title.clone() });
    
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
    
    let stored = state.event_store.append(vec![event]).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
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
    
    let stored = state.event_store.append(vec![event]).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    
    let es = state.event_store.clone();
    tokio::spawn(async move {
        let rule = CompactionRule::PruneIf(|payload| matches!(payload, TodoEvent::TodoDeleted));
        match EventStore::<TodoEvent>::compact(es.as_ref(), rule).await {
            Ok(removed) if removed > 0 => println!("🧹 [LOG COMPACTION] Removed {} stale events from log", removed),
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
    println!("🚀 Starting Distributed Todo App Node (TCP Mesh)...");

    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "3000".to_string())
        .parse()
        .expect("PORT must be a number");
    
    let host = std::env::var("HOST")
        .unwrap_or_else(|_| "0.0.0.0".to_string());

    let data_dir = std::env::var("DATA_DIR")
        .unwrap_or_else(|_| "./data".to_string());
    
    let node_id_str = std::env::var("NODE_ID").ok();
    let node_id = node_id_str
        .and_then(|s| Uuid::parse_str(&s).ok())
        .unwrap_or_else(Uuid::new_v4);

    // Node Discovery: Environment (Static) or DNS (Kubernetes Headless)
    let peer_discovery: Arc<dyn rust_eventbus::distributed::NodeDiscovery> = 
        if let Ok(dns_query) = std::env::var("DNS_QUERY") {
            println!("Discovery: DNS Query ({})", dns_query);
            Arc::new(DnsNodeDiscovery::new(dns_query, port))
        } else {
            println!("Discovery: Environment (PEERS)");
            Arc::new(EnvironmentNodeDiscovery::new())
        };

    tokio::fs::create_dir_all(&data_dir).await?;

    let event_store_path = format!("{}/todo_events.bin", data_dir);
    let snapshot_store_path = format!("{}/todo_snapshots", data_dir);

    let event_store = Arc::new(FileEventStore::new(&event_store_path).await?);
    let snapshot_store = Arc::new(FileSnapshotStore::new(&snapshot_store_path).await?);

    let listen_addr = format!("{}:{}", host, port);
    let mesh = Arc::new(TcpPubSub::new(node_id, listen_addr.clone(), peer_discovery));

    println!("Node ID:   {}", node_id);
    println!("Listening: {}", listen_addr);
    println!("Storage:   {}", data_dir);

    let bus = EventBus::<TodoEvent>::new(1024);
    let mock_lock_manager = Arc::new(MockLockManager);

    let mesh_clone = mesh.clone();
    let bus_clone = bus.clone();
    tokio::spawn(async move {
        let mut stream = mesh_clone.subscribe().await;
        while let Some(item) = stream.next().await {
            if let Ok(event) = item {
                let _ = bus_clone.publish(event);
            }
        }
    });

    let ephemeral_actor = EphemeralProjectionActor::new(
        bus.clone(),
        event_store.clone(),
        Arc::new(TodoProjection),
        snapshot_store.clone(),
    );
    let projection_state = ephemeral_actor.get_state();
    tokio::spawn(ephemeral_actor.spawn());

    let durable_actor = DurableProjectionActor::new(
        bus.clone(),
        event_store.clone(),
        Arc::new(EmailNotificationProjection),
        snapshot_store.clone(),
        mock_lock_manager,
        node_id,
    );
    tokio::spawn(durable_actor.spawn());

    let app_state = AppState {
        node_id,
        bus,
        mesh,
        event_store,
        projection_state,
    };

    let app = Router::new()
        .route("/todos", axum::routing::get(get_todos).post(create_todo))
        .route("/todos/{id}/complete", put(complete_todo))
        .route("/todos/{id}", delete(delete_todo))
        .with_state(app_state);

    let addr: SocketAddr = listen_addr.parse()?;
    let listener = TcpListener::bind(addr).await?;
    
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c().await.ok();
            println!("\nShutting down gracefully...");
        })
        .await?;

    Ok(())
}
