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
    distributed::{DistributedError, DistributedPubSub, LockError, ProjectionLockManager},
    event::{Event, EventPayload},
    projection::{DurableProjectionActor, EphemeralProjectionActor, Projection, ProjectionError},
    store::{EventStore, FileEventStore, FileSnapshotStore},
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
/// e.g. Sending emails, integrating with external systems.
pub struct EmailNotificationProjection;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct EmailState {
    pub count: u32, // Just tracking something simple for snapshotting
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

// 4. Mock Distributed Implementations

#[derive(Clone)]
pub struct MockCluster {
    pub network: tokio::sync::broadcast::Sender<(Uuid, Event<TodoEvent>)>,
    pub lock_state: Arc<tokio::sync::Mutex<Option<Uuid>>>,
}

pub struct NodeMesh {
    node_id: Uuid,
    cluster: MockCluster,
}

#[async_trait]
impl DistributedPubSub<TodoEvent> for NodeMesh {
    async fn publish(&self, event: &Event<TodoEvent>) -> Result<(), DistributedError> {
        let _ = self.cluster.network.send((self.node_id, event.clone()));
        Ok(())
    }

    async fn subscribe(&self) -> futures::stream::BoxStream<'static, Result<Event<TodoEvent>, DistributedError>> {
        let rx = self.cluster.network.subscribe();
        let my_id = self.node_id;
        
        let stream = futures::stream::unfold(rx, move |mut rx| async move {
            loop {
                match rx.recv().await {
                    Ok((sender_id, event)) => {
                        if sender_id != my_id {
                            return Some((Ok(event), rx));
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        return None;
                    }
                }
            }
        });
            
        Box::pin(stream)
    }
}

pub struct MockLockManager {
    cluster: MockCluster,
}

#[async_trait]
impl ProjectionLockManager for MockLockManager {
    async fn acquire_lock(&self, _projection_name: &str, node_id: &Uuid) -> Result<bool, LockError> {
        let mut l = self.cluster.lock_state.lock().await;
        if l.is_none() || *l == Some(*node_id) {
            *l = Some(*node_id);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn keep_alive(&self, _projection_name: &str, node_id: &Uuid) -> Result<(), LockError> {
        let l = self.cluster.lock_state.lock().await;
        if *l == Some(*node_id) {
            Ok(())
        } else {
            Err(LockError::AlreadyHeld)
        }
    }

    async fn release_lock(&self, _projection_name: &str, node_id: &Uuid) -> Result<(), LockError> {
        let mut l = self.cluster.lock_state.lock().await;
        if *l == Some(*node_id) {
            *l = None;
        }
        Ok(())
    }
}


// 5. Shared Web State
#[derive(Clone)]
struct AppState {
    node_id: Uuid,
    bus: EventBus<TodoEvent>,
    mesh: Arc<NodeMesh>,
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
    let aggregate_id = Uuid::new_v4().to_string();
    let event = Event::new(&aggregate_id, 1, TodoEvent::TodoCreated { title: req.title.clone() });
    
    // Persist to Shared EventStore
    let stored = state.event_store.append(vec![event]).await.map_err(|e| {
        eprintln!("Store error: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    
    // Publish locally and to cluster
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
    for e in stored {
        let _ = state.bus.publish(e.clone());
        let _ = state.mesh.publish(&e).await;
    }
    
    Ok(StatusCode::OK)
}

// 7. Wiring
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting Distributed Todo App Cluster...");

    let (mesh_tx, _) = tokio::sync::broadcast::channel(1024);
    let mock_cluster = MockCluster {
        network: mesh_tx,
        lock_state: Arc::new(tokio::sync::Mutex::new(None)),
    };

    // Shared storage!
    let event_store = Arc::new(FileEventStore::new("todo_events_cluster.bin").await?);
    let snapshot_store = Arc::new(FileSnapshotStore::new("todo_snapshots_cluster").await?);

    // Spawn Node 1
    spawn_node(
        3000, 
        mock_cluster.clone(), 
        event_store.clone(), 
        snapshot_store.clone()
    ).await;
    
    // Spawn Node 2
    spawn_node(
        3001, 
        mock_cluster.clone(), 
        event_store.clone(), 
        snapshot_store.clone()
    ).await;

    // Keep main thread alive
    tokio::signal::ctrl_c().await?;
    println!("Shutting down...");
    Ok(())
}

async fn spawn_node(
    port: u16,
    mock_cluster: MockCluster,
    event_store: Arc<FileEventStore>,
    snapshot_store: Arc<FileSnapshotStore>,
) {
    let node_id = Uuid::new_v4();
    println!("🚀 Starting Node {} on port {}", node_id, port);

    let bus = EventBus::<TodoEvent>::new(1024);
    
    let mesh = Arc::new(NodeMesh {
        node_id,
        cluster: mock_cluster.clone(),
    });

    let mock_lock_manager = Arc::new(MockLockManager { cluster: mock_cluster });

    // 1. Setup Background task to forward distributed events -> local bus
    let mesh_clone = mesh.clone();
    let bus_clone = bus.clone();
    tokio::spawn(async move {
        let mut stream = mesh_clone.subscribe().await;
        while let Some(Ok(event)) = stream.next().await {
            // Arrived from network -> broadcast locally
            let _ = bus_clone.publish(event);
        }
    });

    // 2. Setup Ephemeral Projection (Runs on all nodes)
    let ephemeral_actor = EphemeralProjectionActor::new(
        bus.clone(),
        event_store.clone(),
        Arc::new(TodoProjection),
        snapshot_store.clone(),
    );
    let projection_state = ephemeral_actor.get_state();
    tokio::spawn(ephemeral_actor.spawn());

    // 3. Setup Durable Projection (Runs on only ONE node)
    let durable_actor = DurableProjectionActor::new(
        bus.clone(),
        event_store.clone(),
        Arc::new(EmailNotificationProjection),
        snapshot_store.clone(),
        mock_lock_manager,
        node_id,
    );
    tokio::spawn(durable_actor.spawn());

    // 4. API Server Setup
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

    tokio::spawn(async move {
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        let listener = TcpListener::bind(addr).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    });
}
