use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, put},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tokio::net::TcpListener;
use uuid::Uuid;

use rust_eventbus::{
    bus::EventBus,
    event::{Event, EventPayload},
    projection::{Projection, ProjectionActor, ProjectionError},
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

// 3. Define Projection Logic
pub struct TodoProjection;

#[async_trait::async_trait]
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

// 4. Define Shared Web State
#[derive(Clone)]
struct AppState {
    bus: EventBus<TodoEvent>,
    event_store: Arc<FileEventStore>,
    // Web endpoints can quickly read the latest materialized view
    projection_state: Arc<tokio::sync::Mutex<TodoState>>,
}

// 5. Define API Handlers

async fn get_todos(State(state): State<AppState>) -> Json<Vec<TodoItem>> {
    let proj = state.projection_state.lock().await;
    let mut todos: Vec<TodoItem> = proj.todos.values().cloned().collect();
    // Sort deterministically for consistent API responses
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
    
    // In a full DDD app we would load aggregate, apply command, get sequence num.
    // Here we use 1 for creation.
    let event = Event::new(&aggregate_id, 1, TodoEvent::TodoCreated { title: req.title.clone() });
    
    // Persist
    let stored = state.event_store.append(vec![event]).await.map_err(|e| {
        eprintln!("Store error: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    
    // Publish
    for e in stored {
        let _ = state.bus.publish(e);
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
    // We optimistically use sequence 2 for example purposes.
    let event = Event::new(&id, 2, TodoEvent::TodoCompleted);
    
    let stored = state.event_store.append(vec![event]).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    for e in stored {
        let _ = state.bus.publish(e);
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
        let _ = state.bus.publish(e);
    }
    
    Ok(StatusCode::OK)
}

// 6. Wiring
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting Todo Web API...");

    // Setup Storage
    let event_store = Arc::new(FileEventStore::new("todo_events.bin").await?);
    let snapshot_store = Arc::new(FileSnapshotStore::new("todo_snapshots").await?);
    
    // Setup Event Bus
    let bus = EventBus::<TodoEvent>::new(1024);
    
    // Setup Projection Actor
    let projection = Arc::new(TodoProjection);
    let projection_actor = ProjectionActor::new(
        bus.clone(),
        event_store.clone(),
        projection,
        snapshot_store.clone(),
    );
    
    // Expose state reference to Axum state
    let projection_state = projection_actor.get_state();
    
    // Spawn actor in background
    let _projection_handle = projection_actor.spawn().await;
    
    // Start Web Server
    let app_state = AppState {
        bus,
        event_store,
        projection_state,
    };
    
    let app = Router::new()
        .route("/todos", axum::routing::get(get_todos).post(create_todo))
        .route("/todos/{id}/complete", put(complete_todo))
        .route("/todos/{id}", delete(delete_todo))
        .with_state(app_state);
        
    let listener = TcpListener::bind("127.0.0.1:3000").await?;
    println!("Listening on http://{}", listener.local_addr()?);
    
    axum::serve(listener, app).await?;
    
    Ok(())
}
