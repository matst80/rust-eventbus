use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

use rust_eventbus::{
    bus::EventBus,
    event::{Event, EventPayload},
    projection::{Projection, ProjectionActor, ProjectionError},
    store::{EventStore, FileEventStore, FileSnapshotStore, SnapshotStore},
};

// 1. Define our Domain Events
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AppEvent {
    UserRegistered { username: String },
    UserRenamed { old_name: String, new_name: String },
}

impl EventPayload for AppEvent {
    fn event_type(&self) -> &'static str {
        match self {
            AppEvent::UserRegistered { .. } => "UserRegistered",
            AppEvent::UserRenamed { .. } => "UserRenamed",
        }
    }
}

// 2. Define our Projection State
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct UserCountState {
    pub total_users: u64,
}

// 3. Define our Projection Logic
pub struct UserCountProjection;

#[async_trait]
impl Projection<AppEvent, UserCountState> for UserCountProjection {
    fn name(&self) -> &'static str {
        "user_count_projection"
    }

    async fn handle(
        &self,
        state: &mut UserCountState,
        event: &Event<AppEvent>,
    ) -> Result<(), ProjectionError> {
        match &event.payload {
            AppEvent::UserRegistered { username } => {
                println!("Projection: New user registered: {}", username);
                state.total_users += 1;
            }
            AppEvent::UserRenamed { old_name, new_name } => {
                println!("Projection: User renamed from {} to {}", old_name, new_name);
                // Renaming doesn't change total count
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting Simple App Example...");

    // Setup Storage
    let event_store = Arc::new(FileEventStore::new("events.bin").await?);
    let snapshot_store = Arc::new(FileSnapshotStore::new("snapshots").await?);

    // Setup Event Bus
    let bus = EventBus::<AppEvent>::new(1024);

    // Setup Projection Manager Loop
    // The projection will run in its own fully independent Tokio task, preventing global locking!
    let projection = Arc::new(UserCountProjection);
    let projection_actor = ProjectionActor::new(
        bus.clone(),
        event_store.clone(),
        projection,
        snapshot_store.clone(), // So the actor can save the state
    );

    // Spawn the actor in the background
    let _projection_handle = projection_actor.spawn().await;

    // Simulate appending new events programmatically
    let e1 = Event::new(
        "user-1",
        1,
        AppEvent::UserRegistered {
            username: "alice".to_string(),
        },
    );
    let e2 = Event::new(
        "user-2",
        1,
        AppEvent::UserRegistered {
            username: "bob".to_string(),
        },
    );
    let e3 = Event::new(
        "user-1",
        2,
        AppEvent::UserRenamed {
            old_name: "alice".to_string(),
            new_name: "alicia".to_string(),
        },
    );

    println!("Appending events to store...");
    let stored_events = event_store
        .append(vec![e1, e2, e3])
        .await?;

    println!("Publishing events to bus...");
    // Publish so real-time listeners receive it
    for event in stored_events {
        let _ = bus.publish(event);
    }

    // Give projection background task 100ms to process
    sleep(Duration::from_millis(100)).await;

    // Verify state was saved to snapshot
    let snapshot: Option<(u64, UserCountState)> =
        snapshot_store.load("user_count_projection").await?;
    if let Some((seq, state)) = snapshot {
        println!(
            "Final State from Snapshot (Seq {}): {} users",
            seq, state.total_users
        );
    } else {
        println!("Failed to load snapshot!");
    }

    // Cleanup generated files for example cleanliness (optional)
    let _ = tokio::fs::remove_file("events.bin").await;
    let _ = tokio::fs::remove_dir_all("snapshots").await;

    println!("Example complete safely!");
    Ok(())
}
