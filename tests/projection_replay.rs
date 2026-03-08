use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

use rust_eventbus::{
    bus::EventBus,
    event::{Event, EventPayload},
    projection::{EphemeralProjectionActor, Projection, ProjectionError},
    store::{EventStore, FileEventStore, FileSnapshotStore, SnapshotStore},
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TestEvent {
    Increment,
    Decrement,
}

impl EventPayload for TestEvent {
    fn event_type(&self) -> &'static str {
        match self {
            TestEvent::Increment => "Increment",
            TestEvent::Decrement => "Decrement",
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct CounterState {
    pub count: i64,
}

pub struct CounterProjection;

#[async_trait]
impl Projection<TestEvent, CounterState> for CounterProjection {
    fn name(&self) -> &'static str {
        "counter_projection"
    }

    async fn handle(
        &self,
        state: &mut CounterState,
        event: &Event<TestEvent>,
    ) -> Result<(), ProjectionError> {
        match &event.payload {
            TestEvent::Increment => state.count += 1,
            TestEvent::Decrement => state.count -= 1,
        }
        Ok(())
    }
}

fn temp_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "{}_{}",
        name,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros()
    ))
}

#[tokio::test]
async fn test_projection_replay() {
    let event_path = temp_path("replay_events");
    let snap_path = temp_path("replay_snaps");

    let event_store = Arc::new(FileEventStore::new(&event_path).await.unwrap());
    let snapshot_store = Arc::new(FileSnapshotStore::new(&snap_path).await.unwrap());
    let bus = EventBus::<TestEvent>::new(1024);

    // Append some events
    let events = vec![
        Event::new("a", 1, TestEvent::Increment),
        Event::new("a", 2, TestEvent::Increment),
        Event::new("a", 3, TestEvent::Increment),
    ];
    let stored = event_store.append(events).await.unwrap();

    let actor = EphemeralProjectionActor::new(
        bus.clone(),
        event_store.clone(),
        Arc::new(CounterProjection),
        snapshot_store.clone(),
    );
    let state = actor.get_state();
    let handle = actor.get_handle();
    let _join = actor.spawn().await;

    // Publish events to bus so the projection processes them
    for e in &stored {
        let _ = bus.publish(e.clone());
    }
    sleep(Duration::from_millis(200)).await;

    // Verify initial state
    {
        let s = state.lock().await;
        assert_eq!(s.count, 3, "After 3 increments, count should be 3");
    }

    // Trigger replay — should rebuild from event store
    handle.replay();
    sleep(Duration::from_millis(200)).await;

    {
        let s = state.lock().await;
        assert_eq!(s.count, 3, "After replay, count should still be 3");
    }

    // Cleanup
    let _ = tokio::fs::remove_file(&event_path).await;
    let _ = tokio::fs::remove_dir_all(&snap_path).await;
}

#[tokio::test]
async fn test_projection_reset() {
    let event_path = temp_path("reset_events");
    let snap_path = temp_path("reset_snaps");

    let event_store = Arc::new(FileEventStore::new(&event_path).await.unwrap());
    let snapshot_store = Arc::new(FileSnapshotStore::new(&snap_path).await.unwrap());
    let bus = EventBus::<TestEvent>::new(1024);

    let events = vec![
        Event::new("a", 1, TestEvent::Increment),
        Event::new("a", 2, TestEvent::Increment),
    ];
    let stored = event_store.append(events).await.unwrap();

    let actor = EphemeralProjectionActor::new(
        bus.clone(),
        event_store.clone(),
        Arc::new(CounterProjection),
        snapshot_store.clone(),
    );
    let state = actor.get_state();
    let handle = actor.get_handle();
    let _join = actor.spawn().await;

    for e in &stored {
        let _ = bus.publish(e.clone());
    }
    sleep(Duration::from_millis(200)).await;

    {
        let s = state.lock().await;
        assert_eq!(s.count, 2);
    }

    // Verify snapshot was saved
    let snap: Option<(u64, CounterState)> =
        snapshot_store.load("counter_projection").await.unwrap();
    assert!(snap.is_some(), "Snapshot should exist before reset");

    // Reset — deletes snapshot and replays
    handle.reset();
    sleep(Duration::from_millis(200)).await;

    {
        let s = state.lock().await;
        assert_eq!(s.count, 2, "After reset+replay, count should still be 2");
    }

    // Cleanup
    let _ = tokio::fs::remove_file(&event_path).await;
    let _ = tokio::fs::remove_dir_all(&snap_path).await;
}

#[tokio::test]
async fn test_snapshot_interval() {
    let event_path = temp_path("interval_events");
    let snap_path = temp_path("interval_snaps");

    let event_store = Arc::new(FileEventStore::new(&event_path).await.unwrap());
    let snapshot_store = Arc::new(FileSnapshotStore::new(&snap_path).await.unwrap());
    let bus = EventBus::<TestEvent>::new(1024);

    // Append 5 events
    let events = vec![
        Event::new("a", 1, TestEvent::Increment),
        Event::new("a", 2, TestEvent::Increment),
        Event::new("a", 3, TestEvent::Increment),
        Event::new("a", 4, TestEvent::Increment),
        Event::new("a", 5, TestEvent::Increment),
    ];
    let stored = event_store.append(events).await.unwrap();

    let actor = EphemeralProjectionActor::new(
        bus.clone(),
        event_store.clone(),
        Arc::new(CounterProjection),
        snapshot_store.clone(),
    )
    .with_snapshot_interval(100); // Very high interval — should still save after catch-up flush

    let state = actor.get_state();
    let _join = actor.spawn().await;

    for e in &stored {
        let _ = bus.publish(e.clone());
    }
    sleep(Duration::from_millis(200)).await;

    {
        let s = state.lock().await;
        assert_eq!(s.count, 5);
    }

    // Even with high interval, the end-of-catchup flush should have saved
    let snap: Option<(u64, CounterState)> =
        snapshot_store.load("counter_projection").await.unwrap();
    assert!(
        snap.is_some(),
        "Snapshot should be saved after catch-up flush"
    );

    let _ = tokio::fs::remove_file(&event_path).await;
    let _ = tokio::fs::remove_dir_all(&snap_path).await;
}
