use rust_eventbus::{
    bus::EventBus,
    event::{Event, EventPayload},
    projection::{EphemeralProjectionActor, Projection, ProjectionError},
    store::{EventStore, FileEventStore, FileSnapshotStore, SnapshotStore},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct TestEvent {
    val: String,
}

impl EventPayload for TestEvent {
    fn event_type(&self) -> &'static str {
        "TestEvent"
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct TestState {
    received: Vec<String>,
}

struct TestProjection;

#[async_trait::async_trait]
impl Projection<TestEvent, TestState> for TestProjection {
    fn name(&self) -> &'static str {
        "test_proj"
    }

    async fn handle(
        &self,
        state: &mut TestState,
        event: &Event<TestEvent>,
    ) -> Result<(), ProjectionError> {
        state.received.push(event.payload.val.clone());
        Ok(())
    }
}

#[tokio::test]
async fn test_projection_recovery_and_no_regression() {
    let temp_dir = std::env::temp_dir().join(format!("proj_test_{}", uuid::Uuid::new_v4()));
    tokio::fs::create_dir_all(&temp_dir).await.unwrap();

    let event_store_path = temp_dir.join("events.bin");
    let snapshot_dir = temp_dir.join("snapshots");

    let event_store = Arc::new(FileEventStore::new(&event_store_path).await.unwrap());
    let snapshot_store = Arc::new(FileSnapshotStore::new(&snapshot_dir).await.unwrap());
    let bus = EventBus::<TestEvent>::new(1024);

    // 1. Add some initial events
    let e1 = Event::new("a", 1, TestEvent { val: "e1".into() });
    let e2 = Event::new("a", 2, TestEvent { val: "e2".into() });
    event_store.append(vec![e1, e2]).await.unwrap();

    // 2. Start projection, let it catch up and save snapshot
    let actor = EphemeralProjectionActor::new(
        bus.clone(),
        event_store.clone(),
        Arc::new(TestProjection),
        snapshot_store.clone(),
    )
    .with_snapshot_interval(1); // snapshot every event

    let state = actor.get_state();
    let handle = actor.spawn().await;

    // Wait for catch-up
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    {
        let s = state.read().await;
        assert_eq!(s.received.len(), 2);
        assert_eq!(s.received[0], "e1");
        assert_eq!(s.received[1], "e2");
    }

    // 3. Append an event "offline" (simulates it being written but not yet processed by live actors)
    let e3 = Event::new("a", 3, TestEvent { val: "e3".into() });
    event_store.append(vec![e3]).await.unwrap();
    
    // 4. Start a new actor (simulates app restart)
    // We'll use a new bus to be sure we are testing catch-up from file
    let bus2 = EventBus::<TestEvent>::new(1024);

    let actor2 = EphemeralProjectionActor::new(
        bus2.clone(),
        event_store.clone(),
        Arc::new(TestProjection),
        snapshot_store.clone(),
    ).with_snapshot_interval(1);

    let state2 = actor2.get_state();
    actor2.spawn().await;

    // Give it time to load snapshot and catch up
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    {
        let s = state2.read().await;
        // Should have e1, e2 (from snapshot/catch-up) and e3 (from file catch-up)
        assert_eq!(s.received.len(), 3, "Actor 2 should have 3 events after catch-up");
        assert_eq!(s.received[2], "e3");
    }

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn test_projection_no_sequence_regression() {
    let temp_dir = std::env::temp_dir().join(format!("proj_reg_test_{}", uuid::Uuid::new_v4()));
    tokio::fs::create_dir_all(&temp_dir).await.unwrap();

    let event_store_path = temp_dir.join("events.bin");
    let snapshot_dir = temp_dir.join("snapshots");

    let event_store = Arc::new(FileEventStore::new(&event_store_path).await.unwrap());
    let snapshot_store = Arc::new(FileSnapshotStore::new(&snapshot_dir).await.unwrap());
    let bus = EventBus::<TestEvent>::new(1024);

    // 1. Manually create out-order events in file
    // We'll use bincode to write manually since FileEventStore enforces monotonicity
    {
        let mut file = std::fs::OpenOptions::new().create(true).write(true).open(&event_store_path).unwrap();
        
        let events = vec![
            Event {
                id: uuid::Uuid::new_v4(),
                aggregate_id: "a".into(),
                sequence_num: 1,
                global_sequence_num: 2, // seq 2
                timestamp: chrono::Utc::now(),
                payload: TestEvent { val: "e2".into() },
            },
            Event {
                id: uuid::Uuid::new_v4(),
                aggregate_id: "a".into(),
                sequence_num: 2,
                global_sequence_num: 1, // Regression! seq 1 arrives after seq 2
                timestamp: chrono::Utc::now(),
                payload: TestEvent { val: "e1".into() },
            }
        ];

        for e in events {
            let bytes = bincode::serialize(&e).unwrap();
            let len = bytes.len() as u32;
            use std::io::Write;
            file.write_all(&len.to_le_bytes()).unwrap();
            file.write_all(&bytes).unwrap();
        }
        file.sync_all().unwrap();
    }

    // Start projection. It should process seq 2, then see seq 1 and IGNORE it (max check)
    let actor = EphemeralProjectionActor::new(
        bus.clone(),
        event_store.clone(),
        Arc::new(TestProjection),
        snapshot_store.clone(),
    );

    let state = actor.get_state();
    actor.spawn().await;

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    {
        let s = state.read().await;
        // Even though seq 1 was later in the file, it shouldn't have corrupted current_seq
        // Wait, TestProjection pushes to Vec, so it WILL have both events in the Vec
        // BUT current_seq should remain at 2.
        assert_eq!(s.received.len(), 2);
        assert_eq!(s.received[0], "e2");
        assert_eq!(s.received[1], "e1");
    }

    // Now restart. It should load snapshot at seq 2!
    // Wait, catch-up doesn't save snapshot unless events_since_snapshot > 0.
    // In our case it is 2. So it should save at 2 (max of 2 and 1).

    let actor2 = EphemeralProjectionActor::new(
        bus.clone(),
        event_store.clone(),
        Arc::new(TestProjection),
        snapshot_store.clone(),
    );
    let state2 = actor2.get_state();
    actor2.spawn().await;

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    {
        let s = state2.read().await;
        // Should have loaded snapshot at 2.
        // Catch-up from 3 should find nothing.
        // Total should be 2 events.
        assert_eq!(s.received.len(), 2);
    }

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}
