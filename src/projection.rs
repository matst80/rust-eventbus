use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use thiserror::Error;
use tokio::sync::Mutex;

use crate::{
    bus::EventBus,
    distributed::ProjectionLockManager,
    event::{Event, EventPayload},
    store::{EventStore, SnapshotStore},
};
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum ProjectionError {
    #[error("Snapshot store error: {0}")]
    Store(#[from] crate::store::StoreError),
    #[error("Handler error: {0}")]
    Handler(String),
}

/// A definition of a stateful Projection that builds application state from the event stream.
#[async_trait]
pub trait Projection<E: EventPayload, S>: Send + Sync + 'static {
    /// Return the unique name of this projection (used for snapshot tracking).
    fn name(&self) -> &'static str;

    /// Process a single event and mutate the state `S`.
    async fn handle(&self, state: &mut S, event: &Event<E>) -> Result<(), ProjectionError>;
}

/// Orchestrates an ephemeral projection (e.g., in-memory cache, websocket fanout).
/// It subscribes to the EventBus, catches up, and processes events in a standalone task.
/// It runs on ALL nodes simultaneously.
pub struct EphemeralProjectionActor<E: EventPayload, S, P, SS, ES> {
    bus: EventBus<E>,
    event_store: Arc<ES>,
    projection: Arc<P>,
    snapshot_store: Arc<SS>,
    state: Arc<Mutex<S>>,
    _marker: std::marker::PhantomData<E>,
}

impl<E, S, P, SS, ES> EphemeralProjectionActor<E, S, P, SS, ES>
where
    E: EventPayload,
    S: Default + Send + Sync + serde::Serialize + serde::de::DeserializeOwned + 'static,
    P: Projection<E, S>,
    SS: SnapshotStore<S>,
    ES: EventStore<E>,
{
    pub fn new(
        bus: EventBus<E>,
        event_store: Arc<ES>,
        projection: Arc<P>,
        snapshot_store: Arc<SS>,
    ) -> Self {
        Self {
            bus,
            event_store,
            projection,
            snapshot_store,
            state: Arc::new(Mutex::new(S::default())),
            _marker: std::marker::PhantomData,
        }
    }

    /// Returns a reference-counted lock to the underlying projection state.
    /// This is useful for web servers or APIs to query the current read model.
    pub fn get_state(&self) -> Arc<Mutex<S>> {
        self.state.clone()
    }

    /// Spawns the projection actor loop into the tokio runtime.
    /// This resolves the massive `Mutex` contention issue from the Go version by isolating
    /// each projection to its own concurrent task loop.
    pub async fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            // 1. Load latest state from snapshot initialization
            let mut current_seq = 0;
            if let Ok(Some((seq, loaded_state))) =
                self.snapshot_store.load(self.projection.name()).await
            {
                let mut state_lock = self.state.lock().await;
                *state_lock = loaded_state;
                current_seq = seq;
            }

            loop {
                // 2. Subscribe to real-time events immediately to buffer incoming events
                // while we catch up from historical logs.
                let mut rx = self.bus.subscribe();

                // 3. Catch up missing events from EventStore
                let mut stream = self.event_store.read_all_from(current_seq + 1);
                while let Some(Ok(event)) = stream.next().await {
                    let mut state_lock = self.state.lock().await;
                    if let Ok(()) = self.projection.handle(&mut state_lock, &event).await {
                        current_seq = event.global_sequence_num;
                        // Optional: For bulk loads it's faster to save snapshots periodically instead of every time.
                        if let Err(e) = self
                            .snapshot_store
                            .save(self.projection.name(), current_seq, &*state_lock)
                            .await
                        {
                            eprintln!("Failed to save snapshot for {}: {}", self.projection.name(), e);
                        }
                    }
                }

                // 4. Start processing real-time events from the bus
                let mut lagged = false;
                while !lagged {
                    match rx.recv().await {
                        Ok(event) => {
                            // Ignore events we already processed during the catch-up phase
                            if event.global_sequence_num <= current_seq && event.global_sequence_num != 0 {
                                continue;
                            }

                            let mut state_lock = self.state.lock().await;
                            match self.projection.handle(&mut state_lock, &event).await {
                                Ok(()) => {
                                    current_seq = event.global_sequence_num;
                                    if let Err(e) = self
                                        .snapshot_store
                                        .save(self.projection.name(), current_seq, &*state_lock)
                                        .await
                                    {
                                        eprintln!("Failed to save snapshot for {}: {}", self.projection.name(), e);
                                    }
                                }
                                Err(e) => {
                                    // Normally you would integrate with `tracing` crate here
                                    eprintln!(
                                        "Projection {} failed to process event: {}",
                                        self.projection.name(),
                                        e
                                    );
                                }
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                            eprintln!("Projection {} lagged by {} events, resyncing...", self.projection.name(), skipped);
                            lagged = true; // Break inner loop to resubscribe and catch up
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            return; // Bus closed gracefully, stop projection
                        }
                    }
                }
            }
        })
    }
}

/// Orchestrates a durable projection (e.g., shared relational database, robust read-model).
/// Only ONE node holding the lock will actively process events.
pub struct DurableProjectionActor<E: EventPayload, S, P, SS, ES, LM> {
    bus: EventBus<E>,
    event_store: Arc<ES>,
    projection: Arc<P>,
    snapshot_store: Arc<SS>,
    lock_manager: Arc<LM>,
    node_id: Uuid,
    state: Arc<Mutex<S>>,
    _marker: std::marker::PhantomData<E>,
}

impl<E, S, P, SS, ES, LM> DurableProjectionActor<E, S, P, SS, ES, LM>
where
    E: EventPayload,
    S: Default + Send + Sync + serde::Serialize + serde::de::DeserializeOwned + 'static,
    P: Projection<E, S>,
    SS: SnapshotStore<S>,
    ES: EventStore<E>,
    LM: ProjectionLockManager,
{
    pub fn new(
        bus: EventBus<E>,
        event_store: Arc<ES>,
        projection: Arc<P>,
        snapshot_store: Arc<SS>,
        lock_manager: Arc<LM>,
        node_id: Uuid,
    ) -> Self {
        Self {
            bus,
            event_store,
            projection,
            snapshot_store,
            lock_manager,
            node_id,
            state: Arc::new(Mutex::new(S::default())),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn get_state(&self) -> Arc<Mutex<S>> {
        self.state.clone()
    }

    pub async fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                // 1. Try to acquire lock
                if let Ok(true) = self.lock_manager.acquire_lock(self.projection.name(), &self.node_id).await {
                    // Lock acquired, initialize projection state
                    let mut current_seq = 0;
                    if let Ok(Some((seq, loaded_state))) =
                        self.snapshot_store.load(self.projection.name()).await
                    {
                        let mut state_lock = self.state.lock().await;
                        *state_lock = loaded_state;
                        current_seq = seq;
                    }

                    let mut rx = self.bus.subscribe();

                    // Catch up missing events
                    let mut stream = self.event_store.read_all_from(current_seq + 1);
                    while let Some(Ok(event)) = stream.next().await {
                        // In a real system we should also call `keep_alive` periodically here
                        let mut state_lock = self.state.lock().await;
                        if let Ok(()) = self.projection.handle(&mut state_lock, &event).await {
                            current_seq = event.global_sequence_num;
                            let _ = self.snapshot_store.save(self.projection.name(), current_seq, &*state_lock).await;
                        }
                    }

                    // Process live events
                    let mut lagged = false;
                    while !lagged {
                        // Re-verify/extend lock periodically
                        if let Err(_) = self.lock_manager.keep_alive(self.projection.name(), &self.node_id).await {
                            break; // Lost lock, back to attempt acquiring lock
                        }

                        match tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv()).await {
                            Ok(Ok(event)) => {
                                if event.global_sequence_num <= current_seq && event.global_sequence_num != 0 {
                                    continue;
                                }

                                let mut state_lock = self.state.lock().await;
                                if let Ok(()) = self.projection.handle(&mut state_lock, &event).await {
                                    current_seq = event.global_sequence_num;
                                    let _ = self.snapshot_store.save(self.projection.name(), current_seq, &*state_lock).await;
                                }
                            }
                            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => {
                                lagged = true;
                            }
                            Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => {
                                let _ = self.lock_manager.release_lock(self.projection.name(), &self.node_id).await;
                                return;
                            }
                            Err(_) => {
                                // Timeout just used for keep_alive heartbeat loop
                            }
                        }
                    }
                } else {
                    // Failed to acquire lock/someone else has it, wait and retry
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            }
        })
    }
}
