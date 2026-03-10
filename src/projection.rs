use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures::StreamExt;
use thiserror::Error;
use tokio::sync::{watch};
use parking_lot::{RwLock, Mutex};

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
pub trait Projection<E: EventPayload, S>: Send + Sync + 'static {
    /// Return the unique name of this projection (used for snapshot tracking).
    fn name(&self) -> &'static str;

    /// Process a single event and mutate the state `S`.
    fn handle(&self, state: &mut S, event: &Event<E>) -> Result<(), ProjectionError>;
}

/// Commands that can be sent to a running projection actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProjectionCommand {
    /// Normal operation.
    Run,
    /// Replay from sequence 0, keeping the current snapshot.
    Replay,
    /// Delete snapshot then replay from 0.
    Reset,
}

/// Handle returned from spawning a projection actor. Use this to trigger replay/reset.
#[derive(Clone)]
pub struct ProjectionHandle {
    cmd_tx: watch::Sender<ProjectionCommand>,
}

impl ProjectionHandle {
    /// Replay all events from the beginning, rebuilding state from scratch.
    /// The snapshot is preserved until the replay catches up.
    pub fn replay(&self) {
        let _ = self.cmd_tx.send(ProjectionCommand::Replay);
    }

    /// Delete the snapshot and replay all events from scratch.
    pub fn reset(&self) {
        let _ = self.cmd_tx.send(ProjectionCommand::Reset);
    }
}

/// Orchestrates an ephemeral projection (e.g., in-memory cache, websocket fanout).
/// It subscribes to the EventBus, catches up, and processes events in a standalone task.
/// It runs on ALL nodes simultaneously.
pub struct EphemeralProjectionActor<E: EventPayload, S, P, SS, ES> {
    bus: EventBus<E>,
    event_store: Arc<ES>,
    projection: Arc<P>,
    snapshot_store: Arc<SS>,
    state: Arc<RwLock<S>>,
    snapshot_interval: u64,
    cmd_tx: watch::Sender<ProjectionCommand>,
    cmd_rx: watch::Receiver<ProjectionCommand>,
    _marker: std::marker::PhantomData<E>,
    version: Option<Arc<AtomicU64>>,
    task_limiter: Option<crate::task_limiter::TaskLimiter>,
}

impl<E, S, P, SS, ES> EphemeralProjectionActor<E, S, P, SS, ES>
where
    E: EventPayload,
    S: Default + Clone + Send + Sync + serde::Serialize + serde::de::DeserializeOwned + 'static,
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
        let (cmd_tx, cmd_rx) = watch::channel(ProjectionCommand::Run);
        Self {
            bus,
            event_store,
            projection,
            snapshot_store,
            state: Arc::new(parking_lot::RwLock::new(S::default())),
            snapshot_interval: 100,
            cmd_tx,
            cmd_rx,
            _marker: std::marker::PhantomData,
            version: None,
            task_limiter: None,
        }
    }

    /// Set how often to persist snapshots (every N events). Default is 1.
    pub fn with_snapshot_interval(mut self, interval: u64) -> Self {
        self.snapshot_interval = interval.max(1);
        self
    }

    /// Set a version counter to track projection changes (for ETag support).
    pub fn with_version(mut self, version: Arc<AtomicU64>) -> Self {
        self.version = Some(version);
        self
    }

    /// Set a task limiter to control concurrency.
    pub fn with_task_limiter(mut self, task_limiter: crate::task_limiter::TaskLimiter) -> Self {
        self.task_limiter = Some(task_limiter);
        self
    }

    /// Returns a reference-counted lock to the underlying projection state.
    pub fn get_state(&self) -> Arc<parking_lot::RwLock<S>> {
        Arc::clone(&self.state)
    }

    /// Returns a handle for sending replay/reset commands to the running actor.
    pub fn get_handle(&self) -> ProjectionHandle {
        ProjectionHandle {
            cmd_tx: self.cmd_tx.clone(),
        }
    }

    /// Spawns the projection actor loop into the tokio runtime.
    pub async fn spawn(self) -> tokio::task::JoinHandle<()> {
        let task = async move {
            let mut cmd_rx = self.cmd_rx.clone();

            loop {
                // Check for pending replay/reset commands before starting a cycle.
                let cmd = *cmd_rx.borrow_and_update();
                match cmd {
                    ProjectionCommand::Reset => {
                        if let Err(_e) = self.snapshot_store.delete(self.projection.name()).await {
                            // Log error if needed, but don't block
                        }
                        let mut state_lock = self.state.write();
                        *state_lock = S::default();
                    }
                    ProjectionCommand::Replay => {
                        let mut state_lock = self.state.write();
                        *state_lock = S::default();
                    }
                    ProjectionCommand::Run => {}
                }
                // Acknowledge command by resetting to Run (only if it was a command).
                if cmd != ProjectionCommand::Run {
                    let _ = self.cmd_tx.send(ProjectionCommand::Run);
                }

                // 1. Load latest state from snapshot
                let mut current_seq = 0;
                if cmd == ProjectionCommand::Run {
                    if let Ok(Some((seq, loaded_state))) =
                        self.snapshot_store.load(self.projection.name()).await
                    {
                        let mut state_lock = self.state.write();
                        *state_lock = loaded_state;
                        current_seq = seq;
                    }
                }

                // 2. Subscribe to real-time events
                let mut rx = self.bus.subscribe();
                let mut events_since_snapshot: u64 = 0;

                // 3. Catch up from EventStore
                let mut stream = self.event_store.read_all_from(current_seq + 1);
                while let Some(Ok(event)) = stream.next().await {
                    {
                        let mut state_lock = self.state.write();
                        if let Ok(()) = self.projection.handle(&mut state_lock, &event) {
                            current_seq = current_seq.max(event.global_sequence_num);
                            if let Some(v) = &self.version {
                                v.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    tokio::task::yield_now().await;
                }
                // Save snapshot after catch-up if there were unsaved events.
                // Save snapshot after catch-up.
                {
                    let state_clone = self.state.read().clone();
                    let _ = self
                        .snapshot_store
                        .save(self.projection.name(), current_seq, &state_clone)
                        .await;
                }

                // 4. Process real-time events
                let mut restart = false;
                // Mark the watch channel as seen so we don't immediately restart
                cmd_rx.borrow_and_update();
                while !restart {
                    tokio::select! {
                        biased;
                        // Watch for replay/reset commands.
                        result = cmd_rx.changed() => {
                            if result.is_ok() {
                                restart = true;
                            }
                        },
                        recv = rx.recv() => {
                            match recv {
                                Ok(event) => {
                                    if event.global_sequence_num <= current_seq && event.global_sequence_num != 0 {
                                        continue;
                                    }
                                    let mut needs_snapshot = false;
                                    let mut state_clone = None;
                                    {
                                        let mut state_lock = self.state.write();
                                        match self.projection.handle(&mut state_lock, &event) {
                                            Ok(()) => {
                                                current_seq = current_seq.max(event.global_sequence_num);
                                                events_since_snapshot += 1;
                                                if let Some(v) = &self.version {
                                                    v.fetch_add(1, Ordering::Relaxed);
                                                }
                                                if events_since_snapshot >= self.snapshot_interval {
                                                    events_since_snapshot = 0;
                                                    state_clone = Some(state_lock.clone());
                                                    needs_snapshot = true;
                                                }
                                            }
                                            Err(e) => {
                                                eprintln!("Projection {} failed to process event: {}", self.projection.name(), e);
                                            }
                                        }
                                    }

                                    if needs_snapshot {
                                        if let Err(e) = self.snapshot_store
                                            .save(self.projection.name(), current_seq, &state_clone.unwrap())
                                            .await
                                        {
                                            eprintln!("Failed to save snapshot for {}: {}", self.projection.name(), e);
                                        }
                                    }
                                }
                                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                                    eprintln!("Projection {} lagged by {} events, resyncing...", self.projection.name(), skipped);
                                    restart = true;
                                }
                                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                    return; // Bus closed, stop projection
                                }
                            }
                        },
                    }
                }
            }
        };

        if let Some(limiter) = self.task_limiter.clone() {
            limiter.spawn(task).await
        } else {
            tokio::spawn(task)
        }
    }
}

/// Orchestrates a durable projection (e.g., shared relational database, robust read-model).
/// Only ONE node holding the lock will actively process events.
pub struct DurableProjectionActor<E: EventPayload, S, P, SS, ES, LM: ?Sized> {
    bus: EventBus<E>,
    event_store: Arc<ES>,
    projection: Arc<P>,
    snapshot_store: Arc<SS>,
    lock_manager: Arc<LM>,
    node_id: Uuid,
    state: Arc<Mutex<S>>,
    snapshot_interval: u64,
    cmd_tx: watch::Sender<ProjectionCommand>,
    cmd_rx: watch::Receiver<ProjectionCommand>,
    _marker: std::marker::PhantomData<E>,
    version: Option<Arc<AtomicU64>>,
    task_limiter: Option<crate::task_limiter::TaskLimiter>,
}

impl<E, S, P, SS, ES, LM> DurableProjectionActor<E, S, P, SS, ES, LM>
where
    E: EventPayload,
    S: Default + Clone + Send + Sync + serde::Serialize + serde::de::DeserializeOwned + 'static,
    P: Projection<E, S>,
    SS: SnapshotStore<S>,
    ES: EventStore<E>,
    LM: ProjectionLockManager + ?Sized,
{
    pub fn new(
        bus: EventBus<E>,
        event_store: Arc<ES>,
        projection: Arc<P>,
        snapshot_store: Arc<SS>,
        lock_manager: Arc<LM>,
        node_id: Uuid,
    ) -> Self {
        let (cmd_tx, cmd_rx) = watch::channel(ProjectionCommand::Run);
        Self {
            bus,
            event_store,
            projection,
            snapshot_store,
            lock_manager,
            node_id,
            state: Arc::new(parking_lot::Mutex::new(S::default())),
            snapshot_interval: 100,
            cmd_tx,
            cmd_rx,
            _marker: std::marker::PhantomData,
            version: None,
            task_limiter: None,
        }
    }

    pub fn with_snapshot_interval(mut self, interval: u64) -> Self {
        self.snapshot_interval = interval.max(1);
        self
    }

    pub fn with_version(mut self, version: Arc<AtomicU64>) -> Self {
        self.version = Some(version);
        self
    }

    pub fn with_task_limiter(mut self, task_limiter: crate::task_limiter::TaskLimiter) -> Self {
        self.task_limiter = Some(task_limiter);
        self
    }

    pub fn get_state(&self) -> Arc<parking_lot::Mutex<S>> {
        Arc::clone(&self.state)
    }

    pub fn get_handle(&self) -> ProjectionHandle {
        ProjectionHandle {
            cmd_tx: self.cmd_tx.clone(),
        }
    }

    pub async fn spawn(self) -> tokio::task::JoinHandle<()> {
        let task = async move {
            let mut cmd_rx = self.cmd_rx.clone();

            loop {
                // 1. Try to acquire lock
                if let Ok(true) = self
                    .lock_manager
                    .acquire_lock(self.projection.name(), &self.node_id)
                    .await
                {
                    // Check for pending replay/reset commands.
                    let cmd = *cmd_rx.borrow_and_update();
                    match cmd {
                        ProjectionCommand::Reset => {
                            let _ = self.snapshot_store.delete(self.projection.name()).await;
                            let mut state_lock = self.state.lock();
                            *state_lock = S::default();
                        }
                        ProjectionCommand::Replay => {
                            let mut state_lock = self.state.lock();
                            *state_lock = S::default();
                        }
                        ProjectionCommand::Run => {}
                    }
                    if cmd != ProjectionCommand::Run {
                        let _ = self.cmd_tx.send(ProjectionCommand::Run);
                    }

                    let mut current_seq = 0;
                    if cmd == ProjectionCommand::Run {
                        if let Ok(Some((seq, loaded_state))) =
                            self.snapshot_store.load(self.projection.name()).await
                        {
                            let mut state_lock = self.state.lock();
                            *state_lock = loaded_state;
                            current_seq = seq;
                        }
                    }

                    let mut rx = self.bus.subscribe();
                    let mut events_since_snapshot: u64 = 0;

                    // Catch up from EventStore
                    let mut stream = self.event_store.read_all_from(current_seq + 1);
                    while let Some(Ok(event)) = stream.next().await {
                        {
                            let mut state_lock = self.state.lock();
                            if let Ok(()) = self.projection.handle(&mut state_lock, &event) {
                                current_seq = current_seq.max(event.global_sequence_num);
                                if let Some(v) = &self.version {
                                    v.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                        tokio::task::yield_now().await;
                    }
                    // Save snapshot after catch-up.
                    {
                        let state_clone = self.state.lock().clone();
                        if let Err(e) = self
                            .snapshot_store
                            .save(self.projection.name(), current_seq, &state_clone)
                            .await
                        {
                            eprintln!(
                                "Failed to save snapshot after catch-up for {}: {}",
                                self.projection.name(),
                                e
                            );
                        }
                    }

                    // Process live events
                    let mut restart = false;
                    // Mark the watch channel as seen so we don't immediately restart
                    cmd_rx.borrow_and_update();
                    while !restart {
                        // Re-verify/extend lock periodically
                        if self
                            .lock_manager
                            .keep_alive(self.projection.name(), &self.node_id)
                            .await
                            .is_err()
                        {
                            break; // Lost lock
                        }

                        tokio::select! {
                            biased;
                            result = cmd_rx.changed() => {
                                if result.is_ok() {
                                    restart = true;
                                }
                            },
                            recv = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv()) => {
                                match recv {
                                    Ok(Ok(event)) => {
                                        if event.global_sequence_num <= current_seq && event.global_sequence_num != 0 {
                                            continue;
                                        }
                                        let mut needs_snapshot = false;
                                        let mut state_clone = None;
                                        {
                                            let mut state_lock = self.state.lock();
                                            match self.projection.handle(&mut state_lock, &event) {
                                                Ok(()) => {
                                                    current_seq = current_seq.max(event.global_sequence_num);
                                                    events_since_snapshot += 1;
                                                    if let Some(v) = &self.version {
                                                        v.fetch_add(1, Ordering::Relaxed);
                                                    }
                                                    if events_since_snapshot >= self.snapshot_interval {
                                                        events_since_snapshot = 0;
                                                        state_clone = Some(state_lock.clone());
                                                        needs_snapshot = true;
                                                    }
                                                }
                                                Err(e) => {
                                                    eprintln!("Durable projection {} failed to process event: {}", self.projection.name(), e);
                                                }
                                            }
                                        }
                                        if needs_snapshot {
                                            if let Err(e) = self.snapshot_store.save(self.projection.name(), current_seq, &state_clone.unwrap()).await {
                                                eprintln!("Failed to save snapshot for {}: {}", self.projection.name(), e);
                                            }
                                        }
                                    }
                                    Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => {
                                        restart = true;
                                    }
                                    Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => {
                                        let _ = self.lock_manager.release_lock(self.projection.name(), &self.node_id).await;
                                        return;
                                    }
                                    Err(_) => {
                                        // Timeout for keep_alive heartbeat
                                    }
                                }
                            },
                        }
                        tokio::task::yield_now().await;
                    }
                } else {
                    // Failed to acquire lock, wait and retry
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            }
        };

        if let Some(limiter) = self.task_limiter.clone() {
            limiter.spawn(task).await
        } else {
            tokio::spawn(task)
        }
    }
}
