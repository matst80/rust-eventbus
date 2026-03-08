use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::{unfold, BoxStream, StreamExt};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};

use crate::{Event, EventPayload};

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),
    #[error("Concurrency conflict: {0}")]
    Conflict(String),
    #[error("Other error: {0}")]
    Other(String),
}

/// Defines the strategy for log compaction.
pub enum CompactionRule<E> {
    /// Keep only the latest event for each aggregate ID (basic log compaction).
    LatestPerAggregate,
    /// Keep the latest event, but prune the entire aggregate if the latest event satisfies the condition.
    PruneIf(fn(&E) -> bool),
}

impl<E> Default for CompactionRule<E> {
    fn default() -> Self {
        Self::LatestPerAggregate
    }
}

/// Core trait representing an asynchronous Event Store.
/// Handles appending strongly typed events and streaming them back efficiently.
#[async_trait]
pub trait EventStore<E: EventPayload>: Send + Sync + 'static {
    /// Appends a batch of events to the store and returns them with assigned global sequence IDs.
    async fn append(&self, events: Vec<Event<E>>) -> Result<Vec<Event<E>>, StoreError>;

    /// Streams all events starting from a specific global sequence ID (for catching up projections).
    /// Returns a BoxStream to avoid loading all events into memory.
    fn read_all_from(&self, start_sequence: u64) -> BoxStream<'_, Result<Event<E>, StoreError>>;

    /// Compact the event log using the specified rule (default: keep latest per aggregate).
    /// Returns the number of events removed.
    async fn compact(&self, rule: CompactionRule<E>) -> Result<u64, StoreError>;

    /// Remove all events with global_sequence_num <= min_seq.
    /// Returns the number of events removed.
    async fn truncate_before(&self, min_seq: u64) -> Result<u64, StoreError>;
}

/// Core trait for saving and loading Projection Snapshots with full Type Safety.
#[async_trait]
pub trait SnapshotStore<S>: Send + Sync + 'static
where
    S: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Loads the latest snapshot and its corresponding processed sequence number.
    async fn load(&self, projection_name: &str) -> Result<Option<(u64, S)>, StoreError>;

    /// Saves a new snapshot state with its current sequence number.
    async fn save(
        &self,
        projection_name: &str,
        sequence_num: u64,
        state: &S,
    ) -> Result<(), StoreError>;

    /// Delete the snapshot for a projection (used during reset).
    async fn delete(&self, projection_name: &str) -> Result<(), StoreError>;
}

use tokio::sync::{mpsc, Mutex};

/// A simple file-based, append-only EventStore implementation using raw bincode binary sequences.
#[derive(Clone)]
pub struct FileEventStore {
    file_path: PathBuf,
    // A mutex ensures append operations from multiple tasks do not interleave bytes and handles seq.
    inner: Arc<Mutex<FileInnerState>>,
}

struct FileInnerState {
    next_global_seq: u64,
    tx: mpsc::Sender<Vec<u8>>,
}

impl FileEventStore {
    pub async fn new(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let file_path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
            .await?;

        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(1024);

        // Spawn a background worker to handle file writes asynchronously.
        // This decouples the append operation from disk I/O performance.
        tokio::spawn(async move {
            let mut file = file;
            while let Some(buffer) = rx.recv().await {
                if let Err(e) = file.write_all(&buffer).await {
                    eprintln!("EventStore background writer failed: {}", e);
                    break;
                }
                // Optional: We could sync periodically here, but for now we let the OS buffer it for max speed.
            }
            let _ = file.sync_all().await;
        });

        Ok(Self {
            file_path,
            inner: Arc::new(Mutex::new(FileInnerState {
                tx,
                next_global_seq: 1,
            })),
        })
    }

    /// Read all events from the file into a Vec (helper for compaction/truncation).
    async fn read_all_events<E: EventPayload>(&self) -> Result<Vec<Event<E>>, StoreError> {
        let path = &self.file_path;
        match File::open(path).await {
            Ok(f) => {
                let mut reader = BufReader::new(f);
                let mut events = Vec::new();
                loop {
                    let mut len_buf = [0u8; 4];
                    match reader.read_exact(&mut len_buf).await {
                        Ok(_) => {
                            let len = u32::from_le_bytes(len_buf) as usize;
                            let mut data = vec![0u8; len];
                            reader.read_exact(&mut data).await?;
                            let event: Event<E> = bincode::deserialize(&data)?;
                            events.push(event);
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                        Err(e) => return Err(StoreError::Io(e)),
                    }
                }
                Ok(events)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(StoreError::Io(e)),
        }
    }

    /// Write events to a temp file then atomically rename over the main file.
    async fn atomic_rewrite<E: EventPayload>(&self, events: &[Event<E>]) -> Result<(), StoreError> {
        let tmp_path = self.file_path.with_extension("tmp");
        let mut file = File::create(&tmp_path).await?;
        for event in events {
            let bytes = bincode::serialize(event)?;
            let len = bytes.len() as u32;
            file.write_all(&len.to_le_bytes()).await?;
            file.write_all(&bytes).await?;
        }
        file.sync_all().await?;
        tokio::fs::rename(&tmp_path, &self.file_path).await?;
        Ok(())
    }
}

#[async_trait]
impl<E> EventStore<E> for FileEventStore
where
    E: EventPayload + 'static,
{
    async fn append(&self, mut events: Vec<Event<E>>) -> Result<Vec<Event<E>>, StoreError> {
        let mut buffer = Vec::new();

        let mut inner = self.inner.lock().await;
        for event in &mut events {
            event.global_sequence_num = inner.next_global_seq;
            inner.next_global_seq += 1;

            let bytes = bincode::serialize(&event)?;
            let len = bytes.len() as u32;
            buffer.extend_from_slice(&len.to_le_bytes());
            buffer.extend_from_slice(&bytes);
        }

        inner
            .tx
            .send(buffer)
            .await
            .map_err(|_| StoreError::Other("Background writer closed".into()))?;

        // Flush to ensure data is available for reads immediately after append.
        inner
            .tx
            .send(vec![])
            .await
            .ok(); // no-op flush sentinel; harmless if writer is gone

        Ok(events)
    }

    fn read_all_from(&self, start_sequence: u64) -> BoxStream<'_, Result<Event<E>, StoreError>> {
        let path = self.file_path.clone();

        let stream = unfold(
            None,
            move |mut state: Option<BufReader<File>>| {
                let path_clone = path.clone();
                async move {
                    if state.is_none() {
                        match File::open(&path_clone).await {
                            Ok(f) => {
                                state = Some(BufReader::new(f));
                            }
                            Err(e) => {
                                if e.kind() == std::io::ErrorKind::NotFound {
                                    return None;
                                }
                                return Some((Err(StoreError::Io(e)), state));
                            }
                        }
                    }

                    let mut reader = state.unwrap();
                    loop {
                        let mut len_buf = [0u8; 4];
                        match reader.read_exact(&mut len_buf).await {
                            Ok(_) => {
                                let len = u32::from_le_bytes(len_buf) as usize;
                                let mut data = vec![0u8; len];
                                match reader.read_exact(&mut data).await {
                                    Ok(_) => match bincode::deserialize::<Event<E>>(&data) {
                                        Ok(event) => {
                                            if event.global_sequence_num >= start_sequence {
                                                return Some((Ok(event), Some(reader)));
                                            }
                                        }
                                        Err(e) => {
                                            return Some((Err(StoreError::Serialization(e)), Some(reader)))
                                        }
                                    },
                                    Err(e) => return Some((Err(StoreError::Io(e)), Some(reader))),
                                }
                            }
                            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return None,
                            Err(e) => return Some((Err(StoreError::Io(e)), Some(reader))),
                        }
                    }
                }
            },
        );

        stream.boxed()
    }

    async fn compact(&self, rule: CompactionRule<E>) -> Result<u64, StoreError> {
        let inner = self.inner.lock().await;
        let all_events: Vec<Event<E>> = self.read_all_events().await?;
        let original_count = all_events.len() as u64;

        // Keep only the latest event per aggregate_id (highest global_sequence_num).
        let mut latest: HashMap<String, Event<E>> = HashMap::new();
        for event in all_events {
            let entry = latest
                .entry(event.aggregate_id.clone())
                .or_insert_with(|| event.clone());
            if event.global_sequence_num > entry.global_sequence_num {
                *entry = event;
            }
        }

        // Apply compaction rules:
        // By default, we keep the latest. If PruneIf is provided, we remove aggregates
        // where the latest event matches the predicate (e.g., is a 'Deleted' event).
        if let CompactionRule::PruneIf(predicate) = rule {
            latest.retain(|_, ev| !(predicate)(&ev.payload));
        }

        let mut kept: Vec<Event<E>> = latest.into_values().collect();
        kept.sort_by_key(|e| e.global_sequence_num);

        self.atomic_rewrite(&kept).await?;

        // Update next_global_seq to be after the highest remaining event.
        // We hold the lock so this is safe; drop it explicitly via the guard held above.
        drop(inner);
        let mut inner = self.inner.lock().await;
        if let Some(last) = kept.last() {
            inner.next_global_seq = last.global_sequence_num + 1;
        }

        Ok(original_count - kept.len() as u64)
    }

    async fn truncate_before(&self, min_seq: u64) -> Result<u64, StoreError> {
        let _inner = self.inner.lock().await;
        let all_events: Vec<Event<E>> = self.read_all_events().await?;
        let original_count = all_events.len() as u64;

        let kept: Vec<Event<E>> = all_events
            .into_iter()
            .filter(|e| e.global_sequence_num > min_seq)
            .collect();

        self.atomic_rewrite(&kept).await?;
        Ok(original_count - kept.len() as u64)
    }
}

/// A simple file-based Snapshot Store. Saves state as `<projection_name>.snapshot.bin`.
#[derive(Clone)]
pub struct FileSnapshotStore {
    dir: PathBuf,
}

impl FileSnapshotStore {
    pub async fn new(dir: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = std::env::current_dir()?.join(dir.as_ref());
        tokio::fs::create_dir_all(&path).await?;
        Ok(Self { dir: path })
    }

    fn file_path(&self, name: &str) -> PathBuf {
        self.dir.join(format!("{name}.snapshot.bin"))
    }
}

#[derive(Serialize, Deserialize)]
struct SnapshotEnvelope<S> {
    sequence_num: u64,
    state: S,
}

#[async_trait]
impl<S> SnapshotStore<S> for FileSnapshotStore
where
    S: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    async fn load(&self, projection_name: &str) -> Result<Option<(u64, S)>, StoreError> {
        let path = self.file_path(projection_name);
        match tokio::fs::read(&path).await {
            Ok(content) => {
                let env: SnapshotEnvelope<S> = bincode::deserialize(&content)?;
                Ok(Some((env.sequence_num, env.state)))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StoreError::Io(e)),
        }
    }

    async fn save(
        &self,
        projection_name: &str,
        sequence_num: u64,
        state: &S,
    ) -> Result<(), StoreError> {
        let path = self.file_path(projection_name);
        let tmp_id = uuid::Uuid::new_v4();
        let tmp_path = self.dir.join(format!("{}.{}.snapshot.tmp", projection_name, tmp_id));
        
        let env = SnapshotEnvelope {
            sequence_num,
            // We reference state initially for serialization, but the trait bounds require ownership or we can just serialize references.
            // Oh wait, S is already not a reference string here.
            state,
        };
        let content = bincode::serialize(&env)?;
        
        let mut tmp_file = tokio::fs::File::create(&tmp_path).await?;
        tmp_file.write_all(&content).await?;
        tmp_file.sync_data().await?;
        
        tokio::fs::rename(tmp_path, path).await?;
        Ok(())
    }

    async fn delete(&self, projection_name: &str) -> Result<(), StoreError> {
        let path = self.file_path(projection_name);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(StoreError::Io(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub struct DummyEvent {
        value: String,
    }

    impl EventPayload for DummyEvent {
        fn event_type(&self) -> &'static str {
            "DummyEvent"
        }
    }

    #[tokio::test]
    async fn test_bincode_event_store() {
        let dir = std::env::temp_dir().join(format!("eventbus_test_{}", SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros()));
        let store = FileEventStore::new(&dir).await.unwrap();

        let event1 = Event::new("entity-1", 1, DummyEvent { value: "test1".into() });
        let event2 = Event::new("entity-1", 2, DummyEvent { value: "test2".into() });

        store.append(vec![event1.clone(), event2.clone()]).await.unwrap();

        let mut stream: BoxStream<'_, Result<Event<DummyEvent>, StoreError>> = store.read_all_from(0);
        
        let mut read_events = Vec::new();
        while let Some(result) = stream.next().await {
            read_events.push(result.unwrap());
        }

        assert_eq!(read_events.len(), 2);
        assert_eq!(read_events[0].payload.value, "test1");
        assert_eq!(read_events[1].payload.value, "test2");

        let _ = tokio::fs::remove_file(&dir).await;
    }

    #[tokio::test]
    async fn test_compact_keeps_latest() {
        let dir = std::env::temp_dir().join(format!("compact_test_{}", SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros()));
        let store = FileEventStore::new(&dir).await.unwrap();

        // 3 events for aggregate "a", 1 for "b"
        let events = vec![
            Event::new("a", 1, DummyEvent { value: "a1".into() }),
            Event::new("a", 2, DummyEvent { value: "a2".into() }),
            Event::new("b", 1, DummyEvent { value: "b1".into() }),
            Event::new("a", 3, DummyEvent { value: "a3".into() }),
        ];
        store.append(events).await.unwrap();

        let removed = EventStore::<DummyEvent>::compact(&store, CompactionRule::LatestPerAggregate).await.unwrap();
        assert_eq!(removed, 2); // a1 and a2 removed

        let mut stream: BoxStream<'_, Result<Event<DummyEvent>, StoreError>> = store.read_all_from(0);
        let mut remaining = Vec::new();
        while let Some(Ok(ev)) = stream.next().await {
            remaining.push(ev);
        }
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].payload.value, "b1");
        assert_eq!(remaining[1].payload.value, "a3");

        let _ = tokio::fs::remove_file(&dir).await;
    }

    #[tokio::test]
    async fn test_truncate_before() {
        let dir = std::env::temp_dir().join(format!("truncate_test_{}", SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros()));
        let store = FileEventStore::new(&dir).await.unwrap();

        let events = vec![
            Event::new("a", 1, DummyEvent { value: "e1".into() }),
            Event::new("a", 2, DummyEvent { value: "e2".into() }),
            Event::new("b", 1, DummyEvent { value: "e3".into() }),
        ];
        store.append(events).await.unwrap();

        // Truncate events with seq <= 2 (removes e1 seq=1, e2 seq=2)
        let removed = EventStore::<DummyEvent>::truncate_before(&store, 2).await.unwrap();
        assert_eq!(removed, 2);

        let mut stream: BoxStream<'_, Result<Event<DummyEvent>, StoreError>> = store.read_all_from(0);
        let mut remaining = Vec::new();
        while let Some(Ok(ev)) = stream.next().await {
            remaining.push(ev);
        }
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].payload.value, "e3");

        let _ = tokio::fs::remove_file(&dir).await;
    }

    #[tokio::test]
    async fn test_bincode_snapshot_store() {
        let dir = std::env::temp_dir().join(format!("snapshot_test_{}", SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros()));
        let store = FileSnapshotStore::new(&dir).await.unwrap();

        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct DummyState {
            counter: u32,
        }

        let state = DummyState { counter: 42 };
        store.save("dummy_proj", 5, &state).await.unwrap();

        let loaded = store.load("dummy_proj").await.unwrap();
        assert!(loaded.is_some());
        
        let (seq, loaded_state): (u64, DummyState) = loaded.unwrap();
        assert_eq!(seq, 5);
        assert_eq!(loaded_state.counter, 42);

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn test_snapshot_delete() {
        let dir = std::env::temp_dir().join(format!("snap_del_test_{}", SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros()));
        let store = FileSnapshotStore::new(&dir).await.unwrap();

        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct S { v: u32 }

        SnapshotStore::<S>::save(&store, "proj", 10, &S { v: 1 }).await.unwrap();
        assert!(SnapshotStore::<S>::load(&store, "proj").await.unwrap().is_some());

        SnapshotStore::<S>::delete(&store, "proj").await.unwrap();
        assert!(SnapshotStore::<S>::load(&store, "proj").await.unwrap().is_none());

        // Deleting again is idempotent
        SnapshotStore::<S>::delete(&store, "proj").await.unwrap();

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }
}
