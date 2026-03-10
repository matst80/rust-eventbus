use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use uuid::Uuid;

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
    tx: mpsc::Sender<(Vec<u8>, Option<tokio::sync::oneshot::Sender<()>>)>,
    /// In-memory index of event id -> global_sequence_num to provide idempotent appends.
    id_index: HashMap<Uuid, u64>,
}

impl FileEventStore {
    pub async fn new(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let file_path = path.as_ref().to_path_buf();

        // Scan the file to find the highest sequence number and build an id -> seq index
        let mut next_global_seq = 1u64;
        let mut id_index: HashMap<Uuid, u64> = HashMap::new();
        if let Ok(file) = File::open(&file_path).await {
            let mut reader = BufReader::new(file);
            loop {
                let mut len_buf = [0u8; 4];
                match reader.read_exact(&mut len_buf).await {
                    Ok(_) => {
                        let len = u32::from_le_bytes(len_buf) as usize;
                        let mut data = vec![0u8; len];
                        if reader.read_exact(&mut data).await.is_ok() {
                            #[derive(Deserialize)]
                            struct IdSeqPeek {
                                id: Uuid,
                                global_sequence_num: u64,
                            }
                            if let Ok(peek) = bincode::deserialize::<IdSeqPeek>(&data) {
                                if peek.global_sequence_num >= next_global_seq {
                                    next_global_seq = peek.global_sequence_num + 1;
                                }
                                id_index.insert(peek.id, peek.global_sequence_num);
                            }
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(StoreError::Io(e)),
                }
            }
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
            .await?;

        let (tx, mut rx) = mpsc::channel::<(Vec<u8>, Option<tokio::sync::oneshot::Sender<()>>)>(1024);

        // Spawn a background worker to handle file writes asynchronously.
        tokio::spawn(async move {
            let mut file = file;
            while let Some((buffer, sync_tx)) = rx.recv().await {
                let mut needs_sync = buffer.is_empty();
                let mut sync_waiters = Vec::new();
                if let Some(stx) = sync_tx {
                    sync_waiters.push(stx);
                }

                if !buffer.is_empty() {
                    if let Err(e) = file.write_all(&buffer).await {
                        eprintln!("EventStore background writer failed: {}", e);
                        break;
                    }
                }

                // ⚡ Bolt Optimization: Batch I/O writes and fsync requests
                while let Ok((next_buffer, next_sync_tx)) = rx.try_recv() {
                    if let Some(stx) = next_sync_tx {
                        sync_waiters.push(stx);
                    }
                    if next_buffer.is_empty() {
                        needs_sync = true;
                    } else if let Err(e) = file.write_all(&next_buffer).await {
                        eprintln!("EventStore background writer failed: {}", e);
                        break;
                    }
                }

                if needs_sync || !sync_waiters.is_empty() {
                    let _ = file.sync_data().await;
                    eprintln!("DEBUG: EventStore synced to disk");
                    for stx in sync_waiters {
                        let _ = stx.send(());
                    }
                }
            }
            let _ = file.sync_all().await;
        });

        Ok(Self {
            file_path,
            inner: Arc::new(Mutex::new(FileInnerState {
                next_global_seq,
                tx,
                id_index,
            })),
        })
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
            // Idempotency: if we already saw this event id, return the known global_sequence_num
            if let Some(existing_seq) = inner.id_index.get(&event.id) {
                event.global_sequence_num = *existing_seq;
                continue; // skip writing duplicate
            }

            event.global_sequence_num = inner.next_global_seq;
            inner.next_global_seq += 1;
            inner.id_index.insert(event.id, event.global_sequence_num);

            let bytes = bincode::serialize(&event)?;
            let len = bytes.len() as u32;
            buffer.extend_from_slice(&len.to_le_bytes());
            buffer.extend_from_slice(&bytes);
        }

        // Only send a write if there's new data to persist.
        if !buffer.is_empty() {
            let (sync_tx, sync_rx) = tokio::sync::oneshot::channel();
            inner
                .tx
                .send((buffer, Some(sync_tx)))
                .await
                .map_err(|_| StoreError::Other("Background writer closed".into()))?;

            // ⚡ Optimization: Release the lock before waiting for disk I/O.
            // This allows other threads to prepare their batches while we wait for the sync.
            drop(inner);

            // Await synchronization for durability
            let _ = sync_rx.await;
        }

        Ok(events)
    }

    fn read_all_from(&self, start_sequence: u64) -> BoxStream<'_, Result<Event<E>, StoreError>> {
        let path = self.file_path.clone();

        // State: (Reader, Buffer)
        let stream = unfold(None, move |mut state: Option<(BufReader<File>, Vec<u8>)>| {
            let path_clone = path.clone();
            async move {
                if state.is_none() {
                    match File::open(&path_clone).await {
                        Ok(f) => {
                            state = Some((BufReader::new(f), Vec::with_capacity(1024)));
                        }
                        Err(e) => {
                            if e.kind() == std::io::ErrorKind::NotFound {
                                return None;
                            }
                            return Some((Err(StoreError::Io(e)), state));
                        }
                    }
                }

                let (mut reader, mut buf) = state.expect("State should not be None");
                loop {
                    let mut len_buf = [0u8; 4];
                    match reader.read_exact(&mut len_buf).await {
                        Ok(_) => {
                            let len = u32::from_le_bytes(len_buf) as usize;
                            if buf.len() < len {
                                buf.resize(len, 0);
                            }
                            let data = &mut buf[..len];
                            match reader.read_exact(data).await {
                                Ok(_) => {
                                    // Optimization: Peek at the global_sequence_num without full deserialization if possible.
                                    match bincode::deserialize::<Event<E>>(data) {
                                        Ok(event) => {
                                            if event.global_sequence_num >= start_sequence {
                                                return Some((Ok(event), Some((reader, buf))));
                                            }
                                        }
                                        Err(e) => {
                                            return Some((
                                                Err(StoreError::Serialization(e)),
                                                Some((reader, buf)),
                                            ))
                                        }
                                    }
                                }
                                Err(e) => {
                                    return Some((Err(StoreError::Io(e)), Some((reader, buf))))
                                }
                            }
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return None,
                        Err(e) => return Some((Err(StoreError::Io(e)), Some((reader, buf)))),
                    }
                }
            }
        });

        stream.boxed()
    }

    async fn compact(&self, rule: CompactionRule<E>) -> Result<u64, StoreError> {
        let mut guard = self.inner.lock().await; // Guard the whole operation

        // Pass 1: Streaming through the file to find the latest sequence for each aggregate.
        let mut latest_map: HashMap<String, u64> = HashMap::new();
        let mut original_count = 0u64;
        
        {
            let mut stream = <Self as EventStore<E>>::read_all_from(self, 0);
            while let Some(res) = stream.next().await {
                let event = res?;
                original_count += 1;
                let entry = latest_map.entry(event.aggregate_id.clone()).or_insert(0);
                if event.global_sequence_num > *entry {
                    *entry = event.global_sequence_num;
                }
            }
        }

        // Apply "PruneIf" logic to our latest_map
        if let CompactionRule::PruneIf(predicate) = rule {
            // We need one more pass or we store the payloads. 
            // Better: find which aggregates satisfy PruneIf.
            let mut prune_set = std::collections::HashSet::new();
            let mut stream = <Self as EventStore<E>>::read_all_from(self, 0);
            while let Some(res) = stream.next().await {
                let event = res?;
                if let Some(&latest_seq) = latest_map.get(&event.aggregate_id) {
                    if event.global_sequence_num == latest_seq {
                        if (predicate)(&event.payload) {
                            prune_set.insert(event.aggregate_id.clone());
                        }
                    }
                }
            }
            for agg_id in prune_set {
                latest_map.remove(&agg_id);
            }
        }

        // Pass 2: Streaming through again and writing kept events to a temp file.
        let tmp_path = self.file_path.with_extension("compact.tmp");
        let mut tmp_file = File::create(&tmp_path).await?;
        let mut kept_count = 0u64;
        let mut max_seq = 0u64;
        let mut new_id_index = HashMap::new();

        {
            let mut stream = <Self as EventStore<E>>::read_all_from(self, 0);
            while let Some(res) = stream.next().await {
                let event = res?;
                if let Some(&latest_seq) = latest_map.get(&event.aggregate_id) {
                    if event.global_sequence_num == latest_seq {
                        let bytes = bincode::serialize(&event)?;
                        let len = bytes.len() as u32;
                        tmp_file.write_all(&len.to_le_bytes()).await?;
                        tmp_file.write_all(&bytes).await?;
                        
                        kept_count += 1;
                        max_seq = max_seq.max(event.global_sequence_num);
                        new_id_index.insert(event.id, event.global_sequence_num);
                    }
                }
            }
        }
        
        tmp_file.sync_all().await?;
        tokio::fs::rename(&tmp_path, &self.file_path).await?;

        // Update inner state
        guard.id_index = new_id_index;
        guard.next_global_seq = max_seq + 1;

        Ok(original_count - kept_count)
    }

    async fn truncate_before(&self, min_seq: u64) -> Result<u64, StoreError> {
        let mut guard = self.inner.lock().await;
        let tmp_path = self.file_path.with_extension("truncate.tmp");
        let mut tmp_file = File::create(&tmp_path).await?;
        
        let mut original_count = 0u64;
        let mut kept_count = 0u64;
        let mut max_seq = 0u64;
        let mut new_id_index = HashMap::new();

        {
            let mut stream = <Self as EventStore<E>>::read_all_from(self, 0);
            while let Some(res) = stream.next().await {
                let event = res?;
                original_count += 1;
                if event.global_sequence_num > min_seq {
                    let bytes = bincode::serialize(&event)?;
                    let len = bytes.len() as u32;
                    tmp_file.write_all(&len.to_le_bytes()).await?;
                    tmp_file.write_all(&bytes).await?;
                    
                    kept_count += 1;
                    max_seq = max_seq.max(event.global_sequence_num);
                    new_id_index.insert(event.id, event.global_sequence_num);
                }
            }
        }

        tmp_file.sync_all().await?;
        tokio::fs::rename(&tmp_path, &self.file_path).await?;

        guard.id_index = new_id_index;
        guard.next_global_seq = max_seq + 1;

        Ok(original_count - kept_count)
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
                eprintln!("DEBUG: SnapshotStore loaded {} at seq {}", projection_name, env.sequence_num);
                Ok(Some((env.sequence_num, env.state)))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                eprintln!("DEBUG: SnapshotStore found no snapshot for {}", projection_name);
                Ok(None)
            }
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
        let tmp_path = self
            .dir
            .join(format!("{}.{}.snapshot.tmp", projection_name, tmp_id));

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
        eprintln!("DEBUG: SnapshotStore saved {} at seq {}", projection_name, sequence_num);
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
        let dir = std::env::temp_dir().join(format!(
            "eventbus_test_{}",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_micros()
        ));
        let store = FileEventStore::new(&dir).await.unwrap();

        let event1 = Event::new(
            "entity-1",
            1,
            DummyEvent {
                value: "test1".into(),
            },
        );
        let event2 = Event::new(
            "entity-1",
            2,
            DummyEvent {
                value: "test2".into(),
            },
        );

        store
            .append(vec![event1.clone(), event2.clone()])
            .await
            .unwrap();

        let mut stream: BoxStream<'_, Result<Event<DummyEvent>, StoreError>> =
            store.read_all_from(0);

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
        let dir = std::env::temp_dir().join(format!(
            "compact_test_{}",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_micros()
        ));
        let store = FileEventStore::new(&dir).await.unwrap();

        // 3 events for aggregate "a", 1 for "b"
        let events = vec![
            Event::new("a", 1, DummyEvent { value: "a1".into() }),
            Event::new("a", 2, DummyEvent { value: "a2".into() }),
            Event::new("b", 1, DummyEvent { value: "b1".into() }),
            Event::new("a", 3, DummyEvent { value: "a3".into() }),
        ];
        store.append(events).await.unwrap();

        let removed = EventStore::<DummyEvent>::compact(&store, CompactionRule::LatestPerAggregate)
            .await
            .unwrap();
        assert_eq!(removed, 2); // a1 and a2 removed

        let mut stream: BoxStream<'_, Result<Event<DummyEvent>, StoreError>> =
            store.read_all_from(0);
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
        let dir = std::env::temp_dir().join(format!(
            "truncate_test_{}",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_micros()
        ));
        let store = FileEventStore::new(&dir).await.unwrap();

        let events = vec![
            Event::new("a", 1, DummyEvent { value: "e1".into() }),
            Event::new("a", 2, DummyEvent { value: "e2".into() }),
            Event::new("b", 1, DummyEvent { value: "e3".into() }),
        ];
        store.append(events).await.unwrap();

        // Truncate events with seq <= 2 (removes e1 seq=1, e2 seq=2)
        let removed = EventStore::<DummyEvent>::truncate_before(&store, 2)
            .await
            .unwrap();
        assert_eq!(removed, 2);

        let mut stream: BoxStream<'_, Result<Event<DummyEvent>, StoreError>> =
            store.read_all_from(0);
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
        let dir = std::env::temp_dir().join(format!(
            "snapshot_test_{}",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_micros()
        ));
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
        let dir = std::env::temp_dir().join(format!(
            "snap_del_test_{}",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_micros()
        ));
        let store = FileSnapshotStore::new(&dir).await.unwrap();

        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct S {
            v: u32,
        }

        SnapshotStore::<S>::save(&store, "proj", 10, &S { v: 1 })
            .await
            .unwrap();
        assert!(SnapshotStore::<S>::load(&store, "proj")
            .await
            .unwrap()
            .is_some());

        SnapshotStore::<S>::delete(&store, "proj").await.unwrap();
        assert!(SnapshotStore::<S>::load(&store, "proj")
            .await
            .unwrap()
            .is_none());

        // Deleting again is idempotent
        SnapshotStore::<S>::delete(&store, "proj").await.unwrap();

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }
}
