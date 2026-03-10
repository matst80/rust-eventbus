use async_trait::async_trait;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::{oneshot, Mutex};
use uuid::Uuid;

use crate::distributed::types::{DistributedError, LockError};
use crate::distributed::NodeDiscovery;
use crate::distributed::ProjectionLockManager;
use crate::distributed::PubSubBackend;

/// Simple quorum-based lock manager that negotiates ownership by broadcasting
/// proposals on the mesh `locks` topic and collecting votes. This is intentionally
/// lightweight — it relies on the mesh' pubsub for message exchange and an
/// observed-node set derived from incoming lock messages to approximate cluster
/// membership for majority calculations.
///
/// Caveats:
/// - This is NOT a full Raft implementation. It is a pragmatic approach to avoid
///   relying on local filesystem locks in multi-host environments.
/// - For stronger guarantees consider integrating a proven consensus library.
#[derive(Clone)]
pub struct QuorumLockManager {
    node_id: Uuid,
    backend: Arc<dyn PubSubBackend>,
    // Node discovery provider used to compute authoritative membership for majority.
    discovery: Arc<dyn NodeDiscovery>,
    // Pending proposals waiting for votes keyed by projection name.
    pending: Arc<Mutex<HashMap<String, ProposalState>>>,
    // Current owners with last heartbeat timestamp
    owners: Arc<Mutex<HashMap<String, OwnerInfo>>>,
    // Lease TTL used to declare owners stale if no heartbeat seen.
    lease_ttl: Duration,
}

struct ProposalState {
    proposer: Uuid,
    term: u64,
    votes_for: usize,
    votes_against: usize,
    voters_seen: HashSet<Uuid>,
    complete_tx: Option<oneshot::Sender<bool>>,
}

struct OwnerInfo {
    owner: Uuid,
    last_seen: Instant,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum LockMessage {
    Proposal {
        projection: String,
        proposer: Uuid,
        term: u64,
        timestamp_millis: u128,
    },
    Vote {
        projection: String,
        voter: Uuid,
        term: u64,
        accept: bool,
    },
    Granted {
        projection: String,
        owner: Uuid,
        term: u64,
    },
    Heartbeat {
        projection: String,
        owner: Uuid,
        term: u64,
    },
}

impl QuorumLockManager {
    pub fn new(
        node_id: Uuid,
        backend: Arc<dyn PubSubBackend>,
        discovery: Arc<dyn NodeDiscovery>,
        lease_ttl: Duration,
    ) -> Self {
        Self {
            node_id,
            backend,
            discovery,
            pending: Arc::new(Mutex::new(HashMap::new())),
            owners: Arc::new(Mutex::new(HashMap::new())),
            lease_ttl,
        }
    }

    /// Start background listener for lock topic messages. This must be started
    /// once after creating the manager so it can respond to proposals and gather votes.
    pub async fn start_listener(self: Arc<Self>) -> Result<(), DistributedError> {
        let mut stream = self.backend.subscribe_bytes("locks");
        let me = self.clone();

        // Spawn a task to consume incoming lock messages.
        tokio::spawn(async move {
            while let Some(res) = stream.next().await {
                match res {
                    Ok(bytes) => {
                        match bincode::deserialize::<LockMessage>(&bytes) {
                            Ok(msg) => {
                                // best-effort: ignore errors
                                let _ = me.handle_incoming(msg).await;
                            }
                            Err(_) => {
                                // ignore malformed messages
                            }
                        }
                    }
                    Err(_) => {
                        // ignore transient backend errors
                    }
                }
            }
        });

        Ok(())
    }

    async fn broadcast(&self, msg: &LockMessage) -> Result<(), LockError> {
        let bytes =
            bincode::serialize(msg).map_err(|e| LockError::Storage(format!("serialize: {}", e)))?;
        self.backend
            .publish_bytes("locks", bytes)
            .await
            .map_err(|e| LockError::Network(format!("pubsub publish failed: {:?}", e)))?;
        Ok(())
    }

    async fn handle_incoming(&self, msg: LockMessage) -> Result<(), ()> {
        match msg {
            LockMessage::Proposal {
                projection,
                proposer: _,
                term,
                timestamp_millis: _,
            } => {
                // Simple policy: accept proposal if there's no current owner or owner is stale,
                // otherwise reject. In a more advanced setup we'd persist last-voted-term etc.
                let accept = {
                    let owners = self.owners.lock().await;
                    match owners.get(&projection) {
                        Some(owner_info) => {
                            // owner still fresh?
                            owner_info.last_seen.elapsed() > self.lease_ttl
                        }
                        None => true,
                    }
                };

                let vote = LockMessage::Vote {
                    projection: projection.clone(),
                    voter: self.node_id,
                    term,
                    accept,
                };

                // Fire-and-forget best-effort vote
                let _ = self.broadcast(&vote).await;
            }
            LockMessage::Vote {
                projection,
                voter,
                term,
                accept,
            } => {
                // Compute authoritative majority using discovery BEFORE acquiring the pending lock.
                let discovered_total = match self.discovery.discover_nodes().await {
                    Ok(nodes) => nodes.len().max(1),
                    Err(_) => 1usize,
                };
                let min_required = (discovered_total / 2) + 1;

                // If we have a matching pending proposal, tally the vote.
                // We'll compute whether an election succeeded while holding the lock,
                // then perform broadcasting after we release the lock to avoid awaiting
                // while holding the `pending` mutex.
                let mut maybe_result: Option<(bool, Uuid, u64)> = None; // (accepted, owner, term)

                {
                    let mut pending = self.pending.lock().await;
                    if let Some(state) = pending.get_mut(&projection) {
                        if state.term == term && !state.voters_seen.contains(&voter) {
                            state.voters_seen.insert(voter);
                            if accept {
                                state.votes_for += 1;
                            } else {
                                state.votes_against += 1;
                            }

                            if state.votes_for >= min_required {
                                // success: capture necessary fields and remove pending entry safely
                                let proposer_id = state.proposer;
                                let term_v = state.term;
                                let maybe_tx = state.complete_tx.take();
                                pending.remove(&projection);
                                if let Some(tx) = maybe_tx {
                                    let _ = tx.send(true);
                                }
                                maybe_result = Some((true, proposer_id, term_v));
                            } else if state.votes_against >= min_required {
                                // lost election
                                let proposer_id = state.proposer;
                                let term_v = state.term;
                                let maybe_tx = state.complete_tx.take();
                                pending.remove(&projection);
                                if let Some(tx) = maybe_tx {
                                    let _ = tx.send(false);
                                }
                                maybe_result = Some((false, proposer_id, term_v));
                            } else {
                                // keep waiting
                            }
                        }
                    }
                }

                // If we decided the election outcome, broadcast Granted if accepted.
                if let Some((accepted, owner, term)) = maybe_result {
                    if accepted {
                        let granted = LockMessage::Granted {
                            projection: projection.clone(),
                            owner,
                            term,
                        };
                        let _ = self.broadcast(&granted).await;
                    }
                }
            }
            LockMessage::Granted {
                projection,
                owner,
                term: _,
            } => {
                let mut owners = self.owners.lock().await;
                owners.insert(
                    projection,
                    OwnerInfo {
                        owner,
                        last_seen: Instant::now(),
                    },
                );
            }
            LockMessage::Heartbeat {
                projection,
                owner,
                term: _,
            } => {
                let mut owners = self.owners.lock().await;
                owners.insert(
                    projection,
                    OwnerInfo {
                        owner,
                        last_seen: Instant::now(),
                    },
                );
            }
        }
        Ok(())
    }

    // Helper to mark owner stale if necessary
    async fn owner_is_stale(&self, projection: &str) -> bool {
        let owners = self.owners.lock().await;
        match owners.get(projection) {
            Some(info) => info.last_seen.elapsed() > self.lease_ttl,
            None => true,
        }
    }

    // Helper to cleanup expired owners periodically (not strictly required, but helpful)
    #[allow(dead_code)]
    async fn prune_stale_owners(&self) {
        let mut owners = self.owners.lock().await;
        let ttl = self.lease_ttl;
        owners.retain(|_, info| info.last_seen.elapsed() <= ttl);
    }
}

#[async_trait]
impl ProjectionLockManager for QuorumLockManager {
    async fn acquire_lock(&self, projection_name: &str, node_id: &Uuid) -> Result<bool, LockError> {
        // Basic sanity: ensure caller's node_id matches manager node (best-effort)
        if *node_id != self.node_id {
            // allow but warn: caller provided a different node id; use manager node_id
        }

        // Prepare a persistent term file location using NODE_DATA_DIR or default ./data.
        let node_dir = std::env::var("NODE_DATA_DIR").unwrap_or_else(|_| "./data".to_string());
        let lock_dir = format!("{}/locks", node_dir);
        let term_path = format!("{}/{}.term", lock_dir, projection_name);

        // Ensure lock dir exists (best-effort)
        if let Err(e) = fs::create_dir_all(&lock_dir).await {
            return Err(LockError::Storage(format!(
                "failed to create lock dir {}: {}",
                lock_dir, e
            )));
        }

        // Read last voted term from disk, fall back to 0.
        let mut last_voted_term: u64 = match fs::read_to_string(&term_path).await {
            Ok(s) => s.trim().parse::<u64>().unwrap_or(0),
            Err(_) => 0,
        };

        // Max attempts / backoff configuration
        let max_attempts = 5usize;
        let mut attempt = 0usize;

        loop {
            // Ensure mesh quorum is reachable first
            self.backend
                .check_quorum()
                .await
                .map_err(|e| LockError::Network(format!("quorum check failed: {:?}", e)))?;

            // If an owner exists and is not stale, we cannot acquire
            if !self.owner_is_stale(projection_name).await {
                return Ok(false);
            }

            // Compute a candidate term: monotonic greater than last_voted_term
            let now_term = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            let candidate_term = if now_term > last_voted_term {
                now_term
            } else {
                last_voted_term.saturating_add(1)
            };

            // Persist last-voted-term to disk before proposing (durable vote)
            let term_bytes = candidate_term.to_string().into_bytes();
            match fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&term_path)
                .await
            {
                Ok(mut f) => {
                    if let Err(e) = f.write_all(&term_bytes).await {
                        return Err(LockError::Storage(format!(
                            "failed to write term file: {}",
                            e
                        )));
                    }
                }
                Err(e) => {
                    return Err(LockError::Storage(format!(
                        "failed to open term file: {}",
                        e
                    )));
                }
            }

            // create oneshot to wait for election result
            let (tx, rx) = oneshot::channel();

            {
                let mut pending = self.pending.lock().await;
                pending.insert(
                    projection_name.to_string(),
                    ProposalState {
                        proposer: self.node_id,
                        term: candidate_term,
                        votes_for: 0,
                        votes_against: 0,
                        voters_seen: HashSet::new(),
                        complete_tx: Some(tx),
                    },
                );
            }

            // Broadcast proposal
            let proposal = LockMessage::Proposal {
                projection: projection_name.to_string(),
                proposer: self.node_id,
                term: candidate_term,
                timestamp_millis: SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_millis())
                    .unwrap_or(0),
            };

            if let Err(e) = self.broadcast(&proposal).await {
                // failed to broadcast: cleanup and consider retrying
                let mut pending = self.pending.lock().await;
                let _ = pending.remove(projection_name);
                return Err(LockError::Network(format!("broadcast error: {:?}", e)));
            }

            // The proposer implicitly votes for itself
            {
                let mut pending = self.pending.lock().await;
                if let Some(state) = pending.get_mut(projection_name) {
                    state.voters_seen.insert(self.node_id);
                    state.votes_for += 1;
                }
            }

            // Wait for election outcome with per-attempt timeout.
            let sleep_future = tokio::time::sleep(Duration::from_millis(1500));
            tokio::pin!(sleep_future);

            let mut won = false;
            tokio::select! {
                _ = &mut sleep_future => {
                    // attempt timed out: remove pending and retry with backoff
                    let mut pending = self.pending.lock().await;
                    if let Some(state) = pending.remove(projection_name) {
                        if let Some(tx) = state.complete_tx {
                            let _ = tx.send(false);
                        }
                    }
                }
                res = rx => {
                    match res {
                        Ok(accepted) => {
                            if accepted {
                                // Broadcast Granted (owner announcement)
                                let granted = LockMessage::Granted {
                                    projection: projection_name.to_string(),
                                    owner: self.node_id,
                                    term: candidate_term,
                                };
                                let _ = self.broadcast(&granted).await;
                                // store owner info
                                let mut owners = self.owners.lock().await;
                                owners.insert(projection_name.to_string(), OwnerInfo { owner: self.node_id, last_seen: Instant::now() });
                                // persist last_voted_term as candidate_term (already written earlier)
                                last_voted_term = candidate_term;
                                won = true;
                            } else {
                                // Lost election immediately; fallthrough to retry/backoff
                                // Read updated last_voted_term from disk (other nodes may have higher term)
                                last_voted_term = match fs::read_to_string(&term_path).await {
                                    Ok(s) => s.trim().parse::<u64>().unwrap_or(last_voted_term),
                                    Err(_) => last_voted_term,
                                };
                            }
                        }
                        Err(_) => {
                            // channel closed, treat as failure
                        }
                    }
                }
            }

            if won {
                return Ok(true);
            }

            // Backoff before next attempt
            attempt = attempt.saturating_add(1);
            if attempt >= max_attempts {
                break;
            }
            let multiplier = 1u64.checked_shl(attempt as u32).unwrap_or(1);
            let mut backoff_ms = 200u64.saturating_mul(multiplier);
            if backoff_ms > 2000 {
                backoff_ms = 2000;
            }
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;

            // refresh last_voted_term from disk before retry
            last_voted_term = match fs::read_to_string(&term_path).await {
                Ok(s) => s.trim().parse::<u64>().unwrap_or(last_voted_term),
                Err(_) => last_voted_term,
            };
        }

        // If we exit loop without success, return failure (No quorum / election unsuccessful)
        Err(LockError::Network(
            "failed to acquire lock after retries".into(),
        ))
    }

    async fn keep_alive(&self, projection_name: &str, node_id: &Uuid) -> Result<(), LockError> {
        if *node_id != self.node_id {
            // best-effort: disallow keep_alive for foreign node ids
            return Err(LockError::Network(
                "keep_alive for mismatched node id".into(),
            ));
        }

        // broadcast heartbeat
        let hb = LockMessage::Heartbeat {
            projection: projection_name.to_string(),
            owner: self.node_id,
            term: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        };

        self.broadcast(&hb).await
    }

    async fn release_lock(&self, projection_name: &str, node_id: &Uuid) -> Result<(), LockError> {
        // only owner may release (best-effort check)
        let mut owners = self.owners.lock().await;
        if let Some(info) = owners.get(projection_name) {
            if info.owner != *node_id {
                // not owner: no-op
                return Ok(());
            }
        } else {
            // no owner: no-op
            return Ok(());
        }

        // announce released owner as nil
        let granted = LockMessage::Granted {
            projection: projection_name.to_string(),
            owner: Uuid::nil(),
            term: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        };

        // remove owner locally
        owners.remove(projection_name);

        self.broadcast(&granted).await
    }
}
