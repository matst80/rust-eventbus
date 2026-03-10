use rust_eventbus::event::{Event, EventPayload};
use rust_eventbus::store::{FileEventStore, EventStore};
use rust_eventbus::distributed::{TcpPubSub, NodeDiscovery, Node, DistributedError, DistributedPubSub, DiscoveryHandler};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::time::Duration;
use uuid::Uuid;
use async_trait::async_trait;
use futures::StreamExt;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct TestEvent {
    data: String,
}

impl EventPayload for TestEvent {
    fn event_type(&self) -> &'static str { "TestEvent" }
}

/// A "Split" Discovery implementation that allows us to simulate network partitions.
struct SplitDiscovery {
    my_id: Uuid,
    nodes: Arc<parking_lot::Mutex<Vec<Node>>>,
    partitioned: Arc<std::sync::atomic::AtomicBool>,
}

#[async_trait]
impl NodeDiscovery for SplitDiscovery {
    async fn discover_nodes(&self) -> Result<Vec<Node>, DistributedError> {
        let all_nodes = self.nodes.lock().clone();
        if self.partitioned.load(std::sync::atomic::Ordering::Relaxed) {
             // Return only ourself to trigger removal of others
             return Ok(all_nodes.into_iter().filter(|n| n.id == self.my_id).collect());
        }
        Ok(all_nodes)
    }
    async fn register(&self, node: Node) -> Result<(), DistributedError> {
        self.nodes.lock().push(node);
        Ok(())
    }
    async fn unregister(&self, _node: &Node) -> Result<(), DistributedError> { Ok(()) }
    async fn watch(&self, handler: Arc<dyn DiscoveryHandler>) -> Result<(), DistributedError> {
        let mut last_nodes = std::collections::HashSet::<String>::new();
        loop {
            if let Ok(nodes) = self.discover_nodes().await {
                let current_addrs: std::collections::HashSet<String> = nodes.iter().map(|n| n.address.clone()).collect();
                
                // Added
                for node in &nodes {
                    if !last_nodes.contains(&node.address) {
                        handler.on_node_added(node.clone()).await;
                    }
                }
                
                // Removed (Not explicitly needed for the test's original logic but good for completeness)
                // Actually we NEED removals to test the new functionality!
                for addr in &last_nodes {
                    if !current_addrs.contains(addr) {
                        handler.on_node_removed(Node { id: Uuid::nil(), address: addr.clone() }).await;
                    }
                }
                
                last_nodes = current_addrs;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

#[tokio::test]
async fn test_severe_split_brain_divergence() {
    let temp_dir = std::env::temp_dir().join(format!("split_brain_{}", Uuid::new_v4()));
    tokio::fs::create_dir_all(&temp_dir).await.unwrap();

    let node1_dir = temp_dir.join("node1");
    let node2_dir = temp_dir.join("node2");
    tokio::fs::create_dir_all(&node1_dir).await.unwrap();
    tokio::fs::create_dir_all(&node2_dir).await.unwrap();

    let node1_id = Uuid::new_v4();
    let node2_id = Uuid::new_v4();

    let discovery1 = Arc::new(SplitDiscovery {
        my_id: node1_id,
        nodes: Arc::new(parking_lot::Mutex::new(vec![])),
        partitioned: Arc::new(std::sync::atomic::AtomicBool::new(false)),
    });
    let discovery2 = Arc::new(SplitDiscovery {
        my_id: node2_id,
        nodes: Arc::new(parking_lot::Mutex::new(vec![])),
        partitioned: Arc::new(std::sync::atomic::AtomicBool::new(false)),
    });

    // Node 1 setup
    let mesh1: Arc<TcpPubSub<TestEvent>> = Arc::new(TcpPubSub::new_with_advertised_addr(
        node1_id,
        "127.0.0.1:15001".to_string(),
        "127.0.0.1:15001".to_string(),
        discovery1.clone(),
    ));
    let store1 = Arc::new(FileEventStore::new(node1_dir.join("events.bin")).await.unwrap());

    // Node 2 setup
    let mesh2: Arc<TcpPubSub<TestEvent>> = Arc::new(TcpPubSub::new_with_advertised_addr(
        node2_id,
        "127.0.0.1:15002".to_string(),
        "127.0.0.1:15002".to_string(),
        discovery2.clone(),
    ));
    let store2 = Arc::new(FileEventStore::new(node2_dir.join("events.bin")).await.unwrap());

    // Register nodes
    discovery1.register(Node { id: node1_id, address: "127.0.0.1:15001".to_string() }).await.unwrap();
    discovery1.register(Node { id: node2_id, address: "127.0.0.1:15002".to_string() }).await.unwrap();
    discovery2.register(Node { id: node1_id, address: "127.0.0.1:15001".to_string() }).await.unwrap();
    discovery2.register(Node { id: node2_id, address: "127.0.0.1:15002".to_string() }).await.unwrap();

    // Start mesh listeners (simplified version of the loop in todo_app.rs)
    let store1_c = store1.clone();
    let mut sub1 = mesh1.subscribe().await;
    tokio::spawn(async move {
        while let Some(Ok(ev)) = sub1.next().await {
            let _ = store1_c.append(vec![ev]).await;
        }
    });

    let store2_c = store2.clone();
    let mut sub2 = mesh2.subscribe().await;
    tokio::spawn(async move {
        while let Some(Ok(ev)) = sub2.next().await {
            let _ = store2_c.append(vec![ev]).await;
        }
    });

    // Start mesh managers
    mesh1.clone().start_background_manager().await;
    mesh2.clone().start_background_manager().await;

    // Allow time for background connection tasks to establish connections
    tokio::time::sleep(Duration::from_millis(500)).await;

    // --- PHASE 1: Healthy Cluster ---
    let ev1 = Event::new("shared", 1, TestEvent { data: "initial".into() });
    let stored = store1.append(vec![ev1.clone()]).await.unwrap();
    mesh1.publish(&stored[0]).await.unwrap();

    // Allow time for propagation
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify both have it
    let count1 = EventStore::<TestEvent>::read_all_from(store1.as_ref(), 0).count().await;
    let count2 = EventStore::<TestEvent>::read_all_from(store2.as_ref(), 0).count().await;
    assert_eq!(count1, 1);
    assert_eq!(count2, 1);

    // --- PHASE 2: Severe Split Brain (Partition) ---
    discovery1.partitioned.store(true, std::sync::atomic::Ordering::Relaxed);
    discovery2.partitioned.store(true, std::sync::atomic::Ordering::Relaxed);

    // Node 1 writes while partitioned
    let ev2_node1 = Event::new("shared", 2, TestEvent { data: "node1_only".into() });
    let stored1 = store1.append(vec![ev2_node1]).await.unwrap();
    // Allow time for discovery cache to expire (5s) and connections to be dropped
    tokio::time::sleep(Duration::from_secs(6)).await;
    mesh1.publish(&stored1[0]).await.unwrap();

    // Node 2 writes while partitioned
    let ev2_node2 = Event::new("shared", 2, TestEvent { data: "node2_only".into() });
    let stored2 = store2.append(vec![ev2_node2]).await.unwrap();
    mesh2.publish(&stored2[0]).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify divergence: Both nodes now have a DIFFERENT event at global_sequence_num 2
    let s1_events: Vec<Result<Event<TestEvent>, _>> = EventStore::<TestEvent>::read_all_from(store1.as_ref(), 0).collect().await;
    let s2_events: Vec<Result<Event<TestEvent>, _>> = EventStore::<TestEvent>::read_all_from(store2.as_ref(), 0).collect().await;

    println!("Node 1 Phase 2 events:");
    for (i, ev) in s1_events.iter().enumerate() {
        println!("  {}: seq={}, data={}", i, ev.as_ref().unwrap().global_sequence_num, ev.as_ref().unwrap().payload.data);
    }
    println!("Node 2 Phase 2 events:");
    for (i, ev) in s2_events.iter().enumerate() {
        println!("  {}: seq={}, data={}", i, ev.as_ref().unwrap().global_sequence_num, ev.as_ref().unwrap().payload.data);
    }

    assert_eq!(s1_events.len(), 2, "Node 1 should have exactly 2 events");
    assert_eq!(s2_events.len(), 2, "Node 2 should have exactly 2 events");
    assert_ne!(s1_events[1].as_ref().unwrap().payload.data, s2_events[1].as_ref().unwrap().payload.data);
    println!("Diverged! Node 1 Seq 2: {}, Node 2 Seq 2: {}", 
        s1_events[1].as_ref().unwrap().payload.data, 
        s2_events[1].as_ref().unwrap().payload.data);

    // --- PHASE 3: Heal Partition (The "Severe" part) ---
    discovery1.partitioned.store(false, std::sync::atomic::Ordering::Relaxed);
    discovery2.partitioned.store(false, std::sync::atomic::Ordering::Relaxed);

    // Allow time for reconnection
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Node 1 writes a new event after healing
    let ev3_node1 = Event::new("shared", 3, TestEvent { data: "after_heal".into() });
    let stored3 = store1.append(vec![ev3_node1]).await.unwrap();
    mesh1.publish(&stored3[0]).await.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Now look at Node 2's store
    let s2_final: Vec<Result<Event<TestEvent>, _>> = EventStore::<TestEvent>::read_all_from(store2.as_ref(), 0).collect().await;
    // Node 2 will have:
    // 1. Initial (seq 1)
    // 2. node2_only (seq 2) - its OWN version
    // 3. after_heal (seq 3) - received from Node 1
    // It TOTALLY MISSED node1_only (seq 2)!
    
    println!("Node 2 events:");
    for (i, ev) in s2_final.iter().enumerate() {
        println!("  {}: seq={}, data={}", i, ev.as_ref().unwrap().global_sequence_num, ev.as_ref().unwrap().payload.data);
    }

    assert_eq!(s2_final.len(), 3);
    // The "after_heal" event successfully propagated, but the gap at sequence 2 remains diverged.
    
    // Clean up
    tokio::fs::remove_dir_all(&temp_dir).await.unwrap();
}
