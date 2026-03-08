// libp2p demo scaffold
// Run with: `cargo run --example libp2p_demo --features libp2p-backend`

#[cfg(not(feature = "libp2p-backend"))]
fn main() {
    println!(
        "libp2p example is behind the `libp2p-backend` feature.\nRun: cargo run --example libp2p_demo --features libp2p-backend"
    );
}

#[cfg(feature = "libp2p-backend")]
#[tokio::main]
async fn main() {
    use std::sync::Arc;
    use rust_eventbus::event::{Event, EventPayload};
    use rust_eventbus::distributed::{BackendPubSub, DistributedPubSub};
    use futures::StreamExt;
    use rust_eventbus::libp2p_adapter::Libp2pAdapter;

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct DemoPayload {
        pub msg: String,
    }

    impl EventPayload for DemoPayload {
        fn event_type(&self) -> &'static str {
            "demo.payload"
        }
    }

    // Create the libp2p adapter and backend wrapper
    let adapter = Libp2pAdapter::new("rust-eventbus-topic").await.expect("libp2p init");
    let backend: Arc<dyn rust_eventbus::distributed::PubSubBackend> = adapter;
    let backend_pub = BackendPubSub::new(backend, "rust-eventbus-topic".to_string());

    // Subscribe and print incoming events
    let mut sub = <BackendPubSub as DistributedPubSub<DemoPayload>>::subscribe(&backend_pub).await;
    tokio::spawn(async move {
        while let Some(res) = sub.next().await {
            match res {
                Ok(ev) => println!("INBOUND event: {:?}", ev),
                Err(e) => eprintln!("inbound error: {}", e),
            }
        }
    });

    // Publish a test event
    let payload = DemoPayload { msg: "hello from libp2p demo".into() };
    let event = Event::new("demo-agg", 1, payload.clone());
    <BackendPubSub as DistributedPubSub<DemoPayload>>::publish(&backend_pub, &event).await.expect("publish");

    // Keep the program alive briefly to allow message propagation
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
}
