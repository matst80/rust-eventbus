// Simple demo showing `TcpPubSubBackend` subscribe path and a simulated remote sender.
// Run: `cargo run --example tcp_demo`

#[tokio::main]
async fn main() {
    use std::sync::Arc;
    use uuid::Uuid;
    use futures::StreamExt;
    use tokio::io::AsyncWriteExt;
    use rust_eventbus::distributed::{TcpPubSubBackend, BackendPubSub, EnvironmentNodeDiscovery, DistributedPubSub};

    // node identity and listen addr
    let node_id = Uuid::new_v4();
    let listen = "127.0.0.1:4006".to_string();
    let discovery = Arc::new(EnvironmentNodeDiscovery::new());

    let backend = TcpPubSubBackend::new_with_advertised_addr(node_id, listen.clone(), listen.clone(), discovery);
    let backend_arc: Arc<dyn rust_eventbus::distributed::PubSubBackend> = Arc::new(backend);
    let backend_pub = BackendPubSub::new(backend_arc, "ignored-topic".to_string());

    // demo payload type
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct DemoPayload {
        pub msg: String,
    }

    impl rust_eventbus::event::EventPayload for DemoPayload {
        fn event_type(&self) -> &'static str {
            "demo.payload"
        }
    }

    // subscribe for Event<DemoPayload>
    let mut sub = <BackendPubSub as DistributedPubSub<DemoPayload>>::subscribe(&backend_pub).await;
    tokio::spawn(async move {
        while let Some(res) = sub.next().await {
            match res {
                Ok(event) => println!("INBOUND event.payload: {:?}", event.payload),
                Err(e) => eprintln!("inbound error: {}", e),
            }
        }
    });

    // Simulate a remote peer by connecting and sending a framed message containing a bincode-serialized Event<DemoPayload>
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let mut stream = tokio::net::TcpStream::connect(&listen).await.expect("connect");
    let fake_sender = Uuid::new_v4();
    let payload = DemoPayload { msg: "hello-from-remote".into() };
    let event = rust_eventbus::event::Event::new("demo-agg", 1, payload);
    let serialized = bincode::serialize(&event).expect("bincode");
    let total_len = (16 + serialized.len()) as u32;
    let mut buf = Vec::new();
    buf.extend_from_slice(&total_len.to_be_bytes());
    buf.extend_from_slice(fake_sender.as_bytes());
    buf.extend_from_slice(&serialized);
    let _ = stream.write_all(&buf).await;

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
}
