// libp2p adapter implementing PubSubBackend using Gossipsub
#[cfg(feature = "libp2p-backend")]
mod libp2p_adapter {
    use futures::stream::{StreamExt, TryStreamExt};
    use libp2p_gossipsub::{Gossipsub, GossipsubConfig, GossipsubEvent, IdentTopic as Topic, MessageAuthenticity};
    use libp2p::identity;
    use libp2p::swarm::{Swarm, SwarmEvent};
    use libp2p::PeerId;
    use std::error::Error;
    use std::sync::Arc;
    use tokio::sync::{mpsc, broadcast};
    use tokio_stream::wrappers::{BroadcastStream, ReceiverStream};
    use futures::stream::BoxStream;

    use crate::distributed::{DistributedError, PubSubBackend};

    // A simple Gossipsub-based backend that publishes raw bytes to a topic and
    // exposes an async stream of incoming payloads.
    struct PublishCmd {
        topic: String,
        payload: Vec<u8>,
    }

    pub struct Libp2pAdapter {
        topic: String,
        publish_tx: mpsc::Sender<PublishCmd>,
        inbound_tx: broadcast::Sender<Vec<u8>>,
    }

    impl Libp2pAdapter {
        pub async fn new(topic: &str) -> Result<Arc<Self>, Box<dyn Error + Send + Sync>> {
            let local_key = identity::Keypair::generate_ed25519();
            let peer_id = PeerId::from(local_key.public());

            let transport = libp2p::tokio_development_transport(local_key.clone()).await?;

            let gossipsub_config = GossipsubConfig::default();
            let mut gossipsub = Gossipsub::new(MessageAuthenticity::Signed(local_key.clone()), gossipsub_config)
                .map_err(|e| format!("gossipsub init: {}", e))?;

            let topic_name = topic.to_string();
            let topic_obj = Topic::new(topic_name.clone());
            let _ = gossipsub.subscribe(&topic_obj);

            let mut swarm = Swarm::new(transport, gossipsub, peer_id, libp2p::swarm::Config::with_tokio_executor());

            // Channel for delivering inbound messages to consumers (broadcast so multiple subscribers can receive)
            let (inbound_tx, _inbound_rx) = broadcast::channel(256);

            // Channel for receiving publish commands from callers
            let (publish_tx, mut publish_rx) = mpsc::channel::<PublishCmd>(32);

            // Spawn the swarm event loop
            let inbound_tx_clone = inbound_tx.clone();
            let _topic_name_clone = topic_name.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        Some(cmd) = publish_rx.recv() => {
                            // publish via gossipsub
                            let t = Topic::new(cmd.topic);
                            let _ = swarm.behaviour_mut().publish(t, cmd.payload);
                        }
                        event = swarm.select_next_some() => {
                            match event {
                                SwarmEvent::Behaviour(GossipsubEvent::Message { propagation_source: _src, message_id: _id, message }) => {
                                    let _ = inbound_tx_clone.send(message.data.clone());
                                }
                                _ => {}
                            }
                        }
                    }
                }
            });

            Ok(Arc::new(Self { topic: topic.to_string(), publish_tx, inbound_tx }))
        }
    }

    #[async_trait::async_trait]
    impl PubSubBackend for Libp2pAdapter {
        async fn publish_bytes(&self, topic: &str, payload: Vec<u8>) -> Result<(), DistributedError> {
            let cmd = PublishCmd { topic: topic.to_string(), payload };
            self.publish_tx
                .send(cmd)
                .await
                .map_err(|e| DistributedError::Network(format!("publish channel closed: {}", e)))
        }

        fn subscribe_bytes(&self, _topic: &str) -> BoxStream<'static, Result<Vec<u8>, DistributedError>> {
            let rx = self.inbound_tx.subscribe();
            let stream = BroadcastStream::new(rx).map(|res| res.map_err(|e| DistributedError::Network(e.to_string())));
            Box::pin(stream)
        }
    }
}

#[cfg(feature = "libp2p-backend")]
pub use libp2p_adapter::Libp2pAdapter;
