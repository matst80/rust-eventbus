//! Real libp2p gossipsub+mdns swarm integration.
#![cfg(feature = "libp2p-backend")]

use std::error::Error;
use futures::prelude::*;
use tokio::sync::{broadcast, mpsc};
use tracing::{info, warn};

use libp2p::{identity, PeerId, swarm::Swarm, swarm::SwarmEvent};
use libp2p_gossipsub::{Gossipsub, GossipsubConfig, GossipsubEvent, MessageAuthenticity, IdentTopic as Topic};
use libp2p_mdns::{Mdns, MdnsEvent};

#[derive(Debug)]
pub struct SwarmHandle {
    pub publish_tx: mpsc::Sender<Vec<u8>>,
    pub inbound_tx: broadcast::Sender<Vec<u8>>,
}

/// Spawn a gossipsub+mdns swarm and return handles for publish/subscribe.
pub async fn spawn_swarm(listen_addr: Option<String>) -> Result<SwarmHandle, Box<dyn Error + Send + Sync>> {
    let (inbound_tx, _inbound_rx) = broadcast::channel(512);
    let (publish_tx, mut publish_rx) = mpsc::channel::<Vec<u8>>(64);

    // identity
    let local_key = identity::Keypair::generate_ed25519();
    let local_peer_id = PeerId::from(local_key.public());
    info!("Local peer id: {}", local_peer_id);

    // transport helper (use development transport helper if available)
    let transport = match libp2p::development_transport(local_key.clone()).await {
        Ok(t) => t,
        Err(_) => libp2p::tokio_development_transport(local_key.clone()).await?,
    };

    // gossipsub
    let gossipsub_config = GossipsubConfig::default();
    let mut gossipsub = Gossipsub::new(MessageAuthenticity::Signed(local_key.clone()), gossipsub_config)?;

    // mdns
    let mdns = Mdns::new(Default::default()).await?;

    #[derive(libp2p::swarm::NetworkBehaviour)]
    #[behaviour(out_event = "OutEvent")]
    struct Behaviour {
        gossipsub: Gossipsub,
        mdns: Mdns,
    }

    #[derive(Debug)]
    enum OutEvent {
        Gossipsub(GossipsubEvent),
        Mdns(MdnsEvent),
    }

    impl From<GossipsubEvent> for OutEvent { fn from(v: GossipsubEvent) -> Self { OutEvent::Gossipsub(v) } }
    impl From<MdnsEvent> for OutEvent { fn from(v: MdnsEvent) -> Self { OutEvent::Mdns(v) } }

    let behaviour = Behaviour { gossipsub, mdns };

    let mut swarm = Swarm::new(transport, behaviour, local_peer_id, Default::default());

    let topic = Topic::new("eventbus");
    swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

    if let Some(addr) = listen_addr {
        if let Ok(ma) = addr.parse() {
            if let Err(e) = swarm.listen_on(ma) {
                warn!("listen_on failed: {:?}", e);
            }
        }
    }

    let inbound_tx_clone = inbound_tx.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                maybe_cmd = publish_rx.recv() => {
                    if let Some(payload) = maybe_cmd {
                        if let Err(e) = swarm.behaviour_mut().gossipsub.publish(topic.clone(), payload) {
                            warn!("gossipsub publish error: {:?}", e);
                        }
                    } else { break; }
                }
                event = swarm.select_next_some() => match event {
                    SwarmEvent::Behaviour(OutEvent::Gossipsub(GossipsubEvent::Message { message, .. })) => {
                        let _ = inbound_tx_clone.send(message.data.clone());
                    }
                    SwarmEvent::Behaviour(OutEvent::Mdns(MdnsEvent::Discovered(list))) => {
                        for (peer, _addr) in list {
                            info!("mdns discovered: {}", peer);
                            let _ = swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer);
                        }
                    }
                    SwarmEvent::Behaviour(OutEvent::Mdns(MdnsEvent::Expired(list))) => {
                        for (peer, _addr) in list {
                            info!("mdns expired: {}", peer);
                            let _ = swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer);
                        }
                    }
                    SwarmEvent::NewListenAddr { address, .. } => { info!("listening on {}", address); }
                    _ => {}
                }
            }
        }
    });

    Ok(SwarmHandle { publish_tx, inbound_tx })
}
