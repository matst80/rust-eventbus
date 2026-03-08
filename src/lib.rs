//! rust-eventbus
//!
//! A robust, asynchronous event bus and projection manager written in idiomatic Rust.

pub mod bus;
pub mod distributed;
pub mod cluster;
pub mod event;
pub mod projection;
pub mod store;
pub mod libp2p_adapter;
#[cfg(feature = "libp2p-backend")]
pub mod libp2p_swarm;

pub use bus::{EventBus, SubscriberId};
pub use event::{Event, EventPayload};
pub use projection::ProjectionHandle;
