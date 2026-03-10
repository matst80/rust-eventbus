//! rust-eventbus
//!
//! A robust, asynchronous event bus and projection manager written in idiomatic Rust.

pub mod bus;
pub mod cluster;
pub mod cluster_config;
pub mod distributed;
pub mod event;
pub mod projection;
pub mod store;
pub mod task_limiter;

pub use bus::{EventBus, SubscriberId};
pub use event::{Event, EventPayload};
pub use projection::ProjectionHandle;
