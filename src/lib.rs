//! rust-eventbus
//!
//! A robust, asynchronous event bus and projection manager written in idiomatic Rust.

pub mod bus;
pub mod event;
pub mod projection;
pub mod store;

pub use bus::{EventBus, SubscriberId};
pub use event::{Event, EventPayload};
