use tokio::sync::broadcast;
use uuid::Uuid;

use crate::{Event, EventPayload};

/// A unique token representing a subscriber.
///
/// In Rust, `tokio::sync::broadcast` handles unsubscription automatically when the `Receiver`
/// is dropped. However, we provide this ID pattern in case consumers want to track
/// active listeners in a custom external registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriberId(pub Uuid);

impl SubscriberId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for SubscriberId {
    fn default() -> Self {
        Self::new()
    }
}

/// A highly concurrent, thread-safe EventBus.
///
/// Solves the Go version's unsubscription memory leak by utilizing `tokio::sync::broadcast`.
/// Rust's Drop semantics ensure that when a `Receiver` is dropped, it is automatically
/// unsubscribed from the channel with zero memory leak risk.
#[derive(Debug, Clone)]
pub struct EventBus<T: EventPayload> {
    sender: broadcast::Sender<Event<T>>,
}

impl<T: EventPayload> EventBus<T> {
    /// Creates a new `EventBus` with a specified capacity.
    ///
    /// If receivers fall behind the sender by more than `capacity` messages,
    /// they will encounter a `RecvError::Lagged` error, providing explicit backpressure visibility.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }

    /// Publishes an event to all active subscribers.
    ///
    /// Returns the number of active subscribers that received the message.
    /// If there are no subscribers, it returns an error `SendError`, which can safely be ignored
    /// if zero subscribers is an acceptable state in your application.
    pub fn publish(&self, event: Event<T>) -> Result<usize, broadcast::error::SendError<Event<T>>> {
        self.sender.send(event)
    }

    /// Subscribes to the event bus, returning a `broadcast::Receiver`.
    ///
    /// # Memory Safety
    /// When the returned `broadcast::Receiver` is dropped, the subscription is automatically
    /// removed from the bus. No manual `unsubscribe` call is required!
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<Event<T>> {
        self.sender.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub enum TestEvent {
        UserCreated { username: String },
    }
    impl EventPayload for TestEvent {
        fn event_type(&self) -> &'static str {
            "TestEvent"
        }
    }

    #[tokio::test]
    async fn test_bus_publish_subscribe_and_drop() {
        let bus = EventBus::<TestEvent>::new(10);
        let mut rx1 = bus.subscribe();
        let mut rx2 = bus.subscribe();

        let event = Event::new(
            "user-1",
            1,
            TestEvent::UserCreated {
                username: "Alice".into(),
            },
        );

        // 2 receivers
        assert_eq!(bus.publish(event.clone()).unwrap(), 2);

        // both get the event
        assert_eq!(rx1.recv().await.unwrap(), event);
        assert_eq!(rx2.recv().await.unwrap(), event);

        // Drop one receiver (simulating unsubscription memory leak FIX)
        drop(rx1);

        let event2 = Event::new(
            "user-2",
            2,
            TestEvent::UserCreated {
                username: "Bob".into(),
            },
        );

        // Now only 1 receiver gets it
        assert_eq!(bus.publish(event2.clone()).unwrap(), 1);
        assert_eq!(rx2.recv().await.unwrap(), event2);
    }
}
