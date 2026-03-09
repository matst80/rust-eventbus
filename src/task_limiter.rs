use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;

/// Limiter for concurrent async tasks.
/// Used to prevent resource exhaustion on limited hardware like Raspberry Pi.
#[derive(Clone, Debug)]
pub struct TaskLimiter {
    semaphore: Arc<Semaphore>,
}

impl TaskLimiter {
    pub fn new(max_tasks: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_tasks)),
        }
    }

    /// Spawns a task and holds a permit until the task completes.
    /// If the limit is reached, this will wait until a slot is available.
    pub async fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("Semaphore should not be closed");
            
        tokio::spawn(async move {
            let res = future.await;
            // Permit is dropped here when the task finishes
            drop(permit);
            res
        })
    }
    
    /// Returns a permit that must be held by the caller.
    /// Useful for tasks that are spawned externally but should be counted.
    pub async fn acquire_permit(&self) -> OwnedSemaphorePermit {
        self.semaphore.clone().acquire_owned().await.expect("Semaphore should not be closed")
    }
}
