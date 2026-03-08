## 2024-03-08 - FileEventStore I/O Batching
**Learning:** Sequential fsync calls in the background writer task of the `FileEventStore` formed a massive I/O bottleneck. `file.sync_all()` was being called per `append` request rather than batching.
**Action:** When working with Tokio channels (`mpsc`) and I/O tasks, aggressively drain the channel using `try_recv()` inside the `while let Some = recv().await` loop to batch writes into a single disk synchronization. This improved append times by ~90%.
