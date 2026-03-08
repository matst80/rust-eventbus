use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rust_eventbus::store::{EventStore, FileEventStore};
use rust_eventbus::{Event, EventPayload};
use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use tokio::runtime::Runtime;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum BenchEvent {
    BenchCreated { payload: String },
}

impl EventPayload for BenchEvent {
    fn event_type(&self) -> &'static str {
        "BenchEvent"
    }
}

pub fn bench_store_append(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let temp_dir = std::env::temp_dir().join(format!(
        "store_bench_{}",
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros()
    ));
    std::fs::create_dir_all(&temp_dir).unwrap();
    let file_path = temp_dir.join("event_store.bin");

    let store = rt.block_on(async { FileEventStore::new(&file_path).await.unwrap() });

    let mut group = c.benchmark_group("EventStore Append");
    group.bench_function("append 1 event", |b| {
        let event = Event::new(
            "entity-1",
            1,
            BenchEvent::BenchCreated {
                payload: "some data".into(),
            },
        );
        b.to_async(&rt).iter(|| async {
            black_box(store.append(black_box(vec![event.clone()])).await.unwrap());
        });
    });
    group.finish();

    let _ = std::fs::remove_dir_all(&temp_dir);
}

criterion_group!(benches, bench_store_append);
criterion_main!(benches);
