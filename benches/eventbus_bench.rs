use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use rust_eventbus::{Event, EventBus, EventPayload};
use serde::{Deserialize, Serialize};
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

pub fn bench_eventbus_publish(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("EventBus Publish");

    for subscribers_count in [1, 10, 100].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(subscribers_count),
            subscribers_count,
            |b, &sc| {
                let bus = EventBus::<BenchEvent>::new(1024);
                let mut receivers = Vec::new();
                for _ in 0..sc {
                    receivers.push(bus.subscribe());
                }

                let event = Event::new(
                    "bench-1",
                    1,
                    BenchEvent::BenchCreated {
                        payload: "some data".into(),
                    },
                );

                b.to_async(&rt).iter(|| async {
                    black_box(bus.publish(black_box(event.clone())).unwrap());
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_eventbus_publish);
criterion_main!(benches);
