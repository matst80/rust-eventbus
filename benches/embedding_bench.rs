use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rust_eventbus::embedding::{downloader::ModelDownloader, onnx::OnnxEmbeddingService};
use tokio::runtime::Runtime;
use std::sync::Arc;

pub fn bench_embedding(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let model_cache = temp_dir.path().join("models");
    
    // Download model outside of iteration
    let (model_path, tokenizer_path) = rt.block_on(async {
        let downloader = ModelDownloader::new(&model_cache);
        downloader.get_bge_small().await.expect("Failed to download model for bench")
    });
    
    let service = Arc::new(OnnxEmbeddingService::new(model_path, tokenizer_path).expect("Failed to init GPU service"));
    
    let mut group = c.benchmark_group("Embedding Service");
    
    let sample_text = "This is a sample sentence for embedding benchmarking. It should be long enough to represent real usage.".to_string();
    
    // Bench single embedding
    group.bench_function("Single Embedding", |b| {
        b.iter(|| {
            black_box(service.embed(black_box(&sample_text)).unwrap());
        });
    });

    // Bench batch embedding (16)
    let batch_texts: Vec<String> = (0..16).map(|_| sample_text.clone()).collect();
    group.bench_function("Batch Embedding (16)", |b| {
        b.iter(|| {
            black_box(service.embed_batch(black_box(&batch_texts)).unwrap());
        });
    });

    group.finish();
}

criterion_group!(benches, bench_embedding);
criterion_main!(benches);
