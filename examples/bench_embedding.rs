use std::sync::Arc;
use std::time::Instant;
use rust_eventbus::embedding::{downloader::ModelDownloader, onnx::OnnxEmbeddingService};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    ort::init().with_name("embedding").commit();
    
    let cache_dir = std::path::PathBuf::from("./data/models");
    let downloader = ModelDownloader::new(&cache_dir);
    
    println!("Fetching model and tokenizer...");
    let (model_path, tokenizer_path) = downloader.get_bge_small().await?;
    
    println!("Initializing ONNX Service (trying CUDA)...");
    let service = Arc::new(OnnxEmbeddingService::new(model_path, tokenizer_path)?);
    
    let text = "This is a sample text for benchmarking the embedding extraction process. We want to see how many embeddings per second we can generate using the RTX 2080.";
    
    // Warmup
    println!("Warming up...");
    for _ in 0..10 {
        let _ = service.embed(text)?;
    }
    
    // Single inference benchmark
    println!("Benchmarking single inference (latency)...");
    let start = Instant::now();
    let iterations = 100;
    for _ in 0..iterations {
        let _ = service.embed(text)?;
    }
    let duration = start.elapsed();
    println!("Single Latency: {:?}", duration / iterations as u32);
    println!("Single Throughput: {:.2} req/s", iterations as f64 / duration.as_secs_f64());
    
    // Batch inference benchmark
    let batch_sizes = [1, 4, 8, 16, 32, 64];
    for &size in &batch_sizes {
        let batch: Vec<String> = (0..size).map(|_| text.to_string()).collect();
        println!("\nBenchmarking batch size {}...", size);
        
        let start = Instant::now();
        let iters = 50;
        for _ in 0..iters {
            let _ = service.embed_batch(&batch);
        }
        let duration = start.elapsed();
        let total_reqs = iters * size;
        println!("Batch {} Latency: {:?}", size, duration / iters as u32);
        println!("Batch {} Throughput: {:.2} req/s", size, total_reqs as f64 / duration.as_secs_f64());
    }
    
    Ok(())
}
