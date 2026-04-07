use anyhow::Result;
use rust_eventbus::{
    embedding::OnnxEmbeddingService,
    graph::GraphState,
};
use std::io::{self, Write};

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Initialize Embedding Service
    let model_path = "data/models/bge-small-en-v1.5.onnx";
    let tokenizer_path = "data/models/tokenizer.json";
    
    if !std::path::Path::new(model_path).exists() {
        println!("Error: Embedding model not found at {}. Please run graph_pipeline first or ensure models are downloaded.", model_path);
        return Ok(());
    }
    
    let onnx_service = OnnxEmbeddingService::new(model_path, tokenizer_path)?;
    println!("Embedding service initialized.");

    // 2. Load Graph State
    let state_path = "examples/outputs/graph_state.json";
    if !std::path::Path::new(state_path).exists() {
        println!("Error: Graph state not found at {}. Please run graph_pipeline first.", state_path);
        return Ok(());
    }
    
    let json = std::fs::read_to_string(state_path)?;
    let state: GraphState = serde_json::from_str(&json)?;
    println!("Loaded graph with {} nodes and {} edges.", state.nodes.len(), state.edges.len());

    // 3. Search Loop
    loop {
        print!("\nEnter search query (or 'exit'): ");
        io::stdout().flush()?;
        
        let mut query = String::new();
        io::stdin().read_line(&mut query)?;
        let query = query.trim();
        
        if query == "exit" || query.is_empty() {
            break;
        }

        println!("Searching for: '{}'...", query);
        let query_emb = onnx_service.embed(query)?;

        // Calculate similarities
        let mut results = Vec::new();
        for (id, node) in &state.nodes {
            if let Some(node_emb) = &node.embedding {
                let score = cosine_similarity(&query_emb, node_emb);
                results.push((score, id, node));
            }
        }

        // Sort by similarity descending
        results.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

        // Show top 3 results
        println!("\nTop 3 Search Results:");
        for (score, id, node) in results.into_iter().take(3) {
            println!("\n[ Score: {:.4} ] ID: {}", score, id);
            println!("Page:    {}", node.metadata.get("page_title").or(node.metadata.get("title")).unwrap_or(&"Unknown".to_string()));
            
            if let Some(section) = node.metadata.get("section") {
                println!("Section: {}", section);
            }
            
            if let Some(content) = node.metadata.get("content") {
                // Peek at the content
                
                println!("\n--- Chunk ---\n{}\n----------------------", content);
            }
        }
    }

    Ok(())
}

fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm_a == 0.0 || norm_b == 0.0 { 0.0 } else { dot / (norm_a * norm_b) }
}
