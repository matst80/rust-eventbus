use anyhow::Result;
use rust_eventbus::{
    embedding::OnnxEmbeddingService,
    graph::GraphState,
};
use std::sync::Arc;

/// Represents a result in our retrieval pipeline
#[derive(Debug, Clone)]
struct ScoredResult {
    id: String,
    content: String,
    vector_score: f32,
    rerank_score: f32,
}

/// A Reranker Trait to show how it should be implemented.
/// Usually, this would call a "Cross-Encoder" model.
trait Reranker {
    fn rerank_batch(&self, query: &str, results: &mut Vec<ScoredResult>);
}

/// A heuristic reranker that boosts results containing exact keywords from the query.
/// This simulates why a reranker is more precise than simple embedding similarity.
struct KeywordBoostReranker;

impl Reranker for KeywordBoostReranker {
    fn rerank_batch(&self, query: &str, results: &mut Vec<ScoredResult>) {
        let query_terms: Vec<&str> = query.split_whitespace().map(|s| s.trim_matches(|c: char| !c.is_alphanumeric())).filter(|s| !s.is_empty()).collect();
        
        for res in results.iter_mut() {
            let mut boost = 0.0;
            let content_lower = res.content.to_lowercase();
            
            for term in &query_terms {
                if content_lower.contains(&term.to_lowercase()) {
                    // Exact keyword matches get a significant boost in this simulation
                    boost += 0.1; 
                }
            }
            
            // Final score is a combination (Stage 2)
            res.rerank_score = res.vector_score + boost;
        }

        // Sort by the new reranked score
        results.sort_by(|a, b| b.rerank_score.partial_cmp(&a.rerank_score).unwrap());
    }
}

pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm_a == 0.0 || norm_b == 0.0 { 0.0 } else { dot / (norm_a * norm_b) }
}

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Setup paths
    let model_path = "data/models/bge-small-en-v1.5.onnx";
    let tokenizer_path = "data/models/tokenizer.json";
    let state_path = "examples/outputs/graph_state.json";

    if !std::path::Path::new(model_path).exists() || !std::path::Path::new(state_path).exists() {
        println!("Please run 'graph_pipeline' first to generate models and graph state.");
        return Ok(());
    }

    // 2. Load services
    let onnx = Arc::new(OnnxEmbeddingService::new(model_path, tokenizer_path)?);
    let json = std::fs::read_to_string(state_path)?;
    let state: GraphState = serde_json::from_str(&json)?;

    let query = "multithreaded web server";
    println!("--- Query: '{}' ---", query);

    // --- STAGE 1: Retrieval (Bi-Encoder / Embeddings) ---
    // Fast but sometimes noisy
    println!("\n[Stage 1] Performing Vector Search (Bi-Encoder)...");
    let query_emb = onnx.embed(query)?;
    let mut initial_results = Vec::new();

    for (id, node) in &state.nodes {
        if let Some(node_emb) = &node.embedding {
            let score = cosine_similarity(&query_emb, node_emb);
            let content = node.metadata.get("content").cloned().unwrap_or_default();
            
            initial_results.push(ScoredResult {
                id: id.clone(),
                content,
                vector_score: score,
                rerank_score: 0.0,
            });
        }
    }

    // Sort by vector score
    initial_results.sort_by(|a, b| b.vector_score.partial_cmp(&a.vector_score).unwrap());
    
    // Take Top 10 for reranking (This is the "Two-Stage" pattern)
    let mut top_n: Vec<ScoredResult> = initial_results.into_iter().take(10).collect();

    println!("Vector Top 3:");
    for res in top_n.iter().take(3) {
        println!("  {:.4} | {}", res.vector_score, res.id);
    }

    // --- STAGE 2: Reranking (Cross-Encoder / Heuristic) ---
    // Slower, but examines the relationship between query and text more deeply
    println!("\n[Stage 2] Performing Reranking (Refining the Top 10)...");
    let reranker = KeywordBoostReranker;
    reranker.rerank_batch(query, &mut top_n);

    println!("Reranked Top 3 (New order!):");
    for (i, res) in top_n.iter().take(3).enumerate() {
        println!("  #{}: {:.4} (Vector: {:.4}) | {}", i+1, res.rerank_score, res.vector_score, res.id);
        // Note: You can see how a result with a lower vector score might "climb" 
        // to the top if the reranker finds it's actually more relevant.
    }

    println!("\n--- Why use this? ---");
    println!("1. Vector search is great for finding 'nearby' concepts.");
    println!("2. Reranking picks the 'exact' answer from those nearby concepts.");
    println!("3. This pipeline balances speed (Stage 1) with accuracy (Stage 2).");

    Ok(())
}
