use crate::{
    app_event::{AppEvent, GraphEvent},
    crawler::event::CrawlerEvent,
    embedding::event::EmbeddingEvent,
    event::Event,
    projection::{Projection, ProjectionError},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::debug;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    pub metadata: HashMap<String, String>,
    pub embedding: Option<Vec<f32>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Edge {
    pub from: String,
    pub to: String,
    pub relation: String,
    pub weight: f32,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct GraphState {
    pub nodes: HashMap<String, Node>,
    /// Keyed by "{from}|{to}|{relation}" for O(1) deduplication.
    pub edges: HashMap<String, Edge>,
}

impl GraphState {
    fn edge_key(from: &str, to: &str, relation: &str) -> String {
        format!("{}|{}|{}", from, to, relation)
    }

    fn insert_edge(&mut self, from: String, to: String, relation: String, weight: f32) {
        let key = Self::edge_key(&from, &to, &relation);
        self.edges.entry(key).or_insert(Edge {
            from,
            to,
            relation,
            weight,
        });
    }
}

pub struct GraphProjection;

impl Projection<AppEvent, GraphState> for GraphProjection {
    fn name(&self) -> &'static str {
        "graph_projection"
    }

    fn handle(
        &self,
        state: &mut GraphState,
        event: &Event<AppEvent>,
    ) -> Result<(), ProjectionError> {
        match &event.payload {
            // 1. Handle explicit Node creation or updates
            AppEvent::Graph(GraphEvent::NodeCreated { id, metadata }) => {
                state
                    .nodes
                    .entry(id.clone())
                    .and_modify(|n| n.metadata.extend(metadata.clone()))
                    .or_insert_with(|| Node {
                        id: id.clone(),
                        metadata: metadata.clone(),
                        embedding: None,
                    });
            }

            // 2. Handle automatic node creation from Crawler
            AppEvent::Crawler(CrawlerEvent::PageIngested { url, title, .. }) => {
                let mut metadata = HashMap::new();
                metadata.insert("title".to_string(), title.clone());
                metadata.insert("source".to_string(), "crawler".to_string());

                state
                    .nodes
                    .entry(url.clone())
                    .and_modify(|n| n.metadata.extend(metadata.clone()))
                    .or_insert_with(|| Node {
                        id: url.clone(),
                        metadata: metadata.clone(),
                        embedding: None,
                    });
            }

            // 3. Handle explicit Edge additions
            AppEvent::Graph(GraphEvent::EdgeAdded {
                from,
                to,
                relation,
                weight,
            }) => {
                state.insert_edge(from.clone(), to.clone(), relation.clone(), *weight);
            }

            // 4. Handle Embeddings for similarity logic
            AppEvent::Embedding(EmbeddingEvent::EmbeddingExtracted { id, embedding }) => {
                let node = state.nodes.entry(id.clone()).or_insert_with(|| {
                    debug!(
                        "GraphProjection: creating missing node for embedding id={}",
                        id
                    );
                    Node {
                        id: id.clone(),
                        metadata: HashMap::new(),
                        embedding: None,
                    }
                });
                node.embedding = Some(embedding.clone());

                // Find top-K most similar nodes to avoid O(N²) edge explosion.
                // With N=1000 chunks, uncapped similarity would create ~500k edges (~75MB).
                const MAX_SIMILAR_PER_NODE: usize = 5;

                let mut top_similar: Vec<(String, f32)> = Vec::with_capacity(MAX_SIMILAR_PER_NODE);
                for (other_id, other_node) in &state.nodes {
                    if other_id == id
                        || other_node.embedding.is_none()
                        || state.edges.contains_key(&GraphState::edge_key(
                            id,
                            other_id,
                            "similar_to",
                        ))
                    {
                        continue;
                    }

                    let other_emb = other_node.embedding.as_ref().unwrap();
                    // L2-normalized vectors: cosine similarity == dot product
                    let sim: f32 = embedding
                        .iter()
                        .zip(other_emb.iter())
                        .map(|(x, y)| x * y)
                        .sum();
                    if sim <= 0.85 {
                        continue;
                    }

                    if top_similar.len() < MAX_SIMILAR_PER_NODE {
                        top_similar.push((other_id.clone(), sim));
                        continue;
                    }

                    if let Some((min_idx, (_, min_sim))) =
                        top_similar.iter().enumerate().min_by(|(_, a), (_, b)| {
                            a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
                        })
                    {
                        if sim > *min_sim {
                            top_similar[min_idx] = (other_id.clone(), sim);
                        }
                    }
                }

                for (other_id, sim) in top_similar {
                    state.insert_edge(id.clone(), other_id, "similar_to".to_string(), sim);
                }
            }
            _ => {}
        }
        Ok(())
    }
}
