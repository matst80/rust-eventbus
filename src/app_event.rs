use crate::crawler::event::CrawlerEvent;
use crate::embedding::event::EmbeddingEvent;
use crate::event::EventPayload;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", content = "data")]
pub enum AppEvent {
    Crawler(CrawlerEvent),
    Embedding(EmbeddingEvent),
    Graph(GraphEvent),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", content = "data")]
pub enum GraphEvent {
    NodeCreated {
        id: String,
        metadata: std::collections::HashMap<String, String>,
    },
    EdgeAdded {
        from: String,
        to: String,
        relation: String,
        weight: f32,
    },
    RequestEmbedding {
        id: String,
        content: String,
    },
}

impl EventPayload for AppEvent {
    fn event_type(&self) -> &'static str {
        match self {
            AppEvent::Crawler(e) => e.event_type(),
            AppEvent::Embedding(e) => e.event_type(),
            AppEvent::Graph(e) => e.event_type(),
        }
    }
}

impl EventPayload for GraphEvent {
    fn event_type(&self) -> &'static str {
        match self {
            GraphEvent::NodeCreated { .. } => "GraphEvent::NodeCreated",
            GraphEvent::EdgeAdded { .. } => "GraphEvent::EdgeAdded",
            GraphEvent::RequestEmbedding { .. } => "GraphEvent::RequestEmbedding",
        }
    }
}

impl From<CrawlerEvent> for AppEvent {
    fn from(e: CrawlerEvent) -> Self {
        AppEvent::Crawler(e)
    }
}

impl From<EmbeddingEvent> for AppEvent {
    fn from(e: EmbeddingEvent) -> Self {
        AppEvent::Embedding(e)
    }
}

impl From<GraphEvent> for AppEvent {
    fn from(e: GraphEvent) -> Self {
        AppEvent::Graph(e)
    }
}
