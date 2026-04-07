use crate::event::EventPayload;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum EmbeddingEvent {
    TextChunkCreated { id: String, text: String },
    EmbeddingExtracted { id: String, embedding: Vec<f32> },
}

impl EventPayload for EmbeddingEvent {
    fn event_type(&self) -> &'static str {
        match self {
            Self::TextChunkCreated { .. } => "TextChunkCreated",
            Self::EmbeddingExtracted { .. } => "EmbeddingExtracted",
        }
    }
}
