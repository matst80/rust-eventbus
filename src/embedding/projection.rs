use crate::{
    app_event::AppEvent,
    event::Event,
    projection::{Projection, ProjectionError},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::event::EmbeddingEvent;

pub struct EmbeddingProjection;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct EmbeddingState {
    pub embeddings: HashMap<String, Vec<f32>>,
}

impl Projection<AppEvent, EmbeddingState> for EmbeddingProjection {
    fn name(&self) -> &'static str {
        "embedding_projection"
    }

    fn handle(
        &self,
        state: &mut EmbeddingState,
        event: &Event<AppEvent>,
    ) -> Result<(), ProjectionError> {
        if let AppEvent::Embedding(EmbeddingEvent::EmbeddingExtracted { id, embedding }) =
            &event.payload
        {
            state.embeddings.insert(id.clone(), embedding.clone());
        }

        Ok(())
    }
}
