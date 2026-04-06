use serde::{Deserialize, Serialize};
use crate::event::EventPayload;

/// Events related to the HTML parsing process.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", content = "data")]
pub enum ParserEvent {
    /// A request to parse specific HTML content.
    ParseRequested {
        url: String,
        html: String,
    },
    /// Emitted when a page has been successfully parsed.
    PageParsed {
        url: String,
        title: String,
        markdown: String,
        links: Vec<String>,
        /// Extracted structured data (LD-JSON, etc.)
        structured_data: Vec<serde_json::Value>,
        /// Extracted metadata (Meta tags, OpenGraph, etc.)
        metadata: std::collections::HashMap<String, String>,
    },
    /// Emitted when a parse attempt fails.
    ParseFailed {
        url: String,
        error: String,
    },
}

impl EventPayload for ParserEvent {
    fn event_type(&self) -> &'static str {
        match self {
            ParserEvent::ParseRequested { .. } => "ParserEvent::ParseRequested",
            ParserEvent::PageParsed { .. } => "ParserEvent::PageParsed",
            ParserEvent::ParseFailed { .. } => "ParserEvent::ParseFailed",
        }
    }
}
