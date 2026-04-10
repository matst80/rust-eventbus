use serde::{Deserialize, Serialize};
use crate::event::EventPayload;
use crate::parser::Chunk;

/// Events related to the web crawling process.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", content = "data")]
pub enum CrawlerEvent {
    /// A request to crawl a specific URL.
    CrawlRequested {
        url: String,
        /// Optional selector to wait for (for SPAs)
        wait_selector: Option<String>,
        /// Optional per-request chunk limit override.
        max_chunks: Option<usize>,
    },
    /// Emitted when a page has been successfully crawled and pre-processed.
    /// The raw HTML is converted to chunks and links inside the crawler before publishing,
    /// so that large HTML strings never enter the event bus ring-buffer.
    PageIngested {
        url: String,
        title: String,
        /// All hyperlinks extracted from the page (before domain filtering).
        links: Vec<String>,
        /// Markdown chunks ready for embedding. Each chunk is ≤ max_chunk_size bytes.
        chunks: Vec<Chunk>,
    },
    /// Emitted when a crawl attempt fails.
    CrawlFailed { 
        url: String, 
        error: String 
    },
}

impl EventPayload for CrawlerEvent {
    fn event_type(&self) -> &'static str {
        match self {
            CrawlerEvent::CrawlRequested { .. } => "CrawlerEvent::CrawlRequested",
            CrawlerEvent::PageIngested { .. } => "CrawlerEvent::PageIngested",
            CrawlerEvent::CrawlFailed { .. } => "CrawlerEvent::CrawlFailed",
        }
    }
}
