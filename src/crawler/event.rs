use serde::{Deserialize, Serialize};
use crate::event::EventPayload;

/// Events related to the web crawling process.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", content = "data")]
pub enum CrawlerEvent {
    /// A request to crawl a specific URL.
    CrawlRequested { 
        url: String,
        /// Optional selector to wait for (for SPAs)
        wait_selector: Option<String>,
    },
    /// Emitted when a page has been successfully crawled.
    PageIngested { 
        url: String, 
        /// The HTML content of the page.
        content: String,
        /// The title of the page.
        title: String,
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
