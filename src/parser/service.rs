use anyhow::{Result};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info, debug};
use scraper::Html;

use crate::bus::EventBus;
use crate::event::Event;
use crate::crawler::event::CrawlerEvent;
use super::event::ParserEvent;
use super::extractor::Extractor;

pub struct ParserService {
    bus: Arc<EventBus<CrawlerEvent>>,
    parser_bus: Arc<EventBus<ParserEvent>>,
    extra_excludes: Vec<String>,
}

impl ParserService {
    pub fn new(bus: Arc<EventBus<CrawlerEvent>>, parser_bus: Arc<EventBus<ParserEvent>>) -> Self {
        Self { bus, parser_bus, extra_excludes: Vec::new() }
    }

    /// Sets additional CSS selectors to exclude from markdown extraction.
    pub fn with_excludes(mut self, excludes: Vec<String>) -> Self {
        self.extra_excludes = excludes;
        self
    }

    pub async fn run(&self) -> Result<()> {
        info!("Starting ParserService...");

        let (tx, mut rx_internal) = mpsc::channel::<Event<CrawlerEvent>>(100);
        let mut rx = self.bus.subscribe();

        // Worker for processing events
        let parser_bus_clone = Arc::clone(&self.parser_bus);
        let extra_excludes = self.extra_excludes.clone();
        
        tokio::spawn(async move {
            while let Some(event) = rx_internal.recv().await {
                if let CrawlerEvent::PageIngested { url, content, title } = event.payload {
                    debug!("Parsing: {}", url);
                    
                    let html = Html::parse_document(&content);
                    let links = Extractor::extract_links(&html, &url);
                    let structured_data = Extractor::extract_structured_data(&html);
                    let metadata = Extractor::extract_metadata(&html);
                    
                    let exclude_refs: Vec<&str> = extra_excludes.iter().map(|s| s.as_str()).collect();
                    let markdown = Extractor::to_markdown(&content, &url, &exclude_refs);

                    let result_event = Event::new(
                        event.aggregate_id.clone(),
                        event.sequence_num + 1,
                        ParserEvent::PageParsed {
                            url,
                            title,
                            markdown,
                            links,
                            structured_data,
                            metadata,
                        },
                    );

                    let _ = parser_bus_clone.publish(result_event);
                }
            }
        });

        // Main subscriber loop
        loop {
            match rx.recv().await {
                Ok(event) => {
                    if let CrawlerEvent::PageIngested { .. } = &event.payload {
                        if let Err(e) = tx.send(event).await {
                            error!("Failed to queue parse task: {:?}", e);
                        }
                    }
                }
                Err(e) => {
                    error!("ParserService subscription error: {:?}", e);
                    break;
                }
            }
        }

        Ok(())
    }
}
