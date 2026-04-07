use anyhow::Result;
use rust_eventbus::{Event, EventBus};
use rust_eventbus::crawler::{CrawlerConfig, CrawlerEvent, CrawlerService};
use rust_eventbus::app_event::AppEvent;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    // Setup tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into())
                .add_directive("chromiumoxide=error".parse().unwrap())
        )
        .init();

    // 1. Create an EventBus for AppEvents
    let bus = Arc::new(EventBus::<AppEvent>::new(100));

    // 2. Initialize the CrawlerService with headless=false and 2 runners
    let config = CrawlerConfig {
        concurrency: 2,
        headless: false,
        ..Default::default()
    };
    let service = CrawlerService::new(Arc::clone(&bus), config);

    // 3. Start the service in the background
    tokio::spawn(async move {
        if let Err(e) = service.run().await {
            eprintln!("Crawler service error: {:?}", e);
        }
    });

    // 4. Subscribe to the results
    let mut rx = bus.subscribe();
    tokio::spawn(async move {
        while let Ok(event) = rx.recv().await {
            match &event.payload {
                AppEvent::Crawler(CrawlerEvent::PageIngested { url, title, chunks, .. }) => {
                    info!("--- CRAWL SUCCESS: {} ---", title);
                    info!("URL: {}", url);
                    info!("Chunks: {}", chunks.len());
                }
                AppEvent::Crawler(CrawlerEvent::CrawlFailed { url, error }) => {
                    info!("--- CRAWL FAILED: {} ---", url);
                    info!("Error: {}", error);
                }
                _ => {} 
            }
        }
    });

    // Wait for the service to be ready
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // 5. Trigger multiple crawls to demonstrate queueing (since concurrency is 2)
    let urls = vec![
        "https://www.elgiganten.se",
        "https://www.google.se",
        "https://www.github.com",
    ];

    for (i, url) in urls.into_iter().enumerate() {
        info!("Triggering crawl #{} for: {}", i + 1, url);
        let crawl_request = Event::new(
            format!("crawler-demo-{}", i),
            1,
            AppEvent::Crawler(CrawlerEvent::CrawlRequested { 
                url: url.to_string(),
                wait_selector: if url.contains("elgiganten") { Some("main[id='main-content']".into()) } else { None },
            }),
        );
        bus.publish(crawl_request)?;
    }

    // Keep the app running for a while to see the results
    info!("Waiting for ingestion (60s timeout)...");
    tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;

    Ok(())
}
